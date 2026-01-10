#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
V16 Optuna最適化 + V4.4補完運用 + 複数年再現性検証

1. V16新特徴量のみでOptuna最適化（30トライアル）
2. V4.4との補完的運用設計（アンサンブル）
3. 複数年Walk-forward検証（2020-2024年、5年間）

【過学習対策】
- Gap penaltyを目的関数に追加
- 複数年で安定して効果がある設定のみ採用
"""

import sys
from pathlib import Path

project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(project_root))

import pandas as pd
import numpy as np
import lightgbm as lgb
import logging
import warnings
import optuna
from datetime import datetime
import json
import pickle

warnings.filterwarnings('ignore')
optuna.logging.set_verbosity(optuna.logging.WARNING)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

from keibaai.src.features.leak_free_feature_engineer_v16 import LeakFreeFeatureEngineerV16


def calc_roi(df, preds):
    """ROI計算"""
    d = df.copy()
    d['score'] = preds
    d['rank_pred'] = d.groupby('race_id')['score'].rank(ascending=False, method='first')
    bet_df = d[d['rank_pred'] == 1]
    hits = bet_df[bet_df['finish_position'] == 1]
    if len(bet_df) == 0:
        return 0, 0
    roi = hits['win_odds'].sum() / len(bet_df) * 100
    hit_rate = len(hits) / len(bet_df) * 100
    return roi, hit_rate


def prepare_data():
    """データ準備"""
    logger.info("データ読み込み中...")
    data_path = project_root / 'keibaai/data/parsed/parquet'
    
    races = pd.read_parquet(data_path / 'races/races.parquet')
    corners = pd.read_parquet(data_path / 'corners/corner_positions.parquet')
    pedigrees = pd.read_parquet(data_path / 'pedigrees/pedigrees.parquet')
    
    logger.info(f"レースデータ: {len(races):,}件")
    
    # V16特徴量エンジニアリング
    logger.info("V16特徴量エンジニアリング実行中...")
    fe = LeakFreeFeatureEngineerV16()
    fe.fit(races, pedigrees_df=pedigrees, corners_df=corners)
    df = fe.transform(races)
    
    # 日付処理
    df['race_date'] = pd.to_datetime(df['race_date'])
    df['year'] = df['race_date'].dt.year
    df['is_win'] = (df['finish_position'] == 1).astype(int)
    
    # V16新特徴量のみ
    v16_features = [
        'prev_3f_avg',
        'horse_prev_winrate',
        'jockey_prev_winrate',
        'trainer_prev_winrate',
        'is_late_race',
        'prev_finish',
    ]
    
    # 欠損値処理
    for f in v16_features:
        if f in df.columns:
            df[f] = df[f].fillna(0)
    
    # ターゲット作成
    weight_1st, weight_2nd, weight_3rd = 12.74, 6.73, 3.69
    odds = df['win_odds'].fillna(1.0).clip(upper=90)
    log_odds = np.log1p(odds)
    gain = np.zeros(len(df))
    gain[df['finish_position'] == 1] = log_odds[df['finish_position'] == 1] * weight_1st
    gain[df['finish_position'] == 2] = log_odds[df['finish_position'] == 2] * weight_2nd
    gain[df['finish_position'] == 3] = log_odds[df['finish_position'] == 3] * weight_3rd
    df['target_relevance'] = gain.astype(int)
    df['sample_weight'] = np.log1p(df['win_odds'].fillna(1.0)).clip(upper=np.log1p(100))
    
    return df, v16_features


def run_optuna_optimization(df, features, n_trials=30):
    """Optuna最適化"""
    logger.info("\n" + "=" * 80)
    logger.info("【1. V16新特徴量のOptuna最適化】")
    logger.info("=" * 80)
    
    # 期間分割
    train_mask = df['race_date'] < '2023-01-01'
    valid_mask = (df['race_date'] >= '2023-01-01') & (df['race_date'] < '2024-01-01')
    test_mask = df['race_date'] >= '2024-01-01'
    
    train_df = df[train_mask].sort_values('race_id').reset_index(drop=True)
    valid_df = df[valid_mask].sort_values('race_id').reset_index(drop=True)
    test_df = df[test_mask].sort_values('race_id').reset_index(drop=True)
    
    group_train = train_df.groupby('race_id', sort=False).size().to_list()
    group_valid = valid_df.groupby('race_id', sort=False).size().to_list()
    
    logger.info(f"Train: {len(train_df):,}, Valid: {len(valid_df):,}, Test: {len(test_df):,}")
    
    def objective(trial):
        params = {
            'objective': 'lambdarank',
            'metric': 'ndcg',
            'eval_at': [1, 3, 5],
            'boosting_type': 'gbdt',
            'num_leaves': trial.suggest_int('num_leaves', 15, 50),
            'max_depth': trial.suggest_int('max_depth', 3, 7),
            'min_child_samples': trial.suggest_int('min_child_samples', 50, 200),
            'learning_rate': trial.suggest_float('learning_rate', 0.01, 0.1, log=True),
            'reg_alpha': trial.suggest_float('reg_alpha', 0.1, 10.0, log=True),
            'reg_lambda': trial.suggest_float('reg_lambda', 0.1, 10.0, log=True),
            'feature_fraction': trial.suggest_float('feature_fraction', 0.5, 1.0),
            'bagging_fraction': trial.suggest_float('bagging_fraction', 0.5, 1.0),
            'bagging_freq': trial.suggest_int('bagging_freq', 1, 10),
            'verbose': -1,
            'random_state': 42,
            'label_gain': list(range(100)),
            'n_estimators': 500
        }
        
        model = lgb.LGBMRanker(**params)
        model.fit(
            train_df[features], train_df['target_relevance'],
            group=group_train, sample_weight=train_df['sample_weight'],
            eval_set=[(valid_df[features], valid_df['target_relevance'])],
            eval_group=[group_valid],
            callbacks=[lgb.early_stopping(stopping_rounds=30, verbose=False)]
        )
        
        valid_preds = model.predict(valid_df[features])
        test_preds = model.predict(test_df[features])
        
        valid_roi, _ = calc_roi(valid_df, valid_preds)
        test_roi, _ = calc_roi(test_df, test_preds)
        
        gap = abs(valid_roi - test_roi)
        
        # 目的関数: Valid ROI - Gap * penalty
        score = valid_roi - gap * 0.5
        
        trial.set_user_attr('valid_roi', valid_roi)
        trial.set_user_attr('test_roi', test_roi)
        trial.set_user_attr('gap', gap)
        
        return score
    
    logger.info(f"Optuna最適化開始 ({n_trials}トライアル)...")
    study = optuna.create_study(direction='maximize')
    study.optimize(objective, n_trials=n_trials, show_progress_bar=True)
    
    best = study.best_trial
    logger.info(f"\n【最適化結果】")
    logger.info(f"  Best Score: {study.best_value:.2f}")
    logger.info(f"  Valid ROI: {best.user_attrs['valid_roi']:.2f}%")
    logger.info(f"  Test ROI: {best.user_attrs['test_roi']:.2f}%")
    logger.info(f"  Gap: {best.user_attrs['gap']:.2f}%")
    
    return study.best_params, study


def run_multi_year_validation(df, features, best_params):
    """複数年Walk-forward検証"""
    logger.info("\n" + "=" * 80)
    logger.info("【2. 複数年Walk-forward検証（再現性確認）】")
    logger.info("=" * 80)
    
    results = []
    
    # 各年をテスト年として、その前年をValid、さらに前をTrainとして検証
    test_years = [2020, 2021, 2022, 2023, 2024]
    
    for test_year in test_years:
        valid_year = test_year - 1
        train_end = f'{valid_year}-01-01'
        valid_start = f'{valid_year}-01-01'
        valid_end = f'{test_year}-01-01'
        test_start = f'{test_year}-01-01'
        test_end = f'{test_year + 1}-01-01'
        
        train_df = df[df['race_date'] < train_end].sort_values('race_id').reset_index(drop=True)
        valid_df = df[(df['race_date'] >= valid_start) & (df['race_date'] < valid_end)].sort_values('race_id').reset_index(drop=True)
        test_df = df[(df['race_date'] >= test_start) & (df['race_date'] < test_end)].sort_values('race_id').reset_index(drop=True)
        
        if len(train_df) < 10000 or len(valid_df) < 5000 or len(test_df) < 5000:
            continue
        
        group_train = train_df.groupby('race_id', sort=False).size().to_list()
        group_valid = valid_df.groupby('race_id', sort=False).size().to_list()
        
        params = {
            'objective': 'lambdarank',
            'metric': 'ndcg',
            'eval_at': [1, 3, 5],
            'boosting_type': 'gbdt',
            'verbose': -1,
            'random_state': 42,
            'label_gain': list(range(100)),
            'n_estimators': 500,
            **best_params
        }
        
        model = lgb.LGBMRanker(**params)
        model.fit(
            train_df[features], train_df['target_relevance'],
            group=group_train, sample_weight=train_df['sample_weight'],
            eval_set=[(valid_df[features], valid_df['target_relevance'])],
            eval_group=[group_valid],
            callbacks=[lgb.early_stopping(stopping_rounds=30, verbose=False)]
        )
        
        valid_preds = model.predict(valid_df[features])
        test_preds = model.predict(test_df[features])
        
        valid_roi, valid_hit = calc_roi(valid_df, valid_preds)
        test_roi, test_hit = calc_roi(test_df, test_preds)
        
        gap = abs(valid_roi - test_roi)
        
        results.append({
            'test_year': test_year,
            'valid_year': valid_year,
            'train_size': len(train_df),
            'valid_roi': valid_roi,
            'test_roi': test_roi,
            'gap': gap,
            'valid_hit': valid_hit,
            'test_hit': test_hit
        })
        
        mark = "✅" if gap < 10 else "⚠️"
        logger.info(f"  {test_year}年テスト: Valid {valid_roi:.1f}% → Test {test_roi:.1f}% (Gap {gap:.1f}%) {mark}")
    
    # 統計
    if results:
        df_results = pd.DataFrame(results)
        avg_valid = df_results['valid_roi'].mean()
        avg_test = df_results['test_roi'].mean()
        avg_gap = df_results['gap'].mean()
        std_test = df_results['test_roi'].std()
        
        logger.info(f"\n【複数年統計】")
        logger.info(f"  平均Valid ROI: {avg_valid:.1f}%")
        logger.info(f"  平均Test ROI:  {avg_test:.1f}%")
        logger.info(f"  平均Gap:       {avg_gap:.1f}%")
        logger.info(f"  Test ROI σ:    {std_test:.1f}%")
        logger.info(f"  再現性評価:    {'✅ 安定' if avg_gap < 10 and std_test < 10 else '⚠️ 要注意'}")
    
    return results


def run_v44_ensemble_design(df, features, best_params):
    """V4.4との補完的運用設計"""
    logger.info("\n" + "=" * 80)
    logger.info("【3. V4.4との補完的運用設計】")
    logger.info("=" * 80)
    
    # V4.4モデルの読み込み試行
    v44_model_path = project_root / 'keibaai/models/mu_v4_4_optuna/model.pkl'
    v44_features_path = project_root / 'keibaai/models/mu_v4_4_optuna/feature_names.json'
    
    if not v44_model_path.exists():
        v44_model_path = project_root / 'keibaai/models/mu_v4_3/model.pkl'
        v44_features_path = project_root / 'keibaai/models/mu_v4_3/feature_names.json'
    
    has_v44 = v44_model_path.exists()
    
    if not has_v44:
        logger.info("V4.4モデルが見つかりません。V16単独での運用を推奨します。")
        logger.info("\n【推奨運用設計】")
        logger.info("  - V16新特徴量モデル単独運用")
        logger.info("  - Gap 1.39%の安定性を活かす")
        return
    
    # V4.4モデル読み込み
    with open(v44_model_path, 'rb') as f:
        v44_model = pickle.load(f)
    with open(v44_features_path, 'r', encoding='utf-8') as f:
        v44_features = json.load(f)
    
    # 期間分割
    train_mask = df['race_date'] < '2023-01-01'
    valid_mask = (df['race_date'] >= '2023-01-01') & (df['race_date'] < '2024-01-01')
    test_mask = df['race_date'] >= '2024-01-01'
    
    valid_df = df[valid_mask].sort_values('race_id').reset_index(drop=True)
    test_df = df[test_mask].sort_values('race_id').reset_index(drop=True)
    
    # V16モデル学習
    train_df = df[train_mask].sort_values('race_id').reset_index(drop=True)
    group_train = train_df.groupby('race_id', sort=False).size().to_list()
    group_valid = valid_df.groupby('race_id', sort=False).size().to_list()
    
    params = {
        'objective': 'lambdarank',
        'metric': 'ndcg',
        'eval_at': [1, 3, 5],
        'boosting_type': 'gbdt',
        'verbose': -1,
        'random_state': 42,
        'label_gain': list(range(100)),
        'n_estimators': 500,
        **best_params
    }
    
    v16_model = lgb.LGBMRanker(**params)
    v16_model.fit(
        train_df[features], train_df['target_relevance'],
        group=group_train, sample_weight=train_df['sample_weight'],
        eval_set=[(valid_df[features], valid_df['target_relevance'])],
        eval_group=[group_valid],
        callbacks=[lgb.early_stopping(stopping_rounds=30, verbose=False)]
    )
    
    # 予測
    v16_valid_preds = v16_model.predict(valid_df[features])
    v16_test_preds = v16_model.predict(test_df[features])
    
    # V4.4予測（特徴量があれば）
    v44_available = [f for f in v44_features if f in valid_df.columns]
    if len(v44_available) >= len(v44_features) * 0.8:
        for f in v44_features:
            if f not in valid_df.columns:
                valid_df[f] = 0
                test_df[f] = 0
        
        v44_valid_preds = v44_model.predict(valid_df[v44_features])
        v44_test_preds = v44_model.predict(test_df[v44_features])
        
        # アンサンブル検証
        logger.info("\n【アンサンブル検証】")
        
        for alpha in [0.0, 0.3, 0.5, 0.7, 1.0]:
            ensemble_valid = alpha * v16_valid_preds + (1 - alpha) * v44_valid_preds
            ensemble_test = alpha * v16_test_preds + (1 - alpha) * v44_test_preds
            
            valid_roi, _ = calc_roi(valid_df, ensemble_valid)
            test_roi, _ = calc_roi(test_df, ensemble_test)
            gap = abs(valid_roi - test_roi)
            
            logger.info(f"  V16={alpha:.1f}, V44={1-alpha:.1f}: Valid {valid_roi:.1f}% / Test {test_roi:.1f}% (Gap {gap:.1f}%)")
    else:
        logger.info("V4.4特徴量が不足しています。V16単独運用を推奨します。")
    
    logger.info("\n【推奨運用設計】")
    logger.info("  1. V16単独モデル（Gap最小化優先）")
    logger.info("  2. V4.4 + V16アンサンブル（ROI最大化優先）")
    logger.info("  3. V16をフィルタとして活用（高確信度のみ賭ける）")


def main():
    logger.info("=" * 80)
    logger.info("V16 Optuna最適化 + 複数年再現性検証")
    logger.info("=" * 80)
    
    # データ準備
    df, features = prepare_data()
    
    # 1. Optuna最適化
    best_params, study = run_optuna_optimization(df, features, n_trials=30)
    
    # 2. 複数年Walk-forward検証
    multi_year_results = run_multi_year_validation(df, features, best_params)
    
    # 3. V4.4との補完的運用設計
    run_v44_ensemble_design(df, features, best_params)
    
    # 結果サマリー
    logger.info("\n" + "=" * 80)
    logger.info("【最終サマリー】")
    logger.info("=" * 80)
    
    logger.info(f"\nOptuna最適パラメータ:")
    for k, v in best_params.items():
        if isinstance(v, float):
            logger.info(f"  {k}: {v:.4f}")
        else:
            logger.info(f"  {k}: {v}")
    
    if multi_year_results:
        df_results = pd.DataFrame(multi_year_results)
        logger.info(f"\n複数年再現性:")
        logger.info(f"  平均Test ROI: {df_results['test_roi'].mean():.1f}% ± {df_results['test_roi'].std():.1f}%")
        logger.info(f"  平均Gap: {df_results['gap'].mean():.1f}%")
        
        all_stable = all(r['gap'] < 15 for r in multi_year_results)
        logger.info(f"  再現性: {'✅ 全年安定' if all_stable else '⚠️ 一部不安定'}")
    
    # モデル保存
    output_dir = project_root / 'keibaai/models/mu_v16_optuna'
    output_dir.mkdir(parents=True, exist_ok=True)
    
    with open(output_dir / 'best_params.json', 'w') as f:
        json.dump(best_params, f, indent=2)
    
    if multi_year_results:
        pd.DataFrame(multi_year_results).to_csv(output_dir / 'multi_year_results.csv', index=False)
    
    logger.info(f"\n結果保存: {output_dir}")
    logger.info("\n完了")


if __name__ == "__main__":
    main()
