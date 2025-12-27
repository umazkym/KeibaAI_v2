#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
μ_v5統合検証: 改善版μモデルの効果検証

【目的】
アプローチ2で改善されたμモデル(V5)予測値を、アプローチ1の手法(特徴量統合)でV15に組み込み、
ROIが改善するか検証する。

【改善点】
- μモデル: TimeFeatureEngineerV4 (季節性あり) + Huber Loss
"""
import pandas as pd
import numpy as np
import lightgbm as lgb
from pathlib import Path
import sys
import logging
import warnings
from scipy.stats import spearmanr

warnings.filterwarnings('ignore')

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(project_root))


def load_data():
    data_dir = project_root / "keibaai/data/parsed/parquet"
    races = pd.read_parquet(data_dir / "races/races.parquet")
    pedigrees = pd.read_parquet(data_dir / "pedigrees/pedigrees.parquet")
    corners = pd.read_parquet(data_dir / "corners/corner_positions.parquet")
    race_details = pd.read_parquet(data_dir / "race_details/race_details.parquet")
    horses = pd.read_parquet(data_dir / "horses/horses.parquet")
    
    # 馬体重変動の結合
    if 'horse_weight_change' not in races.columns:
        horses_subset = horses[['race_id', 'horse_number', 'horse_weight_change']]
        races = races.merge(horses_subset, on=['race_id', 'horse_number'], how='left')

    races = races[races['finish_position'].notna()].copy()
    races['race_date'] = pd.to_datetime(races['race_date'])
    return races, pedigrees, corners, race_details, horses


def calc_roi(df, preds):
    d = df.copy()
    d['score'] = preds
    d['rank'] = d.groupby('race_id')['score'].rank(ascending=False, method='first')
    bets = d[d['rank'] == 1]
    hits = bets[bets['finish_position'] == 1]
    if len(bets) == 0:
        return 0, 0, 0
    roi = hits['win_odds'].sum() / len(bets) * 100
    hit_rate = len(hits) / len(bets) * 100
    return roi, hit_rate, len(bets)


def normalize(x):
    return (x - x.min()) / (x.max() - x.min() + 1e-8)


def train_v15(train_df, feature_cols):
    params = {
        'objective': 'binary', 'metric': 'auc', 'verbosity': -1,
        'learning_rate': 0.03, 'num_leaves': 20, 'max_depth': 3,
        'min_child_samples': 100, 'reg_alpha': 3.0, 'reg_lambda': 5.0,
        'bagging_fraction': 0.7, 'bagging_freq': 3, 'feature_fraction': 0.7,
    }
    y_train = (train_df['finish_position'] == 1).astype(int)
    train_ds = lgb.Dataset(train_df[feature_cols].fillna(0), y_train)
    model = lgb.train(params, train_ds, num_boost_round=200)
    return model


def train_v44_residual(train_df, feature_cols, v15_preds):
    train_df = train_df.copy()
    train_df['v15_pred'] = v15_preds
    
    odds = train_df['win_odds'].fillna(1.0).clip(upper=90)
    log_odds = np.log1p(odds)
    
    actual = (train_df['finish_position'] == 1).astype(float)
    residual = actual - train_df['v15_pred']
    
    residual_gain = np.zeros(len(train_df))
    residual_gain[train_df['finish_position'] == 1] = residual[train_df['finish_position'] == 1] * log_odds[train_df['finish_position'] == 1] * 10
    residual_gain[train_df['finish_position'] == 2] = residual[train_df['finish_position'] == 2] * log_odds[train_df['finish_position'] == 2] * 5
    residual_gain[train_df['finish_position'] == 3] = residual[train_df['finish_position'] == 3] * log_odds[train_df['finish_position'] == 3] * 2
    residual_gain = np.maximum(residual_gain, 0)
    
    train_df['target'] = residual_gain.astype(int)
    train_df = train_df.sort_values('race_id')
    groups = train_df.groupby('race_id', sort=False).size().to_list()
    
    params = {
        'objective': 'lambdarank', 'boosting_type': 'gbdt',
        'num_leaves': 20, 'max_depth': 3, 'min_child_samples': 200,
        'learning_rate': 0.03, 'reg_alpha': 8.0, 'reg_lambda': 12.0,
        'feature_fraction': 0.5, 'bagging_fraction': 0.6, 'bagging_freq': 3,
        'verbose': -1, 'random_state': 42, 'label_gain': list(range(100))
    }
    
    model = lgb.LGBMRanker(**params, n_estimators=150)
    model.fit(train_df[feature_cols].fillna(0), train_df['target'], group=groups)
    return model


def train_mu_time_v5_huber(train_df, mu_feature_cols):
    """v5: Huber Loss + Strong Regularization"""
    train_valid = train_df.dropna(subset=['normalized_time']).copy()
    
    params = {
        'objective': 'huber',
        'alpha': 1.0,
        'metric': 'rmse',
        'verbosity': -1,
        'learning_rate': 0.02, 
        'num_leaves': 31, 
        'max_depth': 4,
        'min_child_samples': 100, 
        'reg_alpha': 1.0,
        'reg_lambda': 1.5,
        'bagging_fraction': 0.8, 
        'bagging_freq': 3, 
        'feature_fraction': 0.8,
        'n_estimators': 1000
    }
    
    y_train = train_valid['normalized_time']
    train_ds = lgb.Dataset(train_valid[mu_feature_cols].fillna(0), y_train)
    # n_estimators is passed to train via num_boost_round
    model = lgb.train(params, train_ds, num_boost_round=1000)
    return model


def add_mu_meta_features(df, mu_preds, v15_rank):
    """μ予測からメタ特徴量を生成"""
    df = df.copy()
    df['_mu_pred'] = mu_preds
    
    # μ予測の順位
    df['mu_pred_rank'] = df.groupby('race_id')['_mu_pred'].rank(ascending=True, method='min')
    
    # パーセンタイル順位
    df['mu_rank_pct'] = df.groupby('race_id')['_mu_pred'].rank(pct=True, ascending=True)
    
    # V15予測との乖離
    df['mu_v15_diff'] = df['mu_pred_rank'] - v15_rank
    df['mu_v15_diff_abs'] = df['mu_v15_diff'].abs()
    
    # μ予測値そのもの（正規化済み）
    df['mu_pred_normalized'] = df.groupby('race_id')['_mu_pred'].transform(lambda x: (x - x.mean()) / (x.std() + 1e-8))
    
    df = df.drop(columns=['_mu_pred'])
    return df


def main():
    logger.info("=" * 80)
    logger.info("μ_v5改善モデル統合検証 (季節性+Huber)")
    logger.info("=" * 80)
    
    from keibaai.src.features.leak_free_feature_engineer_v15_fixed import LeakFreeFeatureEngineerV15Fixed
    from keibaai.src.features.time_feature_engineer_v4 import TimeFeatureEngineerV4
    
    races, pedigrees, corners, race_details, horses = load_data()
    
    test_years = [2021, 2022, 2023, 2024]
    results_base = []
    results_with_mu_meta = []
    
    for test_year in test_years:
        train_end = f'{test_year - 1}-12-31' # 直前までtrainに使用して精度アップを狙う（v5戦略）
        test_start = f'{test_year}-01-01'
        test_end = f'{test_year}-12-31'
        
        # ただしFeature EngineeringのfitはLeak防止のため2年前までがセオリーだが、
        # V15系はLeakFreeなので全期間fitはNG。これまで通り2年前基準が無難か？
        # しかしv5の訓練スクリプトではテストデータの直前まで使っていた。
        # ここでは「モデル学習データ」は直前まで、「特徴量統計」は2年前まで、とするのが正しい。
        # LeakFreeFeatureEngineerはtransform時に未来情報を使わない（shiftなど）ので、
        # fitは全データでやっても基本大丈夫なはずだが、累積統計などは注意。
        # 安全を見て、訓練データ期間（test_year - 2）までは変えず、学習データを増やす戦略はとらないでおく
        # （比較のため条件を揃える）
        train_end_strict = f'{test_year - 2}-12-31'
        
        logger.info(f"\n{'='*60}")
        logger.info(f"検証: {test_year}年")
        logger.info("=" * 60)
        
        train_fe = races[races['race_date'] <= train_end_strict].copy()
        
        # 学習用データは前年まで使う実験（v5の性能を出すため）
        # ただしFeature Engineerのfitは厳密に実施
        
        train = races[races['race_date'] <= train_end_strict].copy() # 従来通り
        test = races[(races['race_date'] >= test_start) & (races['race_date'] <= test_end)].copy()
        
        # ===== V15特徴量生成 =====
        engine_v15 = LeakFreeFeatureEngineerV15Fixed()
        engine_v15.fit(train_fe, pedigrees, corners, race_details, horses_df=horses)
        train_v15f = engine_v15.transform(train)
        test_v15f = engine_v15.transform(test)
        v15_feature_cols = [c for c in engine_v15.get_feature_columns() if c in train_v15f.columns]
        
        # ===== μ_v5特徴量生成 (TimeFeatureEngineerV4) =====
        engine_mu = TimeFeatureEngineerV4(min_samples=30)
        engine_mu.fit(train_fe)
        train_muf = engine_mu.transform(train)
        test_muf = engine_mu.transform(test)
        mu_feature_cols = [c for c in engine_mu.get_feature_columns() if c in train_muf.columns]
        
        # カテゴリ変数の処理
        basic_cols = ['distance_m', 'bracket_number', 'horse_weight', 'age', 'basis_weight']
        for col in ['track_surface', 'track_condition', 'venue', 'distance_category']:
            if col in train_muf.columns:
                train_muf[col + '_enc'] = train_muf[col].astype('category').cat.codes
                test_muf[col + '_enc'] = test_muf[col].astype('category').cat.codes
                basic_cols.append(col + '_enc')
        
        # V4追加特徴量含め、mu_feature_colsを更新
        mu_feature_cols_full = mu_feature_cols + [c for c in basic_cols if c in train_muf.columns]
        # 重複排除
        mu_feature_cols_full = list(dict.fromkeys(mu_feature_cols_full))

        # ===== ベースライン (V15 + V4.4) =====
        model_v15_base = train_v15(train_v15f, v15_feature_cols)
        v15_train_pred = model_v15_base.predict(train_v15f[v15_feature_cols].fillna(0))
        v15_test_pred = model_v15_base.predict(test_v15f[v15_feature_cols].fillna(0))
        
        model_v44 = train_v44_residual(train_v15f, v15_feature_cols, v15_train_pred)
        v44_test_pred = model_v44.predict(test_v15f[v15_feature_cols].fillna(0))
        
        base_score = normalize(v15_test_pred) * 0.5 + normalize(v44_test_pred) * 0.5
        roi_base, hit_base, n_base = calc_roi(test_v15f, base_score)
        
        logger.info(f"  ベースライン (V15+V4.4): {roi_base:.1f}% (的中率={hit_base:.1f}%)")
        results_base.append({'year': test_year, 'roi': roi_base, 'hit': hit_base})
        
        # ===== μ_v5モデル学習 (Huber Loss) =====
        model_mu = train_mu_time_v5_huber(train_muf, mu_feature_cols_full)
        mu_train_pred = model_mu.predict(train_muf[mu_feature_cols_full].fillna(0))
        mu_test_pred = model_mu.predict(test_muf[mu_feature_cols_full].fillna(0))
        
        # Spearman確認
        def calc_spearman(df, preds, col_name):
            d = df.copy()
            d['pred'] = preds
            def _race_corr(g):
                if len(g) < 3: return np.nan
                return spearmanr(g[col_name], g['pred'])[0]
            return d.groupby('race_id').apply(_race_corr, include_groups=False).mean()
        
        sp_corr = calc_spearman(test_muf, mu_test_pred, 'normalized_time')
        logger.info(f"  μ_v5 Test Spearman: {sp_corr:.4f}")

        # ===== μメタ特徴量をV15特徴量に追加 =====
        train_v15f['_v15_pred'] = v15_train_pred
        train_v15f['v15_rank'] = train_v15f.groupby('race_id')['_v15_pred'].rank(ascending=False, method='min')
        train_v15f = add_mu_meta_features(train_v15f, mu_train_pred, train_v15f['v15_rank'])
        train_v15f = train_v15f.drop(columns=['_v15_pred', 'v15_rank'])
        
        test_v15f['_v15_pred'] = v15_test_pred
        test_v15f['v15_rank'] = test_v15f.groupby('race_id')['_v15_pred'].rank(ascending=False, method='min')
        test_v15f = add_mu_meta_features(test_v15f, mu_test_pred, test_v15f['v15_rank'])
        test_v15f = test_v15f.drop(columns=['_v15_pred', 'v15_rank'])
        
        mu_meta_cols = ['mu_pred_rank', 'mu_rank_pct', 'mu_v15_diff', 'mu_v15_diff_abs', 'mu_pred_normalized']
        v15_with_mu_cols = v15_feature_cols + mu_meta_cols
        
        # ===== V15 + μメタ特徴量で再学習 =====
        model_v15_with_mu = train_v15(train_v15f, v15_with_mu_cols)
        v15_mu_train_pred = model_v15_with_mu.predict(train_v15f[v15_with_mu_cols].fillna(0))
        v15_mu_test_pred = model_v15_with_mu.predict(test_v15f[v15_with_mu_cols].fillna(0))
        
        # V4.4 Residualも再学習
        model_v44_with_mu = train_v44_residual(train_v15f, v15_with_mu_cols, v15_mu_train_pred)
        v44_mu_test_pred = model_v44_with_mu.predict(test_v15f[v15_with_mu_cols].fillna(0))
        
        # アンサンブル
        with_mu_score = normalize(v15_mu_test_pred) * 0.5 + normalize(v44_mu_test_pred) * 0.5
        roi_with_mu, hit_with_mu, n_with_mu = calc_roi(test_v15f, with_mu_score)
        
        logger.info(f"  +μ_v5メタ統合: {roi_with_mu:.1f}% (的中率={hit_with_mu:.1f}%)")
        logger.info(f"  改善: {roi_with_mu - roi_base:+.1f}%")
        results_with_mu_meta.append({'year': test_year, 'roi': roi_with_mu, 'hit': hit_with_mu})
        
        for col in mu_meta_cols:
            if col in test_v15f.columns:
                test_v15f = test_v15f.drop(columns=[col])
    
    # ===== 最終サマリー =====
    logger.info("\n" + "=" * 80)
    logger.info("最終サマリー (アプローチ2: μ_v5統合)")
    logger.info("=" * 80)
    
    base_df = pd.DataFrame(results_base)
    mu_df = pd.DataFrame(results_with_mu_meta)
    
    logger.info(f"\n{'Year':>6} {'Base':>8} {'+μ_v5':>8} {'改善':>8}")
    logger.info("-" * 40)
    for i, year in enumerate(test_years):
        base_roi = base_df[base_df['year'] == year]['roi'].values[0]
        mu_roi = mu_df[mu_df['year'] == year]['roi'].values[0]
        diff = mu_roi - base_roi
        logger.info(f"{year:>6} {base_roi:>7.1f}% {mu_roi:>7.1f}% {diff:>+7.1f}%")
    
    logger.info("-" * 40)
    avg_base = base_df['roi'].mean()
    avg_mu = mu_df['roi'].mean()
    logger.info(f"{'Avg':>6} {avg_base:>7.1f}% {avg_mu:>7.1f}% {avg_mu - avg_base:>+7.1f}%")

if __name__ == "__main__":
    main()
