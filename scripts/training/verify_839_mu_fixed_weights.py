#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
83.9%アンサンブル + μモデル統合検証（修正版A）

【修正点】
1. μモデルはTimeFeatureEngineerV3を使用（専用特徴量で正規化タイム予測）
2. 重みは固定パターンで検証（Look-ahead biasを回避）
3. 2021-2024年Walk-forward（solve_overfitting.pyと同一条件）

【構成】
- V15 Fixed (LeakFreeFeatureEngineerV15Fixed)
- V4.4 Residual Method 2 (LeakFreeFeatureEngineerV15Fixed)  
- μ Model (TimeFeatureEngineerV3 → normalized_time予測)

【固定重みパターン】
- Base: 0.5:0.5:0.0 (ベースライン83.9%)
- Pattern1: 0.45:0.45:0.1
- Pattern2: 0.4:0.4:0.2
- Pattern3: 0.35:0.45:0.2
"""
import pandas as pd
import numpy as np
import lightgbm as lgb
from pathlib import Path
import sys
import logging
import warnings
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
        return 0, 0
    roi = hits['win_odds'].sum() / len(bets) * 100
    hit_rate = len(hits) / len(bets) * 100
    return roi, hit_rate


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
    """V4.4 Residual Method 2"""
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


def train_mu_time(train_df, mu_feature_cols):
    """μ Model: normalized_time予測（TimeFeatureEngineerV3用）"""
    train_valid = train_df.dropna(subset=['normalized_time']).copy()
    
    params = {
        'objective': 'regression', 'metric': 'rmse', 'verbosity': -1,
        'learning_rate': 0.02, 'num_leaves': 31, 'max_depth': 4,
        'min_child_samples': 100, 'reg_alpha': 2.0, 'reg_lambda': 3.0,
        'bagging_fraction': 0.8, 'bagging_freq': 3, 'feature_fraction': 0.8,
    }
    
    y_train = train_valid['normalized_time']
    train_ds = lgb.Dataset(train_valid[mu_feature_cols].fillna(0), y_train)
    model = lgb.train(params, train_ds, num_boost_round=300)
    return model


def main():
    logger.info("=" * 80)
    logger.info("83.9%アンサンブル + μモデル統合検証（修正版A）")
    logger.info("=" * 80)
    logger.info("μモデル: TimeFeatureEngineerV3（正規化タイム予測）")
    logger.info("重み: 固定パターンで検証（Look-ahead bias回避）")
    
    from keibaai.src.features.leak_free_feature_engineer_v15_fixed import LeakFreeFeatureEngineerV15Fixed
    from keibaai.src.features.time_feature_engineer_v3 import TimeFeatureEngineerV3
    
    races, pedigrees, corners, race_details, horses = load_data()
    
    # 2021-2024 Walk-forward
    test_years = [2021, 2022, 2023, 2024]
    
    # 固定重みパターン
    weight_patterns = [
        (0.50, 0.50, 0.00, 'Base (83.9%)'),
        (0.45, 0.45, 0.10, '+μ 10%'),
        (0.40, 0.40, 0.20, '+μ 20%'),
        (0.35, 0.45, 0.20, 'V15↓+μ 20%'),
        (0.40, 0.45, 0.15, 'Balanced'),
    ]
    
    all_results = []
    
    for test_year in test_years:
        train_end = f'{test_year - 2}-12-31'
        test_start = f'{test_year}-01-01'
        test_end = f'{test_year}-12-31'
        
        logger.info(f"\n{'='*60}")
        logger.info(f"Test Year: {test_year}")
        logger.info("=" * 60)
        
        train = races[races['race_date'] <= train_end].copy()
        test = races[(races['race_date'] >= test_start) & (races['race_date'] <= test_end)].copy()
        
        logger.info(f"  Train: {len(train):,}, Test: {len(test):,}")
        
        # --- V15/V4.4用特徴量 ---
        logger.info("  V15特徴量生成中...")
        engine_v15 = LeakFreeFeatureEngineerV15Fixed()
        engine_v15.fit(train, pedigrees, corners, race_details, horses_df=horses)
        train_v15f = engine_v15.transform(train)
        test_v15f = engine_v15.transform(test)
        v15_feature_cols = [c for c in engine_v15.get_feature_columns() if c in train_v15f.columns]
        
        # --- μ用特徴量 ---
        logger.info("  μ特徴量生成中...")
        engine_mu = TimeFeatureEngineerV3(min_samples=30)
        engine_mu.fit(train)
        train_muf = engine_mu.transform(train)
        test_muf = engine_mu.transform(test)
        mu_feature_cols = [c for c in engine_mu.get_feature_columns() if c in train_muf.columns]
        
        # 基本特徴量を追加（μモデル用）
        basic_cols = ['distance_m', 'bracket_number', 'horse_weight', 'age', 'basis_weight']
        for col in ['track_surface', 'track_condition', 'venue']:
            if col in train_muf.columns:
                train_muf[col + '_enc'] = train_muf[col].astype('category').cat.codes
                test_muf[col + '_enc'] = test_muf[col].astype('category').cat.codes
                basic_cols.append(col + '_enc')
        
        mu_feature_cols_full = mu_feature_cols + [c for c in basic_cols if c in train_muf.columns]
        
        logger.info(f"  V15特徴量: {len(v15_feature_cols)}, μ特徴量: {len(mu_feature_cols_full)}")
        
        # --- モデル学習 ---
        logger.info("  V15 Fixed 訓練中...")
        model_v15 = train_v15(train_v15f, v15_feature_cols)
        v15_train_pred = model_v15.predict(train_v15f[v15_feature_cols].fillna(0))
        v15_test_pred = model_v15.predict(test_v15f[v15_feature_cols].fillna(0))
        
        logger.info("  V4.4 Residual 訓練中...")
        model_v44 = train_v44_residual(train_v15f, v15_feature_cols, v15_train_pred)
        v44_test_pred = model_v44.predict(test_v15f[v15_feature_cols].fillna(0))
        
        logger.info("  μ Model 訓練中...")
        model_mu = train_mu_time(train_muf, mu_feature_cols_full)
        mu_test_pred = model_mu.predict(test_muf[mu_feature_cols_full].fillna(0))
        
        # μ予測をレース内順位スコアに変換（低タイム=高スコア）
        test_muf['_mu_pred'] = mu_test_pred
        mu_rank_score = test_muf.groupby('race_id')['_mu_pred'].rank(pct=True, ascending=True).values
        # ascending=True: 低い予測タイム（速い）ほど高いrank_pct
        
        # 正規化
        norm_v15 = normalize(v15_test_pred)
        norm_v44 = normalize(v44_test_pred)
        norm_mu = mu_rank_score  # 既に0-1
        
        # --- 各重みパターンでROI計算 ---
        year_results = {'year': test_year, 'patterns': {}}
        
        for w_v15, w_v44, w_mu, name in weight_patterns:
            combined = norm_v15 * w_v15 + norm_v44 * w_v44 + norm_mu * w_mu
            roi, hit = calc_roi(test_v15f, combined)
            year_results['patterns'][name] = {'roi': roi, 'hit': hit, 'weights': (w_v15, w_v44, w_mu)}
            logger.info(f"    {name}: ROI={roi:.1f}%, Hit={hit:.1f}%")
        
        all_results.append(year_results)
    
    # --- サマリー ---
    logger.info("\n" + "=" * 80)
    logger.info("4年間サマリー")
    logger.info("=" * 80)
    
    pattern_names = [p[3] for p in weight_patterns]
    
    # ヘッダー
    header = f"{'Pattern':<20}"
    for year_result in all_results:
        header += f" {year_result['year']:>8}"
    header += f" {'Avg':>8}"
    logger.info(header)
    logger.info("-" * (20 + 9 * (len(all_results) + 1)))
    
    # 各パターン
    for pat_name in pattern_names:
        line = f"{pat_name:<20}"
        rois = []
        for year_result in all_results:
            roi = year_result['patterns'][pat_name]['roi']
            rois.append(roi)
            line += f" {roi:>7.1f}%"
        avg_roi = np.mean(rois)
        line += f" {avg_roi:>7.1f}%"
        logger.info(line)
    
    # ベストパターン特定
    best_pattern = None
    best_avg = 0
    base_avg = 0
    
    for pat_name in pattern_names:
        rois = [r['patterns'][pat_name]['roi'] for r in all_results]
        avg = np.mean(rois)
        if pat_name == 'Base (83.9%)':
            base_avg = avg
        if avg > best_avg:
            best_avg = avg
            best_pattern = pat_name
    
    logger.info("-" * (20 + 9 * (len(all_results) + 1)))
    logger.info(f"\nベースライン (V15+V4.4 50:50): {base_avg:.1f}%")
    logger.info(f"ベストパターン: {best_pattern} ({best_avg:.1f}%)")
    logger.info(f"改善幅: {best_avg - base_avg:+.1f}%")
    
    if best_avg > base_avg + 0.5:
        logger.info("\n✅ μモデル追加による改善を確認！")
    elif best_avg > base_avg:
        logger.info("\n△ μモデル追加でわずかな改善（+0.5%未満）")
    else:
        logger.info("\n❌ μモデル追加効果なし。ベースラインが最良。")


if __name__ == "__main__":
    main()
