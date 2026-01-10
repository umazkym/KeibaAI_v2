# -*- coding: utf-8 -*-
"""
V22 vs V15 Fixed 比較検証スクリプト（本番同等設定版）

【修正内容】
1. Train/Valid/Test 3分割（本番同等）
2. early_stopping(stopping_rounds=20)追加
3. 正則化強化（reg_alpha=5.0, reg_lambda=8.0）
4. num_leaves=20, max_depth=3（本番パラメータ）

これにより、14_モデル改善戦略.mdと同等の結果を得られる：
- V15 Fixed: ROI 82.8%, Gap 25.5%
"""

import os
import sys
import logging
from pathlib import Path
from datetime import datetime

import pandas as pd
import numpy as np
import lightgbm as lgb

# パス設定
PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from keibaai.src.features.leak_free_feature_engineer_v15_fixed import LeakFreeFeatureEngineerV15Fixed
from keibaai.src.features.leak_free_feature_engineer_v22 import LeakFreeFeatureEngineerV22

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)


def load_data():
    """データ読み込み"""
    data_dir = PROJECT_ROOT / 'keibaai' / 'data' / 'parsed' / 'parquet'
    
    logger.info("データ読み込み中...")
    races_df = pd.read_parquet(data_dir / 'races' / 'races.parquet')
    pedigrees_df = pd.read_parquet(data_dir / 'pedigrees' / 'pedigrees.parquet')
    corners_df = pd.read_parquet(data_dir / 'corners' / 'corner_positions.parquet')
    race_details_df = pd.read_parquet(data_dir / 'race_details' / 'race_details.parquet')
    
    # finish_positionがNAのレコードを除外
    races_df = races_df[races_df['finish_position'].notna()].copy()
    races_df['race_date'] = pd.to_datetime(races_df['race_date'], errors='coerce')
    
    logger.info(f"  races: {len(races_df):,}件")
    
    return races_df, pedigrees_df, corners_df, race_details_df


def calc_roi(df: pd.DataFrame, pred_col: str) -> dict:
    """ROI計算"""
    df = df.copy()
    df['rank'] = df.groupby('race_id')[pred_col].rank(ascending=False, method='first')
    top1 = df[df['rank'] == 1]
    
    total = len(top1)
    returns = top1.loc[top1['finish_position'] == 1, 'win_odds'].sum()
    roi = (returns / total * 100) if total > 0 else 0
    hit_rate = (top1['finish_position'] == 1).mean() * 100
    
    return {'roi': roi, 'hit_rate': hit_rate, 'bets': total}


def calc_segment_roi(df: pd.DataFrame, pred_col: str) -> dict:
    """セグメント別ROI計算"""
    df = df.copy()
    df['rank'] = df.groupby('race_id')[pred_col].rank(ascending=False, method='first')
    top1 = df[df['rank'] == 1].copy()
    
    results = {}
    
    # 夏場（8-9月）
    top1['month'] = top1['race_date'].dt.month
    summer = top1[top1['month'].isin([8, 9])]
    if len(summer) > 0:
        summer_return = summer.loc[summer['finish_position'] == 1, 'win_odds'].sum()
        results['summer_roi'] = (summer_return / len(summer) * 100)
    else:
        results['summer_roi'] = 0.0
    
    # 苦手競馬場
    weak_venues = ['札幌', '新潟', '阪神']
    weak = top1[top1['venue'].isin(weak_venues)]
    if len(weak) > 0:
        weak_return = weak.loc[weak['finish_position'] == 1, 'win_odds'].sum()
        results['weak_venue_roi'] = (weak_return / len(weak) * 100)
    else:
        results['weak_venue_roi'] = 0.0
    
    return results


def train_binary_model(train_df, valid_df, feature_cols, is_v22=False):
    """
    Binary Classification モデル学習（本番同等設定）
    
    V22の場合はさらに正則化を強化
    """
    y_train = (train_df['finish_position'] == 1).astype(int)
    y_valid = (valid_df['finish_position'] == 1).astype(int)
    X_train = train_df[feature_cols].fillna(0)
    X_valid = valid_df[feature_cols].fillna(0)
    
    # 本番同等パラメータ
    params = {
        'objective': 'binary',
        'metric': 'auc',
        'boosting_type': 'gbdt',
        'num_leaves': 20,           # 本番同等
        'max_depth': 3,             # 本番同等
        'learning_rate': 0.03,
        'min_child_samples': 100,
        'reg_alpha': 5.0,           # 本番同等
        'reg_lambda': 8.0,          # 本番同等
        'bagging_fraction': 0.6,
        'bagging_freq': 3,
        'feature_fraction': 0.6,
        'verbose': -1,
    }
    
    # V22は過学習傾向があるためさらに正則化強化
    if is_v22:
        params['reg_alpha'] = 10.0
        params['reg_lambda'] = 15.0
        params['num_leaves'] = 15
        params['min_child_samples'] = 150
        params['feature_fraction'] = 0.5
    
    train_data = lgb.Dataset(X_train, y_train)
    valid_data = lgb.Dataset(X_valid, y_valid, reference=train_data)
    
    # early_stopping追加（本番同等）
    model = lgb.train(
        params, train_data, num_boost_round=500,
        valid_sets=[valid_data],
        callbacks=[lgb.early_stopping(stopping_rounds=20, verbose=False)]
    )
    
    return model


def train_residual_model(train_df, valid_df, feature_cols, base_pred_col='pred_v15'):
    """残差学習モデル（V4.4 Residual相当）本番同等設定"""
    train_df = train_df.copy()
    valid_df = valid_df.copy()
    
    # 残差ターゲット: 実際に勝ったのにV15が低く評価した馬
    train_df['actual_win'] = (train_df['finish_position'] == 1).astype(int)
    train_df['pred_rank'] = train_df.groupby('race_id')[base_pred_col].rank(ascending=False, method='first')
    train_df['residual_target'] = train_df['actual_win'] * (1.0 - train_df[base_pred_col])
    
    valid_df['actual_win'] = (valid_df['finish_position'] == 1).astype(int)
    valid_df['pred_rank'] = valid_df.groupby('race_id')[base_pred_col].rank(ascending=False, method='first')
    valid_df['residual_target'] = valid_df['actual_win'] * (1.0 - valid_df[base_pred_col])
    
    y_train = train_df['residual_target']
    y_valid = valid_df['residual_target']
    X_train = train_df[feature_cols].fillna(0)
    X_valid = valid_df[feature_cols].fillna(0)
    
    # 本番同等パラメータ
    params = {
        'objective': 'regression',
        'metric': 'rmse',
        'boosting_type': 'gbdt',
        'num_leaves': 15,
        'max_depth': 3,
        'learning_rate': 0.03,
        'min_child_samples': 150,
        'reg_alpha': 8.0,
        'reg_lambda': 12.0,
        'bagging_fraction': 0.5,
        'feature_fraction': 0.5,
        'verbose': -1,
    }
    
    train_data = lgb.Dataset(X_train, y_train)
    valid_data = lgb.Dataset(X_valid, y_valid, reference=train_data)
    
    model = lgb.train(
        params, train_data, num_boost_round=300,
        valid_sets=[valid_data],
        callbacks=[lgb.early_stopping(stopping_rounds=20, verbose=False)]
    )
    
    return model


def evaluate_year(train_df, valid_df, test_df, pedigrees_df, corners_df, race_details_df, 
                  test_year: int, use_v22: bool = False):
    """1年分の検証を実行（本番同等）"""
    logger.info(f"  Train: {len(train_df):,}件, Valid: {len(valid_df):,}件, Test: {len(test_df):,}件")
    
    # 特徴量エンジン選択
    if use_v22:
        engine = LeakFreeFeatureEngineerV22()
        engine_name = "V22"
    else:
        engine = LeakFreeFeatureEngineerV15Fixed()
        engine_name = "V15 Fixed"
    
    engine.fit(train_df, pedigrees_df, corners_df, race_details_df)
    
    train_features = engine.transform(train_df)
    valid_features = engine.transform(valid_df)
    test_features = engine.transform(test_df)
    
    feature_cols = engine.get_feature_columns()
    logger.info(f"  特徴量エンジン: {engine_name} ({len(feature_cols)}特徴量)")
    
    results = {}
    
    # === Model 1: Binary Classification ===
    model_binary = train_binary_model(train_features, valid_features, feature_cols, is_v22=use_v22)
    
    X_train = train_features[feature_cols].fillna(0)
    X_valid = valid_features[feature_cols].fillna(0)
    X_test = test_features[feature_cols].fillna(0)
    
    train_features['pred_binary'] = model_binary.predict(X_train)
    valid_features['pred_binary'] = model_binary.predict(X_valid)
    test_features['pred_binary'] = model_binary.predict(X_test)
    
    valid_roi_binary = calc_roi(valid_features, 'pred_binary')
    test_roi_binary = calc_roi(test_features, 'pred_binary')
    test_segment = calc_segment_roi(test_features, 'pred_binary')
    
    results['binary'] = {
        'valid_roi': valid_roi_binary['roi'],
        'test_roi': test_roi_binary['roi'],
        'gap': valid_roi_binary['roi'] - test_roi_binary['roi'],  # Valid-Test Gap
        'summer_roi': test_segment['summer_roi'],
        'weak_venue_roi': test_segment['weak_venue_roi'],
    }
    
    logger.info(f"  Binary: Valid {valid_roi_binary['roi']:.1f}%, Test {test_roi_binary['roi']:.1f}%, Gap {results['binary']['gap']:.1f}%")
    
    # === Model 2: Residual ===
    model_residual = train_residual_model(train_features, valid_features, feature_cols, 'pred_binary')
    train_features['pred_residual'] = model_residual.predict(X_train)
    valid_features['pred_residual'] = model_residual.predict(X_valid)
    test_features['pred_residual'] = model_residual.predict(X_test)
    
    valid_roi_residual = calc_roi(valid_features, 'pred_residual')
    test_roi_residual = calc_roi(test_features, 'pred_residual')
    
    results['residual'] = {
        'valid_roi': valid_roi_residual['roi'],
        'test_roi': test_roi_residual['roi'],
        'gap': valid_roi_residual['roi'] - test_roi_residual['roi'],
    }
    
    logger.info(f"  Residual: Valid {valid_roi_residual['roi']:.1f}%, Test {test_roi_residual['roi']:.1f}%, Gap {results['residual']['gap']:.1f}%")
    
    # === Model 3: Ensemble ===
    test_features['pred_ensemble'] = test_features['pred_binary'] * 0.5 + test_features['pred_residual'] * 0.5
    valid_features['pred_ensemble'] = valid_features['pred_binary'] * 0.5 + valid_features['pred_residual'] * 0.5
    
    valid_roi_ensemble = calc_roi(valid_features, 'pred_ensemble')
    test_roi_ensemble = calc_roi(test_features, 'pred_ensemble')
    test_segment_ens = calc_segment_roi(test_features, 'pred_ensemble')
    
    results['ensemble'] = {
        'valid_roi': valid_roi_ensemble['roi'],
        'test_roi': test_roi_ensemble['roi'],
        'gap': valid_roi_ensemble['roi'] - test_roi_ensemble['roi'],
        'summer_roi': test_segment_ens['summer_roi'],
        'weak_venue_roi': test_segment_ens['weak_venue_roi'],
    }
    
    logger.info(f"  Ensemble: Valid {valid_roi_ensemble['roi']:.1f}%, Test {test_roi_ensemble['roi']:.1f}%, Gap {results['ensemble']['gap']:.1f}%")
    
    return results


def run_comparison(races_df, pedigrees_df, corners_df, race_details_df):
    """4年Walk-forward比較（本番同等: Train/Valid/Test 3分割）"""
    # 本番同等: Valid=前年、Test=当年
    test_periods = [
        (2021, '2019-12-31', '2020-01-01', '2020-12-31', '2021-01-01', '2021-12-31'),
        (2022, '2020-12-31', '2021-01-01', '2021-12-31', '2022-01-01', '2022-12-31'),
        (2023, '2021-12-31', '2022-01-01', '2022-12-31', '2023-01-01', '2023-12-31'),
        (2024, '2022-12-31', '2023-01-01', '2023-12-31', '2024-01-01', '2024-12-31'),
    ]
    
    all_results = {
        'V15_Fixed': {'binary': [], 'residual': [], 'ensemble': []},
        'V22': {'binary': [], 'residual': [], 'ensemble': []},
    }
    
    for test_year, train_end, valid_start, valid_end, test_start, test_end in test_periods:
        train_df = races_df[races_df['race_date'] <= train_end].copy()
        valid_df = races_df[(races_df['race_date'] >= valid_start) & (races_df['race_date'] <= valid_end)].copy()
        test_df = races_df[(races_df['race_date'] >= test_start) & (races_df['race_date'] <= test_end)].copy()
        
        if len(train_df) < 1000 or len(valid_df) < 1000 or len(test_df) < 1000:
            logger.warning(f"  {test_year}年: データ不足")
            continue
        
        # V15 Fixed評価
        logger.info(f"\n{'='*60}")
        logger.info(f"V15 Fixed - {test_year}年")
        logger.info(f"{'='*60}")
        v15_results = evaluate_year(
            train_df, valid_df, test_df, pedigrees_df, corners_df, race_details_df,
            test_year, use_v22=False
        )
        for model_type, result in v15_results.items():
            all_results['V15_Fixed'][model_type].append(result)
        
        # V22評価
        logger.info(f"\n{'='*60}")
        logger.info(f"V22 - {test_year}年")
        logger.info(f"{'='*60}")
        v22_results = evaluate_year(
            train_df, valid_df, test_df, pedigrees_df, corners_df, race_details_df,
            test_year, use_v22=True
        )
        for model_type, result in v22_results.items():
            all_results['V22'][model_type].append(result)
    
    return all_results


def print_summary(all_results):
    """結果サマリー出力"""
    logger.info("\n" + "=" * 80)
    logger.info("4年平均結果（本番同等設定）")
    logger.info("=" * 80)
    
    for version in ['V15_Fixed', 'V22']:
        logger.info(f"\n[{version}]")
        for model_type in ['binary', 'residual', 'ensemble']:
            results = all_results[version][model_type]
            if results:
                avg_test = np.mean([r['test_roi'] for r in results])
                avg_gap = np.mean([r['gap'] for r in results])
                std_test = np.std([r['test_roi'] for r in results])
                logger.info(f"  {model_type}: ROI {avg_test:.1f}% (±{std_test:.1f}%), Gap {avg_gap:.1f}%")
    
    # 夏場・苦手場のセグメント結果
    logger.info("\n" + "=" * 80)
    logger.info("セグメント別ROI（4年平均）")
    logger.info("=" * 80)
    
    for version in ['V15_Fixed', 'V22']:
        binary_results = all_results[version]['binary']
        if binary_results and 'summer_roi' in binary_results[0]:
            avg_summer = np.mean([r.get('summer_roi', 0) for r in binary_results])
            avg_weak = np.mean([r.get('weak_venue_roi', 0) for r in binary_results])
            logger.info(f"  {version}: 夏場ROI {avg_summer:.1f}%, 苦手場ROI {avg_weak:.1f}%")
    
    # 目標達成チェック
    logger.info("\n" + "=" * 80)
    logger.info("V22採用判定")
    logger.info("=" * 80)
    
    v15_binary = all_results['V15_Fixed']['binary']
    v22_binary = all_results['V22']['binary']
    
    if v15_binary and v22_binary:
        v15_roi = np.mean([r['test_roi'] for r in v15_binary])
        v22_roi = np.mean([r['test_roi'] for r in v22_binary])
        v22_gap = np.mean([r['gap'] for r in v22_binary])
        v15_summer = np.mean([r.get('summer_roi', 0) for r in v15_binary])
        v22_summer = np.mean([r.get('summer_roi', 0) for r in v22_binary])
        
        logger.info(f"  ROI改善: {'✅' if v22_roi > v15_roi else '❌'} (V15: {v15_roi:.1f}% → V22: {v22_roi:.1f}%)")
        logger.info(f"  夏場ROI改善: {'✅' if v22_summer > v15_summer else '❌'} (V15: {v15_summer:.1f}% → V22: {v22_summer:.1f}%)")
        logger.info(f"  Gap < 30%: {'✅' if v22_gap < 30 else '❌'} (V22 Gap: {v22_gap:.1f}%)")


def main():
    logger.info("V22 vs V15 Fixed 比較検証（本番同等設定版）")
    logger.info(f"実行日時: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    races_df, pedigrees_df, corners_df, race_details_df = load_data()
    
    all_results = run_comparison(races_df, pedigrees_df, corners_df, race_details_df)
    print_summary(all_results)
    
    logger.info("\n検証完了")


if __name__ == "__main__":
    main()
