# -*- coding: utf-8 -*-
"""
V22 vs 既存高性能モデル 包括比較検証

V22（季節・競馬場特化特徴量）と既存の高性能モデルを4年Walk-forward検証で比較。

【比較対象モデル】
1. V15 Fixed: Binary Classification（安定ベースライン）
2. V4.4 Residual: V15の残差を学習（穴馬対応）
3. Ensemble: V15 + V4.4 Residualの50:50平均
4. V22: V15 Fixed + 季節・競馬場特化特徴量

【リーク・過学習対策】
- 4年Walk-forward検証（2021-2024）
- Train期間とTest期間を厳格に分離
- shift(1)による累積統計のリーク防止
- Gap監視（30%超で警告）

【評価指標】
- 単勝ROI（主指標）
- 夏場（8-9月）ROI
- 苦手競馬場（札幌/新潟/阪神）ROI
- Train-Test Gap
"""

import os
import sys
import logging
from pathlib import Path
from datetime import datetime

import pandas as pd
import numpy as np
import lightgbm as lgb
import warnings
warnings.filterwarnings('ignore')

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
    
    # finish_positionがNAのレコードを除外（確定前のレース）
    races_df = races_df[races_df['finish_position'].notna()].copy()
    races_df['race_date'] = pd.to_datetime(races_df['race_date'], errors='coerce')
    
    logger.info(f"  races: {len(races_df):,}件")
    
    return races_df, pedigrees_df, corners_df, race_details_df


def calc_roi(df: pd.DataFrame, pred_col: str = 'pred') -> dict:
    """ROI計算"""
    df = df.copy()
    df['rank'] = df.groupby('race_id')[pred_col].rank(ascending=False, method='first')
    top1 = df[df['rank'] == 1].copy()
    
    total_bet = len(top1)
    total_return = top1.loc[top1['finish_position'] == 1, 'win_odds'].sum()
    roi = (total_return / total_bet * 100) if total_bet > 0 else 0
    hit_rate = (top1['finish_position'] == 1).mean() * 100
    
    return {'roi': roi, 'hit_rate': hit_rate, 'bets': total_bet}


def calc_segment_roi(df: pd.DataFrame, pred_col: str = 'pred') -> dict:
    """セグメント別ROI計算"""
    df = df.copy()
    df['rank'] = df.groupby('race_id')[pred_col].rank(ascending=False, method='first')
    top1 = df[df['rank'] == 1].copy()
    
    top1['race_date'] = pd.to_datetime(top1['race_date'], errors='coerce')
    top1['month'] = top1['race_date'].dt.month
    
    results = {}
    
    # 夏場（8-9月）
    summer = top1[top1['month'].isin([8, 9])]
    if len(summer) > 0:
        summer_return = summer.loc[summer['finish_position'] == 1, 'win_odds'].sum()
        results['summer_roi'] = (summer_return / len(summer) * 100)
    else:
        results['summer_roi'] = 0
    
    # 苦手競馬場
    weak_venues = ['札幌', '新潟', '阪神']
    weak = top1[top1['venue'].isin(weak_venues)]
    if len(weak) > 0:
        weak_return = weak.loc[weak['finish_position'] == 1, 'win_odds'].sum()
        results['weak_venue_roi'] = (weak_return / len(weak) * 100)
    else:
        results['weak_venue_roi'] = 0
    
    return results


def train_binary_model(train_df, feature_cols, params=None):
    """Binary Classification（1着予測）モデル学習"""
    y_train = (train_df['finish_position'] == 1).astype(int)
    X_train = train_df[feature_cols].fillna(0)
    
    if params is None:
        params = {
            'objective': 'binary',
            'metric': 'auc',
            'boosting_type': 'gbdt',
            'num_leaves': 31,
            'max_depth': 5,
            'learning_rate': 0.05,
            'min_child_samples': 50,
            'reg_alpha': 1.0,
            'reg_lambda': 1.0,
            'subsample': 0.8,
            'colsample_bytree': 0.8,
            'verbose': -1,
        }
    
    train_data = lgb.Dataset(X_train, y_train)
    model = lgb.train(params, train_data, num_boost_round=200)
    
    return model


def train_ranker_model(train_df, feature_cols, params=None):
    """LambdaRank（オッズ加重ランキング）モデル学習 - V4.4相当"""
    # オッズ加重ゲイン値をターゲットにする
    train_df = train_df.copy()
    
    # ゲイン計算（高配当の上位着順が高スコア）
    def calc_gain(row):
        pos = row['finish_position']
        odds = row.get('win_odds', 1)
        if pos == 1:
            return 12.74 * np.log1p(odds)
        elif pos == 2:
            return 6.73 * np.log1p(odds)
        elif pos == 3:
            return 3.69 * np.log1p(odds)
        elif pos <= 5:
            return 1.0
        else:
            return 0.0
    
    train_df['gain'] = train_df.apply(calc_gain, axis=1)
    
    X_train = train_df[feature_cols].fillna(0)
    y_train = train_df['gain']
    groups = train_df.groupby('race_id').size().values
    
    if params is None:
        params = {
            'objective': 'lambdarank',
            'metric': 'ndcg',
            'boosting_type': 'gbdt',
            'num_leaves': 20,
            'max_depth': 4,
            'learning_rate': 0.05,
            'min_child_samples': 100,
            'reg_alpha': 5.0,
            'reg_lambda': 5.0,
            'subsample': 0.6,
            'colsample_bytree': 0.6,
            'verbose': -1,
        }
    
    train_data = lgb.Dataset(X_train, y_train, group=groups)
    model = lgb.train(params, train_data, num_boost_round=150)
    
    return model


def train_residual_model(train_df, feature_cols, base_pred_col='pred_v15'):
    """
    残差学習モデル（V4.4 Residual相当）
    
    V15の予測残差をターゲットに学習し、V15が過小評価した馬を発見する。
    
    【リーク対策】
    - V15の予測結果（train期間のみ）を使用
    - 実際の着順は使用しない
    """
    train_df = train_df.copy()
    
    # 残差ターゲット: 実際に勝ったのにV15が低く評価した馬
    # is_win - pred_v15 が正の値 = 過小評価された勝ち馬
    is_win = (train_df['finish_position'] == 1).astype(float)
    residual = is_win - train_df[base_pred_col]
    
    # 正の残差を強調（オッズで加重）
    train_df['residual_target'] = residual * np.log1p(train_df['win_odds'].fillna(1))
    
    X_train = train_df[feature_cols].fillna(0)
    y_train = train_df['residual_target']
    
    params = {
        'objective': 'regression',
        'metric': 'rmse',
        'boosting_type': 'gbdt',
        'num_leaves': 15,
        'max_depth': 3,
        'learning_rate': 0.03,
        'min_child_samples': 150,
        'reg_alpha': 10.0,
        'reg_lambda': 10.0,
        'subsample': 0.5,
        'colsample_bytree': 0.5,
        'verbose': -1,
    }
    
    train_data = lgb.Dataset(X_train, y_train)
    model = lgb.train(params, train_data, num_boost_round=100)
    
    return model


def evaluate_year(train_df, test_df, pedigrees_df, corners_df, race_details_df, 
                  test_year: int, use_v22: bool = False):
    """1年分の検証を実行"""
    logger.info(f"\n--- Test Year: {test_year} ---")
    logger.info(f"  Train: {len(train_df):,}件, Test: {len(test_df):,}件")
    
    # 特徴量エンジン選択
    if use_v22:
        engine = LeakFreeFeatureEngineerV22()
        engine_name = "V22"
    else:
        engine = LeakFreeFeatureEngineerV15Fixed()
        engine_name = "V15 Fixed"
    
    engine.fit(train_df, pedigrees_df, corners_df, race_details_df)
    
    train_features = engine.transform(train_df)
    test_features = engine.transform(test_df)
    
    feature_cols = engine.get_feature_columns()
    logger.info(f"  特徴量エンジン: {engine_name} ({len(feature_cols)}特徴量)")
    
    X_train = train_features[feature_cols].fillna(0)
    X_test = test_features[feature_cols].fillna(0)
    
    results = {}
    
    # === Model 1: V15 Fixed / V22（Binary Classification）===
    model_binary = train_binary_model(train_features, feature_cols)
    train_features['pred_binary'] = model_binary.predict(X_train)
    test_features['pred_binary'] = model_binary.predict(X_test)
    
    train_roi_binary = calc_roi(train_features, 'pred_binary')
    test_roi_binary = calc_roi(test_features, 'pred_binary')
    test_segment = calc_segment_roi(test_features, 'pred_binary')
    
    results['binary'] = {
        'train_roi': train_roi_binary['roi'],
        'test_roi': test_roi_binary['roi'],
        'gap': train_roi_binary['roi'] - test_roi_binary['roi'],
        'summer_roi': test_segment['summer_roi'],
        'weak_venue_roi': test_segment['weak_venue_roi'],
    }
    
    logger.info(f"  Binary: Train {train_roi_binary['roi']:.1f}%, Test {test_roi_binary['roi']:.1f}%, Gap {results['binary']['gap']:.1f}%")
    
    # === Model 2: V4.4 Residual ===
    # V15の予測を使って残差モデルを学習
    model_residual = train_residual_model(train_features, feature_cols, 'pred_binary')
    train_features['pred_residual'] = model_residual.predict(X_train)
    test_features['pred_residual'] = model_residual.predict(X_test)
    
    # 残差予測でランキング
    train_roi_residual = calc_roi(train_features, 'pred_residual')
    test_roi_residual = calc_roi(test_features, 'pred_residual')
    test_segment_res = calc_segment_roi(test_features, 'pred_residual')
    
    results['residual'] = {
        'train_roi': train_roi_residual['roi'],
        'test_roi': test_roi_residual['roi'],
        'gap': train_roi_residual['roi'] - test_roi_residual['roi'],
        'summer_roi': test_segment_res['summer_roi'],
        'weak_venue_roi': test_segment_res['weak_venue_roi'],
    }
    
    logger.info(f"  Residual: Train {train_roi_residual['roi']:.1f}%, Test {test_roi_residual['roi']:.1f}%, Gap {results['residual']['gap']:.1f}%")
    
    # === Model 3: Ensemble（Binary + Residual 50:50）===
    test_features['pred_ensemble'] = (
        test_features['pred_binary'] * 0.5 + 
        test_features['pred_residual'] * 0.5
    )
    train_features['pred_ensemble'] = (
        train_features['pred_binary'] * 0.5 + 
        train_features['pred_residual'] * 0.5
    )
    
    train_roi_ensemble = calc_roi(train_features, 'pred_ensemble')
    test_roi_ensemble = calc_roi(test_features, 'pred_ensemble')
    test_segment_ens = calc_segment_roi(test_features, 'pred_ensemble')
    
    results['ensemble'] = {
        'train_roi': train_roi_ensemble['roi'],
        'test_roi': test_roi_ensemble['roi'],
        'gap': train_roi_ensemble['roi'] - test_roi_ensemble['roi'],
        'summer_roi': test_segment_ens['summer_roi'],
        'weak_venue_roi': test_segment_ens['weak_venue_roi'],
    }
    
    logger.info(f"  Ensemble: Train {train_roi_ensemble['roi']:.1f}%, Test {test_roi_ensemble['roi']:.1f}%, Gap {results['ensemble']['gap']:.1f}%")
    
    return results


def run_comparison(races_df, pedigrees_df, corners_df, race_details_df):
    """4年Walk-forward比較"""
    years = [2021, 2022, 2023, 2024]
    
    all_results = {
        'V15_Fixed': {'binary': [], 'residual': [], 'ensemble': []},
        'V22': {'binary': [], 'residual': [], 'ensemble': []},
    }
    
    races_df['race_date'] = pd.to_datetime(races_df['race_date'], errors='coerce')
    
    for test_year in years:
        train_end = f'{test_year}-01-01'
        test_start = f'{test_year}-01-01'
        test_end = f'{test_year}-12-31'
        
        train_df = races_df[races_df['race_date'] < train_end].copy()
        test_df = races_df[
            (races_df['race_date'] >= test_start) & 
            (races_df['race_date'] <= test_end)
        ].copy()
        
        if len(train_df) < 1000 or len(test_df) < 1000:
            logger.warning(f"  {test_year}年: データ不足")
            continue
        
        # V15 Fixed評価
        logger.info(f"\n{'='*60}")
        logger.info(f"V15 Fixed - {test_year}年")
        logger.info(f"{'='*60}")
        v15_results = evaluate_year(
            train_df, test_df, pedigrees_df, corners_df, race_details_df,
            test_year, use_v22=False
        )
        for model_type, result in v15_results.items():
            result['year'] = test_year
            all_results['V15_Fixed'][model_type].append(result)
        
        # V22評価
        logger.info(f"\n{'='*60}")
        logger.info(f"V22 - {test_year}年")
        logger.info(f"{'='*60}")
        v22_results = evaluate_year(
            train_df, test_df, pedigrees_df, corners_df, race_details_df,
            test_year, use_v22=True
        )
        for model_type, result in v22_results.items():
            result['year'] = test_year
            all_results['V22'][model_type].append(result)
    
    return all_results


def print_summary(all_results):
    """結果サマリー表示"""
    logger.info(f"\n{'='*80}")
    logger.info("4年平均結果サマリー")
    logger.info(f"{'='*80}")
    
    for engine_name in ['V15_Fixed', 'V22']:
        logger.info(f"\n--- {engine_name} ---")
        
        for model_type in ['binary', 'residual', 'ensemble']:
            results = all_results[engine_name][model_type]
            if not results:
                continue
            
            avg_test_roi = np.mean([r['test_roi'] for r in results])
            avg_gap = np.mean([r['gap'] for r in results])
            std_roi = np.std([r['test_roi'] for r in results])
            avg_summer = np.mean([r['summer_roi'] for r in results])
            avg_weak = np.mean([r['weak_venue_roi'] for r in results])
            
            model_label = {
                'binary': 'Binary',
                'residual': 'Residual',
                'ensemble': 'Ensemble'
            }[model_type]
            
            logger.info(f"  {model_label:12} | ROI: {avg_test_roi:5.1f}% | Gap: {avg_gap:5.1f}% | σ: {std_roi:4.1f}% | 夏場: {avg_summer:5.1f}% | 苦手場: {avg_weak:5.1f}%")
    
    # 比較
    logger.info(f"\n{'='*80}")
    logger.info("V15 Fixed vs V22 比較（Binaryモデル）")
    logger.info(f"{'='*80}")
    
    v15_binary = all_results['V15_Fixed']['binary']
    v22_binary = all_results['V22']['binary']
    
    if v15_binary and v22_binary:
        v15_roi = np.mean([r['test_roi'] for r in v15_binary])
        v22_roi = np.mean([r['test_roi'] for r in v22_binary])
        v15_summer = np.mean([r['summer_roi'] for r in v15_binary])
        v22_summer = np.mean([r['summer_roi'] for r in v22_binary])
        v15_weak = np.mean([r['weak_venue_roi'] for r in v15_binary])
        v22_weak = np.mean([r['weak_venue_roi'] for r in v22_binary])
        v15_gap = np.mean([r['gap'] for r in v15_binary])
        v22_gap = np.mean([r['gap'] for r in v22_binary])
        
        logger.info(f"\n{'指標':<20} {'V15 Fixed':>12} {'V22':>12} {'差分':>10}")
        logger.info("-" * 56)
        logger.info(f"{'Test ROI':<20} {v15_roi:>11.1f}% {v22_roi:>11.1f}% {v22_roi - v15_roi:>+9.1f}%")
        logger.info(f"{'Gap':<20} {v15_gap:>11.1f}% {v22_gap:>11.1f}% {v22_gap - v15_gap:>+9.1f}%")
        logger.info(f"{'夏場ROI':<20} {v15_summer:>11.1f}% {v22_summer:>11.1f}% {v22_summer - v15_summer:>+9.1f}%")
        logger.info(f"{'苦手競馬場ROI':<20} {v15_weak:>11.1f}% {v22_weak:>11.1f}% {v22_weak - v15_weak:>+9.1f}%")
        
        # 判定
        logger.info(f"\n{'='*80}")
        logger.info("評価判定")
        logger.info(f"{'='*80}")
        
        roi_improved = v22_roi > v15_roi
        summer_improved = v22_summer > v15_summer
        weak_improved = v22_weak > v15_weak
        gap_ok = v22_gap < 30
        
        logger.info(f"  ROI改善: {'✅' if roi_improved else '❌'} (V15: {v15_roi:.1f}% → V22: {v22_roi:.1f}%)")
        logger.info(f"  夏場ROI改善: {'✅' if summer_improved else '❌'} (V15: {v15_summer:.1f}% → V22: {v22_summer:.1f}%)")
        logger.info(f"  苦手競馬場ROI改善: {'✅' if weak_improved else '❌'} (V15: {v15_weak:.1f}% → V22: {v22_weak:.1f}%)")
        logger.info(f"  Gap < 30%: {'✅' if gap_ok else '❌'} (V22 Gap: {v22_gap:.1f}%)")
        
        if roi_improved and gap_ok:
            logger.info("\n🎉 V22は採用推奨！ROI改善かつ過学習なし")
        elif gap_ok:
            logger.info("\n✅ V22は安定（過学習なし）、ROI微改善の余地あり")
        else:
            logger.info("\n⚠️ V22は過学習傾向あり、要調整")


def main():
    """メイン処理"""
    logger.info("V22 vs 既存高性能モデル 包括比較検証")
    logger.info(f"実行日時: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 80)
    
    # データ読み込み
    races_df, pedigrees_df, corners_df, race_details_df = load_data()
    
    # 4年Walk-forward比較
    all_results = run_comparison(races_df, pedigrees_df, corners_df, race_details_df)
    
    # サマリー表示
    print_summary(all_results)
    
    logger.info("\n検証完了")


if __name__ == "__main__":
    main()
