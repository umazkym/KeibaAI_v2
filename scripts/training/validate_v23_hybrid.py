# -*- coding: utf-8 -*-
"""
V23 Hybrid Model 包括的検証スクリプト

【比較対象モデル】
1. V15 Fixed Binary: 安定ベースライン
2. V4.4 Residual (V15ベース): 穴馬発見
3. V15 + V4.4 Ensemble: 現行ベスト (0.5/0.5)
4. V22 Binary (強正則化): 夏場特化
5. V23 Hybrid: V22 Binary + V15 Residual (NEW)

【評価指標】
- ROI (単勝)
- 的中率
- 平均的中オッズ
- 平均的中人気
- 夏場ROI (8-9月)
- 苦手競馬場ROI
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
    
    races_df = races_df[races_df['finish_position'].notna()].copy()
    races_df['race_date'] = pd.to_datetime(races_df['race_date'], errors='coerce')
    
    logger.info(f"  races: {len(races_df):,}件")
    
    return races_df, pedigrees_df, corners_df, race_details_df


def calc_detailed_metrics(df: pd.DataFrame, pred_col: str = 'pred') -> dict:
    """詳細メトリクス計算"""
    df = df.copy()
    df['rank'] = df.groupby('race_id')[pred_col].rank(ascending=False, method='first')
    top1 = df[df['rank'] == 1].copy()
    
    total_bet = len(top1)
    wins = top1[top1['finish_position'] == 1]
    total_return = wins['win_odds'].sum()
    
    # 基本指標
    roi = (total_return / total_bet * 100) if total_bet > 0 else 0
    hit_rate = len(wins) / total_bet * 100 if total_bet > 0 else 0
    
    # 的中時の詳細
    avg_win_odds = wins['win_odds'].mean() if len(wins) > 0 else 0
    avg_win_popularity = wins['popularity'].mean() if len(wins) > 0 else 0
    
    # 人気別的中内訳
    pop_1_3 = len(wins[wins['popularity'] <= 3]) / len(wins) * 100 if len(wins) > 0 else 0
    pop_4_6 = len(wins[(wins['popularity'] >= 4) & (wins['popularity'] <= 6)]) / len(wins) * 100 if len(wins) > 0 else 0
    pop_7_plus = len(wins[wins['popularity'] >= 7]) / len(wins) * 100 if len(wins) > 0 else 0
    
    return {
        'roi': roi,
        'hit_rate': hit_rate,
        'avg_win_odds': avg_win_odds,
        'avg_win_popularity': avg_win_popularity,
        'bets': total_bet,
        'wins': len(wins),
        'pop_1_3_pct': pop_1_3,
        'pop_4_6_pct': pop_4_6,
        'pop_7_plus_pct': pop_7_plus,
    }


def calc_segment_metrics(df: pd.DataFrame, pred_col: str = 'pred') -> dict:
    """セグメント別メトリクス"""
    df = df.copy()
    df['rank'] = df.groupby('race_id')[pred_col].rank(ascending=False, method='first')
    top1 = df[df['rank'] == 1].copy()
    
    top1['race_date'] = pd.to_datetime(top1['race_date'], errors='coerce')
    top1['month'] = top1['race_date'].dt.month
    
    results = {}
    
    # 夏場（8-9月）
    summer = top1[top1['month'].isin([8, 9])]
    if len(summer) > 0:
        summer_wins = summer[summer['finish_position'] == 1]
        results['summer_roi'] = summer_wins['win_odds'].sum() / len(summer) * 100
        results['summer_hit_rate'] = len(summer_wins) / len(summer) * 100
    else:
        results['summer_roi'] = 0
        results['summer_hit_rate'] = 0
    
    # 苦手競馬場
    weak_venues = ['札幌', '新潟', '阪神']
    weak = top1[top1['venue'].isin(weak_venues)]
    if len(weak) > 0:
        weak_wins = weak[weak['finish_position'] == 1]
        results['weak_venue_roi'] = weak_wins['win_odds'].sum() / len(weak) * 100
        results['weak_venue_hit_rate'] = len(weak_wins) / len(weak) * 100
    else:
        results['weak_venue_roi'] = 0
        results['weak_venue_hit_rate'] = 0
    
    return results


def train_binary_model(train_df, feature_cols, strong_reg=False):
    """Binary Classification モデル"""
    y_train = (train_df['finish_position'] == 1).astype(int)
    X_train = train_df[feature_cols].fillna(0)
    
    if strong_reg:
        # V22.1 / V23用の強正則化
        params = {
            'objective': 'binary',
            'metric': 'auc',
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
    else:
        # V15 Fixed用の標準設定
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


def train_residual_model(train_df, feature_cols, base_pred_col='pred_binary'):
    """残差学習モデル（V4.4 Residual相当）"""
    train_df = train_df.copy()
    
    is_win = (train_df['finish_position'] == 1).astype(float)
    residual = is_win - train_df[base_pred_col]
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


def evaluate_year(train_df, test_df, pedigrees_df, corners_df, race_details_df, test_year: int):
    """1年分の全モデル評価"""
    logger.info(f"\n{'='*80}")
    logger.info(f"Test Year: {test_year}")
    logger.info(f"{'='*80}")
    logger.info(f"  Train: {len(train_df):,}件, Test: {len(test_df):,}件")
    
    results = {}
    
    # ===== V15 Fixed 特徴量 =====
    engine_v15 = LeakFreeFeatureEngineerV15Fixed()
    engine_v15.fit(train_df, pedigrees_df, corners_df, race_details_df)
    
    train_v15 = engine_v15.transform(train_df)
    test_v15 = engine_v15.transform(test_df)
    feature_cols_v15 = engine_v15.get_feature_columns()
    
    X_train_v15 = train_v15[feature_cols_v15].fillna(0)
    X_test_v15 = test_v15[feature_cols_v15].fillna(0)
    
    # ===== V22 特徴量 =====
    engine_v22 = LeakFreeFeatureEngineerV22()
    engine_v22.fit(train_df, pedigrees_df, corners_df, race_details_df)
    
    train_v22 = engine_v22.transform(train_df)
    test_v22 = engine_v22.transform(test_df)
    feature_cols_v22 = engine_v22.get_feature_columns()
    
    X_train_v22 = train_v22[feature_cols_v22].fillna(0)
    X_test_v22 = test_v22[feature_cols_v22].fillna(0)
    
    logger.info(f"  V15特徴量: {len(feature_cols_v15)}個, V22特徴量: {len(feature_cols_v22)}個")
    
    # ===== Model 1: V15 Fixed Binary =====
    logger.info("  [1/5] V15 Fixed Binary...")
    model_v15_binary = train_binary_model(train_v15, feature_cols_v15, strong_reg=False)
    train_v15['pred_v15_binary'] = model_v15_binary.predict(X_train_v15)
    test_v15['pred_v15_binary'] = model_v15_binary.predict(X_test_v15)
    
    train_metrics = calc_detailed_metrics(train_v15, 'pred_v15_binary')
    test_metrics = calc_detailed_metrics(test_v15, 'pred_v15_binary')
    segment_metrics = calc_segment_metrics(test_v15, 'pred_v15_binary')
    
    results['V15_Binary'] = {
        'train_roi': train_metrics['roi'],
        'test_roi': test_metrics['roi'],
        'gap': train_metrics['roi'] - test_metrics['roi'],
        'hit_rate': test_metrics['hit_rate'],
        'avg_win_odds': test_metrics['avg_win_odds'],
        'avg_win_pop': test_metrics['avg_win_popularity'],
        'summer_roi': segment_metrics['summer_roi'],
        'weak_venue_roi': segment_metrics['weak_venue_roi'],
    }
    
    # ===== Model 2: V4.4 Residual (V15ベース) =====
    logger.info("  [2/5] V4.4 Residual (V15ベース)...")
    model_v15_residual = train_residual_model(train_v15, feature_cols_v15, 'pred_v15_binary')
    train_v15['pred_v15_residual'] = model_v15_residual.predict(X_train_v15)
    test_v15['pred_v15_residual'] = model_v15_residual.predict(X_test_v15)
    
    train_metrics = calc_detailed_metrics(train_v15, 'pred_v15_residual')
    test_metrics = calc_detailed_metrics(test_v15, 'pred_v15_residual')
    segment_metrics = calc_segment_metrics(test_v15, 'pred_v15_residual')
    
    results['V44_Residual'] = {
        'train_roi': train_metrics['roi'],
        'test_roi': test_metrics['roi'],
        'gap': train_metrics['roi'] - test_metrics['roi'],
        'hit_rate': test_metrics['hit_rate'],
        'avg_win_odds': test_metrics['avg_win_odds'],
        'avg_win_pop': test_metrics['avg_win_popularity'],
        'summer_roi': segment_metrics['summer_roi'],
        'weak_venue_roi': segment_metrics['weak_venue_roi'],
    }
    
    # ===== Model 3: V15 + V4.4 Ensemble =====
    logger.info("  [3/5] V15 + V4.4 Ensemble...")
    test_v15['pred_v15_ensemble'] = test_v15['pred_v15_binary'] * 0.5 + test_v15['pred_v15_residual'] * 0.5
    train_v15['pred_v15_ensemble'] = train_v15['pred_v15_binary'] * 0.5 + train_v15['pred_v15_residual'] * 0.5
    
    train_metrics = calc_detailed_metrics(train_v15, 'pred_v15_ensemble')
    test_metrics = calc_detailed_metrics(test_v15, 'pred_v15_ensemble')
    segment_metrics = calc_segment_metrics(test_v15, 'pred_v15_ensemble')
    
    results['V15_V44_Ensemble'] = {
        'train_roi': train_metrics['roi'],
        'test_roi': test_metrics['roi'],
        'gap': train_metrics['roi'] - test_metrics['roi'],
        'hit_rate': test_metrics['hit_rate'],
        'avg_win_odds': test_metrics['avg_win_odds'],
        'avg_win_pop': test_metrics['avg_win_popularity'],
        'summer_roi': segment_metrics['summer_roi'],
        'weak_venue_roi': segment_metrics['weak_venue_roi'],
    }
    
    # ===== Model 4: V22 Binary (強正則化) =====
    logger.info("  [4/5] V22 Binary (強正則化)...")
    model_v22_binary = train_binary_model(train_v22, feature_cols_v22, strong_reg=True)
    train_v22['pred_v22_binary'] = model_v22_binary.predict(X_train_v22)
    test_v22['pred_v22_binary'] = model_v22_binary.predict(X_test_v22)
    
    train_metrics = calc_detailed_metrics(train_v22, 'pred_v22_binary')
    test_metrics = calc_detailed_metrics(test_v22, 'pred_v22_binary')
    segment_metrics = calc_segment_metrics(test_v22, 'pred_v22_binary')
    
    results['V22_Binary'] = {
        'train_roi': train_metrics['roi'],
        'test_roi': test_metrics['roi'],
        'gap': train_metrics['roi'] - test_metrics['roi'],
        'hit_rate': test_metrics['hit_rate'],
        'avg_win_odds': test_metrics['avg_win_odds'],
        'avg_win_pop': test_metrics['avg_win_popularity'],
        'summer_roi': segment_metrics['summer_roi'],
        'weak_venue_roi': segment_metrics['weak_venue_roi'],
    }
    
    # ===== Model 5: V23 Hybrid (V22 Binary + V15 Residual) =====
    logger.info("  [5/5] V23 Hybrid (V22 Binary + V15 Residual)...")
    
    # V22 Binaryの予測をV15データに転送（race_id + horse_idベースでマージ）
    # V22とV15のDataFrameで行数が異なる可能性があるためマージで対応
    v22_pred_train = train_v22[['race_id', 'horse_id', 'pred_v22_binary']].copy()
    v22_pred_test = test_v22[['race_id', 'horse_id', 'pred_v22_binary']].copy()
    
    # 重複を削除（同一race_id + horse_idで複数行がある場合）
    v22_pred_train = v22_pred_train.drop_duplicates(subset=['race_id', 'horse_id'])
    v22_pred_test = v22_pred_test.drop_duplicates(subset=['race_id', 'horse_id'])
    
    train_v15 = train_v15.merge(v22_pred_train, on=['race_id', 'horse_id'], how='left')
    test_v15 = test_v15.merge(v22_pred_test, on=['race_id', 'horse_id'], how='left')
    
    # V15特徴量でV22 Binaryの残差を学習
    # NaNがある場合はV15 Binaryで補完
    train_v15['pred_v22_binary'] = train_v15['pred_v22_binary'].fillna(train_v15['pred_v15_binary'])
    test_v15['pred_v22_binary'] = test_v15['pred_v22_binary'].fillna(test_v15['pred_v15_binary'])
    
    model_v23_residual = train_residual_model(train_v15, feature_cols_v15, 'pred_v22_binary')
    train_v15['pred_v23_residual'] = model_v23_residual.predict(X_train_v15)
    test_v15['pred_v23_residual'] = model_v23_residual.predict(X_test_v15)
    
    # V23 Ensemble = V22 Binary + V15 Residual
    test_v15['pred_v23'] = test_v15['pred_v22_binary'] * 0.5 + test_v15['pred_v23_residual'] * 0.5
    train_v15['pred_v23'] = train_v15['pred_v22_binary'] * 0.5 + train_v15['pred_v23_residual'] * 0.5
    
    train_metrics = calc_detailed_metrics(train_v15, 'pred_v23')
    test_metrics = calc_detailed_metrics(test_v15, 'pred_v23')
    segment_metrics = calc_segment_metrics(test_v15, 'pred_v23')
    
    results['V23_Hybrid'] = {
        'train_roi': train_metrics['roi'],
        'test_roi': test_metrics['roi'],
        'gap': train_metrics['roi'] - test_metrics['roi'],
        'hit_rate': test_metrics['hit_rate'],
        'avg_win_odds': test_metrics['avg_win_odds'],
        'avg_win_pop': test_metrics['avg_win_popularity'],
        'summer_roi': segment_metrics['summer_roi'],
        'weak_venue_roi': segment_metrics['weak_venue_roi'],
    }
    
    # 結果ログ出力
    for model_name, m in results.items():
        logger.info(f"  {model_name:20} | ROI: {m['test_roi']:5.1f}% | Hit: {m['hit_rate']:4.1f}% | AvgOdds: {m['avg_win_odds']:4.1f} | Gap: {m['gap']:5.1f}%")
    
    return results


def run_validation(races_df, pedigrees_df, corners_df, race_details_df):
    """4年Walk-forward検証"""
    years = [2021, 2022, 2023, 2024]
    
    all_results = {
        'V15_Binary': [],
        'V44_Residual': [],
        'V15_V44_Ensemble': [],
        'V22_Binary': [],
        'V23_Hybrid': [],
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
        
        year_results = evaluate_year(
            train_df, test_df, pedigrees_df, corners_df, race_details_df, test_year
        )
        
        for model_name, result in year_results.items():
            result['year'] = test_year
            all_results[model_name].append(result)
    
    return all_results


def print_summary(all_results):
    """結果サマリー"""
    logger.info(f"\n{'='*100}")
    logger.info("4年平均結果サマリー (2021-2024)")
    logger.info(f"{'='*100}")
    
    summary_data = []
    
    for model_name, results in all_results.items():
        if not results:
            continue
        
        avg_roi = np.mean([r['test_roi'] for r in results])
        std_roi = np.std([r['test_roi'] for r in results])
        avg_gap = np.mean([r['gap'] for r in results])
        avg_hit = np.mean([r['hit_rate'] for r in results])
        avg_odds = np.mean([r['avg_win_odds'] for r in results])
        avg_pop = np.mean([r['avg_win_pop'] for r in results])
        avg_summer = np.mean([r['summer_roi'] for r in results])
        avg_weak = np.mean([r['weak_venue_roi'] for r in results])
        
        summary_data.append({
            'model': model_name,
            'roi': avg_roi,
            'std': std_roi,
            'gap': avg_gap,
            'hit_rate': avg_hit,
            'avg_odds': avg_odds,
            'avg_pop': avg_pop,
            'summer_roi': avg_summer,
            'weak_venue_roi': avg_weak,
        })
    
    # テーブル表示
    logger.info(f"\n{'モデル':20} | {'ROI':>6} | {'σ':>5} | {'Gap':>6} | {'的中率':>6} | {'平均オッズ':>8} | {'平均人気':>6} | {'夏場':>6} | {'苦手場':>6}")
    logger.info("-" * 100)
    
    for s in summary_data:
        logger.info(f"{s['model']:20} | {s['roi']:5.1f}% | {s['std']:4.1f}% | {s['gap']:5.1f}% | {s['hit_rate']:5.1f}% | {s['avg_odds']:7.1f}x | {s['avg_pop']:5.1f} | {s['summer_roi']:5.1f}% | {s['weak_venue_roi']:5.1f}%")
    
    # 勝者判定
    logger.info(f"\n{'='*100}")
    logger.info("評価判定")
    logger.info(f"{'='*100}")
    
    # ROI最高モデル
    best_roi_model = max(summary_data, key=lambda x: x['roi'])
    logger.info(f"  ROI最高: {best_roi_model['model']} ({best_roi_model['roi']:.1f}%)")
    
    # 安定性最高モデル
    best_stable_model = min(summary_data, key=lambda x: x['std'])
    logger.info(f"  最安定 (σ最小): {best_stable_model['model']} (σ={best_stable_model['std']:.1f}%)")
    
    # 夏場最高モデル
    best_summer_model = max(summary_data, key=lambda x: x['summer_roi'])
    logger.info(f"  夏場ROI最高: {best_summer_model['model']} ({best_summer_model['summer_roi']:.1f}%)")
    
    # V23 vs V15+V44 Ensemble比較
    v23 = next((s for s in summary_data if s['model'] == 'V23_Hybrid'), None)
    v15_ens = next((s for s in summary_data if s['model'] == 'V15_V44_Ensemble'), None)
    
    if v23 and v15_ens:
        logger.info(f"\n  V23 Hybrid vs V15+V4.4 Ensemble:")
        logger.info(f"    ROI: {v23['roi']:.1f}% vs {v15_ens['roi']:.1f}% ({v23['roi'] - v15_ens['roi']:+.1f}%)")
        logger.info(f"    Gap: {v23['gap']:.1f}% vs {v15_ens['gap']:.1f}% ({v23['gap'] - v15_ens['gap']:+.1f}%)")
        logger.info(f"    夏場: {v23['summer_roi']:.1f}% vs {v15_ens['summer_roi']:.1f}% ({v23['summer_roi'] - v15_ens['summer_roi']:+.1f}%)")
        
        if v23['roi'] > v15_ens['roi'] and v23['gap'] < 30:
            logger.info(f"\n  🎉 V23 Hybrid採用推奨！ROI改善かつ過学習なし")
        elif v23['roi'] > v15_ens['roi']:
            logger.info(f"\n  ⚠️ V23 HybridはROI改善だが、Gap要確認")
        else:
            logger.info(f"\n  ℹ️ V15+V4.4 Ensembleが依然最強")


def main():
    """メイン処理"""
    logger.info("V23 Hybrid Model 包括的検証")
    logger.info(f"実行日時: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 100)
    
    # データ読み込み
    races_df, pedigrees_df, corners_df, race_details_df = load_data()
    
    # 4年Walk-forward検証
    all_results = run_validation(races_df, pedigrees_df, corners_df, race_details_df)
    
    # サマリー表示
    print_summary(all_results)
    
    logger.info("\n検証完了")


if __name__ == "__main__":
    main()
