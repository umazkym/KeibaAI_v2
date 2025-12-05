#!/usr/bin/env python3
"""
Edge≥0.15の194件を詳細分析スクリプト

【目的】
- Edge閾値0.15の194件がどんなレースかを詳細分析
- 統計的有意性（分散、信頼区間）の検証
- 時系列での安定性確認
"""

import sys
from pathlib import Path

project_root = Path(__file__).resolve().parent.parent.parent
sys.path.append(str(project_root))

import pandas as pd
import numpy as np
import pickle
import json
import logging
import re
from datetime import datetime
from scipy.special import softmax
from scipy import stats

from keibaai.src.models.calibration import IsotonicCalibrator

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def run_edge_deep_analysis():
    """Edge≥0.15の詳細分析"""
    logging.info("=" * 70)
    logging.info("Edge≥0.15 詳細分析")
    logging.info("=" * 70)
    
    # データ読み込み（簡易版）
    base_model_dir = Path('keibaai/models/mu_v3_3')
    v54_dir = Path('keibaai/models/mu_v5_4')
    v60_dir = Path('keibaai/models/mu_v6_0')
    
    # モデル
    with open(v54_dir / 'mu_v5_4_ranker.pkl', 'rb') as f:
        model = pickle.load(f)
    with open(v54_dir / 'feature_names.json', 'r', encoding='utf-8') as f:
        feature_cols = json.load(f)
    calibrator = IsotonicCalibrator.load(v60_dir / 'isotonic_calibrator.pkl')
    
    # データ
    train_data = pd.read_parquet(base_model_dir / 'train_data_mu_v3_3.parquet')
    train_data['race_date'] = pd.to_datetime(train_data['race_date'])
    
    races_df = pd.read_parquet('keibaai/data/parsed/parquet/races/races.parquet')
    races_df['race_date'] = pd.to_datetime(races_df['race_date'])
    races_df['horse_id'] = races_df['horse_id'].astype(str)
    train_data['horse_id'] = train_data['horse_id'].astype(str)
    
    # マージ
    race_info = races_df[['race_id', 'horse_id', 'track_condition', 'distance_m', 'track_surface', 'venue']].drop_duplicates(subset=['race_id', 'horse_id'])
    for col in ['track_condition', 'distance_m', 'track_surface', 'venue']:
        if col not in train_data.columns:
            train_data = train_data.merge(race_info[['race_id', 'horse_id', col]], on=['race_id', 'horse_id'], how='left')
    
    # Test期間
    test_df = train_data[train_data['race_date'] >= '2024-01-01'].copy()
    logging.info(f"Testデータ: {len(test_df):,}")
    
    # 特徴量補完
    for col in feature_cols:
        if col in test_df.columns:
            test_df[col] = test_df[col].fillna(test_df[col].median())
    available_features = [f for f in feature_cols if f in test_df.columns]
    
    # 予測
    test_df['score'] = model.predict(test_df[available_features])
    test_df['pred_prob'] = test_df.groupby('race_id')['score'].transform(lambda x: softmax(x.values))
    test_df['calibrated_prob'] = calibrator.predict_proba(test_df['pred_prob'].values)
    
    # 市場勝率
    test_df['inv_odds'] = 1 / test_df['win_odds'].clip(lower=1.1)
    test_df['sum_inv_odds'] = test_df.groupby('race_id')['inv_odds'].transform('sum')
    test_df['market_prob'] = test_df['inv_odds'] / test_df['sum_inv_odds']
    
    # Edge
    test_df['edge'] = test_df['calibrated_prob'] - test_df['market_prob']
    test_df['rank_pred'] = test_df.groupby('race_id')['score'].rank(ascending=False, method='first')
    
    # Edge≥0.15 かつ Top1
    high_edge = test_df[(test_df['rank_pred'] == 1) & (test_df['edge'] >= 0.15)].copy()
    high_edge['is_hit'] = (high_edge['finish_position'] == 1).astype(int)
    
    logging.info(f"\n対象件数: {len(high_edge)}")
    logging.info(f"的中数: {high_edge['is_hit'].sum()}")
    logging.info(f"的中率: {high_edge['is_hit'].mean():.1%}")
    roi = high_edge[high_edge['is_hit'] == 1]['win_odds'].sum() / len(high_edge)
    logging.info(f"ROI: {roi:.2%}")
    
    # === 統計的有意性 ===
    logging.info("\n【統計的有意性】")
    
    # 的中率の95%信頼区間（二項分布）
    n = len(high_edge)
    hits = high_edge['is_hit'].sum()
    hit_rate = hits / n
    se = np.sqrt(hit_rate * (1 - hit_rate) / n)
    ci_lower = hit_rate - 1.96 * se
    ci_upper = hit_rate + 1.96 * se
    logging.info(f"  的中率 95%CI: [{ci_lower:.1%}, {ci_upper:.1%}]")
    
    # ROIの信頼区間（ブートストラップ）
    n_bootstrap = 1000
    roi_samples = []
    odds_values = high_edge['win_odds'].values
    hit_values = high_edge['is_hit'].values
    for _ in range(n_bootstrap):
        idx = np.random.choice(len(high_edge), size=len(high_edge), replace=True)
        sample_roi = (hit_values[idx] * odds_values[idx]).sum() / len(idx)
        roi_samples.append(sample_roi)
    roi_lower = np.percentile(roi_samples, 2.5)
    roi_upper = np.percentile(roi_samples, 97.5)
    logging.info(f"  ROI 95%CI (Bootstrap): [{roi_lower:.2%}, {roi_upper:.2%}]")
    
    # ベースラインとの比較（全レースTop1）
    baseline = test_df[test_df['rank_pred'] == 1]
    baseline_hit_rate = (baseline['finish_position'] == 1).mean()
    baseline_roi = baseline[baseline['finish_position'] == 1]['win_odds'].sum() / len(baseline)
    
    logging.info(f"\n  ベースライン（全レースTop1）:")
    logging.info(f"    的中率: {baseline_hit_rate:.1%}")
    logging.info(f"    ROI: {baseline_roi:.2%}")
    
    # 有意差検定（的中率）
    chi2, p_value = stats.fisher_exact([[hits, n - hits], 
                                         [int(baseline_hit_rate * len(baseline)), 
                                          len(baseline) - int(baseline_hit_rate * len(baseline))]])
    logging.info(f"  Fisher正確検定 p値: {p_value:.4f}")
    
    if p_value < 0.05:
        logging.info("  ✓ 的中率の差は統計的に有意 (p < 0.05)")
    else:
        logging.info("  ⚠️ 統計的に有意でない可能性あり")
    
    # === 属性分析 ===
    logging.info("\n【属性分布】")
    
    # 1. 人気分布
    logging.info("\n1. 人気分布:")
    pop_stats = high_edge.groupby('popularity')['is_hit'].agg(['sum', 'count', 'mean'])
    for pop in sorted(high_edge['popularity'].dropna().unique())[:10]:
        row = pop_stats.loc[pop]
        sub = high_edge[high_edge['popularity'] == pop]
        sub_roi = sub[sub['is_hit'] == 1]['win_odds'].sum() / len(sub) if len(sub) > 0 else 0
        logging.info(f"   {int(pop)}番人気: {int(row['count'])}件, 的中:{int(row['sum'])}, 的中率:{row['mean']:.1%}, ROI:{sub_roi:.1%}")
    
    # 2. オッズ帯
    logging.info("\n2. オッズ帯分布:")
    for low, high in [(1, 5), (5, 10), (10, 20), (20, 50), (50, 100)]:
        mask = (high_edge['win_odds'] >= low) & (high_edge['win_odds'] < high)
        sub = high_edge[mask]
        if len(sub) > 0:
            sub_roi = sub[sub['is_hit'] == 1]['win_odds'].sum() / len(sub)
            logging.info(f"   {low}-{high}倍: {len(sub)}件, 的中:{sub['is_hit'].sum()}, ROI:{sub_roi:.1%}")
    
    # 3. 時系列
    logging.info("\n3. 時系列（月別）:")
    high_edge['month'] = high_edge['race_date'].dt.to_period('M')
    for month in sorted(high_edge['month'].unique()):
        sub = high_edge[high_edge['month'] == month]
        sub_roi = sub[sub['is_hit'] == 1]['win_odds'].sum() / len(sub) if len(sub) > 0 else 0
        logging.info(f"   {month}: {len(sub)}件, 的中:{sub['is_hit'].sum()}, ROI:{sub_roi:.1%}")
    
    # 4. 馬場状態
    logging.info("\n4. 馬場状態:")
    for cond in ['良', '稍重', '重', '不良']:
        sub = high_edge[high_edge['track_condition'] == cond]
        if len(sub) > 0:
            sub_roi = sub[sub['is_hit'] == 1]['win_odds'].sum() / len(sub)
            logging.info(f"   {cond}: {len(sub)}件, 的中:{sub['is_hit'].sum()}, ROI:{sub_roi:.1%}")
    
    # === サンプルレース ===
    logging.info("\n【サンプル（的中レース上位5件）】")
    hit_races = high_edge[high_edge['is_hit'] == 1].nlargest(5, 'win_odds')
    for _, row in hit_races.iterrows():
        logging.info(f"  {row['race_date'].strftime('%Y-%m-%d')}: オッズ{row['win_odds']:.1f}倍, {int(row['popularity'])}番人気, Edge:{row['edge']:.3f}")
    
    logging.info("\n" + "=" * 70)
    
    return {
        'count': len(high_edge),
        'hits': int(high_edge['is_hit'].sum()),
        'hit_rate': float(high_edge['is_hit'].mean()),
        'roi': float(roi),
        'roi_ci_lower': float(roi_lower),
        'roi_ci_upper': float(roi_upper),
    }


if __name__ == "__main__":
    run_edge_deep_analysis()
