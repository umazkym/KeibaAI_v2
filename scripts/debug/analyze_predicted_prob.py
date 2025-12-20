#!/usr/bin/env python3
"""
予測勝率の詳細分析

- 予測勝率の分布
- 予測勝率 vs 実際の勝率（キャリブレーション）
- 予測勝率 vs オッズ
- 期待値の分布と特性
- 勝ち馬の予測勝率分布
"""
from pathlib import Path
import sys
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

import pandas as pd
import numpy as np
import lightgbm as lgb
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def analyze_predicted_probabilities():
    """予測勝率の詳細分析"""
    from keibaai.src.features.leak_free_feature_engineer_v17 import LeakFreeFeatureEngineerV17
    
    # データ読み込み
    races = pd.read_parquet('keibaai/data/parsed/parquet/races/races.parquet')
    races = races[(races['finish_position'].notna()) & (races['finish_position'] > 0)].copy()
    races['race_date'] = pd.to_datetime(races['race_date'])
    
    try:
        pedigrees = pd.read_parquet('keibaai/data/parsed/parquet/pedigrees/pedigrees.parquet')
    except:
        pedigrees = None
    
    try:
        race_details = pd.read_parquet('keibaai/data/parsed/parquet/race_details/race_details.parquet')
    except:
        race_details = None
    
    corners = []
    for corner in [1, 2, 3, 4]:
        col = f'passing_order_{corner}'
        if col in races.columns:
            temp = races[['race_id', 'horse_number', col]].copy()
            temp = temp[temp[col].notna()]
            temp['corner'] = corner
            temp['position'] = temp[col]
            temp['gap_from_leader'] = 0
            corners.append(temp[['race_id', 'horse_number', 'corner', 'position', 'gap_from_leader']])
    corners_df = pd.concat(corners, ignore_index=True) if corners else None
    
    # 2024年で分析
    train_df = races[(races['race_date'] >= '2016-01-01') & (races['race_date'] <= '2023-12-31')].copy()
    test_df = races[(races['race_date'] >= '2024-01-01') & (races['race_date'] <= '2024-12-31')].copy()
    
    logger.info(f"Train: {len(train_df):,}, Test: {len(test_df):,}")
    
    # V17 Feature Engineering
    v17 = LeakFreeFeatureEngineerV17()
    v17.fit(train_df, pedigrees, corners_df, race_details)
    
    train_feat = v17.transform(train_df)
    test_feat = v17.transform(test_df)
    
    # ターゲット
    train_feat['is_win'] = (train_feat['finish_position'] == 1).astype(int)
    test_feat['is_win'] = (test_feat['finish_position'] == 1).astype(int)
    
    feature_cols = v17.get_feature_columns()
    available_cols = [c for c in feature_cols if c in train_feat.columns and c in test_feat.columns]
    
    X_train = train_feat[available_cols].fillna(-999)
    X_test = test_feat[available_cols].fillna(-999)
    
    # 勝率予測モデル
    params_cls = {
        'objective': 'binary',
        'metric': 'auc',
        'boosting_type': 'gbdt',
        'learning_rate': 0.05,
        'num_leaves': 63,
        'feature_fraction': 0.8,
        'bagging_fraction': 0.8,
        'bagging_freq': 5,
        'verbose': -1,
        'seed': 42,
        'is_unbalance': True
    }
    
    y_train = train_feat['is_win']
    train_data = lgb.Dataset(X_train, label=y_train)
    model = lgb.train(params_cls, train_data, num_boost_round=500)
    
    test_feat['pred_win_prob'] = model.predict(X_test)
    
    # === 分析1: 予測勝率の分布 ===
    logger.info(f"\n{'='*60}")
    logger.info("1. 予測勝率の分布")
    logger.info(f"{'='*60}")
    
    logger.info(f"平均: {test_feat['pred_win_prob'].mean()*100:.2f}%")
    logger.info(f"中央値: {test_feat['pred_win_prob'].median()*100:.2f}%")
    logger.info(f"最小: {test_feat['pred_win_prob'].min()*100:.2f}%")
    logger.info(f"最大: {test_feat['pred_win_prob'].max()*100:.2f}%")
    logger.info(f"標準偏差: {test_feat['pred_win_prob'].std()*100:.2f}%")
    
    # パーセンタイル分布
    percentiles = [1, 5, 10, 25, 50, 75, 90, 95, 99]
    logger.info(f"\nパーセンタイル分布:")
    for p in percentiles:
        val = np.percentile(test_feat['pred_win_prob'], p) * 100
        logger.info(f"  {p}%ile: {val:.2f}%")
    
    # === 分析2: 予測勝率 vs 実際の勝率（キャリブレーション） ===
    logger.info(f"\n{'='*60}")
    logger.info("2. キャリブレーション（予測勝率 vs 実際の勝率）")
    logger.info(f"{'='*60}")
    
    # 予測勝率をビンに分割
    bins = [0, 0.02, 0.05, 0.10, 0.15, 0.20, 0.30, 0.50, 1.0]
    test_feat['prob_bin'] = pd.cut(test_feat['pred_win_prob'], bins=bins)
    
    calibration = test_feat.groupby('prob_bin', observed=True).agg({
        'is_win': ['mean', 'sum', 'count'],
        'pred_win_prob': 'mean'
    }).round(4)
    calibration.columns = ['actual_win_rate', 'wins', 'count', 'pred_prob_mean']
    
    logger.info(f"\n予測勝率ビン | 予測平均 | 実勝率 | 件数 | 勝利数")
    for idx, row in calibration.iterrows():
        logger.info(f"{str(idx):20s} | {row['pred_prob_mean']*100:5.1f}% | {row['actual_win_rate']*100:5.1f}% | {row['count']:5.0f} | {row['wins']:4.0f}")
    
    # === 分析3: 予測勝率 vs オッズ ===
    logger.info(f"\n{'='*60}")
    logger.info("3. 予測勝率 vs オッズ")
    logger.info(f"{'='*60}")
    
    test_feat['implied_prob'] = 1 / test_feat['win_odds']
    
    corr = test_feat['pred_win_prob'].corr(test_feat['implied_prob'])
    logger.info(f"相関係数（予測勝率 vs オッズ換算勝率）: {corr:.4f}")
    
    # オッズ換算勝率との差
    test_feat['prob_diff'] = test_feat['pred_win_prob'] - test_feat['implied_prob']
    logger.info(f"\n予測 - オッズ換算の差:")
    logger.info(f"  平均: {test_feat['prob_diff'].mean()*100:+.2f}%")
    logger.info(f"  標準偏差: {test_feat['prob_diff'].std()*100:.2f}%")
    
    # オッズ帯別の分析
    odds_bins = [0, 2, 3, 5, 10, 20, 50, 100, 1000]
    test_feat['odds_bin'] = pd.cut(test_feat['win_odds'], bins=odds_bins)
    
    odds_analysis = test_feat.groupby('odds_bin', observed=True).agg({
        'pred_win_prob': 'mean',
        'implied_prob': 'mean',
        'is_win': 'mean',
        'race_id': 'count'
    }).round(4)
    odds_analysis.columns = ['pred_prob', 'implied_prob', 'actual_win_rate', 'count']
    
    logger.info(f"\nオッズ帯別分析:")
    logger.info(f"オッズ帯     | 予測勝率 | オッズ換算 | 実勝率 | 差(予測-換算)")
    for idx, row in odds_analysis.iterrows():
        diff = (row['pred_prob'] - row['implied_prob']) * 100
        logger.info(f"{str(idx):13s} | {row['pred_prob']*100:5.1f}% | {row['implied_prob']*100:6.1f}% | {row['actual_win_rate']*100:5.1f}% | {diff:+5.1f}%")
    
    # === 分析4: 勝ち馬の予測勝率 ===
    logger.info(f"\n{'='*60}")
    logger.info("4. 勝ち馬の予測勝率")
    logger.info(f"{'='*60}")
    
    winners = test_feat[test_feat['is_win'] == 1]
    losers = test_feat[test_feat['is_win'] == 0]
    
    logger.info(f"勝ち馬の予測勝率平均: {winners['pred_win_prob'].mean()*100:.2f}%")
    logger.info(f"負け馬の予測勝率平均: {losers['pred_win_prob'].mean()*100:.2f}%")
    logger.info(f"差: {(winners['pred_win_prob'].mean() - losers['pred_win_prob'].mean())*100:.2f}%")
    
    logger.info(f"\n勝ち馬の予測勝率分布:")
    for p in [10, 25, 50, 75, 90]:
        val = np.percentile(winners['pred_win_prob'], p) * 100
        logger.info(f"  {p}%ile: {val:.2f}%")
    
    # 勝ち馬が予測Top1に入っていた割合
    test_feat['pred_rank'] = test_feat.groupby('race_id')['pred_win_prob'].rank(ascending=False, method='first')
    top1_wins = test_feat[(test_feat['pred_rank'] == 1) & (test_feat['is_win'] == 1)]
    total_races = test_feat['race_id'].nunique()
    logger.info(f"\n予測Top1が勝ち馬だった割合: {len(top1_wins) / total_races * 100:.1f}%")
    
    # === 分析5: 期待値の分布 ===
    logger.info(f"\n{'='*60}")
    logger.info("5. 期待値(EV)の分布")
    logger.info(f"{'='*60}")
    
    test_feat['expected_value'] = test_feat['pred_win_prob'] * test_feat['win_odds']
    
    logger.info(f"期待値の統計:")
    logger.info(f"  平均: {test_feat['expected_value'].mean():.3f}")
    logger.info(f"  中央値: {test_feat['expected_value'].median():.3f}")
    logger.info(f"  標準偏差: {test_feat['expected_value'].std():.3f}")
    
    # EV>=1.0の馬の分析
    ev_high = test_feat[test_feat['expected_value'] >= 1.0]
    ev_low = test_feat[test_feat['expected_value'] < 1.0]
    
    logger.info(f"\nEV>=1.0の馬: {len(ev_high):,}頭 ({len(ev_high)/len(test_feat)*100:.1f}%)")
    logger.info(f"  実勝率: {ev_high['is_win'].mean()*100:.2f}%")
    logger.info(f"  予測勝率平均: {ev_high['pred_win_prob'].mean()*100:.2f}%")
    logger.info(f"  オッズ平均: {ev_high['win_odds'].mean():.1f}倍")
    
    logger.info(f"\nEV<1.0の馬: {len(ev_low):,}頭 ({len(ev_low)/len(test_feat)*100:.1f}%)")
    logger.info(f"  実勝率: {ev_low['is_win'].mean()*100:.2f}%")
    logger.info(f"  予測勝率平均: {ev_low['pred_win_prob'].mean()*100:.2f}%")
    logger.info(f"  オッズ平均: {ev_low['win_odds'].mean():.1f}倍")
    
    # === 分析6: EV最高馬の特性 ===
    logger.info(f"\n{'='*60}")
    logger.info("6. 各レースでEV最高馬の特性")
    logger.info(f"{'='*60}")
    
    test_feat['ev_rank'] = test_feat.groupby('race_id')['expected_value'].rank(ascending=False, method='first')
    ev_top1 = test_feat[test_feat['ev_rank'] == 1]
    
    logger.info(f"EV Top1の馬:")
    logger.info(f"  予測勝率平均: {ev_top1['pred_win_prob'].mean()*100:.2f}%")
    logger.info(f"  オッズ平均: {ev_top1['win_odds'].mean():.1f}倍")
    logger.info(f"  実勝率: {ev_top1['is_win'].mean()*100:.2f}%")
    logger.info(f"  期待値平均: {ev_top1['expected_value'].mean():.2f}")
    
    # 人気順との関係
    if 'popularity' in ev_top1.columns:
        logger.info(f"\n  EV Top1の人気: {ev_top1['popularity'].mean():.1f}番人気（平均）")
        pop_dist = ev_top1['popularity'].value_counts().sort_index().head(10)
        logger.info(f"  人気分布 (Top10):")
        for pop, cnt in pop_dist.items():
            pct = cnt / len(ev_top1) * 100
            logger.info(f"    {pop}番人気: {cnt}頭 ({pct:.1f}%)")
    
    # === 分析7: 予測Top1 vs EV Top1の比較 ===
    logger.info(f"\n{'='*60}")
    logger.info("7. 予測Top1 vs EV Top1 比較")
    logger.info(f"{'='*60}")
    
    pred_top1 = test_feat[test_feat['pred_rank'] == 1]
    
    logger.info(f"予測勝率 Top1:")
    logger.info(f"  予測勝率平均: {pred_top1['pred_win_prob'].mean()*100:.2f}%")
    logger.info(f"  オッズ平均: {pred_top1['win_odds'].mean():.1f}倍")
    logger.info(f"  実勝率: {pred_top1['is_win'].mean()*100:.2f}%")
    
    logger.info(f"\nEV Top1:")
    logger.info(f"  予測勝率平均: {ev_top1['pred_win_prob'].mean()*100:.2f}%")
    logger.info(f"  オッズ平均: {ev_top1['win_odds'].mean():.1f}倍")
    logger.info(f"  実勝率: {ev_top1['is_win'].mean()*100:.2f}%")
    
    # 両者が一致するケース
    merge = test_feat[(test_feat['pred_rank'] == 1) & (test_feat['ev_rank'] == 1)]
    logger.info(f"\n予測Top1 = EV Top1 の一致率: {len(merge) / total_races * 100:.1f}%")


if __name__ == "__main__":
    analyze_predicted_probabilities()
