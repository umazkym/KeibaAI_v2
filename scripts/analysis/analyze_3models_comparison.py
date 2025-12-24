#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
3モデル比較の予測特性分析

【対象モデル】
1. V15 Fixed
2. V4.4 オリジナル
3. V4.4 正則化強化

【分析項目】
- 市場との比較（ROI、的中率）
- 穴馬的中パターン
- 的中馬vs不的中馬の特徴量差異
- モデル間の予測差異
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


def calc_stats(df, preds, name):
    """モデル別の統計を計算"""
    d = df.copy()
    d['score'] = preds
    d['ai_rank'] = d.groupby('race_id')['score'].rank(ascending=False, method='first')
    d['popularity'] = d.groupby('race_id')['win_odds'].rank(method='first')
    
    # AI 1位選択
    ai1 = d[d['ai_rank'] == 1]
    ai1_hits = ai1[ai1['finish_position'] == 1]
    
    # 人気1位
    pop1 = d[d['popularity'] == 1]
    pop1_hits = pop1[pop1['finish_position'] == 1]
    
    # 穴馬的中
    longshot_hits = ai1[(ai1['popularity'] > 3) & (ai1['finish_position'] == 1)]
    
    stats = {
        'name': name,
        'n_bets': len(ai1),
        'n_hits': len(ai1_hits),
        'hit_rate': len(ai1_hits) / len(ai1) * 100,
        'roi': ai1_hits['win_odds'].sum() / len(ai1) * 100,
        'pop1_roi': pop1_hits['win_odds'].sum() / len(pop1) * 100,
        'pop1_hit_rate': len(pop1_hits) / len(pop1) * 100,
        'longshot_hits': len(longshot_hits),
        'longshot_return': longshot_hits['win_odds'].sum(),
        'ai1_data': ai1,
        'ai1_hits': ai1_hits,
    }
    return stats


def analyze_hit_features(ai1_hits, ai1_miss, name, feature_cols):
    """的中馬vs不的中馬の特徴量差異"""
    logger.info(f"")
    logger.info(f"  【{name}：的中馬 vs 不的中馬の特徴量比較】")
    
    compare_features = ['horse_avg_c4_gap', 'horse_last_finish', 'jockey_top3_rate',
                       'heavy_track_fit', 'distance_change', 'horse_top3_rate',
                       'horse_last3_avg_finish', 'popularity']
    
    results = {}
    for feat in compare_features:
        if feat in ai1_hits.columns:
            hit_mean = ai1_hits[feat].mean()
            miss_mean = ai1_miss[feat].mean()
            if pd.notna(hit_mean) and pd.notna(miss_mean):
                diff = hit_mean - miss_mean
                diff_str = f"+{diff:.3f}" if diff > 0 else f"{diff:.3f}"
                logger.info(f"    {feat:30s}: 的中={hit_mean:.3f}, 不的中={miss_mean:.3f}, 差={diff_str}")
                results[feat] = {'hit': hit_mean, 'miss': miss_mean, 'diff': diff}
    
    return results


def main():
    logger.info("=" * 70)
    logger.info("3モデル比較の予測特性分析")
    logger.info("=" * 70)
    
    from keibaai.src.features.leak_free_feature_engineer_v15_fixed import LeakFreeFeatureEngineerV15Fixed
    
    races, pedigrees, corners, race_details, horses = load_data()
    
    train = races[races['race_date'] < '2023-01-01'].copy()
    test = races[(races['race_date'] >= '2024-01-01') & (races['race_date'] < '2025-01-01')].copy()
    
    logger.info(f"Train: {len(train):,}")
    logger.info(f"Test:  {len(test):,}")
    
    # 特徴量生成
    engine = LeakFreeFeatureEngineerV15Fixed()
    engine.fit(train, pedigrees, corners, race_details, horses_df=horses)
    
    train_f = engine.transform(train)
    test_f = engine.transform(test)
    
    feature_cols = [c for c in engine.get_feature_columns() if c in train_f.columns]
    
    # ===== モデル1: V15 Fixed =====
    logger.info("")
    logger.info("=" * 70)
    logger.info("モデル1: V15 Fixed 訓練中...")
    logger.info("=" * 70)
    
    v15_params = {
        'objective': 'binary', 'metric': 'auc', 'verbosity': -1,
        'learning_rate': 0.03, 'num_leaves': 20, 'max_depth': 3,
        'min_child_samples': 100, 'reg_alpha': 3.0, 'reg_lambda': 5.0,
        'bagging_fraction': 0.7, 'bagging_freq': 3, 'feature_fraction': 0.7,
    }
    
    X_train = train_f[feature_cols].fillna(0)
    y_train = (train_f['finish_position'] == 1).astype(int)
    train_ds = lgb.Dataset(X_train, y_train)
    model_v15 = lgb.train(v15_params, train_ds, num_boost_round=200)
    
    pred_v15 = model_v15.predict(test_f[feature_cols].fillna(0))
    
    # ===== モデル2: V4.4 オリジナル =====
    logger.info("モデル2: V4.4 オリジナル 訓練中...")
    
    v44_original = {
        'objective': 'lambdarank', 'boosting_type': 'gbdt',
        'num_leaves': 25, 'max_depth': 4, 'min_child_samples': 150,
        'learning_rate': 0.05, 'reg_alpha': 5.0, 'reg_lambda': 8.0,
        'feature_fraction': 0.6, 'bagging_fraction': 0.7, 'bagging_freq': 5,
        'verbose': -1, 'random_state': 42, 'label_gain': list(range(100))
    }
    
    # ターゲット計算（オッズ加重）
    weight_1st, weight_2nd, weight_3rd = 12.74, 6.73, 3.69
    odds = train_f['win_odds'].fillna(1.0).clip(upper=90)
    log_odds = np.log1p(odds)
    gain = np.zeros(len(train_f))
    gain[train_f['finish_position'] == 1] = log_odds[train_f['finish_position'] == 1] * weight_1st
    gain[train_f['finish_position'] == 2] = log_odds[train_f['finish_position'] == 2] * weight_2nd
    gain[train_f['finish_position'] == 3] = log_odds[train_f['finish_position'] == 3] * weight_3rd
    
    train_df = train_f.copy()
    train_df['target'] = gain.astype(int)
    train_df = train_df.sort_values('race_id')
    groups = train_df.groupby('race_id', sort=False).size().to_list()
    
    model_v44_orig = lgb.LGBMRanker(**v44_original, n_estimators=200)
    model_v44_orig.fit(train_df[feature_cols].fillna(0), train_df['target'], group=groups)
    
    pred_v44_orig = model_v44_orig.predict(test_f[feature_cols].fillna(0))
    
    # ===== モデル3: V4.4 正則化強化 =====
    logger.info("モデル3: V4.4 正則化強化 訓練中...")
    
    v44_regularized = {
        'objective': 'lambdarank', 'boosting_type': 'gbdt',
        'num_leaves': 15, 'max_depth': 3, 'min_child_samples': 300,
        'learning_rate': 0.03, 'reg_alpha': 10.0, 'reg_lambda': 15.0,
        'feature_fraction': 0.5, 'bagging_fraction': 0.6, 'bagging_freq': 3,
        'verbose': -1, 'random_state': 42, 'label_gain': list(range(100))
    }
    
    model_v44_reg = lgb.LGBMRanker(**v44_regularized, n_estimators=150)
    model_v44_reg.fit(train_df[feature_cols].fillna(0), train_df['target'], group=groups)
    
    pred_v44_reg = model_v44_reg.predict(test_f[feature_cols].fillna(0))
    
    # ===== 統計計算 =====
    stats_v15 = calc_stats(test_f, pred_v15, "V15 Fixed")
    stats_v44_orig = calc_stats(test_f, pred_v44_orig, "V4.4 オリジナル")
    stats_v44_reg = calc_stats(test_f, pred_v44_reg, "V4.4 正則化強化")
    
    # ===== 1. 市場との比較 =====
    logger.info("")
    logger.info("=" * 70)
    logger.info("1. 市場（人気1位）との比較")
    logger.info("=" * 70)
    
    logger.info(f"")
    logger.info(f"  人気1位: ROI={stats_v15['pop1_roi']:.1f}%, 的中率={stats_v15['pop1_hit_rate']:.1f}%")
    logger.info(f"")
    logger.info(f"  {'モデル':20s} {'ROI':>10s} {'的中率':>10s} {'ROI差分':>10s}")
    logger.info("-" * 55)
    
    for s in [stats_v15, stats_v44_orig, stats_v44_reg]:
        roi_diff = s['roi'] - s['pop1_roi']
        diff_str = f"+{roi_diff:.1f}%" if roi_diff > 0 else f"{roi_diff:.1f}%"
        logger.info(f"  {s['name']:20s} {s['roi']:>9.1f}% {s['hit_rate']:>9.1f}% {diff_str:>10s}")
    
    # ===== 2. 穴馬的中分析 =====
    logger.info("")
    logger.info("=" * 70)
    logger.info("2. 穴馬的中（4番人気以下で勝利）")
    logger.info("=" * 70)
    
    logger.info(f"")
    logger.info(f"  {'モデル':20s} {'穴馬的中数':>10s} {'穴馬回収':>12s} {'全体回収に占める割合':>20s}")
    logger.info("-" * 70)
    
    for s in [stats_v15, stats_v44_orig, stats_v44_reg]:
        total_return = s['ai1_hits']['win_odds'].sum()
        ratio = s['longshot_return'] / total_return * 100 if total_return > 0 else 0
        logger.info(f"  {s['name']:20s} {s['longshot_hits']:>10d} {s['longshot_return']:>11.1f}倍 {ratio:>19.1f}%")
    
    # ===== 3. 的中馬vs不的中馬の特徴量差異 =====
    logger.info("")
    logger.info("=" * 70)
    logger.info("3. 的中馬 vs 不的中馬の特徴量差異")
    logger.info("=" * 70)
    
    for s in [stats_v15, stats_v44_orig, stats_v44_reg]:
        ai1_miss = s['ai1_data'][s['ai1_data']['finish_position'] != 1]
        analyze_hit_features(s['ai1_hits'], ai1_miss, s['name'], feature_cols)
    
    # ===== 4. モデル間の予測差異 =====
    logger.info("")
    logger.info("=" * 70)
    logger.info("4. モデル間の予測差異分析")
    logger.info("=" * 70)
    
    test_f['v15_rank'] = test_f.groupby('race_id').apply(
        lambda x: pd.Series(pred_v15[x.index]).rank(ascending=False, method='first')
    ).values
    test_f['v44_orig_rank'] = test_f.groupby('race_id').apply(
        lambda x: pd.Series(pred_v44_orig[x.index]).rank(ascending=False, method='first')
    ).values
    test_f['v44_reg_rank'] = test_f.groupby('race_id').apply(
        lambda x: pd.Series(pred_v44_reg[x.index]).rank(ascending=False, method='first')
    ).values
    test_f['popularity'] = test_f.groupby('race_id')['win_odds'].rank(method='first')
    
    # 各モデルが1位と予測した馬の人気分布
    logger.info("")
    logger.info("  【各モデルが1位選択した馬の人気分布】")
    logger.info(f"  {'人気帯':12s} {'V15 Fixed':>12s} {'V4.4 Orig':>12s} {'V4.4 Reg':>12s}")
    logger.info("-" * 55)
    
    for pop_range in [(1, 1), (2, 3), (4, 6), (7, 18)]:
        pop_min, pop_max = pop_range
        label = f"{pop_min}-{pop_max}番人気" if pop_min != pop_max else f"{pop_min}番人気"
        
        v15_count = len(test_f[(test_f['v15_rank'] == 1) & (test_f['popularity'] >= pop_min) & (test_f['popularity'] <= pop_max)])
        v44_orig_count = len(test_f[(test_f['v44_orig_rank'] == 1) & (test_f['popularity'] >= pop_min) & (test_f['popularity'] <= pop_max)])
        v44_reg_count = len(test_f[(test_f['v44_reg_rank'] == 1) & (test_f['popularity'] >= pop_min) & (test_f['popularity'] <= pop_max)])
        
        logger.info(f"  {label:12s} {v15_count:>12d} {v44_orig_count:>12d} {v44_reg_count:>12d}")
    
    # モデル間の一致率
    v15_1 = test_f[test_f['v15_rank'] == 1]
    v44_orig_1 = test_f[test_f['v44_orig_rank'] == 1]
    v44_reg_1 = test_f[test_f['v44_reg_rank'] == 1]
    
    # V15とV4.4オリジナルの一致
    v15_v44_match = len(pd.merge(v15_1[['race_id', 'horse_id']], v44_orig_1[['race_id', 'horse_id']], on=['race_id', 'horse_id']))
    v15_v44_reg_match = len(pd.merge(v15_1[['race_id', 'horse_id']], v44_reg_1[['race_id', 'horse_id']], on=['race_id', 'horse_id']))
    v44_orig_reg_match = len(pd.merge(v44_orig_1[['race_id', 'horse_id']], v44_reg_1[['race_id', 'horse_id']], on=['race_id', 'horse_id']))
    
    n_races = len(test_f['race_id'].unique())
    
    logger.info("")
    logger.info("  【モデル間の1位選択一致率】")
    logger.info(f"    V15 Fixed ↔ V4.4 Orig:  {v15_v44_match}/{n_races}レース ({v15_v44_match/n_races*100:.1f}%)")
    logger.info(f"    V15 Fixed ↔ V4.4 Reg:   {v15_v44_reg_match}/{n_races}レース ({v15_v44_reg_match/n_races*100:.1f}%)")
    logger.info(f"    V4.4 Orig ↔ V4.4 Reg:   {v44_orig_reg_match}/{n_races}レース ({v44_orig_reg_match/n_races*100:.1f}%)")
    
    # ===== 5. 穴馬的中時の具体例（各モデル上位2件） =====
    logger.info("")
    logger.info("=" * 70)
    logger.info("5. 各モデルの穴馬的中Top2の具体例")
    logger.info("=" * 70)
    
    for s in [stats_v15, stats_v44_orig, stats_v44_reg]:
        ai1 = s['ai1_data']
        longshots = ai1[(ai1['popularity'] > 3) & (ai1['finish_position'] == 1)].sort_values('win_odds', ascending=False)
        
        logger.info(f"")
        logger.info(f"  ===== {s['name']} =====")
        
        for i, (_, row) in enumerate(longshots.head(2).iterrows()):
            logger.info(f"")
            logger.info(f"    [{i+1}] race_id: {row['race_id']}")
            logger.info(f"        人気: {int(row['popularity'])}番人気, オッズ: {row['win_odds']:.1f}倍")
            
            key_features = ['horse_last_finish', 'horse_last3_avg_finish', 'jockey_top3_rate', 
                           'horse_avg_c4_gap', 'distance_change']
            for feat in key_features:
                if feat in row.index and pd.notna(row[feat]):
                    logger.info(f"        {feat}: {row[feat]:.2f}")
    
    # ===== 6. サマリ =====
    logger.info("")
    logger.info("=" * 70)
    logger.info("6. サマリ")
    logger.info("=" * 70)
    
    logger.info("""
  【各モデルの特徴】
  
  ■ V15 Fixed
    - ROI: {v15_roi:.1f}% (人気比 +{v15_diff:.1f}%)
    - 穴馬率: {v15_ls_rate:.1f}%
    - 特徴: 安定志向、前走着順を重視
  
  ■ V4.4 オリジナル
    - ROI: {v44_orig_roi:.1f}% (人気比 +{v44_orig_diff:.1f}%)
    - 穴馬率: {v44_orig_ls_rate:.1f}%
    - 特徴: ハイリスク・ハイリターン、オッズ加重で穴馬狙い
  
  ■ V4.4 正則化強化
    - ROI: {v44_reg_roi:.1f}% (人気比 +{v44_reg_diff:.1f}%)
    - 穴馬率: {v44_reg_ls_rate:.1f}%
    - 特徴: V4.4オリジナルの穴馬狙いを抑制、安定化
    """.format(
        v15_roi=stats_v15['roi'],
        v15_diff=stats_v15['roi'] - stats_v15['pop1_roi'],
        v15_ls_rate=stats_v15['longshot_return'] / stats_v15['ai1_hits']['win_odds'].sum() * 100 if stats_v15['ai1_hits']['win_odds'].sum() > 0 else 0,
        v44_orig_roi=stats_v44_orig['roi'],
        v44_orig_diff=stats_v44_orig['roi'] - stats_v44_orig['pop1_roi'],
        v44_orig_ls_rate=stats_v44_orig['longshot_return'] / stats_v44_orig['ai1_hits']['win_odds'].sum() * 100 if stats_v44_orig['ai1_hits']['win_odds'].sum() > 0 else 0,
        v44_reg_roi=stats_v44_reg['roi'],
        v44_reg_diff=stats_v44_reg['roi'] - stats_v44_reg['pop1_roi'],
        v44_reg_ls_rate=stats_v44_reg['longshot_return'] / stats_v44_reg['ai1_hits']['win_odds'].sum() * 100 if stats_v44_reg['ai1_hits']['win_odds'].sum() > 0 else 0,
    ))
    
    logger.info("=" * 70)
    logger.info("完了")
    logger.info("=" * 70)


if __name__ == "__main__":
    main()
