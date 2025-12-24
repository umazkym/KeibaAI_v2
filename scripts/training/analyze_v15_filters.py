#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
V15 運用フィルタ分析スクリプト

ドキュメントで言及された「AI ≠ 人気」フィルタの効果を検証。

【検証項目】
1. AI予測1位 ≠ 人気1位 の場合のROI
2. 人気帯別のROI分析
3. オッズ帯別のROI分析
4. 騎手勝率帯別のROI分析

【目的】
- フィルタリングによるROI最大化戦略の検証
- モデル改修なしで運用改善を実現
"""
import pandas as pd
import numpy as np
import lightgbm as lgb
from pathlib import Path
import logging
import sys
import pickle
import warnings
warnings.filterwarnings('ignore')

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))
from keibaai.src.features.leak_free_feature_engineer_v15 import LeakFreeFeatureEngineerV15


def load_data():
    data_dir = project_root / "keibaai/data/parsed/parquet"
    
    races_df = pd.read_parquet(data_dir / "races/races.parquet")
    pedigrees = pd.read_parquet(data_dir / "pedigrees/pedigrees.parquet")
    corners = pd.read_parquet(data_dir / "corners/corner_positions.parquet")
    race_details = pd.read_parquet(data_dir / "race_details/race_details.parquet")
    horses = pd.read_parquet(data_dir / "horses/horses.parquet")
    returns_df = pd.read_parquet(data_dir / "returns/returns.parquet")
    
    races_df = races_df[races_df['finish_position'].notna()].copy()
    races_df['race_date'] = pd.to_datetime(races_df['race_date'])
    
    return races_df, pedigrees, corners, race_details, horses, returns_df


def calc_roi_stats(df, suffix=""):
    """ROI統計を計算"""
    if len(df) == 0:
        return {f'n_bets{suffix}': 0, f'n_hits{suffix}': 0, f'roi{suffix}': 0, f'hit_rate{suffix}': 0}
    
    hits = df[df['finish_position'] == 1]
    roi = hits['win_odds'].sum() / len(df) * 100
    hit_rate = len(hits) / len(df) * 100
    
    return {
        f'n_bets{suffix}': len(df),
        f'n_hits{suffix}': len(hits),
        f'roi{suffix}': roi,
        f'hit_rate{suffix}': hit_rate
    }


def main():
    logger.info("=" * 60)
    logger.info("V15 運用フィルタ分析")
    logger.info("=" * 60)
    
    races, pedigrees, corners, race_details, horses, returns_df = load_data()
    
    # 2020-2024年のデータで分析（過去5年）
    test_years = [2020, 2021, 2022, 2023, 2024]
    test_data = races[(races['race_date'].dt.year >= 2020) & 
                      (races['race_date'].dt.year <= 2024)].copy()
    
    logger.info(f"  分析対象: {len(test_data):,}件 (2020-2024)")
    
    # V15モデルを読み込み（なければ訓練）
    model_path = project_root / "keibaai/models/v15/v15_model.pkl"
    
    if model_path.exists():
        logger.info("  既存V15モデルを読み込み...")
        with open(model_path, 'rb') as f:
            model = pickle.load(f)
        
        # 特徴量エンジンをfit
        train = races[races['race_date'] < '2020-01-01'].copy()
        engine = LeakFreeFeatureEngineerV15()
        engine.fit(train, pedigrees, corners, race_details, horses_df=horses)
        
        # Test特徴量生成
        test_f = engine.transform(test_data)
        feature_cols = [c for c in engine.get_feature_columns() if c in test_f.columns]
        
        X_test = test_f[feature_cols].fillna(0)
        test_f['pred'] = model.predict(X_test)
    else:
        logger.info("  V15モデルを訓練中...")
        train = races[races['race_date'] < '2020-01-01'].copy()
        
        engine = LeakFreeFeatureEngineerV15()
        engine.fit(train, pedigrees, corners, race_details, horses_df=horses)
        
        train_f = engine.transform(train)
        test_f = engine.transform(test_data)
        
        feature_cols = [c for c in engine.get_feature_columns() if c in train_f.columns]
        
        X_train = train_f[feature_cols].fillna(0)
        y_train = (train_f['finish_position'] == 1).astype(int)
        
        params = {
            'objective': 'binary',
            'metric': 'auc',
            'verbosity': -1,
            'learning_rate': 0.03,
            'num_leaves': 20,
            'max_depth': 3,
            'min_child_samples': 100,
            'reg_alpha': 3.0,
            'reg_lambda': 5.0,
        }
        
        train_ds = lgb.Dataset(X_train, y_train)
        model = lgb.train(params, train_ds, num_boost_round=200)
        
        X_test = test_f[feature_cols].fillna(0)
        test_f['pred'] = model.predict(X_test)
    
    # 予測ランクと人気を計算
    test_f['ai_rank'] = test_f.groupby('race_id')['pred'].rank(ascending=False, method='first').astype(int)
    
    # 人気を追加（win_oddsからランク計算）
    test_f['odds_rank'] = test_f.groupby('race_id')['win_odds'].rank(ascending=True, method='first').astype(int)
    
    # AI予測1位のみ抽出
    ai_top1 = test_f[test_f['ai_rank'] == 1].copy()
    
    logger.info("")
    logger.info("=" * 60)
    logger.info("1. 全体ベースライン")
    logger.info("=" * 60)
    baseline = calc_roi_stats(ai_top1)
    logger.info(f"  賭け数: {baseline['n_bets']:,}件")
    logger.info(f"  的中: {baseline['n_hits']:,}件")
    logger.info(f"  ROI: {baseline['roi']:.1f}%")
    logger.info(f"  的中率: {baseline['hit_rate']:.1f}%")
    
    # ==============================
    # 2. AI ≠ 人気フィルタ
    # ==============================
    logger.info("")
    logger.info("=" * 60)
    logger.info("2. AI ≠ 人気フィルタ分析")
    logger.info("=" * 60)
    
    # AI予測1位と人気1位が一致するか
    ai_top1['ai_eq_pop'] = (ai_top1['odds_rank'] == 1)
    
    # 一致する場合
    match = ai_top1[ai_top1['ai_eq_pop'] == True]
    match_stats = calc_roi_stats(match)
    logger.info(f"\n  [AI = 人気1位] {match_stats['n_bets']:,}件")
    logger.info(f"    ROI: {match_stats['roi']:.1f}%, 的中率: {match_stats['hit_rate']:.1f}%")
    
    # 一致しない場合
    diff = ai_top1[ai_top1['ai_eq_pop'] == False]
    diff_stats = calc_roi_stats(diff)
    logger.info(f"\n  [AI ≠ 人気1位] {diff_stats['n_bets']:,}件 ⭐")
    logger.info(f"    ROI: {diff_stats['roi']:.1f}%, 的中率: {diff_stats['hit_rate']:.1f}%")
    logger.info(f"    ⚡ 改善: {diff_stats['roi'] - baseline['roi']:+.1f}%")
    
    # ==============================
    # 3. オッズ帯別分析
    # ==============================
    logger.info("")
    logger.info("=" * 60)
    logger.info("3. オッズ帯別ROI分析")
    logger.info("=" * 60)
    
    odds_bins = [(0, 3), (3, 5), (5, 10), (10, 20), (20, 50), (50, 100), (100, 999)]
    
    for low, high in odds_bins:
        subset = ai_top1[(ai_top1['win_odds'] >= low) & (ai_top1['win_odds'] < high)]
        stats = calc_roi_stats(subset)
        if stats['n_bets'] > 50:
            logger.info(f"  オッズ {low:3d}-{high:3d}: ROI={stats['roi']:6.1f}%, 的中率={stats['hit_rate']:5.1f}%, 賭け数={stats['n_bets']:,}")
    
    # ==============================
    # 4. 人気帯別×AI差分分析
    # ==============================
    logger.info("")
    logger.info("=" * 60)
    logger.info("4. 人気帯別×AI差分分析")
    logger.info("=" * 60)
    
    for pop in [1, 2, 3, 4, 5]:
        subset = ai_top1[ai_top1['odds_rank'] == pop]
        stats = calc_roi_stats(subset)
        if stats['n_bets'] > 50:
            marker = "✅" if stats['roi'] > 100 else ("⚠️" if stats['roi'] > 80 else "❌")
            logger.info(f"  人気{pop}位をAI1位予測: ROI={stats['roi']:6.1f}%, 的中率={stats['hit_rate']:5.1f}%, 件数={stats['n_bets']:,} {marker}")
    
    # ==============================
    # 5. 年別×フィルタ別分析
    # ==============================
    logger.info("")
    logger.info("=" * 60)
    logger.info("5. 年別×フィルタ別分析")
    logger.info("=" * 60)
    
    ai_top1['year'] = ai_top1['race_date'].dt.year
    
    logger.info(f"\n  {'Year':<6} {'全体ROI':>10} {'AI≠人気ROI':>12} {'件数':>10}")
    logger.info(f"  {'-'*6} {'-'*10} {'-'*12} {'-'*10}")
    
    yearly_results = []
    for year in sorted(ai_top1['year'].unique()):
        year_all = ai_top1[ai_top1['year'] == year]
        year_diff = ai_top1[(ai_top1['year'] == year) & (ai_top1['ai_eq_pop'] == False)]
        
        all_stats = calc_roi_stats(year_all)
        diff_stats = calc_roi_stats(year_diff)
        
        improvement = diff_stats['roi'] - all_stats['roi']
        marker = "📈" if improvement > 0 else "📉"
        
        logger.info(f"  {year:<6} {all_stats['roi']:>10.1f}% {diff_stats['roi']:>12.1f}% {diff_stats['n_bets']:>10} {marker}")
        
        yearly_results.append({
            'year': year,
            'all_roi': all_stats['roi'],
            'filtered_roi': diff_stats['roi'],
            'improvement': improvement
        })
    
    # 平均改善
    avg_improvement = np.mean([r['improvement'] for r in yearly_results])
    logger.info(f"\n  平均改善効果: {avg_improvement:+.1f}%")
    
    # ==============================
    # 6. 最適フィルタ組み合わせ
    # ==============================
    logger.info("")
    logger.info("=" * 60)
    logger.info("6. 最適フィルタ組み合わせ探索")
    logger.info("=" * 60)
    
    # AI ≠ 人気 かつ オッズ制限
    best_roi = 0
    best_config = {}
    
    for max_odds in [20, 30, 50, 60, 100]:
        for min_pop_diff in [1, 2]:  # AI予測と人気の差が1以上、2以上
            subset = ai_top1[
                (ai_top1['ai_eq_pop'] == False) &
                (ai_top1['win_odds'] < max_odds) &
                (abs(ai_top1['odds_rank'] - 1) >= min_pop_diff)
            ]
            stats = calc_roi_stats(subset)
            
            if stats['n_bets'] >= 500 and stats['roi'] > best_roi:
                best_roi = stats['roi']
                best_config = {
                    'max_odds': max_odds,
                    'min_pop_diff': min_pop_diff,
                    **stats
                }
    
    if best_config:
        logger.info(f"\n  ⭐ 最適フィルタ:")
        logger.info(f"    条件: AI≠人気 & オッズ<{best_config['max_odds']} & 人気差>={best_config['min_pop_diff']}")
        logger.info(f"    ROI: {best_config['roi']:.1f}%")
        logger.info(f"    的中率: {best_config['hit_rate']:.1f}%")
        logger.info(f"    賭け数: {best_config['n_bets']:,}件")
        logger.info(f"    改善: {best_config['roi'] - baseline['roi']:+.1f}%")
    
    # ==============================
    # 7. 複勝（2位予測）分析
    # ==============================
    logger.info("")
    logger.info("=" * 60)
    logger.info("7. 複勝向け分析（AI 2位予測）")
    logger.info("=" * 60)
    
    ai_top2 = test_f[test_f['ai_rank'] == 2].copy()
    ai_top2['is_top3'] = (ai_top2['finish_position'] <= 3)
    
    # 複勝的中率
    top3_rate = ai_top2['is_top3'].mean() * 100
    logger.info(f"  AI 2位予測の3着内率: {top3_rate:.1f}%")
    
    # 人気との比較
    pop2 = test_f[test_f['odds_rank'] == 2].copy()
    pop2['is_top3'] = (pop2['finish_position'] <= 3)
    pop2_rate = pop2['is_top3'].mean() * 100
    logger.info(f"  人気 2位馬の3着内率: {pop2_rate:.1f}%")
    logger.info(f"  差分: {top3_rate - pop2_rate:+.1f}%")
    
    logger.info("")
    logger.info("=" * 60)
    logger.info("分析完了")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
