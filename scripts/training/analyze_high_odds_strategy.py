"""
高オッズ特化戦略の評価

発見: 50-100倍帯でROI 101.2%
このセグメントに焦点を当て、詳細な検証を行う。

検証項目:
1. 統計的有意性の確認（サンプル数、時系列安定性）
2. 最適な閾値の探索
3. 複数馬券戦略（複勝、馬連）の可能性
"""

import sys
from pathlib import Path
import numpy as np
import pandas as pd
import logging
from scipy import stats

project_root = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(project_root))

from keibaai.src.features.leak_free_feature_engineer import LeakFreeFeatureEngineer

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def main():
    data_dir = Path("keibaai/data")
    
    # データ読み込み
    logger.info("データ読み込み中...")
    races_df = pd.read_parquet(data_dir / "parsed/parquet/races/races.parquet")
    races_df['race_date'] = pd.to_datetime(races_df['race_date'])
    races_df = races_df[(races_df['race_date'] >= '2020-01-01') & (races_df['race_date'] <= '2024-12-31')]
    races_df = races_df.dropna(subset=['finish_position', 'win_odds'])
    
    pedigrees_df = pd.read_parquet(data_dir / "parsed/parquet/pedigrees/pedigrees.parquet")
    
    # 時系列分割
    max_date = races_df['race_date'].max()
    test_start = max_date - pd.DateOffset(months=12)  # 1年間のテスト
    train_end = test_start - pd.DateOffset(months=1)
    
    train_df = races_df[races_df['race_date'] < train_end].copy()
    test_df = races_df[races_df['race_date'] >= test_start].copy()
    
    logger.info(f"Train: {len(train_df):,} records ({train_df['race_date'].min()} - {train_df['race_date'].max()})")
    logger.info(f"Test: {len(test_df):,} records ({test_df['race_date'].min()} - {test_df['race_date'].max()})")
    
    # 特徴量生成
    logger.info("\n特徴量生成中...")
    feature_engineer = LeakFreeFeatureEngineer()
    feature_engineer.fit(train_df, pedigrees_df)
    
    train_features = feature_engineer.transform(train_df)
    test_features = feature_engineer.transform(test_df)
    
    feature_cols = feature_engineer.get_feature_columns()
    feature_cols = [c for c in feature_cols if c in train_features.columns]
    
    # モデル学習
    logger.info("\nモデル学習中...")
    import lightgbm as lgb
    
    train_features_sorted = train_features.sort_values('race_id')
    X_train = train_features_sorted[feature_cols].fillna(0)
    y_train = train_features_sorted['finish_position']
    groups_sorted = train_features_sorted['race_id']
    group_sizes = groups_sorted.value_counts().sort_index().values
    
    y_relevance = np.zeros(len(y_train))
    y_relevance[y_train.values == 1] = 5
    y_relevance[y_train.values == 2] = 4
    y_relevance[y_train.values == 3] = 3
    y_relevance[(y_train.values >= 4) & (y_train.values <= 5)] = 1
    
    model = lgb.LGBMRanker(
        objective='lambdarank',
        metric='ndcg',
        n_estimators=500,
        learning_rate=0.05,
        max_depth=5,
        num_leaves=31,
        random_state=42,
        n_jobs=-1,
        verbose=-1
    )
    
    model.fit(X_train, y_relevance, group=group_sizes)
    logger.info("学習完了")
    
    # テスト予測
    X_test = test_features[feature_cols].fillna(0)
    test_features = test_features.copy()
    test_features['score'] = model.predict(X_test)
    test_features['rank'] = test_features.groupby('race_id')['score'].rank(ascending=False)
    
    # 人気計算
    if 'popularity' not in test_features.columns:
        test_features['popularity'] = test_features.groupby('race_id')['win_odds'].rank()
    
    # 月ごとの情報
    test_features['month'] = test_features['race_date'].dt.to_period('M')
    
    # =========================================================================
    # 分析1: 高オッズ帯の詳細分析
    # =========================================================================
    logger.info("\n" + "=" * 70)
    logger.info("高オッズ特化戦略の分析")
    logger.info("=" * 70)
    
    # Top1選択のみ（モデル予測1位）
    top1_df = test_features[test_features['rank'] == 1].copy()
    
    # オッズ帯別の詳細分析
    odds_ranges = [
        (1, 3), (3, 5), (5, 10), (10, 20), (20, 30), (30, 50), (50, 80), (80, 150), (150, 500)
    ]
    
    logger.info("\n[1] オッズ帯別のTop1戦略成績:")
    logger.info("-" * 60)
    logger.info(f"{'オッズ帯':>15} {'件数':>8} {'的中':>6} {'的中率':>8} {'ROI':>10} {'95%CI':>15}")
    logger.info("-" * 60)
    
    for odds_min, odds_max in odds_ranges:
        subset = top1_df[(top1_df['win_odds'] >= odds_min) & (top1_df['win_odds'] < odds_max)]
        if len(subset) == 0:
            continue
            
        hits = (subset['finish_position'] == 1).sum()
        hit_rate = hits / len(subset) * 100
        
        # ROI計算
        bet_amount = len(subset) * 100
        payout = (subset[subset['finish_position'] == 1]['win_odds'] * 100).sum()
        roi = payout / bet_amount * 100
        
        # 95%信頼区間（二項分布）
        if len(subset) > 0:
            ci_low, ci_high = stats.binom.interval(0.95, len(subset), hits/len(subset) if hits > 0 else 0.001)
            avg_odds = subset['win_odds'].mean()
            roi_low = (ci_low * avg_odds * 100) / bet_amount * 100
            roi_high = (ci_high * avg_odds * 100) / bet_amount * 100
        else:
            roi_low, roi_high = 0, 0
        
        status = "✓" if roi >= 100 else ""
        logger.info(f"{odds_min}-{odds_max}倍: {len(subset):>6} {hits:>6} {hit_rate:>7.1f}% {roi:>9.1f}% [{roi_low:.0f}-{roi_high:.0f}%] {status}")
    
    # =========================================================================
    # 分析2: 50倍以上の馬に絞った月別安定性
    # =========================================================================
    logger.info("\n\n[2] 50倍以上のTop1選択 - 月別推移:")
    logger.info("-" * 60)
    
    high_odds_df = top1_df[top1_df['win_odds'] >= 50].copy()
    
    monthly_results = []
    for month in sorted(high_odds_df['month'].unique()):
        month_data = high_odds_df[high_odds_df['month'] == month]
        hits = (month_data['finish_position'] == 1).sum()
        bet = len(month_data) * 100
        payout = (month_data[month_data['finish_position'] == 1]['win_odds'] * 100).sum()
        roi = payout / bet * 100 if bet > 0 else 0
        monthly_results.append({'month': str(month), 'bets': len(month_data), 'hits': hits, 'roi': roi})
        status = "✓" if roi >= 100 else ""
        logger.info(f"  {month}: {len(month_data):>4}件, 的中{hits:>2}, ROI {roi:>6.1f}% {status}")
    
    monthly_df = pd.DataFrame(monthly_results)
    if len(monthly_df) > 0:
        roi_mean = monthly_df['roi'].mean()
        roi_std = monthly_df['roi'].std()
        positive_months = (monthly_df['roi'] >= 100).sum()
        logger.info("-" * 60)
        logger.info(f"平均ROI: {roi_mean:.1f}%, 標準偏差: {roi_std:.1f}%")
        logger.info(f"プラス月: {positive_months}/{len(monthly_df)} ({positive_months/len(monthly_df)*100:.1f}%)")
    
    # =========================================================================
    # 分析3: 複勝戦略の検討
    # =========================================================================
    logger.info("\n\n[3] 複勝戦略（50倍以上のTop1）:")
    logger.info("-" * 60)
    
    # 複勝データがあるか確認
    # 3着以内で的中
    high_odds_df = top1_df[top1_df['win_odds'] >= 50].copy()
    
    top3_hits = (high_odds_df['finish_position'] <= 3).sum()
    total = len(high_odds_df)
    top3_rate = top3_hits / total * 100 if total > 0 else 0
    
    # 複勝オッズは単勝の約30%と仮定
    estimated_fukusho_odds = high_odds_df['win_odds'] * 0.3
    estimated_fukusho_payout = (high_odds_df[high_odds_df['finish_position'] <= 3]['win_odds'] * 0.3 * 100).sum()
    fukusho_roi = estimated_fukusho_payout / (total * 100) * 100 if total > 0 else 0
    
    logger.info(f"対象: {total}件")
    logger.info(f"3着以内率: {top3_rate:.1f}% ({top3_hits}/{total})")
    logger.info(f"推定複勝ROI: {fukusho_roi:.1f}% (※単勝×0.3で概算)")
    
    # =========================================================================
    # 分析4: 最適戦略の探索
    # =========================================================================
    logger.info("\n\n[4] 最適オッズ閾値の探索:")
    logger.info("-" * 60)
    
    best_roi = 0
    best_threshold = 0
    
    for min_odds in range(20, 150, 5):
        subset = top1_df[top1_df['win_odds'] >= min_odds]
        if len(subset) < 20:  # 最低20件必要
            continue
        
        bet = len(subset) * 100
        payout = (subset[subset['finish_position'] == 1]['win_odds'] * 100).sum()
        roi = payout / bet * 100
        
        if roi > best_roi and len(subset) >= 50:
            best_roi = roi
            best_threshold = min_odds
    
    logger.info(f"最適閾値: {best_threshold}倍以上")
    optimal_df = top1_df[top1_df['win_odds'] >= best_threshold]
    hits = (optimal_df['finish_position'] == 1).sum()
    logger.info(f"件数: {len(optimal_df)}, 的中: {hits}, 的中率: {hits/len(optimal_df)*100:.1f}%")
    logger.info(f"ROI: {best_roi:.1f}%")
    
    # =========================================================================
    # 推奨戦略
    # =========================================================================
    logger.info("\n\n" + "=" * 70)
    logger.info("推奨戦略")
    logger.info("=" * 70)
    
    if best_roi >= 100:
        logger.info(f"✓ 単勝戦略: {best_threshold}倍以上のTop1選択でROI {best_roi:.1f}%")
        logger.info(f"  - 月間ベット数: 約{len(optimal_df)/12:.0f}件")
        logger.info(f"  - 的中率: {hits/len(optimal_df)*100:.1f}%")
    else:
        logger.info("⚠ 単純なTop1戦略では100%超は困難")
        logger.info("  → 追加フィルター（特徴量ベース）の検討が必要")


if __name__ == '__main__':
    main()
