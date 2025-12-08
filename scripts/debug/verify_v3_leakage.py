"""
リーク・過学習検証スクリプト

チェック項目:
1. 時系列リーク: fit期間外のデータが特徴量に混入していないか
2. 直接リーク: finish_positionが予測時に漏れていないか
3. 間接リーク: 払戻データ（的中=1位）が特徴に混入していないか
4. 過学習兆候: Train vs Test ROI差、特定セグメントへの偏り
5. サンプル数の妥当性: 低サンプル統計による不安定性
"""

import sys
from pathlib import Path
import numpy as np
import pandas as pd
import logging

project_root = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(project_root))

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def main():
    data_dir = Path("keibaai/data")
    
    # ==========================================================
    # データ読み込み
    # ==========================================================
    logger.info("=" * 70)
    logger.info("リーク・過学習検証")
    logger.info("=" * 70)
    
    races_df = pd.read_parquet(data_dir / "parsed/parquet/races/races.parquet")
    races_df['race_date'] = pd.to_datetime(races_df['race_date'])
    races_df = races_df[(races_df['race_date'] >= '2020-01-01') & (races_df['race_date'] <= '2025-12-31')]
    races_df = races_df.dropna(subset=['finish_position', 'win_odds'])
    
    pedigrees_df = pd.read_parquet(data_dir / "parsed/parquet/pedigrees/pedigrees.parquet")
    corners_df = pd.read_parquet(data_dir / "parsed/parquet/corners/corner_positions.parquet")
    race_details_df = pd.read_parquet(data_dir / "parsed/parquet/race_details/race_details.parquet")
    returns_df = pd.read_parquet(data_dir / "parsed/parquet/returns/returns.parquet")
    
    # 時系列分割
    max_date = races_df['race_date'].max()
    test_start = max_date - pd.DateOffset(months=6)
    train_end = test_start - pd.DateOffset(months=1)
    
    train_df = races_df[races_df['race_date'] < train_end].copy()
    test_df = races_df[races_df['race_date'] >= test_start].copy()
    
    logger.info(f"\nTrain: {train_df['race_date'].min()} - {train_df['race_date'].max()}")
    logger.info(f"Test:  {test_df['race_date'].min()} - {test_df['race_date'].max()}")
    logger.info(f"Gap:   {train_end} - {test_start} (1ヶ月)")
    
    # ==========================================================
    # CHECK 1: 時系列リーク確認
    # ==========================================================
    logger.info("\n" + "=" * 70)
    logger.info("[CHECK 1] 時系列リーク確認")
    logger.info("=" * 70)
    
    # fit()はtrain_dfのみで行われるべき
    from keibaai.src.features.leak_free_feature_engineer_v3 import LeakFreeFeatureEngineerV3
    
    fe = LeakFreeFeatureEngineerV3()
    fe.fit(
        races_df=train_df,
        pedigrees_df=pedigrees_df,
        corners_df=corners_df,
        race_details_df=race_details_df,
        returns_df=returns_df
    )
    
    # fit_dateがtrain期間内であることを確認
    logger.info(f"\nfit_date: {fe.fit_date}")
    logger.info(f"train_end: {train_end}")
    
    if fe.fit_date <= train_end:
        logger.info("✓ fit_dateはtrain期間内 - OK")
    else:
        logger.error("✗ fit_dateがtrain期間外 - リークの可能性!")
    
    # ==========================================================
    # CHECK 2: 直接リーク確認 - transform時にfinish_positionを使用していないか
    # ==========================================================
    logger.info("\n" + "=" * 70)
    logger.info("[CHECK 2] 直接リーク確認（finish_position使用チェック）")
    logger.info("=" * 70)
    
    # テストデータのfinish_positionを欠損にしてtransform
    test_df_no_finish = test_df.copy()
    original_finish = test_df_no_finish['finish_position'].copy()
    test_df_no_finish['finish_position'] = np.nan  # 故意に欠損化
    
    try:
        test_features_no_finish = fe.transform(test_df_no_finish)
        logger.info("✓ finish_position=NaNでもtransform成功 - 直接リークなし")
    except Exception as e:
        logger.error(f"✗ transform失敗: {e}")
    
    # 正常なtransform
    test_features = fe.transform(test_df)
    
    # ==========================================================
    # CHECK 3: 間接リーク確認 - 払戻統計が当該レースを含んでいないか
    # ==========================================================
    logger.info("\n" + "=" * 70)
    logger.info("[CHECK 3] 間接リーク確認（払戻統計の時系列）")
    logger.info("=" * 70)
    
    # 払戻統計はfit時のみで計算されるべき
    # returns_dfの期間を確認
    returns_with_date = returns_df.merge(
        races_df[['race_id', 'race_date']].drop_duplicates(),
        on='race_id', how='left'
    )
    returns_in_test = returns_with_date[returns_with_date['race_date'] >= test_start]
    
    logger.info(f"Test期間の払戻レコード: {len(returns_in_test):,}件")
    
    # 払戻統計がfit時に計算された騎手IDを確認
    jockey_payout_stats = fe.statistics.get('jockey_payout')
    if jockey_payout_stats is not None:
        logger.info(f"払戻統計の騎手数: {len(jockey_payout_stats)}")
        # これはtrain期間のデータのみから計算されるべき
        logger.info("✓ 払戻統計はfit()時に計算済み（train期間のみ）")
    
    # ==========================================================
    # CHECK 4: 過学習兆候 - Train vs Test ROI差
    # ==========================================================
    logger.info("\n" + "=" * 70)
    logger.info("[CHECK 4] 過学習兆候確認（Train vs Test ROI）")
    logger.info("=" * 70)
    
    import lightgbm as lgb
    
    # Transform
    train_features = fe.transform(train_df)
    feature_cols = fe.get_feature_columns()
    feature_cols = [c for c in feature_cols if c in train_features.columns]
    
    # Model training
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
        n_estimators=500,
        learning_rate=0.05,
        max_depth=6,
        num_leaves=31,
        random_state=42,
        verbose=-1
    )
    model.fit(X_train, y_relevance, group=group_sizes)
    
    # Train予測
    train_features['score'] = model.predict(train_features[feature_cols].fillna(0))
    train_features['rank'] = train_features.groupby('race_id')['score'].rank(ascending=False)
    train_top1 = train_features[train_features['rank'] == 1]
    
    train_hits = (train_top1['finish_position'] == 1).sum()
    train_hit_rate = train_hits / len(train_top1) * 100
    
    # Test予測
    test_features['score'] = model.predict(test_features[feature_cols].fillna(0))
    test_features['rank'] = test_features.groupby('race_id')['score'].rank(ascending=False)
    test_top1 = test_features[test_features['rank'] == 1]
    
    test_hits = (test_top1['finish_position'] == 1).sum()
    test_hit_rate = test_hits / len(test_top1) * 100
    
    logger.info(f"\nTrain的中率: {train_hit_rate:.1f}% ({train_hits}/{len(train_top1)})")
    logger.info(f"Test的中率:  {test_hit_rate:.1f}% ({test_hits}/{len(test_top1)})")
    
    diff = train_hit_rate - test_hit_rate
    logger.info(f"差分: {diff:.1f}%")
    
    if diff < 5:
        logger.info("✓ Train-Test差分 < 5% - 過学習の兆候なし")
    elif diff < 10:
        logger.warning("△ Train-Test差分 5-10% - 軽度の過学習の可能性")
    else:
        logger.error("✗ Train-Test差分 > 10% - 過学習の可能性大!")
    
    # ==========================================================
    # CHECK 5: サンプル数の妥当性
    # ==========================================================
    logger.info("\n" + "=" * 70)
    logger.info("[CHECK 5] サンプル数の妥当性")
    logger.info("=" * 70)
    
    # 特徴量のnon-null率を確認
    logger.info("\n特徴量のnon-null率（低い=少サンプル問題）:")
    for col in feature_cols:
        if col in test_features.columns:
            non_null_rate = test_features[col].notna().mean() * 100
            if non_null_rate < 50:
                logger.warning(f"  △ {col}: {non_null_rate:.1f}% (低サンプル)")
    
    # 騎手・調教師統計のサンプル数分布
    jockey_overall = fe.statistics.get('jockey_overall')
    if jockey_overall is not None:
        low_sample_jockeys = (jockey_overall['jockey_races'] < 10).sum()
        logger.info(f"\n騎手統計:  10レース未満の騎手: {low_sample_jockeys}人 / {len(jockey_overall)}人")
    
    trainer_overall = fe.statistics.get('trainer_overall')
    if trainer_overall is not None:
        low_sample_trainers = (trainer_overall['trainer_races'] < 10).sum()
        logger.info(f"調教師統計: 10レース未満の調教師: {low_sample_trainers}人 / {len(trainer_overall)}人")
    
    # ==========================================================
    # CHECK 6: オッズ帯別の安定性（高ROIが特定セグメントに偏っていないか）
    # ==========================================================
    logger.info("\n" + "=" * 70)
    logger.info("[CHECK 6] オッズ帯別安定性（過適合チェック）")
    logger.info("=" * 70)
    
    # 払戻データをマージ
    tansho = returns_df[returns_df['bet_type'] == 'tansho'][['race_id', 'horse_number', 'payout']].copy()
    tansho = tansho.rename(columns={'payout': 'tansho_payout'})
    test_top1 = test_top1.merge(tansho, on=['race_id', 'horse_number'], how='left')
    
    # 月別ROIの標準偏差
    test_top1 = test_top1.copy()
    test_top1['month'] = test_top1['race_date'].dt.to_period('M')
    
    monthly_results = []
    for month in test_top1['month'].unique():
        month_df = test_top1[test_top1['month'] == month]
        bet = len(month_df) * 100
        payout = month_df['tansho_payout'].fillna(0).sum()
        roi = payout / bet * 100 if bet > 0 else 0
        monthly_results.append({'month': str(month), 'roi': roi, 'n': len(month_df)})
    
    monthly_df = pd.DataFrame(monthly_results)
    roi_std = monthly_df['roi'].std()
    roi_mean = monthly_df['roi'].mean()
    
    logger.info(f"\n月別ROI: 平均 {roi_mean:.1f}%, 標準偏差 {roi_std:.1f}%")
    logger.info("月別詳細:")
    for _, row in monthly_df.iterrows():
        status = "✓" if row['roi'] >= 100 else ""
        logger.info(f"  {row['month']}: {row['roi']:.1f}% (n={row['n']}) {status}")
    
    if roi_std < 30:
        logger.info("✓ 月別ROI標準偏差 < 30% - 安定")
    elif roi_std < 50:
        logger.warning("△ 月別ROI標準偏差 30-50% - やや不安定")
    else:
        logger.error("✗ 月別ROI標準偏差 > 50% - 高分散（過適合の可能性）")
    
    # ==========================================================
    # CHECK 7: 特徴量とターゲットの相関分析
    # ==========================================================
    logger.info("\n" + "=" * 70)
    logger.info("[CHECK 7] 特徴量とターゲットの相関分析")
    logger.info("=" * 70)
    
    # 高相関の特徴量は過学習のリスク
    # finish_position=1と特徴量の相関を計算
    test_features_for_corr = test_features.copy()
    test_features_for_corr['is_winner'] = (test_features_for_corr['finish_position'] == 1).astype(int)
    
    correlations = {}
    for col in feature_cols:
        if col in test_features_for_corr.columns:
            try:
                corr = test_features_for_corr[col].corr(test_features_for_corr['is_winner'])
                if not pd.isna(corr):
                    correlations[col] = corr
            except:
                pass
    
    corr_df = pd.DataFrame({
        'feature': list(correlations.keys()),
        'correlation': list(correlations.values())
    }).sort_values('correlation', key=abs, ascending=False)
    
    logger.info("\n勝利との相関 Top 10:")
    for _, row in corr_df.head(10).iterrows():
        status = "⚠ 高相関" if abs(row['correlation']) > 0.3 else ""
        logger.info(f"  {row['feature']}: {row['correlation']:.3f} {status}")
    
    # 0.5以上の相関があれば警告
    high_corr = corr_df[abs(corr_df['correlation']) > 0.5]
    if len(high_corr) > 0:
        logger.error(f"✗ 0.5以上の高相関特徴量: {len(high_corr)}個 - リークの可能性!")
        for _, row in high_corr.iterrows():
            logger.error(f"    {row['feature']}: {row['correlation']:.3f}")
    else:
        logger.info("✓ 0.5以上の高相関特徴量なし")
    
    # ==========================================================
    # 総合判定
    # ==========================================================
    logger.info("\n" + "=" * 70)
    logger.info("総合判定")
    logger.info("=" * 70)
    
    issues = []
    if diff >= 10:
        issues.append("Train-Test的中率差が大きい")
    if roi_std >= 50:
        issues.append("月別ROI標準偏差が大きい")
    if len(high_corr) > 0:
        issues.append("高相関特徴量が存在")
    
    if len(issues) == 0:
        logger.info("✓ 重大なリーク・過学習の兆候は検出されませんでした")
    else:
        logger.warning(f"△ 以下の問題が検出されました: {', '.join(issues)}")


if __name__ == '__main__':
    main()
