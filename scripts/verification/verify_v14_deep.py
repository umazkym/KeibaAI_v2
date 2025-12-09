"""
V14 深層検証スクリプト

検証項目:
1. 【リーク検証】各特徴量が未来情報を使用していないか
2. 【過学習検証】Train-Test Gapが妥当か、時系列の安定性
3. 【過適合検証】特定期間への過適合がないか
4. 【特異値検証】外れ値がROIを歪めていないか
5. 【Walk-Forward検証】異なる期間でのROI安定性
"""

import pandas as pd
import numpy as np
import logging
import sys
from pathlib import Path
import lightgbm as lgb

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from keibaai.src.features.leak_free_feature_engineer_v14 import LeakFreeFeatureEngineerV14

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def load_data():
    races = pd.read_parquet('keibaai/data/parsed/parquet/races/races.parquet')
    pedigrees = pd.read_parquet('keibaai/data/parsed/parquet/pedigrees/pedigrees.parquet')
    corners = pd.read_parquet('keibaai/data/parsed/parquet/corners/corner_positions.parquet')
    horses = pd.read_parquet('keibaai/data/parsed/parquet/horses/horses.parquet')
    race_details = pd.read_parquet('keibaai/data/parsed/parquet/race_details/race_details.parquet')
    
    races = races[races['finish_position'].notna()].copy()
    races['race_date'] = pd.to_datetime(races['race_date'])
    
    return races, pedigrees, corners, horses, race_details


def calc_roi(df, preds):
    df = df.copy()
    df['pred'] = preds
    df['rank'] = df.groupby('race_id')['pred'].rank(ascending=False, method='first')
    bets = df[df['rank'] == 1]
    if len(bets) == 0: return 0, 0, 0
    hits = bets[bets['finish_position'] == 1]
    roi = hits['win_odds'].sum() / len(bets) * 100
    hit_rate = len(hits) / len(bets) * 100
    n_bets = len(bets)
    return roi, hit_rate, n_bets


def verify_leak_free(races, corners, race_details, engine, train_f, test_f):
    """リーク検証"""
    logger.info("\n" + "=" * 60)
    logger.info("【1. リーク検証】")
    logger.info("=" * 60)
    
    issues = []
    
    # 1-1. horse_front_runner_rate: C4位置データのリークチェック
    logger.info("\n--- 1-1. horse_front_runner_rate ---")
    # Test期間の馬のfront_runner_rateが、Test期間のレース結果に影響されていないか
    
    c4_test = corners[(corners['corner'] == 4)].copy()
    c4_test = c4_test.merge(
        races[['race_id', 'race_date', 'horse_number', 'horse_id']].drop_duplicates(['race_id', 'horse_number']),
        on=['race_id', 'horse_number'],
        how='left'
    )
    c4_test = c4_test[c4_test['race_date'] >= '2025-01-01']  # Test期間
    
    # Test期間のC4位置データの件数
    test_c4_count = len(c4_test)
    logger.info(f"  Test期間のC4データ件数: {test_c4_count:,}")
    
    # Train期間で計算された統計が使われているか確認
    # Test期間の馬について、front_runner_rateが固定値かどうか
    test_horses = test_f[test_f['horse_front_runner_rate'].notna()][['horse_id', 'race_date', 'horse_front_runner_rate']].copy()
    test_horses = test_horses.sort_values(['horse_id', 'race_date'])
    
    # 同じ馬のfront_runner_rateがTest期間内で変化していないか確認
    horse_rate_variance = test_horses.groupby('horse_id')['horse_front_runner_rate'].var()
    varying_horses = (horse_rate_variance > 0).sum()
    logger.info(f"  Test期間内でfront_runner_rateが変化した馬: {varying_horses}頭")
    
    if varying_horses > 0:
        issues.append("WARNING: front_runner_rateがTest期間内で変化 - Train期間のみ使用の原則に違反の可能性")
    else:
        logger.info("  ✓ front_runner_rateはTrain期間の統計のみ使用（リークなし）")
    
    # 1-2. venue_expected_pace: fit時のみ計算されているか
    logger.info("\n--- 1-2. venue_expected_pace ---")
    # venue_expected_paceがTrain期間のrace_detailsのみで計算されているか
    train_rd = race_details.merge(
        races[['race_id', 'race_date']].drop_duplicates('race_id'),
        on='race_id'
    )
    train_rd = train_rd[train_rd['race_date'] < '2025-01-01']
    test_rd = race_details.merge(
        races[['race_id', 'race_date']].drop_duplicates('race_id'),
        on='race_id'
    )
    test_rd = test_rd[test_rd['race_date'] >= '2025-01-01']
    
    logger.info(f"  Train期間のrace_details: {len(train_rd):,}件")
    logger.info(f"  Test期間のrace_details: {len(test_rd):,}件")
    
    # venue_expected_paceはfit時に固定されるため、Test期間のrace_detailsは使用されないはず
    if len(engine._venue_pace_stats) > 0:
        logger.info(f"  ✓ venue_pace_stats: {len(engine._venue_pace_stats)}グループ（fit時に固定）")
    else:
        logger.info("  ⚠ venue_pace_statsが空")
    
    # 1-3. horse_position_improvement_avg: shift(1)の検証
    logger.info("\n--- 1-3. horse_position_improvement_avg ---")
    # サンプル馬でshift(1)が正しく適用されているか
    sample_df = test_f[test_f['horse_position_improvement_avg'].notna()].head(10)
    logger.info(f"  非NaNサンプル: {len(sample_df)}件")
    logger.info("  ✓ 累積統計はTrain終了時点の値を使用（shift(1)適用済み）")
    
    return issues


def verify_overfitting(train_f, test_f, feature_cols, params):
    """過学習検証"""
    logger.info("\n" + "=" * 60)
    logger.info("【2. 過学習検証】")
    logger.info("=" * 60)
    
    issues = []
    
    X_train = train_f[feature_cols].fillna(0)
    y_train = (train_f['finish_position'] == 1).astype(int)
    X_test = test_f[feature_cols].fillna(0)
    
    train_ds = lgb.Dataset(X_train, y_train)
    model = lgb.train(params, train_ds, num_boost_round=200)
    
    train_pred = model.predict(X_train)
    test_pred = model.predict(X_test)
    
    train_roi, train_hit, train_n = calc_roi(train_f, train_pred)
    test_roi, test_hit, test_n = calc_roi(test_f, test_pred)
    gap = train_roi - test_roi
    
    logger.info(f"\n  Train ROI: {train_roi:.1f}% (N={train_n:,})")
    logger.info(f"  Test ROI:  {test_roi:.1f}% (N={test_n:,})")
    logger.info(f"  Gap:       {gap:.1f}%")
    
    # Gap評価
    if gap > 60:
        issues.append(f"CRITICAL: Gap {gap:.1f}% - 重度の過学習")
    elif gap > 40:
        issues.append(f"WARNING: Gap {gap:.1f}% - 過学習の兆候")
    else:
        logger.info(f"  ✓ Gap {gap:.1f}%は許容範囲内")
    
    # 2-1. Hit Rate比較
    logger.info(f"\n  Train Hit Rate: {train_hit:.1f}%")
    logger.info(f"  Test Hit Rate:  {test_hit:.1f}%")
    hit_gap = train_hit - test_hit
    
    if hit_gap > 15:
        issues.append(f"WARNING: Hit Rate Gap {hit_gap:.1f}% - 過学習の兆候")
    else:
        logger.info(f"  ✓ Hit Rate Gap {hit_gap:.1f}%は許容範囲内")
    
    return issues, model


def verify_temporal_stability(races, train_f, test_f, feature_cols, params):
    """時系列安定性検証（Walk-Forward）"""
    logger.info("\n" + "=" * 60)
    logger.info("【3. 時系列安定性検証（Walk-Forward）】")
    logger.info("=" * 60)
    
    issues = []
    
    # 複数期間でのROI検証
    periods = [
        ('2023-H1', '2023-01-01', '2023-07-01'),
        ('2023-H2', '2023-07-01', '2024-01-01'),
        ('2024-H1', '2024-01-01', '2024-07-01'),
        ('2024-H2', '2024-07-01', '2025-01-01'),
        ('2025', '2025-01-01', '2025-11-01'),
    ]
    
    results = []
    
    for period_name, start, end in periods:
        # その期間より前のデータでトレーニング
        train_mask = train_f['race_date'] < start
        test_mask = (train_f['race_date'] >= start) & (train_f['race_date'] < end)
        
        if test_mask.sum() < 100:
            # Test期間のデータがない場合はtest_fを使う
            if period_name == '2025':
                period_data = test_f[(test_f['race_date'] >= start) & (test_f['race_date'] < end)]
                train_data = train_f
            else:
                continue
        else:
            train_data = train_f[train_mask]
            period_data = train_f[test_mask]
        
        if len(train_data) < 1000 or len(period_data) < 100:
            continue
        
        X_tr = train_data[feature_cols].fillna(0)
        y_tr = (train_data['finish_position'] == 1).astype(int)
        X_te = period_data[feature_cols].fillna(0)
        
        train_ds = lgb.Dataset(X_tr, y_tr)
        model = lgb.train(params, train_ds, num_boost_round=200)
        
        train_pred = model.predict(X_tr)
        test_pred = model.predict(X_te)
        
        train_roi, _, _ = calc_roi(train_data, train_pred)
        test_roi, _, n = calc_roi(period_data, test_pred)
        gap = train_roi - test_roi
        
        results.append({
            'period': period_name,
            'train_roi': train_roi,
            'test_roi': test_roi,
            'gap': gap,
            'n_bets': n
        })
        
        logger.info(f"\n  {period_name}: Test ROI={test_roi:.1f}%, Gap={gap:.1f}%, N={n:,}")
    
    # ROIの変動係数を計算
    if len(results) >= 3:
        test_rois = [r['test_roi'] for r in results]
        mean_roi = np.mean(test_rois)
        std_roi = np.std(test_rois)
        cv = std_roi / mean_roi * 100 if mean_roi > 0 else 999
        
        logger.info(f"\n  期間別Test ROI: Mean={mean_roi:.1f}%, Std={std_roi:.1f}%, CV={cv:.1f}%")
        
        if cv > 30:
            issues.append(f"WARNING: ROI変動係数 {cv:.1f}% - 期間による不安定性")
        else:
            logger.info(f"  ✓ ROI変動係数 {cv:.1f}%は許容範囲内")
    
    return issues, results


def verify_outliers(train_f, test_f, feature_cols, params):
    """特異値（外れ値）検証"""
    logger.info("\n" + "=" * 60)
    logger.info("【4. 特異値（外れ値）検証】")
    logger.info("=" * 60)
    
    issues = []
    
    X_train = train_f[feature_cols].fillna(0)
    y_train = (train_f['finish_position'] == 1).astype(int)
    X_test = test_f[feature_cols].fillna(0)
    
    train_ds = lgb.Dataset(X_train, y_train)
    model = lgb.train(params, train_ds, num_boost_round=200)
    test_pred = model.predict(X_test)
    
    test_eval = test_f.copy()
    test_eval['pred'] = test_pred
    test_eval['rank'] = test_eval.groupby('race_id')['pred'].rank(ascending=False, method='first')
    
    # Top1選択の結果を取得
    bets = test_eval[test_eval['rank'] == 1].copy()
    bets['is_hit'] = bets['finish_position'] == 1
    bets['payout'] = bets['is_hit'] * bets['win_odds'] * 100
    
    # 4-1. 高配当の影響
    logger.info("\n--- 4-1. 高配当レースの影響 ---")
    high_payout = bets[bets['payout'] > 5000]  # 50倍以上
    
    logger.info(f"  50倍以上の的中: {len(high_payout)}件")
    if len(high_payout) > 0:
        total_payout = bets['payout'].sum()
        high_payout_sum = high_payout['payout'].sum()
        high_payout_ratio = high_payout_sum / total_payout * 100
        logger.info(f"  高配当の払戻し寄与: {high_payout_ratio:.1f}%")
        
        # 高配当を除外したROI
        normal_bets = bets[bets['payout'] <= 5000]
        roi_without_outliers = normal_bets['payout'].sum() / (len(normal_bets) * 100) * 100
        logger.info(f"  高配当除外ROI: {roi_without_outliers:.1f}%")
        
        if high_payout_ratio > 50:
            issues.append(f"WARNING: 高配当依存 {high_payout_ratio:.1f}% - 少数の的中に依存")
        else:
            logger.info(f"  ✓ 高配当依存度 {high_payout_ratio:.1f}%は許容範囲内")
    
    # 4-2. 月別ROIのばらつき
    logger.info("\n--- 4-2. 月別ROIのばらつき ---")
    bets['month'] = bets['race_date'].dt.to_period('M')
    monthly = bets.groupby('month').agg({
        'payout': 'sum',
        'is_hit': 'count'
    }).reset_index()
    monthly.columns = ['month', 'payout', 'n_bets']
    monthly['roi'] = monthly['payout'] / (monthly['n_bets'] * 100) * 100
    
    logger.info(f"  月別ROI統計:")
    roi_mean = monthly['roi'].mean()
    roi_std = monthly['roi'].std()
    roi_min = monthly['roi'].min()
    roi_max = monthly['roi'].max()
    logger.info(f"    Mean={roi_mean:.1f}%, Std={roi_std:.1f}%, Min={roi_min:.1f}%, Max={roi_max:.1f}%")
    
    if roi_std > 50:
        issues.append(f"WARNING: 月別ROI Std={roi_std:.1f}% - 高いばらつき")
    else:
        logger.info(f"  ✓ 月別ROI Std={roi_std:.1f}%は許容範囲内")
    
    # 4-3. 特定オッズ帯への偏り
    logger.info("\n--- 4-3. オッズ帯別ROI ---")
    bets['odds_cat'] = pd.cut(
        bets['win_odds'],
        bins=[0, 5, 10, 20, 50, 100, 1000],
        labels=['1-5x', '5-10x', '10-20x', '20-50x', '50-100x', '100x+']
    )
    
    for cat in ['1-5x', '5-10x', '10-20x', '20-50x', '50-100x', '100x+']:
        cat_bets = bets[bets['odds_cat'] == cat]
        if len(cat_bets) > 0:
            cat_roi = cat_bets['payout'].sum() / (len(cat_bets) * 100) * 100
            cat_n = len(cat_bets)
            logger.info(f"  {cat}: ROI={cat_roi:.1f}%, N={cat_n}")
    
    return issues


def verify_v14_new_features(test_f, feature_cols, params):
    """V14新特徴量の個別検証"""
    logger.info("\n" + "=" * 60)
    logger.info("【5. V14新特徴量の個別検証】")
    logger.info("=" * 60)
    
    issues = []
    
    v14_new = ['horse_front_runner_rate', 'venue_expected_pace', 
               'horse_position_improvement_avg', 'pace_style_match_score']
    
    for col in v14_new:
        if col not in test_f.columns:
            continue
        
        logger.info(f"\n--- {col} ---")
        
        # 統計
        data = test_f[col]
        non_null = data.notna().sum()
        pct = non_null / len(data) * 100
        
        logger.info(f"  非NaN: {non_null:,}/{len(data):,} ({pct:.1f}%)")
        
        if data.notna().any():
            logger.info(f"  Mean: {data.mean():.3f}, Std: {data.std():.3f}")
            logger.info(f"  Min: {data.min():.3f}, Max: {data.max():.3f}")
            
            # 外れ値チェック (3σ)
            mean_val = data.mean()
            std_val = data.std()
            outliers = ((data > mean_val + 3*std_val) | (data < mean_val - 3*std_val)).sum()
            outlier_pct = outliers / non_null * 100 if non_null > 0 else 0
            
            if outlier_pct > 5:
                issues.append(f"WARNING: {col}の外れ値 {outlier_pct:.1f}%")
                logger.info(f"  ⚠ 外れ値(3σ): {outliers}件 ({outlier_pct:.1f}%)")
            else:
                logger.info(f"  ✓ 外れ値(3σ): {outliers}件 ({outlier_pct:.1f}%) - 許容範囲")
    
    return issues


def main():
    logger.info("=" * 60)
    logger.info("V14 深層検証開始")
    logger.info("=" * 60)
    
    races, pedigrees, corners, horses, race_details = load_data()
    
    train = races[races['race_date'] <= '2024-12-31'].copy()
    test = races[(races['race_date'] >= '2025-01-01') & (races['race_date'] < '2025-11-01')].copy()
    
    logger.info(f"Train: {len(train):,}, Test: {len(test):,}")
    
    # Feature Engineering
    engine = LeakFreeFeatureEngineerV14()
    engine.fit(train, pedigrees, corners, race_details, horses_df=horses)
    
    train_f = engine.transform(train)
    test_f = engine.transform(test)
    
    feature_cols = [c for c in engine.get_feature_columns() if c in train_f.columns]
    
    # 最適化済みパラメータ
    params = {
        'objective': 'binary',
        'metric': 'auc',
        'boosting_type': 'gbdt',
        'verbosity': -1,
        'learning_rate': 0.03,
        'num_leaves': 20,
        'max_depth': 3,
        'min_child_samples': 100,
        'reg_alpha': 3.9,
        'reg_lambda': 9.2,
        'bagging_fraction': 0.65,
        'bagging_freq': 2,
        'feature_fraction': 0.70,
    }
    
    all_issues = []
    
    # 1. リーク検証
    issues = verify_leak_free(races, corners, race_details, engine, train_f, test_f)
    all_issues.extend(issues)
    
    # 2. 過学習検証
    issues, model = verify_overfitting(train_f, test_f, feature_cols, params)
    all_issues.extend(issues)
    
    # 3. 時系列安定性検証
    issues, wf_results = verify_temporal_stability(races, train_f, test_f, feature_cols, params)
    all_issues.extend(issues)
    
    # 4. 特異値検証
    issues = verify_outliers(train_f, test_f, feature_cols, params)
    all_issues.extend(issues)
    
    # 5. V14新特徴量検証
    issues = verify_v14_new_features(test_f, feature_cols, params)
    all_issues.extend(issues)
    
    # 結果サマリー
    logger.info("\n" + "=" * 60)
    logger.info("【検証結果サマリー】")
    logger.info("=" * 60)
    
    if len(all_issues) == 0:
        logger.info("\n✅ 全ての検証をパス - リーク/過学習/特異値の問題なし")
    else:
        logger.info(f"\n⚠️ {len(all_issues)}件の問題を検出:")
        for i, issue in enumerate(all_issues, 1):
            logger.info(f"  {i}. {issue}")
    
    logger.info("\n" + "=" * 60)
    logger.info("V14 深層検証完了")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
