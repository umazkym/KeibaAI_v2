#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Ensemble結果の深層検証

【検証項目】
1. リークの可能性
   - V15 Fixed: 特徴量にwin_oddsが含まれていないか？
   - V4.4: ターゲットにオッズを使用している影響

2. 過学習の可能性
   - Train/Valid/Test ROI比較
   - 特徴量重要度の確認（過度に特定特徴量に依存していないか）

3. 過適合（アンサンブル比率50:50の妥当性）
   - 50:50は恣意的ではないか？
   - 他の比率でのROI確認

4. ラッキーの可能性
   - 高配当依存度
   - ブートストラップ信頼区間
   - 月別の安定性
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


def calc_roi_detailed(df, preds):
    """詳細なROI統計を計算"""
    d = df.copy()
    d['score'] = preds
    d['rank_pred'] = d.groupby('race_id')['score'].rank(ascending=False, method='first')
    bet_df = d[d['rank_pred'] == 1]
    hits = bet_df[bet_df['finish_position'] == 1]
    
    if len(bet_df) == 0:
        return {}
    
    return {
        'n_bets': len(bet_df),
        'n_hits': len(hits),
        'hit_rate': len(hits) / len(bet_df) * 100,
        'total_return': hits['win_odds'].sum(),
        'roi': hits['win_odds'].sum() / len(bet_df) * 100,
        'avg_hit_odds': hits['win_odds'].mean() if len(hits) > 0 else 0,
        'max_hit_odds': hits['win_odds'].max() if len(hits) > 0 else 0,
        'hit_odds_list': hits['win_odds'].tolist(),
    }


def main():
    logger.info("=" * 70)
    logger.info("Ensemble結果の深層検証")
    logger.info("=" * 70)
    
    from keibaai.src.features.leak_free_feature_engineer_v15_fixed import LeakFreeFeatureEngineerV15Fixed
    
    races, pedigrees, corners, race_details, horses = load_data()
    
    train = races[races['race_date'] < '2023-01-01'].copy()
    valid = races[(races['race_date'] >= '2023-01-01') & (races['race_date'] < '2024-01-01')].copy()
    test = races[(races['race_date'] >= '2024-01-01') & (races['race_date'] < '2025-01-01')].copy()
    
    logger.info(f"Train: {len(train):,}")
    logger.info(f"Valid: {len(valid):,}")
    logger.info(f"Test:  {len(test):,}")
    
    # 特徴量生成
    engine = LeakFreeFeatureEngineerV15Fixed()
    engine.fit(train, pedigrees, corners, race_details, horses_df=horses)
    
    train_f = engine.transform(train)
    valid_f = engine.transform(valid)
    test_f = engine.transform(test)
    
    feature_cols = [c for c in engine.get_feature_columns() if c in train_f.columns]
    
    # ===== 検証1: リークの確認 =====
    logger.info("")
    logger.info("=" * 70)
    logger.info("検証1: リークの確認")
    logger.info("=" * 70)
    
    # 特徴量にオッズ関連が含まれていないか
    odds_features = [c for c in feature_cols if 'odds' in c.lower()]
    logger.info(f"  オッズ関連特徴量: {odds_features}")
    if not odds_features:
        logger.info("  ✅ 特徴量にオッズ関連は含まれていない")
    
    # future関連の特徴量
    future_features = [c for c in feature_cols if 'future' in c.lower() or 'next' in c.lower()]
    logger.info(f"  未来関連特徴量: {future_features}")
    if not future_features:
        logger.info("  ✅ 特徴量に未来関連は含まれていない")
    
    # ===== 検証2: Train/Valid/Test ROI比較 =====
    logger.info("")
    logger.info("=" * 70)
    logger.info("検証2: Train/Valid/Test ROI比較（過学習チェック）")
    logger.info("=" * 70)
    
    # V15 Fixed (Binary)
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
    
    pred_v15_train = model_v15.predict(train_f[feature_cols].fillna(0))
    pred_v15_valid = model_v15.predict(valid_f[feature_cols].fillna(0))
    pred_v15_test = model_v15.predict(test_f[feature_cols].fillna(0))
    
    train_stats = calc_roi_detailed(train_f, pred_v15_train)
    valid_stats = calc_roi_detailed(valid_f, pred_v15_valid)
    test_stats = calc_roi_detailed(test_f, pred_v15_test)
    
    logger.info("  V15 Fixed ROI:")
    logger.info(f"    Train: {train_stats['roi']:.1f}%")
    logger.info(f"    Valid: {valid_stats['roi']:.1f}%")
    logger.info(f"    Test:  {test_stats['roi']:.1f}%")
    
    gap_train_valid = train_stats['roi'] - valid_stats['roi']
    gap_train_test = train_stats['roi'] - test_stats['roi']
    
    logger.info(f"    Train-Valid Gap: {gap_train_valid:.1f}%")
    logger.info(f"    Train-Test Gap:  {gap_train_test:.1f}%")
    
    if gap_train_test < 35:
        logger.info("  ✅ 過学習は見られない (Gap < 35%)")
    else:
        logger.info("  ⚠️ 過学習の可能性あり (Gap >= 35%)")
    
    # V4.4 (LambdaRank)
    v44_params = {
        'objective': 'lambdarank', 'boosting_type': 'gbdt',
        'num_leaves': 25, 'max_depth': 4, 'min_child_samples': 150,
        'learning_rate': 0.05, 'reg_alpha': 5.0, 'reg_lambda': 8.0,
        'feature_fraction': 0.6, 'bagging_fraction': 0.7, 'bagging_freq': 5,
        'verbose': -1, 'random_state': 42, 'label_gain': list(range(100))
    }
    
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
    
    model_v44 = lgb.LGBMRanker(**v44_params, n_estimators=200)
    model_v44.fit(train_df[feature_cols].fillna(0), train_df['target'], group=groups)
    
    pred_v44_train = model_v44.predict(train_f[feature_cols].fillna(0))
    pred_v44_valid = model_v44.predict(valid_f[feature_cols].fillna(0))
    pred_v44_test = model_v44.predict(test_f[feature_cols].fillna(0))
    
    train_stats_v44 = calc_roi_detailed(train_f, pred_v44_train)
    valid_stats_v44 = calc_roi_detailed(valid_f, pred_v44_valid)
    test_stats_v44 = calc_roi_detailed(test_f, pred_v44_test)
    
    logger.info("")
    logger.info("  V4.4 ROI:")
    logger.info(f"    Train: {train_stats_v44['roi']:.1f}%")
    logger.info(f"    Valid: {valid_stats_v44['roi']:.1f}%")
    logger.info(f"    Test:  {test_stats_v44['roi']:.1f}%")
    
    gap_v44 = train_stats_v44['roi'] - test_stats_v44['roi']
    logger.info(f"    Train-Test Gap: {gap_v44:.1f}%")
    
    # ===== 検証3: アンサンブル比率の妥当性 =====
    logger.info("")
    logger.info("=" * 70)
    logger.info("検証3: アンサンブル比率の妥当性")
    logger.info("=" * 70)
    
    def normalize(x):
        return (x - x.min()) / (x.max() - x.min() + 1e-8)
    
    pred_v15_norm_test = normalize(pred_v15_test)
    pred_v44_norm_test = normalize(pred_v44_test)
    
    ratios = [0.3, 0.4, 0.5, 0.6, 0.7]
    logger.info("  異なる比率でのTest ROI:")
    for r in ratios:
        ens_pred = pred_v15_norm_test * r + pred_v44_norm_test * (1 - r)
        ens_stats = calc_roi_detailed(test_f, ens_pred)
        logger.info(f"    V15:{r:.0%} V4.4:{1-r:.0%} → ROI={ens_stats['roi']:.1f}%")
    
    logger.info("")
    logger.info("  50:50が最適かどうか確認...")
    
    # Validationでの最適比率
    pred_v15_norm_valid = normalize(pred_v15_valid)
    pred_v44_norm_valid = normalize(pred_v44_valid)
    
    best_ratio_valid = 0.5
    best_roi_valid = 0
    for r in np.arange(0.3, 0.8, 0.05):
        ens_pred = pred_v15_norm_valid * r + pred_v44_norm_valid * (1 - r)
        ens_stats = calc_roi_detailed(valid_f, ens_pred)
        if ens_stats['roi'] > best_roi_valid:
            best_roi_valid = ens_stats['roi']
            best_ratio_valid = r
    
    logger.info(f"  Validationで最適な比率: V15:{best_ratio_valid:.0%} V4.4:{1-best_ratio_valid:.0%}")
    
    # 50:50と最適比率の差
    if abs(best_ratio_valid - 0.5) < 0.1:
        logger.info("  ✅ 50:50は安全な選択（最適比率との差 < 10%）")
    else:
        logger.info(f"  ⚠️ 50:50は最適ではない可能性（最適は{best_ratio_valid:.0%}）")
    
    # ===== 検証4: ラッキーの可能性 =====
    logger.info("")
    logger.info("=" * 70)
    logger.info("検証4: ラッキーの可能性")
    logger.info("=" * 70)
    
    # 50:50アンサンブルのTest予測
    ens_pred_test = pred_v15_norm_test * 0.5 + pred_v44_norm_test * 0.5
    ens_stats = calc_roi_detailed(test_f, ens_pred_test)
    
    logger.info(f"  Ensemble(50:50) Test ROI: {ens_stats['roi']:.1f}%")
    
    # 高配当依存
    if ens_stats['hit_odds_list']:
        odds_list = sorted(ens_stats['hit_odds_list'], reverse=True)
        top10_sum = sum(odds_list[:10])
        remaining_sum = sum(odds_list[10:]) if len(odds_list) > 10 else 0
        
        logger.info("")
        logger.info("  高配当依存分析:")
        logger.info(f"    Top10的中オッズ合計: {top10_sum:.1f}倍")
        logger.info(f"    残り的中合計: {remaining_sum:.1f}倍")
        if len(ens_stats['hit_odds_list']) > 10:
            roi_without_top10 = remaining_sum / (ens_stats['n_bets'] - 10) * 100
            logger.info(f"    Top10を除いたROI: {roi_without_top10:.1f}%")
            
            if roi_without_top10 > 75:
                logger.info("  ✅ Top10を除いても75%以上のROI（高配当依存は限定的）")
            else:
                logger.info("  ⚠️ 高配当依存度が高い")
    
    # ブートストラップ信頼区間
    logger.info("")
    logger.info("  ブートストラップ信頼区間:")
    
    test_bets = test_f.copy()
    test_bets['ens_pred'] = ens_pred_test
    test_bets['rank_pred'] = test_bets.groupby('race_id')['ens_pred'].rank(ascending=False, method='first')
    test_bets = test_bets[test_bets['rank_pred'] == 1]
    
    n_bootstrap = 1000
    bootstrap_rois = []
    
    np.random.seed(42)
    for _ in range(n_bootstrap):
        sample = test_bets.sample(n=len(test_bets), replace=True)
        hits = sample[sample['finish_position'] == 1]
        roi = hits['win_odds'].sum() / len(sample) * 100
        bootstrap_rois.append(roi)
    
    ci_lower = np.percentile(bootstrap_rois, 2.5)
    ci_upper = np.percentile(bootstrap_rois, 97.5)
    
    logger.info(f"    95%信頼区間: [{ci_lower:.1f}%, {ci_upper:.1f}%]")
    logger.info(f"    幅: {ci_upper - ci_lower:.1f}%")
    
    if ci_lower > 80:
        logger.info("  ✅ 95%信頼区間の下限が80%以上（統計的に有意）")
    elif ci_lower > 75:
        logger.info("  △ 95%信頼区間の下限が75-80%（やや有意）")
    else:
        logger.info("  ⚠️ 95%信頼区間の下限が75%未満（統計的に不安定）")
    
    # 月別安定性
    logger.info("")
    logger.info("  月別ROI分布:")
    
    test_bets['month'] = test_bets['race_date'].dt.to_period('M')
    monthly_rois = []
    
    for ym in sorted(test_bets['month'].unique()):
        subset = test_bets[test_bets['month'] == ym]
        hits = subset[subset['finish_position'] == 1]
        if len(subset) > 0:
            roi = hits['win_odds'].sum() / len(subset) * 100
            monthly_rois.append(roi)
            logger.info(f"    {ym}: {roi:.1f}%")
    
    if monthly_rois:
        monthly_std = np.std(monthly_rois)
        monthly_below_80 = sum(1 for r in monthly_rois if r < 80)
        logger.info(f"")
        logger.info(f"    月別標準偏差: {monthly_std:.1f}%")
        logger.info(f"    80%未満の月: {monthly_below_80}/12")
    
    # ===== 検証5: 特徴量重要度の確認 =====
    logger.info("")
    logger.info("=" * 70)
    logger.info("検証5: 特徴量重要度（過学習の兆候）")
    logger.info("=" * 70)
    
    importance = pd.DataFrame({
        'feature': feature_cols,
        'importance': model_v15.feature_importance()
    }).sort_values('importance', ascending=False)
    
    top_importance = importance.head(5)['importance'].sum()
    total_importance = importance['importance'].sum()
    top5_ratio = top_importance / total_importance * 100
    
    logger.info(f"  Top5特徴量の寄与率: {top5_ratio:.1f}%")
    
    if top5_ratio < 50:
        logger.info("  ✅ 特定特徴量への過度な依存なし")
    else:
        logger.info("  ⚠️ Top5特徴量への依存度が高い（過学習リスク）")
    
    logger.info("")
    logger.info("  Top10特徴量:")
    for i, (_, row) in enumerate(importance.head(10).iterrows()):
        logger.info(f"    {i+1:2d}. {row['feature']}: {row['importance']:.0f}")
    
    # ===== 最終結論 =====
    logger.info("")
    logger.info("=" * 70)
    logger.info("最終結論")
    logger.info("=" * 70)
    
    issues = []
    positives = []
    
    # リーク
    if not odds_features and not future_features:
        positives.append("特徴量にリークなし")
    else:
        issues.append("リークの可能性あり")
    
    # 過学習
    if gap_train_test < 35 and gap_v44 < 35:
        positives.append(f"Train-Test Gap適正 (V15:{gap_train_test:.0f}%, V4.4:{gap_v44:.0f}%)")
    else:
        issues.append("過学習の可能性あり")
    
    # 50:50の妥当性
    if abs(best_ratio_valid - 0.5) < 0.1:
        positives.append("50:50は安定した選択")
    else:
        issues.append(f"最適比率は{best_ratio_valid:.0%}")
    
    # 高配当依存
    if len(ens_stats['hit_odds_list']) > 10:
        roi_without_top10 = remaining_sum / (ens_stats['n_bets'] - 10) * 100
        if roi_without_top10 > 75:
            positives.append(f"高配当除外後もROI {roi_without_top10:.0f}%")
        else:
            issues.append("高配当に依存している")
    
    # 統計的有意性
    if ci_lower > 80:
        positives.append(f"95%CI下限が{ci_lower:.0f}%（有意）")
    else:
        issues.append(f"95%CI下限が{ci_lower:.0f}%（不安定）")
    
    logger.info("")
    logger.info("  ✅ 良い点:")
    for p in positives:
        logger.info(f"    - {p}")
    
    if issues:
        logger.info("")
        logger.info("  ⚠️ 懸念点:")
        for i in issues:
            logger.info(f"    - {i}")
    
    logger.info("")
    if len(positives) > len(issues):
        logger.info("  【総合評価】: Ensemble(50:50)は信頼できる")
    else:
        logger.info("  【総合評価】: Ensemble(50:50)には懸念がある")


if __name__ == "__main__":
    main()
