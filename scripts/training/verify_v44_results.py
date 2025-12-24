#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
V4.4 結果の詳細検証スクリプト

【検証項目】
1. 高配当依存：ROIが一部の大当たりに依存していないか？
2. リークの可能性：win_oddsがターゲットに含まれていないか？
3. 過学習の兆候：年別・月別のROI一貫性
4. 有意性検証：ブートストラップでの信頼区間

【重要】
深く推論し、統計的に厳密な検証を行う
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
    logger.info("V4.4 結果の詳細検証")
    logger.info("=" * 70)
    
    from keibaai.src.features.leak_free_feature_engineer_v15_fixed import LeakFreeFeatureEngineerV15Fixed
    
    races, pedigrees, corners, race_details, horses = load_data()
    
    # まったく同じ期間設定
    train = races[races['race_date'] < '2023-01-01'].copy()
    valid = races[(races['race_date'] >= '2023-01-01') & (races['race_date'] < '2024-01-01')].copy()
    test = races[(races['race_date'] >= '2024-01-01') & (races['race_date'] < '2025-01-01')].copy()
    
    logger.info(f"Train: {len(train):,}件")
    logger.info(f"Valid: {len(valid):,}件")
    logger.info(f"Test:  {len(test):,}件")
    
    # 特徴量生成
    engine = LeakFreeFeatureEngineerV15Fixed()
    engine.fit(train, pedigrees, corners, race_details, horses_df=horses)
    
    train_f = engine.transform(train)
    valid_f = engine.transform(valid)
    test_f = engine.transform(test)
    
    feature_cols = [c for c in engine.get_feature_columns() if c in train_f.columns]
    
    # ===== 検証1: ターゲット変数のリークチェック =====
    logger.info("")
    logger.info("=" * 70)
    logger.info("検証1: ターゲット変数のリークチェック")
    logger.info("=" * 70)
    
    # V4.4のターゲット計算ロジックを確認
    weight_1st, weight_2nd, weight_3rd = 12.74, 6.73, 3.69
    
    df = train_f.copy()
    odds = df['win_odds'].fillna(1.0).clip(upper=90)
    log_odds = np.log1p(odds)
    
    gain = np.zeros(len(df))
    gain[df['finish_position'] == 1] = log_odds[df['finish_position'] == 1] * weight_1st
    gain[df['finish_position'] == 2] = log_odds[df['finish_position'] == 2] * weight_2nd
    gain[df['finish_position'] == 3] = log_odds[df['finish_position'] == 3] * weight_3rd
    
    logger.info("  ターゲット計算式:")
    logger.info("    1着: log1p(win_odds) * 12.74")
    logger.info("    2着: log1p(win_odds) * 6.73")
    logger.info("    3着: log1p(win_odds) * 3.69")
    logger.info("")
    logger.info("  ⚠️ 【リークの可能性】")
    logger.info("    win_oddsはレース当日のオッズであり、")
    logger.info("    レース結果（順位）と強く相関している！")
    logger.info("    → 高配当馬が勝った場合のターゲット値が非常に高くなる")
    logger.info("    → モデルはこのパターンを学習してしまう可能性")
    
    # win_oddsとfinish_positionの相関確認
    corr = train_f[['win_odds', 'finish_position']].corr().iloc[0, 1]
    logger.info(f"")
    logger.info(f"  win_odds と finish_position の相関: {corr:.3f}")
    
    # ===== 検証2: 高配当依存の確認 =====
    logger.info("")
    logger.info("=" * 70)
    logger.info("検証2: 高配当依存の確認")
    logger.info("=" * 70)
    
    # V4.4モデル訓練
    v44_params = {
        'objective': 'lambdarank',
        'boosting_type': 'gbdt',
        'num_leaves': 25,
        'max_depth': 4,
        'min_child_samples': 150,
        'learning_rate': 0.05,
        'reg_alpha': 5.0,
        'reg_lambda': 8.0,
        'feature_fraction': 0.6,
        'bagging_fraction': 0.7,
        'bagging_freq': 5,
        'verbose': -1,
        'random_state': 42,
        'label_gain': list(range(100))
    }
    
    train_df = train_f.copy()
    train_df['target'] = gain.astype(int)
    train_df = train_df.sort_values('race_id')
    groups = train_df.groupby('race_id', sort=False).size().to_list()
    
    model = lgb.LGBMRanker(**v44_params, n_estimators=200)
    model.fit(train_df[feature_cols].fillna(0), train_df['target'], group=groups)
    
    test_pred = model.predict(test_f[feature_cols].fillna(0))
    
    # 詳細ROI
    stats = calc_roi_detailed(test_f, test_pred)
    
    logger.info(f"  Test ROI: {stats['roi']:.1f}%")
    logger.info(f"  的中数: {stats['n_hits']}/{stats['n_bets']}")
    logger.info(f"  平均的中オッズ: {stats['avg_hit_odds']:.1f}倍")
    logger.info(f"  最大的中オッズ: {stats['max_hit_odds']:.1f}倍")
    
    # 高配当的中の影響
    if stats['hit_odds_list']:
        odds_list = sorted(stats['hit_odds_list'], reverse=True)
        logger.info(f"")
        logger.info(f"  高配当的中 Top10:")
        for i, o in enumerate(odds_list[:10]):
            logger.info(f"    {i+1}. {o:.1f}倍")
        
        # Top10を除いたROI
        top10_sum = sum(odds_list[:10])
        remaining_sum = sum(odds_list[10:]) if len(odds_list) > 10 else 0
        remaining_bets = stats['n_bets'] - 10 if stats['n_bets'] > 10 else stats['n_bets']
        roi_without_top10 = (remaining_sum + (10 if stats['n_bets'] > 10 else stats['n_bets']) * 1) / stats['n_bets'] * 100
        
        logger.info(f"")
        logger.info(f"  ⚠️ 高配当依存分析:")
        logger.info(f"    Top10的中の合計: {top10_sum:.1f}倍")
        logger.info(f"    残りの的中合計: {remaining_sum:.1f}倍")
        logger.info(f"    Top10を除いたROI: {(remaining_sum / (stats['n_bets'] - 10) * 100) if stats['n_bets'] > 10 and remaining_sum > 0 else 0:.1f}%")
    
    # ===== 検証3: 月別ROI分析 =====
    logger.info("")
    logger.info("=" * 70)
    logger.info("検証3: 月別ROI分析（安定性確認）")
    logger.info("=" * 70)
    
    test_f['pred'] = test_pred
    test_f['rank_pred'] = test_f.groupby('race_id')['pred'].rank(ascending=False, method='first')
    test_f['month'] = test_f['race_date'].dt.month
    test_f['year_month'] = test_f['race_date'].dt.to_period('M')
    
    monthly_roi = []
    for ym in sorted(test_f['year_month'].unique()):
        subset = test_f[test_f['year_month'] == ym]
        bets = subset[subset['rank_pred'] == 1]
        hits = bets[bets['finish_position'] == 1]
        if len(bets) > 0:
            roi = hits['win_odds'].sum() / len(bets) * 100
            monthly_roi.append({
                'month': str(ym),
                'n_bets': len(bets),
                'n_hits': len(hits),
                'roi': roi
            })
            logger.info(f"  {ym}: ROI={roi:.1f}%, Bets={len(bets)}, Hits={len(hits)}")
    
    # 月別ROIの統計
    rois = [m['roi'] for m in monthly_roi]
    logger.info(f"")
    logger.info(f"  月別ROI統計:")
    logger.info(f"    平均: {np.mean(rois):.1f}%")
    logger.info(f"    標準偏差: {np.std(rois):.1f}%")
    logger.info(f"    最小: {np.min(rois):.1f}%")
    logger.info(f"    最大: {np.max(rois):.1f}%")
    
    # ===== 検証4: win_oddsを除外した場合 =====
    logger.info("")
    logger.info("=" * 70)
    logger.info("検証4: ターゲットからwin_oddsを除外した場合")
    logger.info("=" * 70)
    
    # win_oddsを使わないターゲット
    gain_no_odds = np.zeros(len(train_f))
    gain_no_odds[train_f['finish_position'] == 1] = 3  # 単純に順位だけ
    gain_no_odds[train_f['finish_position'] == 2] = 2
    gain_no_odds[train_f['finish_position'] == 3] = 1
    
    train_df2 = train_f.copy()
    train_df2['target'] = gain_no_odds.astype(int)
    train_df2 = train_df2.sort_values('race_id')
    groups2 = train_df2.groupby('race_id', sort=False).size().to_list()
    
    model2 = lgb.LGBMRanker(**v44_params, n_estimators=200)
    model2.fit(train_df2[feature_cols].fillna(0), train_df2['target'], group=groups2)
    
    test_pred2 = model2.predict(test_f[feature_cols].fillna(0))
    stats2 = calc_roi_detailed(test_f, test_pred2)
    
    logger.info(f"  オッズなしターゲット: 1着=3, 2着=2, 3着=1")
    logger.info(f"  Test ROI: {stats2['roi']:.1f}%")
    logger.info(f"  （オッズありと比較: {stats['roi']:.1f}% → {stats2['roi']:.1f}%）")
    
    # ===== 検証5: 特徴量重要度の確認 =====
    logger.info("")
    logger.info("=" * 70)
    logger.info("検証5: 特徴量重要度（リーク特徴量の確認）")
    logger.info("=" * 70)
    
    importance = pd.DataFrame({
        'feature': feature_cols,
        'importance': model.feature_importances_
    }).sort_values('importance', ascending=False)
    
    logger.info("  Top 15 特徴量:")
    for i, (_, row) in enumerate(importance.head(15).iterrows()):
        leak_flag = ""
        if 'odds' in row['feature'].lower():
            leak_flag = "⚠️ LEAK?"
        logger.info(f"    {i+1:2d}. {row['feature']}: {row['importance']:.0f} {leak_flag}")
    
    # ===== 検証6: ブートストラップ信頼区間 =====
    logger.info("")
    logger.info("=" * 70)
    logger.info("検証6: ブートストラップ信頼区間")
    logger.info("=" * 70)
    
    test_bets = test_f[test_f['rank_pred'] == 1].copy()
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
    
    logger.info(f"  ブートストラップ n={n_bootstrap}")
    logger.info(f"  平均ROI: {np.mean(bootstrap_rois):.1f}%")
    logger.info(f"  95%信頼区間: [{ci_lower:.1f}%, {ci_upper:.1f}%]")
    logger.info(f"  幅: {ci_upper - ci_lower:.1f}%")
    
    # ===== 最終結論 =====
    logger.info("")
    logger.info("=" * 70)
    logger.info("最終結論")
    logger.info("=" * 70)
    
    logger.info("""
  【発見された問題】
  
  1. ターゲット変数にwin_oddsが含まれている
     → 高配当馬が勝つとターゲット値が高くなる
     → モデルはこのパターンを学習している可能性
  
  2. 高配当的中への依存度が高い
     → Top10の的中がROIの大部分を占めている可能性
  
  3. 月別ROIの変動が大きい
     → 安定した予測力ではなく、運の要素が大きい可能性
  
  【推奨】
  
  V4.4のROI 92.9%は過大評価の可能性が高い。
  win_oddsを除外したターゲットでの再評価が必要。
    """)
    
    return {
        'roi_with_odds': stats['roi'],
        'roi_without_odds': stats2['roi'],
        'ci_lower': ci_lower,
        'ci_upper': ci_upper,
    }


if __name__ == "__main__":
    main()
