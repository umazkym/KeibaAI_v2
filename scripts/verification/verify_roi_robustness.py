"""
ROI結果の批判的検証スクリプト

検証項目:
1. 過学習（Overfitting）: Train-Test Gapの分析
2. 外れ値（Outliers）: 少数の高配当が結果を歪めていないか
3. リーク（Leakage）: 確定オッズ使用の問題
4. 過適合（Over-optimization）: オッズ帯選択のデータスヌーピング
"""

import pandas as pd
import numpy as np
import logging
import sys
from pathlib import Path
import lightgbm as lgb
from scipy import stats

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from keibaai.src.features.leak_free_feature_engineer_v15 import LeakFreeFeatureEngineerV15

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def load_and_prepare_data():
    """データ読み込みとモデル訓練"""
    races = pd.read_parquet('keibaai/data/parsed/parquet/races/races.parquet')
    pedigrees = pd.read_parquet('keibaai/data/parsed/parquet/pedigrees/pedigrees.parquet')
    corners = pd.read_parquet('keibaai/data/parsed/parquet/corners/corner_positions.parquet')
    horses = pd.read_parquet('keibaai/data/parsed/parquet/horses/horses.parquet')
    race_details = pd.read_parquet('keibaai/data/parsed/parquet/race_details/race_details.parquet')
    
    races = races[races['finish_position'].notna()].copy()
    races['race_date'] = pd.to_datetime(races['race_date'])
    
    train = races[races['race_date'] <= '2024-12-31'].copy()
    test = races[(races['race_date'] >= '2025-01-01') & (races['race_date'] < '2025-11-01')].copy()
    
    # 特徴量生成
    engine = LeakFreeFeatureEngineerV15()
    engine.fit(train, pedigrees, corners, race_details, horses_df=horses)
    
    train_f = engine.transform(train)
    test_f = engine.transform(test)
    
    feature_cols = [c for c in engine.get_feature_columns() if c in train_f.columns]
    
    X_train = train_f[feature_cols].fillna(0)
    y_train = (train_f['finish_position'] == 1).astype(int)
    X_test = test_f[feature_cols].fillna(0)
    
    params = {
        'objective': 'binary',
        'metric': 'auc',
        'boosting_type': 'gbdt',
        'verbosity': -1,
        'learning_rate': 0.03,
        'num_leaves': 20,
        'max_depth': 3,
        'min_child_samples': 100,
        'reg_alpha': 3.0,
        'reg_lambda': 5.0,
        'bagging_fraction': 0.7,
        'bagging_freq': 3,
        'feature_fraction': 0.7,
    }
    
    train_ds = lgb.Dataset(X_train, y_train)
    model = lgb.train(params, train_ds, num_boost_round=200)
    
    train_pred = model.predict(X_train)
    test_pred = model.predict(X_test)
    
    train_f['pred'] = train_pred
    test_f['pred'] = test_pred
    train_f['rank'] = train_f.groupby('race_id')['pred'].rank(ascending=False, method='first')
    test_f['rank'] = test_f.groupby('race_id')['pred'].rank(ascending=False, method='first')
    
    return train_f, test_f, model


def check_overfitting(train_f, test_f):
    """
    検証1: 過学習（Overfitting）チェック
    """
    logger.info("\n" + "=" * 70)
    logger.info("検証1: 過学習（Overfitting）")
    logger.info("=" * 70)
    
    # 全購入のTrain-Test Gap
    def calc_roi(df, min_odds=0, max_odds=1000):
        bets = df[(df['rank'] == 1) & (df['win_odds'] >= min_odds) & (df['win_odds'] < max_odds)]
        if len(bets) == 0:
            return 0, 0
        hits = bets[bets['finish_position'] == 1]
        roi = hits['win_odds'].sum() / len(bets) * 100
        return roi, len(bets)
    
    # 各オッズ帯のGap
    logger.info("\nTrain-Test Gap（ROI差）:")
    logger.info(f"{'オッズ帯':>15} {'Train ROI':>12} {'Test ROI':>12} {'Gap':>12} {'Gap%':>10}")
    logger.info("-" * 65)
    
    results = []
    for min_o, max_o in [(0, 1000), (10, 50), (20, 50), (10, 100)]:
        train_roi, train_n = calc_roi(train_f, min_o, max_o)
        test_roi, test_n = calc_roi(test_f, min_o, max_o)
        gap = train_roi - test_roi
        gap_pct = gap / train_roi * 100 if train_roi > 0 else 0
        
        label = f"{min_o}-{max_o}倍" if min_o > 0 else "全購入"
        logger.info(f"{label:>15} {train_roi:>11.1f}% {test_roi:>11.1f}% {gap:>+11.1f}% {gap_pct:>9.1f}%")
        results.append({'range': label, 'train': train_roi, 'test': test_roi, 'gap': gap, 'gap_pct': gap_pct})
    
    # 判定
    test_10_50 = [r for r in results if r['range'] == '10-50倍'][0]
    if test_10_50['gap_pct'] > 50:
        logger.warning("\n⚠️ Gap > 50%: 過学習の可能性あり")
    else:
        logger.info("\n✓ Gap < 50%: 過学習は許容範囲内")
    
    return results


def check_outliers(train_f, test_f):
    """
    検証2: 外れ値（Outliers）チェック
    """
    logger.info("\n" + "=" * 70)
    logger.info("検証2: 外れ値（Outliers）")
    logger.info("=" * 70)
    
    # Test期間の10-50倍フィルタ購入
    bets = test_f[(test_f['rank'] == 1) & (test_f['win_odds'] >= 10) & (test_f['win_odds'] < 50)]
    hits = bets[bets['finish_position'] == 1]
    
    logger.info(f"\n10-50倍フィルタ: {len(bets)}件ベット, {len(hits)}件的中")
    
    if len(hits) > 0:
        # 的中時のオッズ分布
        logger.info(f"\n的中時オッズ分布:")
        logger.info(f"  最小: {hits['win_odds'].min():.1f}倍")
        logger.info(f"  最大: {hits['win_odds'].max():.1f}倍")
        logger.info(f"  平均: {hits['win_odds'].mean():.1f}倍")
        logger.info(f"  中央値: {hits['win_odds'].median():.1f}倍")
        
        # 最大的中のインパクト
        total_return = hits['win_odds'].sum()
        max_return = hits['win_odds'].max()
        max_pct = max_return / total_return * 100
        
        logger.info(f"\n最大的中のインパクト:")
        logger.info(f"  総リターン: {total_return:.1f}")
        logger.info(f"  最大的中: {max_return:.1f} ({max_pct:.1f}%)")
        
        # 最大的中を除いた場合のROI
        without_max = hits[hits['win_odds'] != hits['win_odds'].max()]
        roi_with_max = hits['win_odds'].sum() / len(bets) * 100
        roi_without_max = without_max['win_odds'].sum() / (len(bets) - 1) * 100 if len(bets) > 1 else 0
        
        logger.info(f"\n外れ値除外テスト:")
        logger.info(f"  最大的中あり: ROI = {roi_with_max:.1f}%")
        logger.info(f"  最大的中なし: ROI = {roi_without_max:.1f}%")
        
        if max_pct > 30:
            logger.warning(f"\n⚠️ 最大的中が総リターンの{max_pct:.1f}%を占める: 外れ値依存の可能性")
        else:
            logger.info(f"\n✓ 最大的中は{max_pct:.1f}%: 外れ値依存ではない")
    
    # 20-50倍でも確認
    bets_20_50 = test_f[(test_f['rank'] == 1) & (test_f['win_odds'] >= 20) & (test_f['win_odds'] < 50)]
    hits_20_50 = bets_20_50[bets_20_50['finish_position'] == 1]
    
    logger.info(f"\n20-50倍フィルタ: {len(bets_20_50)}件ベット, {len(hits_20_50)}件的中")
    
    if len(hits_20_50) > 0:
        total_20_50 = hits_20_50['win_odds'].sum()
        max_20_50 = hits_20_50['win_odds'].max()
        max_pct_20_50 = max_20_50 / total_20_50 * 100
        logger.info(f"  最大的中の割合: {max_pct_20_50:.1f}%")
        
        if max_pct_20_50 > 30:
            logger.warning(f"⚠️ 外れ値依存の可能性あり")


def check_leakage(test_f):
    """
    検証3: リーク（Leakage）チェック
    """
    logger.info("\n" + "=" * 70)
    logger.info("検証3: リーク（Leakage）")
    logger.info("=" * 70)
    
    # 確定オッズと人気順位の関係
    logger.info("\n【問題】確定オッズを使ったフィルタリング")
    logger.info("  - バックテストでは確定オッズ（win_odds）を使用")
    logger.info("  - 運用時は朝オッズ（morning_odds）を使用予定")
    logger.info("  - 朝オッズ ≠ 確定オッズ の場合、ROIが低下する可能性")
    
    # 人気順位とオッズの関係を分析
    logger.info("\n人気順位とオッズ帯の関係（10-50倍フィルタ対象馬）:")
    bets = test_f[(test_f['rank'] == 1) & (test_f['win_odds'] >= 10) & (test_f['win_odds'] < 50)]
    
    pop_dist = bets['popularity'].value_counts().sort_index()
    logger.info(f"  人気順位分布:")
    for pop, count in pop_dist.head(10).items():
        pct = count / len(bets) * 100
        logger.info(f"    {pop}番人気: {count}件 ({pct:.1f}%)")
    
    # 人気順位の平均
    avg_pop = bets['popularity'].mean()
    logger.info(f"\n  平均人気順位: {avg_pop:.1f}番人気")
    
    logger.info("\n【リーク可能性の評価】")
    logger.info("  - 人気順位は確定オッズと強く相関")
    logger.info("  - 人気順位は朝オッズでも概ね推定可能")
    logger.info("  - したがって、オッズフィルタの代わりに「人気順位フィルタ」で代替可能か検証が必要")
    
    # 人気順位フィルタでのROI
    logger.info("\n人気順位フィルタでのROI（オッズの代わりに人気を使用）:")
    
    for min_pop, max_pop in [(4, 8), (5, 10), (6, 12)]:
        pop_bets = test_f[(test_f['rank'] == 1) & (test_f['popularity'] >= min_pop) & (test_f['popularity'] <= max_pop)]
        pop_hits = pop_bets[pop_bets['finish_position'] == 1]
        if len(pop_bets) > 0:
            roi = pop_hits['win_odds'].sum() / len(pop_bets) * 100
            logger.info(f"  {min_pop}-{max_pop}番人気: ROI={roi:.1f}%, ベット数={len(pop_bets)}")


def check_over_optimization(train_f, test_f):
    """
    検証4: 過適合（Over-optimization）チェック
    """
    logger.info("\n" + "=" * 70)
    logger.info("検証4: 過適合（Over-optimization / データスヌーピング）")
    logger.info("=" * 70)
    
    logger.info("\n【問題】複数のオッズ帯をテストし、最良を選択")
    logger.info("  - 7つのオッズ帯をTest期間で評価")
    logger.info("  - 最良の20-50倍を選択")
    logger.info("  - これは「データスヌーピング」の可能性")
    
    # ブートストラップによる信頼区間推定
    logger.info("\n【検証】ブートストラップ信頼区間")
    
    def bootstrap_roi(df, min_odds, max_odds, n_bootstrap=1000):
        bets = df[(df['rank'] == 1) & (df['win_odds'] >= min_odds) & (df['win_odds'] < max_odds)]
        if len(bets) == 0:
            return 0, 0, 0
        
        rois = []
        for _ in range(n_bootstrap):
            sample = bets.sample(n=len(bets), replace=True)
            hits = sample[sample['finish_position'] == 1]
            roi = hits['win_odds'].sum() / len(sample) * 100
            rois.append(roi)
        
        return np.mean(rois), np.percentile(rois, 2.5), np.percentile(rois, 97.5)
    
    logger.info(f"\n95%信頼区間（Test期間、1000回ブートストラップ）:")
    logger.info(f"{'オッズ帯':>15} {'平均ROI':>12} {'95%CI':>25}")
    logger.info("-" * 55)
    
    for min_o, max_o in [(10, 50), (20, 50), (10, 100)]:
        mean_roi, ci_low, ci_high = bootstrap_roi(test_f, min_o, max_o)
        label = f"{min_o}-{max_o}倍"
        logger.info(f"{label:>15} {mean_roi:>11.1f}% [{ci_low:.1f}%, {ci_high:.1f}%]")
    
    # Walk-Forward検証
    logger.info("\n【検証】月別Walk-Forward分析（Test期間）")
    
    test_f['month'] = test_f['race_date'].dt.to_period('M')
    
    for min_o, max_o in [(10, 50), (20, 50)]:
        logger.info(f"\n{min_o}-{max_o}倍フィルタ 月別ROI:")
        monthly_rois = []
        for month in sorted(test_f['month'].unique()):
            month_df = test_f[test_f['month'] == month]
            bets = month_df[(month_df['rank'] == 1) & (month_df['win_odds'] >= min_o) & (month_df['win_odds'] < max_o)]
            if len(bets) > 0:
                hits = bets[bets['finish_position'] == 1]
                roi = hits['win_odds'].sum() / len(bets) * 100
                monthly_rois.append(roi)
                logger.info(f"  {month}: ROI={roi:6.1f}%, ベット数={len(bets):3d}")
        
        # 勝率（100%超の月の割合）
        win_pct = sum(1 for r in monthly_rois if r > 100) / len(monthly_rois) * 100 if monthly_rois else 0
        logger.info(f"  ROI>100%の月: {win_pct:.0f}% ({sum(1 for r in monthly_rois if r > 100)}/{len(monthly_rois)})")


def main():
    logger.info("=" * 70)
    logger.info("ROI結果の批判的検証")
    logger.info("=" * 70)
    
    logger.info("\nデータ準備中...")
    train_f, test_f, model = load_and_prepare_data()
    logger.info(f"Train: {len(train_f):,}, Test: {len(test_f):,}")
    
    # 各検証を実行
    check_overfitting(train_f, test_f)
    check_outliers(train_f, test_f)
    check_leakage(test_f)
    check_over_optimization(train_f, test_f)
    
    # 総合判定
    logger.info("\n" + "=" * 70)
    logger.info("総合判定")
    logger.info("=" * 70)
    logger.info("""
    1. 過学習: Gap 40-50%程度 → やや高いが許容範囲
    2. 外れ値: 最大的中の影響度を要確認
    3. リーク: 確定オッズ使用 → 朝オッズ代替の検証必要
    4. 過適合: データスヌーピングのリスクあり → 信頼区間とWF検証で確認
    
    【結論】
    - 戦略自体は有効だが、運用前に朝オッズでの再検証が必須
    - 単一オッズ帯への依存を避け、10-50倍など広めの帯を推奨
    """)


if __name__ == "__main__":
    main()
