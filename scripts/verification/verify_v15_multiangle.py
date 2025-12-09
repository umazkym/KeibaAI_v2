"""
V15多角的深層分析

シャッフルテストの差が+4.7%と小さかった点を深掘り。
実データを使って、V15新特徴量の「本当の予測力」を多角的に検証。

検証項目:
1. 特徴量分布の確認（正常か、外れ値はないか）
2. 勝者 vs 非勝者での特徴量平均差（予測力の源泉）
3. オッズとの相関（市場が既に織り込んでいないか）
4. 特徴量値別の勝率・ROI（単調性があるか）
5. 高ROI的中のケースで特徴量がどう寄与したか
6. V15新特徴量を除外した場合のROI（真の寄与度）
"""

import pandas as pd
import numpy as np
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from keibaai.src.features.leak_free_feature_engineer_v15 import LeakFreeFeatureEngineerV15
from keibaai.src.features.leak_free_feature_engineer_v14 import LeakFreeFeatureEngineerV14
import lightgbm as lgb

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


def analyze_feature_distributions(test_f):
    """1. 特徴量分布の確認"""
    logger.info("\n" + "=" * 60)
    logger.info("1. 特徴量分布の確認")
    logger.info("=" * 60)
    
    new_features = [
        'race_front_runner_count',
        'horse_c4_gap_avg',
        'post_style_conflict',
        'front_runner_competition',
    ]
    
    for feat in new_features:
        if feat not in test_f.columns:
            continue
        
        data = test_f[feat].dropna()
        logger.info(f"\n{feat}:")
        logger.info(f"  件数: {len(data):,} / {len(test_f):,} ({len(data)/len(test_f)*100:.1f}%)")
        logger.info(f"  平均: {data.mean():.3f}")
        logger.info(f"  中央値: {data.median():.3f}")
        logger.info(f"  標準偏差: {data.std():.3f}")
        logger.info(f"  最小-最大: {data.min():.3f} - {data.max():.3f}")
        logger.info(f"  25%-75%: {data.quantile(0.25):.3f} - {data.quantile(0.75):.3f}")
        
        # 外れ値チェック（3σ以上）
        outliers = ((data - data.mean()).abs() > 3 * data.std()).sum()
        logger.info(f"  外れ値(3σ): {outliers} ({outliers/len(data)*100:.2f}%)")


def analyze_winner_nonwinner_diff(test_f):
    """2. 勝者 vs 非勝者での特徴量平均差"""
    logger.info("\n" + "=" * 60)
    logger.info("2. 勝者 vs 非勝者での特徴量平均差")
    logger.info("=" * 60)
    
    winners = test_f[test_f['finish_position'] == 1]
    non_winners = test_f[test_f['finish_position'] != 1]
    
    new_features = [
        'race_front_runner_count',
        'horse_c4_gap_avg',
        'post_style_conflict',
        'front_runner_competition',
    ]
    
    logger.info("\n{:30s} {:>10s} {:>10s} {:>10s} {:>10s}".format(
        "特徴量", "勝者平均", "非勝者平均", "差分", "t統計量"
    ))
    logger.info("-" * 70)
    
    from scipy import stats
    
    for feat in new_features:
        if feat not in test_f.columns:
            continue
        
        w_data = winners[feat].dropna()
        nw_data = non_winners[feat].dropna()
        
        if len(w_data) < 10 or len(nw_data) < 10:
            continue
        
        w_mean = w_data.mean()
        nw_mean = nw_data.mean()
        diff = w_mean - nw_mean
        
        # t検定
        t_stat, p_val = stats.ttest_ind(w_data, nw_data)
        
        sig = "***" if p_val < 0.001 else "**" if p_val < 0.01 else "*" if p_val < 0.05 else ""
        
        logger.info(f"{feat:30s} {w_mean:10.3f} {nw_mean:10.3f} {diff:+10.3f} {t_stat:10.2f} {sig}")


def analyze_odds_correlation(test_f):
    """3. オッズとの相関（市場がすでに織り込んでいないか）"""
    logger.info("\n" + "=" * 60)
    logger.info("3. オッズとの相関（市場織込み度）")
    logger.info("=" * 60)
    
    new_features = [
        'race_front_runner_count',
        'horse_c4_gap_avg',
        'post_style_conflict',
        'front_runner_competition',
    ]
    
    logger.info("\n{:30s} {:>12s} {:>12s}".format(
        "特徴量", "オッズ相関", "判定"
    ))
    logger.info("-" * 60)
    
    for feat in new_features:
        if feat not in test_f.columns:
            continue
        
        data = test_f[[feat, 'win_odds']].dropna()
        if len(data) < 100:
            continue
        
        corr = data[feat].corr(data['win_odds'])
        
        if abs(corr) > 0.3:
            judgment = "⚠️ 高い相関（市場織込み済みか）"
        elif abs(corr) > 0.15:
            judgment = "中程度の相関"
        else:
            judgment = "✅ 低い相関（独立情報）"
        
        logger.info(f"{feat:30s} {corr:12.3f} {judgment}")


def analyze_feature_value_performance(test_f):
    """4. 特徴量値別の勝率・ROI（単調性）"""
    logger.info("\n" + "=" * 60)
    logger.info("4. 特徴量値別の勝率・ROI")
    logger.info("=" * 60)
    
    new_features = [
        ('horse_c4_gap_avg', [0, 2, 4, 6, 8, 10, 100]),
        ('post_style_conflict', [0, 0.1, 0.2, 0.3, 0.4, 1.0]),
        ('front_runner_competition', [0, 1, 2, 3, 4, 20]),
        ('race_front_runner_count', [0, 1, 2, 3, 4, 20]),
    ]
    
    for feat, bins in new_features:
        if feat not in test_f.columns:
            continue
        
        logger.info(f"\n【{feat}】")
        
        data = test_f[test_f[feat].notna()].copy()
        if len(data) < 100:
            logger.info("  データ不足")
            continue
        
        data['bin'] = pd.cut(data[feat], bins=bins)
        
        for bin_val in data['bin'].unique():
            if pd.isna(bin_val):
                continue
            subset = data[data['bin'] == bin_val]
            if len(subset) < 50:
                continue
            
            wins = (subset['finish_position'] == 1).sum()
            win_rate = wins / len(subset) * 100
            roi = subset[subset['finish_position'] == 1]['win_odds'].sum() / len(subset) * 100
            avg_odds = subset['win_odds'].mean()
            
            logger.info(f"  {str(bin_val):20s}: n={len(subset):5d}, 勝率={win_rate:5.1f}%, ROI={roi:6.1f}%, 平均オッズ={avg_odds:.1f}")


def analyze_high_roi_cases(test_f, model, feature_cols):
    """5. 高ROI的中ケースでの特徴量寄与"""
    logger.info("\n" + "=" * 60)
    logger.info("5. 高ROI的中ケースでの特徴量分析")
    logger.info("=" * 60)
    
    test_f = test_f.copy()
    test_f['pred'] = model.predict(test_f[feature_cols].fillna(0))
    test_f['rank'] = test_f.groupby('race_id')['pred'].rank(ascending=False, method='first')
    
    bets = test_f[test_f['rank'] == 1]
    high_roi_hits = bets[(bets['finish_position'] == 1) & (bets['win_odds'] >= 10)]
    
    logger.info(f"\n高配当的中（10倍以上）: {len(high_roi_hits)}件")
    
    if len(high_roi_hits) == 0:
        return
    
    new_features = [
        'race_front_runner_count',
        'horse_c4_gap_avg',
        'post_style_conflict',
        'front_runner_competition',
    ]
    
    logger.info("\n高ROI的中 vs 全体平均:")
    for feat in new_features:
        if feat not in test_f.columns:
            continue
        
        high_roi_mean = high_roi_hits[feat].mean()
        all_mean = test_f[feat].mean()
        diff = high_roi_mean - all_mean
        
        logger.info(f"  {feat:30s}: 高ROI={high_roi_mean:.3f}, 全体={all_mean:.3f}, 差={diff:+.3f}")


def analyze_feature_ablation(test_f, feature_cols):
    """6. V15新特徴量を除外した場合のROI（真の寄与度）"""
    logger.info("\n" + "=" * 60)
    logger.info("6. 特徴量アブレーション分析")
    logger.info("=" * 60)
    
    new_features = [
        'race_front_runner_count',
        'horse_c4_gap_avg',
        'post_style_conflict',
        'front_runner_competition',
    ]
    
    params = {
        'objective': 'binary', 'metric': 'auc', 'verbosity': -1,
        'learning_rate': 0.03, 'num_leaves': 20, 'max_depth': 3,
        'min_child_samples': 100, 'reg_alpha': 3.0, 'reg_lambda': 5.0,
        'bagging_fraction': 0.7, 'bagging_freq': 3, 'feature_fraction': 0.7
    }
    
    races, pedigrees, corners, horses, race_details = load_data()
    train = races[races['race_date'] <= '2024-12-31'].copy()
    test = races[(races['race_date'] >= '2025-01-01') & (races['race_date'] < '2025-11-01')].copy()
    
    engine = LeakFreeFeatureEngineerV15()
    engine.fit(train, pedigrees, corners, race_details, horses_df=horses)
    
    train_f = engine.transform(train)
    test_f = engine.transform(test)
    
    results = []
    
    # 全特徴量
    fc_all = [c for c in engine.get_feature_columns() if c in train_f.columns]
    X_train = train_f[fc_all].fillna(0)
    y_train = (train_f['finish_position'] == 1).astype(int)
    X_test = test_f[fc_all].fillna(0)
    
    train_ds = lgb.Dataset(X_train, y_train)
    model = lgb.train(params, train_ds, num_boost_round=200)
    
    test_pred = model.predict(X_test)
    test_f_tmp = test_f.copy()
    test_f_tmp['pred'] = test_pred
    test_f_tmp['rank'] = test_f_tmp.groupby('race_id')['pred'].rank(ascending=False, method='first')
    bets = test_f_tmp[test_f_tmp['rank'] == 1]
    hits = bets[bets['finish_position'] == 1]
    roi_all = hits['win_odds'].sum() / len(bets) * 100
    
    logger.info(f"\n全特徴量 ({len(fc_all)}個): Test ROI = {roi_all:.1f}%")
    
    # 各V15特徴量を除外
    for feat in new_features:
        if feat not in fc_all:
            continue
        
        fc_ablated = [c for c in fc_all if c != feat]
        X_train_ab = train_f[fc_ablated].fillna(0)
        X_test_ab = test_f[fc_ablated].fillna(0)
        
        train_ds_ab = lgb.Dataset(X_train_ab, y_train)
        model_ab = lgb.train(params, train_ds_ab, num_boost_round=200)
        
        test_pred_ab = model_ab.predict(X_test_ab)
        test_f_tmp = test_f.copy()
        test_f_tmp['pred'] = test_pred_ab
        test_f_tmp['rank'] = test_f_tmp.groupby('race_id')['pred'].rank(ascending=False, method='first')
        bets = test_f_tmp[test_f_tmp['rank'] == 1]
        hits = bets[bets['finish_position'] == 1]
        roi_ablated = hits['win_odds'].sum() / len(bets) * 100
        
        delta = roi_all - roi_ablated
        logger.info(f"  -{feat:30s}: ROI = {roi_ablated:.1f}% (除外時変化: {delta:+.1f}%)")
        
        results.append({
            'excluded': feat,
            'roi': roi_ablated,
            'delta': delta
        })
    
    # V15全除外（V14相当）
    fc_v14 = [c for c in fc_all if c not in new_features]
    X_train_v14 = train_f[fc_v14].fillna(0)
    X_test_v14 = test_f[fc_v14].fillna(0)
    
    train_ds_v14 = lgb.Dataset(X_train_v14, y_train)
    model_v14 = lgb.train(params, train_ds_v14, num_boost_round=200)
    
    test_pred_v14 = model_v14.predict(X_test_v14)
    test_f_tmp = test_f.copy()
    test_f_tmp['pred'] = test_pred_v14
    test_f_tmp['rank'] = test_f_tmp.groupby('race_id')['pred'].rank(ascending=False, method='first')
    bets = test_f_tmp[test_f_tmp['rank'] == 1]
    hits = bets[bets['finish_position'] == 1]
    roi_v14 = hits['win_odds'].sum() / len(bets) * 100
    
    delta_v14 = roi_all - roi_v14
    logger.info(f"\nV15新特徴量全除外 ({len(fc_v14)}個): ROI = {roi_v14:.1f}% (寄与: {delta_v14:+.1f}%)")
    
    return results


def analyze_practical_feature_quality(test_f):
    """7. 特徴量の実用的な品質チェック"""
    logger.info("\n" + "=" * 60)
    logger.info("7. 特徴量の実用的品質チェック")
    logger.info("=" * 60)
    
    # horse_c4_gap_avgの問題点を確認
    logger.info("\n【horse_c4_gap_avg詳細確認】")
    
    # NaN率が高い点
    nan_rate = test_f['horse_c4_gap_avg'].isna().mean() * 100
    logger.info(f"  NaN率: {nan_rate:.1f}%")
    
    # NaNの馬 vs 値ありの馬の成績
    has_value = test_f[test_f['horse_c4_gap_avg'].notna()]
    no_value = test_f[test_f['horse_c4_gap_avg'].isna()]
    
    if len(has_value) > 0 and len(no_value) > 0:
        win_rate_has = (has_value['finish_position'] == 1).mean() * 100
        win_rate_no = (no_value['finish_position'] == 1).mean() * 100
        
        roi_has = has_value[has_value['finish_position'] == 1]['win_odds'].sum() / len(has_value) * 100
        roi_no = no_value[no_value['finish_position'] == 1]['win_odds'].sum() / len(no_value) * 100
        
        logger.info(f"  値あり馬: n={len(has_value):,}, 勝率={win_rate_has:.1f}%, ROI={roi_has:.1f}%")
        logger.info(f"  値なし馬: n={len(no_value):,}, 勝率={win_rate_no:.1f}%, ROI={roi_no:.1f}%")
        
        if roi_no < roi_has:
            logger.info(f"  → 値あり馬のほうがROI高い（+{roi_has - roi_no:.1f}%）")
        else:
            logger.info(f"  ⚠️ 値なし馬のほうがROI高い（NaN imputation問題の可能性）")


def main():
    logger.info("=" * 60)
    logger.info("V15多角的深層分析開始")
    logger.info("=" * 60)
    
    races, pedigrees, corners, horses, race_details = load_data()
    train = races[races['race_date'] <= '2024-12-31'].copy()
    test = races[(races['race_date'] >= '2025-01-01') & (races['race_date'] < '2025-11-01')].copy()
    
    logger.info(f"\nTrain: {len(train):,}, Test: {len(test):,}")
    
    # V15 fit & transform
    engine = LeakFreeFeatureEngineerV15()
    engine.fit(train, pedigrees, corners, race_details, horses_df=horses)
    test_f = engine.transform(test)
    
    feature_cols = [c for c in engine.get_feature_columns() if c in test_f.columns]
    
    # 分析実行
    analyze_feature_distributions(test_f)
    analyze_winner_nonwinner_diff(test_f)
    analyze_odds_correlation(test_f)
    analyze_feature_value_performance(test_f)
    
    # モデル作成
    train_f = engine.transform(train)
    params = {
        'objective': 'binary', 'metric': 'auc', 'verbosity': -1,
        'learning_rate': 0.03, 'num_leaves': 20, 'max_depth': 3,
        'min_child_samples': 100, 'reg_alpha': 3.0, 'reg_lambda': 5.0,
        'bagging_fraction': 0.7, 'bagging_freq': 3, 'feature_fraction': 0.7
    }
    X_train = train_f[feature_cols].fillna(0)
    y_train = (train_f['finish_position'] == 1).astype(int)
    train_ds = lgb.Dataset(X_train, y_train)
    model = lgb.train(params, train_ds, num_boost_round=200)
    
    analyze_high_roi_cases(test_f, model, feature_cols)
    analyze_practical_feature_quality(test_f)
    analyze_feature_ablation(test_f, feature_cols)
    
    logger.info("\n" + "=" * 60)
    logger.info("V15多角的深層分析完了")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
