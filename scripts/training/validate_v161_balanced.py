# -*- coding: utf-8 -*-
"""
V16.1: バランス調整版

【V16の問題】
- 正則化が強すぎてROIが犠牲になった（Gap軽減は成功）
- 平均ROI: 75.1% vs V15の79.9%

【V16.1の方針】
- 正則化を緩和してROIを改善しつつ、Gapは許容範囲に維持
- データ品質向上と欠損補完は維持（これらは効果あり）
- パラメータをV15とV16の中間に調整

【各改修の効果判定】
- 改修1(データ品質): 効果あり（維持）
- 改修2(欠損補完): 効果あり（維持）
- 改修3(正則化): 効果過剰（緩和）
- 改修4(アンサンブル): 年によって最適重みが異なる
"""

import pandas as pd
import numpy as np
import lightgbm as lgb
from pathlib import Path
import sys
import warnings
warnings.filterwarnings('ignore')

project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(project_root))


# ==============================================================================
# 改修1&2: データ品質向上 + 欠損補完（V16と同じ）
# ==============================================================================
def apply_data_quality_improvements(df):
    df = df.copy()
    winsorize_cols = ['finish_time_seconds', 'last_3f_time', 'horse_weight',
                      'weight_change', 'horse_weight_deviation']
    for col in winsorize_cols:
        if col in df.columns:
            valid_mask = df[col].notna()
            if valid_mask.sum() > 100:
                lower = df.loc[valid_mask, col].quantile(0.01)
                upper = df.loc[valid_mask, col].quantile(0.99)
                df[col] = df[col].clip(lower=lower, upper=upper)
    return df


def apply_imputation_improvements(df, imputation_stats=None):
    df = df.copy()
    continuous_cols = [
        'jockey_win_rate', 'jockey_top3_rate', 'trainer_win_rate',
        'horse_win_rate', 'horse_top3_rate', 'horse_avg_finish',
        'horse_last_finish', 'horse_last3_avg_finish',
        'sire_win_rate', 'bms_win_rate',
        'horse_front_runner_rate', 'horse_c4_gap_avg',
    ]
    
    if imputation_stats is None:
        imputation_stats = {'median': {}}
        for col in continuous_cols:
            if col in df.columns:
                imputation_stats['median'][col] = df[col].median()
    
    for col, median_val in imputation_stats['median'].items():
        if col in df.columns and pd.notna(median_val):
            df[col] = df[col].fillna(median_val)
    
    return df, imputation_stats


# ==============================================================================
# 改修3: 正則化（緩和版 - V15とV16の中間）
# ==============================================================================
def get_v161_params():
    """V16.1パラメータ（V15とV16の中間）"""
    return {
        'objective': 'binary',
        'metric': 'auc',
        'verbosity': -1,
        'learning_rate': 0.028,      # V15: 0.03, V16: 0.025
        'num_leaves': 18,            # V15: 20, V16: 15
        'max_depth': 3,              # V15: 3, V16: 4
        'min_child_samples': 120,    # V15: 100, V16: 150
        'reg_alpha': 5.5,            # V15: 5.0, V16: 6.0
        'reg_lambda': 9.0,           # V15: 8.0, V16: 10.0
        'bagging_fraction': 0.6,
        'bagging_freq': 3,
        'feature_fraction': 0.58,     # V15: 0.6, V16: 0.55
        'random_state': 42,
    }


def get_baseline_params():
    """V15の元パラメータ"""
    return {
        'objective': 'binary', 'metric': 'auc', 'verbosity': -1,
        'learning_rate': 0.03, 'num_leaves': 20, 'max_depth': 3,
        'min_child_samples': 100, 'reg_alpha': 5.0, 'reg_lambda': 8.0,
        'bagging_fraction': 0.6, 'bagging_freq': 3, 'feature_fraction': 0.6,
    }


# ==============================================================================
# 共通関数
# ==============================================================================
def calc_roi(df, preds):
    d = df.copy()
    d['score'] = preds
    d['rank_pred'] = d.groupby('race_id')['score'].rank(ascending=False, method='first')
    top1 = d[d['rank_pred'] == 1]
    hits = top1[top1['finish_position'] == 1]
    if len(top1) == 0:
        return 0, 0
    roi = hits['win_odds'].sum() / len(top1) * 100
    hit_rate = len(hits) / len(top1) * 100
    return roi, hit_rate


def train_model(train_df, valid_df, feature_cols, params):
    X_train = train_df[feature_cols].fillna(0)
    X_valid = valid_df[feature_cols].fillna(0)
    y_train = (train_df['finish_position'] == 1).astype(int)
    y_valid = (valid_df['finish_position'] == 1).astype(int)
    
    train_ds = lgb.Dataset(X_train, y_train)
    valid_ds = lgb.Dataset(X_valid, y_valid, reference=train_ds)
    
    model = lgb.train(params, train_ds, num_boost_round=500, valid_sets=[valid_ds],
                      callbacks=[lgb.early_stopping(stopping_rounds=30, verbose=False)])
    return model


def main():
    from keibaai.src.features.leak_free_feature_engineer_v15_fixed import LeakFreeFeatureEngineerV15Fixed
    
    print("=" * 80)
    print("V16.1: バランス調整版検証")
    print("=" * 80)
    print("\n【改修内容】")
    print("  - データ品質向上: 維持")
    print("  - 欠損補完改善: 維持")
    print("  - 正則化: V15とV16の中間値に調整")
    
    data_dir = project_root / "keibaai/data/parsed/parquet"
    races = pd.read_parquet(data_dir / "races/races.parquet")
    pedigrees = pd.read_parquet(data_dir / "pedigrees/pedigrees.parquet")
    corners = pd.read_parquet(data_dir / "corners/corner_positions.parquet")
    race_details = pd.read_parquet(data_dir / "race_details/race_details.parquet")
    horses = pd.read_parquet(data_dir / "horses/horses.parquet")
    
    races = races[races['finish_position'].notna()].copy()
    races['race_date'] = pd.to_datetime(races['race_date'])
    
    print(f"\n総データ: {len(races):,}件")
    
    test_periods = [
        ('2024', '2022-12-31', '2023-01-01', '2023-12-31', '2024-01-01', '2025-01-01'),
        ('2023', '2021-12-31', '2022-01-01', '2022-12-31', '2023-01-01', '2024-01-01'),
        ('2022', '2020-12-31', '2021-01-01', '2021-12-31', '2022-01-01', '2023-01-01'),
        ('2021', '2019-12-31', '2020-01-01', '2020-12-31', '2021-01-01', '2022-01-01'),
    ]
    
    all_results = []
    
    for year, train_end, valid_start, valid_end, test_start, test_end in test_periods:
        print(f"\n{'='*80}")
        print(f"[{year}年テスト]")
        print("=" * 80)
        
        train = races[races['race_date'] <= train_end].copy()
        valid = races[(races['race_date'] >= valid_start) & (races['race_date'] < valid_end)].copy()
        test = races[(races['race_date'] >= test_start) & (races['race_date'] < test_end)].copy()
        
        print(f"  データ: Train={len(train):,}, Valid={len(valid):,}, Test={len(test):,}")
        
        if len(train) < 10000:
            print("  ⚠️ スキップ")
            continue
        
        # 特徴量生成
        engine = LeakFreeFeatureEngineerV15Fixed()
        engine.fit(train, pedigrees, corners, race_details, horses_df=horses)
        
        train_f = engine.transform(train)
        valid_f = engine.transform(valid)
        test_f = engine.transform(test)
        
        feature_cols = [c for c in engine.get_feature_columns() if c in train_f.columns]
        
        # ========== V15ベースライン ==========
        baseline_params = get_baseline_params()
        model_v15 = train_model(train_f, valid_f, feature_cols, baseline_params)
        
        X_test = test_f[feature_cols].fillna(0)
        preds_v15 = model_v15.predict(X_test)
        roi_v15, hit_v15 = calc_roi(test_f, preds_v15)
        
        X_train = train_f[feature_cols].fillna(0)
        train_preds_v15 = model_v15.predict(X_train)
        train_roi_v15, _ = calc_roi(train_f, train_preds_v15)
        gap_v15 = train_roi_v15 - roi_v15
        
        # ========== V16.1 ==========
        # 改修1&2適用
        train_v161 = apply_data_quality_improvements(train_f)
        valid_v161 = apply_data_quality_improvements(valid_f)
        test_v161 = apply_data_quality_improvements(test_f)
        
        train_v161, imputation_stats = apply_imputation_improvements(train_v161)
        valid_v161, _ = apply_imputation_improvements(valid_v161, imputation_stats)
        test_v161, _ = apply_imputation_improvements(test_v161, imputation_stats)
        
        # 改修3（緩和版）
        v161_params = get_v161_params()
        model_v161 = train_model(train_v161, valid_v161, feature_cols, v161_params)
        
        X_test_v161 = test_v161[feature_cols].fillna(0)
        preds_v161 = model_v161.predict(X_test_v161)
        roi_v161, hit_v161 = calc_roi(test_v161, preds_v161)
        
        X_train_v161 = train_v161[feature_cols].fillna(0)
        train_preds_v161 = model_v161.predict(X_train_v161)
        train_roi_v161, _ = calc_roi(train_v161, train_preds_v161)
        gap_v161 = train_roi_v161 - roi_v161
        
        diff = roi_v161 - roi_v15
        
        print(f"\n  結果（単勝ROI）:")
        print(f"    ┌──────────────┬──────────┬─────────┬──────────┐")
        print(f"    │   モデル     │ Test ROI │ 的中率  │ Gap      │")
        print(f"    ├──────────────┼──────────┼─────────┼──────────┤")
        print(f"    │ V15 Fixed    │  {roi_v15:>6.1f}% │ {hit_v15:>5.1f}% │ {gap_v15:>6.1f}% │")
        print(f"    │ V16.1        │  {roi_v161:>6.1f}% │ {hit_v161:>5.1f}% │ {gap_v161:>6.1f}% │")
        print(f"    └──────────────┴──────────┴─────────┴──────────┘")
        print(f"    差分: {diff:+.1f}%")
        
        all_results.append({
            'year': year,
            'roi_v15': roi_v15, 'roi_v161': roi_v161,
            'gap_v15': gap_v15, 'gap_v161': gap_v161,
            'diff': diff,
        })
    
    # サマリー
    print("\n" + "=" * 80)
    print("4年間検証結果サマリー")
    print("=" * 80)
    
    print(f"\n{'年':<6} {'V15Base':<12} {'V16.1':<12} {'差分':<10} {'Gap(V15)':<10} {'Gap(V16.1)':<10}")
    print("-" * 70)
    
    for r in all_results:
        print(f"{r['year']:<6} {r['roi_v15']:>8.1f}%   {r['roi_v161']:>8.1f}%   {r['diff']:>+7.1f}%  {r['gap_v15']:>8.1f}%  {r['gap_v161']:>8.1f}%")
    
    avg_v15 = np.mean([r['roi_v15'] for r in all_results])
    avg_v161 = np.mean([r['roi_v161'] for r in all_results])
    avg_diff = np.mean([r['diff'] for r in all_results])
    avg_gap_v15 = np.mean([r['gap_v15'] for r in all_results])
    avg_gap_v161 = np.mean([r['gap_v161'] for r in all_results])
    
    print("-" * 70)
    print(f"{'平均':<6} {avg_v15:>8.1f}%   {avg_v161:>8.1f}%   {avg_diff:>+7.1f}%  {avg_gap_v15:>8.1f}%  {avg_gap_v161:>8.1f}%")
    
    print("\n" + "=" * 80)
    print("結論")
    print("=" * 80)
    
    if avg_diff > 0:
        print(f"\n  ✅ V16.1がV15より改善 (平均 +{avg_diff:.1f}%)")
    elif avg_diff > -1:
        print(f"\n  ⚪ V16.1はV15とほぼ同等 ({avg_diff:+.1f}%)")
    else:
        print(f"\n  ❌ V16.1はV15より悪化 ({avg_diff:.1f}%)")
    
    if avg_gap_v161 < avg_gap_v15:
        gap_reduction = avg_gap_v15 - avg_gap_v161
        print(f"  ✅ 過学習軽減: Gap {avg_gap_v161:.1f}% (V15: {avg_gap_v15:.1f}%, -{gap_reduction:.1f}%)")
    
    # ROIが同等でGapが軽減されていれば成功
    if avg_diff >= -1 and avg_gap_v161 < avg_gap_v15:
        print(f"\n  🎯 推奨: V16.1 (ROI維持 + 過学習軽減)")


if __name__ == "__main__":
    main()
