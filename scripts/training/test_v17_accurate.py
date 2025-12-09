"""
V17 正確な比較テスト

ドキュメント(17_リークフリー特徴量エンジニアV7実装レポート.md)と同じ設定で
V15 vs V17の比較を行う。

設定:
- モデル: Binary Classification (objective='binary')
- Train期間: 〜2024-12-31
- Test期間: 2025-01-01〜
- ハイパーパラメータ: test_v15_features.pyと同じ
"""

import sys
sys.path.insert(0, '.')

import pandas as pd
import numpy as np
import lightgbm as lgb
from pathlib import Path
import logging

logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)


def load_data():
    """データ読み込み"""
    data_dir = Path("keibaai/data/parsed/parquet")
    
    races = pd.read_parquet(data_dir / "races/races.parquet")
    pedigrees = pd.read_parquet(data_dir / "pedigrees/pedigrees.parquet")
    corners = pd.read_parquet(data_dir / "corners/corner_positions.parquet")
    horses = pd.read_parquet(data_dir / "horses/horses.parquet")
    race_details = pd.read_parquet(data_dir / "race_details/race_details.parquet")
    
    races = races[races['finish_position'].notna()].copy()
    races['race_date'] = pd.to_datetime(races['race_date'])
    
    # ドキュメントと同じ期間設定
    train = races[races['race_date'] <= '2024-12-31'].copy()
    test = races[(races['race_date'] >= '2025-01-01') & (races['race_date'] < '2025-11-01')].copy()
    
    return train, test, pedigrees, corners, horses, race_details


def calc_roi(df, preds):
    """ROI計算"""
    df = df.copy()
    df['pred'] = preds
    df['rank'] = df.groupby('race_id')['pred'].rank(ascending=False, method='first')
    bets = df[df['rank'] == 1]
    hits = bets[bets['finish_position'] == 1]
    roi = hits['win_odds'].sum() / len(bets) * 100 if len(bets) > 0 else 0
    hit_rate = len(hits) / len(bets) * 100 if len(bets) > 0 else 0
    return roi, hit_rate


def main():
    print("="*80)
    print("V17 正確な比較テスト (ドキュメントと同じ設定)")
    print("="*80)
    
    train, test, pedigrees, corners, horses, race_details = load_data()
    print(f"\nTrain: {len(train):,} ({train['race_date'].min()} 〜 {train['race_date'].max()})")
    print(f"Test:  {len(test):,} ({test['race_date'].min()} 〜 {test['race_date'].max()})")
    
    # ドキュメントと同じハイパーパラメータ
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
    
    results = []
    
    # ==========================================================
    # V15テスト
    # ==========================================================
    print("\n" + "="*80)
    print("V15テスト")
    print("="*80)
    
    from keibaai.src.features.leak_free_feature_engineer_v15 import LeakFreeFeatureEngineerV15
    
    engine_v15 = LeakFreeFeatureEngineerV15()
    engine_v15.fit(train, pedigrees, corners, race_details, horses_df=horses)
    
    train_f_v15 = engine_v15.transform(train)
    test_f_v15 = engine_v15.transform(test)
    
    feature_cols_v15 = [c for c in engine_v15.get_feature_columns() if c in train_f_v15.columns]
    print(f"   Features: {len(feature_cols_v15)}")
    
    X_train = train_f_v15[feature_cols_v15].fillna(0)
    y_train = (train_f_v15['finish_position'] == 1).astype(int)
    X_test = test_f_v15[feature_cols_v15].fillna(0)
    
    train_ds = lgb.Dataset(X_train, y_train)
    model_v15 = lgb.train(params, train_ds, num_boost_round=200)
    
    train_pred = model_v15.predict(X_train)
    test_pred = model_v15.predict(X_test)
    
    train_roi_v15, train_hit_v15 = calc_roi(train_f_v15, train_pred)
    test_roi_v15, test_hit_v15 = calc_roi(test_f_v15, test_pred)
    gap_v15 = train_roi_v15 - test_roi_v15
    
    print(f"\n   V15 Results:")
    print(f"     Train ROI: {train_roi_v15:.1f}% (Hit: {train_hit_v15:.1f}%)")
    print(f"     Test  ROI: {test_roi_v15:.1f}% (Hit: {test_hit_v15:.1f}%)")
    print(f"     Gap:       {gap_v15:.1f}%")
    
    results.append({
        'version': 'V15',
        'train_roi': train_roi_v15,
        'test_roi': test_roi_v15,
        'gap': gap_v15,
        'n_features': len(feature_cols_v15)
    })
    
    # ==========================================================
    # V17テスト
    # ==========================================================
    print("\n" + "="*80)
    print("V17テスト")
    print("="*80)
    
    from keibaai.src.features.leak_free_feature_engineer_v17 import LeakFreeFeatureEngineerV17
    
    engine_v17 = LeakFreeFeatureEngineerV17()
    engine_v17.fit(train, pedigrees, corners, race_details, horses_df=horses)
    
    train_f_v17 = engine_v17.transform(train)
    test_f_v17 = engine_v17.transform(test)
    
    feature_cols_v17 = [c for c in engine_v17.get_feature_columns() if c in train_f_v17.columns]
    print(f"   Features: {len(feature_cols_v17)}")
    
    # 新特徴量の統計確認
    print("\n   新特徴量の統計(Test):")
    new_cols = [
        'is_best_distance',
        'jockey_changed',
        'best_dist_win_rate'
    ]
    for col in new_cols:
        if col in test_f_v17.columns:
            non_null = test_f_v17[col].notna().sum()
            mean_val = test_f_v17[col].mean()
            print(f"     {col}: 非NaN {non_null:,}/{len(test_f_v17):,} ({non_null/len(test_f_v17)*100:.1f}%), mean={mean_val:.2f}")
    
    X_train_v17 = train_f_v17[feature_cols_v17].fillna(0)
    y_train_v17 = (train_f_v17['finish_position'] == 1).astype(int)
    X_test_v17 = test_f_v17[feature_cols_v17].fillna(0)
    
    train_ds_v17 = lgb.Dataset(X_train_v17, y_train_v17)
    model_v17 = lgb.train(params, train_ds_v17, num_boost_round=200)
    
    train_pred_v17 = model_v17.predict(X_train_v17)
    test_pred_v17 = model_v17.predict(X_test_v17)
    
    train_roi_v17, train_hit_v17 = calc_roi(train_f_v17, train_pred_v17)
    test_roi_v17, test_hit_v17 = calc_roi(test_f_v17, test_pred_v17)
    gap_v17 = train_roi_v17 - test_roi_v17
    
    print(f"\n   V17 Results:")
    print(f"     Train ROI: {train_roi_v17:.1f}% (Hit: {train_hit_v17:.1f}%)")
    print(f"     Test  ROI: {test_roi_v17:.1f}% (Hit: {test_hit_v17:.1f}%)")
    print(f"     Gap:       {gap_v17:.1f}%")
    
    results.append({
        'version': 'V17',
        'train_roi': train_roi_v17,
        'test_roi': test_roi_v17,
        'gap': gap_v17,
        'n_features': len(feature_cols_v17)
    })
    
    # 新特徴量の重要度
    imp = pd.DataFrame({
        'feature': feature_cols_v17,
        'gain': model_v17.feature_importance(importance_type='gain')
    }).sort_values('gain', ascending=False)
    
    print("\n   Top 10 Feature Importances (V17):")
    for i, (_, row) in enumerate(imp.head(10).iterrows()):
        new_marker = " ★NEW" if row['feature'] in new_cols else ""
        print(f"     {i+1}. {row['feature']}: {row['gain']:.1f}{new_marker}")
        
    # 新特徴量の順位確認
    print("\n   新特徴量の順位:")
    for col in new_cols:
        if col in imp['feature'].values:
            rank = imp[imp['feature'] == col].index[0] + 1
            gain = imp[imp['feature'] == col]['gain'].values[0]
            print(f"     {col}: Rank {rank}/{len(feature_cols_v17)}, Gain {gain:.1f}")
    
    # ==========================================================
    # 結果サマリー
    # ==========================================================
    print("\n" + "="*80)
    print("結果サマリー")
    print("="*80)
    
    print("\n   バージョン    | 特徴量数 | Train ROI | Test ROI | Gap")
    print("   " + "-"*60)
    for r in results:
        print(f"   {r['version']:12s} | {r['n_features']:8d} | {r['train_roi']:8.1f}% | {r['test_roi']:7.1f}% | {r['gap']:5.1f}%")
    
    # 比較
    print("\n   --- V15との比較 ---")
    for r in results[1:]:
        delta_roi = r['test_roi'] - results[0]['test_roi']
        delta_gap = r['gap'] - results[0]['gap']
        print(f"   {r['version']}: Test ROI {delta_roi:+.1f}%, Gap {delta_gap:+.1f}%")
    
    # 推奨判定
    best = max(results, key=lambda x: x['test_roi'])
    
    if best['version'] != 'V15':
        if best['gap'] < 40:
             print(f"\n   ✅ 推奨: {best['version']} (ROI {best['test_roi']:.1f}%, Gap {best['gap']:.1f}%)")
        else:
             print(f"\n   ⚠️ {best['version']}はROI向上するがGap {best['gap']:.1f}%（過学習リスク）")
    else:
        print("\n   ⚠️ V17はV15を上回れませんでした")
    
    print("\n   完了")


if __name__ == "__main__":
    main()
