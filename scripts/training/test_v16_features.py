"""
V16特徴量テストスクリプト

目的:
- V16特徴量エンジニアの動作確認
- V15 vs V16 のROI比較
- リーク検証
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


def main():
    data_dir = Path("keibaai/data/parsed/parquet")
    
    print("="*80)
    print("V16特徴量テスト: V15 vs V16 比較")
    print("="*80)
    
    # データ読み込み
    print("\n1. データ読み込み...")
    races_df = pd.read_parquet(data_dir / "races/races.parquet")
    races_df['race_date'] = pd.to_datetime(races_df['race_date'])
    races_df = races_df[(races_df['race_date'] >= '2020-01-01')]
    races_df = races_df.dropna(subset=['finish_position', 'win_odds'])
    
    pedigrees_df = pd.read_parquet(data_dir / "pedigrees/pedigrees.parquet")
    corners_df = pd.read_parquet(data_dir / "corners/corner_positions.parquet")
    race_details_df = pd.read_parquet(data_dir / "race_details/race_details.parquet")
    
    # 期間分割
    train_df = races_df[races_df['race_date'] < '2024-07-01'].copy()
    valid_df = races_df[(races_df['race_date'] >= '2024-07-01') & (races_df['race_date'] < '2025-01-01')].copy()
    test_df = races_df[races_df['race_date'] >= '2025-01-01'].copy()
    
    print(f"   Train: {len(train_df):,} ({train_df['race_date'].min()} 〜 {train_df['race_date'].max()})")
    print(f"   Valid: {len(valid_df):,}")
    print(f"   Test:  {len(test_df):,} ({test_df['race_date'].min()} 〜 {test_df['race_date'].max()})")
    
    # ==========================================================
    # V15での評価
    # ==========================================================
    print("\n" + "="*80)
    print("2. V15での評価")
    print("="*80)
    
    from keibaai.src.features.leak_free_feature_engineer_v15 import LeakFreeFeatureEngineerV15
    
    fe_v15 = LeakFreeFeatureEngineerV15()
    fe_v15.fit(
        races_df=train_df,
        pedigrees_df=pedigrees_df,
        corners_df=corners_df,
        race_details_df=race_details_df
    )
    
    train_feat_v15 = fe_v15.transform(train_df)
    valid_feat_v15 = fe_v15.transform(valid_df)
    test_feat_v15 = fe_v15.transform(test_df)
    
    feature_cols_v15 = fe_v15.get_feature_columns()
    feature_cols_v15 = [c for c in feature_cols_v15 if c in train_feat_v15.columns]
    print(f"   V15特徴量数: {len(feature_cols_v15)}")
    
    # モデル学習
    model_v15, roi_v15 = train_and_evaluate(
        train_feat_v15, valid_feat_v15, test_feat_v15, feature_cols_v15, "V15"
    )
    
    # ==========================================================
    # V16での評価
    # ==========================================================
    print("\n" + "="*80)
    print("3. V16での評価")
    print("="*80)
    
    from keibaai.src.features.leak_free_feature_engineer_v16 import LeakFreeFeatureEngineerV16
    
    fe_v16 = LeakFreeFeatureEngineerV16()
    fe_v16.fit(
        races_df=train_df,
        pedigrees_df=pedigrees_df,
        corners_df=corners_df,
        race_details_df=race_details_df
    )
    
    train_feat_v16 = fe_v16.transform(train_df)
    valid_feat_v16 = fe_v16.transform(valid_df)
    test_feat_v16 = fe_v16.transform(test_df)
    
    feature_cols_v16 = fe_v16.get_feature_columns()
    feature_cols_v16 = [c for c in feature_cols_v16 if c in train_feat_v16.columns]
    print(f"   V16特徴量数: {len(feature_cols_v16)}")
    
    # 新特徴量の統計確認
    print("\n   新特徴量の統計:")
    new_cols = [
        'venue_month_condition_standard_time',
        'horse_time_deviation_avg',
        'horse_best_time_deviation'
    ]
    for col in new_cols:
        if col in test_feat_v16.columns:
            non_null = test_feat_v16[col].notna().sum()
            mean_val = test_feat_v16[col].mean()
            print(f"     {col}: 非NaN {non_null:,}/{len(test_feat_v16):,} ({non_null/len(test_feat_v16)*100:.1f}%), mean={mean_val:.2f}")
    
    # モデル学習
    model_v16, roi_v16 = train_and_evaluate(
        train_feat_v16, valid_feat_v16, test_feat_v16, feature_cols_v16, "V16"
    )
    
    # ==========================================================
    # リーク検証
    # ==========================================================
    print("\n" + "="*80)
    print("4. リーク検証")
    print("="*80)
    
    # Test期間内で新特徴量が変化していないか確認
    # （リークがなければ、Test期間内では同じ馬に対して同じ値が付与されるはず）
    print("\n   Test期間内での特徴量変化チェック:")
    for col in new_cols:
        if col in test_feat_v16.columns:
            # 馬ごとに特徴量のユニーク値数を確認
            unique_counts = test_feat_v16.groupby('horse_id')[col].nunique()
            horses_with_change = (unique_counts > 1).sum()
            print(f"     {col}: 変化した馬 {horses_with_change}/{len(unique_counts)} ({horses_with_change/len(unique_counts)*100:.2f}%)")
    
    # ==========================================================
    # 結果サマリー
    # ==========================================================
    print("\n" + "="*80)
    print("5. 結果サマリー")
    print("="*80)
    
    print(f"\n   V15 Test ROI: {roi_v15['roi']:.1f}% (的中率: {roi_v15['hit_rate']*100:.1f}%)")
    print(f"   V16 Test ROI: {roi_v16['roi']:.1f}% (的中率: {roi_v16['hit_rate']*100:.1f}%)")
    print(f"   差分: {roi_v16['roi'] - roi_v15['roi']:+.1f}%")
    
    if roi_v16['roi'] > roi_v15['roi']:
        print("\n   ✅ V16がV15を上回りました！")
    else:
        print("\n   ⚠️ V16はV15を下回りました。特徴量の見直しが必要です。")
    
    print("\n   完了")


def train_and_evaluate(train_feat, valid_feat, test_feat, feature_cols, name):
    """モデル学習と評価"""
    print(f"\n   {name}モデル学習中...")
    
    # ソート
    train_sorted = train_feat.sort_values('race_id')
    valid_sorted = valid_feat.sort_values('race_id')
    test_sorted = test_feat.sort_values('race_id')
    
    # 特徴量・ターゲット準備
    X_train = train_sorted[feature_cols].fillna(0)
    y_train = train_sorted['finish_position']
    groups_train = train_sorted.groupby('race_id').size().values
    
    X_valid = valid_sorted[feature_cols].fillna(0)
    y_valid = valid_sorted['finish_position']
    groups_valid = valid_sorted.groupby('race_id').size().values
    
    # 関連度ラベル
    y_rel_train = np.zeros(len(y_train))
    y_rel_train[y_train.values == 1] = 5
    y_rel_train[y_train.values == 2] = 4
    y_rel_train[y_train.values == 3] = 3
    y_rel_train[(y_train.values >= 4) & (y_train.values <= 5)] = 1
    
    y_rel_valid = np.zeros(len(y_valid))
    y_rel_valid[y_valid.values == 1] = 5
    y_rel_valid[y_valid.values == 2] = 4
    y_rel_valid[y_valid.values == 3] = 3
    y_rel_valid[(y_valid.values >= 4) & (y_valid.values <= 5)] = 1
    
    # モデル（強力な正則化）
    model = lgb.LGBMRanker(
        objective='lambdarank',
        n_estimators=3000,
        learning_rate=0.005,
        max_depth=3,
        num_leaves=20,
        min_child_samples=100,
        reg_alpha=3.0,
        reg_lambda=5.0,
        subsample=0.5,
        colsample_bytree=0.5,
        random_state=42,
        verbose=-1
    )
    
    model.fit(
        X_train, y_rel_train,
        group=groups_train,
        eval_set=[(X_valid, y_rel_valid)],
        eval_group=[groups_valid],
        callbacks=[lgb.early_stopping(50, verbose=False)]
    )
    
    # Train評価
    train_sorted['score'] = model.predict(X_train)
    train_sorted['rank'] = train_sorted.groupby('race_id')['score'].rank(ascending=False)
    train_top1 = train_sorted[train_sorted['rank'] == 1]
    train_hits = train_top1[train_top1['finish_position'] == 1]
    train_roi = train_hits['win_odds'].sum() / len(train_top1) * 100
    train_hit_rate = len(train_hits) / len(train_top1)
    
    # Test評価
    X_test = test_sorted[feature_cols].fillna(0)
    test_sorted['score'] = model.predict(X_test)
    test_sorted['rank'] = test_sorted.groupby('race_id')['score'].rank(ascending=False)
    test_top1 = test_sorted[test_sorted['rank'] == 1]
    test_hits = test_top1[test_top1['finish_position'] == 1]
    test_roi = test_hits['win_odds'].sum() / len(test_top1) * 100
    test_hit_rate = len(test_hits) / len(test_top1)
    
    gap = train_roi - test_roi
    
    print(f"     Train ROI: {train_roi:.1f}% (的中率: {train_hit_rate*100:.1f}%)")
    print(f"     Test  ROI: {test_roi:.1f}% (的中率: {test_hit_rate*100:.1f}%)")
    print(f"     Gap: {gap:.1f}%")
    
    # 特徴量重要度Top10
    importance = pd.DataFrame({
        'feature': feature_cols,
        'importance': model.feature_importances_
    }).sort_values('importance', ascending=False)
    
    print(f"\n     特徴量重要度 Top10:")
    for i, (_, row) in enumerate(importance.head(10).iterrows()):
        print(f"       {i+1}. {row['feature']}: {row['importance']:.0f}")
    
    return model, {'roi': test_roi, 'hit_rate': test_hit_rate, 'gap': gap}


if __name__ == "__main__":
    main()
