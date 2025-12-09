"""
V16 過学習対策版テスト

問題:
- V16 Train-Test Gap: 57.0%（過学習）
- 新特徴量の重要度が非常に高い（#1, #2）

対策:
1. 正則化の強化
2. 新特徴量の重要度を下げるために特徴量選択
3. または新特徴量のみ削除して効果を確認
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
    print("V16 過学習対策版テスト")
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
    
    print(f"   Train: {len(train_df):,}")
    print(f"   Valid: {len(valid_df):,}")
    print(f"   Test:  {len(test_df):,}")
    
    # V16特徴量生成
    print("\n2. V16特徴量生成...")
    from keibaai.src.features.leak_free_feature_engineer_v16 import LeakFreeFeatureEngineerV16
    
    fe_v16 = LeakFreeFeatureEngineerV16()
    fe_v16.fit(
        races_df=train_df,
        pedigrees_df=pedigrees_df,
        corners_df=corners_df,
        race_details_df=race_details_df
    )
    
    train_feat = fe_v16.transform(train_df)
    valid_feat = fe_v16.transform(valid_df)
    test_feat = fe_v16.transform(test_df)
    
    feature_cols = fe_v16.get_feature_columns()
    feature_cols = [c for c in feature_cols if c in train_feat.columns]
    
    # 新特徴量を除外した特徴量リスト
    new_features = [
        'venue_month_condition_standard_time',
        'horse_time_deviation_avg',
        'horse_best_time_deviation'
    ]
    feature_cols_without_new = [c for c in feature_cols if c not in new_features]
    
    # ==========================================================
    # テスト1: オリジナルV16（強正則化なし）
    # ==========================================================
    print("\n" + "="*80)
    print("テスト1: オリジナルV16（参照用）")
    print("="*80)
    
    _, roi_orig = train_and_evaluate(
        train_feat, valid_feat, test_feat, feature_cols, "V16-Original",
        max_depth=3, num_leaves=20, min_child_samples=100
    )
    
    # ==========================================================
    # テスト2: 超強力正則化版
    # ==========================================================
    print("\n" + "="*80)
    print("テスト2: V16 超強力正則化版")
    print("="*80)
    
    _, roi_strong = train_and_evaluate(
        train_feat, valid_feat, test_feat, feature_cols, "V16-StrongReg",
        max_depth=2, num_leaves=4, min_child_samples=300,
        reg_alpha=10.0, reg_lambda=20.0, subsample=0.3
    )
    
    # ==========================================================
    # テスト3: 新特徴量なし版（V15相当）
    # ==========================================================
    print("\n" + "="*80)
    print("テスト3: V16 新特徴量なし版（V15相当）")
    print("="*80)
    
    _, roi_no_new = train_and_evaluate(
        train_feat, valid_feat, test_feat, feature_cols_without_new, "V16-NoNew",
        max_depth=2, num_leaves=4, min_child_samples=300,
        reg_alpha=10.0, reg_lambda=20.0, subsample=0.3
    )
    
    # ==========================================================
    # テスト4: 新特徴量のうちtime_deviation_avgのみ
    # ==========================================================
    print("\n" + "="*80)
    print("テスト4: V16 time_deviation_avgのみ追加")
    print("="*80)
    
    feature_cols_one = feature_cols_without_new + ['horse_time_deviation_avg']
    _, roi_one = train_and_evaluate(
        train_feat, valid_feat, test_feat, feature_cols_one, "V16-OneNew",
        max_depth=2, num_leaves=4, min_child_samples=300,
        reg_alpha=10.0, reg_lambda=20.0, subsample=0.3
    )
    
    # ==========================================================
    # 結果サマリー
    # ==========================================================
    print("\n" + "="*80)
    print("結果サマリー")
    print("="*80)
    
    results = [
        ("V16-Original（弱正則化）", roi_orig),
        ("V16-StrongReg（強正則化）", roi_strong),
        ("V16-NoNew（新特徴量なし）", roi_no_new),
        ("V16-OneNew（time_dev_avgのみ）", roi_one),
    ]
    
    print("\n   モデル                          | Test ROI | Gap    | 的中率")
    print("   " + "-"*65)
    for name, roi in results:
        print(f"   {name:30s} | {roi['roi']:6.1f}% | {roi['gap']:5.1f}% | {roi['hit_rate']*100:5.1f}%")
    
    # 推奨
    best = min(results, key=lambda x: x[1]['gap'])  # 最もGapが小さい
    best_roi = max(results, key=lambda x: x[1]['roi'])  # 最もROIが高い
    
    print(f"\n   最小Gap: {best[0]} (Gap {best[1]['gap']:.1f}%)")
    print(f"   最高ROI: {best_roi[0]} (ROI {best_roi[1]['roi']:.1f}%)")
    
    # 過学習が許容範囲内（Gap < 30%）かつROIが最も高いモデルを推奨
    acceptable = [r for r in results if r[1]['gap'] < 30]
    if acceptable:
        recommended = max(acceptable, key=lambda x: x[1]['roi'])
        print(f"\n   ✅ 推奨: {recommended[0]} (ROI {recommended[1]['roi']:.1f}%, Gap {recommended[1]['gap']:.1f}%)")
    else:
        print("\n   ⚠️ 全てのモデルでGap > 30%のため、更なる調整が必要です")
    
    print("\n   完了")


def train_and_evaluate(train_feat, valid_feat, test_feat, feature_cols, name,
                       max_depth=3, num_leaves=20, min_child_samples=100,
                       reg_alpha=3.0, reg_lambda=5.0, subsample=0.5):
    """モデル学習と評価"""
    print(f"\n   {name}モデル学習中...")
    print(f"     パラメータ: depth={max_depth}, leaves={num_leaves}, min_samples={min_child_samples}")
    
    # ソート
    train_sorted = train_feat.sort_values('race_id')
    valid_sorted = valid_feat.sort_values('race_id')
    test_sorted = test_feat.sort_values('race_id')
    
    # 特徴量準備
    available_cols = [c for c in feature_cols if c in train_sorted.columns]
    
    X_train = train_sorted[available_cols].fillna(0)
    y_train = train_sorted['finish_position']
    groups_train = train_sorted.groupby('race_id').size().values
    
    X_valid = valid_sorted[available_cols].fillna(0)
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
    
    # モデル
    model = lgb.LGBMRanker(
        objective='lambdarank',
        n_estimators=3000,
        learning_rate=0.005,
        max_depth=max_depth,
        num_leaves=num_leaves,
        min_child_samples=min_child_samples,
        reg_alpha=reg_alpha,
        reg_lambda=reg_lambda,
        subsample=subsample,
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
    train_sorted = train_sorted.copy()
    train_sorted['score'] = model.predict(X_train)
    train_sorted['rank'] = train_sorted.groupby('race_id')['score'].rank(ascending=False)
    train_top1 = train_sorted[train_sorted['rank'] == 1]
    train_hits = train_top1[train_top1['finish_position'] == 1]
    train_roi = train_hits['win_odds'].sum() / len(train_top1) * 100
    train_hit_rate = len(train_hits) / len(train_top1)
    
    # Test評価
    X_test = test_sorted[available_cols].fillna(0)
    test_sorted = test_sorted.copy()
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
    
    return model, {'roi': test_roi, 'hit_rate': test_hit_rate, 'gap': gap}


if __name__ == "__main__":
    main()
