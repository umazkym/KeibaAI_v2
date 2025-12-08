"""
V7リーク精密テスト

【目的】
finish_positionを使わずに予測した場合のROIを確認。
もしリークがあれば、finish_positionを隠した場合にROIが下がるはず。
"""

import sys
from pathlib import Path
import pandas as pd
import numpy as np
import lightgbm as lgb

project_root = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(project_root))

def calculate_roi(features_df, returns_df):
    """ROI計算"""
    tansho = returns_df[returns_df['bet_type'] == 'tansho'][['race_id', 'horse_number', 'payout']].copy()
    tansho['race_id'] = tansho['race_id'].astype(str)
    tansho['horse_number'] = pd.to_numeric(tansho['horse_number'], errors='coerce')
    
    top1 = features_df[features_df['rank'] == 1].copy()
    top1['race_id'] = top1['race_id'].astype(str)
    top1['horse_number'] = pd.to_numeric(top1['horse_number'], errors='coerce')
    
    merged = top1.merge(tansho, on=['race_id', 'horse_number'], how='left')
    
    total_bet = len(top1) * 100
    total_payout = merged['payout'].fillna(0).sum()
    roi = total_payout / total_bet * 100 if total_bet > 0 else 0
    
    hits = (merged['payout'] > 0).sum()
    hit_rate = hits / len(top1) * 100 if len(top1) > 0 else 0
    
    return roi, hit_rate, len(top1)


def main():
    data_dir = Path("keibaai/data/parsed/parquet")
    
    print("="*70)
    print("V7リーク精密テスト")
    print("="*70)
    
    # データ読み込み
    races_df = pd.read_parquet(data_dir / "races/races.parquet")
    races_df['race_date'] = pd.to_datetime(races_df['race_date'])
    races_df = races_df[(races_df['race_date'] >= '2020-01-01')]
    races_df = races_df.dropna(subset=['finish_position', 'win_odds'])
    
    pedigrees_df = pd.read_parquet(data_dir / "pedigrees/pedigrees.parquet")
    corners_df = pd.read_parquet(data_dir / "corners/corner_positions.parquet")
    race_details_df = pd.read_parquet(data_dir / "race_details/race_details.parquet")
    returns_df = pd.read_parquet(data_dir / "returns/returns.parquet")
    
    # 時系列分割
    train_df = races_df[races_df['race_date'] < '2024-07-01'].copy()
    valid_df = races_df[(races_df['race_date'] >= '2024-07-01') & 
                        (races_df['race_date'] < '2025-01-01')].copy()
    test_df = races_df[races_df['race_date'] >= '2025-01-01'].copy()
    
    print(f"Train: {len(train_df):,}行")
    print(f"Valid: {len(valid_df):,}行")
    print(f"Test: {len(test_df):,}行")
    
    from keibaai.src.features.leak_free_feature_engineer_v7 import LeakFreeFeatureEngineerV7
    
    # =========================================================================
    # テスト1: 通常のV7（現行）
    # =========================================================================
    print("\n" + "="*70)
    print("[テスト1] 通常のV7（現行）")
    print("="*70)
    
    fe = LeakFreeFeatureEngineerV7()
    fe.fit(races_df=train_df, pedigrees_df=pedigrees_df, corners_df=corners_df,
           race_details_df=race_details_df, returns_df=returns_df)
    
    train_features = fe.transform(train_df)
    valid_features = fe.transform(valid_df)
    test_features = fe.transform(test_df)
    
    feature_cols = [c for c in fe.get_feature_columns() if c in train_features.columns]
    print(f"特徴量数: {len(feature_cols)}")
    
    # モデル学習
    train_sorted = train_features.sort_values('race_id')
    X_train = train_sorted[feature_cols].fillna(0)
    y_train = train_sorted['finish_position']
    groups = train_sorted.groupby('race_id').size().values
    
    y_rel = np.zeros(len(y_train))
    y_rel[y_train.values == 1] = 5
    y_rel[y_train.values == 2] = 4
    y_rel[y_train.values == 3] = 3
    
    model = lgb.LGBMRanker(
        objective='lambdarank', n_estimators=500, learning_rate=0.01,
        max_depth=3, num_leaves=8, min_child_samples=100,
        reg_alpha=2.0, reg_lambda=3.0, subsample=0.6, colsample_bytree=0.6,
        random_state=42, verbose=-1
    )
    model.fit(X_train, y_rel, group=groups)
    
    # 予測
    for df, name in [(train_features, 'Train'), (valid_features, 'Valid'), (test_features, 'Test')]:
        df['score'] = model.predict(df[feature_cols].fillna(0))
        df['rank'] = df.groupby('race_id')['score'].rank(ascending=False, method='first')
    
    train_roi, train_hr, train_n = calculate_roi(train_features, returns_df)
    valid_roi, valid_hr, valid_n = calculate_roi(valid_features, returns_df)
    test_roi, test_hr, test_n = calculate_roi(test_features, returns_df)
    
    print(f"\n結果:")
    print(f"  Train: ROI={train_roi:.1f}%, 的中率={train_hr:.1f}%")
    print(f"  Valid: ROI={valid_roi:.1f}%, 的中率={valid_hr:.1f}%")
    print(f"  Test:  ROI={test_roi:.1f}%, 的中率={test_hr:.1f}%")
    
    # =========================================================================
    # テスト2: Test期間のfinish_positionを隠したV7
    # =========================================================================
    print("\n" + "="*70)
    print("[テスト2] Test期間のfinish_positionを隠したV7")
    print("="*70)
    
    # Test期間のfinish_positionを一時的にNaNにして transform
    test_df_hidden = test_df.copy()
    test_df_hidden['finish_position_orig'] = test_df_hidden['finish_position']
    test_df_hidden['finish_position'] = np.nan
    
    # 新しいfeatureエンジンで変換
    fe2 = LeakFreeFeatureEngineerV7()
    fe2.fit(races_df=train_df, pedigrees_df=pedigrees_df, corners_df=corners_df,
            race_details_df=race_details_df, returns_df=returns_df)
    
    test_features_hidden = fe2.transform(test_df_hidden)
    
    # 元のfinish_positionを復元（ROI計算用）
    test_features_hidden['finish_position'] = test_df_hidden['finish_position_orig'].values
    
    # 予測
    test_features_hidden['score'] = model.predict(test_features_hidden[feature_cols].fillna(0))
    test_features_hidden['rank'] = test_features_hidden.groupby('race_id')['score'].rank(ascending=False, method='first')
    
    test_roi_hidden, test_hr_hidden, test_n_hidden = calculate_roi(test_features_hidden, returns_df)
    
    print(f"\n結果:")
    print(f"  Test (隠し): ROI={test_roi_hidden:.1f}%, 的中率={test_hr_hidden:.1f}%")
    
    # =========================================================================
    # 比較
    # =========================================================================
    print("\n" + "="*70)
    print("[比較]")
    print("="*70)
    
    roi_diff = test_roi - test_roi_hidden
    hr_diff = test_hr - test_hr_hidden
    
    print(f"  通常Test: ROI={test_roi:.1f}%, 的中率={test_hr:.1f}%")
    print(f"  隠しTest: ROI={test_roi_hidden:.1f}%, 的中率={test_hr_hidden:.1f}%")
    print(f"  差分: ROI={roi_diff:+.1f}%, 的中率={hr_diff:+.1f}%")
    
    if roi_diff > 20:
        print("\n  ⚠ 重大なリーク発見！finish_positionがリークしています")
    elif roi_diff > 5:
        print("\n  ⚠ 軽度のリーク可能性あり")
    else:
        print("\n  ✓ finish_positionのリークはありません")
        print("  → 高ROIの原因は別にあります（モデルの過学習 or 期間特性）")


if __name__ == '__main__':
    main()
