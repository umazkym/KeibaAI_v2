"""
ROI計算バグの詳細調査

【仮説】
test_v8_features.pyのROI計算で、horse_numberの型不一致により
重複マージが発生している可能性
"""

import sys
from pathlib import Path
import pandas as pd
import numpy as np
import lightgbm as lgb

project_root = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(project_root))

def main():
    data_dir = Path("keibaai/data/parsed/parquet")
    
    print("="*70)
    print("ROI計算バグの詳細調査")
    print("="*70)
    
    # データ読み込み
    races_df = pd.read_parquet(data_dir / "races/races.parquet")
    races_df['race_date'] = pd.to_datetime(races_df['race_date'])
    races_df = races_df[(races_df['race_date'] >= '2020-01-01')]
    races_df = races_df.dropna(subset=['finish_position', 'win_odds'])
    
    returns_df = pd.read_parquet(data_dir / "returns/returns.parquet")
    pedigrees_df = pd.read_parquet(data_dir / "pedigrees/pedigrees.parquet")
    corners_df = pd.read_parquet(data_dir / "corners/corner_positions.parquet")
    race_details_df = pd.read_parquet(data_dir / "race_details/race_details.parquet")
    
    # 時系列分割
    train_df = races_df[races_df['race_date'] < '2024-07-01'].copy()
    test_df = races_df[races_df['race_date'] >= '2025-01-01'].copy()
    
    print(f"Train: {len(train_df):,}行")
    print(f"Test: {len(test_df):,}行")
    
    # V7特徴量エンジニア
    from keibaai.src.features.leak_free_feature_engineer_v7 import LeakFreeFeatureEngineerV7
    
    fe = LeakFreeFeatureEngineerV7()
    fe.fit(races_df=train_df, pedigrees_df=pedigrees_df, corners_df=corners_df, 
           race_details_df=race_details_df, returns_df=returns_df)
    
    test_features = fe.transform(test_df)
    feature_cols = [c for c in fe.get_feature_columns() if c in test_features.columns]
    
    print(f"\n特徴量数: {len(feature_cols)}")
    print(f"Test特徴量: {len(test_features)}行")
    
    # 簡易モデル学習
    train_features = fe.transform(train_df)
    train_sorted = train_features.sort_values('race_id')
    X_train = train_sorted[feature_cols].fillna(0)
    y_train = train_sorted['finish_position']
    groups = train_sorted.groupby('race_id').size().values
    
    y_rel = np.zeros(len(y_train))
    y_rel[y_train.values == 1] = 5
    y_rel[y_train.values == 2] = 4
    y_rel[y_train.values == 3] = 3
    
    model = lgb.LGBMRanker(
        objective='lambdarank', n_estimators=100, learning_rate=0.05,
        max_depth=3, num_leaves=8, min_child_samples=100,
        random_state=42, verbose=-1
    )
    model.fit(X_train, y_rel, group=groups)
    
    # Test予測
    test_features['score'] = model.predict(test_features[feature_cols].fillna(0))
    test_features['rank'] = test_features.groupby('race_id')['score'].rank(ascending=False, method='first')
    
    # Top1を取得
    top1 = test_features[test_features['rank'] == 1].copy()
    print(f"\nTop1: {len(top1)}行")
    
    # horse_number型を確認
    print(f"Top1 horse_number型: {top1['horse_number'].dtype}")
    print(f"Top1 race_id型: {top1['race_id'].dtype}")
    
    # 単勝データ
    tansho = returns_df[returns_df['bet_type'] == 'tansho'][['race_id', 'horse_number', 'payout']].copy()
    print(f"\n単勝データ horse_number型: {tansho['horse_number'].dtype}")
    print(f"単勝データ race_id型: {tansho['race_id'].dtype}")
    
    # 型を統一してマージ
    top1['race_id_str'] = top1['race_id'].astype(str)
    top1['horse_number_int'] = pd.to_numeric(top1['horse_number'], errors='coerce')
    
    tansho['race_id_str'] = tansho['race_id'].astype(str)
    tansho['horse_number_int'] = pd.to_numeric(tansho['horse_number'], errors='coerce')
    
    print(f"\n型変換後:")
    print(f"  Top1 horse_number_int サンプル: {top1['horse_number_int'].head().tolist()}")
    print(f"  単勝 horse_number_int サンプル: {tansho['horse_number_int'].head().tolist()}")
    
    # マージ
    merged = top1.merge(
        tansho[['race_id_str', 'horse_number_int', 'payout']],
        on=['race_id_str', 'horse_number_int'],
        how='left'
    )
    print(f"\nマージ結果: {len(merged)}行（元: {len(top1)}行）")
    
    # 重複確認
    dup = merged.duplicated(subset=['race_id_str', 'horse_number_int']).sum()
    print(f"重複: {dup}行")
    
    # ROI計算
    total_bet = len(top1) * 100
    total_payout = merged['payout'].fillna(0).sum()
    roi = total_payout / total_bet * 100
    
    hits = (merged['payout'] > 0).sum()
    hit_rate = hits / len(top1) * 100
    
    print(f"\n=== 正しいROI計算 ===")
    print(f"  ベット数: {len(top1)}")
    print(f"  的中数: {hits}")
    print(f"  的中率: {hit_rate:.1f}%")
    print(f"  払戻合計: ¥{total_payout:,.0f}")
    print(f"  投資合計: ¥{total_bet:,}")
    print(f"  ROI: {roi:.1f}%")
    
    # オリジナルの計算方法での確認
    print("\n=== オリジナル計算方法 ===")
    tansho_orig = returns_df[returns_df['bet_type'] == 'tansho'][['race_id', 'horse_number', 'payout']].copy()
    tansho_orig.columns = ['race_id', 'horse_number', 'tansho_payout']
    tansho_orig['race_id'] = tansho_orig['race_id'].astype(str)
    tansho_orig['horse_number'] = pd.to_numeric(tansho_orig['horse_number'], errors='coerce')
    
    top1_orig = top1.copy()
    top1_orig['race_id'] = top1_orig['race_id'].astype(str)
    top1_orig['horse_number'] = pd.to_numeric(top1_orig['horse_number'], errors='coerce')
    
    merged_orig = top1_orig.merge(tansho_orig, on=['race_id', 'horse_number'], how='left')
    print(f"  マージ結果: {len(merged_orig)}行（元: {len(top1)}行）")
    
    total_bet_orig = len(merged_orig) * 100  # ここが問題！
    total_payout_orig = merged_orig['tansho_payout'].fillna(0).sum()
    roi_orig = total_payout_orig / total_bet_orig * 100
    
    print(f"  ベット数: {len(merged_orig)}")
    print(f"  ROI: {roi_orig:.1f}%")
    
    # 問題の特定
    if len(merged_orig) != len(top1):
        print(f"\n⚠ 問題発見: マージで行数が変化")
        print(f"  元の行数: {len(top1)}")
        print(f"  マージ後: {len(merged_orig)}")
        print(f"  増加: {len(merged_orig) - len(top1)}")


if __name__ == '__main__':
    main()
