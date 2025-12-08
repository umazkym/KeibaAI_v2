"""
V9特徴量データ検証スクリプト
新特徴量が正しく計算されているか確認
"""

import sys
from pathlib import Path
import pandas as pd
import numpy as np
import logging

project_root = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(project_root))

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def main():
    data_dir = Path("keibaai/data/parsed/parquet")
    
    print("="*70)
    print("V9特徴量データ検証")
    print("="*70)
    
    # データ読み込み
    races_df = pd.read_parquet(data_dir / "races/races.parquet")
    races_df['race_date'] = pd.to_datetime(races_df['race_date'])
    races_df = races_df[(races_df['race_date'] >= '2020-01-01')]
    races_df = races_df.dropna(subset=['finish_position', 'win_odds'])
    
    pedigrees_df = pd.read_parquet(data_dir / "pedigrees/pedigrees.parquet")
    corners_df = pd.read_parquet(data_dir / "corners/corner_positions.parquet")
    race_details_df = pd.read_parquet(data_dir / "race_details/race_details.parquet")
    
    # 分割
    train_df = races_df[races_df['race_date'] < '2024-07-01'].copy()
    test_df = races_df[races_df['race_date'] >= '2025-01-01'].copy()
    
    print(f"\nTrain: {len(train_df):,}件")
    print(f"Test: {len(test_df):,}件")
    
    # V9をインポートしてfit/transform
    from keibaai.src.features.leak_free_feature_engineer_v9 import LeakFreeFeatureEngineerV9
    
    fe = LeakFreeFeatureEngineerV9()
    fe.fit(
        races_df=train_df,
        pedigrees_df=pedigrees_df,
        corners_df=corners_df,
        race_details_df=race_details_df
    )
    
    print("\n" + "="*70)
    print("fit結果の確認")
    print("="*70)
    
    # 母父統計
    print(f"\n[母父統計]")
    if fe._damsire_mapping is not None:
        print(f"  damsire_mapping: {len(fe._damsire_mapping)}頭")
    else:
        print("  damsire_mapping: None")
        
    if fe._damsire_stats is not None:
        print(f"  damsire_stats: {len(fe._damsire_stats)}件")
        print(f"  非NaN件数: {fe._damsire_stats['damsire_win_rate'].notna().sum()}件")
        print(f"  統計サンプル:")
        print(fe._damsire_stats.head())
    else:
        print("  damsire_stats: None")
    
    # ペース統計
    print(f"\n[ペース統計]")
    if fe._pace_stats is not None:
        print(f"  pace_stats: {len(fe._pace_stats)}件")
        print(f"  非NaN件数: {fe._pace_stats['venue_distance_pace_avg'].notna().sum()}件")
    else:
        print("  pace_stats: None")
    
    # 逃げ馬有利度
    print(f"\n[逃げ馬有利度統計]")
    if fe._front_runner_stats is not None:
        print(f"  front_runner_stats: {len(fe._front_runner_stats)}件")
        print(f"  非NaN件数: {fe._front_runner_stats['front_runner_advantage'].notna().sum()}件")
    else:
        print("  front_runner_stats: None")
    
    # transform結果の確認
    print("\n" + "="*70)
    print("transform結果の確認（Testデータの最初の1000件）")
    print("="*70)
    
    test_sample = test_df.head(1000).copy()
    result = fe.transform(test_sample)
    
    # V9新特徴量のカラム
    v9_new_cols = [
        'damsire_win_rate', 'damsire_avg_finish',
        'venue_distance_pace_avg', 'front_runner_advantage',
        'horse_position_trend', 'horse_early_speed',
        'weight_change_from_last', 'weight_trend_3'
    ]
    
    print(f"\n新特徴量の存在確認:")
    for col in v9_new_cols:
        if col in result.columns:
            non_null = result[col].notna().sum()
            print(f"  {col}: {non_null}/{len(result)} ({non_null/len(result)*100:.1f}%非NaN)")
        else:
            print(f"  {col}: カラムなし ❌")
    
    # サンプルデータ表示
    print(f"\n新特徴量のサンプル（最初の5件）:")
    existing_cols = [c for c in v9_new_cols if c in result.columns]
    if existing_cols:
        print(result[['race_id', 'horse_number'] + existing_cols].head())
    
    # 統計サマリー
    print(f"\n新特徴量の統計:")
    for col in existing_cols:
        print(f"  {col}: mean={result[col].mean():.4f}, std={result[col].std():.4f}, NaN={result[col].isna().sum()}")


if __name__ == '__main__':
    main()
