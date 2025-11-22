import pandas as pd
from pathlib import Path
import os

# 特徴量parquetを直接読み込み - ディレクトリ構造を確認
features_dir = Path('keibaai/data/features/parquet')
print(f"Checking {features_dir}...")

# year=2024/month=1 などのサブディレクトリを探す
for root, dirs, files in os.walk(features_dir):
    if files:
        for file in files[:5]:  # 最初の5ファイル
            file_path = Path(root) / file
            if file_path.suffix == '.parquet':
                print(f"\nFound parquet: {file_path}")
                try:
                    df = pd.read_parquet(file_path)
                    print(f"  Loaded: {len(df)} rows, {len(df.columns)} columns")
                    print(f"  Columns sample: {list(df.columns)[:10]}")
                    
                    # ゼロでないカラムを数える
                    numeric_cols = df.select_dtypes(include=['number']).columns
                    non_zero_count = sum((df[col].abs().sum() > 0) for col in numeric_cols)
                    zero_count = len(numeric_cols) - non_zero_count
                    print(f"  Non-zero features: {non_zero_count}, All-zero: {zero_count}")
                    
                    # いくつかの重要な特徴量の統計
                    important_features = ['age', 'horse_weight_kg', 'career_starts', 'career_wins', 
                                         'past_3_finish_position_mean', 'past_10_finish_time_seconds_mean']
                    print("\n  重要な特徴量:")
                    for feat in important_features:
                        if feat in df.columns:
                            non_null = df[feat].notna().sum()
                            non_zero = (df[feat] != 0).sum()
                            mean_val = df[feat].mean() if non_null > 0 else 0
                            print(f"    {feat}: Non-zero={non_zero}/{len(df)}, Mean={mean_val:.2f}")
                    
                except Exception as e:
                    print(f"  Error loading: {e}")
            elif file.endswith('.yaml'):
                print(f"\nFound yaml: {file_path}")
        break  # 最初のディレクトリのみ確認
