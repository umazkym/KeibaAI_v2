import pandas as pd
from pathlib import Path
import os

# 特徴量parquetを直接読み込み - ディレクトリ構造を確認
features_dir = Path('keibaai/data/features/parquet')
print(f"Checking {features_dir}...")

# year=2024/month=1 などのサブディレクトリを探す
for root, dirs, files in os.walk(features_dir):
    if files:
        for file in files[:3]:  # 最初の3ファイルのみ
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
                    
                    break  # 1つ確認できたら終了
                except Exception as e:
                    print(f"  Error: {e}")
        if files:
            break

print(f"\n読み込み完了: {len(df)}行 x {len(df.columns)}列")
print(f"\nカラムサンプル: {list(df.columns)[:20]}")

# ゼロでないカラムを数える
numeric_cols = df.select_dtypes(include=['number']).columns
non_zero_cols = []
zero_cols = []

for col in numeric_cols:
    if df[col].abs().sum() > 0:
        non_zero_cols.append(col)
    else:
        zero_cols.append(col)

print(f"\nゼロでないカラム: {len(non_zero_cols)}")
print(f"全てゼロのカラム: {len(zero_cols)}")

if zero_cols:
    print(f"\n全てゼロのカラム例: {zero_cols[:20]}")

# horse_id のサンプル
if 'horse_id' in df.columns:
    print(f"\nhorse_id サンプル: {df['horse_id'].head().tolist()}")
    print(f"horse_id ユニーク数: {df['horse_id'].nunique()}")

# いくつかの重要な特徴量の統計
important_features = ['age', 'horse_weight_kg', 'career_starts', 'career_wins', 
                     'past_3_finish_position_mean', 'past_10_finish_time_seconds_mean']
print("\n重要な特徴量の統計:")
for feat in important_features:
    if feat in df.columns:
        non_null = df[feat].notna().sum()
        non_zero = (df[feat] != 0).sum()
        print(f"  {feat}: Non-null={non_null}, Non-zero={non_zero}, Mean={df[feat].mean():.2f}")
    else:
        print(f"  {feat}: カラムが存在しません")
