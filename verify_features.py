import pandas as pd
from pathlib import Path
import glob

# 2024年のparquetファイルを探す
pattern = 'keibaai/data/features/parquet/year=2024/**/*.parquet'
files = glob.glob(pattern, recursive=True)

if not files:
    print("2024年のparquetファイルが見つかりません")
    # 全てのparquetファイルを探す
    pattern = 'keibaai/data/features/parquet/**/*.parquet'
    files = glob.glob(pattern, recursive=True)
    if files:
        print(f"\n見つかったparquetファイル: {len(files)}個")
        print(f"最初のファイル: {files[0]}")
    else:
        print("parquetファイルが1つも見つかりません")
        exit(1)

parquet_file = files[0]
print(f"\nLoading {parquet_file}...")
df = pd.read_parquet(parquet_file)

print(f"Loaded: {len(df)} rows, {len(df.columns)} columns")

# ゼロでないカラムを数える
numeric_cols = df.select_dtypes(include=['number']).columns
non_zero_count = 0
zero_count = 0

for col in numeric_cols:
    if df[col].abs().sum() > 0:
        non_zero_count += 1
    else:
        zero_count += 1

print(f"\nNumeric columns: {len(numeric_cols)}")
print(f"Non-zero features: {non_zero_count}")
print(f"All-zero features: {zero_count}")

# 重要な特徴量
important = ['age', 'career_starts', 'career_wins', 'prize_total',
             'past_3_finish_position_mean', 'past_10_finish_position_mean',
             'jockey_win_rate', 'trainer_win_rate']

print("\n重要な特徴量:")
for feat in important:
    if feat in df.columns:
        non_zero = (df[feat] != 0).sum()
        mean_val = df[feat].mean()
        print(f"  {feat}: Non-zero={non_zero}/{len(df)}, Mean={mean_val:.3f}")
    else:
        print(f"  {feat}: MISSING")

# prize_money確認  
if 'prize_money' in df.columns:
    non_zero_prize = (df['prize_money'] > 0).sum()
    print(f"\nprize_money: Non-zero={non_zero_prize}/{len(df)}, Mean={df['prize_money'].mean():.0f}")
