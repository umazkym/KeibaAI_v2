import pandas as pd
from pathlib import Path

# 2024/01のparquetファイルを直接読み込み
parquet_file = Path('keibaai/data/features/parquet/year=2024/month=1').glob('*.parquet')
parquet_file = list(parquet_file)[0]

print(f"Loading {parquet_file}...")
df = pd.read_parquet(parquet_file)

print(f"Loaded: {len(df)} rows, {len(df.columns)} columns")
print(f"\nColumns sample: {list(df.columns)[:20]}")

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

# いくつかの重要な特徴量の統計
important_features = ['age', 'horse_weight_kg', 'career_starts', 'career_wins', 
                     'past_3_finish_position_mean', 'past_10_finish_time_seconds_mean',
                     'jockey_id_win_rate', 'trainer_id_win_rate']
print("\n重要な特徴量:")
for feat in important_features:
    if feat in df.columns:
        non_null = df[feat].notna().sum()
        non_zero = (df[feat] != 0).sum()
        mean_val = df[feat].mean() if non_null > 0 else 0
        print(f"  {feat}: Non-zero={non_zero}/{len(df)}, Mean={mean_val:.3f}")
    else:
        print(f"  {feat}: MISSING")

# ゼロのカラム例
if zero_count > 0:
    zero_cols = [col for col in numeric_cols if df[col].abs().sum() == 0]
    print(f"\nAll-zero columns (first 20): {zero_cols[:20]}")

# 結果をファイルに保存
with open('feature_check_results.txt', 'w', encoding='utf-8') as f:
    f.write(f"Loaded: {len(df)} rows, {len(df.columns)} columns\n\n")
    f.write(f"Numeric columns: {len(numeric_cols)}\n")
    f.write(f"Non-zero features: {non_zero_count}\n")
    f.write(f"All-zero features: {zero_count}\n\n")
    
    f.write("All columns:\n")
    for i, col in enumerate(df.columns, 1):
        is_zero = "ZERO" if col in numeric_cols and df[col].abs().sum() == 0 else ""
        f.write(f"  {i}. {col} {is_zero}\n")
    
    f.write("\n\nImportant features:\n")
    for feat in important_features:
        if feat in df.columns:
            non_null = df[feat].notna().sum()
            non_zero = (df[feat] != 0).sum()
            mean_val = df[feat].mean() if non_null > 0 else 0
            f.write(f"  {feat}: Non-zero={non_zero}/{len(df)}, Mean={mean_val:.3f}\n")
        else:
            f.write(f"  {feat}: MISSING\n")

print("\n結果を feature_check_results.txt に保存しました")
