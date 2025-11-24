import pandas as pd
from pathlib import Path

# 1. shutuba.parquet の確認
shutuba_path = Path('keibaai/data/parsed/parquet/shutuba/shutuba.parquet')
if shutuba_path.exists():
    print("=" * 80)
    print("SHUTUBA.PARQUET - ALL COLUMNS")
    print("=" * 80)
    shutuba_df = pd.read_parquet(shutuba_path)
    print(f"Total rows: {len(shutuba_df):,}")
    print(f"Total columns: {len(shutuba_df.columns)}")
    print("\nAll columns:")
    for i, col in enumerate(shutuba_df.columns, 1):
        print(f"{i:3d}. {col}")
    
    # レース結果関連と思われるカラムを抽出
    print("\n" + "=" * 80)
    print("POTENTIAL LEAKAGE COLUMNS (Result-related)")
    print("=" * 80)
    result_keywords = ['finish', 'passing', 'last_3f', 'corner', 'pace', 'time', 
                       'position_change', 'deviation', 'popularity']
    leak_cols = [c for c in shutuba_df.columns if any(kw in c.lower() for kw in result_keywords)]
    for col in leak_cols:
        sample_vals = shutuba_df[col].dropna().head(3).tolist()
        print(f"  {col}: {sample_vals}")

# 2. horses_performance_fixed.parquet の確認
perf_path = Path('horses_performance_fixed.parquet')
if perf_path.exists():
    print("\n" + "=" * 80)
    print("HORSES_PERFORMANCE_FIXED.PARQUET - ALL COLUMNS")
    print("=" * 80)
    perf_df = pd.read_parquet(perf_path)
    print(f"Total rows: {len(perf_df):,}")
    print(f"Total columns: {len(perf_df.columns)}")
    print("\nAll columns:")
    for i, col in enumerate(perf_df.columns, 1):
        print(f"{i:3d}. {col}")

# 3. train_data_mu_v2.parquet の確認
train_path = Path('keibaai/models/mu_v2/train_data_mu_v2.parquet')
if train_path.exists():
    print("\n" + "=" * 80)
    print("TRAIN_DATA_MU_V2.PARQUET - LEAK COLUMNS CHECK")
    print("=" * 80)
    train_df = pd.read_parquet(train_path)
    print(f"Total rows: {len(train_df):,}")
    print(f"Total columns: {len(train_df.columns)}")
    
    # リーク可能性のあるカラムをチェック
    leak_cols_in_train = [c for c in train_df.columns if any(kw in c.lower() for kw in result_keywords)]
    print(f"\nLeakage columns found: {len(leak_cols_in_train)}")
    for col in leak_cols_in_train:
        print(f"  - {col}")

print("\n" + "=" * 80)
print("RECOMMENDATION: EXCLUDE LIST")
print("=" * 80)
print("Add these columns to exclude_cols in train_mu_v2_quick.py:")
if shutuba_path.exists():
    for col in leak_cols:
        print(f"    '{col}',")
