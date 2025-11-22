import pandas as pd
from pathlib import Path

# races.parquetの列を確認
races_path = Path('keibaai/data/parsed/parquet/races/races.parquet')
print(f"Loading {races_path}...")
races_df = pd.read_parquet(races_path)

print(f"Loaded: {len(races_df):,} rows, {len(races_df.columns)} columns")

# prize関連の列を探す
prize_cols = [col for col in races_df.columns if 'prize' in col.lower()]
print(f"\nPrize関連の列: {prize_cols}")

# サンプルデータ
if prize_cols:
    sample = races_df[races_df['finish_position'] <= 5][['finish_position'] + prize_cols].head(10)
    print("\nサンプルデータ:")
    print(sample)
    
    # 非ゼロ件数
    for col in prize_cols:
        non_zero = (races_df[col] > 0).sum() if col in races_df.columns else 0
        print(f"  {col}: {non_zero:,}/{len(races_df)} non-zero")
