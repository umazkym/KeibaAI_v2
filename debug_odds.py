import pandas as pd
from pathlib import Path

# horses_performance.parquet の確認
perf_path = Path('keibaai/data/parsed/parquet/horses_performance/horses_performance.parquet')

print(f"File exists: {perf_path.exists()}")

if perf_path.exists():
    df = pd.read_parquet(perf_path)
    print(f"\nTotal rows: {len(df):,}")
    print(f"Columns ({len(df.columns)}): {list(df.columns)[:20]}")
    
    if 'win_odds' in df.columns:
        print(f"\n✓ win_odds column exists")
        print(f"  Non-null count: {df['win_odds'].notna().sum():,} / {len(df):,}")
        print(f"  Null count: {df['win_odds'].isna().sum():,}")
        print(f"  Sample values: {df['win_odds'].dropna().head(10).tolist()}")
    else:
        print(f"\n❌ win_odds column NOT found")
        print(f"Available columns: {list(df.columns)}")
else:
    print("❌ File not found")
