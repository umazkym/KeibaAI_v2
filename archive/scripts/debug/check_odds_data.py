import pandas as pd

# shutuba.parquetのオッズ関連カラムを確認
df = pd.read_parquet('keibaai/data/parsed/parquet/shutuba/shutuba.parquet')
print(f"Total columns: {len(df.columns)}")
print(f"Total rows: {len(df):,}")

odds_cols = [c for c in df.columns if 'odds' in c.lower() or 'popularity' in c.lower() or 'prize' in c.lower()]
print(f"\nOdds-related columns: {odds_cols}")

for col in odds_cols:
    non_null = df[col].notna().sum()
    print(f"  {col}: non-null count = {non_null:,} ({non_null/len(df)*100:.1f}%)")
    if non_null > 0:
        print(f"    Sample values: {df[col].dropna().head(5).tolist()}")
