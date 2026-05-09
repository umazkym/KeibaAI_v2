import pandas as pd

# races.parquetの構造確認
races_df = pd.read_parquet('keibaai/data/parsed/parquet/races/races.parquet')
print("races.parquet:")
print(f"  Total rows: {len(races_df):,}")
print(f"  Columns: {list(races_df.columns)}")
print(f"\nSample data:")
print(races_df.head(2))

# win_oddsがあるか確認
if 'win_odds' in races_df.columns:
    print(f"\nwin_odds存在: {races_df['win_odds'].notna().sum():,} 件")
    print(f"Sample win_odds: {races_df['win_odds'].dropna().head(5).tolist()}")
else:
    print("\n⚠️ win_oddsカラムなし")

print("\n" + "=" * 80)

# shutuba.parquetにwin_oddsがあるか再確認
shutuba_df = pd.read_parquet('keibaai/data/parsed/parquet/shutuba/shutuba.parquet')
print("shutuba.parquet:")
print(f"  Total rows: {len(shutuba_df):,}")

if 'win_odds' in shutuba_df.columns:
    print(f"\nwin_odds存在: {shutuba_df['win_odds'].notna().sum():,} 件")
    print(f"Sample win_odds: {shutuba_df['win_odds'].dropna().head(5).tolist()}")
else:
    print("\n⚠️ win_oddsカラムなし")

# 他のオッズ関連カラム
odds_cols = [c for c in shutuba_df.columns if 'odds' in c.lower()]
print(f"\nOdds関連カラム: {odds_cols}")
for col in odds_cols:
    non_null = shutuba_df[col].notna().sum()
    if non_null > 0:
        print(f"  {col}: {non_null:,} 件 ({non_null/len(shutuba_df)*100:.1f}%)")
        print(f"    Sample: {shutuba_df[col].dropna().head(3).tolist()}")
