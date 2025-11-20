import pandas as pd
from pathlib import Path

# horses_performance.parquetのカラムを確認
perf_path = Path('keibaai/data/parsed/parquet/horses_performance/horses_performance.parquet')

print("=== horses_performance.parquet カラム確認 ===")
df = pd.read_parquet(perf_path)

print(f"総行数: {len(df):,}")
print(f"\n全カラム ({len(df.columns)}個):")
for i, col in enumerate(df.columns, 1):
    print(f"  {i:2d}. {col}")

# win_oddsが存在するか確認
if 'win_odds' in df.columns:
    print("\n✅ win_oddsカラムが存在します")
    print(f"   非null数: {df['win_odds'].notna().sum():,}/{len(df):,}")
else:
    print("\n❌ win_oddsカラムが存在しません")
    print("\nオッズ関連のカラムを探します:")
    odds_cols = [col for col in df.columns if 'odds' in col.lower() or 'オッズ' in col]
    if odds_cols:
        print(f"見つかったオッズ関連カラム: {odds_cols}")
    else:
        print("オッズ関連のカラムが見つかりませんでした")

# race_id, horse_idの存在確認
print(f"\n✅ race_idカラム: {'存在' if 'race_id' in df.columns else '存在しない'}")
print(f"✅ horse_idカラム: {'存在' if 'horse_id' in df.columns else '存在しない'}")

# サンプルデータを表示
print("\n最初の3行:")
print(df.head(3))