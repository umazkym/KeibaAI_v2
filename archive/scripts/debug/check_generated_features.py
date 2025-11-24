"""
最新の特徴量ファイルを直接確認
"""
import pandas as pd
from pathlib import Path

print("=" * 60)
print("最新の特徴量ファイルを確認")
print("=" * 60)

# 特徴量ディレクトリ
features_dir = Path('keibaai/data/features/parquet')

# 2024年1月のファイルを探す
year_dirs = list(features_dir.glob('year=2024'))
if not year_dirs:
    print("ERROR: 2024年のデータが見つかりません")
    exit(1)

month_dirs = list(year_dirs[0].glob('month=1'))
if not month_dirs:
    print("ERROR: 2024年1月のデータが見つかりません")
    exit(1)

# 最初のparquetファイルを読み込み
parquet_files = list(month_dirs[0].glob('*.parquet'))
if not parquet_files:
    print("ERROR: parquetファイルが見つかりません")
    exit(1)

print(f"\n読み込むファイル: {parquet_files[0].name}")

df = pd.read_parquet(parquet_files[0])

print(f"レコード数: {len(df):,}件")
print(f"列数: {len(df.columns)}")

# 賞金関連の列
prize_cols = [col for col in df.columns if 'prize' in col.lower()]
print(f"\n賞金関連の列: {prize_cols}")

# 各列の統計
for col in prize_cols:
    if pd.api.types.is_numeric_dtype(df[col]):
        non_zero = (df[col] > 0).sum()
        mean_val = df[col].mean() if non_zero > 0 else 0
        max_val = df[col].max() if non_zero > 0 else 0
        print(f"\n{col}:")
        print(f"  非ゼロ: {non_zero}/{len(df)} ({non_zero/len(df)*100:.1f}%)")
        print(f"  平均: {mean_val:.1f}")
        print(f"  最大: {max_val:.1f}")

# prize_total上位5件
if 'prize_total' in df.columns and (df['prize_total'] > 0).any():
    print(f"\nprize_total上位5件:")
    top5 = df.nlargest(5, 'prize_total')[['horse_id', 'prize_total', 'career_starts']]
    print(top5)
else:
    print(f"\nWARNING: prize_totalが全てゼロです")
    
    # デバッグ: prize_moneyを確認
    if 'prize_money' in df.columns:
        pm_non_zero = (df['prize_money'] > 0).sum()
        print(f"\nprize_money: 非ゼロ={pm_non_zero}/{len(df)}")
        
        if pm_non_zero > 0:
            print("ERROR: prize_moneyは存在するのにprize_totalがゼロ！")
            print("→ cumsum().shift()の問題")
        else:
            print("ERROR: prize_money自体がゼロ！")
            print("→ _create_combined_timeseries_dfの問題")

# 全列をリスト表示
print(f"\n全列（{len(df.columns)}個）:")
for i, col in enumerate(df.columns, 1):
    print(f"  {i:3d}. {col}")
