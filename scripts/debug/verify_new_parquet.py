"""verify new parquet columns"""
import pandas as pd
from pathlib import Path

parquet_path = Path(r"c:\Users\zk-ht\Keiba\Keiba_AI_v2\keibaai\data\parsed\parquet\races\races.parquet")
df = pd.read_parquet(parquet_path)

print(f"総レコード数: {len(df):,}")
print(f"カラム一覧: {list(df.columns)}")
print()

# 新カラムの確認
if 'course_direction' in df.columns:
    print("course_direction の分布:")
    print(df['course_direction'].value_counts(dropna=False))
else:
    print("course_direction カラムなし")

print()

if 'is_outer_course' in df.columns:
    print("is_outer_course の分布:")
    print(df['is_outer_course'].value_counts(dropna=False))
else:
    print("is_outer_course カラムなし")

# 新潟2000mの確認
print()
niigata_2000 = df[(df['venue'] == '新潟') & (df['distance_m'] == 2000)]
print(f"新潟2000m レコード数: {len(niigata_2000)}")
if 'is_outer_course' in niigata_2000.columns:
    print(f"  is_outer_course分布: {niigata_2000['is_outer_course'].value_counts(dropna=False).to_dict()}")
