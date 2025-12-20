"""新潟2000mの確認"""
import pandas as pd
from pathlib import Path

parquet_path = Path(r"c:\Users\zk-ht\Keiba\Keiba_AI_v2\keibaai\data\parsed\parquet\races\races.parquet")
df = pd.read_parquet(parquet_path)

# 新潟2000mのis_outer_course分布を確認
niigata_2000 = df[(df['venue'] == '新潟') & (df['distance_m'] == 2000) & (df['track_surface'] == '芝')]
print(f"新潟芝2000m: {len(niigata_2000)}件")
print(f"is_outer_course分布:")
print(niigata_2000["is_outer_course"].value_counts(dropna=False))

# passing_order_4がある新潟2000m
with_corner = niigata_2000[niigata_2000['passing_order_4'].notna()]
print(f"\n4コーナー通過順位あり: {len(with_corner)}件")
outer = with_corner[with_corner["is_outer_course"] == True]
inner = with_corner[with_corner["is_outer_course"] != True]
print(f"  is_outer=True: {len(outer)}")
print(f"  is_outer=None/False: {len(inner)}")
