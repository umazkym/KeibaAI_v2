#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
pedigrees.parquetの詳細検査スクリプト
"""

import pandas as pd
import sys
import io

if sys.platform == 'win32':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

# pedigrees.parquetを読み込み
df = pd.read_parquet(r"C:\Users\zk-ht\Keiba\test\keibaai\data\parsed\parquet\pedigrees\pedigrees.parquet")

print("=" * 80)
print("pedigrees.parquet 詳細検査")
print("=" * 80)

print(f"\n総行数: {len(df)}")
print(f"カラム数: {len(df.columns)}")
print(f"カラム: {list(df.columns)}")

print("\n" + "=" * 80)
print("統計情報")
print("=" * 80)

for col in df.columns:
    null_count = df[col].isna().sum()
    empty_count = (df[col] == '').sum() if df[col].dtype == 'object' else 0
    total_values = len(df) - null_count - empty_count

    print(f"\n[{col}]")
    print(f"  総行数: {len(df)}")
    print(f"  有効値: {total_values}行")
    print(f"  Null値: {null_count}行")
    print(f"  空文字列: {empty_count}行")
    print(f"  割合: {total_values}/{len(df)} ({100*total_values/len(df):.1f}%)")

print("\n" + "=" * 80)
print("データサンプル")
print("=" * 80)

print("\n最初の10行:")
print(df.head(10).to_string())

print("\n" + "=" * 80)
print("馬IDごとの祖先数")
print("=" * 80)

horse_counts = df.groupby('horse_id').size().reset_index(name='count')
print(horse_counts.to_string())

print("\n" + "=" * 80)
print("nullが多い馬の詳細確認")
print("=" * 80)

# horse_idごとにancestor_idのnull率を計算
for horse_id in df['horse_id'].unique()[:5]:
    horse_data = df[df['horse_id'] == horse_id]
    null_ancestors = horse_data['ancestor_id'].isna().sum()
    print(f"\nhorse_id: {horse_id}")
    print(f"  祖先数: {len(horse_data)}")
    print(f"  ancestor_id Null: {null_ancestors}")
    print(f"  データ:")
    print(horse_data[['ancestor_id', 'ancestor_name']].to_string())
