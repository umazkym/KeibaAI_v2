#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
shutuba.parquetの修正を検証
"""

import pandas as pd
import sys
import io

if sys.platform == 'win32':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

df = pd.read_parquet(r"C:\Users\zk-ht\Keiba\test\keibaai\data\parsed\parquet\shutuba\shutuba.parquet")

print("=" * 80)
print("shutuba.parquet - 修正検証")
print("=" * 80)

print(f"\n総行数: {len(df)}")
print(f"カラム: {list(df.columns)}")

print("\n" + "=" * 80)
print("問題フィールドの検証")
print("=" * 80)

fields_to_check = {
    'jockey_id': 'jockey_idはすべて値が見つからないと表示される',
    'owner_name': 'owner_nameは全て---.-になっている',
    'morning_odds': 'morning_oddsすべて値が見つからないと表示される',
    'morning_popularity': 'morning_popularityはすべて値が見つからないと表示される',
    'career_stats': 'career_statsはすべて編集と表示される',
    'career_starts': 'career_startsはすべて値が見つからないと表示される',
    'career_wins': 'career_winsはすべて値が見つからないと表示される',
    'career_places': 'career_placesはすべて値が見つからないと表示される',
    'last_5_finishes': 'last_5_finishesは全て編集XXXと表示される',
}

for field, error_description in fields_to_check.items():
    if field not in df.columns:
        print(f"\n[ERROR] {field} がカラムに存在しません")
        continue

    null_count = df[field].isna().sum()
    non_null_count = len(df) - null_count

    if field == 'owner_name':
        dashes_count = (df[field] == '---.-').sum()
        print(f"\n{field}: {error_description}")
        print(f"  Null値: {null_count}/{len(df)}")
        print(f"  '---.-' : {dashes_count}/{len(df)}")
        if null_count == len(df):
            print(f"  ✓ FIXED: すべてが正常にNullになっている")
        elif dashes_count > 0:
            print(f"  ✗ 未修正: {dashes_count}行が'---.-'のままになっている")

    elif field in ['career_stats', 'last_5_finishes']:
        edit_count = (df[field] == '編集').sum()
        edit_xxx_count = (df[field].str.contains('編集', na=False, regex=False)).sum() if df[field].dtype == 'object' else 0
        print(f"\n{field}: {error_description}")
        print(f"  Null値: {null_count}/{len(df)}")
        print(f"  '編集' : {edit_count}/{len(df)}")
        if null_count == len(df):
            print(f"  ✓ FIXED: すべてが正常にNullになっている")
        elif edit_count > 0:
            print(f"  ✗ 未修正: {edit_count}行が'編集'のままになっている")

    else:
        print(f"\n{field}: {error_description}")
        print(f"  有効値: {non_null_count}/{len(df)}")
        print(f"  Null値: {null_count}/{len(df)}")
        if non_null_count == len(df):
            print(f"  ✓ FIXED: すべてのレコードに値があります")
        elif null_count == len(df):
            print(f"  [INFO] 全てNullです（設計通りかもしれません）")
        else:
            print(f"  ⚠ 部分的に値があります")

print("\n" + "=" * 80)
print("データサンプル (最初の3行)")
print("=" * 80)

# 必要なフィールドのみ表示
display_fields = ['horse_id', 'horse_name', 'jockey_id', 'jockey_name',
                  'morning_odds', 'morning_popularity', 'owner_name',
                  'career_stats', 'last_5_finishes']
available_fields = [f for f in display_fields if f in df.columns]

print(df[available_fields].head(3).to_string())
