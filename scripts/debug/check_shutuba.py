#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""shutuba.parquet のカラム・データ構造を確認するスクリプト"""
import sys
from pathlib import Path
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
import pandas as pd

data_root = PROJECT_ROOT / "keibaai" / "data" / "parsed" / "parquet"
shutuba = pd.read_parquet(data_root / "shutuba" / "shutuba.parquet")

print(f"Shape: {shutuba.shape}")
print(f"\nColumns: {shutuba.columns.tolist()}")
print(f"\nDtypes:\n{shutuba.dtypes}")
print(f"\nSample (5 rows):")
print(shutuba.head(5).to_string())

# 2026/4/26 東京 のデータがあるか
shutuba["race_date"] = pd.to_datetime(shutuba.get("race_date", shutuba.get("date", None)), errors="coerce")
if "race_date" in shutuba.columns or "date" in shutuba.columns:
    print(f"\nDate range: {shutuba['race_date'].min()} ~ {shutuba['race_date'].max()}")
    
    target = shutuba[shutuba["race_date"] == "2026-04-26"]
    print(f"\n2026-04-26 データ: {len(target)} rows")
    if len(target) > 0:
        if "venue" in target.columns:
            print(f"  会場: {target['venue'].unique()}")
        if "race_num" in target.columns:
            r11 = target[target["race_num"] == 11]
            print(f"  11R: {len(r11)} rows")
            if len(r11) > 0:
                print(r11[["horse_name", "horse_id", "horse_number"]].head(20).to_string())
