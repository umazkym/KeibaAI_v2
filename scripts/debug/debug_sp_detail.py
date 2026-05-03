#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""事前レポートでの脚質判定をトレース"""
import sys
from pathlib import Path
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
import pandas as pd
import numpy as np

data_root = PROJECT_ROOT / "keibaai" / "data" / "parsed" / "parquet"
races = pd.read_parquet(data_root / "races" / "races.parquet")
rd = pd.read_parquet(data_root / "race_details" / "race_details.parquet")
races["race_date"] = pd.to_datetime(races["race_date"])
races["race_id"] = races["race_id"].astype(str)

from keibaai.src.analysis.speed_index.calculator import SpeedIndexCalculator
from keibaai.src.analysis.speed_index.pace_correction import PaceCorrectionEngine

race_date = pd.Timestamp("2026-03-22")
train = races[races["race_date"] < race_date]

calc = SpeedIndexCalculator()
calc.fit(train)
pace_engine = PaceCorrectionEngine()
pace_engine.fit(train, rd)

# モズロックンロールのhorse_idを取得
moz = races[races["horse_name"] == "モズロックンロール"]
horse_id = moz["horse_id"].iloc[0]
print(f"horse_id: {horse_id}")

# 対象レース: 202405050110
target_race_id = "202405050110"

# --- テスト1: 対象馬のみの過去走（修正前の動作を再現） ---
past_races_only_target = races[
    (races["race_date"] < race_date)
    & (races["horse_id"] == horse_id)
]
past_sp = calc.calculate(past_races_only_target)

print(f"\n--- テスト1: 対象馬のみの過去走をpace_engineに渡す ---")
print(f"  past_races_only_target のrace_id={target_race_id} の行数: "
      f"{len(past_races_only_target[past_races_only_target['race_id'] == target_race_id])}")

past_rd = rd[rd["race_id"].astype(str).isin(past_sp["race_id"])]
result1 = pace_engine.apply(past_sp, past_races_only_target, past_rd)
moz1 = result1[result1["race_id"] == target_race_id]
if len(moz1) > 0:
    r = moz1.iloc[0]
    print(f"  running_style: {r.get('running_style', 'N/A')}")
    print(f"  total_horses: {r.get('total_horses', 'N/A')}")

# --- テスト2: 全出走馬を含む（修正後の動作） ---
print(f"\n--- テスト2: 全出走馬を含めてpace_engineに渡す ---")
past_race_ids = past_sp["race_id"].unique()
all_runners = races[races["race_id"].isin(past_race_ids)]
print(f"  all_runners のrace_id={target_race_id} の行数: "
      f"{len(all_runners[all_runners['race_id'] == target_race_id])}")

result2 = pace_engine.apply(past_sp, all_runners, past_rd)
moz2 = result2[result2["race_id"] == target_race_id]
if len(moz2) > 0:
    r = moz2.iloc[0]
    print(f"  running_style: {r.get('running_style', 'N/A')}")
    print(f"  total_horses: {r.get('total_horses', 'N/A')}")
    
    # 直接計算
    po1 = r.get("passing_order_1") if "passing_order_1" in result2.columns else None
    th = r.get("total_horses")
    if pd.notna(po1) and pd.notna(th):
        ratio = po1 / th
        print(f"  passing_order_1: {po1}")
        print(f"  ratio: {po1}/{th} = {ratio:.2f}")

# --- 確認: 修正が反映されているか ---
print(f"\n--- generate_prerace_sp_report.py の修正確認 ---")
script_path = PROJECT_ROOT / "scripts" / "analysis" / "sp_analysis" / "generate_prerace_sp_report.py"
with open(script_path, "r", encoding="utf-8") as f:
    content = f.read()
if "all_runners_in_past" in content:
    print("  ✅ 修正済み（all_runners_in_past を使用）")
else:
    print("  ❌ 未修正（past_races のまま）")
