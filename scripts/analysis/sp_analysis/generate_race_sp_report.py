#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
レース日別SP値レポート生成

指定日・指定会場の全レースについて、各出走馬のSP値を
インタラクティブHTMLで表示する。

既存の `batch_generate_html.py` と同じ引数形式で実行可能。

Usage:
  python scripts/analysis/sp_analysis/generate_race_sp_report.py --date 20260329 --venue 中山
"""

import sys
import logging
import argparse
import json
from pathlib import Path
from datetime import datetime

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import numpy as np
import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(description="レース日別SP値レポート生成")
    parser.add_argument("--date", required=True, help="日付 (YYYYMMDD)")
    parser.add_argument("--venue", required=True, help="会場名")
    args = parser.parse_args()

    date_str = args.date
    venue = args.venue
    race_date = pd.Timestamp(f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}")

    print("=" * 70)
    print(f"  SP値レポート生成: {date_str} {venue}")
    print("=" * 70)

    from keibaai.src.analysis.speed_index.calculator import SpeedIndexCalculator, get_best_sp_col
    from keibaai.src.analysis.speed_index.pace_correction import PaceCorrectionEngine

    # --- データ読み込み ---
    logger.info("データ読み込み中...")
    data_root = PROJECT_ROOT / "keibaai" / "data" / "parsed" / "parquet"
    races = pd.read_parquet(data_root / "races" / "races.parquet")
    rd = pd.read_parquet(data_root / "race_details" / "race_details.parquet")

    races["race_date"] = pd.to_datetime(races["race_date"])
    races["race_id"] = races["race_id"].astype(str)

    # --- 過去データでfit（対象日以前） ---
    train_races = races[races["race_date"] < race_date]
    logger.info(f"  学習データ: {len(train_races):,} rows (〜{race_date.date()})")

    calc = SpeedIndexCalculator()
    calc.fit(train_races)

    pace_engine = PaceCorrectionEngine()
    pace_engine.fit(train_races, rd)

    # --- 対象日・対象会場のレースを抽出 ---
    target = races[
        (races["race_date"] == race_date) & (races["venue"] == venue)
    ]
    if len(target) == 0:
        print(f"⚠️ {date_str} {venue} のレースが見つかりません。")
        return

    logger.info(f"  対象レース: {target['race_id'].nunique()} R, {len(target)} 頭")

    # --- 対象日のSP値算出 ---
    sp_result = calc.calculate(target)

    # ペース補正
    target_rd = rd[rd["race_id"].astype(str).isin(sp_result["race_id"])]
    if len(target_rd) > 0:
        sp_result = pace_engine.apply(sp_result, target, target_rd)

    # レースレベル補正
    sp_result = calc.refine_with_race_quality(sp_result)

    # --- 各馬の過去SP値を算出（過去走の集約） ---
    sp_col = get_best_sp_col(sp_result)

    # 全馬の過去走SP値
    horse_ids = sp_result["horse_id"].dropna().unique()
    past_races = races[
        (races["race_date"] < race_date)
        & (races["horse_id"].isin(horse_ids))
    ]
    if len(past_races) > 0:
        past_sp = calc.calculate(past_races)
        past_rd = rd[rd["race_id"].astype(str).isin(past_sp["race_id"])]
        if len(past_rd) > 0:
            past_sp = pace_engine.apply(past_sp, past_races, past_rd)

        # 過去走にもレースレベル補正を適用
        past_sp = calc.refine_with_race_quality(past_sp)

        past_sp_col = get_best_sp_col(past_sp)

        # 馬ごとの過去SP統計
        past_stats = (
            past_sp.dropna(subset=[past_sp_col])
            .sort_values(["horse_id", "race_date"])
            .groupby("horse_id")
            .agg(
                sp_avg=(past_sp_col, "mean"),
                sp_max=(past_sp_col, "max"),
                sp_last=(past_sp_col, "last"),
                sp_count=(past_sp_col, "count"),
            )
            .reset_index()
        )

        # 直近3走平均
        last3 = (
            past_sp.dropna(subset=[past_sp_col])
            .sort_values(["horse_id", "race_date"])
            .groupby("horse_id")
            .tail(3)
            .groupby("horse_id")[past_sp_col]
            .mean()
            .reset_index()
            .rename(columns={past_sp_col: "sp_avg_3"})
        )
        past_stats = past_stats.merge(last3, on="horse_id", how="left")

        sp_result = sp_result.merge(past_stats, on="horse_id", how="left")

        # 過去走の履歴（チャート用）
        horse_histories = {}
        for hid in horse_ids:
            h_past = past_sp[past_sp["horse_id"] == hid].sort_values("race_date")
            if len(h_past) > 0:
                horse_histories[str(hid)] = {
                    "dates": h_past["race_date"].dt.strftime("%Y/%m/%d").tolist(),
                    "sp_values": [
                        round(v, 1) if pd.notna(v) else None
                        for v in h_past[past_sp_col].tolist()
                    ],
                    "venues": h_past["venue"].tolist(),
                    "distances": h_past["distance_m"].fillna(0).astype(int).tolist(),
                    "positions": [
                        int(v) if pd.notna(v) else None
                        for v in h_past["finish_position"].tolist()
                    ],
                }
    else:
        horse_histories = {}

    # --- HTMLレポート生成 ---
    output_dir = PROJECT_ROOT / "outputs" / "reports" / "sp_analysis"
    output_path = output_dir / f"sp_race_report_{date_str}_{venue}.html"

    _generate_race_html(
        sp_result, sp_col, venue, date_str, horse_histories, output_path
    )

    print(f"\n✅ レポート生成完了: {output_path}")


def _generate_race_html(
    sp_df, sp_col, venue, date_str, horse_histories, output_path
):
    """レース日別SP値HTMLレポートを生成。"""
    output_path.parent.mkdir(parents=True, exist_ok=True)

    race_ids = sorted(sp_df["race_id"].unique())

    # レースごとのデータを準備
    race_data = {}
    for rid in race_ids:
        r = sp_df[sp_df["race_id"] == rid].copy()
        r = r.sort_values(sp_col, ascending=False, na_position="last")
        # race_numの推定
        race_num = rid[-2:].lstrip("0") if len(rid) >= 2 else rid

        horses = []
        for _, row in r.iterrows():
            horses.append({
                "num": int(row.get("horse_number", 0)) if pd.notna(row.get("horse_number")) else 0,
                "name": row.get("horse_name", ""),
                "sp": round(row[sp_col], 1) if pd.notna(row.get(sp_col)) else None,
                "sp_raw": round(row["sp_raw"], 1) if pd.notna(row.get("sp_raw")) else None,
                "pace_corr": round(row.get("pace_correction", 0), 2) if pd.notna(row.get("pace_correction")) else 0,
                "style": row.get("running_style", ""),
                "rel": row.get("reliability", "C"),
                "position": int(row["finish_position"]) if pd.notna(row.get("finish_position")) else None,
                "weight": row.get("basis_weight", ""),
                "tv": round(row.get("track_variant", 0), 2) if pd.notna(row.get("track_variant")) else 0,
                "sp_avg": round(row["sp_avg"], 1) if pd.notna(row.get("sp_avg")) else None,
                "sp_max": round(row["sp_max"], 1) if pd.notna(row.get("sp_max")) else None,
                "sp_last": round(row["sp_last"], 1) if pd.notna(row.get("sp_last")) else None,
                "sp_avg_3": round(row["sp_avg_3"], 1) if pd.notna(row.get("sp_avg_3")) else None,
                "sp_count": int(row["sp_count"]) if pd.notna(row.get("sp_count")) else 0,
                "horse_id": str(row.get("horse_id", "")),
            })

        race_data[race_num] = {
            "race_id": rid,
            "surface": r["track_surface"].iloc[0] if "track_surface" in r.columns else "",
            "distance": int(r["distance_m"].iloc[0]) if "distance_m" in r.columns else 0,
            "condition": r["track_condition"].iloc[0] if "track_condition" in r.columns else "",
            "horses": horses,
        }

    data_json = json.dumps(race_data, ensure_ascii=False, default=str)
    history_json = json.dumps(horse_histories, ensure_ascii=False, default=str)
    generated_at = datetime.now().strftime("%Y-%m-%d %H:%M")

    html = f"""<!DOCTYPE html>
<html lang="ja">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>SP値レポート {date_str} {venue}</title>
<script src="https://cdn.plot.ly/plotly-2.27.0.min.js"></script>
<style>
*{{margin:0;padding:0;box-sizing:border-box}}
body{{font-family:'Segoe UI','Meiryo',sans-serif;background:#0f172a;color:#e2e8f0}}
.header{{background:linear-gradient(135deg,#1e3a5f,#0f172a);padding:20px 24px;border-bottom:2px solid #334155}}
.header h1{{font-size:22px;color:#60a5fa}}
.header .meta{{color:#94a3b8;font-size:13px;margin-top:4px}}
.tabs{{display:flex;gap:6px;padding:12px 24px;background:#1e293b;overflow-x:auto;border-bottom:1px solid #334155}}
.tab{{padding:8px 16px;border-radius:8px;cursor:pointer;background:#334155;color:#94a3b8;font-size:13px;font-weight:600;white-space:nowrap}}
.tab.active{{background:#2563eb;color:#fff}}
.tab:hover{{background:#475569}}
.container{{max-width:1000px;margin:0 auto;padding:16px 24px}}
.race-card{{background:#1e293b;border-radius:12px;padding:20px;margin-bottom:16px;border:1px solid #334155;display:none}}
.race-card.active{{display:block}}
.race-title{{display:flex;align-items:center;gap:12px;margin-bottom:16px}}
.course-badge{{padding:4px 12px;border-radius:8px;font-size:13px;font-weight:bold;color:#fff}}
.turf{{background:#16a34a}} .dirt{{background:#d97706}}
table{{width:100%;border-collapse:collapse;font-size:13px}}
th{{background:#334155;color:#93c5fd;padding:8px 10px;text-align:left;position:sticky;top:0;white-space:nowrap}}
td{{padding:6px 10px;border-bottom:1px solid #1e293b;white-space:nowrap}}
tr:hover td{{background:#334155}}
.sp-val{{font-weight:bold;font-size:15px}}
.sp-high{{color:#22c55e}} .sp-mid{{color:#eab308}} .sp-low{{color:#ef4444}}
.rel-a{{color:#22c55e;font-weight:bold}} .rel-b{{color:#eab308}} .rel-c{{color:#6b7280}}
.past-sp{{font-size:11px;color:#94a3b8}}
.chart-wrap{{margin-top:16px;height:300px}}
.pos-1{{color:#fbbf24;font-weight:bold}} .pos-2{{color:#94a3b8}} .pos-3{{color:#cd7f32}}
</style>
</head>
<body>
<div class="header">
  <h1>🏇 SP値レポート — {date_str} {venue}</h1>
  <div class="meta">生成: {generated_at} | 馬場指数・ペース補正済み | 過去走SP値含む</div>
</div>
<div class="tabs" id="tabs"></div>
<div class="container" id="content"></div>

<script>
const DATA = {data_json};
const HIST = {history_json};
const raceNums = Object.keys(DATA).sort((a,b)=>parseInt(a)-parseInt(b));
let currentRace = raceNums[0];

function init(){{
  renderTabs();
  renderRace(currentRace);
}}

function renderTabs(){{
  const t=document.getElementById('tabs');
  t.innerHTML=raceNums.map(r=>`<div class="tab ${{r===currentRace?'active':''}}" onclick="selectRace('${{r}}')">${{r}}R</div>`).join('');
}}

function selectRace(r){{
  currentRace=r;
  renderTabs();
  renderRace(r);
}}

function spClass(v){{
  if(v===null)return'';
  if(v>=60)return'sp-high';if(v>=45)return'sp-mid';return'sp-low';
}}

function posClass(p){{
  if(p===1)return'pos-1';if(p===2)return'pos-2';if(p===3)return'pos-3';return'';
}}

function renderRace(rn){{
  const race=DATA[rn];if(!race)return;
  const c=document.getElementById('content');
  const surfClass=race.surface==='芝'?'turf':'dirt';
  const courseLabel=race.surface+race.distance+'m';

  let rows=race.horses.map((h,i)=>{{
    const spStr=h.sp!==null?`<span class="sp-val ${{spClass(h.sp)}}">${{h.sp}}</span>`:'—';
    const posStr=h.position?`<span class="${{posClass(h.position)}}">${{h.position}}</span>`:'—';
    const relCls=h.rel==='A'?'rel-a':h.rel==='B'?'rel-b':'rel-c';
    const pastSp=h.sp_avg_3!==null?`${{h.sp_avg_3}}`:'—';
    const pastMax=h.sp_max!==null?`${{h.sp_max}}`:'—';
    const pastN=h.sp_count||0;
    const corrStr=h.pace_corr!==0?`<span style="color:${{h.pace_corr>0?'#22c55e':'#ef4444'}}">${{h.pace_corr>0?'+':''}}${{h.pace_corr}}</span>`:'—';

    return`<tr onclick="showHistory('${{h.horse_id}}','${{h.name}}')">
      <td style="text-align:center">${{i+1}}</td>
      <td style="text-align:center">${{h.num}}</td>
      <td>${{h.name}}</td>
      <td>${{spStr}}</td>
      <td class="${{relCls}}">${{h.rel}}</td>
      <td>${{corrStr}}</td>
      <td>${{h.style||'—'}}</td>
      <td class="past-sp">${{pastSp}}</td>
      <td class="past-sp">${{pastMax}}</td>
      <td class="past-sp">${{pastN}}</td>
      <td>${{posStr}}</td>
    </tr>`;
  }}).join('');

  c.innerHTML=`
    <div class="race-card active">
      <div class="race-title">
        <span style="font-size:18px;font-weight:bold;color:#60a5fa">${{rn}}R</span>
        <span class="course-badge ${{surfClass}}">${{courseLabel}}</span>
        <span style="color:#94a3b8;font-size:13px">${{race.condition||''}}</span>
        <span style="color:#94a3b8;font-size:13px">馬場指数: ${{race.horses[0]?.tv||0}}</span>
      </div>
      <table>
        <thead><tr>
          <th>SP順</th><th>番</th><th>馬名</th>
          <th>SP値</th><th>信頼</th><th>ﾍﾟｰｽ補正</th><th>脚質</th>
          <th>近3走</th><th>最高</th><th>走数</th>
          <th>着順</th>
        </tr></thead>
        <tbody>${{rows}}</tbody>
      </table>
      <div id="histChart" class="chart-wrap" style="display:none"></div>
    </div>`;
}}

function showHistory(hid,name){{
  const h=HIST[hid];
  const div=document.getElementById('histChart');
  if(!h||!h.sp_values.length){{div.style.display='none';return;}}
  div.style.display='block';

  const labels=h.dates.map((d,i)=>`${{d}} ${{h.venues[i]}}${{h.distances[i]}}m`);
  Plotly.newPlot('histChart',[{{
    x:h.dates,y:h.sp_values,
    type:'scatter',mode:'lines+markers',
    text:labels,hoverinfo:'text+y',
    line:{{color:'#60a5fa',width:2}},
    marker:{{size:6}}
  }}],{{
    title:{{text:name+' — SP値推移',font:{{size:14,color:'#e2e8f0'}}}},
    xaxis:{{color:'#94a3b8'}},
    yaxis:{{title:'SP値',color:'#94a3b8',zeroline:true}},
    height:280,margin:{{t:40,b:40,l:50,r:20}},
    paper_bgcolor:'#1e293b',plot_bgcolor:'#0f172a',
    font:{{color:'#e2e8f0'}}
  }},{{responsive:true}});
}}

init();
</script>
</body>
</html>"""

    output_path.write_text(html, encoding="utf-8")
    logger.info(f"  レポート生成完了: {output_path}")


if __name__ == "__main__":
    main()
