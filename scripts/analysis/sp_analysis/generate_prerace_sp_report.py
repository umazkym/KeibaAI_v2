#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
事前SP値レポート生成（出馬表ベース）

出走予定馬の過去全レースSP値を一覧表示するHTMLレポートを生成。
レース前の判断材料として活用する。

Usage:
  python scripts/analysis/sp_analysis/generate_prerace_sp_report.py --date 20260322 --venue 中山 --race 11
  python scripts/analysis/sp_analysis/generate_prerace_sp_report.py --date 20260322 --venue 中山
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

VENUE_CODES = {
    "01": "札幌", "02": "函館", "03": "福島", "04": "新潟",
    "05": "東京", "06": "中山", "07": "中京", "08": "京都",
    "09": "阪神", "10": "小倉",
}
VENUE_TO_CODE = {v: k for k, v in VENUE_CODES.items()}


def main():
    parser = argparse.ArgumentParser(description="事前SP値レポート生成")
    parser.add_argument("--date", required=True, help="日付 (YYYYMMDD)")
    parser.add_argument("--venue", required=True, help="会場名 (例: 東京)")
    parser.add_argument("--race", type=int, default=None, help="レース番号 (省略時: 全レース)")
    args = parser.parse_args()

    date_str = args.date
    venue = args.venue
    race_num = args.race
    race_date = pd.Timestamp(f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}")

    print("=" * 70)
    print(f"  事前SP値レポート: {date_str} {venue}" + (f" {race_num}R" if race_num else " 全レース"))
    print("=" * 70)

    from keibaai.src.analysis.speed_index.calculator import SpeedIndexCalculator, get_best_sp_col
    from keibaai.src.analysis.speed_index.pace_correction import PaceCorrectionEngine

    # --- データ読み込み ---
    logger.info("データ読み込み中...")
    data_root = PROJECT_ROOT / "keibaai" / "data" / "parsed" / "parquet"
    shutuba = pd.read_parquet(data_root / "shutuba" / "shutuba.parquet")
    races = pd.read_parquet(data_root / "races" / "races.parquet")
    rd = pd.read_parquet(data_root / "race_details" / "race_details.parquet")

    # --- 出馬表から対象レースを抽出 ---
    shutuba["race_date"] = pd.to_datetime(shutuba["race_date"])
    shutuba["race_id"] = shutuba["race_id"].astype(str)
    target_shutuba = shutuba[
        (shutuba["race_date"] == race_date)
        & (~shutuba["scratched"])
    ].copy()

    if len(target_shutuba) == 0:
        print(f"⚠️ {date_str} の出馬表データがありません。")
        print(f"   shutuba.parquet の範囲: {shutuba['race_date'].min().date()} 〜 {shutuba['race_date'].max().date()}")
        return

    # race_id から会場コードを解析してフィルタ
    venue_code = VENUE_TO_CODE.get(venue, "")
    if venue_code:
        target_shutuba = target_shutuba[
            target_shutuba["race_id"].str[4:6] == venue_code
        ]

    if len(target_shutuba) == 0:
        print(f"⚠️ {date_str} {venue} の出馬表データがありません。")
        return

    # race_id からレース番号を抽出
    target_shutuba["race_num"] = target_shutuba["race_id"].str[-2:].astype(int)

    if race_num is not None:
        target_shutuba = target_shutuba[target_shutuba["race_num"] == race_num]
        if len(target_shutuba) == 0:
            print(f"⚠️ {date_str} {venue} {race_num}R の出馬表データがありません。")
            return

    logger.info(f"  対象: {target_shutuba['race_id'].nunique()} レース, {len(target_shutuba)} 頭")

    # --- races.parquet からレース情報を取得（あれば） ---
    races["race_id"] = races["race_id"].astype(str)
    races["race_date"] = pd.to_datetime(races["race_date"])
    race_info_df = races.drop_duplicates("race_id")[
        ["race_id", "venue", "track_surface", "distance_m", "track_condition", "race_name"]
    ]
    target_shutuba = target_shutuba.merge(race_info_df, on="race_id", how="left")

    # --- 全データでSP値計算器をfit ---
    logger.info("SP値計算器を構築中...")
    train_races = races[races["race_date"] < race_date]
    calc = SpeedIndexCalculator()
    calc.fit(train_races)

    pace_engine = PaceCorrectionEngine()
    pace_engine.fit(train_races, rd)

    # --- 各馬の過去全走SP値を算出 ---
    logger.info("過去走SP値を算出中...")
    horse_ids = target_shutuba["horse_id"].dropna().unique()
    past_races_target = races[
        (races["race_date"] < race_date)
        & (races["horse_id"].isin(horse_ids))
    ]
    logger.info(f"  過去走データ: {len(past_races_target):,} rows ({len(horse_ids)} 頭)")

    if len(past_races_target) == 0:
        print("⚠️ 出走予定馬の過去走データがありません。")
        return

    # レースレベル補正のため、過去レースの全出走馬データを取得
    past_race_ids = past_races_target["race_id"].unique()
    all_runners_in_past = races[
        (races["race_date"] < race_date)
        & (races["race_id"].isin(past_race_ids))
    ]
    logger.info(f"  全出走馬データ: {len(all_runners_in_past):,} rows ({len(past_race_ids)} レース)")

    # 全出走馬のSP値を算出
    past_sp_all = calc.calculate(all_runners_in_past)
    past_rd = rd[rd["race_id"].astype(str).isin(past_sp_all["race_id"])]
    if len(past_rd) > 0:
        past_sp_all = pace_engine.apply(past_sp_all, all_runners_in_past, past_rd)

    # --- レースレベル補正（全出走馬で実施） ---
    logger.info("レースレベル補正を適用中...")
    past_sp_all = calc.refine_with_race_quality(past_sp_all)

    # 対象馬のみをフィルタ
    horse_id_set = set(str(h) for h in horse_ids)
    past_sp = past_sp_all[past_sp_all["horse_id"].astype(str).isin(horse_id_set)].copy()

    sp_col = get_best_sp_col(past_sp)

    # --- 馬ごとの過去走データを構築 ---
    horse_data = {}
    for hid in horse_ids:
        h_past = past_sp[past_sp["horse_id"] == hid].sort_values("race_date", ascending=False)
        if len(h_past) == 0:
            continue
        records = []
        for _, row in h_past.iterrows():
            records.append({
                "date": row["race_date"].strftime("%Y/%m/%d") if pd.notna(row.get("race_date")) else "",
                "venue": row.get("venue", ""),
                "surface": row.get("track_surface", ""),
                "dist": int(row["distance_m"]) if pd.notna(row.get("distance_m")) else 0,
                "cond": row.get("track_condition", ""),
                "pos": int(row["finish_position"]) if pd.notna(row.get("finish_position")) else None,
                "sp": round(row[sp_col], 1) if pd.notna(row.get(sp_col)) else None,
                "rel": row.get("reliability", "C"),
                "style": row.get("running_style", ""),
            })

        valid_sp = [r["sp"] for r in records if r["sp"] is not None]
        last3 = valid_sp[:3]
        horse_data[str(hid)] = {
            "records": records,
            "avg": round(np.mean(valid_sp), 1) if valid_sp else None,
            "max": round(max(valid_sp), 1) if valid_sp else None,
            "avg3": round(np.mean(last3), 1) if last3 else None,
            "count": len(valid_sp),
        }

    # --- レースごとのエントリー情報 ---
    race_entries = {}
    for rn in sorted(target_shutuba["race_num"].unique()):
        entries = target_shutuba[target_shutuba["race_num"] == rn].copy()
        horses = []
        for _, row in entries.iterrows():
            hid = str(row.get("horse_id", ""))
            hd = horse_data.get(hid, {})
            horses.append({
                "num": int(row["horse_number"]),
                "bracket": int(row["bracket_number"]),
                "name": row["horse_name"],
                "horse_id": hid,
                "sex_age": row.get("sex_age", ""),
                "weight": row.get("basis_weight", ""),
                "jockey": row.get("jockey_name", ""),
                "avg3": hd.get("avg3"),
                "max": hd.get("max"),
                "avg": hd.get("avg"),
                "count": hd.get("count", 0),
            })
        # 近3走平均で降順ソート
        horses.sort(key=lambda h: h["avg3"] if h["avg3"] is not None else -999, reverse=True)

        rid = entries["race_id"].iloc[0]
        race_entries[str(rn)] = {
            "race_id": rid,
            "surface": entries["track_surface"].iloc[0] if "track_surface" in entries.columns and pd.notna(entries["track_surface"].iloc[0]) else "",
            "distance": int(entries["distance_m"].iloc[0]) if "distance_m" in entries.columns and pd.notna(entries["distance_m"].iloc[0]) else 0,
            "condition": entries["track_condition"].iloc[0] if "track_condition" in entries.columns and pd.notna(entries["track_condition"].iloc[0]) else "",
            "race_name": entries["race_name"].iloc[0] if "race_name" in entries.columns and pd.notna(entries["race_name"].iloc[0]) else "",
            "horses": horses,
        }

    # --- HTML生成 ---
    output_dir = PROJECT_ROOT / "outputs" / "reports" / "sp_analysis"
    suffix = f"_{race_num}R" if race_num else ""
    output_path = output_dir / f"sp_prerace_{date_str}_{venue}{suffix}.html"

    _generate_html(race_entries, horse_data, sp_col, venue, date_str, output_path)
    print(f"\n✅ レポート生成完了: {output_path}")


def _generate_html(race_entries, horse_data, sp_col, venue, date_str, output_path):
    """事前SP値HTMLレポートを生成。"""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    generated_at = datetime.now().strftime("%Y-%m-%d %H:%M")

    entries_json = json.dumps(race_entries, ensure_ascii=False, default=str)
    horses_json = json.dumps(horse_data, ensure_ascii=False, default=str)

    html = f"""<!DOCTYPE html>
<html lang="ja">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>事前SP値レポート {date_str} {venue}</title>
<script src="https://cdn.plot.ly/plotly-2.27.0.min.js"></script>
<style>
*{{margin:0;padding:0;box-sizing:border-box}}
body{{font-family:'Segoe UI','Meiryo',sans-serif;background:#0f172a;color:#e2e8f0;line-height:1.5}}
.header{{background:linear-gradient(135deg,#7c3aed,#1e3a5f);padding:20px 24px;border-bottom:2px solid #334155}}
.header h1{{font-size:22px;color:#c4b5fd}}
.header .meta{{color:#94a3b8;font-size:13px;margin-top:4px}}
.tabs{{display:flex;gap:6px;padding:12px 24px;background:#1e293b;overflow-x:auto;border-bottom:1px solid #334155}}
.tab{{padding:8px 16px;border-radius:8px;cursor:pointer;background:#334155;color:#94a3b8;font-size:13px;font-weight:600;white-space:nowrap;transition:all .2s}}
.tab.active{{background:#7c3aed;color:#fff}}
.tab:hover{{background:#475569}}
.container{{max-width:1100px;margin:0 auto;padding:16px 24px}}
.section{{background:#1e293b;border-radius:12px;padding:20px;margin-bottom:16px;border:1px solid #334155}}
.race-title{{display:flex;align-items:center;gap:12px;margin-bottom:16px;flex-wrap:wrap}}
.badge{{padding:4px 12px;border-radius:8px;font-size:13px;font-weight:bold;color:#fff}}
.turf{{background:#16a34a}} .dirt{{background:#d97706}}
table{{width:100%;border-collapse:collapse;font-size:13px}}
th{{background:#334155;color:#93c5fd;padding:8px 10px;text-align:left;position:sticky;top:0;white-space:nowrap}}
td{{padding:6px 10px;border-bottom:1px solid #0f172a;white-space:nowrap}}
tr{{cursor:pointer;transition:background .15s}}
tr:hover td{{background:#334155}}
tr.selected td{{background:#312e81}}
.sp-high{{color:#22c55e;font-weight:bold}} .sp-mid{{color:#eab308}} .sp-low{{color:#ef4444}}
.detail-panel{{background:#0f172a;border-radius:8px;padding:16px;margin-top:12px;display:none}}
.detail-panel.show{{display:block}}
.detail-panel h3{{color:#c4b5fd;font-size:15px;margin-bottom:12px}}
.detail-table{{font-size:12px}}
.detail-table th{{background:#1e293b;padding:6px 8px}}
.detail-table td{{padding:5px 8px;border-bottom:1px solid #1e293b}}
.chart-wrap{{height:280px;margin-top:12px}}
.pos-1{{color:#fbbf24;font-weight:bold}} .pos-2{{color:#c0c0c0;font-weight:bold}} .pos-3{{color:#cd7f32;font-weight:bold}}
.trend-up{{color:#22c55e}} .trend-down{{color:#ef4444}} .trend-flat{{color:#94a3b8}}
</style>
</head>
<body>
<div class="header">
  <h1>🔮 事前SP値レポート — {date_str} {venue}</h1>
  <div class="meta">生成: {generated_at} | 出馬表ベース | 過去走SP値のみ（当日未確定）</div>
</div>
<div class="tabs" id="tabs"></div>
<div class="container" id="content"></div>

<script>
const ENTRIES={entries_json};
const HORSES={horses_json};
const raceNums=Object.keys(ENTRIES).sort((a,b)=>parseInt(a)-parseInt(b));
let cur=raceNums[0],selHorse=null;

function init(){{renderTabs();renderRace()}}

function renderTabs(){{
  document.getElementById('tabs').innerHTML=raceNums.map(r=>
    `<div class="tab ${{r===cur?'active':''}}" onclick="cur='${{r}}';selHorse=null;renderTabs();renderRace()">${{r}}R</div>`
  ).join('');
}}

function spCls(v){{if(v==null)return'';return v>=60?'sp-high':v>=45?'sp-mid':'sp-low'}}
function posCls(p){{if(p===1)return'pos-1';if(p===2)return'pos-2';if(p===3)return'pos-3';return''}}

function renderRace(){{
  const race=ENTRIES[cur];if(!race)return;
  const c=document.getElementById('content');
  const surfCls=race.surface==='芝'?'turf':'dirt';
  const label=race.surface+(race.distance||'')+'m';
  const rname=race.race_name||'';

  let rows=race.horses.map((h,i)=>{{
    const a3=h.avg3!=null?`<span class="${{spCls(h.avg3)}}">${{h.avg3}}</span>`:'—';
    const mx=h.max!=null?`<span class="${{spCls(h.max)}}">${{h.max}}</span>`:'—';
    const av=h.avg!=null?h.avg:'—';
    const sel=selHorse===h.horse_id?'selected':'';
    return`<tr class="${{sel}}" onclick="selHorse='${{h.horse_id}}';renderRace();setTimeout(drawChart,50)">
      <td style="text-align:center">${{i+1}}</td>
      <td style="text-align:center">${{h.num}}</td>
      <td>${{h.name}}</td><td>${{h.sex_age}}</td><td>${{h.jockey}}</td>
      <td>${{a3}}</td><td>${{mx}}</td><td style="color:#94a3b8">${{av}}</td>
      <td style="color:#94a3b8">${{h.count}}</td>
    </tr>`;
  }}).join('');

  let detailHTML='';
  if(selHorse && HORSES[selHorse]){{
    const hd=HORSES[selHorse];
    const hname=race.horses.find(h=>h.horse_id===selHorse)?.name||'';
    let drows=hd.records.map(r=>{{
      const spStr=r.sp!=null?`<span class="${{spCls(r.sp)}}">${{r.sp}}</span>`:'—';
      const posStr=r.pos!=null?`<span class="${{posCls(r.pos)}}">${{r.pos}}</span>`:'—';
      return`<tr>
        <td>${{r.date}}</td><td>${{r.venue}}</td><td>${{r.surface}}</td>
        <td>${{r.dist}}m</td><td>${{r.cond}}</td><td>${{posStr}}</td>
        <td>${{spStr}}</td><td>${{r.rel}}</td><td>${{r.style}}</td>
      </tr>`;
    }}).join('');
    detailHTML=`<div class="detail-panel show">
      <h3>📋 ${{hname}} — 過去走SP値一覧 (全${{hd.count}}走)</h3>
      <div style="color:#94a3b8;font-size:12px;margin-bottom:8px">
        平均: ${{hd.avg??'—'}} | 最高: ${{hd.max??'—'}} | 近3走: ${{hd.avg3??'—'}}
      </div>
      <div id="spChart" class="chart-wrap"></div>
      <div style="max-height:400px;overflow-y:auto;margin-top:12px">
        <table class="detail-table"><thead><tr>
          <th>日付</th><th>会場</th><th>コース</th><th>距離</th><th>馬場</th>
          <th>着順</th><th>SP値</th><th>信頼</th><th>脚質</th>
        </tr></thead><tbody>${{drows}}</tbody></table>
      </div>
    </div>`;
  }}

  c.innerHTML=`<div class="section">
    <div class="race-title">
      <span style="font-size:18px;font-weight:bold;color:#c4b5fd">${{cur}}R</span>
      ${{race.surface?`<span class="badge ${{surfCls}}">${{label}}</span>`:''}}
      <span style="color:#94a3b8;font-size:13px">${{rname}}</span>
    </div>
    <table><thead><tr>
      <th>SP順</th><th>番</th><th>馬名</th><th>性齢</th><th>騎手</th>
      <th>近3走</th><th>最高</th><th>全平均</th><th>走数</th>
    </tr></thead><tbody>${{rows}}</tbody></table>
    ${{detailHTML}}
  </div>`;
}}

function drawChart(){{
  if(!selHorse||!HORSES[selHorse])return;
  const hd=HORSES[selHorse];
  const recs=hd.records.filter(r=>r.sp!=null).reverse();
  if(recs.length===0)return;
  const hname=ENTRIES[cur].horses.find(h=>h.horse_id===selHorse)?.name||'';
  Plotly.newPlot('spChart',[{{
    x:recs.map(r=>r.date),
    y:recs.map(r=>r.sp),
    text:recs.map(r=>`${{r.venue}}${{r.surface}}${{r.dist}}m ${{r.pos?r.pos+'着':''}}`),
    type:'scatter',mode:'lines+markers',
    hoverinfo:'text+y',
    line:{{color:'#a78bfa',width:2}},
    marker:{{size:7,color:recs.map(r=>r.sp>=60?'#22c55e':r.sp>=45?'#eab308':'#ef4444')}}
  }}],{{
    title:{{text:hname+' SP値推移',font:{{size:14,color:'#e2e8f0'}}}},
    xaxis:{{color:'#94a3b8'}},
    yaxis:{{title:'SP値',color:'#94a3b8',range:[0,Math.max(...recs.map(r=>r.sp))+10]}},
    shapes:[{{type:'line',x0:recs[0].date,x1:recs[recs.length-1].date,y0:50,y1:50,line:{{color:'#475569',dash:'dot',width:1}}}}],
    height:260,margin:{{t:40,b:40,l:50,r:20}},
    paper_bgcolor:'#0f172a',plot_bgcolor:'#1e293b',font:{{color:'#e2e8f0'}}
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
