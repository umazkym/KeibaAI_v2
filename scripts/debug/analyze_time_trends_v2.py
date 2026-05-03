#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
タイム推移ダッシュボード v2 - 正しい比較軸での分析

改善点:
1. 「同一競馬場×同一距離×同一コース」単位で時系列表示
2. 季節性の分離（月別ベースライン除去）
3. ローリング統計（直近3年 vs 全期間）でPKL鮮度を可視化
4. 最小サンプル数フィルタ
5. 競馬場間比較は同一距離×コースに固定して補正値比較
"""

import os, sys, json, logging
from pathlib import Path
from datetime import datetime
import pandas as pd
import numpy as np

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

PARQUET_PATH = PROJECT_ROOT / "keibaai" / "data" / "parsed" / "parquet" / "races" / "races.parquet"
OUTPUT_HTML = PROJECT_ROOT / "outputs" / "reports" / "all_venues_time_trends_v2.html"

MIN_SAMPLES = 9  # 月次集計の最小サンプル数


def build():
    print("📈 タイム推移ダッシュボード v2 生成中...")

    df = pd.read_parquet(PARQUET_PATH, columns=[
        'race_date', 'venue', 'distance_m', 'track_surface',
        'track_condition', 'finish_position', 'finish_time_seconds', 'last_3f_time'
    ])
    df['race_date'] = pd.to_datetime(df['race_date'])
    df['finish_time_seconds'] = pd.to_numeric(df['finish_time_seconds'], errors='coerce')
    df['last_3f_time'] = pd.to_numeric(df['last_3f_time'], errors='coerce')
    df = df.dropna(subset=['finish_time_seconds', 'track_surface', 'venue', 'distance_m'])
    df = df[df['track_surface'].isin(['芝', 'ダート'])].copy()

    df_top3 = df[df['finish_position'].isin([1, 2, 3])].copy()
    df_top3['year'] = df_top3['race_date'].dt.year
    df_top3['month'] = df_top3['race_date'].dt.month
    df_top3['ym'] = df_top3['race_date'].dt.to_period('M').astype(str)
    df_top3['distance_m'] = df_top3['distance_m'].astype(int)

    # ===== データセット1: 競馬場×距離×コース別 月次平均 =====
    print("  競馬場×距離×コース別 月次集計中...")
    grp = df_top3.groupby(['venue', 'track_surface', 'distance_m', 'ym']).agg(
        time_mean=('finish_time_seconds', 'mean'),
        l3f_mean=('last_3f_time', 'mean'),
        count=('finish_time_seconds', 'size')
    ).reset_index()
    grp = grp[grp['count'] >= MIN_SAMPLES]

    # ===== データセット2: 競馬場×距離×コース×月 季節性ベースライン =====
    print("  季節性ベースライン算出中...")
    df_top3_copy = df_top3.copy()
    seasonal = df_top3_copy.groupby(['venue', 'track_surface', 'distance_m', 'month']).agg(
        seasonal_mean=('finish_time_seconds', 'mean'),
        seasonal_count=('finish_time_seconds', 'size')
    ).reset_index()

    # ===== データセット3: ローリング統計 (3年窓) =====
    print("  ローリング統計算出中...")
    df_top3_copy['year_half'] = df_top3_copy['year'].astype(str) + ('H1' if True else 'H2')
    
    rolling_stats = []
    for (venue, surf, dist), sub in df_top3_copy.groupby(['venue', 'track_surface', 'distance_m']):
        if len(sub) < 30:
            continue
        full_mean = sub['finish_time_seconds'].mean()
        full_std = sub['finish_time_seconds'].std()
        full_l3f_mean = sub['last_3f_time'].mean()
        
        for year in range(2017, 2027):
            window = sub[(sub['race_date'] >= f'{year-3}-01-01') & (sub['race_date'] < f'{year}-01-01')]
            if len(window) < 20:
                continue
            roll_mean = window['finish_time_seconds'].mean()
            roll_std = window['finish_time_seconds'].std()
            roll_l3f = window['last_3f_time'].mean()
            rolling_stats.append({
                'venue': venue, 'track_surface': surf, 'distance_m': dist,
                'year': year,
                'full_mean': round(full_mean, 3), 'full_std': round(full_std, 3),
                'roll3y_mean': round(roll_mean, 3), 'roll3y_std': round(roll_std, 3),
                'drift': round(roll_mean - full_mean, 3),
                'full_l3f': round(full_l3f_mean, 3), 'roll3y_l3f': round(roll_l3f, 3),
                'n': len(window)
            })
    rolling_df = pd.DataFrame(rolling_stats)

    # ===== データセット4: 年次 競馬場×距離×コース別 =====
    print("  年次集計中...")
    yearly = df_top3.groupby(['venue', 'track_surface', 'distance_m', 'year']).agg(
        time_mean=('finish_time_seconds', 'mean'),
        l3f_mean=('last_3f_time', 'mean'),
        count=('finish_time_seconds', 'size')
    ).reset_index()
    yearly = yearly[yearly['count'] >= 10]

    # ===== 選択肢リスト =====
    combos = grp.groupby(['venue', 'track_surface', 'distance_m']).agg(
        total=('count', 'sum'), months=('ym', 'count')
    ).reset_index()
    combos = combos[combos['total'] >= 50].sort_values(['track_surface', 'distance_m', 'venue'])
    combo_list = combos[['venue', 'track_surface', 'distance_m']].to_dict('records')

    # ===== 同一距離×コースの全競馬場比較用 =====
    dist_surf_combos = combos.groupby(['track_surface', 'distance_m']).apply(
        lambda x: sorted(x['venue'].unique().tolist())
    ).reset_index()
    dist_surf_combos.columns = ['track_surface', 'distance_m', 'venues']
    dist_surf_list = dist_surf_combos[dist_surf_combos['venues'].apply(len) >= 2].to_dict('records')

    # ===== JSON化 =====
    grp_json = grp.to_json(orient='records', force_ascii=False)
    seasonal_json = seasonal.to_json(orient='records', force_ascii=False)
    rolling_json = rolling_df.to_json(orient='records', force_ascii=False) if len(rolling_df) > 0 else '[]'
    yearly_json = yearly.to_json(orient='records', force_ascii=False)
    combo_json = json.dumps(combo_list, ensure_ascii=False)
    dist_surf_json = json.dumps(dist_surf_list, ensure_ascii=False)

    parquet_min = df_top3['race_date'].min().strftime('%Y-%m-%d')
    parquet_max = df_top3['race_date'].max().strftime('%Y-%m-%d')
    total_count = len(df)
    top3_count = len(df_top3)

    # ===== HTML =====
    html = _generate_html(grp_json, seasonal_json, rolling_json, yearly_json,
                          combo_json, dist_surf_json,
                          parquet_min, parquet_max, total_count, top3_count)

    os.makedirs(OUTPUT_HTML.parent, exist_ok=True)
    with open(OUTPUT_HTML, 'w', encoding='utf-8') as f:
        f.write(html)
    print(f"✅ 完了: {OUTPUT_HTML} ({os.path.getsize(OUTPUT_HTML):,} bytes)")


def _generate_html(grp_json, seasonal_json, rolling_json, yearly_json,
                   combo_json, dist_surf_json,
                   p_min, p_max, total, top3):
    now = datetime.now().strftime('%Y-%m-%d %H:%M')
    
    return f'''<!DOCTYPE html>
<html lang="ja">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>競馬タイム推移 v2</title>
<script src="https://cdn.plot.ly/plotly-2.27.0.min.js"></script>
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');
:root {{ --bg:#0f172a; --card:#1e293b; --text:#f1f5f9; --dim:#94a3b8; --pri:#6366f1; --acc:#22d3ee; --border:#334155; }}
* {{ box-sizing:border-box; margin:0; padding:0; }}
body {{ font-family:'Inter',sans-serif; background:var(--bg); color:var(--text); line-height:1.6; }}
.container {{ max-width:1400px; margin:0 auto; padding:20px; }}
.header {{ background:linear-gradient(135deg,#1e1b4b,#312e81,#4338ca); padding:24px 32px; border-radius:14px; margin-bottom:20px; }}
.header h1 {{ font-size:24px; font-weight:700; background:linear-gradient(90deg,#c7d2fe,#a5b4fc); -webkit-background-clip:text; -webkit-text-fill-color:transparent; }}
.header .sub {{ color:#a5b4fc; font-size:13px; margin-top:4px; }}
.tabs {{ display:flex; gap:3px; margin-bottom:16px; background:var(--card); padding:3px; border-radius:10px; border:1px solid var(--border); overflow-x:auto; }}
.tab {{ padding:8px 16px; border-radius:7px; font-size:12px; font-weight:500; cursor:pointer; white-space:nowrap; color:var(--dim); transition:all .2s; }}
.tab:hover {{ background:rgba(99,102,241,.1); color:var(--text); }}
.tab.active {{ background:var(--pri); color:#fff; }}
.panel {{ background:var(--card); border-radius:10px; padding:16px; margin-bottom:16px; border:1px solid var(--border); display:none; }}
.panel.active {{ display:block; }}
.panel h3 {{ font-size:15px; font-weight:600; color:#818cf8; margin-bottom:10px; }}
.desc {{ font-size:12px; color:var(--dim); margin-bottom:12px; background:rgba(99,102,241,.05); padding:10px; border-radius:6px; border-left:3px solid var(--pri); line-height:1.7; }}
.plot {{ width:100%; min-height:450px; }}
.ctrl {{ display:flex; flex-wrap:wrap; gap:8px; margin-bottom:12px; align-items:center; }}
.ctrl label {{ font-size:11px; color:var(--dim); }}
.ctrl select {{ background:var(--bg); color:var(--text); border:1px solid var(--border); padding:5px 8px; border-radius:5px; font-size:11px; font-family:inherit; }}
.ctrl select:focus {{ outline:none; border-color:var(--pri); }}
.insight {{ background:rgba(34,211,238,.05); border:1px solid rgba(34,211,238,.15); border-radius:8px; padding:14px; margin-top:12px; font-size:12px; line-height:1.8; }}
.insight h4 {{ color:var(--acc); margin-bottom:6px; font-size:13px; }}
.insight .item {{ margin:4px 0; padding-left:12px; border-left:2px solid var(--acc); }}
.vcols {{ color:#888; }}
</style>
</head>
<body>
<div class="container">
<div class="header">
  <h1>🏇 競馬タイム推移分析 v2 — 正しい比較軸</h1>
  <div class="sub">データ: {p_min}〜{p_max} | {total:,}件 (Top3: {top3:,}) | 生成: {now} | ⚠️ 同一(競馬場×距離×コース)単位で比較</div>
</div>

<div class="tabs">
  <div class="tab active" onclick="sw('t1')">📊 月次タイム推移</div>
  <div class="tab" onclick="sw('t2')">🌡️ 季節性分離</div>
  <div class="tab" onclick="sw('t3')">📏 同距離 競馬場間比較</div>
  <div class="tab" onclick="sw('t4')">📅 年次推移</div>
  <div class="tab" onclick="sw('t5')">🔍 PKL鮮度ドリフト</div>
</div>

<div class="panel active" id="t1">
  <h3>月次タイム推移（同一 競馬場×距離×コース）</h3>
  <div class="desc">
    ⚠️ <strong>v1の問題点</strong>: 全競馬場・全距離を混ぜて標準化→月ごとに競馬場構成が変わるためノイズが支配的だった。<br>
    ✅ <strong>v2</strong>: 「同一競馬場×同一距離×同一コース」に固定し、純粋なタイム推移のみを表示。月N≧{MIN_SAMPLES}でフィルタ。
  </div>
  <div class="ctrl">
    <label>競馬場:</label><select id="s1v" onchange="updateT1Dists()"></select>
    <label>コース:</label><select id="s1s" onchange="updateT1Dists()"><option value="芝">芝</option><option value="ダート">ダート</option></select>
    <label>距離:</label><select id="s1d" onchange="drawT1()"></select>
  </div>
  <div id="p1" class="plot"></div>
  <div class="insight" id="a1"></div>
</div>

<div class="panel" id="t2">
  <h3>季節性の分離（月別ベースライン vs 残差）</h3>
  <div class="desc">
    ⚠️ 1月は全期間で平均+0.28Zスコア遅く、5月は-0.24速い（振幅0.52）。これは能力差ではなく馬場状態の物理差。<br>
    ✅ 各(競馬場×距離×コース)の「月別平均」をベースラインとして差し引き、季節を除去した残差トレンドを表示。
  </div>
  <div class="ctrl">
    <label>競馬場:</label><select id="s2v" onchange="updateT2Dists()"></select>
    <label>コース:</label><select id="s2s" onchange="updateT2Dists()"><option value="芝">芝</option><option value="ダート">ダート</option></select>
    <label>距離:</label><select id="s2d" onchange="drawT2()"></select>
  </div>
  <div id="p2" class="plot"></div>
  <div class="insight" id="a2"></div>
</div>

<div class="panel" id="t3">
  <h3>同距離・同コースでの競馬場間タイム比較</h3>
  <div class="desc">
    ⚠️ 同じ芝2000mでも東京120.80s vs 中山121.94s（1.14s差）。異なる競馬場を混ぜると不正確。<br>
    ✅ 距離×コースを固定し、各競馬場の年次平均タイムを並列表示。競馬場間の「持ち時計差」が一目でわかる。
  </div>
  <div class="ctrl">
    <label>コース:</label><select id="s3s" onchange="updateT3Dists()"><option value="芝">芝</option><option value="ダート">ダート</option></select>
    <label>距離:</label><select id="s3d" onchange="drawT3()"></select>
  </div>
  <div id="p3" class="plot"></div>
  <div class="insight" id="a3"></div>
</div>

<div class="panel" id="t4">
  <h3>年次タイム推移（競馬場×距離×コース固定）</h3>
  <div class="desc">
    ⚠️ v1の年次サマリーは全競馬場合算→構成比変動が混入。<br>
    ✅ 特定条件に固定した年次平均。馬場改修や長期高速化トレンドを正確に把握できる。
  </div>
  <div class="ctrl">
    <label>競馬場:</label><select id="s4v" onchange="updateT4Dists()"></select>
    <label>コース:</label><select id="s4s" onchange="updateT4Dists()"><option value="芝">芝</option><option value="ダート">ダート</option></select>
    <label>距離:</label><select id="s4d" onchange="drawT4()"></select>
  </div>
  <div id="p4" class="plot"></div>
  <div class="insight" id="a4"></div>
</div>

<div class="panel" id="t5">
  <h3>PKL統計値のドリフト分析</h3>
  <div class="desc">
    ⚠️ PKLの平均値は全期間(2014〜)の固定値。年間-0.026Zスコアの高速化トレンドにより、10年で約0.26Zスコア分ずれる。<br>
    ✅ 3年ローリング平均と全期間平均の差（ドリフト）を表示。負のドリフト=最近のほうが速い=PKLが古い。
  </div>
  <div class="ctrl">
    <label>競馬場:</label><select id="s5v" onchange="updateT5Dists()"></select>
    <label>コース:</label><select id="s5s" onchange="updateT5Dists()"><option value="芝">芝</option><option value="ダート">ダート</option></select>
    <label>距離:</label><select id="s5d" onchange="drawT5()"></select>
  </div>
  <div id="p5" class="plot"></div>
  <div class="insight" id="a5"></div>
</div>

</div>
<script>
const GRP={grp_json};
const SEASONAL={seasonal_json};
const ROLLING={rolling_json};
const YEARLY={yearly_json};
const COMBOS={combo_json};
const DIST_SURF={dist_surf_json};

const VC={{
  '札幌':'#2196F3','函館':'#00BCD4','福島':'#4CAF50','新潟':'#8BC34A',
  '東京':'#FF9800','中山':'#F44336','中京':'#9C27B0','京都':'#E91E63',
  '阪神':'#3F51B5','小倉':'#009688'
}};
const LB={{paper_bgcolor:'#1e293b',plot_bgcolor:'#0f172a',font:{{family:'Inter',color:'#f1f5f9',size:11}},margin:{{l:55,r:25,t:35,b:50}},xaxis:{{gridcolor:'rgba(148,163,184,.1)',linecolor:'#334155'}},yaxis:{{gridcolor:'rgba(148,163,184,.1)',linecolor:'#334155'}},hovermode:'x unified',legend:{{bgcolor:'rgba(30,41,59,.9)',bordercolor:'#334155',borderwidth:1}}}};

function sw(id){{
  document.querySelectorAll('.panel').forEach(e=>e.classList.remove('active'));
  document.querySelectorAll('.tab').forEach(e=>e.classList.remove('active'));
  document.getElementById(id).classList.add('active');
  const idx=['t1','t2','t3','t4','t5'].indexOf(id);
  document.querySelectorAll('.tab')[idx].classList.add('active');
  setTimeout(()=>{{if(id==='t1')drawT1();if(id==='t2')drawT2();if(id==='t3')drawT3();if(id==='t4')drawT4();if(id==='t5')drawT5();}},50);
}}

function getVenues(surf){{return[...new Set(COMBOS.filter(c=>c.track_surface===surf).map(c=>c.venue))].sort();}}
function getDists(venue,surf){{return[...new Set(COMBOS.filter(c=>c.venue===venue&&c.track_surface===surf).map(c=>c.distance_m))].sort((a,b)=>a-b);}}

function fillSelect(sel,opts,valFn,txtFn){{
  const prev=sel.value;sel.innerHTML='';
  opts.forEach(o=>{{const op=document.createElement('option');op.value=valFn(o);op.text=txtFn(o);sel.appendChild(op);}});
  if([...sel.options].some(o=>o.value===prev))sel.value=prev;
}}

// Tab1
function updateT1Dists(){{
  const v=document.getElementById('s1v').value,s=document.getElementById('s1s').value;
  fillSelect(document.getElementById('s1d'),getDists(v,s),d=>d,d=>d+'m');
  drawT1();
}}
function initT1(){{
  const s=document.getElementById('s1s').value;
  fillSelect(document.getElementById('s1v'),getVenues(s),v=>v,v=>v);
  updateT1Dists();
}}
function drawT1(){{
  const v=document.getElementById('s1v').value,s=document.getElementById('s1s').value,d=parseInt(document.getElementById('s1d').value);
  const data=GRP.filter(r=>r.venue===v&&r.track_surface===s&&r.distance_m===d);
  if(!data.length)return;
  const trace={{x:data.map(r=>r.ym),y:data.map(r=>r.time_mean),type:'scatter',mode:'lines+markers',name:'月次平均タイム',line:{{color:'#6366f1',width:2}},marker:{{size:4}},hovertemplate:'%{{x}}<br>%{{y:.2f}}秒 (N=%{{customdata}})<extra></extra>',customdata:data.map(r=>r.count)}};
  // 12point MA
  const ma=[];const ys=data.map(r=>r.time_mean);
  for(let i=0;i<ys.length;i++){{if(i<11)ma.push(null);else{{let sum=0;for(let j=i-11;j<=i;j++)sum+=ys[j];ma.push(sum/12);}}}}
  const trMA={{x:data.map(r=>r.ym),y:ma,type:'scatter',mode:'lines',name:'12M移動平均',line:{{color:'#f59e0b',width:2.5,dash:'dot'}}}};
  Plotly.newPlot('p1',[trace,trMA],{{...LB,title:{{text:`${{v}} ${{s}}${{d}}m 月次平均タイム (Top3)`,font:{{size:14}}}},yaxis:{{...LB.yaxis,title:'タイム(秒)',autorange:'reversed'}},xaxis:{{...LB.xaxis,title:'年月'}}}},{{responsive:true}});
  // analysis
  const recent=data.slice(-12),old=data.slice(0,12);
  if(recent.length>0&&old.length>0){{
    const rAvg=recent.reduce((a,r)=>a+r.time_mean,0)/recent.length;
    const oAvg=old.reduce((a,r)=>a+r.time_mean,0)/old.length;
    const diff=rAvg-oAvg;
    document.getElementById('a1').innerHTML=`<h4>🔬 分析</h4><div class="item">直近12M平均: ${{rAvg.toFixed(2)}}秒</div><div class="item">初期12M平均: ${{oAvg.toFixed(2)}}秒</div><div class="item">変化: ${{diff>0?'+':''}}${{diff.toFixed(2)}}秒 (${{diff<0?'高速化':'低速化'}})</div>`;
  }}
}}

// Tab2
function updateT2Dists(){{
  const v=document.getElementById('s2v').value,s=document.getElementById('s2s').value;
  fillSelect(document.getElementById('s2d'),getDists(v,s),d=>d,d=>d+'m');drawT2();
}}
function initT2(){{const s=document.getElementById('s2s').value;fillSelect(document.getElementById('s2v'),getVenues(s),v=>v,v=>v);updateT2Dists();}}
function drawT2(){{
  const v=document.getElementById('s2v').value,s=document.getElementById('s2s').value,d=parseInt(document.getElementById('s2d').value);
  const data=GRP.filter(r=>r.venue===v&&r.track_surface===s&&r.distance_m===d);
  const seas=SEASONAL.filter(r=>r.venue===v&&r.track_surface===s&&r.distance_m===d);
  if(!data.length)return;
  const seasMap={{}};seas.forEach(r=>seasMap[r.month]=r.seasonal_mean);
  const overall=data.reduce((a,r)=>a+r.time_mean*r.count,0)/data.reduce((a,r)=>a+r.count,0);
  // seasonal bar
  const months=Array.from({{length:12}},(_, i)=>i+1);
  const seasVals=months.map(m=>seasMap[m]?seasMap[m]-overall:null);
  const trSeas={{x:months.map(m=>m+'月'),y:seasVals,type:'bar',name:'月別偏差(季節性)',marker:{{color:seasVals.map(v=>v&&v<0?'#4ade80':'#f87171'),opacity:0.7}},yaxis:'y2'}};
  // residual line
  const residuals=data.map(r=>{{const m=parseInt(r.ym.split('-')[1]);const base=seasMap[m]||overall;return{{ym:r.ym,residual:r.time_mean-base}};}});
  const trRes={{x:residuals.map(r=>r.ym),y:residuals.map(r=>r.residual),type:'scatter',mode:'lines+markers',name:'季節除去済み残差',line:{{color:'#6366f1',width:2}},marker:{{size:3}}}};
  Plotly.newPlot('p2',[trRes,trSeas],{{...LB,title:{{text:`${{v}} ${{s}}${{d}}m 季節性分離`,font:{{size:14}}}},yaxis:{{...LB.yaxis,title:'残差(秒)',zeroline:true,zerolinecolor:'rgba(248,113,113,.5)',zerolinewidth:2}},yaxis2:{{title:'月別偏差(秒)',overlaying:'y',side:'right',gridcolor:'rgba(148,163,184,.05)',showgrid:false}},xaxis:{{...LB.xaxis,title:'年月'}}}},{{responsive:true}});
  const maxM=months[seasVals.indexOf(Math.max(...seasVals.filter(v=>v!==null)))];
  const minM=months[seasVals.indexOf(Math.min(...seasVals.filter(v=>v!==null)))];
  const swing=Math.max(...seasVals.filter(v=>v!==null))-Math.min(...seasVals.filter(v=>v!==null));
  document.getElementById('a2').innerHTML=`<h4>🌡️ 季節性分析</h4><div class="item">最も遅い月: ${{maxM}}月, 最も速い月: ${{minM}}月</div><div class="item">季節振幅: ${{swing.toFixed(2)}}秒</div><div class="item">紫線（残差）が季節を除外した純粋なトレンド。右下がり=高速化。</div>`;
}}

// Tab3
function updateT3Dists(){{
  const s=document.getElementById('s3s').value;
  const dists=[...new Set(DIST_SURF.filter(r=>r.track_surface===s).map(r=>r.distance_m))].sort((a,b)=>a-b);
  fillSelect(document.getElementById('s3d'),dists,d=>d,d=>d+'m');drawT3();
}}
function initT3(){{updateT3Dists();}}
function drawT3(){{
  const s=document.getElementById('s3s').value,d=parseInt(document.getElementById('s3d').value);
  const entry=DIST_SURF.find(r=>r.track_surface===s&&r.distance_m===d);
  if(!entry)return;
  const venues=entry.venues;
  const traces=venues.map(v=>{{
    const data=YEARLY.filter(r=>r.venue===v&&r.track_surface===s&&r.distance_m===d);
    return{{x:data.map(r=>r.year.toString()),y:data.map(r=>r.time_mean),type:'scatter',mode:'lines+markers',name:v,line:{{color:VC[v]||'#888',width:2}},marker:{{size:5}},hovertemplate:`${{v}}: %{{y:.2f}}秒 (N=%{{customdata}})<extra></extra>`,customdata:data.map(r=>r.count)}};
  }});
  Plotly.newPlot('p3',traces,{{...LB,title:{{text:`${{s}}${{d}}m 競馬場間年次比較`,font:{{size:14}}}},yaxis:{{...LB.yaxis,title:'平均タイム(秒)',autorange:'reversed'}},xaxis:{{...LB.xaxis,title:'年'}}}},{{responsive:true}});
  // analysis
  const latest=YEARLY.filter(r=>r.track_surface===s&&r.distance_m===d&&r.year>=2024);
  if(latest.length){{
    let html='<h4>📏 競馬場間タイム差 (2024-)</h4>';
    const vAvg={{}};venues.forEach(v=>{{const vd=latest.filter(r=>r.venue===v);if(vd.length)vAvg[v]=vd.reduce((a,r)=>a+r.time_mean,0)/vd.length;}});
    const sorted=Object.entries(vAvg).sort((a,b)=>a[1]-b[1]);
    if(sorted.length>0){{const fastest=sorted[0][1];sorted.forEach(([v,t])=>{{html+=`<div class="item">${{v}}: ${{t.toFixed(2)}}秒 (最速比 +${{(t-fastest).toFixed(2)}}秒)</div>`;}});}}
    document.getElementById('a3').innerHTML=html;
  }}
}}

// Tab4
function updateT4Dists(){{const v=document.getElementById('s4v').value,s=document.getElementById('s4s').value;fillSelect(document.getElementById('s4d'),getDists(v,s),d=>d,d=>d+'m');drawT4();}}
function initT4(){{const s=document.getElementById('s4s').value;fillSelect(document.getElementById('s4v'),getVenues(s),v=>v,v=>v);updateT4Dists();}}
function drawT4(){{
  const v=document.getElementById('s4v').value,s=document.getElementById('s4s').value,d=parseInt(document.getElementById('s4d').value);
  const data=YEARLY.filter(r=>r.venue===v&&r.track_surface===s&&r.distance_m===d).sort((a,b)=>a.year-b.year);
  if(!data.length)return;
  const trTime={{x:data.map(r=>r.year.toString()),y:data.map(r=>r.time_mean),type:'bar',name:'年次平均タイム',marker:{{color:data.map(r=>{{const avg=data.reduce((a,r2)=>a+r2.time_mean,0)/data.length;return r.time_mean<avg?'#4ade80':'#f87171';}}),opacity:.8}},hovertemplate:'%{{x}}年: %{{y:.2f}}秒<extra></extra>'}};
  const trL3f={{x:data.map(r=>r.year.toString()),y:data.map(r=>r.l3f_mean),type:'scatter',mode:'lines+markers',name:'上がり3F平均',line:{{color:'#22d3ee',width:2}},marker:{{size:5}},yaxis:'y2'}};
  Plotly.newPlot('p4',[trTime,trL3f],{{...LB,title:{{text:`${{v}} ${{s}}${{d}}m 年次推移`,font:{{size:14}}}},yaxis:{{...LB.yaxis,title:'平均タイム(秒)',autorange:'reversed'}},yaxis2:{{title:'上がり3F(秒)',overlaying:'y',side:'right',autorange:'reversed',gridcolor:'rgba(148,163,184,.05)'}},xaxis:{{...LB.xaxis,title:'年'}}}},{{responsive:true}});
  if(data.length>=4){{
    const first2=data.slice(0,2),last2=data.slice(-2);
    const fAvg=first2.reduce((a,r)=>a+r.time_mean,0)/2;
    const lAvg=last2.reduce((a,r)=>a+r.time_mean,0)/2;
    const diff=lAvg-fAvg;const years=data[data.length-1].year-data[0].year;
    document.getElementById('a4').innerHTML=`<h4>📅 長期トレンド</h4><div class="item">初期2年平均: ${{fAvg.toFixed(2)}}秒 → 直近2年平均: ${{lAvg.toFixed(2)}}秒</div><div class="item">${{years}}年間で${{diff>0?'+':''}}${{diff.toFixed(2)}}秒 (${{diff<0?'高速化':'低速化'}})</div><div class="item">年あたり: ${{(diff/years).toFixed(3)}}秒/年</div>`;
  }}
}}

// Tab5
function updateT5Dists(){{const v=document.getElementById('s5v').value,s=document.getElementById('s5s').value;
  const dists=[...new Set(ROLLING.filter(r=>r.venue===v&&r.track_surface===s).map(r=>r.distance_m))].sort((a,b)=>a-b);
  fillSelect(document.getElementById('s5d'),dists,d=>d,d=>d+'m');drawT5();}}
function initT5(){{const s=document.getElementById('s5s').value;
  const venues=[...new Set(ROLLING.filter(r=>r.track_surface===s).map(r=>r.venue))].sort();
  fillSelect(document.getElementById('s5v'),venues,v=>v,v=>v);updateT5Dists();}}
function drawT5(){{
  const v=document.getElementById('s5v').value,s=document.getElementById('s5s').value,d=parseInt(document.getElementById('s5d').value);
  const data=ROLLING.filter(r=>r.venue===v&&r.track_surface===s&&r.distance_m===d).sort((a,b)=>a.year-b.year);
  if(!data.length)return;
  const trDrift={{x:data.map(r=>r.year.toString()),y:data.map(r=>r.drift),type:'bar',name:'ドリフト(3年平均-全期間平均)',marker:{{color:data.map(r=>r.drift<0?'#4ade80':'#f87171'),opacity:.8}},hovertemplate:'%{{x}}年: %{{y:.3f}}秒<extra></extra>'}};
  const trZero={{x:[data[0].year.toString(),data[data.length-1].year.toString()],y:[0,0],type:'scatter',mode:'lines',name:'全期間平均(=0)',line:{{color:'rgba(248,113,113,.5)',width:2,dash:'dash'}},showlegend:false}};
  Plotly.newPlot('p5',[trDrift,trZero],{{...LB,title:{{text:`${{v}} ${{s}}${{d}}m PKL統計値ドリフト`,font:{{size:14}}}},yaxis:{{...LB.yaxis,title:'ドリフト(秒) 負=高速化'}},xaxis:{{...LB.xaxis,title:'基準年'}}}},{{responsive:true}});
  const latest=data[data.length-1];
  document.getElementById('a5').innerHTML=`<h4>🔍 PKL鮮度</h4><div class="item">全期間平均: ${{latest.full_mean}}秒</div><div class="item">直近3年平均: ${{latest.roll3y_mean}}秒</div><div class="item">ドリフト: ${{latest.drift>0?'+':''}}${{latest.drift}}秒 (${{latest.drift<0?'PKLは現在より遅い値=最近の馬を過大評価':'PKLは現在より速い値'}})</div><div class="item">PKL上がり3F: ${{latest.full_l3f}}秒 → 直近3年: ${{latest.roll3y_l3f}}秒</div>`;
}}

// Init
initT1();initT2();initT3();initT4();initT5();
drawT1();
</script>
</body>
</html>'''


if __name__ == "__main__":
    build()
