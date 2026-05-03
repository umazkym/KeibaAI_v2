#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
平均タイム推移ダッシュボード

races.parquet から全期間・全競馬場・全距離の平均タイム推移を分析し、
Plotly インタラクティブ HTML ダッシュボードを生成する。
同時に、.pklファイルの鮮度を診断する。
"""

import os
import sys
import pickle
import logging
from pathlib import Path
from datetime import datetime

import pandas as pd
import numpy as np

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# ===== Paths =====
PARQUET_PATH = PROJECT_ROOT / "keibaai" / "data" / "parsed" / "parquet" / "races" / "races.parquet"
PKL_DIR = PROJECT_ROOT / "keibaai" / "data" / "processed"
OUTPUT_HTML = PROJECT_ROOT / "outputs" / "reports" / "all_venues_time_trends.html"

STATS_PKL = PKL_DIR / "stats_lookup.pkl"
GLOBAL_PKL = PKL_DIR / "global_stats_lookup.pkl"
VENUE_PKL = PKL_DIR / "venue_adjustment.pkl"


def check_freshness():
    """pklファイルとParquetの鮮度を診断"""
    print("\n" + "=" * 70)
    print("📊 データ鮮度診断レポート")
    print("=" * 70)

    files = {
        "races.parquet (レースデータ本体)": PARQUET_PATH,
        "stats_lookup.pkl (会場別統計)": STATS_PKL,
        "global_stats_lookup.pkl (グローバル統計)": GLOBAL_PKL,
        "venue_adjustment.pkl (競馬場補正)": VENUE_PKL,
    }

    freshness_results = {}

    for name, path in files.items():
        if path.exists():
            mtime = datetime.fromtimestamp(os.path.getmtime(path))
            size = os.path.getsize(path)
            days_old = (datetime.now() - mtime).days
            freshness_results[name] = {
                'mtime': mtime,
                'size': size,
                'days_old': days_old,
                'exists': True
            }
            status = "🟢 最新" if days_old <= 7 else "🟡 やや古い" if days_old <= 30 else "🔴 古い"
            print(f"\n{status} {name}")
            print(f"   最終更新: {mtime.strftime('%Y-%m-%d %H:%M:%S')} ({days_old}日前)")
            print(f"   サイズ: {size:,} bytes")
        else:
            freshness_results[name] = {'exists': False}
            print(f"\n❌ {name} - ファイルが見つかりません!")

    # Parquet の日付範囲を確認
    if PARQUET_PATH.exists():
        print("\n" + "-" * 50)
        print("📅 Parquetデータの日付範囲:")
        df = pd.read_parquet(PARQUET_PATH, columns=['race_date', 'venue', 'distance_m', 'track_surface'])
        df['race_date'] = pd.to_datetime(df['race_date'])
        print(f"   開始日: {df['race_date'].min().strftime('%Y-%m-%d')}")
        print(f"   終了日: {df['race_date'].max().strftime('%Y-%m-%d')}")
        print(f"   レコード数: {len(df):,}")
        print(f"   競馬場数: {df['venue'].nunique()} ({', '.join(sorted(df['venue'].unique()))})")

        # PKLファイルがParquetよりも古いかチェック
        parquet_max_date = df['race_date'].max()
        for name, path in [("stats_lookup.pkl", STATS_PKL), ("global_stats_lookup.pkl", GLOBAL_PKL), ("venue_adjustment.pkl", VENUE_PKL)]:
            if path.exists():
                pkl_mtime = datetime.fromtimestamp(os.path.getmtime(path))
                if pkl_mtime < parquet_max_date.to_pydatetime().replace(tzinfo=None):
                    print(f"\n   ⚠️  {name} はParquetの最新データ({parquet_max_date.strftime('%Y-%m-%d')})よりも古い更新日時({pkl_mtime.strftime('%Y-%m-%d')})です！")
                    print(f"       → 再構築推奨: python scripts/pipelines/build_stats_lookup.py")
                    print(f"       → python scripts/pipelines/build_global_stats.py")

    # PKL中身の確認
    if STATS_PKL.exists():
        with open(STATS_PKL, 'rb') as f:
            stats = pickle.load(f)
        print(f"\n📋 stats_lookup.pkl の内容:")
        print(f"   条件数: {len(stats)}")
        # サンプル表示
        sample_keys = list(stats.keys())[:5]
        for k in sample_keys:
            v = stats[k]
            print(f"   {k}: mean={v['time_mean']:.2f}s, std={v['time_std']:.2f}")

    if GLOBAL_PKL.exists():
        with open(GLOBAL_PKL, 'rb') as f:
            gstats = pickle.load(f)
        print(f"\n📋 global_stats_lookup.pkl の内容:")
        print(f"   条件数: {len(gstats)}")

    if VENUE_PKL.exists():
        with open(VENUE_PKL, 'rb') as f:
            vadj = pickle.load(f)
        print(f"\n📋 venue_adjustment.pkl の内容:")
        print(f"   条件数: {len(vadj)}")

    return freshness_results


def build_dashboard():
    """全期間・全競馬場・全距離の平均タイム推移ダッシュボードをHTML生成"""

    if not PARQUET_PATH.exists():
        print(f"ERROR: {PARQUET_PATH} が見つかりません")
        return

    print("\n" + "=" * 70)
    print("📈 タイム推移ダッシュボード生成中...")
    print("=" * 70)

    # ===== データ読み込み =====
    df = pd.read_parquet(PARQUET_PATH, columns=[
        'race_date', 'venue', 'distance_m', 'track_surface',
        'track_condition', 'finish_position', 'finish_time_seconds', 'last_3f_time'
    ])
    df['race_date'] = pd.to_datetime(df['race_date'])
    df['finish_time_seconds'] = pd.to_numeric(df['finish_time_seconds'], errors='coerce')
    df['last_3f_time'] = pd.to_numeric(df['last_3f_time'], errors='coerce')
    df = df.dropna(subset=['finish_time_seconds'])

    # track_surface / venue に None がある行を除外
    df = df.dropna(subset=['track_surface', 'venue', 'distance_m'])
    # 障害レースなど不明なコースを除外
    df = df[df['track_surface'].isin(['芝', 'ダート'])].copy()

    # Top3のみ（平均値として信頼性の高い範囲）
    df_top3 = df[df['finish_position'].isin([1, 2, 3])].copy()

    # 年月でグルーピング
    df_top3['year_month'] = df_top3['race_date'].dt.to_period('M').astype(str)
    df_top3['year'] = df_top3['race_date'].dt.year

    # ===== 分析1: 競馬場別・距離別の月次平均タイム =====
    venues = sorted(df_top3['venue'].unique())
    surfaces = sorted(df_top3['track_surface'].unique())

    # 主要距離のみ
    popular_dists = df_top3.groupby(['track_surface', 'distance_m']).size().reset_index(name='count')
    popular_dists = popular_dists[popular_dists['count'] >= 50].sort_values('distance_m')

    # ===== 分析2: 全体の年別平均タイム推移（距離標準化） =====
    # 距離毎に標準化してから平均
    print("  距離標準化タイム算出中...")

    # 各(距離, コース)グループの全体平均と標準偏差を計算
    group_stats = df_top3.groupby(['track_surface', 'distance_m']).agg(
        time_mean=('finish_time_seconds', 'mean'),
        time_std=('finish_time_seconds', 'std'),
        l3f_mean=('last_3f_time', 'mean'),
        l3f_std=('last_3f_time', 'std')
    ).reset_index()

    df_top3 = df_top3.merge(group_stats, on=['track_surface', 'distance_m'], how='left')

    # Zスコア化
    df_top3['time_zscore'] = (df_top3['finish_time_seconds'] - df_top3['time_mean']) / df_top3['time_std'].clip(lower=0.5)
    df_top3['l3f_zscore'] = (df_top3['last_3f_time'] - df_top3['l3f_mean']) / df_top3['l3f_std'].clip(lower=0.3)

    # ===== 月次・競馬場別のZスコア推移 =====
    monthly_venue = df_top3.groupby(['year_month', 'venue']).agg(
        time_zscore_mean=('time_zscore', 'mean'),
        l3f_zscore_mean=('l3f_zscore', 'mean'),
        count=('time_zscore', 'size')
    ).reset_index()

    # 全体月次推移
    monthly_all = df_top3.groupby('year_month').agg(
        time_zscore_mean=('time_zscore', 'mean'),
        l3f_zscore_mean=('l3f_zscore', 'mean'),
        count=('time_zscore', 'size')
    ).reset_index()

    # ===== 月次・距離・コース別の平均タイム =====
    monthly_dist_course = df_top3.groupby(['year_month', 'track_surface', 'distance_m']).agg(
        time_mean_actual=('finish_time_seconds', 'mean'),
        l3f_mean_actual=('last_3f_time', 'mean'),
        count=('finish_time_seconds', 'size')
    ).reset_index()

    # ===== 年次推移 =====
    yearly_venue = df_top3.groupby(['year', 'venue']).agg(
        time_zscore_mean=('time_zscore', 'mean'),
        l3f_zscore_mean=('l3f_zscore', 'mean'),
        count=('time_zscore', 'size')
    ).reset_index()

    yearly_all = df_top3.groupby('year').agg(
        time_zscore_mean=('time_zscore', 'mean'),
        l3f_zscore_mean=('l3f_zscore', 'mean'),
        count=('time_zscore', 'size')
    ).reset_index()

    # ===== PKL鮮度情報 =====
    pkl_info = {}
    for name, path in [("stats_lookup", STATS_PKL), ("global_stats", GLOBAL_PKL), ("venue_adj", VENUE_PKL)]:
        if path.exists():
            mtime = datetime.fromtimestamp(os.path.getmtime(path))
            pkl_info[name] = {
                'date': mtime.strftime('%Y-%m-%d %H:%M'),
                'days_old': (datetime.now() - mtime).days,
                'size': os.path.getsize(path)
            }

    parquet_min = df_top3['race_date'].min().strftime('%Y-%m-%d')
    parquet_max = df_top3['race_date'].max().strftime('%Y-%m-%d')
    parquet_count = len(df)
    top3_count = len(df_top3)

    # ===== HTML生成 =====
    print("  HTMLダッシュボード生成中...")

    # データをJSON化
    import json

    # 月次全体
    monthly_all_json = monthly_all.to_json(orient='records', force_ascii=False)

    # 月次会場別
    monthly_venue_json = monthly_venue.to_json(orient='records', force_ascii=False)

    # 月次距離コース別
    monthly_dist_course_json = monthly_dist_course.to_json(orient='records', force_ascii=False)

    # 年次全体
    yearly_all_json = yearly_all.to_json(orient='records', force_ascii=False)

    # 年次会場別
    yearly_venue_json = yearly_venue.to_json(orient='records', force_ascii=False)

    # 競馬場リスト
    venues_json = json.dumps(venues, ensure_ascii=False)

    # コースリスト
    surfaces_json = json.dumps(list(surfaces), ensure_ascii=False)

    # 主要距離リスト（コース別）
    popular_dists_list = popular_dists.to_dict('records')
    popular_dists_json = json.dumps(popular_dists_list, ensure_ascii=False)

    # PKL情報
    pkl_info_json = json.dumps(pkl_info, ensure_ascii=False)

    html = f"""<!DOCTYPE html>
<html lang="ja">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>競馬タイム推移ダッシュボード</title>
    <script src="https://cdn.plot.ly/plotly-2.27.0.min.js"></script>
    <style>
        @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');

        :root {{
            --bg: #0f172a;
            --card: #1e293b;
            --card-hover: #334155;
            --text: #f1f5f9;
            --text-dim: #94a3b8;
            --primary: #6366f1;
            --primary-light: #818cf8;
            --accent: #22d3ee;
            --warning: #fbbf24;
            --danger: #f87171;
            --success: #4ade80;
            --border: #334155;
        }}

        * {{ box-sizing: border-box; margin: 0; padding: 0; }}

        body {{
            font-family: 'Inter', sans-serif;
            background: var(--bg);
            color: var(--text);
            line-height: 1.6;
        }}

        .container {{
            max-width: 1400px;
            margin: 0 auto;
            padding: 20px;
        }}

        .header {{
            background: linear-gradient(135deg, #1e1b4b, #312e81, #4338ca);
            padding: 30px 40px;
            border-radius: 16px;
            margin-bottom: 24px;
            box-shadow: 0 4px 30px rgba(99, 102, 241, 0.3);
        }}

        .header h1 {{
            font-size: 28px;
            font-weight: 700;
            background: linear-gradient(90deg, #c7d2fe, #a5b4fc, #818cf8);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
            margin-bottom: 8px;
        }}

        .header .subtitle {{
            color: #a5b4fc;
            font-size: 14px;
        }}

        .freshness-panel {{
            background: var(--card);
            border-radius: 12px;
            padding: 20px;
            margin-bottom: 24px;
            border: 1px solid var(--border);
        }}

        .freshness-panel h2 {{
            font-size: 18px;
            font-weight: 600;
            margin-bottom: 16px;
            color: var(--accent);
        }}

        .freshness-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
            gap: 12px;
        }}

        .freshness-item {{
            background: rgba(255, 255, 255, 0.03);
            border-radius: 8px;
            padding: 14px 16px;
            border: 1px solid rgba(255, 255, 255, 0.06);
        }}

        .freshness-item .label {{
            font-size: 12px;
            color: var(--text-dim);
            margin-bottom: 4px;
        }}

        .freshness-item .value {{
            font-size: 14px;
            font-weight: 500;
        }}

        .status-dot {{
            display: inline-block;
            width: 8px;
            height: 8px;
            border-radius: 50%;
            margin-right: 6px;
        }}

        .status-good {{ background: var(--success); }}
        .status-warn {{ background: var(--warning); }}
        .status-bad {{ background: var(--danger); }}

        .tabs {{
            display: flex;
            gap: 4px;
            margin-bottom: 20px;
            background: var(--card);
            padding: 4px;
            border-radius: 12px;
            border: 1px solid var(--border);
            overflow-x: auto;
        }}

        .tab {{
            padding: 10px 20px;
            border-radius: 8px;
            font-size: 13px;
            font-weight: 500;
            cursor: pointer;
            transition: all 0.2s;
            white-space: nowrap;
            color: var(--text-dim);
        }}

        .tab:hover {{ background: rgba(99, 102, 241, 0.1); color: var(--text); }}
        .tab.active {{ background: var(--primary); color: white; }}

        .chart-container {{
            background: var(--card);
            border-radius: 12px;
            padding: 20px;
            margin-bottom: 20px;
            border: 1px solid var(--border);
            display: none;
        }}

        .chart-container.active {{ display: block; }}

        .chart-container h3 {{
            font-size: 16px;
            font-weight: 600;
            margin-bottom: 16px;
            color: var(--primary-light);
        }}

        .chart-desc {{
            font-size: 13px;
            color: var(--text-dim);
            margin-bottom: 16px;
            background: rgba(99, 102, 241, 0.05);
            padding: 12px;
            border-radius: 8px;
            border-left: 3px solid var(--primary);
        }}

        .chart-plot {{ width: 100%; min-height: 500px; }}

        .filter-bar {{
            display: flex;
            flex-wrap: wrap;
            gap: 8px;
            margin-bottom: 16px;
            align-items: center;
        }}

        .filter-label {{ font-size: 12px; color: var(--text-dim); margin-right: 4px; }}

        select, input {{
            background: var(--bg);
            color: var(--text);
            border: 1px solid var(--border);
            padding: 6px 12px;
            border-radius: 6px;
            font-size: 12px;
            font-family: 'Inter', sans-serif;
        }}

        select:focus, input:focus {{ outline: none; border-color: var(--primary); }}

        .analysis-box {{
            background: rgba(34, 211, 238, 0.05);
            border: 1px solid rgba(34, 211, 238, 0.15);
            border-radius: 10px;
            padding: 16px;
            margin-top: 16px;
            font-size: 13px;
            line-height: 1.7;
        }}

        .analysis-box h4 {{ color: var(--accent); margin-bottom: 8px; }}
        .analysis-box .insight {{ margin: 8px 0; padding-left: 16px; border-left: 2px solid var(--accent); }}

        .summary-stats {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
            gap: 12px;
            margin-bottom: 20px;
        }}

        .stat-card {{
            background: rgba(255, 255, 255, 0.03);
            border-radius: 8px;
            padding: 16px;
            text-align: center;
            border: 1px solid rgba(255, 255, 255, 0.06);
        }}

        .stat-card .stat-value {{
            font-size: 24px;
            font-weight: 700;
            color: var(--primary-light);
        }}

        .stat-card .stat-label {{
            font-size: 11px;
            color: var(--text-dim);
            margin-top: 4px;
        }}

        footer {{
            text-align: center;
            padding: 20px;
            color: var(--text-dim);
            font-size: 12px;
        }}
    </style>
</head>
<body>
<div class="container">

<div class="header">
    <h1>🏇 競馬タイム推移分析ダッシュボード</h1>
    <div class="subtitle">
        データ期間: {parquet_min} 〜 {parquet_max} |
        レコード数: {parquet_count:,}件 (Top3: {top3_count:,}件) |
        生成日: {datetime.now().strftime('%Y-%m-%d %H:%M')}
    </div>
</div>

<div class="summary-stats">
    <div class="stat-card">
        <div class="stat-value">{parquet_count:,}</div>
        <div class="stat-label">総レコード数</div>
    </div>
    <div class="stat-card">
        <div class="stat-value">{top3_count:,}</div>
        <div class="stat-label">Top3フィニッシャー</div>
    </div>
    <div class="stat-card">
        <div class="stat-value">{len(venues)}</div>
        <div class="stat-label">競馬場数</div>
    </div>
    <div class="stat-card">
        <div class="stat-value">{parquet_min} 〜</div>
        <div class="stat-label">データ開始日</div>
    </div>
    <div class="stat-card">
        <div class="stat-value">{parquet_max}</div>
        <div class="stat-label">データ最終日</div>
    </div>
</div>

<div class="freshness-panel" id="freshnessPanel">
    <h2>🔍 データ鮮度診断</h2>
    <div class="freshness-grid" id="freshnessGrid"></div>
</div>

<div class="tabs" id="mainTabs">
    <div class="tab active" onclick="switchTab('tab1')">📊 全体 Zスコア推移</div>
    <div class="tab" onclick="switchTab('tab2')">🏟️ 競馬場別 推移</div>
    <div class="tab" onclick="switchTab('tab3')">📏 距離別 実タイム</div>
    <div class="tab" onclick="switchTab('tab4')">📅 年次サマリー</div>
    <div class="tab" onclick="switchTab('tab5')">🔬 上がり3F分析</div>
</div>

<!-- Tab 1: 全体Zスコア推移 -->
<div class="chart-container active" id="tab1">
    <h3>全体の標準化タイム推移（月次Zスコア）</h3>
    <div class="chart-desc">
        各(距離×コース)グループの全期間平均・標準偏差で標準化（Zスコア化）した後のタイムの月次推移。<br>
        <strong>Zスコア &lt; 0</strong> = 平均よりも速い月, <strong>Zスコア &gt; 0</strong> = 平均よりも遅い月。<br>
        距離の違いを打ち消し、純粋な「速さの推移」を可視化。
    </div>
    <div id="chart1" class="chart-plot"></div>
    <div class="analysis-box" id="analysis1"></div>
</div>

<!-- Tab 2: 競馬場別 -->
<div class="chart-container" id="tab2">
    <h3>競馬場別 標準化タイム推移（月次）</h3>
    <div class="chart-desc">
        各競馬場ごとのZスコア推移。馬場改修や季節変動を可視化。
    </div>
    <div class="filter-bar">
        <span class="filter-label">表示競馬場:</span>
        <select id="venueSelect" multiple size="5" onchange="updateTab2()">
        </select>
    </div>
    <div id="chart2" class="chart-plot"></div>
    <div class="analysis-box" id="analysis2"></div>
</div>

<!-- Tab 3: 距離別実タイム -->
<div class="chart-container" id="tab3">
    <h3>距離・コース別 実タイム推移（月次平均）</h3>
    <div class="chart-desc">
        生の平均タイム（秒）の推移。距離+コースを選択して表示。<br>
        季節変動（冬場は遅い傾向）や馬場状態の影響が反映される。
    </div>
    <div class="filter-bar">
        <span class="filter-label">コース:</span>
        <select id="surfaceSelect" onchange="updateTab3()">
            <option value="芝">芝</option>
            <option value="ダート">ダート</option>
        </select>
        <span class="filter-label">距離:</span>
        <select id="distSelect" onchange="updateTab3()"></select>
    </div>
    <div id="chart3" class="chart-plot"></div>
</div>

<!-- Tab 4: 年次サマリー -->
<div class="chart-container" id="tab4">
    <h3>年次サマリー（Zスコア平均）</h3>
    <div class="chart-desc">
        年ごとの平均Zスコア（タイム / 上がり3F）。年単位での傾向変化を確認。
    </div>
    <div id="chart4" class="chart-plot"></div>
    <div class="analysis-box" id="analysis4"></div>
</div>

<!-- Tab 5: 上がり3F分析 -->
<div class="chart-container" id="tab5">
    <h3>上がり3F Zスコア推移（月次）</h3>
    <div class="chart-desc">
        標準化された上がり3Fタイムの推移。ペース傾向の変化を可視化。<br>
        Zスコア &lt; 0 = 上がりが速い月。
    </div>
    <div id="chart5" class="chart-plot"></div>
    <div class="analysis-box" id="analysis5"></div>
</div>

</div>

<footer>
    KeibaAI_v2 Time Trend Dashboard | Generated {datetime.now().strftime('%Y-%m-%d %H:%M')}
</footer>

<script>
// ===== DATA =====
const MONTHLY_ALL = {monthly_all_json};
const MONTHLY_VENUE = {monthly_venue_json};
const MONTHLY_DIST = {monthly_dist_course_json};
const YEARLY_ALL = {yearly_all_json};
const YEARLY_VENUE = {yearly_venue_json};
const VENUES = {venues_json};
const SURFACES = {surfaces_json};
const POPULAR_DISTS = {popular_dists_json};
const PKL_INFO = {pkl_info_json};

const VENUE_COLORS = {{
    '札幌': '#2196F3', '函館': '#00BCD4', '福島': '#4CAF50', '新潟': '#8BC34A',
    '東京': '#FF9800', '中山': '#F44336', '中京': '#9C27B0', '京都': '#E91E63',
    '阪神': '#3F51B5', '小倉': '#009688'
}};

const PLOTLY_LAYOUT_BASE = {{
    paper_bgcolor: '#1e293b',
    plot_bgcolor: '#0f172a',
    font: {{ family: 'Inter, sans-serif', color: '#f1f5f9', size: 12 }},
    margin: {{ l: 60, r: 30, t: 40, b: 60 }},
    xaxis: {{
        gridcolor: 'rgba(148, 163, 184, 0.1)',
        linecolor: '#334155',
        tickcolor: '#475569',
    }},
    yaxis: {{
        gridcolor: 'rgba(148, 163, 184, 0.1)',
        linecolor: '#334155',
        tickcolor: '#475569',
        zeroline: true,
        zerolinecolor: 'rgba(248, 113, 113, 0.5)',
        zerolinewidth: 2,
    }},
    hovermode: 'x unified',
    legend: {{ bgcolor: 'rgba(30, 41, 59, 0.9)', bordercolor: '#334155', borderwidth: 1 }}
}};

// ===== FRESHNESS PANEL =====
function renderFreshness() {{
    const grid = document.getElementById('freshnessGrid');
    let html = '';

    // Parquet
    html += `<div class="freshness-item">
        <div class="label">races.parquet (データ本体)</div>
        <div class="value">{parquet_min} 〜 {parquet_max} ({parquet_count:,}件)</div>
    </div>`;

    for (const [key, info] of Object.entries(PKL_INFO)) {{
        const statusClass = info.days_old <= 7 ? 'status-good' : info.days_old <= 30 ? 'status-warn' : 'status-bad';
        const statusText = info.days_old <= 7 ? '最新' : info.days_old <= 30 ? 'やや古い' : '要更新';
        html += `<div class="freshness-item">
            <div class="label">${{key}}.pkl</div>
            <div class="value"><span class="status-dot ${{statusClass}}"></span>${{info.date}} (${{info.days_old}}日前) - ${{statusText}}</div>
        </div>`;
    }}
    grid.innerHTML = html;
}}

// ===== TAB SWITCHING =====
function switchTab(tabId) {{
    document.querySelectorAll('.chart-container').forEach(el => el.classList.remove('active'));
    document.querySelectorAll('.tab').forEach(el => el.classList.remove('active'));
    document.getElementById(tabId).classList.add('active');
    const tabIndex = ['tab1','tab2','tab3','tab4','tab5'].indexOf(tabId);
    document.querySelectorAll('.tab')[tabIndex].classList.add('active');

    // 遅延描画
    setTimeout(() => {{
        if(tabId === 'tab1') drawTab1();
        if(tabId === 'tab2') drawTab2();
        if(tabId === 'tab3') drawTab3();
        if(tabId === 'tab4') drawTab4();
        if(tabId === 'tab5') drawTab5();
    }}, 50);
}}

// ===== CHART DRAWING =====
function drawTab1() {{
    const trace = {{
        x: MONTHLY_ALL.map(d => d.year_month),
        y: MONTHLY_ALL.map(d => d.time_zscore_mean),
        type: 'scatter',
        mode: 'lines+markers',
        name: 'タイムZスコア（全体）',
        line: {{ color: '#6366f1', width: 2 }},
        marker: {{ size: 4 }},
        hovertemplate: '%{{x}}<br>Zスコア: %{{y:.4f}}<extra></extra>'
    }};

    // 12ヶ月移動平均
    const ma12 = [];
    const ys = MONTHLY_ALL.map(d => d.time_zscore_mean);
    for(let i = 0; i < ys.length; i++) {{
        if(i < 11) {{ ma12.push(null); }}
        else {{
            let sum = 0;
            for(let j = i-11; j <= i; j++) sum += ys[j];
            ma12.push(sum / 12);
        }}
    }}

    const traceMA = {{
        x: MONTHLY_ALL.map(d => d.year_month),
        y: ma12,
        type: 'scatter',
        mode: 'lines',
        name: '12ヶ月移動平均',
        line: {{ color: '#f59e0b', width: 3, dash: 'dot' }},
        hovertemplate: '%{{x}}<br>12M MA: %{{y:.4f}}<extra></extra>'
    }};

    const layout = {{
        ...PLOTLY_LAYOUT_BASE,
        title: {{ text: '全体タイムZスコア 月次推移', font: {{ size: 16 }} }},
        yaxis: {{ ...PLOTLY_LAYOUT_BASE.yaxis, title: 'Zスコア (負=速い)', autorange: 'reversed' }},
        xaxis: {{ ...PLOTLY_LAYOUT_BASE.xaxis, title: '年月' }},
        shapes: [{{
            type: 'line', x0: 0, x1: 1, xref: 'paper',
            y0: 0, y1: 0, line: {{ color: 'rgba(248, 113, 113, 0.5)', width: 2, dash: 'dash' }}
        }}]
    }};

    Plotly.newPlot('chart1', [trace, traceMA], layout, {{responsive: true}});

    // 分析コメント
    const recent = MONTHLY_ALL.slice(-6);
    const recentAvg = recent.reduce((a, d) => a + d.time_zscore_mean, 0) / recent.length;
    const overall = MONTHLY_ALL.reduce((a, d) => a + d.time_zscore_mean, 0) / MONTHLY_ALL.length;
    const trend = recentAvg < overall ? '速くなっている' : '遅くなっている';

    document.getElementById('analysis1').innerHTML = `
        <h4>🔬 分析結果</h4>
        <div class="insight">直近6ヶ月平均 Zスコア: <strong>${{recentAvg.toFixed(4)}}</strong></div>
        <div class="insight">全期間平均 Zスコア: <strong>${{overall.toFixed(4)}}</strong></div>
        <div class="insight">トレンド: 直近6ヶ月は全期間平均と比較して <strong>${{trend}}</strong> 傾向</div>
        <div class="insight">⚡ Zスコアが負の方向に推移 = 全体的にタイムが速い時期。馬場状態・季節・出走レベルの影響を含む。</div>
    `;
}}

function drawTab2() {{
    const select = document.getElementById('venueSelect');
    if(select.options.length === 0) {{
        VENUES.forEach(v => {{
            const opt = document.createElement('option');
            opt.value = v;
            opt.text = v;
            if(['東京','中山','阪神','京都'].includes(v)) opt.selected = true;
            select.appendChild(opt);
        }});
    }}
    updateTab2();
}}

function updateTab2() {{
    const select = document.getElementById('venueSelect');
    const selected = Array.from(select.selectedOptions).map(o => o.value);
    if(selected.length === 0) return;

    const traces = selected.map(venue => {{
        const data = MONTHLY_VENUE.filter(d => d.venue === venue);
        return {{
            x: data.map(d => d.year_month),
            y: data.map(d => d.time_zscore_mean),
            type: 'scatter',
            mode: 'lines',
            name: venue,
            line: {{ color: VENUE_COLORS[venue] || '#888', width: 2 }},
            hovertemplate: `${{venue}}: %{{y:.4f}}<extra></extra>`
        }};
    }});

    const layout = {{
        ...PLOTLY_LAYOUT_BASE,
        title: {{ text: '競馬場別 タイムZスコア推移', font: {{ size: 16 }} }},
        yaxis: {{ ...PLOTLY_LAYOUT_BASE.yaxis, title: 'Zスコア (負=速い)', autorange: 'reversed' }},
        xaxis: {{ ...PLOTLY_LAYOUT_BASE.xaxis, title: '年月' }},
    }};

    Plotly.newPlot('chart2', traces, layout, {{responsive: true}});

    // 分析
    let analysisHtml = '<h4>🏟️ 競馬場別分析</h4>';
    selected.forEach(venue => {{
        const data = MONTHLY_VENUE.filter(d => d.venue === venue);
        if(data.length < 3) return;
        const recent = data.slice(-6);
        const avg = recent.reduce((a, d) => a + d.time_zscore_mean, 0) / recent.length;
        const allAvg = data.reduce((a, d) => a + d.time_zscore_mean, 0) / data.length;
        const diff = avg - allAvg;
        const direction = diff < 0 ? '速い方向' : '遅い方向';
        analysisHtml += `<div class="insight">${{venue}}: 直近6M平均=${{avg.toFixed(4)}} (全期間比: ${{diff > 0 ? '+' : ''}}${{diff.toFixed(4)}}, ${{direction}})</div>`;
    }});
    document.getElementById('analysis2').innerHTML = analysisHtml;
}}

function drawTab3() {{
    // 距離セレクトを初期化
    const distSelect = document.getElementById('distSelect');
    if(distSelect.options.length === 0) {{
        const turf = POPULAR_DISTS.filter(d => d.track_surface === '芝');
        turf.forEach(d => {{
            const opt = document.createElement('option');
            opt.value = d.distance_m;
            opt.text = d.distance_m + 'm';
            distSelect.appendChild(opt);
        }});
    }}
    updateTab3();
}}

function updateTab3() {{
    const surface = document.getElementById('surfaceSelect').value;
    const dist = parseInt(document.getElementById('distSelect').value);

    // 距離セレクトを更新
    const distSelect = document.getElementById('distSelect');
    const currentDist = distSelect.value;
    distSelect.innerHTML = '';
    const dists = POPULAR_DISTS.filter(d => d.track_surface === surface);
    dists.forEach(d => {{
        const opt = document.createElement('option');
        opt.value = d.distance_m;
        opt.text = d.distance_m + 'm';
        if(d.distance_m == currentDist) opt.selected = true;
        distSelect.appendChild(opt);
    }});

    const selectedDist = parseInt(distSelect.value);
    const data = MONTHLY_DIST.filter(d => d.track_surface === surface && d.distance_m === selectedDist && d.count >= 3);

    if(data.length === 0) return;

    const trace = {{
        x: data.map(d => d.year_month),
        y: data.map(d => d.time_mean_actual),
        type: 'scatter',
        mode: 'lines+markers',
        name: `${{surface}}${{selectedDist}}m 平均タイム`,
        line: {{ color: surface === '芝' ? '#4ade80' : '#f59e0b', width: 2 }},
        marker: {{ size: 4 }},
        hovertemplate: '%{{x}}<br>平均タイム: %{{y:.2f}}秒<extra></extra>'
    }};

    const layout = {{
        ...PLOTLY_LAYOUT_BASE,
        title: {{ text: `${{surface}}${{selectedDist}}m 月次平均タイム推移 (Top3)`, font: {{ size: 16 }} }},
        yaxis: {{ ...PLOTLY_LAYOUT_BASE.yaxis, title: '平均タイム (秒)', autorange: 'reversed' }},
        xaxis: {{ ...PLOTLY_LAYOUT_BASE.xaxis, title: '年月' }},
    }};

    Plotly.newPlot('chart3', [trace], layout, {{responsive: true}});
}}

function drawTab4() {{
    const traceTime = {{
        x: YEARLY_ALL.map(d => d.year.toString()),
        y: YEARLY_ALL.map(d => d.time_zscore_mean),
        type: 'bar',
        name: 'タイムZスコア',
        marker: {{
            color: YEARLY_ALL.map(d => d.time_zscore_mean < 0 ? '#4ade80' : '#f87171'),
            opacity: 0.8
        }},
        hovertemplate: '%{{x}}年<br>タイムZ: %{{y:.4f}}<extra></extra>'
    }};

    const traceL3f = {{
        x: YEARLY_ALL.map(d => d.year.toString()),
        y: YEARLY_ALL.map(d => d.l3f_zscore_mean),
        type: 'bar',
        name: '上がり3F Zスコア',
        marker: {{ color: '#818cf8', opacity: 0.7 }},
        hovertemplate: '%{{x}}年<br>上がり3F Z: %{{y:.4f}}<extra></extra>'
    }};

    const layout = {{
        ...PLOTLY_LAYOUT_BASE,
        title: {{ text: '年次Zスコアサマリー (負=速い)', font: {{ size: 16 }} }},
        yaxis: {{ ...PLOTLY_LAYOUT_BASE.yaxis, title: 'Zスコア平均', autorange: 'reversed' }},
        xaxis: {{ ...PLOTLY_LAYOUT_BASE.xaxis, title: '年' }},
        barmode: 'group'
    }};

    Plotly.newPlot('chart4', [traceTime, traceL3f], layout, {{responsive: true}});

    // 分析
    let html = '<h4>📅 年次分析</h4>';
    const years = YEARLY_ALL.map(d => d.year);
    const minYear = years[0];
    const maxYear = years[years.length - 1];
    const first3 = YEARLY_ALL.slice(0, 3);
    const last3 = YEARLY_ALL.slice(-3);
    const early = first3.reduce((a,d) => a + d.time_zscore_mean, 0) / first3.length;
    const late = last3.reduce((a,d) => a + d.time_zscore_mean, 0) / last3.length;
    const longTrend = late < early ? '全体的にタイムは速くなっている' : '全体的にタイムは遅くなっている';
    html += `<div class="insight">期間: ${{minYear}}年 〜 ${{maxYear}}年</div>`;
    html += `<div class="insight">初期3年平均Z: ${{early.toFixed(4)}} → 直近3年平均Z: ${{late.toFixed(4)}}</div>`;
    html += `<div class="insight">長期トレンド: <strong>${{longTrend}}</strong></div>`;
    document.getElementById('analysis4').innerHTML = html;
}}

function drawTab5() {{
    const trace = {{
        x: MONTHLY_ALL.map(d => d.year_month),
        y: MONTHLY_ALL.map(d => d.l3f_zscore_mean),
        type: 'scatter',
        mode: 'lines+markers',
        name: '上がり3F Zスコア',
        line: {{ color: '#22d3ee', width: 2 }},
        marker: {{ size: 4 }},
        hovertemplate: '%{{x}}<br>上がり3F Z: %{{y:.4f}}<extra></extra>'
    }};

    const layout = {{
        ...PLOTLY_LAYOUT_BASE,
        title: {{ text: '上がり3F Zスコア 月次推移', font: {{ size: 16 }} }},
        yaxis: {{ ...PLOTLY_LAYOUT_BASE.yaxis, title: 'Zスコア (負=速い)', autorange: 'reversed' }},
        xaxis: {{ ...PLOTLY_LAYOUT_BASE.xaxis, title: '年月' }},
    }};

    Plotly.newPlot('chart5', [trace], layout, {{responsive: true}});

    const recent = MONTHLY_ALL.slice(-6);
    const avgL3f = recent.reduce((a,d) => a + d.l3f_zscore_mean, 0) / recent.length;
    document.getElementById('analysis5').innerHTML = `
        <h4>🔬 上がり3F分析</h4>
        <div class="insight">直近6ヶ月の上がり3F平均Zスコア: <strong>${{avgL3f.toFixed(4)}}</strong></div>
        <div class="insight">上がり3Fのトレンドはペース配分の変化、馬場状態、出走馬のレベル変動を反映します。</div>
    `;
}}

// ===== INIT =====
renderFreshness();
drawTab1();
</script>

</body>
</html>"""

    os.makedirs(OUTPUT_HTML.parent, exist_ok=True)
    with open(OUTPUT_HTML, 'w', encoding='utf-8') as f:
        f.write(html)

    print(f"\n✅ ダッシュボード生成完了: {OUTPUT_HTML}")
    print(f"   ファイルサイズ: {os.path.getsize(OUTPUT_HTML):,} bytes")


if __name__ == "__main__":
    freshness = check_freshness()
    build_dashboard()
