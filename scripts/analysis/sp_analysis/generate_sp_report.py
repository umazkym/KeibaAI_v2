#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SP値分析HTMLレポート生成

【概要】
競馬場別SP値分析のインタラクティブHTMLレポートを生成する。
Plotlyを使ったリッチなビジュアライゼーション。

【レポート内容】
A: 競馬場別サマリー（基準タイム推移・SP値精度・ペース傾向）
B: キャリブレーション結果（グリッドサーチ結果）

【出力先】
  outputs/reports/sp_analysis/

実行:
  python scripts/analysis/sp_analysis/generate_sp_report.py
"""

import sys
import logging
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


def generate_venue_report(
    report_data: dict,
    base_times: pd.DataFrame,
    track_variants: pd.DataFrame,
    output_path: Path,
):
    """レポートA: 競馬場別SP値サマリーHTMLを生成。"""
    logger.info("レポートA: 競馬場別サマリー生成中...")

    bt_trends = report_data.get("base_time_trends", pd.DataFrame())
    tv_dist = report_data.get("track_variants", pd.DataFrame())
    sp_acc = report_data.get("sp_accuracy", pd.DataFrame())
    pace_trends = report_data.get("pace_trends", pd.DataFrame())

    generated_at = datetime.now().strftime("%Y-%m-%d %H:%M")

    # --- 基準タイムテーブル ---
    bt_html = _render_base_times_table(base_times)

    # --- 基準タイム推移チャート（Plotly JSON） ---
    bt_charts = _render_base_time_trends(bt_trends)

    # --- 馬場指数分布 ---
    tv_charts = _render_track_variant_charts(tv_dist)

    # --- SP値精度ヒートマップ ---
    sp_heatmap = _render_sp_accuracy(sp_acc)

    # --- ペース傾向 ---
    pace_section = _render_pace_trends(pace_trends)

    html = f"""<!DOCTYPE html>
<html lang="ja">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>SP値分析 — 競馬場別サマリー</title>
    <script src="https://cdn.plot.ly/plotly-2.27.0.min.js"></script>
    <style>
        * {{ margin: 0; padding: 0; box-sizing: border-box; }}
        body {{
            font-family: 'Segoe UI', 'Meiryo', sans-serif;
            background: #0f172a; color: #e2e8f0;
            line-height: 1.6;
        }}
        .header {{
            background: linear-gradient(135deg, #1e3a5f 0%, #0f172a 100%);
            padding: 32px 40px; border-bottom: 2px solid #334155;
        }}
        .header h1 {{ font-size: 28px; color: #60a5fa; margin-bottom: 8px; }}
        .header .meta {{ color: #94a3b8; font-size: 14px; }}
        .container {{ max-width: 1400px; margin: 0 auto; padding: 24px 40px; }}
        .section {{
            background: #1e293b; border-radius: 12px;
            padding: 24px; margin-bottom: 24px;
            border: 1px solid #334155;
        }}
        .section h2 {{
            font-size: 20px; color: #60a5fa;
            margin-bottom: 16px; padding-bottom: 8px;
            border-bottom: 1px solid #334155;
        }}
        .section h3 {{ font-size: 16px; color: #93c5fd; margin: 16px 0 8px; }}
        table {{
            width: 100%; border-collapse: collapse;
            font-size: 13px; margin-bottom: 16px;
        }}
        th {{
            background: #334155; color: #93c5fd;
            padding: 8px 12px; text-align: left;
            position: sticky; top: 0;
        }}
        td {{ padding: 6px 12px; border-bottom: 1px solid #1e293b; }}
        tr:hover td {{ background: #334155; }}
        .chart-container {{ width: 100%; height: 400px; margin: 16px 0; }}
        .grid {{ display: grid; grid-template-columns: 1fr 1fr; gap: 24px; }}
        @media (max-width: 900px) {{ .grid {{ grid-template-columns: 1fr; }} }}
        .stat-card {{
            background: #0f172a; border-radius: 8px;
            padding: 16px; text-align: center;
        }}
        .stat-card .value {{ font-size: 32px; color: #60a5fa; font-weight: bold; }}
        .stat-card .label {{ font-size: 12px; color: #94a3b8; margin-top: 4px; }}
        .tabs {{ display: flex; gap: 8px; margin-bottom: 16px; flex-wrap: wrap; }}
        .tab {{
            padding: 8px 16px; border-radius: 6px; cursor: pointer;
            background: #334155; color: #94a3b8; border: none; font-size: 13px;
        }}
        .tab.active {{ background: #2563eb; color: white; }}
    </style>
</head>
<body>
    <div class="header">
        <h1>🏇 SP値（スピード指数）分析レポート</h1>
        <div class="meta">生成日時: {generated_at} | データ: 2014-2026</div>
    </div>
    <div class="container">

        <!-- サマリーカード -->
        <div class="section">
            <h2>📊 概要</h2>
            <div class="grid" style="grid-template-columns: repeat(4, 1fr);">
                <div class="stat-card">
                    <div class="value">{len(base_times)}</div>
                    <div class="label">基準タイムパターン</div>
                </div>
                <div class="stat-card">
                    <div class="value">{len(track_variants):,}</div>
                    <div class="label">馬場指数データ（日×会場）</div>
                </div>
                <div class="stat-card">
                    <div class="value">{len(sp_acc)}</div>
                    <div class="label">精度検証セル</div>
                </div>
                <div class="stat-card">
                    <div class="value">10</div>
                    <div class="label">競馬場</div>
                </div>
            </div>
        </div>

        <!-- 基準タイム一覧 -->
        <div class="section">
            <h2>⏱️ 基準タイム一覧</h2>
            <p style="color:#94a3b8;margin-bottom:12px;">
                良馬場・勝ち馬の平均走破タイム（会場×コース×距離別）
            </p>
            {bt_html}
        </div>

        <!-- 基準タイム推移 -->
        <div class="section">
            <h2>📈 基準タイム年次推移</h2>
            <p style="color:#94a3b8;margin-bottom:12px;">
                路盤改修等による変化を可視化（偏差がプラス＝遅くなった、マイナス＝速くなった）
            </p>
            {bt_charts}
        </div>

        <!-- 馬場指数 -->
        <div class="section">
            <h2>🌤️ 馬場指数分布</h2>
            <p style="color:#94a3b8;margin-bottom:12px;">
                プラス＝基準より高速馬場、マイナス＝時計がかかる馬場
            </p>
            {tv_charts}
        </div>

        <!-- SP値精度 -->
        <div class="section">
            <h2>🎯 SP値予測精度（競馬場×距離帯×年度別）</h2>
            <p style="color:#94a3b8;margin-bottom:12px;">
                SP値上位3頭の複勝率とSpearman相関
            </p>
            {sp_heatmap}
        </div>

        <!-- ペース傾向 -->
        <div class="section">
            <h2>🏃 ペース傾向</h2>
            {pace_section}
        </div>

    </div>
</body>
</html>"""

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(html, encoding="utf-8")
    logger.info(f"  レポートA生成完了: {output_path}")


# ======================================================================
# レンダリングヘルパー
# ======================================================================
def _render_base_times_table(bt: pd.DataFrame) -> str:
    """基準タイムのHTMLテーブルを生成。"""
    if bt is None or len(bt) == 0:
        return "<p>データなし</p>"

    bt_sorted = bt.sort_values(
        ["track_surface", "venue", "distance_m"]
    ).copy()

    rows = []
    for _, r in bt_sorted.iterrows():
        sample_cls = "color:#22c55e" if r.get("base_time_sufficient", True) else "color:#ef4444"
        rows.append(
            f"<tr>"
            f"<td>{r['venue']}</td>"
            f"<td>{r['track_surface']}</td>"
            f"<td>{int(r['distance_m'])}m</td>"
            f"<td>{r['base_time']:.3f}</td>"
            f"<td>{r.get('base_time_std', 0):.3f}</td>"
            f"<td style='{sample_cls}'>{int(r.get('base_time_sample_count', 0))}</td>"
            f"</tr>"
        )

    return f"""
    <div style="max-height:500px;overflow-y:auto;">
    <table>
        <thead><tr>
            <th>会場</th><th>コース</th><th>距離</th>
            <th>基準タイム(秒)</th><th>標準偏差</th><th>サンプル数</th>
        </tr></thead>
        <tbody>{''.join(rows)}</tbody>
    </table>
    </div>"""


def _render_base_time_trends(trends: pd.DataFrame) -> str:
    """基準タイム推移のPlotlyチャートを生成。"""
    if trends is None or len(trends) == 0:
        return "<p>データなし</p>"

    # 主要コース（サンプル多い上位12）を選択
    top_courses = (
        trends.groupby(["venue", "track_surface", "distance_m"])["count"]
        .sum()
        .nlargest(12)
        .index.tolist()
    )

    charts = []
    for i, (venue, surface, dist) in enumerate(top_courses):
        sub = trends[
            (trends["venue"] == venue)
            & (trends["track_surface"] == surface)
            & (trends["distance_m"] == dist)
        ].sort_values("year")

        if len(sub) < 3:
            continue

        div_id = f"bt_trend_{i}"
        data = json.dumps({
            "x": sub["year"].tolist(),
            "y": [round(v, 3) for v in sub["deviation_from_overall"].tolist()],
            "type": "scatter",
            "mode": "lines+markers",
            "name": f"{venue}{surface}{int(dist)}m",
            "line": {"width": 2},
        })
        layout = json.dumps({
            "title": f"{venue} {surface} {int(dist)}m",
            "xaxis": {"title": "年度"},
            "yaxis": {"title": "全期間平均との差(秒)", "zeroline": True},
            "height": 300,
            "margin": {"t": 40, "b": 40, "l": 60, "r": 20},
            "paper_bgcolor": "#1e293b",
            "plot_bgcolor": "#0f172a",
            "font": {"color": "#e2e8f0"},
        })
        charts.append(
            f'<div style="display:inline-block;width:48%;min-width:400px;">'
            f'<div id="{div_id}" class="chart-container"></div>'
            f'<script>Plotly.newPlot("{div_id}", [{data}], {layout})</script>'
            f'</div>'
        )

    return "\n".join(charts) if charts else "<p>データ不足</p>"


def _render_track_variant_charts(tv_dist: pd.DataFrame) -> str:
    """馬場指数分布のチャートを生成。"""
    if tv_dist is None or len(tv_dist) == 0:
        return "<p>データなし</p>"

    # 会場別の平均馬場指数テーブル
    venue_summary = (
        tv_dist.groupby(["venue", "track_surface"])
        .agg(
            mean_variant=("mean_variant", "mean"),
            std_variant=("std_variant", "mean"),
            total_days=("count", "sum"),
        )
        .reset_index()
        .sort_values("mean_variant", ascending=False)
    )

    rows = []
    for _, r in venue_summary.iterrows():
        color = "#22c55e" if r["mean_variant"] > 0 else "#ef4444"
        rows.append(
            f"<tr>"
            f"<td>{r['venue']}</td><td>{r['track_surface']}</td>"
            f"<td style='color:{color}'>{r['mean_variant']:+.3f}</td>"
            f"<td>{r['std_variant']:.3f}</td>"
            f"<td>{int(r['total_days'])}</td>"
            f"</tr>"
        )

    return f"""
    <table>
        <thead><tr>
            <th>会場</th><th>コース</th><th>平均馬場指数</th>
            <th>標準偏差(平均)</th><th>延べ日数</th>
        </tr></thead>
        <tbody>{''.join(rows)}</tbody>
    </table>"""


def _render_sp_accuracy(sp_acc: pd.DataFrame) -> str:
    """SP値精度テーブルを生成。"""
    if sp_acc is None or len(sp_acc) == 0:
        return "<p>データなし</p>"

    # 会場×距離帯の全年度平均
    summary = (
        sp_acc.groupby(["venue", "dist_band"])
        .agg(
            mean_place_rate=("top3_place_rate", "mean"),
            mean_winner_rate=("top3_winner_rate", "mean"),
            mean_spearman=("mean_spearman", "mean"),
            total_races=("n_races", "sum"),
        )
        .reset_index()
        .sort_values(["venue", "dist_band"])
    )

    rows = []
    for _, r in summary.iterrows():
        pr = r["mean_place_rate"]
        color = "#22c55e" if pr > 0.35 else ("#f59e0b" if pr > 0.30 else "#ef4444")
        rows.append(
            f"<tr>"
            f"<td>{r['venue']}</td><td>{r['dist_band']}</td>"
            f"<td style='color:{color}'>{pr:.1%}</td>"
            f"<td>{r['mean_winner_rate']:.1%}</td>"
            f"<td>{r['mean_spearman']:.3f}</td>"
            f"<td>{int(r['total_races']):,}</td>"
            f"</tr>"
        )

    return f"""
    <h3>SP値上位3頭の精度（全年度平均）</h3>
    <table>
        <thead><tr>
            <th>会場</th><th>距離帯</th><th>複勝率</th>
            <th>1着含有率</th><th>Spearman</th><th>レース数</th>
        </tr></thead>
        <tbody>{''.join(rows)}</tbody>
    </table>"""


def _render_pace_trends(pace_trends: pd.DataFrame) -> str:
    """ペース傾向テーブルを生成。"""
    if pace_trends is None or len(pace_trends) == 0:
        return "<p>データなし</p>"

    # 会場×コース×距離の全年度平均
    summary = (
        pace_trends.groupby(["venue", "track_surface", "distance_m"])
        .agg(mean_pace=("mean_pace", "mean"), count=("count", "sum"))
        .reset_index()
        .sort_values(["track_surface", "venue", "distance_m"])
    )

    rows = []
    for _, r in summary.iterrows():
        pace = r["mean_pace"]
        if pace < -2.0:
            label, color = "H（前傾）", "#ef4444"
        elif pace > 1.0:
            label, color = "S（後傾）", "#3b82f6"
        else:
            label, color = "M（平均）", "#22c55e"
        rows.append(
            f"<tr>"
            f"<td>{r['venue']}</td><td>{r['track_surface']}</td>"
            f"<td>{int(r['distance_m'])}m</td>"
            f"<td style='color:{color}'>{pace:+.2f} ({label})</td>"
            f"<td>{int(r['count']):,}</td>"
            f"</tr>"
        )

    return f"""
    <h3>コース別ペース傾向（全年度平均）</h3>
    <p style="color:#94a3b8;">正値=前傾(Hペース寄り), 負値=後傾(Sペース寄り)</p>
    <table>
        <thead><tr>
            <th>会場</th><th>コース</th><th>距離</th>
            <th>平均ペース指数</th><th>レース数</th>
        </tr></thead>
        <tbody>{''.join(rows)}</tbody>
    </table>"""


# ======================================================================
# メイン実行
# ======================================================================
def main():
    print("=" * 70)
    print("  SP値分析HTMLレポート生成")
    print("=" * 70)

    from keibaai.src.analysis.speed_index.calculator import SpeedIndexCalculator
    from keibaai.src.analysis.speed_index.pace_correction import PaceCorrectionEngine
    from keibaai.src.analysis.speed_index.venue_analysis import VenueAnalyzer

    # --- データ読み込み ---
    logger.info("データ読み込み中...")
    data_root = PROJECT_ROOT / "keibaai" / "data" / "parsed" / "parquet"
    races = pd.read_parquet(data_root / "races" / "races.parquet")
    rd = pd.read_parquet(data_root / "race_details" / "race_details.parquet")

    # --- SP値算出 ---
    logger.info("SP値算出中...")
    calc = SpeedIndexCalculator()
    calc.fit(races)

    pace_engine = PaceCorrectionEngine()
    pace_engine.fit(races, rd)

    # --- 競馬場別分析 ---
    logger.info("競馬場別分析中...")
    analyzer = VenueAnalyzer()
    analyzer.fit(races, rd, sp_calculator=calc, pace_engine=pace_engine)
    report_data = analyzer.generate_report()

    # --- HTMLレポート生成 ---
    output_dir = PROJECT_ROOT / "outputs" / "reports" / "sp_analysis"
    today = datetime.now().strftime("%Y%m%d")
    output_path = output_dir / f"sp_venue_analysis_{today}.html"

    generate_venue_report(
        report_data=report_data,
        base_times=calc.base_times,
        track_variants=calc.track_variants,
        output_path=output_path,
    )

    print(f"\n✅ レポート生成完了: {output_path}")
    print(f"   ブラウザで開いてください。")


if __name__ == "__main__":
    main()
