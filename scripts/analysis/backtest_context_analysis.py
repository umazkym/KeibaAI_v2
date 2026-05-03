#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
展開分析パラメータ検証（バックテスト）

展開不利/展開利と判定された馬の「次走」成績を検証し、
展開分析パラメータの妥当性を実証的に確認する。

検証項目:
1. 展開不利(adj >= 2.0)の馬は次走で巻き返しているか？
2. 展開利(adj <= -2.0)の馬は次走で成績が下がっているか？
3. 脚質別の感度係数（逃:1.5, 先:1.0, 中:0.5, 差:-1.0）は妥当か？
4. 度外視判定と実際の次走成績の相関

Usage:
    python scripts/analysis/backtest_context_analysis.py
    python scripts/analysis/backtest_context_analysis.py --sample 5000

出力:
    outputs/analysis/context_backtest_results.md
"""

import argparse
import logging
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd

# プロジェクトルートをpathに追加
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from keibaai.analysis.race_context_analyzer import RaceContextAnalyzer

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

OUTPUT_DIR = PROJECT_ROOT / "outputs" / "analysis"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def main():
    parser = argparse.ArgumentParser(description='展開分析バックテスト')
    parser.add_argument('--sample', type=int, default=3000,
                        help='サンプル数（馬単位。デフォルト3000）')
    parser.add_argument('--min-date', type=str, default='2023-01-01',
                        help='分析対象の最小日付')
    args = parser.parse_args()

    analyzer = RaceContextAnalyzer()
    analyzer._load_parquet()
    analyzer._load_pkl_stats()

    df = analyzer._df.copy()

    # 対象データの絞り込み
    df = df[
        (df['race_date'] >= args.min_date) &
        (df['finish_position'].notna()) &
        (df['finish_time_seconds'].notna()) &
        (df['passing_order_4'].notna()) &
        (df['num_runners'] >= 5)
    ].copy()

    logger.info(f"対象レコード数: {len(df)}")

    # horse_id × race_dateでソートし、次走の情報を取得
    df = df.sort_values(['horse_id', 'race_date'])
    df['next_finish_pos'] = df.groupby('horse_id')['finish_position'].shift(-1)
    df['next_num_runners'] = df.groupby('horse_id')['num_runners'].shift(-1)
    df['next_race_date'] = df.groupby('horse_id')['race_date'].shift(-1)

    # 次走がある馬のみ
    has_next = df.dropna(subset=['next_finish_pos']).copy()
    logger.info(f"次走あり: {len(has_next)}")

    # サンプリング（処理時間削減）
    if len(has_next) > args.sample:
        has_next = has_next.sample(n=args.sample, random_state=42)
        logger.info(f"サンプリング後: {len(has_next)}")

    # 各馬の展開分析を実行
    results = []
    t_start = time.time()

    for idx, (_, row) in enumerate(has_next.iterrows()):
        if idx % 500 == 0 and idx > 0:
            elapsed = time.time() - t_start
            eta = elapsed / idx * (len(has_next) - idx)
            logger.info(f"Progress: {idx}/{len(has_next)} ({elapsed:.0f}s elapsed, ETA {eta:.0f}s)")

        race_id = str(row['race_id'])
        horse_pos = int(row['finish_position'])
        corner4 = float(row['passing_order_4'])
        num_runners = int(row['num_runners'])

        try:
            ann = analyzer.annotate_horse_in_race(race_id, corner4, horse_pos)

            results.append({
                'race_id': race_id,
                'horse_id': row['horse_id'],
                'finish_pos': horse_pos,
                'num_runners': num_runners,
                'relative_pos': horse_pos / num_runners,
                'style': ann.get('horse_style', ''),
                'pace_z': ann.get('pace_z', 0.0),
                'pace_adj': ann.get('pace_adj', 0.0),
                'pos_adj': ann.get('position_adj', 0.0),
                'total_adj': ann.get('total_adjustment', 0.0),
                'adj_label': ann.get('adjustment_label', ''),
                'verdict': ann.get('verdict', ''),
                'verdict_level': ann.get('verdict_level', 0),
                'last3f_rank': ann.get('last3f_rank'),
                'formation_label': ann.get('formation_label', ''),
                # 次走情報
                'next_pos': int(row['next_finish_pos']),
                'next_num_runners': int(row['next_num_runners']),
                'next_relative_pos': row['next_finish_pos'] / row['next_num_runners'],
            })
        except Exception as e:
            continue

    elapsed_total = time.time() - t_start
    logger.info(f"分析完了: {len(results)}件, {elapsed_total:.0f}秒")

    rdf = pd.DataFrame(results)

    # ===== 検証1: 展開不利→次走巻き返し率 =====
    report_lines = ["# 展開分析バックテスト結果\n"]
    report_lines.append(f"分析対象: {len(rdf)}件\n")
    report_lines.append(f"対象期間: {args.min_date}〜\n\n")

    # 基準: 全体の次走成績
    all_next_avg = rdf['next_relative_pos'].mean()
    all_top3_rate = (rdf['next_pos'] <= 3).mean()
    report_lines.append(f"## 全体基準\n")
    report_lines.append(f"- 次走平均相対着順: {all_next_avg:.3f}\n")
    report_lines.append(f"- 次走Top3率: {all_top3_rate:.1%}\n\n")

    # 展開補正値別（正しい指標: 次走の絶対成績）
    report_lines.append("## 1. 展開補正値別の次走成績\n\n")
    report_lines.append("| 展開補正 | N | 次走平均相対着順 | 次走Top3率 | vs基準Top3率 | 判定 |\n")
    report_lines.append("|----------|---|-----------------|-----------|-------------|------|\n")

    bins = [
        ('≥ +3.0 (大不利)', rdf[rdf['total_adj'] >= 3.0]),
        ('+2.0〜+3.0 (不利)', rdf[(rdf['total_adj'] >= 2.0) & (rdf['total_adj'] < 3.0)]),
        ('+1.0〜+2.0 (やや不利)', rdf[(rdf['total_adj'] >= 1.0) & (rdf['total_adj'] < 2.0)]),
        ('-1.0〜+1.0 (中立)', rdf[(rdf['total_adj'] > -1.0) & (rdf['total_adj'] < 1.0)]),
        ('-2.0〜-1.0 (やや利)', rdf[(rdf['total_adj'] <= -1.0) & (rdf['total_adj'] > -2.0)]),
        ('-3.0〜-2.0 (利)', rdf[(rdf['total_adj'] <= -2.0) & (rdf['total_adj'] > -3.0)]),
        ('≤ -3.0 (大利)', rdf[rdf['total_adj'] <= -3.0]),
    ]

    for label, grp in bins:
        if len(grp) < 5:
            continue
        avg_next = grp['next_relative_pos'].mean()
        top3_rate = (grp['next_pos'] <= 3).mean()
        diff_top3 = top3_rate - all_top3_rate
        # 正しい判定: 不利馬は次走Top3率が基準より高い（=実力馬）、利馬は低い
        if '不利' in label or label.startswith('≥'):
            verdict = '✅' if diff_top3 > 0.05 else ('⚠️' if diff_top3 > 0 else '❌')
        elif '利' in label or label.startswith('≤'):
            verdict = '✅' if diff_top3 < 0 else ('⚠️' if diff_top3 < 0.05 else '❌')
        else:
            verdict = '—'
        report_lines.append(
            f"| {label} | {len(grp)} | {avg_next:.3f} | {top3_rate:.1%} | {diff_top3:+.1%} | {verdict} |\n")

    # ===== 検証1.5: 生存バイアス排除テスト =====
    report_lines.append("\n## 1.5 真のテスト: 凡走馬の中での比較\n\n")
    report_lines.append("> 展開分析の真の価値は「凡走した馬の中から展開不利を見抜き次走巻き返しを予測する」こと。\n\n")

    bad_runners = rdf[rdf['relative_pos'] > 0.5]  # 下位50%の馬のみ
    if len(bad_runners) >= 50:
        bad_baseline_top3 = (bad_runners['next_pos'] <= 3).mean()
        bad_baseline_avg = bad_runners['next_relative_pos'].mean()
        report_lines.append(f"凡走馬(下位50%)基準: 次走Top3率 {bad_baseline_top3:.1%}, 次走相対着順 {bad_baseline_avg:.3f} (N={len(bad_runners)})\n\n")
        report_lines.append("| 展開判定 | N | 次走Top3率 | 次走相対着順 | vs凡走基準Top3 | 判定 |\n")
        report_lines.append("|----------|---|-----------|-----------|---------------|------|\n")

        bad_groups = [
            ('凡走+展開不利(≥+2.0)', bad_runners[bad_runners['total_adj'] >= 2.0]),
            ('凡走+やや不利(+1~+2)', bad_runners[(bad_runners['total_adj'] >= 1.0) & (bad_runners['total_adj'] < 2.0)]),
            ('凡走+中立(-1~+1)', bad_runners[(bad_runners['total_adj'] > -1.0) & (bad_runners['total_adj'] < 1.0)]),
            ('凡走+展開利(≤-2.0)', bad_runners[bad_runners['total_adj'] <= -2.0]),
        ]
        for label, grp in bad_groups:
            if len(grp) < 5:
                continue
            top3 = (grp['next_pos'] <= 3).mean()
            avg_next = grp['next_relative_pos'].mean()
            diff = top3 - bad_baseline_top3
            verdict = '✅' if ('不利' in label and diff > 0.03) or ('利' in label and diff < 0) else ('⚠️' if abs(diff) < 0.03 else '❌')
            report_lines.append(f"| {label} | {len(grp)} | {top3:.1%} | {avg_next:.3f} | {diff:+.1%} | {verdict} |\n")
    else:
        report_lines.append("N不足のためスキップ\n")

    # ===== 検証2: 脚質別の感度係数検証 =====
    report_lines.append("\n## 2. 脚質別の展開影響度\n\n")
    report_lines.append("| 脚質 | N | ハイ時次走改善率 | スロー時次走改善率 | 差分 | 推定感度 |\n")
    report_lines.append("|------|---|----------------|-----------------|------|----------|\n")

    for style in ['逃げ', '先行', '中団', '差追込']:
        style_data = rdf[rdf['style'] == style]
        if len(style_data) < 20:
            continue
        # ハイペース(pace_z < -0.7)でこの脚質の馬の次走改善率
        hi = style_data[style_data['pace_z'] < -0.7]
        slow = style_data[style_data['pace_z'] > 0.7]
        if len(hi) >= 5 and len(slow) >= 5:
            hi_improve = (hi['next_relative_pos'] < hi['relative_pos']).mean()
            slow_improve = (slow['next_relative_pos'] < slow['relative_pos']).mean()
            diff = hi_improve - slow_improve
            # 逃げ: ハイで苦しい→次走改善率高い, スローで有利→次走改善率低い → 差が大きいはず
            # 差追込: 逆
            est_sensitivity = abs(diff) * 5  # スケール調整
            report_lines.append(
                f"| {style} | {len(style_data)} | {hi_improve:.1%} ({len(hi)}) | {slow_improve:.1%} ({len(slow)}) | {diff:+.1%} | {est_sensitivity:.2f} |\n")
        else:
            report_lines.append(f"| {style} | {len(style_data)} | N/A | N/A | — | — |\n")

    # ===== 検証3: 度外視判定の検証 =====
    report_lines.append("\n## 3. 度外視判定の次走成績\n\n")
    report_lines.append("| 判定 | N | 次走平均相対着順 | 次走Top3率 | 着順改善率 |\n")
    report_lines.append("|------|---|-----------------|-----------|----------|\n")

    verdict_data = rdf[rdf['verdict'] != '']
    if len(verdict_data) > 0:
        for verdict_name in rdf['verdict'].unique():
            if not verdict_name:
                continue
            vgrp = rdf[rdf['verdict'] == verdict_name]
            if len(vgrp) < 5:
                continue
            avg_next = vgrp['next_relative_pos'].mean()
            top3_rate = (vgrp['next_pos'] <= 3).mean()
            improve_rate = (vgrp['next_relative_pos'] < vgrp['relative_pos']).mean()
            report_lines.append(
                f"| {verdict_name} | {len(vgrp)} | {avg_next:.3f} | {top3_rate:.1%} | {improve_rate:.1%} |\n")

    # ===== 検証4: 隊列分析の影響 =====
    report_lines.append("\n## 4. 隊列と展開補正の関係\n\n")
    report_lines.append("| 隊列 | N | 展開不利時次走改善率 | 展開利時次走改善率 |\n")
    report_lines.append("|------|---|-------------------|------------------|\n")

    for form_label in ['凝縮', '標準', '縦長']:
        form_data = rdf[rdf['formation_label'] == form_label]
        if len(form_data) < 10:
            continue
        unfav = form_data[form_data['total_adj'] >= 2.0]
        fav = form_data[form_data['total_adj'] <= -2.0]
        unfav_imp = (unfav['next_relative_pos'] < unfav['relative_pos']).mean() if len(unfav) >= 5 else float('nan')
        fav_imp = (fav['next_relative_pos'] < fav['relative_pos']).mean() if len(fav) >= 5 else float('nan')
        report_lines.append(
            f"| {form_label} | {len(form_data)} | {unfav_imp:.1%} ({len(unfav)}) | {fav_imp:.1%} ({len(fav)}) |\n")

    # ===== 検証5: 上がり3F順位の影響 =====
    report_lines.append("\n## 5. 上がり3F順位と次走成績\n\n")
    report_lines.append("| 上がり順位 | N | 次走平均相対着順 | 次走Top3率 | 着順改善率 |\n")
    report_lines.append("|-----------|---|-----------------|-----------|----------|\n")

    for l3f_label, l3f_grp in [
        ('1位(最速)', rdf[rdf['last3f_rank'] == 1]),
        ('2-3位', rdf[rdf['last3f_rank'].between(2, 3)]),
        ('下位3', rdf[rdf['last3f_rank'].notna() & (rdf['last3f_rank'] >= rdf.get('last3f_total', 99) - 2)]),
    ]:
        if len(l3f_grp) >= 5:
            avg_next = l3f_grp['next_relative_pos'].mean()
            top3_rate = (l3f_grp['next_pos'] <= 3).mean()
            improve_rate = (l3f_grp['next_relative_pos'] < l3f_grp['relative_pos']).mean()
            report_lines.append(
                f"| {l3f_label} | {len(l3f_grp)} | {avg_next:.3f} | {top3_rate:.1%} | {improve_rate:.1%} |\n")

    # ===== 結論 =====
    report_lines.append("\n## 結論\n\n")

    # 自動判定（次走の絶対成績で評価）
    if len(rdf) >= 100:
        unfav = rdf[rdf['total_adj'] >= 2.0]
        fav = rdf[rdf['total_adj'] <= -2.0]
        neutral = rdf[(rdf['total_adj'] > -1.0) & (rdf['total_adj'] < 1.0)]
        if len(unfav) >= 10 and len(fav) >= 10:
            unfav_top3 = (unfav['next_pos'] <= 3).mean()
            fav_top3 = (fav['next_pos'] <= 3).mean()
            neutral_top3 = (neutral['next_pos'] <= 3).mean() if len(neutral) > 0 else all_top3_rate
            unfav_avg = unfav['next_relative_pos'].mean()
            fav_avg = fav['next_relative_pos'].mean()
            # 不利馬は次走好成績、利馬は次走凡走→有効
            top3_spread = unfav_top3 - fav_top3
            avg_spread = fav_avg - unfav_avg  # 不利馬のほうが相対的に良い
            report_lines.append(f"### 主要指標\n")
            report_lines.append(f"- 展開不利(≥+2.0): 次走Top3率 **{unfav_top3:.1%}** (N={len(unfav)}), 次走相対着順 **{unfav_avg:.3f}**\n")
            report_lines.append(f"- 展開利(≤-2.0): 次走Top3率 **{fav_top3:.1%}** (N={len(fav)}), 次走相対着順 **{fav_avg:.3f}**\n")
            report_lines.append(f"- 中立(-1〜+1): 次走Top3率 **{neutral_top3:.1%}** (N={len(neutral)})\n")
            report_lines.append(f"- **不利-利のTop3率差: {top3_spread:+.1%}**, 相対着順差: {avg_spread:+.3f}\n\n")
            if top3_spread > 0.10 and avg_spread > 0.05:
                report_lines.append(f"✅ **パラメータは有効** 展開不利馬は次走Top3率{unfav_top3:.1%}(基準{all_top3_rate:.1%}の{unfav_top3/all_top3_rate:.1f}倍)、展開利馬は{fav_top3:.1%}。分析は馬の実力を正しく評価している。\n\n")
            elif top3_spread > 0.03 or avg_spread > 0.03:
                report_lines.append(f"⚠️ **パラメータは弱い有効性** 方向性は正しいが、不利/利の差が小さい。サンプル増加で再検証を推奨。\n\n")
            else:
                report_lines.append(f"❌ **パラメータの有効性が不十分** 不利/利の差が見られない。パラメータの見直しが必要。\n\n")

    # 出力
    output_path = OUTPUT_DIR / "context_backtest_results.md"
    with open(output_path, 'w', encoding='utf-8') as f:
        f.writelines(report_lines)

    logger.info(f"Results saved to {output_path}")
    print("\n" + "".join(report_lines))


if __name__ == '__main__':
    main()
