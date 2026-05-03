#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
クラス別基準タイムテーブル生成スクリプト

race_context_analyzer.py で使用するクラス補正値を、
実際のデータから算出して保存する。

Usage:
    python scripts/analysis/generate_class_time_table.py

出力:
    keibaai/data/processed/class_time_table.pkl
"""

import pickle
import logging
from pathlib import Path

import numpy as np
import pandas as pd

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
PARQUET_PATH = PROJECT_ROOT / "keibaai" / "data" / "parsed" / "parquet" / "races" / "races.parquet"
OUTPUT_PATH = PROJECT_ROOT / "keibaai" / "data" / "processed" / "class_time_table.pkl"


CLASS_ORDINAL = {
    '新馬': 1, '未勝利': 2,
    '500': 3, '1勝': 3, '1勝クラス': 3,
    '1000': 4, '2勝': 4, '2勝クラス': 4,
    '1600': 5, '3勝': 5, '3勝クラス': 5,
    'OP': 6, 'オープン': 6, 'L': 6, 'リステッド': 6,
    'G3': 7, 'G2': 8, 'G1': 9,
}


def class_ordinal(race_class):
    if not race_class or pd.isna(race_class):
        return None
    for key, val in CLASS_ORDINAL.items():
        if key in str(race_class):
            return val
    return None


def main():
    logger.info(f"Loading parquet: {PARQUET_PATH}")
    df = pd.read_parquet(PARQUET_PATH, columns=[
        'race_id', 'distance_m', 'track_surface', 'track_condition',
        'venue', 'finish_position', 'finish_time_seconds', 'race_class'
    ])

    # 型変換
    for col in ['finish_position', 'finish_time_seconds', 'distance_m']:
        df[col] = pd.to_numeric(df[col], errors='coerce')

    # Top3のみ
    df = df[df['finish_position'] <= 3].copy()
    df = df.dropna(subset=['finish_time_seconds', 'distance_m', 'track_surface'])

    # クラスordinal
    df['class_ordinal'] = df['race_class'].apply(class_ordinal)
    df = df.dropna(subset=['class_ordinal'])
    df['class_ordinal'] = df['class_ordinal'].astype(int)

    # 馬場状態を正規化
    cond_map = {"良": "良", "稍": "稍重", "重": "重", "不": "不良"}
    df['cond_norm'] = df['track_condition'].apply(
        lambda c: cond_map.get(str(c)[:1], c) if pd.notna(c) else '良')

    # 主要距離のみ（サンプルが少ない距離は除外）
    dist_counts = df.groupby('distance_m').size()
    valid_dists = dist_counts[dist_counts >= 50].index
    df = df[df['distance_m'].isin(valid_dists)]

    logger.info(f"Records: {len(df)}, Distances: {len(valid_dists)}")

    # グループ化: (venue, distance, surface, condition, class_ordinal)
    # → 各グループのTop3タイムの中央値を算出
    results = {}

    for (venue, dist, surf, cond), grp in df.groupby(
            ['venue', 'distance_m', 'track_surface', 'cond_norm']):
        class_times = {}
        for cls_ord, cls_grp in grp.groupby('class_ordinal'):
            if len(cls_grp) < 3:
                continue
            med_time = cls_grp['finish_time_seconds'].median()
            class_times[cls_ord] = {
                'median_time': round(float(med_time), 2),
                'count': len(cls_grp),
                'std': round(float(cls_grp['finish_time_seconds'].std()), 3),
            }

        if len(class_times) >= 2:
            # クラス間の秒差を計算
            sorted_classes = sorted(class_times.keys())
            for i in range(len(sorted_classes) - 1):
                c_low = sorted_classes[i]
                c_high = sorted_classes[i + 1]
                diff = class_times[c_low]['median_time'] - class_times[c_high]['median_time']
                # c_low(弱いクラス)のタイム - c_high(強いクラス)のタイム → 通常正の値
                level_gap = c_high - c_low
                if level_gap > 0:
                    per_level = diff / level_gap
                    key = (venue, int(dist), surf, cond)
                    if key not in results:
                        results[key] = {
                            'class_times': class_times,
                            'sec_per_level_samples': [],
                        }
                    results[key]['sec_per_level_samples'].append(per_level)

    # 集約: 各キーのsec_per_levelの中央値
    class_table = {}
    for key, data in results.items():
        samples = data['sec_per_level_samples']
        if samples:
            sec_per_level = float(np.median(samples))
        else:
            sec_per_level = 0.0
        class_table[key] = {
            'sec_per_level': round(sec_per_level, 3),
            'class_times': data['class_times'],
            'sample_pairs': len(samples),
        }

    # グローバルフォールバック（会場・距離別）
    global_samples = {}
    for (venue, dist, surf, cond), data in class_table.items():
        fallback_key = (int(dist), surf)
        if fallback_key not in global_samples:
            global_samples[fallback_key] = []
        global_samples[fallback_key].append(data['sec_per_level'])

    global_fallback = {}
    for key, samples in global_samples.items():
        global_fallback[key] = round(float(np.median(samples)), 3)

    output = {
        'class_table': class_table,
        'global_fallback': global_fallback,
    }

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_PATH, 'wb') as f:
        pickle.dump(output, f)

    logger.info(f"Saved to {OUTPUT_PATH}")
    logger.info(f"Entries: {len(class_table)}, Fallback keys: {len(global_fallback)}")

    # サマリー表示
    print("\n=== クラス別基準タイム差（秒/レベル）===")
    print(f"{'会場':>5} {'距離':>5} {'馬場':>3} {'状態':>4} {'秒/レベル':>10} {'サンプル':>8}")
    print("-" * 50)
    for (venue, dist, surf, cond), data in sorted(class_table.items()):
        print(f"{venue:>5} {dist:>5} {surf:>3} {cond:>4} {data['sec_per_level']:>10.3f} {data['sample_pairs']:>8}")

    print(f"\n=== グローバルフォールバック ===")
    for (dist, surf), val in sorted(global_fallback.items()):
        print(f"  {dist}m {surf}: {val:.3f}秒/レベル")


if __name__ == '__main__':
    main()
