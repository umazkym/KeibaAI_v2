#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
output_finalフォルダのデータ整合性を検証
"""

import pandas as pd
from pathlib import Path
import sys

def main():
    output_dir = Path("output_final")

    print("=" * 70)
    print("データ整合性チェック - output_final")
    print("=" * 70)
    print()

    # 1. ファイルの存在確認
    print("【1】ファイル存在確認")
    files = {
        'race_results': output_dir / 'race_results.csv',
        'shutuba': output_dir / 'shutuba.csv',
        'horses': output_dir / 'horses.csv',
        'horses_performance': output_dir / 'horses_performance.csv'
    }

    for name, path in files.items():
        if path.exists():
            size_kb = path.stat().st_size / 1024
            print(f"  ✓ {name:20s}: {size_kb:7.1f} KB")
        else:
            print(f"  ✗ {name:20s}: 存在しません")
            sys.exit(1)
    print()

    # 2. データ読み込み
    print("【2】データ読み込み")
    try:
        df_race = pd.read_csv(files['race_results'], encoding='utf-8-sig')
        df_shutuba = pd.read_csv(files['shutuba'], encoding='utf-8-sig')
        df_horses = pd.read_csv(files['horses'], encoding='utf-8-sig')
        df_perf = pd.read_csv(files['horses_performance'], encoding='utf-8-sig')

        print(f"  ✓ race_results      : {len(df_race):5d}行 × {len(df_race.columns):2d}列")
        print(f"  ✓ shutuba           : {len(df_shutuba):5d}行 × {len(df_shutuba.columns):2d}列")
        print(f"  ✓ horses (pedigree) : {len(df_horses):5d}行 × {len(df_horses.columns):2d}列")
        print(f"  ✓ horses_performance: {len(df_perf):5d}行 × {len(df_perf.columns):2d}列")
    except Exception as e:
        print(f"  ✗ エラー: {e}")
        sys.exit(1)
    print()

    # 3. レース結果の基本統計
    print("【3】race_results.csv - 基本統計")
    print(f"  race_id数         : {df_race['race_id'].nunique()}件")
    print(f"  総出走頭数        : {len(df_race)}頭")
    print(f"  日付範囲          : {df_race['race_date'].min()} ~ {df_race['race_date'].max()}")
    print()

    # 4. 重要カラムの欠損チェック
    print("【4】race_results.csv - 重要カラム欠損率")
    critical_cols = [
        'race_id', 'distance_m', 'track_surface', 'weather',
        'track_condition', 'venue', 'race_class', 'head_count',
        'horse_id', 'horse_name', 'finish_position', 'jockey_name'
    ]

    issues = []
    for col in critical_cols:
        if col in df_race.columns:
            missing_count = df_race[col].isna().sum()
            missing_pct = 100 * missing_count / len(df_race)
            status = "✓" if missing_pct == 0 else "⚠"
            print(f"  {status} {col:20s}: {missing_count:3d}行 ({missing_pct:5.2f}%)")
            if missing_pct > 0:
                issues.append((col, missing_pct))
        else:
            print(f"  ✗ {col:20s}: カラムが存在しません")
            issues.append((col, 100.0))
    print()

    # 5. shutubaの整合性チェック
    print("【5】shutuba.csv - 整合性チェック")
    print(f"  race_id数         : {df_shutuba['race_id'].nunique()}件")
    print(f"  総出走頭数        : {len(df_shutuba)}頭")

    shutuba_critical = ['race_id', 'horse_id', 'horse_name', 'jockey_name']
    for col in shutuba_critical:
        if col in df_shutuba.columns:
            missing_count = df_shutuba[col].isna().sum()
            missing_pct = 100 * missing_count / len(df_shutuba)
            status = "✓" if missing_pct == 0 else "⚠"
            print(f"  {status} {col:20s}: {missing_count:3d}行 ({missing_pct:5.2f}%)")
            if missing_pct > 0:
                issues.append((f"shutuba.{col}", missing_pct))
    print()

    # 6. race_resultsとshutubaの照合
    print("【6】race_results vs shutuba - 整合性")
    race_ids_results = set(df_race['race_id'].unique())
    race_ids_shutuba = set(df_shutuba['race_id'].unique())

    if race_ids_results == race_ids_shutuba:
        print(f"  ✓ race_id一致: 両方とも{len(race_ids_results)}件")
    else:
        print(f"  ⚠ race_id不一致:")
        print(f"    race_results: {len(race_ids_results)}件")
        print(f"    shutuba     : {len(race_ids_shutuba)}件")
        only_results = race_ids_results - race_ids_shutuba
        only_shutuba = race_ids_shutuba - race_ids_results
        if only_results:
            print(f"    race_resultsのみ: {only_results}")
        if only_shutuba:
            print(f"    shutubaのみ    : {only_shutuba}")

    # 行数の比較
    if len(df_race) == len(df_shutuba):
        print(f"  ✓ 行数一致: {len(df_race)}行")
    else:
        print(f"  ⚠ 行数不一致: race_results={len(df_race)}行, shutuba={len(df_shutuba)}行")
        issues.append(("row_count_mismatch", abs(len(df_race) - len(df_shutuba))))
    print()

    # 7. 馬情報の統計
    print("【7】horses (pedigree) - 統計")
    print(f"  ユニーク馬ID数    : {df_horses['horse_id'].nunique()}頭")
    print(f"  総血統レコード数  : {len(df_horses)}行")
    print(f"  世代分布:")
    gen_dist = df_horses['generation'].value_counts().sort_index()
    for gen, count in gen_dist.items():
        print(f"    世代{gen}: {count:4d}行")
    print()

    # 8. 過去成績の統計
    print("【8】horses_performance - 統計")
    print(f"  ユニーク馬ID数    : {df_perf['horse_id'].nunique()}頭")
    print(f"  総レース出走記録  : {len(df_perf)}走")
    if len(df_perf) > 0:
        avg_races = len(df_perf) / df_perf['horse_id'].nunique()
        print(f"  平均出走回数/頭   : {avg_races:.1f}走")
    print()

    # 9. データ型チェック
    print("【9】race_results.csv - データ型チェック")
    expected_int_cols = [
        'distance_m', 'head_count', 'round_of_year', 'day_of_meeting',
        'finish_position', 'bracket_number', 'horse_number', 'age',
        'popularity', 'horse_weight', 'horse_weight_change'
    ]

    for col in expected_int_cols:
        if col in df_race.columns:
            # 数値に変換できるかチェック
            try:
                numeric_values = pd.to_numeric(df_race[col], errors='coerce')
                non_numeric = numeric_values.isna().sum() - df_race[col].isna().sum()
                if non_numeric == 0:
                    print(f"  ✓ {col:20s}: 数値変換可能")
                else:
                    print(f"  ⚠ {col:20s}: {non_numeric}行が数値変換不可")
                    issues.append((f"dtype.{col}", non_numeric))
            except Exception as e:
                print(f"  ✗ {col:20s}: エラー - {e}")
    print()

    # 10. サマリー
    print("=" * 70)
    print("【最終判定】")
    print("=" * 70)

    if len(issues) == 0:
        print("✓ すべてのチェックに合格しました")
        print("  このデータは実際のパイプラインで使用可能です")
        return 0
    else:
        print(f"⚠ {len(issues)}件の問題が検出されました:")
        for issue, value in issues:
            print(f"  - {issue}: {value}")
        print()
        print("  修正が必要な可能性があります")
        return 1

if __name__ == '__main__':
    sys.exit(main())
