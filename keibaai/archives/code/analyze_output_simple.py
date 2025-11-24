#!/usr/bin/env python3
"""
output_finalフォルダのデータを分析（標準ライブラリのみ）
"""
import csv
import os
from collections import defaultdict, Counter
from pathlib import Path

def analyze_csv_simple(file_path, name):
    """CSVファイルの基本分析"""
    print(f"\n{'='*80}")
    print(f"分析対象: {name}")
    print(f"={'='*80}")

    if not os.path.exists(file_path):
        print(f"✗ ファイルが見つかりません: {file_path}")
        return False

    file_size = os.path.getsize(file_path) / 1024
    print(f"ファイルサイズ: {file_size:.1f} KB")

    try:
        with open(file_path, 'r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            headers = reader.fieldnames

            if not headers:
                print("✗ ヘッダーが見つかりません")
                return False

            print(f"✓ 列数: {len(headers)}")
            print(f"\n【カラム一覧】")
            for i, col in enumerate(headers, 1):
                print(f"  {i:2d}. {col}")

            # データ行を読み込んで分析
            rows = list(reader)
            row_count = len(rows)
            print(f"\n✓ データ行数: {row_count:,}")

            if row_count == 0:
                print("⚠ データが空です")
                return True

            # 欠損値のカウント
            null_counts = defaultdict(int)
            for row in rows:
                for col in headers:
                    if not row.get(col) or row.get(col).strip() == '':
                        null_counts[col] += 1

            # 欠損値があるカラム
            null_cols = [(col, count) for col, count in null_counts.items() if count > 0]
            if null_cols:
                print(f"\n【欠損値あり列】 ({len(null_cols)}列)")
                null_cols.sort(key=lambda x: x[1], reverse=True)
                for col, count in null_cols[:20]:
                    pct = count / row_count * 100
                    print(f"  {col:30s}: {count:6,} ({pct:5.1f}%)")
                if len(null_cols) > 20:
                    print(f"  ... 他 {len(null_cols) - 20}列")
            else:
                print(f"\n✓ 欠損値なし（全カラム完全）")

            # データ品質スコア
            total_cells = row_count * len(headers)
            null_cells = sum(null_counts.values())
            quality_score = (1 - null_cells / total_cells) * 100 if total_cells > 0 else 0
            print(f"\n【データ品質スコア】")
            print(f"  完全性: {quality_score:.2f}% ({total_cells - null_cells:,} / {total_cells:,} cells)")

            # キーカラムの分析
            if 'race_id' in headers:
                race_ids = [row['race_id'] for row in rows if row.get('race_id')]
                unique_race_ids = len(set(race_ids))
                print(f"\n【race_id分析】")
                print(f"  総数: {len(race_ids):,}")
                print(f"  ユニーク数: {unique_race_ids:,}")
                print(f"  重複: {len(race_ids) - unique_race_ids:,}")

                # 形式チェック（12桁）
                invalid_format = [rid for rid in race_ids if not (rid.isdigit() and len(rid) == 12)]
                if invalid_format:
                    print(f"  ⚠ 不正な形式: {len(invalid_format)}件")
                    print(f"    例: {invalid_format[:5]}")
                else:
                    print(f"  ✓ 形式OK（全て12桁の数値）")

            if 'horse_id' in headers:
                horse_ids = [row['horse_id'] for row in rows if row.get('horse_id')]
                unique_horse_ids = len(set(horse_ids))
                print(f"\n【horse_id分析】")
                print(f"  総数: {len(horse_ids):,}")
                print(f"  ユニーク数: {unique_horse_ids:,}")

                # 形式チェック（10桁）
                invalid_format = [hid for hid in horse_ids if not (hid.isdigit() and len(hid) == 10)]
                if invalid_format:
                    print(f"  ⚠ 不正な形式: {len(invalid_format)}件")
                    print(f"    例: {invalid_format[:5]}")
                else:
                    print(f"  ✓ 形式OK（全て10桁の数値）")

            if 'race_date' in headers:
                race_dates = [row['race_date'] for row in rows if row.get('race_date')]
                if race_dates:
                    print(f"\n【race_date分析】")
                    print(f"  最小日付: {min(race_dates)}")
                    print(f"  最大日付: {max(race_dates)}")
                    print(f"  ユニーク日数: {len(set(race_dates))}")

            # ファイル種別専用チェック
            if 'race_results' in name:
                print(f"\n【race_results専用チェック】")

                if 'track_surface' in headers:
                    surfaces = [row['track_surface'] for row in rows if row.get('track_surface')]
                    surface_counts = Counter(surfaces)
                    print(f"  track_surface:")
                    for surface, count in surface_counts.most_common():
                        print(f"    {surface}: {count:,}")

                if 'weather' in headers:
                    weathers = [row['weather'] for row in rows if row.get('weather')]
                    weather_counts = Counter(weathers)
                    print(f"  weather:")
                    for weather, count in weather_counts.most_common():
                        print(f"    {weather}: {count:,}")

                if 'distance_m' in headers:
                    distances = [row['distance_m'] for row in rows if row.get('distance_m') and row['distance_m'].replace('.','').isdigit()]
                    if distances:
                        distances_num = [float(d) for d in distances]
                        print(f"  distance_m:")
                        print(f"    範囲: {min(distances_num):.0f}m ~ {max(distances_num):.0f}m")
                        print(f"    ユニーク値: {len(set(distances))}")
                        print(f"    欠損: {null_counts.get('distance_m', 0)}")

            elif 'shutuba' in name:
                print(f"\n【shutuba専用チェック】")

                if 'scratched' in headers:
                    scratched = [row['scratched'] for row in rows if row.get('scratched')]
                    scratched_true = sum(1 for s in scratched if s.lower() in ['true', '1', 'yes'])
                    print(f"  取消馬: {scratched_true} / {len(scratched)} ({scratched_true/len(scratched)*100:.1f}%)")

            elif 'horses.csv' in name and 'generation' in headers:
                print(f"\n【horses専用チェック（血統データ）】")

                generations = [row['generation'] for row in rows if row.get('generation') and row['generation'].isdigit()]
                gen_counts = Counter(generations)
                print(f"  世代の分布:")
                for gen in sorted(gen_counts.keys(), key=int):
                    print(f"    第{gen}世代: {gen_counts[gen]:,}")

                if 'horse_id' in headers:
                    horse_ids = [row['horse_id'] for row in rows if row.get('horse_id')]
                    unique_horses = len(set(horse_ids))
                    avg_ancestors = len(horse_ids) / unique_horses if unique_horses > 0 else 0
                    print(f"  ユニーク馬数: {unique_horses:,}")
                    print(f"  平均祖先数: {avg_ancestors:.1f}")

            elif 'horses_performance' in name:
                print(f"\n【horses_performance専用チェック】")

                if 'horse_id' in headers:
                    horse_ids = [row['horse_id'] for row in rows if row.get('horse_id')]
                    perf_per_horse = defaultdict(int)
                    for hid in horse_ids:
                        perf_per_horse[hid] += 1

                    counts = list(perf_per_horse.values())
                    print(f"  馬ごとの成績数:")
                    print(f"    平均: {sum(counts)/len(counts):.1f}")
                    print(f"    最大: {max(counts)}")
                    print(f"    最小: {min(counts)}")

            # サンプルデータ
            print(f"\n【サンプルデータ（先頭3行）】")
            for i, row in enumerate(rows[:3], 1):
                print(f"\n  --- 行{i} ---")
                for col in headers[:10]:  # 最初の10カラムのみ
                    print(f"  {col}: {row.get(col, '')}")
                if len(headers) > 10:
                    print(f"  ... 他 {len(headers) - 10}カラム")

            return True

    except Exception as e:
        print(f"✗ エラー: {e}")
        import traceback
        traceback.print_exc()
        return False

def check_consistency():
    """データの整合性チェック"""
    print(f"\n{'='*80}")
    print(f"データ整合性チェック")
    print(f"={'='*80}")

    output_dir = Path('output_final')

    try:
        # ファイル読み込み
        def load_csv(filename):
            with open(output_dir / filename, 'r', encoding='utf-8-sig') as f:
                reader = csv.DictReader(f)
                return list(reader)

        race_results = load_csv('race_results.csv')
        shutuba = load_csv('shutuba.csv')
        horses = load_csv('horses.csv')
        horses_perf = load_csv('horses_performance.csv')

        print(f"\n【1. race_id の整合性】")
        race_results_ids = set(row['race_id'] for row in race_results if row.get('race_id'))
        shutuba_ids = set(row['race_id'] for row in shutuba if row.get('race_id'))

        print(f"  race_results: {len(race_results_ids):,} レース")
        print(f"  shutuba:      {len(shutuba_ids):,} レース")

        common_ids = race_results_ids & shutuba_ids
        print(f"  共通:         {len(common_ids):,} レース")

        only_results = race_results_ids - shutuba_ids
        if only_results:
            print(f"  ⚠ race_resultsのみ: {len(only_results)} (例: {list(only_results)[:5]})")

        only_shutuba = shutuba_ids - race_results_ids
        if only_shutuba:
            print(f"  ⚠ shutubaのみ: {len(only_shutuba)} (例: {list(only_shutuba)[:5]})")

        print(f"\n【2. horse_id の整合性】")
        race_results_horses = set(row['horse_id'] for row in race_results if row.get('horse_id'))
        shutuba_horses = set(row['horse_id'] for row in shutuba if row.get('horse_id'))
        horses_horses = set(row['horse_id'] for row in horses if row.get('horse_id'))
        horses_perf_horses = set(row['horse_id'] for row in horses_perf if row.get('horse_id'))

        print(f"  race_results:       {len(race_results_horses):,} 頭")
        print(f"  shutuba:            {len(shutuba_horses):,} 頭")
        print(f"  horses (血統):      {len(horses_horses):,} 頭")
        print(f"  horses_performance: {len(horses_perf_horses):,} 頭")

        horses_in_results_not_in_horses = race_results_horses - horses_horses
        if horses_in_results_not_in_horses:
            print(f"  ⚠ race_resultsにあるがhorsesにない: {len(horses_in_results_not_in_horses)} 頭")
            print(f"    例: {list(horses_in_results_not_in_horses)[:5]}")
        else:
            print(f"  ✓ race_resultsの全馬がhorsesに存在")

        horses_in_shutuba_not_in_horses = shutuba_horses - horses_horses
        if horses_in_shutuba_not_in_horses:
            print(f"  ⚠ shutubaにあるがhorsesにない: {len(horses_in_shutuba_not_in_horses)} 頭")
            print(f"    例: {list(horses_in_shutuba_not_in_horses)[:5]}")
        else:
            print(f"  ✓ shutubaの全馬がhorsesに存在")

        print(f"\n【3. race_resultsとshutubaのレコード数整合性】")
        results_by_race = defaultdict(int)
        for row in race_results:
            if row.get('race_id'):
                results_by_race[row['race_id']] += 1

        shutuba_by_race = defaultdict(int)
        for row in shutuba:
            if row.get('race_id'):
                shutuba_by_race[row['race_id']] += 1

        mismatches = []
        for race_id in list(common_ids)[:100]:
            results_count = results_by_race.get(race_id, 0)
            shutuba_count = shutuba_by_race.get(race_id, 0)
            if results_count != shutuba_count:
                mismatches.append((race_id, results_count, shutuba_count))

        if mismatches:
            print(f"  ⚠ 馬数不一致: {len(mismatches)} / 100 レース（サンプル確認）")
            print(f"  例:")
            for race_id, res_cnt, sht_cnt in mismatches[:5]:
                print(f"    race_id={race_id}: results={res_cnt}, shutuba={sht_cnt} (差={res_cnt-sht_cnt})")
        else:
            print(f"  ✓ 馬数一致（確認したレース: 100）")

        return True

    except Exception as e:
        print(f"✗ エラー: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    """メイン実行"""
    output_dir = Path('output_final')

    if not output_dir.exists():
        print(f"✗ エラー: {output_dir} が見つかりません")
        return

    files = {
        'race_results.csv': 'レース結果データ',
        'shutuba.csv': '出馬表データ',
        'horses.csv': '馬の血統データ',
        'horses_performance.csv': '馬の過去成績データ'
    }

    for filename, description in files.items():
        file_path = output_dir / filename
        analyze_csv_simple(file_path, filename)

    check_consistency()

    print(f"\n{'='*80}")
    print(f"分析完了")
    print(f"{'='*80}")

if __name__ == '__main__':
    main()
