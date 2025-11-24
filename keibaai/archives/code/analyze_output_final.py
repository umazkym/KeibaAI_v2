#!/usr/bin/env python3
"""
output_finalフォルダのデータを深く分析するスクリプト
"""
import pandas as pd
import numpy as np
from pathlib import Path
import warnings
warnings.filterwarnings('ignore')

def analyze_csv(file_path: Path, name: str):
    """CSVファイルの詳細分析"""
    print(f"\n{'='*80}")
    print(f"分析対象: {name}")
    print(f"{'='*80}")

    try:
        # BOMを考慮してUTF-8で読み込み
        df = pd.read_csv(file_path, encoding='utf-8-sig')

        print(f"✓ ファイル読み込み成功")
        print(f"  行数: {len(df):,}")
        print(f"  列数: {len(df.columns)}")
        print(f"  ファイルサイズ: {file_path.stat().st_size / 1024:.1f} KB")

        # カラム一覧
        print(f"\n【カラム一覧】 ({len(df.columns)}列)")
        for i, col in enumerate(df.columns, 1):
            dtype = df[col].dtype
            null_count = df[col].isna().sum()
            null_pct = null_count / len(df) * 100 if len(df) > 0 else 0
            unique_count = df[col].nunique()
            print(f"  {i:2d}. {col:30s} | dtype: {str(dtype):10s} | null: {null_count:5d} ({null_pct:5.1f}%) | unique: {unique_count:6d}")

        # データ型の分布
        print(f"\n【データ型の分布】")
        dtype_counts = df.dtypes.value_counts()
        for dtype, count in dtype_counts.items():
            print(f"  {dtype}: {count}列")

        # 欠損値の概要
        null_cols = df.columns[df.isna().any()].tolist()
        if null_cols:
            print(f"\n【欠損値あり列】 ({len(null_cols)}列)")
            for col in null_cols[:20]:  # 最初の20列のみ表示
                null_count = df[col].isna().sum()
                null_pct = null_count / len(df) * 100
                print(f"  {col:30s}: {null_count:6d} ({null_pct:5.1f}%)")
            if len(null_cols) > 20:
                print(f"  ... 他 {len(null_cols) - 20}列")
        else:
            print(f"\n✓ 欠損値なし")

        # キーカラムの分析（ファイル種別により異なる）
        if 'race_id' in df.columns:
            print(f"\n【race_idの分析】")
            print(f"  ユニーク数: {df['race_id'].nunique():,}")
            print(f"  重複: {len(df) - df['race_id'].nunique():,}")

            # race_idの形式チェック（12桁の数値であるべき）
            if df['race_id'].dtype == 'object':
                valid_format = df['race_id'].astype(str).str.match(r'^\d{12}$')
                invalid_count = (~valid_format).sum()
                if invalid_count > 0:
                    print(f"  ⚠ 不正な形式: {invalid_count}件")
                    print(f"    例: {df.loc[~valid_format, 'race_id'].head(3).tolist()}")
                else:
                    print(f"  ✓ 形式OK（全て12桁の数値）")

        if 'horse_id' in df.columns:
            print(f"\n【horse_idの分析】")
            print(f"  ユニーク数: {df['horse_id'].nunique():,}")

            # horse_idの形式チェック（10桁の数値であるべき）
            if df['horse_id'].dtype in ['object', 'int64']:
                horse_id_str = df['horse_id'].astype(str)
                valid_format = horse_id_str.str.match(r'^\d{10}$')
                invalid_count = (~valid_format).sum()
                if invalid_count > 0:
                    print(f"  ⚠ 不正な形式: {invalid_count}件")
                    print(f"    例: {df.loc[~valid_format, 'horse_id'].head(3).tolist()}")
                else:
                    print(f"  ✓ 形式OK（全て10桁の数値）")

        if 'race_date' in df.columns:
            print(f"\n【race_dateの分析】")
            print(f"  最小日付: {df['race_date'].min()}")
            print(f"  最大日付: {df['race_date'].max()}")
            print(f"  ユニーク日数: {df['race_date'].nunique()}")

        # 数値カラムの統計
        numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
        if numeric_cols:
            print(f"\n【数値カラムの基本統計】")
            stats = df[numeric_cols].describe()
            print(stats.to_string())

        # サンプルデータ
        print(f"\n【サンプルデータ（先頭3行）】")
        print(df.head(3).to_string())

        # データ品質スコア
        print(f"\n【データ品質スコア】")
        total_cells = len(df) * len(df.columns)
        null_cells = df.isna().sum().sum()
        quality_score = (1 - null_cells / total_cells) * 100 if total_cells > 0 else 0
        print(f"  完全性: {quality_score:.2f}% ({total_cells - null_cells:,} / {total_cells:,} cells)")

        # 特定のファイルタイプ専用チェック
        if name == 'race_results.csv':
            print(f"\n【race_results専用チェック】")

            # finish_positionの分析
            if 'finish_position' in df.columns:
                print(f"  finish_position:")
                print(f"    範囲: {df['finish_position'].min():.1f} ~ {df['finish_position'].max():.1f}")
                print(f"    欠損: {df['finish_position'].isna().sum()}")

            # 距離の分析
            if 'distance_m' in df.columns:
                print(f"  distance_m:")
                print(f"    範囲: {df['distance_m'].min():.0f}m ~ {df['distance_m'].max():.0f}m")
                print(f"    ユニーク値: {df['distance_m'].nunique()}")
                print(f"    欠損: {df['distance_m'].isna().sum()}")

            # 馬場状態
            if 'track_surface' in df.columns:
                print(f"  track_surface:")
                surface_counts = df['track_surface'].value_counts()
                for surface, count in surface_counts.items():
                    print(f"    {surface}: {count:,}")

            # 天候
            if 'weather' in df.columns:
                print(f"  weather:")
                weather_counts = df['weather'].value_counts()
                for weather, count in weather_counts.items():
                    print(f"    {weather}: {count:,}")

        elif name == 'shutuba.csv':
            print(f"\n【shutuba専用チェック】")

            # 取消馬のチェック
            if 'scratched' in df.columns:
                scratched_count = df['scratched'].sum()
                print(f"  取消馬: {scratched_count} / {len(df)} ({scratched_count/len(df)*100:.1f}%)")

        elif name == 'horses.csv':
            print(f"\n【horses専用チェック（血統データ）】")

            # 世代の分布
            if 'generation' in df.columns:
                print(f"  世代の分布:")
                gen_counts = df['generation'].value_counts().sort_index()
                for gen, count in gen_counts.items():
                    print(f"    第{gen}世代: {count:,}")

            # ユニークな馬の数
            if 'horse_id' in df.columns:
                unique_horses = df['horse_id'].nunique()
                total_records = len(df)
                avg_ancestors = total_records / unique_horses if unique_horses > 0 else 0
                print(f"  ユニーク馬数: {unique_horses:,}")
                print(f"  平均祖先数: {avg_ancestors:.1f}")

        elif name == 'horses_performance.csv':
            print(f"\n【horses_performance専用チェック】")

            # 馬ごとの成績数
            if 'horse_id' in df.columns:
                perf_per_horse = df.groupby('horse_id').size()
                print(f"  馬ごとの成績数:")
                print(f"    平均: {perf_per_horse.mean():.1f}")
                print(f"    中央値: {perf_per_horse.median():.1f}")
                print(f"    最大: {perf_per_horse.max()}")
                print(f"    最小: {perf_per_horse.min()}")

        return True

    except Exception as e:
        print(f"✗ エラー: {e}")
        import traceback
        traceback.print_exc()
        return False

def check_data_consistency():
    """データの整合性チェック"""
    print(f"\n{'='*80}")
    print(f"データ整合性チェック")
    print(f"{'='*80}")

    try:
        # ファイル読み込み
        race_results = pd.read_csv('output_final/race_results.csv', encoding='utf-8-sig')
        shutuba = pd.read_csv('output_final/shutuba.csv', encoding='utf-8-sig')
        horses = pd.read_csv('output_final/horses.csv', encoding='utf-8-sig')
        horses_perf = pd.read_csv('output_final/horses_performance.csv', encoding='utf-8-sig')

        print(f"\n【1. race_id の整合性】")
        race_results_ids = set(race_results['race_id'].unique())
        shutuba_ids = set(shutuba['race_id'].unique())

        print(f"  race_results: {len(race_results_ids):,} レース")
        print(f"  shutuba:      {len(shutuba_ids):,} レース")

        # 共通のrace_id
        common_ids = race_results_ids & shutuba_ids
        print(f"  共通:         {len(common_ids):,} レース")

        # race_resultsにあってshutubaにないもの
        only_results = race_results_ids - shutuba_ids
        if only_results:
            print(f"  ⚠ race_resultsのみ: {len(only_results)} ({', '.join(str(x) for x in list(only_results)[:5])}...)")

        # shutubaにあってrace_resultsにないもの
        only_shutuba = shutuba_ids - race_results_ids
        if only_shutuba:
            print(f"  ⚠ shutubaのみ: {len(only_shutuba)} ({', '.join(str(x) for x in list(only_shutuba)[:5])}...)")

        print(f"\n【2. horse_id の整合性】")
        race_results_horses = set(race_results['horse_id'].unique())
        shutuba_horses = set(shutuba['horse_id'].unique())
        horses_horses = set(horses['horse_id'].unique())
        horses_perf_horses = set(horses_perf['horse_id'].unique())

        print(f"  race_results:       {len(race_results_horses):,} 頭")
        print(f"  shutuba:            {len(shutuba_horses):,} 頭")
        print(f"  horses (血統):      {len(horses_horses):,} 頭")
        print(f"  horses_performance: {len(horses_perf_horses):,} 頭")

        # race_resultsの馬がhorsesに存在するか
        horses_in_results_not_in_horses = race_results_horses - horses_horses
        if horses_in_results_not_in_horses:
            print(f"  ⚠ race_resultsにあるがhorsesにない: {len(horses_in_results_not_in_horses)} 頭")
            print(f"    例: {list(horses_in_results_not_in_horses)[:5]}")
        else:
            print(f"  ✓ race_resultsの全馬がhorsesに存在")

        # shutubaの馬がhorsesに存在するか
        horses_in_shutuba_not_in_horses = shutuba_horses - horses_horses
        if horses_in_shutuba_not_in_horses:
            print(f"  ⚠ shutubaにあるがhorsesにない: {len(horses_in_shutuba_not_in_horses)} 頭")
            print(f"    例: {list(horses_in_shutuba_not_in_horses)[:5]}")
        else:
            print(f"  ✓ shutubaの全馬がhorsesに存在")

        print(f"\n【3. race_resultsとshutubaのレコード数整合性】")
        # 各race_idごとの馬数を比較
        results_counts = race_results.groupby('race_id').size()
        shutuba_counts = shutuba.groupby('race_id').size()

        # 共通race_idで比較
        common_race_ids = list(common_ids)[:100]  # 最初の100レースで確認
        mismatches = []
        for race_id in common_race_ids:
            results_count = results_counts.get(race_id, 0)
            shutuba_count = shutuba_counts.get(race_id, 0)
            if results_count != shutuba_count:
                mismatches.append({
                    'race_id': race_id,
                    'results': results_count,
                    'shutuba': shutuba_count,
                    'diff': results_count - shutuba_count
                })

        if mismatches:
            print(f"  ⚠ 馬数不一致: {len(mismatches)} / {len(common_race_ids)} レース")
            print(f"  例:")
            for mismatch in mismatches[:5]:
                print(f"    race_id={mismatch['race_id']}: results={mismatch['results']}, shutuba={mismatch['shutuba']} (差={mismatch['diff']})")
        else:
            print(f"  ✓ 馬数一致（確認したレース: {len(common_race_ids)}）")

        print(f"\n【4. データ日付範囲】")
        if 'race_date' in race_results.columns:
            print(f"  race_results: {race_results['race_date'].min()} ~ {race_results['race_date'].max()}")
        if 'race_date' in shutuba.columns:
            print(f"  shutuba:      {shutuba['race_date'].min()} ~ {shutuba['race_date'].max()}")
        if 'race_date' in horses_perf.columns:
            print(f"  horses_perf:  {horses_perf['race_date'].min()} ~ {horses_perf['race_date'].max()}")

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

    # 各ファイルを分析
    files = {
        'race_results.csv': 'レース結果データ',
        'shutuba.csv': '出馬表データ',
        'horses.csv': '馬の血統データ',
        'horses_performance.csv': '馬の過去成績データ'
    }

    for filename, description in files.items():
        file_path = output_dir / filename
        if file_path.exists():
            analyze_csv(file_path, filename)
        else:
            print(f"\n✗ {filename} が見つかりません")

    # データ整合性チェック
    check_data_consistency()

    print(f"\n{'='*80}")
    print(f"分析完了")
    print(f"{'='*80}")

if __name__ == '__main__':
    main()
