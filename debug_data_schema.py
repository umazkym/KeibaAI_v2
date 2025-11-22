#!/usr/bin/env python3
"""
データスキーマ診断スクリプト
Phase D の特徴量生成でエラーが発生しているカラム名の不一致を調査
"""

import pandas as pd
from pathlib import Path
import sys

def inspect_parquet_schema(file_path, name):
    """Parquetファイルのスキーマを表示"""
    print(f"\n{'='*80}")
    print(f"📊 {name}")
    print(f"{'='*80}")

    try:
        df = pd.read_parquet(file_path)
        print(f"行数: {len(df):,}")
        print(f"カラム数: {len(df.columns)}")
        print(f"\nカラム一覧:")
        for i, col in enumerate(df.columns, 1):
            dtype = df[col].dtype
            null_count = df[col].isna().sum()
            null_pct = (null_count / len(df)) * 100 if len(df) > 0 else 0
            print(f"  {i:2d}. {col:30s} {str(dtype):15s} (NULL: {null_pct:5.1f}%)")

        # サンプルデータ表示
        print(f"\nサンプルデータ (先頭3行):")
        print(df.head(3).to_string())

    except FileNotFoundError:
        print(f"❌ ファイルが見つかりません: {file_path}")
    except Exception as e:
        print(f"❌ エラー: {e}")

def main():
    data_path = Path("keibaai/data")

    print("="*80)
    print("KeibaAI_v2 データスキーマ診断")
    print("="*80)

    # 1. レース結果データ (results_history_df のベース)
    races_file = data_path / "parsed/parquet/races/races.parquet"
    inspect_parquet_schema(races_file, "レース結果データ (races.parquet)")

    # 2. 出馬表データ
    shutuba_file = data_path / "parsed/parquet/shutuba/shutuba.parquet"
    inspect_parquet_schema(shutuba_file, "出馬表データ (shutuba.parquet)")

    # 3. 馬情報データ
    horses_file = data_path / "parsed/parquet/horses/horses.parquet"
    inspect_parquet_schema(horses_file, "馬情報データ (horses.parquet)")

    # 4. 血統データ
    pedigrees_file = data_path / "parsed/parquet/pedigrees/pedigrees.parquet"
    inspect_parquet_schema(pedigrees_file, "血統データ (pedigrees.parquet)")

    # 5. 既存特徴量ファイル (もし存在すれば)
    features_dir = data_path / "features/parquet"
    if features_dir.exists():
        # 最新の特徴量ファイルを探す
        parquet_files = list(features_dir.rglob("*.parquet"))
        if parquet_files:
            latest_features = max(parquet_files, key=lambda p: p.stat().st_mtime)
            inspect_parquet_schema(latest_features, f"既存特徴量データ ({latest_features.relative_to(data_path)})")

    print("\n" + "="*80)
    print("🔍 重要な調査ポイント:")
    print("="*80)
    print("1. 'place' カラムは存在するか？ → おそらく 'venue' または 'race_course'")
    print("2. 'sire_id' カラムは pedigrees.parquet に存在するか？")
    print("3. generate_course_affinity_features で使用するカラム:")
    print("   - 競馬場: 'place' → 実際は？")
    print("   - 距離: 'distance_m' → 存在する？")
    print("   - 馬場: 'track_surface' → 存在する？")
    print("4. generate_race_condition_features で使用するカラム:")
    print("   - 'head_count' (出走頭数)")
    print("   - 'race_date' (日付)")
    print("   - 'prize' (賞金)")
    print("="*80)

if __name__ == "__main__":
    main()
