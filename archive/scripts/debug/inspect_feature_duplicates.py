#!/usr/bin/env python3
"""
Features重複の根本原因調査スクリプト

目的:
    features/parquet/ 内のデータの重複がどのような性質なのかを詳細に分析し、
    根本原因を特定する。

調査項目:
    1. 重複の性質（完全重複 vs 部分重複）
    2. 重複パターンの詳細分析
    3. 重複が発生しているrace_id/horse_idの特徴
    4. 同じ(race_id, horse_id)で値が異なるカラムの特定
"""

import sys
from pathlib import Path
import pandas as pd
import numpy as np

# プロジェクトルートをパスに追加
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

def print_section(title: str):
    """セクションヘッダーを出力"""
    print("\n" + "="*80)
    print(f"  {title}")
    print("="*80)

def analyze_duplicate_nature(df: pd.DataFrame):
    """重複の性質を分析（完全重複 vs 部分重複）"""
    print_section("1. 重複の性質分析")

    total_rows = len(df)

    # 完全重複（全カラムが同じ）
    full_dup_mask = df.duplicated(keep=False)
    full_dup_count = full_dup_mask.sum()

    # キー重複（race_id + horse_idのみ同じ）
    key_dup_mask = df.duplicated(subset=['race_id', 'horse_id'], keep=False)
    key_dup_count = key_dup_mask.sum()

    # 部分重複（キーは同じだが、他のカラムが異なる）
    partial_dup_count = key_dup_count - full_dup_count

    print(f"総行数: {total_rows:,}")
    print(f"\n【重複の分類】")
    print(f"  完全重複（全カラム同一）: {full_dup_count:,}行 ({full_dup_count/total_rows*100:.1f}%)")
    print(f"  部分重複（キーのみ同一）: {partial_dup_count:,}行 ({partial_dup_count/total_rows*100:.1f}%)")
    print(f"  ユニーク行: {total_rows - key_dup_count:,}行 ({(total_rows - key_dup_count)/total_rows*100:.1f}%)")

    return full_dup_mask, key_dup_mask

def analyze_duplicate_patterns(df: pd.DataFrame, key_dup_mask):
    """重複パターンの詳細分析"""
    print_section("2. 重複パターンの詳細分析")

    # 重複しているrace_id + horse_idのペアを取得
    dup_df = df[key_dup_mask]

    # 各(race_id, horse_id)ペアの重複回数を集計
    dup_counts = dup_df.groupby(['race_id', 'horse_id']).size()

    print(f"重複している(race_id, horse_id)ペア数: {len(dup_counts):,}")
    print(f"\n【重複回数の分布】")

    dup_count_dist = dup_counts.value_counts().sort_index()
    for count, freq in dup_count_dist.items():
        print(f"  {count}回重複: {freq:,}ペア")

    # 最も重複が多いペアを表示
    print(f"\n【重複が多い上位5ペア】")
    top_dups = dup_counts.nlargest(5)
    for (race_id, horse_id), count in top_dups.items():
        print(f"  race_id={race_id}, horse_id={horse_id}: {count}回")

    return dup_counts

def analyze_value_differences(df: pd.DataFrame, dup_counts):
    """同じキーの重複行で値が異なるカラムを特定"""
    print_section("3. 重複行での値の差異分析")

    # サンプルとして最も重複が多いペアを調査
    if len(dup_counts) == 0:
        print("重複が存在しないため、分析をスキップします。")
        return

    sample_race_id, sample_horse_id = dup_counts.idxmax()
    sample_rows = df[(df['race_id'] == sample_race_id) & (df['horse_id'] == sample_horse_id)]

    print(f"サンプル: race_id={sample_race_id}, horse_id={sample_horse_id}")
    print(f"この(race_id, horse_id)の重複行数: {len(sample_rows)}")

    # 各カラムで値が異なるかチェック
    print(f"\n【カラムごとの値の一致状況】")

    varying_cols = []
    constant_cols = []

    for col in sample_rows.columns:
        unique_values = sample_rows[col].nunique()
        if unique_values == 1:
            constant_cols.append(col)
        else:
            varying_cols.append(col)

    print(f"値が一致するカラム数: {len(constant_cols)}")
    print(f"値が異なるカラム数: {len(varying_cols)}")

    if varying_cols:
        print(f"\n【値が異なるカラム】")
        for col in varying_cols[:20]:  # 最大20個まで表示
            values = sample_rows[col].unique()
            print(f"  {col}: {len(values)}種類の値")
            if len(values) <= 5:
                print(f"    値: {values}")

    # サンプル行を表示（最初の3行のみ）
    print(f"\n【サンプル重複行（最初3行）】")
    display_cols = ['race_id', 'horse_id', 'age', 'horse_weight', 'distance_m',
                    'jockey_win_rate', 'trainer_win_rate']
    available_cols = [col for col in display_cols if col in sample_rows.columns]

    if available_cols:
        print(sample_rows[available_cols].head(3).to_string(index=True))

    return varying_cols, constant_cols

def analyze_race_distribution(df: pd.DataFrame):
    """レースごとの馬数と重複の関係"""
    print_section("4. レースごとの馬数・重複分析")

    # ユニークな(race_id, horse_id)ペアでカウント
    unique_pairs = df[['race_id', 'horse_id']].drop_duplicates()
    horses_per_race = unique_pairs.groupby('race_id').size()

    # 実際の行数でカウント（重複含む）
    rows_per_race = df.groupby('race_id').size()

    # 重複倍率を計算
    duplication_ratio = rows_per_race / horses_per_race

    print(f"総レース数: {len(horses_per_race):,}")
    print(f"\n【ユニーク馬数の分布】")
    print(horses_per_race.describe())

    print(f"\n【実際の行数（重複含む）の分布】")
    print(rows_per_race.describe())

    print(f"\n【重複倍率の分布】")
    print(duplication_ratio.describe())

    # 重複倍率が高いレース
    print(f"\n【重複倍率が高いレース（上位5）】")
    high_dup_races = duplication_ratio.nlargest(5)
    for race_id, ratio in high_dup_races.items():
        unique_horses = horses_per_race[race_id]
        total_rows = rows_per_race[race_id]
        print(f"  レース {race_id}: {unique_horses}頭 → {total_rows}行（{ratio:.1f}倍）")

def check_file_metadata(file_path: Path):
    """Parquetファイルのメタデータ確認"""
    print_section("5. Parquetファイルのメタデータ")

    import pyarrow.parquet as pq

    parquet_file = pq.ParquetFile(file_path)

    print(f"ファイルパス: {file_path}")
    print(f"行グループ数: {parquet_file.num_row_groups}")
    print(f"総行数: {parquet_file.metadata.num_rows:,}")

    print(f"\n【行グループ別の行数】")
    for i in range(parquet_file.num_row_groups):
        rg = parquet_file.metadata.row_group(i)
        print(f"  行グループ {i}: {rg.num_rows:,}行")

    # 複数の行グループがある場合、それぞれに同じデータが含まれている可能性
    if parquet_file.num_row_groups > 1:
        print(f"\n⚠️ 警告: 行グループが{parquet_file.num_row_groups}個存在します。")
        print("   → 同じデータが複数の行グループに書き込まれている可能性があります。")

def main():
    """メイン処理"""
    # 2024年1月のfeatureファイルを調査
    features_path = project_root / "keibaai" / "data" / "features" / "parquet" / "year=2024" / "month=1"

    print("="*80)
    print("  Features 重複の根本原因調査")
    print("="*80)
    print(f"対象ディレクトリ: {features_path}")

    if not features_path.exists():
        print(f"\n❌ エラー: ディレクトリが存在しません - {features_path}")
        return 1

    # Parquetファイルを読み込み
    print("\nデータ読み込み中...")
    df = pd.read_parquet(features_path)
    print(f"読み込み完了: {len(df):,}行 × {len(df.columns)}列")

    # 各分析を実行
    full_dup_mask, key_dup_mask = analyze_duplicate_nature(df)
    dup_counts = analyze_duplicate_patterns(df, key_dup_mask)
    varying_cols, constant_cols = analyze_value_differences(df, dup_counts)
    analyze_race_distribution(df)

    # 最初のParquetファイルのメタデータを確認
    first_file = list(features_path.glob("*.parquet"))[0]
    check_file_metadata(first_file)

    print_section("調査完了")

    # 次のステップの提案
    print("\n【分析結果からの推論】")

    if len(varying_cols) == 0:
        print("✅ 重複行は完全に同一 → 処理の重複実行またはファイル保存時の問題")
        print("\n推奨される次のステップ:")
        print("  1. generate_features.py のロジックを確認")
        print("  2. Parquet保存処理（append/overwriteモード）を確認")
        print("  3. 重複を削除して保存し直す")
    else:
        print("⚠️ 重複行で値が異なる → ロジックの問題（異なる計算結果が混在）")
        print(f"   値が異なるカラム数: {len(varying_cols)}")
        print("\n推奨される次のステップ:")
        print("  1. feature_engine.py の特徴量計算ロジックを確認")
        print("  2. rolling計算などで意図しない重複が発生していないか確認")
        print("  3. データ生成プロセス全体を見直す")

    return 0

if __name__ == "__main__":
    exit(main())
