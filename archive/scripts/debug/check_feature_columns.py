"""
生成された特徴量データのカラムを確認するスクリプト

目的:
- horse_number カラムが含まれているか確認
- 全カラムリストを表示
- サンプルデータを確認

使用方法:
    python check_feature_columns.py
"""

import pandas as pd
from pathlib import Path
import sys

def check_feature_columns():
    """特徴量データのカラムを確認"""

    print("=" * 70)
    print("📋 特徴量データ カラム確認")
    print("=" * 70)
    print()

    base_path = Path("keibaai/data/features/parquet")

    if not base_path.exists():
        print(f"❌ エラー: {base_path} が存在しません")
        sys.exit(1)

    # 2023年1月のデータを1つ読み込んでカラムを確認
    sample_path = base_path / "year=2023" / "month=1"

    if not sample_path.exists():
        print(f"❌ エラー: {sample_path} が存在しません")
        sys.exit(1)

    parquet_files = list(sample_path.glob("*.parquet"))

    if not parquet_files:
        print(f"❌ エラー: Parquetファイルが見つかりません")
        sys.exit(1)

    # 最初のファイルを読み込み
    sample_file = parquet_files[0]
    print(f"📂 サンプルファイル: {sample_file.name}")
    print()

    df = pd.read_parquet(sample_file)

    # 基本情報
    print("=" * 70)
    print("📊 基本情報")
    print("=" * 70)
    print(f"総行数: {len(df):,}行")
    print(f"総カラム数: {len(df.columns)}カラム")
    print()

    # horse_number の存在確認
    print("=" * 70)
    print("🔍 重要カラムの存在チェック")
    print("=" * 70)

    important_cols = [
        'race_id', 'horse_id', 'horse_number',
        'jockey_id', 'trainer_id',
        'distance_m', 'track_surface', 'weather'
    ]

    for col in important_cols:
        exists = col in df.columns
        status = "✅ 存在" if exists else "❌ 不在"
        print(f"{status}: {col}")

    print()

    # 全カラムリスト
    print("=" * 70)
    print("📋 全カラムリスト")
    print("=" * 70)

    # カテゴリ別に分類
    id_cols = [c for c in df.columns if c.endswith('_id') or c == 'race_id' or c == 'horse_id']
    feature_cols = [c for c in df.columns if c not in id_cols and not c.startswith('year') and not c.startswith('month')]
    partition_cols = [c for c in df.columns if c in ['year', 'month', 'day']]

    print(f"\n🔑 IDカラム ({len(id_cols)}個):")
    for col in sorted(id_cols):
        print(f"  - {col}")

    print(f"\n📊 特徴量カラム ({len(feature_cols)}個):")
    for i, col in enumerate(sorted(feature_cols), 1):
        print(f"  {i:3d}. {col}")

    if partition_cols:
        print(f"\n📅 パーティションカラム ({len(partition_cols)}個):")
        for col in sorted(partition_cols):
            print(f"  - {col}")

    print()

    # horse_number が存在する場合、サンプルデータを表示
    if 'horse_number' in df.columns:
        print("=" * 70)
        print("✅ horse_number カラムのサンプルデータ")
        print("=" * 70)
        print(df[['race_id', 'horse_id', 'horse_number']].head(10))
        print()

        # horse_number の分布確認
        print("horse_number の分布:")
        print(df['horse_number'].value_counts().sort_index().head(20))
        print()

        # horse_number=0 の件数
        zero_count = (df['horse_number'] == 0).sum()
        zero_rate = zero_count / len(df) * 100
        print(f"horse_number=0: {zero_count}件 ({zero_rate:.2f}%)")

    else:
        print("=" * 70)
        print("❌ horse_number カラムが存在しません")
        print("=" * 70)
        print("これはモデル学習に影響する可能性があります。")
        print()
        print("📋 推奨対応:")
        print("1. feature_engine.py を確認し、horse_number が保持されるように修正")
        print("2. generate_features.py を再実行")

    print()
    print("=" * 70)

if __name__ == "__main__":
    try:
        check_feature_columns()
    except Exception as e:
        print(f"❌ エラーが発生しました: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
