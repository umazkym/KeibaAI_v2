"""
元データ（shutuba.parquet）のカラムを確認するスクリプト

目的:
- 出馬表データに必要なカラムが存在するか確認
- horse_number, jockey_id, trainer_id などの存在確認

使用方法:
    python check_source_data.py
"""

import pandas as pd
from pathlib import Path
import sys

def check_source_data():
    """元データのカラムを確認"""

    print("=" * 70)
    print("📋 元データ（shutuba.parquet）カラム確認")
    print("=" * 70)
    print()

    shutuba_path = Path("keibaai/data/parsed/parquet/shutuba/shutuba.parquet")

    if not shutuba_path.exists():
        print(f"❌ エラー: {shutuba_path} が存在しません")
        sys.exit(1)

    print(f"📂 ファイル: {shutuba_path}")
    print()

    # データ読み込み
    df = pd.read_parquet(shutuba_path)

    # 基本情報
    print("=" * 70)
    print("📊 基本情報")
    print("=" * 70)
    print(f"総行数: {len(df):,}行")
    print(f"総カラム数: {len(df.columns)}カラム")
    print()

    # 重要カラムの存在確認
    print("=" * 70)
    print("🔍 重要カラムの存在チェック")
    print("=" * 70)

    required_cols = [
        'race_id', 'horse_id', 'horse_number',
        'jockey_id', 'trainer_id',
        'distance_m', 'track_surface', 'weather',
        'bracket_number', 'sex', 'age', 'basis_weight',
        'morning_odds', 'blinker'
    ]

    missing_cols = []
    existing_cols = []

    for col in required_cols:
        exists = col in df.columns
        status = "✅ 存在" if exists else "❌ 不在"
        print(f"{status}: {col}")

        if exists:
            existing_cols.append(col)
        else:
            missing_cols.append(col)

    print()

    # サンプルデータ表示
    if existing_cols:
        print("=" * 70)
        print("📋 サンプルデータ（先頭5件）")
        print("=" * 70)

        # 存在するカラムのみ表示
        sample_cols = [c for c in required_cols if c in df.columns][:10]
        print(df[sample_cols].head(5).to_string())
        print()

    # horse_number の詳細確認
    if 'horse_number' in df.columns:
        print("=" * 70)
        print("🔍 horse_number カラムの詳細")
        print("=" * 70)

        print(f"データ型: {df['horse_number'].dtype}")
        print(f"ユニーク値数: {df['horse_number'].nunique()}")
        print(f"欠損値数: {df['horse_number'].isna().sum()}")
        print(f"範囲: {df['horse_number'].min()} 〜 {df['horse_number'].max()}")
        print()

        print("horse_number の分布（上位20件）:")
        print(df['horse_number'].value_counts().sort_index().head(20))
        print()

    # まとめ
    print("=" * 70)
    print("📋 まとめ")
    print("=" * 70)

    if missing_cols:
        print(f"❌ 不足カラム ({len(missing_cols)}個):")
        for col in missing_cols:
            print(f"  - {col}")
        print()
        print("これらのカラムは元データに存在しないため、特徴量に含めることができません。")
    else:
        print("✅ 全ての必要カラムが元データに存在します。")
        print()
        print("特徴量生成時にこれらのカラムが保持されるように、")
        print("feature_engine.py を修正する必要があります。")

    print()
    print("=" * 70)

if __name__ == "__main__":
    try:
        check_source_data()
    except Exception as e:
        print(f"❌ エラーが発生しました: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
