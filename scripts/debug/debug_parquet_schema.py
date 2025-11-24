#!/usr/bin/env python3
"""
Parquetファイルのスキーマと問題のあるカラムを診断するスクリプト
"""
import pyarrow.parquet as pq
import pandas as pd
from pathlib import Path

def diagnose_parquet_schema(parquet_path: Path):
    """Parquetファイルのスキーマを詳細に診断"""
    print(f"\n{'='*80}")
    print(f"診断対象: {parquet_path}")
    print(f"{'='*80}\n")

    try:
        # Parquetメタデータを読み込み
        parquet_file = pq.ParquetFile(parquet_path)
        schema = parquet_file.schema_arrow

        print(f"✓ ファイルは正常に開けました")
        print(f"✓ 行数: {parquet_file.metadata.num_rows:,}")
        print(f"✓ カラム数: {len(schema)}")
        print(f"\n{'─'*80}")
        print("スキーマ詳細:")
        print(f"{'─'*80}\n")

        # 問題のあるカラムを検出
        null_type_columns = []
        suspicious_columns = []

        for i, field in enumerate(schema):
            type_str = str(field.type)
            nullable = "nullable" if field.nullable else "non-nullable"

            # null型のカラムを検出
            if type_str == "null":
                null_type_columns.append(field.name)
                print(f"⚠️  {i+1:3d}. {field.name:30s} → {type_str:20s} ({nullable}) **問題あり**")
            # その他の疑わしいカラム
            elif "dictionary" in type_str.lower() or "large" in type_str.lower():
                suspicious_columns.append(field.name)
                print(f"⚠️  {i+1:3d}. {field.name:30s} → {type_str:20s} ({nullable}) *要注意*")
            else:
                print(f"    {i+1:3d}. {field.name:30s} → {type_str:20s} ({nullable})")

        # サマリー
        print(f"\n{'='*80}")
        print("診断結果サマリー")
        print(f"{'='*80}\n")

        if null_type_columns:
            print(f"❌ null型のカラムが見つかりました（{len(null_type_columns)}個）:")
            for col in null_type_columns:
                print(f"   - {col}")
            print("\n→ これらのカラムが ArrowNotImplementedError の原因です")
        else:
            print("✓ null型のカラムはありません")

        if suspicious_columns:
            print(f"\n⚠️  要注意カラム（{len(suspicious_columns)}個）:")
            for col in suspicious_columns:
                print(f"   - {col}")

        # データのサンプルを確認（最初の5行）
        print(f"\n{'─'*80}")
        print("データサンプル（最初の5行）:")
        print(f"{'─'*80}\n")

        try:
            # null型カラムを除外して読み込み
            columns_to_read = [field.name for field in schema if str(field.type) != "null"]

            table = pq.read_table(parquet_path, columns=columns_to_read)
            df = table.to_pandas()

            print(df.head())
            print(f"\n✓ データの読み込みに成功しました（null型カラムを除外）")
            print(f"✓ 実際の行数: {len(df):,}")

            # カラムごとの欠損値を確認
            print(f"\n{'─'*80}")
            print("欠損値の状況:")
            print(f"{'─'*80}\n")

            missing_summary = df.isnull().sum()
            missing_pct = (missing_summary / len(df) * 100).round(2)

            # 欠損値が多いカラムを表示
            high_missing = missing_pct[missing_pct > 50].sort_values(ascending=False)
            if len(high_missing) > 0:
                print("⚠️  欠損値が50%以上のカラム:")
                for col, pct in high_missing.items():
                    print(f"   - {col:30s}: {pct:6.2f}% ({missing_summary[col]:,} / {len(df):,})")
            else:
                print("✓ 欠損値が50%以上のカラムはありません")

        except Exception as e:
            print(f"❌ データ読み込み中にエラー: {e}")

        return null_type_columns, suspicious_columns

    except Exception as e:
        print(f"❌ ファイル診断中にエラーが発生: {e}")
        import traceback
        traceback.print_exc()
        return None, None


def main():
    """メイン処理"""
    # データパスを設定
    base_path = Path(__file__).parent / "keibaai" / "data" / "parsed" / "parquet"

    # 診断対象ファイル
    targets = [
        base_path / "races" / "races.parquet",
        base_path / "shutuba" / "shutuba.parquet",
        base_path / "horses" / "horses.parquet",
    ]

    all_null_columns = {}

    for target in targets:
        if target.exists():
            null_cols, suspicious_cols = diagnose_parquet_schema(target)
            if null_cols:
                all_null_columns[str(target)] = null_cols
        else:
            print(f"\n⚠️  ファイルが見つかりません: {target}")

    # 最終サマリー
    print(f"\n\n{'='*80}")
    print("全体サマリー")
    print(f"{'='*80}\n")

    if all_null_columns:
        print("❌ 修正が必要なファイル:")
        for file_path, columns in all_null_columns.items():
            print(f"\n  {Path(file_path).name}:")
            for col in columns:
                print(f"    - {col} (null型)")

        print("\n\n【推奨される対処法】")
        print("1. これらのnull型カラムを削除する")
        print("2. または、適切なデータ型（string, int64等）に変換する")
        print("\n次のステップ: 修正スクリプトを実行してください")
    else:
        print("✓ 全てのファイルに問題はありません")


if __name__ == "__main__":
    main()
