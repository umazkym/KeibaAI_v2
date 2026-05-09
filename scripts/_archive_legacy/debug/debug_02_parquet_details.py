#!/usr/bin/env python3
"""
デバッグスクリプト02: Parquetファイルの詳細分析
パース済みデータの内容、カラム、データ型、欠損状況を詳しく調査します
"""

import pandas as pd
from pathlib import Path
from datetime import datetime
import numpy as np

def analyze_parquet_file(file_path: Path, file_name: str):
    """Parquetファイルを詳細分析"""
    print("\n" + "=" * 80)
    print(f"📄 {file_name} の詳細分析")
    print("=" * 80)

    if not file_path.exists():
        print(f"❌ ファイルが存在しません: {file_path}")
        return None

    try:
        df = pd.read_parquet(file_path)

        # 基本情報
        print(f"\n📊 基本情報:")
        print(f"  行数: {len(df):,} 行")
        print(f"  列数: {len(df.columns)} 列")
        print(f"  メモリ使用量: {df.memory_usage(deep=True).sum() / 1024 / 1024:.2f} MB")

        # カラム一覧と型
        print(f"\n📋 カラム一覧 ({len(df.columns)}個):")
        print(f"\n{'カラム名':<35s} | {'データ型':<15s} | {'欠損数':<10s} | {'欠損率':<8s}")
        print("-" * 80)

        for col in df.columns:
            dtype = str(df[col].dtype)
            null_count = df[col].isna().sum()
            null_rate = (null_count / len(df)) * 100 if len(df) > 0 else 0
            print(f"{col:<35s} | {dtype:<15s} | {null_count:<10,} | {null_rate:>6.2f}%")

        # データサンプル（最初の3行）
        print(f"\n🔍 データサンプル（最初の3行）:")
        print("-" * 80)
        if len(df) > 0:
            # pandasの表示オプションを一時的に変更
            with pd.option_context('display.max_columns', None,
                                   'display.width', None,
                                   'display.max_colwidth', 30):
                print(df.head(3).to_string())
        else:
            print("  データが空です")

        # 日付範囲の確認（race_dateカラムがあれば）
        if 'race_date' in df.columns:
            print(f"\n📅 日付範囲:")
            try:
                df['race_date'] = pd.to_datetime(df['race_date'])
                min_date = df['race_date'].min()
                max_date = df['race_date'].max()
                unique_dates = df['race_date'].nunique()
                print(f"  最小日付: {min_date.strftime('%Y-%m-%d') if pd.notna(min_date) else 'N/A'}")
                print(f"  最大日付: {max_date.strftime('%Y-%m-%d') if pd.notna(max_date) else 'N/A'}")
                print(f"  ユニーク日数: {unique_dates:,} 日")
            except Exception as e:
                print(f"  ⚠️ 日付解析エラー: {e}")

        # race_idの範囲確認
        if 'race_id' in df.columns:
            print(f"\n🏇 レースID情報:")
            unique_races = df['race_id'].nunique()
            print(f"  ユニークレース数: {unique_races:,}")

            # レースIDの年度別集計
            try:
                df['year'] = df['race_id'].astype(str).str[:4]
                year_counts = df.groupby('year')['race_id'].nunique().sort_index()
                print(f"\n  年度別レース数:")
                for year, count in year_counts.items():
                    print(f"    {year}: {count:>6,} レース")
                df.drop('year', axis=1, inplace=True)
            except Exception as e:
                print(f"  ⚠️ 年度集計エラー: {e}")

        # horse_idの範囲確認
        if 'horse_id' in df.columns:
            print(f"\n🐴 馬ID情報:")
            unique_horses = df['horse_id'].nunique()
            print(f"  ユニーク馬数: {unique_horses:,}")

            # 馬IDの生年別集計
            try:
                df['birth_year'] = df['horse_id'].astype(str).str[:4]
                birth_year_counts = df.groupby('birth_year')['horse_id'].nunique().sort_index()
                print(f"\n  生年別馬数（上位10年）:")
                for birth_year, count in birth_year_counts.tail(10).items():
                    print(f"    {birth_year}: {count:>6,} 頭")
                df.drop('birth_year', axis=1, inplace=True)
            except Exception as e:
                print(f"  ⚠️ 生年集計エラー: {e}")

        # 統計情報（数値カラムのみ）
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        if len(numeric_cols) > 0:
            print(f"\n📈 数値カラムの統計情報:")
            print("-" * 80)
            stats = df[numeric_cols].describe().T
            print(stats.to_string())

        return df

    except Exception as e:
        print(f"\n❌ ファイル読み込みエラー: {e}")
        import traceback
        traceback.print_exc()
        return None

def check_horses_performance():
    """馬過去成績ファイルの確認"""
    print("\n" + "=" * 80)
    print("🔍 馬過去成績ファイルの確認")
    print("=" * 80)

    perf_path = Path("keibaai/data/parsed/parquet/horses/horses_performance.parquet")

    if perf_path.exists():
        print(f"\n✅ horses_performance.parquet が存在します")
        df = analyze_parquet_file(perf_path, "horses_performance")
    else:
        print(f"\n❌ horses_performance.parquet が存在しません")
        print(f"   期待されるパス: {perf_path}")
        print(f"\n💡 REVIEW.mdでは実装済みとなっていますが、ファイルが生成されていません")
        print(f"   → パース処理を再実行する必要があります")

def compare_raw_vs_parsed():
    """RAWデータとパース済みデータの比較"""
    print("\n" + "=" * 80)
    print("📊 RAWデータ vs パース済みデータの比較")
    print("=" * 80)

    # RAWレースファイル数
    raw_race_dir = Path("keibaai/data/raw/html/race")
    raw_race_count = len(list(raw_race_dir.glob("*.bin"))) if raw_race_dir.exists() else 0

    # パース済みレース数
    parsed_race_path = Path("keibaai/data/parsed/parquet/races/races.parquet")
    if parsed_race_path.exists():
        df_races = pd.read_parquet(parsed_race_path)
        parsed_race_count = df_races['race_id'].nunique() if 'race_id' in df_races.columns else len(df_races)
    else:
        parsed_race_count = 0

    # RAW馬ファイル数（profileとperfの半分）
    raw_horse_dir = Path("keibaai/data/raw/html/horse")
    raw_horse_files = list(raw_horse_dir.glob("*.bin")) if raw_horse_dir.exists() else []
    raw_horse_count = len([f for f in raw_horse_files if '_profile.bin' in f.name])

    # パース済み馬数
    parsed_horse_path = Path("keibaai/data/parsed/parquet/horses/horses.parquet")
    if parsed_horse_path.exists():
        df_horses = pd.read_parquet(parsed_horse_path)
        parsed_horse_count = df_horses['horse_id'].nunique() if 'horse_id' in df_horses.columns else len(df_horses)
    else:
        parsed_horse_count = 0

    print(f"\n📈 比較結果:")
    print(f"\n  レースデータ:")
    print(f"    RAW HTML:     {raw_race_count:>8,} 件")
    print(f"    パース済み:   {parsed_race_count:>8,} レース")
    print(f"    パース率:     {(parsed_race_count / raw_race_count * 100) if raw_race_count > 0 else 0:>7.2f} %")
    print(f"    未パース:     {raw_race_count - parsed_race_count:>8,} 件")

    print(f"\n  馬データ:")
    print(f"    RAW HTML:     {raw_horse_count:>8,} 頭")
    print(f"    パース済み:   {parsed_horse_count:>8,} 頭")
    print(f"    パース率:     {(parsed_horse_count / raw_horse_count * 100) if raw_horse_count > 0 else 0:>7.2f} %")
    print(f"    未パース:     {raw_horse_count - parsed_horse_count:>8,} 頭")

    if parsed_race_count < raw_race_count:
        print(f"\n⚠️ 約{raw_race_count - parsed_race_count:,}件のレースがパースされていません")

    if parsed_horse_count < raw_horse_count:
        print(f"\n🚨 約{raw_horse_count - parsed_horse_count:,}頭の馬データがパースされていません")
        print(f"   これはモデル精度に大きく影響します！")

def main():
    """メイン処理"""
    print("\n")
    print("🔍 KeibaAI_v2 Parquetファイル詳細分析")
    print(f"⏰ 実行時刻: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("\n")

    # 1. races.parquet 分析
    analyze_parquet_file(
        Path("keibaai/data/parsed/parquet/races/races.parquet"),
        "races.parquet"
    )

    # 2. shutuba.parquet 分析
    analyze_parquet_file(
        Path("keibaai/data/parsed/parquet/shutuba/shutuba.parquet"),
        "shutuba.parquet"
    )

    # 3. horses.parquet 分析
    analyze_parquet_file(
        Path("keibaai/data/parsed/parquet/horses/horses.parquet"),
        "horses.parquet"
    )

    # 4. pedigrees.parquet 分析
    analyze_parquet_file(
        Path("keibaai/data/parsed/parquet/pedigrees/pedigrees.parquet"),
        "pedigrees.parquet"
    )

    # 5. 馬過去成績ファイルの確認
    check_horses_performance()

    # 6. RAW vs パース済み比較
    compare_raw_vs_parsed()

    # 最終サマリー
    print("\n" + "=" * 80)
    print("📋 分析完了")
    print("=" * 80)
    print("\n次のデバッグステップ:")
    print("  1. パース処理を完全に再実行（未パース分を処理）")
    print("  2. 馬過去成績データの生成確認")
    print("  3. 欠損データの原因特定")
    print("\n" + "=" * 80)
    print("\n")

if __name__ == "__main__":
    main()
