"""
欠損値処理の追跡デバッグスクリプト

目的:
- feature_engine.py でどこで fillna が使われているか確認
- features.yaml の imputation 設定を確認
- 元データ（shutuba.parquet, races.parquet）に欠損値があるか確認
- 特徴量生成過程のどの段階で欠損値が処理されているかを特定

使用方法:
    python debug_nan_handling.py
"""

import pandas as pd
from pathlib import Path
import re
import sys

def check_fillna_in_feature_engine():
    """feature_engine.py で fillna が使われている箇所を検索"""

    print("=" * 70)
    print("🔍 1. feature_engine.py での fillna 使用箇所")
    print("=" * 70)
    print()

    feature_engine_path = Path("keibaai/src/features/feature_engine.py")

    if not feature_engine_path.exists():
        print(f"❌ {feature_engine_path} が見つかりません")
        return

    with open(feature_engine_path, 'r', encoding='utf-8') as f:
        lines = f.readlines()

    fillna_lines = []
    for i, line in enumerate(lines, 1):
        if 'fillna' in line.lower():
            fillna_lines.append((i, line.rstrip()))

    if fillna_lines:
        print(f"✅ fillna が使われている箇所: {len(fillna_lines)}件")
        print()
        for line_num, line_content in fillna_lines:
            print(f"  行{line_num}: {line_content}")
        print()
    else:
        print("✅ fillna は使われていません")
        print()

    # imputation 関連のキーワードも検索
    imputation_keywords = ['imputation', '欠損', 'NaN', 'missing']
    imputation_lines = []

    for i, line in enumerate(lines, 1):
        for keyword in imputation_keywords:
            if keyword in line:
                imputation_lines.append((i, line.rstrip()))
                break

    if imputation_lines:
        print(f"📋 欠損値処理関連のコード: {len(imputation_lines)}件")
        print()
        for line_num, line_content in imputation_lines:
            print(f"  行{line_num}: {line_content}")
        print()

def check_features_yaml_imputation():
    """features.yaml の imputation 設定を確認"""

    print("=" * 70)
    print("🔍 2. features.yaml の欠損値処理設定")
    print("=" * 70)
    print()

    yaml_path = Path("keibaai/configs/features.yaml")

    if not yaml_path.exists():
        print(f"❌ {yaml_path} が見つかりません")
        return

    with open(yaml_path, 'r', encoding='utf-8') as f:
        lines = f.readlines()

    # imputation セクションを探す
    in_imputation_section = False
    imputation_lines = []

    for i, line in enumerate(lines, 1):
        if 'imputation:' in line:
            in_imputation_section = True
            imputation_lines.append((i, line.rstrip()))
        elif in_imputation_section:
            # インデントがある行は imputation セクションの一部
            if line.startswith('  ') or line.strip() == '':
                imputation_lines.append((i, line.rstrip()))
            else:
                # インデントがなくなったらセクション終了
                break

    if imputation_lines:
        print("📋 imputation 設定:")
        print()
        for line_num, line_content in imputation_lines:
            print(f"  行{line_num}: {line_content}")
        print()
    else:
        print("⚠️  imputation 設定が見つかりません")
        print()

def check_source_data_nan():
    """元データ（shutuba.parquet, races.parquet）の欠損値を確認"""

    print("=" * 70)
    print("🔍 3. 元データ（パース済みデータ）の欠損値確認")
    print("=" * 70)
    print()

    # shutuba.parquet の確認
    shutuba_path = Path("keibaai/data/parsed/parquet/shutuba/shutuba.parquet")

    if shutuba_path.exists():
        print("📂 shutuba.parquet の欠損値分析")
        print("-" * 70)

        try:
            df_shutuba = pd.read_parquet(shutuba_path)
            print(f"総行数: {len(df_shutuba):,}行")
            print(f"総カラム数: {len(df_shutuba.columns)}カラム")
            print()

            # 欠損値のあるカラムを集計
            nan_counts = df_shutuba.isna().sum()
            nan_cols = nan_counts[nan_counts > 0].sort_values(ascending=False)

            if len(nan_cols) > 0:
                print(f"⚠️  NaNを持つカラム: {len(nan_cols)}個")
                print()
                for col, count in nan_cols.head(20).items():
                    nan_rate = (count / len(df_shutuba)) * 100
                    print(f"  {col}: {count:,}件 ({nan_rate:.2f}%)")
                print()
            else:
                print("✅ shutuba.parquet には NaN値がありません")
                print()

        except Exception as e:
            print(f"❌ エラー: {e}")
            print()
    else:
        print(f"❌ {shutuba_path} が見つかりません")
        print()

    # races.parquet の確認
    races_path = Path("keibaai/data/parsed/parquet/races/races.parquet")

    if races_path.exists():
        print("📂 races.parquet の欠損値分析")
        print("-" * 70)

        try:
            df_races = pd.read_parquet(races_path)
            print(f"総行数: {len(df_races):,}行")
            print(f"総カラム数: {len(df_races.columns)}カラム")
            print()

            # 欠損値のあるカラムを集計
            nan_counts = df_races.isna().sum()
            nan_cols = nan_counts[nan_counts > 0].sort_values(ascending=False)

            if len(nan_cols) > 0:
                print(f"⚠️  NaNを持つカラム: {len(nan_cols)}個")
                print()
                for col, count in nan_cols.head(20).items():
                    nan_rate = (count / len(df_races)) * 100
                    print(f"  {col}: {count:,}件 ({nan_rate:.2f}%)")
                print()
            else:
                print("✅ races.parquet には NaN値がありません")
                print()

        except Exception as e:
            print(f"❌ エラー: {e}")
            print()
    else:
        print(f"❌ {races_path} が見つかりません")
        print()

def check_generated_features_nan_sample():
    """生成された特徴量データの一部を読み込んでNaN確認（詳細版）"""

    print("=" * 70)
    print("🔍 4. 生成された特徴量データの詳細確認")
    print("=" * 70)
    print()

    features_base = Path("keibaai/data/features/parquet")

    # 2023年1月のデータを1ファイルだけ読み込む
    sample_path = features_base / "year=2023" / "month=1"

    if not sample_path.exists():
        print(f"❌ {sample_path} が見つかりません")
        return

    parquet_files = list(sample_path.glob("*.parquet"))

    if not parquet_files:
        print("❌ Parquetファイルが見つかりません")
        return

    print(f"📂 サンプルファイル: {parquet_files[0].name}")
    print()

    try:
        df = pd.read_parquet(parquet_files[0])

        print(f"総行数: {len(df):,}行")
        print(f"総カラム数: {len(df.columns)}カラム")
        print()

        # 全カラムの型を確認
        print("📊 データ型別のカラム数:")
        dtype_counts = df.dtypes.value_counts()
        for dtype, count in dtype_counts.items():
            print(f"  {dtype}: {count}個")
        print()

        # NaN確認（念のため）
        nan_counts = df.isna().sum()
        nan_cols = nan_counts[nan_counts > 0]

        if len(nan_cols) > 0:
            print(f"⚠️  このファイルには {len(nan_cols)}個のカラムにNaNが存在します:")
            print()
            for col, count in nan_cols.items():
                nan_rate = (count / len(df)) * 100
                print(f"  {col}: {count}件 ({nan_rate:.2f}%)")
            print()
        else:
            print("✅ このファイルには NaN値がありません")
            print()

        # サンプルデータを表示（最初の3行）
        print("📋 サンプルデータ（最初の3行、最初の10カラム）:")
        print()
        sample_cols = df.columns[:10].tolist()
        print(df[sample_cols].head(3).to_string())
        print()

    except Exception as e:
        print(f"❌ エラー: {e}")
        import traceback
        traceback.print_exc()
        print()

def main():
    print("=" * 70)
    print("🔍 欠損値処理の追跡デバッグスクリプト")
    print("=" * 70)
    print()

    # 1. feature_engine.py の fillna 使用箇所
    check_fillna_in_feature_engine()

    # 2. features.yaml の imputation 設定
    check_features_yaml_imputation()

    # 3. 元データの欠損値確認
    check_source_data_nan()

    # 4. 生成された特徴量データの詳細確認
    check_generated_features_nan_sample()

    # まとめ
    print("=" * 70)
    print("📋 まとめ")
    print("=" * 70)
    print()
    print("上記の調査結果から、以下を確認してください:")
    print()
    print("1. feature_engine.py で fillna が使われている場合:")
    print("   → 特徴量生成時に欠損値処理が行われている")
    print("   → predict_bulk.py の fillna(0) は二重処理の可能性")
    print()
    print("2. 元データ（shutuba.parquet, races.parquet）に欠損値がある場合:")
    print("   → パース段階では欠損値が残っている")
    print("   → 特徴量生成時に処理されている")
    print()
    print("3. 特徴量データに NaN がない場合:")
    print("   → 特徴量生成時に完全に処理済み")
    print("   → predict_bulk.py の fillna(0) は不要な可能性")
    print()
    print("=" * 70)

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"❌ エラーが発生しました: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
