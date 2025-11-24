#!/usr/bin/env python3
"""
データ品質検証スクリプト - predictions_2024_full.parquet の詳細調査

目的:
    predict.pyで生成されたpredictions_2024_full.parquetのデータ品質を
    多角的に検証し、馬番0問題以外の潜在的な問題を洗い出す。

検証項目:
    1. 馬番0の件数と割合
    2. 全カラムの欠損率・データ型
    3. μ/σ/νの統計量と異常値
    4. race_idごとの馬数分布（重複・欠損）
    5. 出馬表由来カラムの品質
    6. 重複レコードの詳細分析
"""

import sys
from pathlib import Path
import pandas as pd
import numpy as np
from collections import Counter

# プロジェクトルートをパスに追加
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

def print_section(title: str):
    """セクションヘッダーを出力"""
    print("\n" + "="*80)
    print(f"  {title}")
    print("="*80)

def analyze_horse_number_zero(df: pd.DataFrame):
    """馬番0の詳細分析"""
    print_section("1. 馬番0の詳細分析")

    total_rows = len(df)
    zero_horse_number = (df['horse_number'] == 0).sum()
    zero_ratio = zero_horse_number / total_rows * 100 if total_rows > 0 else 0

    print(f"総レコード数: {total_rows:,}")
    print(f"馬番0のレコード数: {zero_horse_number:,} ({zero_ratio:.2f}%)")
    print(f"正常な馬番のレコード数: {total_rows - zero_horse_number:,} ({100-zero_ratio:.2f}%)")

    # 馬番の分布
    print("\n馬番の分布（上位20）:")
    horse_num_counts = df['horse_number'].value_counts().head(20)
    for num, count in horse_num_counts.items():
        print(f"  馬番{num}: {count:,}件")

    # 馬番0のrace_id分布
    if zero_horse_number > 0:
        zero_races = df[df['horse_number'] == 0]['race_id'].nunique()
        total_races = df['race_id'].nunique()
        print(f"\n馬番0が含まれるレース数: {zero_races}/{total_races} ({zero_races/total_races*100:.1f}%)")

def analyze_column_quality(df: pd.DataFrame):
    """全カラムの品質分析"""
    print_section("2. 全カラムの品質分析")

    total_rows = len(df)
    quality_report = []

    for col in df.columns:
        null_count = df[col].isnull().sum()
        null_ratio = null_count / total_rows * 100 if total_rows > 0 else 0
        dtype = str(df[col].dtype)
        unique_count = df[col].nunique()

        quality_report.append({
            'カラム名': col,
            'データ型': dtype,
            '欠損数': null_count,
            '欠損率(%)': f"{null_ratio:.2f}",
            'ユニーク数': unique_count
        })

    quality_df = pd.DataFrame(quality_report)
    print(quality_df.to_string(index=False))

    # 欠損率が高いカラムを警告
    high_missing = quality_df[quality_df['欠損率(%)'].astype(float) > 10.0]
    if not high_missing.empty:
        print("\n⚠️ 欠損率10%超のカラム:")
        print(high_missing[['カラム名', '欠損率(%)']].to_string(index=False))

def analyze_prediction_values(df: pd.DataFrame):
    """μ/σ/νの統計量分析"""
    print_section("3. 予測値（μ/σ/ν）の統計量分析")

    prediction_cols = ['mu', 'sigma', 'nu']

    for col in prediction_cols:
        if col not in df.columns:
            print(f"⚠️ カラム '{col}' が存在しません")
            continue

        print(f"\n--- {col.upper()} の統計量 ---")
        stats = df[col].describe()
        print(stats)

        # 異常値チェック
        q1 = df[col].quantile(0.25)
        q3 = df[col].quantile(0.75)
        iqr = q3 - q1
        lower_bound = q1 - 3 * iqr
        upper_bound = q3 + 3 * iqr

        outliers = df[(df[col] < lower_bound) | (df[col] > upper_bound)]
        outlier_count = len(outliers)
        outlier_ratio = outlier_count / len(df) * 100 if len(df) > 0 else 0

        print(f"\n異常値（IQR ±3倍）: {outlier_count:,}件 ({outlier_ratio:.2f}%)")
        if outlier_count > 0:
            print(f"  最小異常値: {outliers[col].min():.4f}")
            print(f"  最大異常値: {outliers[col].max():.4f}")

        # 馬番0の予測値分析
        if 'horse_number' in df.columns:
            zero_horse_vals = df[df['horse_number'] == 0][col]
            if len(zero_horse_vals) > 0:
                print(f"\n馬番0の{col}の統計:")
                print(f"  平均: {zero_horse_vals.mean():.4f}")
                print(f"  中央値: {zero_horse_vals.median():.4f}")
                print(f"  標準偏差: {zero_horse_vals.std():.4f}")

def analyze_race_distribution(df: pd.DataFrame):
    """レースごとの馬数分布分析"""
    print_section("4. レースごとの馬数分布分析")

    race_horse_counts = df.groupby('race_id').size()

    print(f"総レース数: {len(race_horse_counts):,}")
    print(f"\n馬数の統計:")
    print(race_horse_counts.describe())

    # 馬数分布のヒストグラム（テキスト版）
    print("\n馬数の分布:")
    horse_count_dist = race_horse_counts.value_counts().sort_index()
    for num_horses, num_races in horse_count_dist.items():
        bar = "█" * min(50, int(num_races / horse_count_dist.max() * 50))
        print(f"  {num_horses:2d}頭: {num_races:4d}レース {bar}")

    # 異常なレース（馬数が極端に多い/少ない）
    print("\n⚠️ 異常な馬数のレース:")
    abnormal_races = race_horse_counts[(race_horse_counts < 5) | (race_horse_counts > 20)]
    if len(abnormal_races) > 0:
        for race_id, count in abnormal_races.items():
            print(f"  レース {race_id}: {count}頭")
    else:
        print("  なし（全レース正常範囲）")

    # 馬番0を含むレースの馬数分布
    if 'horse_number' in df.columns:
        zero_races = df[df['horse_number'] == 0]['race_id'].unique()
        zero_race_counts = race_horse_counts[race_horse_counts.index.isin(zero_races)]

        print(f"\n馬番0を含むレースの馬数分布:")
        print(zero_race_counts.describe())

def analyze_shutuba_columns(df: pd.DataFrame):
    """出馬表由来カラムの品質分析"""
    print_section("5. 出馬表由来カラムの品質分析")

    # 出馬表由来と思われるカラム（CLAUDE.mdのshutuba.parquetスキーマより）
    shutuba_cols = [
        'horse_number', 'bracket_number', 'jockey_id', 'jockey_name',
        'trainer_id', 'trainer_name', 'horse_weight', 'horse_weight_change',
        'morning_odds', 'popularity', 'blinker'
    ]

    present_cols = [col for col in shutuba_cols if col in df.columns]
    missing_cols = [col for col in shutuba_cols if col not in df.columns]

    print(f"存在するカラム: {len(present_cols)}/{len(shutuba_cols)}")
    if missing_cols:
        print(f"⚠️ 欠落しているカラム: {', '.join(missing_cols)}")

    print("\n出馬表カラムの欠損状況:")
    for col in present_cols:
        null_count = df[col].isnull().sum()
        null_ratio = null_count / len(df) * 100 if len(df) > 0 else 0

        # 追加：0値の件数（馬番以外）
        if col != 'horse_number' and pd.api.types.is_numeric_dtype(df[col]):
            zero_count = (df[col] == 0).sum()
            zero_ratio = zero_count / len(df) * 100 if len(df) > 0 else 0
            print(f"  {col:25s}: 欠損 {null_count:6,}件 ({null_ratio:5.2f}%), 0値 {zero_count:6,}件 ({zero_ratio:5.2f}%)")
        else:
            print(f"  {col:25s}: 欠損 {null_count:6,}件 ({null_ratio:5.2f}%)")

def analyze_duplicates(df: pd.DataFrame):
    """重複レコードの詳細分析"""
    print_section("6. 重複レコードの詳細分析")

    total_rows = len(df)

    # 完全重複
    duplicated_all = df.duplicated(keep=False)
    dup_count_all = duplicated_all.sum()
    dup_ratio_all = dup_count_all / total_rows * 100 if total_rows > 0 else 0

    print(f"完全重複レコード数: {dup_count_all:,} ({dup_ratio_all:.2f}%)")

    # race_id + horse_id での重複
    if 'race_id' in df.columns and 'horse_id' in df.columns:
        key_cols = ['race_id', 'horse_id']
        duplicated_key = df.duplicated(subset=key_cols, keep=False)
        dup_count_key = duplicated_key.sum()
        dup_ratio_key = dup_count_key / total_rows * 100 if total_rows > 0 else 0

        print(f"race_id + horse_id 重複: {dup_count_key:,} ({dup_ratio_key:.2f}%)")

        # 重複が多いrace_idを表示
        if dup_count_key > 0:
            dup_race_counts = df[duplicated_key].groupby('race_id').size().sort_values(ascending=False)
            print(f"\n重複が多いレース（上位10）:")
            for race_id, count in dup_race_counts.head(10).items():
                unique_horses = df[df['race_id'] == race_id]['horse_id'].nunique()
                print(f"  レース {race_id}: {count}行（ユニーク馬{unique_horses}頭）")

    # race_id + horse_number での重複
    if 'race_id' in df.columns and 'horse_number' in df.columns:
        key_cols = ['race_id', 'horse_number']
        duplicated_key2 = df.duplicated(subset=key_cols, keep=False)
        dup_count_key2 = duplicated_key2.sum()
        dup_ratio_key2 = dup_count_key2 / total_rows * 100 if total_rows > 0 else 0

        print(f"\nrace_id + horse_number 重複: {dup_count_key2:,} ({dup_ratio_key2:.2f}%)")

def sample_problematic_data(df: pd.DataFrame):
    """問題のあるデータのサンプル表示"""
    print_section("7. 問題データのサンプル表示")

    # 馬番0のサンプル
    if 'horse_number' in df.columns:
        zero_sample = df[df['horse_number'] == 0].head(5)
        if not zero_sample.empty:
            print("馬番0のサンプル（先頭5件）:")
            print(zero_sample[['race_id', 'horse_id', 'horse_number', 'mu', 'sigma', 'nu']].to_string(index=False))
        else:
            print("馬番0のレコードなし")

    # 重複のサンプル
    if 'race_id' in df.columns and 'horse_id' in df.columns:
        duplicated = df[df.duplicated(subset=['race_id', 'horse_id'], keep=False)]
        if not duplicated.empty:
            # 1つのレースを例として表示
            sample_race = duplicated['race_id'].iloc[0]
            dup_sample = duplicated[duplicated['race_id'] == sample_race].head(10)

            print(f"\n\n重複レコードのサンプル（レース {sample_race}）:")
            print(dup_sample[['race_id', 'horse_id', 'horse_number', 'mu', 'sigma']].to_string(index=False))

def main():
    """メイン処理"""
    predictions_path = project_root / "keibaai" / "data" / "predictions" / "predictions_2024_full.parquet"

    print("="*80)
    print("  KeibaAI データ品質検証レポート")
    print("="*80)
    print(f"対象ファイル: {predictions_path}")

    if not predictions_path.exists():
        print(f"\n❌ エラー: ファイルが存在しません - {predictions_path}")
        return 1

    # データ読み込み
    print("\nデータ読み込み中...")
    df = pd.read_parquet(predictions_path)
    print(f"読み込み完了: {len(df):,}行 × {len(df.columns)}列")

    # 各分析を実行
    analyze_horse_number_zero(df)
    analyze_column_quality(df)
    analyze_prediction_values(df)
    analyze_race_distribution(df)
    analyze_shutuba_columns(df)
    analyze_duplicates(df)
    sample_problematic_data(df)

    print_section("検証完了")
    print("✅ データ品質検証が完了しました。")
    print("\n次のステップ:")
    print("  1. predict.pyの結合ロジックを修正")
    print("  2. 修正後に再実行してこのスクリプトで再検証")
    print("  3. 問題が解消されたらシミュレーションを実行")

    return 0

if __name__ == "__main__":
    exit(main())
