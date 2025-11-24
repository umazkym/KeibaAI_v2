"""
特徴量データのNaN値を調査するスクリプト

目的:
- どのカラムにNaN値が存在するかを特定
- NaN値の発生率を計算
- NaN値が発生するパターンを分析（新馬、過去走なし等）
- 適切な欠損値処理方法を決定するための情報を収集

使用方法:
    python investigate_nan_values.py
"""

import pandas as pd
import pyarrow.parquet as pq
from pathlib import Path
import sys
import numpy as np

def load_features_sample(year: int, month: int = 1, limit_rows: int = None):
    """特徴量データの一部を読み込む"""
    features_base_path = Path("keibaai/data/features/parquet")

    year_path = features_base_path / f"year={year}"
    month_path = year_path / f"month={month}"

    if not month_path.exists():
        return None

    parquet_files = list(month_path.glob("*.parquet"))
    if not parquet_files:
        return None

    # 最初のファイルのみ読み込む（サンプル調査用）
    df = pd.read_parquet(parquet_files[0])

    if limit_rows:
        df = df.head(limit_rows)

    return df

def load_all_features(start_year: int = 2020, end_year: int = 2023):
    """全特徴量データを読み込む（NaN統計用）"""
    features_base_path = Path("keibaai/data/features/parquet")

    all_dfs = []

    for year in range(start_year, end_year + 1):
        year_path = features_base_path / f"year={year}"
        if not year_path.exists():
            continue

        print(f"📅 {year}年のデータを読み込み中...")

        for month in range(1, 13):
            month_path = year_path / f"month={month}"
            if not month_path.exists():
                continue

            parquet_files = list(month_path.glob("*.parquet"))
            for pq_file in parquet_files:
                df_part = pd.read_parquet(pq_file)
                all_dfs.append(df_part)

    if not all_dfs:
        return None

    df = pd.concat(all_dfs, ignore_index=True)
    print(f"✅ 読み込み完了: {len(df):,}行")

    return df

def analyze_nan_patterns(df: pd.DataFrame):
    """NaN値のパターンを詳細に分析"""

    print("=" * 70)
    print("📊 NaN値の統計分析")
    print("=" * 70)
    print()

    # 1. 全カラムのNaN率を計算
    nan_stats = []

    for col in df.columns:
        nan_count = df[col].isna().sum()
        nan_rate = (nan_count / len(df)) * 100

        if nan_count > 0:
            nan_stats.append({
                'column': col,
                'nan_count': nan_count,
                'nan_rate': nan_rate,
                'dtype': str(df[col].dtype)
            })

    if not nan_stats:
        print("✅ NaN値は検出されませんでした")
        return

    # NaN率の高い順にソート
    nan_df = pd.DataFrame(nan_stats).sort_values('nan_rate', ascending=False)

    print(f"🔍 NaN値を持つカラム: {len(nan_df)}個")
    print()

    # 2. NaN率の高いカラム上位20件を表示
    print("=" * 70)
    print("📋 NaN率が高いカラム（上位20件）")
    print("=" * 70)
    print(nan_df.head(20).to_string(index=False))
    print()

    # 3. カテゴリ別にNaN率を集計
    print("=" * 70)
    print("📊 カテゴリ別NaN統計")
    print("=" * 70)

    # カラム名からカテゴリを推定
    categories = {
        '過去走集計': [col for col in nan_df['column'] if 'past_' in col or 'avg_' in col or 'median_' in col],
        '騎手統計': [col for col in nan_df['column'] if 'jockey_' in col],
        '調教師統計': [col for col in nan_df['column'] if 'trainer_' in col],
        '血統統計': [col for col in nan_df['column'] if 'sire_' in col or 'dam_' in col],
        'レース内相対': [col for col in nan_df['column'] if '_z_score' in col or '_rank' in col],
        'その他': []
    }

    # カテゴリに分類されなかったカラムを「その他」に追加
    categorized_cols = set()
    for cat_cols in categories.values():
        categorized_cols.update(cat_cols)

    categories['その他'] = [col for col in nan_df['column'] if col not in categorized_cols]

    for category, cols in categories.items():
        if cols:
            avg_nan_rate = nan_df[nan_df['column'].isin(cols)]['nan_rate'].mean()
            print(f"{category}: {len(cols)}カラム (平均NaN率: {avg_nan_rate:.2f}%)")

    print()

    # 4. NaN値を持つ行のパターン分析
    print("=" * 70)
    print("🔍 NaN値を持つ行のパターン分析")
    print("=" * 70)

    # 全カラムでNaN数をカウント
    nan_per_row = df.isna().sum(axis=1)

    print(f"NaN値が0個: {(nan_per_row == 0).sum():,}行 ({(nan_per_row == 0).sum() / len(df) * 100:.2f}%)")
    print(f"NaN値が1-10個: {((nan_per_row > 0) & (nan_per_row <= 10)).sum():,}行")
    print(f"NaN値が11-50個: {((nan_per_row > 10) & (nan_per_row <= 50)).sum():,}行")
    print(f"NaN値が51-100個: {((nan_per_row > 50) & (nan_per_row <= 100)).sum():,}行")
    print(f"NaN値が100個超: {(nan_per_row > 100).sum():,}行")
    print()

    print(f"1行あたり平均NaN数: {nan_per_row.mean():.2f}個")
    print(f"1行あたり最大NaN数: {nan_per_row.max()}個")
    print()

    # 5. NaN値が多い行のサンプルを表示（新馬かどうか確認）
    print("=" * 70)
    print("📋 NaN値が多い行のサンプル（上位5件）")
    print("=" * 70)

    # NaN数が多い行を抽出
    top_nan_indices = nan_per_row.nlargest(5).index

    # 識別情報とNaN数を表示
    display_cols = ['race_id', 'horse_id']
    if 'horse_number' in df.columns:
        display_cols.append('horse_number')
    if 'career_starts' in df.columns:
        display_cols.append('career_starts')
    if 'age' in df.columns:
        display_cols.append('age')

    sample_df = df.loc[top_nan_indices, display_cols].copy()
    sample_df['nan_count'] = nan_per_row.loc[top_nan_indices]

    print(sample_df.to_string(index=False))
    print()

    # 6. NaN値がない行のサンプル（比較用）
    print("=" * 70)
    print("📋 NaN値がない行のサンプル（5件）")
    print("=" * 70)

    zero_nan_indices = nan_per_row[nan_per_row == 0].index[:5]

    sample_df_zero = df.loc[zero_nan_indices, display_cols].copy()
    sample_df_zero['nan_count'] = 0

    print(sample_df_zero.to_string(index=False))
    print()

    # 7. 新馬とNaN数の関係を分析
    if 'career_starts' in df.columns:
        print("=" * 70)
        print("📊 career_starts（過去走数）とNaN数の関係")
        print("=" * 70)

        career_nan_analysis = pd.DataFrame({
            'career_starts': df['career_starts'],
            'nan_count': nan_per_row
        }).dropna()

        # career_starts別の平均NaN数
        career_grouped = career_nan_analysis.groupby('career_starts')['nan_count'].agg(['mean', 'count'])

        print("career_starts別の平均NaN数（上位20件）:")
        print(career_grouped.head(20).to_string())
        print()

        # 新馬（career_starts=0）の統計
        if 0 in career_grouped.index:
            debut_nan_mean = career_grouped.loc[0, 'mean']
            debut_count = career_grouped.loc[0, 'count']
            print(f"⚠️  新馬（career_starts=0）: 平均{debut_nan_mean:.2f}個のNaN ({debut_count}頭)")

        print()

    return nan_df

def investigate_specific_nan_column(df: pd.DataFrame, column: str):
    """特定カラムのNaN値を詳細調査"""

    print("=" * 70)
    print(f"🔍 カラム '{column}' の詳細調査")
    print("=" * 70)
    print()

    if column not in df.columns:
        print(f"❌ カラム '{column}' は存在しません")
        return

    nan_mask = df[column].isna()
    not_nan_mask = ~nan_mask

    nan_count = nan_mask.sum()
    not_nan_count = not_nan_mask.sum()
    nan_rate = (nan_count / len(df)) * 100

    print(f"NaN: {nan_count:,}行 ({nan_rate:.2f}%)")
    print(f"非NaN: {not_nan_count:,}行 ({100 - nan_rate:.2f}%)")
    print()

    # career_startsとの関係
    if 'career_starts' in df.columns:
        print("career_starts別のNaN率:")

        career_nan = pd.DataFrame({
            'career_starts': df['career_starts'],
            'is_nan': nan_mask
        }).dropna(subset=['career_starts'])

        career_grouped = career_nan.groupby('career_starts')['is_nan'].agg(['mean', 'count'])
        career_grouped['mean'] = career_grouped['mean'] * 100  # パーセント表記
        career_grouped.columns = ['nan_rate_%', 'count']

        print(career_grouped.head(10).to_string())
        print()

def main():
    print("=" * 70)
    print("🔍 特徴量データ NaN値調査スクリプト")
    print("=" * 70)
    print()

    # 全データを読み込んでNaN分析
    df = load_all_features(start_year=2020, end_year=2023)

    if df is None:
        print("❌ データが見つかりません")
        sys.exit(1)

    print()

    # NaN値の全体分析
    nan_df = analyze_nan_patterns(df)

    # 代表的なNaN率が高いカラムの詳細調査
    if nan_df is not None and len(nan_df) > 0:
        print()
        print("=" * 70)
        print("🔍 NaN率が最も高いカラムの詳細調査")
        print("=" * 70)
        print()

        top_nan_col = nan_df.iloc[0]['column']
        investigate_specific_nan_column(df, top_nan_col)

    # 推奨対応
    print()
    print("=" * 70)
    print("📋 推奨される欠損値処理方法")
    print("=" * 70)
    print()
    print("調査結果に基づいて、以下の処理が考えられます:")
    print()
    print("1. 【過去走集計系】:")
    print("   - 新馬（career_starts=0）の場合、過去走がないためNaNは正常")
    print("   - 処理: 0で埋める、または専用の特徴量フラグ（is_debut）を追加")
    print()
    print("2. 【騎手・調教師統計】:")
    print("   - データ不足の場合、全体平均で補完する方法もある")
    print("   - 処理: 0で埋める、または全体平均で補完")
    print()
    print("3. 【血統統計】:")
    print("   - 血統データがない場合、0で埋める")
    print("   - 処理: 0で埋める")
    print()
    print("4. 【レース内相対特徴量】:")
    print("   - 計算できない場合は0（平均）で埋める")
    print("   - 処理: 0で埋める（正規化済みなので0=平均）")
    print()
    print("⚠️  現在の fillna(0) は、上記の理由により妥当な処理と考えられます。")
    print("   ただし、is_debut フラグの追加など、さらなる改善の余地があります。")
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
