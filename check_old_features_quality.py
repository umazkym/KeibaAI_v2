"""
2020-2023年の特徴量データの重複チェックスクリプト

目的:
- 修正前に生成された2020-2023年の特徴量データに重複がないか確認
- モデル学習前のデータ品質検証

使用方法:
    python check_old_features_quality.py

出力:
- 年別の重複統計
- 全体サマリー
- 再生成が必要かどうかの推奨
"""

import pandas as pd
import pyarrow.parquet as pq
from pathlib import Path
import sys

def check_feature_quality():
    """2020-2023年の特徴量データの品質をチェック"""

    print("=" * 70)
    print("📊 2020-2023年 特徴量データ重複チェック")
    print("=" * 70)
    print()

    base_path = Path("keibaai/data/features/parquet")

    if not base_path.exists():
        print(f"❌ エラー: {base_path} が存在しません")
        sys.exit(1)

    years = [2020, 2021, 2022, 2023]
    total_summary = []

    for year in years:
        year_path = base_path / f"year={year}"
        if not year_path.exists():
            print(f"❌ {year}年のデータが存在しません")
            continue

        # 年ごとの全データを読み込み（月ごとに読んで結合）
        print(f"📅 {year}年のデータを読み込み中...")
        try:
            year_dfs = []
            for month in range(1, 13):
                month_path = year_path / f"month={month}"
                if month_path.exists():
                    # 月ごとのparquetファイルを検索
                    parquet_files = list(month_path.glob("*.parquet"))
                    if parquet_files:
                        # 各parquetファイルを読み込み
                        for pq_file in parquet_files:
                            month_df = pd.read_parquet(pq_file)
                            year_dfs.append(month_df)

            if not year_dfs:
                print(f"❌ {year}年のParquetファイルが見つかりません")
                continue

            # 全月のデータを結合
            df = pd.concat(year_dfs, ignore_index=True)
        except Exception as e:
            print(f"❌ エラー: {year}年のデータ読み込みに失敗: {e}")
            import traceback
            traceback.print_exc()
            continue

        total_rows = len(df)

        # race_id, horse_id の組み合わせで重複チェック
        duplicates = df.duplicated(subset=['race_id', 'horse_id']).sum()
        unique_rows = len(df.drop_duplicates(subset=['race_id', 'horse_id']))
        dup_rate = (duplicates / total_rows * 100) if total_rows > 0 else 0

        # horse_number の異常チェック
        horse_num_zero = (df['horse_number'] == 0).sum()
        horse_num_zero_rate = (horse_num_zero / total_rows * 100) if total_rows > 0 else 0

        print(f"  総行数: {total_rows:,}行")
        print(f"  重複行: {duplicates:,}行 ({dup_rate:.2f}%)")
        print(f"  ユニーク行: {unique_rows:,}行")
        print(f"  horse_number=0: {horse_num_zero:,}行 ({horse_num_zero_rate:.2f}%)")

        # 警告表示
        if dup_rate > 5:
            print(f"  ⚠️  警告: 重複率が高い（{dup_rate:.2f}%）")
        if horse_num_zero_rate > 1:
            print(f"  ⚠️  警告: horse_number=0 が多い（{horse_num_zero_rate:.2f}%）")

        print()

        total_summary.append({
            'year': year,
            'total_rows': total_rows,
            'duplicates': duplicates,
            'unique_rows': unique_rows,
            'dup_rate': dup_rate,
            'horse_num_zero': horse_num_zero,
            'horse_num_zero_rate': horse_num_zero_rate
        })

    if not total_summary:
        print("❌ 有効なデータが見つかりませんでした")
        sys.exit(1)

    # サマリー表示
    print("=" * 70)
    print("📊 年別サマリー")
    print("=" * 70)
    summary_df = pd.DataFrame(total_summary)
    print(summary_df.to_string(index=False))
    print()

    # 総計
    total_all = summary_df['total_rows'].sum()
    dup_all = summary_df['duplicates'].sum()
    unique_all = summary_df['unique_rows'].sum()
    dup_rate_all = (dup_all / total_all * 100) if total_all > 0 else 0
    horse_num_zero_all = summary_df['horse_num_zero'].sum()
    horse_num_zero_rate_all = (horse_num_zero_all / total_all * 100) if total_all > 0 else 0

    print("=" * 70)
    print("✅ 全体総計（2020-2023年）")
    print("=" * 70)
    print(f"  総行数: {total_all:,}行")
    print(f"  重複行: {dup_all:,}行 ({dup_rate_all:.2f}%)")
    print(f"  ユニーク行: {unique_all:,}行")
    print(f"  horse_number=0: {horse_num_zero_all:,}行 ({horse_num_zero_rate_all:.2f}%)")
    print()

    # 推奨アクション
    print("=" * 70)
    print("📋 推奨アクション")
    print("=" * 70)

    needs_regeneration = False

    if dup_rate_all > 5:
        print("⚠️  重複率が5%を超えています → 再生成を強く推奨")
        needs_regeneration = True
    elif dup_rate_all > 0:
        print("⚠️  軽微な重複が検出されました → 再生成を推奨")
        needs_regeneration = True

    if horse_num_zero_rate_all > 1:
        print("⚠️  horse_number=0 が1%を超えています → 再生成を強く推奨")
        needs_regeneration = True

    if needs_regeneration:
        print()
        print("🔧 再生成コマンド:")
        print("python keibaai/src/features/generate_features.py \\")
        print("  --start_date 2020-01-01 \\")
        print("  --end_date 2023-12-31")
    else:
        print("✅ データ品質良好。そのままモデル学習に使用可能です。")

    print()
    print("=" * 70)

    return needs_regeneration

if __name__ == "__main__":
    try:
        needs_regeneration = check_feature_quality()
        sys.exit(1 if needs_regeneration else 0)
    except Exception as e:
        print(f"❌ エラーが発生しました: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
