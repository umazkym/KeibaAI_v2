#!/usr/bin/env python3
"""
デバッグスクリプト05: 不足している馬データの差分パース

races.parquet に登場する6,601頭のうち、
horses.parquet に存在しない5,601頭分を追加パースします。

処理内容:
1. 差分馬IDリストを作成（5,601頭）
2. 該当するHTMLファイルを検索
3. 馬プロフィール・血統・過去成績をパース
4. 既存データと結合して保存
"""

import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime
import sys
import logging

# プロジェクトルートをsys.pathに追加
project_root = Path(__file__).resolve().parent
sys.path.append(str(project_root / "keibaai"))

from keibaai.src.parsers import horse_info_parser, pedigree_parser
from keibaai.src import pipeline_core
import sqlite3

# ロギング設定
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def get_missing_horse_ids():
    """racesに登場するがhorsesにない馬IDを抽出"""
    print("=" * 80)
    print("🔍 不足している馬IDの特定")
    print("=" * 80)

    # races.parquet から全馬ID
    races_path = Path("keibaai/data/parsed/parquet/races/races.parquet")
    df_races = pd.read_parquet(races_path)
    races_horse_ids = set(df_races['horse_id'].unique())
    print(f"\nraces.parquet の馬ID数: {len(races_horse_ids):,}")

    # horses.parquet から既存馬ID
    horses_path = Path("keibaai/data/parsed/parquet/horses/horses.parquet")
    if horses_path.exists():
        df_horses = pd.read_parquet(horses_path)
        existing_horse_ids = set(df_horses['horse_id'].unique())
        print(f"horses.parquet の馬ID数: {len(existing_horse_ids):,}")
    else:
        existing_horse_ids = set()
        print("horses.parquet が存在しません（全頭が対象）")

    # 差分
    missing_horse_ids = races_horse_ids - existing_horse_ids
    print(f"\n📊 不足している馬ID数: {len(missing_horse_ids):,}")

    if missing_horse_ids:
        print(f"\nサンプル（最初の10頭）:")
        for horse_id in sorted(missing_horse_ids)[:10]:
            print(f"  - {horse_id}")

    return sorted(missing_horse_ids), existing_horse_ids


def find_html_files(missing_horse_ids):
    """不足している馬のHTMLファイルを検索"""
    print("\n" + "=" * 80)
    print("📁 HTMLファイルの検索")
    print("=" * 80)

    html_dir = Path("keibaai/data/raw/html/horse")
    ped_dir = Path("keibaai/data/raw/html/ped")

    profile_files = []
    perf_files = []
    ped_files = []

    print(f"\n検索対象: {len(missing_horse_ids):,} 頭")

    # プロフィール・過去成績ファイル
    for horse_id in missing_horse_ids:
        profile_file = html_dir / f"{horse_id}_profile.bin"
        perf_file = html_dir / f"{horse_id}_perf.bin"

        if profile_file.exists():
            profile_files.append(str(profile_file))
        if perf_file.exists():
            perf_files.append(str(perf_file))

    # 血統ファイル
    for horse_id in missing_horse_ids:
        ped_file = ped_dir / f"{horse_id}.bin"
        if ped_file.exists():
            ped_files.append(str(ped_file))

    print(f"\n✅ 見つかったファイル:")
    print(f"  プロフィール: {len(profile_files):,} 件")
    print(f"  過去成績: {len(perf_files):,} 件")
    print(f"  血統: {len(ped_files):,} 件")

    missing_profile = len(missing_horse_ids) - len(profile_files)
    if missing_profile > 0:
        print(f"\n⚠️ プロフィールHTMLが見つからない馬: {missing_profile:,} 頭")

    return profile_files, perf_files, ped_files


def parse_horse_profiles(profile_files):
    """馬プロフィールをパース"""
    print("\n" + "=" * 80)
    print("🐴 馬プロフィールのパース")
    print("=" * 80)

    all_horses_data = []
    errors = []

    # DBダミー接続（エラーログ用）
    db_path = Path("keibaai/data/metadata/db.sqlite3")
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)

    total = len(profile_files)
    for i, html_file in enumerate(profile_files, 1):
        if i % 100 == 0:
            print(f"  処理中: {i:,} / {total:,} ({i/total*100:.1f}%)")

        try:
            data = pipeline_core.parse_with_error_handling(
                str(html_file),
                "horse_info_parser",
                horse_info_parser.parse_horse_profile,
                conn
            )

            if data and 'horse_id' in data and data['horse_id']:
                if not data.get('_is_empty', False):
                    all_horses_data.append(data)
        except Exception as e:
            errors.append((html_file, str(e)))
            logger.warning(f"パースエラー: {html_file} - {e}")

    conn.close()

    print(f"\n✅ パース完了: {len(all_horses_data):,} 頭")
    if errors:
        print(f"⚠️ エラー: {len(errors)} 件")

    return pd.DataFrame(all_horses_data) if all_horses_data else pd.DataFrame()


def parse_pedigrees(ped_files):
    """血統データをパース"""
    print("\n" + "=" * 80)
    print("🌳 血統データのパース")
    print("=" * 80)

    all_pedigrees_df = []
    errors = []

    db_path = Path("keibaai/data/metadata/db.sqlite3")
    conn = sqlite3.connect(db_path)

    total = len(ped_files)
    for i, html_file in enumerate(ped_files, 1):
        if i % 100 == 0:
            print(f"  処理中: {i:,} / {total:,} ({i/total*100:.1f}%)")

        try:
            df = pipeline_core.parse_with_error_handling(
                str(html_file),
                "pedigree_parser",
                pedigree_parser.parse_pedigree_html,
                conn
            )

            if df is not None and not df.empty:
                all_pedigrees_df.append(df)
        except Exception as e:
            errors.append((html_file, str(e)))
            logger.warning(f"パースエラー: {html_file} - {e}")

    conn.close()

    if all_pedigrees_df:
        final_df = pd.concat(all_pedigrees_df, ignore_index=True)
        print(f"\n✅ パース完了: {len(final_df):,} 行（{final_df['horse_id'].nunique():,} 頭）")
    else:
        final_df = pd.DataFrame()
        print("\n⚠️ パースできたデータがありません")

    if errors:
        print(f"⚠️ エラー: {len(errors)} 件")

    return final_df


def parse_horse_performance(perf_files):
    """馬過去成績をパース"""
    print("\n" + "=" * 80)
    print("📊 馬過去成績のパース")
    print("=" * 80)

    all_perf_df = []
    errors = []

    db_path = Path("keibaai/data/metadata/db.sqlite3")
    conn = sqlite3.connect(db_path)

    total = len(perf_files)
    for i, html_file in enumerate(perf_files, 1):
        if i % 100 == 0:
            print(f"  処理中: {i:,} / {total:,} ({i/total*100:.1f}%)")

        try:
            df = pipeline_core.parse_with_error_handling(
                str(html_file),
                "horse_performance_parser",
                horse_info_parser.parse_horse_performance,
                conn
            )

            if df is not None and not df.empty:
                all_perf_df.append(df)
        except Exception as e:
            errors.append((html_file, str(e)))
            logger.warning(f"パースエラー: {html_file} - {e}")

    conn.close()

    if all_perf_df:
        final_df = pd.concat(all_perf_df, ignore_index=True)
        print(f"\n✅ パース完了: {len(final_df):,} 行（{final_df['horse_id'].nunique():,} 頭）")
    else:
        final_df = pd.DataFrame()
        print("\n⚠️ パースできたデータがありません")

    if errors:
        print(f"⚠️ エラー: {len(errors)} 件")

    return final_df


def merge_and_save(new_horses_df, new_pedigrees_df, new_performance_df):
    """既存データと結合して保存"""
    print("\n" + "=" * 80)
    print("💾 データの結合と保存")
    print("=" * 80)

    base_dir = Path("keibaai/data/parsed/parquet")

    # --- horses.parquet の更新 ---
    horses_path = base_dir / "horses" / "horses.parquet"
    if horses_path.exists():
        existing_horses = pd.read_parquet(horses_path)
        print(f"\n既存 horses.parquet: {len(existing_horses):,} 頭")
        print(f"新規データ: {len(new_horses_df):,} 頭")

        # 結合
        combined_horses = pd.concat([existing_horses, new_horses_df], ignore_index=True)
        # 重複排除（最新を優先）
        combined_horses = combined_horses.drop_duplicates(subset='horse_id', keep='last')
        print(f"結合後（重複排除）: {len(combined_horses):,} 頭")
    else:
        combined_horses = new_horses_df
        print(f"\n新規作成 horses.parquet: {len(combined_horses):,} 頭")

    combined_horses.to_parquet(horses_path, index=False)
    print(f"✅ 保存完了: {horses_path}")

    # --- pedigrees.parquet の更新 ---
    pedigrees_path = base_dir / "pedigrees" / "pedigrees.parquet"
    if pedigrees_path.exists() and not new_pedigrees_df.empty:
        existing_pedigrees = pd.read_parquet(pedigrees_path)
        print(f"\n既存 pedigrees.parquet: {len(existing_pedigrees):,} 行")
        print(f"新規データ: {len(new_pedigrees_df):,} 行")

        # 結合
        combined_pedigrees = pd.concat([existing_pedigrees, new_pedigrees_df], ignore_index=True)
        # 重複排除
        combined_pedigrees = combined_pedigrees.drop_duplicates(
            subset=['horse_id', 'ancestor_id', 'generation'],
            keep='last'
        )
        print(f"結合後（重複排除）: {len(combined_pedigrees):,} 行")
    else:
        combined_pedigrees = new_pedigrees_df
        print(f"\n新規作成 pedigrees.parquet: {len(combined_pedigrees):,} 行")

    if not combined_pedigrees.empty:
        combined_pedigrees.to_parquet(pedigrees_path, index=False)
        print(f"✅ 保存完了: {pedigrees_path}")

    # --- horses_performance.parquet の作成/更新 ---
    if not new_performance_df.empty:
        perf_dir = base_dir / "horses_performance"
        perf_dir.mkdir(parents=True, exist_ok=True)
        perf_path = perf_dir / "horses_performance.parquet"

        if perf_path.exists():
            existing_perf = pd.read_parquet(perf_path)
            print(f"\n既存 horses_performance.parquet: {len(existing_perf):,} 行")
            print(f"新規データ: {len(new_performance_df):,} 行")

            combined_perf = pd.concat([existing_perf, new_performance_df], ignore_index=True)
            combined_perf = combined_perf.drop_duplicates(
                subset=['horse_id', 'race_date', 'race_number'],
                keep='last'
            )
            print(f"結合後（重複排除）: {len(combined_perf):,} 行")
        else:
            combined_perf = new_performance_df
            print(f"\n新規作成 horses_performance.parquet: {len(combined_perf):,} 行")

        # データ型最適化
        int_columns = ['race_number', 'head_count', 'bracket_number', 'horse_number',
                      'finish_position', 'popularity', 'horse_weight', 'horse_weight_change']
        for col in int_columns:
            if col in combined_perf.columns:
                combined_perf[col] = combined_perf[col].astype('Int64')

        combined_perf.to_parquet(perf_path, index=False)
        print(f"✅ 保存完了: {perf_path}")


def main():
    """メイン処理"""
    print("=" * 80)
    print("🔍 KeibaAI_v2 不足馬データの差分パース")
    print(f"⏰ 実行時刻: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)

    # ステップ1: 不足している馬IDを特定
    missing_horse_ids, existing_horse_ids = get_missing_horse_ids()

    if not missing_horse_ids:
        print("\n✅ すべての馬データが揃っています！")
        return

    # ステップ2: HTMLファイル検索
    profile_files, perf_files, ped_files = find_html_files(missing_horse_ids)

    if not profile_files and not ped_files:
        print("\n⚠️ パース対象のHTMLファイルが見つかりませんでした")
        return

    # ステップ3: パース実行
    print("\n" + "=" * 80)
    print("⚙️ パース処理開始")
    print("=" * 80)

    new_horses_df = parse_horse_profiles(profile_files) if profile_files else pd.DataFrame()
    new_pedigrees_df = parse_pedigrees(ped_files) if ped_files else pd.DataFrame()
    new_performance_df = parse_horse_performance(perf_files) if perf_files else pd.DataFrame()

    # ステップ4: 保存
    if not new_horses_df.empty or not new_pedigrees_df.empty:
        merge_and_save(new_horses_df, new_pedigrees_df, new_performance_df)
    else:
        print("\n⚠️ パースできたデータがありません")

    print("\n" + "=" * 80)
    print("📋 次のステップ")
    print("=" * 80)
    print("""
💡 差分パース完了！次は以下を実行します:

1. ベーステーブルを再作成（血統情報を含む）
   → python debug_04_create_analysis_base_table.py

2. 血統情報の欠損率を確認
   → 91% → 数%に改善されているはず

3. FeatureEngine で特徴量生成
   → python keibaai/src/features/generate_features.py
    """)

    print("=" * 80)
    print("✅ デバッグスクリプト05完了")
    print("=" * 80)


if __name__ == "__main__":
    main()
