#!/usr/bin/env python3
"""
デバッグスクリプト04: サンプルパース処理（100件テスト）
パーサーの動作確認と問題点の早期発見を目的とした軽量版パース処理
"""

import logging
import sqlite3
from pathlib import Path
import yaml
from datetime import datetime
import sys
import pandas as pd
import os

# プロジェクトルートをsys.pathに追加
project_root = Path(__file__).resolve().parent / "keibaai"
sys.path.insert(0, str(project_root))

from src import pipeline_core
from src.modules.parsers import results_parser, shutuba_parser, horse_info_parser, pedigree_parser

# サンプル件数（テスト用に少数に設定）
SAMPLE_SIZE = 100

def load_config():
    """設定ファイルをロードする"""
    config_path = Path("keibaai/configs")
    with open(config_path / "default.yaml", "r", encoding="utf-8") as f:
        default_cfg = yaml.safe_load(f)

    data_root = Path("keibaai") / default_cfg['data_path']
    default_cfg['raw_data_path'] = str(data_root / 'raw')
    default_cfg['parsed_data_path'] = str(data_root / 'parsed')
    default_cfg['database']['path'] = str(data_root / 'metadata' / 'db.sqlite3')

    return {"default": default_cfg}

def setup_logging():
    """ログ設定"""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[logging.StreamHandler()]
    )

def parse_sample_races(sample_size=SAMPLE_SIZE):
    """レース結果のサンプルパース"""
    print("\n" + "=" * 80)
    print(f"🏇 レース結果のサンプルパース（{sample_size}件）")
    print("=" * 80)

    cfg = load_config()
    raw_race_html_dir = Path(cfg["default"]["raw_data_path"]) / "html" / "race"

    # HTMLファイルを取得
    race_html_files = sorted(list(raw_race_html_dir.glob("*.bin")))[:sample_size]

    print(f"\n📊 対象ファイル数: {len(race_html_files)} 件")

    # パース処理
    db_path = Path(cfg["default"]["database"]["path"])
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)

    success_count = 0
    error_count = 0
    all_results_df = []
    error_details = []

    start_time = datetime.now()

    for i, html_file in enumerate(race_html_files, 1):
        try:
            df = results_parser.parse_results_html(str(html_file))

            if df is not None and not df.empty:
                all_results_df.append(df)
                success_count += 1
                print(f"  ✅ [{i:>3}/{len(race_html_files)}] {html_file.name}: {len(df)}行")
            else:
                error_count += 1
                error_details.append((html_file.name, "空のDataFrame"))
                print(f"  ⚠️ [{i:>3}/{len(race_html_files)}] {html_file.name}: 空データ")

        except Exception as e:
            error_count += 1
            error_msg = str(e)[:100]
            error_details.append((html_file.name, error_msg))
            print(f"  ❌ [{i:>3}/{len(race_html_files)}] {html_file.name}: {error_msg}")

    elapsed = (datetime.now() - start_time).total_seconds()

    conn.close()

    # 結果サマリー
    print(f"\n📊 パース結果:")
    print(f"  成功: {success_count:>6} 件 ({success_count/len(race_html_files)*100:>5.1f}%)")
    print(f"  失敗: {error_count:>6} 件 ({error_count/len(race_html_files)*100:>5.1f}%)")
    print(f"  処理時間: {elapsed:.2f} 秒")
    print(f"  平均処理時間: {elapsed/len(race_html_files):.3f} 秒/件")

    # エラー詳細
    if error_details:
        print(f"\n⚠️ エラー詳細（最初の10件）:")
        for file_name, error_msg in error_details[:10]:
            print(f"  - {file_name}: {error_msg}")

    # 成功したデータの内容確認
    if all_results_df:
        final_df = pd.concat(all_results_df, ignore_index=True)
        print(f"\n✅ パース成功データ:")
        print(f"  総行数: {len(final_df):,} 行")
        print(f"  列数: {len(final_df.columns)} 列")
        print(f"\n  カラム一覧:")
        for col in final_df.columns:
            null_rate = final_df[col].isna().sum() / len(final_df) * 100
            print(f"    - {col:35s} (欠損率: {null_rate:>5.1f}%)")

        return final_df, success_count, error_count

    return None, success_count, error_count

def parse_sample_shutuba(sample_size=SAMPLE_SIZE):
    """出馬表のサンプルパース"""
    print("\n" + "=" * 80)
    print(f"📋 出馬表のサンプルパース（{sample_size}件）")
    print("=" * 80)

    cfg = load_config()
    raw_shutuba_html_dir = Path(cfg["default"]["raw_data_path"]) / "html" / "shutuba"

    shutuba_html_files = sorted(list(raw_shutuba_html_dir.glob("*.bin")))[:sample_size]

    print(f"\n📊 対象ファイル数: {len(shutuba_html_files)} 件")

    db_path = Path(cfg["default"]["database"]["path"])
    conn = sqlite3.connect(db_path)

    success_count = 0
    error_count = 0
    all_shutuba_df = []
    error_details = []

    start_time = datetime.now()

    for i, html_file in enumerate(shutuba_html_files, 1):
        try:
            df = shutuba_parser.parse_shutuba_html(str(html_file))

            if df is not None and not df.empty:
                all_shutuba_df.append(df)
                success_count += 1

                # morning_oddsの有無を確認
                has_morning_odds = 'morning_odds' in df.columns and df['morning_odds'].notna().sum() > 0
                odds_status = "✓ オッズあり" if has_morning_odds else "✗ オッズなし"

                print(f"  ✅ [{i:>3}/{len(shutuba_html_files)}] {html_file.name}: {len(df)}行 {odds_status}")
            else:
                error_count += 1
                error_details.append((html_file.name, "空のDataFrame"))
                print(f"  ⚠️ [{i:>3}/{len(shutuba_html_files)}] {html_file.name}: 空データ")

        except Exception as e:
            error_count += 1
            error_msg = str(e)[:100]
            error_details.append((html_file.name, error_msg))
            print(f"  ❌ [{i:>3}/{len(shutuba_html_files)}] {html_file.name}: {error_msg}")

    elapsed = (datetime.now() - start_time).total_seconds()

    conn.close()

    print(f"\n📊 パース結果:")
    print(f"  成功: {success_count:>6} 件 ({success_count/len(shutuba_html_files)*100:>5.1f}%)")
    print(f"  失敗: {error_count:>6} 件 ({error_count/len(shutuba_html_files)*100:>5.1f}%)")
    print(f"  処理時間: {elapsed:.2f} 秒")

    if all_shutuba_df:
        final_df = pd.concat(all_shutuba_df, ignore_index=True)
        print(f"\n✅ パース成功データ:")
        print(f"  総行数: {len(final_df):,} 行")

        # 重要カラムの欠損状況
        important_cols = ['morning_odds', 'morning_popularity', 'owner_name', 'prize_total']
        print(f"\n  重要カラムの欠損状況:")
        for col in important_cols:
            if col in final_df.columns:
                null_count = final_df[col].isna().sum()
                null_rate = null_count / len(final_df) * 100
                status = "❌" if null_rate > 90 else "⚠️" if null_rate > 50 else "✅"
                print(f"    {status} {col:25s}: {null_rate:>5.1f}% 欠損")
            else:
                print(f"    ❌ {col:25s}: カラム自体が存在しない")

        return final_df, success_count, error_count

    return None, success_count, error_count

def parse_sample_horses(sample_size=SAMPLE_SIZE):
    """馬プロフィールのサンプルパース"""
    print("\n" + "=" * 80)
    print(f"🐴 馬プロフィールのサンプルパース（{sample_size}件）")
    print("=" * 80)

    cfg = load_config()
    raw_horse_html_dir = Path(cfg["default"]["raw_data_path"]) / "html" / "horse"

    # プロフィールファイルのみを取得
    horse_profile_files = sorted([
        f for f in raw_horse_html_dir.glob("*_profile.bin")
    ])[:sample_size]

    print(f"\n📊 対象ファイル数: {len(horse_profile_files)} 件")

    db_path = Path(cfg["default"]["database"]["path"])
    conn = sqlite3.connect(db_path)

    success_count = 0
    error_count = 0
    all_horses_data = []
    error_details = []

    start_time = datetime.now()

    for i, html_file in enumerate(horse_profile_files, 1):
        try:
            data = horse_info_parser.parse_horse_profile(str(html_file))

            if data and 'horse_id' in data and data['horse_id']:
                all_horses_data.append(data)
                success_count += 1

                # 血統情報の有無を確認
                has_sire = 'sire_id' in data and data['sire_id']
                pedigree_status = "✓ 血統あり" if has_sire else "✗ 血統なし"

                print(f"  ✅ [{i:>3}/{len(horse_profile_files)}] {html_file.name}: {pedigree_status}")
            else:
                error_count += 1
                error_details.append((html_file.name, "horse_id取得失敗"))
                print(f"  ⚠️ [{i:>3}/{len(horse_profile_files)}] {html_file.name}: データ不完全")

        except Exception as e:
            error_count += 1
            error_msg = str(e)[:100]
            error_details.append((html_file.name, error_msg))
            print(f"  ❌ [{i:>3}/{len(horse_profile_files)}] {html_file.name}: {error_msg}")

    elapsed = (datetime.now() - start_time).total_seconds()

    conn.close()

    print(f"\n📊 パース結果:")
    print(f"  成功: {success_count:>6} 件 ({success_count/len(horse_profile_files)*100:>5.1f}%)")
    print(f"  失敗: {error_count:>6} 件 ({error_count/len(horse_profile_files)*100:>5.1f}%)")
    print(f"  処理時間: {elapsed:.2f} 秒")

    if all_horses_data:
        final_df = pd.DataFrame(all_horses_data)
        print(f"\n✅ パース成功データ:")
        print(f"  総行数: {len(final_df):,} 行")
        print(f"\n  カラム一覧:")
        for col in final_df.columns:
            print(f"    - {col}")

        # 血統情報の確認
        pedigree_cols = ['sire_id', 'dam_id', 'damsire_id']
        print(f"\n  血統カラムの状況:")
        for col in pedigree_cols:
            if col in final_df.columns:
                null_rate = final_df[col].isna().sum() / len(final_df) * 100
                status = "✅" if null_rate < 10 else "⚠️"
                print(f"    {status} {col:15s}: {null_rate:>5.1f}% 欠損")
            else:
                print(f"    ❌ {col:15s}: カラム自体が存在しない")

        return final_df, success_count, error_count

    return None, success_count, error_count

def estimate_full_parse_time(sample_results):
    """全件パース処理の推定時間を計算"""
    print("\n" + "=" * 80)
    print("⏱️ 全件パース処理の推定時間")
    print("=" * 80)

    race_time_per_file = sample_results['race']['time'] / sample_results['race']['sample_size']
    shutuba_time_per_file = sample_results['shutuba']['time'] / sample_results['shutuba']['sample_size']
    horse_time_per_file = sample_results['horse']['time'] / sample_results['horse']['sample_size']

    # 全件数
    total_race_files = 20157
    total_shutuba_files = 20157
    total_horse_files = 27150

    estimated_race_time = race_time_per_file * total_race_files
    estimated_shutuba_time = shutuba_time_per_file * total_shutuba_files
    estimated_horse_time = horse_time_per_file * total_horse_files

    total_estimated_time = estimated_race_time + estimated_shutuba_time + estimated_horse_time

    print(f"\n📊 推定所要時間:")
    print(f"  レース結果: {estimated_race_time/60:>8.1f} 分")
    print(f"  出馬表:     {estimated_shutuba_time/60:>8.1f} 分")
    print(f"  馬情報:     {estimated_horse_time/60:>8.1f} 分")
    print(f"  -----------------------------")
    print(f"  合計:       {total_estimated_time/60:>8.1f} 分 ({total_estimated_time/3600:.1f} 時間)")

    print(f"\n💡 推奨:")
    if total_estimated_time < 600:  # 10分未満
        print(f"  処理時間が短いので、すぐに全件パース処理を実行できます")
    elif total_estimated_time < 3600:  # 1時間未満
        print(f"  処理時間は中程度です。時間に余裕があるときに実行してください")
    else:
        print(f"  処理時間が長いため、夜間バッチ処理での実行を推奨します")

def main():
    """メイン処理"""
    print("\n")
    print("🔍 KeibaAI_v2 サンプルパース処理（動作確認）")
    print(f"⏰ 実行時刻: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"📊 サンプル件数: {SAMPLE_SIZE} 件")
    print("\n")

    setup_logging()

    sample_results = {
        'race': {},
        'shutuba': {},
        'horse': {}
    }

    # 1. レース結果のサンプルパース
    start = datetime.now()
    race_df, race_success, race_error = parse_sample_races(SAMPLE_SIZE)
    sample_results['race'] = {
        'time': (datetime.now() - start).total_seconds(),
        'sample_size': SAMPLE_SIZE,
        'success': race_success,
        'error': race_error
    }

    # 2. 出馬表のサンプルパース
    start = datetime.now()
    shutuba_df, shutuba_success, shutuba_error = parse_sample_shutuba(SAMPLE_SIZE)
    sample_results['shutuba'] = {
        'time': (datetime.now() - start).total_seconds(),
        'sample_size': SAMPLE_SIZE,
        'success': shutuba_success,
        'error': shutuba_error
    }

    # 3. 馬プロフィールのサンプルパース
    start = datetime.now()
    horse_df, horse_success, horse_error = parse_sample_horses(SAMPLE_SIZE)
    sample_results['horse'] = {
        'time': (datetime.now() - start).total_seconds(),
        'sample_size': SAMPLE_SIZE,
        'success': horse_success,
        'error': horse_error
    }

    # 4. 全件パース推定時間
    estimate_full_parse_time(sample_results)

    # 最終サマリー
    print("\n" + "=" * 80)
    print("📋 サンプルパース完了")
    print("=" * 80)

    total_success = race_success + shutuba_success + horse_success
    total_files = SAMPLE_SIZE * 3
    success_rate = total_success / total_files * 100

    print(f"\n総合成功率: {success_rate:.1f}% ({total_success}/{total_files})")

    if success_rate > 90:
        print(f"\n✅ パーサーは正常に動作しています！")
        print(f"💡 次のアクション: 全件パース処理を実行してください")
        print(f"   → python keibaai/src/run_parsing_pipeline_local.py")
    elif success_rate > 50:
        print(f"\n⚠️ パーサーに一部問題がありますが、おおむね動作しています")
        print(f"💡 次のアクション: エラー内容を確認し、必要に応じて修正してください")
    else:
        print(f"\n🚨 パーサーに重大な問題があります！")
        print(f"💡 次のアクション: エラー内容を詳しく調査し、パーサーを修正してください")

    print("\n" + "=" * 80)
    print("\n")

if __name__ == "__main__":
    main()
