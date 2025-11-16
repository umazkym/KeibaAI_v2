# -*- coding: utf-8 -*-
"""
特定日付の完全スクレイピング＆パースパイプライン

機能:
1. 指定日付のレース一覧を取得
2. レース結果 + 出馬表をスクレイピング
3. 馬ID抽出 → 馬プロフィール + 過去成績 + 血統をスクレイピング
4. 全データをパース
5. 複数のCSVを出力（test/test_outputのような構造）

使用方法:
    python debug_full_pipeline_by_date.py --date 2023-10-09 --output-dir output_20231009

    # 軽量モード（スクレイピングなし、既存binファイルのみパース）
    python debug_full_pipeline_by_date.py --date 2023-10-09 --parse-only
"""

import os
import re
import sys
import argparse
import time
from pathlib import Path
from typing import List, Dict, Set
from datetime import datetime

import pandas as pd
from bs4 import BeautifulSoup

# プロジェクトルートをパスに追加
sys.path.append(os.path.abspath('.'))

# スクレイピング関数をインポート
try:
    from keibaai.src.modules.preparing._scrape_html import (
        fetch_html_robust_get,
        scrape_kaisai_date,
        scrape_race_id_list
    )
    SCRAPING_AVAILABLE = True
except ImportError:
    SCRAPING_AVAILABLE = False
    print("[警告] スクレイピングモジュールが利用できません。--parse-only モードのみ使用可能です。")

# パース関数をインポート
from debug_scraping_and_parsing import (
    parse_html_content,
    extract_race_metadata_enhanced,
    parse_int_or_none,
    get_id_from_href
)

# --- 設定 ---
DEFAULT_OUTPUT_DIR = 'output'
DEFAULT_BIN_DIR = 'data/raw/html'

class CompletePipeline:
    """完全なスクレイピング＆パースパイプライン"""

    def __init__(self, target_date: str, output_dir: str, bin_dir: str = DEFAULT_BIN_DIR, parse_only: bool = False):
        self.target_date = target_date
        self.output_dir = Path(output_dir)
        self.bin_dir = Path(bin_dir)
        self.parse_only = parse_only

        # 出力ディレクトリ作成
        self.output_dir.mkdir(parents=True, exist_ok=True)

        # binファイル保存先
        self.race_bin_dir = self.bin_dir / 'race'
        self.shutuba_bin_dir = self.bin_dir / 'shutuba'
        self.horse_bin_dir = self.bin_dir / 'horse'
        self.ped_bin_dir = self.bin_dir / 'ped'

        for d in [self.race_bin_dir, self.shutuba_bin_dir, self.horse_bin_dir, self.ped_bin_dir]:
            d.mkdir(parents=True, exist_ok=True)

    def run(self):
        """パイプライン実行"""
        print(f"{'='*60}")
        print(f"完全スクレイピング＆パースパイプライン")
        print(f"{'='*60}")
        print(f"対象日付: {self.target_date}")
        print(f"出力先: {self.output_dir}")
        print(f"モード: {'パースのみ' if self.parse_only else 'スクレイピング＋パース'}")
        print(f"{'='*60}\n")

        if self.parse_only:
            # パースのみモード
            race_ids = self._find_existing_race_ids()
        else:
            # スクレイピング＋パース
            race_ids = self._scraping_phase()

        if not race_ids:
            print("[!] 対象のレースが見つかりませんでした")
            return

        # パースフェーズ
        self._parsing_phase(race_ids)

        print(f"\n{'='*60}")
        print(f"全ての処理が完了しました")
        print(f"出力先: {self.output_dir}")
        print(f"{'='*60}")

    def _find_existing_race_ids(self) -> List[str]:
        """既存のbinファイルから対象日付のrace_idを取得

        注意: race_idは暦日を含まないため、メタデータファイルから読み込む。
        メタデータファイルが存在しない場合は、全てのbinファイルをパースする。
        """
        print(f"\n【フェーズ】既存binファイルの検索")

        # メタデータファイルを探す
        metadata_file = self.output_dir / 'race_ids_metadata.txt'

        if metadata_file.exists():
            print(f"  メタデータファイルから読み込み: {metadata_file}")
            with open(metadata_file, 'r') as f:
                race_ids = [line.strip() for line in f if line.strip()]
            print(f"  [✓] {len(race_ids)} 件のレースを検出（メタデータより）")
            return race_ids

        # メタデータがない場合は、全てのbinファイルをパース
        print(f"  メタデータなし。全binファイルを検索...")
        race_files = list(self.race_bin_dir.glob('*.bin'))
        race_ids = []

        for f in race_files:
            race_id = f.stem
            # 正しいrace_id形式: 12桁の数字（_profile, _perfなどを除外）
            if len(race_id) == 12 and race_id.isdigit():
                race_ids.append(race_id)

        race_ids.sort()
        print(f"  [✓] {len(race_ids)} 件のレースを検出（全binファイルより）")

        return race_ids

    def _scraping_phase(self) -> List[str]:
        """スクレイピングフェーズ"""
        if not SCRAPING_AVAILABLE:
            print("[!] スクレイピング機能が利用できません。--parse-only を使用してください。")
            return []

        print(f"\n【フェーズ0】開催日とレースIDの取得")

        # 開催日の取得
        kaisai_dates = scrape_kaisai_date(self.target_date, self.target_date)
        print(f"  [✓] {len(kaisai_dates)} 個の開催日を取得")

        if not kaisai_dates:
            return []

        # レースIDの取得
        race_ids = scrape_race_id_list(kaisai_dates)
        print(f"  [✓] {len(race_ids)} 個のレースIDを取得")

        if not race_ids:
            return []

        # フェーズ1: レース結果＋出馬表のスクレイピング
        print(f"\n【フェーズ1】レース結果＋出馬表のスクレイピング")

        for i, race_id in enumerate(race_ids):
            print(f"  [{i+1}/{len(race_ids)}] {race_id}")

            # レース結果
            race_url = f"https://db.netkeiba.com/race/{race_id}/"
            race_html = fetch_html_robust_get(race_url)
            if race_html:
                race_file = self.race_bin_dir / f"{race_id}.bin"
                race_file.write_bytes(race_html)

            time.sleep(2)  # サーバー負荷軽減

            # 出馬表（race_idの末尾を変更）
            # 通常は同じrace_idで出馬表も取得可能
            shutuba_url = f"https://race.netkeiba.com/race/shutuba.html?race_id={race_id}"
            shutuba_html = fetch_html_robust_get(shutuba_url)
            if shutuba_html:
                shutuba_file = self.shutuba_bin_dir / f"{race_id}.bin"
                shutuba_file.write_bytes(shutuba_html)

            time.sleep(2)

        # race_idsをメタデータファイルに保存（--parse-only モード用）
        metadata_file = self.output_dir / 'race_ids_metadata.txt'
        with open(metadata_file, 'w') as f:
            for race_id in race_ids:
                f.write(f"{race_id}\n")
        print(f"  [✓] race_idsをメタデータに保存: {metadata_file}")

        # フェーズ2: 馬IDの抽出
        print(f"\n【フェーズ2】馬IDの抽出")
        # 修正: 日付パターンではなく、実際にスクレイピングしたrace_idsからbinファイルを特定
        bin_files = [self.race_bin_dir / f"{race_id}.bin" for race_id in race_ids]
        bin_files = [f for f in bin_files if f.exists()]
        horse_ids = self._extract_horse_ids_from_bins(bin_files)
        print(f"  [✓] {len(horse_ids)} 頭のユニークな馬IDを抽出")

        # フェーズ3: 馬データのスクレイピング（サンプル版：最初の10頭のみ）
        print(f"\n【フェーズ3】馬データのスクレイピング（最初の10頭）")

        for i, horse_id in enumerate(list(horse_ids)[:10]):
            print(f"  [{i+1}/10] {horse_id}")

            # 馬プロフィール
            profile_url = f"https://db.netkeiba.com/horse/{horse_id}/"
            profile_html = fetch_html_robust_get(profile_url)
            if profile_html:
                profile_file = self.horse_bin_dir / f"{horse_id}_profile.bin"
                profile_file.write_bytes(profile_html)

            time.sleep(2)

            # 馬過去成績（AJAX API）
            perf_url = f"https://db.netkeiba.com/horse/result/{horse_id}/"
            perf_html = fetch_html_robust_get(perf_url)
            if perf_html:
                perf_file = self.horse_bin_dir / f"{horse_id}_perf.bin"
                perf_file.write_bytes(perf_html)

            time.sleep(2)

            # 血統
            ped_url = f"https://db.netkeiba.com/horse/ped/{horse_id}/"
            ped_html = fetch_html_robust_get(ped_url)
            if ped_html:
                ped_file = self.ped_bin_dir / f"{horse_id}.bin"
                ped_file.write_bytes(ped_html)

            time.sleep(2)

        return race_ids

    def _extract_horse_ids_from_bins(self, bin_files: List[Path]) -> Set[str]:
        """binファイルから馬IDを抽出"""
        horse_ids = set()

        for bin_file in bin_files:
            try:
                with open(bin_file, 'rb') as f:
                    html_bytes = f.read()

                html_text = html_bytes.decode('euc_jp', errors='replace')
                soup = BeautifulSoup(html_text, 'html.parser')

                # 馬リンクからIDを抽出
                horse_links = soup.find_all('a', href=re.compile(r'/horse/\d+'))
                for link in horse_links:
                    href = link.get('href')
                    match = re.search(r'/horse/(\d+)', href)
                    if match:
                        horse_ids.add(match.group(1))

            except Exception as e:
                print(f"    [!] エラー: {bin_file.name} - {e}")
                continue

        return horse_ids

    def _parsing_phase(self, race_ids: List[str]):
        """パースフェーズ"""
        print(f"\n【フェーズ4】全データのパース")

        # 1. レース結果のパース
        print(f"\n  --- レース結果のパース ---")
        race_results = []

        for i, race_id in enumerate(race_ids):
            race_file = self.race_bin_dir / f"{race_id}.bin"

            if not race_file.exists():
                print(f"    [{i+1}/{len(race_ids)}] {race_id} - ファイルなし")
                continue

            print(f"    [{i+1}/{len(race_ids)}] {race_id}")

            try:
                with open(race_file, 'rb') as f:
                    html_bytes = f.read()

                df = parse_html_content(html_bytes, race_id)

                if not df.empty:
                    race_results.append(df)
                    print(f"      [✓] {len(df)}頭")
                else:
                    print(f"      [!] データなし")

            except Exception as e:
                print(f"      [!] エラー: {e}")
                continue

        if race_results:
            race_df = pd.concat(race_results, ignore_index=True)
            output_file = self.output_dir / 'race_results.csv'
            race_df.to_csv(output_file, index=False, encoding='utf-8-sig')
            print(f"\n  [✓] race_results.csv: {len(race_df)}行")

            # 簡易統計
            distance_missing = race_df['distance_m'].isna().sum()
            print(f"      distance_m 欠損: {distance_missing}行 ({distance_missing/len(race_df)*100:.2f}%)")

            surface_missing = race_df['track_surface'].isna().sum()
            print(f"      track_surface 欠損: {surface_missing}行 ({surface_missing/len(race_df)*100:.2f}%)")

            # 診断: 欠損が発生しているレースを特定
            if distance_missing > 0:
                print(f"\n      【診断】distance_m 欠損レース:")
                missing_races = race_df[race_df['distance_m'].isna()]['race_id'].unique()
                for race_id in sorted(missing_races):
                    race_data = race_df[race_df['race_id'] == race_id]
                    race_name = race_data.iloc[0]['race_name'] if not race_data.empty else 'N/A'
                    metadata_source = race_data.iloc[0].get('metadata_source', 'Unknown')
                    print(f"        {race_id}: {race_name} ({len(race_data)}頭, source={metadata_source})")

        # 2. 出馬表のパース（正しいパーサーを使用）
        print(f"\n  --- 出馬表のパース ---")

        # 正しいパーサーをインポート
        try:
            from keibaai.src.modules.parsers.shutuba_parser import parse_shutuba_html
            SHUTUBA_PARSER_AVAILABLE = True
        except ImportError:
            print("  [!] shutuba_parser.py が見つかりません。簡易版パースを使用します。")
            SHUTUBA_PARSER_AVAILABLE = False

        shutuba_results = []

        for i, race_id in enumerate(race_ids):
            shutuba_file = self.shutuba_bin_dir / f"{race_id}.bin"

            if not shutuba_file.exists():
                continue

            try:
                if SHUTUBA_PARSER_AVAILABLE:
                    # 正しいパーサーを使用
                    df = parse_shutuba_html(str(shutuba_file), race_id)

                    if not df.empty:
                        shutuba_results.append(df)
                        print(f"    [{i+1}/{len(race_ids)}] {race_id} - ✓ {len(df)}頭")
                    else:
                        print(f"    [{i+1}/{len(race_ids)}] {race_id} - データなし")
                else:
                    # フォールバック: メタデータのみ抽出
                    with open(shutuba_file, 'rb') as f:
                        html_bytes = f.read()
                    html_text = html_bytes.decode('euc_jp', errors='replace')
                    soup = BeautifulSoup(html_text, 'html.parser')
                    metadata = extract_race_metadata_enhanced(soup)
                    metadata['race_id'] = race_id
                    shutuba_results.append(pd.DataFrame([metadata]))

            except Exception as e:
                print(f"    [{i+1}/{len(race_ids)}] {race_id} - エラー: {e}")
                continue

        if shutuba_results:
            shutuba_df = pd.concat(shutuba_results, ignore_index=True)
            output_file = self.output_dir / 'shutuba.csv'
            shutuba_df.to_csv(output_file, index=False, encoding='utf-8-sig')
            print(f"\n  [✓] shutuba.csv: {len(shutuba_df)}行")

            # 簡易統計
            if 'horse_id' in shutuba_df.columns:
                horse_id_missing = shutuba_df['horse_id'].isna().sum()
                print(f"      horse_id 欠損: {horse_id_missing}行 ({horse_id_missing/len(shutuba_df)*100:.2f}%)")

            if 'horse_name' in shutuba_df.columns:
                horse_name_missing = shutuba_df['horse_name'].isna().sum()
                print(f"      horse_name 欠損: {horse_name_missing}行 ({horse_name_missing/len(shutuba_df)*100:.2f}%)")

        # 3. 馬情報のパース（正しいパーサーを使用）
        print(f"\n  --- 馬情報のパース ---")

        # 正しいパーサーをインポート
        try:
            from keibaai.src.modules.parsers.horse_info_parser import (
                parse_horse_profile,
                parse_horse_performance
            )
            from keibaai.src.modules.parsers.pedigree_parser import parse_pedigree_html
            HORSE_PARSERS_AVAILABLE = True
        except ImportError as e:
            print(f"  [!] 馬情報パーサーのインポートに失敗しました: {e}")
            print(f"      Pythonパス: {sys.path[:3]}...")
            print(f"      カレントディレクトリ: {os.getcwd()}")
            print("  [注] パーサーが見つからない場合、binファイルのみ保存されます。")
            HORSE_PARSERS_AVAILABLE = False

        profile_files = list(self.horse_bin_dir.glob('*_profile.bin'))
        perf_files = list(self.horse_bin_dir.glob('*_perf.bin'))
        ped_files = list(self.ped_bin_dir.glob('*.bin'))

        print(f"  馬プロフィール: {len(profile_files)}件")
        print(f"  馬過去成績: {len(perf_files)}件")
        print(f"  血統: {len(ped_files)}件")

        if HORSE_PARSERS_AVAILABLE:
            # プロフィールのパース
            horse_profiles = []
            for i, profile_file in enumerate(profile_files):
                horse_id = profile_file.stem.replace('_profile', '')
                try:
                    # parse_horse_profile は Dict を返すので DataFrame に変換
                    profile_dict = parse_horse_profile(str(profile_file), horse_id)
                    if profile_dict and not profile_dict.get('_is_empty', False):
                        df = pd.DataFrame([profile_dict])
                        horse_profiles.append(df)
                        print(f"    プロフィール [{i+1}/{len(profile_files)}] {horse_id} - ✓")
                    else:
                        print(f"    プロフィール [{i+1}/{len(profile_files)}] {horse_id} - データなし")
                except Exception as e:
                    print(f"    プロフィール [{i+1}/{len(profile_files)}] {horse_id} - エラー: {e}")

            # 血統のパース
            pedigrees = []
            for i, ped_file in enumerate(ped_files):
                horse_id = ped_file.stem
                try:
                    df = parse_pedigree_html(str(ped_file), horse_id)
                    if not df.empty:
                        pedigrees.append(df)
                        print(f"    血統 [{i+1}/{len(ped_files)}] {horse_id} - ✓")
                except Exception as e:
                    print(f"    血統 [{i+1}/{len(ped_files)}] {horse_id} - エラー: {e}")

            # プロフィールと血統をマージして保存
            if horse_profiles:
                profile_df = pd.concat(horse_profiles, ignore_index=True)
                if pedigrees:
                    ped_df = pd.concat(pedigrees, ignore_index=True)
                    horses_df = pd.merge(profile_df, ped_df, on='horse_id', how='left')
                else:
                    horses_df = profile_df

                output_file = self.output_dir / 'horses.csv'
                horses_df.to_csv(output_file, index=False, encoding='utf-8-sig')
                print(f"\n  [✓] horses.csv: {len(horses_df)}行, {len(horses_df.columns)}カラム")

            # 過去成績のパース
            performance_results = []
            for i, perf_file in enumerate(perf_files):
                horse_id = perf_file.stem.replace('_perf', '')
                try:
                    df = parse_horse_performance(str(perf_file), horse_id)
                    if not df.empty:
                        performance_results.append(df)
                        print(f"    過去成績 [{i+1}/{len(perf_files)}] {horse_id} - ✓ {len(df)}走")
                    else:
                        print(f"    過去成績 [{i+1}/{len(perf_files)}] {horse_id} - データなし")
                except Exception as e:
                    print(f"    過去成績 [{i+1}/{len(perf_files)}] {horse_id} - エラー: {e}")

            if performance_results:
                perf_df = pd.concat(performance_results, ignore_index=True)
                output_file = self.output_dir / 'horses_performance.csv'
                perf_df.to_csv(output_file, index=False, encoding='utf-8-sig')
                print(f"\n  [✓] horses_performance.csv: {len(perf_df)}行, {len(perf_df.columns)}カラム")
        else:
            print(f"\n  [注] 馬プロフィール・過去成績・血統の詳細パースは未実装")
            print(f"       binファイルは保存済みです: {self.horse_bin_dir}")


def main():
    """メイン実行処理"""
    parser = argparse.ArgumentParser(
        description='特定日付の完全スクレイピング＆パースパイプライン'
    )
    parser.add_argument(
        '--date',
        type=str,
        required=True,
        help='対象日付（YYYY-MM-DD形式）例: 2023-10-09'
    )
    parser.add_argument(
        '--output-dir',
        type=str,
        default=DEFAULT_OUTPUT_DIR,
        help=f'出力ディレクトリ（デフォルト: {DEFAULT_OUTPUT_DIR}）'
    )
    parser.add_argument(
        '--bin-dir',
        type=str,
        default=DEFAULT_BIN_DIR,
        help=f'binファイルのルートディレクトリ（デフォルト: {DEFAULT_BIN_DIR}）'
    )
    parser.add_argument(
        '--parse-only',
        action='store_true',
        help='パースのみ実行（スクレイピングをスキップ）'
    )

    args = parser.parse_args()

    # パイプライン実行
    pipeline = CompletePipeline(
        target_date=args.date,
        output_dir=args.output_dir,
        bin_dir=args.bin_dir,
        parse_only=args.parse_only
    )

    pipeline.run()


if __name__ == "__main__":
    main()
