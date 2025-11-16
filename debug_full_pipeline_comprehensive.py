# -*- coding: utf-8 -*-
"""
包括的パーサーテストパイプライン (test/test_output準拠版)

機能:
1. 指定日付のレース一覧を取得（またはメタデータから読み込み）
2. レース結果 + 出馬表をスクレイピング
3. 馬ID抽出 → 馬プロフィール + 過去成績 + 血統をスクレイピング
4. 全データを正しいパーサーでパース
5. test/test_outputと同じ構造でCSVを出力:
   - races.csv (レース結果 - results_parser.py使用)
   - shutuba.csv (出馬表 - shutuba_parser.py使用)
   - horses.csv (馬プロフィール+血統 - horse_info_parser.py + pedigree_parser.py使用)
   - horses_performance.csv (馬過去成績 - horse_info_parser.py使用)

使用方法:
    # パースのみ（既存binファイルを使用）
    python debug_full_pipeline_comprehensive.py --date 2023-10-09 --output-dir output_20231009 --parse-only

    # スクレイピング + パース（警告: サーバー負荷に注意）
    python debug_full_pipeline_comprehensive.py --date 2023-10-09 --output-dir output_20231009

    # 馬データ取得数を制限
    python debug_full_pipeline_comprehensive.py --date 2023-10-09 --max-horses 5 --parse-only
"""

import os
import re
import sys
import argparse
import time
from pathlib import Path
from typing import List, Dict, Set, Optional
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

# 正しいパーサーをインポート
from keibaai.src.modules.parsers.results_parser import parse_race_results_html
from keibaai.src.modules.parsers.shutuba_parser import parse_shutuba_html
from keibaai.src.modules.parsers.horse_info_parser import (
    parse_horse_profile_html,
    parse_horse_performance_html
)
from keibaai.src.modules.parsers.pedigree_parser import parse_pedigree_html

# --- 設定 ---
DEFAULT_OUTPUT_DIR = 'output'
DEFAULT_BIN_DIR = 'data/raw/html'
DEFAULT_MAX_HORSES = 10  # スクレイピング時の馬データ取得上限

class ComprehensivePipeline:
    """包括的なスクレイピング＆パースパイプライン（test/test_output準拠）"""

    def __init__(
        self,
        target_date: str,
        output_dir: str,
        bin_dir: str = DEFAULT_BIN_DIR,
        parse_only: bool = False,
        max_horses: int = DEFAULT_MAX_HORSES
    ):
        self.target_date = target_date
        self.output_dir = Path(output_dir)
        self.bin_dir = Path(bin_dir)
        self.parse_only = parse_only
        self.max_horses = max_horses

        # 出力ディレクトリ作成
        self.output_dir.mkdir(parents=True, exist_ok=True)

        # binファイル保存先
        self.race_bin_dir = self.bin_dir / 'race'
        self.shutuba_bin_dir = self.bin_dir / 'shutuba'
        self.horse_bin_dir = self.bin_dir / 'horse'
        self.ped_bin_dir = self.bin_dir / 'ped'

        for d in [self.race_bin_dir, self.shutuba_bin_dir, self.horse_bin_dir, self.ped_bin_dir]:
            d.mkdir(parents=True, exist_ok=True)

        # 統計情報
        self.stats = {
            'races': {'total': 0, 'success': 0, 'failed': 0},
            'shutuba': {'total': 0, 'success': 0, 'failed': 0},
            'horses': {'total': 0, 'success': 0, 'failed': 0},
            'performance': {'total': 0, 'success': 0, 'failed': 0},
            'pedigree': {'total': 0, 'success': 0, 'failed': 0}
        }

    def run(self):
        """パイプライン実行"""
        print(f"{'='*70}")
        print(f"  包括的パーサーテストパイプライン (test/test_output準拠版)")
        print(f"{'='*70}")
        print(f"  対象日付: {self.target_date}")
        print(f"  出力先: {self.output_dir}")
        print(f"  モード: {'パースのみ' if self.parse_only else 'スクレイピング＋パース'}")
        if not self.parse_only:
            print(f"  馬データ取得上限: {self.max_horses}頭")
        print(f"{'='*70}\n")

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

        # 統計サマリー
        self._print_statistics()

        print(f"\n{'='*70}")
        print(f"  全ての処理が完了しました")
        print(f"  出力先: {self.output_dir}")
        print(f"{'='*70}")

    def _find_existing_race_ids(self) -> List[str]:
        """既存のbinファイルから対象日付のrace_idを取得"""
        print(f"\n【フェーズ0】既存binファイルの検索")

        # メタデータファイルを探す
        metadata_file = self.output_dir / 'race_ids_metadata.txt'

        if metadata_file.exists():
            print(f"  メタデータファイルから読み込み: {metadata_file}")
            with open(metadata_file, 'r') as f:
                race_ids = [line.strip() for line in f if line.strip()]
            print(f"  [✓] {len(race_ids)} 件のレースを検出（メタデータより）")
            return race_ids

        # メタデータがない場合は、全てのbinファイルを検索
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

        print(f"\n【フェーズ1】開催日とレースIDの取得")

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

        # フェーズ2: レース結果＋出馬表のスクレイピング
        print(f"\n【フェーズ2】レース結果＋出馬表のスクレイピング")

        for i, race_id in enumerate(race_ids):
            print(f"  [{i+1}/{len(race_ids)}] {race_id}")

            # レース結果
            race_url = f"https://db.netkeiba.com/race/{race_id}/"
            race_html = fetch_html_robust_get(race_url)
            if race_html:
                race_file = self.race_bin_dir / f"{race_id}.bin"
                race_file.write_bytes(race_html)

            time.sleep(2.5)  # サーバー負荷軽減

            # 出馬表
            shutuba_url = f"https://race.netkeiba.com/race/shutuba.html?race_id={race_id}"
            shutuba_html = fetch_html_robust_get(shutuba_url)
            if shutuba_html:
                shutuba_file = self.shutuba_bin_dir / f"{race_id}.bin"
                shutuba_file.write_bytes(shutuba_html)

            time.sleep(2.5)

        # race_idsをメタデータファイルに保存（--parse-only モード用）
        metadata_file = self.output_dir / 'race_ids_metadata.txt'
        with open(metadata_file, 'w') as f:
            for race_id in race_ids:
                f.write(f"{race_id}\n")
        print(f"  [✓] race_idsをメタデータに保存: {metadata_file}")

        # フェーズ3: 馬IDの抽出
        print(f"\n【フェーズ3】馬IDの抽出")
        bin_files = [self.race_bin_dir / f"{race_id}.bin" for race_id in race_ids]
        bin_files = [f for f in bin_files if f.exists()]
        horse_ids = self._extract_horse_ids_from_bins(bin_files)
        print(f"  [✓] {len(horse_ids)} 頭のユニークな馬IDを抽出")

        # フェーズ4: 馬データのスクレイピング
        print(f"\n【フェーズ4】馬データのスクレイピング（最大{self.max_horses}頭）")

        for i, horse_id in enumerate(list(horse_ids)[:self.max_horses]):
            print(f"  [{i+1}/{min(len(horse_ids), self.max_horses)}] {horse_id}")

            # 馬プロフィール
            profile_url = f"https://db.netkeiba.com/horse/{horse_id}/"
            profile_html = fetch_html_robust_get(profile_url)
            if profile_html:
                profile_file = self.horse_bin_dir / f"{horse_id}_profile.bin"
                profile_file.write_bytes(profile_html)

            time.sleep(2.5)

            # 馬過去成績
            perf_url = f"https://db.netkeiba.com/horse/result/{horse_id}/"
            perf_html = fetch_html_robust_get(perf_url)
            if perf_html:
                perf_file = self.horse_bin_dir / f"{horse_id}_perf.bin"
                perf_file.write_bytes(perf_html)

            time.sleep(2.5)

            # 血統
            ped_url = f"https://db.netkeiba.com/horse/ped/{horse_id}/"
            ped_html = fetch_html_robust_get(ped_url)
            if ped_html:
                ped_file = self.ped_bin_dir / f"{horse_id}.bin"
                ped_file.write_bytes(ped_html)

            time.sleep(2.5)

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
        """パースフェーズ（全パーサーを統合）"""
        print(f"\n{'='*70}")
        print(f"  【パースフェーズ】全データのパース開始")
        print(f"{'='*70}")

        # 1. レース結果のパース
        self._parse_race_results(race_ids)

        # 2. 出馬表のパース
        self._parse_shutuba(race_ids)

        # 3. 馬プロフィール＋血統のパース
        self._parse_horse_profiles()

        # 4. 馬過去成績のパース
        self._parse_horse_performance()

    def _parse_race_results(self, race_ids: List[str]):
        """レース結果のパース（results_parser.py使用）"""
        print(f"\n【1/4】レース結果のパース (results_parser.py)")
        race_results = []
        self.stats['races']['total'] = len(race_ids)

        for i, race_id in enumerate(race_ids):
            race_file = self.race_bin_dir / f"{race_id}.bin"

            if not race_file.exists():
                print(f"  [{i+1}/{len(race_ids)}] {race_id} - ファイルなし")
                self.stats['races']['failed'] += 1
                continue

            try:
                df = parse_race_results_html(str(race_file), race_id)

                if not df.empty:
                    # race_dateを追加（TARGET_DATEから）
                    df['race_date'] = self.target_date
                    race_results.append(df)
                    print(f"  [{i+1}/{len(race_ids)}] {race_id} - ✓ {len(df)}頭")
                    self.stats['races']['success'] += 1
                else:
                    print(f"  [{i+1}/{len(race_ids)}] {race_id} - データなし")
                    self.stats['races']['failed'] += 1

            except Exception as e:
                print(f"  [{i+1}/{len(race_ids)}] {race_id} - エラー: {e}")
                self.stats['races']['failed'] += 1
                continue

        if race_results:
            race_df = pd.concat(race_results, ignore_index=True)
            output_file = self.output_dir / 'races.csv'
            race_df.to_csv(output_file, index=False, encoding='utf-8-sig')

            print(f"\n  [✓] races.csv: {len(race_df)}行, {len(race_df.columns)}カラム")

            # 品質統計
            self._print_data_quality_stats(race_df, 'レース結果', [
                'distance_m', 'track_surface', 'weather', 'track_condition',
                'race_name', 'venue', 'post_time'
            ])

    def _parse_shutuba(self, race_ids: List[str]):
        """出馬表のパース（shutuba_parser.py使用）"""
        print(f"\n【2/4】出馬表のパース (shutuba_parser.py)")
        shutuba_results = []
        self.stats['shutuba']['total'] = len(race_ids)

        for i, race_id in enumerate(race_ids):
            shutuba_file = self.shutuba_bin_dir / f"{race_id}.bin"

            if not shutuba_file.exists():
                print(f"  [{i+1}/{len(race_ids)}] {race_id} - ファイルなし")
                self.stats['shutuba']['failed'] += 1
                continue

            try:
                df = parse_shutuba_html(str(shutuba_file), race_id)

                if not df.empty:
                    shutuba_results.append(df)
                    print(f"  [{i+1}/{len(race_ids)}] {race_id} - ✓ {len(df)}頭")
                    self.stats['shutuba']['success'] += 1
                else:
                    print(f"  [{i+1}/{len(race_ids)}] {race_id} - データなし")
                    self.stats['shutuba']['failed'] += 1

            except Exception as e:
                print(f"  [{i+1}/{len(race_ids)}] {race_id} - エラー: {e}")
                self.stats['shutuba']['failed'] += 1
                continue

        if shutuba_results:
            shutuba_df = pd.concat(shutuba_results, ignore_index=True)
            output_file = self.output_dir / 'shutuba.csv'
            shutuba_df.to_csv(output_file, index=False, encoding='utf-8-sig')

            print(f"\n  [✓] shutuba.csv: {len(shutuba_df)}行, {len(shutuba_df.columns)}カラム")

            # 品質統計
            self._print_data_quality_stats(shutuba_df, '出馬表', [
                'horse_id', 'horse_name', 'jockey_id', 'jockey_name',
                'trainer_id', 'trainer_name', 'horse_weight'
            ])

    def _parse_horse_profiles(self):
        """馬プロフィール＋血統のパース"""
        print(f"\n【3/4】馬プロフィール＋血統のパース (horse_info_parser.py + pedigree_parser.py)")

        profile_files = list(self.horse_bin_dir.glob('*_profile.bin'))
        ped_files = list(self.ped_bin_dir.glob('*.bin'))

        self.stats['horses']['total'] = len(profile_files)
        self.stats['pedigree']['total'] = len(ped_files)

        horse_profiles = []
        pedigrees = []

        # プロフィールのパース
        for i, profile_file in enumerate(profile_files):
            horse_id = profile_file.stem.replace('_profile', '')

            try:
                df = parse_horse_profile_html(str(profile_file), horse_id)

                if not df.empty:
                    horse_profiles.append(df)
                    print(f"  [{i+1}/{len(profile_files)}] {horse_id} (profile) - ✓")
                    self.stats['horses']['success'] += 1
                else:
                    print(f"  [{i+1}/{len(profile_files)}] {horse_id} (profile) - データなし")
                    self.stats['horses']['failed'] += 1

            except Exception as e:
                print(f"  [{i+1}/{len(profile_files)}] {horse_id} (profile) - エラー: {e}")
                self.stats['horses']['failed'] += 1
                continue

        # 血統のパース
        for i, ped_file in enumerate(ped_files):
            horse_id = ped_file.stem

            try:
                df = parse_pedigree_html(str(ped_file), horse_id)

                if not df.empty:
                    pedigrees.append(df)
                    print(f"  [{i+1}/{len(ped_files)}] {horse_id} (pedigree) - ✓")
                    self.stats['pedigree']['success'] += 1
                else:
                    print(f"  [{i+1}/{len(ped_files)}] {horse_id} (pedigree) - データなし")
                    self.stats['pedigree']['failed'] += 1

            except Exception as e:
                print(f"  [{i+1}/{len(ped_files)}] {horse_id} (pedigree) - エラー: {e}")
                self.stats['pedigree']['failed'] += 1
                continue

        # プロフィールと血統をマージ
        if horse_profiles:
            profile_df = pd.concat(horse_profiles, ignore_index=True)

            if pedigrees:
                ped_df = pd.concat(pedigrees, ignore_index=True)
                # horse_idでマージ
                horses_df = pd.merge(profile_df, ped_df, on='horse_id', how='left')
            else:
                horses_df = profile_df

            output_file = self.output_dir / 'horses.csv'
            horses_df.to_csv(output_file, index=False, encoding='utf-8-sig')

            print(f"\n  [✓] horses.csv: {len(horses_df)}行, {len(horses_df.columns)}カラム")

            # 品質統計
            self._print_data_quality_stats(horses_df, '馬プロフィール+血統', [
                'horse_name', 'sex', 'birth_date', 'trainer_name',
                'sire_id', 'dam_id'
            ])

    def _parse_horse_performance(self):
        """馬過去成績のパース"""
        print(f"\n【4/4】馬過去成績のパース (horse_info_parser.py)")

        perf_files = list(self.horse_bin_dir.glob('*_perf.bin'))
        self.stats['performance']['total'] = len(perf_files)

        performance_results = []

        for i, perf_file in enumerate(perf_files):
            horse_id = perf_file.stem.replace('_perf', '')

            try:
                df = parse_horse_performance_html(str(perf_file), horse_id)

                if not df.empty:
                    performance_results.append(df)
                    print(f"  [{i+1}/{len(perf_files)}] {horse_id} - ✓ {len(df)}走")
                    self.stats['performance']['success'] += 1
                else:
                    print(f"  [{i+1}/{len(perf_files)}] {horse_id} - データなし")
                    self.stats['performance']['failed'] += 1

            except Exception as e:
                print(f"  [{i+1}/{len(perf_files)}] {horse_id} - エラー: {e}")
                self.stats['performance']['failed'] += 1
                continue

        if performance_results:
            perf_df = pd.concat(performance_results, ignore_index=True)
            output_file = self.output_dir / 'horses_performance.csv'
            perf_df.to_csv(output_file, index=False, encoding='utf-8-sig')

            print(f"\n  [✓] horses_performance.csv: {len(perf_df)}行, {len(perf_df.columns)}カラム")

            # 品質統計
            self._print_data_quality_stats(perf_df, '馬過去成績', [
                'race_date', 'venue', 'race_name', 'finish_position',
                'jockey_name', 'horse_weight'
            ])

    def _print_data_quality_stats(self, df: pd.DataFrame, name: str, key_columns: List[str]):
        """データ品質統計を表示"""
        print(f"\n  【{name} 品質統計】")

        for col in key_columns:
            if col in df.columns:
                missing_count = df[col].isna().sum()
                missing_pct = (missing_count / len(df)) * 100
                status = "✓" if missing_pct == 0 else ("⚠" if missing_pct < 10 else "✗")
                print(f"    {status} {col}: {missing_count}/{len(df)} 欠損 ({missing_pct:.1f}%)")
            else:
                print(f"    - {col}: カラムなし")

    def _print_statistics(self):
        """統計サマリーを表示"""
        print(f"\n{'='*70}")
        print(f"  【パース統計サマリー】")
        print(f"{'='*70}")

        for name, stats in self.stats.items():
            if stats['total'] > 0:
                success_rate = (stats['success'] / stats['total']) * 100
                print(f"  {name:15s}: {stats['success']:3d}/{stats['total']:3d} 成功 ({success_rate:5.1f}%)")


def main():
    """メイン実行処理"""
    parser = argparse.ArgumentParser(
        description='包括的パーサーテストパイプライン (test/test_output準拠版)'
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
    parser.add_argument(
        '--max-horses',
        type=int,
        default=DEFAULT_MAX_HORSES,
        help=f'スクレイピング時の馬データ取得上限（デフォルト: {DEFAULT_MAX_HORSES}）'
    )

    args = parser.parse_args()

    # パイプライン実行
    pipeline = ComprehensivePipeline(
        target_date=args.date,
        output_dir=args.output_dir,
        bin_dir=args.bin_dir,
        parse_only=args.parse_only,
        max_horses=args.max_horses
    )

    pipeline.run()


if __name__ == "__main__":
    main()
