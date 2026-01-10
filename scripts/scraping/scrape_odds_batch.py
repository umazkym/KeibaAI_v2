# -*- coding: utf-8 -*-
"""
過去オッズデータの一括取得・パーススクリプト

使用方法:
    # HTML取得（.bin保存）
    python scripts/scraping/scrape_odds_batch.py scrape --start 2024 --end 2024
    
    # パース処理（.bin→.json）
    python scripts/scraping/scrape_odds_batch.py parse --start 2024 --end 2024
    
    # 全処理（取得→パース）
    python scripts/scraping/scrape_odds_batch.py all --start 2024 --end 2024
    
オプション:
    --include-trio        三連複を含める（デフォルト: 含める）
    --include-trifecta    三連単を含める（デフォルト: 含めない、時間がかかるため）
    --no-skip             既存ファイルを再取得/再パース
"""
import sys
import logging
import argparse
from pathlib import Path
from tqdm import tqdm

# プロジェクトルートをパスに追加
PROJECT_ROOT = Path(__file__).parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

def setup_logging():
    """ログ設定を行う"""
    from datetime import datetime
    import sys
    
    # ログディレクトリ設定 (data/logs/YYYY/MM/DD/scrape_odds.log)
    now = datetime.now()
    log_dir = PROJECT_ROOT / 'keibaai' / 'data' / 'logs' / f"{now.year}" / f"{now.month:02}" / f"{now.day:02}"
    log_dir.mkdir(parents=True, exist_ok=True)
    
    log_file = log_dir / 'scrape_odds.log'
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(log_file, encoding='utf-8')
        ]
    )

setup_logging()
logger = logging.getLogger(__name__)

# ディレクトリ設定（既存パイプラインと同様の構造）
RAW_ODDS_DIR = PROJECT_ROOT / 'keibaai' / 'data' / 'raw' / 'html' / 'odds'
PARSED_ODDS_DIR = PROJECT_ROOT / 'keibaai' / 'data' / 'parsed' / 'parquet' / 'odds'


def get_race_ids_from_parquet(start_year: int = None, end_year: int = None, from_date: str = None, to_date: str = None) -> list:
    """
    Parquetからrace_idリストを取得
    
    Args:
        start_year: 開始年 (Optional)
        end_year: 終了年 (Optional)
        from_date: 開始日付 (YYYY-MM-DD or YYYYMMDD, Optional)
        to_date: 終了日付 (YYYY-MM-DD or YYYYMMDD, Optional)
    """
    import pandas as pd
    
    parquet_path = PROJECT_ROOT / 'keibaai' / 'data' / 'parsed' / 'parquet' / 'races' / 'races.parquet'
    
    if not parquet_path.exists():
        logger.error(f"Parquetファイルが見つかりません: {parquet_path}")
        return []
    
    df = pd.read_parquet(parquet_path)
    
    # 日付フィルタリング
    if from_date or to_date:
        # dateカラムまたはrace_dateカラムを確認
        date_col = 'race_date' if 'race_date' in df.columns else 'date'
        
        if date_col in df.columns:
            # datetime型に変換（エラー無視）
            df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
            
            if from_date:
                s_date = pd.to_datetime(from_date)
                df = df[df[date_col] >= s_date]
            
            if to_date:
                e_date = pd.to_datetime(to_date)
                df = df[df[date_col] <= e_date]
        else:
            logger.warning("race_date/dateカラムが存在しないため、日付フィルタリングをスキップします")
            # 年でフォールバック
            if start_year is not None and end_year is not None:
                df['year'] = df['race_id'].astype(str).str[:4].astype(int)
                df = df[(df['year'] >= start_year) & (df['year'] <= end_year)]
    
    # 年フィルタリング（日付指定がない場合、または日付カラムがない場合）
    elif start_year is not None and end_year is not None:
        df['year'] = df['race_id'].astype(str).str[:4].astype(int)
        df = df[(df['year'] >= start_year) & (df['year'] <= end_year)]
    
    race_ids = df['race_id'].astype(str).unique().tolist()
    race_ids.sort()
    
    filter_info = []
    if start_year is not None and end_year is not None:
        filter_info.append(f"年: {start_year}-{end_year}")
    if from_date is not None and to_date is not None:
        filter_info.append(f"日付: {from_date}～{to_date}")
    elif from_date is not None:
        filter_info.append(f"日付: {from_date}以降")
    elif to_date is not None:
        filter_info.append(f"日付: {to_date}以前")

    logger.info(f"race_id取得完了: {len(race_ids)}件 ({', '.join(filter_info) if filter_info else '全期間'})")
    
    # 重複除去
    return sorted(list(set(race_ids)))


def cmd_scrape(args):
    """HTML取得コマンド"""
    from keibaai.src.preparing._scrape_odds_html import scrape_odds_html_batch
    
    validate_args(args)
    
    if args.race_id:
        race_ids = [args.race_id]
        print(f"=== 指定レース: {args.race_id} ===")
    else:
        race_ids = get_race_ids_from_parquet(args.start, args.end, args.from_date, args.to_date)
    
    if not race_ids:
        return
    
    print(f"=== スクレイピング開始: {len(race_ids)}レース ===")
    
    stats = scrape_odds_html_batch(
        race_ids=race_ids,
        output_base_dir=RAW_ODDS_DIR,
        skip_existing=not args.force,
        include_trio=args.include_trio,
        include_trifecta=args.include_trifecta
    )
    
    print("\n=== 完了 ===")
    for bt, count in stats.items():
        if count > 0:
            print(f"  {bt}: {count}件")


def cmd_parse(args):
    """パースコマンド"""
    from keibaai.src.parsers._parse_odds import parse_all_odds_for_race
    
    validate_args(args)

    if args.race_id:
        race_ids = [args.race_id]
    else:
        race_ids = get_race_ids_from_parquet(args.start, args.end, args.from_date, args.to_date)

    if not race_ids:
        return
    
    print(f"=== パース開始: {len(race_ids)}レース ===")
    
    success_count = 0
    total_files = 0
    
    for race_id in tqdm(race_ids):
        # 既存チェック (skip_existing=True かつ force=False の場合)
        if not args.force:
            # 代表として単勝複勝ファイルの存在を確認
            check_path = PARSED_ODDS_DIR / 'tansho_fukusho' / f"{race_id}.parquet"
            if check_path.exists():
                continue

        result = parse_all_odds_for_race(race_id, RAW_ODDS_DIR, PARSED_ODDS_DIR)
        item_count = sum(1 for k in result.keys() if k != 'metadata')
        if item_count > 0:
            success_count += 1
            total_files += item_count
            
    print(f"\n=== 完了 ===")
    print(f"  処理レース数: {success_count}/{len(race_ids)}")
    print(f"  生成Parquet数: {total_files}")


def cmd_all(args):
    """全処理コマンド"""
    validate_args(args)
    print("=== Phase 1: HTML取得 ===")
    cmd_scrape(args)
    
    print("\n=== Phase 2: パース処理 ===")
    cmd_parse(args)


def cmd_test(args):
    """テストコマンド（少数レースで動作確認）"""
    from keibaai.src.preparing._scrape_odds_html import scrape_odds_html_batch
    from keibaai.src.parsers._parse_odds import parse_all_odds_for_race
    
    # 特定のレースIDが指定された場合
    if args.race_id:
        test_race_ids = [args.race_id]
        test_count = 1
    else:
        # 2024年のレースから少数を取得
        race_ids = get_race_ids_from_parquet(2024, 2024)
        if not race_ids:
            print("テスト対象のレースIDがありません")
            return
        
        test_count = min(args.count, len(race_ids))
        test_race_ids = race_ids[:test_count]
    
    print(f"\n=== テスト実行: {test_count}レース ===")
    print(f"対象: {test_race_ids}")
    
    # HTML取得（全券種）
    print("\n--- Phase 1: HTML取得 ---")
    stats = scrape_odds_html_batch(
        race_ids=test_race_ids,
        output_base_dir=RAW_ODDS_DIR,
        skip_existing=False,
        include_trio=True,
        include_trifecta=True  # 三連単も含める
    )
    
    print("\nHTML取得結果:")
    for bt, count in stats.items():
        print(f"  {bt}: {count}件")
    
    # パース
    print("\n--- Phase 2: パース ---")
    for race_id in test_race_ids:
        result = parse_all_odds_for_race(race_id, RAW_ODDS_DIR, PARSED_ODDS_DIR)
        print(f"  {race_id}: {len([k for k in result.keys() if k != 'metadata'])}券種")
    
    print("\n=== テスト完了 ===")
    print(f"HTML保存先: {RAW_ODDS_DIR}")
    print(f"パース結果: {PARSED_ODDS_DIR}")


    print(f"HTML保存先: {RAW_ODDS_DIR}")
    print(f"パース結果: {PARSED_ODDS_DIR}")


def validate_args(args):
    """引数のバリデーション"""
    if not (args.start and args.end) and not (args.from_date or args.to_date) and not hasattr(args, 'race_id'):
        print("エラー: 年範囲 (--start, --end) または日付範囲 (--from-date, --to-date) を指定してください")
        sys.exit(1)
    
    if hasattr(args, 'race_id') and args.race_id:
        return

    if (args.start and not args.end) or (not args.start and args.end):
         print("エラー: 年範囲は --start と --end の両方を指定してください")
         sys.exit(1)
    
    if (args.from_date and not args.to_date) or (not args.from_date and args.to_date):
         print("エラー: 日付範囲は --from-date と --to-date の両方を指定してください")
         sys.exit(1)


def main():
    parser = argparse.ArgumentParser(description="過去オッズデータの一括取得・パース")
    subparsers = parser.add_subparsers(dest='command', help='コマンド')
    
    # 共通引数を追加する関数
    def add_common_args(p):
        p.add_argument("--start", type=int, help="開始年 (例: 2021)")
        p.add_argument("--end", type=int, help="終了年 (例: 2024)")
        p.add_argument("--from-date", type=str, help="開始日付 (例: 2024-01-01)")
        p.add_argument("--to-date", type=str, help="終了日付 (例: 2024-01-31)")
        p.add_argument("--include-trio", action="store_true", default=True, help="三連複を含める")
        p.add_argument("--include-trifecta", action="store_true", default=True, help="三連単を含める（時間がかかる）")
        p.add_argument("--force", action="store_true", help="既存ファイルを再取得/再パース")
        p.add_argument("--race_id", type=str, help="特定のレースIDを指定 (指定時は他の期間指定を無視)")
    # scrapeコマンド
    scrape_parser = subparsers.add_parser('scrape', help='オッズHTMLを取得')
    add_common_args(scrape_parser)
    
    # parseコマンド
    parse_parser = subparsers.add_parser('parse', help='オッズHTMLをパース')
    add_common_args(parse_parser)
    
    # allコマンド
    all_parser = subparsers.add_parser('all', help='取得→パースを一括実行')
    add_common_args(all_parser)
    
    # テストコマンド（少数レース）
    test_parser = subparsers.add_parser('test', help='少数レースでテスト実行')
    test_parser.add_argument("--count", type=int, default=3, help="テストレース数")
    test_parser.add_argument("--race_id", type=str, help="特定のレースIDを指定")
    
    args = parser.parse_args()
    
    if args.command == 'scrape':
        cmd_scrape(args)
    elif args.command == 'parse':
        cmd_parse(args)
    elif args.command == 'all':
        cmd_all(args)
    elif args.command == 'test':
        cmd_test(args)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
