"""
2024年のレース結果HTMLを再パースするスクリプト
"""
import logging
import sys
from pathlib import Path
import pandas as pd
from tqdm import tqdm
import re

# プロジェクトルートを追加
project_root = Path(__file__).resolve().parent / "keibaai"
sys.path.append(str(project_root))

from src.modules.parsers import results_parser

logging.basicConfig(level=logging.INFO, format='%(message)s')
log = logging.getLogger(__name__)

def extract_year_from_filename(filename):
    """ファイル名から年を抽出"""
    match = re.match(r'(\d{4})', filename)
    return int(match.group(1)) if match else None

def main():
    # パス設定
    raw_race_dir = Path("keibaai/data/raw/html/race")
    output_parquet = Path("keibaai/data/parsed/parquet/races/races.parquet")
    
    if not raw_race_dir.exists():
        log.error(f"ディレクトリが見つかりません: {raw_race_dir}")
        return
    
    # 2024年のHTMLファイルを収集
    race_html_files = []
    for root, _, files in raw_race_dir.walk():
        for file in files:
            if file.endswith((".html", ".bin")):
                year = extract_year_from_filename(file)
                if year == 2024:
                    race_html_files.append(root / file)
    
    log.info(f"2024年のレース結果HTMLファイル: {len(race_html_files)}件")
    
    if not race_html_files:
        log.error("2024年のデータが見つかりません")
        return
    
    # パース実行
    all_results = []
    errors = 0
    
    for html_file in tqdm(race_html_files, desc="パース中"):
        try:
            race_id = html_file.stem  # ファイル名から拡張子を除いたもの
            df = results_parser.parse_results_html(str(html_file), race_id)
            if df is not None and not df.empty:
                all_results.append(df)
        except Exception as e:
            errors += 1
            if errors <= 5:  # 最初の5件だけログに出力
                log.warning(f"エラー: {html_file.name} - {str(e)[:100]}")
    
    if not all_results:
        log.error("パース可能なデータがありませんでした")
        return
    
    # 結合して保存
    final_df = pd.concat(all_results, ignore_index=True)
    
    log.info(f"\n{'='*60}")
    log.info(f"パース結果:")
    log.info(f"  総レコード数: {len(final_df):,}件")
    log.info(f"  エラー数: {errors}件")
    
    # 賞金データを確認
    if 'prize_money' in final_df.columns:
        prize_stats = final_df['prize_money'].describe()
        non_null = final_df['prize_money'].notna().sum()
        non_zero = (final_df['prize_money'] > 0).sum()
        log.info(f"\n賞金データ統計:")
        log.info(f"  非null: {non_null:,} / {len(final_df):,} ({non_null/len(final_df)*100:.1f}%)")
        log.info(f"  非ゼロ: {non_zero:,} / {len(final_df):,} ({non_zero/len(final_df)*100:.1f}%)")
        if non_zero > 0:
            log.info(f"  平均賞金: {final_df[final_df['prize_money']>0]['prize_money'].mean():.1f}万円")
    
    # 既存のparquetと統合する場合
    output_parquet.parent.mkdir(parents=True, exist_ok=True)
    
    if output_parquet.exists():
        log.info(f"\n既存のparquetを読み込み中...")
        existing_df = pd.read_parquet(output_parquet)
        log.info(f"  既存レコード: {len(existing_df):,}件")
        
        # 2024年以外のデータを保持
        non_2024_df = existing_df[~existing_df['race_id'].str.startswith('2024')]
        log.info(f"  2024年以外: {len(non_2024_df):,}件")
        
        # 統合
        final_df = pd.concat([non_2024_df, final_df], ignore_index=True)
        log.info(f"  統合後: {len(final_df):,}件")
    
    # 保存
    final_df.to_parquet(output_parquet, index=False)
    log.info(f"\n✓ 保存完了: {output_parquet}")
    log.info(f"{'='*60}")

if __name__ == "__main__":
    main()
