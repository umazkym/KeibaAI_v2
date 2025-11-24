#!/usr/bin/env python3
"""
2024年のレース結果データのみを再パースして horses_performance.parquet を更新するスクリプト
"""

import logging
import sqlite3
from pathlib import Path
import sys
import pandas as pd
from tqdm import tqdm

# プロジェクトルートをsys.pathに追加
project_root = Path(__file__).resolve().parent
sys.path.append(str(project_root))

from keibaai.src import pipeline_core
from keibaai.src.modules.parsers import results_parser

# ロギング設定
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()]
)
log = logging.getLogger(__name__)

def main():
    log.info("="*80)
    log.info("2024年レース結果データの再パース開始")
    log.info("="*80)
    
    # パスの設定
    raw_html_dir = Path('keibaai/data/raw/html/race')
    parsed_output_dir = Path('keibaai/data/parsed/parquet/horses_performance')
    parsed_output_dir.mkdir(parents=True, exist_ok=True)
    
    db_path = Path('keibaai/data/metadata/db.sqlite3')
    conn = sqlite3.connect(db_path)
    
    # 2024年のHTMLファイルのみを取得
    # ファイル名形式: 202401010101.bin (YYYYMMDDRRGG)
    race_html_files = []
    for file_path in raw_html_dir.glob("2024*.bin"):
        race_html_files.append(file_path)
    
    race_html_files.sort()  # ソート
    
    log.info(f"2024年のレース結果HTMLファイル: {len(race_html_files):,}件")
    
    if not race_html_files:
        log.error("2024年のHTMLファイルが見つかりません！")
        return
    
    # パース処理
    all_results_df = []
    for html_file in tqdm(race_html_files, desc="2024レース結果パース", unit="件"):
        df = pipeline_core.parse_with_error_handling(
            str(html_file), 
            "results_parser", 
            results_parser.parse_results_html, 
            conn
        )
        if df is not None and not df.empty:
            all_results_df.append(df)
    
    conn.close()
    
    # データ結合と保存
    if all_results_df:
        final_df = pd.concat(all_results_df, ignore_index=True)
        
        # 既存の2023年以前のデータを読み込む
        existing_parquet = parsed_output_dir / "horses_performance.parquet"
        if existing_parquet.exists():
            log.info("既存のhorses_performance.parquetを読み込み中...")
            existing_df = pd.read_parquet(existing_parquet)
            
            # 2024年のデータを削除（置き換え）
            existing_df = existing_df[existing_df['race_date'] < '2024-01-01']
            log.info(f"  既存データ（2023年以前）: {len(existing_df):,}レコード")
            
            # 新しい2024年データと結合
            combined_df = pd.concat([existing_df, final_df], ignore_index=True)
        else:
            combined_df = final_df
        
        # 保存
        output_path = parsed_output_dir / "horses_performance.parquet"
        combined_df.to_parquet(output_path, index=False)
        
        log.info("="*80)
        log.info(f"✓ 保存完了: {output_path}")
        log.info(f"✓ 2024年の新規レコード: {len(final_df):,}件")
        log.info(f"✓ 総レコード数: {len(combined_df):,}件")
        
        # win_oddsの統計
        odds_count = final_df['win_odds'].notna().sum()
        log.info(f"✓ オッズデータ取得数: {odds_count:,}/{len(final_df):,} ({odds_count/len(final_df)*100:.1f}%)")
        
        if odds_count > 0:
            log.info(f"✓ オッズ統計: 最小={final_df['win_odds'].min():.1f}, 最大={final_df['win_odds'].max():.1f}, 平均={final_df['win_odds'].mean():.1f}")
        
        log.info("="*80)
        log.info("2024年レース結果データの再パース完了！")
        log.info("="*80)
    else:
        log.error("パース処理でデータが取得できませんでした")

if __name__ == "__main__":
    main()
