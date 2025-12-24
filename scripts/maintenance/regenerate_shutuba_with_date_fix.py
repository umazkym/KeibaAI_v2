"""
shutuba.parquetを全期間（2014-2025）で再生成
races.parquetから日付をマージすることで日付抽出問題を解決
"""
import sys
from pathlib import Path
import pandas as pd
from tqdm import tqdm

# パス設定
project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(project_root))

from keibaai.src.parsers import shutuba_parser

def main():
    html_dir = project_root / 'keibaai' / 'data' / 'raw' / 'html' / 'shutuba'
    output_dir = project_root / 'keibaai' / 'data' / 'parsed' / 'parquet' / 'shutuba'
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # races.parquetから日付情報を取得
    races_path = project_root / 'keibaai' / 'data' / 'parsed' / 'parquet' / 'races' / 'races.parquet'
    races = pd.read_parquet(races_path)
    race_dates = races[['race_id', 'race_date']].drop_duplicates('race_id')
    print(f"races.parquetから{len(race_dates):,}件の日付情報を取得")
    
    # 全てのHTMLファイルを取得
    html_files = sorted(html_dir.glob('*.bin'))
    print(f"HTMLファイル数: {len(html_files):,}")
    
    all_dfs = []
    errors = 0
    
    for f in tqdm(html_files, desc="パース中"):
        try:
            df = shutuba_parser.parse_shutuba_html(str(f))
            if df is not None and not df.empty:
                all_dfs.append(df)
        except Exception as e:
            errors += 1
    
    print(f"\n成功: {len(all_dfs):,}件, エラー: {errors}件")
    
    if all_dfs:
        final_df = pd.concat(all_dfs, ignore_index=True)
        
        # 日付がNoneの行を抽出
        null_dates_before = final_df['race_date'].isna().sum()
        print(f"日付がNULLの行: {null_dates_before:,}/{len(final_df):,}")
        
        # races.parquetから日付をマージ
        if null_dates_before > 0:
            print("races.parquetから日付をマージ中...")
            final_df = final_df.drop(columns=['race_date'])
            final_df = final_df.merge(race_dates, on='race_id', how='left')
            null_dates_after = final_df['race_date'].isna().sum()
            print(f"マージ後のNULL: {null_dates_after:,}/{len(final_df):,}")
        
        print(f"合計レコード: {len(final_df):,}")
        print(f"race_id範囲: {final_df['race_id'].min()} ~ {final_df['race_id'].max()}")
        
        # 保存
        output_path = output_dir / 'shutuba.parquet'
        final_df.to_parquet(output_path, index=False)
        print(f"\n保存完了: {output_path}")

if __name__ == '__main__':
    import logging
    logging.disable(logging.WARNING)  # 警告を抑制
    main()
