"""
shutuba.parquetを全期間（2014-2025）で再生成 - 修正版パーサー使用
titleタグから日付を抽出するため、races.parquetへの依存なし
"""
import sys
from pathlib import Path
import pandas as pd
from concurrent.futures import ProcessPoolExecutor, as_completed
from tqdm import tqdm
import multiprocessing
import logging

# 警告を抑制
logging.disable(logging.WARNING)

project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(project_root))

def parse_single_file(file_path):
    """1ファイルをパース"""
    import sys
    from pathlib import Path
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
    import logging
    logging.disable(logging.WARNING)
    
    from keibaai.src.parsers import shutuba_parser
    
    try:
        df = shutuba_parser.parse_shutuba_html(str(file_path))
        if df is not None and not df.empty:
            return df.to_dict('records')
    except:
        pass
    return None

def main():
    html_dir = project_root / 'keibaai' / 'data' / 'raw' / 'html' / 'shutuba'
    output_dir = project_root / 'keibaai' / 'data' / 'parsed' / 'parquet' / 'shutuba'
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # 全てのHTMLファイル
    html_files = sorted(html_dir.glob('*.bin'))
    print(f"HTMLファイル数: {len(html_files):,}")
    
    # 並列処理
    num_workers = min(8, multiprocessing.cpu_count())
    print(f"ワーカー数: {num_workers}")
    
    all_records = []
    
    with ProcessPoolExecutor(max_workers=num_workers) as executor:
        futures = {executor.submit(parse_single_file, f): f for f in html_files}
        
        for future in tqdm(as_completed(futures), total=len(html_files), desc="パース中"):
            result = future.result()
            if result:
                all_records.extend(result)
    
    print(f"\nパース成功レコード: {len(all_records):,}")
    
    if all_records:
        final_df = pd.DataFrame(all_records)
        
        # 日付状況を確認
        null_count = final_df['race_date'].isna().sum()
        print(f"日付NULL数: {null_count:,}/{len(final_df):,}")
        
        print(f"合計レコード: {len(final_df):,}")
        print(f"race_id範囲: {final_df['race_id'].min()} ~ {final_df['race_id'].max()}")
        print(f"race_date範囲: {final_df['race_date'].min()} ~ {final_df['race_date'].max()}")
        
        output_path = output_dir / 'shutuba.parquet'
        final_df.to_parquet(output_path, index=False)
        print(f"\n保存完了: {output_path}")

if __name__ == '__main__':
    main()
