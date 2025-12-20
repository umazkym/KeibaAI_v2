"""
shutuba.parquetを全期間（2014-2025）で再生成 - 並列処理版
"""
import sys
from pathlib import Path
import pandas as pd
from concurrent.futures import ProcessPoolExecutor, as_completed
from tqdm import tqdm
import multiprocessing

# パス設定
project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(project_root))

def parse_single_file(file_path):
    """1ファイルをパース（ワーカープロセス用）"""
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
    from keibaai.src.modules.parsers import shutuba_parser
    
    try:
        df = shutuba_parser.parse_shutuba_html(str(file_path))
        if df is not None and not df.empty:
            return df.to_dict('records')
    except Exception as e:
        pass
    return None

def main():
    from keibaai.src.modules.parsers import shutuba_parser
    
    html_dir = project_root / 'keibaai' / 'data' / 'raw' / 'html' / 'shutuba'
    output_dir = project_root / 'keibaai' / 'data' / 'parsed' / 'parquet' / 'shutuba'
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # 全てのHTMLファイルを取得
    html_files = sorted(html_dir.glob('*.bin'))
    print(f"HTMLファイル数: {len(html_files):,}")
    print(f"最初: {html_files[0].name}, 最後: {html_files[-1].name}")
    
    # 並列処理
    num_workers = min(8, multiprocessing.cpu_count())
    print(f"ワーカー数: {num_workers}")
    
    all_records = []
    errors = 0
    
    with ProcessPoolExecutor(max_workers=num_workers) as executor:
        futures = {executor.submit(parse_single_file, f): f for f in html_files}
        
        for future in tqdm(as_completed(futures), total=len(html_files), desc="パース中"):
            result = future.result()
            if result:
                all_records.extend(result)
            else:
                errors += 1
    
    print(f"\n成功レコード: {len(all_records):,}, エラー: {errors}")
    
    if all_records:
        final_df = pd.DataFrame(all_records)
        print(f"合計レコード: {len(final_df):,}")
        print(f"race_id範囲: {final_df['race_id'].min()} ~ {final_df['race_id'].max()}")
        
        # 保存
        output_path = output_dir / 'shutuba.parquet'
        final_df.to_parquet(output_path, index=False)
        print(f"\n保存完了: {output_path}")

if __name__ == '__main__':
    main()
