"""
shutuba.parquetを全期間（2014-2025）で再生成するスクリプト
"""
import sys
from pathlib import Path
import pandas as pd
from tqdm import tqdm

# パス設定
project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(project_root))

from keibaai.src.modules.parsers import shutuba_parser

def main():
    html_dir = project_root / 'keibaai' / 'data' / 'raw' / 'html' / 'shutuba'
    output_dir = project_root / 'keibaai' / 'data' / 'parsed' / 'parquet' / 'shutuba'
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # 全てのHTMLファイルを取得
    html_files = sorted(html_dir.glob('*.bin'))
    print(f"HTMLファイル数: {len(html_files):,}")
    print(f"最初のファイル: {html_files[0].name if html_files else 'なし'}")
    print(f"最後のファイル: {html_files[-1].name if html_files else 'なし'}")
    
    all_dfs = []
    errors = 0
    
    for f in tqdm(html_files, desc="パース中"):
        try:
            df = shutuba_parser.parse_shutuba_html(str(f))
            if df is not None and not df.empty:
                all_dfs.append(df)
        except Exception as e:
            errors += 1
            if errors <= 5:
                print(f"\nエラー: {f.name}: {e}")
    
    print(f"\n成功: {len(all_dfs):,}件, エラー: {errors}件")
    
    if all_dfs:
        final_df = pd.concat(all_dfs, ignore_index=True)
        print(f"合計レコード: {len(final_df):,}")
        print(f"race_id範囲: {final_df['race_id'].min()} ~ {final_df['race_id'].max()}")
        
        # 保存
        output_path = output_dir / 'shutuba.parquet'
        final_df.to_parquet(output_path, index=False)
        print(f"\n保存完了: {output_path}")

if __name__ == '__main__':
    main()
