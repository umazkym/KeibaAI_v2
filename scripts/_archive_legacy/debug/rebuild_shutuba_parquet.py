"""
shutuba.parquetの統計情報を完全に削除するため、CSV経由で再構築
"""
import logging
import pandas as pd
from pathlib import Path
import pyarrow as pa
import pyarrow.parquet as pq

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')

def main():
    shutuba_path = Path('keibaai/data/parsed/parquet/shutuba/shutuba.parquet')
    csv_path = Path('keibaai/data/parsed/parquet/shutuba/shutuba_temp.csv')
    new_parquet_path = Path('keibaai/data/parsed/parquet/shutuba/shutuba_new.parquet')
    
    logging.info(f"shutuba.parquetをロード: {shutuba_path}")
    shutuba = pd.read_parquet(shutuba_path)
    logging.info(f"  総行数: {len(shutuba):,}行")
    logging.info(f"  race_date型: {shutuba['race_date'].dtype}")
    
    # 念のため型確認
    if shutuba['race_date'].dtype != 'datetime64[ns]':
        logging.warning("race_dateがdatetime型ではありません。変換します。")
    # 新しいParquetファイルとして保存（統計情報なし）
    logging.info(f"\\nステップ4: 新しいParquetファイルとして保存: {new_parquet_path}")
    table = pa.Table.from_pandas(shutuba_clean, preserve_index=False)
    
    pq.write_table(
        table,
        new_parquet_path,
        compression='snappy',
        write_statistics=False,  # 統計情報を無効化
        use_dictionary=True,
        version='2.6'
    )
    logging.info("✅ Parquet保存完了")
    
    # 元ファイルをバックアップして置き換え
    logging.info(f"\\nステップ5: 元ファイルを置き換え")
    backup_path = shutuba_path.parent / f"{shutuba_path.stem}_backup.parquet"
    
    if backup_path.exists():
        logging.warning(f"バックアップファイル既存: {backup_path}")
        backup_path.unlink()
    
    shutuba_path.rename(backup_path)
    logging.info(f"  バックアップ: {backup_path}")
    
    new_parquet_path.rename(shutuba_path)
    logging.info(f"  新ファイル配置: {shutuba_path}")
    
    # CSV削除
    csv_path.unlink()
    logging.info(f"  中間CSV削除: {csv_path}")
    
    logging.info("\\n✅ 完了")
    logging.info(f"  元ファイル: {backup_path} (バックアップ)")
    logging.info(f"  新ファイル: {shutuba_path}")
    
    # 確認
    logging.info("\\n最終確認:")
    test_df = pd.read_parquet(shutuba_path)
    logging.info(f"  行数: {len(test_df):,}行")
    logging.info(f"  race_date型: {test_df['race_date'].dtype}")
    logging.info(f"  Null数: {test_df['race_date'].isna().sum():,}行")
    logging.info(f"  最小日付: {test_df['race_date'].min()}")
    logging.info(f"  最大日付: {test_df['race_date'].max()}")

if __name__ == '__main__':
    main()
