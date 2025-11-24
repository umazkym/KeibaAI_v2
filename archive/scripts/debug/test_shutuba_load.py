"""
shutuba.parquetから2020-03-28のデータをロードできるかテスト
"""
import sys
from pathlib import Path
import pandas as pd
import pyarrow.dataset as ds
from datetime import datetime

# プロジェクトルート設定
script_dir = Path(__file__).resolve().parent
project_root = script_dir
sys.path.insert(0, str(project_root / 'keibaai/src'))

from utils.data_utils import load_parquet_data_by_date

# テスト
shutuba_dir = project_root / 'keibaai/data/parsed/parquet/shutuba'
start_dt = datetime(2020, 3, 28)
end_dt = datetime(2020, 3, 28)

print(f"テスト: {shutuba_dir} から {start_dt.date()} のデータをロード")
print(f"ディレクトリ存在: {shutuba_dir.exists()}")

# 直接PyArrowでロードしてみる
print("\n=== PyArrow Datasetでのロード ===")
try:
    files = list(shutuba_dir.rglob("*.parquet"))
    print(f"ファイル数: {len(files)}")
    
    dataset = ds.dataset(files, format="parquet", partitioning="hive")
    print(f"スキーマ: {dataset.schema}")
    
    # フィルタなしで一部ロード
    table = dataset.to_table()
    df_all = table.to_pandas()
    print(f"総行数: {len(df_all)}")
    print(f"race_date型: {df_all['race_date'].dtype}")
    print(f"race_date範囲: {df_all['race_date'].min()} ～ {df_all['race_date'].max()}")
    
    # 3/28のデータを確認
    df_all['race_date'] = pd.to_datetime(df_all['race_date'])
    df_0328 = df_all[df_all['race_date'] == '2020-03-28']
    print(f"\n2020-03-28のデータ: {len(df_0328)}行")
    
except Exception as e:
    print(f"PyArrowエラー: {e}")
    import traceback
    traceback.print_exc()

# load_parquet_data_by_date を使う
print("\n=== load_parquet_data_by_date でのロード ===")
try:
    df = load_parquet_data_by_date(shutuba_dir, start_dt, end_dt, date_col='race_date')
    print(f"ロード成功: {len(df)}行")
    if not df.empty:
        print(f"race_date範囲: {df['race_date'].min()} ～ {df['race_date'].max()}")
except Exception as e:
    print(f"load_parquet_data_by_dateエラー: {e}")
    import traceback
    traceback.print_exc()
