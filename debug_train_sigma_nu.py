import sys
sys.path.insert(0, 'keibaai/src')

import pandas as pd
from pathlib import Path

print("=== train_sigma_nu_models.py デバッグ ===\n")

# μ予測データをロード
mu_predictions_path = Path('keibaai/data/predictions/parquet/mu_predictions.parquet')
mu_df = pd.read_parquet(mu_predictions_path)

print(f"μ予測データ:")
print(f"  Shape: {mu_df.shape}")
print(f"  Columns: {list(mu_df.columns)}")
print(f"  Index: {mu_df.index}")
print(f"  horse_id unique: {mu_df['horse_id'].nunique()}")
print(f"  horse_id duplicates: {mu_df['horse_id'].duplicated().sum()}")

print(f"\nSample:")
print(mu_df.head())

# mu_seriesを作成（horse_id → mu のマッピング）
print(f"\n=== mu_series 作成テスト ===")
try:
    # 方法1: set_index（重複があるとエラー）
    mu_series = mu_df.set_index('horse_id')['mu']
    print(f"✓ set_index成功")
    print(f"  Series index unique: {mu_series.index.is_unique}")
except Exception as e:
    print(f"✗ set_index失敗: {e}")
    
    # 方法2: groupbyで平均を取る（重複対策）
    mu_series_alt = mu_df.groupby('horse_id')['mu'].mean()
    print(f"\n代替方法（groupby mean）:")
    print(f"  Series length: {len(mu_series_alt)}")
    print(f"  Sample: {mu_series_alt.head()}")
