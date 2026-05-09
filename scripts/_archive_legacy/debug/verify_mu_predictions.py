import sys
sys.path.insert(0, 'keibaai/src')

import pandas as pd
from pathlib import Path

print("=== μモデル推論結果の検証 ===\n")

prediction_file = Path('keibaai/data/predictions/parquet/mu_predictions.parquet')

if prediction_file.exists():
    df = pd.read_parquet(prediction_file)
    
    print(f"ファイル: {prediction_file}")
    print(f"行数: {len(df):,}")
    print(f"カラム: {list(df.columns)}")
    print(f"\nカラム検証:")
    print(f"  race_id: {'✓' if 'race_id' in df.columns else '✗'}")
    print(f"  horse_id: {'✓' if 'horse_id' in df.columns else '✗'}")
    print(f"  mu: {'✓' if 'mu' in df.columns else '✗'}")
    print(f"  sigma: {'✓' if 'sigma' in df.columns else '✗'}")
    print(f"  nu: {'✓' if 'nu' in df.columns else '✗'}")
    
    print(f"\nサンプルデータ:")
    print(df.head(5))
    
    if 'race_id' in df.columns:
        print(f"\nrace_id unique: {df['race_id'].nunique()}")
    
    print("\n✅ 検証完了")
else:
    print(f"❌ ファイルが見つかりません: {prediction_file}")
