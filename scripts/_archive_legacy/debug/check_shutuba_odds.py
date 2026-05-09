import pandas as pd
from pathlib import Path

# shutubaデータを確認
shutuba_path = Path('keibaai/data/parsed/parquet/shutuba')

if shutuba_path.exists():
    print("=== shutuba データ確認 ===")
    parquet_file = shutuba_path / 'shutuba.parquet'
    if parquet_file.exists():
        df = pd.read_parquet(parquet_file)
        print(f"行数: {len(df)}")
        print(f"\nカラム一覧:")
        for col in df.columns:
            print(f"  - {col}")
        
        # オッズ関連カラムを探す
        odds_cols = [c for c in df.columns if 'odds' in c.lower() or 'オッズ' in c or 'win' in c.lower()]
        print(f"\nオッズ/勝利関連カラム: {odds_cols}")
        
        if odds_cols:
            for col in odds_cols:
                sample_vals = df[col].dropna().head(10).tolist()
                print(f"\n{col}:")
                print(f"  非null数: {df[col].notna().sum()}")
                print(f"  サンプル値: {sample_vals}")
        
        print(f"\nサンプルデータ（最初の1行、オッズ関連のみ）:")
        if odds_cols:
            print(df[odds_cols].head(1))
    else:
        print("shutuba.parquetが見つかりません")
else:
    print("shutubaディレクトリが見つかりません")
