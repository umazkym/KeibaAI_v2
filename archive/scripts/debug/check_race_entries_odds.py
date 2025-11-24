import pandas as pd
from pathlib import Path

# race_entriesデータを確認
entries_path = Path('keibaai/data/parsed/parquet/race_entries')

if entries_path.exists():
    print("=== race_entries データ確認 ===")
    parquet_files = list(entries_path.glob('*.parquet'))
    if parquet_files:
        df = pd.read_parquet(parquet_files[0])
        print(f"行数: {len(df)}")
        print(f"\nカラム一覧:")
        for col in df.columns:
            print(f"  - {col}")
        
        # オッズ関連カラムを探す
        odds_cols = [c for c in df.columns if 'odds' in c.lower() or 'オッズ' in c]
        print(f"\nオッズ関連カラム: {odds_cols}")
        
        if odds_cols:
            for col in odds_cols:
                print(f"\n{col}の統計:")
                print(f"  非null数: {df[col].notna().sum()}")
                print(f"  サンプル値: {df[col].dropna().head(5).tolist()}")
        
        # 着順と馬IDも確認
        if 'finish_position' in df.columns:
            print(f"\n1着の数: {(df['finish_position'] == 1).sum()}")
        
        print(f"\nサンプルデータ（最初の2行）:")
        print(df.head(2))
    else:
        print("Parquetファイルが見つかりません")
else:
    print("race_entriesディレクトリが見つかりません")
