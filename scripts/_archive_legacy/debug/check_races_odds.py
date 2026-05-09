import pandas as pd
from pathlib import Path

# racesデータを確認
races_path = Path('keibaai/data/parsed/parquet/races/races.parquet')

if races_path.exists():
    print("=== races データ確認 ===")
    df = pd.read_parquet(races_path)
    print(f"行数: {len(df)}")
    print(f"\n全カラム一覧 ({len(df.columns)}個):")
    for i, col in enumerate(df.columns):
        print(f"  {i+1}. {col}")
    
    # オッズ関連カラムを探す
    odds_cols = [c for c in df.columns if 'odds' in c.lower() or 'オッズ' in c or 'win' in c.lower() or 'pay' in c.lower()]
    print(f"\nオッズ/配当関連カラム: {odds_cols}")
    
    if odds_cols:
        for col in odds_cols:
            sample_vals = df[col].dropna().head(5).tolist()
            print(f"\n{col}:")
            print(f"  非null数: {df[col].notna().sum()}/{len(df)}")
            print(f"  サンプル値: {sample_vals}")
    
    # race_idでグループ化して、1レースあたりの行数を確認
    print(f"\nrace_idでグループ化:")
    print(f"  ユニークなrace_id数: {df['race_id'].nunique()}")
    print(f"  1レースあたりの平均行数: {len(df) / df['race_id'].nunique():.1f}")
    
    # horse_idやfinish_positionのようなカラムがあるか確認
    entry_cols = [c for c in df.columns if 'horse' in c.lower() or 'finish' in c.lower() or 'position' in c.lower()]
    print(f"\n馬・着順関連カラム: {entry_cols}")
else:
    print("races.parquetが見つかりません")
