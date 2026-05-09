import pandas as pd
from pathlib import Path

# horses_performanceデータを確認
perf_path = Path('keibaai/data/parsed/parquet/horses_performance')

if perf_path.exists():
    print("=== horses_performance データ確認 ===")
    parquet_file = perf_path / 'horses_performance.parquet'
    if parquet_file.exists():
        df = pd.read_parquet(parquet_file)
        print(f"行数: {len(df)}")
        print(f"\n全カラム一覧 ({len(df.columns)}個):")
        for i, col in enumerate(df.columns):
            print(f"  {i+1}. {col}")
        
        # オッズ関連カラムを探す
        odds_cols = [c for c in df.columns if 'odds' in c.lower() or 'オッズ' in c or '配当' in c or 'payout' in c.lower() or 'pay' in c.lower()]
        print(f"\nオッズ/配当関連カラム: {odds_cols}")
        
        if odds_cols:
            for col in odds_cols:
                sample_vals = df[col].dropna().head(10).tolist()
                print(f"\n{col}:")
                print(f"  非null数: {df[col].notna().sum()}/{len(df)}")
                print(f"  データ型: {df[col].dtype}")
                print(f"  サンプル値: {sample_vals[:5]}")
                if len(sample_vals) > 0:
                    print(f"  最小値: {df[col].min()}")
                    print(f"  最大値: {df[col].max()}")
                    print(f"  平均値: {df[col].mean():.2f}")
        
        # 着順関連カラム
        finish_cols = [c for c in df.columns if 'finish' in c.lower() or 'position' in c.lower() or '着' in c]
        print(f"\n着順関連カラム: {finish_cols}")
        
        # サンプルデータ
        print(f"\nサンプルデータ（最初の2行）:")
        display_cols = [c for c in df.columns if c in ['race_id', 'horse_id', 'finish_position'] + odds_cols]
        if display_cols:
            print(df[display_cols].head(2))
    else:
        print("horses_performance.parquetが見つかりません")
        # ファイル一覧を表示
        files = list(perf_path.glob('*.parquet'))
        print(f"  利用可能なファイル: {[f.name for f in files]}")
else:
    print("horses_performanceディレクトリが見つかりません")
