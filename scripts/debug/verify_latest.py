import pandas as pd
from pathlib import Path

# 最新ファイルを自動検出
base_dir = Path("keibaai/data/features/parquet/year=2024/month=1")

if not base_dir.exists():
    print(f"ディレクトリが見つかりません: {base_dir}")
else:
    files = sorted(base_dir.glob("*.parquet"), key=lambda x: x.stat().st_mtime, reverse=True)
    
    if not files:
        print(f"Parquetファイルが見つかりません: {base_dir}")
    else:
        file_path = files[0]
        print(f"最新ファイル: {file_path.name}")
        print(f"更新時刻: {pd.Timestamp.fromtimestamp(file_path.stat().st_mtime)}\n")
        
        df = pd.read_parquet(file_path)
        print(f"行数: {len(df):,}\n")
        
        # past_finish_position_mean系をチェック
        past_finish_cols = [c for c in df.columns if 'past_' in c and 'finish_position_mean' in c]
        
        print(f"過去着順平均特徴量: {len(past_finish_cols)}個\n")
        
        for col in sorted(past_finish_cols):
            data = df[col]
            zeros = (data == 0).sum()
            nans = data.isna().sum()
            non_zero = ((data != 0) & (data.notna())).sum()
            
            print(f"{col}:")
            print(f"  ゼロ: {zeros:,} ({zeros/len(df):.2%})")
            print(f"  NaN: {nans:,} ({nans/len(df):.2%})")
            print(f"  非ゼロ有効値: {non_zero:,} ({non_zero/len(df):.2%})")
            if non_zero > 0:
                valid_data = data[data!=0]
                print(f"  平均: {valid_data.mean():.3f}, 最小: {valid_data.min():.1f}, 最大: {valid_data.max():.1f}")
            print()
