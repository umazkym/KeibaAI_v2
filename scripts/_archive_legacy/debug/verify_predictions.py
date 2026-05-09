import pandas as pd
import numpy as np
from pathlib import Path

def verify_predictions(file_path):
    print(f"=== 予測データ検証: {file_path} ===\n")
    
    if not Path(file_path).exists():
        print(f"エラー: ファイルが見つかりません: {file_path}")
        return

    df = pd.read_parquet(file_path)
    
    print(f"形状: {df.shape}")
    print(f"カラム: {list(df.columns)}")
    print("-" * 40)
    
    # 1. 基本統計量
    cols_to_check = ['mu', 'sigma', 'nu']
    for col in cols_to_check:
        if col in df.columns:
            print(f"\n【{col} の統計量】")
            print(df[col].describe())
            
            # 異常値チェック
            nan_count = df[col].isna().sum()
            inf_count = np.isinf(df[col]).sum()
            print(f"  欠損値: {nan_count}")
            print(f"  無限大: {inf_count}")
            
            if col in ['sigma', 'nu']:
                default_val_count = (df[col] == 1.0).sum()
                print(f"  デフォルト値(1.0)の数: {default_val_count} / {len(df)} ({default_val_count/len(df):.1%})")
                if default_val_count == len(df):
                    print(f"  ⚠️ 警告: 全ての値がデフォルト値(1.0)です。推論が正しく行われていない可能性があります。")
                else:
                    print(f"  ✅ 正常: 値が変動しています。")
        else:
            print(f"\n⚠️ 警告: カラム {col} が存在しません。")

    # 2. レースIDごとの整合性
    if 'race_id' in df.columns:
        unique_races = df['race_id'].nunique()
        print(f"\nレース数: {unique_races}")
        print(f"1レースあたりの平均馬数: {len(df) / unique_races:.1f}")
    
    print("\n" + "=" * 40)

if __name__ == "__main__":
    verify_predictions('keibaai/data/predictions/parquet/predictions_2024_full.parquet')
