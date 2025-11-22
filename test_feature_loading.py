#!/usr/bin/env python3
"""最終確認: モデル学習を手動で実行"""
import pandas as pd
from pathlib import Path

# 特徴量パスを確認
features_path = Path('keibaai/data/features/parquet')
print(f"特徴量ディレクトリ: {features_path}")
print(f"存在: {features_path.exists()}")

if features_path.exists():
    # サブディレクトリを確認
    subdirs = list(features_path.glob('year=*'))
    print(f"\nパーティション数: {len(subdirs)}")
    for subdir in subdirs[:5]:
        print(f"  - {subdir.name}")
    
    # pd.read_parquetで読み込みテスト
    try:
        df = pd.read_parquet(features_path)
        print(f"\n✓ 読み込み成功: {len(df)}行, {len(df.columns)}列")
        
        # ターゲット変数チェック
        target_vars = ['finish_position', 'finish_time_seconds', 'prize_money', 'popularity']
        leaked = [v for v in target_vars if v in df.columns]
        if leaked:
            print(f"  ✗ ターゲット変数: {leaked}")
        else:
            print(f"  ✓ ターゲット変数なし")
            
    except Exception as e:
        print(f"\n✗ 読み込み失敗: {e}")
