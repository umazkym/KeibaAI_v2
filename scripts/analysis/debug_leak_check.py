#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
リーク検証の詳細調査スクリプト

検証失敗の原因を調査する
"""

import pandas as pd
import numpy as np
from pathlib import Path
import warnings
warnings.filterwarnings('ignore')

BASE_PATH = Path(r"c:\Users\zk-ht\Keiba\Keiba_AI_v2\keibaai\data\parsed\parquet")

def main():
    print("=" * 80)
    print("リーク検証の詳細調査")
    print("=" * 80)
    
    # データ読み込み
    print("\nデータ読み込み中...")
    races = pd.read_parquet(BASE_PATH / "races" / "races.parquet")
    
    races['race_date'] = pd.to_datetime(races['race_date'])
    races['is_win'] = (races['finish_position'] == 1).astype(int)
    
    # 時系列でソート
    races = races.sort_values(['horse_id', 'race_date']).reset_index(drop=True)
    
    print(f"レースデータ: {len(races):,}件")
    print(f"ユニーク馬数: {races['horse_id'].nunique():,}頭")
    
    # 累積統計を計算
    print("\n累積統計を計算中...")
    races['horse_cumwins'] = races.groupby('horse_id')['is_win'].cumsum().shift(1).fillna(0)
    races['horse_cumraces'] = races.groupby('horse_id').cumcount()
    races['horse_prev_winrate'] = races['horse_cumwins'] / races['horse_cumraces'].replace(0, np.nan)
    
    races['prev_finish'] = races.groupby('horse_id')['finish_position'].shift(1)
    
    # 各馬の最初のレースを抽出
    print("\n各馬の最初のレースを確認...")
    first_races = races.groupby('horse_id').first().reset_index()
    
    print(f"\n最初のレース数: {len(first_races):,}件")
    
    # 最初のレースでの統計値を確認
    print("\n【最初のレースでの統計値】")
    
    print(f"\nhorse_cumraces (最初のレースは0のはず):")
    print(first_races['horse_cumraces'].value_counts().head(10))
    
    print(f"\nhorse_prev_winrate (最初のレースはNaNのはず):")
    print(f"  NaN数: {first_races['horse_prev_winrate'].isna().sum():,}")
    print(f"  非NaN数: {first_races['horse_prev_winrate'].notna().sum():,}")
    print(f"  NaN率: {first_races['horse_prev_winrate'].isna().mean()*100:.2f}%")
    
    # 非NaNの例を確認
    non_nan_first = first_races[first_races['horse_prev_winrate'].notna()]
    if len(non_nan_first) > 0:
        print(f"\n  【問題】最初のレースなのに過去勝率があるケース: {len(non_nan_first)}件")
        print("  サンプル:")
        for _, row in non_nan_first.head(5).iterrows():
            print(f"    horse_id={row['horse_id']}, cumraces={row['horse_cumraces']}, winrate={row['horse_prev_winrate']:.4f}")
    
    print(f"\nprev_finish (最初のレースはNaNのはず):")
    print(f"  NaN数: {first_races['prev_finish'].isna().sum():,}")
    print(f"  非NaN数: {first_races['prev_finish'].notna().sum():,}")
    print(f"  NaN率: {first_races['prev_finish'].isna().mean()*100:.2f}%")
    
    # 非NaNの例を確認
    non_nan_prev = first_races[first_races['prev_finish'].notna()]
    if len(non_nan_prev) > 0:
        print(f"\n  【問題】最初のレースなのに前走着順があるケース: {len(non_nan_prev)}件")
    
    # 具体的な馬で確認
    print("\n" + "=" * 80)
    print("具体的な馬のデータを確認")
    print("=" * 80)
    
    # サンプル馬を抽出（複数レースがある馬）
    horse_counts = races.groupby('horse_id').size()
    multi_race_horses = horse_counts[horse_counts >= 5].index[:3]
    
    for horse_id in multi_race_horses:
        horse_data = races[races['horse_id'] == horse_id].head(5)
        print(f"\n--- horse_id: {horse_id} ---")
        print(horse_data[['race_date', 'finish_position', 'is_win', 'horse_cumraces', 'horse_cumwins', 'horse_prev_winrate', 'prev_finish']].to_string(index=False))

if __name__ == "__main__":
    main()
