"""
cornersデータのマージ問題調査

問題: corners.parquetは99.3%のレースカバレッジがあるのに、
V9ではhorse_position_trendが24.3%しか非NaNにならなかった

原因を調査する
"""

import sys
from pathlib import Path
import pandas as pd
import numpy as np

project_root = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(project_root))

def main():
    data_dir = Path("keibaai/data/parsed/parquet")
    
    print("="*80)
    print("cornersデータのマージ問題調査")
    print("="*80)
    
    # データ読み込み
    races = pd.read_parquet(data_dir / "races/races.parquet")
    races['race_date'] = pd.to_datetime(races['race_date'])
    races = races[races['race_date'] >= '2020-01-01']
    races = races.dropna(subset=['finish_position', 'win_odds'])
    
    corners = pd.read_parquet(data_dir / "corners/corner_positions.parquet")
    
    print(f"\nraces: {len(races):,}件")
    print(f"corners: {len(corners):,}件")
    
    # データ型の確認
    print("\n" + "="*80)
    print("データ型の確認")
    print("="*80)
    
    print(f"\nracesのrace_id型: {races['race_id'].dtype}")
    print(f"cornersのrace_id型: {corners['race_id'].dtype}")
    print(f"racesのhorse_number型: {races['horse_number'].dtype}")
    print(f"cornersのhorse_number型: {corners['horse_number'].dtype}")
    
    # サンプルデータ比較
    print("\n" + "="*80)
    print("サンプルデータ比較")
    print("="*80)
    
    print(f"\nraces.race_idサンプル:")
    print(races['race_id'].head())
    
    print(f"\ncorners.race_idサンプル:")
    print(corners['race_id'].head())
    
    print(f"\nraces.horse_numberサンプル:")
    print(races['horse_number'].head())
    
    print(f"\ncorners.horse_numberサンプル:")
    print(corners['horse_number'].head())
    
    # マージテスト
    print("\n" + "="*80)
    print("マージテスト")
    print("="*80)
    
    # race_idの共通集合
    races_race_ids = set(races['race_id'])
    corners_race_ids = set(corners['race_id'])
    common_race_ids = races_race_ids & corners_race_ids
    print(f"\nrace_id共通: {len(common_race_ids):,}/{len(races_race_ids):,} ({len(common_race_ids)/len(races_race_ids)*100:.1f}%)")
    
    # 4コーナーのデータのみ抽出
    c4 = corners[corners['corner'] == 4].copy()
    print(f"\n4コーナーデータ: {len(c4):,}件")
    
    # 型を揃えてマージ
    races_test = races.copy()
    c4_test = c4.copy()
    
    # race_idを文字列に統一
    races_test['race_id'] = races_test['race_id'].astype(str)
    c4_test['race_id'] = c4_test['race_id'].astype(str)
    
    # horse_numberを整数に統一
    races_test['horse_number'] = pd.to_numeric(races_test['horse_number'], errors='coerce').astype('Int64')
    c4_test['horse_number'] = pd.to_numeric(c4_test['horse_number'], errors='coerce').astype('Int64')
    
    # マージ
    merged = races_test.merge(
        c4_test[['race_id', 'horse_number', 'position', 'gap_from_leader']],
        on=['race_id', 'horse_number'],
        how='left'
    )
    
    print(f"\nマージ後の非NaN率:")
    print(f"  position: {merged['position'].notna().sum():,}/{len(merged):,} ({merged['position'].notna().mean()*100:.1f}%)")
    print(f"  gap_from_leader: {merged['gap_from_leader'].notna().sum():,}/{len(merged):,} ({merged['gap_from_leader'].notna().mean()*100:.1f}%)")
    
    # V9での問題再現
    print("\n" + "="*80)
    print("V9での問題再現")
    print("="*80)
    
    # V9のコードと同じ方法でマージ
    # _merge_running_styleのロジック
    hist = races.copy()
    hist = hist.sort_values(['horse_id', 'race_date', 'race_id'])
    
    # c4を取得
    c4_v9 = corners[corners['corner'] == 4][['race_id', 'horse_number', 'position', 'gap_from_leader']].copy()
    c4_v9.columns = ['race_id', 'horse_number', 'c4_position', 'c4_gap']
    
    # マージ前のデータ型確認
    print(f"\nhist.race_id型: {hist['race_id'].dtype}")
    print(f"c4_v9.race_id型: {c4_v9['race_id'].dtype}")
    
    # マージ
    hist_with_c4 = hist.merge(c4_v9, on=['race_id', 'horse_number'], how='left')
    print(f"\nV9式マージ後の非NaN率:")
    print(f"  c4_position: {hist_with_c4['c4_position'].notna().sum():,}/{len(hist_with_c4):,} ({hist_with_c4['c4_position'].notna().mean()*100:.1f}%)")
    
    # 馬ごとの4コーナー平均を計算
    horse_c4_avg = hist_with_c4.groupby('horse_id')['c4_position'].mean().reset_index()
    horse_c4_avg.columns = ['horse_id', 'horse_avg_c4_pos']
    print(f"\n馬ごとのc4平均: {len(horse_c4_avg):,}頭")
    print(f"  非NaN: {horse_c4_avg['horse_avg_c4_pos'].notna().sum():,} ({horse_c4_avg['horse_avg_c4_pos'].notna().mean()*100:.1f}%)")
    
    # Testデータにマージ
    test = races[races['race_date'] >= '2025-01-01'].copy()
    test_with_c4 = test.merge(horse_c4_avg, on='horse_id', how='left')
    print(f"\nTestデータへのマージ後:")
    print(f"  horse_avg_c4_pos非NaN: {test_with_c4['horse_avg_c4_pos'].notna().sum():,}/{len(test_with_c4):,} ({test_with_c4['horse_avg_c4_pos'].notna().mean()*100:.1f}%)")
    
    # 問題の原因特定：新馬・未出走馬
    print("\n" + "="*80)
    print("原因特定：新馬・未出走馬の割合")
    print("="*80)
    
    # 各馬の過去レース数
    horse_race_count = hist_with_c4.groupby('horse_id').size()
    
    # Testデータの馬が過去に何レース走っているか
    test_horses = test['horse_id'].unique()
    test_horse_hist = horse_race_count.reindex(test_horses).fillna(0)
    
    print(f"\nTestデータの馬の過去レース数分布:")
    print(f"  0レース（新馬）: {(test_horse_hist == 0).sum():,}頭 ({(test_horse_hist == 0).mean()*100:.1f}%)")
    print(f"  1-2レース: {((test_horse_hist >= 1) & (test_horse_hist <= 2)).sum():,}頭")
    print(f"  3-5レース: {((test_horse_hist >= 3) & (test_horse_hist <= 5)).sum():,}頭")
    print(f"  6+レース: {(test_horse_hist >= 6).sum():,}頭")
    
    # 4コーナーデータを持つ馬
    horses_with_c4 = hist_with_c4[hist_with_c4['c4_position'].notna()].groupby('horse_id').size()
    test_horse_c4 = horses_with_c4.reindex(test_horses).fillna(0)
    
    print(f"\nTestデータの馬の4コーナーデータ保有数分布:")
    print(f"  0レース: {(test_horse_c4 == 0).sum():,}頭 ({(test_horse_c4 == 0).mean()*100:.1f}%)")
    print(f"  1+レース: {(test_horse_c4 >= 1).sum():,}頭 ({(test_horse_c4 >= 1).mean()*100:.1f}%)")


if __name__ == '__main__':
    main()
