"""
データ深層分析スクリプト

V9特徴量が効果なかった原因を探るため、実際のデータを詳細に分析する。

分析項目:
1. pedigrees.parquetの構造を確認し、母父の正しい取得方法を検証
2. corners.parquetのカバレッジ問題を調査
3. race_details.parquetのラップタイムデータの有効性を確認
4. 勝者と非勝者の特徴量分布を比較
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
    print("データ深層分析")
    print("="*80)
    
    # =================================================================
    # 1. pedigrees.parquetの構造分析
    # =================================================================
    print("\n" + "="*80)
    print("1. pedigrees.parquet 構造分析")
    print("="*80)
    
    pedigrees = pd.read_parquet(data_dir / "pedigrees/pedigrees.parquet")
    print(f"\n総レコード数: {len(pedigrees):,}")
    print(f"カラム: {list(pedigrees.columns)}")
    print(f"\ngeneration別レコード数:")
    print(pedigrees['generation'].value_counts().sort_index())
    
    # 特定の馬の血統を確認
    sample_horse = pedigrees['horse_id'].iloc[0]
    horse_pedigree = pedigrees[pedigrees['horse_id'] == sample_horse].sort_values('generation')
    print(f"\nサンプル馬 ({sample_horse}) の血統:")
    print(horse_pedigree)
    
    # generation=2のレコード詳細（母父候補）
    gen2 = pedigrees[pedigrees['generation'] == 2]
    print(f"\ngeneration=2 のレコード数: {len(gen2):,}")
    print(f"ユニーク馬数: {gen2['horse_id'].nunique():,}")
    
    # 各馬でgeneration=2のレコードが何件あるか
    gen2_per_horse = gen2.groupby('horse_id').size()
    print(f"\n馬あたりのgeneration=2レコード数:")
    print(gen2_per_horse.value_counts().sort_index())
    
    # =================================================================
    # 2. corners.parquetのカバレッジ分析
    # =================================================================
    print("\n" + "="*80)
    print("2. corners.parquet カバレッジ分析")
    print("="*80)
    
    corners = pd.read_parquet(data_dir / "corners/corner_positions.parquet")
    races = pd.read_parquet(data_dir / "races/races.parquet")
    races['race_date'] = pd.to_datetime(races['race_date'])
    races = races[races['race_date'] >= '2020-01-01']
    
    print(f"\n総コーナーレコード数: {len(corners):,}")
    
    # レースカバレッジ
    all_race_ids = set(races['race_id'].unique())
    corner_race_ids = set(corners['race_id'].unique())
    coverage = len(corner_race_ids & all_race_ids) / len(all_race_ids) * 100
    print(f"レースカバレッジ: {coverage:.1f}% ({len(corner_race_ids & all_race_ids):,}/{len(all_race_ids):,})")
    
    # コーナー別レコード数
    print(f"\nコーナー別レコード数:")
    print(corners['corner'].value_counts().sort_index())
    
    # 年別カバレッジ
    races_with_year = races.copy()
    races_with_year['year'] = races_with_year['race_date'].dt.year
    corners_races = corners.merge(races_with_year[['race_id', 'year']].drop_duplicates(), on='race_id', how='left')
    
    print(f"\n年別コーナーデータレコード数:")
    if 'year' in corners_races.columns:
        print(corners_races.groupby('year').size())
    
    # =================================================================
    # 3. race_details.parquetの分析
    # =================================================================
    print("\n" + "="*80)
    print("3. race_details.parquet 分析")
    print("="*80)
    
    race_details = pd.read_parquet(data_dir / "race_details/race_details.parquet")
    print(f"\n総レコード数: {len(race_details):,}")
    print(f"カラム: {list(race_details.columns)}")
    
    # カバレッジ
    details_race_ids = set(race_details['race_id'].unique())
    detail_coverage = len(details_race_ids & all_race_ids) / len(all_race_ids) * 100
    print(f"レースカバレッジ: {detail_coverage:.1f}% ({len(details_race_ids & all_race_ids):,}/{len(all_race_ids):,})")
    
    # first_half, second_halfの欠損
    print(f"\nfirst_half 非NaN: {race_details['first_half'].notna().sum():,} ({race_details['first_half'].notna().mean()*100:.1f}%)")
    print(f"second_half 非NaN: {race_details['second_half'].notna().sum():,} ({race_details['second_half'].notna().mean()*100:.1f}%)")
    
    # サンプルデータ
    print(f"\nサンプル:")
    print(race_details[['race_id', 'first_half', 'second_half', 'lap_times']].head(10))
    
    # =================================================================
    # 4. 勝者 vs 非勝者の特徴量分布
    # =================================================================
    print("\n" + "="*80)
    print("4. 勝者 vs 非勝者の分析")
    print("="*80)
    
    # V8で使われている特徴量の分布を比較
    races_clean = races.dropna(subset=['finish_position', 'win_odds'])
    races_clean['is_win'] = (races_clean['finish_position'] == 1).astype(int)
    
    # 前走着順との関係
    races_sorted = races_clean.sort_values(['horse_id', 'race_date', 'race_id'])
    races_sorted['prev_finish'] = races_sorted.groupby('horse_id')['finish_position'].shift(1)
    
    print(f"\n前走着順別の勝率:")
    prev_finish_win = races_sorted.groupby('prev_finish').agg(
        count=('is_win', 'count'),
        win_rate=('is_win', 'mean')
    ).head(10)
    print(prev_finish_win)
    
    # class_changeの効果（V8.1で最も重要だった）
    print("\n" + "="*80)
    print("5. class_change (V8.1最重要特徴量) の詳細分析")
    print("="*80)
    
    # race_nameからクラスを推定
    def parse_class(name):
        if pd.isna(name): return np.nan
        name = str(name)
        if 'G1' in name or 'GⅠ' in name: return 10
        if 'G2' in name or 'GⅡ' in name: return 9
        if 'G3' in name or 'GⅢ' in name: return 8
        if '(L)' in name or 'リステッド' in name: return 7
        if 'オープン' in name or 'OP' in name: return 6
        if '3勝' in name or '1600万' in name: return 5
        if '2勝' in name or '1000万' in name: return 4
        if '1勝' in name or '500万' in name: return 3
        if '未勝利' in name: return 2
        if '新馬' in name: return 1
        return np.nan
    
    races_sorted['race_class'] = races_sorted['race_name'].apply(parse_class)
    races_sorted['prev_race_class'] = races_sorted.groupby('horse_id')['race_class'].shift(1)
    races_sorted['class_change'] = races_sorted['race_class'] - races_sorted['prev_race_class']
    
    # クラス変動別の勝率
    print(f"\nクラス変動別の勝率:")
    class_change_win = races_sorted.groupby('class_change').agg(
        count=('is_win', 'count'),
        win_rate=('is_win', 'mean'),
        avg_odds=('win_odds', 'mean')
    )
    class_change_win = class_change_win[class_change_win['count'] >= 100]
    print(class_change_win.sort_index())
    
    # ROI推定（オッズを使用）
    print(f"\nクラス変動別のROI推定:")
    for cc in sorted(class_change_win.index):
        subset = races_sorted[races_sorted['class_change'] == cc]
        wins = subset[subset['is_win'] == 1]
        roi = wins['win_odds'].sum() * 100 / (len(subset) * 100) * 100 if len(subset) > 0 else 0
        print(f"  class_change={cc:+.0f}: ROI={roi:.1f}%, n={len(subset):,}")
    
    # =================================================================
    # 6. 母父統計の有効性検証
    # =================================================================
    print("\n" + "="*80)
    print("6. 母父統計の有効性検証（手動計算）")
    print("="*80)
    
    # 正しい母父取得: generation=2の4番目のレコード（母の父）
    # pedigreeの構造を確認
    gen2_sample = pedigrees[pedigrees['generation'] == 2].head(20)
    print(f"\ngeneration=2のサンプル:")
    print(gen2_sample)
    
    # 各馬のgeneration=2レコードを取得し、何番目が母父か推定
    sample_horses = pedigrees['horse_id'].unique()[:5]
    for horse in sample_horses:
        gen2_horse = pedigrees[(pedigrees['horse_id'] == horse) & (pedigrees['generation'] == 2)]
        print(f"\n馬 {horse}:")
        for idx, row in gen2_horse.reset_index().iterrows():
            print(f"  {idx}: {row['ancestor_id']} ({row['ancestor_name']})")


if __name__ == '__main__':
    main()
