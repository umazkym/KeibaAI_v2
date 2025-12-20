"""
発見された問題点の詳細調査スクリプト

問題1: 着順NULLが4,700件存在
問題2: corners欠損が286レース
問題3: 50馬身超のギャップが85件
問題4: 1着が複数いるレースが63件
問題5: 2014-2019データの品質
"""

import pandas as pd
import numpy as np
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = PROJECT_ROOT / "keibaai" / "data" / "parsed" / "parquet"

def print_section(title):
    print("\n" + "=" * 80)
    print(f"  {title}")
    print("=" * 80)

def investigate_null_finish_position():
    """着順NULLの詳細調査"""
    print_section("問題1: 着順NULLの詳細調査")
    
    races = pd.read_parquet(DATA_DIR / "races" / "races.parquet")
    races['race_date'] = pd.to_datetime(races['race_date'])
    
    null_records = races[races['finish_position'].isnull()]
    
    print(f"\n  着順NULLレコード数: {len(null_records):,}")
    print(f"  全体に占める割合: {len(null_records)/len(races)*100:.2f}%")
    
    # 原因別に分類
    print(f"\n  【原因分析】")
    
    # 出走取消（scratched）を確認
    if 'scratched' in races.columns:
        scratched = null_records[null_records['scratched'] == True]
        print(f"  出走取消: {len(scratched)}件")
    
    # finish_time_secondsがNULLのもの（競走中止？）
    if 'finish_time_seconds' in races.columns:
        no_time = null_records[null_records['finish_time_seconds'].isnull()]
        print(f"  タイム記録なし: {len(no_time)}件")
    
    # サンプルデータ表示
    print(f"\n  【サンプルデータ】")
    sample_cols = ['race_id', 'race_date', 'horse_number', 'horse_name', 'finish_position', 
                   'finish_time_seconds', 'win_odds', 'popularity']
    sample_cols = [c for c in sample_cols if c in null_records.columns]
    print(null_records[sample_cols].head(20).to_string())
    
    # これらがモデル学習に与える影響
    print(f"\n  【モデルへの影響】")
    print(f"  - 着順NULLは学習対象から除外される")
    print(f"  - is_win (1着フラグ) の計算に影響")
    print(f"  - 訓練データの約 {len(null_records)/len(races)*100:.2f}% が影響")
    
    return null_records

def investigate_missing_corners():
    """コーナーデータ欠損の詳細調査"""
    print_section("問題2: コーナーデータ欠損の詳細調査")
    
    races = pd.read_parquet(DATA_DIR / "races" / "races.parquet")
    corners = pd.read_parquet(DATA_DIR / "corners" / "corner_positions.parquet")
    
    races_ids = set(races['race_id'].unique())
    corners_ids = set(corners['race_id'].unique())
    
    missing_ids = races_ids - corners_ids
    
    print(f"\n  racesにあってcornersにないレース: {len(missing_ids)}件")
    
    # 欠損レースの詳細
    missing_races = races[races['race_id'].isin(missing_ids)]
    missing_races['race_date'] = pd.to_datetime(missing_races['race_date'])
    
    # 1レースあたりのサンプル（レース単位で見る）
    missing_race_info = missing_races.groupby('race_id').first().reset_index()
    
    print(f"\n  【欠損レースの特徴】")
    
    # 距離別
    if 'distance_m' in missing_race_info.columns:
        print(f"\n  距離別分布:")
        dist_counts = missing_race_info['distance_m'].value_counts().head(10)
        for dist, count in dist_counts.items():
            print(f"    {dist}m: {count}件")
    
    # 馬場別
    if 'track_surface' in missing_race_info.columns:
        print(f"\n  馬場別分布:")
        surf_counts = missing_race_info['track_surface'].value_counts()
        for surf, count in surf_counts.items():
            print(f"    {surf}: {count}件")
    
    # 競馬場別
    if 'venue' in missing_race_info.columns:
        print(f"\n  競馬場別分布:")
        venue_counts = missing_race_info['venue'].value_counts()
        for venue, count in venue_counts.items():
            print(f"    {venue}: {count}件")
    
    # サンプル
    print(f"\n  【サンプルレース】")
    sample_cols = ['race_id', 'race_date', 'distance_m', 'track_surface', 'venue', 'race_name']
    sample_cols = [c for c in sample_cols if c in missing_race_info.columns]
    print(missing_race_info[sample_cols].head(10).to_string())
    
    # race_detailsとの照合
    race_details = pd.read_parquet(DATA_DIR / "race_details" / "race_details.parquet")
    
    # 欠損レースのrace_detailsを確認
    missing_in_details = missing_race_info[~missing_race_info['race_id'].isin(race_details['race_id'])]
    missing_has_details = missing_race_info[missing_race_info['race_id'].isin(race_details['race_id'])]
    
    print(f"\n  【race_detailsとの照合】")
    print(f"  欠損レースのうちrace_detailsにある: {len(missing_has_details)}件")
    print(f"  欠損レースのうちrace_detailsにもない: {len(missing_in_details)}件")
    
    # corner_X_rawを確認
    if len(missing_has_details) > 0:
        sample_detail_ids = missing_has_details['race_id'].head(5).tolist()
        detail_sample = race_details[race_details['race_id'].isin(sample_detail_ids)]
        print(f"\n  【race_detailsのcorner_X_rawサンプル】")
        corner_cols = ['race_id', 'corner_1_raw', 'corner_2_raw', 'corner_3_raw', 'corner_4_raw']
        corner_cols = [c for c in corner_cols if c in detail_sample.columns]
        print(detail_sample[corner_cols].to_string())
    
    return missing_ids

def investigate_large_gaps():
    """50馬身超ギャップの詳細調査"""
    print_section("問題3: 50馬身超ギャップの詳細調査")
    
    corners = pd.read_parquet(DATA_DIR / "corners" / "corner_positions.parquet")
    
    large_gaps = corners[corners['gap_from_leader'] > 50]
    
    print(f"\n  50馬身超のレコード数: {len(large_gaps)}")
    
    # 分布
    print(f"\n  【gap_from_leaderの分布】")
    print(f"  50-60馬身: {len(large_gaps[(large_gaps['gap_from_leader'] >= 50) & (large_gaps['gap_from_leader'] < 60)])}件")
    print(f"  60-70馬身: {len(large_gaps[(large_gaps['gap_from_leader'] >= 60) & (large_gaps['gap_from_leader'] < 70)])}件")
    print(f"  70馬身超: {len(large_gaps[large_gaps['gap_from_leader'] >= 70])}件")
    
    # サンプル
    print(f"\n  【サンプルデータ】")
    print(large_gaps.sort_values('gap_from_leader', ascending=False).head(20).to_string())
    
    # 対応するrace_detailsのcorner_rawを調査
    race_details = pd.read_parquet(DATA_DIR / "race_details" / "race_details.parquet")
    
    sample_race_ids = large_gaps['race_id'].head(5).unique().tolist()
    print(f"\n  【対応するrace_detailsのcorner_raw】")
    for race_id in sample_race_ids:
        detail = race_details[race_details['race_id'] == race_id]
        if len(detail) > 0:
            print(f"\n  race_id: {race_id}")
            for i in range(1, 5):
                col = f'corner_{i}_raw'
                if col in detail.columns:
                    val = detail[col].iloc[0]
                    print(f"    {col}: {val}")
            
            # このレースのギャップ
            race_gaps = large_gaps[large_gaps['race_id'] == race_id]
            print(f"    50馬身超の馬: {race_gaps[['corner', 'horse_number', 'gap_from_leader']].to_string(index=False)}")

def investigate_multiple_winners():
    """1着複数レースの詳細調査"""
    print_section("問題4: 1着複数レースの詳細調査")
    
    races = pd.read_parquet(DATA_DIR / "races" / "races.parquet")
    
    first_place = races[races['finish_position'] == 1]
    multi_winners = first_place.groupby('race_id').size()
    multi_winners = multi_winners[multi_winners > 1]
    
    print(f"\n  1着が複数いるレース: {len(multi_winners)}件")
    
    # 分布
    print(f"\n  【馬数別分布】")
    for count, num_races in multi_winners.value_counts().sort_index().items():
        print(f"    {count}頭1着: {num_races}レース")
    
    # サンプル
    print(f"\n  【サンプルレース】")
    sample_ids = multi_winners.head(5).index.tolist()
    for race_id in sample_ids:
        race = races[races['race_id'] == race_id]
        first = race[race['finish_position'] == 1]
        print(f"\n  race_id: {race_id}")
        print(f"    日付: {race['race_date'].iloc[0]}")
        print(f"    1着馬:")
        for _, row in first.iterrows():
            print(f"      #{row['horse_number']} {row.get('horse_name', 'N/A')} オッズ:{row.get('win_odds', 'N/A')}")
    
    # これが同着（正常）かデータ異常かを確認
    print(f"\n  【同着判定】")
    print(f"  同着は正常データ。2頭同着の場合、両方に1着のオッズ配当が付く。")

def investigate_2014_2019_quality():
    """2014-2019データの品質詳細調査"""
    print_section("問題5: 2014-2019データの品質詳細調査")
    
    races = pd.read_parquet(DATA_DIR / "races" / "races.parquet")
    races['race_date'] = pd.to_datetime(races['race_date'])
    races['year'] = races['race_date'].dt.year
    
    old_data = races[races['year'] < 2020]
    new_data = races[races['year'] >= 2020]
    
    print(f"\n  2014-2019: {len(old_data):,}件")
    print(f"  2020-2025: {len(new_data):,}件")
    
    # カラムごとのNULL率比較
    print(f"\n  【カラム別NULL率比較】")
    print(f"  {'カラム名':<30} {'2014-2019':>12} {'2020-2025':>12} {'差分':>12}")
    print("-" * 70)
    
    important_cols = ['finish_position', 'win_odds', 'horse_weight', 'last_3f_time', 
                     'passing_order_1', 'passing_order_2', 'passing_order_3', 'passing_order_4',
                     'jockey_id', 'trainer_id', 'horse_id']
    
    for col in important_cols:
        if col in races.columns:
            old_null = old_data[col].isnull().sum() / len(old_data) * 100
            new_null = new_data[col].isnull().sum() / len(new_data) * 100
            diff = old_null - new_null
            flag = "⚠️" if abs(diff) > 1 else ""
            print(f"  {col:<30} {old_null:>10.1f}% {new_null:>10.1f}% {diff:>+10.1f}% {flag}")
    
    # コーナーデータの比較
    corners = pd.read_parquet(DATA_DIR / "corners" / "corner_positions.parquet")
    corners['year'] = corners['race_id'].astype(str).str[:4].astype(int)
    
    old_corners = corners[corners['year'] < 2020]
    new_corners = corners[corners['year'] >= 2020]
    
    print(f"\n  【コーナーデータ比較】")
    print(f"  2014-2019: {len(old_corners):,}件、{old_corners['race_id'].nunique():,}レース")
    print(f"  2020-2025: {len(new_corners):,}件、{new_corners['race_id'].nunique():,}レース")
    
    # 1レースあたりのコーナー記録数
    old_per_race = len(old_corners) / old_corners['race_id'].nunique() if old_corners['race_id'].nunique() > 0 else 0
    new_per_race = len(new_corners) / new_corners['race_id'].nunique() if new_corners['race_id'].nunique() > 0 else 0
    
    print(f"\n  1レースあたりの記録数:")
    print(f"    2014-2019: {old_per_race:.1f}件")
    print(f"    2020-2025: {new_per_race:.1f}件")
    
    # gap_from_leader統計
    print(f"\n  【gap_from_leader統計比較】")
    print(f"  {'項目':<10} {'2014-2019':>12} {'2020-2025':>12}")
    print("-" * 40)
    print(f"  {'mean':<10} {old_corners['gap_from_leader'].mean():>12.2f} {new_corners['gap_from_leader'].mean():>12.2f}")
    print(f"  {'std':<10} {old_corners['gap_from_leader'].std():>12.2f} {new_corners['gap_from_leader'].std():>12.2f}")
    print(f"  {'max':<10} {old_corners['gap_from_leader'].max():>12.2f} {new_corners['gap_from_leader'].max():>12.2f}")

def investigate_race_detail_raw_parsing():
    """race_detailsのcorner_raw解析の確認"""
    print_section("問題6: corner_raw解析の詳細確認")
    
    race_details = pd.read_parquet(DATA_DIR / "race_details" / "race_details.parquet")
    corners = pd.read_parquet(DATA_DIR / "corners" / "corner_positions.parquet")
    
    # corner_X_rawがあるがcorner_positionsにないレースを調査
    details_ids = set(race_details['race_id'].unique())
    corners_ids = set(corners['race_id'].unique())
    
    has_raw_no_parsed = details_ids - corners_ids
    
    print(f"\n  race_detailsにあってcorner_positionsにないレース: {len(has_raw_no_parsed)}件")
    
    if len(has_raw_no_parsed) > 0:
        sample_ids = list(has_raw_no_parsed)[:10]
        sample_details = race_details[race_details['race_id'].isin(sample_ids)]
        
        print(f"\n  【サンプル】corner_rawの内容:")
        for _, row in sample_details.iterrows():
            print(f"\n  race_id: {row['race_id']}")
            for i in range(1, 5):
                col = f'corner_{i}_raw'
                if col in row.index:
                    val = row[col]
                    print(f"    {col}: {val}")

def investigate_feature_impact():
    """特徴量への影響調査"""
    print_section("問題7: 特徴量エンジニアリングへの影響")
    
    races = pd.read_parquet(DATA_DIR / "races" / "races.parquet")
    corners = pd.read_parquet(DATA_DIR / "corners" / "corner_positions.parquet")
    
    races['race_date'] = pd.to_datetime(races['race_date'])
    
    # horse_c4_gap_avgに影響する馬頭数
    # corner == 4のデータがある馬
    c4_data = corners[corners['corner'] == 4]
    horses_with_c4 = set(c4_data.merge(races[['race_id', 'horse_number', 'horse_id']], 
                                        on=['race_id', 'horse_number'])['horse_id'].dropna().unique())
    
    # 全馬頭数
    all_horses = set(races['horse_id'].dropna().unique())
    
    print(f"\n  全馬頭数: {len(all_horses):,}")
    print(f"  C4データがある馬頭数: {len(horses_with_c4):,}")
    print(f"  C4カバー率: {len(horses_with_c4)/len(all_horses)*100:.1f}%")
    
    # 年別のC4カバー率
    races['year'] = races['race_date'].dt.year
    c4_race_ids = set(c4_data['race_id'].unique())
    
    print(f"\n  【年別C4データカバー率】")
    for year in sorted(races['year'].unique()):
        year_races = races[races['year'] == year]
        year_race_ids = set(year_races['race_id'].unique())
        covered = len(year_race_ids & c4_race_ids)
        total = len(year_race_ids)
        print(f"    {year}: {covered}/{total} ({covered/total*100:.1f}%)")

def main():
    print("=" * 80)
    print("  KeibaAI_v2 データ品質問題詳細調査レポート")
    print("=" * 80)
    
    investigate_null_finish_position()
    investigate_missing_corners()
    investigate_large_gaps()
    investigate_multiple_winners()
    investigate_2014_2019_quality()
    investigate_race_detail_raw_parsing()
    investigate_feature_impact()
    
    print("\n" + "=" * 80)
    print("  調査完了")
    print("=" * 80)

if __name__ == "__main__":
    main()
