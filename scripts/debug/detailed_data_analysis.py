"""
詳細データ品質分析スクリプト

発見された問題の原因を深堀りする。
"""

import pandas as pd
import numpy as np
from pathlib import Path

DATA_DIR = Path(r"c:\Users\zk-ht\Keiba\Keiba_AI_v2\keibaai\data\parsed\parquet")

def main():
    print("=" * 80)
    print("詳細データ品質分析")
    print("=" * 80)
    
    # データ読み込み
    races = pd.read_parquet(DATA_DIR / 'races' / 'races.parquet')
    corners = pd.read_parquet(DATA_DIR / 'corners' / 'corner_positions.parquet')
    race_details = pd.read_parquet(DATA_DIR / 'race_details' / 'race_details.parquet')
    
    races['race_date'] = pd.to_datetime(races['race_date'])
    races['year'] = races['race_date'].dt.year
    
    # ===============================
    # 1. distance_m欠損の詳細分析
    # ===============================
    print("\n" + "=" * 80)
    print("1. distance_m欠損の詳細分析（5,211件）")
    print("=" * 80)
    
    dist_null = races[races['distance_m'].isnull()]
    print(f"\n欠損レコード数: {len(dist_null):,}")
    print(f"欠損レース数: {dist_null['race_id'].nunique():,}")
    
    # 年別分布
    print("\n年別欠損レコード数:")
    for year, cnt in dist_null.groupby('year').size().items():
        print(f"  {year}: {cnt:,}")
    
    # track_surface別
    print("\ntrack_surface別欠損レコード数:")
    for surface, cnt in dist_null['track_surface'].value_counts().items():
        print(f"  {surface}: {cnt:,}")
    
    # 競馬場別
    if 'venue' in dist_null.columns:
        print("\nvenue別欠損レコード数（上位10）:")
        for venue, cnt in dist_null['venue'].value_counts().head(10).items():
            print(f"  {venue}: {cnt:,}")
    
    # サンプルレース確認
    print("\n欠損レースのサンプル（10件）:")
    sample_races = dist_null[['race_id', 'race_date', 'venue', 'track_surface', 'race_name']].drop_duplicates().head(10)
    print(sample_races.to_string())
    
    # ===============================
    # 2. passing_order_4の高欠損率分析
    # ===============================
    print("\n" + "=" * 80)
    print("2. passing_order_4の高欠損率分析（57.8%）")
    print("=" * 80)
    
    po4_null = races[races['passing_order_4'].isnull()]
    po4_valid = races[races['passing_order_4'].notna()]
    
    print(f"\npassing_order_4 NULLレコード数: {len(po4_null):,}")
    print(f"passing_order_4 有効レコード数: {len(po4_valid):,}")
    
    # 距離別分析
    print("\n【距離別passing_order_4欠損率】")
    for dist in [1000, 1200, 1400, 1600, 1800, 2000, 2200, 2400, 2500, 3000]:
        dist_data = races[races['distance_m'] == dist]
        if len(dist_data) > 0:
            null_pct = dist_data['passing_order_4'].isnull().mean() * 100
            flag = "⚠️" if null_pct > 70 else ""
            print(f"  {dist}m: {null_pct:.1f}% 欠損 ({len(dist_data):,}件) {flag}")
    
    # コーナー数との関係
    # 短距離レースは2コーナー、長距離は4コーナー
    print("\n【距離カテゴリ別コーナー数分析】")
    
    # 各レースのコーナー数を確認
    corner_counts = corners.groupby('race_id')['corner'].nunique()
    races_with_corners = races[['race_id', 'distance_m', 'venue']].drop_duplicates().set_index('race_id')
    races_with_corners['num_corners'] = corner_counts
    races_with_corners = races_with_corners.dropna(subset=['distance_m'])
    
    print("\n距離別の平均コーナー数:")
    dist_corners = races_with_corners.groupby('distance_m')['num_corners'].mean().sort_index()
    for dist, avg_corners in dist_corners.items():
        if dist in [1000, 1200, 1400, 1600, 1800, 2000, 2200, 2400]:
            print(f"  {int(dist)}m: 平均 {avg_corners:.1f} コーナー")
    
    # ===============================
    # 3. 賞金カラム100%欠損の分析
    # ===============================
    print("\n" + "=" * 80)
    print("3. 賞金カラム100%欠損の分析")
    print("=" * 80)
    
    prize_cols = ['prize_1st', 'prize_2nd', 'prize_3rd', 'prize_4th', 'prize_5th']
    for col in prize_cols:
        if col in races.columns:
            null_count = races[col].isnull().sum()
            print(f"  {col}: {null_count:,} / {len(races):,} NULL ({null_count/len(races)*100:.1f}%)")
    
    # prize_moneyカラムの状況
    if 'prize_money' in races.columns:
        print(f"\n  prize_money: ")
        print(f"    NULL: {races['prize_money'].isnull().sum():,} ({races['prize_money'].isnull().mean()*100:.1f}%)")
        print(f"    有効: {races['prize_money'].notna().sum():,}")
        valid_prize = races['prize_money'].dropna()
        print(f"    min: {valid_prize.min():,.0f}, max: {valid_prize.max():,.0f}")
    
    # ===============================
    # 4. finish_time_seconds異常値分析
    # ===============================
    print("\n" + "=" * 80)
    print("4. finish_time_seconds異常値分析（>300秒: 17件）")
    print("=" * 80)
    
    time_anomaly = races[races['finish_time_seconds'] > 300]
    print(f"\n300秒超のレコード数: {len(time_anomaly):,}")
    
    if len(time_anomaly) > 0:
        print("\n異常レコードの詳細:")
        cols = ['race_id', 'race_date', 'venue', 'distance_m', 'track_surface', 'finish_time_seconds', 'finish_position']
        print(time_anomaly[cols].to_string())
        
        # これは障害レース？
        print("\ntrack_surface別:")
        print(time_anomaly['track_surface'].value_counts())
    
    # ===============================
    # 5. コーナーデータがないレースの原因分析
    # ===============================
    print("\n" + "=" * 80)
    print("5. コーナーデータがないレース分析（286件）")
    print("=" * 80)
    
    races_ids = set(races['race_id'].unique())
    corners_races = set(corners['race_id'].unique())
    no_corner_races = races_ids - corners_races
    
    print(f"\nコーナーデータがないレース数: {len(no_corner_races):,}")
    
    # これらのレースの特徴
    no_corner_df = races[races['race_id'].isin(no_corner_races)][['race_id', 'race_date', 'venue', 'distance_m', 'track_surface', 'race_name']].drop_duplicates()
    
    print("\n競馬場別:")
    print(no_corner_df['venue'].value_counts())
    
    print("\n距離別:")
    print(no_corner_df['distance_m'].value_counts().head(10))
    
    print("\ntrack_surface別:")
    print(no_corner_df['track_surface'].value_counts())
    
    # ===============================
    # 6. race_detailsとracesの整合性
    # ===============================
    print("\n" + "=" * 80)
    print("6. race_detailsとracesの整合性")
    print("=" * 80)
    
    race_detail_ids = set(race_details['race_id'].unique())
    
    print(f"\nrace_detailsのレース数: {len(race_detail_ids):,}")
    print(f"racesのレース数: {len(races_ids):,}")
    
    missing_in_details = races_ids - race_detail_ids
    print(f"race_detailsにないレース数: {len(missing_in_details):,}")
    
    if len(missing_in_details) > 0:
        # 年別
        missing_df = races[races['race_id'].isin(missing_in_details)].groupby('year')['race_id'].nunique()
        print("\n年別のrace_details欠損レース数:")
        for year, cnt in missing_df.items():
            total = races[races['year'] == year]['race_id'].nunique()
            print(f"  {year}: {cnt:,} / {total:,} ({cnt/total*100:.1f}%)")
    
    # ===============================
    # 7. 2014-2019 vs 2020-2025 の特徴量分布比較
    # ===============================
    print("\n" + "=" * 80)
    print("7. 2014-2019 vs 2020-2025 の特徴量分布比較")
    print("=" * 80)
    
    old = races[races['year'] <= 2019]
    new = races[races['year'] >= 2020]
    
    # 数値カラムの分布比較
    numeric_cols = ['distance_m', 'horse_weight', 'win_odds', 'finish_time_seconds', 
                   'last_3f_time', 'basis_weight', 'age']
    
    print("\n【数値カラムの統計比較】")
    print(f"{'カラム':<22} {'2014-2019 mean':<16} {'2020-2025 mean':<16} {'差分':<10} {'警告'}")
    print("-" * 80)
    
    for col in numeric_cols:
        if col in races.columns:
            old_mean = old[col].mean()
            new_mean = new[col].mean()
            diff_pct = (new_mean - old_mean) / old_mean * 100 if old_mean != 0 else 0
            warning = "⚠️" if abs(diff_pct) > 5 else ""
            print(f"{col:<22} {old_mean:>14.2f} {new_mean:>14.2f} {diff_pct:>+8.1f}% {warning}")
    
    # カテゴリカラムの分布比較
    print("\n【track_surface分布比較】")
    old_surface = old['track_surface'].value_counts(normalize=True) * 100
    new_surface = new['track_surface'].value_counts(normalize=True) * 100
    
    for surface in old_surface.index:
        old_pct = old_surface.get(surface, 0)
        new_pct = new_surface.get(surface, 0)
        diff = new_pct - old_pct
        warning = "⚠️" if abs(diff) > 2 else ""
        print(f"  {surface}: {old_pct:.1f}% → {new_pct:.1f}% ({diff:+.1f}%) {warning}")
    
    print("\n" + "=" * 80)
    print("分析完了")
    print("=" * 80)


if __name__ == '__main__':
    main()
