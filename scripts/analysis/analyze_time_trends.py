"""
時期別・競馬場別タイム傾向分析スクリプト

目的:
- 競馬場×距離×時期別の「標準タイム」を分析
- 時期による馬場速度差を可視化
- レースレベル特徴量の可能性を探索
"""

import pandas as pd
import numpy as np
from pathlib import Path

def main():
    data_dir = Path("keibaai/data/parsed/parquet")
    
    print("="*80)
    print("時期別・競馬場別タイム傾向分析")
    print("="*80)
    
    # データ読み込み
    print("\n1. データ読み込み...")
    races = pd.read_parquet(data_dir / "races/races.parquet")
    races['race_date'] = pd.to_datetime(races['race_date'])
    
    # 2020年以降に限定
    races = races[races['race_date'] >= '2020-01-01']
    
    print(f"   レコード数: {len(races):,}")
    print(f"   期間: {races['race_date'].min()} 〜 {races['race_date'].max()}")
    
    # 月情報を追加
    races['year_month'] = races['race_date'].dt.to_period('M')
    races['month'] = races['race_date'].dt.month
    races['year'] = races['race_date'].dt.year
    
    # タイム情報がある行のみ
    races_with_time = races[races['finish_time_seconds'].notna()].copy()
    print(f"   タイム情報あり: {len(races_with_time):,}")
    
    # ==========================================================
    # 分析1: 競馬場×距離×月別の標準タイム
    # ==========================================================
    print("\n" + "="*80)
    print("分析1: 競馬場×距離×月別の標準タイム（芝1600m例）")
    print("="*80)
    
    # 芝1600mに限定
    turf_1600 = races_with_time[
        (races_with_time['track_surface'] == '芝') &
        (races_with_time['distance_m'] == 1600)
    ].copy()
    
    print(f"\n芝1600m レコード数: {len(turf_1600):,}")
    
    # 競馬場別×月別のタイム統計
    venue_month_stats = turf_1600.groupby(['venue', 'month']).agg({
        'finish_time_seconds': ['mean', 'std', 'count']
    }).round(3)
    venue_month_stats.columns = ['mean_time', 'std_time', 'count']
    
    # 十分なサンプル数のあるデータのみ
    venue_month_stats = venue_month_stats[venue_month_stats['count'] >= 50]
    
    print("\n競馬場×月別 平均タイム（芝1600m）:")
    for venue in ['東京', '阪神', '中山', '京都']:
        venue_data = venue_month_stats.loc[venue] if venue in venue_month_stats.index.get_level_values(0) else None
        if venue_data is not None and len(venue_data) > 0:
            print(f"\n  【{venue}】")
            for month in venue_data.index:
                row = venue_data.loc[month]
                print(f"    {month}月: {row['mean_time']:.2f}秒 (std={row['std_time']:.2f}, n={int(row['count'])})")
    
    # ==========================================================
    # 分析2: 開催時期による馬場速度差（年間トレンド）
    # ==========================================================
    print("\n" + "="*80)
    print("分析2: 開催時期による馬場速度差（東京芝1600m）")
    print("="*80)
    
    tokyo_turf = turf_1600[turf_1600['venue'] == '東京']
    
    # 1着馬のみで分析（レースレベルの代表値）
    tokyo_turf_1st = tokyo_turf[tokyo_turf['finish_position'] == 1]
    
    month_stats = tokyo_turf_1st.groupby('month').agg({
        'finish_time_seconds': ['mean', 'std', 'count'],
        'last_3f_time': ['mean', 'std']
    }).round(2)
    month_stats.columns = ['time_mean', 'time_std', 'count', 'l3f_mean', 'l3f_std']
    
    print("\n東京芝1600m 月別タイム（1着馬）:")
    overall_mean = tokyo_turf_1st['finish_time_seconds'].mean()
    print(f"全体平均: {overall_mean:.2f}秒")
    print()
    for month in range(1, 13):
        if month in month_stats.index:
            row = month_stats.loc[month]
            diff = row['time_mean'] - overall_mean
            indicator = "🐢遅" if diff > 0.3 else ("🐇速" if diff < -0.3 else "  普通")
            print(f"  {month:2d}月: {row['time_mean']:.2f}秒 ({diff:+.2f}秒) {indicator} | 上り{row['l3f_mean']:.1f}秒 | n={int(row['count'])}")
    
    # ==========================================================
    # 分析3: 馬場状態による影響
    # ==========================================================
    print("\n" + "="*80)
    print("分析3: 馬場状態による影響（芝1600m全体）")
    print("="*80)
    
    turf_1600_1st = turf_1600[turf_1600['finish_position'] == 1]
    
    condition_stats = turf_1600_1st.groupby('track_condition').agg({
        'finish_time_seconds': ['mean', 'std', 'count']
    }).round(2)
    condition_stats.columns = ['mean', 'std', 'count']
    
    print("\n馬場状態別タイム:")
    for cond in ['良', '稍重', '重', '不良']:
        if cond in condition_stats.index:
            row = condition_stats.loc[cond]
            print(f"  {cond}: {row['mean']:.2f}秒 (std={row['std']:.2f}, n={int(row['count'])})")
    
    # ==========================================================
    # 分析4: 上がり3Fとレース全体タイムの関係
    # ==========================================================
    print("\n" + "="*80)
    print("分析4: 上がり3Fの分析（期待値に影響する可能性）")
    print("="*80)
    
    races_with_l3f = races_with_time[races_with_time['last_3f_time'].notna()].copy()
    
    # 1着馬の上がり3Fランク（当日の他馬との比較）
    print(f"\n上がり3Fデータあり: {len(races_with_l3f):,}")
    
    # last3f_rankの分布と勝率
    if 'last3f_rank' in races_with_l3f.columns:
        l3f_rank_stats = races_with_l3f.groupby('last3f_rank').agg({
            'finish_position': lambda x: (x == 1).mean()  # 勝率
        }).round(3)
        l3f_rank_stats.columns = ['win_rate']
        
        print("\n上がり3F順位別 勝率:")
        for rank in [1, 2, 3, 4, 5]:
            if rank in l3f_rank_stats.index:
                print(f"  {rank}位: {l3f_rank_stats.loc[rank, 'win_rate']*100:.1f}%")
    
    # ==========================================================
    # 分析5: race_details.parquetの確認
    # ==========================================================
    print("\n" + "="*80)
    print("分析5: race_details.parquet（ペース分析）")
    print("="*80)
    
    try:
        race_details = pd.read_parquet(data_dir / "race_details/race_details.parquet")
        print(f"\nレコード数: {len(race_details):,}")
        print(f"カラム: {race_details.columns.tolist()}")
        
        # ペース差の分析
        if 'first_half' in race_details.columns and 'second_half' in race_details.columns:
            race_details['pace_diff'] = race_details['second_half'] - race_details['first_half']
            
            print(f"\nペース差統計:")
            print(f"  mean: {race_details['pace_diff'].mean():.2f}秒")
            print(f"  std: {race_details['pace_diff'].std():.2f}秒")
            print(f"  min: {race_details['pace_diff'].min():.2f}秒 (ハイペース)")
            print(f"  max: {race_details['pace_diff'].max():.2f}秒 (スローペース)")
    except Exception as e:
        print(f"  エラー: {e}")
    
    # ==========================================================
    # 分析6: 潜在的な新特徴量の候補
    # ==========================================================
    print("\n" + "="*80)
    print("分析6: 潜在的な新特徴量候補")
    print("="*80)
    
    print("""
【候補1】venue_month_time_deviation
- 計算: (実際タイム - 場×距離×月の標準タイム) / 標準偏差
- 目的: レースレベルの速さ/遅さを数値化
- リスク: レース結果（タイム）を使うためリーク可能性あり

【候補2】venue_month_expected_time
- 計算: 場×距離×月×馬場状態の標準タイム（Train期間で固定計算）
- 目的: 「遅い時期なのに速い馬」「速い時期なのに遅い馬」を検出
- リスク: fit時にTrain期間のみで計算すればリークなし

【候補3】horse_time_deviation_avg
- 計算: 過去の（実タイム - 標準タイム）の累積平均（shift済み）
- 目的: 馬の「相対的な速さ」を評価
- リスク: cumsum().shift(1)パターンでリークなし

【候補4】race_level_score（要注意）
- 計算: レース勝ち時計の標準タイムからの偏差
- 目的: そのレースが「ハイレベル」か判定
- リスク: ⚠️ 当日のレース結果を使うとリーク！過去累積統計に変換必要

【候補5】horse_l3f_improvement_trend
- 計算: 過去上がり3Fタイムの改善傾向（回帰係数）
- 目的: 「成長中の馬」を検出
- リスク: 過去統計のみなのでリークなし
""")
    
    print("\n分析完了")

if __name__ == "__main__":
    main()
