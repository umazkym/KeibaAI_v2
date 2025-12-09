"""
特徴量候補のROI影響分析

目的:
- 時期別馬場速度補正特徴量がROIを向上させるか検証
- リークなしで実装可能な形式で分析
"""

import pandas as pd
import numpy as np
from pathlib import Path

def main():
    data_dir = Path("keibaai/data/parsed/parquet")
    
    print("="*80)
    print("特徴量候補のROI影響分析")
    print("="*80)
    
    # データ読み込み
    print("\n1. データ読み込み...")
    races = pd.read_parquet(data_dir / "races/races.parquet")
    races['race_date'] = pd.to_datetime(races['race_date'])
    
    # 2020年以降、タイムとオッズがあるもの
    races = races[
        (races['race_date'] >= '2020-01-01') &
        races['finish_time_seconds'].notna() &
        races['win_odds'].notna() &
        races['finish_position'].notna()
    ].copy()
    
    races['month'] = races['race_date'].dt.month
    races['year'] = races['race_date'].dt.year
    
    print(f"   レコード数: {len(races):,}")
    
    # ==========================================================
    # 分析1: 場×距離×月の標準タイムを計算（1着馬のみ）
    # ==========================================================
    print("\n" + "="*80)
    print("分析1: 標準タイムの計算（Train期間のみ使用）")
    print("="*80)
    
    # Train/Test分割
    train = races[races['race_date'] < '2025-01-01']
    test = races[races['race_date'] >= '2025-01-01']
    
    print(f"   Train: {len(train):,} ({train['race_date'].min()} 〜 {train['race_date'].max()})")
    print(f"   Test: {len(test):,} ({test['race_date'].min()} 〜 {test['race_date'].max()})")
    
    # 1着馬のみで標準タイムを計算
    train_1st = train[train['finish_position'] == 1]
    
    # 場×距離×月×馬場状態で標準タイム計算
    standard_times = train_1st.groupby(
        ['venue', 'distance_m', 'track_surface', 'month', 'track_condition']
    ).agg({
        'finish_time_seconds': ['mean', 'std', 'count']
    })
    standard_times.columns = ['std_time', 'std_time_std', 'count']
    standard_times = standard_times[standard_times['count'] >= 10].reset_index()
    
    print(f"\n   標準タイムグループ数: {len(standard_times)}")
    
    # ==========================================================
    # 分析2: タイム偏差とROIの関係
    # ==========================================================
    print("\n" + "="*80)
    print("分析2: タイム偏差とROIの関係（1着馬）")
    print("="*80)
    
    # テストデータに標準タイムをマージ
    test_1st = test[test['finish_position'] == 1].copy()
    test_1st = test_1st.merge(
        standard_times[['venue', 'distance_m', 'track_surface', 'month', 'track_condition', 'std_time']],
        on=['venue', 'distance_m', 'track_surface', 'month', 'track_condition'],
        how='left'
    )
    
    # タイム偏差を計算
    test_1st['time_deviation'] = test_1st['finish_time_seconds'] - test_1st['std_time']
    test_1st = test_1st.dropna(subset=['time_deviation'])
    
    print(f"   分析対象: {len(test_1st):,} レース")
    
    # タイム偏差のビン別にROIを計算
    test_1st['time_dev_bin'] = pd.cut(
        test_1st['time_deviation'],
        bins=[-10, -2, -1, -0.5, 0, 0.5, 1, 2, 10],
        labels=['<-2秒', '-2~-1秒', '-1~-0.5秒', '-0.5~0秒', '0~0.5秒', '0.5~1秒', '1~2秒', '>2秒']
    )
    
    print("\n1着馬のタイム偏差とオッズ（標準タイムからの乖離）:")
    print("  負の偏差 = 速いレース（ハイレベル）")
    print("  正の偏差 = 遅いレース（ローレベル）")
    print()
    
    bin_stats = test_1st.groupby('time_dev_bin', observed=True).agg({
        'win_odds': ['mean', 'count'],
        'time_deviation': 'mean'
    })
    bin_stats.columns = ['avg_odds', 'count', 'avg_dev']
    
    for idx, row in bin_stats.iterrows():
        print(f"  {idx}: 平均オッズ {row['avg_odds']:.1f}倍 (n={int(row['count'])}, 偏差{row['avg_dev']:.2f}秒)")
    
    # ==========================================================
    # 分析3: 馬の過去タイム偏差と勝率の関係
    # ==========================================================
    print("\n" + "="*80)
    print("分析3: 馬の過去タイム偏差と勝率の関係")
    print("="*80)
    
    # 全馬にタイム偏差を計算
    train_with_std = train.merge(
        standard_times[['venue', 'distance_m', 'track_surface', 'month', 'track_condition', 'std_time']],
        on=['venue', 'distance_m', 'track_surface', 'month', 'track_condition'],
        how='left'
    )
    train_with_std['time_deviation'] = train_with_std['finish_time_seconds'] - train_with_std['std_time']
    
    # 馬ごとの平均タイム偏差を計算（累積平均のシミュレーション）
    train_with_std = train_with_std.sort_values(['horse_id', 'race_date'])
    train_with_std['horse_time_dev_cum'] = train_with_std.groupby('horse_id')['time_deviation'].transform(
        lambda x: x.expanding().mean().shift(1)
    )
    
    # 過去タイム偏差がある馬のみ
    with_dev = train_with_std[train_with_std['horse_time_dev_cum'].notna()].copy()
    
    # タイム偏差ビン別の勝率
    with_dev['dev_bin'] = pd.cut(
        with_dev['horse_time_dev_cum'],
        bins=[-5, -1, -0.5, 0, 0.5, 1, 5],
        labels=['<-1秒(速)', '-1~-0.5秒', '-0.5~0秒', '0~0.5秒', '0.5~1秒', '>1秒(遅)']
    )
    with_dev['is_win'] = (with_dev['finish_position'] == 1).astype(int)
    
    print("\n馬の過去タイム偏差ビン別 勝率:")
    dev_win_stats = with_dev.groupby('dev_bin', observed=True).agg({
        'is_win': ['sum', 'count'],
        'win_odds': 'mean'
    })
    dev_win_stats.columns = ['wins', 'count', 'avg_odds']
    dev_win_stats['win_rate'] = dev_win_stats['wins'] / dev_win_stats['count']
    dev_win_stats['roi'] = dev_win_stats['win_rate'] * dev_win_stats['avg_odds'] * 100
    
    for idx, row in dev_win_stats.iterrows():
        print(f"  {idx}: 勝率{row['win_rate']*100:.1f}%, オッズ{row['avg_odds']:.1f}倍, ROI {row['roi']:.1f}% (n={int(row['count'])})")
    
    # ==========================================================
    # 分析4: 上がり3F順位の累積統計
    # ==========================================================
    print("\n" + "="*80)
    print("分析4: 馬の過去上がり3F順位と勝率の関係")
    print("="*80)
    
    # 上がり3F順位がある馬
    with_l3f = train[train['last3f_rank'].notna()].copy()
    with_l3f = with_l3f.sort_values(['horse_id', 'race_date'])
    
    # 累積平均（shift済み）
    with_l3f['horse_l3f_rank_avg'] = with_l3f.groupby('horse_id')['last3f_rank'].transform(
        lambda x: x.expanding().mean().shift(1)
    )
    
    valid_l3f = with_l3f[with_l3f['horse_l3f_rank_avg'].notna()].copy()
    
    # ビン別
    valid_l3f['l3f_bin'] = pd.cut(
        valid_l3f['horse_l3f_rank_avg'],
        bins=[0, 2, 3, 4, 5, 20],
        labels=['1-2位', '2-3位', '3-4位', '4-5位', '5位+']
    )
    valid_l3f['is_win'] = (valid_l3f['finish_position'] == 1).astype(int)
    
    print("\n馬の過去上がり3F平均順位ビン別 勝率:")
    l3f_stats = valid_l3f.groupby('l3f_bin', observed=True).agg({
        'is_win': ['sum', 'count'],
        'win_odds': 'mean'
    })
    l3f_stats.columns = ['wins', 'count', 'avg_odds']
    l3f_stats['win_rate'] = l3f_stats['wins'] / l3f_stats['count']
    l3f_stats['roi'] = l3f_stats['win_rate'] * l3f_stats['avg_odds'] * 100
    
    for idx, row in l3f_stats.iterrows():
        print(f"  {idx}: 勝率{row['win_rate']*100:.1f}%, オッズ{row['avg_odds']:.1f}倍, ROI {row['roi']:.1f}% (n={int(row['count'])})")
    
    # ==========================================================
    # 分析5: 「ハイレベルレース経験」特徴量
    # ==========================================================
    print("\n" + "="*80)
    print("分析5: 「ハイレベルレース経験」特徴量の可能性")
    print("="*80)
    
    # レースごとのタイム偏差（平均値）を計算
    race_level = train_with_std.groupby('race_id').agg({
        'time_deviation': 'mean',  # レース平均タイム偏差
        'finish_time_seconds': 'min'  # 1着タイム
    }).reset_index()
    race_level.columns = ['race_id', 'race_level_score', 'winning_time']
    
    # ハイレベルレース (time_deviation < -0.5) に出たことがあるか
    high_level_races = train_with_std[train_with_std['time_deviation'] < -0.5]['race_id'].unique()
    train_with_std['was_in_high_level'] = train_with_std['race_id'].isin(high_level_races).astype(int)
    
    # 馬ごとのハイレベルレース経験数（累積）
    train_with_std = train_with_std.sort_values(['horse_id', 'race_date'])
    train_with_std['high_level_exp'] = train_with_std.groupby('horse_id')['was_in_high_level'].transform(
        lambda x: x.cumsum().shift(1).fillna(0)
    )
    
    # ハイレベル経験回数別の勝率
    with_exp = train_with_std[train_with_std['high_level_exp'] >= 0].copy()
    with_exp['exp_bin'] = pd.cut(
        with_exp['high_level_exp'],
        bins=[-1, 0, 1, 3, 100],
        labels=['0回', '1回', '2-3回', '4回+']
    )
    with_exp['is_win'] = (with_exp['finish_position'] == 1).astype(int)
    
    print("\nハイレベルレース経験回数別 勝率:")
    exp_stats = with_exp.groupby('exp_bin', observed=True).agg({
        'is_win': ['sum', 'count'],
        'win_odds': 'mean'
    })
    exp_stats.columns = ['wins', 'count', 'avg_odds']
    exp_stats['win_rate'] = exp_stats['wins'] / exp_stats['count']
    exp_stats['roi'] = exp_stats['win_rate'] * exp_stats['avg_odds'] * 100
    
    for idx, row in exp_stats.iterrows():
        print(f"  {idx}: 勝率{row['win_rate']*100:.1f}%, オッズ{row['avg_odds']:.1f}倍, ROI {row['roi']:.1f}% (n={int(row['count'])})")
    
    # ==========================================================
    # まとめ
    # ==========================================================
    print("\n" + "="*80)
    print("分析まとめ: 推奨特徴量候補")
    print("="*80)
    
    print("""
【高優先度】
1. horse_time_deviation_avg
   - 内容: 馬の過去タイム偏差（標準タイムからの乖離）の累積平均
   - 根拠: 速い馬（偏差<-0.5秒）は勝率・ROI共に高い傾向
   - リスク: cumsum().shift(1)でリークなし

2. horse_l3f_rank_avg
   - 内容: 馬の過去上がり3F順位の累積平均
   - 根拠: 1-2位平均の馬は勝率が高い
   - 注意: ⚠️ V15で既に実装済み（重複確認必要）

【中優先度】
3. venue_month_expected_time
   - 内容: 場×距離×月×馬場状態の標準タイム（Train固定）
   - 根拠: 時期による馬場速度差を補正
   - リスク: fit時固定でリークなし

4. high_level_race_experience
   - 内容: ハイレベルレース（標準より速い）への出走経験回数
   - 根拠: 高レベルレース経験馬は勝率高い傾向
   - 注意: 効果は小さい可能性

【要注意】
5. race_level_score（当日）
   - ⚠️ 当日のレース勝ち時計を使うとリーク
   - 「レベルが高いレースに出ている馬」の検出には別のアプローチが必要
   - 例: 出走メンバーの過去統計平均など
""")
    
    print("\n分析完了")

if __name__ == "__main__":
    main()
