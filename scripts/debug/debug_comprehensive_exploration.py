"""
全データソース多角的探索スクリプト

7つのParquetファイルから未活用の特徴量を探索:
1. races.parquet - 58カラム中の未活用カラム
2. returns.parquet - 払い戻しパターン分析
3. race_details.parquet - ラップタイム詳細分析
4. corner_positions.parquet - gap_from_leader活用
5. horses.parquet - 生産地、調教師統計
6. pedigrees.parquet - 血統の別活用方法

各カラムについて:
- 非NaN率
- 勝者と非勝者の分布差
- オッズとの相関
- ROIへの寄与可能性
"""

import sys
from pathlib import Path
import pandas as pd
import numpy as np

project_root = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(project_root))

def analyze_column_roi(df, col, target='is_win', odds_col='win_odds'):
    """カラムのROI効果を分析"""
    if df[col].isna().all():
        return None
    
    # 数値カラムの場合は四分位で分析
    if pd.api.types.is_numeric_dtype(df[col]):
        try:
            df['_quartile'] = pd.qcut(df[col].dropna(), 4, labels=['Q1', 'Q2', 'Q3', 'Q4'], duplicates='drop')
        except ValueError:
            # ビン数不足の場合はスキップ
            return None
        result = df.groupby('_quartile', observed=True).apply(
            lambda x: pd.Series({
                'count': len(x),
                'win_rate': x[target].mean() if target in x else np.nan,
                'avg_odds': x[odds_col].mean() if odds_col in x else np.nan,
                'roi': (x[x[target] == 1][odds_col].sum() * 100 / (len(x) * 100) * 100) if target in x and odds_col in x else np.nan
            }), include_groups=False
        )
        df.drop(columns=['_quartile'], inplace=True, errors='ignore')
        return result
    else:
        # カテゴリカラムの場合はグループ別
        result = df.groupby(col).apply(
            lambda x: pd.Series({
                'count': len(x),
                'win_rate': x[target].mean() if target in x else np.nan,
                'avg_odds': x[odds_col].mean() if odds_col in x else np.nan,
                'roi': (x[x[target] == 1][odds_col].sum() * 100 / (len(x) * 100) * 100) if target in x and odds_col in x else np.nan
            }), include_groups=False
        )
        return result.sort_values('roi', ascending=False).head(20)


def main():
    data_dir = Path("keibaai/data/parsed/parquet")
    
    print("="*100)
    print("全データソース多角的探索")
    print("="*100)
    
    # =================================================================
    # 1. races.parquetの未活用カラム分析
    # =================================================================
    print("\n" + "="*100)
    print("1. races.parquet 未活用カラム分析")
    print("="*100)
    
    races = pd.read_parquet(data_dir / "races/races.parquet")
    races['race_date'] = pd.to_datetime(races['race_date'])
    races = races[races['race_date'] >= '2020-01-01']
    races = races.dropna(subset=['finish_position', 'win_odds'])
    races['is_win'] = (races['finish_position'] == 1).astype(int)
    
    print(f"\n総レコード: {len(races):,}")
    print(f"総カラム: {len(races.columns)}")
    
    # V8.1で使用されている特徴量
    v81_used = [
        'distance_m', 'track_surface', 'track_condition', 'venue', 'race_class',
        'bracket_number', 'horse_number', 'sex', 'age', 'basis_weight', 
        'horse_weight', 'jockey_id', 'trainer_id', 'horse_id', 
        'finish_position', 'win_odds', 'popularity', 'last_3f_time',
        'race_id', 'race_date', 'horse_name', 'jockey_name', 'trainer_name'
    ]
    
    # 未活用カラム
    unused_cols = [c for c in races.columns if c not in v81_used and c != 'is_win']
    print(f"\n未活用カラム ({len(unused_cols)}個):")
    
    for col in unused_cols:
        non_null = races[col].notna().sum()
        non_null_rate = non_null / len(races) * 100
        dtype = races[col].dtype
        print(f"  {col}: {dtype}, 非NaN={non_null_rate:.1f}%")
    
    # 有望そうな未活用カラムの詳細分析
    interesting_cols = [
        'passing_order_1', 'passing_order_2', 'passing_order_3', 'passing_order_4',
        'pace_index', 'last3f_rank', 'position_change_1_2', 'position_change_2_3',
        'position_change_3_4', 'final_corner_to_finish', 'horse_weight_deviation',
        'popularity_finish_diff', 'relative_odds', 'prize_money'
    ]
    
    print("\n有望カラムの詳細分析:")
    for col in interesting_cols:
        if col in races.columns:
            non_null_rate = races[col].notna().mean() * 100
            if non_null_rate > 0:
                print(f"\n  [{col}] 非NaN={non_null_rate:.1f}%")
                result = analyze_column_roi(races, col)
                if result is not None and len(result) > 0:
                    print(result.to_string())
    
    # =================================================================
    # 2. returns.parquet - 高配当馬の特徴分析
    # =================================================================
    print("\n" + "="*100)
    print("2. returns.parquet 高配当馬の特徴分析")
    print("="*100)
    
    returns = pd.read_parquet(data_dir / "returns/returns.parquet")
    
    # 単勝データを取得
    tansho = returns[returns['bet_type'] == 'tansho'][['race_id', 'horse_number', 'payout']].copy()
    print(f"\n単勝データ: {len(tansho):,}件")
    
    # 高配当（万馬券以上）の分析
    high_payout = tansho[tansho['payout'] >= 10000]
    print(f"万馬券以上: {len(high_payout):,}件 ({len(high_payout)/len(tansho)*100:.1f}%)")
    
    # 高配当馬の特徴を分析
    tansho_merged = races.merge(
        tansho.rename(columns={'payout': 'actual_payout'}),
        on=['race_id', 'horse_number'],
        how='left'
    )
    
    # 高配当馬の特徴
    high_payout_horses = tansho_merged[tansho_merged['actual_payout'] >= 10000]
    normal_winners = tansho_merged[(tansho_merged['is_win'] == 1) & (tansho_merged['actual_payout'] < 10000)]
    
    if len(high_payout_horses) > 0:
        print(f"\n万馬券勝馬の特徴比較:")
        compare_cols = ['popularity', 'age', 'horse_weight', 'last_3f_time']
        for col in compare_cols:
            if col in high_payout_horses.columns:
                hp_mean = high_payout_horses[col].mean()
                nw_mean = normal_winners[col].mean()
                print(f"  {col}: 万馬券={hp_mean:.1f}, 通常勝馬={nw_mean:.1f}, 差={hp_mean-nw_mean:+.1f}")
    
    # =================================================================
    # 3. race_details.parquet - ラップタイム詳細分析
    # =================================================================
    print("\n" + "="*100)
    print("3. race_details.parquet ラップタイム詳細分析")
    print("="*100)
    
    race_details = pd.read_parquet(data_dir / "race_details/race_details.parquet")
    print(f"\nレース詳細: {len(race_details):,}件")
    
    # ペース傾向（前半-後半）を計算
    race_details['pace_diff'] = race_details['first_half'] - race_details['second_half']
    
    print(f"\nペース差(前半-後半)の分布:")
    print(f"  平均: {race_details['pace_diff'].mean():.2f}秒")
    print(f"  標準偏差: {race_details['pace_diff'].std():.2f}秒")
    
    # レースにマージ
    races_with_pace = races.merge(
        race_details[['race_id', 'first_half', 'second_half', 'pace_diff']],
        on='race_id',
        how='left'
    )
    
    # ペースタイプ別のROI
    races_with_pace['pace_type'] = pd.cut(
        races_with_pace['pace_diff'],
        bins=[-999, -2, 2, 999],
        labels=['後傾(スロー)', '平均', '前傾(ハイ)']
    )
    
    print(f"\nペースタイプ別の分析:")
    for pace_type in ['後傾(スロー)', '平均', '前傾(ハイ)']:
        subset = races_with_pace[races_with_pace['pace_type'] == pace_type]
        if len(subset) > 0:
            win_rate = subset['is_win'].mean()
            wins = subset[subset['is_win'] == 1]
            roi = wins['win_odds'].sum() * 100 / (len(subset) * 100) * 100
            print(f"  {pace_type}: n={len(subset):,}, 勝率={win_rate:.1%}, ROI={roi:.1f}%")
    
    # =================================================================
    # 4. corner_positions.parquet - gap_from_leader詳細分析
    # =================================================================
    print("\n" + "="*100)
    print("4. corner_positions.parquet gap_from_leader活用")
    print("="*100)
    
    corners = pd.read_parquet(data_dir / "corners/corner_positions.parquet")
    print(f"\nコーナーデータ: {len(corners):,}件")
    
    # 4コーナーのgap_from_leaderを分析
    c4 = corners[corners['corner'] == 4][['race_id', 'horse_number', 'gap_from_leader']].copy()
    
    races_with_gap = races.merge(c4, on=['race_id', 'horse_number'], how='left')
    
    # gap_from_leader別のROI
    races_with_gap['gap_category'] = pd.cut(
        races_with_gap['gap_from_leader'],
        bins=[-1, 0, 2, 5, 10, 999],
        labels=['先頭', '1-2馬身', '2-5馬身', '5-10馬身', '10馬身+']
    )
    
    print(f"\n4コーナーgap_from_leader別の分析:")
    for gap in ['先頭', '1-2馬身', '2-5馬身', '5-10馬身', '10馬身+']:
        subset = races_with_gap[races_with_gap['gap_category'] == gap]
        if len(subset) > 100:
            win_rate = subset['is_win'].mean()
            wins = subset[subset['is_win'] == 1]
            roi = wins['win_odds'].sum() * 100 / (len(subset) * 100) * 100
            avg_odds = subset['win_odds'].mean()
            print(f"  {gap}: n={len(subset):,}, 勝率={win_rate:.1%}, 平均オッズ={avg_odds:.1f}倍, ROI={roi:.1f}%")
    
    # =================================================================
    # 5. horses.parquet - 生産地分析
    # =================================================================
    print("\n" + "="*100)
    print("5. horses.parquet 生産地・繁殖者分析")
    print("="*100)
    
    horses = pd.read_parquet(data_dir / "horses/horses.parquet")
    print(f"\n馬データ: {len(horses):,}頭")
    print(f"カラム: {list(horses.columns)}")
    
    # 生産地別統計
    races_with_area = races.merge(
        horses[['horse_id', 'producing_area', 'breeder_name']],
        on='horse_id',
        how='left'
    )
    
    print(f"\n生産地別の分析 (Top 10):")
    area_stats = races_with_area.groupby('producing_area').apply(
        lambda x: pd.Series({
            'count': len(x),
            'win_rate': x['is_win'].mean(),
            'roi': (x[x['is_win'] == 1]['win_odds'].sum() * 100 / (len(x) * 100) * 100)
        })
    ).sort_values('roi', ascending=False)
    area_stats = area_stats[area_stats['count'] >= 100]
    print(area_stats.head(10).to_string())
    
    # =================================================================
    # 6. 複合条件の探索
    # =================================================================
    print("\n" + "="*100)
    print("6. 複合条件の探索（Test期間）")
    print("="*100)
    
    test = races[races['race_date'] >= '2025-01-01'].copy()
    
    # 条件組み合わせ
    conditions = []
    
    # 条件1: 人気と最終着順のギャップ
    # popularityがオッズ順位なので、低い=人気
    # 人気が高いのに勝率が低いパターンを探す
    
    # 条件2: 馬体重変動
    if 'horse_weight_change' in test.columns:
        test['weight_trend'] = pd.cut(
            test['horse_weight_change'],
            bins=[-999, -5, 5, 999],
            labels=['減量', '維持', '増量']
        )
        
        print(f"\n馬体重変動別:")
        for trend in ['減量', '維持', '増量']:
            subset = test[test['weight_trend'] == trend]
            if len(subset) > 100:
                wins = subset[subset['is_win'] == 1]
                roi = wins['win_odds'].sum() * 100 / (len(subset) * 100) * 100
                print(f"  {trend}: n={len(subset):,}, ROI={roi:.1f}%")
    
    # 条件3: オッズと人気の乖離
    if 'popularity' in test.columns:
        # 人気よりオッズが高い馬（過小評価）
        test['odds_vs_pop'] = test['win_odds'] / test['popularity']
        
        print(f"\nオッズ/人気比率別:")
        for low, high, label in [(0, 3, '妥当'), (3, 6, 'やや過小評価'), (6, 20, '過小評価'), (20, 999, '大穴')]:
            subset = test[(test['odds_vs_pop'] >= low) & (test['odds_vs_pop'] < high)]
            if len(subset) > 100:
                wins = subset[subset['is_win'] == 1]
                roi = wins['win_odds'].sum() * 100 / (len(subset) * 100) * 100
                win_rate = subset['is_win'].mean()
                print(f"  {label}: n={len(subset):,}, 勝率={win_rate:.1%}, ROI={roi:.1f}%")
    
    # 条件4: 天候・馬場状態
    if 'weather' in test.columns and 'track_condition' in test.columns:
        print(f"\n天候×馬場状態:")
        for weather in test['weather'].dropna().unique():
            for condition in test['track_condition'].dropna().unique():
                subset = test[(test['weather'] == weather) & (test['track_condition'] == condition)]
                if len(subset) > 100:
                    wins = subset[subset['is_win'] == 1]
                    roi = wins['win_odds'].sum() * 100 / (len(subset) * 100) * 100
                    if roi > 90 or roi < 60:  # 極端なROIのみ表示
                        print(f"  {weather}×{condition}: n={len(subset):,}, ROI={roi:.1f}%")


if __name__ == '__main__':
    main()
