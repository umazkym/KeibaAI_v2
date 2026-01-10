# -*- coding: utf-8 -*-
"""
脚質・展開・ペース分析スクリプト

V31設計のための深い分析
- 脚質の定量化
- ペースラップと有利不利
- レース内の脚質構成
- 展開予測
"""
import pandas as pd
import numpy as np
from pathlib import Path
import warnings
warnings.filterwarnings('ignore')

project_root = Path(__file__).resolve().parent.parent.parent


def load_data():
    """データを読み込む"""
    print("データ読み込み中...")
    data_dir = project_root / "keibaai/data/parsed/parquet"
    
    races = pd.read_parquet(data_dir / "races/races.parquet")
    corners = pd.read_parquet(data_dir / "corners/corner_positions.parquet")
    race_details = pd.read_parquet(data_dir / "race_details/race_details.parquet")
    
    races = races[races['finish_position'].notna()].copy()
    races['race_date'] = pd.to_datetime(races['race_date'])
    
    return races, corners, race_details


def analyze_pace_and_style(races, corners, race_details):
    """ペースと脚質の詳細分析"""
    
    print("\n" + "=" * 80)
    print("【ペース分析】")
    print("=" * 80)
    
    # ペースデータをマージ
    pace = race_details[['race_id', 'first_half', 'second_half']].copy()
    pace = pace.dropna()
    pace['pace_diff'] = pace['second_half'] - pace['first_half']
    
    print(f"\nペースデータ: {len(pace):,}レース")
    print(f"  前半平均: {pace['first_half'].mean():.1f}秒 (σ={pace['first_half'].std():.1f})")
    print(f"  後半平均: {pace['second_half'].mean():.1f}秒 (σ={pace['second_half'].std():.1f})")
    print(f"  後半-前半（ペース差）平均: {pace['pace_diff'].mean():.1f}秒")
    
    # ペースカテゴリを定義
    pace['pace_category'] = pd.cut(pace['pace_diff'], 
        bins=[-99, -1, 1, 99],
        labels=['ハイペース', '平均ペース', 'スローペース'])
    
    print("\n【ペースカテゴリ分布】")
    print(pace['pace_category'].value_counts())
    
    # レースとマージ
    races_with_pace = races.merge(pace, on='race_id', how='left')
    
    # ===== C4位置による脚質分類 =====
    print("\n" + "=" * 80)
    print("【脚質分析（C4位置基準）】")
    print("=" * 80)
    
    c4 = corners[corners['corner'] == 4][['race_id', 'horse_number', 'position', 'gap_from_leader']].copy()
    c4.columns = ['race_id', 'horse_number', 'c4_pos', 'c4_gap']
    
    # 各レースの出走頭数を追加
    field_size = races.groupby('race_id')['horse_number'].count().reset_index()
    field_size.columns = ['race_id', 'field_size']
    
    c4 = c4.merge(field_size, on='race_id', how='left')
    c4['relative_c4'] = c4['c4_pos'] / c4['field_size']
    
    # 脚質カテゴリ
    c4['style'] = pd.cut(c4['relative_c4'],
        bins=[0, 0.2, 0.5, 0.8, 1.01],
        labels=['逃げ・先行', '好位', '中団', '追込'])
    
    print("\n【脚質分布】")
    print(c4['style'].value_counts())
    
    # レース結果とマージ
    races_with_c4 = races_with_pace.merge(c4, on=['race_id', 'horse_number'], how='left')
    
    # ===== 脚質別の勝率 =====
    print("\n【脚質別の勝率】")
    print("-" * 50)
    
    for style in ['逃げ・先行', '好位', '中団', '追込']:
        subset = races_with_c4[races_with_c4['style'] == style]
        wins = subset[subset['finish_position'] == 1]
        if len(subset) > 1000:
            win_rate = len(wins) / len(subset) * 100
            print(f"  {style}: 勝率 {win_rate:.1f}% ({len(subset):,}件)")
    
    # ===== ペース×脚質の交互作用 =====
    print("\n" + "=" * 80)
    print("【ペース×脚質 交互作用分析】")
    print("=" * 80)
    
    print("\n【脚質別×ペース別 勝率】")
    print("-" * 60)
    
    for pace_cat in ['ハイペース', '平均ペース', 'スローペース']:
        print(f"\n■ {pace_cat}")
        for style in ['逃げ・先行', '好位', '中団', '追込']:
            subset = races_with_c4[(races_with_c4['pace_category'] == pace_cat) & (races_with_c4['style'] == style)]
            wins = subset[subset['finish_position'] == 1]
            if len(subset) > 500:
                win_rate = len(wins) / len(subset) * 100
                print(f"    {style}: 勝率 {win_rate:.1f}% ({len(subset):,}件)")
    
    # ===== レース内の脚質構成 =====
    print("\n" + "=" * 80)
    print("【レース内脚質構成分析】")
    print("=" * 80)
    
    # 各レースの逃げ・先行馬の数
    front_runners = c4[c4['style'] == '逃げ・先行'].groupby('race_id').size().reset_index(name='front_count')
    races_with_c4 = races_with_c4.merge(front_runners, on='race_id', how='left')
    races_with_c4['front_count'] = races_with_c4['front_count'].fillna(0)
    
    print("\n【逃げ・先行馬の数と勝率】")
    print("-" * 50)
    
    for count in range(1, 8):
        subset = races_with_c4[(races_with_c4['front_count'] == count) & (races_with_c4['style'] == '逃げ・先行')]
        wins = subset[subset['finish_position'] == 1]
        if len(subset) > 500:
            win_rate = len(wins) / len(subset) * 100
            print(f"  逃げ先行{count}頭: 逃げ先行馬の勝率 {win_rate:.1f}% ({len(subset):,}件)")
    
    # ===== C4馬身差と着順の関係 =====
    print("\n" + "=" * 80)
    print("【C4馬身差と着順の関係】")
    print("=" * 80)
    
    print("\n【C4馬身差帯別の勝率】")
    print("-" * 50)
    
    for gap_min, gap_max, label in [(0, 1, '0-1馬身'), (1, 3, '1-3馬身'), (3, 6, '3-6馬身'), (6, 10, '6-10馬身'), (10, 99, '10馬身以上')]:
        subset = races_with_c4[(races_with_c4['c4_gap'] >= gap_min) & (races_with_c4['c4_gap'] < gap_max)]
        wins = subset[subset['finish_position'] == 1]
        if len(subset) > 1000:
            win_rate = len(wins) / len(subset) * 100
            print(f"  {label}: 勝率 {win_rate:.1f}% ({len(subset):,}件)")
    
    # ===== 脚質×上がり順位の関係 =====
    print("\n" + "=" * 80)
    print("【脚質×上がり順位の関係】")
    print("=" * 80)
    
    if 'last_3f_time' in races_with_c4.columns:
        races_with_c4['last3f_rank'] = races_with_c4.groupby('race_id')['last_3f_time'].rank(method='first')
        
        print("\n【脚質別の平均上がり順位】")
        print("-" * 50)
        
        for style in ['逃げ・先行', '好位', '中団', '追込']:
            subset = races_with_c4[races_with_c4['style'] == style]
            if len(subset) > 1000:
                avg_last3f_rank = subset['last3f_rank'].mean()
                print(f"  {style}: 平均上がり順位 {avg_last3f_rank:.1f}位")
        
        print("\n【脚質×上がり順位 勝率（逃げ先行馬が上がり上位なら強い）】")
        print("-" * 60)
        
        for style in ['逃げ・先行', '好位']:
            for rank_max in [1, 3, 5]:
                subset = races_with_c4[(races_with_c4['style'] == style) & (races_with_c4['last3f_rank'] <= rank_max)]
                wins = subset[subset['finish_position'] == 1]
                if len(subset) > 500:
                    win_rate = len(wins) / len(subset) * 100
                    print(f"  {style} & 上がり{rank_max}位以内: 勝率 {win_rate:.1f}% ({len(subset):,}件)")
    
    print("\n" + "=" * 80)
    print("分析完了")
    print("=" * 80)
    
    return races_with_c4


def main():
    races, corners, race_details = load_data()
    print(f"\n総データ: レース{len(races):,}件, コーナー{len(corners):,}件")
    
    result = analyze_pace_and_style(races, corners, race_details)
    
    print("\n【V31に向けた特徴量候補】")
    print("-" * 60)
    print("""
1. レース内脚質構成
   - race_front_runner_count_v2: レース内の逃げ先行馬数（C4基準）
   - race_closer_count: レース内の追込馬数
   - style_competition: 同脚質の競合数

2. ペース予測・適性
   - predicted_pace_category: 予想ペース（逃げ馬数から推定）
   - horse_pace_preference: 馬のペース適性（過去成績から）
   - pace_style_advantage: ペース×脚質の有利度

3. C4馬身差活用
   - horse_c4_gap_consistency: C4馬身差の安定性
   - c4_gap_to_win_ratio: C4馬身差と勝率の関係

4. 脚質×上がり交互作用
   - front_last3f_ability: 逃げ先行馬の上がり能力
   - style_adjusted_last3f: 脚質調整済み上がり順位
""")


if __name__ == "__main__":
    main()
