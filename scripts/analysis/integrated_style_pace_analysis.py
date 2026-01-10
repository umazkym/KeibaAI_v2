# -*- coding: utf-8 -*-
"""
C1-C4 + 上がり3ハロン + ペースラップ 統合分析

多角的に馬の性質と展開を分析
※ペースラップは先頭馬のラップであることに注意
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


def analyze_integrated_style(races, corners, race_details):
    """C1-C4 + 上がり + ペースの統合分析"""
    
    print("\n" + "=" * 80)
    print("【C1-C4 + 上がり3ハロン + ペースラップ 統合分析】")
    print("=" * 80)
    
    # 出走頭数
    field_size = races.groupby('race_id')['horse_number'].count().reset_index()
    field_size.columns = ['race_id', 'field_size']
    
    # コーナーデータをピボット
    corner_pivot = corners.pivot_table(
        index=['race_id', 'horse_number'],
        columns='corner',
        values=['position', 'gap_from_leader']
    ).reset_index()
    corner_pivot.columns = [f'{col[0]}_c{col[1]}' if isinstance(col, tuple) and col[1] else col[0] for col in corner_pivot.columns]
    
    # レースデータとマージ
    data = races[['race_id', 'horse_number', 'horse_id', 'finish_position', 
                  'win_odds', 'last_3f_time', 'popularity']].copy()
    data = data.merge(field_size, on='race_id', how='left')
    data = data.merge(corner_pivot, on=['race_id', 'horse_number'], how='left')
    
    # ペースデータをマージ
    pace = race_details[['race_id', 'first_half', 'second_half']].copy()
    pace = pace.dropna()
    pace['pace_diff'] = pace['second_half'] - pace['first_half']
    pace['pace_category'] = pd.cut(pace['pace_diff'], 
        bins=[-99, -1, 1, 99], labels=['ハイペース', '平均', 'スロー'])
    data = data.merge(pace, on='race_id', how='left')
    
    # 相対位置を計算
    for c in [1, 2, 3, 4]:
        pos_col = f'position_c{c}'
        if pos_col in data.columns:
            data[f'relative_c{c}'] = data[pos_col] / data['field_size']
    
    # C1-C4平均相対位置（脚質スコア）
    rel_cols = [f'relative_c{c}' for c in [1, 2, 3, 4] if f'relative_c{c}' in data.columns]
    data['style_score'] = data[rel_cols].mean(axis=1)
    
    # 位置変動
    if 'relative_c1' in data.columns and 'relative_c4' in data.columns:
        data['position_change'] = data['relative_c4'] - data['relative_c1']
    
    # 上がり順位
    data['last3f_rank'] = data.groupby('race_id')['last_3f_time'].rank(method='first')
    data['relative_last3f'] = data['last3f_rank'] / data['field_size']
    
    # ===== 分析1: C4位置と上がり順位の組み合わせ =====
    print("\n【C4位置 × 上がり順位 の勝率】")
    print("-" * 60)
    print("※ペースラップは先頭馬のものなので、各馬の位置により展開の受け方が異なる")
    
    # C4位置を3分類、上がり順位を3分類
    data['c4_category'] = pd.cut(data['relative_c4'], bins=[0, 0.33, 0.66, 1.01], labels=['前方', '中団', '後方'])
    data['last3f_category'] = pd.cut(data['relative_last3f'], bins=[0, 0.33, 0.66, 1.01], labels=['上がり速', '上がり中', '上がり遅'])
    
    print("\n          | 上がり速 | 上がり中 | 上がり遅 |")
    print("-" * 50)
    
    for c4_cat in ['前方', '中団', '後方']:
        row = f"{c4_cat:>6} |"
        for last3f_cat in ['上がり速', '上がり中', '上がり遅']:
            subset = data[(data['c4_category'] == c4_cat) & (data['last3f_category'] == last3f_cat)]
            wins = subset[subset['finish_position'] == 1]
            if len(subset) > 500:
                win_rate = len(wins) / len(subset) * 100
                row += f" {win_rate:>6.1f}% |"
            else:
                row += "    N/A |"
        print(row)
    
    # ===== 分析2: ペース × C4位置 × 上がり =====
    print("\n【ペース × C4位置 × 上がり の勝率】")
    print("-" * 60)
    
    for pace_cat in ['ハイペース', '平均', 'スロー']:
        print(f"\n■ {pace_cat}")
        print("          | 上がり速 | 上がり中 | 上がり遅 |")
        print("-" * 50)
        for c4_cat in ['前方', '中団', '後方']:
            row = f"{c4_cat:>6} |"
            for last3f_cat in ['上がり速', '上がり中', '上がり遅']:
                subset = data[
                    (data['pace_category'] == pace_cat) & 
                    (data['c4_category'] == c4_cat) & 
                    (data['last3f_category'] == last3f_cat)
                ]
                wins = subset[subset['finish_position'] == 1]
                if len(subset) > 300:
                    win_rate = len(wins) / len(subset) * 100
                    row += f" {win_rate:>6.1f}% |"
                else:
                    row += "    N/A |"
            print(row)
    
    # ===== 分析3: 位置変動 × 上がり =====
    print("\n【位置変動（C1→C4） × 上がり順位 の勝率・ROI】")
    print("-" * 60)
    
    data['change_category'] = pd.cut(data['position_change'], 
        bins=[-1, -0.15, 0.1, 1], labels=['追上げ型', '維持型', '後退型'])
    
    for change in ['追上げ型', '維持型', '後退型']:
        print(f"\n■ {change}")
        for last3f_cat in ['上がり速', '上がり中', '上がり遅']:
            subset = data[(data['change_category'] == change) & (data['last3f_category'] == last3f_cat)]
            wins = subset[subset['finish_position'] == 1]
            if len(subset) > 500:
                win_rate = len(wins) / len(subset) * 100
                roi = wins['win_odds'].sum() / len(subset) * 100
                print(f"    {last3f_cat}: 勝率 {win_rate:.1f}%, ROI {roi:.1f}% ({len(subset):,}件)")
    
    # ===== 分析4: 先頭馬（C4=1位）の上がりとペースの関係 =====
    print("\n" + "=" * 80)
    print("【先頭馬分析（ペースラップの主体）】")
    print("=" * 80)
    print("※ペースラップは主に先頭馬が刻むため、先頭馬の分析が重要")
    
    front_runners = data[data['position_c4'] == 1].copy()
    print(f"\n先頭馬（C4=1位）: {len(front_runners):,}件")
    print(f"  勝率: {(front_runners['finish_position']==1).mean()*100:.1f}%")
    print(f"  平均上がり順位: {front_runners['last3f_rank'].mean():.1f}位")
    
    print("\n【先頭馬が上がり○位だった時の勝率】")
    for rank in range(1, 6):
        subset = front_runners[front_runners['last3f_rank'] == rank]
        if len(subset) > 100:
            win_rate = (subset['finish_position'] == 1).mean() * 100
            print(f"  先頭馬&上がり{rank}位: 勝率 {win_rate:.1f}% ({len(subset):,}件)")
    
    # ===== 分析5: 各馬の脚質×ペース適性 =====
    print("\n" + "=" * 80)
    print("【脚質スコア × ペース適性 分析】")
    print("=" * 80)
    
    data['style_quintile'] = pd.qcut(data['style_score'], 5, 
        labels=['Q1(前)', 'Q2', 'Q3', 'Q4', 'Q5(後)'], duplicates='drop')
    
    print("\n【脚質5分位 × ペース別 勝率】")
    print("-" * 60)
    print("            | ハイペース |   平均  |  スロー |")
    print("-" * 60)
    
    for style in ['Q1(前)', 'Q2', 'Q3', 'Q4', 'Q5(後)']:
        row = f"{style:>8} |"
        for pace_cat in ['ハイペース', '平均', 'スロー']:
            subset = data[(data['style_quintile'] == style) & (data['pace_category'] == pace_cat)]
            wins = subset[subset['finish_position'] == 1]
            if len(subset) > 300:
                win_rate = len(wins) / len(subset) * 100
                row += f"   {win_rate:>5.1f}% |"
            else:
                row += "     N/A |"
        print(row)
    
    print("\n" + "=" * 80)
    print("分析完了")
    print("=" * 80)
    
    print("\n【V31への示唆】")
    print("-" * 60)
    print("""
1. C4位置×上がり順位の組み合わせが非常に有効
   → 前方×上がり速 = 最強パターン

2. ペースによって脚質有利度が変化
   → ハイペース時は中団・後方×上がり速が有利
   → スローペース時は前方維持が有利

3. 位置変動×上がりの組み合わせ
   → 追い上げ型×上がり速 = 高勝率
   → 後退型 = 厳しい

4. 先頭馬の上がり順位が重要
   → 先頭馬&上がり1位 = 非常に高勝率
   → ペースの主体と上がりの関係

5. 特徴量案:
   - front_runner_last3f_rank: 先頭馬の上がり順位（展開難易度）
   - style_last3f_combo: 脚質×上がりの複合スコア
   - pace_style_match: ペース×脚質の適合度
""")


def main():
    races, corners, race_details = load_data()
    print(f"\n総データ: レース{len(races):,}件")
    
    analyze_integrated_style(races, corners, race_details)


if __name__ == "__main__":
    main()
