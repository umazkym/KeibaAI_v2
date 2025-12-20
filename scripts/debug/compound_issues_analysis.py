"""
複合問題の深堀り分析

モデル精度に影響する可能性のある複合問題を調査
"""

import pandas as pd
import numpy as np
from pathlib import Path

DATA_DIR = Path(r"c:\Users\zk-ht\Keiba\Keiba_AI_v2\keibaai\data\parsed\parquet")

def main():
    print("=" * 80)
    print("複合問題の深堀り分析")
    print("=" * 80)
    
    races = pd.read_parquet(DATA_DIR / 'races' / 'races.parquet')
    corners = pd.read_parquet(DATA_DIR / 'corners' / 'corner_positions.parquet')
    
    races['race_date'] = pd.to_datetime(races['race_date'])
    races['year'] = races['race_date'].dt.year
    
    # 平地レースのみ
    flat_races = races[~races['race_name'].str.contains('障害', na=False)]
    
    # ===============================
    # 1. 脚質統計の不完全性分析
    # ===============================
    print("\n" + "=" * 80)
    print("1. 脚質統計の不完全性分析")
    print("=" * 80)
    
    # 各馬のpassing_order_1の有効率を計算
    horse_po_coverage = flat_races.groupby('horse_id').agg({
        'passing_order_1': lambda x: x.notna().sum() / len(x) * 100,
        'race_id': 'count'
    }).rename(columns={'passing_order_1': 'po1_coverage', 'race_id': 'race_count'})
    
    print(f"\n【馬別passing_order_1カバレッジ分布】")
    print(f"  100%カバー: {(horse_po_coverage['po1_coverage'] == 100).sum():,} 馬")
    print(f"  90-99%カバー: {((horse_po_coverage['po1_coverage'] >= 90) & (horse_po_coverage['po1_coverage'] < 100)).sum():,} 馬")
    print(f"  50-89%カバー: {((horse_po_coverage['po1_coverage'] >= 50) & (horse_po_coverage['po1_coverage'] < 90)).sum():,} 馬")
    print(f"  50%未満: {(horse_po_coverage['po1_coverage'] < 50).sum():,} 馬")
    
    # 低カバレッジ馬の特徴
    low_coverage_horses = horse_po_coverage[horse_po_coverage['po1_coverage'] < 90]
    print(f"\n低カバレッジ馬（90%未満）: {len(low_coverage_horses):,} 馬")
    
    if len(low_coverage_horses) > 0:
        print(f"  平均出走回数: {low_coverage_horses['race_count'].mean():.1f}")
        print(f"  中央値出走回数: {low_coverage_horses['race_count'].median():.1f}")
    
    # ===============================
    # 2. 短距離専門馬のC4特徴量問題
    # ===============================
    print("\n" + "=" * 80)
    print("2. 短距離専門馬のC4特徴量問題")
    print("=" * 80)
    
    # 馬別の平均距離を計算
    horse_distance = flat_races.groupby('horse_id').agg({
        'distance_m': ['mean', 'min', 'max', 'count'],
        'passing_order_4': lambda x: x.notna().sum()
    })
    horse_distance.columns = ['avg_dist', 'min_dist', 'max_dist', 'race_count', 'po4_valid']
    horse_distance['po4_coverage'] = horse_distance['po4_valid'] / horse_distance['race_count'] * 100
    
    # 距離カテゴリ別分析
    sprinters = horse_distance[horse_distance['max_dist'] <= 1400]
    milers = horse_distance[(horse_distance['min_dist'] >= 1400) & (horse_distance['max_dist'] <= 1800)]
    stayers = horse_distance[horse_distance['min_dist'] >= 1800]
    versatile = horse_distance[(horse_distance['min_dist'] <= 1400) & (horse_distance['max_dist'] >= 1800)]
    
    print(f"\n【距離適性別C4カバレッジ】")
    print(f"  スプリンター(max≤1400m): {len(sprinters):,} 馬, 平均C4カバレッジ: {sprinters['po4_coverage'].mean():.1f}%")
    print(f"  マイラー(1400-1800m only): {len(milers):,} 馬, 平均C4カバレッジ: {milers['po4_coverage'].mean():.1f}%")
    print(f"  ステイヤー(min≥1800m): {len(stayers):,} 馬, 平均C4カバレッジ: {stayers['po4_coverage'].mean():.1f}%")
    print(f"  万能型(1400m以下〜1800m以上): {len(versatile):,} 馬, 平均C4カバレッジ: {versatile['po4_coverage'].mean():.1f}%")
    
    # ===============================
    # 3. 新馬・若馬の過去走不足問題
    # ===============================
    print("\n" + "=" * 80)
    print("3. 新馬・若馬の過去走不足問題")
    print("=" * 80)
    
    # 馬別の初出走日と出走回数
    horse_career = flat_races.groupby('horse_id').agg({
        'race_date': ['min', 'max', 'count'],
        'finish_position': ['mean', 'std']
    })
    horse_career.columns = ['first_race', 'last_race', 'total_races', 'avg_finish', 'finish_std']
    
    # 出走回数別分布
    print(f"\n【馬別出走回数分布】")
    print(f"  1走のみ: {(horse_career['total_races'] == 1).sum():,} 馬")
    print(f"  2-5走: {((horse_career['total_races'] >= 2) & (horse_career['total_races'] <= 5)).sum():,} 馬")
    print(f"  6-10走: {((horse_career['total_races'] >= 6) & (horse_career['total_races'] <= 10)).sum():,} 馬")
    print(f"  11走以上: {(horse_career['total_races'] > 10).sum():,} 馬")
    
    # 1-2走の馬が全体レースに占める割合
    few_races_horses = horse_career[horse_career['total_races'] <= 2].index
    few_races_records = flat_races[flat_races['horse_id'].isin(few_races_horses)]
    
    print(f"\n【過去走不足問題】")
    print(f"  1-2走のみの馬: {len(few_races_horses):,} 馬")
    print(f"  これらの馬の出走記録数: {len(few_races_records):,} ({len(few_races_records)/len(flat_races)*100:.1f}%)")
    print(f"  → これらの馬は過去走統計が不完全なため、特徴量欠損が多い可能性")
    
    # ===============================
    # 4. 年代間の騎手・調教師統計の断絶
    # ===============================
    print("\n" + "=" * 80)
    print("4. 年代間の騎手・調教師統計の断絶")
    print("=" * 80)
    
    old_data = flat_races[flat_races['year'] <= 2019]
    new_data = flat_races[flat_races['year'] >= 2020]
    
    # 2020年以降に新規参入した騎手
    old_jockeys = set(old_data['jockey_id'].dropna().unique())
    new_jockeys = set(new_data['jockey_id'].dropna().unique())
    new_only_jockeys = new_jockeys - old_jockeys
    
    print(f"\n【2020年以降新規参入騎手】")
    print(f"  新規騎手数: {len(new_only_jockeys)}")
    
    # これらの騎手の出走記録数
    new_jockey_records = new_data[new_data['jockey_id'].isin(new_only_jockeys)]
    print(f"  出走記録数: {len(new_jockey_records):,} ({len(new_jockey_records)/len(new_data)*100:.1f}%)")
    
    # 逆に2019年以前のみ活動した騎手
    old_only_jockeys = old_jockeys - new_jockeys
    print(f"\n【2019年以前のみ活動騎手】")
    print(f"  騎手数: {len(old_only_jockeys)}")
    old_jockey_records = old_data[old_data['jockey_id'].isin(old_only_jockeys)]
    print(f"  出走記録数: {len(old_jockey_records):,} ({len(old_jockey_records)/len(old_data)*100:.1f}%)")
    
    # ===============================
    # 5. 特定条件でのデータ偏り
    # ===============================
    print("\n" + "=" * 80)
    print("5. 特定条件でのデータ偏り")
    print("=" * 80)
    
    # クラス別分布の年代比較
    print(f"\n【クラス別分布の年代比較】")
    old_class = old_data['race_class'].value_counts(normalize=True) * 100
    new_class = new_data['race_class'].value_counts(normalize=True) * 100
    
    all_classes = set(old_class.index) | set(new_class.index)
    
    for cls in sorted(all_classes):
        old_pct = old_class.get(cls, 0)
        new_pct = new_class.get(cls, 0)
        diff = new_pct - old_pct
        if abs(diff) > 1:
            flag = "⚠️"
        else:
            flag = ""
        if old_pct > 1 or new_pct > 1:  # 1%以上のみ表示
            print(f"  {cls}: {old_pct:.1f}% → {new_pct:.1f}% ({diff:+.1f}%) {flag}")
    
    # ===============================
    # 6. 勝率・オッズ分布の年代比較
    # ===============================
    print("\n" + "=" * 80)
    print("6. 勝率・オッズ分布の年代比較")
    print("=" * 80)
    
    # 人気別的中率
    print(f"\n【人気別1着率の比較】")
    for pop in range(1, 6):
        old_pop = old_data[old_data['popularity'] == pop]
        new_pop = new_data[new_data['popularity'] == pop]
        
        old_win_rate = (old_pop['finish_position'] == 1).mean() * 100
        new_win_rate = (new_pop['finish_position'] == 1).mean() * 100
        diff = new_win_rate - old_win_rate
        
        flag = "⚠️" if abs(diff) > 2 else ""
        print(f"  {pop}番人気: {old_win_rate:.1f}% → {new_win_rate:.1f}% ({diff:+.1f}%) {flag}")
    
    # オッズ分布
    print(f"\n【オッズ分布の比較】")
    for low, high in [(1, 2), (2, 5), (5, 10), (10, 20), (20, 50), (50, 100)]:
        old_odds = old_data[(old_data['win_odds'] >= low) & (old_data['win_odds'] < high)]
        new_odds = new_data[(new_data['win_odds'] >= low) & (new_data['win_odds'] < high)]
        
        old_pct = len(old_odds) / len(old_data) * 100
        new_pct = len(new_odds) / len(new_data) * 100
        diff = new_pct - old_pct
        
        flag = "⚠️" if abs(diff) > 2 else ""
        print(f"  {low}-{high}倍: {old_pct:.1f}% → {new_pct:.1f}% ({diff:+.1f}%) {flag}")
    
    print("\n" + "=" * 80)
    print("分析完了")
    print("=" * 80)


if __name__ == '__main__':
    main()
