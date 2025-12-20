"""
コーナー特徴量・脚質統計の詳細分析

V15モデルでの使用状況と影響度を分析
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import pandas as pd
import numpy as np

DATA_DIR = Path(r"c:\Users\zk-ht\Keiba\Keiba_AI_v2\keibaai\data\parsed\parquet")


def main():
    print("=" * 80)
    print("コーナー特徴量・脚質統計の詳細分析")
    print("=" * 80)
    
    races = pd.read_parquet(DATA_DIR / 'races' / 'races.parquet')
    races['race_date'] = pd.to_datetime(races['race_date'])
    races['year'] = races['race_date'].dt.year
    
    # 平地レースのみ
    flat_races = races[~races['race_name'].str.contains('障害', na=False)]
    
    # ===============================
    # 1. passing_orderの現状
    # ===============================
    print("\n" + "=" * 80)
    print("1. passing_orderカラムの現状")
    print("=" * 80)
    
    for col in ['passing_order_1', 'passing_order_2', 'passing_order_3', 'passing_order_4']:
        if col in flat_races.columns:
            null_count = flat_races[col].isna().sum()
            null_pct = null_count / len(flat_races) * 100
            print(f"  {col}: {null_count:,} NULL ({null_pct:.1f}%)")
    
    # ===============================
    # 2. 距離別の欠損状況
    # ===============================
    print("\n" + "=" * 80)
    print("2. 距離別のpassing_order欠損率")
    print("=" * 80)
    
    distances = [1000, 1200, 1400, 1600, 1800, 2000, 2200, 2400]
    
    print(f"\n{'距離':<8} {'PO1欠損':>10} {'PO2欠損':>10} {'PO3欠損':>10} {'PO4欠損':>10} {'レコード数':>12}")
    print("-" * 70)
    
    for dist in distances:
        dist_data = flat_races[flat_races['distance_m'] == dist]
        if len(dist_data) > 0:
            po1_null = dist_data['passing_order_1'].isna().mean() * 100
            po2_null = dist_data['passing_order_2'].isna().mean() * 100
            po3_null = dist_data['passing_order_3'].isna().mean() * 100 if 'passing_order_3' in dist_data else 100
            po4_null = dist_data['passing_order_4'].isna().mean() * 100 if 'passing_order_4' in dist_data else 100
            print(f"{dist}m   {po1_null:>8.1f}% {po2_null:>8.1f}% {po3_null:>8.1f}% {po4_null:>8.1f}% {len(dist_data):>12,}")
    
    # ===============================
    # 3. 新潟直線コースの影響
    # ===============================
    print("\n" + "=" * 80)
    print("3. 新潟直線コース（1000m芝）の影響")
    print("=" * 80)
    
    niigata_1000 = flat_races[(flat_races['venue'] == '新潟') & 
                              (flat_races['distance_m'] == 1000) &
                              (flat_races['track_surface'] == '芝')]
    
    print(f"\n新潟1000m芝レコード数: {len(niigata_1000):,}")
    print(f"ユニークレース数: {niigata_1000['race_id'].nunique()}")
    print(f"ユニーク馬数: {niigata_1000['horse_id'].nunique()}")
    
    # これらの馬の他レースでの出走
    straight_horses = set(niigata_1000['horse_id'].unique())
    other_races = flat_races[flat_races['horse_id'].isin(straight_horses) & 
                             ~((flat_races['venue'] == '新潟') & (flat_races['distance_m'] == 1000))]
    
    print(f"\n直線コース出走馬の他レース:")
    print(f"  レコード数: {len(other_races):,}")
    print(f"  全体に占める割合: {len(other_races)/len(flat_races)*100:.1f}%")
    
    # ===============================
    # 4. 脚質特徴量の計算への影響
    # ===============================
    print("\n" + "=" * 80)
    print("4. 脚質特徴量への影響分析")
    print("=" * 80)
    
    # 馬ごとのpassing_order_1のカバレッジを計算
    horse_po_coverage = flat_races.groupby('horse_id').agg({
        'passing_order_1': lambda x: x.notna().sum(),
        'passing_order_4': lambda x: x.notna().sum(),
        'race_id': 'count'
    }).rename(columns={
        'passing_order_1': 'po1_valid_count',
        'passing_order_4': 'po4_valid_count',
        'race_id': 'total_races'
    })
    
    horse_po_coverage['po1_coverage'] = horse_po_coverage['po1_valid_count'] / horse_po_coverage['total_races'] * 100
    horse_po_coverage['po4_coverage'] = horse_po_coverage['po4_valid_count'] / horse_po_coverage['total_races'] * 100
    
    print("\n【馬別PO1カバレッジ分布】")
    print(f"  100%: {(horse_po_coverage['po1_coverage'] == 100).sum():,} 馬")
    print(f"  90-99%: {((horse_po_coverage['po1_coverage'] >= 90) & (horse_po_coverage['po1_coverage'] < 100)).sum():,} 馬")
    print(f"  50-89%: {((horse_po_coverage['po1_coverage'] >= 50) & (horse_po_coverage['po1_coverage'] < 90)).sum():,} 馬")
    print(f"  50%未満: {(horse_po_coverage['po1_coverage'] < 50).sum():,} 馬")
    
    print("\n【馬別PO4カバレッジ分布】")
    print(f"  100%: {(horse_po_coverage['po4_coverage'] == 100).sum():,} 馬")
    print(f"  50-99%: {((horse_po_coverage['po4_coverage'] >= 50) & (horse_po_coverage['po4_coverage'] < 100)).sum():,} 馬")
    print(f"  1-49%: {((horse_po_coverage['po4_coverage'] >= 1) & (horse_po_coverage['po4_coverage'] < 50)).sum():,} 馬")
    print(f"  0%: {(horse_po_coverage['po4_coverage'] == 0).sum():,} 馬")
    
    # ===============================
    # 5. 現在の特徴量での使用状況
    # ===============================
    print("\n" + "=" * 80)
    print("5. 関連する派生特徴量の現状")
    print("=" * 80)
    
    derived_cols = ['position_change_1_2', 'position_change_2_3', 'position_change_3_4', 
                    'final_corner_to_finish']
    
    for col in derived_cols:
        if col in flat_races.columns:
            null_count = flat_races[col].isna().sum()
            null_pct = null_count / len(flat_races) * 100
            print(f"  {col}: {null_pct:.1f}% NULL")
    
    # ===============================
    # 6. 対応策の検討
    # ===============================
    print("\n" + "=" * 80)
    print("6. 対応策の検討")
    print("=" * 80)
    
    print("""
    【問題の本質】
    
    1. 短距離レース（〜1600m）は2コーナーしか通過しない
       → passing_order_3/4 は「存在しない」のでNULLは正常
       → 対応: C3/C4特徴量のフォールバック or 短距離用特徴量の分離
    
    2. 新潟直線コース（1000m芝）はコーナーが0
       → 全passing_orderがNULL
       → 対応: 直線コースフラグで識別し、別ロジックを適用
    
    【推奨対応】
    
    A. 特徴量生成時の欠損ハンドリング:
       - PO4がNULLの場合、PO2を使用
       - PO2もNULLの場合（直線コース）、グローバル平均を使用
    
    B. 脚質統計の計算改善:
       - 直線コース出走は脚質統計から除外
       - または直線コース専用の脚質カテゴリを作成
    
    C. 距離適性特徴量の追加:
       - 馬の距離別成績を短距離/マイル/中距離/長距離に分類
       - 短距離適性スコアで補完
    """)
    
    print("\n" + "=" * 80)
    print("分析完了")
    print("=" * 80)


if __name__ == '__main__':
    main()
