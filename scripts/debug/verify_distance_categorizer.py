"""
距離カテゴリ特徴量の検証
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import pandas as pd
from keibaai.src.utils.distance_categorizer import DistanceCategorizer

DATA_DIR = Path(r"c:\Users\zk-ht\Keiba\Keiba_AI_v2\keibaai\data\parsed\parquet")


def main():
    print("=" * 80)
    print("距離カテゴリ特徴量の検証")
    print("=" * 80)
    
    categorizer = DistanceCategorizer()
    races = pd.read_parquet(DATA_DIR / 'races' / 'races.parquet')
    
    # 平地レースのみ
    flat_races = races[~races['race_name'].str.contains('障害', na=False)]
    print(f"\n平地レース数: {len(flat_races):,}")
    
    # 距離カテゴリ特徴量を追加
    print("\n距離カテゴリ特徴量を追加中...")
    flat_races = categorizer.add_features(flat_races)
    print("完了")
    
    # カテゴリ分布
    print("\n" + "=" * 80)
    print("距離カテゴリ分布")
    print("=" * 80)
    
    for cat, cnt in flat_races['distance_category'].value_counts().items():
        ordinal = categorizer.get_ordinal(cat)
        pct = cnt / len(flat_races) * 100
        print(f"  {cat} (Lv.{ordinal}): {cnt:,} ({pct:.1f}%)")
    
    # 4コーナーフラグ
    print("\n" + "=" * 80)
    print("4コーナーフラグ分布")
    print("=" * 80)
    
    has_4c_true = flat_races['has_4_corners'].sum()
    has_4c_false = len(flat_races) - has_4c_true
    print(f"  has_4_corners=True: {has_4c_true:,} ({has_4c_true/len(flat_races)*100:.1f}%)")
    print(f"  has_4_corners=False: {has_4c_false:,} ({has_4c_false/len(flat_races)*100:.1f}%)")
    
    # passing_order_4との整合性確認
    print("\n" + "=" * 80)
    print("has_4_cornersとpassing_order_4の整合性")
    print("=" * 80)
    
    # has_4_corners=Trueの場合のPO4欠損率
    has_4c = flat_races[flat_races['has_4_corners'] == True]
    po4_null_rate_true = has_4c['passing_order_4'].isna().mean() * 100
    print(f"\n  has_4_corners=True のPO4欠損率: {po4_null_rate_true:.1f}%")
    
    # has_4_corners=Falseの場合のPO4欠損率
    no_4c = flat_races[flat_races['has_4_corners'] == False]
    po4_null_rate_false = no_4c['passing_order_4'].isna().mean() * 100
    print(f"  has_4_corners=False のPO4欠損率: {po4_null_rate_false:.1f}%")
    
    # 期待される結果
    print(f"\n  期待: has_4_corners=False → PO4欠損率≒100%")
    if po4_null_rate_false > 95:
        print("  ✅ 期待通り")
    else:
        print(f"  ⚠️ 予期しない値: {po4_null_rate_false:.1f}%")
    
    # 追加されたカラムの確認
    print("\n" + "=" * 80)
    print("追加されたカラム")
    print("=" * 80)
    
    new_cols = ['distance_category', 'distance_category_ordinal', 
                'is_sprint', 'is_mile', 'is_middle', 'is_long', 'is_extreme_long',
                'has_4_corners']
    
    for col in new_cols:
        if col in flat_races.columns:
            print(f"  ✓ {col}")
        else:
            print(f"  ✗ {col} (見つからない)")
    
    print("\n" + "=" * 80)
    print("検証完了")
    print("=" * 80)


if __name__ == '__main__':
    main()
