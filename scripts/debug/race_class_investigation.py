"""
race_classの異常分布を詳細調査

2014-2019と2020-2025でrace_classの分布が大幅に異なる原因を特定
"""

import pandas as pd
import numpy as np
from pathlib import Path

DATA_DIR = Path(r"c:\Users\zk-ht\Keiba\Keiba_AI_v2\keibaai\data\parsed\parquet")

def main():
    print("=" * 80)
    print("race_class異常分布の詳細調査")
    print("=" * 80)
    
    races = pd.read_parquet(DATA_DIR / 'races' / 'races.parquet')
    races['race_date'] = pd.to_datetime(races['race_date'])
    races['year'] = races['race_date'].dt.year
    
    # 平地レースのみ
    flat_races = races[~races['race_name'].str.contains('障害', na=False)]
    
    # ===============================
    # 1. race_classの全値確認
    # ===============================
    print("\n" + "=" * 80)
    print("1. race_classの全値確認")
    print("=" * 80)
    
    all_classes = flat_races['race_class'].value_counts().sort_values(ascending=False)
    print(f"\n全period race_class分布:")
    for cls, cnt in all_classes.items():
        print(f"  {cls}: {cnt:,} ({cnt/len(flat_races)*100:.1f}%)")
    
    # ===============================
    # 2. 年別race_class分布
    # ===============================
    print("\n" + "=" * 80)
    print("2. 年別race_class分布（主要クラスのみ）")
    print("=" * 80)
    
    # 年別×クラス別のクロス集計
    class_by_year = pd.crosstab(flat_races['year'], flat_races['race_class'], normalize='index') * 100
    
    # 主要クラスのみ表示
    major_classes = ['未勝利', '新馬', '1勝', '2勝', '3勝', 'OP', 'G1', 'G2', 'G3', '500', '1000']
    existing_classes = [c for c in major_classes if c in class_by_year.columns]
    
    print(f"\n年別分布（%）:")
    print(class_by_year[existing_classes].to_string())
    
    # ===============================
    # 3. 「500」「1000」の正体
    # ===============================
    print("\n" + "=" * 80)
    print("3. 「500」「1000」クラスの正体")
    print("=" * 80)
    
    cls_500 = flat_races[flat_races['race_class'] == '500']
    cls_1000 = flat_races[flat_races['race_class'] == '1000']
    
    print(f"\n「500」クラス:")
    print(f"  レコード数: {len(cls_500):,}")
    print(f"  年分布: ")
    for year, cnt in cls_500.groupby('year').size().items():
        print(f"    {year}: {cnt:,}")
    
    # サンプル確認
    print(f"\n  サンプルレース:")
    sample_500 = cls_500[['race_id', 'race_date', 'venue', 'race_name', 'distance_m']].drop_duplicates().head(5)
    print(sample_500.to_string())
    
    print(f"\n「1000」クラス:")
    print(f"  レコード数: {len(cls_1000):,}")
    print(f"\n  サンプルレース:")
    sample_1000 = cls_1000[['race_id', 'race_date', 'venue', 'race_name', 'distance_m']].drop_duplicates().head(5)
    print(sample_1000.to_string())
    
    # ===============================
    # 4. 「1勝」「2勝」「3勝」クラスの確認
    # ===============================
    print("\n" + "=" * 80)
    print("4. 「1勝」「2勝」「3勝」クラスの確認（2019年のクラス再編）")
    print("=" * 80)
    
    # 2019年9月以降、クラス名が変更になった
    # 500万下 → 1勝クラス
    # 1000万下 → 2勝クラス
    # 1600万下 → 3勝クラス
    
    for cls in ['1勝', '2勝', '3勝']:
        if cls in flat_races['race_class'].values:
            cls_data = flat_races[flat_races['race_class'] == cls]
            print(f"\n「{cls}」クラス:")
            print(f"  レコード数: {len(cls_data):,}")
            print(f"  年分布: ")
            for year, cnt in cls_data.groupby('year').size().items():
                print(f"    {year}: {cnt:,}")
    
    # ===============================
    # 5. クラス変換ルールの確認
    # ===============================
    print("\n" + "=" * 80)
    print("5. クラス変換ルール（旧→新）")
    print("=" * 80)
    
    # 2019年9月のクラス再編
    print("""
    【2019年9月のクラス制度改編】
    
    旧クラス名(〜2019年夏)     →  新クラス名(2019年秋〜)
    ─────────────────────────────────────────────
    500万下                    →  1勝クラス
    1000万下                   →  2勝クラス  
    1600万下                   →  3勝クラス
    オープン                   →  オープン（変更なし）
    
    ※「500」「1000」は賞金ベースのクラス区分を示す
    """)
    
    # クラス名のマッピング問題
    print("\n【問題点】")
    print("  2014-2019のデータで「500」「1000」が発生")
    print("  2020-2025のデータでは「1勝」「2勝」「3勝」に変更済み")
    print("  → モデルが同一クラスを別物として認識している可能性")
    
    # ===============================
    # 6. 統一クラス名での再集計
    # ===============================
    print("\n" + "=" * 80)
    print("6. クラス名を統一した場合の年代比較")
    print("=" * 80)
    
    # クラス名のマッピング
    class_mapping = {
        '500': '1勝クラス相当',
        '1000': '2勝クラス相当',
        '1600': '3勝クラス相当',
        '1勝': '1勝クラス相当',
        '2勝': '2勝クラス相当',
        '3勝': '3勝クラス相当',
    }
    
    flat_races_mapped = flat_races.copy()
    flat_races_mapped['race_class_unified'] = flat_races_mapped['race_class'].map(
        lambda x: class_mapping.get(x, x)
    )
    
    old = flat_races_mapped[flat_races_mapped['year'] <= 2019]
    new = flat_races_mapped[flat_races_mapped['year'] >= 2020]
    
    print("\n【統一クラス名での比較】")
    old_class = old['race_class_unified'].value_counts(normalize=True) * 100
    new_class = new['race_class_unified'].value_counts(normalize=True) * 100
    
    all_classes = sorted(set(old_class.index) | set(new_class.index))
    
    for cls in all_classes:
        old_pct = old_class.get(cls, 0)
        new_pct = new_class.get(cls, 0)
        diff = new_pct - old_pct
        if old_pct > 1 or new_pct > 1:
            flag = "⚠️" if abs(diff) > 3 else ""
            print(f"  {cls}: {old_pct:.1f}% → {new_pct:.1f}% ({diff:+.1f}%) {flag}")
    
    print("\n" + "=" * 80)
    print("調査完了")
    print("=" * 80)


if __name__ == '__main__':
    main()
