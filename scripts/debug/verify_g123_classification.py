"""
G1/G2/G3分類の詳細調査
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import pandas as pd
from keibaai.src.utils.race_class_normalizer import RaceClassNormalizer

DATA_DIR = Path(r"c:\Users\zk-ht\Keiba\Keiba_AI_v2\keibaai\data\parsed\parquet")


def main():
    print("=" * 80)
    print("G1/G2/G3分類の詳細調査")
    print("=" * 80)
    
    normalizer = RaceClassNormalizer()
    races = pd.read_parquet(DATA_DIR / 'races' / 'races.parquet')
    races['race_date'] = pd.to_datetime(races['race_date'])
    races['year'] = races['race_date'].dt.year
    
    flat_races = races[~races['race_name'].str.contains('障害', na=False)]
    
    # ===============================
    # 1. 元のrace_classでのG1/G2/G3
    # ===============================
    print("\n" + "=" * 80)
    print("1. 元のrace_class（パーサー出力）でのG1/G2/G3")
    print("=" * 80)
    
    for cls in ['G1', 'G2', 'G3']:
        count = (flat_races['race_class'] == cls).sum()
        print(f"  {cls}: {count:,}")
    
    # ===============================
    # 2. 正規化後のG1/G2/G3
    # ===============================
    print("\n" + "=" * 80)
    print("2. 正規化後のG1/G2/G3")
    print("=" * 80)
    
    flat_races = normalizer.normalize_dataframe(flat_races)
    
    for cls in ['G1', 'G2', 'G3', 'リステッド']:
        count = (flat_races['race_class_normalized'] == cls).sum()
        pct = count / len(flat_races) * 100
        print(f"  {cls}: {count:,} ({pct:.2f}%)")
    
    # ===============================
    # 3. race_nameでのG1/G2/G3パターン確認
    # ===============================
    print("\n" + "=" * 80)
    print("3. race_nameに含まれるパターン（生データ確認）")
    print("=" * 80)
    
    # G1パターン
    g1_patterns = ['(G1)', '（G1）', '(GI)', 'GⅠ']
    for p in g1_patterns:
        count = flat_races['race_name'].str.contains(p, na=False, regex=False).sum()
        if count > 0:
            print(f"  '{p}': {count:,}件")
    
    # G2パターン
    g2_patterns = ['(G2)', '（G2）', '(GII)', 'GⅡ']
    for p in g2_patterns:
        count = flat_races['race_name'].str.contains(p, na=False, regex=False).sum()
        if count > 0:
            print(f"  '{p}': {count:,}件")
    
    # G3パターン
    g3_patterns = ['(G3)', '（G3）', '(GIII)', 'GⅢ']
    for p in g3_patterns:
        count = flat_races['race_name'].str.contains(p, na=False, regex=False).sum()
        if count > 0:
            print(f"  '{p}': {count:,}件")
    
    # ===============================
    # 4. G1/G2/G3のサンプル
    # ===============================
    print("\n" + "=" * 80)
    print("4. race_nameサンプル")
    print("=" * 80)
    
    # G1サンプル
    g1_names = flat_races[flat_races['race_name'].str.contains('G1|GI|GⅠ', na=False, regex=True)]['race_name'].drop_duplicates().head(5)
    print("\n【G1パターンを含むrace_name】")
    for name in g1_names:
        print(f"  - {name}")
    
    # G2サンプル
    g2_names = flat_races[flat_races['race_name'].str.contains('G2|GII|GⅡ', na=False, regex=True)]['race_name'].drop_duplicates().head(5)
    print("\n【G2パターンを含むrace_name】")
    for name in g2_names:
        print(f"  - {name}")
    
    # G3サンプル
    g3_names = flat_races[flat_races['race_name'].str.contains('G3|GIII|GⅢ', na=False, regex=True)]['race_name'].drop_duplicates().head(5)
    print("\n【G3パターンを含むrace_name】")
    for name in g3_names:
        print(f"  - {name}")
    
    # ===============================
    # 5. パーサーのG1分類の確認
    # ===============================
    print("\n" + "=" * 80)
    print("5. パーサーがG1と判定したレースのrace_name確認")
    print("=" * 80)
    
    g1_by_parser = flat_races[flat_races['race_class'] == 'G1']['race_name'].drop_duplicates()
    print(f"\nパーサーが'G1'と判定したレース種類: {len(g1_by_parser):,}")
    
    # G2/G3を含む可能性
    g1_but_g2 = g1_by_parser[g1_by_parser.str.contains('G2|GII', na=False, regex=True)]
    g1_but_g3 = g1_by_parser[g1_by_parser.str.contains('G3|GIII', na=False, regex=True)]
    
    print(f"  うちG2パターンを含む: {len(g1_but_g2):,}")
    print(f"  うちG3パターンを含む: {len(g1_but_g3):,}")
    
    if len(g1_but_g2) > 0:
        print("\n  G1判定だがG2パターンを含むrace_name:")
        for name in g1_but_g2.head(5):
            print(f"    - {name}")
    
    if len(g1_but_g3) > 0:
        print("\n  G1判定だがG3パターンを含むrace_name:")
        for name in g1_but_g3.head(5):
            print(f"    - {name}")
    
    print("\n" + "=" * 80)
    print("調査完了")
    print("=" * 80)


if __name__ == '__main__':
    main()
