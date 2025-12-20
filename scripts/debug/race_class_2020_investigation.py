"""
2020年以降のrace_class欠損を詳細調査

なぜ1勝/2勝/3勝クラスが認識されていないか
"""

import pandas as pd
import numpy as np
from pathlib import Path
import re

DATA_DIR = Path(r"c:\Users\zk-ht\Keiba\Keiba_AI_v2\keibaai\data\parsed\parquet")

def main():
    print("=" * 80)
    print("2020年以降のrace_class詳細調査")
    print("=" * 80)
    
    races = pd.read_parquet(DATA_DIR / 'races' / 'races.parquet')
    races['race_date'] = pd.to_datetime(races['race_date'])
    races['year'] = races['race_date'].dt.year
    
    # 2020年以降の平地レース
    flat_2020 = races[(races['year'] >= 2020) & (~races['race_name'].str.contains('障害', na=False))]
    
    # ===============================
    # 1. 2020年以降のrace_name調査
    # ===============================
    print("\n" + "=" * 80)
    print("1. 2020年以降のrace_nameサンプル")
    print("=" * 80)
    
    # race_classが「未勝利」のレースのrace_name
    mishirou = flat_2020[flat_2020['race_class'] == '未勝利']
    print(f"\n「未勝利」分類レース数: {mishirou['race_id'].nunique():,}")
    
    # 実際のrace_nameを確認
    print("\n「未勝利」分類されたrace_nameサンプル（ユニーク50件）:")
    sample_names = mishirou['race_name'].drop_duplicates().head(50)
    for name in sample_names:
        print(f"  - {name}")
    
    # ===============================
    # 2. 「1勝」「2勝」「3勝」を含むrace_nameの確認
    # ===============================
    print("\n" + "=" * 80)
    print("2. 「1勝」「2勝」「3勝」を含むrace_nameの確認")
    print("=" * 80)
    
    # 1勝クラス
    has_1win = flat_2020[flat_2020['race_name'].str.contains('1勝', na=False)]
    print(f"\nrace_nameに「1勝」を含むレコード: {len(has_1win):,}")
    if len(has_1win) > 0:
        print(f"  対応するrace_class: {has_1win['race_class'].value_counts().to_dict()}")
        print(f"  サンプル: {has_1win['race_name'].iloc[0]}")
    
    # 2勝クラス
    has_2win = flat_2020[flat_2020['race_name'].str.contains('2勝', na=False)]
    print(f"\nrace_nameに「2勝」を含むレコード: {len(has_2win):,}")
    if len(has_2win) > 0:
        print(f"  対応するrace_class: {has_2win['race_class'].value_counts().to_dict()}")
        print(f"  サンプル: {has_2win['race_name'].iloc[0]}")
    
    # 3勝クラス
    has_3win = flat_2020[flat_2020['race_name'].str.contains('3勝', na=False)]
    print(f"\nrace_nameに「3勝」を含むレコード: {len(has_3win):,}")
    if len(has_3win) > 0:
        print(f"  対応するrace_class: {has_3win['race_class'].value_counts().to_dict()}")
        print(f"  サンプル: {has_3win['race_name'].iloc[0]}")
    
    # ===============================
    # 3. 「未勝利」分類だが「未勝利」を含まないrace_name
    # ===============================
    print("\n" + "=" * 80)
    print("3. 「未勝利」分類だが「未勝利」を含まないrace_name")
    print("=" * 80)
    
    mishirou_but_not = mishirou[~mishirou['race_name'].str.contains('未勝利', na=False)]
    print(f"\n「未勝利」分類だが「未勝利」を含まない: {mishirou_but_not['race_id'].nunique():,} レース")
    
    # サンプル確認
    print("\nサンプル（30件）:")
    for name in mishirou_but_not['race_name'].drop_duplicates().head(30):
        print(f"  - {name}")
    
    # ===============================
    # 4. race_classがNoneのレコード
    # ===============================
    print("\n" + "=" * 80)
    print("4. race_classがNone/NaNのレコード")
    print("=" * 80)
    
    none_class = flat_2020[flat_2020['race_class'].isna() | (flat_2020['race_class'] == 'None')]
    print(f"\nrace_classがNone/NaN: {len(none_class):,} レコード")
    
    if len(none_class) > 0:
        print("\nサンプル:")
        for name in none_class['race_name'].drop_duplicates().head(10):
            print(f"  - {name}")
    
    # ===============================
    # 5. 根本原因の特定
    # ===============================
    print("\n" + "=" * 80)
    print("5. 根本原因の特定")
    print("=" * 80)
    
    # パーサーの条件を再確認
    # if '未勝利' in race_name:
    #     metadata['race_class'] = '未勝利'
    
    # この条件が「1勝クラス」より先に評価されている場合、
    # race_nameに「未勝利」を含まなくても、他の条件に合致しない場合はNoneになる
    
    # 条件クラスのrace_nameパターンを分析
    print("""
    【パーサーの問題点】
    
    現在のパーサー条件（results_parser.py L238-256）:
    1. 'G1' in race_name → 'G1'
    2. 'G2' in race_name → 'G2'
    3. 'G3' in race_name → 'G3'
    4. 'オープン' or 'OP' → 'OP'
    5. '1600万' → '1600'
    6. '1000万' → '1000'
    7. '500万' → '500'
    8. '未勝利' → '未勝利'
    9. '新馬' → '新馬'
    
    【問題】
    - 「1勝クラス」「2勝クラス」「3勝クラス」のパターンがない！
    - 2020年以降のレースでこれらにマッチする条件がない
    """)
    
    # 条件クラスがどう分類されているか
    print("\n【条件クラスの分類状況】")
    
    # 「1勝」「2勝」「3勝」を含むrace_nameのrace_class分布
    for pattern in ['1勝', '2勝', '3勝']:
        matched = flat_2020[flat_2020['race_name'].str.contains(pattern, na=False)]
        if len(matched) > 0:
            print(f"\n「{pattern}」を含むrace_name ({len(matched):,}件):")
            print(f"  race_class分布: {matched['race_class'].value_counts().to_dict()}")
    
    # ===============================
    # 6. 正しい分布の推定
    # ===============================
    print("\n" + "=" * 80)
    print("6. 正しいクラス分布の推定（race_nameから再抽出）")
    print("=" * 80)
    
    def extract_class_v3(race_name):
        if pd.isna(race_name):
            return None
        name = str(race_name)
        
        # 重賞
        if any(p in name for p in ['(G1)', '(GI)', 'GⅠ']):
            return 'G1'
        if any(p in name for p in ['(G2)', '(GII)', 'GⅡ']):
            return 'G2'
        if any(p in name for p in ['(G3)', '(GIII)', 'GⅢ']):
            return 'G3'
        
        # リステッド
        if '(L)' in name:
            return 'リステッド'
        
        # 条件クラス（新制度）
        if '3勝' in name:
            return '3勝クラス'
        if '2勝' in name:
            return '2勝クラス'
        if '1勝' in name:
            return '1勝クラス'
        
        # 条件クラス（旧制度）
        if '1600万' in name:
            return '3勝クラス'
        if '1000万' in name:
            return '2勝クラス'
        if '500万' in name:
            return '1勝クラス'
        
        # 新馬・未勝利
        if '新馬' in name:
            return '新馬'
        if '未勝利' in name:
            return '未勝利'
        
        # オープン
        if 'オープン' in name or '(OP)' in name:
            return 'オープン'
        
        return None
    
    flat_2020['extracted_class'] = flat_2020['race_name'].apply(extract_class_v3)
    
    print("\n【正しいクラス分布（race_nameから推定）】")
    for cls, cnt in flat_2020['extracted_class'].value_counts().items():
        pct = cnt / len(flat_2020) * 100
        print(f"  {cls}: {cnt:,} ({pct:.1f}%)")
    
    # 元のrace_classと比較
    print("\n【現race_class vs 推定クラス】")
    comparison = flat_2020.groupby(['race_class', 'extracted_class']).size().unstack(fill_value=0)
    print(comparison.to_string())
    
    print("\n" + "=" * 80)
    print("調査完了")
    print("=" * 80)


if __name__ == '__main__':
    main()
