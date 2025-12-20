"""
race_class現状の詳細調査

全race_class値とrace_nameとの関係を分析し、
JRA公式クラス制度への変換可能性を調査
"""

import pandas as pd
import numpy as np
from pathlib import Path
import re

DATA_DIR = Path(r"c:\Users\zk-ht\Keiba\Keiba_AI_v2\keibaai\data\parsed\parquet")

def main():
    print("=" * 80)
    print("race_class現状の詳細調査")
    print("=" * 80)
    
    races = pd.read_parquet(DATA_DIR / 'races' / 'races.parquet')
    races['race_date'] = pd.to_datetime(races['race_date'])
    races['year'] = races['race_date'].dt.year
    
    # 平地レースのみ
    flat_races = races[~races['race_name'].str.contains('障害', na=False)]
    
    # ===============================
    # 1. 全race_class値の確認
    # ===============================
    print("\n" + "=" * 80)
    print("1. 全race_class値（ユニーク）")
    print("=" * 80)
    
    all_classes = flat_races['race_class'].value_counts().sort_values(ascending=False)
    print(f"\nユニーク値数: {len(all_classes)}")
    print(f"\n全race_class値と件数:")
    for cls, cnt in all_classes.items():
        print(f"  '{cls}': {cnt:,} ({cnt/len(flat_races)*100:.1f}%)")
    
    # ===============================
    # 2. race_nameの分析（JRAクラス抽出）
    # ===============================
    print("\n" + "=" * 80)
    print("2. race_nameからのクラス抽出分析")
    print("=" * 80)
    
    # race_nameのサンプル確認
    print("\nrace_nameサンプル（各クラス5件）:")
    for cls in ['未勝利', '新馬', '500', '1000', 'OP', 'G1', 'G2', 'G3']:
        if cls in flat_races['race_class'].values:
            samples = flat_races[flat_races['race_class'] == cls]['race_name'].drop_duplicates().head(5)
            print(f"\n  【{cls}】")
            for name in samples:
                print(f"    - {name}")
    
    # ===============================
    # 3. race_nameからクラスを再推定
    # ===============================
    print("\n" + "=" * 80)
    print("3. race_nameからのクラス再推定テスト")
    print("=" * 80)
    
    def extract_class_from_name(race_name):
        """race_nameからJRAクラスを抽出"""
        if pd.isna(race_name):
            return None
        
        name = str(race_name)
        
        # 重賞
        if 'GⅠ' in name or '(G1)' in name or '（G1）' in name or '(GI)' in name:
            return 'G1'
        if 'GⅡ' in name or '(G2)' in name or '（G2）' in name or '(GII)' in name:
            return 'G2'
        if 'GⅢ' in name or '(G3)' in name or '（G3）' in name or '(GIII)' in name:
            return 'G3'
        
        # リステッド
        if '(L)' in name or '（L）' in name or 'リステッド' in name:
            return 'リステッド'
        
        # 条件クラス（新制度 2019年秋以降）
        if '3勝クラス' in name or '3勝' in name:
            return '3勝クラス'
        if '2勝クラス' in name or '2勝' in name:
            return '2勝クラス'
        if '1勝クラス' in name or '1勝' in name:
            return '1勝クラス'
        
        # 条件クラス（旧制度）
        if '1600万' in name or '1600万下' in name:
            return '3勝クラス'  # 旧1600万下 = 新3勝
        if '1000万' in name or '1000万下' in name:
            return '2勝クラス'  # 旧1000万下 = 新2勝
        if '500万' in name or '500万下' in name:
            return '1勝クラス'  # 旧500万下 = 新1勝
        
        # オープン
        if 'オープン' in name or '(OP)' in name or '（OP）' in name:
            return 'オープン'
        if 'OP' in name and '500' not in name:  # 500万OPを除外
            return 'オープン'
        
        # 新馬・未勝利
        if '新馬' in name:
            return '新馬'
        if '未勝利' in name:
            return '未勝利'
        
        return None
    
    # テスト適用
    flat_races['extracted_class'] = flat_races['race_name'].apply(extract_class_from_name)
    
    # 結果確認
    extraction_result = flat_races.groupby(['race_class', 'extracted_class']).size().unstack(fill_value=0)
    print("\n現race_class × 抽出クラスのクロス集計:")
    print(extraction_result.to_string())
    
    # 抽出成功率
    extracted_count = flat_races['extracted_class'].notna().sum()
    print(f"\n抽出成功率: {extracted_count:,} / {len(flat_races):,} ({extracted_count/len(flat_races)*100:.1f}%)")
    
    # 抽出失敗のサンプル
    failed = flat_races[flat_races['extracted_class'].isna()]
    print(f"\n抽出失敗サンプル（10件）:")
    for _, row in failed[['race_name', 'race_class']].drop_duplicates().head(10).iterrows():
        print(f"  race_class='{row['race_class']}', race_name='{row['race_name']}'")
    
    # ===============================
    # 4. 改善版抽出ロジック
    # ===============================
    print("\n" + "=" * 80)
    print("4. 改善版抽出ロジック")
    print("=" * 80)
    
    def extract_class_v2(race_name):
        """改善版: race_nameからJRAクラスを抽出"""
        if pd.isna(race_name):
            return None
        
        name = str(race_name)
        
        # 重賞（優先度高）
        g1_patterns = ['(G1)', '（G1）', '(GI)', '(Ⅰ)', 'GⅠ', 'G１']
        g2_patterns = ['(G2)', '（G2）', '(GII)', '(Ⅱ)', 'GⅡ', 'G２']
        g3_patterns = ['(G3)', '（G3）', '(GIII)', '(Ⅲ)', 'GⅢ', 'G３']
        
        for p in g1_patterns:
            if p in name:
                return 'G1'
        for p in g2_patterns:
            if p in name:
                return 'G2'
        for p in g3_patterns:
            if p in name:
                return 'G3'
        
        # リステッド
        if '(L)' in name or '（L）' in name:
            return 'リステッド'
        
        # 新馬・未勝利（確実性高）
        if '新馬' in name:
            return '新馬'
        if '未勝利' in name:
            return '未勝利'
        
        # 条件クラス
        # 新制度
        if re.search(r'[123]勝クラス', name):
            return re.search(r'([123])勝クラス', name).group(1) + '勝クラス'
        if re.search(r'[123]勝', name):
            return re.search(r'([123])勝', name).group(1) + '勝クラス'
        
        # 旧制度
        if '1600万' in name:
            return '3勝クラス'
        if '1000万' in name:
            return '2勝クラス'
        if '500万' in name:
            return '1勝クラス'
        
        # オープン
        if 'オープン' in name:
            return 'オープン'
        if '(OP)' in name or '（OP）' in name:
            return 'オープン'
        
        return None
    
    flat_races['extracted_class_v2'] = flat_races['race_name'].apply(extract_class_v2)
    
    # 抽出成功率
    extracted_v2_count = flat_races['extracted_class_v2'].notna().sum()
    print(f"\n改善版抽出成功率: {extracted_v2_count:,} / {len(flat_races):,} ({extracted_v2_count/len(flat_races)*100:.1f}%)")
    
    # 抽出結果分布
    print(f"\n改善版抽出クラス分布:")
    for cls, cnt in flat_races['extracted_class_v2'].value_counts().items():
        print(f"  {cls}: {cnt:,} ({cnt/len(flat_races)*100:.1f}%)")
    
    # まだ失敗のサンプル
    still_failed = flat_races[flat_races['extracted_class_v2'].isna()]
    print(f"\nまだ抽出失敗: {len(still_failed):,}件 ({len(still_failed)/len(flat_races)*100:.1f}%)")
    
    if len(still_failed) > 0:
        print(f"\n抽出失敗サンプル（20件）:")
        for _, row in still_failed[['race_name', 'race_class', 'year']].drop_duplicates().head(20).iterrows():
            print(f"  [{row['year']}] race_class='{row['race_class']}', race_name='{row['race_name']}'")
    
    print("\n" + "=" * 80)
    print("調査完了")
    print("=" * 80)


if __name__ == '__main__':
    main()
