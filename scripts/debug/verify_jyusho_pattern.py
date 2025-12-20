"""
「(重賞)」パターンの正規化確認
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import pandas as pd
from keibaai.src.utils.race_class_normalizer import RaceClassNormalizer

DATA_DIR = Path(r"c:\Users\zk-ht\Keiba\Keiba_AI_v2\keibaai\data\parsed\parquet")


def main():
    races = pd.read_parquet(DATA_DIR / 'races' / 'races.parquet')
    
    # 旧「重賞」表記のサンプル確認
    jyusho = races[races['race_name'].str.contains('(重賞)', na=False, regex=False)]
    print(f"「(重賞)」を含むレコード数: {len(jyusho)}")
    print(f"ユニークレース数: {jyusho['race_id'].nunique()}")
    
    # 正規化
    normalizer = RaceClassNormalizer()
    jyusho_sample = jyusho[['race_name', 'race_class']].drop_duplicates()
    
    print("\n正規化結果:")
    for _, row in jyusho_sample.iterrows():
        race_name = row['race_name']
        race_class = row['race_class']
        normalized = normalizer.normalize(race_class, race_name)
        print(f"  {race_name}")
        print(f"    元: {race_class} -> 正規化後: {normalized}")


if __name__ == '__main__':
    main()
