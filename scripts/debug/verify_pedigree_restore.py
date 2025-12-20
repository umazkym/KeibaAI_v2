"""
血統データ修正後の確認スクリプト
"""

import pandas as pd
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = PROJECT_ROOT / "keibaai" / "data" / "parsed" / "parquet"

def verify_pedigree_restore():
    """リストア後の確認"""
    print("=" * 80)
    print("  血統データ修正後の確認")
    print("=" * 80)
    
    pedigrees = pd.read_parquet(DATA_DIR / "pedigrees" / "pedigrees.parquet")
    
    print(f"\n  【pedigrees.parquet の状態】")
    print(f"    総レコード数: {len(pedigrees):,}")
    print(f"    ユニーク馬数: {pedigrees['horse_id'].nunique():,}")
    
    # 馬あたりのレコード数
    horse_counts = pedigrees.groupby('horse_id').size()
    avg_per_horse = horse_counts.mean()
    
    print(f"    馬あたり平均レコード数: {avg_per_horse:.1f}")
    
    # 世代別カウント
    if 'generation' in pedigrees.columns:
        print(f"\n  世代別カウント:")
        gen_counts = pedigrees['generation'].value_counts().sort_index()
        for gen, count in gen_counts.items():
            print(f"    世代{gen}: {count:,}件")
    
    # 世代1（父母）のデータ確認
    gen1 = pedigrees[pedigrees['generation'] == 1] if 'generation' in pedigrees.columns else pd.DataFrame()
    
    if len(gen1) > 0:
        print(f"\n  ✅ 世代1（父母）データ: {len(gen1):,}件")
        
        # サンプル
        print(f"\n  世代1のサンプル:")
        print(gen1.head(10).to_string())
    else:
        print(f"\n  ⚠️ 世代1（父母）データがありません")
    
    # races.parquetとの照合
    races = pd.read_parquet(DATA_DIR / "races" / "races.parquet")
    race_horse_ids = set(races['horse_id'].dropna().unique())
    ped_horse_ids = set(pedigrees['horse_id'].unique())
    
    coverage = len(race_horse_ids & ped_horse_ids) / len(race_horse_ids) * 100 if len(race_horse_ids) > 0 else 0
    
    print(f"\n  【races.parquetとの照合】")
    print(f"    レース出走馬: {len(race_horse_ids):,}頭")
    print(f"    血統データあり: {len(race_horse_ids & ped_horse_ids):,}頭")
    print(f"    カバー率: {coverage:.1f}%")
    
    # V15特徴量エンジンで使用される sire_id の取得可能性
    if 'generation' in pedigrees.columns:
        sire_data = pedigrees[pedigrees['generation'] == 1]
        sire_horse_ids = set(sire_data['horse_id'].unique())
        sire_coverage = len(race_horse_ids & sire_horse_ids) / len(race_horse_ids) * 100 if len(race_horse_ids) > 0 else 0
        
        print(f"\n  【sire_id取得可能性（V7特徴量）】")
        print(f"    sire_idを取得可能な馬: {len(race_horse_ids & sire_horse_ids):,}頭")
        print(f"    カバー率: {sire_coverage:.1f}%")

def main():
    verify_pedigree_restore()
    
    print("\n" + "=" * 80)
    print("  確認完了")
    print("=" * 80)

if __name__ == "__main__":
    main()
