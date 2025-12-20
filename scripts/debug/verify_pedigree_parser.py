"""
血統パーサーの出力を検証するスクリプト

問題: pedigrees.parquetが1馬あたり1件しかない
原因の特定: パーサー自体の問題か、パイプラインの保存処理の問題か
"""

import pandas as pd
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.append(str(PROJECT_ROOT / 'keibaai'))

from src.modules.parsers import pedigree_parser

DATA_DIR = PROJECT_ROOT / "keibaai" / "data"

def test_parser_output():
    """パーサーの出力を直接確認"""
    print("=" * 80)
    print("  血統パーサーの出力検証")
    print("=" * 80)
    
    # サンプルのHTMLファイルを取得
    ped_dir = DATA_DIR / "raw" / "html" / "ped"
    
    # いくつかのファイルを取得
    ped_files = list(ped_dir.rglob("*.bin"))[:5]
    
    if not ped_files:
        print("  ❌ 血統HTMLファイルが見つかりません")
        return
    
    print(f"\n  サンプルファイル数: {len(ped_files)}")
    
    for ped_file in ped_files:
        print(f"\n  ファイル: {ped_file.name}")
        
        # パースを実行
        df = pedigree_parser.parse_pedigree_html(str(ped_file))
        
        if df.empty:
            print(f"    ❌ 空のDataFrameが返されました")
            continue
        
        print(f"    レコード数: {len(df)}")
        print(f"    カラム: {list(df.columns)}")
        
        # 世代別カウント
        gen_counts = df['generation'].value_counts().sort_index()
        print(f"    世代別:")
        for gen, count in gen_counts.items():
            print(f"      世代{gen}: {count}件")
        
        # サンプルデータ
        print(f"    サンプル:")
        print(df.head(10).to_string())

def check_existing_parquet():
    """既存のpedigrees.parquetを確認"""
    print("\n" + "=" * 80)
    print("  既存のpedigrees.parquetの確認")
    print("=" * 80)
    
    parquet_path = DATA_DIR / "parsed" / "parquet" / "pedigrees" / "pedigrees.parquet"
    
    if not parquet_path.exists():
        print("  ❌ ファイルが見つかりません")
        return
    
    df = pd.read_parquet(parquet_path)
    
    print(f"\n  レコード数: {len(df):,}")
    print(f"  ユニークhorse_id: {df['horse_id'].nunique():,}")
    print(f"  カラム: {list(df.columns)}")
    
    # 世代別
    print(f"\n  世代別カウント:")
    gen_counts = df['generation'].value_counts().sort_index()
    for gen, count in gen_counts.items():
        print(f"    世代{gen}: {count:,}件")
    
    # 馬あたりのレコード数
    horse_rec_counts = df.groupby('horse_id').size()
    print(f"\n  馬あたりのレコード数分布:")
    print(f"    1件: {(horse_rec_counts == 1).sum():,}頭")
    print(f"    2件以上: {(horse_rec_counts >= 2).sum():,}頭")
    
    # position カラムの有無
    if 'position' in df.columns:
        print(f"\n  ✓ 'position'カラムあり")
    else:
        print(f"\n  ⚠️ 'position'カラムなし")

def check_old_parquet():
    """古いpedigrees_old.parquetを確認"""
    print("\n" + "=" * 80)
    print("  古いpedigrees_old.parquetの確認")
    print("=" * 80)
    
    parquet_path = DATA_DIR / "parsed" / "parquet" / "pedigrees" / "pedigrees_old.parquet"
    
    if not parquet_path.exists():
        print("  ファイルが見つかりません")
        return
    
    df = pd.read_parquet(parquet_path)
    
    print(f"\n  レコード数: {len(df):,}")
    print(f"  ユニークhorse_id: {df['horse_id'].nunique():,}")
    print(f"  カラム: {list(df.columns)}")
    
    # 世代別
    if 'generation' in df.columns:
        print(f"\n  世代別カウント:")
        gen_counts = df['generation'].value_counts().sort_index()
        for gen, count in gen_counts.items():
            print(f"    世代{gen}: {count:,}件")
    
    # 馬あたりのレコード数
    horse_rec_counts = df.groupby('horse_id').size()
    print(f"\n  馬あたりのレコード数分布:")
    print(f"    1件: {(horse_rec_counts == 1).sum():,}頭")
    print(f"    2件以上: {(horse_rec_counts >= 2).sum():,}頭")
    
    if (horse_rec_counts >= 2).sum() > 0:
        # 2件以上ある馬のサンプル
        multi_horse = horse_rec_counts[horse_rec_counts >= 2].index[0]
        print(f"\n  2件以上ある馬のサンプル ({multi_horse}):")
        print(df[df['horse_id'] == multi_horse].to_string())

def identify_root_cause():
    """根本原因の特定"""
    print("\n" + "=" * 80)
    print("  根本原因の分析")
    print("=" * 80)
    
    # パーサーの出力を確認
    ped_dir = DATA_DIR / "raw" / "html" / "ped"
    ped_file = next(ped_dir.rglob("*.bin"), None)
    
    if ped_file:
        df = pedigree_parser.parse_pedigree_html(str(ped_file))
        
        print(f"\n  【パーサー出力分析】")
        print(f"    出力カラム: {list(df.columns)}")
        
        if 'position' not in df.columns:
            print(f"    ⚠️ 'position'カラムがパーサーから出力されていない")
            print(f"\n    パイプライン (run_parsing_resumable.py L506-512) では、")
            print(f"    'position'カラムがない場合、horse_idのみで重複排除される")
            print(f"    → 結果として1馬あたり1件のみになる")
        
        # 世代別
        if len(df) > 0:
            expected_per_horse = len(df)
            print(f"\n    パーサーは1馬あたり{expected_per_horse}件を出力")
            print(f"    (期待値: 62件 = 2^1 + 2^2 + 2^3 + 2^4 + 2^5 - 2)")
            
            if expected_per_horse < 62:
                print(f"\n    ⚠️ パーサーの出力も不完全（{expected_per_horse}/62）")
                
                # パーサーのロジックを確認
                print(f"\n    パーサーがすべてのrowspanを正しく認識しているか確認が必要")

def main():
    test_parser_output()
    check_existing_parquet()
    check_old_parquet()
    identify_root_cause()
    
    print("\n" + "=" * 80)
    print("  検証完了")
    print("=" * 80)

if __name__ == "__main__":
    main()
