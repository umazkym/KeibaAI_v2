#!/usr/bin/env python3
"""
デバッグスクリプト06: 血統データ構造の確認と統合準備
pedigrees.parquetの構造を理解し、horses.parquetへの統合方法を検証
"""

import pandas as pd
from pathlib import Path

def analyze_pedigree_structure():
    """血統データの構造を分析"""
    print("=" * 80)
    print("📊 pedigrees.parquet の構造分析")
    print("=" * 80)

    pedigree_path = Path("keibaai/data/parsed/parquet/pedigrees/pedigrees.parquet")

    if not pedigree_path.exists():
        print(f"\n❌ ファイルが存在しません: {pedigree_path}")
        return None

    df = pd.read_parquet(pedigree_path)

    print(f"\n📄 基本情報:")
    print(f"  総行数: {len(df):,} 行")
    print(f"  列数: {len(df.columns)} 列")
    print(f"  カラム: {df.columns.tolist()}")

    print(f"\n📊 世代別の分布:")
    generation_counts = df['generation'].value_counts().sort_index()
    for gen, count in generation_counts.items():
        print(f"  generation {gen}: {count:>8,} 行")

    print(f"\n🔍 サンプルデータ（1頭分の全血統）:")
    # 1頭のすべての血統を表示
    sample_horse_id = df['horse_id'].iloc[0]
    sample_data = df[df['horse_id'] == sample_horse_id].sort_values('generation')

    print(f"\n  対象馬ID: {sample_horse_id}")
    print(f"  血統レコード数: {len(sample_data)} 件")
    print(f"\n  詳細:")

    for _, row in sample_data.head(10).iterrows():
        print(f"    generation {row['generation']}: {row['ancestor_name']:20s} (ID: {row['ancestor_id']})")

    print(f"\n💡 血統データの構造理解:")
    print(f"  - generation=1: 父と母（2頭）")
    print(f"  - generation=2: 祖父母（4頭）")
    print(f"  - generation=3: 曾祖父母（8頭）")
    print(f"  - generation=4: 高祖父母（16頭）")
    print(f"  - generation=5: 5代前（32頭）")
    print(f"  合計: 2+4+8+16+32 = 62頭（理論値）")

    # 実際の1頭あたりの平均血統数
    avg_ancestors = df.groupby('horse_id').size().mean()
    print(f"\n  実際の平均血統数: {avg_ancestors:.1f} 頭/馬")

    return df

def extract_sire_dam_info(pedigrees_df):
    """父・母・母父の情報を抽出"""
    print("\n" + "=" * 80)
    print("🔧 父・母・母父の抽出処理")
    print("=" * 80)

    if pedigrees_df is None:
        return None

    # generation=1のデータ（父と母）
    gen1 = pedigrees_df[pedigrees_df['generation'] == 1].copy()

    print(f"\n📊 generation=1のデータ:")
    print(f"  行数: {len(gen1):,} 行")

    # 馬ごとにグループ化
    gen1_grouped = gen1.groupby('horse_id')

    print(f"  ユニーク馬数: {gen1_grouped.ngroups:,} 頭")

    # 各馬のgeneration=1の数を確認
    counts = gen1_grouped.size()
    print(f"\n  馬ごとのgeneration=1の数:")
    print(f"    1件: {(counts == 1).sum():,} 頭 (片親のみ)")
    print(f"    2件: {(counts == 2).sum():,} 頭 (両親あり)")
    print(f"    3件以上: {(counts > 2).sum():,} 頭 (データ異常?)")

    # サンプル表示
    print(f"\n🔍 サンプル（最初の5頭）:")
    for horse_id, group in list(gen1_grouped)[:5]:
        print(f"\n  馬ID: {horse_id}")
        for _, row in group.iterrows():
            print(f"    - {row['ancestor_name']:20s} (ID: {row['ancestor_id']})")

    print(f"\n💡 問題点:")
    print(f"  - generation=1だけでは父と母を区別できない")
    print(f"  - 血統テーブルは系統樹構造なので、位置情報が必要")
    print(f"  - 解決策: 別のアプローチが必要")

    return gen1

def check_horses_parquet():
    """horses.parquetの現状確認"""
    print("\n" + "=" * 80)
    print("📄 horses.parquet の現状確認")
    print("=" * 80)

    horses_path = Path("keibaai/data/parsed/parquet/horses/horses.parquet")

    if not horses_path.exists():
        print(f"\n❌ ファイルが存在しません: {horses_path}")
        return None

    df = pd.read_parquet(horses_path)

    print(f"\n📊 基本情報:")
    print(f"  行数: {len(df):,} 行")
    print(f"  カラム: {df.columns.tolist()}")

    print(f"\n🔍 サンプルデータ（最初の3頭）:")
    print(df.head(3).to_string())

    return df

def propose_solution(pedigrees_df, horses_df):
    """解決策の提案"""
    print("\n" + "=" * 80)
    print("💡 解決策の提案")
    print("=" * 80)

    if pedigrees_df is None:
        print("\n⚠️ pedigrees.parquetが読み込めないため、解決策を提案できません")
        return

    print(f"\n📋 現状:")
    print(f"  1. pedigrees.parquetには59,125行の血統データが存在")
    print(f"  2. しかし、系統樹の「位置」情報（父側/母側）が含まれていない")
    print(f"  3. generation番号だけでは父・母・母父を特定できない")

    print(f"\n🔧 解決策の選択肢:")
    print(f"\n  【方法A】血統HTMLを再パース（推奨）")
    print(f"    - keibaai/data/raw/html/ped/*.bin を再度パース")
    print(f"    - 血統テーブルの「位置」情報を保存")
    print(f"    - 列番号や行番号から父・母・母父を特定")
    print(f"    - 所要時間: 約5-10分")
    print(f"    - 実装難易度: 中")

    print(f"\n  【方法B】pedigree_parser.pyを修正")
    print(f"    - パーサーを拡張して位置情報を記録")
    print(f"    - ancestor_position列を追加 (例: 'sire', 'dam', 'paternal_grandsire')")
    print(f"    - 所要時間: 約10-15分")
    print(f"    - 実装難易度: 高")

    print(f"\n  【方法C】簡易的な推定")
    print(f"    - generation=1の最初を父、2番目を母とする（不正確）")
    print(f"    - 所要時間: 数秒")
    print(f"    - 実装難易度: 低")
    print(f"    - 精度: 低（順序が保証されない可能性）")

    print(f"\n✅ 推奨: 方法A（血統HTML再パース）")
    print(f"    - 正確性が最も高い")
    print(f"    - 一度実装すれば再利用可能")
    print(f"    - pedigree_parser.pyの改善にもつながる")

def main():
    """メイン処理"""
    print("\n")
    print("🔍 KeibaAI_v2 血統データ構造分析")
    print("\n")

    # 1. pedigrees.parquetの構造分析
    pedigrees_df = analyze_pedigree_structure()

    # 2. 父・母・母父の抽出試行
    gen1_df = extract_sire_dam_info(pedigrees_df)

    # 3. horses.parquetの現状確認
    horses_df = check_horses_parquet()

    # 4. 解決策の提案
    propose_solution(pedigrees_df, horses_df)

    print("\n" + "=" * 80)
    print("📋 分析完了")
    print("=" * 80)
    print("\n")

if __name__ == "__main__":
    main()
