"""
pedigrees.parquetのスキーマを確認し、damsire_id情報の取得可能性を検証
"""

from pathlib import Path
import pandas as pd

def check_pedigrees():
    print("=" * 80)
    print("🔍 pedigrees.parquet スキーマ確認")
    print("=" * 80)
    print()

    pedigrees_path = Path('keibaai/data/parsed/parquet/pedigrees/pedigrees.parquet')

    if not pedigrees_path.exists():
        print(f"❌ エラー: {pedigrees_path} が存在しません")
        return

    # Parquetファイルを読み込み
    df = pd.read_parquet(pedigrees_path)

    print(f"📊 pedigrees.parquet: {len(df):,} 行 × {len(df.columns)} 列")
    print()

    # 全カラム名を表示
    print("📋 全カラム一覧:")
    print("-" * 80)
    for i, col in enumerate(df.columns, 1):
        dtype = str(df[col].dtype)
        missing_pct = (df[col].isna().sum() / len(df)) * 100
        print(f"{i:2}. {col:25} ({dtype:10}) 欠損: {missing_pct:5.1f}%")
    print()

    # サンプルデータを表示
    print("=" * 80)
    print("🔍 サンプルデータ（先頭5行）")
    print("=" * 80)
    print()
    print(df.head(5).to_string(index=False))
    print()

    # 血統構造の確認
    print("=" * 80)
    print("📋 血統データ構造の分析")
    print("=" * 80)
    print()

    # 特定の馬の血統を追跡
    if 'horse_id' in df.columns and 'ancestor_id' in df.columns:
        sample_horse = df['horse_id'].iloc[0]
        horse_pedigree = df[df['horse_id'] == sample_horse]

        print(f"🐴 サンプル馬: {sample_horse}")
        print(f"   血統レコード数: {len(horse_pedigree)}")
        print()
        print(horse_pedigree.to_string(index=False))
        print()

        # 父母の特定
        print("💡 damsire_id（母父ID）の取得方法:")
        print("-" * 80)
        print("pedigrees.parquetは「馬 → 祖先」の関係を記録しています")
        print()
        print("damsire_idを取得するには:")
        print("1. horse_id から母（dam）のancestor_idを取得")
        print("2. 母のancestor_idを使って、その父（=母父）を取得")
        print()
        print("この処理は horses.parquet または merged_data に")
        print("sire_id, dam_id を追加した後に実施可能です")
        print()
        print("▼ 現在の状況確認:")

        # horses.parquetにsire_id, dam_idが存在するか確認
        horses_path = Path('keibaai/data/parsed/parquet/horses/horses.parquet')
        if horses_path.exists():
            horses_df = pd.read_parquet(horses_path)
            print(f"   horses.parquet: {len(horses_df):,} 行 × {len(horses_df.columns)} 列")
            print()
            print("   カラム一覧:")
            for col in horses_df.columns:
                print(f"     - {col}")
            print()

            if 'sire_id' in horses_df.columns and 'dam_id' in horses_df.columns:
                sire_missing = (horses_df['sire_id'].isna().sum() / len(horses_df)) * 100
                dam_missing = (horses_df['dam_id'].isna().sum() / len(horses_df)) * 100
                print(f"   ✅ sire_id 存在: 欠損 {sire_missing:.1f}%")
                print(f"   ✅ dam_id  存在: 欠損 {dam_missing:.1f}%")
                print()
                print("   → pedigrees.parquetから damsire_id を生成可能です")
            else:
                print("   ❌ sire_id / dam_id が存在しません")
                print()
                print("   💡 対策:")
                print("      1. pedigree_parser.py を修正して sire_id, dam_id を抽出")
                print("      2. または、feature_engine.py でマージ時に pedigrees から取得")
        else:
            print(f"   ⚠️  horses.parquet が存在しません")

    print()

if __name__ == '__main__':
    check_pedigrees()
