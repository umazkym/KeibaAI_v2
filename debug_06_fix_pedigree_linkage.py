#!/usr/bin/env python3
"""
デバッグスクリプト06: 血統情報の補完
pedigrees.parquet から horses.parquet の sire_id, dam_id を補完
"""

import pandas as pd
from pathlib import Path
from datetime import datetime

def main():
    print("=" * 80)
    print("🔧 KeibaAI_v2 血統情報の補完")
    print(f"⏰ 実行時刻: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)

    base_dir = Path("keibaai/data/parsed/parquet")

    # ファイルパス
    horses_path = base_dir / "horses" / "horses.parquet"
    pedigrees_path = base_dir / "pedigrees" / "pedigrees.parquet"

    print("\n" + "=" * 80)
    print("📁 データのロード")
    print("=" * 80)

    # horses.parquet をロード
    horses_df = pd.read_parquet(horses_path)
    print(f"\n✅ horses.parquet: {len(horses_df):,}行 × {len(horses_df.columns)}列")

    # pedigrees.parquet をロード
    pedigrees_df = pd.read_parquet(pedigrees_path)
    print(f"✅ pedigrees.parquet: {len(pedigrees_df):,}行 × {len(pedigrees_df.columns)}列")

    print("\n" + "=" * 80)
    print("🔍 現在の血統情報の欠損状況")
    print("=" * 80)

    total = len(horses_df)
    sire_null = horses_df['sire_id'].isna().sum()
    dam_null = horses_df['dam_id'].isna().sum()

    print(f"\nsire_id の欠損: {sire_null:,} / {total:,} ({sire_null/total*100:.2f}%)")
    print(f"dam_id の欠損: {dam_null:,} / {total:,} ({dam_null/total*100:.2f}%)")

    print("\n" + "=" * 80)
    print("⚙️ pedigrees.parquet から血統情報を補完")
    print("=" * 80)

    # pedigrees から generation=1 のデータを抽出
    # generation=1 は第1世代（父母）を示す
    # 血統テーブルの構造上、最初の2つのancestorが父と母
    print("\n🔍 pedigrees.parquet の構造分析...")
    print(f"  ユニーク馬数: {pedigrees_df['horse_id'].nunique():,}")
    print(f"  世代の種類: {sorted(pedigrees_df['generation'].unique())}")

    # 各馬の generation=1 データを取得
    gen1_df = pedigrees_df[pedigrees_df['generation'] == 1].copy()
    print(f"\n  generation=1 のレコード数: {len(gen1_df):,}")

    # 各馬ごとに父母を特定
    # 通常、血統テーブルの並び順で最初が父、2番目が母
    print("\n🔨 各馬の父母を特定中...")

    sire_dam_map = {}

    for horse_id in gen1_df['horse_id'].unique():
        ancestors = gen1_df[gen1_df['horse_id'] == horse_id].sort_index()

        if len(ancestors) >= 2:
            # 最初の2つを父と母と仮定
            sire_id = ancestors.iloc[0]['ancestor_id']
            dam_id = ancestors.iloc[1]['ancestor_id']
            sire_dam_map[horse_id] = {
                'sire_id': sire_id,
                'dam_id': dam_id,
                'sire_name': ancestors.iloc[0]['ancestor_name'],
                'dam_name': ancestors.iloc[1]['ancestor_name']
            }
        elif len(ancestors) == 1:
            # 1つしかない場合は父のみ
            sire_id = ancestors.iloc[0]['ancestor_id']
            sire_dam_map[horse_id] = {
                'sire_id': sire_id,
                'dam_id': None,
                'sire_name': ancestors.iloc[0]['ancestor_name'],
                'dam_name': None
            }

    print(f"  特定できた馬数: {len(sire_dam_map):,}")

    # horses_df に補完
    print("\n🔧 horses.parquet に血統情報を補完中...")

    updated_count = 0

    for idx, row in horses_df.iterrows():
        horse_id = row['horse_id']

        # 既に血統情報がある場合はスキップ
        if pd.notna(row['sire_id']) and pd.notna(row['dam_id']):
            continue

        # pedigrees から取得
        if horse_id in sire_dam_map:
            pedigree_info = sire_dam_map[horse_id]

            # sire_id が欠損している場合のみ補完
            if pd.isna(row['sire_id']):
                horses_df.at[idx, 'sire_id'] = pedigree_info['sire_id']
                horses_df.at[idx, 'sire_name'] = pedigree_info['sire_name']

            # dam_id が欠損している場合のみ補完
            if pd.isna(row['dam_id']):
                horses_df.at[idx, 'dam_id'] = pedigree_info['dam_id']
                horses_df.at[idx, 'dam_name'] = pedigree_info['dam_name']

            updated_count += 1

    print(f"  補完した馬数: {updated_count:,}")

    print("\n" + "=" * 80)
    print("📊 補完後の血統情報の状況")
    print("=" * 80)

    sire_null_after = horses_df['sire_id'].isna().sum()
    dam_null_after = horses_df['dam_id'].isna().sum()

    print(f"\nsire_id の欠損: {sire_null_after:,} / {total:,} ({sire_null_after/total*100:.2f}%)")
    print(f"  改善: {sire_null - sire_null_after:,} 件")

    print(f"\ndam_id の欠損: {dam_null_after:,} / {total:,} ({dam_null_after/total*100:.2f}%)")
    print(f"  改善: {dam_null - dam_null_after:,} 件")

    # サンプル確認
    print("\n=== 補完されたデータのサンプル（最新20件） ===")
    print(horses_df[['horse_id', 'horse_name', 'sire_id', 'sire_name', 'dam_id', 'dam_name']].tail(20).to_string())

    print("\n" + "=" * 80)
    print("💾 更新データの保存")
    print("=" * 80)

    # バックアップ
    backup_path = horses_path.parent / f"horses_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"
    horses_df_original = pd.read_parquet(horses_path)
    horses_df_original.to_parquet(backup_path, index=False)
    print(f"\n✅ バックアップ保存: {backup_path}")

    # 上書き保存
    horses_df.to_parquet(horses_path, index=False)
    print(f"✅ 更新版を保存: {horses_path}")

    print("\n" + "=" * 80)
    print("📋 次のステップ")
    print("=" * 80)

    print("\n💡 血統情報の補完完了！次は以下を実行します:")
    print("\n1. ベーステーブルを再作成")
    print("   → python debug_04_create_analysis_base_table.py")
    print("\n2. 血統情報の欠損率を再確認")
    print("   → 91% → 数%に改善されているはず")
    print("\n3. 特徴量生成テスト")
    print("   → python keibaai/src/features/generate_features.py")

    print("\n" + "=" * 80)
    print("✅ デバッグスクリプト06完了")
    print("=" * 80)


if __name__ == "__main__":
    main()
