#!/usr/bin/env python3
"""
horses.parquetに血統情報を追加するスクリプト
pedigrees.parquetのgeneration=1（両親）データを統合
"""

import pandas as pd
from pathlib import Path
from datetime import datetime

def add_pedigree_to_horses():
    """horses.parquetに血統情報を追加"""

    print("\n" + "=" * 80)
    print("🔧 horses.parquetへの血統情報追加処理")
    print("=" * 80)

    # 1. ファイルパスの設定
    horses_path = Path("keibaai/data/parsed/parquet/horses/horses.parquet")
    pedigrees_path = Path("keibaai/data/parsed/parquet/pedigrees/pedigrees.parquet")
    output_path = horses_path  # 上書き保存

    # バックアップ作成
    backup_path = horses_path.parent / f"horses_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"

    # 2. データ読み込み
    print("\n📂 データ読み込み中...")

    if not horses_path.exists():
        print(f"❌ horses.parquetが見つかりません: {horses_path}")
        return False

    if not pedigrees_path.exists():
        print(f"❌ pedigrees.parquetが見つかりません: {pedigrees_path}")
        return False

    horses_df = pd.read_parquet(horses_path)
    pedigrees_df = pd.read_parquet(pedigrees_path)

    print(f"  ✅ horses.parquet: {len(horses_df):,} 行")
    print(f"  ✅ pedigrees.parquet: {len(pedigrees_df):,} 行")

    # 3. バックアップ作成
    print(f"\n💾 バックアップ作成中...")
    horses_df.to_parquet(backup_path, index=False)
    print(f"  ✅ バックアップ保存: {backup_path}")

    # 4. generation=1のデータを抽出
    print(f"\n🔍 generation=1（両親）のデータを抽出...")
    gen1_df = pedigrees_df[pedigrees_df['generation'] == 1].copy()
    print(f"  ✅ generation=1: {len(gen1_df):,} 行")

    # 5. 各馬ごとに両親情報を整形
    print(f"\n🔧 両親情報を整形中...")

    # horse_idでグループ化して、1頭目と2頭目を取得
    parent_data = []

    for horse_id, group in gen1_df.groupby('horse_id'):
        # ソート（念のため、ancestor_idでソート）
        group = group.sort_values('ancestor_id')

        row = {'horse_id': horse_id}

        if len(group) >= 1:
            # 1頭目を便宜的に sire（父）とする
            first = group.iloc[0]
            row['sire_id'] = first['ancestor_id']
            row['sire_name'] = first['ancestor_name']

        if len(group) >= 2:
            # 2頭目を便宜的に dam（母）とする
            second = group.iloc[1]
            row['dam_id'] = second['ancestor_id']
            row['dam_name'] = second['ancestor_name']

        parent_data.append(row)

    parent_df = pd.DataFrame(parent_data)
    print(f"  ✅ 両親情報を整形: {len(parent_df):,} 頭")

    # 6. horses_dfに結合
    print(f"\n🔗 horses.parquetに血統情報を結合中...")

    # 結合前のカラム数
    before_cols = len(horses_df.columns)

    # 左結合（horses_dfをベースに、parent_dfを結合）
    merged_df = horses_df.merge(
        parent_df,
        on='horse_id',
        how='left'
    )

    # 結合後のカラム数
    after_cols = len(merged_df.columns)

    print(f"  ✅ 結合完了")
    print(f"    - 結合前: {before_cols} カラム")
    print(f"    - 結合後: {after_cols} カラム")
    print(f"    - 追加カラム: {after_cols - before_cols} 個")

    # 7. 統計情報の表示
    print(f"\n📊 血統情報の統計:")

    if 'sire_id' in merged_df.columns:
        sire_filled = merged_df['sire_id'].notna().sum()
        sire_rate = sire_filled / len(merged_df) * 100
        print(f"  sire_id:   {sire_filled:>6,} / {len(merged_df):,} 頭 ({sire_rate:>5.1f}%)")

    if 'dam_id' in merged_df.columns:
        dam_filled = merged_df['dam_id'].notna().sum()
        dam_rate = dam_filled / len(merged_df) * 100
        print(f"  dam_id:    {dam_filled:>6,} / {len(merged_df):,} 頭 ({dam_rate:>5.1f}%)")

    # 8. サンプルデータの表示
    print(f"\n🔍 サンプルデータ（最初の3頭）:")
    print("-" * 80)

    # 表示用に一部カラムを選択
    display_cols = ['horse_id', 'horse_name', 'sire_name', 'dam_name']
    available_cols = [col for col in display_cols if col in merged_df.columns]

    if available_cols:
        print(merged_df[available_cols].head(3).to_string(index=False))

    # 9. カラム一覧の表示
    print(f"\n📋 更新後のカラム一覧:")
    for i, col in enumerate(merged_df.columns, 1):
        null_count = merged_df[col].isna().sum()
        null_rate = null_count / len(merged_df) * 100
        marker = "🆕" if col in ['sire_id', 'sire_name', 'dam_id', 'dam_name'] else "  "
        print(f"  {marker} {i:>2}. {col:25s} (欠損: {null_rate:>5.1f}%)")

    # 10. 保存
    print(f"\n💾 保存中...")
    merged_df.to_parquet(output_path, index=False)
    print(f"  ✅ 保存完了: {output_path}")

    # 11. 検証
    print(f"\n✅ 検証:")
    verification_df = pd.read_parquet(output_path)
    print(f"  読み込み確認: {len(verification_df):,} 行 × {len(verification_df.columns)} 列")

    # 血統カラムの存在確認
    pedigree_cols = ['sire_id', 'sire_name', 'dam_id', 'dam_name']
    missing_cols = [col for col in pedigree_cols if col not in verification_df.columns]

    if missing_cols:
        print(f"  ⚠️ 欠落カラム: {missing_cols}")
        return False
    else:
        print(f"  ✅ すべての血統カラムが存在します")

    return True

def main():
    """メイン処理"""
    print("\n")
    print("🔍 KeibaAI_v2 血統情報統合処理")
    print(f"⏰ 実行時刻: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("\n")

    success = add_pedigree_to_horses()

    print("\n" + "=" * 80)
    if success:
        print("✅ 処理完了")
        print("=" * 80)
        print("\n💡 次のアクション:")
        print("  1. 確認: python debug_02_parquet_details.py")
        print("  2. 全件パース処理を実行: python keibaai/src/run_parsing_pipeline_local.py")
    else:
        print("❌ 処理失敗")
        print("=" * 80)

    print("\n")

if __name__ == "__main__":
    main()
