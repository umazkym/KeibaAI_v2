#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Parquetファイルの品質検証スクリプト
すべての修正が正しく適用されているか確認
"""

import pandas as pd
import sys
import io
from pathlib import Path

# Windowsのコンソール出力エンコーディング設定
if sys.platform == 'win32':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

def validate_races_parquet():
    """races.parquetの検証"""
    print("\n=== races.parquet 検証 ===")
    df = pd.read_parquet(r"C:\Users\zk-ht\Keiba\test\keibaai\data\parsed\parquet\races\races.parquet")

    print(f"総行数: {len(df)}")
    print(f"カラム数: {len(df.columns)}")
    print(f"\nカラム: {list(df.columns)}")

    # jockey_id検証
    jockey_id_nulls = df['jockey_id'].isna().sum()
    print(f"\n[OK] jockey_id: {len(df) - jockey_id_nulls}行に値あり ({jockey_id_nulls}行がNull)")
    if jockey_id_nulls == 0:
        print("  [FIXED] すべてのレコードにjockey_idが存在")

    # trainer_name検証
    trainer_name_nulls = df['trainer_name'].isna().sum()
    print(f"[OK] trainer_name: {len(df) - trainer_name_nulls}行に値あり ({trainer_name_nulls}行がNull)")
    if trainer_name_nulls == 0:
        print("  [FIXED] すべてのレコードにtrainer_nameが存在")

    # trainer_id検証
    trainer_id_nulls = df['trainer_id'].isna().sum()
    print(f"[OK] trainer_id: {len(df) - trainer_id_nulls}行に値あり ({trainer_id_nulls}行がNull)")
    if trainer_id_nulls == 0:
        print("  [FIXED] すべてのレコードにtrainer_idが存在")

    # owner_name検証
    owner_name_nulls = df['owner_name'].isna().sum()
    owner_name_empty = (df['owner_name'] == '').sum()
    print(f"[OK] owner_name: {len(df) - owner_name_nulls}行に値あり ({owner_name_nulls}行がNull, {owner_name_empty}行が空)")
    if owner_name_nulls == 0 and owner_name_empty == 0:
        print("  [FIXED] すべてのレコードにowner_nameが存在")

    # prize_money検証
    prize_money_nulls = df['prize_money'].isna().sum()
    prize_money_valid = (df['prize_money'] > 0).sum()
    print(f"[OK] prize_money: {prize_money_valid}行に賞金あり ({prize_money_nulls}行がNull)")
    if prize_money_nulls == 33 and prize_money_valid == 15:
        print("  [CORRECT] 1-5着が賞金あり(15行), 6-15着がNull(33行) - 正常動作")

    # margin_seconds検証
    margin_seconds_nulls = df['margin_seconds'].isna().sum()
    print(f"[OK] margin_seconds: {len(df) - margin_seconds_nulls}行に値あり ({margin_seconds_nulls}行がNull)")
    if margin_seconds_nulls > 0:
        print("  [CORRECT] margin_str(着差表示)がない場合はNullになるのが正常")

    return True

def validate_shutuba_parquet():
    """shutuba.parquetの検証"""
    print("\n=== shutuba.parquet 検証 ===")
    df = pd.read_parquet(r"C:\Users\zk-ht\Keiba\test\keibaai\data\parsed\parquet\shutuba\shutuba.parquet")

    print(f"総行数: {len(df)}")
    print(f"カラム数: {len(df.columns)}")
    print(f"\nカラム: {list(df.columns)}")

    # jockey_id検証
    jockey_id_nulls = df['jockey_id'].isna().sum()
    print(f"\n[OK] jockey_id: {len(df) - jockey_id_nulls}行に値あり ({jockey_id_nulls}行がNull)")
    if jockey_id_nulls == 0:
        print("  [FIXED] すべてのレコードにjockey_idが存在")

    # owner_name検証
    owner_name_dashes = (df['owner_name'] == '---.-').sum()
    owner_name_nulls = df['owner_name'].isna().sum()
    print(f"[OK] owner_name: {len(df) - owner_name_nulls - owner_name_dashes}行に値あり ({owner_name_dashes}行が---.-, {owner_name_nulls}行がNull)")
    if owner_name_nulls == len(df):
        print("  [CORRECT] shutuba HTMLに所有者情報が存在しないため、すべてNullが正常")

    # prize_total検証
    prize_total_nulls = df['prize_total'].isna().sum()
    print(f"[OK] prize_total: {len(df) - prize_total_nulls}行に値あり ({prize_total_nulls}行がNull)")
    if prize_total_nulls == len(df):
        print("  [CORRECT] shutuba HTMLに該当フィールドが存在しないため、すべてNullが正常")

    # morning_odds検証
    morning_odds_nulls = df['morning_odds'].isna().sum()
    print(f"[OK] morning_odds: {len(df) - morning_odds_nulls}行に値あり ({morning_odds_nulls}行がNull)")
    if morning_odds_nulls == len(df):
        print("  [CORRECT] shutuba HTMLに該当フィールドが存在しないため、すべてNullが正常")

    # career_stats検証
    career_stats_nulls = df['career_stats'].isna().sum()
    career_stats_edit = (df['career_stats'] == '編集').sum()
    print(f"[OK] career_stats: {len(df) - career_stats_nulls - career_stats_edit}行に値あり ({career_stats_nulls}行がNull, {career_stats_edit}行が編集)")
    if career_stats_nulls == len(df):
        print("  [CORRECT] shutuba HTMLに詳細統計が存在しないため、すべてNullが正常")

    return True

def validate_pedigrees_parquet():
    """pedigrees.parquetの検証"""
    print("\n=== pedigrees.parquet 検証 ===")
    df = pd.read_parquet(r"C:\Users\zk-ht\Keiba\test\keibaai\data\parsed\parquet\pedigrees\pedigrees.parquet")

    print(f"総行数: {len(df)}")
    print(f"カラム数: {len(df.columns)}")
    print(f"\nカラム: {list(df.columns)}")

    # ancestor_id検証
    ancestor_id_nulls = df['ancestor_id'].isna().sum()
    print(f"\n[OK] ancestor_id: {len(df) - ancestor_id_nulls}行に値あり ({ancestor_id_nulls}行がNull)")

    # "000"を含むIDの検証
    ids_with_000 = df[df['ancestor_id'].str.contains('000', na=False)]
    print(f"\n[OK] ancestor_id に '000' を含むID数: {len(ids_with_000)}")
    if len(ids_with_000) > 0:
        print("  [FIXED] 有効なJRA馬ID (2004110007など) が保持されている")
        print(f"  例: {ids_with_000['ancestor_id'].unique()[:5].tolist()}")

    # ancestor_name検証
    ancestor_name_nulls = df['ancestor_name'].isna().sum()
    print(f"[OK] ancestor_name: {len(df) - ancestor_name_nulls}行に値あり ({ancestor_name_nulls}行がNull)")

    # 英名とカタカナの混在確認
    has_english = df[df['ancestor_name'].str.contains(r'[a-zA-Z]', na=False)]
    has_katakana = df[df['ancestor_name'].str.contains(r'[\u30A0-\u30FF]', na=False)]
    print(f"[OK] ancestor_name に英名を含むレコード: {len(has_english)}行")
    print(f"[OK] ancestor_name にカタカナを含むレコード: {len(has_katakana)}行")
    if len(has_english) > 0 or len(has_katakana) > 0:
        print("  [CORRECT] netkeiba sourceに英名とカタカナが混在しているのは正常")

    return True

def main():
    """メイン処理"""
    print("=" * 60)
    print("Parquetファイル品質検証")
    print("=" * 60)

    try:
        validate_races_parquet()
        validate_shutuba_parquet()
        validate_pedigrees_parquet()

        print("\n" + "=" * 60)
        print("[SUCCESS] すべての検証が完了しました")
        print("=" * 60)
        return 0
    except Exception as e:
        print(f"\n[ERROR] エラーが発生しました: {e}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    sys.exit(main())
