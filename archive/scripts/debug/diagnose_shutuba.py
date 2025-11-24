"""
shutuba.parquetと races.parquetの結合テスト
"""
import pandas as pd

# データ読み込み
print("=" * 60)
print("shutuba.parquetの確認")
print("=" * 60)

shutuba = pd.read_parquet('keibaai/data/parsed/parquet/shutuba/shutuba.parquet')
print(f"総レコード数: {len(shutuba):,}件")

# 2024年データの確認
shutuba_2024 = shutuba[shutuba['race_id'].str.startswith('2024')]
print(f"2024年データ: {len(shutuba_2024):,}件")

if len(shutuba_2024) > 0:
    print(f"\n列数: {len(shutuba_2024.columns)}")
    print(f"サンプル race_id: {shutuba_2024['race_id'].iloc[0]}")
    print(f"\n主要な列:")
    for col in ['race_id', 'horse_id', 'horse_name', 'bracket_number', 'horse_number']:
        if col in shutuba_2024.columns:
            print(f"  {col}: ✓")
        else:
            print(f"  {col}: ✗ (存在しません)")

print("\n" + "=" * 60)
print("races.parquetの確認")
print("=" * 60)

races = pd.read_parquet('keibaai/data/parsed/parquet/races/races.parquet')
races_2024 = races[races['race_id'].str.startswith('2024')]
print(f"2024年データ: {len(races_2024):,}件")

# 賞金列の確認
prize_cols = [col for col in races_2024.columns if 'prize' in col.lower()]
print(f"\n賞金関連列: {prize_cols}")

if 'prize_money' in races_2024.columns:
    non_zero_prize = (races_2024['prize_money'] > 0).sum()
    print(f"prize_money非ゼロ: {non_zero_prize:,}件 ({non_zero_prize/len(races_2024)*100:.1f}%)")

print("\n" + "=" * 60)
print("結合テスト")
print("=" * 60)

# 結合キーの確認
common_races = set(shutuba_2024['race_id']) & set(races_2024['race_id'])
print(f"共通のrace_id: {len(common_races)}件")

# horse_idで結合を試みる
if len(shutuba_2024) > 0 and len(races_2024) > 0:
    # race_idとhorse_idで結合
    merged = pd.merge(
        shutuba_2024,
        races_2024,
        on=['race_id', 'horse_id'],
        how='left',
        suffixes=('_shutuba', '_races')
    )
    
    print(f"\n結合後のレコード数: {len(merged):,}件")
    print(f"結合成功率: {(merged['finish_position'].notna().sum() / len(merged) * 100):.1f}%")
    
    # 賞金列の確認
    if 'prize_money' in merged.columns:
        non_zero = (merged['prize_money'] > 0).sum()
        print(f"prize_money非ゼロ: {non_zero:,}件 ({non_zero/len(merged)*100:.1f}%)")
        
        # サンプル表示
        print(f"\nサンプルデータ（賞金あり）:")
        sample = merged[merged['prize_money'] > 0][['race_id', 'horse_name_races', 'finish_position', 'prize_money']].head(10)
        print(sample)
    else:
        print(f"WARNING: prize_money列が結合後のデータに存在しません")
        print(f"結合後の列: {[col for col in merged.columns if 'prize' in col.lower()]}")

else:
    print("WARNING: shutuba_2024またはraces_2024が空です")

print("\n" + "=" * 60)
print("結論")
print("=" * 60)

if len(shutuba_2024) == 0:
    print("❌ shutuba.parquetに2024年データが含まれていません")
    print("   → shutuba HTMLも再パースが必要です")
elif len(common_races) == 0:
    print("❌ shutubaとracesでrace_idが一致していません")
    print("   → データ形式の問題を確認が必要です")
else:
    print("✓ データは存在します")
    print("  次のステップ: feature_engine.pyのデバッグが必要")
