"""
races.parquetから直接prize_totalを計算して確認
feature_engine.pyを使わないテスト
"""
import pandas as pd

print("=" * 60)
print("手動でprize_total計算テスト")
print("=" * 60)

# races.parquetを読み込み
races = pd.read_parquet('keibaai/data/parsed/parquet/races/races.parquet')

print(f"\n総レコード数: {len(races):,}件")

# prize_money列の確認
if 'prize_money' in races.columns:
    non_zero = (races['prize_money'] > 0).sum()
    print(f"prize_money非ゼロ: {non_zero:,}件 ({non_zero/len(races)*100:.1f}%)")
    print(f"prize_money平均: {races[races['prize_money'] > 0]['prize_money'].mean():.1f}万円")
else:
    print("ERROR: prize_money列が存在しません")
    print(f"利用可能な列: {races.columns.tolist()}")
    exit(1)

# 各馬のprize_totalを計算
print("\n" + "=" * 60)
print("馬ごとの総獲得賞金を計算")
print("=" * 60)

prize_total_by_horse = races.groupby('horse_id')['prize_money'].sum().reset_index()
prize_total_by_horse.columns = ['horse_id', 'prize_total']

print(f"\n馬の数: {len(prize_total_by_horse):,}頭")

# 賞金を獲得したことがある馬
horses_with_prize = prize_total_by_horse[prize_total_by_horse['prize_total'] > 0]
print(f"賞金獲得経験あり: {len(horses_with_prize):,}頭 ({len(horses_with_prize)/len(prize_total_by_horse)*100:.1f}%)")

# 統計
print(f"\nprize_total統計:")
print(f"  最小: {prize_total_by_horse['prize_total'].min():.1f}万円")
print(f"  最大: {prize_total_by_horse['prize_total'].max():.1f}万円")
print(f"  平均: {horses_with_prize['prize_total'].mean():.1f}万円")
print(f"  中央値: {horses_with_prize['prize_total'].median():.1f}万円")

# 上位10頭
print(f"\n総獲得賞金TOP10:")
top10 = prize_total_by_horse.nlargest(10, 'prize_total')
for idx, row in top10.iterrows():
    print(f"  {row['horse_id']}: {row['prize_total']:,.0f}万円")

# 出馬表と結合してテスト
print("\n" + "=" * 60)
print("shutuba.parquetと結合テスト")
print("=" * 60)

shutuba = pd.read_parquet('keibaai/data/parsed/parquet/shutuba/shutuba.parquet')
shutuba_2024 = shutuba[shutuba['race_id'].str.startswith('202401')].copy()

print(f"2024年1月の出馬表: {len(shutuba_2024)}件")

# prize_totalをマージ
shutuba_with_prize = shutuba_2024.merge(prize_total_by_horse, on='horse_id', how='left')
shutuba_with_prize['prize_total'] = shutuba_with_prize['prize_total'].fillna(0)

print(f"\n結合後:")
non_zero = (shutuba_with_prize['prize_total'] > 0).sum()
print(f"  prize_total非ゼロ: {non_zero}/{len(shutuba_with_prize)} ({non_zero/len(shutuba_with_prize)*100:.1f}%)")
print(f"  prize_total平均: {shutuba_with_prize[shutuba_with_prize['prize_total'] > 0]['prize_total'].mean():.1f}万円")

# サンプル表示
print(f"\nprize_total上位5件:")
sample = shutuba_with_prize.nlargest(5, 'prize_total')[['horse_id', 'horse_name', 'prize_total']]
print(sample)

print("\n" + "=" * 60)
print("結論")
print("=" * 60)

if non_zero > 0:
    print("✅ races.parquetから手動でprize_totalを計算できました")
    print("✅ shutubaと結合も成功しました")
    print("\n→ feature_engine.pyに問題があることが確定")
    print("  次のステップ: feature_engine.pyのデバッグログを確認")
else:
    print("❌ 手動計算でもprize_totalがゼロです")
    print("  races.parquetのデータに問題がある可能性")
