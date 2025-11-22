"""
feature_engine.pyの_create_combined_timeseries_dfメソッドをデバッグ
"""
import pandas as pd
import logging
import sys
from pathlib import Path

# プロジェクトルートを追加
project_root = Path(__file__).resolve().parent / "keibaai"
sys.path.append(str(project_root))

# ログ設定
logging.basicConfig(level=logging.DEBUG, format='%(message)s')

# データ読み込み
races = pd.read_parquet('keibaai/data/parsed/parquet/races/races.parquet')
shutuba = pd.read_parquet('keibaai/data/parsed/parquet/shutuba/shutuba.parquet')

# 2024年1月のデータでテスト
races = races[races['race_id'].str.startswith('202401')].copy()
shutuba = shutuba[shutuba['race_id'].str.startswith('202401')].copy()

print("=" * 60)
print("データ確認")
print("=" * 60)
print(f"races: {len(races)}件")
print(f"shutuba: {len(shutuba)}件")

# prize関連列
prize_cols = [col for col in races.columns if 'prize' in col.lower()]
print(f"\nraces の賞金列: {prize_cols}")

if 'prize_money' in races.columns:
    non_zero = (races['prize_money'] > 0).sum()
    print(f"  prize_money非ゼロ: {non_zero}件")
    
    # サンプル表示
    sample = races[races['prize_money'] > 0][['race_id', 'horse_name', 'finish_position', 'prize_money']].head(5)
    print(f"\nサンプル（prize_money > 0）:")
    print(sample)

# 結合
print("\n" + "=" * 60)
print("結合テスト")
print("=" * 60)

combined = pd.merge(
    shutuba,
    races,
    on=['race_id', 'horse_id'],
    how='left',
    suffixes=('', '_hist')
)

print(f"結合後: {len(combined)}件")

# prize_moneyが結合後に存在するか
if 'prize_money' in combined.columns:
    non_zero = (combined['prize_money'] > 0).sum()
    print(f"prize_money非ゼロ: {non_zero}件 ({non_zero/len(combined)*100:.1f}%)")
    
    # サンプル
    sample = combined[combined['prize_money'] > 0][['race_id', 'horse_id', 'finish_position', 'prize_money']].head(10)
    print(f"\n結合後のサンプル（prize_money > 0）:")
    print(sample)
else:
    print("WARNING: prize_money列が存在しません")
    print(f"利用可能な列: {[col for col in combined.columns if 'prize' in col.lower()]}")

# 次に、feature_engine.pyの _create_combined_timeseries_df のロジックを模倣
print("\n" + "=" * 60)
print("feature_engine.pyのロジックを模倣")
print("=" * 60)

# shutubaとracesを両方とも含む時系列データフレーム
all_races = races.copy()
all_races = all_races.sort_values(by=['horse_id', 'race_date'], ascending=[True, True])

print(f"\nall_races: {len(all_races)}件")

# prize_moneyを計算（feature_engine.pyの Line 168-179）
if 'finish_position' in all_races.columns:
    all_races['prize_money_calc'] = 0.0
    for pos in range(1, 6):
        prize_col = f'prize_{["1st", "2nd", "3rd", "4th", "5th"][pos-1]}'
        if prize_col in all_races.columns:
            mask = all_races['finish_position'] == pos
            all_races.loc[mask, 'prize_money_calc'] = all_races.loc[mask, prize_col].fillna(0)
    
    non_zero_calc = (all_races['prize_money_calc'] > 0).sum()
    print(f"prize_money_calc非ゼロ: {non_zero_calc}件 ({non_zero_calc/len(all_races)*100:.1f}%)")
    
    # 元のprize_moneyと比較
    if 'prize_money' in all_races.columns:
        match = (all_races['prize_money'] == all_races['prize_money_calc']).sum()
        print(f"prize_moneyとprize_money_calcの一致: {match}/{len(all_races)}件")

print("\n" + "=" * 60)
print("結論")
print("=" * 60)

if non_zero_calc > 0:
    print("✓ prize_money計算ロジックは正常に動作しています")
    print("  問題は feature_engine.py の他の部分にある可能性があります")
    print("  → _add_career_stats や _add_past_performance_features を確認")
else:
    print("✗ prize_money計算ロジックに問題があります")
    print("  → prize_1st~prize_5th列が存在しないか、値が全てNaN")
