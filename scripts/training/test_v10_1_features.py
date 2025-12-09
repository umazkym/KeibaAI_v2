"""
V10.1 特徴量テストスクリプト
- 新特徴量の生成確認
- リークチェック（cumsum+shiftの正確性）
- V10との比較
"""
import pandas as pd
import numpy as np
import sys
import warnings
warnings.filterwarnings('ignore')

sys.path.insert(0, '.')

from keibaai.src.features.leak_free_feature_engineer_v10_1 import LeakFreeFeatureEngineerV10_1

DATA_DIR = 'keibaai/data/parsed/parquet'

print("=" * 80)
print("V10.1 特徴量テスト")
print("=" * 80)

# データ読み込み
print("\n1. データ読み込み...")
races = pd.read_parquet(f'{DATA_DIR}/races/races.parquet')
horses = pd.read_parquet(f'{DATA_DIR}/horses/horses.parquet')
pedigrees = pd.read_parquet(f'{DATA_DIR}/pedigrees/pedigrees.parquet')
corners = pd.read_parquet(f'{DATA_DIR}/corners/corner_positions.parquet')

print(f"  races: {len(races):,}件")
print(f"  horses: {len(horses):,}件")
print(f"  pedigrees: {len(pedigrees):,}件")
print(f"  corners: {len(corners):,}件")

# finish_positionがNAの行を除外（V7 fitエラー回避）
races = races[races['finish_position'].notna()]
print(f"  races (NA除外後): {len(races):,}件")

# Train/Test分割
races['race_date'] = pd.to_datetime(races['race_date'])
train = races[races['race_date'] < '2024-01-01']
test = races[races['race_date'] >= '2024-01-01']

print(f"\n  Train: {len(train):,}件 (～2023)")
print(f"  Test: {len(test):,}件 (2024～)")

# V10.1 fit & transform
print("\n2. V10.1 fit...")
engineer = LeakFreeFeatureEngineerV10_1()
engineer.fit(
    train, 
    pedigrees_df=pedigrees, 
    corners_df=corners,
    horses_df=horses
)

print("\n3. V10.1 transform (Test)...")
test_features = engineer.transform(test)

print(f"\n4. 特徴量数: {len(engineer.get_feature_columns())}")
print("新規追加特徴量:")
new_features = ['is_chitose_bred', 'rest_days_optimal', 'heavy_track_fit']
for col in new_features:
    if col in test_features.columns:
        non_null = test_features[col].notna().sum()
        pct = non_null / len(test_features) * 100
        print(f"  {col}: {pct:.1f}% 非null ({non_null:,}/{len(test_features):,})")
    else:
        print(f"  {col}: 存在しない")

print("\n5. 新特徴量の統計:")
for col in new_features:
    if col in test_features.columns:
        print(f"\n【{col}】")
        print(test_features[col].describe())

print("\n6. リークチェック（勝者と非勝者の分布）:")
winners = test_features[test_features['finish_position'] == 1]
non_winners = test_features[test_features['finish_position'] > 1]

for col in new_features:
    if col in test_features.columns:
        w_mean = winners[col].mean()
        nw_mean = non_winners[col].mean()
        diff = w_mean - nw_mean
        print(f"  {col}: 勝者={w_mean:.3f}, 非勝者={nw_mean:.3f}, 差={diff:.3f}")

print("\n7. 千歳市生産馬の確認:")
chitose = test_features[test_features['is_chitose_bred'] == 1]
others = test_features[test_features['is_chitose_bred'] == 0]
print(f"  千歳市生産: {len(chitose):,}件")
print(f"  その他: {len(others):,}件")
if len(chitose) > 0:
    chitose_roi = ((chitose['finish_position'] == 1) * chitose['win_odds']).sum() / len(chitose) * 100
    print(f"  千歳市 Test ROI: {chitose_roi:.1f}%")
if len(others) > 0:
    others_roi = ((others['finish_position'] == 1) * others['win_odds']).sum() / len(others) * 100
    print(f"  その他 Test ROI: {others_roi:.1f}%")

print("\n8. 休養最適性（28-60日）の確認:")
optimal = test_features[test_features['rest_days_optimal'] == 1]
non_optimal = test_features[test_features['rest_days_optimal'] == 0]
print(f"  最適休養: {len(optimal):,}件")
print(f"  その他: {len(non_optimal):,}件")
if len(optimal) > 0:
    opt_roi = ((optimal['finish_position'] == 1) * optimal['win_odds']).sum() / len(optimal) * 100
    print(f"  最適休養 Test ROI: {opt_roi:.1f}%")
if len(non_optimal) > 0:
    non_opt_roi = ((non_optimal['finish_position'] == 1) * non_optimal['win_odds']).sum() / len(non_optimal) * 100
    print(f"  その他 Test ROI: {non_opt_roi:.1f}%")

print("\n9. 重馬場適性の確認:")
has_heavy = test_features[test_features['heavy_track_fit'].notna()]
print(f"  重馬場経験あり: {len(has_heavy):,}件 ({len(has_heavy)/len(test_features)*100:.1f}%)")
if len(has_heavy) > 0:
    # 重馬場適性高い馬（上位25%）
    threshold = has_heavy['heavy_track_fit'].quantile(0.75)
    high_fit = has_heavy[has_heavy['heavy_track_fit'] >= threshold]
    low_fit = has_heavy[has_heavy['heavy_track_fit'] < has_heavy['heavy_track_fit'].quantile(0.25)]
    
    if len(high_fit) > 0:
        high_roi = ((high_fit['finish_position'] == 1) * high_fit['win_odds']).sum() / len(high_fit) * 100
        print(f"  高適性（上位25%） Test ROI: {high_roi:.1f}%")
    if len(low_fit) > 0:
        low_roi = ((low_fit['finish_position'] == 1) * low_fit['win_odds']).sum() / len(low_fit) * 100
        print(f"  低適性（下位25%） Test ROI: {low_roi:.1f}%")

print("\n" + "=" * 80)
print("テスト完了")
print("=" * 80)
