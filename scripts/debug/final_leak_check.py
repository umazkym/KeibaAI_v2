"""
V7 最終検証: 33特徴量での完全検証
"""
import sys
sys.path.insert(0, '.')
import pandas as pd
import numpy as np
import lightgbm as lgb
from keibaai.src.features.leak_free_feature_engineer_v7 import LeakFreeFeatureEngineerV7

print("="*80)
print("V7 最終検証: 33特徴量での完全検証（test_v7と同条件）")
print("="*80)

# データ読み込み
races_df = pd.read_parquet('keibaai/data/parsed/parquet/races/races.parquet')
races_df['race_date'] = pd.to_datetime(races_df['race_date'])
races_df = races_df.dropna(subset=['finish_position', 'win_odds'])

pedigrees_df = pd.read_parquet('keibaai/data/parsed/parquet/pedigrees/pedigrees.parquet')
corners_df = pd.read_parquet('keibaai/data/parsed/parquet/corners/corner_positions.parquet')
race_details_df = pd.read_parquet('keibaai/data/parsed/parquet/race_details/race_details.parquet')
returns_df = pd.read_parquet('keibaai/data/parsed/parquet/returns/returns.parquet')

train_df = races_df[races_df['race_date'] < '2024-07-01'].copy()
valid_df = races_df[(races_df['race_date'] >= '2024-07-01') & (races_df['race_date'] < '2025-01-01')].copy()
test_df = races_df[races_df['race_date'] >= '2025-01-01'].copy()

print(f"\nTrain: {len(train_df):,}")
print(f"Valid: {len(valid_df):,}")
print(f"Test:  {len(test_df):,}")

# V7 fit (全データ渡す - test_v7と同条件)
fe = LeakFreeFeatureEngineerV7()
fe.fit(
    races_df=train_df,
    pedigrees_df=pedigrees_df,
    corners_df=corners_df,
    race_details_df=race_details_df,
    returns_df=returns_df
)

train_features = fe.transform(train_df)
valid_features = fe.transform(valid_df)
test_features = fe.transform(test_df)

feature_cols = fe.get_feature_columns()
feature_cols = [c for c in feature_cols if c in train_features.columns]
print(f"使用特徴量: {len(feature_cols)}")
print(f"特徴量リスト: {feature_cols}")

# モデル学習（test_v7と同じパラメータ）
train_sorted = train_features.sort_values('race_id')
X_train = train_sorted[feature_cols].fillna(0)
y_train = train_sorted['finish_position']
groups_train = train_sorted.groupby('race_id').size().values

y_rel_train = np.zeros(len(y_train))
y_rel_train[y_train.values == 1] = 5
y_rel_train[y_train.values == 2] = 4
y_rel_train[y_train.values == 3] = 3
y_rel_train[(y_train.values >= 4) & (y_train.values <= 5)] = 1

valid_sorted = valid_features.sort_values('race_id')
X_valid = valid_sorted[feature_cols].fillna(0)
y_valid = valid_sorted['finish_position']
groups_valid = valid_sorted.groupby('race_id').size().values

y_rel_valid = np.zeros(len(y_valid))
y_rel_valid[y_valid.values == 1] = 5
y_rel_valid[y_valid.values == 2] = 4
y_rel_valid[y_valid.values == 3] = 3
y_rel_valid[(y_valid.values >= 4) & (y_valid.values <= 5)] = 1

model = lgb.LGBMRanker(
    objective='lambdarank',
    n_estimators=3000,
    learning_rate=0.005,
    max_depth=2,
    num_leaves=4,
    min_child_samples=200,
    reg_alpha=3.0,
    reg_lambda=5.0,
    subsample=0.5,
    colsample_bytree=0.5,
    max_bin=63,
    min_split_gain=0.1,
    random_state=42,
    verbose=-1
)

model.fit(
    X_train, y_rel_train,
    group=groups_train,
    eval_set=[(X_valid, y_rel_valid)],
    eval_group=[groups_valid],
    callbacks=[lgb.early_stopping(50, verbose=False)]
)

print(f"Best iteration: {model.best_iteration_}")

# Test予測
test_features['pred_score'] = model.predict(test_features[feature_cols].fillna(0))
test_features['pred_rank'] = test_features.groupby('race_id')['pred_score'].rank(ascending=False)
v7_top1 = test_features[test_features['pred_rank'] == 1].copy()

# ROI計算
tansho = returns_df[returns_df['bet_type'] == 'tansho'][['race_id', 'horse_number', 'payout']].copy()
tansho = tansho.rename(columns={'payout': 'tansho_payout'})
v7_top1 = v7_top1.merge(tansho, on=['race_id', 'horse_number'], how='left')

v7_bet = len(v7_top1) * 100
v7_roi = v7_top1['tansho_payout'].fillna(0).sum() / v7_bet * 100
hit_rate = (v7_top1['finish_position'] == 1).mean() * 100

print(f"\n【全体結果】")
print(f"  レース数: {len(v7_top1)}")
print(f"  的中率: {hit_rate:.1f}%")
print(f"  単勝ROI: {v7_roi:.1f}%")

# 月別ROI
print(f"\n【月別ROI】")
v7_top1['month'] = v7_top1['race_date'].dt.to_period('M')
monthly_data = []
for month, group in v7_top1.groupby('month'):
    bet = len(group) * 100
    roi = group['tansho_payout'].fillna(0).sum() / bet * 100
    hit = (group['finish_position'] == 1).mean() * 100
    monthly_data.append({'month': str(month), 'n': len(group), 'hit': hit, 'roi': roi})
    print(f"  {month}: n={len(group):>4}, 的中率={hit:>5.1f}%, ROI={roi:>6.1f}%")

# 月別ROIの平均
avg_monthly_roi = np.mean([d['roi'] for d in monthly_data])
print(f"\n月別ROIの単純平均: {avg_monthly_roi:.1f}%")

# オッズ帯別ROI
print(f"\n【オッズ帯別ROI】")
for odds_min, odds_max in [(1,3), (3,5), (5,10), (10,20), (20,50), (50,80)]:
    subset = v7_top1[(v7_top1['win_odds'] >= odds_min) & (v7_top1['win_odds'] < odds_max)]
    if len(subset) > 0:
        bet = len(subset) * 100
        roi = subset['tansho_payout'].fillna(0).sum() / bet * 100
        print(f"  {odds_min:>2}-{odds_max:<2}倍: n={len(subset):>3}, ROI={roi:>6.1f}%")

print("\n" + "="*80)
print("検証完了")
print("="*80)
