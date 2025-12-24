"""
V7 追加検証: モデル予測のバイアスチェック

重要な疑問:
1. テストスクリプトでのモデル学習は正しいTrainデータのみで行われているか
2. Top1予測の的中率が高い理由は何か
3. 特徴量にリークが隠れていないか
"""
import sys
sys.path.insert(0, '.')
import pandas as pd
import numpy as np
import lightgbm as lgb
from keibaai.src.features.leak_free_feature_engineer_v7 import LeakFreeFeatureEngineerV7

print("="*80)
print("V7 追加検証: モデル予測バイアスチェック")
print("="*80)

# データ読み込み
races_df = pd.read_parquet('keibaai/data/parsed/parquet/races/races.parquet')
races_df['race_date'] = pd.to_datetime(races_df['race_date'])
races_df = races_df.dropna(subset=['finish_position', 'win_odds'])
returns_df = pd.read_parquet('keibaai/data/parsed/parquet/returns/returns.parquet')

train_df = races_df[races_df['race_date'] < '2024-07-01'].copy()
valid_df = races_df[(races_df['race_date'] >= '2024-07-01') & (races_df['race_date'] < '2025-01-01')].copy()
test_df = races_df[races_df['race_date'] >= '2025-01-01'].copy()

print(f"\nTrain: {len(train_df):,}")
print(f"Valid: {len(valid_df):,}")
print(f"Test:  {len(test_df):,}")

# V7 fit & transform
fe = LeakFreeFeatureEngineerV7()
fe.fit(races_df=train_df)

train_features = fe.transform(train_df)
valid_features = fe.transform(valid_df)
test_features = fe.transform(test_df)

# 使用する特徴量を確認
feature_cols = fe.get_feature_columns()
feature_cols = [c for c in feature_cols if c in train_features.columns]
print(f"\n使用特徴量: {len(feature_cols)}")

# ================================================================
# 検証4: finish_positionが特徴量に混入していないか
# ================================================================
print("\n" + "="*80)
print("検証4: finish_positionが特徴量に混入していないか")
print("="*80)

if 'finish_position' in feature_cols:
    print("⚠️ 重大なリーク: finish_positionが特徴量に含まれています!")
else:
    print("✓ finish_positionは特徴量に含まれていません")

# ================================================================
# 検証5: horse_last_finishとfinish_positionの相関
# ================================================================
print("\n" + "="*80)
print("検証5: horse_last_finish（前走着順）とfinish_position（今回着順）の関係")
print("="*80)

# Test期間での相関
corr = test_features[['horse_last_finish', 'finish_position']].corr()
print(f"相関係数: {corr.iloc[0,1]:.3f}")

# horse_last_finishが1（前走1着）の馬の今回着順
prev_winners = test_features[test_features['horse_last_finish'] == 1]
current_pos = prev_winners['finish_position'].mean()
print(f"\n前走1着の馬の今回平均着順: {current_pos:.2f}")

# horse_last_finishが10以上の馬の今回着順
prev_losers = test_features[test_features['horse_last_finish'] >= 10]
current_pos = prev_losers['finish_position'].mean()
print(f"前走10着以下の馬の今回平均着順: {current_pos:.2f}")

# ================================================================
# 検証6: ランダムベースラインとの比較
# ================================================================
print("\n" + "="*80)
print("検証6: ランダム予測 vs V7予測のROI比較")
print("="*80)

# ランダム予測
np.random.seed(42)
test_features_copy = test_features.copy()
test_features_copy['random_score'] = np.random.randn(len(test_features_copy))
test_features_copy['random_rank'] = test_features_copy.groupby('race_id')['random_score'].rank(ascending=False)
random_top1 = test_features_copy[test_features_copy['random_rank'] == 1]

# 払い戻しデータをマージ
tansho = returns_df[returns_df['bet_type'] == 'tansho'][['race_id', 'horse_number', 'payout']].copy()
tansho = tansho.rename(columns={'payout': 'tansho_payout'})
random_top1 = random_top1.merge(tansho, on=['race_id', 'horse_number'], how='left')

random_bet = len(random_top1) * 100
random_roi = random_top1['tansho_payout'].fillna(0).sum() / random_bet * 100
print(f"ランダム予測のROI: {random_roi:.1f}%")

# オッズ最低の馬を選ぶ単純戦略
test_features_copy['odds_rank'] = test_features_copy.groupby('race_id')['win_odds'].rank(ascending=True)
favorite_top1 = test_features_copy[test_features_copy['odds_rank'] == 1]
favorite_top1 = favorite_top1.merge(tansho, on=['race_id', 'horse_number'], how='left')

fav_bet = len(favorite_top1) * 100
fav_roi = favorite_top1['tansho_payout'].fillna(0).sum() / fav_bet * 100
print(f"1番人気を買う戦略のROI: {fav_roi:.1f}%")

# ================================================================
# 検証7: モデル予測と実際のオッズの関係
# ================================================================
print("\n" + "="*80)
print("検証7: モデルの予測傾向分析")
print("="*80)

# モデル学習（簡易版）
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

# Test予測
test_features['pred_score'] = model.predict(test_features[feature_cols].fillna(0))
test_features['pred_rank'] = test_features.groupby('race_id')['pred_score'].rank(ascending=False)

# Top1予測の馬のオッズ分布
v7_top1 = test_features[test_features['pred_rank'] == 1]
print(f"\nV7がTop1予測した馬のオッズ分布：")
print(f"  平均: {v7_top1['win_odds'].mean():.1f}")
print(f"  中央値: {v7_top1['win_odds'].median():.1f}")

# V7 Top1予測と実際の1番人気の一致率
v7_top1_with_fav = v7_top1.merge(
    test_features_copy[test_features_copy['odds_rank'] == 1][['race_id', 'horse_number']].rename(columns={'horse_number': 'fav_horse'}),
    on='race_id', how='left'
)
match_rate = (v7_top1_with_fav['horse_number'] == v7_top1_with_fav['fav_horse']).mean() * 100
print(f"\nV7 Top1と1番人気の一致率: {match_rate:.1f}%")

# V7 Top1のROI
v7_top1 = v7_top1.merge(tansho, on=['race_id', 'horse_number'], how='left')
v7_bet = len(v7_top1) * 100
v7_roi = v7_top1['tansho_payout'].fillna(0).sum() / v7_bet * 100
print(f"V7予測のROI: {v7_roi:.1f}%")

# ================================================================
# 検証8: 時系列での安定性
# ================================================================
print("\n" + "="*80)
print("検証8: 月別ROIの安定性")
print("="*80)

v7_top1['month'] = v7_top1['race_date'].dt.to_period('M')
for month, group in v7_top1.groupby('month'):
    bet = len(group) * 100
    roi = group['tansho_payout'].fillna(0).sum() / bet * 100
    hit = (group['finish_position'] == 1).mean() * 100
    print(f"  {month}: n={len(group):>4}, 的中率={hit:>5.1f}%, ROI={roi:>6.1f}%")

print("\n" + "="*80)
print("検証完了")
print("="*80)
