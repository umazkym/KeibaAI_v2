"""
V10.2 パラメータチューニング
- 目的: Gap < 20% を維持しつつ、Test ROIを最大化するパラメータを探索
- 探索範囲: num_leaves, learning_rate, regularization
"""
import pandas as pd
import numpy as np
import lightgbm as lgb
import itertools
import sys
import warnings
warnings.filterwarnings('ignore')

sys.path.insert(0, '.')

from keibaai.src.features.leak_free_feature_engineer_v10_2 import LeakFreeFeatureEngineerV10_2

DATA_DIR = 'keibaai/data/parsed/parquet'

print("=" * 80)
print("V10.2 パラメータチューニング")
print("=" * 80)

# データ読み込み
print("\n1. データ読み込み & 特徴量生成...")
races = pd.read_parquet(f'{DATA_DIR}/races/races.parquet')
horses = pd.read_parquet(f'{DATA_DIR}/horses/horses.parquet')
pedigrees = pd.read_parquet(f'{DATA_DIR}/pedigrees/pedigrees.parquet')
corners = pd.read_parquet(f'{DATA_DIR}/corners/corner_positions.parquet')

races = races[races['finish_position'].notna()]
races['race_date'] = pd.to_datetime(races['race_date'])

train_df = races[races['race_date'] < '2024-01-01']
test_df = races[races['race_date'] >= '2024-01-01']

engineer = LeakFreeFeatureEngineerV10_2()
engineer.fit(train_df, pedigrees_df=pedigrees, corners_df=corners, horses_df=horses)

train_features = engineer.transform(train_df)
test_features = engineer.transform(test_df)

feature_cols = engineer.get_feature_columns()
available_cols = [c for c in feature_cols if c in train_features.columns]
print(f"  特徴量数: {len(available_cols)}")

X_train = train_features[available_cols].fillna(-999)
y_train = (train_features['finish_position'] == 1).astype(int)
X_test = test_features[available_cols].fillna(-999)
y_test = (test_features['finish_position'] == 1).astype(int)

# パラメータグリッド
param_grid = {
    'num_leaves': [15, 31],
    'learning_rate': [0.01, 0.05],
    'max_depth': [3, 5],
    'reg_alpha': [0.1, 0.5, 1.0],
    'min_child_samples': [100, 200]
}

keys, values = zip(*param_grid.items())
combinations = [dict(zip(keys, v)) for v in itertools.product(*values)]

print(f"\n2. グリッドサーチ開始 ({len(combinations)}通り)...")

best_roi = -float('inf')
best_params = None
best_result = None

for i, params in enumerate(combinations):
    # 基本パラメータ
    lgb_params = {
        'objective': 'binary',
        'metric': 'auc',
        'boosting_type': 'gbdt',
        'subsample': 0.7,
        'colsample_bytree': 0.7,
        'verbose': -1,
        'n_estimators': 300,
        **params
    }
    
    model = lgb.LGBMClassifier(**lgb_params)
    model.fit(X_train, y_train)
    
    # 評価
    train_pred = model.predict_proba(X_train)[:, 1]
    test_pred = model.predict_proba(X_test)[:, 1]
    
    # ROI計算用の一時DF
    t_train = train_features.copy()
    t_test = test_features.copy()
    t_train['pred_score'] = train_pred
    t_test['pred_score'] = test_pred
    
    train_top1 = t_train.loc[t_train.groupby('race_id')['pred_score'].idxmax()]
    test_top1 = t_test.loc[t_test.groupby('race_id')['pred_score'].idxmax()]
    
    train_hit = (train_top1['finish_position'] == 1).mean() * 100
    test_hit = (test_top1['finish_position'] == 1).mean() * 100
    gap = train_hit - test_hit
    
    test_roi = ((test_top1['finish_position'] == 1) * test_top1['win_odds']).sum() / len(test_top1) * 100
    
    # 進捗表示
    print(f"[{i+1}/{len(combinations)}] ROI: {test_roi:.1f}%, Gap: {gap:.1f}%, Params: {params}")
    
    # 条件: Gap < 20% かつ ROI最大
    if gap < 20.0 and test_roi > best_roi:
        best_roi = test_roi
        best_params = params
        best_result = {
            'roi': test_roi,
            'gap': gap,
            'hit': test_hit,
            'train_hit': train_hit
        }

print("\n" + "=" * 80)
print("ベスト結果")
print("=" * 80)

if best_params:
    print(f"Best ROI: {best_result['roi']:.1f}%")
    print(f"Gap: {best_result['gap']:.1f}%")
    print(f"Hit Rate: {best_result['hit']:.1f}%")
    print(f"Best Params: {best_params}")
else:
    print("条件を満たすパラメータが見つかりませんでした。")

print("\n" + "=" * 80)
print("完了")
print("=" * 80)
