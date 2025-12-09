"""
V10.1 モデル訓練・ROI検証スクリプト（強化正則化版）
- 過学習を抑制するため正則化を大幅に強化
- V10 vs V10.1 のROI比較
"""
import pandas as pd
import numpy as np
import sys
import warnings
warnings.filterwarnings('ignore')

sys.path.insert(0, '.')

from keibaai.src.features.leak_free_feature_engineer_v10_1 import LeakFreeFeatureEngineerV10_1
from keibaai.src.features.leak_free_feature_engineer_v10 import LeakFreeFeatureEngineerV10
import lightgbm as lgb

DATA_DIR = 'keibaai/data/parsed/parquet'

print("=" * 80)
print("V10.1 モデル訓練・ROI検証（強化正則化版）")
print("=" * 80)

# データ読み込み
print("\n1. データ読み込み...")
races = pd.read_parquet(f'{DATA_DIR}/races/races.parquet')
horses = pd.read_parquet(f'{DATA_DIR}/horses/horses.parquet')
pedigrees = pd.read_parquet(f'{DATA_DIR}/pedigrees/pedigrees.parquet')
corners = pd.read_parquet(f'{DATA_DIR}/corners/corner_positions.parquet')

# NA除外
races = races[races['finish_position'].notna()]
races['race_date'] = pd.to_datetime(races['race_date'])

# Train/Test分割
train = races[races['race_date'] < '2024-01-01']
test = races[races['race_date'] >= '2024-01-01']

print(f"  Train: {len(train):,}件")
print(f"  Test: {len(test):,}件")

# LightGBMのパラメータ（非常に強い正則化）
lgb_params = {
    'objective': 'binary',
    'metric': 'auc',
    'boosting_type': 'gbdt',
    'learning_rate': 0.005,     # より低い学習率
    'num_leaves': 7,            # 非常に少ない葉
    'max_depth': 2,             # 非常に浅い木
    'min_child_samples': 500,   # 非常に大きな最小サンプル
    'subsample': 0.5,           # 強いサブサンプリング
    'colsample_bytree': 0.5,    # 強い列サブサンプリング
    'reg_alpha': 2.0,           # 強いL1正則化
    'reg_lambda': 2.0,          # 強いL2正則化
    'verbose': -1,
    'n_estimators': 500,
}

def train_and_evaluate(engineer, train_df, test_df, model_name, corners_df=None, horses_df=None, pedigrees_df=None):
    """特徴量エンジニアでモデル訓練・評価"""
    print(f"\n--- {model_name} ---")
    
    # fit
    if hasattr(engineer, '_horses_df') and horses_df is not None:
        engineer.fit(train_df, pedigrees_df=pedigrees_df, corners_df=corners_df, horses_df=horses_df)
    else:
        engineer.fit(train_df, pedigrees_df=pedigrees_df, corners_df=corners_df)
    
    # transform
    train_features = engineer.transform(train_df)
    test_features = engineer.transform(test_df)
    
    feature_cols = engineer.get_feature_columns()
    available_cols = [c for c in feature_cols if c in train_features.columns]
    
    print(f"  特徴量数: {len(available_cols)}")
    
    # 訓練データ準備
    X_train = train_features[available_cols].fillna(-999)
    y_train = (train_features['finish_position'] == 1).astype(int)
    
    X_test = test_features[available_cols].fillna(-999)
    y_test = (test_features['finish_position'] == 1).astype(int)
    
    # モデル訓練
    model = lgb.LGBMClassifier(**lgb_params)
    model.fit(X_train, y_train)
    
    # 予測
    train_pred = model.predict_proba(X_train)[:, 1]
    test_pred = model.predict_proba(X_test)[:, 1]
    
    train_features['pred_score'] = train_pred
    test_features['pred_score'] = test_pred
    
    # レースごとにTop1を選択
    train_top1 = train_features.loc[train_features.groupby('race_id')['pred_score'].idxmax()]
    test_top1 = test_features.loc[test_features.groupby('race_id')['pred_score'].idxmax()]
    
    # ROI計算
    train_wins = (train_top1['finish_position'] == 1)
    train_roi = (train_wins * train_top1['win_odds']).sum() / len(train_top1) * 100
    train_hit = train_wins.mean() * 100
    
    test_wins = (test_top1['finish_position'] == 1)
    test_roi = (test_wins * test_top1['win_odds']).sum() / len(test_top1) * 100
    test_hit = test_wins.mean() * 100
    
    gap = train_hit - test_hit
    
    print(f"  Train ROI: {train_roi:.1f}%, Hit: {train_hit:.1f}%")
    print(f"  Test ROI: {test_roi:.1f}%, Hit: {test_hit:.1f}%")
    print(f"  Gap (Train-Test Hit): {gap:.1f}%")
    
    # 特徴量重要度Top10
    importance = pd.DataFrame({
        'feature': available_cols,
        'importance': model.feature_importances_
    }).sort_values('importance', ascending=False)
    print("  Top10 特徴量重要度:")
    for _, row in importance.head(10).iterrows():
        print(f"    {row['feature']}: {row['importance']:.0f}")
    
    return {
        'model': model_name,
        'train_roi': train_roi,
        'test_roi': test_roi,
        'train_hit': train_hit,
        'test_hit': test_hit,
        'gap': gap,
        'features': len(available_cols)
    }

# V10 ベースライン
print("\n2. V10 (ベースライン) 訓練...")
v10 = LeakFreeFeatureEngineerV10()
v10_results = train_and_evaluate(v10, train, test, "V10", corners_df=corners, pedigrees_df=pedigrees)

# V10.1 新特徴量追加
print("\n3. V10.1 (新特徴量追加) 訓練...")
v10_1 = LeakFreeFeatureEngineerV10_1()
v10_1_results = train_and_evaluate(v10_1, train, test, "V10.1", corners_df=corners, horses_df=horses, pedigrees_df=pedigrees)

# 比較結果
print("\n" + "=" * 80)
print("結果比較（強化正則化版）")
print("=" * 80)

results_df = pd.DataFrame([v10_results, v10_1_results])
print(results_df.to_string(index=False))

# 改善度
roi_improvement = v10_1_results['test_roi'] - v10_results['test_roi']
print(f"\n【ROI改善】: V10 {v10_results['test_roi']:.1f}% → V10.1 {v10_1_results['test_roi']:.1f}% ({roi_improvement:+.1f}%)")

if roi_improvement > 0:
    print("✅ V10.1はROI改善に成功")
else:
    print("⚠️ V10.1はROI改善に失敗")

# 過学習チェック
if abs(v10_1_results['gap']) < 15:
    print("✅ 過学習は許容範囲内 (Gap < 15%)")
else:
    print("⚠️ 過学習の疑いあり (Gap >= 15%)")

print("\n" + "=" * 80)
print("完了")
print("=" * 80)
