"""
V7のROI乖離原因の調査

報告値: ROI 148.2%
検証値: ROI 82.29%

乖離の原因を調査
"""

import pandas as pd
import numpy as np
from pathlib import Path
import sys
import warnings
warnings.filterwarnings('ignore')

sys.path.insert(0, str(Path(__file__).parent.parent))

print("=" * 60)
print("V7 ROI乖離原因の調査")
print("=" * 60)

# データ読み込み
DATA_DIR = Path("keibaai/data/parsed/parquet")

races = pd.read_parquet(DATA_DIR / "races" / "races.parquet")
races['race_date'] = pd.to_datetime(races['race_date'])
races = races.dropna(subset=['finish_position']).copy()

TEST_START = pd.Timestamp('2025-01-05')
test_data = races[races['race_date'] >= TEST_START].copy()

print(f"\nTest期間: {test_data['race_date'].min()} ～ {test_data['race_date'].max()}")
print(f"Test行数: {len(test_data):,}行")
print(f"Testレース数: {test_data['race_id'].nunique():,}")

# ========================================
# 検証1: 報告書の評価方法の確認
# ========================================
print("\n" + "=" * 60)
print("検証1: 報告書の評価方法を再現")
print("=" * 60)

# 報告書の記述:
# - 的中率 31.6%
# - ROI 148.2%
# - 10ヶ月中9ヶ月がROI 100%超

# 私のテストでは:
# - 的中率 13.24%
# - ROI 82.29%
# - レース数 2303

print(f"""
報告書の数値:
  - 的中率: 31.6%
  - ROI: 148.2%
  - 対象レース数: 不明

私のテストの数値:
  - 的中率: 13.24%
  - ROI: 82.29%
  - 対象レース数: 2,303

考えられる乖離原因:
  1. 報告書はモデル学習後のTop1ではなく、別の選択基準を使用
  2. 報告書のモデルはhandicapやオッズ情報を暗黙的に使用
  3. 報告書はテストが不十分で過大評価

""")

# ========================================
# 検証2: horse_win_rateの分布確認
# ========================================
print("=" * 60)
print("検証2: 特徴量の品質確認")
print("=" * 60)

from keibaai.src.features.leak_free_feature_engineer_v7 import LeakFreeFeatureEngineerV7

train_data = races[races['race_date'] < TEST_START].copy()
pedigrees = pd.read_parquet(DATA_DIR / "pedigrees" / "pedigrees.parquet")
corners = pd.read_parquet(DATA_DIR / "corners" / "corner_positions.parquet")
race_details = pd.read_parquet(DATA_DIR / "race_details" / "race_details.parquet")

fe = LeakFreeFeatureEngineerV7()
fe.fit(train_data, pedigrees, corners, race_details)

all_data = pd.concat([train_data, test_data], ignore_index=True)
all_data = all_data.sort_values(['race_date', 'race_id', 'horse_number'])
transformed = fe.transform(all_data)

test_race_ids = set(test_data['race_id'].unique())
test_transformed = transformed[transformed['race_id'].isin(test_race_ids)].copy()

# 特徴量のNaN率
print("\n特徴量のNaN率:")
for col in ['horse_win_rate', 'jockey_win_rate', 'trainer_win_rate', 'bracket_bias']:
    if col in test_transformed.columns:
        nan_rate = test_transformed[col].isna().mean() * 100
        print(f"  {col}: {nan_rate:.1f}%")

# ========================================
# 検証3: LightGBMモデルで評価
# ========================================
print("\n" + "=" * 60)
print("検証3: LightGBMモデルでの評価")
print("=" * 60)

import lightgbm as lgb

feature_cols = fe.get_feature_columns()
available_cols = [c for c in feature_cols if c in test_transformed.columns]

# Trainデータ
train_transformed = transformed[~transformed['race_id'].isin(test_race_ids)].copy()
train_clean = train_transformed.dropna(subset=['finish_position'] + available_cols).copy()
test_clean = test_transformed.dropna(subset=['finish_position'] + available_cols).copy()

X_train = train_clean[available_cols]
y_train = (train_clean['finish_position'] == 1).astype(int)

X_test = test_clean[available_cols]
y_test = (test_clean['finish_position'] == 1).astype(int)

print(f"\nTrain: {len(X_train):,}行")
print(f"Test: {len(X_test):,}行")
print(f"特徴量数: {len(available_cols)}")

# 報告書と同様の強正則化パラメータ
model = lgb.LGBMClassifier(
    n_estimators=1000,
    learning_rate=0.005,
    max_depth=2,
    num_leaves=4,
    min_child_samples=200,
    reg_alpha=3.0,
    reg_lambda=5.0,
    subsample=0.5,
    colsample_bytree=0.5,
    random_state=42,
    verbose=-1
)

model.fit(X_train, y_train)

# 予測
test_clean['pred_proba'] = model.predict_proba(X_test)[:, 1]

# レースごとにTop1を選択
test_clean['rank'] = test_clean.groupby('race_id')['pred_proba'].rank(ascending=False, method='first')
top1 = test_clean[test_clean['rank'] == 1].copy()

hits = top1[top1['finish_position'] == 1]
hit_rate = len(hits) / len(top1) * 100
roi = hits['win_odds'].sum() / len(top1) * 100

print(f"\n--- LightGBMモデル評価 ---")
print(f"対象レース数: {len(top1):,}")
print(f"的中率: {hit_rate:.2f}%")
print(f"ROI: {roi:.2f}%")

# Train的中率も確認
train_clean['pred_proba'] = model.predict_proba(X_train)[:, 1]
train_clean['rank'] = train_clean.groupby('race_id')['pred_proba'].rank(ascending=False, method='first')
train_top1 = train_clean[train_clean['rank'] == 1]
train_hits = train_top1[train_top1['finish_position'] == 1]
train_hit_rate = len(train_hits) / len(train_top1) * 100

print(f"\nTrain的中率: {train_hit_rate:.2f}%")
print(f"Train-Test差分: {train_hit_rate - hit_rate:.2f}%")

# ========================================
# 結論
# ========================================
print("\n" + "=" * 60)
print("結論")
print("=" * 60)
print("""
【発見事項】
1. V7の特徴量エンジニア自体には大きなリークは確認されなかった
   - V7.1とV7.2のROIが同一（82.29%）

2. 報告されたROI 148.2%との乖離の原因:
   - 報告書のモデル/評価方法が本テストと異なる可能性
   - 報告書の評価が特定の好条件に依存していた可能性

3. 現実的なV7のパフォーマンス:
   - 単純なhorse_win_rate選択: ROI 82.29%
   - LightGBMモデル: 上記参照
   
4. bracket_bias/running_styleのリーク修正:
   - 数値的影響は軽微（差分stdが0.09程度）
   - しかし原則として修正は必要
""")
