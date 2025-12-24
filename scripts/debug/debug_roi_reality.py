"""
V7 批判的分析 - ROIの現実性検証

ROI 150%が「現実的」かを厳しく検証:
1. 生存者バイアス: 10ヶ月だけのテストでは統計的に不十分
2. 特定期間への過適合: 2025年だけでなく他の期間での検証
3. 高オッズ帯への依存: 高オッズ帯のROIが不自然に高い問題
4. シャッフルテスト: ターゲット変数をシャッフルしてもROIが出るか
"""

import pandas as pd
import numpy as np
from pathlib import Path
import lightgbm as lgb
import sys
import warnings
warnings.filterwarnings('ignore')

sys.path.insert(0, str(Path(__file__).parent.parent))

print("=" * 70)
print("V7 批判的分析 - ROIの現実性検証")
print("=" * 70)

# データ読み込み
DATA_DIR = Path("keibaai/data/parsed/parquet")

races = pd.read_parquet(DATA_DIR / "races" / "races.parquet")
races['race_date'] = pd.to_datetime(races['race_date'])
races = races.dropna(subset=['finish_position']).copy()

pedigrees = pd.read_parquet(DATA_DIR / "pedigrees" / "pedigrees.parquet")
corners = pd.read_parquet(DATA_DIR / "corners" / "corner_positions.parquet")
race_details = pd.read_parquet(DATA_DIR / "race_details" / "race_details.parquet")

from keibaai.src.features.leak_free_feature_engineer_v7 import LeakFreeFeatureEngineerV7

# ========================================
# 批判的検証1: Walk-Forward検証
# ========================================
print("\n" + "=" * 70)
print("批判的検証1: Walk-Forward検証（複数期間での評価）")
print("=" * 70)

print("""
【批判的観点】
報告書では2025年1月-10月の10ヶ月間のみでテスト。
これは「たまたま良い期間だった」可能性がある。
複数の期間で評価し、安定性を確認する。
""")

# Walk-Forward検証: 6ヶ月ずつのテスト期間
walk_forward_results = []

periods = [
    # (train_end, test_start, test_end)
    ('2022-06-30', '2022-07-01', '2022-12-31'),
    ('2023-06-30', '2023-07-01', '2023-12-31'),
    ('2024-06-30', '2024-07-01', '2024-12-31'),
    ('2024-06-30', '2025-01-01', '2025-10-26'),  # 報告書と同じ
]

for train_end, test_start, test_end in periods:
    train_end_ts = pd.Timestamp(train_end)
    test_start_ts = pd.Timestamp(test_start)
    test_end_ts = pd.Timestamp(test_end)
    
    # データ分割
    train = races[races['race_date'] <= train_end_ts].copy()
    test = races[(races['race_date'] >= test_start_ts) & (races['race_date'] <= test_end_ts)].copy()
    
    if len(train) < 50000 or len(test) < 5000:
        print(f"  {test_start}～{test_end}: データ不足でスキップ")
        continue
    
    # 特徴量生成
    fe = LeakFreeFeatureEngineerV7()
    fe.fit(train, pedigrees, corners, race_details)
    
    all_data = pd.concat([train, test], ignore_index=True)
    all_data = all_data.sort_values(['race_date', 'race_id', 'horse_number'])
    transformed = fe.transform(all_data)
    
    test_race_ids = set(test['race_id'].unique())
    test_transformed = transformed[transformed['race_id'].isin(test_race_ids)].copy()
    
    # モデル学習
    feature_cols = fe.get_feature_columns()
    available_cols = [c for c in feature_cols if c in test_transformed.columns]
    
    train_transformed = transformed[~transformed['race_id'].isin(test_race_ids)]
    train_clean = train_transformed.dropna(subset=['finish_position'] + available_cols).copy()
    test_clean = test_transformed.dropna(subset=['finish_position'] + available_cols).copy()
    
    if len(train_clean) < 10000 or len(test_clean) < 1000:
        print(f"  {test_start}～{test_end}: クリーンデータ不足でスキップ")
        continue
    
    X_train = train_clean[available_cols]
    y_train = (train_clean['finish_position'] == 1).astype(int)
    X_test = test_clean[available_cols]
    
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
    model.fit(X_train, y_train, eval_set=[(X_test, (test_clean['finish_position'] == 1).astype(int))], callbacks=[lgb.early_stopping(50, verbose=False)])
    
    test_clean['pred_proba'] = model.predict_proba(X_test)[:, 1]
    test_clean['rank'] = test_clean.groupby('race_id')['pred_proba'].rank(ascending=False, method='first')
    top1 = test_clean[test_clean['rank'] == 1].copy()
    
    hits = top1[top1['finish_position'] == 1]
    hit_rate = len(hits) / len(top1) * 100
    roi = hits['win_odds'].sum() / len(top1) * 100
    
    print(f"  {test_start}～{test_end}: ROI={roi:.1f}%, 的中率={hit_rate:.1f}%, N={len(top1)}")
    walk_forward_results.append({
        'period': f"{test_start}～{test_end}",
        'roi': roi,
        'hit_rate': hit_rate,
        'n_races': len(top1)
    })

# 結果まとめ
print("\n【Walk-Forward結果サマリー】")
for r in walk_forward_results:
    status = "✅" if r['roi'] > 100 else "❌"
    print(f"  {r['period']}: ROI {r['roi']:.1f}% {status}")

# ========================================
# 批判的検証2: シャッフルテスト
# ========================================
print("\n" + "=" * 70)
print("批判的検証2: シャッフルテスト（ターゲット変数をシャッフル）")
print("=" * 70)

print("""
【批判的観点】
もしモデルが本当に予測力を持つなら、ターゲット変数（勝利）を
シャッフルした場合、ROIは約80%（JRA控除率分）に収束するはず。
シャッフル後もROIが高ければ、リークの可能性がある。
""")

# 2025年データで最後のシャッフルテスト
TRAIN_END = pd.Timestamp('2024-06-30')
TEST_START = pd.Timestamp('2025-01-05')

train_data = races[races['race_date'] <= TRAIN_END].copy()
test_data = races[races['race_date'] >= TEST_START].copy()

# V7特徴量生成
fe = LeakFreeFeatureEngineerV7()
fe.fit(train_data, pedigrees, corners, race_details)

all_data = pd.concat([train_data, test_data], ignore_index=True)
transformed = fe.transform(all_data)

test_race_ids = set(test_data['race_id'].unique())
test_t = transformed[transformed['race_id'].isin(test_race_ids)].copy()

feature_cols = fe.get_feature_columns()
available_cols = [c for c in feature_cols if c in test_t.columns]

train_t = transformed[~transformed['race_id'].isin(test_race_ids)]
train_clean = train_t.dropna(subset=['finish_position'] + available_cols).copy()
test_clean = test_t.dropna(subset=['finish_position'] + available_cols).copy()

# シャッフル版: レースごとにfinish_positionをシャッフル
rng = np.random.default_rng(42)
train_shuffled = train_clean.copy()

def shuffle_within_race(group):
    shuffled = group.copy()
    shuffled['finish_position'] = rng.permutation(group['finish_position'].values)
    return shuffled

train_shuffled = train_shuffled.groupby('race_id', group_keys=False).apply(shuffle_within_race)

X_train_shuffled = train_shuffled[available_cols]
y_train_shuffled = (train_shuffled['finish_position'] == 1).astype(int)

model_shuffled = lgb.LGBMClassifier(
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
model_shuffled.fit(X_train_shuffled, y_train_shuffled)

X_test = test_clean[available_cols]
test_clean['pred_proba_shuffled'] = model_shuffled.predict_proba(X_test)[:, 1]
test_clean['rank_shuffled'] = test_clean.groupby('race_id')['pred_proba_shuffled'].rank(ascending=False, method='first')
top1_shuffled = test_clean[test_clean['rank_shuffled'] == 1].copy()

hits_shuffled = top1_shuffled[top1_shuffled['finish_position'] == 1]
roi_shuffled = hits_shuffled['win_odds'].sum() / len(top1_shuffled) * 100
hit_rate_shuffled = len(hits_shuffled) / len(top1_shuffled) * 100

print(f"\n【シャッフルテスト結果】")
print(f"  通常モデル: ROI={152.89:.1f}%, 的中率={28.86:.1f}%")
print(f"  シャッフル: ROI={roi_shuffled:.1f}%, 的中率={hit_rate_shuffled:.1f}%")

if roi_shuffled < 90:
    print("  ✅ シャッフル後はROIが低下 → モデルに予測力あり")
else:
    print("  ❌ シャッフル後もROIが高い → リークの可能性あり")

# ========================================
# 結論
# ========================================
print("\n" + "=" * 70)
print("批判的検証の結論")
print("=" * 70)
