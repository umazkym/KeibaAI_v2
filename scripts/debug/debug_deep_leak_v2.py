"""
V7 深層リーク分析 - 第2回批判的検証

より厳格な観点から、以下を詳細に検証:
1. cumsum+shift計算時のTest期間の結果の扱い
2. transform時の all_data に Test期間の is_win が含まれる問題
3. 特定レースの特徴量をトレースして、リークがないか確認
"""

import pandas as pd
import numpy as np
from pathlib import Path
import sys
import warnings
warnings.filterwarnings('ignore')

sys.path.insert(0, str(Path(__file__).parent.parent))

print("=" * 70)
print("V7 深層リーク分析 - 第2回批判的検証")
print("=" * 70)

# データ読み込み
DATA_DIR = Path("keibaai/data/parsed/parquet")

races = pd.read_parquet(DATA_DIR / "races" / "races.parquet")
races['race_date'] = pd.to_datetime(races['race_date'])
races = races.dropna(subset=['finish_position']).copy()

pedigrees = pd.read_parquet(DATA_DIR / "pedigrees" / "pedigrees.parquet")
corners = pd.read_parquet(DATA_DIR / "corners" / "corner_positions.parquet")
race_details = pd.read_parquet(DATA_DIR / "race_details" / "race_details.parquet")

TRAIN_END = pd.Timestamp('2024-06-30')
TEST_START = pd.Timestamp('2025-01-05')

train_data = races[races['race_date'] <= TRAIN_END].copy()
test_data = races[races['race_date'] >= TEST_START].copy()

print(f"\nTrain: {len(train_data):,}行")
print(f"Test: {len(test_data):,}行")

# ========================================
# 問題点1: transform時のall_dataの構成
# ========================================
print("\n" + "=" * 70)
print("問題点1: transform時のall_dataの構成")
print("=" * 70)

print("""
V7のtransform()では以下の処理を行う:
1. df (transform対象) の is_win/is_top3 を履歴 or finish_position から設定
2. all_data = concat([hist_no_dup, df]) を作成
3. all_data 上で cumsum().shift(1) を計算

【潜在的問題】
- df が Test期間のデータを含む場合、all_data にはTest期間の is_win が含まれる
- cumsum() は time-ordered なので問題ないように見えるが...
- shift(1) は「1行前」を参照するだけで、「1日前」ではない
""")

# 検証: 同日に複数レースがある場合の問題
print("\n【検証A】同日複数レースでの shift(1) の挙動")

# 特定の馬が同日に複数回出走することは稀（ほぼない）
# しかし、騎手は同日複数レースに騎乗する
sample_date = pd.Timestamp('2025-06-15')
sample_jockey_id = test_data[test_data['race_date'] == sample_date]['jockey_id'].value_counts().index[0]

jockey_races = test_data[
    (test_data['jockey_id'] == sample_jockey_id) & 
    (test_data['race_date'] == sample_date)
].sort_values('race_id')

print(f"\n騎手ID {sample_jockey_id} の {sample_date.date()} のレース:")
print(jockey_races[['race_id', 'horse_number', 'finish_position']].head(10))

# ========================================
# 問題点2: cumsum().shift(1) の正確性
# ========================================
print("\n" + "=" * 70)
print("問題点2: cumsum().shift(1) の正確性")
print("=" * 70)

from keibaai.src.features.leak_free_feature_engineer_v7 import LeakFreeFeatureEngineerV7

# V7をfit
fe = LeakFreeFeatureEngineerV7()
fe.fit(train_data, pedigrees, corners, race_details)

# 特定の馬をトレース
sample_horse_id = test_data.groupby('horse_id').size().sort_values(ascending=False).index[0]
horse_races = test_data[test_data['horse_id'] == sample_horse_id].sort_values('race_date')

print(f"\n馬ID {sample_horse_id} のTest期間レース:")
for _, row in horse_races.iterrows():
    print(f"  {row['race_date'].date()}: race_id={row['race_id']}, 着順={int(row['finish_position'])}")

# その馬の特徴量をtransformして確認
all_data = pd.concat([train_data, test_data], ignore_index=True)
all_data = all_data.sort_values(['race_date', 'race_id', 'horse_number'])

transformed = fe.transform(all_data)

# 特定馬のhorse_total_racesをトレース
horse_transformed = transformed[transformed['horse_id'] == sample_horse_id].sort_values('race_date')
print(f"\n馬ID {sample_horse_id} の特徴量推移:")
cols = ['race_date', 'finish_position', 'horse_total_races', 'horse_win_rate', 'horse_avg_finish']
print(horse_transformed[[c for c in cols if c in horse_transformed.columns]].to_string())

# 手動計算との比較
print("\n【手動計算との比較】")
horse_all = all_data[all_data['horse_id'] == sample_horse_id].sort_values('race_date')
horse_all['is_win'] = (horse_all['finish_position'] == 1).astype(int)

manual_cum_races = []
manual_cum_wins = []
cum_races = 0
cum_wins = 0
for _, row in horse_all.iterrows():
    manual_cum_races.append(cum_races)  # shift(1): 今のレース前の値
    manual_cum_wins.append(cum_wins)
    cum_races += 1
    cum_wins += row['is_win']

horse_all = horse_all.copy()
horse_all['manual_cum_races'] = manual_cum_races
horse_all['manual_cum_wins'] = manual_cum_wins

# V7の値と比較
compare = horse_transformed[['race_id', 'horse_total_races', 'horse_win_rate']].merge(
    horse_all[['race_id', 'manual_cum_races', 'manual_cum_wins']],
    on='race_id'
)
compare['match_races'] = compare['horse_total_races'] == compare['manual_cum_races']
print(compare.to_string())

if compare['match_races'].all():
    print("\n✅ 累積レース数は手動計算と一致")
else:
    print("\n❌ 累積レース数が手動計算と不一致！リークの可能性あり")

# ========================================
# 問題点3: transform呼び出し時のデータ順序
# ========================================
print("\n" + "=" * 70)
print("問題点3: transform呼び出し時のデータ順序の影響")
print("=" * 70)

print("""
【批判的観点】
V7は transform() 内で all_data をソートするが、
呼び出し側がどのようなデータを渡すかによって結果が変わる可能性がある。

例: 
- fit(Train) + transform(Train+Test) → OK?
- fit(Train) + transform(Test) → OK?
- fit(Train+Test) + transform(Test) → Leak?

以下で検証:
""")

# パターン1: fit(Train) + transform(Train+Test)
fe1 = LeakFreeFeatureEngineerV7()
fe1.fit(train_data, pedigrees, corners, race_details)
all_data1 = pd.concat([train_data, test_data], ignore_index=True)
transformed1 = fe1.transform(all_data1)
test_t1 = transformed1[transformed1['race_id'].isin(test_data['race_id'])]

# パターン2: fit(Train) + transform(Test only)
fe2 = LeakFreeFeatureEngineerV7()
fe2.fit(train_data, pedigrees, corners, race_details)
transformed2 = fe2.transform(test_data)

# 比較
merged = test_t1[['race_id', 'horse_number', 'horse_win_rate']].merge(
    transformed2[['race_id', 'horse_number', 'horse_win_rate']],
    on=['race_id', 'horse_number'],
    suffixes=('_pattern1', '_pattern2')
)

# NaNを除外して比較
merged_clean = merged.dropna()
if len(merged_clean) > 0:
    diff = (merged_clean['horse_win_rate_pattern1'] - merged_clean['horse_win_rate_pattern2']).abs()
    if diff.max() < 0.001:
        print(f"✅ パターン1とパターン2の結果は同一 (max diff: {diff.max():.6f})")
    else:
        print(f"❌ パターン1とパターン2で差分あり (max diff: {diff.max():.6f})")
        print("   これは順序依存によるリークの可能性を示唆")

# ========================================
# 問題点4: NaN埋めの影響
# ========================================
print("\n" + "=" * 70)
print("問題点4: NaN埋めの影響")
print("=" * 70)

# horse_win_rateのNaN率
test_features = transformed1[transformed1['race_id'].isin(test_data['race_id'])]
nan_rate = test_features['horse_win_rate'].isna().mean() * 100
print(f"Test期間のhorse_win_rateのNaN率: {nan_rate:.1f}%")

print("""
【批判的観点】
- NaN率が高い（27.6%）ということは、多くの馬で過去データが不足
- これらの馬は min_races_for_stats (=3) 未満のレース数
- NaN埋め方法がモデルの性能に大きく影響する可能性
- 「過去データなし」の馬が勝ちやすい傾向があればバイアスになる
""")

# NaN vs non-NaN で勝率を比較
test_with_fp = test_features.dropna(subset=['finish_position'])
nan_horses = test_with_fp[test_with_fp['horse_win_rate'].isna()]
known_horses = test_with_fp[test_with_fp['horse_win_rate'].notna()]

nan_win_rate = (nan_horses['finish_position'] == 1).mean() * 100
known_win_rate = (known_horses['finish_position'] == 1).mean() * 100

print(f"\nNaN馬の勝率: {nan_win_rate:.2f}% ({len(nan_horses)}頭)")
print(f"既知馬の勝率: {known_win_rate:.2f}% ({len(known_horses)}頭)")

if abs(nan_win_rate - known_win_rate) > 2:
    print(f"⚠️ NaN馬と既知馬で勝率に{abs(nan_win_rate - known_win_rate):.1f}%の差がある")

# ========================================
# 結論
# ========================================
print("\n" + "=" * 70)
print("深層分析の結論")
print("=" * 70)
