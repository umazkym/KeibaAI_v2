"""
V7.1 vs V7.2 簡略化比較テスト

リーク修正前後のROI差を検証（シンプル版）
"""

import pandas as pd
import numpy as np
from pathlib import Path
import sys
import warnings
warnings.filterwarnings('ignore')

# プロジェクトルートをパスに追加
sys.path.insert(0, str(Path(__file__).parent.parent))

print("=" * 60)
print("V7.1 vs V7.2 簡略化比較テスト")
print("=" * 60)

# データ読み込み
DATA_DIR = Path("keibaai/data/parsed/parquet")

print("\n[1/4] データ読み込み中...")
races = pd.read_parquet(DATA_DIR / "races" / "races.parquet")
races['race_date'] = pd.to_datetime(races['race_date'])
races = races.dropna(subset=['finish_position']).copy()
print(f"  races: {len(races):,}行（NaN除外後）")

pedigrees = pd.read_parquet(DATA_DIR / "pedigrees" / "pedigrees.parquet")
corners = pd.read_parquet(DATA_DIR / "corners" / "corner_positions.parquet")
race_details = pd.read_parquet(DATA_DIR / "race_details" / "race_details.parquet")

# データ分割
TRAIN_END = pd.Timestamp('2024-06-30')
TEST_START = pd.Timestamp('2025-01-05')

train_data = races[races['race_date'] <= TRAIN_END].copy()
test_data = races[races['race_date'] >= TEST_START].copy()

print(f"\n  Train: {len(train_data):,}行")
print(f"  Test: {len(test_data):,}行")

# インポート
print("\n[2/4] 特徴量エンジニアをインポート中...")
from keibaai.src.features.leak_free_feature_engineer_v7 import LeakFreeFeatureEngineerV7
from keibaai.src.features.leak_free_feature_engineer_v72 import LeakFreeFeatureEngineerV72

def evaluate_roi(df, name):
    """単純なTop1 ROI評価"""
    # レースごとにhorse_win_rateが最大の馬を選択
    if 'horse_win_rate' not in df.columns:
        print(f"  [{name}] horse_win_rateがありません")
        return None
    
    result = df.dropna(subset=['horse_win_rate', 'finish_position', 'win_odds']).copy()
    result['rank'] = result.groupby('race_id')['horse_win_rate'].rank(ascending=False, method='first')
    top1 = result[result['rank'] == 1].copy()
    
    hits = top1[top1['finish_position'] == 1]
    hit_rate = len(hits) / len(top1) * 100 if len(top1) > 0 else 0
    roi = hits['win_odds'].sum() / len(top1) * 100 if len(top1) > 0 else 0
    
    return {'hit_rate': hit_rate, 'roi': roi, 'n_races': len(top1)}

# ========================================
# テスト1: V7.1 + fit(Train only) - 正しい使い方
# ========================================
print("\n[3/4] V7.1 (Train only fit) テスト中...")
fe_v71 = LeakFreeFeatureEngineerV7()
fe_v71.fit(train_data, pedigrees, corners, race_details)

# transformはfitデータを含めた全データで実行（cumulativeが正しく計算されるため）
all_for_transform = pd.concat([train_data, test_data], ignore_index=True)
all_for_transform = all_for_transform.sort_values(['race_date', 'race_id', 'horse_number'])

transformed_v71 = fe_v71.transform(all_for_transform)

# Testデータのみ抽出
test_race_ids = set(test_data['race_id'].unique())
test_v71 = transformed_v71[transformed_v71['race_id'].isin(test_race_ids)].copy()

result_v71 = evaluate_roi(test_v71, "V7.1")
if result_v71:
    print(f"  的中率: {result_v71['hit_rate']:.2f}%")
    print(f"  ROI: {result_v71['roi']:.2f}%")
    print(f"  レース数: {result_v71['n_races']}")

# ========================================
# テスト2: V7.2 + fit(Train+Test) - リーク修正済み
# ========================================
print("\n[4/4] V7.2 (Train+Test fit, 修正済み) テスト中...")
fit_all = pd.concat([train_data, test_data], ignore_index=True)
fe_v72 = LeakFreeFeatureEngineerV72()
fe_v72.fit(fit_all, pedigrees, corners, race_details)

transformed_v72 = fe_v72.transform(all_for_transform)

# Testデータのみ抽出
test_v72 = transformed_v72[transformed_v72['race_id'].isin(test_race_ids)].copy()

result_v72 = evaluate_roi(test_v72, "V7.2")
if result_v72:
    print(f"  的中率: {result_v72['hit_rate']:.2f}%")
    print(f"  ROI: {result_v72['roi']:.2f}%")
    print(f"  レース数: {result_v72['n_races']}")

# ========================================
# 結果比較
# ========================================
print("\n" + "=" * 60)
print("結果サマリー")
print("=" * 60)

if result_v71 and result_v72:
    print(f"""
| バージョン | fit データ | 的中率 | ROI |
|-----------|-----------|--------|-----|
| V7.1      | Train only    | {result_v71['hit_rate']:.2f}% | {result_v71['roi']:.2f}% |
| V7.2      | Train+Test    | {result_v72['hit_rate']:.2f}% | {result_v72['roi']:.2f}% |

【分析】
- V7.1(fit Train only): bracket_bias/running_styleがTrain期間のみで計算される
- V7.2(fit Train+Test): bracket_bias/running_styleがfit時に固定（リーク修正）

- 両者のROIが近ければ、V7.2の修正は有効
- V7.2のROIがV7.1より著しく低い場合、リークが性能に寄与していた可能性
""")

# bracket_biasの特徴量値を比較
print("\n【特徴量値の比較（bracket_bias）】")
if 'bracket_bias' in test_v71.columns and 'bracket_bias' in test_v72.columns:
    v71_bias = test_v71['bracket_bias'].dropna()
    v72_bias = test_v72['bracket_bias'].dropna()
    print(f"  V7.1 bracket_bias: mean={v71_bias.mean():.4f}, std={v71_bias.std():.4f}")
    print(f"  V7.2 bracket_bias: mean={v72_bias.mean():.4f}, std={v72_bias.std():.4f}")
    
    # 差分
    merged = test_v71[['race_id', 'horse_number', 'bracket_bias']].merge(
        test_v72[['race_id', 'horse_number', 'bracket_bias']],
        on=['race_id', 'horse_number'],
        suffixes=('_v71', '_v72')
    )
    merged['diff'] = merged['bracket_bias_v71'] - merged['bracket_bias_v72']
    print(f"  差分: mean={merged['diff'].mean():.4f}, std={merged['diff'].std():.4f}")
