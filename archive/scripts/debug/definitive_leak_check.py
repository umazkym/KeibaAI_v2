#!/usr/bin/env python3
"""
確実な診断: モデルの期待特徴量と実際の評価データを直接確認
"""
import pickle
import pandas as pd
from pathlib import Path

print("=" * 70)
print("データリーク確実診断")
print("=" * 70)

# 1. モデルの期待特徴量を確認
model_path = Path('keibaai/data/models/mu_model.pkl')
with open(model_path, 'rb') as f:
    model = pickle.load(f)

print("\n1. モデルの期待特徴量")
if hasattr(model, 'expected_features'):
    expected = model.expected_features
elif hasattr(model, 'feature_names_'):
    expected = model.feature_names_
else:
    expected = None

if expected:
    print(f"   期待特徴量数: {len(expected)}")
    
    # ターゲット変数が含まれているか確認
    target_vars = ['finish_position', 'finish_time_seconds', 'prize_money', 'is_win', 'odds', 'popularity']
    model_leaked = [v for v in target_vars if v in expected]
    
    if model_leaked:
        print(f"\n   !! モデルの期待特徴量にターゲット変数: {model_leaked}")
        print(f"   → これがデータリークの原因です！")
    else:
        print(f"   ✓ モデルの期待特徴量にターゲット変数なし")
else:
    print("   モデルに期待特徴量リストが見つかりません")

# 2. 生成された特徴量を確認
features_path = Path('keibaai/data/features/parquet/year=2024/month=1')
features_df = pd.read_parquet(features_path)

print(f"\n2. 生成された特徴量（最新）")
print(f"   行数: {len(features_df):,}")
print(f"   列数: {len(features_df.columns)}")

# ターゲット変数が含まれているか
generated_leaked = [v for v in target_vars if v in features_df.columns]
if generated_leaked:
    print(f"\n   !! 生成された特徴量にターゲット変数: {generated_leaked}")
    for col in generated_leaked:
        non_null = features_df[col].notna().sum()
        print(f"      {col}: {non_null:,} non-null")
else:
    print(f"   ✓ 生成された特徴量にターゲット変数なし")

# 3. モデルと実データの特徴量の差分
if expected:
    print(f"\n3. 特徴量の差分")
    
    # モデルが期待しているがデータにない特徴量
    missing_in_data = set(expected) - set(features_df.columns)
    if missing_in_data:
        print(f"   データに欠損: {len(missing_in_data)}個")
        for col in list(missing_in_data)[:10]:
            print(f"      - {col}")
    
    # データにあるがモデルが期待していない特徴量
    extra_in_data = set(features_df.columns) - set(expected) - {'race_id', 'horse_id', 'year', 'month'}
    if extra_in_data:
        print(f"   データに余分: {len(extra_in_data)}個")
        for col in list(extra_in_data)[:10]:
            print(f"      + {col}")

# 4. 結論
print(f"\n{'=' * 70}")
print("診断結果:")
if model_leaked:
    print(f"✗ モデル自体がターゲット変数を期待しています: {model_leaked}")
    print(f"   → モデルを再学習する必要があります")
elif generated_leaked:
    print(f"✗ 特徴量生成でターゲット変数が除外されていません: {generated_leaked}")
    print(f"   → feature_engine.pyの修正が適用されていない可能性")
else:
    print(f"✓ データリークは見つかりませんでした")
    print(f"   → 評価ロジック自体の問題の可能性")
print(f"{'=' * 70}")
