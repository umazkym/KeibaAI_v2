#!/usr/bin/env python3
"""
データリーク調査: 特徴量にターゲット変数が含まれていないか確認
"""
import pandas as pd
from pathlib import Path
import pickle

print("=" * 60)
print("データリーク調査")
print("=" * 60)

# 1. 生成された特徴量を確認
features_path = Path('keibaai/data/features/parquet/year=2024/month=1')
features_df = pd.read_parquet(features_path)

print(f"\n【特徴量データ】")
print(f"行数: {len(features_df):,}")
print(f"列数: {len(features_df.columns)}")

# 2. ターゲット変数が含まれているか確認
target_columns = ['finish_position', 'finish_time_seconds', 'is_win', 
                  'win_flag', 'prize_money', 'odds', 'win_odds']

print(f"\n【ターゲット変数の存在確認】")
leaked_cols = []
for col in target_columns:
    if col in features_df.columns:
        leaked_cols.append(col)
        non_null = features_df[col].notna().sum()
        print(f"  ⚠️  {col}: 存在 ({non_null:,} non-null)")
    else:
        print(f"  ✓ {col}: なし")

# 3. モデルが期待する特徴量リストを確認
model_path = Path('keibaai/data/models/mu_model.pkl')
if model_path.exists():
    with open(model_path, 'rb') as f:
        model = pickle.load(f)
    
    print(f"\n【モデルの期待特徴量】")
    if hasattr(model, 'expected_features'):
        expected = model.expected_features
    elif hasattr(model, 'feature_names_'):
        expected = model.feature_names_
    elif hasattr(model, 'feature_name_'):
        expected = model.feature_name_
    else:
        expected = None
    
    if expected:
        print(f"  期待特徴量数: {len(expected)}")
        
        # ターゲット変数が含まれているか確認
        model_leaked = [col for col in expected if col in target_columns]
        if model_leaked:
            print(f"  ⚠️  モデルの期待特徴量にターゲット変数: {model_leaked}")
        else:
            print(f"  ✓ モデルの期待特徴量にターゲット変数なし")
        
        # 実際の特徴量との比較
        missing_in_data = set(expected) - set(features_df.columns)
        if missing_in_data:
            print(f"\n  データに欠損している特徴量: {len(missing_in_data)}個")
            for col in list(missing_in_data)[:10]:
                print(f"    - {col}")
    else:
        print(f"  モデルに期待特徴量リストが見つかりません")

# 4. 結論
print(f"\n{'=' * 60}")
if leaked_cols:
    print(f"⚠️  データリークの可能性あり!")
    print(f"   特徴量に含まれるターゲット変数: {leaked_cols}")
    print(f"\n   → これらを特徴量から除外する必要があります")
else:
    print(f"✓ 特徴量にターゲット変数は含まれていません")
    print(f"\n   → 異常値の原因は他にあります（評価ロジック、モデル等）")
print(f"{'=' * 60}")
