#!/usr/bin/env python3
"""
学習時に使用された特徴量を確認
"""
import pickle
import pandas as pd
from pathlib import Path

print("=" * 70)
print("学習データのデータリーク確認")
print("=" * 70)

# 1. 新しく学習されたモデルの期待特徴量
model_path = Path('keibaai/data/models/mu_model.pkl')
with open(model_path, 'rb') as f:
    model = pickle.load(f)

print("\n1. 新モデルの期待特徴量")
if hasattr(model, 'expected_features'):
    expected = model.expected_features
elif hasattr(model, 'feature_names_'):
    expected = model.feature_names_
else:
    expected = None

if expected:
    print(f"   期待特徴量数: {len(expected)}")
    
    # ターゲット変数チェック
    target_vars = ['finish_position', 'finish_time_seconds', 'prize_money', 'is_win', 'odds', 'popularity']
    leaked = [v for v in target_vars if v in expected]
    
    if leaked:
        print(f"   ✗ ターゲット変数が含まれる: {leaked}")
    else:
        print(f"   ✓ ターゲット変数なし")
        print(f"   特徴量例（最初の10個）:")
        for feat in expected[:10]:
            print(f"      - {feat}")
else:
    print("   期待特徴量リストが見つかりません")

# 2. 学習期間の特徴量を確認（2020-2023）
print(f"\n2. 学習期間の特徴量を確認")

# 2020年のサンプルを確認
sample_paths = [
    Path('keibaai/data/features/parquet/year=2020/month=1'),
    Path('keibaai/data/features/parquet/year=2023/month=12'),
]

for path in sample_paths:
    if path.exists():
        df = pd.read_parquet(path)
        print(f"\n   {path.name}:")
        print(f"      行数: {len(df):,}, 列数: {len(df.columns)}")
        
        # ターゲット変数チェック
        leaked = [v for v in target_vars if v in df.columns]
        if leaked:
            print(f"      ✗ ターゲット変数: {leaked}")
            for col in leaked:
                non_null = df[col].notna().sum()
                print(f"         {col}: {non_null:,} non-null")
        else:
            print(f"      ✓ ターゲット変数なし")
    else:
        print(f"\n   {path}: 存在しません")

# 3. 結論
print(f"\n{'=' * 70}")
print("診断:")
if expected and any(v in expected for v in target_vars):
    leaked_vars = [v for v in target_vars if v in expected]
    print(f"✗ 新モデルもターゲット変数で学習されています: {leaked_vars}")
    print(f"\n原因:")
    print(f"  → 学習期間（2020-2023）の特徴量にターゲット変数が含まれている")
    print(f"\n解決策:")
    print(f"  1. 学習期間の特徴量を再生成（ターゲット変数除外）")
    print(f"  2. 再度モデルを学習")
else:
    print(f"✓ モデルはクリーンです")
    print(f"   → 評価ロジックまたは評価データの問題の可能性")
print(f"{'=' * 70}")
