#!/usr/bin/env python3
"""モデルファイルを直接確認"""
import pickle
from pathlib import Path

model_path = Path('keibaai/data/models/mu_model.pkl')
with open(model_path, 'rb') as f:
    model = pickle.load(f)

print("モデル属性:")
for attr in dir(model):
    if not attr.startswith('_'):
        print(f"  {attr}")

if hasattr(model, 'expected_features'):
    features = model.expected_features
    print(f"\nexpected_features: {len(features)}個")
    
    target_vars = ['finish_position', 'finish_time_seconds', 'prize_money', 'popularity', 'odds']
    leaked = [v for v in target_vars if v in features]
    
    if leaked:
        print(f"✗ ターゲット変数: {leaked}")
    else:
        print(f"✓ ターゲット変数なし")
        print(f"\n最初の20個の特徴量:")
        for i, feat in enumerate(features[:20], 1):
            print(f"  {i}. {feat}")
