#!/usr/bin/env python3
"""モデルのfeature_names_を確認"""
import pickle
from pathlib import Path

model_path = Path('keibaai/data/models/mu_model.pkl')
with open(model_path, 'rb') as f:
    model = pickle.load(f)

if hasattr(model, 'feature_names_'):
    features = model.feature_names_
    print(f"feature_names_: {len(features)}個")
    
    target_vars = ['finish_position', 'finish_time_seconds', 'prize_money', 'popularity', 'odds', 'win_odds']
    leaked = [v for v in target_vars if v in features]
    
    if leaked:
        print(f"\n✗ ターゲット変数が含まれています: {leaked}")
        for var in leaked:
            idx = features.index(var)
            print(f"   {idx+1}. {var}")
    else:
        print(f"\n✓ ターゲット変数なし！")
        print(f"\nサンプル特徴量（最初の15個）:")
        for i, feat in enumerate(features[:15], 1):
            print(f"  {i}. {feat}")
else:
    print("feature_names_属性が見つかりません")
