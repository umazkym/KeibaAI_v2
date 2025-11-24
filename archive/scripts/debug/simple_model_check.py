#!/usr/bin/env python3
"""簡易検証スクリプト - import srcなしで実行"""
import sys
import pickle
from pathlib import Path

model_path = Path('keibaai/data/models/mu_model.pkl')

if not model_path.exists():
    print("✗ mu_model.pklが見つかりません")
    sys.exit(1)

print("=" * 70)
print("クリーンモデル検証")
print("=" * 70)

# sys.pathにプロジェクトルートを追加
sys.path.insert(0, str(Path.cwd()))

with open(model_path, 'rb') as f:
    model = pickle.load(f)

print(f"\n✓ モデルロード成功")
print(f"   型: {type(model).__name__}")

# 属性確認
if hasattr(model, 'feature_names'):
    features = model.feature_names
    print(f"\n特徴量数: {len(features)}")
    
    target_vars = ['finish_position', 'finish_time_seconds', 'prize_money', 
                   'popularity', 'odds', 'win_odds', 'is_win']
    leaked = [v for v in target_vars if v in features]
    
    if leaked:
        print(f"\n✗ データリーク検出！")
        print(f"   ターゲット変数: {leaked}")
    else:
        print(f"\n✓ データリークなし！")
        print(f"   すべてのターゲット変数が除外されています")
        
elif hasattr(model, 'feature_names_'):
    print(f"\n属性: feature_names_を使用")
    features = model.feature_names_
    print(f"特徴量数: {len(features)}")
else:
    print("\n✗ feature_names属性が見つかりません")

print(f"\n{'=' * 70}")
