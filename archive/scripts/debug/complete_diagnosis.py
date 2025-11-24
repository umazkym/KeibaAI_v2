#!/usr/bin/env python3
"""完全な状況確認と次のアクション提示"""
import pickle
from pathlib import Path
import yaml
from datetime import datetime

print("=" * 70)
print("最終診断")
print("=" * 70)

# 1. feature_names.yamlの内容
yaml_path = Path('keibaai/data/features/parquet/feature_names.yaml')
print(f"\n1. feature_names.yaml")
print(f"   パス: {yaml_path}")
print(f"   存在: {yaml_path.exists()}")
if yaml_path.exists():
    print(f"   更新日時: {datetime.fromtimestamp(yaml_path.stat().st_mtime)}")
    with open(yaml_path, 'r', encoding='utf-8') as f:
        features_from_yaml = yaml.safe_load(f)
    print(f"   特徴量数: {len(features_from_yaml)}")
    
    target_vars = ['finish_position', 'finish_time_seconds', 'prize_money', 'popularity', 'odds', 'win_odds']
    leaked_yaml = [v for v in target_vars if v in features_from_yaml]
    if leaked_yaml:
        print(f"   ✗ YAMLにターゲット変数: {leaked_yaml}")
    else:
        print(f"   ✓ YAMLにターゲット変数なし")

# 2. モデルファイル
model_path = Path('keibaai/data/models/mu_model.pkl')
print(f"\n2. mu_model.pkl")
print(f"   パス: {model_path}")
print(f"   存在: {model_path.exists()}")
if model_path.exists():
    print(f"   更新日時: {datetime.fromtimestamp(model_path.stat().st_mtime)}")
    with open(model_path, 'rb') as f:
        model = pickle.load(f)
    if hasattr(model, 'feature_names_'):
        features_from_model = model.feature_names_
        print(f"   モデルの特徴量数: {len(features_from_model)}")
        
        leaked_model = [v for v in target_vars if v in features_from_model]
        if leaked_model:
            print(f"   ✗ モデルにターゲット変数: {leaked_model}")
        else:
            print(f"   ✓ モデルにターゲット変数なし")

# 3. 結論
print(f"\n{'=' * 70}")
print("診断結果:")

if yaml_path.exists() and model_path.exists():
    if len(features_from_yaml) == len(features_from_model):
        print(f"✓ YAML ({len(features_from_yaml)}) とモデル ({len(features_from_model)}) の特徴量数が一致")
    else:
        print(f"✗ YAML ({len(features_from_yaml)}) とモデル ({len(features_from_model)}) の特徴量数が不一致")
        print(f"   → モデルが古いYAMLで学習された可能性")

    if not leaked_yaml and leaked_model:
        print(f"\n✗ YAMLはクリーンだが、モデルが汚染されている")
        print(f"   → モデルが学習時に古いYAMLを読み込んだ")
        print(f"\n推奨アクション:")
        print(f"   1. feature_names.yamlのバックアップを確認")
        print(f"   2. train_mu_model.pyが最新のYAMLを読み込むことを確認")
        print(f"   3. モデルを再学習")
    elif leaked_yaml and leaked_model:
        print(f"\n✗ YAMLとモデルの両方が汚染されている")
        print(f"   → 特徴量生成が正しく動作していない")
    elif not leaked_yaml and not leaked_model:
        print(f"\n✓ YAMLとモデルの両方がクリーン!")
print(f"{'=' * 70}")
