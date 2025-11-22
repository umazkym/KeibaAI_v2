#!/usr/bin/env python3
"""最終確認スクリプト"""
import pickle
from pathlib import Path
from datetime import datetime

print("=" * 70)
print("クリーンモデル検証")
print("=" * 70)

model_path = Path('keibaai/data/models/mu_model.pkl')

if not model_path.exists():
    print("\n✗ mu_model.pklが見つかりません")
    print(f"   パス: {model_path}")
    exit(1)

# ファイル情報
mod_time = datetime.fromtimestamp(model_path.stat().st_mtime)
print(f"\n✓ モデルファイル発見")
print(f"   更新日時: {mod_time}")
print(f"   サイズ: {model_path.stat().st_size:,} bytes")

# 現在時刻から5分以内に更新されたか
from datetime import datetime, timedelta
now = datetime.now()
if (now - mod_time).total_seconds() < 300:
    print(f"   ✓ 最近更新されました（{int((now - mod_time).total_seconds())}秒前）")
else:
    print(f"   ✗ 古いファイルです（{int((now - mod_time).total_seconds())}秒前）")

# モデル内容確認
with open(model_path, 'rb') as f:
    model = pickle.load(f)

if hasattr(model, 'feature_names_'):
    features = model.feature_names_
    print(f"\n特徴量数: {len(features)}")
    
    target_vars = ['finish_position', 'finish_time_seconds', 'prize_money', 
                   'popularity', 'odds', 'win_odds', 'is_win']
    leaked = [v for v in target_vars if v in features]
    
    if leaked:
        print(f"\n✗ データリーク検出！")
        print(f"   ターゲット変数が含まれています: {leaked}")
        print(f"\n該当特徴量:")
        for var in leaked:
            idx = features.index(var)
            print(f"   [{idx+1}] {var}")
    else:
        print(f"\n✓ データリークなし！")
        print(f"   すべてのターゲット変数が除外されています")
        print(f"\n最初の10個の特徴量:")
        for i, feat in enumerate(features[:10], 1):
            print(f"   {i}. {feat}")

print(f"\n{'=' * 70}")
