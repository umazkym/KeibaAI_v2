#!/usr/bin/env python3
import sys
from pathlib import Path
import joblib
import numpy as np

# モデルディレクトリ
model_dir = Path('keibaai/data/models/mu_phase_f_corrected')

# 較正器をロード
calibrator_path = model_dir / 'calibrator.pkl'
calibrator = joblib.load(calibrator_path)

print("=" * 60)
print("Phase F-Corrected モデル: 較正器の詳細検証")
print("=" * 60)

# 較正器の内部状態を確認
iso_reg = calibrator.calibrator
print(f"\nIsotonicRegression params: {iso_reg.get_params()}")
print(f"X境界の範囲: {iso_reg.X_min_} ~ {iso_reg.X_max_}")
print(f"学習サンプル数: {len(iso_reg.X_thresholds_)}")

# 変換関数のサンプリング
print("\n較正曲線のサンプル:")
test_scores = np.linspace(iso_reg.X_min_, iso_reg.X_max_, 20)
for score in test_scores:
    prob =calibrator.predict_proba(np.array([score]))[0]
    print(f"  Score {score:.4f} → 確率 {prob:.4f}")

# Rankerスコア範囲での確認
print("\n一般的なRankerスコア範囲(1.0 - 1.5)での確率:")
common_scores = [1.0, 1.1, 1.2, 1.3, 1.4, 1.5]
for score in common_scores:
    prob = calibrator.predict_proba(np.array([score]))[0]
    print(f"  Score {score:.2f} → 確率 {prob:.4f}")

print("\n" + "=" * 60)
