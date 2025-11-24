import sys
from pathlib import Path

# パス設定
project_root = Path('c:/Users/zk-ht/Keiba/Keiba_AI_v2').resolve()
sys.path.append(str(project_root))
sys.path.append(str(project_root / 'keibaai')) # srcが見えるようにする

import joblib
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from src.models.calibration import IsotonicCalibrator

def inspect_models():
    model_dir = Path('c:/Users/zk-ht/Keiba/Keiba_AI_v2/keibaai/data/models/mu_phase_f_ev')
    
    print("=== モデル解剖開始 ===")
    
    # 1. Rankerのロード
    try:
        ranker = joblib.load(model_dir / 'ranker.pkl')
        print(f"Ranker loaded: {type(ranker)}")
        # LGBMRankerの場合、predictはスコアを返す
        # ダミーデータで挙動確認したいが、特徴量名が必要
        feature_names_path = model_dir / 'feature_names.json'
        import json
        with open(feature_names_path, 'r', encoding='utf-8') as f:
            feature_names = json.load(f)
        print(f"Features: {len(feature_names)}")
        
    except Exception as e:
        print(f"Ranker load failed: {e}")
        return

    # 2. Calibratorのロード
    try:
        calibrator = IsotonicCalibrator.load(str(model_dir / 'calibrator.pkl'))
        print(f"Calibrator loaded: {type(calibrator)}")
        
        # IsotonicRegressionの内部状態を確認
        iso_reg = calibrator.calibrator
        print(f"IsotonicRegression params: {iso_reg.get_params()}")
        
        # 学習された変換関数を確認
        # 学習された変換関数を確認
        # X_min, X_max
        if hasattr(iso_reg, 'X_min_'):
            print(f"X_min: {iso_reg.X_min_}, X_max: {iso_reg.X_max_}")
        
        # y_min_, y_max_ はバージョンによってはない場合がある
        if hasattr(iso_reg, 'y_min_'):
            print(f"y_min: {iso_reg.y_min_}, y_max: {iso_reg.y_max_}")
            
        # Rankerの挙動確認（ランダムデータ）
        print("\n--- Ranker Output Check ---")
        try:
            # 特徴量数分のランダムデータを作成 (10サンプル)
            n_features = len(feature_names)
            dummy_X = np.random.rand(10, n_features)
            dummy_scores = ranker.predict(dummy_X)
            print(f"Dummy Input Scores: {dummy_scores}")
            print(f"Min: {dummy_scores.min()}, Max: {dummy_scores.max()}")
        except Exception as e:
            print(f"Ranker prediction failed: {e}")

        
        # 変換テーブルの一部を表示
        print("\n--- 変換関数 (f_) のサンプリング ---")
        # 入力（Sigmoid後の確率）を0.0から1.0まで振ってみる
        test_inputs = np.linspace(0, 1, 11)
        predicted_probs = iso_reg.predict(test_inputs)
        
        print("Input (Sigmoid(Score)) -> Output (Calibrated Probability)")
        for inp, out in zip(test_inputs, predicted_probs):
            print(f"{inp:.2f} -> {out:.4f}")
            
        # 増加関数か減少関数か判定
        is_increasing = np.all(np.diff(predicted_probs) >= 0)
        print(f"\n関数は単調増加ですか？: {is_increasing}")
        
        if is_increasing:
            print(">> 分析: 入力が大きいほど、出力（勝率）が高くなる設定です。")
        else:
            print(">> 分析: 入力が大きいほど、出力（勝率）が低くなる設定です。")
            
    except Exception as e:
        print(f"Calibrator load failed: {e}")

if __name__ == "__main__":
    inspect_models()
