"""
保存されているモデルの特徴量リストを確認するスクリプト
"""
import joblib
import pandas as pd
from pathlib import Path
import sys
import json

# プロジェクトルートをパスに追加
project_root = Path('.').resolve()
sys.path.append(str(project_root))

try:
    from keibaai.src.modules.models.model_train import MuEstimator
except ImportError:
    # パスが違う場合のフォールバック
    sys.path.append(str(project_root / 'keibaai' / 'src'))
    from modules.models.model_train import MuEstimator

def check_model_features():
    model_dir = Path('keibaai/data/models')
    mu_model_path = model_dir / 'mu_model.pkl'
    feature_json_path = model_dir / 'feature_names.json'
    
    print(f"Checking model at: {model_dir}")
    
    # 1. JSONファイルがあれば確認
    if feature_json_path.exists():
        print(f"\nLoading feature_names.json...")
        with open(feature_json_path, 'r') as f:
            feature_names = json.load(f)
        print(f"Feature count: {len(feature_names)}")
        print(f"First 10 features: {feature_names[:10]}")
        
        # 非数値カラムが含まれているかチェック
        bad_cols = ['race_id', 'horse_id', 'race_date', 'race_date_str']
        found_bad = [col for col in bad_cols if col in feature_names]
        if found_bad:
            print(f"⚠️ WARNING: Non-numeric columns found in feature_names: {found_bad}")
        else:
            print("✅ No obvious non-numeric columns in feature_names.")
            
    # 2. Pickleファイルからモデルをロードして確認
    if mu_model_path.exists():
        print(f"\nLoading mu_model.pkl...")
        try:
            model = joblib.load(mu_model_path)
            if hasattr(model, 'feature_names_'):
                f_names = model.feature_names_
                print(f"Model.feature_names_ count: {len(f_names)}")
                
                bad_cols = ['race_id', 'horse_id', 'race_date', 'race_date_str']
                found_bad = [col for col in bad_cols if col in f_names]
                if found_bad:
                    print(f"⚠️ WARNING: Non-numeric columns found in model.feature_names_: {found_bad}")
                else:
                    print("✅ No obvious non-numeric columns in model.feature_names_.")
            else:
                print("Model does not have feature_names_ attribute.")
                
            # Rankerの特徴量も確認
            if hasattr(model, 'model_ranker') and model.model_ranker is not None:
                 print(f"Ranker n_features: {model.model_ranker.n_features_}")
            elif hasattr(model, 'ranker') and model.ranker is not None:
                 # LightGBM Booster
                 print(f"Ranker (old) feature count: {model.ranker.booster_.num_feature()}")

        except Exception as e:
            print(f"Error loading model: {e}")
    else:
        print("mu_model.pkl not found.")

if __name__ == "__main__":
    check_model_features()
