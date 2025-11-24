"""
生成された特徴量とモデルの特徴量リストを比較するスクリプト
"""
import joblib
import pandas as pd
from pathlib import Path
import sys
import json

# プロジェクトルートをパスに追加
project_root = Path('.').resolve()
sys.path.append(str(project_root))

def compare_features():
    # 1. モデルの特徴量リストを取得
    model_dir = Path('keibaai/data/models')
    mu_model_path = model_dir / 'mu_model.pkl'
    
    model_features = []
    if mu_model_path.exists():
        try:
            model = joblib.load(mu_model_path)
            if hasattr(model, 'feature_names_'):
                model_features = model.feature_names_
            elif hasattr(model, 'model_ranker') and hasattr(model.model_ranker, 'feature_name_'):
                model_features = model.model_ranker.feature_name_
            elif hasattr(model, 'ranker') and hasattr(model.ranker, 'feature_name_'):
                model_features = model.ranker.feature_name_
        except Exception as e:
            print(f"Error loading model: {e}")
            return

    print(f"Model expects {len(model_features)} features.")
    
    # 2. 生成された特徴量ファイルをロード（最新のものを探す）
    features_dir = Path('keibaai/data/features')
    # 2024年のファイルを探す
    feature_files = list(features_dir.glob('**/*.parquet'))
    if not feature_files:
        print("No feature files found in keibaai/data/features")
        return
        
    # 適当なファイルを1つ読み込む
    target_file = feature_files[0]
    print(f"Loading features from: {target_file}")
    
    try:
        df = pd.read_parquet(target_file)
        generated_features = df.columns.tolist()
        print(f"Generated dataframe has {len(generated_features)} columns.")
        
        with open('feature_diff.txt', 'w', encoding='utf-8') as f:
            sys.stdout = f
            
            # 3. 比較
            missing_in_df = [f for f in model_features if f not in generated_features]
            
            if missing_in_df:
                print(f"\n⚠️ {len(missing_in_df)} features expected by model are MISSING in generated data:")
                for feat in missing_in_df:
                    print(f"  - {feat}")
            else:
                print("\n✅ All model features are present in generated data.")
                
            # 逆のチェック（生成されたがモデルに含まれないもの）
            extra_in_df = [f for f in generated_features if f not in model_features]
            print(f"\nℹ️ {len(extra_in_df)} columns in generated data are NOT in model features (meta info etc.):")
            for feat in extra_in_df:
                print(f"  - {feat}")
                
        sys.stdout = sys.__stdout__
        print("Comparison results written to feature_diff.txt")

    except Exception as e:
        print(f"Error loading parquet: {e}")

if __name__ == "__main__":
    compare_features()
