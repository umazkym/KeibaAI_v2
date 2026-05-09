import joblib
import json
import pandas as pd
from pathlib import Path
import sys

# プロジェクトルートをパスに追加
project_root = Path(__file__).resolve().parent
sys.path.append(str(project_root))

model_dir = Path("data/models/mu_model_v1")

print(f"Checking model in {model_dir}")

# Load feature names
try:
    with open(model_dir / "feature_names.json", "r") as f:
        feature_names = json.load(f)
    print(f"feature_names.json count: {len(feature_names)}")
except Exception as e:
    print(f"Error loading feature_names.json: {e}")

# Load Ranker
try:
    ranker = joblib.load(model_dir / "ranker.pkl")
    print(f"Ranker n_features_: {ranker.n_features_}")
except Exception as e:
    print(f"Error loading ranker.pkl: {e}")

# Load Regressor
try:
    regressor = joblib.load(model_dir / "regressor.pkl")
    print(f"Regressor n_features_: {regressor.n_features_}")
except Exception as e:
    print(f"Error loading regressor.pkl: {e}")

# Load MuEstimator
try:
    estimator = joblib.load(model_dir / "mu_model.pkl")
    print(f"MuEstimator feature_names_ count: {len(estimator.feature_names_)}")
    if hasattr(estimator.ranker, 'n_features_'):
        print(f"MuEstimator.ranker n_features_: {estimator.ranker.n_features_}")
    if hasattr(estimator.regressor, 'n_features_'):
        print(f"MuEstimator.regressor n_features_: {estimator.regressor.n_features_}")
except Exception as e:
    print(f"Error loading mu_model.pkl: {e}")
