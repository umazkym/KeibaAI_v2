import pandas as pd
import numpy as np
import os
import shutil
from keibaai.src.modules.models.optuna_tuner import OptunaTuner
from keibaai.src.modules.models.stacking_ensemble import StackingEnsemble
from keibaai.src.modules.models.model_train import MuEstimator
import lightgbm as lgb

def verify_accuracy_improvements():
    import sys
    import keibaai.src.modules.models.optuna_tuner as optuna_tuner_module
    print(f"DEBUG: OptunaTuner file: {optuna_tuner_module.__file__}")
    print("Verifying Accuracy Improvements (Optuna & Stacking)...")
    
    # 1. Generate Synthetic Data
    n_samples = 200
    n_races = 20
    
    X = pd.DataFrame({
        'feature1': np.random.rand(n_samples),
        'feature2': np.random.rand(n_samples)
    })
    
    race_ids = np.repeat(np.arange(n_races), n_samples // n_races)
    groups = np.array([n_samples // n_races] * n_races)
    
    # True Target
    y = 100 + 10 * X['feature1'] + 5 * X['feature2'] + np.random.normal(0, 1, n_samples)
    
    # 2. Test Optuna Tuner
    print("\n--- Testing OptunaTuner ---")
    tuner = OptunaTuner(n_trials=2, timeout=60) # Very short test
    
    print("Tuning Regressor...")
    print(f"DEBUG: y stats: min={y.min()}, max={y.max()}, mean={y.mean()}")
    best_params_reg = tuner.tune_lgbm_regressor(X, y)
    print(f"Best Regressor Params: {best_params_reg}")
    assert 'learning_rate' in best_params_reg
    
    print("Tuning Ranker...")
    # Ranker target needs to be int (relevance)
    # Simple conversion: higher y (time) -> lower relevance.
    # Let's create a dummy rank based on y within groups
    rank_target = []
    start_idx = 0
    for g in groups:
        group_y = y[start_idx:start_idx+g]
        # argsort twice gives rank (0-indexed)
        ranks = np.argsort(np.argsort(group_y))
        # Relevance: max_rank - rank (so 1st place has highest relevance)
        relevance = (g - 1) - ranks
        rank_target.extend(relevance)
        start_idx += g
    rank_target = np.array(rank_target, dtype=int)
    print(f"Rank Target Sample: {rank_target[:5]}")
    print(f"Rank Target Type: {rank_target.dtype}")
    
    best_params_rank = tuner.tune_lgbm_ranker(X, rank_target, groups)
    print(f"Best Ranker Params: {best_params_rank}")
    assert 'learning_rate' in best_params_rank
    
    # 3. Test MuEstimator with Tuned Params
    print("\n--- Testing MuEstimator with Tuned Params ---")
    mu_model = MuEstimator(ranker_params=best_params_rank, regressor_params=best_params_reg)
    mu_model.fit(X, y, group=groups)
    preds = mu_model.predict(X)
    print(f"Mu Prediction Mean: {preds.mean():.4f}")
    
    # 4. Test Stacking Ensemble
    print("\n--- Testing StackingEnsemble ---")
    # Create two base models with different params
    model1 = lgb.LGBMRegressor(n_estimators=10, random_state=42)
    model2 = lgb.LGBMRegressor(n_estimators=10, random_state=0)
    
    stacking_model = StackingEnsemble(base_models=[model1, model2], n_folds=3)
    stacking_model.fit(X, y)
    
    stack_preds = stacking_model.predict(X)
    print(f"Stacking Prediction Mean: {stack_preds.mean():.4f}")
    
    print("\nVerification Complete.")

if __name__ == "__main__":
    verify_accuracy_improvements()
