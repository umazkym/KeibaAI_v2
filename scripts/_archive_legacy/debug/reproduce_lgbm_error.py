import lightgbm as lgb
import pandas as pd
import numpy as np
from sklearn.model_selection import TimeSeriesSplit
import optuna

def reproduce():
    print("Reproducing LightGBM Error with Optuna...")
    
    # 1. Generate Synthetic Data
    n_samples = 200
    X = pd.DataFrame({
        'feature1': np.random.rand(n_samples),
        'feature2': np.random.rand(n_samples)
    })
    y = 100 + 10 * X['feature1'] + 5 * X['feature2'] + np.random.normal(0, 1, n_samples)
    
    def objective(trial):
        params = {
            'objective': 'regression',
            'metric': 'rmse',
            'verbosity': -1,
            'boosting_type': 'gbdt',
            'lambda_l1': trial.suggest_float('lambda_l1', 1e-8, 10.0, log=True),
            'lambda_l2': trial.suggest_float('lambda_l2', 1e-8, 10.0, log=True),
            'num_leaves': trial.suggest_int('num_leaves', 2, 256),
            'learning_rate': trial.suggest_float('learning_rate', 0.001, 0.1, log=True),
        }
        
        tscv = TimeSeriesSplit(n_splits=3)
        scores = []
        for train_idx, valid_idx in tscv.split(X):
            X_train, X_valid = X.iloc[train_idx], X.iloc[valid_idx]
            y_train, y_valid = y.iloc[train_idx], y.iloc[valid_idx]
            
            dtrain = lgb.Dataset(X_train, label=y_train)
            dvalid = lgb.Dataset(X_valid, label=y_valid, reference=dtrain)
            
            model = lgb.train(
                params, 
                dtrain, 
                valid_sets=[dvalid], 
                num_boost_round=100,
                callbacks=[lgb.early_stopping(stopping_rounds=20, verbose=False)]
            )
            scores.append(model.best_score['valid_0']['rmse'])
        return np.mean(scores)

    study = optuna.create_study(direction='minimize')
    study.optimize(objective, n_trials=2)
    print("Optuna optimization successful.")

if __name__ == "__main__":
    reproduce()
