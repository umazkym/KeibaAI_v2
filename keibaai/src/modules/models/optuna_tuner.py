import optuna
import lightgbm as lgb
import numpy as np
import pandas as pd
from sklearn.model_selection import TimeSeriesSplit
from typing import Dict, Any, Tuple
import logging

logger = logging.getLogger(__name__)

class OptunaTuner:
    """
    Optunaを使用したLightGBMのハイパーパラメータ自動チューニング
    """
    
    def __init__(self, n_trials: int = 50, timeout: int = 3600):
        self.n_trials = n_trials
        self.timeout = timeout
        
    def tune_lgbm_regressor(self, X: pd.DataFrame, y: pd.Series) -> Dict[str, Any]:
        """
        LightGBM Regressor (RMSE) のチューニング
        """
        logger.info("Starting Optuna tuning for LightGBM Regressor...")
        
        def objective(trial):
            params = {
                'objective': 'regression',
                'metric': 'rmse',
                'verbosity': -1,
                'boosting_type': 'gbdt',
                'lambda_l1': trial.suggest_float('lambda_l1', 1e-8, 10.0, log=True),
                'lambda_l2': trial.suggest_float('lambda_l2', 1e-8, 10.0, log=True),
                'num_leaves': trial.suggest_int('num_leaves', 2, 256),
                'feature_fraction': trial.suggest_float('feature_fraction', 0.4, 1.0),
                'bagging_fraction': trial.suggest_float('bagging_fraction', 0.4, 1.0),
                'bagging_freq': trial.suggest_int('bagging_freq', 1, 7),
                'min_child_samples': trial.suggest_int('min_child_samples', 5, 100),
                'learning_rate': trial.suggest_float('learning_rate', 0.001, 0.1, log=True),
            }
            
            # TimeSeriesSplit for validation
            tscv = TimeSeriesSplit(n_splits=3)
            scores = []
            
            for train_idx, valid_idx in tscv.split(X):
                X_train, X_valid = X.iloc[train_idx], X.iloc[valid_idx]
                y_train, y_valid = y.iloc[train_idx], y.iloc[valid_idx]
                
                # Explicitly cast to float for regression
                y_train = y_train.astype(float)
                y_valid = y_valid.astype(float)
                
                dtrain = lgb.Dataset(X_train, label=y_train)
                dvalid = lgb.Dataset(X_valid, label=y_valid, reference=dtrain)
                
                model = lgb.train(
                    params, 
                    dtrain, 
                    valid_sets=[dvalid], 
                    num_boost_round=1000,
                    callbacks=[lgb.early_stopping(stopping_rounds=20, verbose=False)]
                )
                
                preds = model.predict(X_valid)
                rmse = np.sqrt(np.mean((y_valid - preds) ** 2))
                scores.append(rmse)
                
            return np.mean(scores)

        study = optuna.create_study(direction='minimize')
        study.optimize(objective, n_trials=self.n_trials, timeout=self.timeout)
        
        logger.info(f"Best trial: {study.best_trial.params}")
        return study.best_trial.params

    def tune_lgbm_ranker(self, X: pd.DataFrame, y: pd.Series, groups: np.ndarray) -> Dict[str, Any]:
        """
        LightGBM Ranker (NDCG) のチューニング
        """
        logger.info("Starting Optuna tuning for LightGBM Ranker...")
        
        # Note: RankerのCVはグループを考慮する必要があり複雑なため、
        # ここでは簡易的に最後の20%を検証データとするホールドアウト法を使用
        
        # データの分割（時系列を維持）
        n_samples = len(X)
        split_idx = int(n_samples * 0.8)
        
        # グループ境界で分割位置を調整
        # split_idxがグループの途中にならないようにする
        current_idx = 0
        adjusted_split_idx = 0
        for g in groups:
            current_idx += g
            if current_idx >= split_idx:
                adjusted_split_idx = current_idx
                break
        
        # グループの分割
        # groups配列自体も分割する必要がある
        # 累積和を使ってインデックスを特定
        group_cumsum = np.cumsum(groups)
        split_group_idx = np.searchsorted(group_cumsum, adjusted_split_idx)
        
        train_groups = groups[:split_group_idx+1]
        valid_groups = groups[split_group_idx+1:]
        
        X_train = X.iloc[:adjusted_split_idx]
        if isinstance(y, pd.Series):
            y_train = y.iloc[:adjusted_split_idx]
            y_valid = y.iloc[adjusted_split_idx:]
        else:
            y_train = y[:adjusted_split_idx]
            y_valid = y[adjusted_split_idx:]
            
        X_valid = X.iloc[adjusted_split_idx:]
        
        # Ensure labels are integers for ranking task
        if not isinstance(y_train, pd.Series):
            y_train = y_train.astype(int)
            y_valid = y_valid.astype(int)
        else:
            y_train = y_train.astype(int)
            y_valid = y_valid.astype(int)
        
        def objective(trial):
            params = {
                'objective': 'lambdarank',
                'metric': 'ndcg',
                'verbosity': -1,
                'boosting_type': 'gbdt',
                'lambda_l1': trial.suggest_float('lambda_l1', 1e-8, 10.0, log=True),
                'lambda_l2': trial.suggest_float('lambda_l2', 1e-8, 10.0, log=True),
                'num_leaves': trial.suggest_int('num_leaves', 2, 256),
                'feature_fraction': trial.suggest_float('feature_fraction', 0.4, 1.0),
                'bagging_fraction': trial.suggest_float('bagging_fraction', 0.4, 1.0),
                'bagging_freq': trial.suggest_int('bagging_freq', 1, 7),
                'min_child_samples': trial.suggest_int('min_child_samples', 5, 100),
                'learning_rate': trial.suggest_float('learning_rate', 0.001, 0.1, log=True),
            }
            
            dtrain = lgb.Dataset(X_train, label=y_train, group=train_groups)
            dvalid = lgb.Dataset(X_valid, label=y_valid, group=valid_groups, reference=dtrain)
            
            model = lgb.train(
                params, 
                dtrain, 
                valid_sets=[dvalid], 
                num_boost_round=1000,
                callbacks=[lgb.early_stopping(stopping_rounds=20, verbose=False)]
            )
            
            # LightGBMのcallbackはbest_scoreを保存している
            # NDCGは最大化なので、Optunaのdirection='maximize'が必要
            # ここではbest_iterationのスコアを取得
            best_score = model.best_score['valid_0']['ndcg@1'] # Top-1 NDCG
            return best_score

        study = optuna.create_study(direction='maximize')
        study.optimize(objective, n_trials=self.n_trials, timeout=self.timeout)
        
        logger.info(f"Best trial: {study.best_trial.params}")
        return study.best_trial.params
