import numpy as np
import pandas as pd
import lightgbm as lgb
from sklearn.base import BaseEstimator, RegressorMixin
from sklearn.linear_model import Ridge
from sklearn.model_selection import KFold
from typing import List, Dict, Optional, Union, Any
import logging

logger = logging.getLogger(__name__)

class StackingEnsemble(BaseEstimator, RegressorMixin):
    """
    Stacking Ensemble Model
    
    複数のベースモデル（LightGBMの異なる設定など）の予測値を特徴量として、
    メタモデル（Ridge Regressionなど）で最終予測を行う。
    """
    
    def __init__(self, base_models: List[Any], meta_model: Optional[Any] = None, n_folds: int = 5):
        self.base_models = base_models
        self.meta_model = meta_model or Ridge(alpha=1.0)
        self.n_folds = n_folds
        self.fitted_base_models_ = [] # List of List of models (per fold)
        
    def fit(self, X: pd.DataFrame, y: pd.Series):
        """
        Stacking学習
        
        1. K-FoldでOut-of-Fold (OOF) 予測を作成
        2. OOF予測をメタモデルの学習データとする
        3. 全データでベースモデルを再学習（予測用）
        """
        logger.info(f"Training Stacking Ensemble with {len(self.base_models)} base models...")
        
        n_samples = len(X)
        n_models = len(self.base_models)
        
        # OOF Predictions Matrix
        oof_preds = np.zeros((n_samples, n_models))
        
        # K-Fold (Shuffle=False for Time Series nature, or use TimeSeriesSplit)
        # Stackingでは通常KFoldを使うが、時系列データの場合は注意が必要。
        # ここではシンプルにKFoldを使用するが、本来は時系列CVが望ましい。
        kf = KFold(n_splits=self.n_folds, shuffle=False)
        
        for i, (train_idx, valid_idx) in enumerate(kf.split(X)):
            X_train, X_valid = X.iloc[train_idx], X.iloc[valid_idx]
            y_train, y_valid = y.iloc[train_idx], y.iloc[valid_idx]
            
            for j, model in enumerate(self.base_models):
                # Clone model (re-instantiate)
                # ここでは簡易的に同じクラス・パラメータで新規作成と仮定
                # 実際は sklearn.base.clone を使うのが良い
                import copy
                model_instance = copy.deepcopy(model)
                
                model_instance.fit(X_train, y_train)
                oof_preds[valid_idx, j] = model_instance.predict(X_valid)
                
        # Train Meta Model on OOF predictions
        logger.info("Training Meta Model...")
        self.meta_model.fit(oof_preds, y)
        
        # Retrain Base Models on Full Data
        logger.info("Retraining Base Models on full data...")
        self.final_base_models_ = []
        for model in self.base_models:
            import copy
            model_instance = copy.deepcopy(model)
            model_instance.fit(X, y)
            self.final_base_models_.append(model_instance)
            
        return self
    
    def predict(self, X: pd.DataFrame) -> np.ndarray:
        """
        予測
        """
        if not hasattr(self, 'final_base_models_'):
            raise ValueError("Model has not been trained yet.")
            
        n_samples = len(X)
        n_models = len(self.final_base_models_)
        
        # Base Models Predictions
        base_preds = np.zeros((n_samples, n_models))
        for j, model in enumerate(self.final_base_models_):
            base_preds[:, j] = model.predict(X)
            
        # Meta Model Prediction
        final_pred = self.meta_model.predict(base_preds)
        
        return final_pred
