import numpy as np
import pandas as pd
import lightgbm as lgb
from sklearn.base import BaseEstimator, RegressorMixin
from sklearn.linear_model import Ridge
from sklearn.model_selection import TimeSeriesSplit
from typing import List, Dict, Optional, Union, Any
import logging

logger = logging.getLogger(__name__)

class StackingEnsemble(BaseEstimator, RegressorMixin):
    """
    Stacking Ensemble Model
    
    複数のベースモデル（LightGBMの異なる設定など）の予測値を特徴量として、
    メタモデル（Ridge Regressionなど）で最終予測を行う。
    
    [4-2改修] KFold → TimeSeriesSplit に変更。
    競馬データは時系列順であるため、未来のデータで学習してOOF予測を
    生成するリスクを排除。
    """
    
    def __init__(self, base_models: List[Any], meta_model: Optional[Any] = None, n_folds: int = 5):
        self.base_models = base_models
        self.meta_model = meta_model or Ridge(alpha=1.0)
        self.n_folds = n_folds
        self.fitted_base_models_ = [] # List of List of models (per fold)
        
    def fit(self, X: pd.DataFrame, y: pd.Series):
        """
        Stacking学習
        
        1. TimeSeriesSplitでOut-of-Fold (OOF) 予測を作成
        2. OOF予測をメタモデルの学習データとする
        3. 全データでベースモデルを再学習（予測用）
        
        Note: [4-2改修] 時系列データのためTimeSeriesSplitを使用。
        KFoldでは未来のデータで学習するリスクがあった。
        """
        logger.info(f"Training Stacking Ensemble with {len(self.base_models)} base models...")
        
        n_samples = len(X)
        n_models = len(self.base_models)
        
        # OOF Predictions Matrix
        oof_preds = np.zeros((n_samples, n_models))
        oof_mask = np.zeros(n_samples, dtype=bool)
        
        # [4-2改修] TimeSeriesSplitを使用（時系列の順序を保持）
        tscv = TimeSeriesSplit(n_splits=self.n_folds)
        
        for i, (train_idx, valid_idx) in enumerate(tscv.split(X)):
            X_train, X_valid = X.iloc[train_idx], X.iloc[valid_idx]
            y_train, y_valid = y.iloc[train_idx], y.iloc[valid_idx]
            
            for j, model in enumerate(self.base_models):
                import copy
                model_instance = copy.deepcopy(model)
                
                model_instance.fit(X_train, y_train)
                oof_preds[valid_idx, j] = model_instance.predict(X_valid)
            
            # TimeSeriesSplitではvalidation setのみOOF予測がある
            oof_mask[valid_idx] = True
                
        # Train Meta Model on OOF predictions
        # [4-2改修] TimeSeriesSplitでは最初のfoldにvalidation setがないため
        # OOF予測が存在するサンプルのみでメタモデルを学習
        logger.info(f"Training Meta Model on {oof_mask.sum()}/{n_samples} samples with OOF predictions...")
        self.meta_model.fit(oof_preds[oof_mask], y.iloc[np.where(oof_mask)[0]])
        
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
