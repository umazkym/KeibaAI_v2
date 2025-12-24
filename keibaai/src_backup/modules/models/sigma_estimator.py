import numpy as np
import pandas as pd
import lightgbm as lgb
from sklearn.base import BaseEstimator, RegressorMixin
from typing import Dict, Optional, List, Union
import logging

logger = logging.getLogger(__name__)

class SigmaEstimator(BaseEstimator, RegressorMixin):
    """
    馬ごとの完走時間の不確実性（標準偏差σ）を予測するモデル。
    
    アプローチ:
    1. Muモデル（期待値）の予測残差を計算: r = y_true - y_pred
    2. 残差の絶対値（または対数）をターゲットとして学習
    3. 特徴量として、馬の過去の安定性や経験数などを使用
    """
    
    def __init__(self, params: Optional[Dict] = None):
        self.params = params or {
            'objective': 'regression',
            'metric': 'rmse',
            'boosting_type': 'gbdt',
            'n_estimators': 1000,
            'learning_rate': 0.01,
            'num_leaves': 31,
            'max_depth': -1,
            'feature_fraction': 0.8,
            'bagging_fraction': 0.8,
            'bagging_freq': 5,
            'verbose': -1
        }
        self.model = None
        self.feature_names_ = None
        
    def fit(self, X: pd.DataFrame, y: Union[pd.Series, np.ndarray], mu_pred: Union[pd.Series, np.ndarray]):
        """
        モデルの学習
        
        Args:
            X: 特徴量
            y: 実際の完走時間
            mu_pred: Muモデルによる予測完走時間
        """
        # 残差の計算
        residuals = y - mu_pred
        
        # ターゲット: 残差の絶対値の対数（分布を正規に近づけるため）
        # log(|r| + epsilon)
        # 予測時は exp(pred) で戻す
        target = np.log(np.abs(residuals) + 1e-6)
        
        logger.info(f"Training Sigma Estimator with {len(X)} samples")
        
        self.model = lgb.LGBMRegressor(**self.params)
        self.model.fit(X, target)
        self.feature_names_ = X.columns.tolist()
        
        return self
    
    def predict(self, X: pd.DataFrame) -> np.ndarray:
        """
        標準偏差σの予測
        """
        if self.model is None:
            raise ValueError("Model has not been trained yet.")
            
        # 対数スケールで予測
        log_abs_residuals = self.model.predict(X)
        
        # 元のスケールに戻す (exp)
        sigma_pred = np.exp(log_abs_residuals)
        
        return sigma_pred
    
    def save_model(self, filepath: str):
        """モデルの保存"""
        if self.model is None:
            raise ValueError("Model has not been trained yet.")
        self.model.booster_.save_model(filepath)
        
    def load_model(self, filepath: str):
        """モデルの読み込み"""
        self.model = lgb.Booster(model_file=filepath)
        # LGBMRegressorラッパーに戻す（予測APIの統一のため）
        # 注意: 完全な復元ではないが、predictは動作する
        self.model = lgb.LGBMRegressor(**self.params)
        self.model._Booster = lgb.Booster(model_file=filepath)
