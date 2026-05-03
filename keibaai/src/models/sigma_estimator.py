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
    Method 1 (default, 'log_abs_residual'):
        1. Muモデル（期待値）の予測残差を計算: r = y_true - y_pred
        2. 残差の絶対値の対数をターゲットとして学習
        3. 予測時にexpで元のスケールに戻す
    
    Method 2 ('quantile', v5.2追加):
        1. [4-4改修] 対数絶対値の代わりにQuantile Regressionを使用
        2. 90th/10th percentileの差分でスプレッドを直接推定
        3. 符号情報の欠落なし、heteroscedasticityを直接モデル化
    """
    
    def __init__(
        self, 
        params: Optional[Dict] = None,
        method: str = 'log_abs_residual'  # 'log_abs_residual' or 'quantile'
    ):
        self.method = method
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
        self.model_upper = None  # quantile mode用
        self.model_lower = None  # quantile mode用
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
        residuals = np.array(y) - np.array(mu_pred)
        
        logger.info(f"Training Sigma Estimator ({self.method}) with {len(X)} samples")
        
        if self.method == 'quantile':
            # [4-4改修] Quantile Regressionで直接σを推定
            # 90th percentileと10th percentileの差をσの代理変数として使用
            # σ ≈ (Q90 - Q10) / (2 * 1.2816) ≈ spread / 2.5632 (for normal distribution)
            
            # Quantileモデルのパラメータ
            quantile_params_upper = self.params.copy()
            quantile_params_upper['objective'] = 'quantile'
            quantile_params_upper['metric'] = 'quantile'
            quantile_params_upper['alpha'] = 0.9
            
            quantile_params_lower = self.params.copy()
            quantile_params_lower['objective'] = 'quantile'
            quantile_params_lower['metric'] = 'quantile'
            quantile_params_lower['alpha'] = 0.1
            
            # 残差をターゲットとしてQuantileモデルを学習
            self.model_upper = lgb.LGBMRegressor(**quantile_params_upper)
            self.model_upper.fit(X, residuals)
            
            self.model_lower = lgb.LGBMRegressor(**quantile_params_lower)
            self.model_lower.fit(X, residuals)
            
            logger.info("  Quantile models (Q10, Q90) trained successfully")
            
        else:
            # 従来手法: 残差の絶対値の対数
            # ターゲット: log(|r| + epsilon)
            # 予測時は exp(pred) で戻す
            target = np.log(np.abs(residuals) + 1e-6)
            
            self.model = lgb.LGBMRegressor(**self.params)
            self.model.fit(X, target)
        
        self.feature_names_ = X.columns.tolist()
        
        return self
    
    def predict(self, X: pd.DataFrame) -> np.ndarray:
        """
        標準偏差σの予測
        """
        if self.method == 'quantile':
            if self.model_upper is None or self.model_lower is None:
                raise ValueError("Quantile models have not been trained yet.")
            
            # Q90 - Q10 のスプレッドからσを推定
            q90 = self.model_upper.predict(X)
            q10 = self.model_lower.predict(X)
            
            # 正規分布の場合、Q90 - Q10 = 2 * z_0.9 * σ = 2 * 1.2816 * σ
            spread = q90 - q10
            sigma_pred = np.abs(spread) / 2.5632
            
            # σの下限をクリップ（ゼロは避ける）
            sigma_pred = np.clip(sigma_pred, 0.01, None)
            
            return sigma_pred
        else:
            if self.model is None:
                raise ValueError("Model has not been trained yet.")
                
            # 対数スケールで予測
            log_abs_residuals = self.model.predict(X)
            
            # 元のスケールに戻す (exp)
            sigma_pred = np.exp(log_abs_residuals)
            
            return sigma_pred
    
    def save_model(self, filepath: str):
        """モデルの保存"""
        import joblib
        from pathlib import Path
        
        if self.method == 'quantile':
            if self.model_upper is None or self.model_lower is None:
                raise ValueError("Quantile models have not been trained yet.")
            joblib.dump({
                'method': self.method,
                'model_upper': self.model_upper,
                'model_lower': self.model_lower,
                'params': self.params,
            }, filepath)
        else:
            if self.model is None:
                raise ValueError("Model has not been trained yet.")
            self.model.booster_.save_model(filepath)
        
    def load_model(self, filepath: str):
        """モデルの読み込み"""
        import joblib
        from pathlib import Path
        
        if filepath.endswith('.pkl') or filepath.endswith('.joblib'):
            data = joblib.load(filepath)
            if isinstance(data, dict) and data.get('method') == 'quantile':
                self.method = 'quantile'
                self.model_upper = data['model_upper']
                self.model_lower = data['model_lower']
                self.params = data.get('params', self.params)
                return
        
        # 旧形式: LightGBMモデル直接
        self.model = lgb.LGBMRegressor(**self.params)
        self.model._Booster = lgb.Booster(model_file=filepath)

