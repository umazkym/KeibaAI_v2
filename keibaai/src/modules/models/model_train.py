import numpy as np
import pandas as pd
import lightgbm as lgb
from sklearn.base import BaseEstimator, RegressorMixin
from typing import Dict, Optional, List, Union
import logging
from tqdm import tqdm

logger = logging.getLogger(__name__)

class MuEstimator(BaseEstimator, RegressorMixin):
    """
    各馬の期待完走時間（μ）を予測するモデル。
    
    アプローチ:
    2段階学習 (Two-Stage Learning)
    1. Ranking Model (LGBMRanker):
       - レース内の相対的な着順（強さ）を学習
       - 特徴量: 馬、騎手、血統など
       - ターゲット: 着順（小さい方が良い）
    
    2. Regression Model (LGBMRegressor):
       - 絶対的な完走時間を学習
       - 特徴量: 元の特徴量 + Rankerの予測スコア
       - ターゲット: 完走時間（秒）
    """
    
    def __init__(self, ranker_params: Optional[Dict] = None, regressor_params: Optional[Dict] = None):
        self.ranker_params = ranker_params or {
            'objective': 'lambdarank',
            'metric': 'ndcg',
            'boosting_type': 'gbdt',
            'n_estimators': 1000,
            'learning_rate': 0.01,
            'num_leaves': 31,
            'verbose': -1
        }
        self.regressor_params = regressor_params or {
            'objective': 'regression',
            'metric': 'rmse',
            'boosting_type': 'gbdt',
            'n_estimators': 1000,
            'learning_rate': 0.01,
            'num_leaves': 31,
            'verbose': -1
        }
        self.ranker = None
        self.regressor = None
        self.feature_names_ = None
        
    def fit(self, X: pd.DataFrame, y: Union[pd.Series, np.ndarray], group: Optional[Union[List, np.ndarray]] = None):
        """
        モデルの学習
        
        Args:
            X: 特徴量
            y: 完走時間（秒）
            group: レースごとの馬数（Ranker学習に必須）
        """
        if group is None:
            raise ValueError("group (number of horses per race) is required for Ranking model.")
            
        logger.info(f"Training Mu Estimator (Ranker + Regressor) with {len(X)} samples")
        
        # 1. Ranker Training
        # Rankerのターゲットは整数の「関連度」スコアが必要
        # yはタイム（浮動小数点）なので、レース内の順位に変換する
        # 各レース内で: タイムが短い馬ほど高いrelevanceスコアを付与
        rank_target = []
        start_idx = 0
        for g in tqdm(group, desc="Generating Rank Targets"):
            group_y = y[start_idx:start_idx+g]
            # argsortで順位を取得（0-indexed）
            ranks = np.argsort(np.argsort(group_y))
            # relevance: 最大ランク - 現在のランク（1着が最高スコア）
            relevance = (g - 1) - ranks
            # LightGBMのデフォルトlabel_gainは31個までなので、30以下にクリップする
            relevance = np.clip(relevance, 0, 30)
            rank_target.extend(relevance)
            start_idx += g
        rank_target = np.array(rank_target, dtype=int)
        
        self.ranker = lgb.LGBMRanker(**self.ranker_params)
        self.ranker.fit(X, rank_target, group=group)
        
        # 2. Feature Augmentation
        rank_score = self.ranker.predict(X)
        X_aug = X.copy()
        X_aug['rank_score'] = rank_score
        
        # 3. Regressor Training
        self.regressor = lgb.LGBMRegressor(**self.regressor_params)
        self.regressor.fit(X_aug, y)
        
        self.feature_names_ = X.columns.tolist()
        
        return self
    
    def predict(self, X: pd.DataFrame) -> np.ndarray:
        """
        期待完走時間μの予測
        """
        if self.ranker is None or self.regressor is None:
            raise ValueError("Model has not been trained yet.")
            
        # Ranker Prediction
        rank_score = self.ranker.predict(X)
        
        # Feature Augmentation
        X_aug = X.copy()
        X_aug['rank_score'] = rank_score
        
        # Regressor Prediction
        mu_pred = self.regressor.predict(X_aug)
        
        return mu_pred
        
    def save_model(self, filepath_base: str):
        """モデルの保存（2つのモデルを別々に保存）"""
        if self.ranker is None or self.regressor is None:
            raise ValueError("Model has not been trained yet.")
            
        self.ranker.booster_.save_model(f"{filepath_base}_ranker.txt")
        self.regressor.booster_.save_model(f"{filepath_base}_regressor.txt")
        
    def load_model(self, filepath_base: str):
        """モデルの読み込み"""
        self.ranker = lgb.LGBMRanker(**self.ranker_params)
        self.ranker._Booster = lgb.Booster(model_file=f"{filepath_base}_ranker.txt")
        
        self.regressor = lgb.LGBMRegressor(**self.regressor_params)
        self.regressor._Booster = lgb.Booster(model_file=f"{filepath_base}_regressor.txt")
