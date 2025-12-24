import numpy as np
import pandas as pd
import lightgbm as lgb
from scipy.stats import t
from scipy.optimize import minimize_scalar
from sklearn.base import BaseEstimator, RegressorMixin
from typing import Dict, Optional, List, Union
import logging

logger = logging.getLogger(__name__)

class NuEstimator(BaseEstimator, RegressorMixin):
    """
    レースごとの自由度（ν: Nu）を予測するモデル。
    νが小さいほど裾が重い（荒れやすい）、大きいほど正規分布に近い（堅い）。
    
    アプローチ:
    1. レースごとに、残差（標準化済み）の分布に最もフィットするνをMLEで推定。
    2. 推定されたνを正解ラベルとして、レース特徴量からνを予測するモデルを学習。
    """
    
    def __init__(self, params: Optional[Dict] = None):
        self.params = params or {
            'objective': 'regression',
            'metric': 'mae', # νの予測は外れ値の影響を抑えるためMAE推奨
            'boosting_type': 'gbdt',
            'n_estimators': 500,
            'learning_rate': 0.01,
            'num_leaves': 15,
            'max_depth': 5,
            'verbose': -1
        }
        self.model = None
        self.feature_names_ = None
        
    def _estimate_nu_mle(self, standardized_residuals: np.ndarray) -> float:
        """
        標準化残差からMLEで最適なνを推定する
        """
        if len(standardized_residuals) < 3:
            return 5.0 # データ不足時はデフォルト値
            
        def neg_log_likelihood(nu):
            # t分布の対数尤度のマイナスを最小化
            return -np.sum(t.logpdf(standardized_residuals, df=nu))
            
        # νの探索範囲: 2.1 (非常に荒れる) 〜 30.0 (ほぼ正規分布)
        result = minimize_scalar(
            neg_log_likelihood,
            bounds=(2.1, 30.0),
            method='bounded'
        )
        
        return result.x
        
    def fit(self, 
            race_features_df: pd.DataFrame, 
            y: Union[pd.Series, np.ndarray], 
            mu_pred: Union[pd.Series, np.ndarray],
            sigma_pred: Union[pd.Series, np.ndarray],
            race_ids: Union[pd.Series, np.ndarray]):
        """
        モデルの学習
        
        Args:
            race_features_df: レース単位の特徴量（1行1レース）
            y: 各馬の実際の完走時間
            mu_pred: Muモデルの予測値
            sigma_pred: Sigmaモデルの予測値
            race_ids: 各サンプルのレースID
        """
        # 1. 標準化残差の計算 z = (y - μ) / σ
        z = (y - mu_pred) / sigma_pred
        
        # 2. レースごとに最適なνを推定
        df_temp = pd.DataFrame({
            'race_id': race_ids,
            'z': z
        })
        
        race_nu_targets = {}
        unique_races = df_temp['race_id'].unique()
        
        logger.info(f"Estimating optimal Nu for {len(unique_races)} races...")
        
        for rid in unique_races:
            residuals = df_temp[df_temp['race_id'] == rid]['z'].values
            optimal_nu = self._estimate_nu_mle(residuals)
            race_nu_targets[rid] = optimal_nu
            
        # 3. 学習用データの作成
        # race_features_df はすでにレースIDでユニークになっている前提、またはここで集約が必要
        # ここでは race_features_df のインデックスが race_id であるか、カラムに含まれていると仮定
        
        # ターゲット変数のシリーズ作成
        target_series = pd.Series(race_nu_targets, name='target_nu')
        
        # 特徴量とターゲットの結合
        # race_features_df がレース単位（ユニーク）であることを確認
        if 'race_id' in race_features_df.columns:
            train_df = race_features_df.drop_duplicates(subset=['race_id']).set_index('race_id')
        else:
            train_df = race_features_df # インデックスがrace_idと仮定
            
        # 共通するレースIDのみで学習
        common_indices = train_df.index.intersection(target_series.index)
        X_train = train_df.loc[common_indices]
        y_train = target_series.loc[common_indices]
        
        logger.info(f"Training Nu Estimator with {len(X_train)} races")
        
        self.model = lgb.LGBMRegressor(**self.params)
        self.model.fit(X_train, y_train)
        self.feature_names_ = X_train.columns.tolist()
        
        return self
    
    def predict(self, race_features_df: pd.DataFrame) -> np.ndarray:
        """
        レース自由度νの予測
        """
        if self.model is None:
            raise ValueError("Model has not been trained yet.")
            
        # race_idカラムがあれば除外して予測
        X = race_features_df.copy()
        if 'race_id' in X.columns:
             # 重複排除（レース単位にする）
             X = X.drop_duplicates(subset=['race_id']).set_index('race_id')
        
        # モデルの特徴量と一致させる（必要なら）
        if self.feature_names_:
            X = X[self.feature_names_]
            
        return self.model.predict(X)
        
    def save_model(self, filepath: str):
        if self.model is None:
            raise ValueError("Model has not been trained yet.")
        self.model.booster_.save_model(filepath)
