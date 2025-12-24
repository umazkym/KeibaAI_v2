#!/usr/bin/env python3
# src/models/sigma_estimator.py
"""
src/models/sigma_estimator.py (SigmaEstimator)
σ（馬固有の残差分散）を推定するモデルクラス
仕様書 7.7.2章 に基づく実装
"""

import logging
from typing import Dict, List, Optional
from pathlib import Path
import pandas as pd
import numpy as np
import lightgbm as lgb
import joblib
import json

class SigmaEstimator:
    """
    σ（馬固有の残差分散）推定モデル
    
    仕様書 13.4章 (train_sigma_nu_models.py) に基づき、
    LGBMRegressor を使用して残差の分散（または標準偏差）を予測する。
    """
    
    def __init__(self, config: Dict):
        """
        Args:
            config: configs/models.yaml の 'sigma_estimator' セクション
        """
        self.config = config
        self.params = config.get('params', {
            'objective': 'regression',
            'metric': 'rmse',
            'boosting_type': 'gbdt'
        })
        
        self.model: Optional[lgb.LGBMRegressor] = None
        self.feature_names: List[str] = []
        self.global_sigma: float = 1.0 # フォールバック用のグローバル平均

    def train(
        self,
        features_df: pd.DataFrame,
        feature_names: List[str],
        target_sigma: str = 'sigma_target' # 目的変数 (例: squared_error の平均)
    ):
        """
        σモデルを学習
        
        Args:
            features_df: 学習用特徴量DataFrame (馬単位で集約済みを想定)
            feature_names: 使用する特徴量リスト
            target_sigma: 残差分散のターゲットカラム名
        """
        logging.info("σモデルの学習開始...")
        
        self.feature_names = feature_names
        
        X = features_df[self.feature_names]
        y = features_df[target_sigma]
        
        # グローバル平均を計算（予測失敗時のフォールバック用）
        self.global_sigma = np.sqrt(y.mean()) # ターゲットが分散の場合、標準偏差を保存
        
        self.model = lgb.LGBMRegressor(**self.params)
        self.model.fit(X, y)
        logging.info("σモデルの学習完了")

    def predict(
        self,
        features_df: pd.DataFrame
    ) -> np.ndarray:
        """
        σ（残差の標準偏差）を予測
        
        Args:
            features_df: 予測対象の特徴量DataFrame (レース・馬単位)
        
        Returns:
            σ（標準偏差）の配列
        """
        if self.model is None:
            raise RuntimeError("モデルが学習されていません。 train() または load_model() を呼び出してください。")
            
        X = features_df[self.feature_names]
        
        # 予測 (予測ターゲットは分散 'sigma_target' を想定)
        predicted_variance = self.model.predict(X)
        
        # 負の分散をクリップし、標準偏差（σ）に変換
        predicted_sigma = np.sqrt(np.maximum(predicted_variance, 0.0))
        
        # 異常値をグローバル平均で置換
        predicted_sigma = np.nan_to_num(predicted_sigma, nan=self.global_sigma)
        predicted_sigma[predicted_sigma == 0] = self.global_sigma
        
        return predicted_sigma

    def save_model(self, model_dir: str):
        """
        学習済みモデルと特徴量リストを保存
        
        Args:
            model_dir: 保存先ディレクトリ (例: data/models/sigma_model)
        """
        output_path = Path(model_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        
        joblib.dump(self.model, output_path / 'model.pkl')
        
        meta_data = {
            'feature_names': self.feature_names,
            'global_sigma': self.global_sigma
        }
        meta_path = output_path / 'metadata.json'
        with open(meta_path, 'w', encoding='utf-8') as f:
            json.dump(meta_data, f, ensure_ascii=False, indent=2)
            
        logging.info(f"σモデルを {model_dir} に保存しました")

    def load_model(self, model_dir: str):
        """
        モデルと特徴量リストをロード
        
        Args:
            model_dir: ロード元ディレクトリ
        """
        model_path = Path(model_dir)

        if not model_path.exists():
            raise FileNotFoundError(f"モデルディレクトリが見つかりません: {model_dir}")

        self.model = joblib.load(model_path / 'model.pkl')
        
        meta_path = model_path / 'metadata.json'
        with open(meta_path, 'r', encoding='utf-8') as f:
            meta_data = json.load(f)
            self.feature_names = meta_data['feature_names']
            self.global_sigma = meta_data['global_sigma']
            
        logging.info(f"σモデルを {model_dir} からロードしました")