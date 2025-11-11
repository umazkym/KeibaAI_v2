#!/usr/bin/env python3
# src/models/nu_estimator.py
"""
src/models/nu_estimator.py (NuEstimator)
ν（レース荒れ度）を推定するモデルクラス
仕様書 7.7.3章 に基づく実装
"""

import logging
from typing import Dict, List, Optional
from pathlib import Path
import pandas as pd
import numpy as np
import lightgbm as lgb
import joblib
import json

class NuEstimator:
    """
    ν（レース全体の荒れ度）推定モデル
    
    仕様書 13.4章 (train_sigma_nu_models.py) に基づき、
    LGBMRegressor を使用してレース単位の分散（例: 着順の標準偏差）を予測する。
    """
    
    def __init__(self, config: Dict):
        """
        Args:
            config: configs/models.yaml の 'nu_estimator' セクション
        """
        self.config = config
        self.params = config.get('params', {
            'objective': 'regression',
            'metric': 'rmse',
            'boosting_type': 'gbdt'
        })
        
        self.model: Optional[lgb.LGBMRegressor] = None
        self.feature_names: List[str] = []
        self.global_nu: float = 1.0 # フォールバック用のグローバル平均

    def train(
        self,
        features_df: pd.DataFrame,
        feature_names: List[str],
        target_nu: str = 'nu_target' # 目的変数 (例: finish_position の std)
    ):
        """
        νモデルを学習
        
        Args:
            features_df: 学習用特徴量DataFrame (レース単位で集約済みを想定)
            feature_names: 使用する特徴量リスト
            target_nu: レース荒れ度のターゲットカラム名
        """
        logging.info("νモデルの学習開始...")
        
        self.feature_names = feature_names
        
        X = features_df[self.feature_names]
        y = features_df[target_nu]
        
        # グローバル平均を計算（予測失敗時のフォールバック用）
        self.global_nu = y.mean()
        
        self.model = lgb.LGBMRegressor(**self.params)
        self.model.fit(X, y)
        logging.info("νモデルの学習完了")

    def predict(
        self,
        features_df: pd.DataFrame
    ) -> np.ndarray:
        """
        ν（レース荒れ度）を予測
        
        Args:
            features_df: 予測対象の特徴量DataFrame (レース単位)
        
        Returns:
            ν（荒れ度スコア）の配列
        """
        
        if self.model is None:
            raise RuntimeError("モデルが学習されていません。 train() または load_model() を呼び出してください。")
            
        X = features_df[self.feature_names]
        
        predicted_nu = self.model.predict(X)
        
        # 異常値をグローバル平均で置換
        predicted_nu = np.nan_to_num(predicted_nu, nan=self.global_nu)
        
        # 負の値をクリップ
        return np.maximum(predicted_nu, 0.0)

    def save_model(self, model_path: str):
        """
        学習済みモデルと特徴量リストを保存
        
        Args:
            model_path: 保存先パス (例: data/models/nu_model.pkl)
        """
        output_path = Path(model_path)
        output_dir = output_path.parent
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # モデル本体を保存
        joblib.dump(self.model, output_path)
        
        # メタデータを同階層に保存
        meta_data = {
            'feature_names': self.feature_names,
            'global_nu': self.global_nu
        }
        meta_path = output_dir / f"{output_path.stem}_metadata.json"
        with open(meta_path, 'w', encoding='utf-8') as f:
            json.dump(meta_data, f, ensure_ascii=False, indent=2)
            
        logging.info(f"νモデルを {output_path} に保存しました")

    def load_model(self, model_path: str):
        """
        モデルと特徴量リストをロード
        
        Args:
            model_path: ロード元パス
        """
        model_file = Path(model_path)

        if not model_file.exists():
            raise FileNotFoundError(f"モデルファイルが見つかりません: {model_path}")

        self.model = joblib.load(model_file)
        
        meta_path = model_file.parent / f"{model_file.stem}_metadata.json"
        try:
            with open(meta_path, 'r', encoding='utf-8') as f:
                meta_data = json.load(f)
                self.feature_names = meta_data['feature_names']
                self.global_nu = meta_data['global_nu']
        except FileNotFoundError:
            logging.warning(f"メタファイルが見つかりません: {meta_path}。グローバル値がデフォルトになります。")
            self.feature_names = self.model.feature_name_
            self.global_nu = 1.0
            
        logging.info(f"νモデルを {model_path} からロードしました")