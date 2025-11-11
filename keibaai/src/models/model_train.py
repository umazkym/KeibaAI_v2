#!/usr/bin/env python3
# src/models/model_train.py
"""
src/models/model_train.py (MuEstimator)
μ（馬の基礎能力）を推定するモデルクラス
仕様書 7.7.1章 に基づく実装
"""

import logging
from typing import Dict, List, Optional
from pathlib import Path
import pandas as pd
import numpy as np
import lightgbm as lgb
import joblib
import json

class MuEstimator:
    """
    μ（馬の基礎能力スコア）推定モデル
    
    仕様書に基づき、LightGBMのRegressor（基礎スコア）とRanker（順位）を
    内部に持つアンサンブルモデルとして実装（ただし運用はRegressor主体でも可）。
   
    """
    
    def __init__(self, config: Dict):
        """
        Args:
            config: configs/models.yaml の 'mu_estimator' セクション
        """
        self.config = config
        self.regressor_params = config.get('regressor_params', {})
        self.ranker_params = config.get('ranker_params', {})
        
        self.model_regressor: Optional[lgb.LGBMRegressor] = None
        self.model_ranker: Optional[lgb.LGBMRanker] = None
        
        self.feature_names: List[str] = []

    def train(
        self,
        features_df: pd.DataFrame,
        feature_names: List[str],
        target_regressor: str = 'finish_time_seconds', # 回帰ターゲット
        target_ranker: str = 'finish_position',     # ランクターゲット
        group_col: str = 'race_id'
    ):
        """
        μモデル（RegressorとRanker）を学習
        
        Args:
            features_df: 学習用特徴量DataFrame
            feature_names: 使用する特徴量のリスト
            target_regressor: 回帰（スコア）の目的変数名
            target_ranker: ランキング（順位）の目的変数名
            group_col: レースID（グループ）のカラム名
        """
        logging.info("μモデルの学習開始...")
        
        self.feature_names = feature_names
        
        X = features_df[self.feature_names]
        
        # 1. Regressor (基礎スコア) の学習
        logging.info(f"Regressor (LGBMRegressor) を '{target_regressor}' で学習中...")
        y_reg = features_df[target_regressor]
        self.model_regressor = lgb.LGBMRegressor(**self.regressor_params)
        self.model_regressor.fit(X, y_reg)
        logging.info("Regressor の学習完了")
        
        # 2. Ranker (順位) の学習
        logging.info(f"Ranker (LGBMRanker) を '{target_ranker}' で学習中...")
        y_rank = features_df[target_ranker]
        
        # グループ（レースごと）のサンプル数を計算
        # group_col でソートされている必要があるため、ソートを実行
        features_df_sorted = features_df.sort_values(by=group_col)
        X_rank = features_df_sorted[self.feature_names]
        y_rank_sorted = features_df_sorted[target_ranker]
        group_counts = features_df_sorted.groupby(group_col).size().values
        
        self.model_ranker = lgb.LGBMRanker(**self.ranker_params)
        self.model_ranker.fit(X_rank, y_rank_sorted, group=group_counts)
        logging.info("Ranker の学習完了")

    def predict(
        self,
        features_df: pd.DataFrame,
        ensemble_weight_regressor: float = 0.5,
        ensemble_weight_ranker: float = 0.5
    ) -> np.ndarray:
        """
        μスコアを予測
        
        Args:
            features_df: 予測対象の特徴量DataFrame
            ensemble_weight_regressor: Regressorの予測値の重み
            ensemble_weight_ranker: Rankerの予測値の重み
        
        Returns:
            μスコアの配列
        """
        if self.model_regressor is None or self.model_ranker is None:
            raise RuntimeError("モデルが学習されていません。 train() または load_model() を呼び出してください。")
            
        if not self.feature_names:
             raise RuntimeError("特徴量リストがロードされていません。")

        X = features_df[self.feature_names]
        
        # 1. Regressor 予測
        # Regressor (例: タイム予測) は値が小さいほど良い
        pred_regressor = self.model_regressor.predict(X)
        # スコア化 (値が大きいほど良いように反転)
        score_regressor = -pred_regressor
        
        # 2. Ranker 予測
        # Ranker はスコアを直接出力 (値が大きいほど順位が良い)
        score_ranker = self.model_ranker.predict(X)
        
        # 3. アンサンブル (Z-score正規化後に加重平均)
        # レースごとの正規化ではなく、バッチ全体で正規化
        score_regressor_norm = (score_regressor - np.mean(score_regressor)) / (np.std(score_regressor) + 1e-6)
        score_ranker_norm = (score_ranker - np.mean(score_ranker)) / (np.std(score_ranker) + 1e-6)
        
        final_score = (
            score_regressor_norm * ensemble_weight_regressor +
            score_ranker_norm * ensemble_weight_ranker
        )
        
        return final_score

    def save_model(self, model_dir: str):
        """
        学習済みモデルと特徴量リストを保存
        
        Args:
            model_dir: 保存先ディレクトリ (例: data/models/mu_model)
        """
        output_path = Path(model_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        
        # モデル保存
        joblib.dump(self.model_regressor, output_path / 'regressor.pkl')
        joblib.dump(self.model_ranker, output_path / 'ranker.pkl')
        
        # 特徴量リスト保存
        features_path = output_path / 'feature_names.json'
        with open(features_path, 'w', encoding='utf-8') as f:
            json.dump(self.feature_names, f, ensure_ascii=False, indent=2)
            
        logging.info(f"μモデルを {model_dir} に保存しました")

    def load_model(self, model_dir: str):
        """
        モデルと特徴量リストをロード
        
        Args:
            model_dir: ロード元ディレクトリ
        """
        model_path = Path(model_dir)
        
        if not model_path.exists():
            raise FileNotFoundError(f"モデルディレクトリが見つかりません: {model_dir}")
            
        self.model_regressor = joblib.load(model_path / 'regressor.pkl')
        self.model_ranker = joblib.load(model_path / 'ranker.pkl')
        
        features_path = model_path / 'feature_names.json'
        with open(features_path, 'r', encoding='utf-8') as f:
            self.feature_names = json.load(f)
            
        logging.info(f"μモデルを {model_dir} からロードしました")