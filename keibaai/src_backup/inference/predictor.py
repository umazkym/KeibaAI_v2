import pandas as pd
import numpy as np
import lightgbm as lgb
import pickle
import logging
from pathlib import Path
from typing import List, Dict, Optional

class MuV3Predictor:
    """
    μモデル v3.0 (Synergy/Ranker) 推論クラス
    """
    def __init__(self, model_dir: str = 'keibaai/models/mu_v3_0'):
        self.model_dir = Path(model_dir)
        self.model = None
        self.feature_names = []
        self._load_model()

    def _load_model(self):
        """モデルと特徴量リストの読み込み"""
        model_path = self.model_dir / 'mu_v3_0_ranker.pkl'
        feature_path = self.model_dir / 'feature_names.json'
        
        if not model_path.exists():
            raise FileNotFoundError(f"Model not found: {model_path}")
            
        try:
            with open(model_path, 'rb') as f:
                self.model = pickle.load(f)
            logging.info(f"Loaded Mu v3.0 model from {model_path}")
            
            if feature_path.exists():
                import json
                with open(feature_path, 'r') as f:
                    self.feature_names = json.load(f)
            else:
                logging.warning(f"Feature names not found at {feature_path}. Using model's feature names if available.")
                if hasattr(self.model, 'feature_name_'):
                    self.feature_names = self.model.feature_name_
                    
        except Exception as e:
            logging.error(f"Failed to load model: {e}")
            raise

    def predict(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        推論実行
        Args:
            df: 特徴量DataFrame (race_id, horse_id, ... features ...)
        Returns:
            DataFrame with ['race_id', 'horse_id', 'score', 'rank_pred', 'is_candidate']
        """
        if self.model is None:
            raise ValueError("Model not loaded")
            
        # 特徴量の整合性チェック
        missing_cols = [c for c in self.feature_names if c not in df.columns]
        if missing_cols:
            logging.warning(f"Missing features filled with 0: {missing_cols}")
            for c in missing_cols:
                df[c] = 0
                
        # カテゴリ変数の処理 (必要なら)
        # LGBMはcategory型を扱えるが、学習時と型を合わせる必要がある
        # ここでは簡易的にそのまま渡す
        
        X = df[self.feature_names]
        
        # 予測 (スコア)
        scores = self.model.predict(X)
        
        result = df[['race_id', 'horse_id']].copy()
        result['score'] = scores
        
        # ランキング生成
        result['rank_pred'] = result.groupby('race_id')['score'].rank(ascending=False, method='first')
        
        # 候補選定 (Top 5)
        result['is_candidate'] = result['rank_pred'] <= 5
        
        return result

if __name__ == "__main__":
    # 簡易テスト
    logging.basicConfig(level=logging.INFO)
    try:
        predictor = MuV3Predictor()
        logging.info("Predictor initialized successfully")
    except Exception as e:
        logging.warning(f"Predictor initialization failed (expected if model not trained yet): {e}")
