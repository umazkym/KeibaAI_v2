#!/usr/bin/env python3
"""
σモデル v1 学習スクリプト

【目的】
μモデルv3.3の残差から、各馬の「不確実性（σ）」を予測するモデルを学習。
σが低い馬 = 予測が安定している馬 → 高確信度ベット対象

【データリーク防止】
- finish_position, finish_time_seconds は使用禁止（ターゲット変数）
- win_odds, popularity は使用禁止（確定オッズ）
- 過去走集計は .shift(1) で1行シフト済みのもののみ使用

【日本語】
すべてのログ、コメントは日本語で記述。
"""

import sys
from pathlib import Path

project_root = Path(__file__).resolve().parent.parent.parent
sys.path.append(str(project_root))

import pandas as pd
import numpy as np
import lightgbm as lgb
import pickle
import json
import logging
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('train_sigma_v1.log', encoding='utf-8')
    ]
)

class SigmaModelTrainer:
    """σモデル学習クラス"""
    
    def __init__(self):
        self.mu_model_dir = Path('keibaai/models/mu_v3_3')
        self.sigma_model_dir = Path('keibaai/models/sigma_v1')
        self.sigma_model_dir.mkdir(parents=True, exist_ok=True)
        
        self.mu_model = None
        self.feature_cols = None
        self.sigma_model = None
        
        # σモデル用特徴量（リーク安全）
        self.sigma_features = [
            # 過去成績の安定性
            'past_3_finish_position_std',
            'past_5_finish_position_std',
            'past_10_finish_position_std',
            
            # 経験値
            'career_starts',
            'career_wins',
            
            # 馬の状態
            'days_since_last_race',
            'horse_weight_zscore',
            
            # 年齢
            'age',
            'age_zscore',
            
            # 騎手・調教師
            'jockey_win_rate',
            'trainer_win_rate',
            
            # 過去成績の平均（安定性の逆指標として）
            'past_3_finish_position_mean',
            'past_5_finish_position_mean',
            
            # 血統
            'sire_win_rate',
        ]
    
    def load_mu_model(self):
        """μモデルv3.3を読み込み"""
        logging.info("μモデルv3.3を読み込み中...")
        
        with open(self.mu_model_dir / 'mu_v3_3_ranker.pkl', 'rb') as f:
            self.mu_model = pickle.load(f)
        
        with open(self.mu_model_dir / 'feature_names.json', 'r', encoding='utf-8') as f:
            self.feature_cols = json.load(f)
        
        logging.info(f"μモデル読み込み完了: {len(self.feature_cols)}特徴量")
    
    def load_data(self):
        """学習データを読み込み"""
        logging.info("学習データを読み込み中...")
        
        data_path = self.mu_model_dir / 'train_data_mu_v3_3.parquet'
        df = pd.read_parquet(data_path)
        df['race_date'] = pd.to_datetime(df['race_date'])
        
        logging.info(f"データ読み込み完了: {len(df):,} rows")
        return df
    
    def calculate_residuals(self, df: pd.DataFrame) -> pd.DataFrame:
        """μモデルの残差を計算"""
        logging.info("μモデルで予測し、残差を計算中...")
        
        # gap_ability_popularity の動的生成（v3.3で使用）
        ability_col = 'past_5_finish_position_mean'
        if ability_col in df.columns and 'popularity' in df.columns:
            df['ability_rank'] = df.groupby('race_id')[ability_col].rank(ascending=True, method='first')
            df['gap_ability_popularity'] = df['popularity'] - df['ability_rank']
            logging.info("✅ Gap Feature 'gap_ability_popularity' を生成しました")
        
        # 欠損特徴量を0で埋める
        for col in self.feature_cols:
            if col not in df.columns:
                df[col] = 0
                logging.warning(f"⚠️ 特徴量 '{col}' が見つからないため0で埋めました")
        
        # μ予測
        df['mu_pred'] = self.mu_model.predict(df[self.feature_cols])
        
        # レース内でランクを計算（予測値が高い = 強い）
        df['rank_pred'] = df.groupby('race_id')['mu_pred'].rank(
            ascending=False, method='first'
        )
        
        # 残差 = 実際の着順 - 予測ランク
        # 残差が大きい = 予測が外れた
        df['residual'] = df['finish_position'] - df['rank_pred']
        
        # 残差の統計
        abs_residuals = np.abs(df['residual'])
        logging.info(f"残差統計: mean={abs_residuals.mean():.2f}, std={abs_residuals.std():.2f}")
        
        return df
    
    def prepare_sigma_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """σモデル用特徴量を準備"""
        logging.info("σモデル用特徴量を準備中...")
        
        # 利用可能な特徴量のみ選択
        available_features = [f for f in self.sigma_features if f in df.columns]
        missing_features = [f for f in self.sigma_features if f not in df.columns]
        
        if missing_features:
            logging.warning(f"以下の特徴量が見つかりません: {missing_features}")
        
        logging.info(f"使用特徴量: {len(available_features)}個")
        
        return df, available_features
    
    def train(self, df: pd.DataFrame, available_features: list):
        """σモデルを学習"""
        logging.info("=" * 50)
        logging.info("σモデル学習開始")
        logging.info("=" * 50)
        
        # ターゲット: 残差の絶対値の対数
        # log(|residual| + epsilon) で正規分布に近づける
        df['sigma_target'] = np.log(np.abs(df['residual']) + 0.1)
        
        # 時系列分割
        train_mask = df['race_date'] < '2023-01-01'
        valid_mask = (df['race_date'] >= '2023-01-01') & (df['race_date'] < '2024-01-01')
        test_mask = df['race_date'] >= '2024-01-01'
        
        train_df = df[train_mask]
        valid_df = df[valid_mask]
        test_df = df[test_mask]
        
        logging.info(f"Train: {len(train_df):,}, Valid: {len(valid_df):,}, Test: {len(test_df):,}")
        
        # NaN処理
        for feat in available_features:
            train_df[feat] = train_df[feat].fillna(train_df[feat].median())
            valid_df[feat] = valid_df[feat].fillna(valid_df[feat].median())
            test_df[feat] = test_df[feat].fillna(test_df[feat].median())
        
        # モデル学習
        self.sigma_model = lgb.LGBMRegressor(
            objective='regression',
            metric='rmse',
            n_estimators=1000,
            learning_rate=0.01,
            num_leaves=31,
            max_depth=-1,
            feature_fraction=0.8,
            bagging_fraction=0.8,
            bagging_freq=5,
            verbose=-1,
            random_state=42
        )
        
        self.sigma_model.fit(
            train_df[available_features],
            train_df['sigma_target'],
            eval_set=[(valid_df[available_features], valid_df['sigma_target'])],
            callbacks=[lgb.early_stopping(50, verbose=True)]
        )
        
        # 評価
        valid_pred = self.sigma_model.predict(valid_df[available_features])
        valid_rmse = np.sqrt(np.mean((valid_df['sigma_target'] - valid_pred) ** 2))
        
        test_pred = self.sigma_model.predict(test_df[available_features])
        test_rmse = np.sqrt(np.mean((test_df['sigma_target'] - test_pred) ** 2))
        
        logging.info(f"Valid RMSE: {valid_rmse:.4f}")
        logging.info(f"Test RMSE: {test_rmse:.4f}")
        
        # 保存
        self.save_model(available_features, valid_rmse, test_rmse)
        
        return test_df, available_features
    
    def save_model(self, features: list, valid_rmse: float, test_rmse: float):
        """モデルを保存"""
        logging.info("モデルを保存中...")
        
        with open(self.sigma_model_dir / 'sigma_model.pkl', 'wb') as f:
            pickle.dump(self.sigma_model, f)
        
        with open(self.sigma_model_dir / 'sigma_features.json', 'w', encoding='utf-8') as f:
            json.dump(features, f, ensure_ascii=False, indent=2)
        
        metadata = {
            'version': 'v1',
            'created_at': datetime.now().isoformat(),
            'based_on': 'mu_v3_3',
            'valid_rmse': valid_rmse,
            'test_rmse': test_rmse,
            'feature_count': len(features)
        }
        
        with open(self.sigma_model_dir / 'model_info.json', 'w', encoding='utf-8') as f:
            json.dump(metadata, f, ensure_ascii=False, indent=2)
        
        logging.info(f"モデル保存完了: {self.sigma_model_dir}")
    
    def run(self):
        """メイン実行"""
        self.load_mu_model()
        df = self.load_data()
        df = self.calculate_residuals(df)
        df, available_features = self.prepare_sigma_features(df)
        test_df, _ = self.train(df, available_features)
        
        logging.info("=" * 50)
        logging.info("σモデル v1 学習完了")
        logging.info("=" * 50)
        
        return test_df


if __name__ == "__main__":
    trainer = SigmaModelTrainer()
    trainer.run()
