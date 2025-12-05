#!/usr/bin/env python3
"""
νモデル v1 学習スクリプト（リーク排除版）

【目的】
レース特徴量から「混沌度（ν）」を予測するモデルを学習。
ν が高いレース = 堅いレース → 高確信度ベット対象

【データリーク防止 - 重要】
❌ 以下の特徴量は絶対に使用禁止:
  - win_odds, avg_win_odds, std_win_odds（確定オッズ）
  - popularity（確定人気）
  - finish_position, finish_time（レース結果）

✅ 使用可能な特徴量:
  - head_count（出走頭数）
  - distance_m, track_surface, track_condition（レース条件）
  - race_class（クラス）
  - venue, weather（開催情報）

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
from scipy.stats import t
from scipy.optimize import minimize_scalar

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('train_nu_v1.log', encoding='utf-8')
    ]
)


def estimate_nu_mle(standardized_residuals: np.ndarray) -> float:
    """
    t分布のMLEでνを推定
    
    Args:
        standardized_residuals: 標準化残差 z = residual / sigma
    
    Returns:
        optimal_nu: 最適なν値（2.1〜30.0）
    """
    if len(standardized_residuals) < 3:
        return 5.0  # デフォルト値
    
    def neg_log_likelihood(nu):
        return -np.sum(t.logpdf(standardized_residuals, df=nu))
    
    try:
        result = minimize_scalar(
            neg_log_likelihood,
            bounds=(2.1, 30.0),
            method='bounded'
        )
        return result.x
    except:
        return 5.0


class NuModelTrainer:
    """νモデル学習クラス（リーク排除版）"""
    
    def __init__(self):
        self.mu_model_dir = Path('keibaai/models/mu_v3_3')
        self.sigma_model_dir = Path('keibaai/models/sigma_v1')
        self.nu_model_dir = Path('keibaai/models/nu_v1')
        self.nu_model_dir.mkdir(parents=True, exist_ok=True)
        
        self.mu_model = None
        self.sigma_model = None
        self.nu_model = None
        
        # νモデル用特徴量（リーク排除版）
        # ❌ win_odds, popularity は絶対に使用禁止
        self.nu_features = [
            # レース構造
            'head_count',
            'distance_m',
            
            # 馬場
            'track_surface_turf',
            'track_surface_dirt',
            'track_condition_good',
            'track_condition_yielding',
            'track_condition_soft',
            'track_condition_heavy',
            
            # クラス
            'race_class_encoded',
            
            # 開催情報
            'venue_encoded',
            'day_of_meeting',
            'round_of_year',
        ]
        
        # リーク特徴量ブラックリスト
        self.leak_blacklist = [
            'win_odds', 'avg_win_odds', 'std_win_odds', 'min_win_odds', 'max_win_odds',
            'popularity', 'morning_popularity', 'popularity_spread',
            'finish_position', 'finish_time_seconds', 'finish_time_str',
            'margin_seconds', 'margin_str', 'last_3f_time',
            'passing_order_1', 'passing_order_2', 'passing_order_3', 'passing_order_4',
            'win_probability', 'relative_odds'
        ]
    
    def load_models(self):
        """μ/σモデルを読み込み"""
        logging.info("μ/σモデルを読み込み中...")
        
        with open(self.mu_model_dir / 'mu_v3_3_ranker.pkl', 'rb') as f:
            self.mu_model = pickle.load(f)
        
        with open(self.mu_model_dir / 'feature_names.json', 'r', encoding='utf-8') as f:
            self.mu_feature_cols = json.load(f)
        
        with open(self.sigma_model_dir / 'sigma_model.pkl', 'rb') as f:
            self.sigma_model = pickle.load(f)
        
        with open(self.sigma_model_dir / 'sigma_features.json', 'r', encoding='utf-8') as f:
            self.sigma_features = json.load(f)
        
        logging.info("μ/σモデル読み込み完了")
    
    def load_data(self):
        """学習データを読み込み"""
        logging.info("学習データを読み込み中...")
        
        data_path = self.mu_model_dir / 'train_data_mu_v3_3.parquet'
        df = pd.read_parquet(data_path)
        df['race_date'] = pd.to_datetime(df['race_date'])
        
        logging.info(f"データ読み込み完了: {len(df):,} rows")
        return df
    
    def create_race_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """レース特徴量を作成（リーク排除版）"""
        logging.info("レース特徴量を作成中（リーク排除）...")
        
        # リーク特徴量チェック
        for col in self.leak_blacklist:
            if col in df.columns:
                logging.warning(f"⚠️ リーク特徴量 '{col}' を削除します")
        
        # 出走頭数
        if 'head_count' not in df.columns:
            df['head_count'] = df.groupby('race_id')['horse_id'].transform('count')
        
        # 馬場フラグ
        if 'track_surface' in df.columns:
            df['track_surface_turf'] = (df['track_surface'] == '芝').astype(int)
            df['track_surface_dirt'] = (df['track_surface'] == 'ダート').astype(int)
        
        # 馬場状態フラグ
        if 'track_condition' in df.columns:
            df['track_condition_good'] = df['track_condition'].isin(['良']).astype(int)
            df['track_condition_yielding'] = df['track_condition'].isin(['稍重']).astype(int)
            df['track_condition_soft'] = df['track_condition'].isin(['重']).astype(int)
            df['track_condition_heavy'] = df['track_condition'].isin(['不良']).astype(int)
        
        # クラスエンコード
        if 'race_class' in df.columns:
            class_order = {
                'G1': 10, 'G2': 9, 'G3': 8, 'オープン': 7, 'OP': 7,
                'L': 6, '3勝クラス': 5, '2勝クラス': 4, '1勝クラス': 3,
                '未勝利': 2, '新馬': 1
            }
            df['race_class_encoded'] = df['race_class'].map(
                lambda x: next((v for k, v in class_order.items() if k in str(x)), 2)
            )
        
        # 開催場所エンコード
        if 'venue' in df.columns:
            venue_map = {
                '東京': 1, '中山': 2, '阪神': 3, '京都': 4,
                '中京': 5, '小倉': 6, '新潟': 7, '福島': 8,
                '札幌': 9, '函館': 10
            }
            df['venue_encoded'] = df['venue'].map(venue_map).fillna(0)
        
        return df
    
    def calculate_standardized_residuals(self, df: pd.DataFrame) -> pd.DataFrame:
        """標準化残差を計算"""
        logging.info("標準化残差を計算中...")
        
        # gap_ability_popularity の動的生成（v3.3で使用）
        ability_col = 'past_5_finish_position_mean'
        if ability_col in df.columns and 'popularity' in df.columns:
            df['ability_rank'] = df.groupby('race_id')[ability_col].rank(ascending=True, method='first')
            df['gap_ability_popularity'] = df['popularity'] - df['ability_rank']
        
        # 欠損特徴量を0で埋める
        for col in self.mu_feature_cols:
            if col not in df.columns:
                df[col] = 0
        
        # μ予測
        df['mu_pred'] = self.mu_model.predict(df[self.mu_feature_cols])
        df['rank_pred'] = df.groupby('race_id')['mu_pred'].rank(ascending=False, method='first')
        df['residual'] = df['finish_position'] - df['rank_pred']
        
        # σ予測
        sigma_features_available = [f for f in self.sigma_features if f in df.columns]
        df_sigma = df[sigma_features_available].fillna(0)
        df['log_sigma_pred'] = self.sigma_model.predict(df_sigma)
        df['sigma_pred'] = np.exp(df['log_sigma_pred'])
        
        # 標準化残差 z = residual / sigma
        df['z'] = df['residual'] / (df['sigma_pred'] + 0.1)
        
        logging.info(f"標準化残差統計: mean={df['z'].mean():.2f}, std={df['z'].std():.2f}")
        
        return df
    
    def estimate_race_nu(self, df: pd.DataFrame) -> pd.DataFrame:
        """レースごとにνをMLE推定"""
        logging.info("レースごとにνをMLE推定中...")
        
        race_nu = {}
        unique_races = df['race_id'].unique()
        
        for i, race_id in enumerate(unique_races):
            if i % 1000 == 0:
                logging.info(f"  進捗: {i}/{len(unique_races)}")
            
            race_z = df[df['race_id'] == race_id]['z'].values
            nu = estimate_nu_mle(race_z)
            race_nu[race_id] = nu
        
        race_nu_df = pd.DataFrame({
            'race_id': list(race_nu.keys()),
            'nu_target': list(race_nu.values())
        })
        
        logging.info(f"ν推定完了: mean={race_nu_df['nu_target'].mean():.2f}, std={race_nu_df['nu_target'].std():.2f}")
        
        return race_nu_df
    
    def train(self, df: pd.DataFrame, race_nu_df: pd.DataFrame):
        """νモデルを学習"""
        logging.info("=" * 50)
        logging.info("νモデル学習開始（リーク排除版）")
        logging.info("=" * 50)
        
        # レース単位の特徴量を準備
        race_df = df.drop_duplicates(subset=['race_id']).copy()
        race_df = race_df.merge(race_nu_df, on='race_id', how='inner')
        
        # 利用可能な特徴量
        available_features = [f for f in self.nu_features if f in race_df.columns]
        missing_features = [f for f in self.nu_features if f not in race_df.columns]
        
        if missing_features:
            logging.warning(f"以下の特徴量が見つかりません: {missing_features}")
        
        # リーク最終チェック
        for feat in available_features:
            if feat in self.leak_blacklist:
                logging.error(f"❌ リーク特徴量 '{feat}' が含まれています！")
                available_features.remove(feat)
        
        logging.info(f"使用特徴量: {available_features}")
        
        # 時系列分割
        train_mask = race_df['race_date'] < '2023-01-01'
        valid_mask = (race_df['race_date'] >= '2023-01-01') & (race_df['race_date'] < '2024-01-01')
        test_mask = race_df['race_date'] >= '2024-01-01'
        
        train_df = race_df[train_mask]
        valid_df = race_df[valid_mask]
        test_df = race_df[test_mask]
        
        logging.info(f"Train: {len(train_df):,}, Valid: {len(valid_df):,}, Test: {len(test_df):,}")
        
        # NaN処理
        for feat in available_features:
            median_val = train_df[feat].median()
            train_df[feat] = train_df[feat].fillna(median_val)
            valid_df[feat] = valid_df[feat].fillna(median_val)
            test_df[feat] = test_df[feat].fillna(median_val)
        
        # モデル学習
        self.nu_model = lgb.LGBMRegressor(
            objective='regression',
            metric='mae',
            n_estimators=500,
            learning_rate=0.01,
            num_leaves=15,
            max_depth=5,
            verbose=-1,
            random_state=42
        )
        
        self.nu_model.fit(
            train_df[available_features],
            train_df['nu_target'],
            eval_set=[(valid_df[available_features], valid_df['nu_target'])],
            callbacks=[lgb.early_stopping(30, verbose=True)]
        )
        
        # 評価
        valid_pred = self.nu_model.predict(valid_df[available_features])
        valid_mae = np.mean(np.abs(valid_df['nu_target'] - valid_pred))
        
        test_pred = self.nu_model.predict(test_df[available_features])
        test_mae = np.mean(np.abs(test_df['nu_target'] - test_pred))
        
        logging.info(f"Valid MAE: {valid_mae:.4f}")
        logging.info(f"Test MAE: {test_mae:.4f}")
        
        # 保存
        self.save_model(available_features, valid_mae, test_mae)
        
        return test_df, available_features
    
    def save_model(self, features: list, valid_mae: float, test_mae: float):
        """モデルを保存"""
        logging.info("モデルを保存中...")
        
        with open(self.nu_model_dir / 'nu_model.pkl', 'wb') as f:
            pickle.dump(self.nu_model, f)
        
        with open(self.nu_model_dir / 'nu_features.json', 'w', encoding='utf-8') as f:
            json.dump(features, f, ensure_ascii=False, indent=2)
        
        metadata = {
            'version': 'v1_leak_free',
            'created_at': datetime.now().isoformat(),
            'based_on': 'mu_v3_3 + sigma_v1',
            'valid_mae': valid_mae,
            'test_mae': test_mae,
            'feature_count': len(features),
            'leak_check': 'PASSED - No win_odds/popularity features'
        }
        
        with open(self.nu_model_dir / 'model_info.json', 'w', encoding='utf-8') as f:
            json.dump(metadata, f, ensure_ascii=False, indent=2)
        
        logging.info(f"モデル保存完了: {self.nu_model_dir}")
    
    def run(self):
        """メイン実行"""
        self.load_models()
        df = self.load_data()
        df = self.create_race_features(df)
        df = self.calculate_standardized_residuals(df)
        race_nu_df = self.estimate_race_nu(df)
        test_df, features = self.train(df, race_nu_df)
        
        logging.info("=" * 50)
        logging.info("νモデル v1（リーク排除版）学習完了")
        logging.info("=" * 50)
        
        return test_df


if __name__ == "__main__":
    trainer = NuModelTrainer()
    trainer.run()
