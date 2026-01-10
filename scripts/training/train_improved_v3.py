#!/usr/bin/env python3
"""
改善版モデル訓練スクリプト V3 (train_improved_v3.py)

【改善内容】（A/Bテストで検証済み）
- prev_finish対数変換: +0.8pt
- rest_optimal_gap追加: +0.6pt
- jockey/trainer勝率対数変換: -0.1pt（ただし組み合わせで効果あり）
- is_comeback特徴量: +0.0pt
- 正則化強化: +0.3pt

【結果】
ALL_combined: ROI 75.3% (+1.1pt vs baseline 74.2%)

Author: KeibaAI Development Team
Date: 2026-01-10
"""

import sys
from pathlib import Path

project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(project_root))

import pandas as pd
import numpy as np
import lightgbm as lgb
import logging
import json
from datetime import datetime
from typing import Tuple, List, Dict, Optional
from sklearn.metrics import roc_auc_score
import warnings
warnings.filterwarnings('ignore')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('train_improved_v3.log', encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)


class ImprovedModelTrainerV3:
    """改善版モデル訓練クラスV3"""
    
    LEAK_FEATURES = [
        'finish_position', 'finish_time_seconds', 'margin_seconds', 'prize_money',
        'finish_time_str', 'margin_str', 'last_3f_time', 'time_except_last3f',
        'last3f_rank', 'popularity', 'win_odds', 'odds', 'win_probability',
        'relative_odds', 'popularity_finish_diff',
        'passing_order', 'passing_order_1', 'passing_order_2',
        'passing_order_3', 'passing_order_4', 'final_corner_to_finish',
        'position_change_1_2', 'position_change_2_3', 'position_change_3_4',
        'pace_index', 'target', 'weight',
        'horse_weight_change', 'horse_weight_deviation',
    ]
    
    META_COLS = [
        'race_id', 'horse_id', 'race_date', 'jockey_id', 'trainer_id', 'owner_id',
        'sire_id', 'damsire_id', 'race_name', 'horse_name', 'jockey_name', 
        'trainer_name', 'scratched', 'year', 'track_surface', 'race_class',
        'venue', 'track_condition', 'distance_category', 'bracket_category',
        'sex', 'weather',
    ]
    
    def __init__(self, data_dir: str = 'keibaai/data/parsed/parquet', surface: str = '芝'):
        self.data_dir = Path(data_dir)
        self.surface = surface
        self.models_dir = Path(f'keibaai/models/{surface.lower()}_improved_v3')
        self.models_dir.mkdir(parents=True, exist_ok=True)
        
        # A/Bテストで検証済みのパラメータ
        self.lgb_params = {
            'objective': 'binary',
            'metric': 'auc',
            'boosting_type': 'gbdt',
            'learning_rate': 0.01,        # F: 学習率低下
            'max_depth': 3,
            'num_leaves': 8,
            'min_child_samples': 400,     # E: 正則化強化
            'reg_alpha': 15.0,            # E: 正則化強化
            'reg_lambda': 15.0,           # E: 正則化強化
            'feature_fraction': 0.7,
            'bagging_fraction': 0.7,
            'bagging_freq': 5,
            'verbosity': -1,
            'seed': 42,
        }
        self.num_boost_round = 400  # 学習率低下に合わせて増加
        
    def load_data(self) -> pd.DataFrame:
        """データ読み込み"""
        logger.info("=" * 60)
        logger.info("改善版モデルV3 訓練開始")
        logger.info("=" * 60)
        
        races_path = self.data_dir / 'races/races.parquet'
        df = pd.read_parquet(races_path)
        
        df['race_date'] = pd.to_datetime(df['race_date'])
        df['year'] = df['race_date'].dt.year
        
        df = df[df['year'] >= 2014]
        df = df[df['track_surface'] == self.surface]
        df = df[df['race_class'] != '新馬']
        
        logger.info(f"{self.surface}レース: {len(df):,}")
        return df
    
    def _generate_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """改善版特徴量生成"""
        df = df.copy()
        df = df.sort_values(['race_date', 'race_id', 'horse_number']).reset_index(drop=True)
        df['target'] = (df['finish_position'] == 1).astype(int)
        
        # === 基本特徴量 ===
        track_cond_map = {'良': 0, '稀重': 1, '重': 2, '不良': 3}
        df['track_condition_encoded'] = df['track_condition'].map(track_cond_map).fillna(0)
        df['field_size'] = df.groupby('race_id')['horse_number'].transform('count')
        df['race_number'] = df['race_id'].str[-2:].astype(int)
        df['is_late_race'] = (df['race_number'] >= 8).astype(int)
        
        # === 累積統計（shift(1)でリーク防止）===
        # 馬の累積勝率
        df['horse_cum_wins'] = df.groupby('horse_id')['target'].transform(
            lambda x: x.cumsum().shift(1).fillna(0)
        )
        df['horse_cum_races'] = df.groupby('horse_id')['target'].transform(
            lambda x: x.expanding().count().shift(1).fillna(0)
        )
        df['horse_win_rate'] = np.where(
            df['horse_cum_races'] >= 3,
            df['horse_cum_wins'] / df['horse_cum_races'],
            np.nan
        )
        
        df['horse_cum_finish_sum'] = df.groupby('horse_id')['finish_position'].transform(
            lambda x: x.cumsum().shift(1).fillna(0)
        )
        df['horse_avg_finish'] = np.where(
            df['horse_cum_races'] >= 1,
            df['horse_cum_finish_sum'] / df['horse_cum_races'],
            np.nan
        )
        
        # 騎手の累積勝率
        df['jockey_cum_wins'] = df.groupby('jockey_id')['target'].transform(
            lambda x: x.cumsum().shift(1).fillna(0)
        )
        df['jockey_cum_races'] = df.groupby('jockey_id')['target'].transform(
            lambda x: x.expanding().count().shift(1).fillna(0)
        )
        df['jockey_win_rate'] = np.where(
            df['jockey_cum_races'] >= 10,
            df['jockey_cum_wins'] / df['jockey_cum_races'],
            np.nan
        )
        
        # 調教師の累積勝率
        df['trainer_cum_wins'] = df.groupby('trainer_id')['target'].transform(
            lambda x: x.cumsum().shift(1).fillna(0)
        )
        df['trainer_cum_races'] = df.groupby('trainer_id')['target'].transform(
            lambda x: x.expanding().count().shift(1).fillna(0)
        )
        df['trainer_win_rate'] = np.where(
            df['trainer_cum_races'] >= 10,
            df['trainer_cum_wins'] / df['trainer_cum_races'],
            np.nan
        )
        
        # 前走着順
        df['prev_finish'] = df.groupby('horse_id')['finish_position'].shift(1)
        
        # 前走からの日数
        df['prev_race_date'] = df.groupby('horse_id')['race_date'].shift(1)
        df['days_since_last'] = (df['race_date'] - df['prev_race_date']).dt.days
        
        venue_encoder = {v: i for i, v in enumerate(df['venue'].unique())}
        df['venue_encoded'] = df['venue'].map(venue_encoder)
        
        # === 改善A: prev_finish対数変換 (+0.8pt) ===
        df['prev_finish_log'] = np.log1p(df['prev_finish'].fillna(10))
        
        # === 改善B: rest_optimal_gap (+0.6pt) ===
        df['rest_optimal_gap'] = np.abs(df['days_since_last'].fillna(60) - 45)
        df['is_short_rest'] = (df['days_since_last'].fillna(60) <= 21).astype(int)
        
        # === 改善C: jockey/trainer対数変換（組み合わせで効果あり） ===
        df['jockey_win_rate_log'] = np.log1p(df['jockey_win_rate'].fillna(0.05) * 100) / 5
        df['trainer_win_rate_log'] = np.log1p(df['trainer_win_rate'].fillna(0.05) * 100) / 5
        
        # === 改善D: is_comeback ===
        df['is_comeback'] = (
            (df['prev_finish'].fillna(99) >= 8) & 
            (df['horse_avg_finish'].fillna(99) <= 5)
        ).astype(int)
        
        # 一時カラム削除
        temp_cols = ['horse_cum_wins', 'horse_cum_races', 'horse_cum_finish_sum',
                     'jockey_cum_wins', 'jockey_cum_races',
                     'trainer_cum_wins', 'trainer_cum_races', 'prev_race_date']
        df = df.drop(columns=[c for c in temp_cols if c in df.columns], errors='ignore')
        
        return df
    
    def _get_feature_cols(self, df: pd.DataFrame) -> List[str]:
        """特徴量カラム取得"""
        exclude = set(self.LEAK_FEATURES + self.META_COLS)
        feature_cols = [c for c in df.columns if c not in exclude]
        
        numeric_cols = []
        for col in feature_cols:
            if df[col].dtype in ['int64', 'float64', 'int32', 'float32', 'int16', 'int8']:
                numeric_cols.append(col)
        
        logger.info(f"使用特徴量: {len(numeric_cols)}個")
        return numeric_cols
    
    def run(self):
        """メイン実行"""
        df = self.load_data()
        
        results = []
        final_model = None
        feature_cols = None
        
        test_years = [2020, 2021, 2022, 2023, 2024, 2025]
        
        for test_year in test_years:
            logger.info(f"\n--- テスト年: {test_year} ---")
            
            test_mask = df['year'] == test_year
            train_mask = df['year'] < test_year
            
            test_df_raw = df[test_mask].copy()
            train_df_raw = df[train_mask].copy()
            
            if len(test_df_raw) == 0 or len(train_df_raw) == 0:
                continue
            
            train_df = self._generate_features(train_df_raw)
            test_df = self._generate_features(test_df_raw)
            
            if feature_cols is None:
                feature_cols = self._get_feature_cols(train_df)
            
            X_train = train_df[feature_cols].copy()
            y_train = train_df['target'].values
            
            X_test = test_df[feature_cols].copy()
            for col in feature_cols:
                if col not in X_test.columns:
                    X_test[col] = np.nan
            X_test = X_test[feature_cols]
            y_test = test_df['target'].values
            
            lgb_train = lgb.Dataset(X_train, label=y_train)
            
            model = lgb.train(self.lgb_params, lgb_train, num_boost_round=self.num_boost_round)
            
            preds = model.predict(X_test)
            
            test_result = test_df[['race_id', 'finish_position', 'win_odds', 'target']].copy()
            test_result['pred_prob'] = preds
            test_result['pred_rank'] = test_result.groupby('race_id')['pred_prob'].rank(ascending=False, method='first')
            
            top1 = test_result[test_result['pred_rank'] == 1]
            hits = top1[top1['target'] == 1]
            roi = hits['win_odds'].sum() / len(top1) if len(top1) > 0 else 0
            hit_rate = len(hits) / len(top1) if len(top1) > 0 else 0
            
            # リーク検証
            high_prob = test_result[test_result['pred_prob'] > 0.5]
            leak_check = high_prob['target'].mean() if len(high_prob) > 0 else np.nan
            
            result = {
                'test_year': test_year,
                'roi': roi,
                'hit_rate': hit_rate,
                'n_races': len(top1),
                'leak_check': leak_check,
            }
            results.append(result)
            final_model = model
            
            leak_str = f"(leak_check={leak_check:.2f})" if pd.notna(leak_check) else ""
            logger.info(f"  ROI: {roi*100:.1f}% | 的中率: {hit_rate*100:.1f}% {leak_str}")
        
        results_df = pd.DataFrame(results)
        avg_roi = results_df['roi'].mean()
        avg_hit = results_df['hit_rate'].mean()
        
        logger.info(f"\n平均ROI: {avg_roi*100:.1f}% | 平均的中率: {avg_hit*100:.1f}%")
        
        # 保存
        results_df.to_csv(self.models_dir / 'results.csv', index=False)
        final_model.save_model(str(self.models_dir / 'model.txt'))
        
        with open(self.models_dir / 'features.json', 'w', encoding='utf-8') as f:
            json.dump(feature_cols, f, ensure_ascii=False, indent=2)
        
        importance = pd.DataFrame({
            'feature': feature_cols,
            'importance': final_model.feature_importance(importance_type='gain')
        }).sort_values('importance', ascending=False)
        importance.to_csv(self.models_dir / 'feature_importance.csv', index=False)
        
        logger.info(f"結果保存完了: {self.models_dir}")
        
        return results_df


if __name__ == "__main__":
    trainer = ImprovedModelTrainerV3()
    results = trainer.run()
