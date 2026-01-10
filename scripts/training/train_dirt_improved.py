#!/usr/bin/env python3
"""
ダートモデル改善版訓練スクリプト (train_dirt_improved.py)

【改善点】
1. V16特徴量の追加（過去上がり3F平均など）
2. 正則化の強化（過学習防止）
3. ダート特有の特徴量追加（馬場状態の影響が大きい）

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

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('train_dirt_improved.log', encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)


class DirtImprovedTrainer:
    """ダートモデル改善版訓練クラス"""
    
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
    
    def __init__(self, data_dir: str = 'keibaai/data/parsed/parquet'):
        self.data_dir = Path(data_dir)
        self.models_dir = Path('keibaai/models/dirt_improved')
        self.models_dir.mkdir(parents=True, exist_ok=True)
        
        # 改善されたLightGBMパラメータ（正則化強化）
        self.lgb_params = {
            'objective': 'binary',
            'metric': 'auc',
            'boosting_type': 'gbdt',
            'learning_rate': 0.03,  # より小さく（過学習防止）
            'max_depth': 4,  # より浅く
            'num_leaves': 15,  # より少なく
            'min_child_samples': 200,  # より多く
            'reg_alpha': 5.0,  # 強い正則化
            'reg_lambda': 5.0,  # 強い正則化
            'feature_fraction': 0.6,  # 少なめ
            'bagging_fraction': 0.7,
            'bagging_freq': 5,
            'verbosity': -1,
            'seed': 42,
        }
        self.num_boost_round = 500
        
    def load_data(self) -> pd.DataFrame:
        """データ読み込み"""
        logger.info("=" * 60)
        logger.info("データ読み込み開始")
        logger.info("=" * 60)
        
        races_path = self.data_dir / 'races/races.parquet'
        df = pd.read_parquet(races_path)
        
        df['race_date'] = pd.to_datetime(df['race_date'])
        df['year'] = df['race_date'].dt.year
        
        # 2014年以降、ダートのみ、障害・新馬除外
        df = df[df['year'] >= 2014]
        df = df[df['track_surface'] == 'ダート']
        df = df[df['race_class'] != '新馬']
        
        logger.info(f"ダートレース: {len(df):,}")
        
        return df
    
    def _generate_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """特徴量生成（V16特徴量追加）"""
        logger.info("特徴量生成開始（V16拡張）...")
        
        df = df.copy()
        df = df.sort_values(['race_date', 'race_id', 'horse_number']).reset_index(drop=True)
        
        df['target'] = (df['finish_position'] == 1).astype(int)
        
        # === 基本特徴量 ===
        track_cond_map = {'良': 0, '稍重': 1, '重': 2, '不良': 3}
        df['track_condition_encoded'] = df['track_condition'].map(track_cond_map).fillna(0)
        
        df['field_size'] = df.groupby('race_id')['horse_number'].transform('count')
        
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
        
        # 馬の累積平均着順
        df['horse_cum_finish_sum'] = df.groupby('horse_id')['finish_position'].transform(
            lambda x: x.cumsum().shift(1).fillna(0)
        )
        df['horse_avg_finish'] = np.where(
            df['horse_cum_races'] >= 3,
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
        
        # 馬体重変化
        df['weight_change'] = df.groupby('horse_id')['horse_weight'].diff()
        
        # === V16追加特徴量 ===
        # 過去上がり3F累積平均（last_3f_timeから計算）
        if 'last_3f_time' in df.columns:
            df['prev_3f_avg'] = df.groupby('horse_id')['last_3f_time'].transform(
                lambda x: x.expanding().mean().shift(1)
            )
        else:
            df['prev_3f_avg'] = np.nan
        
        # ダート特有: 馬場状態×前走成績の交互作用
        df['track_cond_x_prev_finish'] = df['track_condition_encoded'] * df['prev_finish'].fillna(10)
        
        # 内枠/外枠フラグ（ダートは外枠有利傾向）
        df['is_outer_bracket'] = (df['bracket_number'] >= 6).astype(int)
        
        # レース番号
        df['race_number'] = df['race_id'].str[-2:].astype(int)
        df['is_late_race'] = (df['race_number'] >= 8).astype(int)
        
        # 距離適性（簡易）
        df['distance_match'] = 0
        
        # 競馬場エンコーディング
        venue_encoder = {v: i for i, v in enumerate(df['venue'].unique())}
        df['venue_encoded'] = df['venue'].map(venue_encoder)
        
        # 一時カラムを削除
        temp_cols = ['horse_cum_wins', 'horse_cum_races', 'horse_cum_finish_sum',
                     'jockey_cum_wins', 'jockey_cum_races', 
                     'trainer_cum_wins', 'trainer_cum_races',
                     'prev_race_date']
        df = df.drop(columns=[c for c in temp_cols if c in df.columns], errors='ignore')
        
        logger.info(f"特徴量生成完了: {len(df.columns)} カラム")
        
        return df
    
    def _generate_features_for_test(self, test_df: pd.DataFrame, train_df: pd.DataFrame) -> pd.DataFrame:
        """テストデータ用の特徴量生成"""
        logger.info("  テスト用特徴量生成開始...")
        
        test_df = test_df.copy()
        train_df = train_df.copy()
        
        test_df = test_df.sort_values(['race_date', 'race_id', 'horse_number']).reset_index(drop=True)
        train_df = train_df.sort_values(['race_date', 'race_id', 'horse_number']).reset_index(drop=True)
        
        test_df['target'] = (test_df['finish_position'] == 1).astype(int)
        train_df['target'] = (train_df['finish_position'] == 1).astype(int)
        
        # 基本特徴量
        track_cond_map = {'良': 0, '稍重': 1, '重': 2, '不良': 3}
        test_df['track_condition_encoded'] = test_df['track_condition'].map(track_cond_map).fillna(0)
        test_df['field_size'] = test_df.groupby('race_id')['horse_number'].transform('count')
        
        # 訓練データから統計を計算
        horse_stats = train_df.groupby('horse_id').agg({
            'target': ['sum', 'count'],
            'finish_position': 'mean'
        }).reset_index()
        horse_stats.columns = ['horse_id', 'horse_cum_wins', 'horse_cum_races', 'horse_avg_finish']
        horse_stats['horse_win_rate'] = np.where(
            horse_stats['horse_cum_races'] >= 3,
            horse_stats['horse_cum_wins'] / horse_stats['horse_cum_races'],
            np.nan
        )
        
        jockey_stats = train_df.groupby('jockey_id').agg({
            'target': ['sum', 'count']
        }).reset_index()
        jockey_stats.columns = ['jockey_id', 'jockey_cum_wins', 'jockey_cum_races']
        jockey_stats['jockey_win_rate'] = np.where(
            jockey_stats['jockey_cum_races'] >= 10,
            jockey_stats['jockey_cum_wins'] / jockey_stats['jockey_cum_races'],
            np.nan
        )
        
        trainer_stats = train_df.groupby('trainer_id').agg({
            'target': ['sum', 'count']
        }).reset_index()
        trainer_stats.columns = ['trainer_id', 'trainer_cum_wins', 'trainer_cum_races']
        trainer_stats['trainer_win_rate'] = np.where(
            trainer_stats['trainer_cum_races'] >= 10,
            trainer_stats['trainer_cum_wins'] / trainer_stats['trainer_cum_races'],
            np.nan
        )
        
        # 馬の前走情報
        train_last_race = train_df.sort_values(['horse_id', 'race_date']).groupby('horse_id').last()[
            ['finish_position', 'race_date', 'horse_weight', 'last_3f_time']
        ].reset_index()
        train_last_race.columns = ['horse_id', 'prev_finish', 'prev_race_date', 'prev_horse_weight', 'prev_3f_time']
        
        # 過去上がり3F平均
        if 'last_3f_time' in train_df.columns:
            horse_3f_avg = train_df.groupby('horse_id')['last_3f_time'].mean().reset_index()
            horse_3f_avg.columns = ['horse_id', 'prev_3f_avg']
        else:
            horse_3f_avg = pd.DataFrame({'horse_id': [], 'prev_3f_avg': []})
        
        # マージ
        test_df = test_df.merge(horse_stats[['horse_id', 'horse_win_rate', 'horse_avg_finish']], on='horse_id', how='left')
        test_df = test_df.merge(jockey_stats[['jockey_id', 'jockey_win_rate']], on='jockey_id', how='left')
        test_df = test_df.merge(trainer_stats[['trainer_id', 'trainer_win_rate']], on='trainer_id', how='left')
        test_df = test_df.merge(train_last_race[['horse_id', 'prev_finish', 'prev_race_date', 'prev_horse_weight']], on='horse_id', how='left')
        test_df = test_df.merge(horse_3f_avg, on='horse_id', how='left')
        
        # 前走からの日数
        test_df['days_since_last'] = (test_df['race_date'] - test_df['prev_race_date']).dt.days
        
        # 馬体重変化
        test_df['weight_change'] = test_df['horse_weight'] - test_df['prev_horse_weight']
        
        # ダート特有特徴量
        test_df['track_cond_x_prev_finish'] = test_df['track_condition_encoded'] * test_df['prev_finish'].fillna(10)
        test_df['is_outer_bracket'] = (test_df['bracket_number'] >= 6).astype(int)
        
        # レース番号
        test_df['race_number'] = test_df['race_id'].str[-2:].astype(int)
        test_df['is_late_race'] = (test_df['race_number'] >= 8).astype(int)
        
        test_df['distance_match'] = 0
        
        all_venues = pd.concat([train_df['venue'], test_df['venue']]).unique()
        venue_encoder = {v: i for i, v in enumerate(all_venues)}
        test_df['venue_encoded'] = test_df['venue'].map(venue_encoder)
        
        temp_cols = ['prev_race_date', 'prev_horse_weight']
        test_df = test_df.drop(columns=[c for c in temp_cols if c in test_df.columns], errors='ignore')
        
        return test_df
    
    def _get_feature_cols(self, df: pd.DataFrame) -> List[str]:
        """特徴量カラム取得"""
        exclude = set(self.LEAK_FEATURES + self.META_COLS)
        feature_cols = [c for c in df.columns if c not in exclude]
        
        numeric_cols = []
        for col in feature_cols:
            if df[col].dtype in ['int64', 'float64', 'int32', 'float32', 'int16', 'float16', 'int8']:
                numeric_cols.append(col)
        
        logger.info(f"使用特徴量: {len(numeric_cols)}個")
        return numeric_cols
    
    def _calculate_popularity_baseline(self, test_df: pd.DataFrame) -> Dict:
        """人気ベースラインROI計算"""
        results = {}
        for pop in [1, 2, 3]:
            pop_df = test_df[test_df['popularity'] == pop].copy()
            if len(pop_df) == 0:
                results[f'pop{pop}_roi'] = np.nan
                continue
            hits = pop_df[pop_df['finish_position'] == 1]
            roi = hits['win_odds'].sum() / len(pop_df) if len(pop_df) > 0 else 0
            results[f'pop{pop}_roi'] = roi
        return results
    
    def _train_and_evaluate(self, train_df: pd.DataFrame, test_df: pd.DataFrame, 
                           feature_cols: List[str]) -> Tuple[Dict, lgb.Booster]:
        """訓練と評価"""
        X_train = train_df[feature_cols].copy()
        y_train = train_df['target'].values
        
        X_test = test_df[feature_cols].copy()
        y_test = test_df['target'].values
        
        lgb_train = lgb.Dataset(X_train, label=y_train)
        
        model = lgb.train(
            self.lgb_params,
            lgb_train,
            num_boost_round=self.num_boost_round
        )
        
        preds = model.predict(X_test)
        auc = roc_auc_score(y_test, preds) if y_test.sum() > 0 else np.nan
        
        test_result = test_df[['race_id', 'finish_position', 'win_odds', 'popularity', 'target']].copy()
        test_result['pred_prob'] = preds
        test_result['pred_rank'] = test_result.groupby('race_id')['pred_prob'].rank(ascending=False, method='first')
        
        top1 = test_result[test_result['pred_rank'] == 1].copy()
        hits = top1[top1['target'] == 1]
        roi = hits['win_odds'].sum() / len(top1) if len(top1) > 0 else 0
        hit_rate = len(hits) / len(top1) if len(top1) > 0 else 0
        
        pop_baseline = self._calculate_popularity_baseline(test_df)
        
        result = {
            'train_size': len(train_df),
            'test_size': len(test_df),
            'auc': auc,
            'roi': roi,
            'hit_rate': hit_rate,
            'num_races': len(top1),
            **pop_baseline
        }
        
        return result, model
    
    def run(self, dry_run: bool = False):
        """メイン実行"""
        logger.info("=" * 60)
        logger.info("ダートモデル改善版訓練開始")
        logger.info("=" * 60)
        
        df = self.load_data()
        
        if dry_run:
            logger.info("[DRY RUN] データを縮小")
            df = df[df['year'] >= 2022].copy()
        
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
            test_df = self._generate_features_for_test(test_df_raw, train_df_raw)
            
            if feature_cols is None:
                feature_cols = self._get_feature_cols(train_df)
            
            result, model = self._train_and_evaluate(train_df, test_df, feature_cols)
            result['test_year'] = test_year
            results.append(result)
            final_model = model
            
            logger.info(f"  ROI: {result['roi']:.2%} | 的中率: {result['hit_rate']:.2%}")
            logger.info(f"  人気ベースライン: 1番人気={result['pop1_roi']:.2%}")
        
        results_df = pd.DataFrame(results)
        
        # サマリー
        avg_roi = results_df['roi'].mean()
        avg_pop1 = results_df['pop1_roi'].mean()
        logger.info(f"\n平均ROI: {avg_roi:.2%} (vs人気1: {(avg_roi - avg_pop1)*100:+.1f}pt)")
        
        # 保存
        if final_model:
            results_df.to_csv(self.models_dir / 'improved_results.csv', index=False)
            final_model.save_model(str(self.models_dir / 'dirt_improved_model.txt'))
            
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
    import argparse
    
    parser = argparse.ArgumentParser(description='ダートモデル改善版訓練')
    parser.add_argument('--dry-run', action='store_true', help='小規模データでテスト')
    args = parser.parse_args()
    
    trainer = DirtImprovedTrainer()
    results = trainer.run(dry_run=args.dry_run)
    
    logger.info("\n完了!")
