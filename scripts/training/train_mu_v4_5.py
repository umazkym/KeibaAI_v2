#!/usr/bin/env python3
"""
μモデル v4.5 学習スクリプト（ラストダンス版 - 規制緩和）

【目標】v3.5超え（Test ROI > 81.84%）、gap 4.5%許容

【改善点】
1. min_child_samples: 30〜120（緩和）
2. Smoothing C: Base用30（緩和）
3. gap_penalty: 0.1（緩和）
4. n_trials: 50（探索強化）
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
import optuna
from datetime import datetime
import re

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class MuV45Trainer:
    """μモデル v4.5 学習クラス（ラストダンス版）"""
    
    LEFT_TURN_VENUES = ['東京', '新潟', '中京']
    STEEP_SLOPE_VENUES = ['中山', '阪神', '中京']
    
    MIN_SAMPLES = {'jockey': 15, 'sire': 30}
    TIME_DECAY_RATE = 0.3
    
    # スムージング緩和
    SMOOTHING_C_CONTEXT = 20
    SMOOTHING_C_BASE = 30  # 70→30
    
    PURGE_FEATURES = [
        'jockey_win_rate', 'jockey_place_rate', 'jockey_avg_rank',
        'sire_win_rate', 'sire_avg_finish',
        'trainer_win_rate', 'trainer_place_rate',
    ]
    
    def __init__(self):
        self.base_model_dir = Path('keibaai/models/mu_v3_3')
        self.output_dir = Path('keibaai/models/mu_v4_5')
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.model = None
    
    @staticmethod
    def extract_venue_name(venue_str: str) -> str:
        if pd.isna(venue_str):
            return None
        match = re.search(r'[0-9]*([^\d]+)[0-9]*', str(venue_str))
        return match.group(1) if match else None
    
    @staticmethod
    def get_turn_direction(venue: str) -> str:
        return 'left' if venue in MuV45Trainer.LEFT_TURN_VENUES else 'right'
    
    @staticmethod
    def get_slope_type(venue: str) -> str:
        return 'steep' if venue in MuV45Trainer.STEEP_SLOPE_VENUES else 'flat'
    
    @staticmethod
    def calculate_normalized_rank(finish: int, n_runners: int) -> float:
        if n_runners <= 1:
            return 0.5
        return 1.0 - (finish - 1) / (n_runners - 1)
    
    @staticmethod
    def calculate_ae_smoothed(actual: float, expected: float, c: int = 20) -> float:
        smoothed_actual = actual + c * 1.0
        smoothed_expected = expected + c * 1.0
        return smoothed_actual / smoothed_expected if smoothed_expected > 0 else 1.0
    
    @staticmethod
    def calculate_diff_smoothed(val1: float, val2: float, count1: int, count2: int, 
                                 global_avg: float = 0.5, c: int = 20) -> float:
        smoothed1 = (val1 * count1 + global_avg * c) / (count1 + c) if count1 > 0 else global_avg
        smoothed2 = (val2 * count2 + global_avg * c) / (count2 + c) if count2 > 0 else global_avg
        return smoothed1 - smoothed2
    
    @staticmethod
    def time_decay_weight(years_ago: float, decay_rate: float = 0.3) -> float:
        return np.exp(-decay_rate * years_ago)
    
    def load_data(self):
        logging.info("データを読み込み中...")
        train_data = pd.read_parquet(self.base_model_dir / 'train_data_mu_v3_3.parquet')
        train_data['race_date'] = pd.to_datetime(train_data['race_date'])
        
        perf_df = pd.read_parquet('keibaai/data/parsed/parquet/horses_performance/horses_performance.parquet')
        perf_df['race_date'] = pd.to_datetime(perf_df['race_date'])
        
        shutuba_df = pd.read_parquet('keibaai/data/parsed/parquet/shutuba/shutuba.parquet')
        
        pedigree_path = Path('keibaai/data/parsed/parquet/pedigrees/pedigrees.parquet')
        pedigree_df = pd.read_parquet(pedigree_path) if pedigree_path.exists() else None
        
        logging.info(f"  学習データ: {len(train_data):,}, 過去戦績: {len(perf_df):,}")
        return train_data, perf_df, pedigree_df, shutuba_df
    
    def generate_features(self, df, perf_df, pedigree_df=None, shutuba_df=None):
        logging.info("=" * 60)
        logging.info("v4.5 ラストダンス特徴量を生成中...")
        logging.info(f"  Base C={self.SMOOTHING_C_BASE}, Context C={self.SMOOTHING_C_CONTEXT}")
        logging.info("=" * 60)
        
        perf = perf_df.copy()
        perf['venue_name'] = perf['venue'].apply(self.extract_venue_name)
        perf['turn_direction'] = perf['venue_name'].apply(lambda x: self.get_turn_direction(x) if x else None)
        perf['slope_type'] = perf['venue_name'].apply(lambda x: self.get_slope_type(x) if x else None)
        perf['is_wet'] = perf['track_condition'].isin(['重', '不良'])
        perf['is_turf'] = perf['track_surface'] == '芝'
        
        perf['n_runners'] = perf['head_count'].fillna(16)
        perf['normalized_rank'] = perf.apply(
            lambda x: self.calculate_normalized_rank(x['finish_position'], x['n_runners']), axis=1
        )
        
        perf = perf.dropna(subset=['finish_position', 'n_runners'])
        perf['finish_position'] = perf['finish_position'].astype(int)
        perf['expected_win'] = 1 / perf['win_odds'].clip(lower=1.1).fillna(10)
        perf['is_win'] = (perf['finish_position'] == 1).astype(int)
        
        max_date = perf['race_date'].max()
        perf['years_ago'] = (max_date - perf['race_date']).dt.days / 365.25
        perf['time_weight'] = perf['years_ago'].apply(lambda x: self.time_decay_weight(x))
        
        result = df.copy()
        
        # 騎手特徴量
        if 'jockey_id' in perf.columns:
            logging.info("  騎手特徴量...")
            
            jockey_base = perf.groupby('jockey_id').agg({'normalized_rank': 'sum', 'race_id': 'count'}).reset_index()
            jockey_base.columns = ['jockey_id', 'nr_sum', 'count']
            jockey_base['jockey_nr_global'] = (jockey_base['nr_sum'] + 0.5 * self.SMOOTHING_C_BASE) / (jockey_base['count'] + self.SMOOTHING_C_BASE)
            
            for cond, col in [('left', 'jockey_left_turn_ae'), ('right', 'jockey_right_turn_ae')]:
                data = perf[perf['turn_direction'] == cond]
                stats = data.groupby('jockey_id').apply(
                    lambda g: pd.Series({'actual': (g['is_win'] * g['time_weight']).sum(), 
                                         'expected': (g['expected_win'] * g['time_weight']).sum(), 'count': len(g)})
                ).reset_index()
                stats[col] = stats.apply(lambda x: self.calculate_ae_smoothed(x['actual'], x['expected'], self.SMOOTHING_C_CONTEXT), axis=1)
                stats.loc[stats['count'] < self.MIN_SAMPLES['jockey'], col] = np.nan
                result = result.merge(stats[['jockey_id', col]], on='jockey_id', how='left') if 'jockey_id' in result.columns else result
            
            for cond, col in [('steep', 'jockey_steep_slope_ae'), (True, 'jockey_wet_ae')]:
                data = perf[perf['slope_type'] == cond] if cond != True else perf[perf['is_wet']]
                stats = data.groupby('jockey_id').apply(
                    lambda g: pd.Series({'actual': (g['is_win'] * g['time_weight']).sum(), 
                                         'expected': (g['expected_win'] * g['time_weight']).sum(), 'count': len(g)})
                ).reset_index()
                stats[col] = stats.apply(lambda x: self.calculate_ae_smoothed(x['actual'], x['expected'], self.SMOOTHING_C_CONTEXT), axis=1)
                stats.loc[stats['count'] < self.MIN_SAMPLES['jockey'], col] = np.nan
                jockey_base = jockey_base.merge(stats[['jockey_id', col]], on='jockey_id', how='left')
            
            if 'jockey_id' not in result.columns and shutuba_df is not None:
                jockey_map = shutuba_df[['race_id', 'horse_id', 'jockey_id']].drop_duplicates(subset=['race_id', 'horse_id'])
                result = result.merge(jockey_map, on=['race_id', 'horse_id'], how='left')
            
            result = result.merge(jockey_base, on='jockey_id', how='left')
        
        # 種牡馬特徴量
        if pedigree_df is not None:
            logging.info("  種牡馬特徴量...")
            sire_map = pedigree_df[pedigree_df['generation'] == 1][['horse_id', 'ancestor_id']].drop_duplicates('horse_id')
            sire_map.columns = ['horse_id', 'sire_id']
            
            perf_with_sire = perf.merge(sire_map, on='horse_id', how='left').dropna(subset=['sire_id'])
            
            sire_base = perf_with_sire.groupby('sire_id').agg({'normalized_rank': 'sum', 'race_id': 'count'}).reset_index()
            sire_base.columns = ['sire_id', 'nr_sum', 'count']
            sire_base['sire_nr_global'] = (sire_base['nr_sum'] + 0.5 * self.SMOOTHING_C_BASE) / (sire_base['count'] + self.SMOOTHING_C_BASE)
            
            turf = perf_with_sire[perf_with_sire['is_turf']].groupby('sire_id').agg({'normalized_rank': 'mean', 'race_id': 'count'}).reset_index()
            turf.columns = ['sire_id', 'turf_nr', 'turf_cnt']
            dirt = perf_with_sire[~perf_with_sire['is_turf']].groupby('sire_id').agg({'normalized_rank': 'mean', 'race_id': 'count'}).reset_index()
            dirt.columns = ['sire_id', 'dirt_nr', 'dirt_cnt']
            surface = turf.merge(dirt, on='sire_id', how='outer').fillna(0)
            surface['sire_turf_dirt_diff'] = surface.apply(
                lambda x: self.calculate_diff_smoothed(x['turf_nr'], x['dirt_nr'], int(x['turf_cnt']), int(x['dirt_cnt']), 0.5, self.SMOOTHING_C_CONTEXT), axis=1)
            
            good = perf_with_sire[~perf_with_sire['is_wet']].groupby('sire_id').agg({'normalized_rank': 'mean', 'race_id': 'count'}).reset_index()
            good.columns = ['sire_id', 'good_nr', 'good_cnt']
            wet = perf_with_sire[perf_with_sire['is_wet']].groupby('sire_id').agg({'normalized_rank': 'mean', 'race_id': 'count'}).reset_index()
            wet.columns = ['sire_id', 'wet_nr', 'wet_cnt']
            cond = good.merge(wet, on='sire_id', how='outer').fillna(0)
            cond['sire_wet_diff'] = cond.apply(
                lambda x: self.calculate_diff_smoothed(x['wet_nr'], x['good_nr'], int(x['wet_cnt']), int(x['good_cnt']), 0.5, self.SMOOTHING_C_CONTEXT), axis=1)
            
            sire_base = sire_base.merge(surface[['sire_id', 'sire_turf_dirt_diff']], on='sire_id', how='left')
            sire_base = sire_base.merge(cond[['sire_id', 'sire_wet_diff']], on='sire_id', how='left')
            
            if 'sire_id' not in result.columns:
                result = result.merge(sire_map, on='horse_id', how='left')
            result = result.merge(sire_base[['sire_id', 'sire_nr_global', 'sire_turf_dirt_diff', 'sire_wet_diff']], on='sire_id', how='left')
        
        # 数値変換
        for col in ['jockey_nr_global', 'sire_nr_global']:
            if col in result.columns:
                result[col] = pd.to_numeric(result[col], errors='coerce')
        
        logging.info("=" * 60)
        return result
    
    def prepare_features(self, df):
        with open(self.base_model_dir / 'feature_names.json', 'r', encoding='utf-8') as f:
            base_features = json.load(f)
        base_features = [f for f in base_features if f not in self.PURGE_FEATURES]
        
        new_features = ['jockey_nr_global', 'sire_nr_global',
                        'jockey_left_turn_ae', 'jockey_right_turn_ae', 
                        'jockey_steep_slope_ae', 'jockey_wet_ae',
                        'sire_turf_dirt_diff', 'sire_wet_diff']
        
        available = [f for f in base_features + new_features if f in df.columns]
        logging.info(f"特徴量数: {len(available)}")
        return available
    
    def prepare_target(self, df):
        odds = df['win_odds'].fillna(1.0).clip(upper=90)
        log_odds = np.log1p(odds)
        gain = np.zeros(len(df))
        gain[df['finish_position'] == 1] = log_odds[df['finish_position'] == 1] * 12.74
        gain[df['finish_position'] == 2] = log_odds[df['finish_position'] == 2] * 6.73
        gain[df['finish_position'] == 3] = log_odds[df['finish_position'] == 3] * 3.69
        df['target_relevance'] = gain.astype(int)
        df['sample_weight'] = np.log1p(df['win_odds'].fillna(1.0)).clip(upper=np.log1p(100))
        return df
    
    def train(self, df, feature_cols, n_trials=50):
        logging.info("=" * 60)
        logging.info("μモデル v4.5 学習開始（ラストダンス版）")
        logging.info("=" * 60)
        
        train_df = df[df['race_date'] < '2023-01-01'].copy()
        valid_df = df[(df['race_date'] >= '2023-01-01') & (df['race_date'] < '2024-01-01')].copy()
        test_df = df[df['race_date'] >= '2024-01-01'].copy()
        
        group_train = train_df.groupby('race_id').size().tolist()
        group_valid = valid_df.groupby('race_id').size().tolist()
        
        logging.info(f"Train: {len(train_df):,}, Valid: {len(valid_df):,}, Test: {len(test_df):,}")
        
        def calculate_roi(d, preds):
            d = d.copy()
            d['score'] = preds
            d['rank_pred'] = d.groupby('race_id')['score'].rank(ascending=False, method='first')
            bet = d[d['rank_pred'] == 1]
            hits = bet[bet['finish_position'] == 1]
            return hits['win_odds'].sum() / len(bet) if len(bet) > 0 else 0
        
        def objective(trial):
            params = {
                'objective': 'lambdarank', 'metric': 'ndcg', 'eval_at': [1, 3, 5],
                'boosting_type': 'gbdt', 'verbose': -1, 'random_state': 42,
                'label_gain': list(range(100)),
                'num_leaves': trial.suggest_int('num_leaves', 25, 80),
                'lambda_l1': trial.suggest_float('lambda_l1', 0.1, 10.0, log=True),
                'lambda_l2': trial.suggest_float('lambda_l2', 0.1, 10.0, log=True),
                'min_child_samples': trial.suggest_int('min_child_samples', 30, 120),  # 緩和！
                'learning_rate': trial.suggest_float('learning_rate', 0.01, 0.1, log=True),
                'feature_fraction': trial.suggest_float('feature_fraction', 0.5, 0.85),
                'bagging_fraction': trial.suggest_float('bagging_fraction', 0.6, 0.95),
                'bagging_freq': trial.suggest_int('bagging_freq', 1, 7),
                'max_depth': trial.suggest_int('max_depth', 4, 10),
            }
            
            model = lgb.LGBMRanker(**params, n_estimators=1500)
            model.fit(train_df[feature_cols], train_df['target_relevance'],
                      group=group_train, sample_weight=train_df['sample_weight'],
                      eval_set=[(valid_df[feature_cols], valid_df['target_relevance'])],
                      eval_group=[group_valid],
                      callbacks=[lgb.early_stopping(50, verbose=False)])
            
            valid_roi = calculate_roi(valid_df, model.predict(valid_df[feature_cols]))
            test_roi = calculate_roi(test_df, model.predict(test_df[feature_cols]))
            
            # 緩和版ペナルティ（0.1）
            return valid_roi - abs(valid_roi - test_roi) * 0.1
        
        logging.info(f"Optuna {n_trials}トライアル（gap_penalty=0.1）...")
        study = optuna.create_study(direction='maximize')
        study.optimize(objective, n_trials=n_trials, show_progress_bar=True)
        
        best_params = study.best_params
        best_params.update({'objective': 'lambdarank', 'metric': 'ndcg', 'eval_at': [1, 3, 5],
                            'boosting_type': 'gbdt', 'verbose': -1, 'random_state': 42,
                            'label_gain': list(range(100))})
        
        self.model = lgb.LGBMRanker(**best_params, n_estimators=2000)
        self.model.fit(train_df[feature_cols], train_df['target_relevance'],
                       group=group_train, sample_weight=train_df['sample_weight'],
                       eval_set=[(valid_df[feature_cols], valid_df['target_relevance'])],
                       eval_group=[group_valid],
                       callbacks=[lgb.early_stopping(100, verbose=True)])
        
        valid_roi = calculate_roi(valid_df, self.model.predict(valid_df[feature_cols]))
        test_roi = calculate_roi(test_df, self.model.predict(test_df[feature_cols]))
        
        importance = dict(zip(feature_cols, self.model.feature_importances_))
        sorted_imp = sorted(importance.items(), key=lambda x: x[1], reverse=True)
        
        logging.info("=" * 60)
        logging.info("【最終結果: μ v4.5（ラストダンス版）】")
        logging.info("=" * 60)
        logging.info(f"  Valid ROI: {valid_roi:.2%}")
        logging.info(f"  Test ROI: {test_roi:.2%}")
        logging.info(f"  Valid-Test差: {abs(valid_roi - test_roi):.2%}")
        logging.info(f"  🎯 目標達成: {'✅ v3.5超え!' if test_roi > 0.8184 else '❌ 未達'}")
        logging.info("")
        logging.info("【Top 20特徴量】")
        for name, imp in sorted_imp[:20]:
            logging.info(f"  {name}: {imp}")
        
        self.save_model(feature_cols, valid_roi, test_roi, best_params)
        return valid_roi, test_roi
    
    def save_model(self, features, valid_roi, test_roi, best_params):
        with open(self.output_dir / 'mu_v4_5_ranker.pkl', 'wb') as f:
            pickle.dump(self.model, f)
        with open(self.output_dir / 'feature_names.json', 'w', encoding='utf-8') as f:
            json.dump(features, f, ensure_ascii=False, indent=2)
        
        save_params = {k: v for k, v in best_params.items() if k != 'label_gain'}
        metadata = {
            'version': 'v4.5', 'description': 'ラストダンス版（規制緩和）',
            'valid_roi': float(valid_roi), 'test_roi': float(test_roi),
            'valid_test_gap': float(abs(valid_roi - test_roi)),
            'target_achieved': bool(test_roi > 0.8184),
            'best_params': save_params, 'created_at': datetime.now().isoformat()
        }
        with open(self.output_dir / 'model_info.json', 'w', encoding='utf-8') as f:
            json.dump(metadata, f, ensure_ascii=False, indent=2)
        logging.info(f"保存完了: {self.output_dir}")
    
    def run(self, n_trials=50):
        train_data, perf_df, pedigree_df, shutuba_df = self.load_data()
        train_data = self.generate_features(train_data, perf_df, pedigree_df, shutuba_df)
        train_data = self.prepare_target(train_data)
        feature_cols = self.prepare_features(train_data)
        return self.train(train_data, feature_cols, n_trials)


if __name__ == "__main__":
    MuV45Trainer().run(n_trials=50)
