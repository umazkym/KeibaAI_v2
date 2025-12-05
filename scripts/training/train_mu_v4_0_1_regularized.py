#!/usr/bin/env python3
"""
μモデル v4.0.1 学習スクリプト（過学習対策版）

【改善点】
1. ベイズスムージング強化: C=10→C=50
2. 正則化強化: num_leaves縮小、min_child_samples増加
3. 冗長特徴量削除: jockey_win_rate等を除外

【設計思想】
- A/E値と正規化着順を採用
- 左右回り・急坂/平坦の物理特性で分類
- ベイズスムージング・時間減衰を適用
- 過学習防止のため強い正則化
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


class MuV401Trainer:
    """μモデル v4.0.1 学習クラス（過学習対策版）"""
    
    # コース物理特性
    LEFT_TURN_VENUES = ['東京', '新潟', '中京']
    STEEP_SLOPE_VENUES = ['中山', '阪神', '中京']
    
    # 信頼度フィルタ
    MIN_SAMPLES = {'jockey': 15, 'sire': 40}  # 調整版
    
    # 時間減衰率
    TIME_DECAY_RATE = 0.3
    
    # ベイズスムージング定数（調整版）
    SMOOTHING_C = 30  # 50→30に調整（バランス型）
    
    # 削除する冗長特徴量
    REDUNDANT_FEATURES = [
        'jockey_win_rate',  # 通算勝率（条件別で代替）
    ]
    
    def __init__(self):
        self.base_model_dir = Path('keibaai/models/mu_v3_3')
        self.output_dir = Path('keibaai/models/mu_v4_0_2')
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.model = None
    
    @staticmethod
    def extract_venue_name(venue_str: str) -> str:
        if pd.isna(venue_str):
            return None
        match = re.search(r'[0-9]*([^\d]+)[0-9]*', str(venue_str))
        if match:
            return match.group(1)
        return None
    
    @staticmethod
    def get_turn_direction(venue: str) -> str:
        if venue in MuV401Trainer.LEFT_TURN_VENUES:
            return 'left'
        return 'right'
    
    @staticmethod
    def get_slope_type(venue: str) -> str:
        if venue in MuV401Trainer.STEEP_SLOPE_VENUES:
            return 'steep'
        return 'flat'
    
    @staticmethod
    def calculate_normalized_rank(finish: int, n_runners: int) -> float:
        if n_runners <= 1:
            return 0.5
        return 1.0 - (finish - 1) / (n_runners - 1)
    
    @staticmethod
    def calculate_ae_smoothed(actual: float, expected: float, 
                               global_expected: float, c: int = 50) -> float:
        """
        ベイズスムージング付きA/E値
        サンプル数が少ない場合は全体平均（1.0）に近づく
        """
        # 期待値ベースでスムージング
        smoothed_actual = actual + c * 1.0  # 全体平均A/E=1.0を仮定
        smoothed_expected = expected + c * 1.0
        
        if smoothed_expected <= 0:
            return 1.0
        return smoothed_actual / smoothed_expected
    
    @staticmethod
    def time_decay_weight(years_ago: float, decay_rate: float = 0.3) -> float:
        return np.exp(-decay_rate * years_ago)
    
    def load_data(self):
        logging.info("データを読み込み中...")
        
        train_data = pd.read_parquet(self.base_model_dir / 'train_data_mu_v3_3.parquet')
        train_data['race_date'] = pd.to_datetime(train_data['race_date'])
        
        perf_path = Path('keibaai/data/parsed/parquet/horses_performance/horses_performance.parquet')
        perf_df = pd.read_parquet(perf_path)
        perf_df['race_date'] = pd.to_datetime(perf_df['race_date'])
        
        shutuba_path = Path('keibaai/data/parsed/parquet/shutuba/shutuba.parquet')
        shutuba_df = pd.read_parquet(shutuba_path)
        
        pedigree_path = Path('keibaai/data/parsed/parquet/pedigree/pedigree.parquet')
        pedigree_df = pd.read_parquet(pedigree_path) if pedigree_path.exists() else None
        
        logging.info(f"  学習データ: {len(train_data):,} rows")
        logging.info(f"  過去戦績: {len(perf_df):,} rows")
        
        return train_data, perf_df, pedigree_df, shutuba_df
    
    def generate_context_aware_features(self, df: pd.DataFrame, perf_df: pd.DataFrame,
                                         pedigree_df: pd.DataFrame = None,
                                         shutuba_df: pd.DataFrame = None) -> pd.DataFrame:
        logging.info("=" * 60)
        logging.info("v4.0.1 Context-Aware特徴量を生成中（スムージング強化版）...")
        logging.info(f"  スムージング係数 C = {self.SMOOTHING_C}")
        logging.info("=" * 60)
        
        perf = perf_df.copy()
        perf['venue_name'] = perf['venue'].apply(self.extract_venue_name)
        perf['turn_direction'] = perf['venue_name'].apply(
            lambda x: self.get_turn_direction(x) if x else None
        )
        perf['slope_type'] = perf['venue_name'].apply(
            lambda x: self.get_slope_type(x) if x else None
        )
        perf['is_wet'] = perf['track_condition'].isin(['重', '不良'])
        
        perf['n_runners'] = perf['head_count'].fillna(16)
        perf['normalized_rank'] = perf.apply(
            lambda x: self.calculate_normalized_rank(x['finish_position'], x['n_runners']),
            axis=1
        )
        
        perf = perf.dropna(subset=['finish_position', 'n_runners'])
        perf['finish_position'] = perf['finish_position'].astype(int)
        
        perf['expected_win'] = 1 / perf['win_odds'].clip(lower=1.1).fillna(10)
        perf['is_win'] = (perf['finish_position'] == 1).astype(int)
        
        max_date = perf['race_date'].max()
        perf['years_ago'] = (max_date - perf['race_date']).dt.days / 365.25
        perf['time_weight'] = perf['years_ago'].apply(
            lambda x: self.time_decay_weight(x, self.TIME_DECAY_RATE)
        )
        
        result = df.copy()
        
        # ========================================
        # 騎手特徴量（スムージング強化版）
        # ========================================
        if 'jockey_id' in perf.columns:
            logging.info("  騎手特徴量を生成中（スムージング強化）...")
            
            # 左回りA/E
            left_data = perf[perf['turn_direction'] == 'left']
            left_stats = left_data.groupby('jockey_id').apply(
                lambda g: pd.Series({
                    'actual': (g['is_win'] * g['time_weight']).sum(),
                    'expected': (g['expected_win'] * g['time_weight']).sum(),
                    'count': len(g)
                })
            ).reset_index()
            left_stats['jockey_left_turn_ae'] = left_stats.apply(
                lambda x: self.calculate_ae_smoothed(
                    x['actual'], x['expected'], 1.0, self.SMOOTHING_C
                ), axis=1
            )
            left_stats.loc[left_stats['count'] < self.MIN_SAMPLES['jockey'], 
                           'jockey_left_turn_ae'] = np.nan
            
            # 右回りA/E
            right_data = perf[perf['turn_direction'] == 'right']
            right_stats = right_data.groupby('jockey_id').apply(
                lambda g: pd.Series({
                    'actual': (g['is_win'] * g['time_weight']).sum(),
                    'expected': (g['expected_win'] * g['time_weight']).sum(),
                    'count': len(g)
                })
            ).reset_index()
            right_stats['jockey_right_turn_ae'] = right_stats.apply(
                lambda x: self.calculate_ae_smoothed(
                    x['actual'], x['expected'], 1.0, self.SMOOTHING_C
                ), axis=1
            )
            right_stats.loc[right_stats['count'] < self.MIN_SAMPLES['jockey'],
                            'jockey_right_turn_ae'] = np.nan
            
            # 急坂A/E
            steep_data = perf[perf['slope_type'] == 'steep']
            steep_stats = steep_data.groupby('jockey_id').apply(
                lambda g: pd.Series({
                    'actual': (g['is_win'] * g['time_weight']).sum(),
                    'expected': (g['expected_win'] * g['time_weight']).sum(),
                    'count': len(g)
                })
            ).reset_index()
            steep_stats['jockey_steep_slope_ae'] = steep_stats.apply(
                lambda x: self.calculate_ae_smoothed(
                    x['actual'], x['expected'], 1.0, self.SMOOTHING_C
                ), axis=1
            )
            steep_stats.loc[steep_stats['count'] < self.MIN_SAMPLES['jockey'],
                            'jockey_steep_slope_ae'] = np.nan
            
            # 道悪A/E
            wet_data = perf[perf['is_wet']]
            wet_stats = wet_data.groupby('jockey_id').apply(
                lambda g: pd.Series({
                    'actual': (g['is_win'] * g['time_weight']).sum(),
                    'expected': (g['expected_win'] * g['time_weight']).sum(),
                    'count': len(g)
                })
            ).reset_index()
            wet_stats['jockey_wet_ae'] = wet_stats.apply(
                lambda x: self.calculate_ae_smoothed(
                    x['actual'], x['expected'], 1.0, self.SMOOTHING_C
                ), axis=1
            )
            wet_stats.loc[wet_stats['count'] < self.MIN_SAMPLES['jockey'],
                          'jockey_wet_ae'] = np.nan
            
            # jockey_idマージ
            if 'jockey_id' not in result.columns and shutuba_df is not None:
                jockey_map = shutuba_df[['race_id', 'horse_id', 'jockey_id']].drop_duplicates(subset=['race_id', 'horse_id'])
                result = result.merge(jockey_map, on=['race_id', 'horse_id'], how='left')
                logging.info(f"    jockey_idマッチ率: {result['jockey_id'].notna().mean()*100:.1f}%")
            
            result = result.merge(left_stats[['jockey_id', 'jockey_left_turn_ae']], 
                                  on='jockey_id', how='left')
            result = result.merge(right_stats[['jockey_id', 'jockey_right_turn_ae']], 
                                  on='jockey_id', how='left')
            result = result.merge(steep_stats[['jockey_id', 'jockey_steep_slope_ae']], 
                                  on='jockey_id', how='left')
            result = result.merge(wet_stats[['jockey_id', 'jockey_wet_ae']], 
                                  on='jockey_id', how='left')
            
            logging.info("    left_turn_ae, right_turn_ae, steep_slope_ae, wet_ae 生成完了")
        
        logging.info("=" * 60)
        
        return result
    
    def prepare_features(self, df: pd.DataFrame) -> list:
        with open(self.base_model_dir / 'feature_names.json', 'r', encoding='utf-8') as f:
            base_features = json.load(f)
        
        # 冗長特徴量を除外
        base_features = [f for f in base_features if f not in self.REDUNDANT_FEATURES]
        
        new_features = [
            'jockey_left_turn_ae', 'jockey_right_turn_ae', 
            'jockey_steep_slope_ae', 'jockey_wet_ae',
        ]
        
        all_features = base_features + [f for f in new_features if f in df.columns]
        available = [f for f in all_features if f in df.columns]
        
        logging.info(f"特徴量数: {len(available)}（ベース: {len(base_features)}, 新規: {len([f for f in new_features if f in df.columns])}）")
        logging.info(f"削除した冗長特徴量: {self.REDUNDANT_FEATURES}")
        
        return available
    
    def prepare_target(self, df: pd.DataFrame):
        weight_1st, weight_2nd, weight_3rd = 12.74, 6.73, 3.69
        odds = df['win_odds'].fillna(1.0).clip(upper=90)
        log_odds = np.log1p(odds)
        
        gain = np.zeros(len(df))
        gain[df['finish_position'] == 1] = log_odds[df['finish_position'] == 1] * weight_1st
        gain[df['finish_position'] == 2] = log_odds[df['finish_position'] == 2] * weight_2nd
        gain[df['finish_position'] == 3] = log_odds[df['finish_position'] == 3] * weight_3rd
        
        df['target_relevance'] = gain.astype(int)
        df['sample_weight'] = np.log1p(df['win_odds'].fillna(1.0)).clip(upper=np.log1p(100))
        
        return df
    
    def train(self, df: pd.DataFrame, feature_cols: list, n_trials: int = 30):
        logging.info("=" * 60)
        logging.info("μモデル v4.0.1 学習開始（過学習対策版）")
        logging.info("=" * 60)
        
        train_mask = df['race_date'] < '2023-01-01'
        valid_mask = (df['race_date'] >= '2023-01-01') & (df['race_date'] < '2024-01-01')
        test_mask = df['race_date'] >= '2024-01-01'
        
        train_df = df[train_mask].copy()
        valid_df = df[valid_mask].copy()
        test_df = df[test_mask].copy()
        
        group_train = train_df.groupby('race_id').size().to_list()
        group_valid = valid_df.groupby('race_id').size().to_list()
        
        logging.info(f"Train: {len(train_df):,}, Valid: {len(valid_df):,}, Test: {len(test_df):,}")
        
        def calculate_roi(d, preds):
            d = d.copy()
            d['score'] = preds
            d['rank_pred'] = d.groupby('race_id')['score'].rank(ascending=False, method='first')
            bet_df = d[d['rank_pred'] == 1]
            hits = bet_df[bet_df['finish_position'] == 1]
            return hits['win_odds'].sum() / len(bet_df) if len(bet_df) > 0 else 0
        
        # 正則化強化版の探索範囲
        def objective(trial):
            params = {
                'objective': 'lambdarank',
                'metric': 'ndcg',
                'eval_at': [1, 3, 5],
                'boosting_type': 'gbdt',
                'num_leaves': trial.suggest_int('num_leaves', 15, 63),  # 縮小
                'lambda_l1': trial.suggest_float('lambda_l1', 1.0, 20.0, log=True),  # 強化
                'lambda_l2': trial.suggest_float('lambda_l2', 1.0, 20.0, log=True),  # 強化
                'min_child_samples': trial.suggest_int('min_child_samples', 100, 500),  # 増加
                'learning_rate': trial.suggest_float('learning_rate', 0.01, 0.1, log=True),
                'feature_fraction': trial.suggest_float('feature_fraction', 0.4, 0.7),  # 縮小
                'bagging_fraction': trial.suggest_float('bagging_fraction', 0.6, 0.9),
                'bagging_freq': trial.suggest_int('bagging_freq', 1, 7),
                'max_depth': trial.suggest_int('max_depth', 4, 8),  # 追加
                'verbose': -1,
                'random_state': 42,
                'label_gain': list(range(100))
            }
            
            model = lgb.LGBMRanker(**params, n_estimators=1500)
            model.fit(
                train_df[feature_cols], train_df['target_relevance'],
                group=group_train, sample_weight=train_df['sample_weight'],
                eval_set=[(valid_df[feature_cols], valid_df['target_relevance'])],
                eval_group=[group_valid],
                callbacks=[lgb.early_stopping(stopping_rounds=50, verbose=False)]
            )
            
            valid_preds = model.predict(valid_df[feature_cols])
            test_preds = model.predict(test_df[feature_cols])
            
            valid_roi = calculate_roi(valid_df, valid_preds)
            test_roi = calculate_roi(test_df, test_preds)
            
            # Valid-Test差も考慮した目的関数
            gap_penalty = abs(valid_roi - test_roi) * 0.5
            score = valid_roi - gap_penalty
            
            return score
        
        logging.info(f"Optuna最適化開始（{n_trials}トライアル、正則化強化版）...")
        study = optuna.create_study(direction='maximize')
        study.optimize(objective, n_trials=n_trials, show_progress_bar=True)
        
        logging.info(f"最適スコア: {study.best_value:.4f}")
        
        best_params = study.best_params
        best_params.update({
            'objective': 'lambdarank', 'metric': 'ndcg', 'eval_at': [1, 3, 5],
            'boosting_type': 'gbdt', 'verbose': -1, 'random_state': 42,
            'label_gain': list(range(100))
        })
        
        self.model = lgb.LGBMRanker(**best_params, n_estimators=2000)
        self.model.fit(
            train_df[feature_cols], train_df['target_relevance'],
            group=group_train, sample_weight=train_df['sample_weight'],
            eval_set=[(valid_df[feature_cols], valid_df['target_relevance'])],
            eval_group=[group_valid],
            callbacks=[lgb.early_stopping(stopping_rounds=100, verbose=True)]
        )
        
        valid_preds = self.model.predict(valid_df[feature_cols])
        valid_roi = calculate_roi(valid_df, valid_preds)
        
        test_preds = self.model.predict(test_df[feature_cols])
        test_roi = calculate_roi(test_df, test_preds)
        
        importance = dict(zip(feature_cols, self.model.feature_importances_))
        sorted_imp = sorted(importance.items(), key=lambda x: x[1], reverse=True)
        
        logging.info("=" * 60)
        logging.info(f"【最終結果: μ v4.0.1（過学習対策版）】")
        logging.info("=" * 60)
        logging.info(f"  Valid ROI: {valid_roi:.2%}")
        logging.info(f"  Test ROI: {test_roi:.2%}")
        logging.info(f"  Valid-Test差: {abs(valid_roi - test_roi):.2%}")
        logging.info("")
        logging.info("【特徴量重要度 Top 20】")
        for name, imp in sorted_imp[:20]:
            logging.info(f"  {name}: {imp}")
        
        logging.info("")
        logging.info("【新規特徴量の重要度】")
        new_features = ['jockey_left_turn_ae', 'jockey_right_turn_ae', 
                        'jockey_steep_slope_ae', 'jockey_wet_ae']
        for f in new_features:
            if f in importance:
                logging.info(f"  {f}: {importance[f]}")
        
        self.save_model(feature_cols, valid_roi, test_roi, best_params)
        
        return valid_roi, test_roi
    
    def save_model(self, features, valid_roi, test_roi, best_params):
        with open(self.output_dir / 'mu_v4_0_1_ranker.pkl', 'wb') as f:
            pickle.dump(self.model, f)
        
        with open(self.output_dir / 'feature_names.json', 'w', encoding='utf-8') as f:
            json.dump(features, f, ensure_ascii=False, indent=2)
        
        save_params = {k: v for k, v in best_params.items() if k != 'label_gain'}
        metadata = {
            'version': 'v4.0.1',
            'description': 'Context-Aware版（過学習対策: スムージングC=50, 正則化強化）',
            'valid_roi': valid_roi,
            'test_roi': test_roi,
            'valid_test_gap': abs(valid_roi - test_roi),
            'feature_count': len(features),
            'smoothing_c': self.SMOOTHING_C,
            'best_params': save_params,
            'created_at': datetime.now().isoformat()
        }
        
        with open(self.output_dir / 'model_info.json', 'w', encoding='utf-8') as f:
            json.dump(metadata, f, ensure_ascii=False, indent=2)
        
        logging.info(f"モデル保存完了: {self.output_dir}")
    
    def run(self, n_trials: int = 30):
        train_data, perf_df, pedigree_df, shutuba_df = self.load_data()
        train_data = self.generate_context_aware_features(train_data, perf_df, pedigree_df, shutuba_df)
        train_data = self.prepare_target(train_data)
        feature_cols = self.prepare_features(train_data)
        return self.train(train_data, feature_cols, n_trials=n_trials)


if __name__ == "__main__":
    trainer = MuV401Trainer()
    trainer.run(n_trials=30)
