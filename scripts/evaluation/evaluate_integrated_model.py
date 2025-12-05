#!/usr/bin/env python3
"""
μ/σ/ν統合評価・閾値最適化スクリプト

【目的】
μモデル + σ/νモデルを統合し、高確信度レースのみ選別してROIを最大化。

【戦略】
1. μモデルで各馬のスコアを予測
2. σモデルで馬の不確実性を予測（低い = 確信度高）
3. νモデルでレースの混沌度を予測（高い = 堅いレース）
4. σ < 閾値 AND ν > 閾値 のレースのみ選別
5. 選別後のROIを評価

【日本語】
すべてのログ、コメントは日本語で記述。
"""

import sys
from pathlib import Path

project_root = Path(__file__).resolve().parent.parent.parent
sys.path.append(str(project_root))

import pandas as pd
import numpy as np
import pickle
import json
import logging
import optuna
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('evaluate_integrated.log', encoding='utf-8')
    ]
)


class IntegratedModelEvaluator:
    """μ/σ/ν統合モデル評価クラス"""
    
    def __init__(self):
        self.models_dir = Path('keibaai/models')
        self.mu_model = None
        self.sigma_model = None
        self.nu_model = None
        
        self.mu_features = None
        self.sigma_features = None
        self.nu_features = None
    
    def load_models(self):
        """全モデルを読み込み"""
        logging.info("モデルを読み込み中...")
        
        # μモデル
        with open(self.models_dir / 'mu_v3_3/mu_v3_3_ranker.pkl', 'rb') as f:
            self.mu_model = pickle.load(f)
        with open(self.models_dir / 'mu_v3_3/feature_names.json', 'r', encoding='utf-8') as f:
            self.mu_features = json.load(f)
        
        # σモデル
        with open(self.models_dir / 'sigma_v1/sigma_model.pkl', 'rb') as f:
            self.sigma_model = pickle.load(f)
        with open(self.models_dir / 'sigma_v1/sigma_features.json', 'r', encoding='utf-8') as f:
            self.sigma_features = json.load(f)
        
        # νモデル
        with open(self.models_dir / 'nu_v1/nu_model.pkl', 'rb') as f:
            self.nu_model = pickle.load(f)
        with open(self.models_dir / 'nu_v1/nu_features.json', 'r', encoding='utf-8') as f:
            self.nu_features = json.load(f)
        
        logging.info("全モデル読み込み完了")
    
    def load_data(self):
        """評価用データを読み込み"""
        logging.info("評価データを読み込み中...")
        
        df = pd.read_parquet(self.models_dir / 'mu_v3_3/train_data_mu_v3_3.parquet')
        df['race_date'] = pd.to_datetime(df['race_date'])
        
        logging.info(f"データ読み込み完了: {len(df):,} rows")
        return df
    
    def predict_all(self, df: pd.DataFrame) -> pd.DataFrame:
        """全モデルで予測"""
        logging.info("全モデルで予測中...")
        
        # gap_ability_popularity の動的生成
        ability_col = 'past_5_finish_position_mean'
        if ability_col in df.columns and 'popularity' in df.columns:
            df['ability_rank'] = df.groupby('race_id')[ability_col].rank(ascending=True, method='first')
            df['gap_ability_popularity'] = df['popularity'] - df['ability_rank']
        
        # 欠損特徴量を0で埋める
        for col in self.mu_features:
            if col not in df.columns:
                df[col] = 0
        
        # μ予測
        df['mu_pred'] = self.mu_model.predict(df[self.mu_features])
        df['rank_pred'] = df.groupby('race_id')['mu_pred'].rank(ascending=False, method='first')
        
        # σ予測
        sigma_features_available = [f for f in self.sigma_features if f in df.columns]
        df_sigma = df[sigma_features_available].fillna(0)
        df['sigma_pred'] = np.exp(self.sigma_model.predict(df_sigma))
        
        # レース単位でσの平均を計算（レース選別用）
        df['race_sigma_mean'] = df.groupby('race_id')['sigma_pred'].transform('mean')
        
        # ν予測（レース単位）
        # レース特徴量を準備
        if 'head_count' not in df.columns:
            df['head_count'] = df.groupby('race_id')['horse_id'].transform('count')
        
        if 'track_surface' in df.columns:
            df['track_surface_turf'] = (df['track_surface'] == '芝').astype(int)
            df['track_surface_dirt'] = (df['track_surface'] == 'ダート').astype(int)
        
        if 'track_condition' in df.columns:
            df['track_condition_good'] = df['track_condition'].isin(['良']).astype(int)
            df['track_condition_yielding'] = df['track_condition'].isin(['稍重']).astype(int)
            df['track_condition_soft'] = df['track_condition'].isin(['重']).astype(int)
            df['track_condition_heavy'] = df['track_condition'].isin(['不良']).astype(int)
        
        if 'race_class' in df.columns:
            class_order = {
                'G1': 10, 'G2': 9, 'G3': 8, 'オープン': 7, 'OP': 7,
                'L': 6, '3勝クラス': 5, '2勝クラス': 4, '1勝クラス': 3,
                '未勝利': 2, '新馬': 1
            }
            df['race_class_encoded'] = df['race_class'].map(
                lambda x: next((v for k, v in class_order.items() if k in str(x)), 2)
            )
        
        if 'venue' in df.columns:
            venue_map = {
                '東京': 1, '中山': 2, '阪神': 3, '京都': 4,
                '中京': 5, '小倉': 6, '新潟': 7, '福島': 8,
                '札幌': 9, '函館': 10
            }
            df['venue_encoded'] = df['venue'].map(venue_map).fillna(0)
        
        # レース単位でν予測
        race_df = df.drop_duplicates(subset=['race_id'])[['race_id'] + self.nu_features].copy()
        for feat in self.nu_features:
            race_df[feat] = race_df[feat].fillna(0)
        
        race_df['nu_pred'] = self.nu_model.predict(race_df[self.nu_features])
        
        df = df.merge(race_df[['race_id', 'nu_pred']], on='race_id', how='left')
        
        logging.info("全予測完了")
        return df
    
    def calculate_roi(self, df: pd.DataFrame) -> dict:
        """ROIを計算"""
        # Top1に賭ける
        top1 = df[df['rank_pred'] == 1]
        hits = top1[top1['finish_position'] == 1]
        
        if len(top1) == 0:
            return {'roi': 0.0, 'bet_count': 0, 'hit_count': 0, 'accuracy': 0.0}
        
        roi = hits['win_odds'].sum() / len(top1)
        accuracy = len(hits) / len(top1)
        
        return {
            'roi': roi,
            'bet_count': len(top1),
            'hit_count': len(hits),
            'accuracy': accuracy
        }
    
    def evaluate_with_threshold(self, df: pd.DataFrame, sigma_threshold: float, nu_threshold: float) -> dict:
        """閾値を適用して評価"""
        # 選別条件
        # σが低い（確信度が高い）AND νが高い（堅いレース）
        selected_races = df[
            (df['race_sigma_mean'] < sigma_threshold) &
            (df['nu_pred'] > nu_threshold)
        ]['race_id'].unique()
        
        selected_df = df[df['race_id'].isin(selected_races)]
        
        if len(selected_df) == 0:
            return {
                'roi': 0.0,
                'bet_count': 0,
                'total_races': df['race_id'].nunique(),
                'selected_races': 0,
                'selection_rate': 0.0
            }
        
        result = self.calculate_roi(selected_df)
        result['total_races'] = df['race_id'].nunique()
        result['selected_races'] = len(selected_races)
        result['selection_rate'] = len(selected_races) / df['race_id'].nunique()
        
        return result
    
    def optimize_thresholds(self, valid_df: pd.DataFrame, n_trials: int = 100) -> dict:
        """Optunaで閾値を最適化"""
        logging.info("=" * 50)
        logging.info("閾値最適化開始")
        logging.info("=" * 50)
        
        def objective(trial):
            sigma_t = trial.suggest_float('sigma_threshold', 0.5, 3.0)
            nu_t = trial.suggest_float('nu_threshold', 3.0, 15.0)
            
            result = self.evaluate_with_threshold(valid_df, sigma_t, nu_t)
            
            # 選別率が低すぎる/高すぎる場合はペナルティ
            if result['selection_rate'] < 0.2 or result['selection_rate'] > 0.8:
                return 0.0
            
            return result['roi']
        
        study = optuna.create_study(direction='maximize')
        study.optimize(objective, n_trials=n_trials, show_progress_bar=True)
        
        best_params = study.best_params
        best_roi = study.best_value
        
        logging.info(f"最適閾値: sigma={best_params['sigma_threshold']:.3f}, nu={best_params['nu_threshold']:.3f}")
        logging.info(f"最適ROI: {best_roi:.2%}")
        
        return best_params
    
    def run_full_evaluation(self):
        """完全評価実行"""
        self.load_models()
        df = self.load_data()
        df = self.predict_all(df)
        
        # データ分割
        valid_mask = (df['race_date'] >= '2023-01-01') & (df['race_date'] < '2024-01-01')
        test_mask = df['race_date'] >= '2024-01-01'
        
        valid_df = df[valid_mask]
        test_df = df[test_mask]
        
        logging.info(f"Valid: {valid_df['race_id'].nunique():,} races")
        logging.info(f"Test: {test_df['race_id'].nunique():,} races")
        
        # ベースライン（全レース）
        logging.info("=" * 50)
        logging.info("ベースライン評価（選別なし）")
        logging.info("=" * 50)
        
        baseline_valid = self.calculate_roi(valid_df)
        baseline_test = self.calculate_roi(test_df)
        
        logging.info(f"Valid ROI（全レース）: {baseline_valid['roi']:.2%}")
        logging.info(f"Test ROI（全レース）: {baseline_test['roi']:.2%}")
        
        # 閾値最適化
        best_params = self.optimize_thresholds(valid_df, n_trials=50)
        
        # Test評価
        logging.info("=" * 50)
        logging.info("Test評価（選別後）")
        logging.info("=" * 50)
        
        test_result = self.evaluate_with_threshold(
            test_df,
            best_params['sigma_threshold'],
            best_params['nu_threshold']
        )
        
        logging.info(f"Test ROI（選別後）: {test_result['roi']:.2%}")
        logging.info(f"選別レース数: {test_result['selected_races']:,} / {test_result['total_races']:,}")
        logging.info(f"選別率: {test_result['selection_rate']:.1%}")
        logging.info(f"的中数: {test_result['hit_count']:,} / {test_result['bet_count']:,}")
        
        # 結果保存
        results = {
            'baseline': {
                'valid_roi': baseline_valid['roi'],
                'test_roi': baseline_test['roi']
            },
            'optimized': {
                'sigma_threshold': best_params['sigma_threshold'],
                'nu_threshold': best_params['nu_threshold'],
                'test_roi': test_result['roi'],
                'selection_rate': test_result['selection_rate'],
                'bet_count': test_result['bet_count'],
                'hit_count': test_result['hit_count']
            },
            'improvement': {
                'roi_diff': test_result['roi'] - baseline_test['roi'],
                'roi_ratio': test_result['roi'] / baseline_test['roi'] if baseline_test['roi'] > 0 else 0
            }
        }
        
        with open('keibaai/models/integrated_evaluation_results.json', 'w', encoding='utf-8') as f:
            json.dump(results, f, ensure_ascii=False, indent=2)
        
        logging.info("=" * 50)
        logging.info("統合評価完了")
        logging.info("=" * 50)
        
        return results


if __name__ == "__main__":
    evaluator = IntegratedModelEvaluator()
    results = evaluator.run_full_evaluation()
