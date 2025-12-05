#!/usr/bin/env python3
"""
μモデル v3.6 - オッズ重み強化 + ターゲット再設計版

【改善ポイント】
1. オッズ重みの強化（高配当馬の学習重視）
2. ターゲット変数の再設計（EV直接最適化）
3. 外れ値耐性の向上

【投資機会】
全レース投資を維持（フィルタリングなし）

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
import optuna
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)


class MuV36Trainer:
    """μモデル v3.6 学習クラス（オッズ重み強化版）"""
    
    def __init__(self):
        self.base_model_dir = Path('keibaai/models/mu_v3_3')
        self.output_dir = Path('keibaai/models/mu_v3_6')
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        self.model = None
        self.feature_cols = None
    
    def load_data(self):
        """データ読み込み"""
        logging.info("データを読み込み中...")
        df = pd.read_parquet(self.base_model_dir / 'train_data_mu_v3_3.parquet')
        df['race_date'] = pd.to_datetime(df['race_date'])
        logging.info(f"データ読み込み完了: {len(df):,} rows")
        return df
    
    def add_extended_gap_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Gap Feature拡張（v3.5と同じ）"""
        logging.info("Gap Featuresを拡張中...")
        
        ability_col = 'past_5_finish_position_mean'
        if ability_col in df.columns and 'popularity' in df.columns:
            df['ability_rank'] = df.groupby('race_id')[ability_col].rank(ascending=True, method='first')
            df['gap_ability_popularity'] = df['popularity'] - df['ability_rank']
        
        if 'jockey_win_rate' in df.columns:
            df['jockey_rank'] = df.groupby('race_id')['jockey_win_rate'].rank(ascending=False, method='first')
            df['gap_jockey_popularity'] = df['popularity'] - df['jockey_rank']
        
        if 'trainer_win_rate' in df.columns:
            df['trainer_rank'] = df.groupby('race_id')['trainer_win_rate'].rank(ascending=False, method='first')
            df['gap_trainer_popularity'] = df['popularity'] - df['trainer_rank']
        
        if 'sire_win_rate' in df.columns:
            df['pedigree_rank'] = df.groupby('race_id')['sire_win_rate'].rank(ascending=False, method='first')
            df['gap_pedigree_popularity'] = df['popularity'] - df['pedigree_rank']
        
        if 'sire_course_win_rate' in df.columns:
            df['course_fit_rank'] = df.groupby('race_id')['sire_course_win_rate'].rank(ascending=False, method='first')
            df['gap_course_fit_popularity'] = df['popularity'] - df['course_fit_rank']
        
        if 'horse_weight_zscore' in df.columns:
            df['weight_rank'] = df.groupby('race_id')['horse_weight_zscore'].rank(ascending=False, method='first')
            df['gap_weight_popularity'] = df['popularity'] - df['weight_rank']
        
        if 'pace_fit_score' in df.columns:
            df['pace_rank'] = df.groupby('race_id')['pace_fit_score'].rank(ascending=False, method='first')
            df['gap_pace_popularity'] = df['popularity'] - df['pace_rank']
        
        if 'combo_avg_finish' in df.columns:
            df['combo_rank'] = df.groupby('race_id')['combo_avg_finish'].rank(ascending=True, method='first')
            df['gap_combo_popularity'] = df['popularity'] - df['combo_rank']
        
        if 'form_rank' in df.columns:
            df['form_rank_in_race'] = df.groupby('race_id')['form_rank'].rank(ascending=False, method='first')
            df['gap_form_popularity'] = df['popularity'] - df['form_rank_in_race']
        
        logging.info("Gap Features 拡張完了")
        return df
    
    def prepare_features(self, df: pd.DataFrame) -> list:
        """特徴量を準備"""
        with open(self.base_model_dir / 'feature_names.json', 'r', encoding='utf-8') as f:
            base_features = json.load(f)
        
        new_gap_features = [
            'gap_weight_popularity',
            'gap_pace_popularity',
            'gap_combo_popularity',
            'gap_form_popularity',
        ]
        
        all_features = base_features + new_gap_features
        available_features = [f for f in all_features if f in df.columns]
        
        logging.info(f"特徴量数: {len(available_features)}")
        return available_features
    
    def prepare_target_ev_optimized(self, df: pd.DataFrame):
        """ターゲット変数を準備（EV最適化版）"""
        logging.info("ターゲット変数を準備中（EV最適化版）...")
        
        # 【改善1】EV直接最適化
        # ターゲット = 着順に応じた報酬 × オッズ
        odds = df['win_odds'].fillna(1.0).clip(upper=100)
        
        # 着順報酬
        reward = np.zeros(len(df))
        reward[df['finish_position'] == 1] = 1.0   # 1着
        reward[df['finish_position'] == 2] = 0.3   # 2着
        reward[df['finish_position'] == 3] = 0.1   # 3着
        
        # 【改善2】オッズ × 報酬 = 期待値的なスコア
        df['target_gain'] = reward * np.sqrt(odds)  # sqrt でスケーリング
        df['target_relevance'] = (df['target_gain'] * 10).astype(int).clip(0, 99)
        
        # 【改善3】オッズ重み強化（高配当馬を重視）
        # 通常: log1p(odds)
        # 強化版: odds^0.3 で高配当を更に重視
        df['sample_weight'] = np.power(odds.clip(lower=1.5), 0.4)
        
        # 極端な値をクリップ
        df['sample_weight'] = df['sample_weight'].clip(upper=10)
        
        logging.info(f"ターゲット分布: mean={df['target_relevance'].mean():.2f}, max={df['target_relevance'].max()}")
        
        return df
    
    def train(self, df: pd.DataFrame, feature_cols: list, n_trials: int = 40):
        """モデル学習"""
        logging.info("=" * 60)
        logging.info("μモデル v3.6 学習開始（オッズ重み強化版）")
        logging.info("=" * 60)
        
        # 時系列分割
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
        
        def objective(trial):
            params = {
                'objective': 'lambdarank',
                'metric': 'ndcg',
                'eval_at': [1, 3, 5],
                'boosting_type': 'gbdt',
                'num_leaves': trial.suggest_int('num_leaves', 31, 150),
                'lambda_l1': trial.suggest_float('lambda_l1', 0.01, 20.0, log=True),
                'lambda_l2': trial.suggest_float('lambda_l2', 0.01, 20.0, log=True),
                'min_child_samples': trial.suggest_int('min_child_samples', 30, 300),
                'learning_rate': trial.suggest_float('learning_rate', 0.005, 0.15, log=True),
                'feature_fraction': trial.suggest_float('feature_fraction', 0.4, 0.9),
                'bagging_fraction': trial.suggest_float('bagging_fraction', 0.5, 1.0),
                'bagging_freq': trial.suggest_int('bagging_freq', 1, 7),
                'max_depth': trial.suggest_int('max_depth', 4, 12),
                'verbose': -1,
                'random_state': 42,
                'label_gain': list(range(100))
            }
            
            model = lgb.LGBMRanker(**params, n_estimators=1500)
            model.fit(
                train_df[feature_cols], train_df['target_relevance'],
                group=group_train, sample_weight=train_df['sample_weight'],
                eval_set=[(valid_df[feature_cols], valid_df['target_relevance'])],
                eval_group=[group_valid], eval_sample_weight=[valid_df['sample_weight']],
                eval_metric='ndcg',
                callbacks=[lgb.early_stopping(stopping_rounds=50, verbose=False)]
            )
            
            preds = model.predict(valid_df[feature_cols])
            roi = calculate_roi(valid_df, preds)
            
            return roi
        
        logging.info(f"Optuna最適化開始（{n_trials}トライアル）...")
        study = optuna.create_study(direction='maximize')
        study.optimize(objective, n_trials=n_trials, show_progress_bar=True)
        
        logging.info(f"最適Valid ROI: {study.best_value:.2%}")
        
        best_params = study.best_params
        best_params.update({
            'objective': 'lambdarank',
            'metric': 'ndcg',
            'eval_at': [1, 3, 5],
            'boosting_type': 'gbdt',
            'verbose': -1,
            'random_state': 42,
            'label_gain': list(range(100))
        })
        
        self.model = lgb.LGBMRanker(**best_params, n_estimators=2000)
        self.model.fit(
            train_df[feature_cols], train_df['target_relevance'],
            group=group_train, sample_weight=train_df['sample_weight'],
            eval_set=[(valid_df[feature_cols], valid_df['target_relevance'])],
            eval_group=[group_valid], eval_sample_weight=[valid_df['sample_weight']],
            eval_metric='ndcg',
            callbacks=[lgb.early_stopping(stopping_rounds=100, verbose=True)]
        )
        
        # Test評価
        test_preds = self.model.predict(test_df[feature_cols])
        test_roi = calculate_roi(test_df, test_preds)
        
        # Top 1/5精度
        test_df = test_df.copy()
        test_df['score'] = test_preds
        test_df['rank_pred'] = test_df.groupby('race_id')['score'].rank(ascending=False, method='first')
        top1_acc = (test_df[test_df['rank_pred'] == 1]['finish_position'] == 1).mean()
        top5_recall = test_df[test_df['rank_pred'] <= 5].groupby('race_id').apply(
            lambda x: (x['finish_position'] == 1).any()
        ).mean()
        
        logging.info("=" * 60)
        logging.info(f"【最終結果: μ v3.6（オッズ重み強化版）】")
        logging.info(f"  Test ROI: {test_roi:.2%}")
        logging.info(f"  Top 1 Accuracy: {top1_acc:.2%}")
        logging.info(f"  Top 5 Recall: {top5_recall:.2%}")
        logging.info(f"  レース数: {test_df['race_id'].nunique():,}（全レース投資）")
        logging.info("=" * 60)
        
        self.save_model(feature_cols, test_roi, top1_acc, top5_recall, best_params)
        
        return test_roi
    
    def save_model(self, features: list, test_roi: float, top1_acc: float, top5_recall: float, best_params: dict):
        """モデル保存"""
        with open(self.output_dir / 'mu_v3_6_ranker.pkl', 'wb') as f:
            pickle.dump(self.model, f)
        
        with open(self.output_dir / 'feature_names.json', 'w', encoding='utf-8') as f:
            json.dump(features, f, ensure_ascii=False, indent=2)
        
        save_params = {k: v for k, v in best_params.items() if k != 'label_gain'}
        metadata = {
            'version': 'v3.6',
            'description': 'オッズ重み強化 + EV最適化版',
            'test_roi': test_roi,
            'top1_accuracy': top1_acc,
            'top5_recall': top5_recall,
            'feature_count': len(features),
            'best_params': save_params,
            'created_at': datetime.now().isoformat()
        }
        
        with open(self.output_dir / 'model_info.json', 'w', encoding='utf-8') as f:
            json.dump(metadata, f, ensure_ascii=False, indent=2)
        
        logging.info(f"モデル保存完了: {self.output_dir}")
    
    def run(self, n_trials: int = 40):
        """メイン実行"""
        df = self.load_data()
        df = self.add_extended_gap_features(df)
        df = self.prepare_target_ev_optimized(df)
        feature_cols = self.prepare_features(df)
        self.feature_cols = feature_cols
        test_roi = self.train(df, feature_cols, n_trials=n_trials)
        
        return test_roi


if __name__ == "__main__":
    trainer = MuV36Trainer()
    trainer.run(n_trials=40)
