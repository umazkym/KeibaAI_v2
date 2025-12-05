#!/usr/bin/env python3
"""
μモデル v3.7 - 1着重視ターゲット版

【目的】
ターゲット変数の重み付けを1着に特化することでROI向上を目指す。

【変更点】
- 1着の重み: 12.74 → 20.0（大幅増加）
- 2着の重み: 6.73 → 2.0（大幅減少）
- 3着の重み: 3.69 → 0.5（最小化）

【理論的根拠】
単勝馬券では1着のみが利益を生むため、モデルは1着と2着を
より明確に区別できるようにする必要がある。

【リスク評価】
- データリーク: なし（オッズは予測時利用可能）
- 過学習リスク: 低（Valid-Testで検証可能）
- 投資機会: 維持（全レース投資）
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


class MuV37Trainer:
    """μモデル v3.7 学習クラス（1着重視ターゲット版）"""
    
    def __init__(self):
        self.base_model_dir = Path('keibaai/models/mu_v3_3')
        self.output_dir = Path('keibaai/models/mu_v3_7')
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        self.model = None
        self.feature_cols = None
    
    def load_data(self):
        """データ読み込み"""
        logging.info("データを読み込み中...")
        
        v33_data_path = self.base_model_dir / 'train_data_mu_v3_3.parquet'
        
        if not v33_data_path.exists():
            raise FileNotFoundError(f"学習データが見つかりません: {v33_data_path}")
        
        df = pd.read_parquet(v33_data_path)
        df['race_date'] = pd.to_datetime(df['race_date'])
        logging.info(f"データ読み込み完了: {len(df):,} rows")
        
        return df
    
    def prepare_features(self, df: pd.DataFrame) -> list:
        """特徴量を準備（v3.3と同じ）"""
        with open(self.base_model_dir / 'feature_names.json', 'r', encoding='utf-8') as f:
            features = json.load(f)
        
        available_features = [f for f in features if f in df.columns]
        logging.info(f"特徴量数: {len(available_features)}")
        
        return available_features
    
    def prepare_target_v37(self, df: pd.DataFrame):
        """
        ターゲット変数を準備（v3.7: 1着重視版）
        
        【変更点】
        - 1着: 20.0（v3.5: 12.74）
        - 2着: 2.0（v3.5: 6.73）
        - 3着: 0.5（v3.5: 3.69）
        """
        logging.info("=" * 60)
        logging.info("【v3.7】1着重視ターゲット変数を生成中...")
        logging.info("=" * 60)
        
        # 新しい重み付け
        weight_1st = 20.0   # 大幅増加
        weight_2nd = 2.0    # 大幅減少
        weight_3rd = 0.5    # 最小化
        
        logging.info(f"  1着の重み: {weight_1st}（v3.5: 12.74）")
        logging.info(f"  2着の重み: {weight_2nd}（v3.5: 6.73）")
        logging.info(f"  3着の重み: {weight_3rd}（v3.5: 3.69）")
        
        odds_clip = 90
        
        odds = df['win_odds'].fillna(1.0)
        predictable_mask = odds <= 100.0
        odds_clipped = odds.clip(upper=odds_clip)
        log_odds = np.log1p(odds_clipped)
        
        gain = np.zeros(len(df))
        gain[predictable_mask & (df['finish_position'] == 1)] = log_odds[predictable_mask & (df['finish_position'] == 1)] * weight_1st
        gain[predictable_mask & (df['finish_position'] == 2)] = log_odds[predictable_mask & (df['finish_position'] == 2)] * weight_2nd
        gain[predictable_mask & (df['finish_position'] == 3)] = log_odds[predictable_mask & (df['finish_position'] == 3)] * weight_3rd
        
        df['target_gain'] = gain
        df['target_relevance'] = df['target_gain'].astype(int)
        df['sample_weight'] = np.log1p(df['win_odds'].fillna(1.0)).clip(upper=np.log1p(100))
        
        # 分布の確認
        logging.info(f"  ターゲット分布: mean={df['target_gain'].mean():.2f}, std={df['target_gain'].std():.2f}")
        
        return df
    
    def train(self, df: pd.DataFrame, feature_cols: list, n_trials: int = 30):
        """モデル学習（Optuna最適化）"""
        logging.info("=" * 60)
        logging.info("μモデル v3.7 学習開始（1着重視ターゲット版）")
        logging.info("=" * 60)
        
        # 時系列分割
        train_mask = df['race_date'] < '2023-01-01'
        valid_mask = (df['race_date'] >= '2023-01-01') & (df['race_date'] < '2024-01-01')
        test_mask = df['race_date'] >= '2024-01-01'
        
        train_df = df[train_mask].copy()
        valid_df = df[valid_mask].copy()
        test_df = df[test_mask].copy()
        
        # グループ
        group_train = train_df.groupby('race_id').size().to_list()
        group_valid = valid_df.groupby('race_id').size().to_list()
        
        logging.info(f"Train: {len(train_df):,}, Valid: {len(valid_df):,}, Test: {len(test_df):,}")
        
        # ROI計算関数
        def calculate_roi(d, preds):
            d = d.copy()
            d['score'] = preds
            d['rank_pred'] = d.groupby('race_id')['score'].rank(ascending=False, method='first')
            bet_df = d[d['rank_pred'] == 1]
            hits = bet_df[bet_df['finish_position'] == 1]
            return hits['win_odds'].sum() / len(bet_df) if len(bet_df) > 0 else 0
        
        # Optuna最適化
        def objective(trial):
            params = {
                'objective': 'lambdarank',
                'metric': 'ndcg',
                'eval_at': [1, 3, 5],
                'boosting_type': 'gbdt',
                'num_leaves': trial.suggest_int('num_leaves', 31, 127),
                'lambda_l1': trial.suggest_float('lambda_l1', 0.1, 10.0, log=True),
                'lambda_l2': trial.suggest_float('lambda_l2', 0.1, 10.0, log=True),
                'min_child_samples': trial.suggest_int('min_child_samples', 50, 300),
                'learning_rate': trial.suggest_float('learning_rate', 0.01, 0.2, log=True),
                'feature_fraction': trial.suggest_float('feature_fraction', 0.5, 0.9),
                'bagging_fraction': trial.suggest_float('bagging_fraction', 0.6, 1.0),
                'bagging_freq': trial.suggest_int('bagging_freq', 1, 7),
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
        
        # 最終モデル学習
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
        
        # 評価
        valid_preds = self.model.predict(valid_df[feature_cols])
        valid_roi = calculate_roi(valid_df, valid_preds)
        
        test_preds = self.model.predict(test_df[feature_cols])
        test_roi = calculate_roi(test_df, test_preds)
        
        # 詳細分析
        test_df['score'] = test_preds
        test_df['rank_pred'] = test_df.groupby('race_id')['score'].rank(ascending=False, method='first')
        bet_df = test_df[test_df['rank_pred'] == 1]
        
        # 月別ROI
        bet_df['month'] = pd.to_datetime(bet_df['race_date']).dt.to_period('M')
        monthly_roi = bet_df.groupby('month').apply(
            lambda g: g[g['finish_position'] == 1]['win_odds'].sum() / len(g) if len(g) > 0 else 0
        )
        
        logging.info("=" * 60)
        logging.info(f"【最終結果: μ v3.7（1着重視ターゲット版）】")
        logging.info("=" * 60)
        logging.info(f"  Valid ROI: {valid_roi:.2%}")
        logging.info(f"  Test ROI: {test_roi:.2%}")
        logging.info(f"  Valid-Test差: {abs(valid_roi - test_roi):.2%}")
        logging.info(f"  テストレース数: {test_df['race_id'].nunique():,}")
        logging.info("")
        logging.info("【月別Test ROI】")
        for month, roi in monthly_roi.items():
            logging.info(f"  {month}: {roi:.2%}")
        logging.info(f"  月別ROI標準偏差: {monthly_roi.std():.2%}")
        
        # v3.5との比較
        v35_test_roi = 0.8184
        improvement = test_roi - v35_test_roi
        logging.info("")
        logging.info(f"【v3.5との比較】")
        logging.info(f"  v3.5 Test ROI: {v35_test_roi:.2%}")
        logging.info(f"  v3.7 Test ROI: {test_roi:.2%}")
        logging.info(f"  改善幅: {improvement:+.2%}")
        logging.info("=" * 60)
        
        # 保存
        self.save_model(feature_cols, valid_roi, test_roi, best_params, monthly_roi)
        
        return valid_roi, test_roi
    
    def save_model(self, features: list, valid_roi: float, test_roi: float, 
                   best_params: dict, monthly_roi: pd.Series):
        """モデル保存"""
        with open(self.output_dir / 'mu_v3_7_ranker.pkl', 'wb') as f:
            pickle.dump(self.model, f)
        
        with open(self.output_dir / 'feature_names.json', 'w', encoding='utf-8') as f:
            json.dump(features, f, ensure_ascii=False, indent=2)
        
        save_params = {k: v for k, v in best_params.items() if k != 'label_gain'}
        metadata = {
            'version': 'v3.7',
            'description': '1着重視ターゲット版',
            'target_weights': {
                '1st': 20.0,
                '2nd': 2.0,
                '3rd': 0.5
            },
            'valid_roi': valid_roi,
            'test_roi': test_roi,
            'monthly_roi_std': float(monthly_roi.std()),
            'feature_count': len(features),
            'best_params': save_params,
            'created_at': datetime.now().isoformat()
        }
        
        with open(self.output_dir / 'model_info.json', 'w', encoding='utf-8') as f:
            json.dump(metadata, f, ensure_ascii=False, indent=2)
        
        logging.info(f"モデル保存完了: {self.output_dir}")
    
    def run(self, n_trials: int = 30):
        """メイン実行"""
        df = self.load_data()
        df = self.prepare_target_v37(df)
        feature_cols = self.prepare_features(df)
        self.feature_cols = feature_cols
        valid_roi, test_roi = self.train(df, feature_cols, n_trials=n_trials)
        
        return valid_roi, test_roi


if __name__ == "__main__":
    trainer = MuV37Trainer()
    trainer.run(n_trials=30)
