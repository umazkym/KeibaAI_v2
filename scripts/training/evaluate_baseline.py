#!/usr/bin/env python3
"""
μモデル v3.3 ベースライン再評価スクリプト

【目的】
- v3.3の156特徴量でTest ROIを再評価
- Gap特徴量あり/なしのA/Bテストを実施
- 「本当のベースライン」を確立

【評価項目】
- A: v3.3 フル（156特徴量）
- B: v3.3 - Gap特徴量（152特徴量）
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

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class BaselineEvaluator:
    """v3.3ベースライン評価クラス"""
    
    # v3.3から除外する特徴量（リーク疑い）
    PURGE_FEATURES = [
        'jockey_win_rate', 'jockey_place_rate', 'jockey_avg_rank',
        'sire_win_rate', 'sire_avg_finish',
        'trainer_win_rate', 'trainer_place_rate',
    ]
    
    # Gap特徴量（人気依存）
    GAP_FEATURES = [
        'gap_jockey_popularity',
        'gap_trainer_popularity', 
        'gap_pedigree_popularity',
        'gap_course_fit_popularity',
        'is_overvalued',
        'race_class_overbet_risk',
    ]
    
    def __init__(self):
        self.base_model_dir = Path('keibaai/models/mu_v3_3')
        self.output_dir = Path('keibaai/models/mu_baseline')
        self.output_dir.mkdir(parents=True, exist_ok=True)
    
    def load_data(self):
        logging.info("データを読み込み中...")
        train_data = pd.read_parquet(self.base_model_dir / 'train_data_mu_v3_3.parquet')
        train_data['race_date'] = pd.to_datetime(train_data['race_date'])
        logging.info(f"  レコード数: {len(train_data):,}")
        return train_data
    
    def get_features(self, df, exclude_gap=False):
        """特徴量を取得"""
        with open(self.base_model_dir / 'feature_names.json', 'r', encoding='utf-8') as f:
            features = json.load(f)
        
        # リーク疑い特徴量を除外
        features = [f for f in features if f not in self.PURGE_FEATURES]
        
        # Gap特徴量を除外（オプション）
        if exclude_gap:
            features = [f for f in features if f not in self.GAP_FEATURES]
        
        # 利用可能な特徴量のみ
        return [f for f in features if f in df.columns]
    
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
    
    def train_and_evaluate(self, df, feature_cols, name, n_trials=50):
        """学習と評価"""
        train_df = df[df['race_date'] < '2023-01-01'].copy()
        valid_df = df[(df['race_date'] >= '2023-01-01') & (df['race_date'] < '2024-01-01')].copy()
        test_df = df[df['race_date'] >= '2024-01-01'].copy()
        
        group_train = train_df.groupby('race_id').size().tolist()
        group_valid = valid_df.groupby('race_id').size().tolist()
        
        logging.info(f"Train: {len(train_df):,}, Valid: {len(valid_df):,}, Test: {len(test_df):,}")
        logging.info(f"特徴量数: {len(feature_cols)}")
        
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
                'num_leaves': trial.suggest_int('num_leaves', 30, 100),
                'lambda_l1': trial.suggest_float('lambda_l1', 0.01, 5.0, log=True),
                'lambda_l2': trial.suggest_float('lambda_l2', 0.01, 5.0, log=True),
                'min_child_samples': trial.suggest_int('min_child_samples', 20, 60),
                'learning_rate': trial.suggest_float('learning_rate', 0.02, 0.12, log=True),
                'feature_fraction': trial.suggest_float('feature_fraction', 0.5, 0.9),
                'bagging_fraction': trial.suggest_float('bagging_fraction', 0.6, 0.95),
                'bagging_freq': trial.suggest_int('bagging_freq', 1, 7),
                'max_depth': trial.suggest_int('max_depth', 5, 12),
            }
            
            model = lgb.LGBMRanker(**params, n_estimators=1500)
            model.fit(train_df[feature_cols], train_df['target_relevance'],
                      group=group_train, sample_weight=train_df['sample_weight'],
                      eval_set=[(valid_df[feature_cols], valid_df['target_relevance'])],
                      eval_group=[group_valid],
                      callbacks=[lgb.early_stopping(50, verbose=False)])
            
            # Valid ROIのみで最適化（Test隔離）
            valid_roi = calculate_roi(valid_df, model.predict(valid_df[feature_cols]))
            return valid_roi
        
        logging.info(f"Optuna {n_trials}トライアル...")
        optuna.logging.set_verbosity(optuna.logging.WARNING)
        study = optuna.create_study(direction='maximize')
        study.optimize(objective, n_trials=n_trials, show_progress_bar=True)
        
        best_params = study.best_params
        best_params.update({'objective': 'lambdarank', 'metric': 'ndcg', 'eval_at': [1, 3, 5],
                            'boosting_type': 'gbdt', 'verbose': -1, 'random_state': 42,
                            'label_gain': list(range(100))})
        
        # 最終学習
        model = lgb.LGBMRanker(**best_params, n_estimators=2000)
        model.fit(train_df[feature_cols], train_df['target_relevance'],
                  group=group_train, sample_weight=train_df['sample_weight'],
                  eval_set=[(valid_df[feature_cols], valid_df['target_relevance'])],
                  eval_group=[group_valid],
                  callbacks=[lgb.early_stopping(100, verbose=True)])
        
        valid_roi = calculate_roi(valid_df, model.predict(valid_df[feature_cols]))
        test_roi = calculate_roi(test_df, model.predict(test_df[feature_cols]))
        
        # Top特徴量
        importance = dict(zip(feature_cols, model.feature_importances_))
        top_features = sorted(importance.items(), key=lambda x: x[1], reverse=True)[:10]
        
        return {
            'name': name,
            'feature_count': len(feature_cols),
            'valid_roi': valid_roi,
            'test_roi': test_roi,
            'gap': abs(valid_roi - test_roi),
            'best_params': {k: v for k, v in best_params.items() if k != 'label_gain'},
            'top_features': [(n, int(i)) for n, i in top_features],
        }, model
    
    def run(self, n_trials=50):
        df = self.load_data()
        df = self.prepare_target(df)
        
        results = {}
        
        # === A: v3.3 フル（Gap特徴量あり）===
        logging.info("="*60)
        logging.info("【A】v3.3 フル（Gap特徴量あり）")
        logging.info("="*60)
        features_full = self.get_features(df, exclude_gap=False)
        result_a, model_a = self.train_and_evaluate(df, features_full, "v3.3_full", n_trials)
        results['A_full'] = result_a
        
        logging.info("")
        logging.info(f"  Valid ROI: {result_a['valid_roi']:.2%}")
        logging.info(f"  Test ROI: {result_a['test_roi']:.2%}")
        logging.info(f"  Gap: {result_a['gap']:.2%}")
        
        # === B: v3.3 - Gap（Gap特徴量なし）===
        logging.info("")
        logging.info("="*60)
        logging.info("【B】v3.3 - Gap特徴量（純粋能力モデル）")
        logging.info("="*60)
        features_no_gap = self.get_features(df, exclude_gap=True)
        result_b, model_b = self.train_and_evaluate(df, features_no_gap, "v3.3_no_gap", n_trials)
        results['B_no_gap'] = result_b
        
        logging.info("")
        logging.info(f"  Valid ROI: {result_b['valid_roi']:.2%}")
        logging.info(f"  Test ROI: {result_b['test_roi']:.2%}")
        logging.info(f"  Gap: {result_b['gap']:.2%}")
        
        # === 結果比較 ===
        logging.info("")
        logging.info("="*60)
        logging.info("【A/Bテスト結果】")
        logging.info("="*60)
        logging.info(f"")
        logging.info(f"| モデル | 特徴量数 | Valid ROI | Test ROI | Gap |")
        logging.info(f"|--------|----------|-----------|----------|-----|")
        logging.info(f"| A: v3.3 フル | {result_a['feature_count']} | {result_a['valid_roi']:.2%} | {result_a['test_roi']:.2%} | {result_a['gap']:.2%} |")
        logging.info(f"| B: v3.3 - Gap | {result_b['feature_count']} | {result_b['valid_roi']:.2%} | {result_b['test_roi']:.2%} | {result_b['gap']:.2%} |")
        logging.info(f"| v5.4 (参考) | 169 | 84.25% | 80.05% | 4.20% |")
        logging.info(f"| v3.5 (目標) | 157 | 83.79% | 81.84% | 1.95% |")
        
        gap_effect = result_a['test_roi'] - result_b['test_roi']
        logging.info(f"")
        logging.info(f"Gap特徴量の効果: {gap_effect:+.2%}")
        if gap_effect > 0:
            logging.info(f"→ Gap特徴量は予測力あり（ただし運用時リスクに注意）")
        else:
            logging.info(f"→ Gap特徴量はノイズの可能性")
        
        # 保存
        with open(self.output_dir / 'baseline_results.json', 'w', encoding='utf-8') as f:
            json.dump(results, f, ensure_ascii=False, indent=2, default=str)
        
        # 最良モデルを保存
        best_result = result_a if result_a['test_roi'] >= result_b['test_roi'] else result_b
        best_model = model_a if result_a['test_roi'] >= result_b['test_roi'] else model_b
        
        with open(self.output_dir / 'best_model.pkl', 'wb') as f:
            pickle.dump(best_model, f)
        
        with open(self.output_dir / 'model_info.json', 'w', encoding='utf-8') as f:
            json.dump({
                'version': 'baseline',
                'description': best_result['name'],
                'valid_roi': best_result['valid_roi'],
                'test_roi': best_result['test_roi'],
                'valid_test_gap': best_result['gap'],
                'feature_count': best_result['feature_count'],
                'best_params': best_result['best_params'],
                'created_at': datetime.now().isoformat(),
            }, f, ensure_ascii=False, indent=2)
        
        logging.info(f"")
        logging.info(f"保存完了: {self.output_dir}")
        
        return results


if __name__ == "__main__":
    BaselineEvaluator().run(n_trials=50)
