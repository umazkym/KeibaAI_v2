#!/usr/bin/env python3
"""
μモデル v3.6 学習スクリプト（v3.5再現版）

【目的】
- v3.5（Test ROI 81.84%）を再現
- gap_ability_popularity等の追加Gap特徴量を実装
- min_child_samples=140で強正則化

【v3.5との差分】
- v3.3ベースに9特徴量を追加
- ハイパーパラメータをv3.5と同一に固定
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

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class MuV36Trainer:
    """μモデル v3.6 学習クラス（v3.5再現版）"""
    
    # v3.5のハイパーパラメータ（固定）
    V35_PARAMS = {
        'num_leaves': 113,
        'lambda_l1': 0.19044896042070755,
        'lambda_l2': 0.19378769391906547,
        'min_child_samples': 140,  # ★ 強正則化
        'learning_rate': 0.1138459373623942,
        'feature_fraction': 0.6361319286220131,
        'bagging_fraction': 0.7354821509424282,
        'bagging_freq': 4,
        'objective': 'lambdarank',
        'metric': 'ndcg',
        'eval_at': [1, 3, 5],
        'boosting_type': 'gbdt',
        'verbose': -1,
        'random_state': 42,
    }
    
    def __init__(self):
        self.base_model_dir = Path('keibaai/models/mu_v3_3')
        self.output_dir = Path('keibaai/models/mu_v3_6')
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.model = None
    
    def load_data(self):
        logging.info("データを読み込み中...")
        train_data = pd.read_parquet(self.base_model_dir / 'train_data_mu_v3_3.parquet')
        train_data['race_date'] = pd.to_datetime(train_data['race_date'])
        logging.info(f"  レコード数: {len(train_data):,}")
        return train_data
    
    def generate_gap_features_v35(self, df):
        """
        v3.5と完全に同じGap特徴量計算ロジック
        
        出典: scripts/training/train_mu_v3_5_gap_extended.py
        """
        logging.info("Gap Featuresを生成中（v3.5ロジック）...")
        
        # gap_ability_popularity（最重要）
        ability_col = 'past_5_finish_position_mean'
        if ability_col in df.columns and 'popularity' in df.columns:
            df['ability_rank'] = df.groupby('race_id')[ability_col].rank(ascending=True, method='first')
            df['gap_ability_popularity'] = df['popularity'] - df['ability_rank']
            logging.info(f"  gap_ability_popularity 生成完了")
        
        # gap_jockey_popularity
        if 'jockey_win_rate' in df.columns:
            df['jockey_rank'] = df.groupby('race_id')['jockey_win_rate'].rank(ascending=False, method='first')
            df['gap_jockey_popularity'] = df['popularity'] - df['jockey_rank']
        
        # gap_trainer_popularity
        if 'trainer_win_rate' in df.columns:
            df['trainer_rank'] = df.groupby('race_id')['trainer_win_rate'].rank(ascending=False, method='first')
            df['gap_trainer_popularity'] = df['popularity'] - df['trainer_rank']
        
        # gap_pedigree_popularity
        if 'sire_win_rate' in df.columns:
            df['pedigree_rank'] = df.groupby('race_id')['sire_win_rate'].rank(ascending=False, method='first')
            df['gap_pedigree_popularity'] = df['popularity'] - df['pedigree_rank']
        
        # gap_course_fit_popularity
        if 'sire_course_win_rate' in df.columns:
            df['course_fit_rank'] = df.groupby('race_id')['sire_course_win_rate'].rank(ascending=False, method='first')
            df['gap_course_fit_popularity'] = df['popularity'] - df['course_fit_rank']
        
        # gap_weight_popularity
        if 'horse_weight_zscore' in df.columns:
            df['weight_rank'] = df.groupby('race_id')['horse_weight_zscore'].rank(ascending=False, method='first')
            df['gap_weight_popularity'] = df['popularity'] - df['weight_rank']
        
        # gap_pace_popularity
        if 'pace_fit_score' in df.columns:
            df['pace_rank'] = df.groupby('race_id')['pace_fit_score'].rank(ascending=False, method='first')
            df['gap_pace_popularity'] = df['popularity'] - df['pace_rank']
        
        # gap_combo_popularity
        if 'combo_avg_finish' in df.columns:
            df['combo_rank'] = df.groupby('race_id')['combo_avg_finish'].rank(ascending=True, method='first')
            df['gap_combo_popularity'] = df['popularity'] - df['combo_rank']
        
        # gap_form_popularity
        if 'form_rank' in df.columns:
            df['form_rank_in_race'] = df.groupby('race_id')['form_rank'].rank(ascending=False, method='first')
            df['gap_form_popularity'] = df['popularity'] - df['form_rank_in_race']
        
        logging.info("Gap Features 生成完了")
        return df
    
    def get_features(self, df):
        """v3.5と同じ特徴量セット"""
        with open(self.base_model_dir / 'feature_names.json', 'r', encoding='utf-8') as f:
            base_features = json.load(f)
        
        # v3.5で追加された9特徴量
        v35_additions = [
            'gap_ability_popularity',
            'gap_weight_popularity',
            'gap_pace_popularity',
            'gap_combo_popularity',
            'gap_form_popularity',
            'jockey_win_rate',
            'trainer_win_rate',
            'sire_win_rate',
            'sire_avg_finish',
        ]
        
        all_features = base_features + v35_additions
        all_features = list(dict.fromkeys(all_features))  # 重複除去
        
        available = [f for f in all_features if f in df.columns]
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
    
    def train(self, df, feature_cols):
        logging.info("="*60)
        logging.info("μモデル v3.6 学習開始（v3.5再現版）")
        logging.info("★ ハイパーパラメータ固定（min_child_samples=140）★")
        logging.info("="*60)
        
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
        
        # v3.5パラメータで学習
        params = self.V35_PARAMS.copy()
        params['label_gain'] = list(range(100))
        
        logging.info(f"パラメータ: min_child_samples={params['min_child_samples']}, learning_rate={params['learning_rate']:.4f}")
        
        self.model = lgb.LGBMRanker(**params, n_estimators=2000)
        self.model.fit(
            train_df[feature_cols], train_df['target_relevance'],
            group=group_train, sample_weight=train_df['sample_weight'],
            eval_set=[(valid_df[feature_cols], valid_df['target_relevance'])],
            eval_group=[group_valid],
            callbacks=[lgb.early_stopping(100, verbose=True)]
        )
        
        valid_roi = calculate_roi(valid_df, self.model.predict(valid_df[feature_cols]))
        test_roi = calculate_roi(test_df, self.model.predict(test_df[feature_cols]))
        
        # 特徴量重要度
        importance = dict(zip(feature_cols, self.model.feature_importances_))
        top_features = sorted(importance.items(), key=lambda x: x[1], reverse=True)[:20]
        
        logging.info("="*60)
        logging.info("【最終結果: μ v3.6（v3.5再現版）】")
        logging.info("="*60)
        logging.info(f"  Valid ROI: {valid_roi:.2%}")
        logging.info(f"  Test ROI: {test_roi:.2%}")
        logging.info(f"  Gap: {abs(valid_roi - test_roi):.2%}")
        logging.info(f"")
        logging.info(f"  🎯 v3.5 (81.84%) との差: {(test_roi - 0.8184)*100:+.2f}%")
        logging.info(f"  🎯 v5.4 (80.05%) との差: {(test_roi - 0.8005)*100:+.2f}%")
        logging.info(f"")
        logging.info("【Top 20特徴量】")
        for name, imp in top_features:
            marker = "★" if "gap_ability" in name else ""
            logging.info(f"  {marker}{name}: {imp}")
        
        self.save_model(feature_cols, valid_roi, test_roi)
        return valid_roi, test_roi
    
    def save_model(self, features, valid_roi, test_roi):
        with open(self.output_dir / 'mu_v3_6_ranker.pkl', 'wb') as f:
            pickle.dump(self.model, f)
        with open(self.output_dir / 'feature_names.json', 'w', encoding='utf-8') as f:
            json.dump(features, f, ensure_ascii=False, indent=2)
        
        params_to_save = {k: v for k, v in self.V35_PARAMS.items() if k != 'label_gain'}
        metadata = {
            'version': 'v3.6',
            'description': 'v3.5再現版（gap_ability_popularity + min_child_samples=140）',
            'valid_roi': float(valid_roi),
            'test_roi': float(test_roi),
            'valid_test_gap': float(abs(valid_roi - test_roi)),
            'target_v35_test_roi': 0.8184,
            'feature_count': len(features),
            'best_params': params_to_save,
            'created_at': datetime.now().isoformat(),
        }
        with open(self.output_dir / 'model_info.json', 'w', encoding='utf-8') as f:
            json.dump(metadata, f, ensure_ascii=False, indent=2)
        logging.info(f"保存完了: {self.output_dir}")
    
    def run(self):
        df = self.load_data()
        df = self.generate_gap_features_v35(df)  # v3.5と同じロジック
        df = self.prepare_target(df)
        feature_cols = self.get_features(df)
        return self.train(df, feature_cols)


if __name__ == "__main__":
    MuV36Trainer().run()
