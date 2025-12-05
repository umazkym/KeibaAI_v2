#!/usr/bin/env python3
"""
μモデル v3.6 - 条件別Gap Feature拡張版

【目的】
既存の条件別特徴量を活用したGap Featureを追加してROI向上を目指す。
投資機会は維持（全レース投資）。

【新規Gap Features】
- gap_dist_ability_popularity: 距離別成績 vs 人気
- gap_surface_ability_popularity: 馬場別成績 vs 人気
- gap_jockey_dist_popularity: 騎手距離別勝率 vs 人気

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
    """μモデル v3.6 学習クラス（条件別Gap Feature拡張版）"""
    
    def __init__(self):
        self.base_model_dir = Path('keibaai/models/mu_v3_3')
        self.output_dir = Path('keibaai/models/mu_v3_6')
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
    
    def add_condition_gap_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """条件別Gap Featureの追加"""
        logging.info("=" * 60)
        logging.info("条件別Gap Featureを生成中（v3.6新規）...")
        logging.info("=" * 60)
        
        # 【Gap Feature 1】距離別成績 vs 人気
        if 'dist_avg_finish' in df.columns and 'popularity' in df.columns:
            df['dist_ability_rank'] = df.groupby('race_id')['dist_avg_finish'].rank(
                ascending=True, method='first', na_option='bottom'
            )
            df['gap_dist_ability_popularity'] = df['popularity'] - df['dist_ability_rank']
            logging.info("  gap_dist_ability_popularity 生成完了")
        
        # 【Gap Feature 2】馬場別成績 vs 人気
        if 'surface_avg_finish' in df.columns and 'popularity' in df.columns:
            df['surface_ability_rank'] = df.groupby('race_id')['surface_avg_finish'].rank(
                ascending=True, method='first', na_option='bottom'
            )
            df['gap_surface_ability_popularity'] = df['popularity'] - df['surface_ability_rank']
            logging.info("  gap_surface_ability_popularity 生成完了")
        
        # 【Gap Feature 3】競馬場別成績 vs 人気
        if 'venue_avg_finish' in df.columns and 'popularity' in df.columns:
            df['venue_ability_rank'] = df.groupby('race_id')['venue_avg_finish'].rank(
                ascending=True, method='first', na_option='bottom'
            )
            df['gap_venue_ability_popularity'] = df['popularity'] - df['venue_ability_rank']
            logging.info("  gap_venue_ability_popularity 生成完了")
        
        # 【Gap Feature 4】騎手勝率ランク vs 人気（距離別）
        jockey_dist_cols = [c for c in df.columns if c.startswith('jockey_') and c.endswith('_win_rate') 
                           and c not in ['jockey_win_rate', 'jockey_place_rate']]
        
        if jockey_dist_cols and 'popularity' in df.columns:
            # 現在の距離カテゴリに対応する騎手勝率を使用
            if 'distance_m' in df.columns:
                def get_jockey_dist_rate(row):
                    dist = row.get('distance_m', 0)
                    if dist <= 1400:
                        col = 'jockey_sprint_win_rate'
                    elif dist <= 1800:
                        col = 'jockey_mile_win_rate'
                    elif dist <= 2200:
                        col = 'jockey_intermediate_win_rate'
                    else:
                        col = 'jockey_long_win_rate'
                    return row.get(col, np.nan) if col in row.index else np.nan
                
                if 'jockey_sprint_win_rate' in df.columns:
                    # 各行に距離別勝率を適用
                    jockey_dist_rate = []
                    for idx, row in df.iterrows():
                        dist = row.get('distance_m', 0)
                        if dist <= 1400:
                            val = row.get('jockey_sprint_win_rate', np.nan)
                        elif dist <= 1800:
                            val = row.get('jockey_mile_win_rate', np.nan)
                        elif dist <= 2200:
                            val = row.get('jockey_intermediate_win_rate', np.nan)
                        else:
                            val = row.get('jockey_long_win_rate', np.nan)
                        jockey_dist_rate.append(val)
                    
                    df['jockey_current_dist_rate'] = jockey_dist_rate
                    df['jockey_dist_rank'] = df.groupby('race_id')['jockey_current_dist_rate'].rank(
                        ascending=False, method='first', na_option='bottom'
                    )
                    df['gap_jockey_dist_popularity'] = df['popularity'] - df['jockey_dist_rank']
                    logging.info("  gap_jockey_dist_popularity 生成完了")
        
        logging.info("=" * 60)
        logging.info("条件別Gap Featureの生成完了")
        logging.info("=" * 60)
        
        return df
    
    def add_extended_gap_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Gap Feature拡張（v3.5継承）"""
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
        
        logging.info("Gap Features 拡張完了")
        return df
    
    def prepare_features(self, df: pd.DataFrame) -> list:
        """特徴量を準備"""
        with open(self.base_model_dir / 'feature_names.json', 'r', encoding='utf-8') as f:
            base_features = json.load(f)
        
        # v3.6新規特徴量
        new_features = [
            'gap_dist_ability_popularity',
            'gap_surface_ability_popularity',
            'gap_venue_ability_popularity',
            'gap_jockey_dist_popularity',
        ]
        
        all_features = base_features + new_features
        available_features = [f for f in all_features if f in df.columns]
        
        # 重複排除
        available_features = list(dict.fromkeys(available_features))
        
        new_count = len([f for f in new_features if f in df.columns])
        logging.info(f"特徴量数: {len(available_features)}（ベース: {len(base_features)}, 新規: {new_count}）")
        
        return available_features
    
    def prepare_target(self, df: pd.DataFrame):
        """ターゲット変数を準備"""
        weight_1st, weight_2nd, weight_3rd = 12.74, 6.73, 3.69
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
        
        return df
    
    def train(self, df: pd.DataFrame, feature_cols: list, n_trials: int = 30):
        """モデル学習（Optuna最適化）"""
        logging.info("=" * 60)
        logging.info("μモデル v3.6 学習開始（条件別Gap Feature拡張版）")
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
        
        logging.info("=" * 60)
        logging.info(f"【最終結果: μ v3.6（条件別Gap Feature拡張版）】")
        logging.info(f"  Valid ROI: {valid_roi:.2%}")
        logging.info(f"  Test ROI: {test_roi:.2%}")
        logging.info(f"  Valid-Test差: {abs(valid_roi - test_roi):.2%}")
        logging.info(f"  テストレース数: {test_df['race_id'].nunique():,}（全レース投資）")
        logging.info("=" * 60)
        
        # 特徴量重要度の表示
        importance = self.model.feature_importances_
        importance_df = pd.DataFrame({
            'feature': feature_cols,
            'importance': importance
        }).sort_values('importance', ascending=False)
        
        logging.info("【特徴量重要度 Top 20】")
        for i, row in importance_df.head(20).iterrows():
            logging.info(f"  {row['feature']}: {row['importance']}")
        
        # 新規特徴量の重要度確認
        new_features = ['gap_dist_ability_popularity', 'gap_surface_ability_popularity', 
                       'gap_venue_ability_popularity', 'gap_jockey_dist_popularity']
        logging.info("\n【新規特徴量の重要度】")
        for f in new_features:
            if f in feature_cols:
                imp = importance[feature_cols.index(f)]
                logging.info(f"  {f}: {imp}")
        
        # 保存
        self.save_model(feature_cols, valid_roi, test_roi, best_params)
        
        return valid_roi, test_roi
    
    def save_model(self, features: list, valid_roi: float, test_roi: float, best_params: dict):
        """モデル保存"""
        with open(self.output_dir / 'mu_v3_6_ranker.pkl', 'wb') as f:
            pickle.dump(self.model, f)
        
        with open(self.output_dir / 'feature_names.json', 'w', encoding='utf-8') as f:
            json.dump(features, f, ensure_ascii=False, indent=2)
        
        save_params = {k: v for k, v in best_params.items() if k != 'label_gain'}
        metadata = {
            'version': 'v3.6',
            'description': '条件別Gap Feature拡張版',
            'valid_roi': valid_roi,
            'test_roi': test_roi,
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
        df = self.add_condition_gap_features(df)
        df = self.add_extended_gap_features(df)
        df = self.prepare_target(df)
        feature_cols = self.prepare_features(df)
        self.feature_cols = feature_cols
        valid_roi, test_roi = self.train(df, feature_cols, n_trials=n_trials)
        
        return valid_roi, test_roi


if __name__ == "__main__":
    trainer = MuV36Trainer()
    trainer.run(n_trials=30)
