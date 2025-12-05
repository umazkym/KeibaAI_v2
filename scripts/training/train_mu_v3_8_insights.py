#!/usr/bin/env python3
"""
μモデル v3.8 - 分析insights活用版

【分析から得た知見】
1. 中穴ゾーン（10-50倍）でROI 100%超
2. 5番人気、8番人気でROI 100%超  
3. 中距離（1800-2200m）が最高ROI
4. 外れても37%が2-3着（予測は大きく外れていない）

【改善アプローチ】
- 高ROIゾーンの馬を重視する学習重みを設計
- オッズベースではなく、人気順ベースの重み付け
- 中穴ゾーンに最適化されたターゲット設計

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


class MuV38Trainer:
    """μモデル v3.8 学習クラス（分析insights活用版）"""
    
    def __init__(self):
        self.base_model_dir = Path('keibaai/models/mu_v3_3')
        self.output_dir = Path('keibaai/models/mu_v3_8')
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
        
        for col, name in [
            ('jockey_win_rate', 'jockey'),
            ('trainer_win_rate', 'trainer'),
            ('sire_win_rate', 'pedigree'),
            ('sire_course_win_rate', 'course_fit'),
            ('horse_weight_zscore', 'weight'),
            ('pace_fit_score', 'pace'),
        ]:
            if col in df.columns:
                df[f'{name}_rank'] = df.groupby('race_id')[col].rank(ascending=False, method='first')
                df[f'gap_{name}_popularity'] = df['popularity'] - df[f'{name}_rank']
        
        if 'combo_avg_finish' in df.columns:
            df['combo_rank'] = df.groupby('race_id')['combo_avg_finish'].rank(ascending=True, method='first')
            df['gap_combo_popularity'] = df['popularity'] - df['combo_rank']
        
        if 'form_rank' in df.columns:
            df['form_rank_in_race'] = df.groupby('race_id')['form_rank'].rank(ascending=False, method='first')
            df['gap_form_popularity'] = df['popularity'] - df['form_rank_in_race']
        
        logging.info("Gap Features 拡張完了")
        return df
    
    def add_value_zone_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """【新規】バリューゾーン特徴量を追加"""
        logging.info("バリューゾーン特徴量を追加中...")
        
        # 中穴ゾーン指標（10-50倍がベスト）
        if 'win_odds' in df.columns:
            # 注意：学習時にはwin_oddsは使わないが、ターゲット設計には使う
            odds = df['win_odds'].fillna(10)
            # 中穴ゾーンからの距離（0に近いほど良い）
            df['odds_zone_score'] = np.abs(np.log(odds) - np.log(20))  # 20倍が中心
        
        # 5番人気・8番人気フラグ（高ROIゾーン）
        if 'popularity' in df.columns:
            df['is_sweet_spot_pop'] = df['popularity'].isin([5, 8]).astype(float)
            # 人気順に基づくバリュースコア（5, 8番人気にピーク）
            pop_value_map = {1: 0.0, 2: 0.2, 3: 0.3, 4: 0.4, 5: 1.0, 6: 0.6, 7: 0.7, 8: 1.0, 9: 0.5, 10: 0.4}
            df['popularity_value_score'] = df['popularity'].map(pop_value_map).fillna(0.3)
        
        # 中距離フラグ（1800-2200mがベスト）
        if 'distance_m' in df.columns:
            df['is_mid_distance'] = ((df['distance_m'] >= 1800) & (df['distance_m'] <= 2200)).astype(float)
        
        # Gap（実力 vs 人気）が正の馬 = 過小評価馬
        if 'gap_ability_popularity' in df.columns:
            df['is_undervalued'] = (df['gap_ability_popularity'] > 0).astype(float)
        
        logging.info("バリューゾーン特徴量追加完了")
        return df
    
    def prepare_features(self, df: pd.DataFrame) -> list:
        """特徴量を準備"""
        with open(self.base_model_dir / 'feature_names.json', 'r', encoding='utf-8') as f:
            base_features = json.load(f)
        
        new_features = [
            'gap_weight_popularity',
            'gap_pace_popularity',
            'gap_combo_popularity',
            'gap_form_popularity',
            'is_sweet_spot_pop',
            'popularity_value_score',
            'is_mid_distance',
            'is_undervalued',
        ]
        
        all_features = base_features + new_features
        available_features = [f for f in all_features if f in df.columns]
        
        # odds_zone_score は学習には使わない（リークの可能性）
        available_features = [f for f in available_features if f != 'odds_zone_score']
        
        logging.info(f"特徴量数: {len(available_features)}")
        return available_features
    
    def prepare_target_with_insights(self, df: pd.DataFrame):
        """【改善】分析insights を反映したターゲット設計"""
        logging.info("ターゲット変数を準備中（insights活用版）...")
        
        # 基本ターゲット
        weight_1st, weight_2nd, weight_3rd = 12.74, 6.73, 3.69
        odds = df['win_odds'].fillna(1.0).clip(upper=90)
        log_odds = np.log1p(odds)
        
        gain = np.zeros(len(df))
        gain[df['finish_position'] == 1] = log_odds[df['finish_position'] == 1] * weight_1st
        gain[df['finish_position'] == 2] = log_odds[df['finish_position'] == 2] * weight_2nd
        gain[df['finish_position'] == 3] = log_odds[df['finish_position'] == 3] * weight_3rd
        
        df['target_gain'] = gain
        df['target_relevance'] = df['target_gain'].astype(int)
        
        # 【改善】分析insights を反映したサンプル重み
        # 高ROIゾーン（中穴、5/8番人気、中距離）を重視
        base_weight = np.log1p(odds).clip(upper=np.log1p(100))
        
        # 中穴ボーナス（10-50倍に+50%）
        mid_odds_bonus = ((odds >= 10) & (odds <= 50)).astype(float) * 0.5
        
        # 5番人気・8番人気ボーナス（+30%）
        sweet_spot_bonus = df['is_sweet_spot_pop'] * 0.3 if 'is_sweet_spot_pop' in df.columns else 0
        
        # 中距離ボーナス（+20%）
        mid_dist_bonus = df['is_mid_distance'] * 0.2 if 'is_mid_distance' in df.columns else 0
        
        # 過小評価馬ボーナス（+20%）
        undervalued_bonus = df['is_undervalued'] * 0.2 if 'is_undervalued' in df.columns else 0
        
        # 総合重み
        df['sample_weight'] = base_weight * (1 + mid_odds_bonus + sweet_spot_bonus + mid_dist_bonus + undervalued_bonus)
        
        logging.info(f"サンプル重み統計: mean={df['sample_weight'].mean():.2f}, max={df['sample_weight'].max():.2f}")
        
        return df
    
    def train(self, df: pd.DataFrame, feature_cols: list, n_trials: int = 50):
        """モデル学習"""
        logging.info("=" * 60)
        logging.info("μモデル v3.8 学習開始（分析insights活用版）")
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
                'num_leaves': trial.suggest_int('num_leaves', 31, 127),
                'lambda_l1': trial.suggest_float('lambda_l1', 0.1, 10.0, log=True),
                'lambda_l2': trial.suggest_float('lambda_l2', 0.1, 10.0, log=True),
                'min_child_samples': trial.suggest_int('min_child_samples', 50, 300),
                'learning_rate': trial.suggest_float('learning_rate', 0.01, 0.15, log=True),
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
        
        # Top 1精度
        test_df = test_df.copy()
        test_df['score'] = test_preds
        test_df['rank_pred'] = test_df.groupby('race_id')['score'].rank(ascending=False, method='first')
        top1_acc = (test_df[test_df['rank_pred'] == 1]['finish_position'] == 1).mean()
        
        logging.info("=" * 60)
        logging.info(f"【最終結果: μ v3.8（分析insights活用版）】")
        logging.info(f"  Test ROI: {test_roi:.2%}")
        logging.info(f"  Top 1 Accuracy: {top1_acc:.2%}")
        logging.info(f"  レース数: {test_df['race_id'].nunique():,}（全レース投資）")
        logging.info("=" * 60)
        
        self.save_model(feature_cols, test_roi, top1_acc, best_params)
        
        return test_roi
    
    def save_model(self, features: list, test_roi: float, top1_acc: float, best_params: dict):
        """モデル保存"""
        with open(self.output_dir / 'mu_v3_8_ranker.pkl', 'wb') as f:
            pickle.dump(self.model, f)
        
        with open(self.output_dir / 'feature_names.json', 'w', encoding='utf-8') as f:
            json.dump(features, f, ensure_ascii=False, indent=2)
        
        save_params = {k: v for k, v in best_params.items() if k != 'label_gain'}
        metadata = {
            'version': 'v3.8',
            'description': '分析insights活用版 - 高ROIゾーン重み付け',
            'test_roi': test_roi,
            'top1_accuracy': top1_acc,
            'feature_count': len(features),
            'best_params': save_params,
            'created_at': datetime.now().isoformat()
        }
        
        with open(self.output_dir / 'model_info.json', 'w', encoding='utf-8') as f:
            json.dump(metadata, f, ensure_ascii=False, indent=2)
        
        logging.info(f"モデル保存完了: {self.output_dir}")
    
    def run(self, n_trials: int = 50):
        """メイン実行"""
        df = self.load_data()
        df = self.add_extended_gap_features(df)
        df = self.add_value_zone_features(df)
        df = self.prepare_target_with_insights(df)
        feature_cols = self.prepare_features(df)
        self.feature_cols = feature_cols
        test_roi = self.train(df, feature_cols, n_trials=n_trials)
        
        return test_roi


if __name__ == "__main__":
    trainer = MuV38Trainer()
    trainer.run(n_trials=50)
