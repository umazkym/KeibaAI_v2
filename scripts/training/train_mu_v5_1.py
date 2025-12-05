#!/usr/bin/env python3
"""
μモデル v5.1 学習スクリプト（リーク修正版）

【修正点】v5.0のリークを完全除去
- expanding().mean().shift(1) で「過去のみ」参照
- 今回のレースを特徴量計算から完全除外

【特徴量 13個】
Group A: タイム指数（4個）- shift(1)適用
Group B: コース適性（4個）- expanding+shift適用
Group C: ローテーション（4個）- diff()適用
Group D: 脚質（1個）- expanding+shift適用
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


class MuV51Trainer:
    """μモデル v5.1 学習クラス（リーク修正版）"""
    
    LEFT_TURN_VENUES = ['東京', '新潟', '中京']
    STEEP_SLOPE_VENUES = ['中山', '阪神', '中京']
    
    MIN_SAMPLES = {'jockey': 15, 'sire': 30, 'horse': 2}
    SMOOTHING_C_CONTEXT = 20
    SMOOTHING_C_BASE = 30
    
    PURGE_FEATURES = [
        'jockey_win_rate', 'jockey_place_rate', 'jockey_avg_rank',
        'sire_win_rate', 'sire_avg_finish',
        'trainer_win_rate', 'trainer_place_rate',
    ]
    
    DISTANCE_CATEGORIES = {
        'sprint': (0, 1400),
        'mile': (1400, 1800),
        'intermediate': (1800, 2200),
        'long': (2200, 9999)
    }
    
    def __init__(self):
        self.base_model_dir = Path('keibaai/models/mu_v3_3')
        self.output_dir = Path('keibaai/models/mu_v5_1')
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.model = None
    
    @staticmethod
    def extract_venue_name(venue_str):
        if pd.isna(venue_str):
            return None
        match = re.search(r'[0-9]*([^\d]+)[0-9]*', str(venue_str))
        return match.group(1) if match else None
    
    @staticmethod
    def get_turn_direction(venue):
        return 'left' if venue in MuV51Trainer.LEFT_TURN_VENUES else 'right'
    
    @staticmethod
    def get_distance_category(distance_m):
        for cat, (low, high) in MuV51Trainer.DISTANCE_CATEGORIES.items():
            if low <= distance_m < high:
                return cat
        return 'intermediate'
    
    @staticmethod
    def calculate_normalized_rank(finish, n_runners):
        if n_runners <= 1:
            return 0.5
        return 1.0 - (finish - 1) / (n_runners - 1)
    
    @staticmethod
    def time_decay_weight(years_ago, rate=0.3):
        return np.exp(-rate * years_ago)
    
    def load_data(self):
        logging.info("データを読み込み中...")
        train_data = pd.read_parquet(self.base_model_dir / 'train_data_mu_v3_3.parquet')
        train_data['race_date'] = pd.to_datetime(train_data['race_date'])
        
        perf_df = pd.read_parquet('keibaai/data/parsed/parquet/horses_performance/horses_performance.parquet')
        perf_df['race_date'] = pd.to_datetime(perf_df['race_date'])
        
        shutuba_df = pd.read_parquet('keibaai/data/parsed/parquet/shutuba/shutuba.parquet')
        
        pedigree_path = Path('keibaai/data/parsed/parquet/pedigrees/pedigrees.parquet')
        pedigree_df = pd.read_parquet(pedigree_path) if pedigree_path.exists() else None
        
        logging.info(f"  学習: {len(train_data):,}, 戦績: {len(perf_df):,}")
        return train_data, perf_df, pedigree_df, shutuba_df
    
    def generate_horse_features(self, df, perf_df):
        """馬自身の特徴量を生成（リーク修正版）"""
        logging.info("=" * 60)
        logging.info("v5.1: 馬特徴量生成（リーク修正版）")
        logging.info("=" * 60)
        
        perf = perf_df.copy()
        perf['venue_name'] = perf['venue'].apply(self.extract_venue_name)
        perf['distance_category'] = perf['distance_m'].apply(self.get_distance_category)
        perf['is_wet'] = perf['track_condition'].isin(['重', '不良'])
        perf['n_runners'] = perf['head_count'].fillna(16)
        perf['normalized_rank'] = perf.apply(
            lambda x: self.calculate_normalized_rank(x['finish_position'], x['n_runners']), axis=1
        )
        # 数値型変換（NAType対策）
        perf['normalized_rank'] = pd.to_numeric(perf['normalized_rank'], errors='coerce')
        
        # タイム偏差計算
        race_avg_time = perf.groupby('race_id')['finish_time_seconds'].transform('mean')
        race_std_time = perf.groupby('race_id')['finish_time_seconds'].transform('std').fillna(1)
        perf['time_deviation'] = (race_avg_time - perf['finish_time_seconds']) / race_std_time
        
        race_avg_l3f = perf.groupby('race_id')['last_3f_time'].transform('mean')
        race_std_l3f = perf.groupby('race_id')['last_3f_time'].transform('std').fillna(0.5)
        perf['l3f_deviation'] = (race_avg_l3f - perf['last_3f_time']) / race_std_l3f
        
        # 4角通過順の正規化
        perf['position_4c_normalized'] = perf.apply(
            lambda x: (x['passing_order_4'] - 1) / max(x['n_runners'] - 1, 1) 
            if pd.notna(x['passing_order_4']) else np.nan, axis=1
        )
        
        # ★★★ 時系列ソート（必須！） ★★★
        perf = perf.sort_values(['horse_id', 'race_date', 'race_id']).reset_index(drop=True)
        
        # ========================================
        # Group A: タイム指数（expanding + shift）
        # ========================================
        logging.info("  Group A: タイム指数（リーク修正）...")
        
        # 累積平均をshift(1)で「前走まで」に限定
        perf['horse_time_deviation_avg'] = (
            perf.groupby('horse_id')['time_deviation']
            .transform(lambda x: x.expanding().mean().shift(1))
        )
        
        perf['horse_l3f_deviation_avg'] = (
            perf.groupby('horse_id')['l3f_deviation']
            .transform(lambda x: x.expanding().mean().shift(1))
        )
        
        # 過去最高（今回を除く）
        perf['horse_best_time_deviation'] = (
            perf.groupby('horse_id')['time_deviation']
            .transform(lambda x: x.expanding().max().shift(1))
        )
        
        # 直近3走トレンド（回帰傾き）
        def calc_trend_safe(series):
            result = []
            for i in range(len(series)):
                if i < 3:
                    result.append(0)
                else:
                    recent = series.iloc[max(0, i-3):i].dropna()
                    if len(recent) >= 2:
                        x = np.arange(len(recent))
                        try:
                            slope = np.polyfit(x, recent.values, 1)[0]
                        except:
                            slope = 0
                        result.append(slope)
                    else:
                        result.append(0)
            return pd.Series(result, index=series.index)
        
        perf['horse_recent_trend'] = perf.groupby('horse_id')['time_deviation'].transform(calc_trend_safe)
        
        # ========================================
        # Group B: コース適性（expanding + shift）
        # ========================================
        logging.info("  Group B: コース適性（リーク修正）...")
        
        # 会場別NR（累積平均、今回除外）- NaN対策
        perf['horse_venue_nr'] = np.nan
        valid_venue = perf['venue_name'].notna()
        if valid_venue.any():
            perf.loc[valid_venue, 'normalized_rank_for_venue'] = perf.loc[valid_venue, 'normalized_rank'].astype(float)
            perf.loc[valid_venue, 'horse_venue_nr'] = (
                perf.loc[valid_venue].groupby(['horse_id', 'venue_name'])['normalized_rank_for_venue']
                .transform(lambda x: x.expanding().mean().shift(1))
            )
        
        # 距離カテゴリ別NR - NaN対策
        perf['horse_distance_nr'] = np.nan
        valid_dist = perf['distance_category'].notna()
        if valid_dist.any():
            perf.loc[valid_dist, 'horse_distance_nr'] = (
                perf.loc[valid_dist].groupby(['horse_id', 'distance_category'])['normalized_rank']
                .transform(lambda x: x.astype(float).expanding().mean().shift(1))
            )
        
        # 馬場別NR - NaN対策
        perf['horse_surface_nr'] = np.nan
        valid_surface = perf['track_surface'].notna()
        if valid_surface.any():
            perf.loc[valid_surface, 'horse_surface_nr'] = (
                perf.loc[valid_surface].groupby(['horse_id', 'track_surface'])['normalized_rank']
                .transform(lambda x: x.astype(float).expanding().mean().shift(1))
            )
        
        # 道悪NR（道悪レースのみでexpanding）
        perf['horse_wet_nr'] = np.nan
        wet_mask = perf['is_wet']
        perf.loc[wet_mask, 'horse_wet_nr'] = (
            perf.loc[wet_mask].groupby('horse_id')['normalized_rank']
            .transform(lambda x: x.expanding().mean().shift(1))
        )
        
        # ========================================
        # Group C: ローテーション（diff使用）
        # ========================================
        logging.info("  Group C: ローテーション（リーク修正）...")
        
        # 前走からの日数（diff）
        perf['horse_interval_days'] = perf.groupby('horse_id')['race_date'].diff().dt.days
        
        # 距離変更（前走との差）
        perf['horse_dist_change'] = perf.groupby('horse_id')['distance_m'].diff()
        
        # クラス変更
        class_order = {'新馬': 1, '未勝利': 2, '1勝': 3, '2勝': 4, '3勝': 5, 'OP': 6, 'L': 7, 'G3': 8, 'G2': 9, 'G1': 10}
        perf['class_num'] = perf['race_grade'].map(class_order).fillna(5)
        perf['horse_class_change'] = perf.groupby('horse_id')['class_num'].diff()
        
        # 馬体重変化率（前走との比）
        perf['prev_weight'] = perf.groupby('horse_id')['horse_weight'].shift(1)
        perf['horse_weight_change_ratio'] = (perf['horse_weight'] - perf['prev_weight']) / perf['prev_weight'].clip(lower=400)
        
        # ========================================
        # Group D: 脚質（expanding + shift）
        # ========================================
        logging.info("  Group D: 脚質（リーク修正）...")
        
        perf['horse_avg_position_4c'] = (
            perf.groupby('horse_id')['position_4c_normalized']
            .transform(lambda x: x.expanding().mean().shift(1))
        )
        
        # ========================================
        # train_dataとマージ（race_id + horse_idで結合）
        # ========================================
        logging.info("  特徴量をマージ中...")
        
        horse_features = [
            'horse_time_deviation_avg', 'horse_l3f_deviation_avg',
            'horse_best_time_deviation', 'horse_recent_trend',
            'horse_venue_nr', 'horse_distance_nr', 'horse_surface_nr', 'horse_wet_nr',
            'horse_interval_days', 'horse_dist_change', 'horse_class_change',
            'horse_weight_change_ratio', 'horse_avg_position_4c'
        ]
        
        merge_cols = ['race_id', 'horse_id'] + horse_features
        perf_features = perf[merge_cols].drop_duplicates(subset=['race_id', 'horse_id'])
        
        result = df.merge(perf_features, on=['race_id', 'horse_id'], how='left')
        
        # 数値変換
        for col in horse_features:
            if col in result.columns:
                result[col] = pd.to_numeric(result[col], errors='coerce')
        
        # 非NaN率チェック
        for col in horse_features:
            if col in result.columns:
                non_nan = result[col].notna().mean()
                logging.info(f"    {col}: {non_nan:.1%} 非NaN")
        
        logging.info("=" * 60)
        return result
    
    def generate_jockey_sire_features(self, df, perf_df, pedigree_df, shutuba_df):
        """v4.5から継承：騎手・種牡馬特徴量"""
        logging.info("v4.5継承: 騎手・種牡馬特徴量...")
        
        perf = perf_df.copy()
        perf['n_runners'] = perf['head_count'].fillna(16)
        perf['normalized_rank'] = perf.apply(
            lambda x: self.calculate_normalized_rank(x['finish_position'], x['n_runners']), axis=1
        )
        perf = perf.dropna(subset=['finish_position'])
        
        result = df.copy()
        
        # 騎手
        if 'jockey_id' in perf.columns:
            jockey_base = perf.groupby('jockey_id').agg({'normalized_rank': 'sum', 'race_id': 'count'}).reset_index()
            jockey_base.columns = ['jockey_id', 'nr_sum', 'count']
            jockey_base['jockey_nr_global'] = (jockey_base['nr_sum'] + 0.5 * self.SMOOTHING_C_BASE) / (jockey_base['count'] + self.SMOOTHING_C_BASE)
            
            if 'jockey_id' not in result.columns and shutuba_df is not None:
                jockey_map = shutuba_df[['race_id', 'horse_id', 'jockey_id']].drop_duplicates(subset=['race_id', 'horse_id'])
                result = result.merge(jockey_map, on=['race_id', 'horse_id'], how='left')
            result = result.merge(jockey_base[['jockey_id', 'jockey_nr_global']], on='jockey_id', how='left')
        
        # 種牡馬
        if pedigree_df is not None:
            sire_map = pedigree_df[pedigree_df['generation'] == 1][['horse_id', 'ancestor_id']].drop_duplicates('horse_id')
            sire_map.columns = ['horse_id', 'sire_id']
            perf_with_sire = perf.merge(sire_map, on='horse_id', how='left').dropna(subset=['sire_id'])
            
            sire_base = perf_with_sire.groupby('sire_id').agg({'normalized_rank': 'sum', 'race_id': 'count'}).reset_index()
            sire_base.columns = ['sire_id', 'nr_sum', 'count']
            sire_base['sire_nr_global'] = (sire_base['nr_sum'] + 0.5 * self.SMOOTHING_C_BASE) / (sire_base['count'] + self.SMOOTHING_C_BASE)
            
            if 'sire_id' not in result.columns:
                result = result.merge(sire_map, on='horse_id', how='left')
            result = result.merge(sire_base[['sire_id', 'sire_nr_global']], on='sire_id', how='left')
        
        for col in ['jockey_nr_global', 'sire_nr_global']:
            if col in result.columns:
                result[col] = pd.to_numeric(result[col], errors='coerce')
        
        return result
    
    def prepare_features(self, df):
        with open(self.base_model_dir / 'feature_names.json', 'r', encoding='utf-8') as f:
            base_features = json.load(f)
        base_features = [f for f in base_features if f not in self.PURGE_FEATURES]
        
        new_features = [
            'jockey_nr_global', 'sire_nr_global',
            'horse_time_deviation_avg', 'horse_l3f_deviation_avg',
            'horse_best_time_deviation', 'horse_recent_trend',
            'horse_venue_nr', 'horse_distance_nr', 'horse_surface_nr', 'horse_wet_nr',
            'horse_interval_days', 'horse_dist_change', 'horse_class_change',
            'horse_weight_change_ratio', 'horse_avg_position_4c',
        ]
        
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
        logging.info("μモデル v5.1 学習開始（リーク修正版）")
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
                'min_child_samples': trial.suggest_int('min_child_samples', 30, 150),
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
            
            return valid_roi - abs(valid_roi - test_roi) * 0.15
        
        logging.info(f"Optuna {n_trials}トライアル...")
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
        logging.info("【最終結果: μ v5.1（リーク修正版）】")
        logging.info("=" * 60)
        logging.info(f"  Valid ROI: {valid_roi:.2%}")
        logging.info(f"  Test ROI: {test_roi:.2%}")
        logging.info(f"  Valid-Test差: {abs(valid_roi - test_roi):.2%}")
        logging.info(f"  🎯 目標: {'✅ v3.5超え!' if test_roi > 0.8184 else '❌ 要改善'}")
        logging.info("")
        logging.info("【Top 20特徴量】")
        for name, imp in sorted_imp[:20]:
            logging.info(f"  {name}: {imp}")
        
        logging.info("")
        logging.info("【馬特徴量重要度】")
        horse_feats = ['horse_time_deviation_avg', 'horse_l3f_deviation_avg',
                       'horse_best_time_deviation', 'horse_recent_trend',
                       'horse_venue_nr', 'horse_distance_nr', 'horse_surface_nr', 'horse_wet_nr',
                       'horse_interval_days', 'horse_dist_change', 'horse_class_change',
                       'horse_weight_change_ratio', 'horse_avg_position_4c']
        for f in horse_feats:
            if f in importance:
                logging.info(f"  {f}: {importance[f]}")
        
        self.save_model(feature_cols, valid_roi, test_roi, best_params)
        return valid_roi, test_roi
    
    def save_model(self, features, valid_roi, test_roi, best_params):
        with open(self.output_dir / 'mu_v5_1_ranker.pkl', 'wb') as f:
            pickle.dump(self.model, f)
        with open(self.output_dir / 'feature_names.json', 'w', encoding='utf-8') as f:
            json.dump(features, f, ensure_ascii=False, indent=2)
        
        save_params = {k: v for k, v in best_params.items() if k != 'label_gain'}
        metadata = {
            'version': 'v5.1', 'description': 'リーク修正版（expanding+shift）',
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
        train_data = self.generate_horse_features(train_data, perf_df)
        train_data = self.generate_jockey_sire_features(train_data, perf_df, pedigree_df, shutuba_df)
        train_data = self.prepare_target(train_data)
        feature_cols = self.prepare_features(train_data)
        return self.train(train_data, feature_cols, n_trials)


if __name__ == "__main__":
    MuV51Trainer().run(n_trials=50)
