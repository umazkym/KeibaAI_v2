#!/usr/bin/env python3
"""
μモデル v5.6 学習スクリプト（Forward Selection版）

【目的】
- v3.5のベース特徴量から開始
- v5.4新規特徴量を1つずつ追加
- 各追加時のValid ROIを測定し、最適な特徴量セットを特定

【レビュー指摘対応】
- 「引き算」の開発：まずv3.5を再現し、慎重に特徴量を追加
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


class MuV56Trainer:
    """μモデル v5.6 学習クラス（Forward Selection版）"""
    
    DISTANCE_CATEGORIES = {
        'sprint': (0, 1400),
        'mile': (1400, 1800),
        'intermediate': (1800, 2200),
        'long': (2200, 9999)
    }
    
    SMOOTHING_C_BASE = 30
    
    # v3.3から除外する特徴量
    PURGE_FEATURES = [
        'jockey_win_rate', 'jockey_place_rate', 'jockey_avg_rank',
        'sire_win_rate', 'sire_avg_finish',
        'trainer_win_rate', 'trainer_place_rate',
    ]
    
    # v5.4で追加した新規特徴量（Forward Selectionの候補）
    NEW_FEATURES = [
        'jockey_nr_global',
        'sire_nr_global',
        'horse_time_deviation_avg',
        'horse_l3f_deviation_avg',
        'horse_best_time_deviation',
        'horse_venue_nr',
        'horse_distance_nr',
        'horse_surface_nr',
        'horse_best_nr',
        'horse_interval_days',
        'horse_dist_change',
        'horse_weight_change_ratio',
        'horse_avg_position_4c',
    ]
    
    def __init__(self):
        self.base_model_dir = Path('keibaai/models/mu_v3_3')
        self.output_dir = Path('keibaai/models/mu_v5_6')
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.model = None
    
    @staticmethod
    def extract_venue_name(venue_str):
        if pd.isna(venue_str):
            return None
        match = re.search(r'[0-9]*([^\d]+)[0-9]*', str(venue_str))
        return match.group(1) if match else None
    
    @staticmethod
    def get_distance_category(distance_m):
        for cat, (low, high) in MuV56Trainer.DISTANCE_CATEGORIES.items():
            if low <= distance_m < high:
                return cat
        return 'intermediate'
    
    @staticmethod
    def calculate_normalized_rank(finish, n_runners):
        if pd.isna(finish) or pd.isna(n_runners) or n_runners <= 1:
            return np.nan
        return 1.0 - (finish - 1) / (n_runners - 1)
    
    def load_data(self):
        logging.info("データを読み込み中...")
        train_data = pd.read_parquet(self.base_model_dir / 'train_data_mu_v3_3.parquet')
        train_data['race_date'] = pd.to_datetime(train_data['race_date'])
        
        races_df = pd.read_parquet('keibaai/data/parsed/parquet/races/races.parquet')
        races_df['race_date'] = pd.to_datetime(races_df['race_date'])
        
        shutuba_df = pd.read_parquet('keibaai/data/parsed/parquet/shutuba/shutuba.parquet')
        
        pedigree_path = Path('keibaai/data/parsed/parquet/pedigrees/pedigrees.parquet')
        pedigree_df = pd.read_parquet(pedigree_path) if pedigree_path.exists() else None
        
        logging.info(f"  学習: {len(train_data):,}, レース結果: {len(races_df):,}")
        
        train_data['horse_id'] = train_data['horse_id'].astype(str)
        races_df['horse_id'] = races_df['horse_id'].astype(str)
        
        return train_data, races_df, pedigree_df, shutuba_df
    
    def generate_all_features(self, df, races_df, pedigree_df, shutuba_df):
        """全特徴量を生成"""
        logging.info("="*60)
        logging.info("特徴量生成中...")
        logging.info("="*60)
        
        # 馬特徴量
        perf = races_df.copy()
        perf['venue_name'] = perf['venue'].apply(self.extract_venue_name)
        perf['distance_category'] = perf['distance_m'].apply(self.get_distance_category)
        
        perf['n_runners'] = perf.groupby('race_id')['horse_id'].transform('count')
        perf['finish_position'] = pd.to_numeric(perf['finish_position'], errors='coerce')
        perf['finish_time_seconds'] = pd.to_numeric(perf['finish_time_seconds'], errors='coerce')
        perf['last_3f_time'] = pd.to_numeric(perf['last_3f_time'], errors='coerce')
        perf['horse_weight'] = pd.to_numeric(perf['horse_weight'], errors='coerce')
        perf['passing_order_4'] = pd.to_numeric(perf['passing_order_4'], errors='coerce')
        
        perf['normalized_rank'] = perf.apply(
            lambda x: self.calculate_normalized_rank(x['finish_position'], x['n_runners']), axis=1
        )
        
        race_avg_time = perf.groupby('race_id')['finish_time_seconds'].transform('mean')
        race_std_time = perf.groupby('race_id')['finish_time_seconds'].transform('std').fillna(1).replace(0, 1)
        perf['time_deviation'] = (race_avg_time - perf['finish_time_seconds']) / race_std_time
        
        race_avg_l3f = perf.groupby('race_id')['last_3f_time'].transform('mean')
        race_std_l3f = perf.groupby('race_id')['last_3f_time'].transform('std').fillna(0.5).replace(0, 0.5)
        perf['l3f_deviation'] = (race_avg_l3f - perf['last_3f_time']) / race_std_l3f
        
        perf['position_4c_normalized'] = (perf['passing_order_4'] - 1) / (perf['n_runners'] - 1).clip(lower=1)
        
        perf = perf.sort_values(['horse_id', 'race_date']).reset_index(drop=True)
        
        # 馬特徴量
        perf['horse_time_deviation_avg'] = perf.groupby('horse_id')['time_deviation'].transform(lambda x: x.expanding().mean().shift(1))
        perf['horse_l3f_deviation_avg'] = perf.groupby('horse_id')['l3f_deviation'].transform(lambda x: x.expanding().mean().shift(1))
        perf['horse_best_time_deviation'] = perf.groupby('horse_id')['time_deviation'].transform(lambda x: x.expanding().max().shift(1))
        perf['horse_venue_nr'] = perf.groupby(['horse_id', 'venue_name'])['normalized_rank'].transform(lambda x: x.expanding().mean().shift(1))
        perf['horse_distance_nr'] = perf.groupby(['horse_id', 'distance_category'])['normalized_rank'].transform(lambda x: x.expanding().mean().shift(1))
        perf['horse_surface_nr'] = perf.groupby(['horse_id', 'track_surface'])['normalized_rank'].transform(lambda x: x.expanding().mean().shift(1))
        perf['horse_best_nr'] = perf.groupby('horse_id')['normalized_rank'].transform(lambda x: x.expanding().max().shift(1))
        perf['horse_interval_days'] = perf.groupby('horse_id')['race_date'].diff().dt.days
        perf['horse_dist_change'] = perf.groupby('horse_id')['distance_m'].diff()
        perf['prev_weight'] = perf.groupby('horse_id')['horse_weight'].shift(1)
        perf['horse_weight_change_ratio'] = (perf['horse_weight'] - perf['prev_weight']) / perf['prev_weight'].clip(lower=400)
        perf['horse_avg_position_4c'] = perf.groupby('horse_id')['position_4c_normalized'].transform(lambda x: x.expanding().mean().shift(1))
        
        horse_features = [
            'horse_time_deviation_avg', 'horse_l3f_deviation_avg', 'horse_best_time_deviation',
            'horse_venue_nr', 'horse_distance_nr', 'horse_surface_nr', 'horse_best_nr',
            'horse_interval_days', 'horse_dist_change', 'horse_weight_change_ratio',
            'horse_avg_position_4c'
        ]
        
        merge_df = perf[['horse_id', 'race_date'] + horse_features].copy()
        merge_df = merge_df.drop_duplicates(subset=['horse_id', 'race_date'], keep='last')
        for col in horse_features:
            merge_df[col] = pd.to_numeric(merge_df[col], errors='coerce')
        
        result = df.merge(merge_df, on=['horse_id', 'race_date'], how='left')
        
        # 騎手・種牡馬NR
        perf_clean = perf.dropna(subset=['finish_position'])
        
        if 'jockey_id' in perf_clean.columns:
            jockey_base = perf_clean.groupby('jockey_id').agg({'normalized_rank': 'sum', 'race_id': 'count'}).reset_index()
            jockey_base.columns = ['jockey_id', 'nr_sum', 'count']
            jockey_base['jockey_nr_global'] = (jockey_base['nr_sum'] + 0.5 * self.SMOOTHING_C_BASE) / (jockey_base['count'] + self.SMOOTHING_C_BASE)
            
            if 'jockey_id' not in result.columns and shutuba_df is not None:
                jockey_map = shutuba_df[['race_id', 'horse_id', 'jockey_id']].drop_duplicates(subset=['race_id', 'horse_id'])
                jockey_map['horse_id'] = jockey_map['horse_id'].astype(str)
                result = result.merge(jockey_map, on=['race_id', 'horse_id'], how='left')
            result = result.merge(jockey_base[['jockey_id', 'jockey_nr_global']], on='jockey_id', how='left')
        
        if pedigree_df is not None:
            sire_map = pedigree_df[pedigree_df['generation'] == 1][['horse_id', 'ancestor_id']].drop_duplicates('horse_id')
            sire_map.columns = ['horse_id', 'sire_id']
            sire_map['horse_id'] = sire_map['horse_id'].astype(str)
            perf_clean['horse_id'] = perf_clean['horse_id'].astype(str)
            perf_with_sire = perf_clean.merge(sire_map, on='horse_id', how='left').dropna(subset=['sire_id'])
            
            sire_base = perf_with_sire.groupby('sire_id').agg({'normalized_rank': 'sum', 'race_id': 'count'}).reset_index()
            sire_base.columns = ['sire_id', 'nr_sum', 'count']
            sire_base['sire_nr_global'] = (sire_base['nr_sum'] + 0.5 * self.SMOOTHING_C_BASE) / (sire_base['count'] + self.SMOOTHING_C_BASE)
            
            if 'sire_id' not in result.columns:
                result = result.merge(sire_map, on='horse_id', how='left')
            result = result.merge(sire_base[['sire_id', 'sire_nr_global']], on='sire_id', how='left')
        
        return result
    
    def get_base_features(self, df):
        """v3.3ベースの特徴量を取得（新規特徴量なし）"""
        with open(self.base_model_dir / 'feature_names.json', 'r', encoding='utf-8') as f:
            base_features = json.load(f)
        base_features = [f for f in base_features if f not in self.PURGE_FEATURES]
        # 新規特徴量を除外
        base_features = [f for f in base_features if f not in self.NEW_FEATURES]
        return [f for f in base_features if f in df.columns]
    
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
    
    def quick_train(self, train_df, valid_df, feature_cols, n_trials=10):
        """高速学習（トライアル数を減らす）"""
        group_train = train_df.groupby('race_id').size().tolist()
        group_valid = valid_df.groupby('race_id').size().tolist()
        
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
            
            model = lgb.LGBMRanker(**params, n_estimators=500)
            model.fit(train_df[feature_cols], train_df['target_relevance'],
                      group=group_train, sample_weight=train_df['sample_weight'],
                      eval_set=[(valid_df[feature_cols], valid_df['target_relevance'])],
                      eval_group=[group_valid],
                      callbacks=[lgb.early_stopping(30, verbose=False)])
            
            valid_roi = calculate_roi(valid_df, model.predict(valid_df[feature_cols]))
            return valid_roi
        
        optuna.logging.set_verbosity(optuna.logging.WARNING)
        study = optuna.create_study(direction='maximize')
        study.optimize(objective, n_trials=n_trials, show_progress_bar=False)
        
        return study.best_value
    
    def run(self):
        train_data, races_df, pedigree_df, shutuba_df = self.load_data()
        train_data = self.generate_all_features(train_data, races_df, pedigree_df, shutuba_df)
        train_data = self.prepare_target(train_data)
        
        train_df = train_data[train_data['race_date'] < '2023-01-01'].copy()
        valid_df = train_data[(train_data['race_date'] >= '2023-01-01') & (train_data['race_date'] < '2024-01-01')].copy()
        test_df = train_data[train_data['race_date'] >= '2024-01-01'].copy()
        
        logging.info(f"Train: {len(train_df):,}, Valid: {len(valid_df):,}, Test: {len(test_df):,}")
        
        # ベース特徴量でのROI
        base_features = self.get_base_features(train_data)
        logging.info(f"ベース特徴量数: {len(base_features)}")
        
        logging.info("="*60)
        logging.info("Forward Selection開始")
        logging.info("="*60)
        
        # ベースラインROI
        logging.info(f"\n【ベースライン（v3.3ベース特徴量のみ）】")
        base_roi = self.quick_train(train_df, valid_df, base_features, n_trials=20)
        logging.info(f"  Valid ROI: {base_roi:.2%}")
        
        # 各新規特徴量を追加してROI変化を測定
        results = []
        current_features = base_features.copy()
        
        for new_feat in self.NEW_FEATURES:
            if new_feat not in train_data.columns:
                logging.info(f"\n【{new_feat}】- スキップ（カラムなし）")
                continue
            
            test_features = current_features + [new_feat]
            logging.info(f"\n【{new_feat}を追加】")
            new_roi = self.quick_train(train_df, valid_df, test_features, n_trials=10)
            delta = new_roi - base_roi
            logging.info(f"  Valid ROI: {new_roi:.2%} (Δ{delta:+.2%})")
            
            results.append({
                'feature': new_feat,
                'valid_roi': new_roi,
                'delta': delta,
                'add': delta > 0.005  # +0.5%以上なら採用
            })
        
        # 結果サマリー
        logging.info("="*60)
        logging.info("【Forward Selection結果】")
        logging.info("="*60)
        logging.info(f"ベースラインROI: {base_roi:.2%}")
        
        adopted = []
        for r in sorted(results, key=lambda x: x['delta'], reverse=True):
            status = "✅ 採用" if r['add'] else "❌ 不採用"
            logging.info(f"  {r['feature']}: {r['valid_roi']:.2%} (Δ{r['delta']:+.2%}) {status}")
            if r['add']:
                adopted.append(r['feature'])
        
        # 最終モデル学習
        final_features = base_features + adopted
        logging.info(f"\n最終特徴量数: {len(final_features)} (ベース{len(base_features)} + 採用{len(adopted)})")
        
        # 保存
        output = {
            'base_roi': base_roi,
            'results': results,
            'adopted_features': adopted,
            'final_feature_count': len(final_features),
        }
        with open(self.output_dir / 'forward_selection_results.json', 'w', encoding='utf-8') as f:
            json.dump(output, f, ensure_ascii=False, indent=2)
        
        logging.info(f"\n保存完了: {self.output_dir}")
        return results


if __name__ == "__main__":
    MuV56Trainer().run()
