#!/usr/bin/env python3
"""
Phase 10: 馬身差・ペース特徴量を追加してモデル再学習

【新特徴量】
1. 馬身差特徴量（corner_positions.parquetから）
   - horse_avg_gap_4c: 4コーナーでの平均馬身差（過去実績）
   - horse_front_rate: 前走り率（4C上位3位以内の割合）
   
2. ペース特徴量（race_details.parquetから）
   - race_pace_type: ハイペース/ミドル/スローの分類
   - horse_pace_preference: 馬のペース適性（過去実績）
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
import re
from datetime import datetime
from scipy.special import softmax
import lightgbm as lgb

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class Phase10Trainer:
    """Phase 10: 新特徴量追加とモデル再学習"""
    
    DISTANCE_CATEGORIES = {
        'sprint': (0, 1400),
        'mile': (1400, 1800),
        'intermediate': (1800, 2200),
        'long': (2200, 9999)
    }
    
    def __init__(self):
        self.base_model_dir = Path('keibaai/models/mu_v3_3')
        self.v54_dir = Path('keibaai/models/mu_v5_4')
        self.output_dir = Path('keibaai/models/mu_v7_0')
        self.output_dir.mkdir(parents=True, exist_ok=True)
    
    @staticmethod
    def extract_venue_name(venue_str):
        if pd.isna(venue_str):
            return None
        match = re.search(r'[0-9]*([^\d]+)[0-9]*', str(venue_str))
        return match.group(1) if match else None
    
    @staticmethod
    def get_distance_category(distance_m):
        for cat, (low, high) in Phase10Trainer.DISTANCE_CATEGORIES.items():
            if low <= distance_m < high:
                return cat
        return 'intermediate'
    
    @staticmethod
    def calculate_normalized_rank(finish, n_runners):
        if pd.isna(finish) or pd.isna(n_runners) or n_runners <= 1:
            return np.nan
        return 1.0 - (finish - 1) / (n_runners - 1)
    
    def generate_gap_features(self, df, corner_positions_df):
        """馬身差特徴量を生成"""
        logging.info("馬身差特徴量を生成中...")
        
        # 4コーナーのみ使用
        corner4 = corner_positions_df[corner_positions_df['corner'] == 4].copy()
        
        # races_dfから必要な情報を取得するためにマージ
        # race_id + horse_number でマージ
        corner4['race_id'] = corner4['race_id'].astype(str)
        corner4['horse_number'] = corner4['horse_number'].astype(int)
        
        # dfにもrace_id, horse_number があるはず
        df = df.copy()
        df['race_id'] = df['race_id'].astype(str)
        
        # 馬の過去の4C成績を集計
        # まずcorner4にrace_dateを付与
        race_dates = df[['race_id', 'race_date', 'horse_id', 'horse_number']].drop_duplicates()
        corner4 = corner4.merge(race_dates[['race_id', 'race_date', 'horse_id']], on=['race_id'], how='left')
        
        # 馬ごとにソート
        corner4 = corner4.sort_values(['horse_id', 'race_date']).reset_index(drop=True)
        
        # 過去平均馬身差（expanding mean with shift）
        corner4['horse_avg_gap_4c'] = corner4.groupby('horse_id')['gap_from_leader'].transform(
            lambda x: x.expanding().mean().shift(1)
        )
        
        # 前走り率（4C上位3位以内の割合）
        corner4['is_front'] = (corner4['position'] <= 3).astype(int)
        corner4['horse_front_rate'] = corner4.groupby('horse_id')['is_front'].transform(
            lambda x: x.expanding().mean().shift(1)
        )
        
        # dfにマージ
        gap_features = corner4[['race_id', 'horse_id', 'horse_avg_gap_4c', 'horse_front_rate']].drop_duplicates(
            subset=['race_id', 'horse_id'], keep='last'
        )
        
        df = df.merge(gap_features, on=['race_id', 'horse_id'], how='left')
        
        logging.info(f"  horse_avg_gap_4c: mean={df['horse_avg_gap_4c'].mean():.2f}")
        logging.info(f"  horse_front_rate: mean={df['horse_front_rate'].mean():.2%}")
        
        return df
    
    def generate_pace_features(self, df, race_details_df):
        """ペース特徴量を生成"""
        logging.info("ペース特徴量を生成中...")
        
        race_details_df = race_details_df.copy()
        race_details_df['race_id'] = race_details_df['race_id'].astype(str)
        
        # ペースタイプ分類（first_half / second_half）
        race_details_df['pace_diff'] = race_details_df['first_half'] - race_details_df['second_half']
        
        # ハイペース: 前半が速い（pace_diff < -1）
        # スロー: 後半が速い（pace_diff > 1）
        # ミドル: それ以外
        def classify_pace(diff):
            if pd.isna(diff):
                return 'unknown'
            if diff < -1:
                return 'high'
            elif diff > 1:
                return 'slow'
            else:
                return 'middle'
        
        race_details_df['pace_type'] = race_details_df['pace_diff'].apply(classify_pace)
        
        # dfにマージ
        df = df.merge(
            race_details_df[['race_id', 'first_half', 'second_half', 'pace_diff', 'pace_type']], 
            on='race_id', 
            how='left'
        )
        
        # 馬のペース適性（過去のペースタイプ別成績）
        df = df.sort_values(['horse_id', 'race_date']).reset_index(drop=True)
        
        # 過去のハイペースでの成績
        df['was_high_pace'] = (df['pace_type'] == 'high').astype(int)
        df['high_pace_finish'] = df['finish_position'].where(df['was_high_pace'] == 1)
        
        # 過去平均（expanding）
        df['horse_high_pace_avg'] = df.groupby('horse_id')['high_pace_finish'].transform(
            lambda x: x.expanding().mean().shift(1)
        )
        
        logging.info(f"  pace_type分布: {df['pace_type'].value_counts().to_dict()}")
        
        return df
    
    def _generate_horse_features(self, df, races_df):
        """馬特徴量生成（v5.4と同様）"""
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
        
        merge_df = perf[['horse_id', 'race_date'] + horse_features].drop_duplicates(subset=['horse_id', 'race_date'], keep='last')
        return df.merge(merge_df, on=['horse_id', 'race_date'], how='left')
    
    def _generate_jockey_sire_features(self, df, races_df, pedigree_df, shutuba_df):
        """騎手・種牡馬特徴量"""
        SMOOTHING_C = 30
        perf = races_df.copy()
        perf['n_runners'] = perf.groupby('race_id')['horse_id'].transform('count')
        perf['finish_position'] = pd.to_numeric(perf['finish_position'], errors='coerce')
        perf['normalized_rank'] = perf.apply(
            lambda x: self.calculate_normalized_rank(x['finish_position'], x['n_runners']), axis=1
        )
        perf = perf.dropna(subset=['finish_position'])
        
        result = df.copy()
        
        if 'jockey_id' in perf.columns:
            jockey_base = perf.groupby('jockey_id').agg({'normalized_rank': 'sum', 'race_id': 'count'}).reset_index()
            jockey_base.columns = ['jockey_id', 'nr_sum', 'count']
            jockey_base['jockey_nr_global'] = (jockey_base['nr_sum'] + 0.5 * SMOOTHING_C) / (jockey_base['count'] + SMOOTHING_C)
            
            if 'jockey_id' not in result.columns and shutuba_df is not None:
                jockey_map = shutuba_df[['race_id', 'horse_id', 'jockey_id']].drop_duplicates(subset=['race_id', 'horse_id'])
                jockey_map['horse_id'] = jockey_map['horse_id'].astype(str)
                result = result.merge(jockey_map, on=['race_id', 'horse_id'], how='left')
            result = result.merge(jockey_base[['jockey_id', 'jockey_nr_global']], on='jockey_id', how='left')
        
        if pedigree_df is not None:
            sire_map = pedigree_df[pedigree_df['generation'] == 1][['horse_id', 'ancestor_id']].drop_duplicates('horse_id')
            sire_map.columns = ['horse_id', 'sire_id']
            sire_map['horse_id'] = sire_map['horse_id'].astype(str)
            perf['horse_id'] = perf['horse_id'].astype(str)
            perf_with_sire = perf.merge(sire_map, on='horse_id', how='left').dropna(subset=['sire_id'])
            
            sire_base = perf_with_sire.groupby('sire_id').agg({'normalized_rank': 'sum', 'race_id': 'count'}).reset_index()
            sire_base.columns = ['sire_id', 'nr_sum', 'count']
            sire_base['sire_nr_global'] = (sire_base['nr_sum'] + 0.5 * SMOOTHING_C) / (sire_base['count'] + SMOOTHING_C)
            
            if 'sire_id' not in result.columns:
                result = result.merge(sire_map, on='horse_id', how='left')
            result = result.merge(sire_base[['sire_id', 'sire_nr_global']], on='sire_id', how='left')
        
        return result
    
    def run(self):
        """メイン実行"""
        logging.info("=" * 70)
        logging.info("Phase 10: 新特徴量追加とモデル再学習")
        logging.info("=" * 70)
        
        # データ読み込み
        logging.info("\n【データ読み込み】")
        
        train_data = pd.read_parquet(self.base_model_dir / 'train_data_mu_v3_3.parquet')
        train_data['race_date'] = pd.to_datetime(train_data['race_date'])
        
        races_df = pd.read_parquet('keibaai/data/parsed/parquet/races/races.parquet')
        races_df['race_date'] = pd.to_datetime(races_df['race_date'])
        
        shutuba_df = pd.read_parquet('keibaai/data/parsed/parquet/shutuba/shutuba.parquet')
        
        pedigree_path = Path('keibaai/data/parsed/parquet/pedigrees/pedigrees.parquet')
        pedigree_df = pd.read_parquet(pedigree_path) if pedigree_path.exists() else None
        
        # 新データ
        corner_positions_df = pd.read_parquet('keibaai/data/parsed/parquet/corners/corner_positions.parquet')
        race_details_df = pd.read_parquet('keibaai/data/parsed/parquet/race_details/race_details.parquet')
        
        logging.info(f"  corner_positions: {len(corner_positions_df):,}")
        logging.info(f"  race_details: {len(race_details_df):,}")
        
        train_data['horse_id'] = train_data['horse_id'].astype(str)
        races_df['horse_id'] = races_df['horse_id'].astype(str)
        
        # 既存特徴量生成
        logging.info("\n【既存特徴量生成】")
        train_data = self._generate_horse_features(train_data, races_df)
        train_data = self._generate_jockey_sire_features(train_data, races_df, pedigree_df, shutuba_df)
        
        # 新特徴量生成
        logging.info("\n【新特徴量生成】")
        train_data = self.generate_gap_features(train_data, corner_positions_df)
        train_data = self.generate_pace_features(train_data, race_details_df)
        
        # v5.4特徴量を読み込み
        with open(self.v54_dir / 'feature_names.json', 'r', encoding='utf-8') as f:
            v54_features = json.load(f)
        
        # 新特徴量を追加
        new_features = ['horse_avg_gap_4c', 'horse_front_rate', 'first_half', 'second_half', 'pace_diff', 'horse_high_pace_avg']
        all_features = v54_features + [f for f in new_features if f not in v54_features]
        
        # 利用可能特徴量をフィルタ
        available_features = [f for f in all_features if f in train_data.columns]
        logging.info(f"\n  利用可能特徴量: {len(available_features)} (v5.4: {len(v54_features)}, +{len(available_features) - len(v54_features)})")
        
        # 期間分割
        train_df = train_data[(train_data['race_date'] >= '2020-01-01') & (train_data['race_date'] < '2023-01-01')].copy()
        valid_df = train_data[(train_data['race_date'] >= '2023-01-01') & (train_data['race_date'] < '2024-01-01')].copy()
        test_df = train_data[train_data['race_date'] >= '2024-01-01'].copy()
        
        logging.info(f"\n  Train: {len(train_df):,}")
        logging.info(f"  Valid: {len(valid_df):,}")
        logging.info(f"  Test: {len(test_df):,}")
        
        # 欠損値補完
        for col in available_features:
            if col in train_df.columns:
                median_val = train_df[col].median()
                if pd.isna(median_val):
                    median_val = 0
                train_df[col] = train_df[col].fillna(median_val)
                valid_df[col] = valid_df[col].fillna(median_val)
                test_df[col] = test_df[col].fillna(median_val)
        
        # モデル学習
        logging.info("\n【モデル学習】")
        
        X_train = train_df[available_features]
        y_train = (train_df['finish_position'] == 1).astype(int)
        train_groups = train_df.groupby('race_id').size().values
        
        X_valid = valid_df[available_features]
        y_valid = (valid_df['finish_position'] == 1).astype(int)
        valid_groups = valid_df.groupby('race_id').size().values
        
        train_dataset = lgb.Dataset(X_train, label=y_train, group=train_groups)
        valid_dataset = lgb.Dataset(X_valid, label=y_valid, group=valid_groups, reference=train_dataset)
        
        params = {
            'objective': 'lambdarank',
            'metric': 'ndcg',
            'ndcg_eval_at': [1, 3, 5],
            'learning_rate': 0.05,
            'num_leaves': 31,
            'max_depth': 6,
            'feature_fraction': 0.8,
            'bagging_fraction': 0.8,
            'bagging_freq': 5,
            'verbose': -1,
            'seed': 42
        }
        
        model = lgb.train(
            params,
            train_dataset,
            num_boost_round=500,
            valid_sets=[valid_dataset],
            callbacks=[lgb.early_stopping(stopping_rounds=50), lgb.log_evaluation(100)]
        )
        
        # 評価
        logging.info("\n【評価】")
        
        X_test = test_df[available_features]
        test_df['score'] = model.predict(X_test)
        test_df['rank_pred'] = test_df.groupby('race_id')['score'].rank(ascending=False, method='first')
        
        # ROI計算
        bet = test_df[test_df['rank_pred'] == 1]
        hits = bet[bet['finish_position'] == 1]
        roi = hits['win_odds'].sum() / len(bet)
        hit_rate = len(hits) / len(bet)
        
        logging.info(f"\n  v7.0 Test結果:")
        logging.info(f"    ROI: {roi:.2%}")
        logging.info(f"    的中率: {hit_rate:.1%}")
        logging.info(f"    投資数: {len(bet):,}")
        
        # v5.4との比較
        logging.info(f"\n  v5.4比較: {roi:.2%} vs 79.43% ({(roi - 0.7943) * 100:+.2f}pp)")
        
        # 新特徴量の重要度
        importance = model.feature_importance(importance_type='gain')
        feature_importance = pd.DataFrame({
            'feature': available_features,
            'importance': importance
        }).sort_values('importance', ascending=False)
        
        logging.info("\n  【新特徴量の重要度】")
        for f in new_features:
            if f in feature_importance['feature'].values:
                rank = feature_importance[feature_importance['feature'] == f].index[0] + 1
                imp = feature_importance[feature_importance['feature'] == f]['importance'].values[0]
                logging.info(f"    {f}: rank={rank}, importance={imp:.0f}")
        
        # モデル保存
        with open(self.output_dir / 'mu_v7_0_ranker.pkl', 'wb') as f:
            pickle.dump(model, f)
        
        with open(self.output_dir / 'feature_names.json', 'w', encoding='utf-8') as f:
            json.dump(available_features, f, ensure_ascii=False, indent=2)
        
        results = {
            'model_version': 'v7.0',
            'train_date': datetime.now().isoformat(),
            'n_features': len(available_features),
            'new_features': new_features,
            'test_roi': float(roi),
            'test_hit_rate': float(hit_rate),
            'n_bets': int(len(bet)),
            'vs_v54': float(roi - 0.7943)
        }
        
        with open(self.output_dir / 'training_results.json', 'w', encoding='utf-8') as f:
            json.dump(results, f, ensure_ascii=False, indent=2)
        
        logging.info(f"\n  モデル保存: {self.output_dir}")
        logging.info("=" * 70)
        
        return results


if __name__ == "__main__":
    trainer = Phase10Trainer()
    trainer.run()
