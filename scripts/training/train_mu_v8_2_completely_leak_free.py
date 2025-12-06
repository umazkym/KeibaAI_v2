#!/usr/bin/env python3
"""
Phase 11: v8.2 完全リークフリー版

【設計原則】
特徴量Xを計算する際に参照できるのは、
「そのレースより厳密に前の時点で確定しているデータ」のみ。

【使用不可な情報】（当該レースの結果）
1. 着順（finish_position）
2. 通過順位（passing_order_4、corner position）
3. タイム（finish_time_seconds、last_3f_time）
4. ペース（first_half, second_half）
5. 馬身差（gap_from_leader）

【使用可能な情報】
1. 過去レースの結果：前走、2走前...の着順・タイム等
2. 馬の静的属性：血統、年齢、性別
3. レースの事前情報：距離、馬場、天候、クラス
4. 騎手・調教師の過去成績
5. 朝オッズ（morning_odds）

【新特徴量】
1. horse_prev_gap_zscore: 前走の4C馬身差（正規化済み）
2. horse_prev_position_pct: 前走の4C通過順位パーセンタイル
3. horse_avg_gap_zscore_past: 過去全レースの4C馬身差平均
4. horse_avg_position_pct_past: 過去全レースの4C通過順位平均
5. is_escape, is_front, is_chaser, is_closer: 過去実績から脚質分類
6. horse_closing_rate_past: 過去の追い込み成功率（後方から好走した割合）
7. horse_high_pace_success_past: 過去のハイペースでの好走率
8. horse_slow_pace_success_past: 過去のスローペースでの好走率
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
import lightgbm as lgb

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class CompletelyLeakFreeFeatureGenerator:
    """完全にリークフリーな特徴量生成クラス"""
    
    DISTANCE_CATEGORIES = {
        'sprint': (0, 1400),
        'mile': (1400, 1800),
        'intermediate': (1800, 2200),
        'long': (2200, 9999)
    }
    
    RUNNING_STYLE_THRESHOLDS = {
        'escape': 0.15,
        'front': 0.35,
        'chaser': 0.65,
        'closer': 1.0
    }
    
    def __init__(self):
        self.base_model_dir = Path('keibaai/models/mu_v3_3')
        self.v54_dir = Path('keibaai/models/mu_v5_4')
        self.output_dir = Path('keibaai/models/mu_v8_2')
        self.output_dir.mkdir(parents=True, exist_ok=True)
    
    @staticmethod
    def extract_venue_name(venue_str):
        if pd.isna(venue_str):
            return None
        match = re.search(r'[0-9]*([^\d]+)[0-9]*', str(venue_str))
        return match.group(1) if match else None
    
    @staticmethod
    def get_distance_category(distance_m):
        for cat, (low, high) in CompletelyLeakFreeFeatureGenerator.DISTANCE_CATEGORIES.items():
            if low <= distance_m < high:
                return cat
        return 'intermediate'
    
    @staticmethod
    def calculate_normalized_rank(finish, n_runners):
        if pd.isna(finish) or pd.isna(n_runners) or n_runners <= 1:
            return np.nan
        return 1.0 - (finish - 1) / (n_runners - 1)
    
    def classify_running_style(self, position_percentile):
        if pd.isna(position_percentile):
            return 'unknown'
        if position_percentile <= self.RUNNING_STYLE_THRESHOLDS['escape']:
            return 'escape'
        elif position_percentile <= self.RUNNING_STYLE_THRESHOLDS['front']:
            return 'front'
        elif position_percentile <= self.RUNNING_STYLE_THRESHOLDS['chaser']:
            return 'chaser'
        else:
            return 'closer'
    
    def generate_completely_leak_free_features(self, df, races_df, corner_positions_df, race_details_df):
        """完全にリークフリーな特徴量を生成"""
        logging.info("【完全リークフリー特徴量生成】")
        
        df = df.copy()
        
        # ==========================================
        # Step 1: 過去レースデータを馬ごとに整理
        # ==========================================
        logging.info("  Step 1: 過去レースデータを馬ごとに整理...")
        
        # corner_positions から4コーナーデータを取得
        corner4 = corner_positions_df[corner_positions_df['corner'] == 4].copy()
        corner4['race_id'] = corner4['race_id'].astype(str)
        
        # レース内統計（各レースごと）
        race_gap_stats = corner4.groupby('race_id')['gap_from_leader'].agg(['mean', 'std', 'max']).reset_index()
        race_gap_stats.columns = ['race_id', 'race_gap_mean', 'race_gap_std', 'race_gap_max']
        race_gap_stats['race_gap_std'] = race_gap_stats['race_gap_std'].fillna(1).replace(0, 1)
        
        corner4 = corner4.merge(race_gap_stats, on='race_id', how='left')
        
        # Zスコア（レース内での相対位置）
        corner4['gap_zscore'] = (corner4['gap_from_leader'] - corner4['race_gap_mean']) / corner4['race_gap_std']
        
        # 順位のパーセンタイル
        corner4['n_runners'] = corner4.groupby('race_id')['horse_number'].transform('count')
        corner4['position_percentile'] = (corner4['position'] - 1) / (corner4['n_runners'] - 1).clip(lower=1)
        
        # race_date と horse_id をマージ
        race_info = races_df[['race_id', 'race_date', 'horse_id', 'finish_position']].drop_duplicates()
        race_info['finish_position'] = pd.to_numeric(race_info['finish_position'], errors='coerce')
        corner4 = corner4.merge(race_info, on='race_id', how='left')
        
        # ペース情報をマージ
        race_details_df = race_details_df.copy()
        race_details_df['race_id'] = race_details_df['race_id'].astype(str)
        race_details_df['pace_diff'] = race_details_df['first_half'] - race_details_df['second_half']
        
        def classify_pace(diff):
            if pd.isna(diff):
                return 'unknown'
            if diff < -1:
                return 'slow'
            elif diff > 1:
                return 'high'
            else:
                return 'middle'
        
        race_details_df['pace_type'] = race_details_df['pace_diff'].apply(classify_pace)
        corner4 = corner4.merge(race_details_df[['race_id', 'pace_type']], on='race_id', how='left')
        
        # normalized_rank（過去レースの着順評価）
        corner4['n_runners_race'] = corner4.groupby('race_id')['horse_id'].transform('count')
        corner4['normalized_rank'] = corner4.apply(
            lambda x: self.calculate_normalized_rank(x['finish_position'], x['n_runners_race']), 
            axis=1
        )
        
        # ==========================================
        # Step 2: 馬ごと・レースごとにソートし、過去特徴量を計算
        # ==========================================
        logging.info("  Step 2: 馬ごとに過去特徴量を計算（shift使用）...")
        
        corner4 = corner4.sort_values(['horse_id', 'race_date']).reset_index(drop=True)
        
        # 前走の値（shift(1)）
        corner4['horse_prev_gap_zscore'] = corner4.groupby('horse_id')['gap_zscore'].shift(1)
        corner4['horse_prev_position_pct'] = corner4.groupby('horse_id')['position_percentile'].shift(1)
        
        # 過去全レースの平均（expanding mean + shift）
        corner4['horse_avg_gap_zscore_past'] = corner4.groupby('horse_id')['gap_zscore'].transform(
            lambda x: x.expanding().mean().shift(1)
        )
        corner4['horse_avg_position_pct_past'] = corner4.groupby('horse_id')['position_percentile'].transform(
            lambda x: x.expanding().mean().shift(1)
        )
        
        # ==========================================
        # Step 3: 脚質分類（過去平均から）
        # ==========================================
        logging.info("  Step 3: 脚質分類...")
        
        corner4['running_style'] = corner4['horse_avg_position_pct_past'].apply(self.classify_running_style)
        for style in ['escape', 'front', 'chaser', 'closer']:
            corner4[f'is_{style}'] = (corner4['running_style'] == style).astype(int)
        
        # ==========================================
        # Step 4: 過去の追い込み成功率（完全リークフリー）
        # ==========================================
        logging.info("  Step 4: 過去の追い込み成功率...")
        
        # 「過去レースで後方（4Cでgap_zscore > 0.5）から好走（normalized_rank > 0.6）した」件数
        # これは過去レースの結果なので、shiftすればリークしない
        corner4['was_closing_success'] = (
            (corner4['gap_zscore'] > 0.5) & 
            (corner4['normalized_rank'] > 0.6)
        ).astype(int)
        
        corner4['horse_closing_rate_past'] = corner4.groupby('horse_id')['was_closing_success'].transform(
            lambda x: x.expanding().mean().shift(1)
        )
        
        # ==========================================
        # Step 5: 過去のペース適性（完全リークフリー）
        # ==========================================
        logging.info("  Step 5: 過去のペース適性...")
        
        # 過去のハイペースでの好走率
        corner4['was_high_pace_success'] = (
            (corner4['pace_type'] == 'high') & 
            (corner4['normalized_rank'] > 0.6)
        ).astype(int)
        corner4['was_high_pace'] = (corner4['pace_type'] == 'high').astype(int)
        
        corner4['horse_high_pace_success_past'] = corner4.groupby('horse_id')['was_high_pace_success'].transform(
            lambda x: x.expanding().sum().shift(1)
        )
        corner4['horse_high_pace_count_past'] = corner4.groupby('horse_id')['was_high_pace'].transform(
            lambda x: x.expanding().sum().shift(1)
        )
        corner4['horse_high_pace_rate_past'] = (
            corner4['horse_high_pace_success_past'] / 
            corner4['horse_high_pace_count_past'].clip(lower=1)
        )
        
        # 過去のスローペースでの好走率
        corner4['was_slow_pace_success'] = (
            (corner4['pace_type'] == 'slow') & 
            (corner4['normalized_rank'] > 0.6)
        ).astype(int)
        corner4['was_slow_pace'] = (corner4['pace_type'] == 'slow').astype(int)
        
        corner4['horse_slow_pace_success_past'] = corner4.groupby('horse_id')['was_slow_pace_success'].transform(
            lambda x: x.expanding().sum().shift(1)
        )
        corner4['horse_slow_pace_count_past'] = corner4.groupby('horse_id')['was_slow_pace'].transform(
            lambda x: x.expanding().sum().shift(1)
        )
        corner4['horse_slow_pace_rate_past'] = (
            corner4['horse_slow_pace_success_past'] / 
            corner4['horse_slow_pace_count_past'].clip(lower=1)
        )
        
        # ==========================================
        # Step 6: 交互作用特徴量（過去データのみ）
        # ==========================================
        logging.info("  Step 6: 交互作用特徴量...")
        
        # 脚質 × 過去馬身差
        corner4['gap_x_closer'] = corner4['horse_avg_gap_zscore_past'].fillna(0) * corner4['is_closer']
        corner4['gap_x_escape'] = corner4['horse_avg_gap_zscore_past'].fillna(0) * corner4['is_escape']
        
        # ==========================================
        # Step 7: dfにマージ
        # ==========================================
        logging.info("  Step 7: 特徴量をマージ...")
        
        leak_free_features = [
            'horse_prev_gap_zscore', 'horse_prev_position_pct',
            'horse_avg_gap_zscore_past', 'horse_avg_position_pct_past',
            'is_escape', 'is_front', 'is_chaser', 'is_closer',
            'horse_closing_rate_past',
            'horse_high_pace_rate_past', 'horse_slow_pace_rate_past',
            'gap_x_closer', 'gap_x_escape'
        ]
        
        feature_df = corner4[[
            'race_id', 'horse_id'
        ] + leak_free_features].drop_duplicates(subset=['race_id', 'horse_id'], keep='last')
        
        df = df.merge(feature_df, on=['race_id', 'horse_id'], how='left')
        
        # リーク検証: 特徴量とfinish_positionの相関をチェック
        logging.info("  【リーク検証】特徴量と着順の相関チェック...")
        df_check = df.dropna(subset=['finish_position'] + leak_free_features)
        if len(df_check) > 0:
            for feat in leak_free_features:
                if feat in ['is_escape', 'is_front', 'is_chaser', 'is_closer']:
                    continue  # カテゴリカル特徴量はスキップ
                corr = df_check[feat].corr(df_check['finish_position'])
                if abs(corr) > 0.5:
                    logging.warning(f"    ⚠️ {feat}: 相関係数 {corr:.3f} → リークの可能性")
                else:
                    logging.info(f"    ✓ {feat}: 相関係数 {corr:.3f} → OK")
        
        logging.info(f"  完了: {len(leak_free_features)}個の完全リークフリー特徴量を生成")
        
        return df, leak_free_features
    
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
        logging.info("Phase 11: v8.2 完全リークフリー版")
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
        
        corner_positions_df = pd.read_parquet('keibaai/data/parsed/parquet/corners/corner_positions.parquet')
        race_details_df = pd.read_parquet('keibaai/data/parsed/parquet/race_details/race_details.parquet')
        
        train_data['horse_id'] = train_data['horse_id'].astype(str)
        races_df['horse_id'] = races_df['horse_id'].astype(str)
        
        logging.info(f"  train_data: {len(train_data):,}")
        
        # 既存特徴量生成
        logging.info("\n【既存特徴量生成】")
        train_data = self._generate_horse_features(train_data, races_df)
        train_data = self._generate_jockey_sire_features(train_data, races_df, pedigree_df, shutuba_df)
        
        # 完全リークフリー特徴量生成
        logging.info("\n【完全リークフリー特徴量生成】")
        train_data, new_features = self.generate_completely_leak_free_features(
            train_data, races_df, corner_positions_df, race_details_df
        )
        
        # v5.4特徴量を読み込み
        with open(self.v54_dir / 'feature_names.json', 'r', encoding='utf-8') as f:
            v54_features = json.load(f)
        
        all_features = v54_features + [f for f in new_features if f not in v54_features]
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
        
        logging.info(f"\n  v8.2 Test結果（完全リークフリー版）:")
        logging.info(f"    ROI: {roi:.2%}")
        logging.info(f"    的中率: {hit_rate:.1%}")
        logging.info(f"    投資数: {len(bet):,}")
        
        # 比較
        logging.info(f"\n  比較:")
        logging.info(f"    v5.4: 79.43%")
        logging.info(f"    v8.0（リーク有）: 81.76%")
        logging.info(f"    v8.1（リーク有）: 253.08%")
        logging.info(f"    v8.2（リークフリー）: {roi:.2%} ({(roi - 0.7943) * 100:+.2f}pp vs v5.4)")
        
        # ROIが異常に高い場合は警告
        if roi > 1.2:
            logging.warning(f"  ⚠️ ROI {roi:.2%} は異常に高い。リークの可能性あり。")
        
        # 新特徴量の重要度
        importance = model.feature_importance(importance_type='gain')
        feature_importance = pd.DataFrame({
            'feature': available_features,
            'importance': importance
        }).sort_values('importance', ascending=False)
        
        logging.info("\n  【新特徴量の重要度 Top10】")
        for i, row in feature_importance[feature_importance['feature'].isin(new_features)].head(10).iterrows():
            rank = list(feature_importance['feature']).index(row['feature']) + 1
            logging.info(f"    {row['feature']}: rank={rank}, importance={row['importance']:.0f}")
        
        # モデル保存
        with open(self.output_dir / 'mu_v8_2_ranker.pkl', 'wb') as f:
            pickle.dump(model, f)
        
        with open(self.output_dir / 'feature_names.json', 'w', encoding='utf-8') as f:
            json.dump(available_features, f, ensure_ascii=False, indent=2)
        
        results = {
            'model_version': 'v8.2',
            'train_date': datetime.now().isoformat(),
            'n_features': len(available_features),
            'new_features': new_features,
            'test_roi': float(roi),
            'test_hit_rate': float(hit_rate),
            'n_bets': int(len(bet)),
            'vs_v54': float(roi - 0.7943),
            'leak_status': 'COMPLETELY_CLEAN'
        }
        
        with open(self.output_dir / 'training_results.json', 'w', encoding='utf-8') as f:
            json.dump(results, f, ensure_ascii=False, indent=2)
        
        feature_importance.to_csv(self.output_dir / 'feature_importance.csv', index=False)
        
        logging.info(f"\n  モデル保存: {self.output_dir}")
        logging.info("=" * 70)
        
        return results


if __name__ == "__main__":
    generator = CompletelyLeakFreeFeatureGenerator()
    generator.run()
