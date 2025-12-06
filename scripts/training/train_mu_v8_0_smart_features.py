#!/usr/bin/env python3
"""
Phase 11: スマート特徴量エンジニアリング

【目的】
生データをAIが認識しやすい形に前処理する

【改善点】
1. 正規化（Zスコア）：距離・馬場ごとの期待値からの偏差
2. 相対化（パーセンタイル）：レース内での相対順位（0-1スケール）
3. 脚質分類：過去成績から逃げ/先行/差し/追込を分類
4. ペース適性スコア：馬のペース preference とレースペースのマッチング
5. 交互作用特徴量：条件 × 適性の組み合わせ
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


class SmartFeatureGenerator:
    """スマート特徴量生成クラス"""
    
    DISTANCE_CATEGORIES = {
        'sprint': (0, 1400),
        'mile': (1400, 1800),
        'intermediate': (1800, 2200),
        'long': (2200, 9999)
    }
    
    # 脚質分類の閾値（4コーナー通過順位パーセンタイル）
    RUNNING_STYLE_THRESHOLDS = {
        'escape': 0.15,    # 逃げ: 上位15%
        'front': 0.35,     # 先行: 16-35%
        'chaser': 0.65,    # 差し: 36-65%
        'closer': 1.0      # 追込: 66%以上
    }
    
    def __init__(self):
        self.base_model_dir = Path('keibaai/models/mu_v3_3')
        self.v54_dir = Path('keibaai/models/mu_v5_4')
        self.output_dir = Path('keibaai/models/mu_v8_0')
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # 条件ごとの期待値（学習データから計算）
        self.condition_stats = {}
    
    @staticmethod
    def extract_venue_name(venue_str):
        if pd.isna(venue_str):
            return None
        match = re.search(r'[0-9]*([^\d]+)[0-9]*', str(venue_str))
        return match.group(1) if match else None
    
    @staticmethod
    def get_distance_category(distance_m):
        for cat, (low, high) in SmartFeatureGenerator.DISTANCE_CATEGORIES.items():
            if low <= distance_m < high:
                return cat
        return 'intermediate'
    
    @staticmethod
    def calculate_normalized_rank(finish, n_runners):
        if pd.isna(finish) or pd.isna(n_runners) or n_runners <= 1:
            return np.nan
        return 1.0 - (finish - 1) / (n_runners - 1)
    
    def classify_running_style(self, position_percentile):
        """通過順位パーセンタイルから脚質を分類"""
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
    
    def generate_normalized_features(self, df, races_df, corner_positions_df, race_details_df):
        """正規化・相対化された特徴量を生成"""
        logging.info("【スマート特徴量生成】")
        
        df = df.copy()
        
        # ==========================================
        # 1. 馬身差の正規化（レース内Zスコア）
        # ==========================================
        logging.info("  1. 馬身差の正規化...")
        
        corner4 = corner_positions_df[corner_positions_df['corner'] == 4].copy()
        corner4['race_id'] = corner4['race_id'].astype(str)
        
        # レース内統計
        race_gap_stats = corner4.groupby('race_id')['gap_from_leader'].agg(['mean', 'std', 'max']).reset_index()
        race_gap_stats.columns = ['race_id', 'race_gap_mean', 'race_gap_std', 'race_gap_max']
        race_gap_stats['race_gap_std'] = race_gap_stats['race_gap_std'].fillna(1).replace(0, 1)
        
        corner4 = corner4.merge(race_gap_stats, on='race_id', how='left')
        
        # Zスコア: (値 - 平均) / 標準偏差
        corner4['gap_zscore'] = (corner4['gap_from_leader'] - corner4['race_gap_mean']) / corner4['race_gap_std']
        
        # パーセンタイル: 値 / 最大値
        corner4['gap_percentile'] = corner4['gap_from_leader'] / corner4['race_gap_max'].clip(lower=1)
        
        # 順位のパーセンタイル
        corner4['n_runners'] = corner4.groupby('race_id')['horse_number'].transform('count')
        corner4['position_percentile'] = (corner4['position'] - 1) / (corner4['n_runners'] - 1).clip(lower=1)
        
        # race_dateとhorse_idをdfからマージ
        race_dates = df[['race_id', 'race_date', 'horse_id', 'horse_number']].drop_duplicates()
        corner4 = corner4.merge(
            race_dates[['race_id', 'race_date', 'horse_id']], 
            on='race_id', 
            how='left'
        )
        
        # ==========================================
        # 2. 脚質分類（過去実績から）
        # ==========================================
        logging.info("  2. 脚質分類...")
        
        corner4 = corner4.sort_values(['horse_id', 'race_date']).reset_index(drop=True)
        
        # 過去の平均position_percentile
        corner4['horse_avg_position_pct'] = corner4.groupby('horse_id')['position_percentile'].transform(
            lambda x: x.expanding().mean().shift(1)
        )
        
        # 脚質分類
        corner4['running_style'] = corner4['horse_avg_position_pct'].apply(self.classify_running_style)
        
        # ワンホットエンコーディング
        for style in ['escape', 'front', 'chaser', 'closer']:
            corner4[f'is_{style}'] = (corner4['running_style'] == style).astype(int)
        
        # 過去のgap平均（正規化済み）
        corner4['horse_avg_gap_zscore'] = corner4.groupby('horse_id')['gap_zscore'].transform(
            lambda x: x.expanding().mean().shift(1)
        )
        
        # ==========================================
        # 3. ペース特徴量の正規化
        # ==========================================
        logging.info("  3. ペース特徴量の正規化...")
        
        race_details_df = race_details_df.copy()
        race_details_df['race_id'] = race_details_df['race_id'].astype(str)
        
        # races_dfから距離・馬場を取得
        race_conditions = races_df[['race_id', 'distance_m', 'track_surface', 'track_condition']].drop_duplicates()
        race_details_df = race_details_df.merge(race_conditions, on='race_id', how='left')
        
        # 条件ごとの平均ペースを計算（学習データ用）
        logging.info("    条件ごとの期待値を計算中...")
        
        # 距離カテゴリ
        race_details_df['distance_category'] = race_details_df['distance_m'].apply(self.get_distance_category)
        
        # 条件ごとの平均（距離×馬場）
        condition_pace = race_details_df.groupby(['distance_category', 'track_surface']).agg({
            'first_half': ['mean', 'std'],
            'second_half': ['mean', 'std']
        }).reset_index()
        condition_pace.columns = ['distance_category', 'track_surface', 
                                   'first_half_mean', 'first_half_std',
                                   'second_half_mean', 'second_half_std']
        
        # std=0の場合は1に置換
        for col in ['first_half_std', 'second_half_std']:
            condition_pace[col] = condition_pace[col].fillna(1).replace(0, 1)
        
        race_details_df = race_details_df.merge(condition_pace, on=['distance_category', 'track_surface'], how='left')
        
        # ペースのZスコア
        race_details_df['first_half_zscore'] = (
            (race_details_df['first_half'] - race_details_df['first_half_mean']) / 
            race_details_df['first_half_std']
        )
        race_details_df['second_half_zscore'] = (
            (race_details_df['second_half'] - race_details_df['second_half_mean']) / 
            race_details_df['second_half_std']
        )
        
        # ペースタイプ分類（正規化後）
        race_details_df['pace_diff_zscore'] = race_details_df['first_half_zscore'] - race_details_df['second_half_zscore']
        
        def classify_pace_normalized(diff):
            if pd.isna(diff):
                return 'unknown'
            if diff < -0.5:  # 前半がかなり遅い（Zスコア基準）
                return 'slow'
            elif diff > 0.5:  # 前半がかなり速い
                return 'high'
            else:
                return 'middle'
        
        race_details_df['pace_type'] = race_details_df['pace_diff_zscore'].apply(classify_pace_normalized)
        
        # ワンホットエンコーディング
        for pace in ['slow', 'middle', 'high']:
            race_details_df[f'is_{pace}_pace'] = (race_details_df['pace_type'] == pace).astype(int)
        
        # ==========================================
        # 4. dfにマージ
        # ==========================================
        logging.info("  4. 特徴量をマージ...")
        
        # corner4からの特徴量
        gap_features = corner4[[
            'race_id', 'horse_id', 
            'gap_zscore', 'gap_percentile', 'position_percentile',
            'horse_avg_position_pct', 'horse_avg_gap_zscore',
            'is_escape', 'is_front', 'is_chaser', 'is_closer'
        ]].drop_duplicates(subset=['race_id', 'horse_id'], keep='last')
        
        df = df.merge(gap_features, on=['race_id', 'horse_id'], how='left')
        
        # race_detailsからの特徴量
        pace_features = race_details_df[[
            'race_id',
            'first_half_zscore', 'second_half_zscore', 'pace_diff_zscore',
            'is_slow_pace', 'is_middle_pace', 'is_high_pace'
        ]].drop_duplicates(subset=['race_id'], keep='last')
        
        df = df.merge(pace_features, on='race_id', how='left')
        
        # ==========================================
        # 5. ペース適性スコア（交互作用）
        # ==========================================
        logging.info("  5. ペース適性スコア（交互作用）...")
        
        # 逃げ馬 × ハイペース → 不利
        df['escape_x_high_pace'] = df['is_escape'] * df['is_high_pace']
        
        # 逃げ馬 × スローペース → 有利
        df['escape_x_slow_pace'] = df['is_escape'] * df['is_slow_pace']
        
        # 追込馬 × ハイペース → 有利
        df['closer_x_high_pace'] = df['is_closer'] * df['is_high_pace']
        
        # 追込馬 × スローペース → 不利
        df['closer_x_slow_pace'] = df['is_closer'] * df['is_slow_pace']
        
        # 差し馬 × ミドルペース → 有利（バランス型）
        df['chaser_x_middle_pace'] = df['is_chaser'] * df['is_middle_pace']
        
        # ==========================================
        # 6. 馬身差の適性（追い込み能力）
        # ==========================================
        logging.info("  6. 追い込み能力...")
        
        # 過去のgap_zscoreが高い（後方）けど勝率が高い馬 = 追い込み能力
        # これは既存の特徴量と組み合わせで表現
        
        # gap_zscore × 脚質の交互作用
        df['gap_x_closer'] = df['horse_avg_gap_zscore'].fillna(0) * df['is_closer']
        df['gap_x_escape'] = df['horse_avg_gap_zscore'].fillna(0) * df['is_escape']
        
        logging.info("  スマート特徴量生成完了")
        
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
        logging.info("Phase 11: スマート特徴量エンジニアリング（v8.0）")
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
        logging.info(f"  corner_positions: {len(corner_positions_df):,}")
        logging.info(f"  race_details: {len(race_details_df):,}")
        
        # 既存特徴量生成
        logging.info("\n【既存特徴量生成】")
        train_data = self._generate_horse_features(train_data, races_df)
        train_data = self._generate_jockey_sire_features(train_data, races_df, pedigree_df, shutuba_df)
        
        # スマート特徴量生成
        logging.info("\n【スマート特徴量生成】")
        train_data = self.generate_normalized_features(train_data, races_df, corner_positions_df, race_details_df)
        
        # v5.4特徴量を読み込み
        with open(self.v54_dir / 'feature_names.json', 'r', encoding='utf-8') as f:
            v54_features = json.load(f)
        
        # 新特徴量リスト
        smart_features = [
            # 正規化された馬身差
            'gap_zscore', 'gap_percentile', 'position_percentile',
            'horse_avg_position_pct', 'horse_avg_gap_zscore',
            # 脚質
            'is_escape', 'is_front', 'is_chaser', 'is_closer',
            # ペース
            'first_half_zscore', 'second_half_zscore', 'pace_diff_zscore',
            'is_slow_pace', 'is_middle_pace', 'is_high_pace',
            # 交互作用
            'escape_x_high_pace', 'escape_x_slow_pace',
            'closer_x_high_pace', 'closer_x_slow_pace',
            'chaser_x_middle_pace',
            'gap_x_closer', 'gap_x_escape'
        ]
        
        all_features = v54_features + [f for f in smart_features if f not in v54_features]
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
        
        logging.info(f"\n  v8.0 Test結果:")
        logging.info(f"    ROI: {roi:.2%}")
        logging.info(f"    的中率: {hit_rate:.1%}")
        logging.info(f"    投資数: {len(bet):,}")
        
        # 比較
        logging.info(f"\n  比較:")
        logging.info(f"    v5.4: 79.43%")
        logging.info(f"    v7.0: 78.92%")
        logging.info(f"    v8.0: {roi:.2%} ({(roi - 0.7943) * 100:+.2f}pp)")
        
        # 新特徴量の重要度
        importance = model.feature_importance(importance_type='gain')
        feature_importance = pd.DataFrame({
            'feature': available_features,
            'importance': importance
        }).sort_values('importance', ascending=False)
        
        logging.info("\n  【新特徴量の重要度 Top10】")
        for i, row in feature_importance[feature_importance['feature'].isin(smart_features)].head(10).iterrows():
            rank = feature_importance.index.get_loc(i) + 1
            logging.info(f"    {row['feature']}: rank={rank}, importance={row['importance']:.0f}")
        
        # モデル保存
        with open(self.output_dir / 'mu_v8_0_ranker.pkl', 'wb') as f:
            pickle.dump(model, f)
        
        with open(self.output_dir / 'feature_names.json', 'w', encoding='utf-8') as f:
            json.dump(available_features, f, ensure_ascii=False, indent=2)
        
        results = {
            'model_version': 'v8.0',
            'train_date': datetime.now().isoformat(),
            'n_features': len(available_features),
            'smart_features': smart_features,
            'test_roi': float(roi),
            'test_hit_rate': float(hit_rate),
            'n_bets': int(len(bet)),
            'vs_v54': float(roi - 0.7943),
            'vs_v70': float(roi - 0.7892)
        }
        
        with open(self.output_dir / 'training_results.json', 'w', encoding='utf-8') as f:
            json.dump(results, f, ensure_ascii=False, indent=2)
        
        # 特徴量重要度保存
        feature_importance.to_csv(self.output_dir / 'feature_importance.csv', index=False)
        
        logging.info(f"\n  モデル保存: {self.output_dir}")
        logging.info("=" * 70)
        
        return results


if __name__ == "__main__":
    generator = SmartFeatureGenerator()
    generator.run()
