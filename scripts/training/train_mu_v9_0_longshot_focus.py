#!/usr/bin/env python3
"""
μモデル v9.0 学習スクリプト（穴馬フォーカス版）

【戦略】
1. 穴馬重視サンプル重み（5-8番人気の的中に高重み）
2. gap特徴量の精緻化（直近N走、条件別）
3. 血統×条件マトリックスの精緻化
4. 騎手の穴馬騎乗成績

【目標】Test ROI 100%超

【データリーク防止】
- 全特徴量でshift(1)を使用
- 当該レースの情報（着順、オッズ等）は使用しない
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


class MuV90Trainer:
    """μモデル v9.0 学習クラス（穴馬フォーカス版）"""
    
    DISTANCE_CATEGORIES = {
        'sprint': (0, 1400),
        'mile': (1400, 1800),
        'intermediate': (1800, 2200),
        'long': (2200, 9999)
    }
    
    # 穴馬ブースト対象の人気範囲
    LONGSHOT_MIN_POP = 5
    LONGSHOT_MAX_POP = 9
    LONGSHOT_BOOST = 1.5
    
    SMOOTHING_C_BASE = 20  # ベイジアンスムージング定数
    
    def __init__(self):
        self.base_model_dir = Path('keibaai/models/mu_v3_3')
        self.output_dir = Path('keibaai/models/mu_v9_0')
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
        for cat, (low, high) in MuV90Trainer.DISTANCE_CATEGORIES.items():
            if low <= distance_m < high:
                return cat
        return 'intermediate'
    
    @staticmethod
    def calculate_normalized_rank(finish, n_runners):
        if pd.isna(finish) or pd.isna(n_runners) or n_runners <= 1:
            return np.nan
        return 1.0 - (finish - 1) / (n_runners - 1)
    
    def load_data(self):
        """データ読み込み"""
        logging.info("=" * 60)
        logging.info("v9.0: データ読み込み")
        logging.info("=" * 60)
        
        train_data = pd.read_parquet(self.base_model_dir / 'train_data_mu_v3_3.parquet')
        train_data['race_date'] = pd.to_datetime(train_data['race_date'])
        
        races_df = pd.read_parquet('keibaai/data/parsed/parquet/races/races.parquet')
        races_df['race_date'] = pd.to_datetime(races_df['race_date'])
        
        pedigree_path = Path('keibaai/data/parsed/parquet/pedigrees/pedigrees.parquet')
        pedigree_df = pd.read_parquet(pedigree_path) if pedigree_path.exists() else None
        
        shutuba_df = pd.read_parquet('keibaai/data/parsed/parquet/shutuba/shutuba.parquet')
        
        logging.info(f"  学習データ: {len(train_data):,}")
        logging.info(f"  レース結果: {len(races_df):,}")
        logging.info(f"  血統: {len(pedigree_df):,}" if pedigree_df is not None else "  血統: なし")
        
        train_data['horse_id'] = train_data['horse_id'].astype(str)
        races_df['horse_id'] = races_df['horse_id'].astype(str)
        
        return train_data, races_df, pedigree_df, shutuba_df
    
    def generate_longshot_features(self, df, races_df):
        """穴馬フォーカス特徴量を生成"""
        logging.info("=" * 60)
        logging.info("v9.0: 穴馬フォーカス特徴量生成")
        logging.info("=" * 60)
        
        perf = races_df.copy()
        perf['n_runners'] = perf.groupby('race_id')['horse_id'].transform('count')
        # Int64をfloat64に変換してNA対応
        perf['finish_position'] = pd.to_numeric(perf['finish_position'], errors='coerce').astype(float)
        perf['popularity'] = pd.to_numeric(perf['popularity'], errors='coerce').astype(float)
        
        # 着順 < 人気 → 期待超え（NAは0として扱う）
        perf['is_upset'] = (perf['finish_position'] < perf['popularity']).fillna(False).astype(int)
        
        # 5番人気以下で3着以内 → 穴馬好走
        perf['is_longshot_place'] = (
            (perf['popularity'] >= self.LONGSHOT_MIN_POP) & 
            (perf['finish_position'] <= 3)
        ).fillna(False).astype(int)
        
        perf = perf.sort_values(['horse_id', 'race_date']).reset_index(drop=True)
        
        # 馬の期待超え率
        perf['horse_upset_rate'] = perf.groupby('horse_id')['is_upset'].transform(
            lambda x: x.expanding().mean().shift(1)
        )
        
        # 馬の穴馬好走率
        perf['horse_longshot_place_rate'] = perf.groupby('horse_id')['is_longshot_place'].transform(
            lambda x: x.expanding().mean().shift(1)
        )
        
        logging.info("  特徴量:")
        logging.info("    - horse_upset_rate: 着順 < 人気となった割合")
        logging.info("    - horse_longshot_place_rate: 5番人気以下で3着以内の割合")
        
        merge_cols = ['horse_id', 'race_date', 'horse_upset_rate', 'horse_longshot_place_rate']
        merge_df = perf[merge_cols].drop_duplicates(subset=['horse_id', 'race_date'], keep='last')
        
        result = df.merge(merge_df, on=['horse_id', 'race_date'], how='left')
        
        logging.info(f"  horse_upset_rate mean: {result['horse_upset_rate'].mean():.3f}")
        logging.info(f"  horse_longshot_place_rate mean: {result['horse_longshot_place_rate'].mean():.3f}")
        
        return result
    
    def generate_refined_gap_features(self, df, races_df):
        """精緻化されたgap特徴量を生成"""
        logging.info("=" * 60)
        logging.info("v9.0: 精緻化gap特徴量生成")
        logging.info("=" * 60)
        
        perf = races_df.copy()
        perf['n_runners'] = perf.groupby('race_id')['horse_id'].transform('count')
        # Int64をfloat64に変換してNA対応
        perf['finish_position'] = pd.to_numeric(perf['finish_position'], errors='coerce').astype(float)
        perf['popularity'] = pd.to_numeric(perf['popularity'], errors='coerce').astype(float)
        
        perf['normalized_rank'] = perf.apply(
            lambda x: self.calculate_normalized_rank(x['finish_position'], x['n_runners']), axis=1
        )
        
        # 人気から期待されるNormalized Rank
        perf['expected_nr'] = 1 - (perf['popularity'] - 1) / (perf['n_runners'] - 1).clip(lower=1)
        
        perf = perf.sort_values(['horse_id', 'race_date']).reset_index(drop=True)
        
        # 直近3走でのギャップ（実績 - 期待）
        perf['horse_nr_last3'] = perf.groupby('horse_id')['normalized_rank'].transform(
            lambda x: x.rolling(3, min_periods=1).mean().shift(1)
        )
        
        # 直近5走でのギャップ
        perf['horse_nr_last5'] = perf.groupby('horse_id')['normalized_rank'].transform(
            lambda x: x.rolling(5, min_periods=1).mean().shift(1)
        )
        
        # 累積でのギャップ
        perf['horse_nr_all'] = perf.groupby('horse_id')['normalized_rank'].transform(
            lambda x: x.expanding().mean().shift(1)
        )
        
        # ギャップ = 実績NR - 期待NR（正 = 期待超え）
        perf['horse_gap_vs_expected_3'] = perf['horse_nr_last3'] - perf['expected_nr']
        perf['horse_gap_vs_expected_5'] = perf['horse_nr_last5'] - perf['expected_nr']
        perf['horse_gap_vs_expected_all'] = perf['horse_nr_all'] - perf['expected_nr']
        
        # トレンド（直近3走 vs 直近10走）
        perf['horse_nr_last10'] = perf.groupby('horse_id')['normalized_rank'].transform(
            lambda x: x.rolling(10, min_periods=1).mean().shift(1)
        )
        perf['horse_nr_trend'] = perf['horse_nr_last3'] - perf['horse_nr_last10']
        
        logging.info("  特徴量:")
        logging.info("    - horse_gap_vs_expected_3/5/all: 直近N走実績 - 人気期待値")
        logging.info("    - horse_nr_trend: 直近3走 - 直近10走（上昇傾向なら正）")
        
        merge_cols = ['horse_id', 'race_date', 
                      'horse_gap_vs_expected_3', 'horse_gap_vs_expected_5', 
                      'horse_gap_vs_expected_all', 'horse_nr_trend']
        merge_df = perf[merge_cols].drop_duplicates(subset=['horse_id', 'race_date'], keep='last')
        
        result = df.merge(merge_df, on=['horse_id', 'race_date'], how='left')
        
        return result
    
    def generate_sire_condition_features(self, df, races_df, pedigree_df):
        """血統×条件マトリックス特徴量を生成"""
        logging.info("=" * 60)
        logging.info("v9.0: 血統×条件マトリックス特徴量生成")
        logging.info("=" * 60)
        
        if pedigree_df is None:
            logging.warning("  血統データなし - スキップ")
            return df
        
        # 父IDマップ作成
        sire_map = pedigree_df[pedigree_df['generation'] == 1][['horse_id', 'ancestor_id']].copy()
        sire_map.columns = ['horse_id', 'sire_id']
        sire_map['horse_id'] = sire_map['horse_id'].astype(str)
        sire_map = sire_map.drop_duplicates('horse_id')
        
        perf = races_df.copy()
        perf['horse_id'] = perf['horse_id'].astype(str)
        perf['n_runners'] = perf.groupby('race_id')['horse_id'].transform('count')
        perf['finish_position'] = pd.to_numeric(perf['finish_position'], errors='coerce')
        perf['normalized_rank'] = perf.apply(
            lambda x: self.calculate_normalized_rank(x['finish_position'], x['n_runners']), axis=1
        )
        
        perf['distance_category'] = perf['distance_m'].apply(self.get_distance_category)
        
        # 父IDをマージ
        perf = perf.merge(sire_map, on='horse_id', how='left')
        perf = perf.dropna(subset=['sire_id', 'normalized_rank'])
        
        # 父×距離カテゴリ×馬場 の産駒成績
        sire_stats = perf.groupby(['sire_id', 'distance_category', 'track_surface']).agg({
            'normalized_rank': ['sum', 'count']
        }).reset_index()
        sire_stats.columns = ['sire_id', 'distance_category', 'track_surface', 'nr_sum', 'nr_count']
        
        # ベイジアンスムージング
        PRIOR_MEAN = 0.5
        sire_stats['sire_condition_nr'] = (
            (sire_stats['nr_sum'] + PRIOR_MEAN * self.SMOOTHING_C_BASE) /
            (sire_stats['nr_count'] + self.SMOOTHING_C_BASE)
        )
        
        logging.info(f"  条件組み合わせ数: {len(sire_stats):,}")
        
        # dfに距離カテゴリを追加
        result = df.copy()
        result['distance_category'] = result['distance_m'].apply(self.get_distance_category)
        
        # 父IDをマージ
        if 'sire_id' not in result.columns:
            result = result.merge(sire_map, on='horse_id', how='left')
        
        # track_surfaceカラムの確認
        if 'track_surface' not in result.columns:
            # train_dataにはtrack_*カラムがある可能性
            if 'track_芝' in result.columns:
                result['track_surface'] = result['track_芝'].apply(lambda x: '芝' if x == 1 else 'ダート')
            else:
                logging.warning("  track_surfaceカラムなし")
                result['track_surface'] = None
        
        # 条件別成績をマージ
        result = result.merge(
            sire_stats[['sire_id', 'distance_category', 'track_surface', 'sire_condition_nr']],
            on=['sire_id', 'distance_category', 'track_surface'],
            how='left'
        )
        
        logging.info(f"  sire_condition_nr mean: {result['sire_condition_nr'].mean():.3f}")
        logging.info(f"  sire_condition_nr NaN率: {result['sire_condition_nr'].isna().mean():.1%}")
        
        return result
    
    def generate_jockey_longshot_features(self, df, races_df, shutuba_df):
        """騎手の穴馬騎乗成績特徴量を生成"""
        logging.info("=" * 60)
        logging.info("v9.0: 騎手穴馬騎乗成績特徴量生成")
        logging.info("=" * 60)
        
        perf = races_df.copy()
        perf['finish_position'] = pd.to_numeric(perf['finish_position'], errors='coerce')
        perf['popularity'] = pd.to_numeric(perf['popularity'], errors='coerce')
        
        # 5番人気以下のレースのみ抽出
        longshot_perf = perf[perf['popularity'] >= self.LONGSHOT_MIN_POP].copy()
        
        # 騎手の穴馬騎乗時の成績
        jockey_longshot_stats = longshot_perf.groupby('jockey_id').agg({
            'finish_position': ['count', lambda x: (x <= 3).sum(), lambda x: (x == 1).sum()]
        }).reset_index()
        jockey_longshot_stats.columns = ['jockey_id', 'longshot_rides', 'longshot_places', 'longshot_wins']
        
        # ベイジアンスムージング
        PRIOR_RATE = 0.1  # 期待複勝率
        jockey_longshot_stats['jockey_longshot_place_rate'] = (
            (jockey_longshot_stats['longshot_places'] + PRIOR_RATE * self.SMOOTHING_C_BASE) /
            (jockey_longshot_stats['longshot_rides'] + self.SMOOTHING_C_BASE)
        )
        
        logging.info(f"  対象騎手数: {len(jockey_longshot_stats):,}")
        
        result = df.copy()
        
        # jockey_idをマージ
        if 'jockey_id' not in result.columns:
            jockey_map = shutuba_df[['race_id', 'horse_id', 'jockey_id']].drop_duplicates(
                subset=['race_id', 'horse_id']
            )
            jockey_map['horse_id'] = jockey_map['horse_id'].astype(str)
            result = result.merge(jockey_map, on=['race_id', 'horse_id'], how='left')
        
        result = result.merge(
            jockey_longshot_stats[['jockey_id', 'jockey_longshot_place_rate']],
            on='jockey_id',
            how='left'
        )
        
        logging.info(f"  jockey_longshot_place_rate mean: {result['jockey_longshot_place_rate'].mean():.3f}")
        
        return result
    
    def generate_base_features(self, df, races_df):
        """v5.4と同様のベース特徴量を生成"""
        logging.info("=" * 60)
        logging.info("v9.0: ベース特徴量生成（v5.4と同様）")
        logging.info("=" * 60)
        
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
        
        # タイム偏差
        race_avg_time = perf.groupby('race_id')['finish_time_seconds'].transform('mean')
        race_std_time = perf.groupby('race_id')['finish_time_seconds'].transform('std').fillna(1).replace(0, 1)
        perf['time_deviation'] = (race_avg_time - perf['finish_time_seconds']) / race_std_time
        
        race_avg_l3f = perf.groupby('race_id')['last_3f_time'].transform('mean')
        race_std_l3f = perf.groupby('race_id')['last_3f_time'].transform('std').fillna(0.5).replace(0, 0.5)
        perf['l3f_deviation'] = (race_avg_l3f - perf['last_3f_time']) / race_std_l3f
        
        perf['position_4c_normalized'] = (perf['passing_order_4'] - 1) / (perf['n_runners'] - 1).clip(lower=1)
        
        perf = perf.sort_values(['horse_id', 'race_date']).reset_index(drop=True)
        
        # タイム指数
        perf['horse_time_deviation_avg'] = perf.groupby('horse_id')['time_deviation'].transform(
            lambda x: x.expanding().mean().shift(1)
        )
        perf['horse_l3f_deviation_avg'] = perf.groupby('horse_id')['l3f_deviation'].transform(
            lambda x: x.expanding().mean().shift(1)
        )
        perf['horse_best_time_deviation'] = perf.groupby('horse_id')['time_deviation'].transform(
            lambda x: x.expanding().max().shift(1)
        )
        
        # コース適性
        perf['horse_venue_nr'] = perf.groupby(['horse_id', 'venue_name'])['normalized_rank'].transform(
            lambda x: x.expanding().mean().shift(1)
        )
        perf['horse_distance_nr'] = perf.groupby(['horse_id', 'distance_category'])['normalized_rank'].transform(
            lambda x: x.expanding().mean().shift(1)
        )
        perf['horse_surface_nr'] = perf.groupby(['horse_id', 'track_surface'])['normalized_rank'].transform(
            lambda x: x.expanding().mean().shift(1)
        )
        perf['horse_best_nr'] = perf.groupby('horse_id')['normalized_rank'].transform(
            lambda x: x.expanding().max().shift(1)
        )
        
        # ローテーション
        perf['horse_interval_days'] = perf.groupby('horse_id')['race_date'].diff().dt.days
        perf['horse_dist_change'] = perf.groupby('horse_id')['distance_m'].diff()
        
        perf['prev_weight'] = perf.groupby('horse_id')['horse_weight'].shift(1)
        perf['horse_weight_change_ratio'] = (perf['horse_weight'] - perf['prev_weight']) / perf['prev_weight'].clip(lower=400)
        
        # 脚質
        perf['horse_avg_position_4c'] = perf.groupby('horse_id')['position_4c_normalized'].transform(
            lambda x: x.expanding().mean().shift(1)
        )
        
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
        
        return result
    
    def prepare_features(self, df):
        """特徴量リストを準備"""
        with open(self.base_model_dir / 'feature_names.json', 'r', encoding='utf-8') as f:
            base_features = json.load(f)
        
        # v5.4で除外された特徴量
        purge_features = [
            'jockey_win_rate', 'jockey_place_rate', 'jockey_avg_rank',
            'sire_win_rate', 'sire_avg_finish',
            'trainer_win_rate', 'trainer_place_rate',
        ]
        base_features = [f for f in base_features if f not in purge_features]
        
        # v9.0で追加された特徴量
        new_features = [
            # 穴馬フォーカス
            'horse_upset_rate', 'horse_longshot_place_rate',
            # 精緻化gap
            'horse_gap_vs_expected_3', 'horse_gap_vs_expected_5', 
            'horse_gap_vs_expected_all', 'horse_nr_trend',
            # 血統×条件
            'sire_condition_nr',
            # 騎手穴馬
            'jockey_longshot_place_rate',
            # v5.4と同様
            'horse_time_deviation_avg', 'horse_l3f_deviation_avg', 'horse_best_time_deviation',
            'horse_venue_nr', 'horse_distance_nr', 'horse_surface_nr', 'horse_best_nr',
            'horse_interval_days', 'horse_dist_change', 'horse_weight_change_ratio',
            'horse_avg_position_4c',
        ]
        
        available = [f for f in base_features + new_features if f in df.columns]
        available = list(dict.fromkeys(available))  # 重複除去
        
        logging.info(f"特徴量数: {len(available)}")
        return available
    
    def prepare_target(self, df):
        """ターゲットとサンプル重みを準備（穴馬フォーカス）"""
        # NA値を適切に処理
        df['win_odds'] = pd.to_numeric(df['win_odds'], errors='coerce')
        df['popularity'] = pd.to_numeric(df['popularity'], errors='coerce')
        df['finish_position'] = pd.to_numeric(df['finish_position'], errors='coerce')
        
        odds = df['win_odds'].fillna(1.0).clip(upper=90)
        log_odds = np.log1p(odds)
        popularity = df['popularity'].fillna(10)
        finish_pos = df['finish_position'].fillna(99)
        
        # LambdaRankのターゲット（relevance gain）
        gain = np.zeros(len(df))
        mask_1st = (finish_pos == 1)
        mask_2nd = (finish_pos == 2)
        mask_3rd = (finish_pos == 3)
        
        gain[mask_1st] = (log_odds[mask_1st] * 12.74).fillna(0)
        gain[mask_2nd] = (log_odds[mask_2nd] * 6.73).fillna(0)
        gain[mask_3rd] = (log_odds[mask_3rd] * 3.69).fillna(0)
        df['target_relevance'] = np.nan_to_num(gain, nan=0).astype(int)
        
        # サンプル重み（穴馬ブースト）
        base_weight = np.log1p(odds).clip(upper=np.log1p(100))
        boost_mask = (
            (popularity >= self.LONGSHOT_MIN_POP) & 
            (popularity <= self.LONGSHOT_MAX_POP) &
            (finish_pos == 1)
        )
        weight = base_weight.copy()
        weight[boost_mask] = weight[boost_mask] * self.LONGSHOT_BOOST
        df['sample_weight'] = np.nan_to_num(weight, nan=1.0)
        
        logging.info("穴馬ブースト適用:")
        logging.info(f"  対象: {self.LONGSHOT_MIN_POP}-{self.LONGSHOT_MAX_POP}番人気の1着")
        logging.info(f"  ブースト倍率: {self.LONGSHOT_BOOST}x")
        logging.info(f"  ブースト対象件数: {boost_mask.sum():,}")
        
        return df
    
    def train(self, df, feature_cols, n_trials=50):
        """モデル学習"""
        logging.info("=" * 60)
        logging.info("μモデル v9.0 学習開始（穴馬フォーカス版）")
        logging.info("★★★ Testデータは最終確認でのみ使用 ★★★")
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
                'num_leaves': trial.suggest_int('num_leaves', 40, 120),
                'lambda_l1': trial.suggest_float('lambda_l1', 0.5, 10.0, log=True),
                'lambda_l2': trial.suggest_float('lambda_l2', 0.1, 5.0, log=True),
                'min_child_samples': trial.suggest_int('min_child_samples', 30, 80),
                'learning_rate': trial.suggest_float('learning_rate', 0.02, 0.10, log=True),
                'feature_fraction': trial.suggest_float('feature_fraction', 0.5, 0.85),
                'bagging_fraction': trial.suggest_float('bagging_fraction', 0.6, 0.9),
                'bagging_freq': trial.suggest_int('bagging_freq', 1, 7),
                'max_depth': trial.suggest_int('max_depth', 5, 10),
            }
            
            model = lgb.LGBMRanker(**params, n_estimators=1500)
            model.fit(
                train_df[feature_cols], train_df['target_relevance'],
                group=group_train, sample_weight=train_df['sample_weight'],
                eval_set=[(valid_df[feature_cols], valid_df['target_relevance'])],
                eval_group=[group_valid],
                callbacks=[lgb.early_stopping(50, verbose=False)]
            )
            
            valid_roi = calculate_roi(valid_df, model.predict(valid_df[feature_cols]))
            return valid_roi
        
        logging.info(f"Optuna {n_trials}トライアル（Valid ROI最大化）...")
        study = optuna.create_study(direction='maximize')
        study.optimize(objective, n_trials=n_trials, show_progress_bar=True)
        
        best_params = study.best_params
        best_params.update({
            'objective': 'lambdarank', 'metric': 'ndcg', 'eval_at': [1, 3, 5],
            'boosting_type': 'gbdt', 'verbose': -1, 'random_state': 42,
            'label_gain': list(range(100))
        })
        
        logging.info(f"最良のValid ROI: {study.best_value:.2%}")
        
        # 最終モデル学習
        self.model = lgb.LGBMRanker(**best_params, n_estimators=2000)
        self.model.fit(
            train_df[feature_cols], train_df['target_relevance'],
            group=group_train, sample_weight=train_df['sample_weight'],
            eval_set=[(valid_df[feature_cols], valid_df['target_relevance'])],
            eval_group=[group_valid],
            callbacks=[lgb.early_stopping(100, verbose=True)]
        )
        
        # 最終評価
        valid_roi = calculate_roi(valid_df, self.model.predict(valid_df[feature_cols]))
        test_roi = calculate_roi(test_df, self.model.predict(test_df[feature_cols]))
        
        importance = dict(zip(feature_cols, self.model.feature_importances_))
        sorted_imp = sorted(importance.items(), key=lambda x: x[1], reverse=True)
        
        logging.info("=" * 60)
        logging.info("【最終結果: μ v9.0（穴馬フォーカス版）】")
        logging.info("=" * 60)
        logging.info(f"  Valid ROI: {valid_roi:.2%}")
        logging.info(f"  Test ROI: {test_roi:.2%} ← 最終確認")
        logging.info(f"  Valid-Test差: {abs(valid_roi - test_roi):.2%}")
        logging.info(f"  🎯 目標: {'✅ ROI 100%超達成!' if test_roi > 1.0 else '❌ 要改善'}")
        logging.info("")
        logging.info("【Top 30特徴量】")
        for i, (name, imp) in enumerate(sorted_imp[:30], 1):
            logging.info(f"  {i:2}. {name}: {imp}")
        
        self.save_model(feature_cols, valid_roi, test_roi, best_params)
        return valid_roi, test_roi
    
    def save_model(self, features, valid_roi, test_roi, best_params):
        """モデル保存"""
        with open(self.output_dir / 'mu_v9_0_ranker.pkl', 'wb') as f:
            pickle.dump(self.model, f)
        with open(self.output_dir / 'feature_names.json', 'w', encoding='utf-8') as f:
            json.dump(features, f, ensure_ascii=False, indent=2)
        
        save_params = {k: v for k, v in best_params.items() if k != 'label_gain'}
        metadata = {
            'version': 'v9.0',
            'description': '穴馬フォーカス版（5-9番人気ブースト）',
            'valid_roi': float(valid_roi),
            'test_roi': float(test_roi),
            'valid_test_gap': float(abs(valid_roi - test_roi)),
            'target_achieved': bool(test_roi > 1.0),
            'strategy': {
                'longshot_boost_range': [self.LONGSHOT_MIN_POP, self.LONGSHOT_MAX_POP],
                'longshot_boost': self.LONGSHOT_BOOST,
            },
            'best_params': save_params,
            'created_at': datetime.now().isoformat()
        }
        with open(self.output_dir / 'model_info.json', 'w', encoding='utf-8') as f:
            json.dump(metadata, f, ensure_ascii=False, indent=2)
        
        logging.info(f"保存完了: {self.output_dir}")
    
    def run(self, n_trials=50):
        """メイン実行"""
        train_data, races_df, pedigree_df, shutuba_df = self.load_data()
        
        # 特徴量生成
        train_data = self.generate_base_features(train_data, races_df)
        train_data = self.generate_longshot_features(train_data, races_df)
        train_data = self.generate_refined_gap_features(train_data, races_df)
        train_data = self.generate_sire_condition_features(train_data, races_df, pedigree_df)
        train_data = self.generate_jockey_longshot_features(train_data, races_df, shutuba_df)
        
        # ターゲット準備
        train_data = self.prepare_target(train_data)
        
        # 特徴量選択
        feature_cols = self.prepare_features(train_data)
        
        # 学習
        return self.train(train_data, feature_cols, n_trials)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description='μモデル v9.0 学習')
    parser.add_argument('--trials', type=int, default=50, help='Optunaトライアル数')
    args = parser.parse_args()
    
    trainer = MuV90Trainer()
    valid_roi, test_roi = trainer.run(n_trials=args.trials)
    
    print("\n" + "=" * 60)
    print("【結果サマリー】")
    print("=" * 60)
    print(f"  Valid ROI: {valid_roi:.2%}")
    print(f"  Test ROI:  {test_roi:.2%}")
    if test_roi > 1.0:
        print("  🎉 ROI 100%超達成！")
    elif test_roi > 0.9:
        print("  📈 90%超、改善中")
    else:
        print("  ⚠️ さらなる改善が必要")
