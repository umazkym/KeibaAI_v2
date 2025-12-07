#!/usr/bin/env python3
"""
μモデル v11.0 学習スクリプト（穴馬発掘強化版）

【v10.5からの改善点】
1. race_details.parquetを活用したペース特徴量（4種）
2. returns.parquetを活用した配当傾向特徴量（6種）
3. 穴馬ブースト学習（5番人気以下の的中を重視）
4. 馬の穴馬傾向特徴量

【リーク回避戦略】
- 会場/血統統計: Train期間（2022年末まで）で固定
- 累積統計: expanding().mean().shift(1)
- レース結果由来の情報は未来リーク排除

【目標】
- Test ROI > 90%（v10.5の81.77%を超える）
- 1番人気選択率を76%から60%以下に
- Valid-Test差 < 8%
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

# 新特徴量エンジンをインポート
from keibaai.src.features.pace_features import PaceFeatureEngine
from keibaai.src.features.payout_features import PayoutFeatureEngine

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class MuV110Trainer:
    """μモデル v11.0 穴馬発掘強化版"""
    
    # 過学習防止のための正則化パラメータ範囲
    REGULARIZATION_RANGES = {
        'num_leaves': (25, 60),
        'lambda_l1': (3.0, 20.0),
        'lambda_l2': (2.0, 12.0),
        'min_child_samples': (60, 150),
        'learning_rate': (0.01, 0.05),
        'feature_fraction': (0.45, 0.70),
        'bagging_fraction': (0.6, 0.85),
        'max_depth': (4, 7),
    }
    
    # リーク版として除外する特徴量パターン
    LEAK_PATTERNS = [
        # 騎手条件別（リークフリー版を使用）
        'jockey_intermediate_win_rate', 'jockey_long_win_rate', 
        'jockey_marathon_win_rate', 'jockey_mile_win_rate',
        'jockey_sprint_win_rate', 'jockey_unknown_win_rate',
        'jockey_ダート_win_rate', 'jockey_芝_win_rate',
        # 騎手会場別
        'jockey_東京_win_rate', 'jockey_中山_win_rate', 'jockey_京都_win_rate',
        'jockey_阪神_win_rate', 'jockey_中京_win_rate', 'jockey_小倉_win_rate',
        'jockey_福島_win_rate', 'jockey_新潟_win_rate', 'jockey_函館_win_rate',
        'jockey_札幌_win_rate',
        # 調教師会場別
        'trainer_東京_win_rate', 'trainer_中山_win_rate', 'trainer_京都_win_rate',
        'trainer_阪神_win_rate', 'trainer_中京_win_rate', 'trainer_小倉_win_rate',
        'trainer_福島_win_rate', 'trainer_新潟_win_rate', 'trainer_函館_win_rate',
        'trainer_札幌_win_rate',
        # コンボ（リークフリー版を使用）
        'combo_win_rate', 'combo_avg_finish', 'combo_overperform', 'combo_races',
        # 血統（Train固定版を使用）
        'sire_course_win_rate', 'sire_course_avg_finish', 'sire_course_place_rate',
        'sire_wet_boost', 'bms_win_rate', 'bms_avg_finish',
        'nicks_win_rate', 'nicks_avg_finish',
        # 元のリーク版gap
        'gap_jockey_popularity', 'gap_pedigree_popularity',
        'gap_course_fit_popularity', 'gap_trainer_popularity',
        'gap_speed_popularity',
        # 元のリーク版勝率
        'jockey_win_rate', 'jockey_place_rate', 'jockey_avg_rank',
        'trainer_win_rate', 'trainer_place_rate',
        'sire_win_rate', 'sire_avg_finish',
    ]
    
    def __init__(self, train_cutoff: str = '2023-01-01'):
        self.train_cutoff = pd.Timestamp(train_cutoff)
        self.base_model_dir = Path('keibaai/models/mu_v3_3')  # ベースモデルのデータを流用
        self.output_dir = Path('keibaai/models/mu_v11_0')
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.model = None
        self.pedigree_df = None
        self.train_sire_stats = None
        
        # 新特徴量エンジン
        self.pace_engine = PaceFeatureEngine(train_cutoff=train_cutoff)
        self.payout_engine = PayoutFeatureEngine(train_cutoff=train_cutoff)
    
    def load_data(self):
        """データ読み込み"""
        logging.info("=" * 60)
        logging.info("v11.0: データ読み込み")
        logging.info("=" * 60)
        
        # ベースデータ（v3.3のデータを流用）
        train_data = pd.read_parquet(self.base_model_dir / 'train_data_mu_v3_3.parquet')
        train_data['race_date'] = pd.to_datetime(train_data['race_date'])
        
        # レース結果
        races_df = pd.read_parquet('keibaai/data/parsed/parquet/races/races.parquet')
        races_df['race_date'] = pd.to_datetime(races_df['race_date'])
        
        # race_details（ペース分析用）
        race_details_df = pd.read_parquet('keibaai/data/parsed/parquet/race_details/race_details.parquet')
        
        # returns（配当分析用）
        returns_df = pd.read_parquet('keibaai/data/parsed/parquet/returns/returns.parquet')
        
        # 血統
        pedigree_path = Path('keibaai/data/parsed/parquet/pedigrees/pedigrees.parquet')
        if pedigree_path.exists():
            self.pedigree_df = pd.read_parquet(pedigree_path)
        
        logging.info(f"  ベースデータ: {len(train_data):,}")
        logging.info(f"  レース結果: {len(races_df):,}")
        logging.info(f"  race_details: {len(race_details_df):,}")
        logging.info(f"  returns: {len(returns_df):,}")
        
        # 型変換
        for col in ['horse_id', 'jockey_id', 'trainer_id']:
            if col in train_data.columns:
                train_data[col] = train_data[col].astype(str)
            if col in races_df.columns:
                races_df[col] = races_df[col].astype(str)
        
        # ベースデータに必要なカラムをraces_dfからマージ
        train_data['race_id'] = train_data['race_id'].astype(str)
        train_data['horse_id'] = train_data['horse_id'].astype(str)
        races_df['race_id'] = races_df['race_id'].astype(str)
        races_df['horse_id'] = races_df['horse_id'].astype(str)
        
        # race_id + horse_idでjockey_id, trainer_idをマージ
        if 'jockey_id' not in train_data.columns:
            horse_race_info = races_df[['race_id', 'horse_id', 'jockey_id', 'trainer_id']].drop_duplicates(
                subset=['race_id', 'horse_id']
            )
            train_data = train_data.merge(horse_race_info, on=['race_id', 'horse_id'], how='left')
            logging.info(f"  jockey_id/trainer_idをマージ完了")
        
        # race_idでvenue, track_surface, distance_mをマージ（pace featuresに必要）
        if 'venue' not in train_data.columns:
            race_meta = races_df.drop_duplicates('race_id')[['race_id', 'venue', 'track_surface', 'distance_m']]
            train_data = train_data.merge(race_meta, on='race_id', how='left')
            logging.info(f"  venue/track_surface/distance_mをマージ完了")
        
        return train_data, races_df, race_details_df, returns_df
    
    def generate_leak_free_features(self, df, races_df):
        """既存のリークフリー特徴量を生成（v10.1と同じ）"""
        logging.info("=" * 60)
        logging.info("v11.0: 既存リークフリー特徴量生成")
        logging.info("=" * 60)
        
        perf = races_df.copy()
        perf = perf.sort_values(['race_date', 'race_id']).reset_index(drop=True)
        
        # 型変換
        perf['finish_position'] = pd.to_numeric(perf['finish_position'], errors='coerce').astype(float)
        perf['popularity'] = pd.to_numeric(perf['popularity'], errors='coerce').astype(float)
        perf['is_win'] = (perf['finish_position'] == 1).fillna(False).astype(int)
        
        # 騎手/調教師の累積勝率（リークフリー）
        logging.info("  騎手/調教師累積勝率...")
        perf['jockey_win_rate_lf'] = perf.groupby('jockey_id')['is_win'].transform(
            lambda x: x.expanding().mean().shift(1)
        )
        perf['trainer_win_rate_lf'] = perf.groupby('trainer_id')['is_win'].transform(
            lambda x: x.expanding().mean().shift(1)
        )
        
        # 騎手×コース（リークフリー）
        if 'venue' in perf.columns:
            perf['jockey_venue_key'] = perf['jockey_id'] + '_' + perf['venue'].astype(str)
            perf['jockey_venue_win_rate_lf'] = perf.groupby('jockey_venue_key')['is_win'].transform(
                lambda x: x.expanding().mean().shift(1)
            )
        
        # 騎手×馬場（リークフリー）
        if 'track_surface' in perf.columns:
            perf['jockey_surface_key'] = perf['jockey_id'] + '_' + perf['track_surface'].astype(str)
            perf['jockey_surface_win_rate_lf'] = perf.groupby('jockey_surface_key')['is_win'].transform(
                lambda x: x.expanding().mean().shift(1)
            )
        
        # 血統統計（Train期間固定）
        logging.info("  血統統計（Train期間固定）...")
        train_perf = perf[perf['race_date'] < self.train_cutoff].copy()
        
        if self.pedigree_df is not None:
            sire_map = self.pedigree_df[self.pedigree_df['generation'] == 1][['horse_id', 'ancestor_id']].copy()
            sire_map.columns = ['horse_id', 'sire_id']
            sire_map['horse_id'] = sire_map['horse_id'].astype(str)
            sire_map = sire_map.drop_duplicates('horse_id')
            
            train_perf = train_perf.merge(sire_map, on='horse_id', how='left')
            perf = perf.merge(sire_map, on='horse_id', how='left')
            
            # Train期間の父別勝率
            sire_stats = train_perf.groupby('sire_id').agg({
                'is_win': ['mean', 'count']
            }).reset_index()
            sire_stats.columns = ['sire_id', 'sire_win_rate_fixed', 'sire_count']
            sire_stats.loc[sire_stats['sire_count'] < 20, 'sire_win_rate_fixed'] = np.nan
            
            self.train_sire_stats = sire_stats
            perf = perf.merge(sire_stats[['sire_id', 'sire_win_rate_fixed']], on='sire_id', how='left')
        
        # gap特徴量（リークフリー）
        logging.info("  gap特徴量（リークフリー）...")
        perf['jockey_rank_lf'] = perf.groupby('race_id')['jockey_win_rate_lf'].rank(ascending=False, method='average')
        perf['gap_jockey_pop_lf'] = perf['popularity'] - perf['jockey_rank_lf']
        
        perf['trainer_rank_lf'] = perf.groupby('race_id')['trainer_win_rate_lf'].rank(ascending=False, method='average')
        perf['gap_trainer_pop_lf'] = perf['popularity'] - perf['trainer_rank_lf']
        
        if 'sire_win_rate_fixed' in perf.columns:
            perf['sire_rank_fixed'] = perf.groupby('race_id')['sire_win_rate_fixed'].rank(ascending=False, method='average')
            perf['gap_sire_pop_lf'] = perf['popularity'] - perf['sire_rank_fixed']
        
        # マージ
        merge_cols = ['horse_id', 'race_date', 
                      'jockey_win_rate_lf', 'trainer_win_rate_lf',
                      'gap_jockey_pop_lf', 'gap_trainer_pop_lf']
        
        if 'jockey_venue_win_rate_lf' in perf.columns:
            merge_cols.append('jockey_venue_win_rate_lf')
        if 'jockey_surface_win_rate_lf' in perf.columns:
            merge_cols.append('jockey_surface_win_rate_lf')
        if 'sire_win_rate_fixed' in perf.columns:
            merge_cols.extend(['sire_win_rate_fixed', 'gap_sire_pop_lf'])
        if 'sire_id' in perf.columns:
            merge_cols.append('sire_id')
        
        merge_df = perf[merge_cols].drop_duplicates(subset=['horse_id', 'race_date'], keep='last')
        result = df.merge(merge_df, on=['horse_id', 'race_date'], how='left')
        
        logging.info(f"  既存リークフリー特徴量: {len(merge_cols) - 2}個")
        
        return result
    
    def generate_new_features(self, df, races_df, race_details_df, returns_df):
        """v11.0 新特徴量を生成"""
        logging.info("=" * 60)
        logging.info("v11.0: 新特徴量生成（ペース・配当傾向）")
        logging.info("=" * 60)
        
        # 1. ペース特徴量
        df = self.pace_engine.generate_features(df, races_df, race_details_df)
        
        # 2. 配当傾向特徴量
        df = self.payout_engine.generate_features(df, races_df, returns_df, self.pedigree_df)
        
        # 3. 馬の穴馬傾向（累積、リークフリー）
        df = self._add_horse_upset_features(df, races_df)
        
        return df
    
    def _add_horse_upset_features(self, df, races_df):
        """馬の穴馬傾向特徴量（リークフリー）"""
        logging.info("  馬の穴馬傾向特徴量...")
        
        perf = races_df[['race_id', 'race_date', 'horse_id', 'finish_position', 'popularity', 'win_odds']].copy()
        perf['race_date'] = pd.to_datetime(perf['race_date'])
        perf['horse_id'] = perf['horse_id'].astype(str)
        perf['finish_position'] = pd.to_numeric(perf['finish_position'], errors='coerce')
        perf['popularity'] = pd.to_numeric(perf['popularity'], errors='coerce')
        perf['win_odds'] = pd.to_numeric(perf['win_odds'], errors='coerce')
        
        perf['is_win'] = (perf['finish_position'] == 1).fillna(False).astype(int)
        perf['is_longshot'] = (perf['popularity'] >= 5).fillna(False)
        perf['is_longshot_win'] = (perf['is_win'] == 1) & perf['is_longshot']
        
        # 人気と着順の差（過小評価スコア）
        perf['undervalued_score'] = perf['popularity'] - perf['finish_position']
        
        # ソート
        perf = perf.sort_values(['horse_id', 'race_date']).reset_index(drop=True)
        
        # 累積計算（shift(1)でリーク防止）
        # 穴馬勝利回数
        perf['horse_longshot_wins_lf'] = perf.groupby('horse_id')['is_longshot_win'].transform(
            lambda x: x.expanding().sum().shift(1)
        )
        
        # 過小評価スコア平均
        perf['horse_undervalued_avg_lf'] = perf.groupby('horse_id')['undervalued_score'].transform(
            lambda x: x.expanding().mean().shift(1)
        )
        
        # dfにマージ
        merge_cols = ['race_id', 'horse_id', 'horse_longshot_wins_lf', 'horse_undervalued_avg_lf']
        df = df.merge(
            perf[merge_cols].drop_duplicates(subset=['race_id', 'horse_id']),
            on=['race_id', 'horse_id'],
            how='left'
        )
        
        logging.info("    生成完了: horse_longshot_wins_lf, horse_undervalued_avg_lf")
        
        return df
    
    def remove_leak_features(self, features):
        """リーク版特徴量を除外"""
        clean_features = [f for f in features if f not in self.LEAK_PATTERNS]
        removed = len(features) - len(clean_features)
        logging.info(f"  リーク版特徴量を除外: {removed}件")
        return clean_features
    
    def prepare_features(self, df):
        """特徴量リストを準備"""
        with open(self.base_model_dir / 'feature_names.json', 'r', encoding='utf-8') as f:
            base_features = json.load(f)
        
        # リーク版を除外
        base_features = self.remove_leak_features(base_features)
        
        # リークフリー版を追加（既存）
        leak_free_features = [
            'jockey_win_rate_lf', 'trainer_win_rate_lf',
            'gap_jockey_pop_lf', 'gap_trainer_pop_lf',
            'jockey_venue_win_rate_lf', 'jockey_surface_win_rate_lf',
            'sire_win_rate_fixed', 'gap_sire_pop_lf',
        ]
        
        # v11.0 新特徴量
        new_features = [
            # ペース特徴量
            'venue_surface_pace_tendency', 'horse_pace_preference', 'horse_avg_pace_lf', 'pace_fit',
            # 配当傾向特徴量
            'jockey_avg_payout_lf', 'jockey_upset_rate_lf',
            'trainer_avg_payout_lf', 'trainer_upset_rate_lf',
            'sire_avg_payout_fixed', 'sire_upset_rate_fixed',
            # 馬の穴馬傾向
            'horse_longshot_wins_lf', 'horse_undervalued_avg_lf',
        ]
        
        all_features = base_features + leak_free_features + new_features
        available = [f for f in all_features if f in df.columns]
        available = list(dict.fromkeys(available))  # 重複除去
        
        logging.info(f"特徴量数（v11.0）: {len(available)}")
        return available
    
    def prepare_target(self, df):
        """ターゲット準備（穴馬ブースト付き）"""
        df['win_odds'] = pd.to_numeric(df['win_odds'], errors='coerce')
        df['popularity'] = pd.to_numeric(df['popularity'], errors='coerce')
        df['finish_position'] = pd.to_numeric(df['finish_position'], errors='coerce')
        
        odds = df['win_odds'].fillna(1.0).clip(upper=100)
        log_odds = np.log1p(odds)
        finish_pos = df['finish_position'].fillna(99)
        popularity = df['popularity'].fillna(1)
        
        # ターゲット: オッズ加重のrelevanceスコア
        gain = np.zeros(len(df))
        gain[finish_pos == 1] = np.nan_to_num(log_odds[finish_pos == 1] * 12.74, nan=0)
        gain[finish_pos == 2] = np.nan_to_num(log_odds[finish_pos == 2] * 6.73, nan=0)
        gain[finish_pos == 3] = np.nan_to_num(log_odds[finish_pos == 3] * 3.69, nan=0)
        df['target_relevance'] = np.nan_to_num(gain, nan=0).astype(int)
        
        # サンプル重み（穴馬ブースト）
        # 5番人気以下での勝利は2倍、3着以内は1.5倍
        base_weight = np.log1p(odds).clip(upper=np.log1p(100))
        is_longshot = (popularity >= 5)
        is_win = (finish_pos == 1)
        is_place = (finish_pos <= 3)
        
        boost = np.ones(len(df))
        boost[is_longshot & is_win] = 2.0  # 穴馬勝利は2倍
        boost[is_longshot & is_place & ~is_win] = 1.5  # 穴馬好走は1.5倍
        
        df['sample_weight'] = np.nan_to_num(base_weight * boost, nan=1.0)
        
        return df
    
    def train(self, df, feature_cols, n_trials=30):
        """モデル学習"""
        logging.info("=" * 60)
        logging.info("μモデル v11.0 学習開始")
        logging.info("★★★ 穴馬発掘強化 + 新特徴量統合 ★★★")
        logging.info("=" * 60)
        
        # データ分割
        train_df = df[df['race_date'] < '2023-01-01'].copy()
        valid_df = df[(df['race_date'] >= '2023-01-01') & (df['race_date'] < '2024-01-01')].copy()
        test_df = df[df['race_date'] >= '2024-01-01'].copy()
        
        # 欠損値処理
        for d in [train_df, valid_df, test_df]:
            for col in feature_cols:
                d[col] = d[col].fillna(0)
        
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
        
        def calculate_popularity_dist(d, preds):
            """人気別選択分布を計算"""
            d = d.copy()
            d['score'] = preds
            d['rank_pred'] = d.groupby('race_id')['score'].rank(ascending=False, method='first')
            selected = d[d['rank_pred'] == 1]
            pop_counts = selected['popularity'].value_counts().sort_index()
            fav1_rate = (selected['popularity'] == 1).sum() / len(selected) if len(selected) > 0 else 0
            return fav1_rate
        
        def objective(trial):
            reg = self.REGULARIZATION_RANGES
            params = {
                'objective': 'lambdarank', 'metric': 'ndcg', 'eval_at': [1, 3, 5],
                'boosting_type': 'gbdt', 'verbose': -1, 'random_state': 42,
                'label_gain': list(range(100)),
                'num_leaves': trial.suggest_int('num_leaves', *reg['num_leaves']),
                'lambda_l1': trial.suggest_float('lambda_l1', *reg['lambda_l1'], log=True),
                'lambda_l2': trial.suggest_float('lambda_l2', *reg['lambda_l2'], log=True),
                'min_child_samples': trial.suggest_int('min_child_samples', *reg['min_child_samples']),
                'learning_rate': trial.suggest_float('learning_rate', *reg['learning_rate'], log=True),
                'feature_fraction': trial.suggest_float('feature_fraction', *reg['feature_fraction']),
                'bagging_fraction': trial.suggest_float('bagging_fraction', *reg['bagging_fraction']),
                'bagging_freq': trial.suggest_int('bagging_freq', 1, 7),
                'max_depth': trial.suggest_int('max_depth', *reg['max_depth']),
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
        
        logging.info(f"Optuna {n_trials}トライアル...")
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
        valid_preds = self.model.predict(valid_df[feature_cols])
        test_preds = self.model.predict(test_df[feature_cols])
        
        valid_roi = calculate_roi(valid_df, valid_preds)
        test_roi = calculate_roi(test_df, test_preds)
        gap = abs(valid_roi - test_roi)
        
        valid_fav1_rate = calculate_popularity_dist(valid_df, valid_preds)
        test_fav1_rate = calculate_popularity_dist(test_df, test_preds)
        
        importance = dict(zip(feature_cols, self.model.feature_importances_))
        sorted_imp = sorted(importance.items(), key=lambda x: x[1], reverse=True)
        
        logging.info("=" * 60)
        logging.info("【最終結果: μ v11.0（穴馬発掘強化）】")
        logging.info("=" * 60)
        logging.info(f"  Valid ROI: {valid_roi:.2%}")
        logging.info(f"  Test ROI: {test_roi:.2%}")
        logging.info(f"  Valid-Test差: {gap:.2%}")
        logging.info(f"  過学習判定: {'✅ OK' if gap < 0.08 else '⚠️ 要注意' if gap < 0.12 else '🚨 過学習'}")
        logging.info("")
        logging.info(f"  Valid 1番人気選択率: {valid_fav1_rate:.1%}")
        logging.info(f"  Test  1番人気選択率: {test_fav1_rate:.1%}")
        logging.info(f"  穴馬発掘判定: {'✅ 改善' if test_fav1_rate < 0.70 else '△ やや改善' if test_fav1_rate < 0.75 else '✗ 未改善'}")
        logging.info("")
        logging.info("【Top 20特徴量】")
        for i, (name, imp) in enumerate(sorted_imp[:20], 1):
            leak_status = '✅' if any(x in name for x in ['lf', 'fixed', 'past_', 'pace', 'payout', 'upset']) else '❓'
            logging.info(f"  {i:2}. {leak_status} {name}: {imp}")
        
        self.save_model(feature_cols, valid_roi, test_roi, gap, best_params, 
                       valid_fav1_rate, test_fav1_rate)
        return valid_roi, test_roi, gap, test_fav1_rate
    
    def save_model(self, features, valid_roi, test_roi, gap, best_params, 
                   valid_fav1_rate, test_fav1_rate):
        """モデル保存"""
        with open(self.output_dir / 'mu_v11_0_ranker.pkl', 'wb') as f:
            pickle.dump(self.model, f)
        with open(self.output_dir / 'feature_names.json', 'w', encoding='utf-8') as f:
            json.dump(features, f, ensure_ascii=False, indent=2)
        
        save_params = {k: v for k, v in best_params.items() if k != 'label_gain'}
        metadata = {
            'version': 'v11.0',
            'description': '穴馬発掘強化版（ペース特徴量+配当傾向+穴馬ブースト学習）',
            'valid_roi': float(valid_roi),
            'test_roi': float(test_roi),
            'valid_test_gap': float(gap),
            'valid_fav1_rate': float(valid_fav1_rate),
            'test_fav1_rate': float(test_fav1_rate),
            'leak_free': True,
            'longshot_boost': True,
            'new_features': [
                'venue_surface_pace_tendency', 'horse_pace_preference', 'horse_avg_pace_lf', 'pace_fit',
                'jockey_avg_payout_lf', 'jockey_upset_rate_lf',
                'trainer_avg_payout_lf', 'trainer_upset_rate_lf',
                'sire_avg_payout_fixed', 'sire_upset_rate_fixed',
                'horse_longshot_wins_lf', 'horse_undervalued_avg_lf',
            ],
            'best_params': save_params,
            'created_at': datetime.now().isoformat()
        }
        with open(self.output_dir / 'model_info.json', 'w', encoding='utf-8') as f:
            json.dump(metadata, f, ensure_ascii=False, indent=2)
        
        logging.info(f"保存完了: {self.output_dir}")
    
    def run(self, n_trials=30):
        """メイン実行"""
        # データ読み込み
        train_data, races_df, race_details_df, returns_df = self.load_data()
        
        # 既存リークフリー特徴量
        train_data = self.generate_leak_free_features(train_data, races_df)
        
        # v11.0 新特徴量
        train_data = self.generate_new_features(train_data, races_df, race_details_df, returns_df)
        
        # ターゲット準備
        train_data = self.prepare_target(train_data)
        
        # 特徴量準備
        feature_cols = self.prepare_features(train_data)
        
        # 学習
        return self.train(train_data, feature_cols, n_trials)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description='μモデル v11.0 学習（穴馬発掘強化）')
    parser.add_argument('--trials', type=int, default=30, help='Optunaトライアル数')
    args = parser.parse_args()
    
    trainer = MuV110Trainer()
    valid_roi, test_roi, gap, fav1_rate = trainer.run(n_trials=args.trials)
    
    print("\n" + "=" * 60)
    print("【v11.0 結果サマリー】")
    print("=" * 60)
    print(f"  Valid ROI: {valid_roi:.2%}")
    print(f"  Test ROI:  {test_roi:.2%}")
    print(f"  Valid-Test差: {gap:.2%}")
    print(f"  v10.5比:    {test_roi - 0.8177:+.2%}")
    print(f"  1番人気選択率: {fav1_rate:.1%}")
    print(f"  過学習判定: {'✅ OK' if gap < 0.08 else '⚠️' if gap < 0.12 else '🚨'}")
    print(f"  穴馬発掘: {'✅ 改善' if fav1_rate < 0.70 else '△' if fav1_rate < 0.75 else '✗'}")
