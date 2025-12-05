#!/usr/bin/env python3
"""
μモデル v4.0 学習スクリプト

【新規特徴量】
- 騎手: left_turn_ae, right_turn_ae, steep_slope_ae, wet_ae
- 種牡馬: turf_dirt_diff, wet_diff, left_right_diff, steep_flat_diff

【設計思想】
- A/E値と正規化着順を採用
- 左右回り・急坂/平坦の物理特性で分類
- ベイズスムージング・時間減衰を適用
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


class MuV40Trainer:
    """μモデル v4.0 学習クラス"""
    
    # コース物理特性
    LEFT_TURN_VENUES = ['東京', '新潟', '中京']
    RIGHT_TURN_VENUES = ['中山', '京都', '阪神', '札幌', '函館', '福島', '小倉']
    STEEP_SLOPE_VENUES = ['中山', '阪神', '中京']
    
    # 信頼度フィルタ
    MIN_SAMPLES = {'jockey': 10, 'sire': 30}
    
    # 時間減衰率
    TIME_DECAY_RATE = 0.3
    
    def __init__(self):
        self.base_model_dir = Path('keibaai/models/mu_v3_3')
        self.output_dir = Path('keibaai/models/mu_v4_0')
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.model = None
    
    @staticmethod
    def extract_venue_name(venue_str: str) -> str:
        """'5中山8'から'中山'を抽出"""
        if pd.isna(venue_str):
            return None
        match = re.search(r'[0-9]*([^\d]+)[0-9]*', str(venue_str))
        if match:
            return match.group(1)
        return None
    
    @staticmethod
    def get_turn_direction(venue: str) -> str:
        if venue in MuV40Trainer.LEFT_TURN_VENUES:
            return 'left'
        return 'right'
    
    @staticmethod
    def get_slope_type(venue: str) -> str:
        if venue in MuV40Trainer.STEEP_SLOPE_VENUES:
            return 'steep'
        return 'flat'
    
    @staticmethod
    def calculate_normalized_rank(finish: int, n_runners: int) -> float:
        if n_runners <= 1:
            return 0.5
        return 1.0 - (finish - 1) / (n_runners - 1)
    
    @staticmethod
    def calculate_ae(actual: float, expected: float) -> float:
        if expected <= 0:
            return 1.0
        return actual / expected
    
    @staticmethod
    def time_decay_weight(years_ago: float, decay_rate: float = 0.3) -> float:
        return np.exp(-decay_rate * years_ago)
    
    def load_data(self):
        """データ読み込み"""
        logging.info("データを読み込み中...")
        
        # 学習データ（特徴量付き）
        train_data = pd.read_parquet(self.base_model_dir / 'train_data_mu_v3_3.parquet')
        train_data['race_date'] = pd.to_datetime(train_data['race_date'])
        
        # 過去戦績データ（Context-Aware特徴量生成用）
        perf_path = Path('keibaai/data/parsed/parquet/horses_performance/horses_performance.parquet')
        perf_df = pd.read_parquet(perf_path)
        perf_df['race_date'] = pd.to_datetime(perf_df['race_date'])
        
        # 出馬表データ（jockey_id取得用）- race_id形式がtrain_dataと一致
        shutuba_path = Path('keibaai/data/parsed/parquet/shutuba/shutuba.parquet')
        shutuba_df = pd.read_parquet(shutuba_path)
        
        # 血統データ
        pedigree_path = Path('keibaai/data/parsed/parquet/pedigree/pedigree.parquet')
        pedigree_df = pd.read_parquet(pedigree_path) if pedigree_path.exists() else None
        
        logging.info(f"  学習データ: {len(train_data):,} rows")
        logging.info(f"  過去戦績: {len(perf_df):,} rows")
        logging.info(f"  出馬表: {len(shutuba_df):,} rows")
        
        return train_data, perf_df, pedigree_df, shutuba_df
    
    def generate_context_aware_features(self, df: pd.DataFrame, perf_df: pd.DataFrame,
                                         pedigree_df: pd.DataFrame = None,
                                         shutuba_df: pd.DataFrame = None) -> pd.DataFrame:
        """Context-Aware特徴量を生成"""
        logging.info("=" * 60)
        logging.info("v4.0 Context-Aware特徴量を生成中...")
        logging.info("=" * 60)
        
        # perf_dfの前処理
        perf = perf_df.copy()
        perf['venue_name'] = perf['venue'].apply(self.extract_venue_name)
        perf['turn_direction'] = perf['venue_name'].apply(
            lambda x: self.get_turn_direction(x) if x else None
        )
        perf['slope_type'] = perf['venue_name'].apply(
            lambda x: self.get_slope_type(x) if x else None
        )
        perf['is_wet'] = perf['track_condition'].isin(['重', '不良'])
        
        # 正規化着順
        perf['n_runners'] = perf['head_count'].fillna(16)
        perf['normalized_rank'] = perf.apply(
            lambda x: self.calculate_normalized_rank(x['finish_position'], x['n_runners']),
            axis=1
        )
        
        # NaN除去
        perf = perf.dropna(subset=['finish_position', 'n_runners'])
        perf['finish_position'] = perf['finish_position'].astype(int)
        
        # 期待勝利数（オッズベース）
        perf['expected_win'] = 1 / perf['win_odds'].clip(lower=1.1).fillna(10)
        perf['is_win'] = (perf['finish_position'] == 1).astype(int)
        
        # 時間減衰重み
        max_date = perf['race_date'].max()
        perf['years_ago'] = (max_date - perf['race_date']).dt.days / 365.25
        perf['time_weight'] = perf['years_ago'].apply(
            lambda x: self.time_decay_weight(x, self.TIME_DECAY_RATE)
        )
        
        result = df.copy()
        
        # ========================================
        # 騎手特徴量
        # ========================================
        if 'jockey_id' in perf.columns:
            logging.info("  騎手特徴量を生成中...")
            
            # 左回りA/E
            left_data = perf[perf['turn_direction'] == 'left']
            left_stats = left_data.groupby('jockey_id').apply(
                lambda g: pd.Series({
                    'actual': (g['is_win'] * g['time_weight']).sum(),
                    'expected': (g['expected_win'] * g['time_weight']).sum(),
                    'count': len(g)
                })
            ).reset_index()
            left_stats['jockey_left_turn_ae'] = left_stats.apply(
                lambda x: self.calculate_ae(x['actual'], x['expected']), axis=1
            )
            left_stats.loc[left_stats['count'] < self.MIN_SAMPLES['jockey'], 
                           'jockey_left_turn_ae'] = np.nan
            
            # 右回りA/E
            right_data = perf[perf['turn_direction'] == 'right']
            right_stats = right_data.groupby('jockey_id').apply(
                lambda g: pd.Series({
                    'actual': (g['is_win'] * g['time_weight']).sum(),
                    'expected': (g['expected_win'] * g['time_weight']).sum(),
                    'count': len(g)
                })
            ).reset_index()
            right_stats['jockey_right_turn_ae'] = right_stats.apply(
                lambda x: self.calculate_ae(x['actual'], x['expected']), axis=1
            )
            right_stats.loc[right_stats['count'] < self.MIN_SAMPLES['jockey'],
                            'jockey_right_turn_ae'] = np.nan
            
            # 急坂A/E
            steep_data = perf[perf['slope_type'] == 'steep']
            steep_stats = steep_data.groupby('jockey_id').apply(
                lambda g: pd.Series({
                    'actual': (g['is_win'] * g['time_weight']).sum(),
                    'expected': (g['expected_win'] * g['time_weight']).sum(),
                    'count': len(g)
                })
            ).reset_index()
            steep_stats['jockey_steep_slope_ae'] = steep_stats.apply(
                lambda x: self.calculate_ae(x['actual'], x['expected']), axis=1
            )
            steep_stats.loc[steep_stats['count'] < self.MIN_SAMPLES['jockey'],
                            'jockey_steep_slope_ae'] = np.nan
            
            # 道悪A/E
            wet_data = perf[perf['is_wet']]
            wet_stats = wet_data.groupby('jockey_id').apply(
                lambda g: pd.Series({
                    'actual': (g['is_win'] * g['time_weight']).sum(),
                    'expected': (g['expected_win'] * g['time_weight']).sum(),
                    'count': len(g)
                })
            ).reset_index()
            wet_stats['jockey_wet_ae'] = wet_stats.apply(
                lambda x: self.calculate_ae(x['actual'], x['expected']), axis=1
            )
            wet_stats.loc[wet_stats['count'] < self.MIN_SAMPLES['jockey'],
                          'jockey_wet_ae'] = np.nan
            
            # train_dataにjockey_idを追加（shutubaから取得 - race_id形式が一致）
            if 'jockey_id' not in result.columns and shutuba_df is not None:
                jockey_map = shutuba_df[['race_id', 'horse_id', 'jockey_id']].drop_duplicates(subset=['race_id', 'horse_id'])
                result = result.merge(jockey_map, on=['race_id', 'horse_id'], how='left')
                logging.info(f"    jockey_idマッチ率: {result['jockey_id'].notna().mean()*100:.1f}%")
            
            # マージ
            result = result.merge(left_stats[['jockey_id', 'jockey_left_turn_ae']], 
                                  on='jockey_id', how='left')
            result = result.merge(right_stats[['jockey_id', 'jockey_right_turn_ae']], 
                                  on='jockey_id', how='left')
            result = result.merge(steep_stats[['jockey_id', 'jockey_steep_slope_ae']], 
                                  on='jockey_id', how='left')
            result = result.merge(wet_stats[['jockey_id', 'jockey_wet_ae']], 
                                  on='jockey_id', how='left')
            
            logging.info("    left_turn_ae, right_turn_ae, steep_slope_ae, wet_ae 生成完了")
        
        # ========================================
        # 種牡馬特徴量
        # ========================================
        if pedigree_df is not None and 'generation' in pedigree_df.columns:
            logging.info("  種牡馬特徴量を生成中...")
            
            # sire_idマッピング
            sire_map = pedigree_df[pedigree_df['generation'] == 1][['horse_id', 'ancestor_id']].drop_duplicates('horse_id')
            sire_map.columns = ['horse_id', 'sire_id']
            
            perf_with_sire = perf.merge(sire_map, on='horse_id', how='left')
            perf_with_sire = perf_with_sire.dropna(subset=['sire_id'])
            
            # 芝NR
            turf_data = perf_with_sire[perf_with_sire['track_surface'] == '芝']
            turf_stats = turf_data.groupby('sire_id').agg({
                'normalized_rank': 'mean',
                'race_id': 'count'
            }).reset_index()
            turf_stats.columns = ['sire_id', 'sire_turf_nr', 'sire_turf_count']
            turf_stats.loc[turf_stats['sire_turf_count'] < self.MIN_SAMPLES['sire'],
                           'sire_turf_nr'] = np.nan
            
            # ダートNR
            dirt_data = perf_with_sire[perf_with_sire['track_surface'] == 'ダート']
            dirt_stats = dirt_data.groupby('sire_id').agg({
                'normalized_rank': 'mean',
                'race_id': 'count'
            }).reset_index()
            dirt_stats.columns = ['sire_id', 'sire_dirt_nr', 'sire_dirt_count']
            dirt_stats.loc[dirt_stats['sire_dirt_count'] < self.MIN_SAMPLES['sire'],
                           'sire_dirt_nr'] = np.nan
            
            # 芝-ダート差分
            surface_stats = turf_stats.merge(dirt_stats, on='sire_id', how='outer')
            surface_stats['sire_turf_dirt_diff'] = surface_stats['sire_turf_nr'].fillna(0.5) - surface_stats['sire_dirt_nr'].fillna(0.5)
            
            # 左回りNR
            left_sire = perf_with_sire[perf_with_sire['turn_direction'] == 'left']
            left_sire_stats = left_sire.groupby('sire_id').agg({
                'normalized_rank': 'mean',
                'race_id': 'count'
            }).reset_index()
            left_sire_stats.columns = ['sire_id', 'sire_left_nr', 'sire_left_count']
            
            # 右回りNR
            right_sire = perf_with_sire[perf_with_sire['turn_direction'] == 'right']
            right_sire_stats = right_sire.groupby('sire_id').agg({
                'normalized_rank': 'mean',
                'race_id': 'count'
            }).reset_index()
            right_sire_stats.columns = ['sire_id', 'sire_right_nr', 'sire_right_count']
            
            # 左-右差分
            turn_stats = left_sire_stats.merge(right_sire_stats, on='sire_id', how='outer')
            turn_stats['sire_left_right_diff'] = turn_stats['sire_left_nr'].fillna(0.5) - turn_stats['sire_right_nr'].fillna(0.5)
            
            # 良馬場NR
            good_sire = perf_with_sire[~perf_with_sire['is_wet']]
            good_sire_stats = good_sire.groupby('sire_id').agg({'normalized_rank': 'mean'}).reset_index()
            good_sire_stats.columns = ['sire_id', 'sire_good_nr']
            
            # 道悪NR
            wet_sire = perf_with_sire[perf_with_sire['is_wet']]
            wet_sire_stats = wet_sire.groupby('sire_id').agg({
                'normalized_rank': 'mean',
                'race_id': 'count'
            }).reset_index()
            wet_sire_stats.columns = ['sire_id', 'sire_wet_nr', 'sire_wet_count']
            wet_sire_stats.loc[wet_sire_stats['sire_wet_count'] < 10, 'sire_wet_nr'] = np.nan
            
            # 道悪-良馬場差分
            cond_stats = good_sire_stats.merge(wet_sire_stats, on='sire_id', how='outer')
            cond_stats['sire_wet_diff'] = cond_stats['sire_wet_nr'].fillna(0.5) - cond_stats['sire_good_nr'].fillna(0.5)
            
            # dfにsire_idをマージ
            result = result.merge(sire_map, on='horse_id', how='left')
            result = result.merge(surface_stats[['sire_id', 'sire_turf_dirt_diff']], 
                                  on='sire_id', how='left')
            result = result.merge(turn_stats[['sire_id', 'sire_left_right_diff']], 
                                  on='sire_id', how='left')
            result = result.merge(cond_stats[['sire_id', 'sire_wet_diff']], 
                                  on='sire_id', how='left')
            
            logging.info("    turf_dirt_diff, left_right_diff, wet_diff 生成完了")
        
        logging.info("=" * 60)
        
        return result
    
    def prepare_features(self, df: pd.DataFrame) -> list:
        """特徴量準備"""
        with open(self.base_model_dir / 'feature_names.json', 'r', encoding='utf-8') as f:
            base_features = json.load(f)
        
        new_features = [
            'jockey_left_turn_ae', 'jockey_right_turn_ae', 
            'jockey_steep_slope_ae', 'jockey_wet_ae',
            'sire_turf_dirt_diff', 'sire_left_right_diff', 'sire_wet_diff'
        ]
        
        all_features = base_features + [f for f in new_features if f in df.columns]
        available = [f for f in all_features if f in df.columns]
        
        logging.info(f"特徴量数: {len(available)}（ベース: {len(base_features)}, 新規: {len(available) - len(base_features)}）")
        
        return available
    
    def prepare_target(self, df: pd.DataFrame):
        """ターゲット変数準備"""
        weight_1st, weight_2nd, weight_3rd = 12.74, 6.73, 3.69
        odds = df['win_odds'].fillna(1.0).clip(upper=90)
        log_odds = np.log1p(odds)
        
        gain = np.zeros(len(df))
        gain[df['finish_position'] == 1] = log_odds[df['finish_position'] == 1] * weight_1st
        gain[df['finish_position'] == 2] = log_odds[df['finish_position'] == 2] * weight_2nd
        gain[df['finish_position'] == 3] = log_odds[df['finish_position'] == 3] * weight_3rd
        
        df['target_relevance'] = gain.astype(int)
        df['sample_weight'] = np.log1p(df['win_odds'].fillna(1.0)).clip(upper=np.log1p(100))
        
        return df
    
    def train(self, df: pd.DataFrame, feature_cols: list, n_trials: int = 30):
        """モデル学習"""
        logging.info("=" * 60)
        logging.info("μモデル v4.0 学習開始（Context-Aware版）")
        logging.info("=" * 60)
        
        # 分割
        train_mask = df['race_date'] < '2023-01-01'
        valid_mask = (df['race_date'] >= '2023-01-01') & (df['race_date'] < '2024-01-01')
        test_mask = df['race_date'] >= '2024-01-01'
        
        train_df = df[train_mask].copy()
        valid_df = df[valid_mask].copy()
        test_df = df[test_mask].copy()
        
        group_train = train_df.groupby('race_id').size().to_list()
        group_valid = valid_df.groupby('race_id').size().to_list()
        
        logging.info(f"Train: {len(train_df):,}, Valid: {len(valid_df):,}, Test: {len(test_df):,}")
        
        # ROI計算
        def calculate_roi(d, preds):
            d = d.copy()
            d['score'] = preds
            d['rank_pred'] = d.groupby('race_id')['score'].rank(ascending=False, method='first')
            bet_df = d[d['rank_pred'] == 1]
            hits = bet_df[bet_df['finish_position'] == 1]
            return hits['win_odds'].sum() / len(bet_df) if len(bet_df) > 0 else 0
        
        # Optuna
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
                eval_group=[group_valid],
                callbacks=[lgb.early_stopping(stopping_rounds=50, verbose=False)]
            )
            
            preds = model.predict(valid_df[feature_cols])
            return calculate_roi(valid_df, preds)
        
        logging.info(f"Optuna最適化開始（{n_trials}トライアル）...")
        study = optuna.create_study(direction='maximize')
        study.optimize(objective, n_trials=n_trials, show_progress_bar=True)
        
        logging.info(f"最適Valid ROI: {study.best_value:.2%}")
        
        # 最終モデル
        best_params = study.best_params
        best_params.update({
            'objective': 'lambdarank', 'metric': 'ndcg', 'eval_at': [1, 3, 5],
            'boosting_type': 'gbdt', 'verbose': -1, 'random_state': 42,
            'label_gain': list(range(100))
        })
        
        self.model = lgb.LGBMRanker(**best_params, n_estimators=2000)
        self.model.fit(
            train_df[feature_cols], train_df['target_relevance'],
            group=group_train, sample_weight=train_df['sample_weight'],
            eval_set=[(valid_df[feature_cols], valid_df['target_relevance'])],
            eval_group=[group_valid],
            callbacks=[lgb.early_stopping(stopping_rounds=100, verbose=True)]
        )
        
        # 評価
        valid_preds = self.model.predict(valid_df[feature_cols])
        valid_roi = calculate_roi(valid_df, valid_preds)
        
        test_preds = self.model.predict(test_df[feature_cols])
        test_roi = calculate_roi(test_df, test_preds)
        
        # 特徴量重要度
        importance = dict(zip(feature_cols, self.model.feature_importances_))
        sorted_imp = sorted(importance.items(), key=lambda x: x[1], reverse=True)
        
        logging.info("=" * 60)
        logging.info(f"【最終結果: μ v4.0（Context-Aware版）】")
        logging.info("=" * 60)
        logging.info(f"  Valid ROI: {valid_roi:.2%}")
        logging.info(f"  Test ROI: {test_roi:.2%}")
        logging.info(f"  Valid-Test差: {abs(valid_roi - test_roi):.2%}")
        logging.info("")
        logging.info("【特徴量重要度 Top 20】")
        for name, imp in sorted_imp[:20]:
            logging.info(f"  {name}: {imp}")
        
        logging.info("")
        logging.info("【新規特徴量の重要度】")
        new_features = ['jockey_left_turn_ae', 'jockey_right_turn_ae', 
                        'jockey_steep_slope_ae', 'jockey_wet_ae',
                        'sire_turf_dirt_diff', 'sire_left_right_diff', 'sire_wet_diff']
        for f in new_features:
            if f in importance:
                logging.info(f"  {f}: {importance[f]}")
        
        # 保存
        self.save_model(feature_cols, valid_roi, test_roi, best_params)
        
        return valid_roi, test_roi
    
    def save_model(self, features, valid_roi, test_roi, best_params):
        """モデル保存"""
        with open(self.output_dir / 'mu_v4_0_ranker.pkl', 'wb') as f:
            pickle.dump(self.model, f)
        
        with open(self.output_dir / 'feature_names.json', 'w', encoding='utf-8') as f:
            json.dump(features, f, ensure_ascii=False, indent=2)
        
        save_params = {k: v for k, v in best_params.items() if k != 'label_gain'}
        metadata = {
            'version': 'v4.0',
            'description': 'Context-Aware版（A/E値・正規化着順・左右回り・急坂/平坦）',
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
        train_data, perf_df, pedigree_df, shutuba_df = self.load_data()
        train_data = self.generate_context_aware_features(train_data, perf_df, pedigree_df, shutuba_df)
        train_data = self.prepare_target(train_data)
        feature_cols = self.prepare_features(train_data)
        return self.train(train_data, feature_cols, n_trials=n_trials)


if __name__ == "__main__":
    trainer = MuV40Trainer()
    trainer.run(n_trials=30)
