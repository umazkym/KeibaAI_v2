#!/usr/bin/env python3
"""
Phase 9: モデル精度向上 & 投資対象変更

【実行内容】
1. XGBoost vs LightGBM比較
2. 複勝（Top3予測）的中率分析
3. アンサンブル学習効果検証
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

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class ModelImprovementAnalyzer:
    """モデル改善分析クラス"""
    
    DISTANCE_CATEGORIES = {
        'sprint': (0, 1400),
        'mile': (1400, 1800),
        'intermediate': (1800, 2200),
        'long': (2200, 9999)
    }
    
    def __init__(self):
        self.base_model_dir = Path('keibaai/models/mu_v3_3')
        self.v54_dir = Path('keibaai/models/mu_v5_4')
        self.output_dir = Path('keibaai/models/phase9_analysis')
        self.output_dir.mkdir(parents=True, exist_ok=True)
    
    @staticmethod
    def extract_venue_name(venue_str):
        if pd.isna(venue_str):
            return None
        match = re.search(r'[0-9]*([^\d]+)[0-9]*', str(venue_str))
        return match.group(1) if match else None
    
    @staticmethod
    def get_distance_category(distance_m):
        for cat, (low, high) in ModelImprovementAnalyzer.DISTANCE_CATEGORIES.items():
            if low <= distance_m < high:
                return cat
        return 'intermediate'
    
    @staticmethod
    def calculate_normalized_rank(finish, n_runners):
        if pd.isna(finish) or pd.isna(n_runners) or n_runners <= 1:
            return np.nan
        return 1.0 - (finish - 1) / (n_runners - 1)
    
    def _generate_horse_features(self, df, races_df):
        """馬特徴量生成（簡略版）"""
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
    
    def analyze_place_prediction(self, test_df, model, feature_cols):
        """複勝（3着以内）予測分析"""
        logging.info("\n" + "=" * 70)
        logging.info("【分析1】複勝（Top3）予測分析")
        logging.info("=" * 70)
        
        test_df = test_df.copy()
        test_df['rank_pred'] = test_df.groupby('race_id')['score'].rank(ascending=False, method='first')
        
        # Top1, Top2, Top3それぞれの複勝的中率を確認
        results = []
        for top_n in [1, 2, 3]:
            bet = test_df[test_df['rank_pred'] == top_n]
            hits = bet[bet['finish_position'] <= 3]
            n_bets = len(bet)
            n_hits = len(hits)
            hit_rate = n_hits / n_bets if n_bets > 0 else 0
            
            # 複勝オッズがないため、単勝オッズから推定
            # 複勝オッズ ≈ (単勝オッズ - 1) / 3 + 1 （経験則）
            hits['estimated_place_odds'] = (hits['win_odds'] - 1) / 3 + 1
            estimated_roi = hits['estimated_place_odds'].sum() / n_bets if n_bets > 0 else 0
            
            results.append({
                'top_n': top_n,
                'n_bets': n_bets,
                'n_hits': n_hits,
                'hit_rate': hit_rate,
                'estimated_place_roi': estimated_roi
            })
            
            logging.info(f"  Top{top_n}: 的中率 {hit_rate:.1%} ({n_hits}/{n_bets}), 推定ROI {estimated_roi:.2%}")
        
        # 参考: 実際の複勝期待値
        logging.info("\n  【参考】複勝の理論値")
        logging.info("    期待的中率: 約30% (平均3/10頭)")
        logging.info("    複勝控除率: 20%")
        logging.info("    損益分岐的中率: 約37.5%")
        
        return results
    
    def train_xgboost(self, train_df, valid_df, feature_cols):
        """XGBoostモデルを学習"""
        logging.info("\n" + "=" * 70)
        logging.info("【分析2】XGBoost vs LightGBM比較")
        logging.info("=" * 70)
        
        try:
            import xgboost as xgb
        except ImportError:
            logging.warning("  XGBoostがインストールされていません")
            return None
        
        # データ準備
        available_features = [f for f in feature_cols if f in train_df.columns and f in valid_df.columns]
        
        X_train = train_df[available_features]
        y_train = (train_df['finish_position'] == 1).astype(int)
        X_valid = valid_df[available_features]
        y_valid = (valid_df['finish_position'] == 1).astype(int)
        
        # レースごとのグループ
        train_groups = train_df.groupby('race_id').size().values
        valid_groups = valid_df.groupby('race_id').size().values
        
        logging.info(f"  特徴量数: {len(available_features)}")
        logging.info(f"  Train: {len(X_train):,}, Valid: {len(X_valid):,}")
        
        # XGBoost Rankerモデル
        logging.info("  XGBoost学習中...")
        
        xgb_model = xgb.XGBRanker(
            objective='rank:pairwise',
            n_estimators=200,
            learning_rate=0.05,
            max_depth=6,
            subsample=0.8,
            colsample_bytree=0.8,
            random_state=42,
            n_jobs=-1
        )
        
        # 欠損値を0で埋める
        X_train_filled = X_train.fillna(0)
        X_valid_filled = X_valid.fillna(0)
        
        xgb_model.fit(
            X_train_filled, y_train,
            group=train_groups,
            eval_set=[(X_valid_filled, y_valid)],
            eval_group=[valid_groups],
            verbose=False
        )
        
        logging.info("  XGBoost学習完了")
        
        return xgb_model, available_features
    
    def compare_models(self, test_df, lgb_model, xgb_model, feature_cols):
        """LightGBMとXGBoostを比較"""
        logging.info("\n【モデル比較】")
        
        available_features = [f for f in feature_cols if f in test_df.columns]
        X_test = test_df[available_features].fillna(0)
        
        # LightGBM予測
        test_df['lgb_score'] = lgb_model.predict(X_test)
        test_df['lgb_rank'] = test_df.groupby('race_id')['lgb_score'].rank(ascending=False, method='first')
        
        # XGBoost予測
        test_df['xgb_score'] = xgb_model.predict(X_test)
        test_df['xgb_rank'] = test_df.groupby('race_id')['xgb_score'].rank(ascending=False, method='first')
        
        # ROI計算
        lgb_bet = test_df[test_df['lgb_rank'] == 1]
        lgb_hits = lgb_bet[lgb_bet['finish_position'] == 1]
        lgb_roi = lgb_hits['win_odds'].sum() / len(lgb_bet) if len(lgb_bet) > 0 else 0
        
        xgb_bet = test_df[test_df['xgb_rank'] == 1]
        xgb_hits = xgb_bet[xgb_bet['finish_position'] == 1]
        xgb_roi = xgb_hits['win_odds'].sum() / len(xgb_bet) if len(xgb_bet) > 0 else 0
        
        logging.info(f"  | モデル | ROI | 的中率 |")
        logging.info(f"  |--------|-----|--------|")
        logging.info(f"  | LightGBM | {lgb_roi:.2%} | {len(lgb_hits)/len(lgb_bet):.1%} |")
        logging.info(f"  | XGBoost | {xgb_roi:.2%} | {len(xgb_hits)/len(xgb_bet):.1%} |")
        
        # アンサンブル
        test_df['ensemble_score'] = (test_df['lgb_score'] + test_df['xgb_score']) / 2
        test_df['ensemble_rank'] = test_df.groupby('race_id')['ensemble_score'].rank(ascending=False, method='first')
        
        ens_bet = test_df[test_df['ensemble_rank'] == 1]
        ens_hits = ens_bet[ens_bet['finish_position'] == 1]
        ens_roi = ens_hits['win_odds'].sum() / len(ens_bet) if len(ens_bet) > 0 else 0
        
        logging.info(f"  | Ensemble | {ens_roi:.2%} | {len(ens_hits)/len(ens_bet):.1%} |")
        
        return {
            'lgb_roi': float(lgb_roi),
            'xgb_roi': float(xgb_roi),
            'ensemble_roi': float(ens_roi)
        }
    
    def run(self):
        """分析実行"""
        logging.info("=" * 70)
        logging.info("Phase 9: モデル精度向上 & 投資対象変更")
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
        
        train_data['horse_id'] = train_data['horse_id'].astype(str)
        races_df['horse_id'] = races_df['horse_id'].astype(str)
        
        # 特徴量生成
        logging.info("特徴量生成中...")
        train_data = self._generate_horse_features(train_data, races_df)
        train_data = self._generate_jockey_sire_features(train_data, races_df, pedigree_df, shutuba_df)
        
        # モデル読み込み
        with open(self.v54_dir / 'mu_v5_4_ranker.pkl', 'rb') as f:
            lgb_model = pickle.load(f)
        with open(self.v54_dir / 'feature_names.json', 'r', encoding='utf-8') as f:
            feature_cols = json.load(f)
        
        # 期間分割
        train_df = train_data[(train_data['race_date'] >= '2020-01-01') & (train_data['race_date'] < '2023-01-01')].copy()
        valid_df = train_data[(train_data['race_date'] >= '2023-01-01') & (train_data['race_date'] < '2024-01-01')].copy()
        test_df = train_data[train_data['race_date'] >= '2024-01-01'].copy()
        
        logging.info(f"  Train: {len(train_df):,}")
        logging.info(f"  Valid: {len(valid_df):,}")
        logging.info(f"  Test: {len(test_df):,}")
        
        # 特徴量補完
        for col in feature_cols:
            if col in train_df.columns:
                median_val = train_df[col].median()
                train_df[col] = train_df[col].fillna(median_val)
                valid_df[col] = valid_df[col].fillna(median_val)
                test_df[col] = test_df[col].fillna(median_val)
        
        available_features = [f for f in feature_cols if f in test_df.columns]
        
        # LightGBM予測（既存）
        test_df['score'] = lgb_model.predict(test_df[available_features])
        
        # 分析1: 複勝予測
        place_results = self.analyze_place_prediction(test_df, lgb_model, feature_cols)
        
        # 分析2: XGBoost比較
        xgb_result = self.train_xgboost(train_df, valid_df, feature_cols)
        
        if xgb_result is not None:
            xgb_model, available_features = xgb_result
            comparison = self.compare_models(test_df, lgb_model, xgb_model, available_features)
        else:
            comparison = None
        
        # 結果保存
        results = {
            'analysis_date': datetime.now().isoformat(),
            'place_prediction': place_results,
            'model_comparison': comparison
        }
        
        with open(self.output_dir / 'phase9_results.json', 'w', encoding='utf-8') as f:
            json.dump(results, f, ensure_ascii=False, indent=2)
        
        logging.info(f"\n  結果保存: {self.output_dir}")
        logging.info("=" * 70)
        
        return results


if __name__ == "__main__":
    analyzer = ModelImprovementAnalyzer()
    analyzer.run()
