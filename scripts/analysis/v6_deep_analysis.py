#!/usr/bin/env python3
"""
v6.0 深掘り分析スクリプト

【目的】
1. EV閾値1.5の293件の詳細分析
2. Edge-Based Filtering の検証
3. Calibration過補正の検証

【レビュー指摘対応】
- 「どんな条件でモデルが優位性を持つか」を把握
- 市場勝率との差分（Edge）による選別を検証
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
from collections import Counter

from keibaai.src.models.calibration import IsotonicCalibrator

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class V6DeepAnalyzer:
    """v6.0深掘り分析クラス"""
    
    DISTANCE_CATEGORIES = {
        'sprint': (0, 1400),
        'mile': (1400, 1800),
        'intermediate': (1800, 2200),
        'long': (2200, 9999)
    }
    
    def __init__(self):
        self.base_model_dir = Path('keibaai/models/mu_v3_3')
        self.v54_dir = Path('keibaai/models/mu_v5_4')
        self.v60_dir = Path('keibaai/models/mu_v6_0')
        self.output_dir = Path('keibaai/models/mu_v6_0/deep_analysis')
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.model = None
        self.calibrator = None
    
    @staticmethod
    def extract_venue_name(venue_str):
        if pd.isna(venue_str):
            return None
        match = re.search(r'[0-9]*([^\d]+)[0-9]*', str(venue_str))
        return match.group(1) if match else None
    
    @staticmethod
    def get_distance_category(distance_m):
        for cat, (low, high) in V6DeepAnalyzer.DISTANCE_CATEGORIES.items():
            if low <= distance_m < high:
                return cat
        return 'intermediate'
    
    @staticmethod
    def calculate_normalized_rank(finish, n_runners):
        if pd.isna(finish) or pd.isna(n_runners) or n_runners <= 1:
            return np.nan
        return 1.0 - (finish - 1) / (n_runners - 1)
    
    def load_models(self):
        """モデルとCalibratorを読み込み"""
        logging.info("モデル読み込み中...")
        
        with open(self.v54_dir / 'mu_v5_4_ranker.pkl', 'rb') as f:
            self.model = pickle.load(f)
        
        with open(self.v54_dir / 'feature_names.json', 'r', encoding='utf-8') as f:
            self.feature_cols = json.load(f)
        
        self.calibrator = IsotonicCalibrator.load(self.v60_dir / 'isotonic_calibrator.pkl')
        
        logging.info(f"  特徴量数: {len(self.feature_cols)}")
        logging.info(f"  Calibrator: {self.calibrator}")
    
    def load_and_prepare_data(self):
        """データ読み込みと特徴量生成"""
        logging.info("データ読み込み中...")
        
        train_data = pd.read_parquet(self.base_model_dir / 'train_data_mu_v3_3.parquet')
        train_data['race_date'] = pd.to_datetime(train_data['race_date'])
        
        races_df = pd.read_parquet('keibaai/data/parsed/parquet/races/races.parquet')
        races_df['race_date'] = pd.to_datetime(races_df['race_date'])
        
        shutuba_df = pd.read_parquet('keibaai/data/parsed/parquet/shutuba/shutuba.parquet')
        
        pedigree_path = Path('keibaai/data/parsed/parquet/pedigrees/pedigrees.parquet')
        pedigree_df = pd.read_parquet(pedigree_path) if pedigree_path.exists() else None
        
        train_data['horse_id'] = train_data['horse_id'].astype(str)
        races_df['horse_id'] = races_df['horse_id'].astype(str)
        
        # 特徴量生成（v5.4と同様）
        logging.info("特徴量生成中...")
        train_data = self._generate_horse_features(train_data, races_df)
        train_data = self._generate_jockey_sire_features(train_data, races_df, pedigree_df, shutuba_df)
        
        # 分析に必要なカラムをraces_dfからマージ
        race_info = races_df[['race_id', 'horse_id', 'track_condition', 'distance_m', 'track_surface', 'venue']].drop_duplicates(subset=['race_id', 'horse_id'])
        for col in ['track_condition', 'distance_m', 'track_surface', 'venue']:
            if col not in train_data.columns:
                train_data = train_data.merge(race_info[['race_id', 'horse_id', col]], on=['race_id', 'horse_id'], how='left')
        
        return train_data
    
    def _generate_horse_features(self, df, races_df):
        """馬特徴量生成"""
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
        for col in horse_features:
            merge_df[col] = pd.to_numeric(merge_df[col], errors='coerce')
        
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
    
    def scores_to_probs(self, df, score_col='score'):
        """スコアを確率に変換"""
        return df.groupby('race_id')[score_col].transform(lambda x: softmax(x.values))
    
    def calculate_market_prob(self, df):
        """市場勝率を計算（控除率補正後）"""
        df = df.copy()
        df['inv_odds'] = 1 / df['win_odds'].clip(lower=1.1)
        df['sum_inv_odds'] = df.groupby('race_id')['inv_odds'].transform('sum')
        df['market_prob'] = df['inv_odds'] / df['sum_inv_odds']
        return df['market_prob']
    
    def analyze_ev_threshold_15(self, test_df):
        """EV閾値1.5の293件を詳細分析"""
        logging.info("\n" + "=" * 70)
        logging.info("【分析1】EV閾値1.5の詳細分析")
        logging.info("=" * 70)
        
        # EV計算
        test_df['ev'] = test_df['calibrated_prob'] * test_df['win_odds']
        test_df['rank_pred'] = test_df.groupby('race_id')['score'].rank(ascending=False, method='first')
        
        # EV≥1.5 かつ Top1の馬を抽出
        high_ev = test_df[(test_df['rank_pred'] == 1) & (test_df['ev'] >= 1.5)].copy()
        logging.info(f"対象件数: {len(high_ev)}")
        
        # 的中/不的中を判定
        high_ev['is_hit'] = (high_ev['finish_position'] == 1).astype(int)
        hit_count = high_ev['is_hit'].sum()
        hit_rate = high_ev['is_hit'].mean()
        logging.info(f"的中数: {hit_count}, 的中率: {hit_rate:.1%}")
        
        # ROI計算
        roi = high_ev[high_ev['is_hit'] == 1]['win_odds'].sum() / len(high_ev)
        logging.info(f"ROI: {roi:.2%}")
        
        # === 属性分析 ===
        logging.info("\n【属性分布】")
        
        # 1. 人気分布
        logging.info("\n1. 人気分布:")
        pop_dist = high_ev['popularity'].value_counts().sort_index()
        hit_by_pop = high_ev.groupby('popularity')['is_hit'].agg(['sum', 'count', 'mean'])
        for pop in sorted(high_ev['popularity'].dropna().unique())[:10]:
            cnt = pop_dist.get(pop, 0)
            hit_info = hit_by_pop.loc[pop] if pop in hit_by_pop.index else {'sum': 0, 'count': 0, 'mean': 0}
            logging.info(f"   {int(pop)}番人気: {cnt}件 (的中:{int(hit_info['sum'])}件, {hit_info['mean']:.1%})")
        
        # 2. オッズ帯分布
        logging.info("\n2. オッズ帯分布:")
        odds_bins = [(1, 5), (5, 10), (10, 20), (20, 50), (50, 100)]
        for low, high in odds_bins:
            mask = (high_ev['win_odds'] >= low) & (high_ev['win_odds'] < high)
            subset = high_ev[mask]
            if len(subset) > 0:
                hits = subset['is_hit'].sum()
                sub_roi = subset[subset['is_hit'] == 1]['win_odds'].sum() / len(subset) if len(subset) > 0 else 0
                logging.info(f"   {low}-{high}倍: {len(subset)}件, 的中:{hits}, ROI:{sub_roi:.2%}")
        
        # 3. 距離分布
        logging.info("\n3. 距離カテゴリ分布:")
        high_ev['dist_cat'] = high_ev['distance_m'].apply(self.get_distance_category)
        for cat in ['sprint', 'mile', 'intermediate', 'long']:
            subset = high_ev[high_ev['dist_cat'] == cat]
            if len(subset) > 0:
                hits = subset['is_hit'].sum()
                sub_roi = subset[subset['is_hit'] == 1]['win_odds'].sum() / len(subset) if len(subset) > 0 else 0
                logging.info(f"   {cat}: {len(subset)}件, 的中:{hits}, ROI:{sub_roi:.2%}")
        
        # 4. 馬場状態分布
        logging.info("\n4. 馬場状態分布:")
        for cond in ['良', '稍重', '重', '不良']:
            subset = high_ev[high_ev['track_condition'] == cond]
            if len(subset) > 0:
                hits = subset['is_hit'].sum()
                sub_roi = subset[subset['is_hit'] == 1]['win_odds'].sum() / len(subset) if len(subset) > 0 else 0
                logging.info(f"   {cond}: {len(subset)}件, 的中:{hits}, ROI:{sub_roi:.2%}")
        
        # 5. 時系列分布
        logging.info("\n5. 時系列分布（月別）:")
        high_ev['month'] = high_ev['race_date'].dt.to_period('M')
        for month in sorted(high_ev['month'].unique()):
            subset = high_ev[high_ev['month'] == month]
            hits = subset['is_hit'].sum()
            sub_roi = subset[subset['is_hit'] == 1]['win_odds'].sum() / len(subset) if len(subset) > 0 else 0
            logging.info(f"   {month}: {len(subset)}件, 的中:{hits}, ROI:{sub_roi:.2%}")
        
        return high_ev
    
    def analyze_edge_based_filtering(self, test_df):
        """Edge-Based Filtering（市場勝率との差分）を検証"""
        logging.info("\n" + "=" * 70)
        logging.info("【分析2】Edge-Based Filtering")
        logging.info("=" * 70)
        
        # 市場勝率を計算
        test_df['market_prob'] = self.calculate_market_prob(test_df)
        
        # Edge = モデル勝率 - 市場勝率
        test_df['edge'] = test_df['calibrated_prob'] - test_df['market_prob']
        test_df['edge_raw'] = test_df['pred_prob'] - test_df['market_prob']  # Calibration前
        
        test_df['rank_pred'] = test_df.groupby('race_id')['score'].rank(ascending=False, method='first')
        
        logging.info("\n【Edge閾値別ROI】")
        logging.info("  | Edge閾値 | ROI | 投資比率 | 投資数 | 的中率 |")
        logging.info("  |----------|-----|----------|--------|--------|")
        
        edge_results = []
        for threshold in [-0.05, 0.0, 0.02, 0.05, 0.08, 0.10, 0.15]:
            bet = test_df[(test_df['rank_pred'] == 1) & (test_df['edge'] >= threshold)]
            n_races = test_df['race_id'].nunique()
            n_bets = len(bet)
            
            if n_bets == 0:
                continue
            
            hits = bet[bet['finish_position'] == 1]
            roi = hits['win_odds'].sum() / n_bets
            hit_rate = len(hits) / n_bets
            ratio = n_bets / n_races
            
            marker = "★" if roi > 1.0 else ("▲" if roi > 0.85 else "")
            logging.info(f"  | {threshold:+.2f} | {roi:.2%} | {ratio:.1%} | {n_bets:,} | {hit_rate:.1%} | {marker}")
            
            edge_results.append({
                'threshold': threshold,
                'roi': float(roi),
                'investment_ratio': float(ratio),
                'n_bets': int(n_bets),
                'hit_rate': float(hit_rate)
            })
        
        # Calibration前との比較
        logging.info("\n【Calibration前のEdge閾値別ROI】")
        logging.info("  | Edge閾値 | ROI | 投資比率 | 投資数 |")
        logging.info("  |----------|-----|----------|--------|")
        
        for threshold in [0.0, 0.05, 0.10]:
            bet = test_df[(test_df['rank_pred'] == 1) & (test_df['edge_raw'] >= threshold)]
            n_races = test_df['race_id'].nunique()
            n_bets = len(bet)
            
            if n_bets == 0:
                continue
            
            hits = bet[bet['finish_position'] == 1]
            roi = hits['win_odds'].sum() / n_bets
            ratio = n_bets / n_races
            
            logging.info(f"  | {threshold:+.2f} | {roi:.2%} | {ratio:.1%} | {n_bets:,} |")
        
        return edge_results
    
    def analyze_calibration_overcorrection(self, test_df):
        """Calibration過補正を検証"""
        logging.info("\n" + "=" * 70)
        logging.info("【分析3】Calibration過補正の検証")
        logging.info("=" * 70)
        
        # EV計算（補正前後）
        test_df['ev_before'] = test_df['pred_prob'] * test_df['win_odds']
        test_df['ev_after'] = test_df['calibrated_prob'] * test_df['win_odds']
        
        # EV<1.0→≥1.0に移行した馬
        moved_up = test_df[(test_df['ev_before'] < 1.0) & (test_df['ev_after'] >= 1.0)]
        logging.info(f"\nEV<1.0 → ≥1.0 に移行した馬: {len(moved_up):,}件")
        
        actual_win_rate = (moved_up['finish_position'] == 1).mean()
        expected_win_rate = moved_up['calibrated_prob'].mean()
        logging.info(f"  実際の勝率: {actual_win_rate:.2%}")
        logging.info(f"  予測勝率（補正後）: {expected_win_rate:.2%}")
        logging.info(f"  差分: {actual_win_rate - expected_win_rate:+.2%}")
        
        if actual_win_rate < expected_win_rate * 0.8:
            logging.warning("  ⚠️ 過補正の可能性あり！予測より20%以上低い勝率")
        
        # EV≥1.0→<1.0に移行した馬
        moved_down = test_df[(test_df['ev_before'] >= 1.0) & (test_df['ev_after'] < 1.0)]
        logging.info(f"\nEV≥1.0 → <1.0 に移行した馬: {len(moved_down):,}件")
        
        if len(moved_down) > 0:
            actual_win_rate_down = (moved_down['finish_position'] == 1).mean()
            expected_win_rate_down = moved_down['pred_prob'].mean()
            logging.info(f"  実際の勝率: {actual_win_rate_down:.2%}")
            logging.info(f"  予測勝率（補正前）: {expected_win_rate_down:.2%}")
        
        # 確率帯別の補正効果
        logging.info("\n【確率帯別の補正効果】")
        logging.info("  | 帯 | 予測(前) | 予測(後) | 実勝率 | 判定 |")
        logging.info("  |----|----------|----------|--------|------|")
        
        for low, high in [(0.05, 0.10), (0.10, 0.15), (0.15, 0.20), (0.20, 0.30)]:
            mask = (test_df['pred_prob'] >= low) & (test_df['pred_prob'] < high)
            subset = test_df[mask]
            if len(subset) > 0:
                pred_before = subset['pred_prob'].mean()
                pred_after = subset['calibrated_prob'].mean()
                actual = (subset['finish_position'] == 1).mean()
                
                diff_before = abs(actual - pred_before)
                diff_after = abs(actual - pred_after)
                judgment = "✓改善" if diff_after < diff_before else "✗悪化"
                
                logging.info(f"  | {low:.0%}-{high:.0%} | {pred_before:.3f} | {pred_after:.3f} | {actual:.3f} | {judgment} |")
        
        return moved_up, moved_down
    
    def run(self):
        """メイン実行"""
        logging.info("=" * 70)
        logging.info("v6.0 深掘り分析")
        logging.info("=" * 70)
        
        # モデル読み込み
        self.load_models()
        
        # データ準備
        train_data = self.load_and_prepare_data()
        
        # Test期間のデータ
        test_df = train_data[train_data['race_date'] >= '2024-01-01'].copy()
        logging.info(f"\nTestデータ: {len(test_df):,} ({test_df['race_id'].nunique():,}レース)")
        
        # 欠損値補完
        for col in self.feature_cols:
            if col in test_df.columns:
                test_df[col] = test_df[col].fillna(test_df[col].median())
        
        available_features = [f for f in self.feature_cols if f in test_df.columns]
        
        # 予測
        test_df['score'] = self.model.predict(test_df[available_features])
        test_df['pred_prob'] = self.scores_to_probs(test_df, 'score')
        test_df['calibrated_prob'] = self.calibrator.predict_proba(test_df['pred_prob'].values)
        
        # 分析実行
        high_ev_df = self.analyze_ev_threshold_15(test_df)
        edge_results = self.analyze_edge_based_filtering(test_df)
        moved_up, moved_down = self.analyze_calibration_overcorrection(test_df)
        
        # 結果保存
        logging.info("\n【保存中】...")
        
        results = {
            'analysis_date': datetime.now().isoformat(),
            'ev_threshold_15': {
                'count': len(high_ev_df),
                'hit_count': int(high_ev_df['is_hit'].sum()),
                'hit_rate': float(high_ev_df['is_hit'].mean()),
                'roi': float(high_ev_df[high_ev_df['is_hit'] == 1]['win_odds'].sum() / len(high_ev_df)),
            },
            'edge_results': edge_results,
            'calibration_check': {
                'moved_up_count': len(moved_up),
                'moved_up_actual_win_rate': float((moved_up['finish_position'] == 1).mean()) if len(moved_up) > 0 else None,
                'moved_up_predicted_win_rate': float(moved_up['calibrated_prob'].mean()) if len(moved_up) > 0 else None,
            }
        }
        
        with open(self.output_dir / 'deep_analysis_results.json', 'w', encoding='utf-8') as f:
            json.dump(results, f, ensure_ascii=False, indent=2)
        
        logging.info(f"  保存完了: {self.output_dir}")
        logging.info("=" * 70)
        
        return results


if __name__ == "__main__":
    analyzer = V6DeepAnalyzer()
    analyzer.run()
