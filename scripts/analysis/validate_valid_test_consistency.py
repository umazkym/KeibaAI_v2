#!/usr/bin/env python3
"""
Valid-Test一貫性検証スクリプト

【目的】
Edgeフィルタリングが「過学習」か「真の優位性」かを検証

【方法】
1. Valid期間（2023年）で最適Edge閾値を探索
2. その閾値をTest期間（2024年）にそのまま適用
3. Valid ROI と Test ROI の差を確認

【判定基準】
- 差が5%未満: フィルタリングは有効
- 差が5%以上: 過学習（フィルタリング戦略は無効）
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

from keibaai.src.models.calibration import IsotonicCalibrator

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class ValidTestConsistencyValidator:
    """Valid-Test一貫性検証クラス"""
    
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
        self.output_dir = Path('keibaai/models/mu_v6_0/valid_test_consistency')
        self.output_dir.mkdir(parents=True, exist_ok=True)
    
    @staticmethod
    def extract_venue_name(venue_str):
        if pd.isna(venue_str):
            return None
        match = re.search(r'[0-9]*([^\d]+)[0-9]*', str(venue_str))
        return match.group(1) if match else None
    
    @staticmethod
    def get_distance_category(distance_m):
        for cat, (low, high) in ValidTestConsistencyValidator.DISTANCE_CATEGORIES.items():
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
    
    def calculate_roi_for_edge_threshold(self, df, threshold):
        """Edge閾値でフィルタリングしたROIを計算"""
        df = df.copy()
        df['rank_pred'] = df.groupby('race_id')['score'].rank(ascending=False, method='first')
        
        bet = df[(df['rank_pred'] == 1) & (df['edge'] >= threshold)]
        n_races = df['race_id'].nunique()
        n_bets = len(bet)
        
        if n_bets == 0:
            return 0.0, 0.0, 0
        
        hits = bet[bet['finish_position'] == 1]
        roi = hits['win_odds'].sum() / n_bets
        ratio = n_bets / n_races
        
        return roi, ratio, n_bets
    
    def run(self):
        """検証実行"""
        logging.info("=" * 70)
        logging.info("Valid-Test一貫性検証")
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
            model = pickle.load(f)
        with open(self.v54_dir / 'feature_names.json', 'r', encoding='utf-8') as f:
            feature_cols = json.load(f)
        calibrator = IsotonicCalibrator.load(self.v60_dir / 'isotonic_calibrator.pkl')
        
        # 期間分割
        valid_df = train_data[(train_data['race_date'] >= '2023-01-01') & (train_data['race_date'] < '2024-01-01')].copy()
        test_df = train_data[train_data['race_date'] >= '2024-01-01'].copy()
        
        logging.info(f"  Valid: {len(valid_df):,} ({valid_df['race_id'].nunique():,}レース)")
        logging.info(f"  Test: {len(test_df):,} ({test_df['race_id'].nunique():,}レース)")
        
        # 特徴量補完
        for col in feature_cols:
            if col in valid_df.columns:
                median_val = valid_df[col].median()
                valid_df[col] = valid_df[col].fillna(median_val)
                test_df[col] = test_df[col].fillna(median_val)
        
        available_features = [f for f in feature_cols if f in valid_df.columns]
        logging.info(f"  利用可能特徴量: {len(available_features)}")
        
        # 予測とCalibration
        logging.info("\n【予測とCalibration】")
        
        for df, name in [(valid_df, 'Valid'), (test_df, 'Test')]:
            df['score'] = model.predict(df[available_features])
            df['pred_prob'] = df.groupby('race_id')['score'].transform(lambda x: softmax(x.values))
            df['calibrated_prob'] = calibrator.predict_proba(df['pred_prob'].values)
            
            # 市場勝率
            df['inv_odds'] = 1 / df['win_odds'].clip(lower=1.1)
            df['sum_inv_odds'] = df.groupby('race_id')['inv_odds'].transform('sum')
            df['market_prob'] = df['inv_odds'] / df['sum_inv_odds']
            
            # Edge
            df['edge'] = df['calibrated_prob'] - df['market_prob']
            
            logging.info(f"  {name}: 予測完了")
        
        # ========================================
        # Step 1: Valid期間で最適Edge閾値を探索
        # ========================================
        logging.info("\n" + "=" * 70)
        logging.info("【Step 1】Valid期間（2023年）で最適Edge閾値を探索")
        logging.info("=" * 70)
        
        edge_thresholds = [-0.05, 0.0, 0.02, 0.05, 0.08, 0.10, 0.12, 0.15, 0.18, 0.20]
        valid_results = []
        
        logging.info("\n  | 閾値 | ROI | 投資比率 | 投資数 |")
        logging.info("  |------|-----|----------|--------|")
        
        for threshold in edge_thresholds:
            roi, ratio, n_bets = self.calculate_roi_for_edge_threshold(valid_df, threshold)
            valid_results.append({
                'threshold': threshold,
                'roi': roi,
                'ratio': ratio,
                'n_bets': n_bets
            })
            marker = "★" if roi > 1.0 else ""
            logging.info(f"  | {threshold:+.2f} | {roi:.2%} | {ratio:.1%} | {n_bets:,} | {marker}")
        
        # 投資数500以上で最大ROIの閾値を選択
        valid_results_filtered = [r for r in valid_results if r['n_bets'] >= 200]
        if valid_results_filtered:
            best_valid = max(valid_results_filtered, key=lambda x: x['roi'])
        else:
            best_valid = max(valid_results, key=lambda x: x['roi'])
        
        optimal_threshold = best_valid['threshold']
        valid_roi = best_valid['roi']
        valid_n_bets = best_valid['n_bets']
        
        logging.info(f"\n  → 最適閾値: Edge≥{optimal_threshold:+.2f}")
        logging.info(f"  → Valid ROI: {valid_roi:.2%} (n={valid_n_bets})")
        
        # ========================================
        # Step 2: Test期間に最適閾値を適用
        # ========================================
        logging.info("\n" + "=" * 70)
        logging.info("【Step 2】Test期間（2024年）に最適閾値を適用")
        logging.info("=" * 70)
        
        test_roi, test_ratio, test_n_bets = self.calculate_roi_for_edge_threshold(test_df, optimal_threshold)
        
        logging.info(f"\n  閾値: Edge≥{optimal_threshold:+.2f}")
        logging.info(f"  Test ROI: {test_roi:.2%} (n={test_n_bets})")
        logging.info(f"  投資比率: {test_ratio:.1%}")
        
        # ========================================
        # Step 3: Valid-Test差を確認
        # ========================================
        logging.info("\n" + "=" * 70)
        logging.info("【Step 3】Valid-Test一貫性評価")
        logging.info("=" * 70)
        
        roi_diff = valid_roi - test_roi
        
        logging.info(f"\n  | 期間 | ROI | 投資数 |")
        logging.info(f"  |------|-----|--------|")
        logging.info(f"  | Valid (2023) | {valid_roi:.2%} | {valid_n_bets} |")
        logging.info(f"  | Test (2024) | {test_roi:.2%} | {test_n_bets} |")
        logging.info(f"  | **差分** | **{roi_diff:+.2%}** | - |")
        
        # 判定
        logging.info("\n【判定】")
        if abs(roi_diff) < 0.05:
            logging.info("  ✓ 差分5%未満 → フィルタリングは**一貫性あり**")
            if test_roi >= 1.0:
                logging.info("  ✓ Test ROI 100%超 → Edgeフィルタリングは有効な戦略")
                judgment = "VALID_AND_PROFITABLE"
            else:
                logging.info("  ⚠️ Test ROI 100%未満 → Edgeフィルタリングは一貫性あるが収益性なし")
                judgment = "VALID_BUT_NOT_PROFITABLE"
        else:
            logging.info(f"  ✗ 差分{abs(roi_diff):.1%} → フィルタリングは**過学習**")
            logging.info("  → フィルタリング戦略は無効。モデル精度向上に方向転換すべき")
            judgment = "OVERFIT"
        
        # ベースライン（全投資）との比較
        logging.info("\n【ベースラインとの比較】")
        
        baseline_valid_roi, _, baseline_valid_n = self.calculate_roi_for_edge_threshold(valid_df, -999)
        baseline_test_roi, _, baseline_test_n = self.calculate_roi_for_edge_threshold(test_df, -999)
        
        logging.info(f"  Valid ベースライン: {baseline_valid_roi:.2%}")
        logging.info(f"  Test ベースライン: {baseline_test_roi:.2%}")
        logging.info(f"  フィルタ適用後の改善: {test_roi - baseline_test_roi:+.2%}")
        
        # 結果保存
        results = {
            'analysis_date': datetime.now().isoformat(),
            'optimal_threshold': optimal_threshold,
            'valid': {
                'roi': float(valid_roi),
                'n_bets': valid_n_bets,
                'baseline_roi': float(baseline_valid_roi)
            },
            'test': {
                'roi': float(test_roi),
                'n_bets': test_n_bets,
                'baseline_roi': float(baseline_test_roi)
            },
            'roi_diff': float(roi_diff),
            'judgment': judgment,
            'all_thresholds_valid': valid_results
        }
        
        with open(self.output_dir / 'valid_test_consistency.json', 'w', encoding='utf-8') as f:
            json.dump(results, f, ensure_ascii=False, indent=2)
        
        logging.info(f"\n  結果保存: {self.output_dir}")
        logging.info("=" * 70)
        
        return results


if __name__ == "__main__":
    validator = ValidTestConsistencyValidator()
    validator.run()
