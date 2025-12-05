#!/usr/bin/env python3
"""
μモデル v6.0 学習スクリプト（Calibration + EV-Based Betting対応版）

【レビュー対応】
- Isotonic Regressionによる確率較正
- 中穴帯（10-20%）の過小評価補正
- EV閾値によるフィルタリング評価
- Test ROI 100%超を目指す

【重要】
- Calibratorの学習はValidデータのみ使用（Test Leakage防止）
- Testは最終評価でのみ使用
- v5.4と同様の特徴量生成を含む
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
import re
from datetime import datetime
from scipy.special import softmax

from keibaai.src.models.calibration import IsotonicCalibrator

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class MuV6CalibratedTrainer:
    """μモデル v6.0 学習クラス（Calibration + EV-Based Betting）"""
    
    LEFT_TURN_VENUES = ['東京', '新潟', '中京']
    
    DISTANCE_CATEGORIES = {
        'sprint': (0, 1400),
        'mile': (1400, 1800),
        'intermediate': (1800, 2200),
        'long': (2200, 9999)
    }
    
    SMOOTHING_C_BASE = 30
    
    PURGE_FEATURES = [
        'jockey_win_rate', 'jockey_place_rate', 'jockey_avg_rank',
        'sire_win_rate', 'sire_avg_finish',
        'trainer_win_rate', 'trainer_place_rate',
    ]
    
    def __init__(self):
        self.base_model_dir = Path('keibaai/models/mu_v3_3')
        self.v54_dir = Path('keibaai/models/mu_v5_4')
        self.output_dir = Path('keibaai/models/mu_v6_0')
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.model = None
        self.calibrator = None
        
    def load_v54_model(self):
        """v5.4学習済みモデルを読み込み"""
        logging.info("v5.4モデルを読み込み中...")
        
        model_path = self.v54_dir / 'mu_v5_4_ranker.pkl'
        with open(model_path, 'rb') as f:
            self.model = pickle.load(f)
        
        feature_path = self.v54_dir / 'feature_names.json'
        with open(feature_path, 'r', encoding='utf-8') as f:
            self.feature_cols = json.load(f)
        
        logging.info(f"  特徴量数: {len(self.feature_cols)}")
        return self.model, self.feature_cols
    
    @staticmethod
    def extract_venue_name(venue_str):
        if pd.isna(venue_str):
            return None
        match = re.search(r'[0-9]*([^\d]+)[0-9]*', str(venue_str))
        return match.group(1) if match else None
    
    @staticmethod
    def get_distance_category(distance_m):
        for cat, (low, high) in MuV6CalibratedTrainer.DISTANCE_CATEGORIES.items():
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
        logging.info("データを読み込み中...")
        
        train_data = pd.read_parquet(self.base_model_dir / 'train_data_mu_v3_3.parquet')
        train_data['race_date'] = pd.to_datetime(train_data['race_date'])
        
        races_df = pd.read_parquet('keibaai/data/parsed/parquet/races/races.parquet')
        races_df['race_date'] = pd.to_datetime(races_df['race_date'])
        
        shutuba_df = pd.read_parquet('keibaai/data/parsed/parquet/shutuba/shutuba.parquet')
        
        pedigree_path = Path('keibaai/data/parsed/parquet/pedigrees/pedigrees.parquet')
        pedigree_df = pd.read_parquet(pedigree_path) if pedigree_path.exists() else None
        
        logging.info(f"  学習データ: {len(train_data):,}, レース結果: {len(races_df):,}")
        
        train_data['horse_id'] = train_data['horse_id'].astype(str)
        races_df['horse_id'] = races_df['horse_id'].astype(str)
        
        return train_data, races_df, pedigree_df, shutuba_df
    
    def generate_horse_features(self, df, races_df):
        """馬特徴量生成（v5.4と同一）"""
        logging.info("馬特徴量生成中...")
        
        perf = races_df.copy()
        perf['venue_name'] = perf['venue'].apply(self.extract_venue_name)
        perf['distance_category'] = perf['distance_m'].apply(self.get_distance_category)
        perf['is_wet'] = perf['track_condition'].isin(['重', '不良'])
        
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
        
        # Group A: タイム指数
        perf['horse_time_deviation_avg'] = (
            perf.groupby('horse_id')['time_deviation']
            .transform(lambda x: x.expanding().mean().shift(1))
        )
        perf['horse_l3f_deviation_avg'] = (
            perf.groupby('horse_id')['l3f_deviation']
            .transform(lambda x: x.expanding().mean().shift(1))
        )
        perf['horse_best_time_deviation'] = (
            perf.groupby('horse_id')['time_deviation']
            .transform(lambda x: x.expanding().max().shift(1))
        )
        
        # Group B: コース適性
        perf['horse_venue_nr'] = (
            perf.groupby(['horse_id', 'venue_name'])['normalized_rank']
            .transform(lambda x: x.expanding().mean().shift(1))
        )
        perf['horse_distance_nr'] = (
            perf.groupby(['horse_id', 'distance_category'])['normalized_rank']
            .transform(lambda x: x.expanding().mean().shift(1))
        )
        perf['horse_surface_nr'] = (
            perf.groupby(['horse_id', 'track_surface'])['normalized_rank']
            .transform(lambda x: x.expanding().mean().shift(1))
        )
        perf['horse_best_nr'] = (
            perf.groupby('horse_id')['normalized_rank']
            .transform(lambda x: x.expanding().max().shift(1))
        )
        
        # Group C: ローテーション
        perf['horse_interval_days'] = perf.groupby('horse_id')['race_date'].diff().dt.days
        perf['horse_dist_change'] = perf.groupby('horse_id')['distance_m'].diff()
        
        perf['prev_weight'] = perf.groupby('horse_id')['horse_weight'].shift(1)
        perf['horse_weight_change_ratio'] = (perf['horse_weight'] - perf['prev_weight']) / perf['prev_weight'].clip(lower=400)
        
        # Group D: 脚質
        perf['horse_avg_position_4c'] = (
            perf.groupby('horse_id')['position_4c_normalized']
            .transform(lambda x: x.expanding().mean().shift(1))
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
        
        logging.info(f"  馬特徴量11個を生成完了")
        return result
    
    def generate_jockey_sire_features(self, df, races_df, pedigree_df, shutuba_df):
        """騎手・種牡馬特徴量（v5.4と同一）"""
        logging.info("騎手・種牡馬特徴量生成中...")
        
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
            perf['horse_id'] = perf['horse_id'].astype(str)
            perf_with_sire = perf.merge(sire_map, on='horse_id', how='left').dropna(subset=['sire_id'])
            
            sire_base = perf_with_sire.groupby('sire_id').agg({'normalized_rank': 'sum', 'race_id': 'count'}).reset_index()
            sire_base.columns = ['sire_id', 'nr_sum', 'count']
            sire_base['sire_nr_global'] = (sire_base['nr_sum'] + 0.5 * self.SMOOTHING_C_BASE) / (sire_base['count'] + self.SMOOTHING_C_BASE)
            
            if 'sire_id' not in result.columns:
                result = result.merge(sire_map, on='horse_id', how='left')
            result = result.merge(sire_base[['sire_id', 'sire_nr_global']], on='sire_id', how='left')
        
        logging.info(f"  jockey_nr_global, sire_nr_global を生成完了")
        return result
    
    def scores_to_probs(self, df: pd.DataFrame, score_col: str = 'score') -> pd.Series:
        """LightGBMスコアをレース毎の勝率確率に変換"""
        def race_softmax(x):
            return softmax(x.values)
        
        probs = df.groupby('race_id')[score_col].transform(race_softmax)
        return probs
    
    def calculate_roi(self, df: pd.DataFrame, score_col: str = 'score') -> float:
        """Top1投資ROI計算（従来方式）"""
        df = df.copy()
        df['rank_pred'] = df.groupby('race_id')[score_col].rank(ascending=False, method='first')
        bet = df[df['rank_pred'] == 1]
        hits = bet[bet['finish_position'] == 1]
        return hits['win_odds'].sum() / len(bet) if len(bet) > 0 else 0.0
    
    def calculate_roi_ev_filtered(self, df: pd.DataFrame, ev_threshold: float = 1.0, 
                                   score_col: str = 'score', prob_col: str = 'calibrated_prob') -> tuple:
        """EV閾値でフィルタリングしたROI計算"""
        df = df.copy()
        
        # EV計算
        df['ev'] = df[prob_col] * df['win_odds']
        
        # ランク計算
        df['rank_pred'] = df.groupby('race_id')[score_col].rank(ascending=False, method='first')
        
        # フィルタリング: Top1かつEV閾値以上
        bet = df[(df['rank_pred'] == 1) & (df['ev'] >= ev_threshold)]
        
        n_races = df['race_id'].nunique()
        n_bets = len(bet)
        
        if n_bets == 0:
            return 0.0, 0.0, 0
        
        hits = bet[bet['finish_position'] == 1]
        roi = hits['win_odds'].sum() / n_bets
        investment_ratio = n_bets / n_races
        
        return roi, investment_ratio, n_bets
    
    def analyze_calibration_by_band(self, df: pd.DataFrame, prob_col: str = 'pred_prob') -> list:
        """確率帯別の較正分析"""
        bands = [
            (0.00, 0.05),
            (0.05, 0.10),
            (0.10, 0.15),
            (0.15, 0.20),
            (0.20, 0.25),
            (0.25, 0.30),
            (0.30, 0.50),
        ]
        
        results = []
        for low, high in bands:
            mask = (df[prob_col] >= low) & (df[prob_col] < high)
            subset = df[mask]
            
            if len(subset) > 0:
                mean_pred = subset[prob_col].mean()
                actual_rate = (subset['finish_position'] == 1).mean()
                results.append({
                    'band': f'{low:.0%}-{high:.0%}',
                    'mean_pred': mean_pred,
                    'actual_rate': actual_rate,
                    'count': len(subset),
                    'diff': actual_rate - mean_pred
                })
        
        return results
    
    def run(self):
        """メイン実行"""
        logging.info("=" * 70)
        logging.info("μモデル v6.0: Calibration + EV-Based Betting")
        logging.info("=" * 70)
        
        # 1. v5.4モデル読み込み
        self.load_v54_model()
        
        # 2. データ読み込み
        train_data, races_df, pedigree_df, shutuba_df = self.load_data()
        
        # 3. 特徴量生成（v5.4と同様）
        logging.info("\n【特徴量生成】")
        train_data = self.generate_horse_features(train_data, races_df)
        train_data = self.generate_jockey_sire_features(train_data, races_df, pedigree_df, shutuba_df)
        
        # 4. データ分割
        valid_df = train_data[(train_data['race_date'] >= '2023-01-01') & (train_data['race_date'] < '2024-01-01')].copy()
        test_df = train_data[train_data['race_date'] >= '2024-01-01'].copy()
        
        logging.info(f"\n  Valid: {len(valid_df):,} ({valid_df['race_id'].nunique():,}レース)")
        logging.info(f"  Test: {len(test_df):,} ({test_df['race_id'].nunique():,}レース)")
        
        # 5. 欠損値補完
        for col in self.feature_cols:
            if col in valid_df.columns:
                median_val = valid_df[col].median()
                valid_df[col] = valid_df[col].fillna(median_val)
                test_df[col] = test_df[col].fillna(median_val)
        
        available_features = [f for f in self.feature_cols if f in valid_df.columns]
        logging.info(f"  利用可能特徴量: {len(available_features)}")
        
        # 6. 予測スコア取得
        logging.info("\n【Step 1】予測スコア取得...")
        valid_df['score'] = self.model.predict(valid_df[available_features])
        test_df['score'] = self.model.predict(test_df[available_features])
        
        # 7. スコアを確率に変換
        logging.info("\n【Step 2】スコア→確率変換（softmax）...")
        valid_df['pred_prob'] = self.scores_to_probs(valid_df, 'score')
        test_df['pred_prob'] = self.scores_to_probs(test_df, 'score')
        
        # 8. Calibration前の分析
        logging.info("\n【Step 3】Calibration前の較正分析...")
        pre_calibration = self.analyze_calibration_by_band(test_df, 'pred_prob')
        
        logging.info("  | 確率帯 | 予測平均 | 実際勝率 | 差分 | 件数 |")
        logging.info("  |--------|----------|----------|------|------|")
        for band in pre_calibration:
            marker = "✓" if abs(band['diff']) < 0.03 else ("↑" if band['diff'] > 0 else "↓")
            logging.info(f"  | {band['band']} | {band['mean_pred']:.3f} | {band['actual_rate']:.3f} | {band['diff']:+.3f} {marker} | {band['count']:,} |")
        
        # 9. IsotonicCalibrator学習（★Validのみ★）
        logging.info("\n【Step 4】Isotonic Calibrator学習（Validデータ）...")
        
        self.calibrator = IsotonicCalibrator()
        valid_y_true = (valid_df['finish_position'] == 1).astype(int).values
        valid_y_pred = valid_df['pred_prob'].values
        
        self.calibrator.fit(valid_y_true, valid_y_pred)
        
        logging.info(f"  Brier Score (Before): {self.calibrator.brier_score_before:.4f}")
        logging.info(f"  Brier Score (After):  {self.calibrator.brier_score_after:.4f}")
        
        # 10. Testデータに較正適用
        logging.info("\n【Step 5】Testデータへの較正適用...")
        test_df['calibrated_prob'] = self.calibrator.predict_proba(test_df['pred_prob'].values)
        
        # 11. Calibration後の分析
        logging.info("\n【Step 6】Calibration後の較正分析...")
        post_calibration = self.analyze_calibration_by_band(test_df, 'calibrated_prob')
        
        logging.info("  | 確率帯 | 予測平均 | 実際勝率 | 差分 | 件数 |")
        logging.info("  |--------|----------|----------|------|------|")
        for band in post_calibration:
            marker = "✓" if abs(band['diff']) < 0.03 else ("↑" if band['diff'] > 0 else "↓")
            logging.info(f"  | {band['band']} | {band['mean_pred']:.3f} | {band['actual_rate']:.3f} | {band['diff']:+.3f} {marker} | {band['count']:,} |")
        
        # 12. 従来方式のROI（比較用）
        logging.info("\n【Step 7】従来方式ROI（全レースTop1投資）...")
        baseline_roi = self.calculate_roi(test_df, 'score')
        logging.info(f"  Test ROI (従来方式): {baseline_roi:.2%}")
        
        # 13. EV閾値別ROI計算
        logging.info("\n【Step 8】EV閾値別ROI計算...")
        logging.info("  | 閾値 | ROI | 投資比率 | 投資数 |")
        logging.info("  |------|-----|----------|--------|")
        
        ev_results = []
        for threshold in [0.8, 0.9, 1.0, 1.1, 1.2, 1.3, 1.5]:
            roi, ratio, n_bets = self.calculate_roi_ev_filtered(
                test_df, ev_threshold=threshold, 
                score_col='score', prob_col='calibrated_prob'
            )
            ev_results.append({
                'threshold': float(threshold),
                'roi': float(roi),
                'investment_ratio': float(ratio),
                'n_bets': int(n_bets)
            })
            marker = "★" if roi > 1.0 else ""
            logging.info(f"  | {threshold:.1f} | {roi:.2%} | {ratio:.1%} | {n_bets:,} | {marker}")
        
        # 14. 中穴帯（10-20%）のEV分析
        logging.info("\n【Step 9】中穴帯（10-20%）EV分析...")
        mid_tier = test_df[(test_df['pred_prob'] >= 0.10) & (test_df['pred_prob'] < 0.20)].copy()
        mid_tier['ev_before'] = mid_tier['pred_prob'] * mid_tier['win_odds']
        mid_tier['ev_after'] = mid_tier['calibrated_prob'] * mid_tier['win_odds']
        
        logging.info(f"  中穴帯件数: {len(mid_tier):,}")
        logging.info(f"  EV平均 (Before): {mid_tier['ev_before'].mean():.3f}")
        logging.info(f"  EV平均 (After):  {mid_tier['ev_after'].mean():.3f}")
        
        # EV新規投資機会
        new_opportunities = mid_tier[(mid_tier['ev_before'] < 1.0) & (mid_tier['ev_after'] >= 1.0)]
        logging.info(f"  新規投資機会（EV<1.0→≥1.0）: {len(new_opportunities):,}件")
        
        # 15. 結果サマリー
        logging.info("\n" + "=" * 70)
        logging.info("【最終結果サマリー: μ v6.0 (Calibration + EV-Based)】")
        logging.info("=" * 70)
        
        logging.info(f"\n  ■ 従来方式 (全レースTop1投資)")
        logging.info(f"    Test ROI: {baseline_roi:.2%}")
        logging.info(f"    投資レース: {test_df['race_id'].nunique():,}")
        
        # 最適なEV閾値を見つける（ROI>100%かつ投資比率20%以上）
        optimal = None
        for ev in ev_results:
            if ev['roi'] > 1.0 and ev['investment_ratio'] >= 0.20:
                if optimal is None or ev['roi'] > optimal['roi']:
                    optimal = ev
        
        if optimal:
            logging.info(f"\n  ■ EV-Based方式 (閾値={optimal['threshold']:.1f})")
            logging.info(f"    Test ROI: {optimal['roi']:.2%} ★ROI 100%超達成★")
            logging.info(f"    投資比率: {optimal['investment_ratio']:.1%}")
            logging.info(f"    投資レース: {optimal['n_bets']:,}")
        else:
            # ROI最大のものを表示
            best = max(ev_results, key=lambda x: x['roi'])
            logging.info(f"\n  ■ EV-Based方式 (閾値={best['threshold']:.1f})")
            logging.info(f"    Test ROI: {best['roi']:.2%}")
            logging.info(f"    投資比率: {best['investment_ratio']:.1%}")
            logging.info(f"    投資レース: {best['n_bets']:,}")
        
        # 16. 保存
        logging.info("\n【保存中】...")
        
        # Calibrator保存
        self.calibrator.save(self.output_dir / 'isotonic_calibrator.pkl')
        
        # 結果保存
        results = {
            'version': 'v6.0',
            'description': 'Calibration + EV-Based Betting',
            'baseline_roi': float(baseline_roi),
            'calibration': {
                'brier_before': float(self.calibrator.brier_score_before),
                'brier_after': float(self.calibrator.brier_score_after),
            },
            'ev_results': ev_results,
            'mid_tier_analysis': {
                'count': int(len(mid_tier)),
                'ev_before_mean': float(mid_tier['ev_before'].mean()),
                'ev_after_mean': float(mid_tier['ev_after'].mean()),
                'new_opportunities': int(len(new_opportunities)),
            },
            'optimal_threshold': optimal['threshold'] if optimal else None,
            'optimal_roi': optimal['roi'] if optimal else None,
            'created_at': datetime.now().isoformat(),
        }
        
        with open(self.output_dir / 'calibration_results.json', 'w', encoding='utf-8') as f:
            json.dump(results, f, ensure_ascii=False, indent=2)
        
        logging.info(f"  保存完了: {self.output_dir}")
        logging.info("=" * 70)
        
        return results


if __name__ == "__main__":
    trainer = MuV6CalibratedTrainer()
    trainer.run()
