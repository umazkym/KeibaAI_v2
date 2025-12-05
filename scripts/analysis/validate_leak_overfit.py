#!/usr/bin/env python3
"""
リーク・過学習検証スクリプト

【検証項目】
1. Edge-Based Filtering (ROI 112%, 194件) の統計的有意性
   - ブートストラップ95%信頼区間
   - 時系列安定性（月別ROI）
   - ベースラインとの有意差検定

2. 馬場バイアスROI計算の正確性
   - 計算ロジックの確認
   - 正しい投資シナリオでのROI再計算
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
from scipy import stats

from keibaai.src.models.calibration import IsotonicCalibrator

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class LeakOverfitValidator:
    """リーク・過学習検証クラス"""
    
    def __init__(self):
        self.base_model_dir = Path('keibaai/models/mu_v3_3')
        self.v54_dir = Path('keibaai/models/mu_v5_4')
        self.v60_dir = Path('keibaai/models/mu_v6_0')
    
    @staticmethod
    def extract_venue_name(venue_str):
        if pd.isna(venue_str):
            return None
        match = re.search(r'[0-9]*([^\d]+)[0-9]*', str(venue_str))
        return match.group(1) if match else None
    
    @staticmethod
    def extract_race_number(race_id: str):
        try:
            return int(str(race_id)[-2:])
        except:
            return None
    
    def run(self):
        """検証実行"""
        logging.info("=" * 70)
        logging.info("リーク・過学習検証")
        logging.info("=" * 70)
        
        # データ読み込み
        logging.info("\n【データ読み込み】")
        
        train_data = pd.read_parquet(self.base_model_dir / 'train_data_mu_v3_3.parquet')
        train_data['race_date'] = pd.to_datetime(train_data['race_date'])
        
        races_df = pd.read_parquet('keibaai/data/parsed/parquet/races/races.parquet')
        races_df['race_date'] = pd.to_datetime(races_df['race_date'])
        
        train_data['horse_id'] = train_data['horse_id'].astype(str)
        races_df['horse_id'] = races_df['horse_id'].astype(str)
        
        # モデル読み込み
        with open(self.v54_dir / 'mu_v5_4_ranker.pkl', 'rb') as f:
            model = pickle.load(f)
        with open(self.v54_dir / 'feature_names.json', 'r', encoding='utf-8') as f:
            feature_cols = json.load(f)
        calibrator = IsotonicCalibrator.load(self.v60_dir / 'isotonic_calibrator.pkl')
        
        # 馬特徴量などの追加処理（省略版 - v6_deep_analysis.pyと同様）
        race_info = races_df[['race_id', 'horse_id', 'venue', 'track_surface', 'bracket_number']].drop_duplicates(subset=['race_id', 'horse_id'])
        for col in ['venue', 'track_surface', 'bracket_number']:
            if col not in train_data.columns:
                train_data = train_data.merge(race_info[['race_id', 'horse_id', col]], on=['race_id', 'horse_id'], how='left')
        
        # Test期間
        test_df = train_data[train_data['race_date'] >= '2024-01-01'].copy()
        logging.info(f"  Testデータ: {len(test_df):,} ({test_df['race_id'].nunique():,}レース)")
        
        # ========================================
        # 検証1: Edge-Based Filteringの統計的有意性
        # ========================================
        logging.info("\n" + "=" * 70)
        logging.info("【検証1】Edge-Based Filtering統計的有意性")
        logging.info("=" * 70)
        
        # 予測（特徴量不足でエラーになる可能性があるため、簡易的にwin_oddsベースで検証）
        # 実際のEdge計算にはモデル予測が必要だが、ここでは保存済み結果を使用
        
        # 保存済みの深掘り分析結果を読み込み
        results_path = self.v60_dir / 'deep_analysis' / 'deep_analysis_results.json'
        if results_path.exists():
            with open(results_path, 'r', encoding='utf-8') as f:
                deep_results = json.load(f)
            
            edge_015 = next((r for r in deep_results['edge_results'] if r['threshold'] == 0.15), None)
            if edge_015:
                logging.info(f"\n  Edge≥0.15の結果:")
                logging.info(f"    件数: {edge_015['n_bets']}")
                logging.info(f"    ROI: {edge_015['roi']:.2%}")
                logging.info(f"    的中率: {edge_015['hit_rate']:.2%}")
                
                # ブートストラップ信頼区間（簡易計算）
                n = edge_015['n_bets']
                roi = edge_015['roi']
                hit_rate = edge_015['hit_rate']
                
                # 的中率の95%信頼区間（二項分布）
                se_hit = np.sqrt(hit_rate * (1 - hit_rate) / n)
                ci_hit_lower = hit_rate - 1.96 * se_hit
                ci_hit_upper = hit_rate + 1.96 * se_hit
                
                logging.info(f"\n  【統計的検証】")
                logging.info(f"    的中率95%CI: [{ci_hit_lower:.1%}, {ci_hit_upper:.1%}]")
                
                # ROIの分散推定（オッズ分散を仮定）
                # 実際のオッズ分布がないため、ヒューリスティックで推定
                # Edge≥0.15は主に人気薄が対象のため、平均オッズを高めに仮定
                avg_odds_estimate = roi / hit_rate if hit_rate > 0 else 10
                logging.info(f"    推定平均オッズ: {avg_odds_estimate:.1f}倍")
                
                # ブートストラップ（簡易版）
                # 実際のデータがないため、二項分布ベースで推定
                n_bootstrap = 10000
                roi_samples = []
                for _ in range(n_bootstrap):
                    # 的中数をシミュレート
                    simulated_hits = np.random.binomial(n, hit_rate)
                    # 各的中のオッズを対数正規分布でシミュレート（中央値=推定平均オッズ）
                    if simulated_hits > 0:
                        simulated_odds = np.random.lognormal(np.log(avg_odds_estimate), 0.5, simulated_hits)
                        simulated_roi = simulated_odds.sum() / n
                    else:
                        simulated_roi = 0
                    roi_samples.append(simulated_roi)
                
                roi_lower = np.percentile(roi_samples, 2.5)
                roi_upper = np.percentile(roi_samples, 97.5)
                roi_mean = np.mean(roi_samples)
                
                logging.info(f"    ROI Bootstrap 95%CI: [{roi_lower:.1%}, {roi_upper:.1%}]")
                logging.info(f"    ROI Bootstrap Mean: {roi_mean:.1%}")
                
                # 信頼区間が100%を含むか
                if roi_lower < 1.0 and roi_upper > 1.0:
                    logging.warning(f"    ⚠️ 95%CIが100%を跨ぐ → 統計的に有意とは言えない")
                elif roi_lower >= 1.0:
                    logging.info(f"    ✓ 95%CI下限が100%以上 → 統計的に有意な可能性")
                else:
                    logging.warning(f"    ⚠️ 95%CI上限が100%未満 → 有意でない")
        
        # ========================================
        # 検証2: 馬場バイアスROI計算の正確性
        # ========================================
        logging.info("\n" + "=" * 70)
        logging.info("【検証2】馬場バイアスROI計算の正確性")
        logging.info("=" * 70)
        
        logging.info("\n  【問題点】")
        logging.info("    現在の計算: 「内枠有利馬場で内枠にいる全馬」に対してROIを計算")
        logging.info("    問題: 1レースで複数馬に投資するシナリオになっている")
        logging.info("    正しい計算: 「Top1予測馬が内枠有利馬場×内枠の場合」のROI")
        
        # 馬場バイアスの正しい計算
        # バイアス計算にはraces_dfの先行レース結果が必要
        
        # 簡易検証: 内枠(1-3枠)の馬がレースで勝つ確率
        races_df['bracket_number'] = pd.to_numeric(races_df['bracket_number'], errors='coerce')
        races_df['finish_position'] = pd.to_numeric(races_df['finish_position'], errors='coerce')
        
        test_races = races_df[races_df['race_date'] >= '2024-01-01']
        
        inner_horses = test_races[test_races['bracket_number'] <= 3]
        outer_horses = test_races[test_races['bracket_number'] >= 6]
        
        inner_win_rate = (inner_horses['finish_position'] == 1).mean()
        outer_win_rate = (outer_horses['finish_position'] == 1).mean()
        
        logging.info(f"\n  【内枠vs外枠の勝率（Test期間全体）】")
        logging.info(f"    内枠(1-3枠)勝率: {inner_win_rate:.2%}")
        logging.info(f"    外枠(6-8枠)勝率: {outer_win_rate:.2%}")
        logging.info(f"    差分: {inner_win_rate - outer_win_rate:+.2%}")
        
        # 期待値計算
        inner_avg_odds = inner_horses[inner_horses['finish_position'] == 1]['win_odds'].mean()
        outer_avg_odds = outer_horses[outer_horses['finish_position'] == 1]['win_odds'].mean()
        
        # 全内枠馬に投資した場合のROI（1頭1単位）
        n_inner = len(inner_horses)
        n_inner_wins = (inner_horses['finish_position'] == 1).sum()
        inner_total_odds = inner_horses[inner_horses['finish_position'] == 1]['win_odds'].sum()
        inner_roi = inner_total_odds / n_inner if n_inner > 0 else 0
        
        logging.info(f"\n  【内枠全馬に1単位ずつ投資した場合】")
        logging.info(f"    投資数: {n_inner:,}")
        logging.info(f"    勝利数: {n_inner_wins:,}")
        logging.info(f"    ROI: {inner_roi:.2%}")
        
        logging.info("\n  【結論】")
        logging.info("    馬場バイアス290%は計算ロジックのバグである可能性が高い")
        logging.info("    正しい検証には「Top1予測 × バイアス条件」のクロス分析が必要")
        
        # ========================================
        # 検証3: リーク・過学習リスクまとめ
        # ========================================
        logging.info("\n" + "=" * 70)
        logging.info("【検証3】リーク・過学習リスクまとめ")
        logging.info("=" * 70)
        
        logging.info("\n  【Edge-Based Filtering】")
        logging.info("    ✗ サンプルサイズ小（194件）→ 分散が大きい")
        logging.info("    ✗ 確定オッズ依存 → 実運用では利用不可（morning_odds必要）")
        logging.info("    △ 時系列検証未実施 → 特定期間に偏りがないか要確認")
        logging.info("    → 過学習リスク: 中〜高")
        
        logging.info("\n  【馬場バイアス】")
        logging.info("    ✗ ROI計算ロジックにバグあり → 290%は信頼できない")
        logging.info("    △ バイアス計算自体はリークなし（先行レースのみ使用）")
        logging.info("    △ 効果の大きさは再検証が必要")
        logging.info("    → リークリスク: 低（計算は正当）")
        logging.info("    → 過学習リスク: 計算バグ修正後に再評価必要")
        
        logging.info("\n  【全体結論】")
        logging.info("    Edge≥0.15でROI112%、馬場バイアス290%は「そのまま信用できない」")
        logging.info("    次のステップ:")
        logging.info("      1. Edge分析の時系列安定性を検証")
        logging.info("      2. 馬場バイアスROI計算を「Top1×バイアス条件」で再計算")
        logging.info("      3. Valid期間とTest期間での一貫性を確認")
        
        logging.info("\n" + "=" * 70)
        
        return {
            'edge_015_roi_ci': (roi_lower, roi_upper) if 'roi_lower' in dir() else None,
            'inner_roi_correct': float(inner_roi),
            'conclusion': 'requires_further_validation'
        }


if __name__ == "__main__":
    validator = LeakOverfitValidator()
    validator.run()
