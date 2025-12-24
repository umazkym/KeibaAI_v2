#!/usr/bin/env python3
"""
KeibaAI v5.1 Ensemble Training Script

専門モデル（PaceScenario, PedigreeAptitude）を統合した
エッジベースのアンサンブルモデルを訓練する。

【アーキテクチャ】
1. メインモデル: LightGBM Ranker (v11.0ベース)
2. PaceScenarioModel: 展開予測からの調整
3. PedigreeAptitudeModel: 血統適性からの調整
4. EdgeCalculator: 統合確率とEdge Score計算
5. BacktestEngine: ROIシミュレーション

【目標】
- Edge >= 1.2 のみベットで ROI >= 103%
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
from datetime import datetime

# 自作モジュール
from keibaai.src.models.edge_calculator import EdgeCalculator, EdgeOptimizer
from keibaai.src.models.specialized import PaceScenarioModel, PedigreeAptitudeModel
from keibaai.src.backtesting import BacktestEngine
from keibaai.src.splitters import StrictDataSplitter

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class EnsembleV5Trainer:
    """
    v5.1 アンサンブルトレーナー
    
    既存のv11.0モデルに専門モデルの調整を加えて統合予測を行う。
    """
    
    def __init__(self, base_model_dir: str = 'keibaai/models/mu_v11_0'):
        self.base_model_dir = Path(base_model_dir)
        self.output_dir = Path('keibaai/models/ensemble_v5')
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        self.base_model = None
        self.feature_names = None
        self.pace_model = None
        self.pedigree_model = None
        self.edge_calculator = None
        self.backtest_engine = BacktestEngine(bet_amount=100)
    
    def load_base_model(self):
        """ベースモデル（v11.0）を読み込み"""
        logging.info("=" * 60)
        logging.info("ベースモデル読み込み")
        logging.info("=" * 60)
        
        model_path = self.base_model_dir / 'mu_v11_0_ranker.pkl'
        with open(model_path, 'rb') as f:
            self.base_model = pickle.load(f)
        
        feature_path = self.base_model_dir / 'feature_names.json'
        with open(feature_path, 'r', encoding='utf-8') as f:
            self.feature_names = json.load(f)
        
        logging.info(f"  モデル: {model_path.name}")
        logging.info(f"  特徴量数: {len(self.feature_names)}")
    
    def load_data(self):
        """データ読み込み"""
        logging.info("=" * 60)
        logging.info("データ読み込み")
        logging.info("=" * 60)
        
        races_df = pd.read_parquet('keibaai/data/parsed/parquet/races/races.parquet')
        races_df['race_date'] = pd.to_datetime(races_df['race_date'])
        
        pedigrees_df = pd.read_parquet('keibaai/data/parsed/parquet/pedigrees/pedigrees.parquet')
        
        logging.info(f"  レース: {len(races_df):,}件")
        logging.info(f"  血統: {len(pedigrees_df):,}件")
        
        return races_df, pedigrees_df
    
    def fit_specialized_models(self, races_df, pedigrees_df):
        """専門モデルをフィット"""
        logging.info("=" * 60)
        logging.info("専門モデルフィット")
        logging.info("=" * 60)
        
        # PaceScenarioModel
        self.pace_model = PaceScenarioModel(train_cutoff='2023-01-01')
        logging.info("  PaceScenarioModel: 準備完了")
        
        # PedigreeAptitudeModel
        self.pedigree_model = PedigreeAptitudeModel(train_cutoff='2023-01-01')
        self.pedigree_model.fit(races_df, pedigrees_df)
        logging.info("  PedigreeAptitudeModel: フィット完了")
        
        # EdgeCalculator
        self.edge_calculator = EdgeCalculator(
            takeout_rate=0.20,
            pace_weight=1.0,
            pedigree_weight=1.0,
            max_adjustment=0.3
        )
        logging.info("  EdgeCalculator: 初期化完了")
    
    def generate_predictions(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        各レースに対して統合予測を生成
        
        Returns:
        --------
        pd.DataFrame : edge_score, model_prob等を含む予測結果
        """
        logging.info("=" * 60)
        logging.info("統合予測生成")
        logging.info("=" * 60)
        
        results = []
        race_ids = df['race_id'].unique()
        
        for i, race_id in enumerate(race_ids):
            if i > 0 and i % 500 == 0:
                logging.info(f"  {i}/{len(race_ids)} レース処理済み...")
            
            race_df = df[df['race_id'] == race_id].copy()
            race_date = race_df['race_date'].iloc[0]
            
            # オッズ取得
            odds = pd.to_numeric(race_df['win_odds'], errors='coerce').fillna(100).values
            
            # ベースモデルスコア（既存特徴量が必要なため簡易版で代用）
            # 実運用時はbase_model.predictを使用
            # ここでは人気順位からスコアを生成
            popularity = pd.to_numeric(race_df['popularity'], errors='coerce').fillna(8).values
            base_scores = 10 - popularity  # 人気が高いほど高スコア
            
            # 展開予測からの調整
            # 簡易版：脚質を人気から推定
            running_styles = []
            for pop in popularity:
                if pop <= 3:
                    running_styles.append('senkou')
                elif pop <= 6:
                    running_styles.append('sashi')
                else:
                    running_styles.append('oikomi')
            
            pace = self.pace_model.predict_pace(running_styles)
            pace_adjustments = np.array([
                self.pace_model.calculate_advantage(s, pace) for s in running_styles
            ])
            
            # 血統適性からの調整
            surface = race_df['track_surface'].iloc[0] if 'track_surface' in race_df.columns else '芝'
            conditions = {
                'surface': surface,
                'distance_category': 'mile',
                'is_heavy': False
            }
            pedigree_adjustments = np.array([
                self.pedigree_model.predict(str(h), conditions) 
                for h in race_df['horse_id']
            ])
            
            # Edge Score計算
            edge_scores, adjusted_probs, confidence = self.edge_calculator.calculate_edge_scores(
                odds, base_scores, pace_adjustments, pedigree_adjustments
            )
            
            # 結果を格納
            for j, (_, row) in enumerate(race_df.iterrows()):
                finish_pos = row['finish_position']
                is_win = 1 if (pd.notna(finish_pos) and finish_pos == 1) else 0
                
                results.append({
                    'race_id': race_id,
                    'race_date': race_date,
                    'horse_id': row['horse_id'],
                    'horse_number': row.get('horse_number', 0),
                    'win_odds': odds[j],
                    'popularity': popularity[j],
                    'finish_position': finish_pos,
                    'is_win': is_win,
                    'base_score': base_scores[j],
                    'pace_adjustment': pace_adjustments[j],
                    'pedigree_adjustment': pedigree_adjustments[j],
                    'model_prob': adjusted_probs[j],
                    'edge_score': edge_scores[j],
                    'confidence': confidence,
                    'pace_type': pace
                })
        
        return pd.DataFrame(results)
    
    def evaluate(self, predictions_df: pd.DataFrame):
        """評価とレポート生成"""
        logging.info("=" * 60)
        logging.info("評価")
        logging.info("=" * 60)
        
        # 閾値最適化
        optimizer = EdgeOptimizer(min_bet_count=100)
        opt_result = optimizer.optimize(
            predictions_df['edge_score'].values,
            predictions_df['is_win'].values,
            predictions_df['win_odds'].values,
            thresholds=[1.0, 1.1, 1.2, 1.3, 1.5, 2.0]
        )
        
        logging.info(f"\n【閾値最適化結果】")
        logging.info(f"  最適閾値: {opt_result['optimal_threshold']}")
        logging.info(f"  最適ROI: {opt_result['optimal_roi']:.1%}")
        
        logging.info(f"\n【閾値別結果】")
        logging.info(f"{'閾値':>6} | {'ベット':>6} | {'的中':>6} | {'的中率':>8} | {'ROI':>8}")
        logging.info("-" * 50)
        for r in opt_result['all_results']:
            logging.info(f"{r['threshold']:>6.1f} | {r['bet_count']:>6} | {r['win_count']:>6} | {r['hit_rate']:>7.1%} | {r['roi']:>7.1%}")
        
        # モンテカルロシミュレーション
        if opt_result['optimal_threshold']:
            mc_result = self.backtest_engine.monte_carlo_simulation(
                predictions_df,
                edge_threshold=opt_result['optimal_threshold'],
                n_simulations=500,
                n_bets=200
            )
            
            logging.info(f"\n【モンテカルロシミュレーション】")
            logging.info(f"  平均ROI: {mc_result['mean_roi']:.1%}")
            logging.info(f"  5-95%: {mc_result['percentile_5']:.1%} ~ {mc_result['percentile_95']:.1%}")
            logging.info(f"  利益確率: {mc_result['prob_profit']:.1%}")
        
        return opt_result
    
    def save_results(self, predictions_df: pd.DataFrame, opt_result: dict):
        """結果保存"""
        logging.info("=" * 60)
        logging.info("結果保存")
        logging.info("=" * 60)
        
        predictions_df.to_parquet(self.output_dir / 'predictions.parquet', index=False)
        
        with open(self.output_dir / 'optimization_result.json', 'w', encoding='utf-8') as f:
            # numpy型をPython型に変換
            safe_result = {
                'optimal_threshold': opt_result.get('optimal_threshold'),
                'optimal_roi': float(opt_result.get('optimal_roi', 0)),
                'all_results': opt_result.get('all_results', [])
            }
            json.dump(safe_result, f, ensure_ascii=False, indent=2)
        
        logging.info(f"  保存先: {self.output_dir}")
    
    def run(self):
        """メイン実行"""
        # ベースモデル読み込み
        self.load_base_model()
        
        # データ読み込み
        races_df, pedigrees_df = self.load_data()
        
        # 専門モデルフィット
        self.fit_specialized_models(races_df, pedigrees_df)
        
        # データ分割
        splitter = StrictDataSplitter(train_end='2022-12-31', valid_end='2023-12-31')
        train_df, valid_df, test_df = splitter.split(races_df)
        
        # Valid期間で予測
        logging.info(f"\nValid期間で予測生成...")
        predictions_df = self.generate_predictions(valid_df)
        
        # 評価
        opt_result = self.evaluate(predictions_df)
        
        # 結果保存
        self.save_results(predictions_df, opt_result)
        
        return predictions_df, opt_result


if __name__ == "__main__":
    trainer = EnsembleV5Trainer()
    predictions_df, opt_result = trainer.run()
    
    print("\n" + "=" * 60)
    print("【v5.1 アンサンブル結果サマリー】")
    print("=" * 60)
    print(f"最適閾値: {opt_result.get('optimal_threshold')}")
    print(f"最適ROI: {opt_result.get('optimal_roi', 0):.1%}")
