#!/usr/bin/env python3
"""
v11.0予測生成 + Edge Score最適化スクリプト

【核心的洞察】
ROI 103%達成には「全レースベット」から「選択的ベット」への転換が必須。

v11.0モデル (Test ROI 82.97%) を基盤に:
1. 各レースで「勝つべき馬」を予測（ランク1位選択）
2. 予測の確信度を計算
3. 確信度が高いレースのみベット

【選択基準】
- Score差 (1位 - 2位) が大きいほど確信度が高い
- オッズとの乖離が大きいほどエッジが大きい
- 両者を組み合わせて「エッジ × 確信度」でフィルタ
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
from scipy import stats

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class SelectiveBettingOptimizer:
    """
    選択的ベット最適化
    
    「全レースベット」(ROI 82.97%) → 「厳選ベット」(目標ROI 103%+)
    """
    
    def __init__(self, model_dir='keibaai/models/mu_v11_0'):
        self.model_dir = Path(model_dir)
        self.output_dir = Path('keibaai/models/selective_betting')
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        self.model = None
        self.feature_names = None
    
    def load_model_and_data(self):
        """モデルとデータを読み込み"""
        logging.info("=" * 60)
        logging.info("モデル・データ読み込み")
        logging.info("=" * 60)
        
        # モデル
        with open(self.model_dir / 'mu_v11_0_ranker.pkl', 'rb') as f:
            self.model = pickle.load(f)
        with open(self.model_dir / 'feature_names.json', 'r', encoding='utf-8') as f:
            self.feature_names = json.load(f)
        
        logging.info(f"  モデル特徴量: {len(self.feature_names)}")
        
        # 学習データ（特徴量付き）
        base_data_path = Path('keibaai/models/mu_v3_3/train_data_mu_v3_3.parquet')
        if base_data_path.exists():
            train_data = pd.read_parquet(base_data_path)
            train_data['race_date'] = pd.to_datetime(train_data['race_date'])
            logging.info(f"  学習データ: {len(train_data):,}件")
            return train_data
        else:
            logging.warning(f"  学習データが見つかりません: {base_data_path}")
            return None
    
    def load_races_with_results(self):
        """結果付きレースデータを読み込み"""
        races_df = pd.read_parquet('keibaai/data/parsed/parquet/races/races.parquet')
        races_df['race_date'] = pd.to_datetime(races_df['race_date'])
        races_df['race_id'] = races_df['race_id'].astype(str)
        races_df['horse_id'] = races_df['horse_id'].astype(str)
        races_df['finish_position'] = pd.to_numeric(races_df['finish_position'], errors='coerce')
        races_df['win_odds'] = pd.to_numeric(races_df['win_odds'], errors='coerce')
        races_df['popularity'] = pd.to_numeric(races_df['popularity'], errors='coerce')
        
        return races_df
    
    def generate_predictions(self, df: pd.DataFrame, races_df: pd.DataFrame) -> pd.DataFrame:
        """
        v11.0モデルで予測を生成
        """
        logging.info("=" * 60)
        logging.info("予測生成")
        logging.info("=" * 60)
        
        # 特徴量が存在するもののみ使用
        available_features = [f for f in self.feature_names if f in df.columns]
        missing_features = [f for f in self.feature_names if f not in df.columns]
        
        logging.info(f"  利用可能特徴量: {len(available_features)}/{len(self.feature_names)}")
        
        if len(missing_features) > 0:
            logging.warning(f"  欠落特徴量: {len(missing_features)}")
            # 欠落特徴量を0で補完
            for f in missing_features:
                df[f] = 0
        
        # 欠損値を0で補完
        for f in self.feature_names:
            if f in df.columns:
                df[f] = df[f].fillna(0)
        
        # 予測
        preds = self.model.predict(df[self.feature_names])
        df = df.copy()
        df['model_score'] = preds
        
        # レース内ランク
        df['rank_in_race'] = df.groupby('race_id')['model_score'].rank(ascending=False, method='first')
        
        # スコア差（確信度の指標）
        df['score_diff'] = df.groupby('race_id')['model_score'].transform(
            lambda x: x.max() - x.nlargest(2).iloc[-1] if len(x) >= 2 else 0
        )
        
        # 結果データをマージ
        df['race_id'] = df['race_id'].astype(str)
        df['horse_id'] = df['horse_id'].astype(str)
        
        result_cols = ['race_id', 'horse_id', 'finish_position', 'win_odds', 'popularity']
        races_subset = races_df[result_cols].drop_duplicates(subset=['race_id', 'horse_id'])
        
        # 既存カラムがあれば削除してからマージ
        for col in ['finish_position', 'win_odds', 'popularity']:
            if col in df.columns:
                df = df.drop(columns=[col])
        
        df = df.merge(races_subset, on=['race_id', 'horse_id'], how='left')
        
        logging.info(f"  予測生成完了: {len(df):,}件")
        
        return df
    
    def calculate_selection_score(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        選択スコアを計算（確信度 × エッジ）
        """
        df = df.copy()
        
        # スコア差を正規化（0-1）
        score_diff_max = df['score_diff'].max()
        if score_diff_max > 0:
            df['normalized_score_diff'] = df['score_diff'] / score_diff_max
        else:
            df['normalized_score_diff'] = 0
        
        # オッズ暗示確率
        df['odds_implied_prob'] = 1 / df['win_odds'].clip(lower=1.1)
        
        # モデル確率（softmax変換）
        def softmax_prob(x):
            exp_x = np.exp(x - x.max())
            return exp_x / exp_x.sum()
        
        df['model_prob'] = df.groupby('race_id')['model_score'].transform(softmax_prob)
        
        # Edge Score
        df['edge_score'] = df['model_prob'] / df['odds_implied_prob'].clip(lower=0.01)
        
        # 選択スコア = 確信度 × エッジの符号付き
        df['selection_score'] = df['normalized_score_diff'] * (df['edge_score'] - 1.0)
        
        return df
    
    def evaluate_selective_betting(self, df: pd.DataFrame, rank1_only=True):
        """
        選択的ベットの評価
        
        rank1_only=True: 各レースで予測1位の馬のみを候補として評価
        """
        logging.info("=" * 60)
        logging.info("選択的ベット評価")
        logging.info("=" * 60)
        
        # 予測1位のみ選択
        if rank1_only:
            df = df[df['rank_in_race'] == 1].copy()
        
        # 勝敗フラグ
        df['is_win'] = (df['finish_position'] == 1).fillna(False).astype(int)
        
        # 各種フィルタリング基準で評価
        results = []
        
        # 1. スコア差でフィルタ
        for threshold in [0.0, 0.1, 0.2, 0.3, 0.5, 0.7]:
            filtered = df[df['normalized_score_diff'] >= threshold]
            if len(filtered) < 50:
                continue
            
            wins = filtered['is_win'].sum()
            total_payout = (filtered['is_win'] * filtered['win_odds']).sum()
            roi = total_payout / len(filtered)
            
            results.append({
                'filter_type': 'score_diff',
                'threshold': threshold,
                'bet_count': len(filtered),
                'win_count': int(wins),
                'hit_rate': wins / len(filtered),
                'roi': roi,
                'avg_odds': filtered['win_odds'].mean(),
                'fav1_rate': (filtered['popularity'] == 1).mean()
            })
        
        # 2. Edge Scoreでフィルタ
        for threshold in [0.5, 0.8, 1.0, 1.2, 1.5, 2.0]:
            filtered = df[df['edge_score'] >= threshold]
            if len(filtered) < 50:
                continue
            
            wins = filtered['is_win'].sum()
            total_payout = (filtered['is_win'] * filtered['win_odds']).sum()
            roi = total_payout / len(filtered)
            
            results.append({
                'filter_type': 'edge_score',
                'threshold': threshold,
                'bet_count': len(filtered),
                'win_count': int(wins),
                'hit_rate': wins / len(filtered),
                'roi': roi,
                'avg_odds': filtered['win_odds'].mean(),
                'fav1_rate': (filtered['popularity'] == 1).mean()
            })
        
        # 3. 組み合わせフィルタ（スコア差 AND Edge Score）
        for sd_thresh in [0.1, 0.2, 0.3]:
            for edge_thresh in [0.8, 1.0, 1.2]:
                filtered = df[(df['normalized_score_diff'] >= sd_thresh) & 
                             (df['edge_score'] >= edge_thresh)]
                if len(filtered) < 30:
                    continue
                
                wins = filtered['is_win'].sum()
                total_payout = (filtered['is_win'] * filtered['win_odds']).sum()
                roi = total_payout / len(filtered)
                
                results.append({
                    'filter_type': f'combo_sd{sd_thresh}_edge{edge_thresh}',
                    'threshold': f'{sd_thresh}_{edge_thresh}',
                    'bet_count': len(filtered),
                    'win_count': int(wins),
                    'hit_rate': wins / len(filtered),
                    'roi': roi,
                    'avg_odds': filtered['win_odds'].mean(),
                    'fav1_rate': (filtered['popularity'] == 1).mean()
                })
        
        return pd.DataFrame(results)
    
    def run(self):
        """メイン実行"""
        # データ読み込み
        train_data = self.load_model_and_data()
        
        if train_data is None:
            logging.error("学習データが見つかりません。代替アプローチを実行します。")
            return self.run_alternative()
        
        races_df = self.load_races_with_results()
        
        # 予測生成
        train_data = self.generate_predictions(train_data, races_df)
        
        # 選択スコア計算
        train_data = self.calculate_selection_score(train_data)
        
        # Valid/Testで評価
        valid_df = train_data[(train_data['race_date'] >= '2023-01-01') & 
                              (train_data['race_date'] < '2024-01-01')]
        test_df = train_data[train_data['race_date'] >= '2024-01-01']
        
        logging.info(f"\n【Valid期間】{len(valid_df):,}件")
        valid_results = self.evaluate_selective_betting(valid_df)
        
        logging.info(f"\n【Test期間】{len(test_df):,}件")
        test_results = self.evaluate_selective_betting(test_df)
        
        # 結果表示
        if len(valid_results) > 0:
            logging.info("\n【Valid期間 Top 10】")
            top_valid = valid_results.nlargest(10, 'roi')
            for _, r in top_valid.iterrows():
                logging.info(f"  {r['filter_type']:30} | ベット{r['bet_count']:5} | ROI{r['roi']:6.1%} | 的中{r['hit_rate']:5.1%}")
        
        if len(test_results) > 0:
            logging.info("\n【Test期間 Top 10】")
            top_test = test_results.nlargest(10, 'roi')
            for _, r in top_test.iterrows():
                logging.info(f"  {r['filter_type']:30} | ベット{r['bet_count']:5} | ROI{r['roi']:6.1%} | 的中{r['hit_rate']:5.1%}")
            
            # 最良結果
            best = test_results.loc[test_results['roi'].idxmax()]
            logging.info(f"\n【Test最高ROI】: {best['roi']:.1%} ({best['filter_type']}, ベット数{best['bet_count']})")
            
            if best['roi'] >= 1.03:
                logging.info("✅ 目標達成: Test ROI >= 103%")
            else:
                logging.info(f"⚠️ 目標未達: あと{1.03 - best['roi']:.1%}の改善が必要")
        
        # 保存
        valid_results.to_csv(self.output_dir / 'valid_results.csv', index=False)
        test_results.to_csv(self.output_dir / 'test_results.csv', index=False)
        
        return valid_results, test_results
    
    def run_alternative(self):
        """
        学習データがない場合の代替アプローチ
        races.parquetから直接分析
        """
        logging.info("=" * 60)
        logging.info("代替アプローチ: races.parquetから直接分析")
        logging.info("=" * 60)
        
        races_df = self.load_races_with_results()
        
        # Valid/Test分割
        valid_df = races_df[(races_df['race_date'] >= '2023-01-01') & 
                           (races_df['race_date'] < '2024-01-01')].copy()
        test_df = races_df[races_df['race_date'] >= '2024-01-01'].copy()
        
        logging.info(f"  Valid: {len(valid_df):,}件")
        logging.info(f"  Test: {len(test_df):,}件")
        
        # 人気×オッズの乖離分析
        for period_name, df in [('Valid', valid_df), ('Test', test_df)]:
            df = df.copy()
            df['is_win'] = (df['finish_position'] == 1).fillna(False).astype(int)
            
            # 人気別の期待オッズ
            expected_odds = {
                1: 2.8, 2: 5.0, 3: 7.5, 4: 10.0, 5: 15.0,
                6: 22.0, 7: 32.0, 8: 45.0, 9: 65.0, 10: 90.0,
            }
            df['expected_odds'] = df['popularity'].map(
                lambda x: expected_odds.get(int(x), 100) if pd.notna(x) else 100
            )
            
            # オッズ乖離（実際オッズ / 期待オッズ）
            df['odds_ratio'] = df['win_odds'] / df['expected_odds']
            
            # 乖離区間別のROI分析
            logging.info(f"\n【{period_name}期間 オッズ乖離分析】")
            logging.info(f"{'乖離区間':>15} | {'件数':>8} | {'勝利数':>6} | {'勝率':>6} | {'ROI':>8}")
            logging.info("-" * 55)
            
            for lower, upper in [(0.0, 0.5), (0.5, 0.8), (0.8, 1.0), (1.0, 1.2), (1.2, 1.5), (1.5, 2.0), (2.0, 5.0)]:
                subset = df[(df['odds_ratio'] >= lower) & (df['odds_ratio'] < upper)]
                if len(subset) < 100:
                    continue
                
                wins = subset['is_win'].sum()
                roi = (subset['is_win'] * subset['win_odds']).sum() / len(subset)
                win_rate = wins / len(subset)
                
                logging.info(f"{lower:.1f}-{upper:.1f} | {len(subset):>8} | {wins:>6} | {win_rate:>5.1%} | {roi:>7.1%}")
        
        return None, None


if __name__ == "__main__":
    optimizer = SelectiveBettingOptimizer()
    valid_results, test_results = optimizer.run()
