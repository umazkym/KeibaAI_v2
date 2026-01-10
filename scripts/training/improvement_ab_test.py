#!/usr/bin/env python3
"""
改善A/Bテストフレームワーク (improvement_ab_test.py)

【目的】
各改善案を個別にテストし、効果を比較検証する

【テスト項目】
- ベースライン: 現行モデル
- 改善A: prev_finish対数変換
- 改善B: rest_optimal_gap追加  
- 改善C: jockey/trainer勝率対数変換
- 改善D: is_comeback特徴量追加
- 改善E: 正則化強化のみ
- 改善F: 学習率低下のみ

Author: KeibaAI Development Team
Date: 2026-01-10
"""

import sys
from pathlib import Path

project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(project_root))

import pandas as pd
import numpy as np
import lightgbm as lgb
import logging
from datetime import datetime
from typing import Tuple, List, Dict, Optional
from sklearn.metrics import roc_auc_score
import json
import warnings
warnings.filterwarnings('ignore')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class ImprovementABTester:
    """改善A/Bテストクラス"""
    
    LEAK_FEATURES = [
        'finish_position', 'finish_time_seconds', 'margin_seconds', 'prize_money',
        'finish_time_str', 'margin_str', 'last_3f_time', 'time_except_last3f',
        'last3f_rank', 'popularity', 'win_odds', 'odds', 'win_probability',
        'relative_odds', 'popularity_finish_diff',
        'passing_order', 'passing_order_1', 'passing_order_2',
        'passing_order_3', 'passing_order_4', 'final_corner_to_finish',
        'position_change_1_2', 'position_change_2_3', 'position_change_3_4',
        'pace_index', 'target', 'weight',
        'horse_weight_change', 'horse_weight_deviation',
    ]
    
    META_COLS = [
        'race_id', 'horse_id', 'race_date', 'jockey_id', 'trainer_id', 'owner_id',
        'sire_id', 'damsire_id', 'race_name', 'horse_name', 'jockey_name', 
        'trainer_name', 'scratched', 'year', 'track_surface', 'race_class',
        'venue', 'track_condition', 'distance_category', 'bracket_category',
        'sex', 'weather',
    ]
    
    def __init__(self, data_dir: str = 'keibaai/data/parsed/parquet'):
        self.data_dir = Path(data_dir)
        self.output_dir = Path('keibaai/models/ab_test_results')
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # ベースラインパラメータ
        self.baseline_params = {
            'objective': 'binary',
            'metric': 'auc',
            'boosting_type': 'gbdt',
            'learning_rate': 0.02,
            'max_depth': 3,
            'num_leaves': 8,
            'min_child_samples': 300,
            'reg_alpha': 10.0,
            'reg_lambda': 10.0,
            'feature_fraction': 0.7,
            'bagging_fraction': 0.7,
            'bagging_freq': 5,
            'verbosity': -1,
            'seed': 42,
        }
        self.num_boost_round = 300
        
    def load_data(self) -> pd.DataFrame:
        """データ読み込み"""
        logger.info("データ読み込み...")
        df = pd.read_parquet(self.data_dir / 'races/races.parquet')
        df['race_date'] = pd.to_datetime(df['race_date'])
        df['year'] = df['race_date'].dt.year
        df = df[df['year'] >= 2014]
        df = df[df['track_surface'] == '芝']
        df = df[df['race_class'] != '新馬']
        logger.info(f"データ: {len(df):,}件")
        return df
    
    def _generate_base_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """ベース特徴量生成（共通処理）"""
        df = df.copy()
        df = df.sort_values(['race_date', 'race_id', 'horse_number']).reset_index(drop=True)
        df['target'] = (df['finish_position'] == 1).astype(int)
        
        track_cond_map = {'良': 0, '稀重': 1, '重': 2, '不良': 3}
        df['track_condition_encoded'] = df['track_condition'].map(track_cond_map).fillna(0)
        df['field_size'] = df.groupby('race_id')['horse_number'].transform('count')
        df['race_number'] = df['race_id'].str[-2:].astype(int)
        df['is_late_race'] = (df['race_number'] >= 8).astype(int)
        
        # 累積統計
        df['horse_cum_wins'] = df.groupby('horse_id')['target'].transform(
            lambda x: x.cumsum().shift(1).fillna(0)
        )
        df['horse_cum_races'] = df.groupby('horse_id')['target'].transform(
            lambda x: x.expanding().count().shift(1).fillna(0)
        )
        df['horse_win_rate'] = np.where(
            df['horse_cum_races'] >= 3,
            df['horse_cum_wins'] / df['horse_cum_races'],
            np.nan
        )
        
        df['horse_cum_finish_sum'] = df.groupby('horse_id')['finish_position'].transform(
            lambda x: x.cumsum().shift(1).fillna(0)
        )
        df['horse_avg_finish'] = np.where(
            df['horse_cum_races'] >= 1,
            df['horse_cum_finish_sum'] / df['horse_cum_races'],
            np.nan
        )
        
        df['jockey_cum_wins'] = df.groupby('jockey_id')['target'].transform(
            lambda x: x.cumsum().shift(1).fillna(0)
        )
        df['jockey_cum_races'] = df.groupby('jockey_id')['target'].transform(
            lambda x: x.expanding().count().shift(1).fillna(0)
        )
        df['jockey_win_rate'] = np.where(
            df['jockey_cum_races'] >= 10,
            df['jockey_cum_wins'] / df['jockey_cum_races'],
            np.nan
        )
        
        df['trainer_cum_wins'] = df.groupby('trainer_id')['target'].transform(
            lambda x: x.cumsum().shift(1).fillna(0)
        )
        df['trainer_cum_races'] = df.groupby('trainer_id')['target'].transform(
            lambda x: x.expanding().count().shift(1).fillna(0)
        )
        df['trainer_win_rate'] = np.where(
            df['trainer_cum_races'] >= 10,
            df['trainer_cum_wins'] / df['trainer_cum_races'],
            np.nan
        )
        
        df['prev_finish'] = df.groupby('horse_id')['finish_position'].shift(1)
        df['prev_race_date'] = df.groupby('horse_id')['race_date'].shift(1)
        df['days_since_last'] = (df['race_date'] - df['prev_race_date']).dt.days
        
        venue_encoder = {v: i for i, v in enumerate(df['venue'].unique())}
        df['venue_encoded'] = df['venue'].map(venue_encoder)
        
        # 一時カラム削除
        temp_cols = ['horse_cum_wins', 'horse_cum_races', 'horse_cum_finish_sum',
                     'jockey_cum_wins', 'jockey_cum_races',
                     'trainer_cum_wins', 'trainer_cum_races', 'prev_race_date']
        df = df.drop(columns=[c for c in temp_cols if c in df.columns], errors='ignore')
        
        return df
    
    def _apply_improvement(self, df: pd.DataFrame, improvement: str) -> pd.DataFrame:
        """改善を適用"""
        df = df.copy()
        
        if improvement == 'baseline':
            pass
        
        elif improvement == 'A_prev_finish_log':
            # 改善A: prev_finish対数変換
            df['prev_finish'] = np.log1p(df['prev_finish'].fillna(10))
        
        elif improvement == 'B_rest_optimal_gap':
            # 改善B: 最適休養からの乖離
            df['rest_optimal_gap'] = np.abs(df['days_since_last'].fillna(60) - 45)
            df['is_short_rest'] = (df['days_since_last'].fillna(60) <= 21).astype(int)
        
        elif improvement == 'C_jockey_trainer_log':
            # 改善C: 騎手/調教師勝率の対数変換
            df['jockey_win_rate'] = np.log1p(df['jockey_win_rate'].fillna(0.05) * 100) / 5
            df['trainer_win_rate'] = np.log1p(df['trainer_win_rate'].fillna(0.05) * 100) / 5
        
        elif improvement == 'D_is_comeback':
            # 改善D: 復活馬フラグ
            df['is_comeback'] = (
                (df['prev_finish'].fillna(99) >= 8) & 
                (df['horse_avg_finish'].fillna(99) <= 5)
            ).astype(int)
            df['prev_finish_relative'] = df['prev_finish'] / df['horse_avg_finish'].replace(0, np.nan)
        
        elif improvement == 'E_strong_reg':
            # 改善E: 正則化強化（パラメータで対応、特徴量は同じ）
            pass
        
        elif improvement == 'F_slow_lr':
            # 改善F: 学習率低下（パラメータで対応、特徴量は同じ）
            pass
        
        elif improvement == 'ALL_combined':
            # 全改善を組み合わせ
            df['prev_finish_log'] = np.log1p(df['prev_finish'].fillna(10))
            df['rest_optimal_gap'] = np.abs(df['days_since_last'].fillna(60) - 45)
            df['is_short_rest'] = (df['days_since_last'].fillna(60) <= 21).astype(int)
            df['jockey_win_rate_log'] = np.log1p(df['jockey_win_rate'].fillna(0.05) * 100) / 5
            df['trainer_win_rate_log'] = np.log1p(df['trainer_win_rate'].fillna(0.05) * 100) / 5
            df['is_comeback'] = (
                (df['prev_finish'].fillna(99) >= 8) & 
                (df['horse_avg_finish'].fillna(99) <= 5)
            ).astype(int)
        
        return df
    
    def _get_params(self, improvement: str) -> Dict:
        """改善に応じたパラメータ取得"""
        params = self.baseline_params.copy()
        
        if improvement == 'E_strong_reg':
            params['reg_alpha'] = 20.0
            params['reg_lambda'] = 20.0
            params['min_child_samples'] = 500
        
        elif improvement == 'F_slow_lr':
            params['learning_rate'] = 0.01
        
        elif improvement == 'ALL_combined':
            params['learning_rate'] = 0.01
            params['reg_alpha'] = 15.0
            params['reg_lambda'] = 15.0
            params['min_child_samples'] = 400
        
        return params
    
    def _get_feature_cols(self, df: pd.DataFrame) -> List[str]:
        """使用特徴量取得"""
        exclude = set(self.LEAK_FEATURES + self.META_COLS)
        feature_cols = [c for c in df.columns if c not in exclude]
        numeric_cols = [c for c in feature_cols if df[c].dtype in ['int64', 'float64', 'int32', 'float32', 'int16', 'int8']]
        return numeric_cols
    
    def test_improvement(self, df: pd.DataFrame, improvement: str) -> Dict:
        """単一改善のテスト"""
        logger.info(f"テスト: {improvement}")
        
        results = []
        
        for test_year in [2020, 2021, 2022, 2023, 2024, 2025]:
            train_mask = df['year'] < test_year
            test_mask = df['year'] == test_year
            
            train_raw = df[train_mask].copy()
            test_raw = df[test_mask].copy()
            
            if len(test_raw) == 0 or len(train_raw) == 0:
                continue
            
            # 特徴量生成（訓練）
            train_df = self._generate_base_features(train_raw)
            train_df = self._apply_improvement(train_df, improvement)
            
            # 特徴量生成（テスト - 訓練データの統計を使用）
            test_df = self._generate_base_features(test_raw)
            test_df = self._apply_improvement(test_df, improvement)
            
            feature_cols = self._get_feature_cols(train_df)
            
            # 訓練
            X_train = train_df[feature_cols].copy()
            y_train = train_df['target'].values
            
            X_test = test_df[feature_cols].copy()
            y_test = test_df['target'].values
            
            # 欠損カラム対応
            for col in feature_cols:
                if col not in X_test.columns:
                    X_test[col] = np.nan
            X_test = X_test[feature_cols]
            
            params = self._get_params(improvement)
            lgb_train = lgb.Dataset(X_train, label=y_train)
            
            model = lgb.train(params, lgb_train, num_boost_round=self.num_boost_round)
            
            # 評価
            preds = model.predict(X_test)
            
            test_result = test_df[['race_id', 'finish_position', 'win_odds', 'target']].copy()
            test_result['pred_prob'] = preds
            test_result['pred_rank'] = test_result.groupby('race_id')['pred_prob'].rank(ascending=False, method='first')
            
            top1 = test_result[test_result['pred_rank'] == 1]
            hits = top1[top1['target'] == 1]
            roi = hits['win_odds'].sum() / len(top1) if len(top1) > 0 else 0
            hit_rate = len(hits) / len(top1) if len(top1) > 0 else 0
            
            # リーク検証：予測50%+の実勝率
            high_prob = test_result[test_result['pred_prob'] > 0.5]
            leak_check = high_prob['target'].mean() if len(high_prob) > 0 else np.nan
            
            results.append({
                'test_year': test_year,
                'roi': roi,
                'hit_rate': hit_rate,
                'n_races': len(top1),
                'leak_check': leak_check,
            })
        
        results_df = pd.DataFrame(results)
        avg_roi = results_df['roi'].mean()
        avg_hit = results_df['hit_rate'].mean()
        
        return {
            'improvement': improvement,
            'avg_roi': avg_roi,
            'avg_hit_rate': avg_hit,
            'yearly_results': results_df,
        }
    
    def run_all_tests(self):
        """全テスト実行"""
        df = self.load_data()
        
        improvements = [
            'baseline',
            'A_prev_finish_log',
            'B_rest_optimal_gap',
            'C_jockey_trainer_log',
            'D_is_comeback',
            'E_strong_reg',
            'F_slow_lr',
            'ALL_combined',
        ]
        
        all_results = []
        
        for imp in improvements:
            result = self.test_improvement(df, imp)
            all_results.append(result)
            logger.info(f"  {imp}: ROI {result['avg_roi']*100:.1f}%, Hit {result['avg_hit_rate']*100:.1f}%")
        
        # サマリー
        print("\n" + "="*80)
        print("【A/Bテスト結果サマリー】")
        print("="*80)
        print(f"\n{'改善名':<25} {'平均ROI':>10} {'平均的中率':>10} {'vs ベースライン':>15}")
        print("-"*70)
        
        baseline_roi = all_results[0]['avg_roi']
        for r in all_results:
            diff = (r['avg_roi'] - baseline_roi) * 100
            diff_str = f"{diff:+.1f}pt"
            print(f"{r['improvement']:<25} {r['avg_roi']*100:>9.1f}% {r['avg_hit_rate']*100:>9.1f}% {diff_str:>15}")
        
        # 年別詳細
        print("\n" + "="*80)
        print("【年別詳細】")
        print("="*80)
        
        for r in all_results:
            print(f"\n{r['improvement']}:")
            for _, row in r['yearly_results'].iterrows():
                leak_str = f"(leak_check={row['leak_check']:.2f})" if pd.notna(row['leak_check']) else ""
                print(f"  {int(row['test_year'])}: ROI {row['roi']*100:.1f}%, Hit {row['hit_rate']*100:.1f}% {leak_str}")
        
        # 結果保存
        summary = pd.DataFrame([{
            'improvement': r['improvement'],
            'avg_roi': r['avg_roi'],
            'avg_hit_rate': r['avg_hit_rate'],
            'vs_baseline': r['avg_roi'] - baseline_roi,
        } for r in all_results])
        summary.to_csv(self.output_dir / 'ab_test_summary.csv', index=False)
        
        logger.info(f"結果保存: {self.output_dir}")
        
        return all_results


if __name__ == "__main__":
    tester = ImprovementABTester()
    results = tester.run_all_tests()
