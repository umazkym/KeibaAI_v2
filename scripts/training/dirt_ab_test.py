#!/usr/bin/env python3
"""
ダートモデル A/Bテスト (dirt_ab_test.py)

芝で有効だった改善がダートでも効果的か検証
"""

import sys
from pathlib import Path
import pandas as pd
import numpy as np
import lightgbm as lgb
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class DirtABTester:
    """ダートモデルA/Bテスト"""
    
    LEAK_FEATURES = [
        'finish_position', 'finish_time_seconds', 'margin_seconds', 'prize_money',
        'finish_time_str', 'margin_str', 'last_3f_time', 'time_except_last3f',
        'last3f_rank', 'popularity', 'win_odds', 'odds', 'win_probability',
        'relative_odds', 'popularity_finish_diff', 'passing_order', 
        'passing_order_1', 'passing_order_2', 'passing_order_3', 'passing_order_4',
        'final_corner_to_finish', 'position_change_1_2', 'position_change_2_3',
        'position_change_3_4', 'pace_index', 'target', 'weight',
        'horse_weight_change', 'horse_weight_deviation',
    ]
    
    META_COLS = [
        'race_id', 'horse_id', 'race_date', 'jockey_id', 'trainer_id', 'owner_id',
        'sire_id', 'damsire_id', 'race_name', 'horse_name', 'jockey_name', 
        'trainer_name', 'scratched', 'year', 'track_surface', 'race_class',
        'venue', 'track_condition', 'distance_category', 'bracket_category',
        'sex', 'weather',
    ]
    
    def __init__(self):
        self.data_dir = Path('keibaai/data/parsed/parquet')
        self.output_dir = Path('keibaai/models/dirt_ab_test_results')
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        self.baseline_params = {
            'objective': 'binary', 'metric': 'auc', 'boosting_type': 'gbdt',
            'learning_rate': 0.02, 'max_depth': 3, 'num_leaves': 8,
            'min_child_samples': 300, 'reg_alpha': 10.0, 'reg_lambda': 10.0,
            'feature_fraction': 0.7, 'bagging_fraction': 0.7, 'bagging_freq': 5,
            'verbosity': -1, 'seed': 42,
        }
        
    def load_data(self):
        df = pd.read_parquet(self.data_dir / 'races/races.parquet')
        df['race_date'] = pd.to_datetime(df['race_date'])
        df['year'] = df['race_date'].dt.year
        df = df[df['year'] >= 2014]
        df = df[df['track_surface'] == 'ダート']  # ダートのみ
        df = df[df['race_class'] != '新馬']
        logger.info(f"ダートデータ: {len(df):,}件")
        return df
    
    def _generate_features(self, df, improvement):
        df = df.copy()
        df = df.sort_values(['race_date', 'race_id', 'horse_number']).reset_index(drop=True)
        df['target'] = (df['finish_position'] == 1).astype(int)
        
        track_map = {'良': 0, '稀重': 1, '重': 2, '不良': 3}
        df['track_condition_encoded'] = df['track_condition'].map(track_map).fillna(0)
        df['field_size'] = df.groupby('race_id')['horse_number'].transform('count')
        df['race_number'] = df['race_id'].str[-2:].astype(int)
        
        df['horse_cum_wins'] = df.groupby('horse_id')['target'].transform(lambda x: x.cumsum().shift(1).fillna(0))
        df['horse_cum_races'] = df.groupby('horse_id')['target'].transform(lambda x: x.expanding().count().shift(1).fillna(0))
        df['horse_win_rate'] = np.where(df['horse_cum_races'] >= 3, df['horse_cum_wins'] / df['horse_cum_races'], np.nan)
        
        df['horse_cum_finish_sum'] = df.groupby('horse_id')['finish_position'].transform(lambda x: x.cumsum().shift(1).fillna(0))
        df['horse_avg_finish'] = np.where(df['horse_cum_races'] >= 1, df['horse_cum_finish_sum'] / df['horse_cum_races'], np.nan)
        
        df['jockey_cum_wins'] = df.groupby('jockey_id')['target'].transform(lambda x: x.cumsum().shift(1).fillna(0))
        df['jockey_cum_races'] = df.groupby('jockey_id')['target'].transform(lambda x: x.expanding().count().shift(1).fillna(0))
        df['jockey_win_rate'] = np.where(df['jockey_cum_races'] >= 10, df['jockey_cum_wins'] / df['jockey_cum_races'], np.nan)
        
        df['trainer_cum_wins'] = df.groupby('trainer_id')['target'].transform(lambda x: x.cumsum().shift(1).fillna(0))
        df['trainer_cum_races'] = df.groupby('trainer_id')['target'].transform(lambda x: x.expanding().count().shift(1).fillna(0))
        df['trainer_win_rate'] = np.where(df['trainer_cum_races'] >= 10, df['trainer_cum_wins'] / df['trainer_cum_races'], np.nan)
        
        df['prev_finish'] = df.groupby('horse_id')['finish_position'].shift(1)
        df['prev_race_date'] = df.groupby('horse_id')['race_date'].shift(1)
        df['days_since_last'] = (df['race_date'] - df['prev_race_date']).dt.days
        
        # 改善適用
        if improvement == 'A_prev_finish_log':
            df['prev_finish'] = np.log1p(df['prev_finish'].fillna(10))
        elif improvement == 'B_rest_optimal_gap':
            df['rest_optimal_gap'] = np.abs(df['days_since_last'].fillna(60) - 45)
            df['is_short_rest'] = (df['days_since_last'].fillna(60) <= 21).astype(int)
        elif improvement == 'C_jockey_trainer_log':
            df['jockey_win_rate'] = np.log1p(df['jockey_win_rate'].fillna(0.05) * 100) / 5
            df['trainer_win_rate'] = np.log1p(df['trainer_win_rate'].fillna(0.05) * 100) / 5
        elif improvement == 'D_is_comeback':
            df['is_comeback'] = ((df['prev_finish'].fillna(99) >= 8) & (df['horse_avg_finish'].fillna(99) <= 5)).astype(int)
        elif improvement == 'ALL_combined':
            df['prev_finish_log'] = np.log1p(df['prev_finish'].fillna(10))
            df['rest_optimal_gap'] = np.abs(df['days_since_last'].fillna(60) - 45)
            df['is_short_rest'] = (df['days_since_last'].fillna(60) <= 21).astype(int)
            df['jockey_win_rate_log'] = np.log1p(df['jockey_win_rate'].fillna(0.05) * 100) / 5
            df['trainer_win_rate_log'] = np.log1p(df['trainer_win_rate'].fillna(0.05) * 100) / 5
            df['is_comeback'] = ((df['prev_finish'].fillna(99) >= 8) & (df['horse_avg_finish'].fillna(99) <= 5)).astype(int)
        
        # 一時カラム削除
        temp_cols = ['horse_cum_wins', 'horse_cum_races', 'horse_cum_finish_sum',
                     'jockey_cum_wins', 'jockey_cum_races', 'trainer_cum_wins', 
                     'trainer_cum_races', 'prev_race_date']
        df = df.drop(columns=[c for c in temp_cols if c in df.columns], errors='ignore')
        
        return df
    
    def _get_feature_cols(self, df):
        exclude = set(self.LEAK_FEATURES + self.META_COLS)
        return [c for c in df.columns if c not in exclude and df[c].dtype in ['int64', 'float64', 'int32', 'float32']]
    
    def _get_params(self, improvement):
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
    
    def test_improvement(self, df, improvement):
        logger.info(f"テスト: {improvement}")
        results = []
        
        for test_year in [2020, 2021, 2022, 2023, 2024, 2025]:
            train_mask = df['year'] < test_year
            test_mask = df['year'] == test_year
            
            train_df = self._generate_features(df[train_mask].copy(), improvement)
            test_df = self._generate_features(df[test_mask].copy(), improvement)
            
            if len(test_df) == 0:
                continue
            
            feature_cols = self._get_feature_cols(train_df)
            X_train = train_df[feature_cols]
            y_train = train_df['target'].values
            X_test = test_df[feature_cols].copy()
            for col in feature_cols:
                if col not in X_test.columns:
                    X_test[col] = np.nan
            X_test = X_test[feature_cols]
            
            params = self._get_params(improvement)
            lgb_train = lgb.Dataset(X_train, label=y_train)
            model = lgb.train(params, lgb_train, num_boost_round=300)
            
            preds = model.predict(X_test)
            test_result = test_df[['race_id', 'finish_position', 'win_odds', 'target']].copy()
            test_result['pred_prob'] = preds
            test_result['pred_rank'] = test_result.groupby('race_id')['pred_prob'].rank(ascending=False, method='first')
            
            top1 = test_result[test_result['pred_rank'] == 1]
            hits = top1[top1['target'] == 1]
            roi = hits['win_odds'].sum() / len(top1) if len(top1) > 0 else 0
            results.append(roi)
        
        avg_roi = np.mean(results)
        return {'improvement': improvement, 'avg_roi': avg_roi}
    
    def run_all(self):
        df = self.load_data()
        
        improvements = ['baseline', 'A_prev_finish_log', 'B_rest_optimal_gap', 
                        'C_jockey_trainer_log', 'D_is_comeback', 'E_strong_reg', 
                        'F_slow_lr', 'ALL_combined']
        
        all_results = []
        for imp in improvements:
            result = self.test_improvement(df, imp)
            all_results.append(result)
            logger.info(f"  {imp}: ROI {result['avg_roi']*100:.1f}%")
        
        print("\n" + "="*60)
        print("【ダートA/Bテスト結果】")
        print("="*60)
        
        baseline_roi = all_results[0]['avg_roi']
        for r in all_results:
            diff = (r['avg_roi'] - baseline_roi) * 100
            print(f"  {r['improvement']:<25}: {r['avg_roi']*100:.1f}% ({diff:+.1f}pt)")
        
        # 保存
        summary = pd.DataFrame(all_results)
        summary.to_csv(self.output_dir / 'dirt_ab_test_summary.csv', index=False)
        
        return all_results


if __name__ == "__main__":
    tester = DirtABTester()
    results = tester.run_all()
