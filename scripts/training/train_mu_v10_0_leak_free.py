#!/usr/bin/env python3
"""
μモデル v10.0 学習スクリプト（リークフリー版）

【改善点】
1. リークフリー版gap特徴量を使用
2. 穴馬フォーカス戦略（v9.0と同様）
3. v9.0の新特徴量も継続使用

【データリーク防止】
- 全特徴量でexpanding().mean().shift(1)を使用
- 当該レースの情報（着順、オッズ等）は使用しない

【目標】Test ROI 85%以上（リークフリーで）
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

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class MuV100Trainer:
    """μモデル v10.0 学習クラス（リークフリー版）"""
    
    LONGSHOT_MIN_POP = 5
    LONGSHOT_MAX_POP = 9
    LONGSHOT_BOOST = 1.5
    
    def __init__(self):
        self.base_model_dir = Path('keibaai/models/mu_v3_3')
        self.output_dir = Path('keibaai/models/mu_v10_0')
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.model = None
        self.pedigree_df = None
    
    def load_data(self):
        """データ読み込み"""
        logging.info("=" * 60)
        logging.info("v10.0: データ読み込み")
        logging.info("=" * 60)
        
        # ベースデータ（v3.3の特徴量付き）
        train_data = pd.read_parquet(self.base_model_dir / 'train_data_mu_v3_3.parquet')
        train_data['race_date'] = pd.to_datetime(train_data['race_date'])
        
        # レース結果（リークフリー特徴量生成用）
        races_df = pd.read_parquet('keibaai/data/parsed/parquet/races/races.parquet')
        races_df['race_date'] = pd.to_datetime(races_df['race_date'])
        
        # 血統
        pedigree_path = Path('keibaai/data/parsed/parquet/pedigrees/pedigrees.parquet')
        if pedigree_path.exists():
            self.pedigree_df = pd.read_parquet(pedigree_path)
        
        logging.info(f"  ベースデータ: {len(train_data):,}")
        logging.info(f"  レース結果: {len(races_df):,}")
        
        train_data['horse_id'] = train_data['horse_id'].astype(str)
        races_df['horse_id'] = races_df['horse_id'].astype(str)
        
        return train_data, races_df
    
    def generate_leak_free_gap_features(self, df, races_df):
        """リークフリー版gap特徴量を生成"""
        logging.info("=" * 60)
        logging.info("v10.0: リークフリー版gap特徴量生成")
        logging.info("=" * 60)
        
        perf = races_df.copy()
        perf = perf.sort_values(['race_date', 'race_id']).reset_index(drop=True)
        
        # 型変換
        perf['finish_position'] = pd.to_numeric(perf['finish_position'], errors='coerce').astype(float)
        perf['popularity'] = pd.to_numeric(perf['popularity'], errors='coerce').astype(float)
        perf['jockey_id'] = perf['jockey_id'].astype(str)
        perf['trainer_id'] = perf['trainer_id'].astype(str)
        
        # 勝利フラグ
        perf['is_win'] = (perf['finish_position'] == 1).fillna(False).astype(int)
        
        # 騎手累積勝率（リークフリー）
        logging.info("  騎手累積勝率を計算中...")
        perf['jockey_win_rate_lf'] = perf.groupby('jockey_id')['is_win'].transform(
            lambda x: x.expanding().mean().shift(1)
        )
        
        # 調教師累積勝率（リークフリー）
        logging.info("  調教師累積勝率を計算中...")
        perf['trainer_win_rate_lf'] = perf.groupby('trainer_id')['is_win'].transform(
            lambda x: x.expanding().mean().shift(1)
        )
        
        # 父累積勝率（リークフリー）
        if self.pedigree_df is not None:
            logging.info("  父累積勝率を計算中...")
            sire_map = self.pedigree_df[self.pedigree_df['generation'] == 1][['horse_id', 'ancestor_id']].copy()
            sire_map.columns = ['horse_id', 'sire_id']
            sire_map['horse_id'] = sire_map['horse_id'].astype(str)
            sire_map = sire_map.drop_duplicates('horse_id')
            
            perf = perf.merge(sire_map, on='horse_id', how='left')
            perf['sire_win_rate_lf'] = perf.groupby('sire_id')['is_win'].transform(
                lambda x: x.expanding().mean().shift(1)
            )
        else:
            perf['sire_win_rate_lf'] = np.nan
        
        # gap特徴量を計算
        logging.info("  gap特徴量を計算中...")
        
        # 各レース内で順位を計算
        perf['jockey_rank_lf'] = perf.groupby('race_id')['jockey_win_rate_lf'].rank(ascending=False, method='average')
        perf['gap_jockey_pop_lf'] = perf['popularity'] - perf['jockey_rank_lf']
        
        perf['trainer_rank_lf'] = perf.groupby('race_id')['trainer_win_rate_lf'].rank(ascending=False, method='average')
        perf['gap_trainer_pop_lf'] = perf['popularity'] - perf['trainer_rank_lf']
        
        if 'sire_win_rate_lf' in perf.columns:
            perf['sire_rank_lf'] = perf.groupby('race_id')['sire_win_rate_lf'].rank(ascending=False, method='average')
            perf['gap_sire_pop_lf'] = perf['popularity'] - perf['sire_rank_lf']
        else:
            perf['gap_sire_pop_lf'] = 0
        
        # マージ
        merge_cols = ['horse_id', 'race_date', 
                      'jockey_win_rate_lf', 'trainer_win_rate_lf', 'sire_win_rate_lf',
                      'gap_jockey_pop_lf', 'gap_trainer_pop_lf', 'gap_sire_pop_lf']
        merge_df = perf[merge_cols].drop_duplicates(subset=['horse_id', 'race_date'], keep='last')
        
        result = df.merge(merge_df, on=['horse_id', 'race_date'], how='left')
        
        logging.info(f"  gap_jockey_pop_lf mean: {result['gap_jockey_pop_lf'].mean():.3f}")
        logging.info(f"  gap_trainer_pop_lf mean: {result['gap_trainer_pop_lf'].mean():.3f}")
        logging.info(f"  gap_sire_pop_lf mean: {result['gap_sire_pop_lf'].mean():.3f}")
        
        return result
    
    def prepare_features(self, df):
        """特徴量リストを準備"""
        # v3.3のベース特徴量
        with open(self.base_model_dir / 'feature_names.json', 'r', encoding='utf-8') as f:
            base_features = json.load(f)
        
        # リーク版gap特徴量を除外
        leak_features = [
            'gap_jockey_popularity', 'gap_pedigree_popularity',
            'gap_course_fit_popularity', 'gap_trainer_popularity',
            'gap_speed_popularity',
            'jockey_win_rate', 'jockey_place_rate', 'jockey_avg_rank',
            'sire_win_rate', 'sire_avg_finish',
            'trainer_win_rate', 'trainer_place_rate',
        ]
        base_features = [f for f in base_features if f not in leak_features]
        
        # リークフリー版gap特徴量を追加
        new_features = [
            'jockey_win_rate_lf', 'trainer_win_rate_lf', 'sire_win_rate_lf',
            'gap_jockey_pop_lf', 'gap_trainer_pop_lf', 'gap_sire_pop_lf',
        ]
        
        available = [f for f in base_features + new_features if f in df.columns]
        available = list(dict.fromkeys(available))  # 重複除去
        
        logging.info(f"特徴量数: {len(available)}")
        return available
    
    def prepare_target(self, df):
        """ターゲットとサンプル重みを準備"""
        df['win_odds'] = pd.to_numeric(df['win_odds'], errors='coerce')
        df['popularity'] = pd.to_numeric(df['popularity'], errors='coerce')
        df['finish_position'] = pd.to_numeric(df['finish_position'], errors='coerce')
        
        odds = df['win_odds'].fillna(1.0).clip(upper=90)
        log_odds = np.log1p(odds)
        popularity = df['popularity'].fillna(10)
        finish_pos = df['finish_position'].fillna(99)
        
        # LambdaRankのターゲット
        gain = np.zeros(len(df))
        gain[finish_pos == 1] = np.nan_to_num(log_odds[finish_pos == 1] * 12.74, nan=0)
        gain[finish_pos == 2] = np.nan_to_num(log_odds[finish_pos == 2] * 6.73, nan=0)
        gain[finish_pos == 3] = np.nan_to_num(log_odds[finish_pos == 3] * 3.69, nan=0)
        df['target_relevance'] = np.nan_to_num(gain, nan=0).astype(int)
        
        # サンプル重み（穴馬ブースト）
        base_weight = np.log1p(odds).clip(upper=np.log1p(100))
        boost_mask = (
            (popularity >= self.LONGSHOT_MIN_POP) & 
            (popularity <= self.LONGSHOT_MAX_POP) &
            (finish_pos == 1)
        )
        weight = base_weight.copy()
        weight[boost_mask] = weight[boost_mask] * self.LONGSHOT_BOOST
        df['sample_weight'] = np.nan_to_num(weight, nan=1.0)
        
        logging.info(f"穴馬ブースト: {self.LONGSHOT_MIN_POP}-{self.LONGSHOT_MAX_POP}人気 × {self.LONGSHOT_BOOST}x")
        logging.info(f"ブースト対象: {boost_mask.sum():,}件")
        
        return df
    
    def train(self, df, feature_cols, n_trials=30):
        """モデル学習"""
        logging.info("=" * 60)
        logging.info("μモデル v10.0 学習開始（リークフリー版）")
        logging.info("★★★ Testデータは最終確認でのみ使用 ★★★")
        logging.info("=" * 60)
        
        train_df = df[df['race_date'] < '2023-01-01'].copy()
        valid_df = df[(df['race_date'] >= '2023-01-01') & (df['race_date'] < '2024-01-01')].copy()
        test_df = df[df['race_date'] >= '2024-01-01'].copy()
        
        # NaNを除去
        for d in [train_df, valid_df, test_df]:
            for col in feature_cols:
                d[col] = d[col].fillna(0)
        
        group_train = train_df.groupby('race_id').size().tolist()
        group_valid = valid_df.groupby('race_id').size().tolist()
        
        logging.info(f"Train: {len(train_df):,}, Valid: {len(valid_df):,}, Test: {len(test_df):,}")
        
        def calculate_roi(d, preds):
            d = d.copy()
            d['score'] = preds
            d['rank_pred'] = d.groupby('race_id')['score'].rank(ascending=False, method='first')
            bet = d[d['rank_pred'] == 1]
            hits = bet[bet['finish_position'] == 1]
            return hits['win_odds'].sum() / len(bet) if len(bet) > 0 else 0
        
        def objective(trial):
            params = {
                'objective': 'lambdarank', 'metric': 'ndcg', 'eval_at': [1, 3, 5],
                'boosting_type': 'gbdt', 'verbose': -1, 'random_state': 42,
                'label_gain': list(range(100)),
                'num_leaves': trial.suggest_int('num_leaves', 40, 100),
                'lambda_l1': trial.suggest_float('lambda_l1', 1.0, 15.0, log=True),
                'lambda_l2': trial.suggest_float('lambda_l2', 0.5, 8.0, log=True),
                'min_child_samples': trial.suggest_int('min_child_samples', 40, 100),
                'learning_rate': trial.suggest_float('learning_rate', 0.02, 0.08, log=True),
                'feature_fraction': trial.suggest_float('feature_fraction', 0.5, 0.8),
                'bagging_fraction': trial.suggest_float('bagging_fraction', 0.6, 0.85),
                'bagging_freq': trial.suggest_int('bagging_freq', 1, 7),
                'max_depth': trial.suggest_int('max_depth', 5, 8),
            }
            
            model = lgb.LGBMRanker(**params, n_estimators=1500)
            model.fit(
                train_df[feature_cols], train_df['target_relevance'],
                group=group_train, sample_weight=train_df['sample_weight'],
                eval_set=[(valid_df[feature_cols], valid_df['target_relevance'])],
                eval_group=[group_valid],
                callbacks=[lgb.early_stopping(50, verbose=False)]
            )
            
            valid_roi = calculate_roi(valid_df, model.predict(valid_df[feature_cols]))
            return valid_roi
        
        logging.info(f"Optuna {n_trials}トライアル...")
        study = optuna.create_study(direction='maximize')
        study.optimize(objective, n_trials=n_trials, show_progress_bar=True)
        
        best_params = study.best_params
        best_params.update({
            'objective': 'lambdarank', 'metric': 'ndcg', 'eval_at': [1, 3, 5],
            'boosting_type': 'gbdt', 'verbose': -1, 'random_state': 42,
            'label_gain': list(range(100))
        })
        
        logging.info(f"最良のValid ROI: {study.best_value:.2%}")
        
        # 最終モデル学習
        self.model = lgb.LGBMRanker(**best_params, n_estimators=2000)
        self.model.fit(
            train_df[feature_cols], train_df['target_relevance'],
            group=group_train, sample_weight=train_df['sample_weight'],
            eval_set=[(valid_df[feature_cols], valid_df['target_relevance'])],
            eval_group=[group_valid],
            callbacks=[lgb.early_stopping(100, verbose=True)]
        )
        
        # 最終評価
        valid_roi = calculate_roi(valid_df, self.model.predict(valid_df[feature_cols]))
        test_roi = calculate_roi(test_df, self.model.predict(test_df[feature_cols]))
        
        importance = dict(zip(feature_cols, self.model.feature_importances_))
        sorted_imp = sorted(importance.items(), key=lambda x: x[1], reverse=True)
        
        logging.info("=" * 60)
        logging.info("【最終結果: μ v10.0（リークフリー版）】")
        logging.info("=" * 60)
        logging.info(f"  Valid ROI: {valid_roi:.2%}")
        logging.info(f"  Test ROI: {test_roi:.2%}")
        logging.info(f"  Valid-Test差: {abs(valid_roi - test_roi):.2%}")
        logging.info("")
        logging.info("【Top 20特徴量】")
        for i, (name, imp) in enumerate(sorted_imp[:20], 1):
            logging.info(f"  {i:2}. {name}: {imp}")
        
        self.save_model(feature_cols, valid_roi, test_roi, best_params)
        return valid_roi, test_roi
    
    def save_model(self, features, valid_roi, test_roi, best_params):
        """モデル保存"""
        with open(self.output_dir / 'mu_v10_0_ranker.pkl', 'wb') as f:
            pickle.dump(self.model, f)
        with open(self.output_dir / 'feature_names.json', 'w', encoding='utf-8') as f:
            json.dump(features, f, ensure_ascii=False, indent=2)
        
        save_params = {k: v for k, v in best_params.items() if k != 'label_gain'}
        metadata = {
            'version': 'v10.0',
            'description': 'リークフリー版gap特徴量 + 穴馬フォーカス',
            'valid_roi': float(valid_roi),
            'test_roi': float(test_roi),
            'valid_test_gap': float(abs(valid_roi - test_roi)),
            'leak_free': True,
            'best_params': save_params,
            'created_at': datetime.now().isoformat()
        }
        with open(self.output_dir / 'model_info.json', 'w', encoding='utf-8') as f:
            json.dump(metadata, f, ensure_ascii=False, indent=2)
        
        logging.info(f"保存完了: {self.output_dir}")
    
    def run(self, n_trials=30):
        """メイン実行"""
        train_data, races_df = self.load_data()
        
        # リークフリーgap特徴量生成
        train_data = self.generate_leak_free_gap_features(train_data, races_df)
        
        # ターゲット準備
        train_data = self.prepare_target(train_data)
        
        # 特徴量選択
        feature_cols = self.prepare_features(train_data)
        
        # 学習
        return self.train(train_data, feature_cols, n_trials)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description='μモデル v10.0 学習')
    parser.add_argument('--trials', type=int, default=30, help='Optunaトライアル数')
    args = parser.parse_args()
    
    trainer = MuV100Trainer()
    valid_roi, test_roi = trainer.run(n_trials=args.trials)
    
    print("\n" + "=" * 60)
    print("【結果サマリー】")
    print("=" * 60)
    print(f"  Valid ROI: {valid_roi:.2%}")
    print(f"  Test ROI:  {test_roi:.2%}")
    print(f"  v5.4比:    {test_roi - 0.8005:+.2%}")
