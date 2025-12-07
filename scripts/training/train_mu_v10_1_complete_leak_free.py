#!/usr/bin/env python3
"""
μモデル v10.1 学習スクリプト（完全リークフリー版）

【リーク回避戦略】
1. 騎手/調教師/コンボ: expanding().mean().shift(1) で再計算
2. 血統関連: Train期間の統計を固定使用
3. リーク版特徴量を全て除外

【過学習防止】
1. 強正則化（L1/L2を高く設定）
2. num_leavesを小さく
3. max_depthを浅く
4. feature_fractionを低く
5. Valid-Test差を監視

【目標】
- リークフリーでValid-Test差 < 5%
- Test ROI > 82%（v5.4の80%を超える）
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


class MuV101TrainerLeakFree:
    """μモデル v10.1 完全リークフリー版"""
    
    # 過学習防止のための強正則化パラメータ範囲
    REGULARIZATION_RANGES = {
        'num_leaves': (20, 50),  # 小さめに
        'lambda_l1': (5.0, 30.0),  # 高めに
        'lambda_l2': (3.0, 15.0),  # 高めに
        'min_child_samples': (80, 200),  # 高めに
        'learning_rate': (0.01, 0.04),  # 低めに
        'feature_fraction': (0.4, 0.65),  # 低めに
        'bagging_fraction': (0.6, 0.8),
        'max_depth': (4, 6),  # 浅めに
    }
    
    # リーク版として除外する特徴量パターン
    LEAK_PATTERNS = [
        # 騎手条件別（v10.1で再計算するので除外）
        'jockey_intermediate_win_rate', 'jockey_long_win_rate', 
        'jockey_marathon_win_rate', 'jockey_mile_win_rate',
        'jockey_sprint_win_rate', 'jockey_unknown_win_rate',
        'jockey_ダート_win_rate', 'jockey_芝_win_rate',
        # 騎手会場別
        'jockey_東京_win_rate', 'jockey_中山_win_rate', 'jockey_京都_win_rate',
        'jockey_阪神_win_rate', 'jockey_中京_win_rate', 'jockey_小倉_win_rate',
        'jockey_福島_win_rate', 'jockey_新潟_win_rate', 'jockey_函館_win_rate',
        'jockey_札幌_win_rate',
        # 調教師会場別
        'trainer_東京_win_rate', 'trainer_中山_win_rate', 'trainer_京都_win_rate',
        'trainer_阪神_win_rate', 'trainer_中京_win_rate', 'trainer_小倉_win_rate',
        'trainer_福島_win_rate', 'trainer_新潟_win_rate', 'trainer_函館_win_rate',
        'trainer_札幌_win_rate',
        # コンボ（v10.1で再計算するので除外）
        'combo_win_rate', 'combo_avg_finish', 'combo_overperform', 'combo_races',
        # 血統（Train固定版を使うので元版は除外）
        'sire_course_win_rate', 'sire_course_avg_finish', 'sire_course_place_rate',
        'sire_wet_boost', 'bms_win_rate', 'bms_avg_finish',
        'nicks_win_rate', 'nicks_avg_finish',
        # 元のリーク版gap
        'gap_jockey_popularity', 'gap_pedigree_popularity',
        'gap_course_fit_popularity', 'gap_trainer_popularity',
        'gap_speed_popularity',
        # 元のリーク版勝率
        'jockey_win_rate', 'jockey_place_rate', 'jockey_avg_rank',
        'trainer_win_rate', 'trainer_place_rate',
        'sire_win_rate', 'sire_avg_finish',
    ]
    
    def __init__(self):
        self.base_model_dir = Path('keibaai/models/mu_v3_3')
        self.output_dir = Path('keibaai/models/mu_v10_1')
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.model = None
        self.pedigree_df = None
        self.train_sire_stats = None  # Train期間の血統統計
    
    def load_data(self):
        """データ読み込み"""
        logging.info("=" * 60)
        logging.info("v10.1: データ読み込み")
        logging.info("=" * 60)
        
        train_data = pd.read_parquet(self.base_model_dir / 'train_data_mu_v3_3.parquet')
        train_data['race_date'] = pd.to_datetime(train_data['race_date'])
        
        races_df = pd.read_parquet('keibaai/data/parsed/parquet/races/races.parquet')
        races_df['race_date'] = pd.to_datetime(races_df['race_date'])
        
        pedigree_path = Path('keibaai/data/parsed/parquet/pedigrees/pedigrees.parquet')
        if pedigree_path.exists():
            self.pedigree_df = pd.read_parquet(pedigree_path)
        
        logging.info(f"  ベースデータ: {len(train_data):,}")
        logging.info(f"  レース結果: {len(races_df):,}")
        
        # 型変換（存在するカラムのみ）
        for col in ['horse_id', 'jockey_id', 'trainer_id']:
            if col in train_data.columns:
                train_data[col] = train_data[col].astype(str)
            if col in races_df.columns:
                races_df[col] = races_df[col].astype(str)
        
        return train_data, races_df
    
    def generate_leak_free_features(self, df, races_df):
        """完全リークフリー特徴量を生成"""
        logging.info("=" * 60)
        logging.info("v10.1: 完全リークフリー特徴量生成")
        logging.info("★★★ 過学習防止のため慎重に計算 ★★★")
        logging.info("=" * 60)
        
        perf = races_df.copy()
        perf = perf.sort_values(['race_date', 'race_id']).reset_index(drop=True)
        
        # 型変換
        perf['finish_position'] = pd.to_numeric(perf['finish_position'], errors='coerce').astype(float)
        perf['popularity'] = pd.to_numeric(perf['popularity'], errors='coerce').astype(float)
        perf['is_win'] = (perf['finish_position'] == 1).fillna(False).astype(int)
        
        # ====== Phase A: 騎手/調教師の累積勝率（リークフリー） ======
        logging.info("  Phase A: 騎手/調教師累積勝率...")
        
        perf['jockey_win_rate_lf'] = perf.groupby('jockey_id')['is_win'].transform(
            lambda x: x.expanding().mean().shift(1)
        )
        perf['trainer_win_rate_lf'] = perf.groupby('trainer_id')['is_win'].transform(
            lambda x: x.expanding().mean().shift(1)
        )
        
        # 騎手×コース（リークフリー）
        if 'venue' in perf.columns:
            perf['jockey_venue_key'] = perf['jockey_id'] + '_' + perf['venue'].astype(str)
            perf['jockey_venue_win_rate_lf'] = perf.groupby('jockey_venue_key')['is_win'].transform(
                lambda x: x.expanding().mean().shift(1)
            )
        
        # 騎手×馬場（リークフリー）
        if 'track_surface' in perf.columns:
            perf['jockey_surface_key'] = perf['jockey_id'] + '_' + perf['track_surface'].astype(str)
            perf['jockey_surface_win_rate_lf'] = perf.groupby('jockey_surface_key')['is_win'].transform(
                lambda x: x.expanding().mean().shift(1)
            )
        
        # ====== Phase B: 血統統計（Train期間固定） ======
        logging.info("  Phase B: 血統統計（Train期間固定）...")
        
        train_cutoff = pd.Timestamp('2023-01-01')
        train_perf = perf[perf['race_date'] < train_cutoff].copy()
        
        if self.pedigree_df is not None:
            sire_map = self.pedigree_df[self.pedigree_df['generation'] == 1][['horse_id', 'ancestor_id']].copy()
            sire_map.columns = ['horse_id', 'sire_id']
            sire_map['horse_id'] = sire_map['horse_id'].astype(str)
            sire_map = sire_map.drop_duplicates('horse_id')
            
            train_perf = train_perf.merge(sire_map, on='horse_id', how='left')
            perf = perf.merge(sire_map, on='horse_id', how='left')
            
            # Train期間の父別勝率（固定）
            sire_stats = train_perf.groupby('sire_id').agg({
                'is_win': ['mean', 'count']
            }).reset_index()
            sire_stats.columns = ['sire_id', 'sire_win_rate_fixed', 'sire_count']
            sire_stats.loc[sire_stats['sire_count'] < 20, 'sire_win_rate_fixed'] = np.nan
            
            self.train_sire_stats = sire_stats
            perf = perf.merge(sire_stats[['sire_id', 'sire_win_rate_fixed']], on='sire_id', how='left')
        
        # ====== Phase C: gap特徴量（リークフリー） ======
        logging.info("  Phase C: gap特徴量（リークフリー）...")
        
        perf['jockey_rank_lf'] = perf.groupby('race_id')['jockey_win_rate_lf'].rank(ascending=False, method='average')
        perf['gap_jockey_pop_lf'] = perf['popularity'] - perf['jockey_rank_lf']
        
        perf['trainer_rank_lf'] = perf.groupby('race_id')['trainer_win_rate_lf'].rank(ascending=False, method='average')
        perf['gap_trainer_pop_lf'] = perf['popularity'] - perf['trainer_rank_lf']
        
        if 'sire_win_rate_fixed' in perf.columns:
            perf['sire_rank_fixed'] = perf.groupby('race_id')['sire_win_rate_fixed'].rank(ascending=False, method='average')
            perf['gap_sire_pop_fixed'] = perf['popularity'] - perf['sire_rank_fixed']
        
        # マージ
        merge_cols = ['horse_id', 'race_date', 
                      'jockey_win_rate_lf', 'trainer_win_rate_lf',
                      'gap_jockey_pop_lf', 'gap_trainer_pop_lf']
        
        if 'jockey_venue_win_rate_lf' in perf.columns:
            merge_cols.append('jockey_venue_win_rate_lf')
        if 'jockey_surface_win_rate_lf' in perf.columns:
            merge_cols.append('jockey_surface_win_rate_lf')
        if 'sire_win_rate_fixed' in perf.columns:
            merge_cols.extend(['sire_win_rate_fixed', 'gap_sire_pop_fixed'])
        
        merge_df = perf[merge_cols].drop_duplicates(subset=['horse_id', 'race_date'], keep='last')
        result = df.merge(merge_df, on=['horse_id', 'race_date'], how='left')
        
        logging.info(f"  生成完了: {len(merge_cols) - 2}個のリークフリー特徴量")
        
        return result
    
    def remove_leak_features(self, features):
        """リーク版特徴量を除外"""
        clean_features = [f for f in features if f not in self.LEAK_PATTERNS]
        removed = len(features) - len(clean_features)
        logging.info(f"  リーク版特徴量を除外: {removed}件")
        return clean_features
    
    def prepare_features(self, df):
        """特徴量リストを準備（リーク版除外）"""
        with open(self.base_model_dir / 'feature_names.json', 'r', encoding='utf-8') as f:
            base_features = json.load(f)
        
        # リーク版を除外
        base_features = self.remove_leak_features(base_features)
        
        # リークフリー版を追加
        new_features = [
            'jockey_win_rate_lf', 'trainer_win_rate_lf',
            'gap_jockey_pop_lf', 'gap_trainer_pop_lf',
            'jockey_venue_win_rate_lf', 'jockey_surface_win_rate_lf',
            'sire_win_rate_fixed', 'gap_sire_pop_fixed',
        ]
        
        available = [f for f in base_features + new_features if f in df.columns]
        available = list(dict.fromkeys(available))
        
        logging.info(f"特徴量数（リークフリー）: {len(available)}")
        return available
    
    def prepare_target(self, df):
        """ターゲット準備"""
        df['win_odds'] = pd.to_numeric(df['win_odds'], errors='coerce')
        df['popularity'] = pd.to_numeric(df['popularity'], errors='coerce')
        df['finish_position'] = pd.to_numeric(df['finish_position'], errors='coerce')
        
        odds = df['win_odds'].fillna(1.0).clip(upper=90)
        log_odds = np.log1p(odds)
        finish_pos = df['finish_position'].fillna(99)
        
        gain = np.zeros(len(df))
        gain[finish_pos == 1] = np.nan_to_num(log_odds[finish_pos == 1] * 12.74, nan=0)
        gain[finish_pos == 2] = np.nan_to_num(log_odds[finish_pos == 2] * 6.73, nan=0)
        gain[finish_pos == 3] = np.nan_to_num(log_odds[finish_pos == 3] * 3.69, nan=0)
        df['target_relevance'] = np.nan_to_num(gain, nan=0).astype(int)
        
        # サンプル重み（穴馬ブースト控えめに）
        base_weight = np.log1p(odds).clip(upper=np.log1p(100))
        df['sample_weight'] = np.nan_to_num(base_weight, nan=1.0)
        
        return df
    
    def train(self, df, feature_cols, n_trials=25):
        """モデル学習（強正則化）"""
        logging.info("=" * 60)
        logging.info("μモデル v10.1 学習開始")
        logging.info("★★★ 完全リークフリー + 強正則化 ★★★")
        logging.info("=" * 60)
        
        train_df = df[df['race_date'] < '2023-01-01'].copy()
        valid_df = df[(df['race_date'] >= '2023-01-01') & (df['race_date'] < '2024-01-01')].copy()
        test_df = df[df['race_date'] >= '2024-01-01'].copy()
        
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
            reg = self.REGULARIZATION_RANGES
            params = {
                'objective': 'lambdarank', 'metric': 'ndcg', 'eval_at': [1, 3, 5],
                'boosting_type': 'gbdt', 'verbose': -1, 'random_state': 42,
                'label_gain': list(range(100)),
                'num_leaves': trial.suggest_int('num_leaves', *reg['num_leaves']),
                'lambda_l1': trial.suggest_float('lambda_l1', *reg['lambda_l1'], log=True),
                'lambda_l2': trial.suggest_float('lambda_l2', *reg['lambda_l2'], log=True),
                'min_child_samples': trial.suggest_int('min_child_samples', *reg['min_child_samples']),
                'learning_rate': trial.suggest_float('learning_rate', *reg['learning_rate'], log=True),
                'feature_fraction': trial.suggest_float('feature_fraction', *reg['feature_fraction']),
                'bagging_fraction': trial.suggest_float('bagging_fraction', *reg['bagging_fraction']),
                'bagging_freq': trial.suggest_int('bagging_freq', 1, 7),
                'max_depth': trial.suggest_int('max_depth', *reg['max_depth']),
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
        
        logging.info(f"Optuna {n_trials}トライアル（強正則化範囲）...")
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
        gap = abs(valid_roi - test_roi)
        
        importance = dict(zip(feature_cols, self.model.feature_importances_))
        sorted_imp = sorted(importance.items(), key=lambda x: x[1], reverse=True)
        
        logging.info("=" * 60)
        logging.info("【最終結果: μ v10.1（完全リークフリー）】")
        logging.info("=" * 60)
        logging.info(f"  Valid ROI: {valid_roi:.2%}")
        logging.info(f"  Test ROI: {test_roi:.2%}")
        logging.info(f"  Valid-Test差: {gap:.2%}")
        logging.info(f"  過学習判定: {'✅ OK' if gap < 0.05 else '⚠️ 要注意' if gap < 0.10 else '🚨 過学習'}")
        logging.info("")
        logging.info("【Top 15特徴量】")
        for i, (name, imp) in enumerate(sorted_imp[:15], 1):
            leak_status = '✅' if 'lf' in name or 'fixed' in name or 'past_' in name else '❓'
            logging.info(f"  {i:2}. {leak_status} {name}: {imp}")
        
        self.save_model(feature_cols, valid_roi, test_roi, gap, best_params)
        return valid_roi, test_roi, gap
    
    def save_model(self, features, valid_roi, test_roi, gap, best_params):
        """モデル保存"""
        with open(self.output_dir / 'mu_v10_1_ranker.pkl', 'wb') as f:
            pickle.dump(self.model, f)
        with open(self.output_dir / 'feature_names.json', 'w', encoding='utf-8') as f:
            json.dump(features, f, ensure_ascii=False, indent=2)
        
        save_params = {k: v for k, v in best_params.items() if k != 'label_gain'}
        metadata = {
            'version': 'v10.1',
            'description': '完全リークフリー版（騎手/調教師: expanding.shift(1), 血統: Train固定）',
            'valid_roi': float(valid_roi),
            'test_roi': float(test_roi),
            'valid_test_gap': float(gap),
            'leak_free': True,
            'complete_leak_free': True,
            'regularization': 'strong',
            'best_params': save_params,
            'created_at': datetime.now().isoformat()
        }
        with open(self.output_dir / 'model_info.json', 'w', encoding='utf-8') as f:
            json.dump(metadata, f, ensure_ascii=False, indent=2)
        
        logging.info(f"保存完了: {self.output_dir}")
    
    def run(self, n_trials=25):
        """メイン実行"""
        train_data, races_df = self.load_data()
        train_data = self.generate_leak_free_features(train_data, races_df)
        train_data = self.prepare_target(train_data)
        feature_cols = self.prepare_features(train_data)
        return self.train(train_data, feature_cols, n_trials)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description='μモデル v10.1 学習（完全リークフリー）')
    parser.add_argument('--trials', type=int, default=25, help='Optunaトライアル数')
    args = parser.parse_args()
    
    trainer = MuV101TrainerLeakFree()
    valid_roi, test_roi, gap = trainer.run(n_trials=args.trials)
    
    print("\n" + "=" * 60)
    print("【v10.1 結果サマリー】")
    print("=" * 60)
    print(f"  Valid ROI: {valid_roi:.2%}")
    print(f"  Test ROI:  {test_roi:.2%}")
    print(f"  Valid-Test差: {gap:.2%}")
    print(f"  v5.4比:    {test_roi - 0.8005:+.2%}")
    print(f"  過学習判定: {'✅ OK' if gap < 0.05 else '⚠️' if gap < 0.10 else '🚨'}")
