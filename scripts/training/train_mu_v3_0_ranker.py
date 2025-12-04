#!/usr/bin/env python3
"""
μv3.3 モデル学習スクリプト (train_mu_v3_0_ranker.py)

【変更点 from v3.2 → v3.3 Phase 1】
- オッズクリッピング最適化: Optunaで50-100倍の範囲を探索
- 超穴馬除外: 100倍超の馬を学習から除外（再現性がないため）
- ターゲット重み最適化: 1着・2着・3着の重みをOptunaで最適化
- 馬体重特徴量整理: 冗長な特徴量を除外（horse_weight_diff_from_avg, weight_diff_from_avg）

【v3.2の機能（継承）】
- ROI最大化特化 (ROI Maximization Mode)
- オッズ加重学習 (Odds-Weighted Training)
- Gap Features (実力と人気の乖離)

【目標】
- CV ROI: 84.95% → 95%+
- Test ROI: 77.54% → 85%+
"""

import sys
from pathlib import Path

# プロジェクトルートをsys.pathに追加
project_root = Path(__file__).resolve().parent.parent.parent
sys.path.append(str(project_root))
import pandas as pd
import numpy as np
import lightgbm as lgb
import optuna
import logging
import pickle
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
from datetime import datetime
from dateutil.relativedelta import relativedelta
from typing import Tuple, List, Dict, Optional
from sklearn.metrics import ndcg_score
import gc

# プロジェクト内のモジュールをインポート
from keibaai.src.features.feature_engine import FeatureEngine

# ログ設定
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('train_mu_v3_3.log')
    ]
)

def calculate_roi(df, preds):
    """ROIを計算する"""
    df = df.copy()
    df['score'] = preds
    df['rank_pred'] = df.groupby('race_id')['score'].rank(ascending=False, method='first')
    
    bet_df = df[df['rank_pred'] == 1]
    hits = bet_df[bet_df['finish_position'] == 1]
    return_amount = hits['win_odds'].sum()
    bet_amount = len(bet_df)
    roi = return_amount / bet_amount if bet_amount > 0 else 0
    return roi

class MuV3RankerTrainer:
    def __init__(self, data_dir: str = 'keibaai/data/parsed/parquet'):
        self.data_dir = Path(data_dir)
        # v3.3用のディレクトリ
        self.models_dir = Path('keibaai/models/mu_v3_3')
        self.models_dir.mkdir(parents=True, exist_ok=True)
        
    def load_data(self) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        """必要なデータを読み込む (v2.8と同様)"""
        logging.info("データを読み込んでいます...")
        
        # 1. 成績データ
        perf_path = self.data_dir / 'horses_performance/horses_performance_fixed.parquet'
        if not perf_path.exists():
            perf_path = self.data_dir / 'horses_performance/horses_performance.parquet'
            
        if not perf_path.exists():
            raise FileNotFoundError(f"{perf_path} が見つかりません。")
        
        df = pd.read_parquet(perf_path)
        df['race_date'] = pd.to_datetime(df['race_date'])
        
        # 2. レース情報補完
        races_path = self.data_dir / 'races/races.parquet'
        if races_path.exists():
            races_df = pd.read_parquet(races_path)
            cols_to_add = [
                'round_of_year', 'day_of_meeting', 'track_condition', 'venue', 'track_surface', 
                'win_odds', 'race_class', 'race_name', 'popularity',
                # 過去走集計用（学習時はexclude_colsで除外）
                'margin_seconds', 'finish_time_seconds', 'last_3f_time',
                'passing_order_1', 'passing_order_2', 'passing_order_3', 'passing_order_4'
            ]
            cols_to_merge = [c for c in cols_to_add if c not in df.columns]
            
            if cols_to_merge:
                # races.parquet は race_id + horse_id レベルでデータを持つため、
                # (race_id, horse_id) でマージして重複を防ぐ
                merge_keys = ['race_id', 'horse_id']
                merge_cols = merge_keys + cols_to_merge
                
                # races_df から必要な列を選択し、重複排除
                races_select = races_df[merge_cols].drop_duplicates(subset=merge_keys)
                
                # (race_id, horse_id) でマージ
                df = df.merge(races_select, on=merge_keys, how='left')
                logging.info(f"races.parquet からマージ: {len(cols_to_merge)} columns")
        


        # 4. 血統データ
        ped_path = self.data_dir / 'pedigrees/pedigrees.parquet'
        ped_df = pd.read_parquet(ped_path) if ped_path.exists() else pd.DataFrame()
        
        # 血統ID抽出 (簡易実装: 既にv2.8で実装済みなら同様のロジック)
        if not ped_df.empty and 'sire_id' not in df.columns:
             # ここでは簡易的に省略（必要ならv2.8からコピー）
             pass

        # 4. 馬プロファイル
        prof_path = self.data_dir / 'horses/horses.parquet'
        prof_df = pd.read_parquet(prof_path) if prof_path.exists() else pd.DataFrame()
        
        logging.info(f"データ読み込み完了: {len(df):,} レース結果")
        
        # 重複排除 (Critical Fix for v3.2)
        before_len = len(df)
        df = df.drop_duplicates(subset=['race_id', 'horse_id'])
        logging.info(f"重複排除: {before_len:,} -> {len(df):,} rows")
        
        return df, ped_df, prof_df

    def generate_rolling_features(self, df: pd.DataFrame, ped_df: pd.DataFrame, prof_df: pd.DataFrame, dry_run: bool = False) -> pd.DataFrame:
        """特徴量生成 (v2.8ベース + 新特徴量)"""
        logging.info("=" * 50)
        logging.info("ローリング特徴量生成を開始します (v3.2 ROI Maximization Mode)")
        logging.info("=" * 50)
        
        config_path = Path('keibaai/configs/features.yaml')
        engine = FeatureEngine(config_path)
        
        start_date = df['race_date'].min()
        end_date = df['race_date'].max()
        training_start_date = pd.Timestamp('2020-01-01')
        
        if dry_run:
            training_start_date = pd.Timestamp('2023-01-01') # Dry Runは期間短縮
            
        if start_date > training_start_date:
            training_start_date = start_date
            
        current_date = training_start_date.replace(day=1)
        feature_dfs = []
        
        while current_date <= end_date:
            next_month = current_date + relativedelta(months=1)
            
            target_mask = (df['race_date'] >= current_date) & (df['race_date'] < next_month)
            target_df = df[target_mask].copy()
            
            if target_df.empty:
                current_date = next_month
                continue
                
            history_mask = (df['race_date'] < current_date)
            history_df = df[history_mask].copy()
            
            logging.info(f"[{current_date.strftime('%Y-%m')}] Target: {len(target_df):5,}件")
            
            try:
                # FeatureEngine内部で roi_features.py が呼ばれ、新特徴量も付与される
                features = engine.generate_features(
                    shutuba_df=target_df,
                    results_history_df=history_df,
                    horse_profiles_df=prof_df,
                    pedigree_df=ped_df
                )
                
                # 必要なカラムをマージ
                # v3.2: popularity を追加 (Gap Feature用)
                cols_to_keep = ['race_id', 'horse_id', 'finish_position', 'race_date', 'win_odds', 'popularity']
                cols_to_merge = [c for c in cols_to_keep if c not in features.columns and c in target_df.columns]
                if cols_to_merge:
                    # Join keys must be included in the right DataFrame
                    right_cols = list(set(cols_to_merge + ['race_id', 'horse_id']))
                    features = features.merge(target_df[right_cols], on=['race_id', 'horse_id'], how='left')
                
                # マージ後の重複排除（念のため）
                before_dedup = len(features)
                features = features.drop_duplicates(subset=['race_id', 'horse_id'])
                if before_dedup != len(features):
                    logging.warning(f"[{current_date.strftime('%Y-%m')}] マージ後の重複を削除: {before_dedup} -> {len(features)}")
                
                feature_dfs.append(features)
                
            except Exception as e:
                logging.error(f"Error in {current_date.strftime('%Y-%m')}: {e}")
            
            if dry_run:
                break
            current_date = next_month
            
        if not feature_dfs:
            raise ValueError("特徴量が生成されませんでした。")
            
        full_df = pd.concat(feature_dfs, ignore_index=True)
        
        if not dry_run:
            save_path = self.models_dir / 'train_data_mu_v3_3.parquet'
            full_df.to_parquet(save_path)
            logging.info(f"学習データを保存しました: {save_path}")
            
        return full_df

    def train_model(self, df: pd.DataFrame, n_trials: int = 50, dry_run: bool = False):
        """LGBMRankerによる学習（v3.3 Phase 1改修版）"""
        logging.info("=" * 50)
        logging.info("モデル学習を開始します (v3.3 Phase 1: ROI Maximization + Optimization)")
        logging.info("=" * 50)
        
        # v3.3 Phase 1: ターゲット変数生成はobjective関数内に移動
        # （オッズクリッピングと重みを最適化するため）
        logging.info("v3.3: ターゲット変数はOptuna内で最適化されます")
        
        # ★ CRITICAL FIX (Bug #3): Test Run & Feature Selectionのため、
        # デフォルトパラメータでターゲット変数を事前生成
        logging.info("=" * 50)
        logging.info("デフォルトパラメータでターゲット変数を生成中...")
        logging.info("=" * 50)
        
        default_weight_1st = 10.0
        default_weight_2nd = 3.0
        default_weight_3rd = 1.0
        default_odds_clip = 100
        
        # ターゲット変数生成（v3.3ロジック）
        odds = df['win_odds'].fillna(1.0)
        predictable_mask = odds <= 100.0  # 100倍超除外
        odds_clipped = odds.clip(upper=default_odds_clip)
        log_odds = np.log1p(odds_clipped)
        
        gain = np.zeros(len(df))
        
        # 1着
        mask_1st = predictable_mask & (df['finish_position'] == 1)
        gain[mask_1st] = log_odds[mask_1st] * default_weight_1st
        
        # 2着
        mask_2nd = predictable_mask & (df['finish_position'] == 2)
        gain[mask_2nd] = log_odds[mask_2nd] * default_weight_2nd
        
        # 3着
        mask_3rd = predictable_mask & (df['finish_position'] == 3)
        gain[mask_3rd] = log_odds[mask_3rd] * default_weight_3rd
        
        df['target_gain'] = gain
        df['target_relevance'] = df['target_gain'].astype(int)
        
        logging.info(f"デフォルトターゲット変数生成完了: gain range=[{gain.min():.2f}, {gain.max():.2f}]")
        logging.info(f"  100倍超除外: {(~predictable_mask).sum():,} 頭")
        logging.info("=" * 50)
        
        # --- 3.3 割安感特徴量の追加 (Value-Gap Features) ---
        logging.info("Gap Featuresを生成中...")
        
        # 実力ランクの推定 (過去5走平均着順を使用)
        ability_col = 'past_5_finish_position_mean'
        
        if ability_col not in df.columns:
            logging.warning(f"Ability column '{ability_col}' not found. Skipping Gap Feature.")
            ability_col = None
        
        # ★ CRITICAL FIX (Bug #4): popularity（確定人気）を使用
        # ユーザー方針: 学習時は最終人気を使用し、運用時は直前人気を使用する
        if ability_col and 'popularity' in df.columns:
            # 実力ランク（小さいほど強い）
            df['ability_rank'] = df.groupby('race_id')[ability_col].rank(
                ascending=True, method='first'
            )
            
            # ギャップ = 人気順位 - 実力順位
            # プラスが大きいほど「実力の割に人気がない」= 割安な穴馬候補
            df['gap_ability_popularity'] = df['popularity'] - df['ability_rank']
            
            logging.info(f"✅ Gap Feature 'gap_ability_popularity' created")
            logging.info(f"   使用カラム: popularity (最終人気) + {ability_col}")
            logging.info(f"   Note: 運用時は直前人気を使用すること")
        else:
            logging.warning("Gap Feature skipped: popularity or ability column not found.")
        
        # --- ターゲット変数の生成 (Feature Selection用) ---
        # Note: 本番学習時の重みとは異なる可能性があるが、特徴量選抜には十分
        weight_1st = 10.0
        weight_2nd = 5.0
        weight_3rd = 2.0
        odds_clip_upper = 100
        
        odds = df['win_odds'].fillna(1.0)
        predictable_mask = odds <= 100.0
        odds_clipped = odds.clip(upper=odds_clip_upper)
        log_odds = np.log1p(odds_clipped)
        
        gain = np.zeros(len(df))
        mask_1st = predictable_mask & (df['finish_position'] == 1)
        gain[mask_1st] = log_odds[mask_1st] * weight_1st
        mask_2nd = predictable_mask & (df['finish_position'] == 2)
        gain[mask_2nd] = log_odds[mask_2nd] * weight_2nd
        mask_3rd = predictable_mask & (df['finish_position'] == 3)
        gain[mask_3rd] = log_odds[mask_3rd] * weight_3rd
        
        df['target_gain'] = gain
        df['target_relevance'] = df['target_gain'].astype(int)
        

        # --- 3.1 オッズ加重学習 (Odds-Weighted Training) ---
        # サンプルウェイトの作成
        # log1p(odds) を使用、上限を100倍オッズ相当にクリップ
        df['sample_weight'] = np.log1p(df['win_odds'].fillna(1.0)).clip(upper=np.log1p(100))
        
        # ソート必須 (race_id単位でグループ化するため)
        df = df.sort_values('race_id')
        logging.info(f"DEBUG: df shape before split: {df.shape}")
        
        # データ分割
        if n_trials <= 2 or dry_run:
            # Dry Run
            unique_races = df['race_id'].unique()
            n_races = len(unique_races)
            split_idx = int(n_races * 0.8)
            train_races = unique_races[:split_idx]
            valid_races = unique_races[split_idx:]
            
            train_df = df[df['race_id'].isin(train_races)]
            valid_df = df[df['race_id'].isin(valid_races)]
            test_df = valid_df
            logging.info(f"DEBUG: Dry run split. Train: {train_df.shape}, Valid: {valid_df.shape}")
        else:
            train_mask = df['race_date'] < '2023-01-01'
            valid_mask = (df['race_date'] >= '2023-01-01') & (df['race_date'] < '2024-01-01')
            test_mask = df['race_date'] >= '2024-01-01'
            
            train_df = df[train_mask]
            valid_df = df[valid_mask]
            test_df = df[test_mask]
            
        # グループ情報の作成
        def get_group(d):
            return d.groupby('race_id').size().to_list()
            
        group_train = get_group(train_df)
        group_valid = get_group(valid_df)
        group_test = get_group(test_df)
        
        # 特徴量カラム
        exclude_cols = [
            'race_id', 'horse_id', 'race_date', 'finish_position', 'target_gain', 'target_relevance',
            'win_odds', 'jockey_id', 'trainer_id', 'owner_id', 'sire_id', 'damsire_id',
            'race_name', 'horse_name', 'jockey_name', 'trainer_name',
            'sample_weight', 'ability_rank', # 中間変数は除外
            
            # v3.3 Phase 1 改修③: 馬体重関連の冗長特徴量を除外
            'horse_weight_diff_from_avg',  # horse_weight_zscoreと冗長
            'weight_diff_from_avg',        # horse_weight_zscoreと冗長
            # 保持: horse_weight_zscore, basis_weight_zscore, horse_weight_change
            
            # Feature Selection (v3.1): Remove raw weight features
            'horse_weight', 'basis_weight',
            
            # Leakage（データリーク防止）
            'finish_time_seconds', 'margin_seconds', 'prize_money', 'popularity',
            'odds', 'finish_time_str', 'margin_str', 'last_3f_time', 'passing_order',
            'final_corner_to_finish', 'passing_order_1', 'passing_order_2', 
            'passing_order_3', 'passing_order_4', 'position_change_1_2', 
            'scratched', 'time_except_last3f', 'win_probability', 'pace_index',
            'last3f_rank', 'popularity_finish_diff', # ★ 追加: 未来情報リーク
            'position_change_2_3', 'position_change_3_4', 'relative_odds',
            'running_style', 'time_deviation', # ★ 追加: 潜在的リーク
            'combo_avg_popularity' # ★ 追加: リーク検出
        ]
        feature_cols = [c for c in df.columns if c not in exclude_cols]
        
        # --- 3. Feature Selection (Stepwise) ---
        logging.info("Starting Stepwise Feature Selection...")
        
        # 一時的な学習用データセット作成
        lgb_train_fs = lgb.Dataset(train_df[feature_cols], label=train_df['target_relevance'], group=group_train)
        
        # 固定パラメータで学習（高速化のため）
        params_fs = {
            'objective': 'lambdarank',
            'metric': 'ndcg',
            'ndcg_eval_at': [1, 3, 5],
            'boosting_type': 'gbdt',
            'num_leaves': 31,
            'learning_rate': 0.1,
            'feature_fraction': 0.8,
            'bagging_fraction': 0.8,
            'bagging_freq': 5,
            'verbose': -1,
            'random_state': 42,
            'label_gain': [i for i in range(100)] # 【追加】
        }
        
        model_fs = lgb.train(
            params_fs,
            lgb_train_fs,
            num_boost_round=100, # 少なく設定
        )
        
        # 重要度取得
        importance = model_fs.feature_importance(importance_type='gain')
        feature_imp = pd.DataFrame({'feature': feature_cols, 'importance': importance})
        feature_imp = feature_imp.sort_values('importance', ascending=False)
        
        # 重要度が0の特徴量を削除
        zero_imp_features = feature_imp[feature_imp['importance'] == 0]['feature'].tolist()
        
        # さらに下位20%を削除（ただし最低100個は残す）
        n_features = len(feature_cols)
        n_keep = max(100, int(n_features * 0.8))
        low_imp_features = feature_imp.iloc[n_keep:]['feature'].tolist()
        
        remove_features = list(set(zero_imp_features + low_imp_features))
        
        logging.info(f"Feature Selection: Removing {len(remove_features)} features.")
        logging.info(f"Removed features: {remove_features}")
        
        # exclude_colsに追加
        exclude_cols.extend(remove_features)
        
        # 特徴量リストを更新
        feature_cols = [c for c in df.columns if c not in exclude_cols and pd.api.types.is_numeric_dtype(df[c])]
        logging.info(f"Selected {len(feature_cols)} features for final training.")
        
        logging.info(f"Train: {len(train_df):,} rows, {len(group_train):,} races")
        logging.info(f"Valid: {len(valid_df):,} rows, {len(group_valid):,} races")
        

        
        # カテゴリ変換
        for col in feature_cols:
            if df[col].dtype == 'object':
                df[col] = df[col].astype('category')
                train_df[col] = train_df[col].astype('category')
                valid_df[col] = valid_df[col].astype('category')
                test_df[col] = test_df[col].astype('category')

        logging.info(f"使用特徴量: {len(feature_cols)}個")
        if 'gap_ability_popularity' in feature_cols:
            logging.info("✅ gap_ability_popularity is included in features.")
        else:
            logging.warning("⚠️ gap_ability_popularity is NOT included in features.")
        
        # Test Run
        logging.info("--- Test Run Start ---")
        try:
            test_model = lgb.LGBMRanker(objective='lambdarank', metric='ndcg', n_estimators=10, verbosity=1, label_gain=[i for i in range(100)])
            test_model.fit(
                train_df[feature_cols],
                train_df['target_relevance'],
                group=group_train,
                sample_weight=train_df['sample_weight'], # 【追加】学習データの重み
                eval_set=[(valid_df[feature_cols], valid_df['target_relevance'])],
                eval_group=[group_valid],
                eval_sample_weight=[valid_df['sample_weight']] # 【追加】検証データの重み
            )
            logging.info("Test Run Success!")
        except Exception as e:
            logging.error(f"Test Run Failed: {e}")
            raise e
        logging.info("--- Test Run End ---")

        # v3.3 Phase 1: Optuna Tuning（改修①②統合）
        def objective(trial):
            try:
                # --- v3.3 改修② : ターゲット重みの最適化 ---
                weight_1st = trial.suggest_float('weight_1st', 8.0, 15.0)
                weight_2nd = trial.suggest_float('weight_2nd', 2.0, 8.0)
                weight_3rd = trial.suggest_float('weight_3rd', 0.5, 4.0)
                
                # --- v3.3 改修①: オッズクリッピングの最適化 ---
                odds_clip_upper = trial.suggest_int('odds_clip_upper', 50, 100, step=10)
                
                # ★ CRITICAL FIX: メモリ効率化
                # 全カラムコピーではなく、必要なカラムのみコピー
                required_cols = list(set(feature_cols + ['win_odds', 'finish_position', 'race_date', 'race_id', 'sample_weight']))
                df_trial = df[required_cols].copy()
                
                # --- ターゲット変数生成（v3.3で改修）---
                odds = df_trial['win_odds'].fillna(1.0)
                
                # ★ v3.3 改修①: 超穴馬（100倍超）を学習から除外
                predictable_mask = odds <= 100.0
                
                # クリッピング適用
                odds_clipped = odds.clip(upper=odds_clip_upper)
                log_odds = np.log1p(odds_clipped)
                
                gain = np.zeros(len(df_trial))
                
                # ★ CRITICAL: 100倍以下の馬のみをターゲットに含める
                # 1着
                mask_1st = predictable_mask & (df_trial['finish_position'] == 1)
                gain[mask_1st] = log_odds[mask_1st] * weight_1st
                
                # 2着
                mask_2nd = predictable_mask & (df_trial['finish_position'] == 2)
                gain[mask_2nd] = log_odds[mask_2nd] * weight_2nd
                
                # 3着
                mask_3rd = predictable_mask & (df_trial['finish_position'] == 3)
                gain[mask_3rd] = log_odds[mask_3rd] * weight_3rd
                
                # 100倍超の馬はgain=0のまま（学習対象外）
                
                df_trial['target_gain'] = gain
                df_trial['target_relevance'] = df_trial['target_gain'].astype(int)
                
                # LightGBMパラメータ
                params = {
                    'objective': 'lambdarank',
                    'metric': 'ndcg',
                    'eval_at': [1, 3, 5],
                    'verbosity': -1,
                    'boosting_type': 'gbdt',
                    'lambda_l1': trial.suggest_float('lambda_l1', 1.0, 10.0, log=True),
                    'lambda_l2': trial.suggest_float('lambda_l2', 1.0, 10.0, log=True),
                    'num_leaves': trial.suggest_int('num_leaves', 31, 63),
                    'feature_fraction': trial.suggest_float('feature_fraction', 0.4, 0.8),
                    'bagging_fraction': trial.suggest_float('bagging_fraction', 0.4, 1.0),
                    'bagging_freq': trial.suggest_int('bagging_freq', 1, 7),
                    'min_child_samples': trial.suggest_int('min_child_samples', 100, 500),
                    'learning_rate': trial.suggest_float('learning_rate', 0.01, 0.3, log=True),
                    'label_gain': [i for i in range(100)]
                }
                
                # Time-Series Cross-Validation (3 Folds)
                folds = [
                    ('2020-01-01', '2022-01-01', '2023-01-01'),
                    ('2020-01-01', '2023-01-01', '2024-01-01'),
                    ('2020-01-01', '2023-07-01', '2024-01-01')
                ]
                
                rois = []
                
                for train_start, valid_start, valid_end in folds:
                    cv_train_mask = (df_trial['race_date'] >= train_start) & (df_trial['race_date'] < valid_start)
                    cv_valid_mask = (df_trial['race_date'] >= valid_start) & (df_trial['race_date'] < valid_end)
                    
                    cv_train_df = df_trial[cv_train_mask].copy()
                    cv_valid_df = df_trial[cv_valid_mask].copy()
                    
                    if cv_train_df.empty or cv_valid_df.empty:
                        continue
                        
                    cv_group_train = get_group(cv_train_df)
                    cv_group_valid = get_group(cv_valid_df)
                    
                    model = lgb.LGBMRanker(**params, n_estimators=1000)
                    
                    model.fit(
                        cv_train_df[feature_cols],
                        cv_train_df['target_relevance'],
                        group=cv_group_train,
                        sample_weight=cv_train_df['sample_weight'],
                        eval_set=[(cv_valid_df[feature_cols], cv_valid_df['target_relevance'])],
                        eval_group=[cv_group_valid],
                        eval_sample_weight=[cv_valid_df['sample_weight']],
                        eval_metric='ndcg',
                        callbacks=[lgb.early_stopping(stopping_rounds=50, verbose=False)]
                    )
                    
                    # Evaluate ROI
                    preds = model.predict(cv_valid_df[feature_cols])
                    cv_valid_df_copy = cv_valid_df.copy()
                    cv_valid_df_copy['score'] = preds
                    cv_valid_df_copy['rank_pred'] = cv_valid_df_copy.groupby('race_id')['score'].rank(ascending=False, method='first')
                    
                    roi = calculate_roi(cv_valid_df_copy, preds)
                    rois.append(roi)
                    
                    # メモリ解放
                    del cv_train_df, cv_valid_df, cv_valid_df_copy, model
                    gc.collect()
                
                avg_roi = np.mean(rois)
                logging.info(f"Trial {trial.number}: ROI={avg_roi*100:.2f}%")
                
                # メモリ解放
                del df_trial
                gc.collect()
                
                return avg_roi
                
            except Exception as e:
                logging.error(f"Trial {trial.number} Failed: {e}")
                import traceback
                logging.error(traceback.format_exc())
                raise optuna.exceptions.TrialPruned()

        if dry_run:
            logging.info("Dry Run: Skipping Optuna optimization.")
            best_params = {
                'weight_1st': default_weight_1st,
                'weight_2nd': default_weight_2nd,
                'weight_3rd': default_weight_3rd,
                'odds_clip_upper': default_odds_clip
            }
        else:
            study = optuna.create_study(direction='maximize')
            study.optimize(objective, n_trials=n_trials)
            best_params = study.best_params
            
            logging.info("=" * 50)
            logging.info(f"Best Params: {best_params}")
            logging.info("=" * 50)
        
        # --- 最終学習 (Best Params) ---
        logging.info("最終学習を開始します...")
        
        # ベストパラメータでターゲット変数を再生成
        weight_1st = best_params.get('weight_1st', default_weight_1st)
        weight_2nd = best_params.get('weight_2nd', default_weight_2nd)
        weight_3rd = best_params.get('weight_3rd', default_weight_3rd)
        odds_clip_upper = int(best_params.get('odds_clip_upper', default_odds_clip))
        
        # ターゲット変数生成（objective関数と同じロジック）
        odds = df['win_odds'].fillna(1.0)
        predictable_mask = odds <= 100.0
        odds_clipped = odds.clip(upper=odds_clip_upper)
        log_odds = np.log1p(odds_clipped)
        
        gain = np.zeros(len(df))
        
        # 1着
        mask_1st = predictable_mask & (df['finish_position'] == 1)
        gain[mask_1st] = log_odds[mask_1st] * weight_1st
        # weight系とodds_clip_upperを除外（LightGBMのパラメータではない）
        lgb_params = {k: v for k, v in best_params.items() 
                      if k not in ['weight_1st', 'weight_2nd', 'weight_3rd', 'odds_clip_upper']}
        lgb_params.update({
            'objective': 'lambdarank',
            'metric': 'ndcg',
            'eval_at': [1, 3, 5],
            'label_gain': [i for i in range(100)]
        })
        
        model = lgb.LGBMRanker(**lgb_params, n_estimators=2000)
        model.fit(
            train_df[feature_cols],
            train_df['target_relevance'],
            group=group_train,
            sample_weight=train_df['sample_weight'],
            eval_set=[(valid_df[feature_cols], valid_df['target_relevance'])],
            eval_group=[group_valid],
            eval_sample_weight=[valid_df['sample_weight']],
            eval_metric='ndcg'
        )
        
        # 保存
        with open(self.models_dir / 'mu_v3_3_ranker.pkl', 'wb') as f:
            pickle.dump(model, f)
            
        # 特徴量リスト保存
        with open(self.models_dir / 'feature_names.json', 'w') as f:
            import json
            json.dump(feature_cols, f)
            
        logging.info("モデル保存完了")
        
        # 評価
        self.evaluate_model(model, test_df, feature_cols)
        
    def evaluate_model(self, model, test_df, feature_cols):
        """評価"""
        logging.info("=" * 50)
        logging.info("モデル評価 (Test Data)")
        logging.info("=" * 50)
        
        preds = model.predict(test_df[feature_cols])
        test_df = test_df.copy()
        test_df['score'] = preds
        test_df['rank_pred'] = test_df.groupby('race_id')['score'].rank(ascending=False, method='first')
        
        # Top 1 Metrics
        top1 = test_df[test_df['rank_pred'] == 1]
        hits = top1[top1['finish_position'] == 1]
        accuracy = len(hits) / len(top1)
        roi = hits['win_odds'].sum() / len(top1)
        
        logging.info(f"Top 1 Accuracy: {accuracy:.2%}")
        logging.info(f"Top 1 ROI: {roi:.2%}")
        
        # Top 5 Recall (勝ち馬がTop 5に含まれている確率)
        winners = test_df[test_df['finish_position'] == 1]
        winners_in_top5 = winners[winners['rank_pred'] <= 5]
        recall = len(winners_in_top5) / len(winners)
        
        logging.info(f"Top 5 Recall: {recall:.2%}")
        
        # Feature Importance
        imp = pd.DataFrame({
            'feature': feature_cols,
            'importance': model.feature_importances_
        }).sort_values('importance', ascending=False)
        
        logging.info("\nFeature Importance (Top 20):")
        logging.info(imp.head(20).to_string())
        imp.to_csv(self.models_dir / 'feature_importance.csv', index=False)

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--n_trials', type=int, default=50)
    parser.add_argument('--dry-run', action='store_true')
    args = parser.parse_args()
    
    trainer = MuV3RankerTrainer()
    df, ped_df, prof_df = trainer.load_data()
    
    train_data_path = trainer.models_dir / 'train_data_mu_v3_3.parquet'
    if train_data_path.exists() and not args.dry_run:
        full_df = pd.read_parquet(train_data_path)
        logging.info(f"既存の学習データを読み込みました: {train_data_path}")
    else:
        logging.info("学習データを新規生成します...")
        full_df = trainer.generate_rolling_features(df, ped_df, prof_df, dry_run=args.dry_run)
        
    trainer.train_model(full_df, n_trials=args.n_trials, dry_run=args.dry_run)
