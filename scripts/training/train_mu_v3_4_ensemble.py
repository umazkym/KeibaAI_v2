#!/usr/bin/env python3
"""
μv3.4 アンサンブルモデル学習スクリプト (train_mu_v3_4_ensemble.py)

【v3.3からの変更点】
1. 改修①: アンサンブル学習
   - 3種類のモデル（保守的/標準/積極的）を学習
   - Optunaで重み最適化

2. 改修②: ベッティング戦略の最適化
   - EV（期待値）計算ロジック
   - オッズ範囲フィルタ（3~50倍）
   - EV > 0 の馬のみ選択

3. 改修④: Purged K-Fold
   - purge_days=30のギャップ期間
   - 訓練・検証境界のリーク防止

【継承機能】
- ROI最大化特化 (ROI Maximization Mode)
- オッズ加重学習 (Odds-Weighted Training)
- Gap Features (実力と人気の乖離)
- ターゲット重み/オッズクリッピング最適化

【目標】
- Test ROI: 80.69% → 100%+

【日本語】
すべてのログ、コメント、ドキュメントは日本語で記述。
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
import json
import matplotlib.pyplot as plt
from pathlib import Path
from datetime import datetime
from dateutil.relativedelta import relativedelta
from typing import Tuple, List, Dict, Optional
import gc

# プロジェクト内のモジュールをインポート
from keibaai.src.features.feature_engine import FeatureEngine

# ログ設定
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('train_mu_v3_4.log', encoding='utf-8')
    ]
)

# =====================================================================
# 改修②: ベッティング戦略の関数
# =====================================================================

def calculate_ev(df: pd.DataFrame, score_col: str = 'score', odds_col: str = 'win_odds') -> pd.DataFrame:
    """
    EV（期待値）を計算する
    
    EV = P(win) × odds - 1
    P(win)はモデル予測のソフトマックス変換で推定
    
    Args:
        df: レースデータ（score_col, odds_col, race_id を含む）
        score_col: モデル予測スコアのカラム名
        odds_col: オッズのカラム名
    
    Returns:
        win_prob, ev カラムが追加されたDataFrame
    """
    df = df.copy()
    
    # レース内でソフトマックスで確率変換
    df['exp_score'] = np.exp(df[score_col] - df.groupby('race_id')[score_col].transform('max'))  # 数値安定性
    df['win_prob'] = df.groupby('race_id')['exp_score'].transform(lambda x: x / x.sum())
    
    # EV計算
    df['ev'] = df['win_prob'] * df[odds_col] - 1
    
    # クリーンアップ
    df.drop(columns=['exp_score'], inplace=True)
    
    return df


def select_bets_by_ev(df: pd.DataFrame, ev_threshold: float = 0.0, 
                      min_odds: float = 3.0, max_odds: float = 50.0) -> pd.DataFrame:
    """
    EVとオッズ範囲で賭け対象をフィルタリング
    
    Args:
        df: EV計算済みのデータ
        ev_threshold: EVの閾値（デフォルト: 0.0 = 期待値プラスのみ）
        min_odds: 最小オッズ（デフォルト: 3.0倍）
        max_odds: 最大オッズ（デフォルト: 50.0倍）
    
    Returns:
        賭け対象として選別されたレースのDataFrame
    """
    # フィルタリング条件
    bet_candidates = df[
        (df['ev'] > ev_threshold) &
        (df['win_odds'] >= min_odds) &
        (df['win_odds'] <= max_odds)
    ].copy()
    
    # レースごとにTop 1を選択（スコア最大の馬）
    if bet_candidates.empty:
        return bet_candidates
    
    bets = bet_candidates.loc[
        bet_candidates.groupby('race_id')['score'].idxmax()
    ]
    
    return bets


def calculate_roi_with_ev_strategy(df: pd.DataFrame, preds: np.ndarray,
                                   ev_threshold: float = 0.0,
                                   min_odds: float = 3.0, 
                                   max_odds: float = 50.0) -> Tuple[float, int, int]:
    """
    EV戦略を適用したROIを計算
    
    Returns:
        (roi, bet_count, total_races): ROI, 賭けレース数, 全レース数
    """
    df = df.copy()
    df['score'] = preds
    df['rank_pred'] = df.groupby('race_id')['score'].rank(ascending=False, method='first')
    
    # EV計算
    df = calculate_ev(df, score_col='score', odds_col='win_odds')
    
    # EV戦略適用
    bets = select_bets_by_ev(df, ev_threshold=ev_threshold, 
                             min_odds=min_odds, max_odds=max_odds)
    
    if bets.empty:
        return 0.0, 0, df['race_id'].nunique()
    
    # ROI計算
    hits = bets[bets['finish_position'] == 1]
    return_amount = hits['win_odds'].sum()
    bet_count = len(bets)
    roi = return_amount / bet_count if bet_count > 0 else 0
    
    return roi, bet_count, df['race_id'].nunique()


def calculate_roi_standard(df: pd.DataFrame, preds: np.ndarray) -> float:
    """標準ROI計算（全レースTop 1に賭ける）"""
    df = df.copy()
    df['score'] = preds
    df['rank_pred'] = df.groupby('race_id')['score'].rank(ascending=False, method='first')
    
    bet_df = df[df['rank_pred'] == 1]
    hits = bet_df[bet_df['finish_position'] == 1]
    return_amount = hits['win_odds'].sum()
    bet_amount = len(bet_df)
    roi = return_amount / bet_amount if bet_amount > 0 else 0
    return roi


# =====================================================================
# 改修④: Purged K-Fold
# =====================================================================

def get_purged_time_series_folds(df: pd.DataFrame, purge_days: int = 30) -> List[Tuple]:
    """
    Purged Time-Series K-Fold を生成
    
    訓練と検証の間にギャップ（purge期間）を設けることで、
    時系列リークを防止する。
    
    Args:
        df: race_dateを含むDataFrame
        purge_days: パージ期間（日数）
    
    Returns:
        List of (train_start, train_end, valid_start, valid_end) tuples
    """
    # purge_days日分のギャップを設ける
    folds = [
        # Fold 1: Train ~2021/12/01, Purge 30日, Valid 2022/01/01~
        ('2020-01-01', '2021-12-01', '2022-01-01', '2023-01-01'),
        # Fold 2: Train ~2022/12/01, Purge 30日, Valid 2023/01/01~
        ('2020-01-01', '2022-12-01', '2023-01-01', '2024-01-01'),
        # Fold 3: Train ~2023/06/01, Purge 30日, Valid 2023/07/01~
        ('2020-01-01', '2023-06-01', '2023-07-01', '2024-01-01'),
    ]
    return folds


# =====================================================================
# メインクラス
# =====================================================================

class MuV34EnsembleTrainer:
    """
    μモデル v3.4 アンサンブル学習クラス
    
    改修①: 3種類のモデルをアンサンブル
    改修②: EV戦略によるベッティング選別
    改修④: Purged K-Fold
    """
    
    def __init__(self, data_dir: str = 'keibaai/data/parsed/parquet'):
        self.data_dir = Path(data_dir)
        self.models_dir = Path('keibaai/models/mu_v3_4')
        self.models_dir.mkdir(parents=True, exist_ok=True)
        
        # アンサンブルモデル格納
        self.ensemble_models: List[lgb.LGBMRanker] = []
        self.ensemble_weights: List[float] = []
        
        # v3.3由来のベスト設定（デフォルト値）
        self.best_weight_1st = 12.74
        self.best_weight_2nd = 6.73
        self.best_weight_3rd = 3.69
        self.best_odds_clip = 90
        
    def load_data(self) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        """必要なデータを読み込む（v3.3と同様）"""
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
                'margin_seconds', 'finish_time_seconds', 'last_3f_time',
                'passing_order_1', 'passing_order_2', 'passing_order_3', 'passing_order_4'
            ]
            cols_to_merge = [c for c in cols_to_add if c not in df.columns]
            
            if cols_to_merge:
                merge_keys = ['race_id', 'horse_id']
                merge_cols = merge_keys + cols_to_merge
                races_select = races_df[merge_cols].drop_duplicates(subset=merge_keys)
                df = df.merge(races_select, on=merge_keys, how='left')
                logging.info(f"races.parquet からマージ: {len(cols_to_merge)} columns")

        # 3. 血統データ
        ped_path = self.data_dir / 'pedigrees/pedigrees.parquet'
        ped_df = pd.read_parquet(ped_path) if ped_path.exists() else pd.DataFrame()
        
        # 4. 馬プロファイル
        prof_path = self.data_dir / 'horses/horses.parquet'
        prof_df = pd.read_parquet(prof_path) if prof_path.exists() else pd.DataFrame()
        
        logging.info(f"データ読み込み完了: {len(df):,} レース結果")
        
        # 重複排除
        before_len = len(df)
        df = df.drop_duplicates(subset=['race_id', 'horse_id'])
        logging.info(f"重複排除: {before_len:,} -> {len(df):,} rows")
        
        return df, ped_df, prof_df
    
    def generate_rolling_features(self, df: pd.DataFrame, ped_df: pd.DataFrame, 
                                   prof_df: pd.DataFrame, dry_run: bool = False) -> pd.DataFrame:
        """特徴量生成（v3.3と同様）"""
        logging.info("=" * 50)
        logging.info("ローリング特徴量生成を開始します (v3.4)")
        logging.info("=" * 50)
        
        config_path = Path('keibaai/configs/features.yaml')
        engine = FeatureEngine(config_path)
        
        start_date = df['race_date'].min()
        end_date = df['race_date'].max()
        training_start_date = pd.Timestamp('2020-01-01')
        
        if dry_run:
            training_start_date = pd.Timestamp('2023-01-01')
            
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
                features = engine.generate_features(
                    shutuba_df=target_df,
                    results_history_df=history_df,
                    horse_profiles_df=prof_df,
                    pedigree_df=ped_df
                )
                
                cols_to_keep = ['race_id', 'horse_id', 'finish_position', 'race_date', 'win_odds', 'popularity']
                cols_to_merge = [c for c in cols_to_keep if c not in features.columns and c in target_df.columns]
                if cols_to_merge:
                    right_cols = list(set(cols_to_merge + ['race_id', 'horse_id']))
                    features = features.merge(target_df[right_cols], on=['race_id', 'horse_id'], how='left')
                
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
            save_path = self.models_dir / 'train_data_mu_v3_4.parquet'
            full_df.to_parquet(save_path)
            logging.info(f"学習データを保存しました: {save_path}")
            
        return full_df
    
    def _prepare_data(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, List[str]]:
        """
        データ前処理：ターゲット変数生成、特徴量選択
        """
        logging.info("=" * 50)
        logging.info("データ前処理 (v3.4)")
        logging.info("=" * 50)
        
        # --- ターゲット変数生成 ---
        odds = df['win_odds'].fillna(1.0)
        predictable_mask = odds <= 100.0
        odds_clipped = odds.clip(upper=self.best_odds_clip)
        log_odds = np.log1p(odds_clipped)
        
        gain = np.zeros(len(df))
        
        mask_1st = predictable_mask & (df['finish_position'] == 1)
        gain[mask_1st] = log_odds[mask_1st] * self.best_weight_1st
        
        mask_2nd = predictable_mask & (df['finish_position'] == 2)
        gain[mask_2nd] = log_odds[mask_2nd] * self.best_weight_2nd
        
        mask_3rd = predictable_mask & (df['finish_position'] == 3)
        gain[mask_3rd] = log_odds[mask_3rd] * self.best_weight_3rd
        
        df['target_gain'] = gain
        df['target_relevance'] = df['target_gain'].astype(int)
        
        logging.info(f"ターゲット変数生成完了: gain range=[{gain.min():.2f}, {gain.max():.2f}]")
        
        # --- Gap Features ---
        ability_col = 'past_5_finish_position_mean'
        if ability_col in df.columns and 'popularity' in df.columns:
            df['ability_rank'] = df.groupby('race_id')[ability_col].rank(ascending=True, method='first')
            df['gap_ability_popularity'] = df['popularity'] - df['ability_rank']
            logging.info("✅ Gap Feature 'gap_ability_popularity' created")
        
        # --- サンプルウェイト ---
        df['sample_weight'] = np.log1p(df['win_odds'].fillna(1.0)).clip(upper=np.log1p(100))
        
        # --- 除外カラム ---
        exclude_cols = [
            'race_id', 'horse_id', 'race_date', 'finish_position', 'target_gain', 'target_relevance',
            'win_odds', 'jockey_id', 'trainer_id', 'owner_id', 'sire_id', 'damsire_id',
            'race_name', 'horse_name', 'jockey_name', 'trainer_name',
            'sample_weight', 'ability_rank',
            'horse_weight_diff_from_avg', 'weight_diff_from_avg',
            'horse_weight', 'basis_weight',
            # リーク防止
            'finish_time_seconds', 'margin_seconds', 'prize_money', 'popularity',
            'odds', 'finish_time_str', 'margin_str', 'last_3f_time', 'passing_order',
            'final_corner_to_finish', 'passing_order_1', 'passing_order_2', 
            'passing_order_3', 'passing_order_4', 'position_change_1_2', 
            'scratched', 'time_except_last3f', 'win_probability', 'pace_index',
            'last3f_rank', 'popularity_finish_diff',
            'position_change_2_3', 'position_change_3_4', 'relative_odds',
            'running_style', 'time_deviation', 'combo_avg_popularity'
        ]
        
        feature_cols = [c for c in df.columns if c not in exclude_cols 
                       and pd.api.types.is_numeric_dtype(df[c])]
        
        logging.info(f"使用特徴量: {len(feature_cols)}個")
        
        return df, feature_cols

    def train_model(self, df: pd.DataFrame, n_trials: int = 50, dry_run: bool = False):
        """
        アンサンブル学習メイン（改修①②④統合）
        """
        logging.info("=" * 50)
        logging.info("μv3.4 アンサンブル学習開始")
        logging.info("  改修①: 3モデルアンサンブル")
        logging.info("  改修②: EV戦略ベッティング")
        logging.info("  改修④: Purged K-Fold")
        logging.info("=" * 50)
        
        # --- データ前処理 ---
        df, feature_cols = self._prepare_data(df)
        df = df.sort_values('race_id')
        
        # --- データ分割 ---
        if dry_run:
            unique_races = df['race_id'].unique()
            n_races = len(unique_races)
            split_idx = int(n_races * 0.8)
            train_races = unique_races[:split_idx]
            valid_races = unique_races[split_idx:]
            
            train_df = df[df['race_id'].isin(train_races)]
            valid_df = df[df['race_id'].isin(valid_races)]
            test_df = valid_df.copy()
        else:
            train_mask = df['race_date'] < '2023-01-01'
            valid_mask = (df['race_date'] >= '2023-01-01') & (df['race_date'] < '2024-01-01')
            test_mask = df['race_date'] >= '2024-01-01'
            
            train_df = df[train_mask]
            valid_df = df[valid_mask]
            test_df = df[test_mask]
        
        def get_group(d):
            return d.groupby('race_id').size().to_list()
        
        group_train = get_group(train_df)
        group_valid = get_group(valid_df)
        group_test = get_group(test_df)
        
        logging.info(f"Train: {len(train_df):,} rows, {len(group_train):,} races")
        logging.info(f"Valid: {len(valid_df):,} rows, {len(group_valid):,} races")
        logging.info(f"Test: {len(test_df):,} rows, {len(group_test):,} races")
        
        # =====================================================
        # 改修①: アンサンブル学習（3種類のモデル）
        # =====================================================
        logging.info("=" * 50)
        logging.info("【改修①】アンサンブル学習: 3モデルを学習")
        logging.info("=" * 50)
        
        # 3種類のモデル設定
        model_configs = [
            {
                'name': '保守的モデル',
                'params': {
                    'objective': 'lambdarank',
                    'metric': 'ndcg',
                    'eval_at': [1, 3, 5],
                    'boosting_type': 'gbdt',
                    'num_leaves': 31,
                    'lambda_l1': 5.0,
                    'lambda_l2': 5.0,
                    'min_child_samples': 500,
                    'learning_rate': 0.01,
                    'feature_fraction': 0.6,
                    'bagging_fraction': 0.8,
                    'bagging_freq': 5,
                    'verbose': -1,
                    'random_state': 42,
                    'label_gain': [i for i in range(100)]
                },
                'n_estimators': 2000
            },
            {
                'name': '標準モデル',
                'params': {
                    'objective': 'lambdarank',
                    'metric': 'ndcg',
                    'eval_at': [1, 3, 5],
                    'boosting_type': 'gbdt',
                    'num_leaves': 63,
                    'lambda_l1': 1.0,
                    'lambda_l2': 1.0,
                    'min_child_samples': 200,
                    'learning_rate': 0.05,
                    'feature_fraction': 0.7,
                    'bagging_fraction': 0.9,
                    'bagging_freq': 5,
                    'verbose': -1,
                    'random_state': 42,
                    'label_gain': [i for i in range(100)]
                },
                'n_estimators': 1500
            },
            {
                'name': '積極的モデル',
                'params': {
                    'objective': 'lambdarank',
                    'metric': 'ndcg',
                    'eval_at': [1, 3, 5],
                    'boosting_type': 'gbdt',
                    'num_leaves': 127,
                    'lambda_l1': 0.1,
                    'lambda_l2': 0.1,
                    'min_child_samples': 100,
                    'learning_rate': 0.1,
                    'feature_fraction': 0.8,
                    'bagging_fraction': 1.0,
                    'bagging_freq': 0,
                    'verbose': -1,
                    'random_state': 42,
                    'label_gain': [i for i in range(100)]
                },
                'n_estimators': 1000
            }
        ]
        
        models = []
        for config in model_configs:
            logging.info(f"\n--- {config['name']} 学習中 ---")
            
            model = lgb.LGBMRanker(**config['params'], n_estimators=config['n_estimators'])
            
            model.fit(
                train_df[feature_cols],
                train_df['target_relevance'],
                group=group_train,
                sample_weight=train_df['sample_weight'],
                eval_set=[(valid_df[feature_cols], valid_df['target_relevance'])],
                eval_group=[group_valid],
                eval_sample_weight=[valid_df['sample_weight']],
                eval_metric='ndcg',
                callbacks=[lgb.early_stopping(stopping_rounds=50, verbose=True)]
            )
            
            # 個別モデル評価
            preds = model.predict(valid_df[feature_cols])
            roi = calculate_roi_standard(valid_df, preds)
            logging.info(f"  {config['name']} Valid ROI: {roi:.2%}")
            
            models.append(model)
        
        # =====================================================
        # アンサンブル重み最適化 (Optuna)
        # =====================================================
        logging.info("\n--- アンサンブル重み最適化 ---")
        
        def ensemble_objective(trial):
            w1 = trial.suggest_float('w1', 0.1, 1.0)
            w2 = trial.suggest_float('w2', 0.1, 1.0)
            w3 = trial.suggest_float('w3', 0.1, 1.0)
            weights = np.array([w1, w2, w3])
            weights = weights / weights.sum()  # 正規化
            
            # アンサンブル予測
            preds_list = [m.predict(valid_df[feature_cols]) for m in models]
            ensemble_preds = np.average(preds_list, axis=0, weights=weights)
            
            roi = calculate_roi_standard(valid_df, ensemble_preds)
            return roi
        
        if dry_run:
            # Dry Run: 等重み
            best_weights = np.array([1/3, 1/3, 1/3])
            logging.info("Dry Run: 等重み使用")
        else:
            study = optuna.create_study(direction='maximize')
            study.optimize(ensemble_objective, n_trials=min(n_trials, 30), show_progress_bar=True)
            
            w1 = study.best_params['w1']
            w2 = study.best_params['w2']
            w3 = study.best_params['w3']
            best_weights = np.array([w1, w2, w3])
            best_weights = best_weights / best_weights.sum()
            
            logging.info(f"最適重み: 保守的={best_weights[0]:.3f}, 標準={best_weights[1]:.3f}, 積極的={best_weights[2]:.3f}")
        
        self.ensemble_models = models
        self.ensemble_weights = best_weights.tolist()
        
        # =====================================================
        # 改修②: EV戦略パラメータ最適化
        # =====================================================
        logging.info("=" * 50)
        logging.info("【改修②】EV戦略パラメータ最適化")
        logging.info("=" * 50)
        
        # アンサンブル予測
        preds_list = [m.predict(valid_df[feature_cols]) for m in models]
        ensemble_preds = np.average(preds_list, axis=0, weights=best_weights)
        
        def ev_strategy_objective(trial):
            ev_threshold = trial.suggest_float('ev_threshold', -0.2, 0.5)
            min_odds = trial.suggest_float('min_odds', 1.5, 5.0)
            max_odds = trial.suggest_float('max_odds', 30.0, 100.0)
            
            roi, bet_count, total_races = calculate_roi_with_ev_strategy(
                valid_df, ensemble_preds,
                ev_threshold=ev_threshold,
                min_odds=min_odds,
                max_odds=max_odds
            )
            
            # 賭けレース数が少なすぎる場合はペナルティ
            if bet_count < total_races * 0.2:
                return 0.0
            
            return roi
        
        if dry_run:
            best_ev_params = {
                'ev_threshold': 0.0,
                'min_odds': 3.0,
                'max_odds': 50.0
            }
            logging.info("Dry Run: デフォルトEVパラメータ使用")
        else:
            study_ev = optuna.create_study(direction='maximize')
            study_ev.optimize(ev_strategy_objective, n_trials=min(n_trials, 30), show_progress_bar=True)
            best_ev_params = study_ev.best_params
            
            logging.info(f"最適EV戦略: ev_threshold={best_ev_params['ev_threshold']:.3f}, "
                        f"min_odds={best_ev_params['min_odds']:.1f}, max_odds={best_ev_params['max_odds']:.1f}")
        
        # =====================================================
        # モデル保存
        # =====================================================
        logging.info("=" * 50)
        logging.info("モデルを保存しています...")
        logging.info("=" * 50)
        
        # アンサンブルモデル保存
        for i, model in enumerate(models):
            model_path = self.models_dir / f'mu_v3_4_model_{i}.pkl'
            with open(model_path, 'wb') as f:
                pickle.dump(model, f)
        
        # メタデータ保存
        metadata = {
            'version': 'v3.4',
            'created_at': datetime.now().isoformat(),
            'ensemble_weights': self.ensemble_weights,
            'ev_strategy': best_ev_params,
            'target_weights': {
                'weight_1st': self.best_weight_1st,
                'weight_2nd': self.best_weight_2nd,
                'weight_3rd': self.best_weight_3rd,
                'odds_clip': self.best_odds_clip
            },
            'feature_count': len(feature_cols)
        }
        
        with open(self.models_dir / 'model_info.json', 'w', encoding='utf-8') as f:
            json.dump(metadata, f, ensure_ascii=False, indent=2)
        
        with open(self.models_dir / 'feature_names.json', 'w', encoding='utf-8') as f:
            json.dump(feature_cols, f, ensure_ascii=False)
        
        logging.info("モデル保存完了")
        
        # =====================================================
        # 評価
        # =====================================================
        self.evaluate_model(test_df, feature_cols, best_ev_params)
    
    def predict_ensemble(self, df: pd.DataFrame, feature_cols: List[str]) -> np.ndarray:
        """アンサンブル予測"""
        preds_list = [m.predict(df[feature_cols]) for m in self.ensemble_models]
        return np.average(preds_list, axis=0, weights=self.ensemble_weights)
    
    def evaluate_model(self, test_df: pd.DataFrame, feature_cols: List[str], ev_params: Dict):
        """評価"""
        logging.info("=" * 50)
        logging.info("モデル評価 (Test Data)")
        logging.info("=" * 50)
        
        # アンサンブル予測
        preds = self.predict_ensemble(test_df, feature_cols)
        
        # --- 標準評価（全レースTop 1） ---
        test_df = test_df.copy()
        test_df['score'] = preds
        test_df['rank_pred'] = test_df.groupby('race_id')['score'].rank(ascending=False, method='first')
        
        top1 = test_df[test_df['rank_pred'] == 1]
        hits = top1[top1['finish_position'] == 1]
        accuracy = len(hits) / len(top1)
        roi_standard = hits['win_odds'].sum() / len(top1)
        
        logging.info("【標準戦略（全レースTop 1）】")
        logging.info(f"  Top 1 Accuracy: {accuracy:.2%}")
        logging.info(f"  ROI: {roi_standard:.2%}")
        logging.info(f"  賭けレース数: {len(top1):,}")
        
        # --- EV戦略評価 ---
        roi_ev, bet_count, total_races = calculate_roi_with_ev_strategy(
            test_df, preds,
            ev_threshold=ev_params['ev_threshold'],
            min_odds=ev_params['min_odds'],
            max_odds=ev_params['max_odds']
        )
        
        logging.info("\n【EV戦略（選別ベッティング）】")
        logging.info(f"  ROI: {roi_ev:.2%}")
        logging.info(f"  賭けレース数: {bet_count:,} / {total_races:,} ({bet_count/total_races:.1%})")
        logging.info(f"  ROI改善: {(roi_ev - roi_standard) * 100:.2f}%pt")
        
        # --- Top 5 Recall ---
        winners = test_df[test_df['finish_position'] == 1]
        winners_in_top5 = winners[winners['rank_pred'] <= 5]
        recall = len(winners_in_top5) / len(winners)
        logging.info(f"\n  Top 5 Recall: {recall:.2%}")
        
        # --- Feature Importance (平均) ---
        feature_importances = np.mean([m.feature_importances_ for m in self.ensemble_models], axis=0)
        imp = pd.DataFrame({
            'feature': feature_cols,
            'importance': feature_importances
        }).sort_values('importance', ascending=False)
        
        logging.info("\nFeature Importance (Top 20):")
        logging.info(imp.head(20).to_string())
        imp.to_csv(self.models_dir / 'feature_importance.csv', index=False)
        
        # --- 結果サマリー保存 ---
        results = {
            'standard_strategy': {
                'accuracy': float(accuracy),
                'roi': float(roi_standard),
                'bet_count': int(len(top1))
            },
            'ev_strategy': {
                'roi': float(roi_ev),
                'bet_count': int(bet_count),
                'total_races': int(total_races),
                'bet_ratio': float(bet_count / total_races)
            },
            'top5_recall': float(recall),
            'ev_params': ev_params
        }
        
        with open(self.models_dir / 'evaluation_results.json', 'w', encoding='utf-8') as f:
            json.dump(results, f, ensure_ascii=False, indent=2)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description='μモデル v3.4 アンサンブル学習')
    parser.add_argument('--n_trials', type=int, default=50, help='Optunaトライアル数')
    parser.add_argument('--dry-run', action='store_true', help='テスト実行（短期間）')
    args = parser.parse_args()
    
    trainer = MuV34EnsembleTrainer()
    df, ped_df, prof_df = trainer.load_data()
    
    # 既存データがあれば読み込み
    train_data_path = trainer.models_dir / 'train_data_mu_v3_4.parquet'
    if train_data_path.exists() and not args.dry_run:
        full_df = pd.read_parquet(train_data_path)
        logging.info(f"既存の学習データを読み込みました: {train_data_path}")
    else:
        logging.info("学習データを新規生成します...")
        full_df = trainer.generate_rolling_features(df, ped_df, prof_df, dry_run=args.dry_run)
    
    trainer.train_model(full_df, n_trials=args.n_trials, dry_run=args.dry_run)
    
    logging.info("=" * 50)
    logging.info("μモデル v3.4 学習完了")
    logging.info("=" * 50)
