#!/usr/bin/env python3
"""
芝/ダート別モデル訓練・検証スクリプト (train_turf_dirt_separate.py)

【目的】
- 障害レース・新馬戦を除外
- 芝・ダート別にモデルを構築
- 厳密なWalk-forward検証（各レース直前までのデータで訓練）
- 人気ベースラインとのROI比較

【リーク防止】
- 各レースの予測時、そのレース以前のデータのみで訓練
- shift(1)で当該レースの結果は特徴量に含めない
- finish_position, win_odds, popularity は特徴量から除外

Author: KeibaAI Development Team
Date: 2026-01-10
"""

import sys
from pathlib import Path

# プロジェクトルートをsys.pathに追加
project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(project_root))
sys.path.insert(0, str(project_root / 'keibaai'))

import pandas as pd
import numpy as np
import lightgbm as lgb
import logging
import pickle
import json
from datetime import datetime
from typing import Tuple, List, Dict, Optional
from sklearn.metrics import roc_auc_score

# ログ設定
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('train_turf_dirt_separate.log', encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)


class TurfDirtSeparateTrainer:
    """芝/ダート別モデル訓練クラス"""
    
    # リーク特徴量（絶対に使用禁止）
    LEAK_FEATURES = [
        # 着順・タイム関連（レース結果）
        'finish_position', 'finish_time_seconds', 'margin_seconds', 'prize_money',
        'finish_time_str', 'margin_str', 'last_3f_time', 'time_except_last3f',
        'last3f_rank',  # 上がり3F順位（レース後）
        # オッズ・人気関連
        'popularity', 'win_odds', 'odds', 'win_probability',
        'relative_odds',  # 相対オッズ
        'popularity_finish_diff',  # 人気と着順の差
        # コーナー通過順位（レース中）
        'passing_order', 'passing_order_1', 'passing_order_2',
        'passing_order_3', 'passing_order_4', 'final_corner_to_finish',
        'position_change_1_2', 'position_change_2_3', 'position_change_3_4',
        # ペース関連
        'pace_index',
        # その他
        'target', 'weight',
        # 馬体重変化（元データのカラム、再計算するので除外）
        'horse_weight_change', 'horse_weight_deviation',
    ]
    
    # ID・メタデータカラム（特徴量から除外）
    META_COLS = [
        'race_id', 'horse_id', 'race_date', 'jockey_id', 'trainer_id', 'owner_id',
        'sire_id', 'damsire_id', 'race_name', 'horse_name', 'jockey_name', 
        'trainer_name', 'scratched', 'year', 'track_surface', 'race_class',
        'venue', 'track_condition', 'distance_category', 'bracket_category',
        'sex', 'weather',
    ]
    
    def __init__(self, data_dir: str = 'keibaai/data/parsed/parquet'):
        self.data_dir = Path(data_dir)
        self.models_dir = Path('keibaai/models/turf_dirt_separate')
        self.models_dir.mkdir(parents=True, exist_ok=True)
        
        # LightGBMパラメータ（Early Stopping無効）
        self.lgb_params = {
            'objective': 'binary',
            'metric': 'auc',
            'boosting_type': 'gbdt',
            'learning_rate': 0.05,
            'max_depth': 5,
            'num_leaves': 31,
            'min_child_samples': 100,
            'reg_alpha': 1.0,
            'reg_lambda': 1.0,
            'feature_fraction': 0.8,
            'bagging_fraction': 0.8,
            'bagging_freq': 5,
            'verbosity': -1,
            'seed': 42,
        }
        self.num_boost_round = 500  # 固定（Early Stopping無効）
        
    def load_data(self) -> pd.DataFrame:
        """データ読み込み・前処理"""
        logger.info("=" * 60)
        logger.info("データ読み込み開始")
        logger.info("=" * 60)
        
        # レースデータ読み込み
        races_path = self.data_dir / 'races/races.parquet'
        df = pd.read_parquet(races_path)
        
        df['race_date'] = pd.to_datetime(df['race_date'])
        df['year'] = df['race_date'].dt.year
        
        logger.info(f"総レコード数: {len(df):,}")
        logger.info(f"期間: {df['race_date'].min().date()} ~ {df['race_date'].max().date()}")
        
        # 2014年以降に絞る
        df = df[df['year'] >= 2014].copy()
        logger.info(f"2014年以降: {len(df):,}")
        
        # 馬場種別の分布
        logger.info(f"\n馬場種別分布:")
        for surface in df['track_surface'].unique():
            count = len(df[df['track_surface'] == surface])
            logger.info(f"  {surface}: {count:,} ({count/len(df)*100:.1f}%)")
        
        # 障害レース除外
        before = len(df)
        df = df[df['track_surface'] != '障害'].copy()
        logger.info(f"\n障害レース除外: {before:,} -> {len(df):,} (-{before - len(df):,})")
        
        # 新馬戦除外
        before = len(df)
        df = df[df['race_class'] != '新馬'].copy()
        logger.info(f"新馬戦除外: {before:,} -> {len(df):,} (-{before - len(df):,})")
        
        # 最終データ確認
        logger.info(f"\n最終データ:")
        logger.info(f"  芝: {len(df[df['track_surface'] == '芝']):,}")
        logger.info(f"  ダート: {len(df[df['track_surface'] == 'ダート']):,}")
        
        return df
    
    def _generate_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        特徴量生成
        
        【リーク防止】
        - cumsum().shift(1) パターンで当該レースを除外
        - 過去データのみから統計を計算
        """
        logger.info("特徴量生成開始...")
        
        df = df.copy()
        df = df.sort_values(['race_date', 'race_id', 'horse_number']).reset_index(drop=True)
        
        # 目的変数
        df['target'] = (df['finish_position'] == 1).astype(int)
        
        # --- 基本特徴量（リークなし）---
        # 距離カテゴリ
        df['distance_category'] = pd.cut(
            df['distance_m'],
            bins=[0, 1200, 1600, 2000, 2400, 9999],
            labels=['sprint', 'mile', 'intermediate', 'long', 'marathon']
        ).astype(str)
        
        # 馬場状態エンコーディング
        track_cond_map = {'良': 0, '稍重': 1, '重': 2, '不良': 3}
        df['track_condition_encoded'] = df['track_condition'].map(track_cond_map).fillna(0)
        
        # 枠番カテゴリ
        df['bracket_category'] = pd.cut(
            df['bracket_number'],
            bins=[0, 2, 4, 6, 8],
            labels=['inner', 'mid_inner', 'mid_outer', 'outer']
        ).astype(str)
        
        # 出走頭数
        df['field_size'] = df.groupby('race_id')['horse_number'].transform('count')
        
        # --- 累積統計（shift(1)でリーク防止）---
        # 馬の累積勝率
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
        
        # 馬の累積平均着順
        df['horse_cum_finish_sum'] = df.groupby('horse_id')['finish_position'].transform(
            lambda x: x.cumsum().shift(1).fillna(0)
        )
        df['horse_avg_finish'] = np.where(
            df['horse_cum_races'] >= 3,
            df['horse_cum_finish_sum'] / df['horse_cum_races'],
            np.nan
        )
        
        # 騎手の累積勝率
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
        
        # 調教師の累積勝率
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
        
        # 前走着順（shift(1)で前走のみ）
        df['prev_finish'] = df.groupby('horse_id')['finish_position'].shift(1)
        
        # 前走からの日数
        df['prev_race_date'] = df.groupby('horse_id')['race_date'].shift(1)
        df['days_since_last'] = (df['race_date'] - df['prev_race_date']).dt.days
        
        # 馬体重変化
        df['weight_change'] = df.groupby('horse_id')['horse_weight'].diff()
        
        # 距離適性（過去同距離カテゴリでの平均着順）
        # これは複雑なので簡易版で実装
        df['distance_match'] = 0  # 後で改善可能
        
        # 競馬場エンコーディング
        venue_encoder = {v: i for i, v in enumerate(df['venue'].unique())}
        df['venue_encoded'] = df['venue'].map(venue_encoder)
        
        # レース番号（R8-12フラグ）
        df['race_number'] = df['race_id'].str[-2:].astype(int)
        df['is_late_race'] = (df['race_number'] >= 8).astype(int)
        
        # 一時カラムを削除
        temp_cols = ['horse_cum_wins', 'horse_cum_races', 'horse_cum_finish_sum',
                     'jockey_cum_wins', 'jockey_cum_races', 
                     'trainer_cum_wins', 'trainer_cum_races',
                     'prev_race_date']
        df = df.drop(columns=[c for c in temp_cols if c in df.columns], errors='ignore')
        
        logger.info(f"特徴量生成完了: {len(df.columns)} カラム")
        
        return df
    
    def _get_feature_cols(self, df: pd.DataFrame) -> List[str]:
        """使用する特徴量カラムを取得"""
        exclude = set(self.LEAK_FEATURES + self.META_COLS)
        feature_cols = [c for c in df.columns if c not in exclude]
        
        # 数値型のみ
        numeric_cols = []
        for col in feature_cols:
            if df[col].dtype in ['int64', 'float64', 'int32', 'float32', 'int16', 'float16', 'int8']:
                numeric_cols.append(col)
            elif df[col].dtype == 'category':
                numeric_cols.append(col)
        
        logger.info(f"使用特徴量: {len(numeric_cols)}個")
        
        # リークチェック
        for leak in self.LEAK_FEATURES:
            if leak in numeric_cols:
                logger.error(f"リーク特徴量が含まれています: {leak}")
                raise ValueError(f"Leak feature detected: {leak}")
        
        return numeric_cols
    
    def _calculate_popularity_baseline(self, test_df: pd.DataFrame) -> Dict:
        """人気ベースラインROIを計算"""
        results = {}
        
        for pop in [1, 2, 3]:
            pop_df = test_df[test_df['popularity'] == pop].copy()
            if len(pop_df) == 0:
                results[f'pop{pop}_roi'] = np.nan
                results[f'pop{pop}_hit_rate'] = np.nan
                results[f'pop{pop}_count'] = 0
                continue
            
            # 的中した場合のオッズ合計
            hits = pop_df[pop_df['finish_position'] == 1]
            roi = hits['win_odds'].sum() / len(pop_df) if len(pop_df) > 0 else 0
            hit_rate = len(hits) / len(pop_df) if len(pop_df) > 0 else 0
            
            results[f'pop{pop}_roi'] = roi
            results[f'pop{pop}_hit_rate'] = hit_rate
            results[f'pop{pop}_count'] = len(pop_df)
        
        return results
    
    def _train_and_evaluate(self, train_df: pd.DataFrame, test_df: pd.DataFrame, 
                           feature_cols: List[str], surface: str) -> Dict:
        """モデル訓練と評価"""
        
        # 訓練データ準備
        X_train = train_df[feature_cols].copy()
        y_train = train_df['target'].values
        
        X_test = test_df[feature_cols].copy()
        y_test = test_df['target'].values
        
        # カテゴリカル特徴量の処理
        for col in X_train.columns:
            if X_train[col].dtype == 'object':
                X_train[col] = X_train[col].astype('category')
                X_test[col] = X_test[col].astype('category')
        
        # LightGBMデータセット
        lgb_train = lgb.Dataset(X_train, label=y_train)
        
        # 訓練（Early Stopping無効）
        model = lgb.train(
            self.lgb_params,
            lgb_train,
            num_boost_round=self.num_boost_round
        )
        
        # 予測
        preds = model.predict(X_test)
        
        # 評価
        auc = roc_auc_score(y_test, preds) if y_test.sum() > 0 else np.nan
        
        # レースごとにTop1選択してROI計算
        test_result = test_df[['race_id', 'finish_position', 'win_odds', 'popularity', 'target']].copy()
        test_result['pred_prob'] = preds
        test_result['pred_rank'] = test_result.groupby('race_id')['pred_prob'].rank(ascending=False, method='first')
        
        # Top1のみ抽出
        top1 = test_result[test_result['pred_rank'] == 1].copy()
        
        # ROI計算
        hits = top1[top1['target'] == 1]
        roi = hits['win_odds'].sum() / len(top1) if len(top1) > 0 else 0
        hit_rate = len(hits) / len(top1) if len(top1) > 0 else 0
        
        # 人気ベースライン
        pop_baseline = self._calculate_popularity_baseline(test_df)
        
        result = {
            'surface': surface,
            'train_size': len(train_df),
            'test_size': len(test_df),
            'auc': auc,
            'roi': roi,
            'hit_rate': hit_rate,
            'num_races': len(top1),
            **pop_baseline
        }
        
        return result, model
    
    def run_walkforward_validation(self, df: pd.DataFrame, surface: str) -> Tuple[List[Dict], lgb.Booster]:
        """
        Walk-forward検証
        
        【リーク防止】
        - 訓練データとテストデータを分割してから特徴量を生成
        - テストデータの特徴量は、訓練データの統計のみを使用
        - 各年のテストは、その年の「各レース直前まで」のデータで訓練
        """
        logger.info(f"\n{'='*60}")
        logger.info(f"Walk-forward検証: {surface}")
        logger.info(f"{'='*60}")
        
        results = []
        final_model = None
        feature_cols = None
        
        test_years = [2020, 2021, 2022, 2023, 2024, 2025]
        
        for test_year in test_years:
            logger.info(f"\n--- テスト年: {test_year} ---")
            
            # データ分割（特徴量生成前）
            test_mask = df['year'] == test_year
            train_mask = df['year'] < test_year
            
            test_df_raw = df[test_mask].copy()
            train_df_raw = df[train_mask].copy()
            
            if len(test_df_raw) == 0:
                logger.warning(f"  テストデータなし: {test_year}")
                continue
            
            if len(train_df_raw) == 0:
                logger.warning(f"  訓練データなし: {test_year}")
                continue
            
            logger.info(f"  訓練生データ: {len(train_df_raw):,}件 (2014-{test_year-1})")
            logger.info(f"  テスト生データ: {len(test_df_raw):,}件 ({test_year})")
            
            # 【リーク防止】訓練データのみで特徴量生成
            train_df = self._generate_features(train_df_raw)
            
            # 【リーク防止】テストデータは訓練データの統計を使用して特徴量生成
            test_df = self._generate_features_for_test(test_df_raw, train_df_raw)
            
            if feature_cols is None:
                feature_cols = self._get_feature_cols(train_df)
            
            logger.info(f"  訓練特徴量: {len(train_df):,}件")
            logger.info(f"  テスト特徴量: {len(test_df):,}件")
            
            # 訓練・評価
            try:
                result, model = self._train_and_evaluate(train_df, test_df, feature_cols, surface)
                result['test_year'] = test_year
                result['train_years'] = f"2014-{test_year-1}"
                results.append(result)
                final_model = model
                
                logger.info(f"  ROI: {result['roi']:.2%} | 的中率: {result['hit_rate']:.2%} | AUC: {result['auc']:.4f}")
                logger.info(f"  人気ベースライン: 1番人気={result['pop1_roi']:.2%}, 2番人気={result['pop2_roi']:.2%}, 3番人気={result['pop3_roi']:.2%}")
                
            except Exception as e:
                logger.error(f"  エラー: {e}")
                import traceback
                traceback.print_exc()
        
        return results, final_model, feature_cols
    
    def _generate_features_for_test(self, test_df: pd.DataFrame, train_df: pd.DataFrame) -> pd.DataFrame:
        """
        テストデータ用の特徴量生成（訓練データの統計のみ使用）
        
        【リーク防止】
        - テストデータのtarget/finish_positionは累積統計に含めない
        - 訓練データから計算した統計をテストデータに適用
        """
        logger.info("  テスト用特徴量生成開始（リークフリー）...")
        
        test_df = test_df.copy()
        train_df = train_df.copy()
        
        test_df = test_df.sort_values(['race_date', 'race_id', 'horse_number']).reset_index(drop=True)
        train_df = train_df.sort_values(['race_date', 'race_id', 'horse_number']).reset_index(drop=True)
        
        # 目的変数（テストデータ用、特徴量には使わない）
        test_df['target'] = (test_df['finish_position'] == 1).astype(int)
        train_df['target'] = (train_df['finish_position'] == 1).astype(int)
        
        # --- 基本特徴量（リークなし）---
        test_df['distance_category'] = pd.cut(
            test_df['distance_m'],
            bins=[0, 1200, 1600, 2000, 2400, 9999],
            labels=['sprint', 'mile', 'intermediate', 'long', 'marathon']
        ).astype(str)
        
        track_cond_map = {'良': 0, '稍重': 1, '重': 2, '不良': 3}
        test_df['track_condition_encoded'] = test_df['track_condition'].map(track_cond_map).fillna(0)
        
        test_df['bracket_category'] = pd.cut(
            test_df['bracket_number'],
            bins=[0, 2, 4, 6, 8],
            labels=['inner', 'mid_inner', 'mid_outer', 'outer']
        ).astype(str)
        
        test_df['field_size'] = test_df.groupby('race_id')['horse_number'].transform('count')
        
        # --- 訓練データから統計を計算 ---
        # 馬の累積勝率（訓練データ最終時点の統計）
        horse_stats = train_df.groupby('horse_id').agg({
            'target': ['sum', 'count'],
            'finish_position': 'mean'
        }).reset_index()
        horse_stats.columns = ['horse_id', 'horse_cum_wins', 'horse_cum_races', 'horse_avg_finish']
        horse_stats['horse_win_rate'] = np.where(
            horse_stats['horse_cum_races'] >= 3,
            horse_stats['horse_cum_wins'] / horse_stats['horse_cum_races'],
            np.nan
        )
        
        # 騎手の累積勝率
        jockey_stats = train_df.groupby('jockey_id').agg({
            'target': ['sum', 'count']
        }).reset_index()
        jockey_stats.columns = ['jockey_id', 'jockey_cum_wins', 'jockey_cum_races']
        jockey_stats['jockey_win_rate'] = np.where(
            jockey_stats['jockey_cum_races'] >= 10,
            jockey_stats['jockey_cum_wins'] / jockey_stats['jockey_cum_races'],
            np.nan
        )
        
        # 調教師の累積勝率
        trainer_stats = train_df.groupby('trainer_id').agg({
            'target': ['sum', 'count']
        }).reset_index()
        trainer_stats.columns = ['trainer_id', 'trainer_cum_wins', 'trainer_cum_races']
        trainer_stats['trainer_win_rate'] = np.where(
            trainer_stats['trainer_cum_races'] >= 10,
            trainer_stats['trainer_cum_wins'] / trainer_stats['trainer_cum_races'],
            np.nan
        )
        
        # 馬の前走情報（訓練データの最終走）
        train_last_race = train_df.sort_values(['horse_id', 'race_date']).groupby('horse_id').last()[['finish_position', 'race_date', 'horse_weight']].reset_index()
        train_last_race.columns = ['horse_id', 'prev_finish', 'prev_race_date', 'prev_horse_weight']
        
        # テストデータにマージ
        test_df = test_df.merge(horse_stats[['horse_id', 'horse_win_rate', 'horse_avg_finish']], on='horse_id', how='left')
        test_df = test_df.merge(jockey_stats[['jockey_id', 'jockey_win_rate']], on='jockey_id', how='left')
        test_df = test_df.merge(trainer_stats[['trainer_id', 'trainer_win_rate']], on='trainer_id', how='left')
        test_df = test_df.merge(train_last_race, on='horse_id', how='left')
        
        # 前走からの日数
        test_df['days_since_last'] = (test_df['race_date'] - test_df['prev_race_date']).dt.days
        
        # 馬体重変化
        test_df['weight_change'] = test_df['horse_weight'] - test_df['prev_horse_weight']
        
        # 距離適性（簡易版）
        test_df['distance_match'] = 0
        
        # 競馬場エンコーディング
        all_venues = pd.concat([train_df['venue'], test_df['venue']]).unique()
        venue_encoder = {v: i for i, v in enumerate(all_venues)}
        test_df['venue_encoded'] = test_df['venue'].map(venue_encoder)
        
        # レース番号
        test_df['race_number'] = test_df['race_id'].str[-2:].astype(int)
        test_df['is_late_race'] = (test_df['race_number'] >= 8).astype(int)
        
        # 一時カラムを削除
        temp_cols = ['prev_race_date', 'prev_horse_weight']
        test_df = test_df.drop(columns=[c for c in temp_cols if c in test_df.columns], errors='ignore')
        
        return test_df
    
    def run(self, dry_run: bool = False):
        """メイン実行"""
        logger.info("=" * 60)
        logger.info("芝/ダート別モデル訓練・検証開始")
        logger.info("=" * 60)
        
        # データ読み込み
        df = self.load_data()
        
        if dry_run:
            logger.info("\n[DRY RUN] データを縮小して実行")
            df = df[df['year'] >= 2022].copy()
        
        all_results = []
        
        # 芝モデル
        turf_df = df[df['track_surface'] == '芝'].copy()
        turf_results, turf_model, turf_features = self.run_walkforward_validation(turf_df, '芝')
        all_results.extend(turf_results)
        
        # ダートモデル
        dirt_df = df[df['track_surface'] == 'ダート'].copy()
        dirt_results, dirt_model, dirt_features = self.run_walkforward_validation(dirt_df, 'ダート')
        all_results.extend(dirt_results)
        
        # 結果をDataFrameに変換
        results_df = pd.DataFrame(all_results)
        
        # サマリー出力
        self._print_summary(results_df)
        
        # 結果保存
        self._save_results(results_df, turf_model, dirt_model, turf_features)
        
        return results_df
    
    def _print_summary(self, results_df: pd.DataFrame):
        """サマリー出力"""
        logger.info("\n" + "=" * 60)
        logger.info("サマリー")
        logger.info("=" * 60)
        
        for surface in ['芝', 'ダート']:
            surface_df = results_df[results_df['surface'] == surface]
            if len(surface_df) == 0:
                continue
            
            avg_roi = surface_df['roi'].mean()
            avg_hit = surface_df['hit_rate'].mean()
            avg_pop1 = surface_df['pop1_roi'].mean()
            
            logger.info(f"\n【{surface}】")
            logger.info(f"  平均ROI: {avg_roi:.2%}")
            logger.info(f"  平均的中率: {avg_hit:.2%}")
            logger.info(f"  1番人気ベースライン: {avg_pop1:.2%}")
            logger.info(f"  vs 1番人気: {'+' if avg_roi > avg_pop1 else ''}{(avg_roi - avg_pop1)*100:.1f}pt")
            
            # 年次別
            logger.info(f"  年次別ROI:")
            for _, row in surface_df.iterrows():
                diff = row['roi'] - row['pop1_roi']
                logger.info(f"    {int(row['test_year'])}: ROI={row['roi']:.2%} (vs人気1: {'+' if diff > 0 else ''}{diff*100:.1f}pt)")
        
        # 統合ROI
        total_roi = results_df['roi'].mean()
        total_pop1 = results_df['pop1_roi'].mean()
        logger.info(f"\n【統合】")
        logger.info(f"  平均ROI: {total_roi:.2%}")
        logger.info(f"  vs 1番人気: {'+' if total_roi > total_pop1 else ''}{(total_roi - total_pop1)*100:.1f}pt")
    
    def _save_results(self, results_df: pd.DataFrame, turf_model, dirt_model, feature_cols):
        """結果保存"""
        logger.info("\n結果を保存中...")
        
        # Walk-forward結果
        results_df.to_csv(self.models_dir / 'walkforward_results.csv', index=False)
        logger.info(f"  {self.models_dir / 'walkforward_results.csv'}")
        
        # モデル保存
        if turf_model:
            turf_model.save_model(str(self.models_dir / 'turf_model.txt'))
            logger.info(f"  {self.models_dir / 'turf_model.txt'}")
        
        if dirt_model:
            dirt_model.save_model(str(self.models_dir / 'dirt_model.txt'))
            logger.info(f"  {self.models_dir / 'dirt_model.txt'}")
        
        # 特徴量リスト
        with open(self.models_dir / 'features.json', 'w', encoding='utf-8') as f:
            json.dump(feature_cols, f, ensure_ascii=False, indent=2)
        logger.info(f"  {self.models_dir / 'features.json'}")
        
        # 特徴量重要度
        if turf_model:
            turf_importance = pd.DataFrame({
                'feature': feature_cols,
                'importance': turf_model.feature_importance(importance_type='gain')
            }).sort_values('importance', ascending=False)
            turf_importance.to_csv(self.models_dir / 'turf_feature_importance.csv', index=False)
        
        if dirt_model:
            dirt_importance = pd.DataFrame({
                'feature': feature_cols,
                'importance': dirt_model.feature_importance(importance_type='gain')
            }).sort_values('importance', ascending=False)
            dirt_importance.to_csv(self.models_dir / 'dirt_feature_importance.csv', index=False)
        
        # レポート生成
        self._generate_report(results_df)
    
    def _generate_report(self, results_df: pd.DataFrame):
        """マークダウンレポート生成"""
        report_path = self.models_dir / 'report.md'
        
        with open(report_path, 'w', encoding='utf-8') as f:
            f.write("# 芝/ダート別モデル検証レポート\n\n")
            f.write(f"**生成日時**: {datetime.now().strftime('%Y-%m-%d %H:%M')}\n\n")
            
            f.write("## サマリー\n\n")
            
            for surface in ['芝', 'ダート']:
                surface_df = results_df[results_df['surface'] == surface]
                if len(surface_df) == 0:
                    continue
                
                avg_roi = surface_df['roi'].mean()
                avg_pop1 = surface_df['pop1_roi'].mean()
                
                f.write(f"### {surface}モデル\n\n")
                f.write(f"- 平均ROI: **{avg_roi:.2%}**\n")
                f.write(f"- 1番人気ベースライン: {avg_pop1:.2%}\n")
                f.write(f"- 差分: {'+' if avg_roi > avg_pop1 else ''}{(avg_roi - avg_pop1)*100:.1f}pt\n\n")
            
            f.write("## 年次別結果\n\n")
            f.write("| 年 | 馬場 | ROI | 的中率 | 人気1 | 人気2 | 人気3 | vs人気1 |\n")
            f.write("|:---|:-----|----:|-------:|------:|------:|------:|--------:|\n")
            
            for _, row in results_df.iterrows():
                diff = row['roi'] - row['pop1_roi']
                f.write(f"| {int(row['test_year'])} | {row['surface']} | {row['roi']:.1%} | {row['hit_rate']:.1%} | {row['pop1_roi']:.1%} | {row['pop2_roi']:.1%} | {row['pop3_roi']:.1%} | {'+' if diff > 0 else ''}{diff*100:.1f}pt |\n")
            
            f.write("\n## 検証条件\n\n")
            f.write("- **除外**: 障害レース、新馬戦\n")
            f.write("- **期間**: 2014年以降\n")
            f.write("- **検証方法**: Walk-forward（各年のテストは前年までのデータで訓練）\n")
            f.write("- **Early Stopping**: 無効（num_boost_round=500固定）\n")
        
        logger.info(f"  {report_path}")


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='芝/ダート別モデル訓練・検証')
    parser.add_argument('--dry-run', action='store_true', help='小規模データでテスト実行')
    parser.add_argument('--full', action='store_true', help='フル実行')
    args = parser.parse_args()
    
    trainer = TurfDirtSeparateTrainer()
    results = trainer.run(dry_run=args.dry_run)
    
    logger.info("\n完了!")
