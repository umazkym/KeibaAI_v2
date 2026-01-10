#!/usr/bin/env python3
"""
芝モデル予測スクリプト (predict_turf.py)

【使用方法】
1. 単一レースの予測:
   python scripts/prediction/predict_turf.py --race-id 202505010101

2. 当日全レースの予測:
   python scripts/prediction/predict_turf.py --date 2025-01-10

3. 出馬表データからの予測:
   python scripts/prediction/predict_turf.py --shutuba-file path/to/shutuba.csv

【出力】
- 各馬の勝率予測とランキング
- 推奨馬（Top1）の表示

Author: KeibaAI Development Team
Date: 2026-01-10
"""

import sys
from pathlib import Path

# プロジェクトルートをsys.pathに追加
project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(project_root))

import pandas as pd
import numpy as np
import lightgbm as lgb
import logging
import json
from datetime import datetime
from typing import Optional, List, Dict

# ログ設定
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class TurfPredictor:
    """芝モデル予測クラス"""
    
    def __init__(self, model_dir: str = 'keibaai/models/turf_dirt_separate'):
        self.model_dir = Path(model_dir)
        self.data_dir = Path('keibaai/data/parsed/parquet')
        
        # モデル読み込み
        self.model = self._load_model()
        self.feature_cols = self._load_features()
        
        # 過去データ読み込み（統計計算用）
        self.history_df = self._load_history()
        
    def _load_model(self) -> lgb.Booster:
        """モデル読み込み"""
        model_path = self.model_dir / 'turf_model.txt'
        if not model_path.exists():
            raise FileNotFoundError(f"モデルファイルが見つかりません: {model_path}")
        
        model = lgb.Booster(model_file=str(model_path))
        logger.info(f"モデル読み込み完了: {model_path}")
        return model
    
    def _load_features(self) -> List[str]:
        """特徴量リスト読み込み"""
        features_path = self.model_dir / 'features.json'
        if not features_path.exists():
            raise FileNotFoundError(f"特徴量ファイルが見つかりません: {features_path}")
        
        with open(features_path, 'r', encoding='utf-8') as f:
            features = json.load(f)
        
        logger.info(f"特徴量読み込み完了: {len(features)}個")
        return features
    
    def _load_history(self) -> pd.DataFrame:
        """過去レースデータ読み込み（統計計算用）"""
        races_path = self.data_dir / 'races/races.parquet'
        if not races_path.exists():
            raise FileNotFoundError(f"レースデータが見つかりません: {races_path}")
        
        df = pd.read_parquet(races_path)
        df['race_date'] = pd.to_datetime(df['race_date'])
        
        # 芝のみ、障害・新馬除外
        df = df[df['track_surface'] == '芝']
        df = df[df['race_class'] != '新馬']
        
        # 目的変数計算
        df['target'] = (df['finish_position'] == 1).astype(int)
        
        logger.info(f"履歴データ読み込み完了: {len(df):,}件")
        return df
    
    def _generate_features(self, shutuba_df: pd.DataFrame) -> pd.DataFrame:
        """
        出馬表データから特徴量を生成
        
        【リーク防止】
        - 予測対象レースの日付より前のデータのみから統計を計算
        - 当日レースの結果は絶対に使用しない
        """
        df = shutuba_df.copy()
        
        # 予測対象レースの日付を取得
        if 'race_date' in df.columns:
            df['race_date'] = pd.to_datetime(df['race_date'])
            target_date = df['race_date'].max()
        else:
            # race_dateがない場合は現在日時を使用（実運用時）
            target_date = pd.Timestamp.now()
        
        # 【リーク防止】予測対象レースより前のデータのみを使用
        hist = self.history_df[self.history_df['race_date'] < target_date].copy()
        
        logger.info(f"  予測対象日: {target_date.date()}")
        logger.info(f"  履歴データ: {len(hist):,}件 (～{hist['race_date'].max().date() if len(hist) > 0 else 'N/A'})")
        
        # 基本特徴量
        df['field_size'] = df.groupby('race_id')['horse_number'].transform('count')
        
        track_cond_map = {'良': 0, '稍重': 1, '重': 2, '不良': 3}
        df['track_condition_encoded'] = df['track_condition'].map(track_cond_map).fillna(0)
        
        # レース番号
        df['race_number'] = df['race_id'].str[-2:].astype(int)
        df['is_late_race'] = (df['race_number'] >= 8).astype(int)
        
        # --- 過去データから統計を計算（予測対象日より前のみ）---
        if len(hist) == 0:
            logger.warning("履歴データがありません。特徴量はNaNになります。")
            df['horse_win_rate'] = np.nan
            df['horse_avg_finish'] = np.nan
            df['jockey_win_rate'] = np.nan
            df['trainer_win_rate'] = np.nan
            df['prev_finish'] = np.nan
            df['days_since_last'] = np.nan
            df['weight_change'] = np.nan
            df['distance_match'] = 0
            df['venue_encoded'] = 0
            return df
        
        # 馬の累積勝率（予測対象日より前のデータのみ）
        horse_stats = hist.groupby('horse_id').agg({
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
        jockey_stats = hist.groupby('jockey_id').agg({
            'target': ['sum', 'count']
        }).reset_index()
        jockey_stats.columns = ['jockey_id', 'jockey_cum_wins', 'jockey_cum_races']
        jockey_stats['jockey_win_rate'] = np.where(
            jockey_stats['jockey_cum_races'] >= 10,
            jockey_stats['jockey_cum_wins'] / jockey_stats['jockey_cum_races'],
            np.nan
        )
        
        # 調教師の累積勝率
        trainer_stats = hist.groupby('trainer_id').agg({
            'target': ['sum', 'count']
        }).reset_index()
        trainer_stats.columns = ['trainer_id', 'trainer_cum_wins', 'trainer_cum_races']
        trainer_stats['trainer_win_rate'] = np.where(
            trainer_stats['trainer_cum_races'] >= 10,
            trainer_stats['trainer_cum_wins'] / trainer_stats['trainer_cum_races'],
            np.nan
        )
        
        # 馬の前走情報（予測対象日より前の最新レース）
        train_last_race = hist.sort_values(['horse_id', 'race_date']).groupby('horse_id').last()[
            ['finish_position', 'race_date', 'horse_weight']
        ].reset_index()
        train_last_race.columns = ['horse_id', 'prev_finish', 'prev_race_date', 'prev_horse_weight']
        
        # マージ
        df = df.merge(horse_stats[['horse_id', 'horse_win_rate', 'horse_avg_finish']], on='horse_id', how='left')
        df = df.merge(jockey_stats[['jockey_id', 'jockey_win_rate']], on='jockey_id', how='left')
        df = df.merge(trainer_stats[['trainer_id', 'trainer_win_rate']], on='trainer_id', how='left')
        df = df.merge(train_last_race, on='horse_id', how='left')
        
        # 前走からの日数
        if 'race_date' in df.columns:
            df['days_since_last'] = (df['race_date'] - df['prev_race_date']).dt.days
        else:
            df['days_since_last'] = np.nan
        
        # 馬体重変化
        if 'horse_weight' in df.columns and 'prev_horse_weight' in df.columns:
            df['weight_change'] = df['horse_weight'] - df['prev_horse_weight']
        else:
            df['weight_change'] = np.nan
        
        # 距離適性（簡易）
        df['distance_match'] = 0
        
        # 競馬場エンコーディング
        all_venues = hist['venue'].unique()
        venue_encoder = {v: i for i, v in enumerate(all_venues)}
        df['venue_encoded'] = df['venue'].map(venue_encoder).fillna(-1)
        
        # === V3改善特徴量 ===
        # 改善A: prev_finish対数変換
        df['prev_finish_log'] = np.log1p(df['prev_finish'].fillna(10))
        
        # 改善B: 最適休養からの乖離
        df['rest_optimal_gap'] = np.abs(df['days_since_last'].fillna(60) - 45)
        df['is_short_rest'] = (df['days_since_last'].fillna(60) <= 21).astype(int)
        
        # 改善C: 騎手/調教師勝率対数変換
        df['jockey_win_rate_log'] = np.log1p(df['jockey_win_rate'].fillna(0.05) * 100) / 5
        df['trainer_win_rate_log'] = np.log1p(df['trainer_win_rate'].fillna(0.05) * 100) / 5
        
        # 改善D: 復活馬フラグ
        df['is_comeback'] = (
            (df['prev_finish'].fillna(99) >= 8) & 
            (df['horse_avg_finish'].fillna(99) <= 5)
        ).astype(int)
        
        return df
    
    def predict(self, shutuba_df: pd.DataFrame) -> pd.DataFrame:
        """
        予測実行
        
        Args:
            shutuba_df: 出馬表データ（race_id, horse_id, horse_number等を含む）
        
        Returns:
            予測結果DataFrame（pred_prob, pred_rankを追加）
        """
        logger.info(f"予測開始: {len(shutuba_df)}頭")
        
        # 芝レースのみフィルタ
        if 'track_surface' in shutuba_df.columns:
            shutuba_df = shutuba_df[shutuba_df['track_surface'] == '芝'].copy()
            if len(shutuba_df) == 0:
                logger.warning("芝レースがありません")
                return pd.DataFrame()
        
        # 特徴量生成
        df = self._generate_features(shutuba_df)
        
        # 欠損特徴量の補完
        for col in self.feature_cols:
            if col not in df.columns:
                df[col] = np.nan
                logger.warning(f"特徴量 {col} が見つかりません。NaNで補完します。")
        
        # 予測
        X = df[self.feature_cols]
        preds = self.model.predict(X)
        
        # 結果を追加
        df['pred_prob'] = preds
        df['pred_rank'] = df.groupby('race_id')['pred_prob'].rank(ascending=False, method='first')
        
        # 出力カラム
        output_cols = ['race_id', 'horse_number', 'horse_name', 'pred_prob', 'pred_rank']
        output_cols = [c for c in output_cols if c in df.columns]
        
        result = df[output_cols].sort_values(['race_id', 'pred_rank'])
        
        logger.info(f"予測完了: {len(result)}頭")
        return result
    
    def predict_race(self, race_id: str) -> pd.DataFrame:
        """特定レースIDの予測"""
        # 出馬表データを取得
        shutuba_path = self.data_dir / 'races/races.parquet'
        df = pd.read_parquet(shutuba_path)
        
        race_df = df[df['race_id'] == race_id].copy()
        if len(race_df) == 0:
            logger.error(f"レースが見つかりません: {race_id}")
            return pd.DataFrame()
        
        return self.predict(race_df)
    
    def predict_date(self, date_str: str) -> pd.DataFrame:
        """特定日の全レース予測"""
        shutuba_path = self.data_dir / 'races/races.parquet'
        df = pd.read_parquet(shutuba_path)
        df['race_date'] = pd.to_datetime(df['race_date'])
        
        target_date = pd.to_datetime(date_str)
        date_df = df[df['race_date'].dt.date == target_date.date()].copy()
        
        if len(date_df) == 0:
            logger.error(f"レースが見つかりません: {date_str}")
            return pd.DataFrame()
        
        # 芝レースのみ
        date_df = date_df[date_df['track_surface'] == '芝']
        
        return self.predict(date_df)


def print_predictions(result: pd.DataFrame):
    """予測結果を整形して表示"""
    if len(result) == 0:
        print("予測結果がありません")
        return
    
    for race_id in result['race_id'].unique():
        race_df = result[result['race_id'] == race_id]
        
        print(f"\n{'='*50}")
        print(f"レースID: {race_id}")
        print(f"{'='*50}")
        
        print(f"{'順位':>4} {'馬番':>4} {'馬名':>12} {'勝率':>8}")
        print("-" * 40)
        
        for _, row in race_df.iterrows():
            rank = int(row['pred_rank'])
            num = int(row['horse_number']) if pd.notna(row.get('horse_number')) else '-'
            name = row.get('horse_name', '-')
            prob = row['pred_prob']
            
            marker = '★' if rank == 1 else ''
            print(f"{rank:>4} {num:>4} {name:>12} {prob:>7.1%} {marker}")
        
        # Top1推奨
        top1 = race_df[race_df['pred_rank'] == 1].iloc[0]
        print(f"\n推奨: {int(top1['horse_number'])}番 {top1.get('horse_name', '')} ({top1['pred_prob']:.1%})")


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='芝モデル予測')
    parser.add_argument('--race-id', type=str, help='予測対象のレースID')
    parser.add_argument('--date', type=str, help='予測対象の日付 (YYYY-MM-DD)')
    parser.add_argument('--shutuba-file', type=str, help='出馬表CSVファイル')
    args = parser.parse_args()
    
    predictor = TurfPredictor()
    
    if args.race_id:
        result = predictor.predict_race(args.race_id)
        print_predictions(result)
    elif args.date:
        result = predictor.predict_date(args.date)
        print_predictions(result)
    elif args.shutuba_file:
        shutuba_df = pd.read_csv(args.shutuba_file)
        result = predictor.predict(shutuba_df)
        print_predictions(result)
    else:
        # デモ: 最新の芝レースを予測
        print("引数が指定されていません。最新の芝レースで予測をデモします。")
        
        races_path = Path('keibaai/data/parsed/parquet/races/races.parquet')
        df = pd.read_parquet(races_path)
        df = df[df['track_surface'] == '芝']
        df = df[df['race_class'] != '新馬']
        df['race_date'] = pd.to_datetime(df['race_date'])
        
        # 最新レース
        latest_date = df['race_date'].max()
        latest_races = df[df['race_date'] == latest_date]
        
        # 1レースのみ予測
        sample_race_id = latest_races['race_id'].iloc[0]
        result = predictor.predict_race(sample_race_id)
        print_predictions(result)
