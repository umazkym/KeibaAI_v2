#!/usr/bin/env python3
"""
ダートモデル予測スクリプト (predict_dirt.py)

【使用方法】
1. 単一レースの予測:
   python scripts/prediction/predict_dirt.py --race-id 202505010101

2. 当日全レースの予測:
   python scripts/prediction/predict_dirt.py --date 2025-01-10

3. 出馬表データからの予測:
   python scripts/prediction/predict_dirt.py --shutuba-file path/to/shutuba.csv

【出力】
- 各馬の勝率予測とランキング
- 推奨馬（Top1）の表示

【注意】
ダートモデルはベースライン特徴量を使用（V3改善は芝専用）

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
import json
import argparse
from datetime import datetime
from typing import Optional, List, Dict

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class DirtPredictor:
    """ダートモデル予測クラス"""
    
    def __init__(self, model_dir: str = 'keibaai/models/turf_dirt_separate'):
        self.model_dir = Path(model_dir)
        self.data_dir = Path('keibaai/data/parsed/parquet')
        
        self.model = self._load_model()
        self.feature_cols = self._load_features()
        self.history_df = self._load_history()
        
    def _load_model(self) -> lgb.Booster:
        model_path = self.model_dir / 'dirt_model.txt'
        if not model_path.exists():
            raise FileNotFoundError(f"モデルファイルが見つかりません: {model_path}")
        
        model = lgb.Booster(model_file=str(model_path))
        logger.info(f"モデル読み込み完了: {model_path}")
        return model
    
    def _load_features(self) -> List[str]:
        features_path = self.model_dir / 'features.json'
        if not features_path.exists():
            raise FileNotFoundError(f"特徴量ファイルが見つかりません: {features_path}")
        
        with open(features_path, 'r', encoding='utf-8') as f:
            features = json.load(f)
        
        logger.info(f"特徴量読み込み完了: {len(features)}個")
        return features
    
    def _load_history(self) -> pd.DataFrame:
        races_path = self.data_dir / 'races/races.parquet'
        if not races_path.exists():
            raise FileNotFoundError(f"レースデータが見つかりません: {races_path}")
        
        df = pd.read_parquet(races_path)
        df['race_date'] = pd.to_datetime(df['race_date'])
        
        # ダートのみ、障害・新馬除外
        df = df[df['track_surface'] == 'ダート']
        df = df[df['race_class'] != '新馬']
        
        df['target'] = (df['finish_position'] == 1).astype(int)
        
        logger.info(f"履歴データ読み込み完了: {len(df):,}件")
        return df
    
    def _generate_features(self, shutuba_df: pd.DataFrame) -> pd.DataFrame:
        """
        特徴量生成（ベースライン版 - V3改善なし）
        
        【リーク防止】
        - 予測対象レースの日付より前のデータのみから統計を計算
        """
        df = shutuba_df.copy()
        
        if 'race_date' in df.columns:
            df['race_date'] = pd.to_datetime(df['race_date'])
            target_date = df['race_date'].max()
        else:
            target_date = pd.Timestamp.now()
        
        # 【リーク防止】予測対象レースより前のデータのみを使用
        hist = self.history_df[self.history_df['race_date'] < target_date].copy()
        
        logger.info(f"  予測対象日: {target_date.date()}")
        logger.info(f"  履歴データ: {len(hist):,}件")
        
        # 基本特徴量
        df['field_size'] = df.groupby('race_id')['horse_number'].transform('count')
        
        track_cond_map = {'良': 0, '稍重': 1, '重': 2, '不良': 3}
        df['track_condition_encoded'] = df['track_condition'].map(track_cond_map).fillna(0)
        
        df['race_number'] = df['race_id'].str[-2:].astype(int)
        df['is_late_race'] = (df['race_number'] >= 8).astype(int)
        
        if len(hist) == 0:
            logger.warning("履歴データがありません。特徴量はNaNになります。")
            df['horse_win_rate'] = np.nan
            df['horse_avg_finish'] = np.nan
            df['jockey_win_rate'] = np.nan
            df['trainer_win_rate'] = np.nan
            df['prev_finish'] = np.nan
            df['days_since_last'] = np.nan
            return df
        
        # 馬の累積勝率
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
        
        # 馬の前走情報
        train_last_race = hist.sort_values(['horse_id', 'race_date']).groupby('horse_id').last()[
            ['finish_position', 'race_date', 'horse_weight']
        ].reset_index()
        train_last_race.columns = ['horse_id', 'prev_finish', 'prev_race_date', 'prev_horse_weight']
        
        # マージ
        df = df.merge(horse_stats[['horse_id', 'horse_win_rate', 'horse_avg_finish']], on='horse_id', how='left')
        df = df.merge(jockey_stats[['jockey_id', 'jockey_win_rate']], on='jockey_id', how='left')
        df = df.merge(trainer_stats[['trainer_id', 'trainer_win_rate']], on='trainer_id', how='left')
        df = df.merge(train_last_race, on='horse_id', how='left')
        
        if 'race_date' in df.columns:
            df['days_since_last'] = (df['race_date'] - df['prev_race_date']).dt.days
        else:
            df['days_since_last'] = np.nan
        
        if 'horse_weight' in df.columns and 'prev_horse_weight' in df.columns:
            df['weight_change'] = df['horse_weight'] - df['prev_horse_weight']
        else:
            df['weight_change'] = np.nan
        
        df['distance_match'] = 0
        
        all_venues = hist['venue'].unique()
        venue_encoder = {v: i for i, v in enumerate(all_venues)}
        df['venue_encoded'] = df['venue'].map(venue_encoder).fillna(-1)
        
        return df
    
    def predict(self, shutuba_df: pd.DataFrame) -> pd.DataFrame:
        """予測実行"""
        logger.info(f"予測開始: {len(shutuba_df)}頭")
        
        # ダートレースのみフィルタ
        if 'track_surface' in shutuba_df.columns:
            shutuba_df = shutuba_df[shutuba_df['track_surface'] == 'ダート'].copy()
            if len(shutuba_df) == 0:
                logger.warning("ダートレースがありません")
                return pd.DataFrame()
        
        df = self._generate_features(shutuba_df)
        
        for col in self.feature_cols:
            if col not in df.columns:
                df[col] = np.nan
        
        X = df[self.feature_cols]
        preds = self.model.predict(X)
        
        df['pred_prob'] = preds
        df['pred_rank'] = df.groupby('race_id')['pred_prob'].rank(ascending=False, method='first')
        
        return df
    
    def predict_race(self, race_id: str) -> pd.DataFrame:
        """単一レース予測"""
        shutuba_path = self.data_dir / 'races/races.parquet'
        df = pd.read_parquet(shutuba_path)
        race_df = df[df['race_id'] == race_id].copy()
        
        if len(race_df) == 0:
            logger.error(f"レースが見つかりません: {race_id}")
            return pd.DataFrame()
        
        return self.predict(race_df)
    
    def predict_date(self, date_str: str) -> pd.DataFrame:
        """指定日の全レース予測"""
        shutuba_path = self.data_dir / 'races/races.parquet'
        df = pd.read_parquet(shutuba_path)
        df['race_date'] = pd.to_datetime(df['race_date'])
        
        target_date = pd.to_datetime(date_str)
        day_df = df[df['race_date'] == target_date].copy()
        
        if len(day_df) == 0:
            logger.error(f"レースが見つかりません: {date_str}")
            return pd.DataFrame()
        
        return self.predict(day_df)


def main():
    parser = argparse.ArgumentParser(description='ダートモデル予測')
    parser.add_argument('--race-id', type=str, help='レースID')
    parser.add_argument('--date', type=str, help='日付 (YYYY-MM-DD)')
    parser.add_argument('--shutuba-file', type=str, help='出馬表ファイルパス')
    args = parser.parse_args()
    
    predictor = DirtPredictor()
    
    if args.race_id:
        result = predictor.predict_race(args.race_id)
    elif args.date:
        result = predictor.predict_date(args.date)
    elif args.shutuba_file:
        shutuba_df = pd.read_csv(args.shutuba_file)
        result = predictor.predict(shutuba_df)
    else:
        parser.print_help()
        return
    
    if len(result) == 0:
        return
    
    # 結果表示
    print("\n" + "="*60)
    print("【ダート予測結果】")
    print("="*60)
    
    for race_id, group in result.groupby('race_id'):
        print(f"\nレース: {race_id}")
        group = group.sort_values('pred_rank')
        for _, row in group.head(5).iterrows():
            print(f"  {int(row['pred_rank'])}位: {int(row['horse_number'])}番 (勝率予測: {row['pred_prob']*100:.1f}%)")


if __name__ == "__main__":
    main()
