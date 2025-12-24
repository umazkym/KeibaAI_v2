#!/usr/bin/env python3
"""
トラックバイアス特徴量生成モジュール

【目的】
当日のレース傾向（内枠有利、先行有利など）を特徴量として取り込み、
モデルの予測精度を向上させる。

【特徴量】
- bias_inner_advantage: 内枠（1-3枠）の有利度
- bias_front_advantage: 先行馬（4C通過順1-3位）の有利度
- horse_bias_match_score: 馬の特性とバイアスの適合度

【データリーク防止】
- 当該レースの結果を使わず、先行レースのみ参照
- race_number < current_race_number のレースのみ使用
"""

import pandas as pd
import numpy as np
import re
import logging
from typing import Optional, Tuple

logger = logging.getLogger(__name__)


class TrackBiasFeatureGenerator:
    """トラックバイアス特徴量生成クラス"""
    
    def __init__(self, min_races_for_bias: int = 2):
        """
        Args:
            min_races_for_bias: バイアス計算に必要な最小レース数
        """
        self.min_races_for_bias = min_races_for_bias
    
    @staticmethod
    def extract_venue_name(venue_str):
        """開催場名を抽出"""
        if pd.isna(venue_str):
            return None
        match = re.search(r'[0-9]*([^\d]+)[0-9]*', str(venue_str))
        return match.group(1) if match else None
    
    @staticmethod
    def extract_race_number(race_id: str) -> Optional[int]:
        """race_idからレース番号を抽出"""
        try:
            # race_id形式: YYYYMMDDJJRR (例: 202412060111 = 2024年12月6日1回1R)
            return int(str(race_id)[-2:])
        except:
            return None
    
    def calculate_inner_bias(self, prior_races: pd.DataFrame) -> float:
        """
        内枠バイアスを計算
        
        Args:
            prior_races: 先行レースの結果（finish_position, bracket_number を含む）
        
        Returns:
            内枠バイアス (-1.0 〜 +1.0、正なら内枠有利)
        """
        if len(prior_races) == 0:
            return 0.0
        
        # 内枠（1-3枠）の連対率
        inner = prior_races[prior_races['bracket_number'].isin([1, 2, 3])]
        if len(inner) == 0:
            return 0.0
        
        inner_place_rate = (inner['finish_position'] <= 2).mean()
        
        # 期待連対率: 3/8枠 × 約2頭/枠 × 連対2頭/レース / 約14頭 ≈ 0.25
        expected_rate = 0.25
        
        # バイアス = 実績 - 期待
        bias = inner_place_rate - expected_rate
        
        # -1.0〜+1.0にクリップ
        return np.clip(bias, -1.0, 1.0)
    
    def calculate_front_bias(self, prior_races: pd.DataFrame) -> float:
        """
        先行バイアスを計算
        
        Args:
            prior_races: 先行レースの結果（finish_position, passing_order_4 を含む）
        
        Returns:
            先行バイアス (-1.0 〜 +1.0、正なら前残り馬場)
        """
        if len(prior_races) == 0:
            return 0.0
        
        # 4コーナー通過順1-3位（先行馬）
        prior_races = prior_races.copy()
        prior_races['passing_order_4'] = pd.to_numeric(prior_races['passing_order_4'], errors='coerce')
        
        front_runners = prior_races[prior_races['passing_order_4'] <= 3]
        if len(front_runners) == 0:
            return 0.0
        
        # 先行馬の連対率
        front_place_rate = (front_runners['finish_position'] <= 2).mean()
        
        # 期待連対率: 先行3頭/約14頭 × 連対2頭 ≈ 0.43
        expected_rate = 0.43
        
        bias = front_place_rate - expected_rate
        return np.clip(bias, -1.0, 1.0)
    
    def get_prior_races(self, races_df: pd.DataFrame, 
                         venue: str, race_date: pd.Timestamp, 
                         current_race_num: int,
                         track_surface: Optional[str] = None) -> pd.DataFrame:
        """
        先行レースを取得
        
        Args:
            races_df: 全レース結果
            venue: 開催場
            race_date: レース日
            current_race_num: 当該レース番号
            track_surface: 芝/ダート（Noneなら両方）
        
        Returns:
            先行レースのDataFrame
        """
        mask = (
            (races_df['venue_name'] == venue) & 
            (races_df['race_date'] == race_date) &
            (races_df['race_number'] < current_race_num)
        )
        
        if track_surface is not None:
            mask = mask & (races_df['track_surface'] == track_surface)
        
        return races_df[mask].copy()
    
    def generate_bias_features(self, df: pd.DataFrame, races_df: pd.DataFrame) -> pd.DataFrame:
        """
        バイアス特徴量を生成
        
        Args:
            df: 対象レースデータ（出走表）
            races_df: 全レース結果データ
        
        Returns:
            バイアス特徴量が追加されたDataFrame
        """
        logger.info("トラックバイアス特徴量を生成中...")
        
        # 前処理
        races_df = races_df.copy()
        races_df['race_date'] = pd.to_datetime(races_df['race_date'])
        races_df['venue_name'] = races_df['venue'].apply(self.extract_venue_name)
        races_df['race_number'] = races_df['race_id'].apply(self.extract_race_number)
        races_df['finish_position'] = pd.to_numeric(races_df['finish_position'], errors='coerce')
        races_df['bracket_number'] = pd.to_numeric(races_df['bracket_number'], errors='coerce')
        
        df = df.copy()
        df['race_date'] = pd.to_datetime(df['race_date'])
        
        # レース毎にバイアスを計算
        race_ids = df['race_id'].unique()
        bias_data = []
        
        for race_id in race_ids:
            race_rows = df[df['race_id'] == race_id]
            if len(race_rows) == 0:
                continue
            
            first_row = race_rows.iloc[0]
            race_date = first_row['race_date']
            
            # venue取得
            venue = None
            if 'venue' in first_row:
                venue = self.extract_venue_name(first_row['venue'])
            elif 'venue_name' in first_row:
                venue = first_row['venue_name']
            
            race_num = self.extract_race_number(race_id)
            track_surface = first_row.get('track_surface', None)
            
            if venue is None or race_num is None:
                # バイアス計算不可
                bias_data.append({
                    'race_id': race_id,
                    'bias_inner': 0.0,
                    'bias_front': 0.0
                })
                continue
            
            # 先行レース取得
            prior = self.get_prior_races(races_df, venue, race_date, race_num, track_surface)
            
            if len(prior) < self.min_races_for_bias:
                # レース数不足
                bias_data.append({
                    'race_id': race_id,
                    'bias_inner': 0.0,
                    'bias_front': 0.0
                })
                continue
            
            # バイアス計算
            inner_bias = self.calculate_inner_bias(prior)
            front_bias = self.calculate_front_bias(prior)
            
            bias_data.append({
                'race_id': race_id,
                'bias_inner': inner_bias,
                'bias_front': front_bias
            })
        
        # バイアスデータをマージ
        bias_df = pd.DataFrame(bias_data)
        df = df.merge(bias_df, on='race_id', how='left')
        
        # 馬の特性とのマッチングスコア
        df = self._calculate_match_score(df)
        
        logger.info(f"  bias_inner 平均: {df['bias_inner'].mean():.3f}")
        logger.info(f"  bias_front 平均: {df['bias_front'].mean():.3f}")
        logger.info(f"  バイアス計算可能レース: {(df['bias_inner'] != 0).sum() / len(df):.1%}")
        
        return df
    
    def _calculate_match_score(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        馬の特性とバイアスのマッチングスコアを計算
        
        Returns:
            bias_inner_advantage: 内枠有利度 × 馬の枠位置
            bias_front_advantage: 先行有利度 × 馬の脚質
        """
        df = df.copy()
        
        # 内枠マッチング
        df['bracket_number'] = pd.to_numeric(df.get('bracket_number', 0), errors='coerce').fillna(0)
        
        # 枠番を -1 (外枠) 〜 +1 (内枠) にスケーリング
        # 1-3枠: +1, 4-5枠: 0, 6-8枠: -1
        def bracket_to_factor(b):
            if b <= 3:
                return 1.0
            elif b <= 5:
                return 0.0
            else:
                return -1.0
        
        df['bracket_factor'] = df['bracket_number'].apply(bracket_to_factor)
        df['bias_inner_advantage'] = df['bias_inner'] * df['bracket_factor']
        
        # 先行マッチング（過去の平均4C通過順を使用）
        if 'horse_avg_position_4c' in df.columns:
            # 低い値（先行）→ +1, 高い値（追込）→ -1
            avg_pos = df['horse_avg_position_4c'].fillna(0.5)
            df['front_factor'] = 1.0 - 2.0 * avg_pos  # 0→+1, 1→-1
            df['front_factor'] = df['front_factor'].clip(-1, 1)
        else:
            df['front_factor'] = 0.0
        
        df['bias_front_advantage'] = df['bias_front'] * df['front_factor']
        
        # 不要な中間カラムを削除
        df = df.drop(columns=['bracket_factor', 'front_factor'], errors='ignore')
        
        return df


def generate_track_bias_features(df: pd.DataFrame, races_df: pd.DataFrame) -> pd.DataFrame:
    """
    トラックバイアス特徴量を生成するユーティリティ関数
    
    Args:
        df: 対象レースデータ
        races_df: 全レース結果
        
    Returns:
        特徴量追加済みDataFrame
    """
    generator = TrackBiasFeatureGenerator()
    return generator.generate_bias_features(df, races_df)
