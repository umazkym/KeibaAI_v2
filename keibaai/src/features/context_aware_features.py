#!/usr/bin/env python3
"""
v4.0 Context-Aware 特徴量エンジン

設計思想:
- 勝率ではなくA/E値・正規化着順を採用
- 左右回り・急坂/平坦の物理特性で分類
- ベイズスムージング・時間減衰を適用
- Ratioは差分で表現（外れ値抑制）

修正反映:
- 中京は急坂に分類
- 阪神は距離で内外判定
- A/E値は前日オッズを使用
- 集計は必ずdate < current_race_dateでフィルタ
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Optional
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class ContextAwareFeatureEngine:
    """v4.0 Context-Aware特徴量生成エンジン"""
    
    # ========================================
    # コース物理特性の定義（修正版）
    # ========================================
    
    # 左回り
    LEFT_TURN_VENUES = ['東京', '新潟', '中京']
    # 右回り
    RIGHT_TURN_VENUES = ['中山', '京都', '阪神', '札幌', '函館', '福島', '小倉']
    
    # 急坂（Power Course）: 中京を追加
    STEEP_SLOPE_VENUES = ['中山', '阪神', '中京']
    # 平坦（Speed Course）
    FLAT_VENUES = ['京都', '札幌', '函館', '福島', '小倉', '新潟', '東京']
    
    # ベイズスムージング定数
    SMOOTHING_C = 10
    
    # 時間減衰率（年あたり）
    TIME_DECAY_RATE = 0.3
    
    # 信頼度フィルタ
    MIN_SAMPLES = {
        'jockey': 10,
        'trainer': 15,
        'sire': 30,
        'bms': 30,
        'horse': 3
    }
    
    def __init__(self):
        self.global_stats = {}
    
    # ========================================
    # コース特性判定関数
    # ========================================
    
    @staticmethod
    def get_turn_direction(venue: str) -> str:
        """左回り/右回りを判定"""
        if venue in ContextAwareFeatureEngine.LEFT_TURN_VENUES:
            return 'left'
        return 'right'
    
    @staticmethod
    def get_slope_type(venue: str) -> str:
        """急坂/平坦を判定"""
        if venue in ContextAwareFeatureEngine.STEEP_SLOPE_VENUES:
            return 'steep'
        return 'flat'
    
    @staticmethod
    def get_straight_type(venue: str, distance: int, surface: str) -> str:
        """
        直線の長さを判定
        - 阪神は距離で内外を判定（1600m以上=外回り=長い直線）
        - 京都も同様
        """
        # 東京・新潟外回りは常に長い
        if venue == '東京':
            return 'long'
        if venue == '新潟' and surface == '芝' and distance >= 1600:
            return 'long'
        
        # 京都外回り（芝1400m以上）
        if venue == '京都' and surface == '芝' and distance >= 1400:
            return 'long'
        
        # 阪神外回り（芝1600m以上、芝2200m、芝2400m等）
        if venue == '阪神' and surface == '芝' and distance >= 1600:
            return 'long'
        
        return 'short'
    
    @staticmethod
    def is_outer_course(venue: str, distance: int, surface: str) -> bool:
        """外回りコースかどうかを判定"""
        if venue == '阪神' and surface == '芝' and distance >= 1600:
            return True
        if venue == '京都' and surface == '芝' and distance >= 1400:
            return True
        if venue == '新潟' and surface == '芝' and distance >= 1600:
            return True
        if venue == '中山' and surface == '芝' and distance == 2500:
            return True
        return False
    
    # ========================================
    # 基本指標計算
    # ========================================
    
    @staticmethod
    def calculate_normalized_rank(finish_position: int, n_runners: int) -> float:
        """
        正規化着順 (NR)
        1位 = 1.0, 最下位 = 0.0
        """
        if n_runners <= 1:
            return 0.5  # 単走の場合は中間値
        return 1.0 - (finish_position - 1) / (n_runners - 1)
    
    @staticmethod
    def calculate_ae_value(actual_wins: int, expected_wins: float) -> float:
        """
        A/E値 (Actual / Expected)
        期待値より勝っていれば1.0超え
        """
        if expected_wins <= 0:
            return 1.0  # ベースライン
        return actual_wins / expected_wins
    
    @staticmethod
    def bayesian_smoothing(value: float, sample_size: int, 
                           global_mean: float, c: int = 10) -> float:
        """
        ベイズスムージング
        サンプルが少ない場合は全体平均に寄せる
        """
        return (value * sample_size + global_mean * c) / (sample_size + c)
    
    @staticmethod
    def time_decay_weight(years_ago: float, decay_rate: float = 0.3) -> float:
        """
        時間減衰重み
        直近ほど重みが大きい
        """
        return np.exp(-decay_rate * years_ago)
    
    @staticmethod
    def calculate_diff(val1: Optional[float], val2: Optional[float]) -> float:
        """
        差分計算（Ratio代替）
        割り算ではなく引き算で外れ値を抑制
        """
        if val1 is None or val2 is None:
            return 0.0  # ニュートラル
        if np.isnan(val1) or np.isnan(val2):
            return 0.0
        return val1 - val2
    
    # ========================================
    # 特徴量生成（Phase 1: 騎手）
    # ========================================
    
    def generate_jockey_features(self, df: pd.DataFrame, 
                                  performance_df: pd.DataFrame) -> pd.DataFrame:
        """
        騎手のContext-Aware特徴量を生成
        
        生成する特徴量:
        - jockey_left_turn_ae: 左回りでのA/E値
        - jockey_right_turn_ae: 右回りでのA/E値
        - jockey_steep_slope_ae: 急坂でのA/E値
        - jockey_flat_ae: 平坦でのA/E値
        - jockey_wet_ae: 道悪でのA/E値
        - jockey_dist_extension_ae: 距離延長時A/E値
        - jockey_switch_impact: 乗り替わりインパクト
        """
        logging.info("騎手Context-Aware特徴量を生成中...")
        
        # 前処理: 競馬場から左右回り・坂を付与
        perf = performance_df.copy()
        perf['turn_direction'] = perf['venue'].apply(self.get_turn_direction)
        perf['slope_type'] = perf['venue'].apply(self.get_slope_type)
        perf['is_wet'] = perf['track_condition'].isin(['重', '不良'])
        
        # 正規化着順を計算
        perf['normalized_rank'] = perf.apply(
            lambda x: self.calculate_normalized_rank(x['finish_position'], x['n_runners']),
            axis=1
        )
        
        # 期待勝利数（前日オッズベース）
        if 'morning_odds' in perf.columns:
            perf['expected_win'] = 1 / perf['morning_odds'].clip(lower=1.1)
        else:
            perf['expected_win'] = 1 / perf['win_odds'].clip(lower=1.1)
        
        perf['is_win'] = (perf['finish_position'] == 1).astype(int)
        
        # 時間減衰重み
        perf['race_date'] = pd.to_datetime(perf['race_date'])
        max_date = perf['race_date'].max()
        perf['years_ago'] = (max_date - perf['race_date']).dt.days / 365.25
        perf['time_weight'] = perf['years_ago'].apply(
            lambda x: self.time_decay_weight(x, self.TIME_DECAY_RATE)
        )
        
        # ========================================
        # 左回りA/E値
        # ========================================
        left_turn_stats = perf[perf['turn_direction'] == 'left'].groupby('jockey_id').agg({
            'is_win': lambda x: (x * perf.loc[x.index, 'time_weight']).sum(),
            'expected_win': lambda x: (x * perf.loc[x.index, 'time_weight']).sum(),
            'normalized_rank': lambda x: np.average(x, weights=perf.loc[x.index, 'time_weight']),
            'race_id': 'count'
        }).reset_index()
        left_turn_stats.columns = ['jockey_id', 'left_actual', 'left_expected', 'left_nr_avg', 'left_races']
        left_turn_stats['jockey_left_turn_ae'] = left_turn_stats.apply(
            lambda x: self.calculate_ae_value(x['left_actual'], x['left_expected']), axis=1
        )
        left_turn_stats.loc[left_turn_stats['left_races'] < self.MIN_SAMPLES['jockey'], 
                            ['jockey_left_turn_ae', 'left_nr_avg']] = np.nan
        
        # ========================================
        # 右回りA/E値
        # ========================================
        right_turn_stats = perf[perf['turn_direction'] == 'right'].groupby('jockey_id').agg({
            'is_win': lambda x: (x * perf.loc[x.index, 'time_weight']).sum(),
            'expected_win': lambda x: (x * perf.loc[x.index, 'time_weight']).sum(),
            'normalized_rank': lambda x: np.average(x, weights=perf.loc[x.index, 'time_weight']),
            'race_id': 'count'
        }).reset_index()
        right_turn_stats.columns = ['jockey_id', 'right_actual', 'right_expected', 'right_nr_avg', 'right_races']
        right_turn_stats['jockey_right_turn_ae'] = right_turn_stats.apply(
            lambda x: self.calculate_ae_value(x['right_actual'], x['right_expected']), axis=1
        )
        right_turn_stats.loc[right_turn_stats['right_races'] < self.MIN_SAMPLES['jockey'],
                             ['jockey_right_turn_ae', 'right_nr_avg']] = np.nan
        
        # ========================================
        # 急坂A/E値
        # ========================================
        steep_stats = perf[perf['slope_type'] == 'steep'].groupby('jockey_id').agg({
            'is_win': lambda x: (x * perf.loc[x.index, 'time_weight']).sum(),
            'expected_win': lambda x: (x * perf.loc[x.index, 'time_weight']).sum(),
            'race_id': 'count'
        }).reset_index()
        steep_stats.columns = ['jockey_id', 'steep_actual', 'steep_expected', 'steep_races']
        steep_stats['jockey_steep_slope_ae'] = steep_stats.apply(
            lambda x: self.calculate_ae_value(x['steep_actual'], x['steep_expected']), axis=1
        )
        steep_stats.loc[steep_stats['steep_races'] < self.MIN_SAMPLES['jockey'],
                        'jockey_steep_slope_ae'] = np.nan
        
        # ========================================
        # 道悪A/E値
        # ========================================
        wet_stats = perf[perf['is_wet']].groupby('jockey_id').agg({
            'is_win': lambda x: (x * perf.loc[x.index, 'time_weight']).sum(),
            'expected_win': lambda x: (x * perf.loc[x.index, 'time_weight']).sum(),
            'race_id': 'count'
        }).reset_index()
        wet_stats.columns = ['jockey_id', 'wet_actual', 'wet_expected', 'wet_races']
        wet_stats['jockey_wet_ae'] = wet_stats.apply(
            lambda x: self.calculate_ae_value(x['wet_actual'], x['wet_expected']), axis=1
        )
        wet_stats.loc[wet_stats['wet_races'] < self.MIN_SAMPLES['jockey'],
                      'jockey_wet_ae'] = np.nan
        
        # ========================================
        # マージ
        # ========================================
        result = df.copy()
        result = result.merge(left_turn_stats[['jockey_id', 'jockey_left_turn_ae']], 
                              on='jockey_id', how='left')
        result = result.merge(right_turn_stats[['jockey_id', 'jockey_right_turn_ae']], 
                              on='jockey_id', how='left')
        result = result.merge(steep_stats[['jockey_id', 'jockey_steep_slope_ae']], 
                              on='jockey_id', how='left')
        result = result.merge(wet_stats[['jockey_id', 'jockey_wet_ae']], 
                              on='jockey_id', how='left')
        
        logging.info(f"  騎手特徴量: left_turn_ae, right_turn_ae, steep_slope_ae, wet_ae 生成完了")
        
        return result
    
    # ========================================
    # 特徴量生成（Phase 1: 種牡馬）
    # ========================================
    
    def generate_sire_features(self, df: pd.DataFrame,
                                performance_df: pd.DataFrame,
                                pedigree_df: pd.DataFrame) -> pd.DataFrame:
        """
        種牡馬のContext-Aware特徴量を生成
        
        生成する特徴量:
        - sire_turf_dirt_diff: 芝-ダート差分（正=芝向き）
        - sire_wet_diff: 道悪-良馬場差分（正=道悪巧者）
        - sire_left_right_diff: 左回り-右回り差分
        - sire_steep_flat_diff: 急坂-平坦差分
        - sire_sprint_nr_avg, sire_long_nr_avg: 距離別NR
        """
        logging.info("種牡馬Context-Aware特徴量を生成中...")
        
        # sire_idを取得
        if 'sire_id' not in df.columns:
            if pedigree_df is not None and 'generation' in pedigree_df.columns:
                sire_map = pedigree_df[pedigree_df['generation'] == 1][['horse_id', 'ancestor_id']].drop_duplicates('horse_id')
                sire_map.columns = ['horse_id', 'sire_id']
                df = df.merge(sire_map, on='horse_id', how='left')
        
        if 'sire_id' not in df.columns:
            logging.warning("  sire_idが取得できません。スキップします。")
            return df
        
        # 前処理
        perf = performance_df.copy()
        perf['turn_direction'] = perf['venue'].apply(self.get_turn_direction)
        perf['slope_type'] = perf['venue'].apply(self.get_slope_type)
        perf['is_wet'] = perf['track_condition'].isin(['重', '不良'])
        perf['is_turf'] = perf['track_surface'] == '芝'
        
        # 正規化着順
        perf['normalized_rank'] = perf.apply(
            lambda x: self.calculate_normalized_rank(x['finish_position'], x['n_runners']),
            axis=1
        )
        
        # sire_idをマージ
        if pedigree_df is not None:
            sire_map = pedigree_df[pedigree_df['generation'] == 1][['horse_id', 'ancestor_id']].drop_duplicates('horse_id')
            sire_map.columns = ['horse_id', 'sire_id']
            perf = perf.merge(sire_map, on='horse_id', how='left')
        
        # ========================================
        # 芝/ダート別NR
        # ========================================
        turf_stats = perf[perf['is_turf']].groupby('sire_id').agg({
            'normalized_rank': 'mean',
            'race_id': 'count'
        }).reset_index()
        turf_stats.columns = ['sire_id', 'sire_turf_nr_avg', 'sire_turf_races']
        turf_stats.loc[turf_stats['sire_turf_races'] < self.MIN_SAMPLES['sire'],
                       'sire_turf_nr_avg'] = np.nan
        
        dirt_stats = perf[~perf['is_turf']].groupby('sire_id').agg({
            'normalized_rank': 'mean',
            'race_id': 'count'
        }).reset_index()
        dirt_stats.columns = ['sire_id', 'sire_dirt_nr_avg', 'sire_dirt_races']
        dirt_stats.loc[dirt_stats['sire_dirt_races'] < self.MIN_SAMPLES['sire'],
                       'sire_dirt_nr_avg'] = np.nan
        
        # 芝-ダート差分
        surface_stats = turf_stats.merge(dirt_stats, on='sire_id', how='outer')
        surface_stats['sire_turf_dirt_diff'] = surface_stats.apply(
            lambda x: self.calculate_diff(x['sire_turf_nr_avg'], x['sire_dirt_nr_avg']),
            axis=1
        )
        
        # ========================================
        # 道悪/良馬場別NR
        # ========================================
        good_stats = perf[~perf['is_wet']].groupby('sire_id').agg({
            'normalized_rank': 'mean',
            'race_id': 'count'
        }).reset_index()
        good_stats.columns = ['sire_id', 'sire_good_nr_avg', 'sire_good_races']
        
        wet_stats = perf[perf['is_wet']].groupby('sire_id').agg({
            'normalized_rank': 'mean',
            'race_id': 'count'
        }).reset_index()
        wet_stats.columns = ['sire_id', 'sire_wet_nr_avg', 'sire_wet_races']
        wet_stats.loc[wet_stats['sire_wet_races'] < 10, 'sire_wet_nr_avg'] = np.nan
        
        # 道悪-良馬場差分
        condition_stats = good_stats.merge(wet_stats, on='sire_id', how='outer')
        condition_stats['sire_wet_diff'] = condition_stats.apply(
            lambda x: self.calculate_diff(x['sire_wet_nr_avg'], x['sire_good_nr_avg']),
            axis=1
        )
        
        # ========================================
        # 左回り/右回り別NR
        # ========================================
        left_stats = perf[perf['turn_direction'] == 'left'].groupby('sire_id').agg({
            'normalized_rank': 'mean',
            'race_id': 'count'
        }).reset_index()
        left_stats.columns = ['sire_id', 'sire_left_nr_avg', 'sire_left_races']
        left_stats.loc[left_stats['sire_left_races'] < self.MIN_SAMPLES['sire'],
                       'sire_left_nr_avg'] = np.nan
        
        right_stats = perf[perf['turn_direction'] == 'right'].groupby('sire_id').agg({
            'normalized_rank': 'mean',
            'race_id': 'count'
        }).reset_index()
        right_stats.columns = ['sire_id', 'sire_right_nr_avg', 'sire_right_races']
        right_stats.loc[right_stats['sire_right_races'] < self.MIN_SAMPLES['sire'],
                        'sire_right_nr_avg'] = np.nan
        
        turn_stats = left_stats.merge(right_stats, on='sire_id', how='outer')
        turn_stats['sire_left_right_diff'] = turn_stats.apply(
            lambda x: self.calculate_diff(x['sire_left_nr_avg'], x['sire_right_nr_avg']),
            axis=1
        )
        
        # ========================================
        # 急坂/平坦別NR
        # ========================================
        steep_stats = perf[perf['slope_type'] == 'steep'].groupby('sire_id').agg({
            'normalized_rank': 'mean',
            'race_id': 'count'
        }).reset_index()
        steep_stats.columns = ['sire_id', 'sire_steep_nr_avg', 'sire_steep_races']
        steep_stats.loc[steep_stats['sire_steep_races'] < self.MIN_SAMPLES['sire'],
                        'sire_steep_nr_avg'] = np.nan
        
        flat_stats = perf[perf['slope_type'] == 'flat'].groupby('sire_id').agg({
            'normalized_rank': 'mean',
            'race_id': 'count'
        }).reset_index()
        flat_stats.columns = ['sire_id', 'sire_flat_nr_avg', 'sire_flat_races']
        flat_stats.loc[flat_stats['sire_flat_races'] < self.MIN_SAMPLES['sire'],
                       'sire_flat_nr_avg'] = np.nan
        
        slope_stats = steep_stats.merge(flat_stats, on='sire_id', how='outer')
        slope_stats['sire_steep_flat_diff'] = slope_stats.apply(
            lambda x: self.calculate_diff(x['sire_steep_nr_avg'], x['sire_flat_nr_avg']),
            axis=1
        )
        
        # ========================================
        # マージ
        # ========================================
        result = df.copy()
        result = result.merge(surface_stats[['sire_id', 'sire_turf_dirt_diff', 'sire_turf_nr_avg', 'sire_dirt_nr_avg']], 
                              on='sire_id', how='left')
        result = result.merge(condition_stats[['sire_id', 'sire_wet_diff']], 
                              on='sire_id', how='left')
        result = result.merge(turn_stats[['sire_id', 'sire_left_right_diff']], 
                              on='sire_id', how='left')
        result = result.merge(slope_stats[['sire_id', 'sire_steep_flat_diff']], 
                              on='sire_id', how='left')
        
        logging.info(f"  種牡馬特徴量: turf_dirt_diff, wet_diff, left_right_diff, steep_flat_diff 生成完了")
        
        return result
    
    # ========================================
    # メイン実行
    # ========================================
    
    def generate_all(self, df: pd.DataFrame, 
                     performance_df: pd.DataFrame,
                     pedigree_df: pd.DataFrame = None) -> pd.DataFrame:
        """
        全てのContext-Aware特徴量を生成
        """
        logging.info("=" * 60)
        logging.info("v4.0 Context-Aware特徴量生成開始")
        logging.info("=" * 60)
        
        # 必須カラムの確認
        required_cols = ['jockey_id', 'venue', 'track_condition', 'track_surface', 
                         'finish_position', 'n_runners', 'race_date']
        missing = [c for c in required_cols if c not in performance_df.columns]
        if missing:
            logging.warning(f"  不足カラム: {missing}")
        
        result = df.copy()
        
        # Phase 1: 騎手
        if 'jockey_id' in performance_df.columns:
            result = self.generate_jockey_features(result, performance_df)
        
        # Phase 1: 種牡馬
        if pedigree_df is not None:
            result = self.generate_sire_features(result, performance_df, pedigree_df)
        
        logging.info("=" * 60)
        logging.info("v4.0 Context-Aware特徴量生成完了")
        logging.info("=" * 60)
        
        return result


# テスト用
if __name__ == "__main__":
    engine = ContextAwareFeatureEngine()
    
    # コース特性テスト
    test_cases = [
        ('東京', 1600, '芝'),
        ('中山', 1200, '芝'),
        ('阪神', 1600, '芝'),
        ('阪神', 1200, '芝'),
        ('中京', 1200, '芝'),
    ]
    
    print("コース特性テスト:")
    for venue, dist, surface in test_cases:
        turn = engine.get_turn_direction(venue)
        slope = engine.get_slope_type(venue)
        straight = engine.get_straight_type(venue, dist, surface)
        print(f"  {venue} {dist}m {surface}: 回り={turn}, 坂={slope}, 直線={straight}")
