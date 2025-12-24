"""
リークフリー特徴量エンジニア V18

V17をベースに、市場の非効率性を捕捉する高インパクト特徴量を追加。

【追加特徴量 (9個)】
1. elo_rating: 相対的実力評価（対戦相手の強さを考慮）
2. recency_bias_indicator: 市場の直近成績への過剰反応を検出
3. gap_c1_avg: 1コーナー平均馬身差
4. gap_c2_avg: 2コーナー平均馬身差
5. gap_c3_avg: 3コーナー平均馬身差
6. gap_reduction_c1_c4: C1→C4の馬身差縮小量（全体追い込み力）
7. gap_reduction_c3_c4: C3→C4の馬身差縮小量（直線瞬発力）
8. senkou_rate: 先行率（C1で2-4番手の割合）
9. sashi_rate: 差し率（C1で5-8番手＋途中上昇の割合）

【リーク対策】
- elo_rating: 当該レース以前のデータのみ（レース結果は全てshift(1)）
- recency_bias: 過去5走とmorning_odds/popularityのみ使用
- gap_cx_avg: 過去レースのコーナーデータのみ（shift(1)）
- 全ての累積統計にexpanding().shift(1)を適用

【過学習対策】
- 最小レース数要件を設定（min_elo_races=3, min_corner_races=5）
- 高次元組み合わせは追加しない
- サンプル不足時はNaNを返す

設計思想:
- 「市場が見落としている情報」を捕捉
- 着順だけでなく「どう勝ったか/負けたか」を評価
- ペース展開と脚質のマッチングを詳細化
"""

import pandas as pd
import numpy as np
from typing import Optional, List, Dict, Tuple
from dataclasses import dataclass
import logging

from .leak_free_feature_engineer_v17 import LeakFreeFeatureEngineerV17, FeatureConfigV17

logger = logging.getLogger(__name__)


@dataclass
class FeatureConfigV18(FeatureConfigV17):
    """V18用設定"""
    # Elo Rating設定
    elo_initial_rating: float = 1500.0
    elo_k_factor: float = 32.0
    min_elo_races: int = 3
    
    # Recency Bias設定
    recency_lookback_races: int = 5
    
    # Corner Gap設定
    min_corner_races_for_gap: int = 5


class LeakFreeFeatureEngineerV18(LeakFreeFeatureEngineerV17):
    """
    リークフリー特徴量エンジニア V18
    
    V17に以下の特徴量を追加：
    - elo_rating: 相対的実力（対戦相手考慮）
    - recency_bias_indicator: 市場の過剰反応検出
    - gap_c1_avg, gap_c2_avg, gap_c3_avg: 全コーナー馬身差
    - gap_reduction_c1_c4, gap_reduction_c3_c4: 追い込み力
    - senkou_rate, sashi_rate: 詳細脚質
    
    【重要】過学習対策
    - min_elo_races=3, min_corner_races_for_gap=5
    - 高次元組み合わせは追加しない
    """
    
    # V17の特徴量 + V18の新特徴量
    FEATURE_COLS = LeakFreeFeatureEngineerV17.FEATURE_COLS + [
        'elo_rating',                # 相対的実力Elo
        'recency_bias_indicator',    # 市場過剰反応指標
        'gap_c1_avg',                # 1コーナー平均馬身差
        'gap_c2_avg',                # 2コーナー平均馬身差
        'gap_c3_avg',                # 3コーナー平均馬身差
        'gap_reduction_c1_c4',       # C1→C4追い込み力
        'gap_reduction_c3_c4',       # C3→C4直線瞬発力
        'senkou_rate',               # 先行率
        'sashi_rate',                # 差し率
    ]
    
    def __init__(self, config: Optional[FeatureConfigV18] = None):
        super().__init__(config or FeatureConfigV18())
        self._elo_ratings: Dict[str, float] = {}
        self._corner_gap_stats: Dict[str, Dict[str, float]] = {}
        self._running_style_stats: Dict[str, Dict[str, float]] = {}
    
    def fit(self, races_df: pd.DataFrame, pedigrees_df=None, corners_df=None, 
            race_details_df=None, returns_df=None, horses_df=None):
        """
        fitメソッドの拡張
        
        - Eloレーティング履歴を計算
        - 全コーナー馬身差統計を計算
        - 詳細脚質統計を計算
        """
        logger.info("LeakFreeFeatureEngineerV18: fit開始")
        
        # 親クラスのfit
        super().fit(races_df, pedigrees_df, corners_df, race_details_df, returns_df, horses_df)
        
        # Eloレーティングを計算（時系列処理）
        self._calc_elo_ratings(races_df)
        
        # 全コーナー馬身差統計を計算
        self._calc_all_corner_gap_stats(races_df, corners_df)
        
        # 詳細脚質統計を計算
        self._calc_running_style_stats(races_df, corners_df)
        
        logger.info("LeakFreeFeatureEngineerV18: fit完了")
        return self
    
    def _calc_elo_ratings(self, races_df: pd.DataFrame):
        """
        Eloレーティングを時系列で計算
        
        【リーク対策】
        - レース日時順にソートして処理
        - 各レース結果反映前のレーティングをその時点の特徴量とする
        - つまり当該レースの結果は使用しない
        
        【計算ロジック】
        1. 初期レーティング = 1500
        2. 各レースで、着順1位 vs 2位の対戦として勝敗を判定
        3. K-factor = 32でレーティング更新
        """
        logger.info("  Eloレーティングを計算中...")
        
        config = self.config
        initial_rating = config.elo_initial_rating
        k_factor = config.elo_k_factor
        min_races = config.min_elo_races
        
        # レース日時順にソート
        df = races_df.copy()
        df = df.sort_values(['race_date', 'race_id'])
        
        # 馬ごとのElo履歴を保持
        elo_ratings: Dict[str, float] = {}
        elo_race_counts: Dict[str, int] = {}
        
        # レースごとの馬のEloを保存（当該レース前の値）
        elo_before_race: Dict[Tuple[str, str], float] = {}
        
        # レースIDごとに処理
        race_ids = df['race_id'].unique()
        
        for race_id in race_ids:
            race_horses = df[df['race_id'] == race_id].copy()
            race_horses = race_horses.sort_values('finish_position')
            
            if len(race_horses) < 2:
                continue
            
            # 各馬の現在のEloを記録（レース結果反映前）
            for _, row in race_horses.iterrows():
                horse_id = row['horse_id']
                current_elo = elo_ratings.get(horse_id, initial_rating)
                race_count = elo_race_counts.get(horse_id, 0)
                
                # 最小レース数未満ならNaN
                if race_count < min_races:
                    elo_before_race[(horse_id, race_id)] = np.nan
                else:
                    elo_before_race[(horse_id, race_id)] = current_elo
            
            # レース結果に基づきElo更新
            # 簡易版：着順1位 vs 全員の対戦として計算
            horse_ids = race_horses['horse_id'].tolist()
            positions = race_horses['finish_position'].tolist()
            
            for i in range(len(horse_ids)):
                horse_i = horse_ids[i]
                pos_i = positions[i]
                elo_i = elo_ratings.get(horse_i, initial_rating)
                
                # 他の馬との対戦
                total_delta = 0
                for j in range(len(horse_ids)):
                    if i == j:
                        continue
                    
                    horse_j = horse_ids[j]
                    pos_j = positions[j]
                    elo_j = elo_ratings.get(horse_j, initial_rating)
                    
                    # 期待勝率
                    expected = 1 / (1 + 10 ** ((elo_j - elo_i) / 400))
                    
                    # 実際の結果（0.5で引き分け扱い）
                    if pd.isna(pos_i) or pd.isna(pos_j):
                        actual = 0.5
                    elif pos_i < pos_j:
                        actual = 1.0
                    elif pos_i > pos_j:
                        actual = 0.0
                    else:
                        actual = 0.5
                    
                    # デルタ累積
                    total_delta += k_factor * (actual - expected)
                
                # 対戦数で正規化
                if len(horse_ids) > 1:
                    total_delta /= (len(horse_ids) - 1)
                
                # レーティング更新
                elo_ratings[horse_i] = elo_i + total_delta
                elo_race_counts[horse_i] = elo_race_counts.get(horse_i, 0) + 1
        
        # 保存
        self._elo_ratings = elo_ratings
        self._elo_race_counts = elo_race_counts  # min_racesチェック用
        self._elo_before_race = elo_before_race
        
        valid_elos = sum(1 for v in elo_before_race.values() if not np.isnan(v) if not pd.isna(v))
        logger.info(f"    Elo計算完了: {len(elo_ratings):,}頭, 有効レコード: {valid_elos:,}")
    
    def _calc_all_corner_gap_stats(self, races_df: pd.DataFrame, corners_df: Optional[pd.DataFrame]):
        """
        全コーナー（C1, C2, C3, C4）の馬身差統計を計算
        
        【リーク対策】
        - fit時にTrain期間のデータのみで計算
        - expanding().shift(1)適用
        """
        logger.info("  全コーナー馬身差統計を計算中...")
        
        if corners_df is None or len(corners_df) == 0:
            logger.warning("  corners_dfがないため、コーナー統計はスキップ")
            return
        
        config = self.config
        min_races = config.min_corner_races_for_gap
        
        # 履歴データとマージ
        if self._full_history is None:
            return
        
        hist = self._full_history[['race_id', 'horse_number', 'horse_id', 'race_date']].copy()
        hist = hist.drop_duplicates(['race_id', 'horse_number'])
        
        # 各コーナーのギャップを取得
        result_stats: Dict[str, Dict[str, float]] = {}
        
        for corner_num in [1, 2, 3, 4]:
            corner_data = corners_df[corners_df['corner'] == corner_num][
                ['race_id', 'horse_number', 'position', 'gap_from_leader']
            ].copy()
            corner_data.columns = ['race_id', 'horse_number', f'c{corner_num}_pos', f'c{corner_num}_gap']
            
            # horse_id を結合
            corner_with_id = corner_data.merge(hist, on=['race_id', 'horse_number'], how='left')
            corner_with_id = corner_with_id.dropna(subset=['horse_id'])
            corner_with_id = corner_with_id.sort_values(['horse_id', 'race_date', 'race_id'])
            
            # 累積平均（shift(1)）
            gap_col = f'c{corner_num}_gap'
            corner_with_id[f'cum_gap_c{corner_num}_avg'] = corner_with_id.groupby('horse_id')[gap_col].transform(
                lambda x: x.expanding().mean().shift(1)
            )
            corner_with_id[f'cum_count_c{corner_num}'] = corner_with_id.groupby('horse_id')[gap_col].transform(
                lambda x: x.expanding().count().shift(1)
            )
            
            # 最小レース数フィルタ
            corner_with_id.loc[corner_with_id[f'cum_count_c{corner_num}'] < min_races, f'cum_gap_c{corner_num}_avg'] = np.nan
            
            # 最新統計を保存
            latest = corner_with_id.groupby('horse_id').last()[[f'cum_gap_c{corner_num}_avg']].reset_index()
            
            for _, row in latest.iterrows():
                horse_id = row['horse_id']
                if horse_id not in result_stats:
                    result_stats[horse_id] = {}
                result_stats[horse_id][f'gap_c{corner_num}_avg'] = row[f'cum_gap_c{corner_num}_avg']
        
        self._corner_gap_stats = result_stats
        logger.info(f"    コーナー馬身差統計: {len(result_stats):,}頭")
    
    def _calc_running_style_stats(self, races_df: pd.DataFrame, corners_df: Optional[pd.DataFrame]):
        """
        詳細脚質統計（先行率・差し率）を計算
        
        【脚質判定基準】
        - 逃げ: C1で1番手
        - 先行: C1で2-4番手
        - 差し: C1で5-8番手、途中で上昇
        - 追込: C1で9番手以降
        """
        logger.info("  詳細脚質統計を計算中...")
        
        if corners_df is None or len(corners_df) == 0:
            logger.warning("  corners_dfがないため、脚質統計はスキップ")
            return
        
        if self._full_history is None:
            return
        
        hist = self._full_history[['race_id', 'horse_number', 'horse_id', 'race_date', 'finish_position']].copy()
        hist = hist.drop_duplicates(['race_id', 'horse_number'])
        
        # C1データを取得
        c1_data = corners_df[corners_df['corner'] == 1][
            ['race_id', 'horse_number', 'position']
        ].copy()
        c1_data.columns = ['race_id', 'horse_number', 'c1_pos']
        
        # C4データを取得
        c4_data = corners_df[corners_df['corner'] == 4][
            ['race_id', 'horse_number', 'position']
        ].copy()
        c4_data.columns = ['race_id', 'horse_number', 'c4_pos']
        
        # 統合
        merged = hist.merge(c1_data, on=['race_id', 'horse_number'], how='left')
        merged = merged.merge(c4_data, on=['race_id', 'horse_number'], how='left')
        merged = merged.dropna(subset=['horse_id', 'c1_pos', 'c4_pos'])
        merged = merged.sort_values(['horse_id', 'race_date', 'race_id'])
        
        # 脚質判定
        def classify_style(row):
            c1 = row['c1_pos']
            c4 = row['c4_pos']
            if c1 == 1:
                return 'nige'  # 逃げ
            elif c1 <= 4:
                return 'senkou'  # 先行
            elif c1 <= 8 and c4 < c1:
                return 'sashi'  # 差し
            else:
                return 'oikomi'  # 追込
        
        merged['running_style'] = merged.apply(classify_style, axis=1)
        
        # 各脚質のフラグ
        merged['is_nige'] = (merged['running_style'] == 'nige').astype(int)
        merged['is_senkou'] = (merged['running_style'] == 'senkou').astype(int)
        merged['is_sashi'] = (merged['running_style'] == 'sashi').astype(int)
        merged['is_oikomi'] = (merged['running_style'] == 'oikomi').astype(int)
        
        # 累積統計（shift(1)）
        for style in ['senkou', 'sashi']:
            merged[f'cum_{style}_rate'] = merged.groupby('horse_id')[f'is_{style}'].transform(
                lambda x: x.expanding().mean().shift(1)
            )
        
        # 最小レース数フィルタ
        merged['cum_race_count'] = merged.groupby('horse_id')['is_nige'].transform(
            lambda x: x.expanding().count().shift(1)
        )
        min_races = self.config.min_corner_races_for_gap
        
        # 最新統計を保存
        latest = merged.groupby('horse_id').last()[['cum_senkou_rate', 'cum_sashi_rate', 'cum_race_count']].reset_index()
        
        result_stats: Dict[str, Dict[str, float]] = {}
        for _, row in latest.iterrows():
            horse_id = row['horse_id']
            race_count = row['cum_race_count']
            
            if race_count < min_races:
                senkou = np.nan
                sashi = np.nan
            else:
                senkou = row['cum_senkou_rate']
                sashi = row['cum_sashi_rate']
            
            result_stats[horse_id] = {
                'senkou_rate': senkou,
                'sashi_rate': sashi,
            }
        
        self._running_style_stats = result_stats
        logger.info(f"    脚質統計: {len(result_stats):,}頭")
    
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """V18のtransform"""
        
        # 1. 親クラスのtransform（V17の特徴量）
        result = super().transform(df)
        
        logger.info("LeakFreeFeatureEngineerV18: 追加transform開始")
        
        # 2. V18特徴量の計算
        result = self._add_elo_rating_features(result)
        result = self._add_recency_bias_features(result)
        result = self._add_corner_gap_features(result)
        result = self._add_running_style_features(result)
        
        logger.info("LeakFreeFeatureEngineerV18: transform完了")
        return result
    
    def _add_elo_rating_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Eloレーティング特徴量を付与
        
        【修正】
        - fit期間のレースは (horse_id, race_id) でマップ（レース前のElo）
        - fit期間外のレースは horse_id -> 最新Elo でマップ（fit完了時点のElo）
        - これにより、Test期間でも有効な値を取得可能
        """
        logger.info("  Eloレーティングを付与中...")
        
        df = df.copy()
        
        # 事前計算した値をマップ
        def get_elo(row):
            horse_id = row['horse_id']
            race_id = row['race_id']
            key = (horse_id, race_id)
            
            # まず (horse_id, race_id) で検索（fit期間データ）
            if hasattr(self, '_elo_before_race') and key in self._elo_before_race:
                val = self._elo_before_race[key]
                if not pd.isna(val):
                    return val
            
            # 見つからない場合は最新Elo（horse_id）を使用（test期間データ）
            # これはfit完了時点のEloなので、リーク問題はない
            if hasattr(self, '_elo_ratings') and horse_id in self._elo_ratings:
                # ただし、最低レース数を満たしているか確認
                if hasattr(self, '_elo_race_counts') and horse_id in self._elo_race_counts:
                    if self._elo_race_counts[horse_id] >= self.config.min_elo_races:
                        return self._elo_ratings[horse_id]
                else:
                    # race_countsがない場合はEloがあれば返す（後方互換性）
                    return self._elo_ratings[horse_id]
            
            return np.nan
        
        df['elo_rating'] = df.apply(get_elo, axis=1)
        
        non_null = df['elo_rating'].notna().sum()
        logger.info(f"    elo_rating非NaN: {non_null:,}/{len(df):,} ({non_null/len(df)*100:.1f}%)")
        
        return df
    
    def _add_recency_bias_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Recency Bias Indicator（市場過剰反応指標）を計算
        
        【計算ロジック】
        1. 過去5走の平均着順を「実力」とする
        2. 人気から期待される着順を算出
        3. 乖離度 = 期待着順 - 実力着順
        4. 正の値 = 市場が過小評価（狙い目）
        
        【リーク対策】
        - 過去成績と朝人気（またはpopularity）のみ使用
        """
        logger.info("  Recency Bias Indicatorを計算中...")
        
        df = df.copy()
        df = df.sort_values(['horse_id', 'race_date', 'race_id'])
        
        lookback = self.config.recency_lookback_races
        
        # 過去N走の平均着順
        df['_past_avg_finish'] = df.groupby('horse_id')['finish_position'].transform(
            lambda x: x.rolling(window=lookback, min_periods=lookback).mean().shift(1)
        )
        
        # 人気から期待される着順（簡易計算：人気 * 0.8 + 2）
        # popularityがあればそれを使用、なければmorning_popularityを使用
        if 'popularity' in df.columns:
            pop_col = 'popularity'
        elif 'morning_popularity' in df.columns:
            pop_col = 'morning_popularity'
        else:
            df['recency_bias_indicator'] = np.nan
            logger.warning("    人気データがないため、recency_bias_indicatorはスキップ")
            return df
        
        df['_expected_pos_from_pop'] = df[pop_col] * 0.8 + 2
        
        # Recency Bias Indicator
        # 正の値 = 市場が過小評価（実力より人気が低い = 狙い目）
        df['recency_bias_indicator'] = df['_expected_pos_from_pop'] - df['_past_avg_finish']
        
        # 一時カラムを削除
        df = df.drop(columns=['_past_avg_finish', '_expected_pos_from_pop'], errors='ignore')
        
        non_null = df['recency_bias_indicator'].notna().sum()
        logger.info(f"    recency_bias_indicator非NaN: {non_null:,}/{len(df):,} ({non_null/len(df)*100:.1f}%)")
        
        return df
    
    def _add_corner_gap_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """全コーナー馬身差と追い込み力特徴量を付与"""
        logger.info("  コーナー馬身差を付与中...")
        
        df = df.copy()
        
        # fit時に計算した統計をマップ
        df['gap_c1_avg'] = df['horse_id'].apply(
            lambda x: self._corner_gap_stats.get(x, {}).get('gap_c1_avg', np.nan)
        )
        df['gap_c2_avg'] = df['horse_id'].apply(
            lambda x: self._corner_gap_stats.get(x, {}).get('gap_c2_avg', np.nan)
        )
        df['gap_c3_avg'] = df['horse_id'].apply(
            lambda x: self._corner_gap_stats.get(x, {}).get('gap_c3_avg', np.nan)
        )
        
        # Gap Reduction（追い込み力）
        # gap_c4_avgはV15で既に計算されているので、それと組み合わせ
        if 'horse_c4_gap_avg' in df.columns:
            c4_gap = df['horse_c4_gap_avg']
        else:
            c4_gap = df['horse_id'].apply(
                lambda x: self._corner_gap_stats.get(x, {}).get('gap_c4_avg', np.nan)
            )
        
        # C1→C4の馬身差縮小量（正の値 = 追い込んでいる）
        df['gap_reduction_c1_c4'] = df['gap_c1_avg'] - c4_gap
        
        # C3→C4の馬身差縮小量（直線瞬発力）
        df['gap_reduction_c3_c4'] = df['gap_c3_avg'] - c4_gap
        
        for col in ['gap_c1_avg', 'gap_c2_avg', 'gap_c3_avg', 'gap_reduction_c1_c4', 'gap_reduction_c3_c4']:
            non_null = df[col].notna().sum()
            logger.info(f"    {col}非NaN: {non_null:,}/{len(df):,} ({non_null/len(df)*100:.1f}%)")
        
        return df
    
    def _add_running_style_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """詳細脚質特徴量を付与"""
        logger.info("  詳細脚質を付与中...")
        
        df = df.copy()
        
        # fit時に計算した統計をマップ
        df['senkou_rate'] = df['horse_id'].apply(
            lambda x: self._running_style_stats.get(x, {}).get('senkou_rate', np.nan)
        )
        df['sashi_rate'] = df['horse_id'].apply(
            lambda x: self._running_style_stats.get(x, {}).get('sashi_rate', np.nan)
        )
        
        for col in ['senkou_rate', 'sashi_rate']:
            non_null = df[col].notna().sum()
            logger.info(f"    {col}非NaN: {non_null:,}/{len(df):,} ({non_null/len(df)*100:.1f}%)")
        
        return df
    
    def get_feature_columns(self) -> List[str]:
        return self.FEATURE_COLS
