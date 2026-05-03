"""
リークフリー特徴量エンジニア V15

V14をベースに、データ分析で発見された高ROIパターンを特徴量化。

【追加特徴量 (4個)】
1. race_front_runner_count: レース内逃げ予備軍数
   - 各馬のhorse_front_runner_rate > 0.3をカウント
   - ペース予測向上、逃げ競合リスク検出

2. horse_c4_gap_avg: 過去C4馬身差平均
   - 「前で競馬できる力」を馬身差で数値化
   - 順位だけでなく、どれだけ先頭に近いかを評価

3. post_style_conflict: 馬番×脚質の「不適合」スコア
   - |relative_post - avg_relative_c4|
   - 注意: 分析で「適合度高→ROI低」が判明
   - → 市場が既に織り込んでいるため、「不適合」を検出

4. front_runner_competition: 自分以外の逃げ予備軍数
   - 逃げ馬にとっての競合リスク

【リーク対策】
- 全ての累積統計はcumsum() + shift(1)で計算
- race_front_runner_countは他馬の「過去統計」のみ使用
- min_races=5に強化（過学習対策）

【過学習対策】
- 高次元組み合わせを避ける（馬×距離×馬場×ペース等は実装しない）
- min_races要件を強化（3→5）
- 特徴量数を抑制（4個のみ追加）
"""

import pandas as pd
import numpy as np
from typing import Optional, List, Dict
from dataclasses import dataclass
import logging

from .leak_free_feature_engineer_v14 import LeakFreeFeatureEngineerV14, FeatureConfigV14
from ..utils.course_feature_provider import CourseFeatureProvider

logger = logging.getLogger(__name__)


@dataclass
class FeatureConfigV15(FeatureConfigV14):
    """V15用設定"""
    # 逃げ予備軍判定閾値
    front_runner_threshold_rate: float = 0.3
    # 最小レース数要件（過学習対策で強化）
    min_corner_races: int = 5


class LeakFreeFeatureEngineerV15(LeakFreeFeatureEngineerV14):
    """
    リークフリー特徴量エンジニア V15
    
    V14に以下の特徴量を追加：
    - race_front_runner_count: レース内逃げ予備軍数
    - horse_c4_gap_avg: 過去C4馬身差平均
    - post_style_conflict: 馬番×脚質不適合スコア
    - front_runner_competition: 逃げ競合スコア
    
    【重要】過学習対策
    - min_races=5に強化
    - 高次元組み合わせは追加しない
    """
    
    # V14の特徴量 + V15の新特徴量
    FEATURE_COLS = LeakFreeFeatureEngineerV14.FEATURE_COLS + [
        'race_front_runner_count',      # レース内逃げ予備軍数
        'horse_c4_gap_avg',             # 過去C4馬身差平均
        'post_style_conflict',          # 馬番×脚質不適合スコア
        'front_runner_competition',     # 逃げ競合スコア
        # コース特徴量
        'course_corner_count',          # コーナー通過回数
        'course_start_to_corner_m',     # スタートから最初のコーナーまでの距離
        'course_final_straight_m',      # 最終直線距離
        'course_slope_percent',         # 坂勾配率
        'course_is_outer',              # 外回りフラグ
        'course_turn_direction',        # 回り方向(0=左, 1=右)
        # 交互作用・派生特徴量
        'straight_ratio',               # 直線比率(直線距離/全距離)
        'is_long_straight',             # 長い直線フラグ(>450m)
        # 脚質×コース交互作用特徴量
        'style_straight_match',         # 差し馬×長直線マッチ度
        'front_slope_disadvantage',     # 先行馬×坂不利度
        'closer_long_straight_advantage', # 差し馬×長直線アドバンテージ
    ]
    
    def __init__(self, config: Optional[FeatureConfigV15] = None):
        super().__init__(config or FeatureConfigV15())
        self._horse_c4_gap_stats: Dict = {}
        self._horse_relative_c4_stats: Dict = {}
        self._course_provider = CourseFeatureProvider()
    
    def fit(self, races_df: pd.DataFrame, pedigrees_df=None, corners_df=None, 
            race_details_df=None, returns_df=None, horses_df=None):
        """
        fitメソッドの拡張
        
        - C4馬身差の累積統計を事前計算
        - 相対C4位置の累積統計を事前計算
        """
        logger.info("LeakFreeFeatureEngineerV15: fit開始")
        
        # 親クラスのfit
        super().fit(races_df, pedigrees_df, corners_df, race_details_df, returns_df, horses_df)
        
        # C4馬身差・相対位置の統計を事前計算
        self._calc_c4_gap_stats(races_df, corners_df)
        
        logger.info("LeakFreeFeatureEngineerV15: fit完了")
        return self
    
    def _calc_c4_gap_stats(self, races_df: pd.DataFrame, corners_df: Optional[pd.DataFrame]):
        """
        C4馬身差と相対C4位置の累積統計を計算（Train期間のみ）
        
        【リーク対策】
        - fit時にTrain期間のデータのみで計算
        - shift(1)適用済み
        """
        logger.info("  C4馬身差・相対位置統計を計算中...")
        
        if corners_df is None or len(corners_df) == 0:
            logger.warning("  corners_dfがないため、C4統計はスキップ")
            return
        
        # C4データを取得
        c4 = corners_df[corners_df['corner'] == 4][['race_id', 'horse_number', 'position', 'gap_from_leader']].copy()
        c4.columns = ['race_id', 'horse_number', 'c4_pos', 'c4_gap']
        
        # 履歴データとマージ
        if self._full_history is None:
            return
        
        hist = self._full_history[['race_id', 'horse_number', 'horse_id', 'race_date']].copy()
        hist = hist.drop_duplicates(['race_id', 'horse_number'])
        
        # 出走頭数を追加
        field_size = hist.groupby('race_id').size().reset_index(name='field_size')
        hist = hist.merge(field_size, on='race_id', how='left')
        
        c4_with_id = c4.merge(hist, on=['race_id', 'horse_number'], how='left')
        c4_with_id = c4_with_id.dropna(subset=['horse_id'])
        c4_with_id = c4_with_id.sort_values(['horse_id', 'race_date', 'race_id'])
        
        # 相対C4位置を計算
        c4_with_id['relative_c4'] = c4_with_id['c4_pos'] / c4_with_id['field_size']
        
        # 累積統計を計算（shift(1)でリーク防止）
        # C4馬身差平均
        c4_with_id['cum_gap_avg'] = c4_with_id.groupby('horse_id')['c4_gap'].transform(
            lambda x: x.expanding().mean().shift(1)
        )
        c4_with_id['cum_count_gap'] = c4_with_id.groupby('horse_id')['c4_gap'].transform(
            lambda x: x.expanding().count().shift(1)
        )
        
        # 相対C4位置平均
        c4_with_id['cum_relative_c4'] = c4_with_id.groupby('horse_id')['relative_c4'].transform(
            lambda x: x.expanding().mean().shift(1)
        )
        
        # 最小レース数フィルタ
        min_races = self.config.min_corner_races
        c4_with_id.loc[c4_with_id['cum_count_gap'] < min_races, 'cum_gap_avg'] = np.nan
        c4_with_id.loc[c4_with_id['cum_count_gap'] < min_races, 'cum_relative_c4'] = np.nan
        
        # 最新統計を保存
        latest = c4_with_id.groupby('horse_id').last()[['cum_gap_avg', 'cum_relative_c4']].reset_index()
        self._horse_c4_gap_stats = latest.set_index('horse_id')['cum_gap_avg'].to_dict()
        self._horse_relative_c4_stats = latest.set_index('horse_id')['cum_relative_c4'].to_dict()
        
        logger.info(f"  C4馬身差統計: {len(self._horse_c4_gap_stats):,}頭")
        logger.info(f"  相対C4位置統計: {len(self._horse_relative_c4_stats):,}頭")
    
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """V15のtransform"""
        
        # 1. 親クラスのtransform（V14の特徴量）
        result = super().transform(df)
        
        logger.info("LeakFreeFeatureEngineerV15: 追加transform開始")
        
        # 2. V15特徴量の計算
        result = self._calc_c4_gap_features(result)
        result = self._calc_post_style_conflict(result)
        result = self._calc_race_front_runner_features(result)
        
        # 3. コース特徴量の追加
        result = self._calc_course_features(result)
        
        logger.info("LeakFreeFeatureEngineerV15: transform完了")
        return result
    
    def _calc_c4_gap_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        C4馬身差平均を付与
        
        【リーク対策】
        - fit時に計算した統計値をマップするのみ
        """
        logger.info("  C4馬身差を付与中...")
        
        df['horse_c4_gap_avg'] = df['horse_id'].map(self._horse_c4_gap_stats)
        
        non_null = df['horse_c4_gap_avg'].notna().sum()
        logger.info(f"    horse_c4_gap_avg非NaN: {non_null:,}/{len(df):,} ({non_null/len(df)*100:.1f}%)")
        
        return df
    
    def _calc_post_style_conflict(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        馬番×脚質不適合スコアを計算
        
        【計算方法】
        - relative_post = horse_number / field_size
        - avg_relative_c4 = 過去のC4相対位置平均（shift済み）
        - conflict = |relative_post - avg_relative_c4|
        
        【リーク対策】
        - avg_relative_c4はfit時に計算済み（shift適用済み）
        - 当日馬番と過去統計の組み合わせのみ
        """
        logger.info("  馬番×脚質不適合スコアを計算中...")
        
        df = df.copy()
        
        # 出走頭数を計算
        if 'field_size' not in df.columns:
            df['field_size'] = df.groupby('race_id')['horse_number'].transform('count')
        
        # 相対馬番
        df['relative_post'] = df['horse_number'] / df['field_size']
        
        # 過去の相対C4位置
        df['avg_relative_c4'] = df['horse_id'].map(self._horse_relative_c4_stats)
        
        # 不適合スコア（差の絶対値）
        # 大きいほど「枠順と過去の脚質が合っていない」
        df['post_style_conflict'] = (df['relative_post'] - df['avg_relative_c4']).abs()
        
        # 一時カラムを削除
        df = df.drop(columns=['relative_post', 'avg_relative_c4'], errors='ignore')
        
        non_null = df['post_style_conflict'].notna().sum()
        logger.info(f"    post_style_conflict非NaN: {non_null:,}/{len(df):,} ({non_null/len(df)*100:.1f}%)")
        
        return df
    
    def _calc_race_front_runner_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        レース内逃げ予備軍数と競合スコアを計算
        
        【計算方法】
        - race_front_runner_count: horse_front_runner_rate > threshold の馬をレース内でカウント
        - front_runner_competition: 自分以外の逃げ予備軍数
        
        【リーク対策】
        - horse_front_runner_rateは過去累積統計（shift済み）
        - 他馬の「過去統計」のみを使用
        """
        logger.info("  レース内逃げ予備軍特徴量を計算中...")
        
        df = df.copy()
        threshold = self.config.front_runner_threshold_rate
        
        # 逃げ予備軍フラグ
        df['_is_front_candidate'] = (df['horse_front_runner_rate'] > threshold).astype(float)
        df['_is_front_candidate'] = df['_is_front_candidate'].fillna(0)
        
        # レース内逃げ予備軍数
        df['race_front_runner_count'] = df.groupby('race_id')['_is_front_candidate'].transform('sum')
        
        # 逃げ競合スコア（自分以外）
        df['front_runner_competition'] = df['race_front_runner_count'] - df['_is_front_candidate']
        
        # 一時カラムを削除
        df = df.drop(columns=['_is_front_candidate'], errors='ignore')
        
        logger.info(f"    race_front_runner_count平均: {df['race_front_runner_count'].mean():.2f}")
        logger.info(f"    front_runner_competition平均: {df['front_runner_competition'].mean():.2f}")
        
        return df
    
    def _calc_course_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        コース特徴量を付与
        
        【追加特徴量】
        - course_corner_count: コーナー通過回数
        - course_start_to_corner_m: スタートから最初のコーナーまでの距離
        - course_final_straight_m: 最終直線距離
        - course_slope_percent: 坂勾配率
        - course_is_outer: 外回りフラグ
        - straight_ratio: 直線比率(直線距離/全距離)
        - is_long_straight: 長い直線フラグ(>450m)
        
        【リーク対策】
        - コース情報はレース開催時点で確定しているため、リークなし
        """
        logger.info("  コース特徴量を付与中...")
        
        df = df.copy()
        
        # [3-2改修] iterrows()からベクトル化されたgroupby-mergeに変更
        # 旧実装: iterrows()でN行ループ → O(N) Python呼び出し → 大規模データで極めて低速
        # 新実装: ユニークな(venue, surface, distance, is_outer)の組み合わせでグループ化し、
        #         各組み合わせに対して1回だけルックアップ → O(unique_combos) ≪ O(N)
        
        # ルックアップキーとなるカラムを抽出
        venue_col = df['venue'].fillna('') if 'venue' in df.columns else pd.Series('', index=df.index)
        surface_col = df['track_surface'].fillna('') if 'track_surface' in df.columns else pd.Series('', index=df.index)
        distance_col = df['distance_m'].fillna(0) if 'distance_m' in df.columns else pd.Series(0, index=df.index)
        is_outer_col = df['is_outer_course'] if 'is_outer_course' in df.columns else pd.Series(None, index=df.index)
        
        # ユニークな組み合わせを取得
        lookup_df = pd.DataFrame({
            'venue': venue_col,
            'track_surface': surface_col,
            'distance_m': distance_col,
            'is_outer_course': is_outer_col
        })
        unique_combos = lookup_df.drop_duplicates()
        
        # 各ユニーク組み合わせに対して1回だけルックアップ
        combo_features = {}
        default_features = {
            'corner_count': None,
            'start_to_corner_m': None,
            'final_straight_m': None,
            'slope_percent': None,
            'course_type': None,
            'turn_direction': None,
        }
        
        for _, combo_row in unique_combos.iterrows():
            venue = combo_row['venue']
            surface = combo_row['track_surface']
            distance = combo_row['distance_m']
            is_outer = combo_row['is_outer_course']
            
            key = (venue, surface, distance, is_outer if pd.notna(is_outer) else None)
            
            if venue and surface and distance:
                features = self._course_provider.get_course_features(
                    venue=venue,
                    surface=surface,
                    distance=int(distance) if pd.notna(distance) else 0,
                    is_outer=is_outer
                )
            else:
                features = default_features.copy()
            
            combo_features[key] = features
        
        logger.info(f"    コースルックアップ: {len(combo_features)}種類 (全{len(df):,}行)")
        
        # 各行にルックアップ結果をマッピング（ベクトル化）
        keys = list(zip(
            venue_col.values,
            surface_col.values,
            distance_col.values,
            [v if pd.notna(v) else None for v in is_outer_col.values]
        ))
        
        feature_arrays = {
            'corner_count': [],
            'start_to_corner_m': [],
            'final_straight_m': [],
            'slope_percent': [],
            'course_type': [],
            'turn_direction': [],
        }
        
        for key in keys:
            feat = combo_features.get(key, default_features)
            for col_name in feature_arrays:
                feature_arrays[col_name].append(feat.get(col_name))
        
        features_df = pd.DataFrame(feature_arrays, index=df.index)
        
        # カラム名のリネームと付与
        df['course_corner_count'] = features_df['corner_count']
        df['course_start_to_corner_m'] = features_df['start_to_corner_m']
        df['course_final_straight_m'] = features_df['final_straight_m']
        df['course_slope_percent'] = features_df['slope_percent']
        df['course_is_outer'] = (features_df['course_type'] == 'outer').astype(float)
        # 回り方向 (right=1, left=0)
        df['course_turn_direction'] = (features_df['turn_direction'] == 'right').astype(float)
        
        # 派生特徴量を計算
        # 直線比率 = 直線距離 / 全距離
        df['straight_ratio'] = df['course_final_straight_m'] / df['distance_m']
        df['straight_ratio'] = df['straight_ratio'].fillna(0)
        
        # 長い直線フラグ (450m以上)
        df['is_long_straight'] = (df['course_final_straight_m'] > 450).astype(float)
        
        # ===== 脚質×コース交互作用特徴量 =====
        # horse_front_runner_rate は V14 で既に計算済み（shift適用済みでリークなし）
        fr_rate = df['horse_front_runner_rate'].fillna(0.5)  # デフォルトは平均的
        
        # 1. style_straight_match: 差し馬×長直線マッチ度
        #    (1 - 逃げ率) × 直線比率 → 差し馬×長い直線で高値
        df['style_straight_match'] = (1 - fr_rate) * df['straight_ratio']
        
        # 2. front_slope_disadvantage: 先行馬×坂不利度
        #    逃げ率 × 坂勾配率 → 先行馬×急坂で高値（不利を表す）
        slope = df['course_slope_percent'].fillna(0)
        df['front_slope_disadvantage'] = fr_rate * slope
        
        # 3. closer_long_straight_advantage: 差し馬×長直線アドバンテージ
        #    (逃げ率 < 0.3) × 長い直線フラグ → 差し馬かつ長い直線で1
        is_closer = (fr_rate < 0.3).astype(float)
        df['closer_long_straight_advantage'] = is_closer * df['is_long_straight']
        
        # ログ出力
        non_null = df['course_final_straight_m'].notna().sum()
        logger.info(f"    course_final_straight_m非NaN: {non_null:,}/{len(df):,} ({non_null/len(df)*100:.1f}%)")
        outer_count = df['course_is_outer'].sum()
        logger.info(f"    course_is_outer=True: {int(outer_count):,}件")
        long_straight = df['is_long_straight'].sum()
        logger.info(f"    is_long_straight=True: {int(long_straight):,}件")
        
        # 交互作用特徴量のログ
        closers_advantage = df['closer_long_straight_advantage'].sum()
        logger.info(f"    closer_long_straight_advantage=1: {int(closers_advantage):,}件")
        
        return df
    
    def get_feature_columns(self) -> List[str]:
        return self.FEATURE_COLS
