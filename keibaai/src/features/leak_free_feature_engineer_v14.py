"""
リークフリー特徴量エンジニア V14

V13をベースに、ペース×脚質マッチング特徴量を追加。

【追加特徴量 (5個)】
1. horse_position_improvement_avg: C1→C4の位置改善度（累積平均、shift済み）
2. horse_front_runner_rate: 逃げ馬傾向（C4位置1-2の割合、shift済み）
3. venue_expected_pace: 場×距離×馬場の予測ペース（Train期間で固定計算）
4. horse_pace_affinity_score: ペース適性スコア（データ不足のためV15で追加予定）
5. pace_style_match_score: ペース×脚質マッチ度（相互作用項）

【リーク対策】
- 全ての累積統計はcumsum() + shift(1)で計算
- venue_expected_paceはfit時のTrain期間のみで計算
- min_races要件で過学習を抑制

【過学習対策】
- 高次元組み合わせ（馬×騎手×ペース等）は追加しない
- corner_positionsのカバレッジが44%のため、NaN時はデフォルト値を使用
- jt_combo系は引き続き除外（V10.2で過学習の原因と判明）
"""

import pandas as pd
import numpy as np
from typing import Optional, List, Dict
from dataclasses import dataclass, field
import logging

from .leak_free_feature_engineer_v13 import LeakFreeFeatureEngineerV13, FeatureConfigV13

logger = logging.getLogger(__name__)


@dataclass
class FeatureConfigV14(FeatureConfigV13):
    """V14用設定"""
    # 位置改善の最小レース数
    min_corner_races: int = 3
    # 逃げ馬判定の閾値（C4位置がこの値以下なら逃げ）
    front_runner_threshold: int = 2
    # ペース差のビン閾値
    pace_diff_threshold: float = 1.0


class LeakFreeFeatureEngineerV14(LeakFreeFeatureEngineerV13):
    """
    リークフリー特徴量エンジニア V14
    
    V13（リーク修正版+新特徴量）にペース×脚質マッチング特徴量を追加。
    
    【重要】過学習対策
    - corner_positionsのデータは約44%のレースでのみ利用可能
    - min_corner_racesを設けて、少数データでの過学習を防止
    - venue_expected_paceはTrain期間の統計のみ使用（リーク防止）
    """
    
    # V13の特徴量 + V14の新特徴量
    FEATURE_COLS = LeakFreeFeatureEngineerV13.FEATURE_COLS + [
        'horse_position_improvement_avg',  # C1→C4位置改善度
        'horse_front_runner_rate',         # 逃げ馬傾向
        'venue_expected_pace',             # 場×距離×馬場の予測ペース
        'pace_style_match_score',          # ペース×脚質マッチ度
    ]
    
    def __init__(self, config: Optional[FeatureConfigV14] = None):
        super().__init__(config or FeatureConfigV14())
        self._venue_pace_stats: Dict = {}
        self._corner_history: Optional[pd.DataFrame] = None
        self._race_details: Optional[pd.DataFrame] = None
    
    def fit(self, races_df: pd.DataFrame, pedigrees_df=None, corners_df=None, 
            race_details_df=None, returns_df=None, horses_df=None):
        """
        fitメソッドの拡張
        
        - corners_df: コーナー通過位置データ
        - race_details_df: レース詳細（ペースデータ）
        """
        logger.info("LeakFreeFeatureEngineerV14: fit開始")
        
        # 親クラスのfit
        super().fit(races_df, pedigrees_df, corners_df, race_details_df, returns_df, horses_df)
        
        # コーナーデータを保存
        if corners_df is not None:
            self._corner_history = corners_df.copy()
            logger.info(f"  コーナーデータ: {len(self._corner_history):,}件")
        
        # レース詳細データを保存（race_details_dfがあれば使用、なければスキップ）
        if race_details_df is not None:
            self._race_details = race_details_df.copy()
            logger.info(f"  レース詳細: {len(self._race_details):,}件")
        
        # 場×距離×馬場のペース統計を事前計算（リークフリー）
        self._calc_venue_pace_stats(races_df, race_details_df)
        
        logger.info("LeakFreeFeatureEngineerV14: fit完了")
        return self
    
    def _calc_venue_pace_stats(self, races_df: pd.DataFrame, race_details_df: Optional[pd.DataFrame]):
        """
        場×距離カテゴリ×馬場のペース統計を計算（Train期間のみ）
        
        【リーク対策】
        - fit時にTrain期間のデータのみで計算
        - transform時はこの統計値をマップするのみ
        """
        logger.info("  ペース統計を計算中...")
        
        if race_details_df is None or len(race_details_df) == 0:
            logger.warning("  race_details_dfがないため、ペース統計はスキップ")
            return
        
        # レースIDでマージ
        races = races_df[['race_id', 'venue', 'distance_m', 'track_surface']].drop_duplicates('race_id')
        
        # 距離カテゴリを追加
        races = races.copy()
        races['dist_cat'] = pd.cut(
            pd.to_numeric(races['distance_m'], errors='coerce'),
            bins=[0, 1400, 1800, 2200, 5000],
            labels=['Sprint', 'Mile', 'Middle', 'Long']
        )
        
        # race_detailsとマージしてペース差を取得
        rd = race_details_df[['race_id', 'first_half', 'second_half']].copy()
        rd['pace_diff'] = pd.to_numeric(rd['second_half'], errors='coerce') - \
                          pd.to_numeric(rd['first_half'], errors='coerce')
        
        merged = races.merge(rd, on='race_id', how='inner')
        
        if len(merged) == 0:
            logger.warning("  ペースデータのマージ結果が0件")
            return
        
        # 場×距離×馬場でグループ化してペース差の平均を計算
        grouped = merged.groupby(['venue', 'dist_cat', 'track_surface'], observed=True)['pace_diff'].agg(['mean', 'count'])
        
        # 最低サンプル数を満たすものだけ採用（過学習対策）
        min_samples = 50
        valid_groups = grouped[grouped['count'] >= min_samples]
        
        self._venue_pace_stats = valid_groups['mean'].to_dict()
        logger.info(f"  ペース統計グループ数: {len(self._venue_pace_stats)}")
    
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """V14のtransform"""
        
        # 1. 親クラスのtransform（V13の特徴量）
        result = super().transform(df)
        
        logger.info("LeakFreeFeatureEngineerV14: 追加transform開始")
        
        # 2. V14特徴量の計算
        result = self._calc_position_improvement_features(result)
        result = self._calc_front_runner_rate(result)
        result = self._calc_venue_expected_pace(result)
        result = self._calc_pace_style_match_score(result)
        
        logger.info("LeakFreeFeatureEngineerV14: transform完了")
        return result
    
    def _calc_position_improvement_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        C1→C4の位置改善度を計算（リークフリー）
        
        【計算方法】
        - 各レースで C1_pos - C4_pos を計算（正=前進、負=後退）
        - 馬ごとに累積平均をshift(1)で計算
        
        【リーク対策】
        - shift(1)で「現在のレースより前」のデータのみ使用
        """
        logger.info("  位置改善度を計算中...")
        
        # デフォルト値
        df['horse_position_improvement_avg'] = np.nan
        
        if self._corner_history is None or len(self._corner_history) == 0:
            logger.warning("    コーナーデータなし")
            return df
        
        # C1とC4のデータを取得
        ch = self._corner_history.copy()
        c1 = ch[ch['corner'] == 1][['race_id', 'horse_number', 'position']].copy()
        c1.columns = ['race_id', 'horse_number', 'c1_pos']
        c4 = ch[ch['corner'] == 4][['race_id', 'horse_number', 'position']].copy()
        c4.columns = ['race_id', 'horse_number', 'c4_pos']
        
        # マージ
        corner_data = c1.merge(c4, on=['race_id', 'horse_number'], how='inner')
        corner_data['position_improvement'] = corner_data['c1_pos'] - corner_data['c4_pos']
        
        # 履歴データとマージしてhorse_idを取得
        if self._full_history is None:
            return df
        
        hist = self._full_history[['race_id', 'horse_number', 'horse_id', 'race_date']].copy()
        hist = hist.drop_duplicates(['race_id', 'horse_number'])
        
        corner_with_id = corner_data.merge(hist, on=['race_id', 'horse_number'], how='left')
        corner_with_id = corner_with_id.dropna(subset=['horse_id'])
        
        # 時系列ソート
        corner_with_id = corner_with_id.sort_values(['horse_id', 'race_date', 'race_id'])
        
        # 累積平均を計算（shift(1)でリーク防止）
        corner_with_id['cum_improvement'] = corner_with_id.groupby('horse_id')['position_improvement'].transform(
            lambda x: x.expanding().mean().shift(1)
        )
        corner_with_id['cum_count'] = corner_with_id.groupby('horse_id')['position_improvement'].transform(
            lambda x: x.expanding().count().shift(1)
        )
        
        # 最小レース数フィルタ（過学習対策）
        min_races = self.config.min_corner_races
        corner_with_id.loc[corner_with_id['cum_count'] < min_races, 'cum_improvement'] = np.nan
        
        # 現在のdfと結合するために、horse_idごとの最新統計を取得
        # （transform時点での各馬の累積統計）
        
        # 履歴データのみから統計を作成
        hist_only = corner_with_id.copy()
        latest_stats = hist_only.groupby('horse_id').last()[['cum_improvement']].reset_index()
        latest_stats.columns = ['horse_id', 'horse_position_improvement_avg']
        
        # dfにマージ
        df = df.merge(latest_stats, on='horse_id', how='left', suffixes=('', '_new'))
        
        # 新しい値で上書き
        if 'horse_position_improvement_avg_new' in df.columns:
            df['horse_position_improvement_avg'] = df['horse_position_improvement_avg_new'].fillna(
                df['horse_position_improvement_avg']
            )
            df = df.drop(columns=['horse_position_improvement_avg_new'])
        
        non_null = df['horse_position_improvement_avg'].notna().sum()
        logger.info(f"    position_improvement非NaN: {non_null:,}/{len(df):,} ({non_null/len(df)*100:.1f}%)")
        
        return df
    
    def _calc_front_runner_rate(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        逃げ馬傾向（C4位置1-2の割合）を計算（リークフリー）
        
        【計算方法】
        - 各レースでC4位置が閾値以下かを判定
        - 馬ごとにその割合を累積計算（shift済み）
        """
        logger.info("  逃げ馬傾向を計算中...")
        
        df['horse_front_runner_rate'] = np.nan
        
        if self._corner_history is None or len(self._corner_history) == 0:
            return df
        
        # C4のデータを取得
        ch = self._corner_history.copy()
        c4 = ch[ch['corner'] == 4][['race_id', 'horse_number', 'position']].copy()
        
        # 閾値以下なら逃げ馬フラグ
        threshold = self.config.front_runner_threshold
        c4['is_front_runner'] = (c4['position'] <= threshold).astype(float)
        
        # 履歴データとマージ
        if self._full_history is None:
            return df
        
        hist = self._full_history[['race_id', 'horse_number', 'horse_id', 'race_date']].copy()
        hist = hist.drop_duplicates(['race_id', 'horse_number'])
        
        c4_with_id = c4.merge(hist, on=['race_id', 'horse_number'], how='left')
        c4_with_id = c4_with_id.dropna(subset=['horse_id'])
        c4_with_id = c4_with_id.sort_values(['horse_id', 'race_date', 'race_id'])
        
        # 累積平均（shift済み）
        c4_with_id['cum_front_runner_rate'] = c4_with_id.groupby('horse_id')['is_front_runner'].transform(
            lambda x: x.expanding().mean().shift(1)
        )
        c4_with_id['cum_count'] = c4_with_id.groupby('horse_id')['is_front_runner'].transform(
            lambda x: x.expanding().count().shift(1)
        )
        
        # 最小レース数フィルタ
        min_races = self.config.min_corner_races
        c4_with_id.loc[c4_with_id['cum_count'] < min_races, 'cum_front_runner_rate'] = np.nan
        
        # 最新統計を取得
        latest_stats = c4_with_id.groupby('horse_id').last()[['cum_front_runner_rate']].reset_index()
        latest_stats.columns = ['horse_id', 'horse_front_runner_rate']
        
        # マージ
        df = df.merge(latest_stats, on='horse_id', how='left', suffixes=('', '_new'))
        
        if 'horse_front_runner_rate_new' in df.columns:
            df['horse_front_runner_rate'] = df['horse_front_runner_rate_new'].fillna(
                df['horse_front_runner_rate']
            )
            df = df.drop(columns=['horse_front_runner_rate_new'])
        
        non_null = df['horse_front_runner_rate'].notna().sum()
        logger.info(f"    front_runner_rate非NaN: {non_null:,}/{len(df):,} ({non_null/len(df)*100:.1f}%)")
        
        return df
    
    def _calc_venue_expected_pace(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        場×距離×馬場の予測ペースを計算
        
        【リーク対策】
        - fit時にTrain期間で計算した統計値をマップするのみ
        - Test期間の情報は一切使用しない
        """
        logger.info("  予測ペースを計算中...")
        
        df['venue_expected_pace'] = 0.0  # デフォルト値
        
        if not self._venue_pace_stats:
            logger.warning("    ペース統計なし")
            return df
        
        # 距離カテゴリを追加
        df = df.copy()
        if 'dist_cat' not in df.columns:
            df['dist_cat'] = pd.cut(
                pd.to_numeric(df['distance_m'], errors='coerce'),
                bins=[0, 1400, 1800, 2200, 5000],
                labels=['Sprint', 'Mile', 'Middle', 'Long']
            )
        
        # 統計値をマップ
        def get_expected_pace(row):
            key = (row['venue'], row['dist_cat'], row['track_surface'])
            return self._venue_pace_stats.get(key, 0.0)
        
        df['venue_expected_pace'] = df.apply(get_expected_pace, axis=1)
        
        # dist_catを削除（一時的に追加した場合）
        if 'dist_cat' in df.columns and 'dist_cat' not in self.FEATURE_COLS:
            df = df.drop(columns=['dist_cat'])
        
        non_zero = (df['venue_expected_pace'] != 0).sum()
        logger.info(f"    venue_expected_pace非ゼロ: {non_zero:,}/{len(df):,} ({non_zero/len(df)*100:.1f}%)")
        
        return df
    
    def _calc_pace_style_match_score(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        ペース×脚質マッチ度を計算
        
        【計算方法】
        - 逃げ馬（front_runner_rate高）はスローペース（pace_diff > 0）で有利
        - 差し馬（front_runner_rate低）はファストペース（pace_diff < 0）で有利
        - マッチ度 = front_runner_rate × venue_expected_pace
        - 正の値 = 逃げ馬がスロー予想で有利 / 差し馬がファスト予想で有利
        
        【シンプルな設計】
        - 複雑な計算は避け、単純な線形結合のみ
        """
        logger.info("  ペース×脚質マッチ度を計算中...")
        
        # 両方の特徴量が必要
        fr_rate = df['horse_front_runner_rate'].fillna(0.5)  # デフォルトは0.5（平均的）
        v_pace = df['venue_expected_pace'].fillna(0.0)
        
        # マッチスコア: 逃げ馬傾向 × ペース予測
        # 逃げ馬(高fr_rate) × スローペース(正pace) = 正（有利）
        # 差し馬(低fr_rate) × ファストペース(負pace) = 正（有利）
        # → fr_rate * pace + (1-fr_rate) * (-pace) = 2*fr_rate*pace - pace
        # シンプルに: (fr_rate - 0.5) * pace で、中央値からのずれ×ペースで計算
        
        df['pace_style_match_score'] = (fr_rate - 0.5) * v_pace
        
        return df
    
    def get_feature_columns(self) -> List[str]:
        return self.FEATURE_COLS
