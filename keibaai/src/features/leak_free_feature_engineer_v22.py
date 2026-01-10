"""
リークフリー特徴量エンジニア V22

V15 Fixedをベースに、季節特徴量・競馬場特化特徴量を追加。
弱点（夏場・苦手競馬場）の克服を目的とした汎用特徴量エンジン。

【追加特徴量 (9個)】
季節特徴量:
1. month_sin: sin(2π × month / 12)
2. month_cos: cos(2π × month / 12)
3. is_summer: 8-9月フラグ（弱点月）

馬の季節別成績 (shift(1)厳守):
4. horse_season_win_rate: 同季節（夏/秋冬/春）での馬の勝率
5. horse_summer_performance: 夏場限定の馬の成績

競馬場別成績 (shift(1)厳守):
6. horse_venue_win_rate: 馬の競馬場別勝率
7. jockey_venue_win_rate: 騎手の競馬場別勝率
8. trainer_venue_win_rate: 調教師の競馬場別勝率
9. is_weak_venue: 苦手競馬場フラグ（札幌/新潟/阪神）

【リーク対策】
- 全ての累積統計はshift(1)を厳守
- race_dateに基づく時系列ソート
- 各レース時点で利用可能な過去データのみを使用

【過学習対策】
- 最小レース数要件を設けて少数データでの過学習を防止
- 競馬場別成績は十分なサンプル数がある場合のみ使用

【背景】
docs/prediction_analysis.mdの分析結果:
- 夏場（8-9月）: ROI 60-68%と低迷
- 苦手競馬場（札幌/新潟/阪神）: ROI 64-69%
これらの弱点を特徴量レベルで対策する。
"""

import pandas as pd
import numpy as np
from typing import Optional, List, Dict
from dataclasses import dataclass
import logging

from .leak_free_feature_engineer_v15_fixed import LeakFreeFeatureEngineerV15Fixed, FeatureConfigV15Fixed

logger = logging.getLogger(__name__)


@dataclass
class FeatureConfigV22(FeatureConfigV15Fixed):
    """V22用設定"""
    # 季節別成績の最小レース数
    min_season_races: int = 3
    # 競馬場別成績の最小レース数
    min_venue_races: int = 2
    # 苦手競馬場リスト
    weak_venues: tuple = ('札幌', '新潟', '阪神')
    # 夏月リスト
    summer_months: tuple = (8, 9)


class LeakFreeFeatureEngineerV22(LeakFreeFeatureEngineerV15Fixed):
    """
    リークフリー特徴量エンジニア V22
    
    V15 Fixedに季節・競馬場特化特徴量を追加。
    
    【目的】
    - 夏場（8-9月）のROI低下を改善
    - 苦手競馬場（札幌/新潟/阪神）のROI改善
    
    【リーク対策】
    - 全ての累積統計はshift(1)を厳守
    - race_dateに基づく時系列ソート
    """
    
    # V15 Fixedの特徴量 + V22の新特徴量
    FEATURE_COLS = LeakFreeFeatureEngineerV15Fixed.FEATURE_COLS + [
        # 季節特徴量
        'month_sin',
        'month_cos', 
        'is_summer',
        # 馬の季節別成績
        'horse_season_win_rate',
        'horse_summer_performance',
        # 競馬場別成績
        'horse_venue_win_rate',
        'jockey_venue_win_rate',
        'trainer_venue_win_rate',
        'is_weak_venue',
    ]
    
    def __init__(self, config: Optional[FeatureConfigV22] = None):
        super().__init__(config or FeatureConfigV22())
        # 季節・競馬場統計用の内部変数
        self._horse_season_stats: Dict = {}
        self._horse_venue_stats: Dict = {}
        self._jockey_venue_stats: Dict = {}
        self._trainer_venue_stats: Dict = {}
    
    def fit(self, races_df: pd.DataFrame, pedigrees_df=None, corners_df=None, 
            race_details_df=None, returns_df=None, horses_df=None):
        """
        fitメソッドの拡張
        
        季節別・競馬場別の累積統計を事前計算
        """
        logger.info("LeakFreeFeatureEngineerV22: fit開始")
        
        # 親クラスのfit
        super().fit(races_df, pedigrees_df, corners_df, race_details_df, returns_df, horses_df)
        
        logger.info("LeakFreeFeatureEngineerV22: fit完了")
        return self
    
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """V22のtransform"""
        
        # 1. 親クラスのtransform（V15 Fixedの特徴量）
        result = super().transform(df)
        
        logger.info("LeakFreeFeatureEngineerV22: 追加transform開始")
        
        # 2. 季節特徴量（リークなし：race_dateから直接計算）
        result = self._calc_season_features(result)
        
        # 3. 馬の季節別成績（shift(1)で計算）
        result = self._calc_horse_season_performance(result)
        
        # 4. 競馬場別成績（shift(1)で計算）
        result = self._calc_venue_performance_features(result)
        
        logger.info("LeakFreeFeatureEngineerV22: transform完了")
        return result
    
    def _calc_season_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        季節特徴量を計算（リークなし）
        
        【計算方法】
        - race_dateから月を抽出
        - sin/cosで周期性をエンコード
        - 夏フラグを設定
        
        【リーク対策】
        - レース日付から直接計算するため、リークなし
        """
        logger.info("  季節特徴量を計算中...")
        
        df = df.copy()
        
        # race_dateをパース
        df['race_date'] = pd.to_datetime(df['race_date'], errors='coerce')
        month = df['race_date'].dt.month
        
        # 月のsin/cos変換（周期性をエンコード）
        df['month_sin'] = np.sin(2 * np.pi * month / 12)
        df['month_cos'] = np.cos(2 * np.pi * month / 12)
        
        # 夏フラグ（8-9月）
        summer_months = self.config.summer_months
        df['is_summer'] = month.isin(summer_months).astype(float)
        
        logger.info(f"    is_summer=1の割合: {df['is_summer'].mean()*100:.1f}%")
        
        return df
    
    def _calc_horse_season_performance(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        馬の季節別成績を計算（完全リークフリー版）
        
        【計算方法】
        1. 履歴データ全体で、時間順に「その時点までの累積成績」を計算し、race_idに紐付ける。
        2. df（入力データ）に対し、race_idをキーにして 1. の結果をマージ（これでTrainデータは完璧）。
        3. マージできなかった行（＝履歴にない未来のデータ）については、
           その馬の「履歴データの最後の時点での成績」を埋める。
        
        【リーク対策】
        - 過去のデータのみを参照することが保証される。
        - 未来のレース結果が混入する余地がない。
        """
        logger.info("  馬の季節別成績を計算中...")
        
        df = df.copy()
        
        # 初期化
        df['horse_season_win_rate'] = np.nan
        df['horse_summer_performance'] = np.nan
        
        if self._full_history is None:
            return df
            
        hist = self._full_history.copy()
        hist['race_date'] = pd.to_datetime(hist['race_date'], errors='coerce')
        hist['month'] = hist['race_date'].dt.month
        hist['is_win'] = (hist['finish_position'] == 1).astype(float)
        
        # 季節定義
        def get_season(month):
            if pd.isna(month): return None
            m = int(month)
            if m in (8, 9): return 'summer'
            elif m in (10, 11, 12, 1, 2): return 'winter'
            else: return 'spring'
            
        hist['season'] = hist['month'].apply(get_season)
        
        # df用の季節列
        df['race_date'] = pd.to_datetime(df['race_date'], errors='coerce')
        df['_current_season'] = df['race_date'].dt.month.apply(get_season)
        
        min_races = self.config.min_season_races
        
        # ---------------------------------------------------------
        # 1. 季節別成績 (horse_season_win_rate)
        # ---------------------------------------------------------
        
        # (A) 履歴データ上での「各レース時点」の成績計算
        # race_id ごとの数値を計算する
        season_hist = hist.dropna(subset=['season']).sort_values(['horse_id', 'season', 'race_date', 'race_id'])
        
        # shift(1)して累積
        season_hist['cum_wins'] = season_hist.groupby(['horse_id', 'season'])['is_win'].apply(
            lambda x: x.cumsum().shift(1).fillna(0)
        ).reset_index(level=[0, 1], drop=True)
        season_hist['cum_races'] = season_hist.groupby(['horse_id', 'season']).cumcount()
        
        season_hist['season_win_rate_stat'] = np.where(
            season_hist['cum_races'] >= min_races,
            season_hist['cum_wins'] / season_hist['cum_races'],
            np.nan
        )
        
        # lookup用テーブル: (race_id) -> stat (特定のレースにおける事前成績)
        # 注意: horse_idもキーに含めると安全だが、race_idが一意ならrace_idだけでよい。
        # ここでは念のため (race_id, horse_id) でマージする。
        race_stats_map = season_hist[['race_id', 'horse_id', 'season_win_rate_stat']].set_index(['race_id', 'horse_id'])
        
        # (B) 履歴データの「最新状態」の計算（未来データ用フォールバック）
        # 各馬・各季節ごとの最終成績
        latest_season_stats = season_hist.groupby(['horse_id', 'season'])[['cum_wins', 'cum_races']].last().reset_index()
        # 最終レースの結果を含めるため、last()の時点からもう1レース分更新する必要がある？
        # いや、season_histの 'cum_wins' は shift(1) 済みなので、
        # "そのレース出走前の成績" である。
        # "全ての履歴が終わった後の最新成績" を作るには、
        # shiftしてない is_win.cumsum() の最後を取る必要がある。
        
        # 正しい最新状態の作成:
        # 全履歴を使って集計する
        final_group = hist.groupby(['horse_id', 'season'])['is_win'].agg(['sum', 'count']).reset_index()
        final_group.rename(columns={'sum': 'total_wins', 'count': 'total_races'}, inplace=True)
        final_group['final_win_rate'] = np.where(
            final_group['total_races'] >= min_races,
            final_group['total_wins'] / final_group['total_races'],
            np.nan
        )
        # (horse_id, season) -> final_win_rate
        
        # ---------------------------------------------------------
        # マージ処理
        # ---------------------------------------------------------
        
        # 1. 既知のレース (Trainデータ等) には、その時点の成績をマージ
        df = df.merge(
            season_hist[['race_id', 'horse_id', 'season_win_rate_stat']], 
            on=['race_id', 'horse_id'], 
            how='left'
        )
        
        # 2. 未知のレース (Valid/Testデータ等、race_idが履歴にない行) には、最新成績をマージ
        # 【リーク対策】race_idが履歴に存在する行にはfallbackを適用しない
        # race_idが履歴に存在するかどうかのフラグを作成
        known_race_ids = set(season_hist['race_id'].unique())
        df['_is_known_race'] = df['race_id'].isin(known_race_ids)
        
        # まずはマージ用の列を準備
        df = df.merge(
            final_group[['horse_id', 'season', 'final_win_rate']].rename(columns={'season': '_match_season'}),
            left_on=['horse_id', '_current_season'],
            right_on=['horse_id', '_match_season'],
            how='left'
        )
        
        # 【修正】履歴にないrace_idの場合のみfallbackを適用
        # 履歴にあるrace_id（訓練データ）はstat_valueのみ使用（NaNならNaNのまま）
        df['horse_season_win_rate'] = np.where(
            df['_is_known_race'],
            df['season_win_rate_stat'],  # 訓練データ: fallbackなし
            df['season_win_rate_stat'].fillna(df['final_win_rate'])  # Valid/Test: fallbackあり
        )
        
        # 不要列削除
        df.drop(columns=['season_win_rate_stat', 'final_win_rate', '_match_season', '_is_known_race'], inplace=True)
        
        # ---------------------------------------------------------
        # 2. 夏場限定成績 (horse_summer_performance)
        # ---------------------------------------------------------
        # ロジックは上記と同じ
        
        summer_hist = hist[hist['season'] == 'summer'].copy().sort_values(['horse_id', 'race_date', 'race_id'])
        
        if len(summer_hist) > 0:
            summer_hist['cum_wins'] = summer_hist.groupby('horse_id')['is_win'].apply(
                lambda x: x.cumsum().shift(1).fillna(0)
            ).reset_index(level=0, drop=True)
            summer_hist['cum_races'] = summer_hist.groupby('horse_id').cumcount()
            
            summer_hist['summer_win_rate_stat'] = np.where(
                summer_hist['cum_races'] >= min_races,
                summer_hist['cum_wins'] / summer_hist['cum_races'],
                np.nan
            )
            
            # マージ (Race ID match)
            df = df.merge(
                summer_hist[['race_id', 'horse_id', 'summer_win_rate_stat']],
                on=['race_id', 'horse_id'],
                how='left'
            )
            
            # 最新状態 (Fallback)
            final_summer = hist[hist['season'] == 'summer'].groupby('horse_id')['is_win'].agg(['sum', 'count']).reset_index()
            final_summer['final_summer_rate'] = np.where(
                final_summer['count'] >= min_races,
                final_summer['sum'] / final_summer['count'],
                np.nan
            )
            
            # マージ (Fallback)
            df = df.merge(
                final_summer[['horse_id', 'final_summer_rate']],
                on='horse_id',
                how='left'
            )
            
            # 【リーク対策】履歴にあるrace_idにはfallbackを適用しない
            known_summer_race_ids = set(summer_hist['race_id'].unique())
            df['_is_known_summer_race'] = df['race_id'].isin(known_summer_race_ids)
            
            df['horse_summer_performance'] = np.where(
                df['_is_known_summer_race'],
                df['summer_win_rate_stat'],  # 訓練データ: fallbackなし
                df['summer_win_rate_stat'].fillna(df['final_summer_rate'])  # Valid/Test: fallbackあり
            )
            df.drop(columns=['summer_win_rate_stat', 'final_summer_rate', '_is_known_summer_race'], inplace=True)
            
        # 掃除
        df.drop(columns=['_current_season'], inplace=True)
        
        return df

    def _calc_venue_performance_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        競馬場別成績を計算（完全リークフリー版）
        
        【計算方法】
        - 馬/騎手/調教師の競馬場別勝率を累積計算
        - shift(1)で当該レースを除外
        - race_idでマージして厳密なリーク排除
        
        【リーク対策】
        - 各レース時点より前のデータのみ使用
        - shift(1)を厳守
        - 時系列マージ + フォールバック方式
        """
        logger.info("  競馬場別成績を計算中...")
        
        df = df.copy()
        
        # デフォルト値
        df['horse_venue_win_rate'] = np.nan
        df['jockey_venue_win_rate'] = np.nan
        df['trainer_venue_win_rate'] = np.nan
        df['is_weak_venue'] = 0.0
        
        # 苦手競馬場フラグ（リークなし：競馬場名から直接判定）
        weak_venues = self.config.weak_venues
        df['is_weak_venue'] = df['venue'].isin(weak_venues).astype(float)
        
        if self._full_history is None:
            logger.warning("  履歴データなし")
            return df
        
        # 履歴データを準備
        hist = self._full_history.copy()
        hist['race_date'] = pd.to_datetime(hist['race_date'], errors='coerce')
        hist['is_win'] = (hist['finish_position'] == 1).astype(float)
        hist = hist.sort_values(['race_date', 'race_id'])
        
        min_races = self.config.min_venue_races
        
        # ===== 馬の競馬場別勝率 =====
        df = self._calc_entity_venue_win_rate(
            df, hist, 'horse_id', 'horse_venue_win_rate', min_races
        )
        
        # ===== 騎手の競馬場別勝率 =====
        df = self._calc_entity_venue_win_rate(
            df, hist, 'jockey_id', 'jockey_venue_win_rate', min_races
        )
        
        # ===== 調教師の競馬場別勝率 =====
        df = self._calc_entity_venue_win_rate(
            df, hist, 'trainer_id', 'trainer_venue_win_rate', min_races
        )
        
        non_null_horse = df['horse_venue_win_rate'].notna().sum()
        non_null_jockey = df['jockey_venue_win_rate'].notna().sum()
        non_null_trainer = df['trainer_venue_win_rate'].notna().sum()
        logger.info(f"    horse_venue_win_rate非NaN: {non_null_horse:,}/{len(df):,} ({non_null_horse/len(df)*100:.1f}%)")
        logger.info(f"    jockey_venue_win_rate非NaN: {non_null_jockey:,}/{len(df):,} ({non_null_jockey/len(df)*100:.1f}%)")
        logger.info(f"    trainer_venue_win_rate非NaN: {non_null_trainer:,}/{len(df):,} ({non_null_trainer/len(df)*100:.1f}%)")
        logger.info(f"    is_weak_venue=1の割合: {df['is_weak_venue'].mean()*100:.1f}%")
        
        return df

    def _calc_entity_venue_win_rate(self, df: pd.DataFrame, hist: pd.DataFrame,
                                     entity_col: str, output_col: str, 
                                     min_races: int) -> pd.DataFrame:
        """
        エンティティ×競馬場の勝率を計算（完全リークフリー版）
        
        【計算方法】
        1. 履歴データ上で「各レース時点での成績」を計算 (race_idごとに一意)
        2. dfに race_id でマージ (履歴にある過去のレース用)
        3. dfに entity_id + venue で「最新成績」をマージ (履歴にない未来のレース用)
        4. 2 を優先し、欠損を 3 で埋める
        """
        if entity_col not in hist.columns or entity_col not in df.columns:
            return df
            
        # 1. 履歴データ上での時系列計算
        # group by entity, venue
        # 必要な列だけ抽出してソート
        target_hist = hist[[entity_col, 'venue', 'race_date', 'race_id', 'is_win']].dropna(subset=[entity_col, 'venue']).copy()
        target_hist = target_hist.sort_values([entity_col, 'venue', 'race_date', 'race_id'])
        
        # shift(1) 累積
        # groupby().apply() は遅いので、transformを使うか、うまくベクトル化したいが
        # ここでは可読性と確実性重視で groupby().cumsum() を使う
        # entity_col, venue でグルーピング
        g = target_hist.groupby([entity_col, 'venue'])['is_win']
        target_hist['cum_wins'] = g.apply(
            lambda x: x.cumsum().shift(1).fillna(0)
        ).reset_index(level=[0, 1], drop=True)
        target_hist['cum_races'] = g.cumcount()
        
        target_hist['stat_value'] = np.where(
            target_hist['cum_races'] >= min_races,
            target_hist['cum_wins'] / target_hist['cum_races'],
            np.nan
        )
        
        # 2. 最新状態の計算 (Fallback用)
        final_stats = target_hist.groupby([entity_col, 'venue'])['is_win'].agg(['sum', 'count']).reset_index()
        # 注意: ここでの sum/count は全期間の合計
        # しかし target_hist は hist (fitに渡された全データ) なので、
        # "fit時点での全期間成績" となる。これは未来のテストデータに対しては "学習完了時点の知識" となり、リークではない。
        final_stats['final_value'] = np.where(
            final_stats['count'] >= min_races,
            final_stats['sum'] / final_stats['count'],
            np.nan
        )
        
        # 3. マージ実行
        
        # (A) Race ID マッチ (過去レース用)
        # race_id, entity_col (同レースに同騎手が複数回はありえないが、念のため)
        # 同レースに同調教師はありえるので、entity_colも含めてキーにする必要あり
        
        # マージ前に型を合わせる
        # df[entity_col] と target_hist[entity_col] の型
        
        # dfにマージ
        df = df.merge(
            target_hist[['race_id', entity_col, 'stat_value']],
            on=['race_id', entity_col],
            how='left'
        )
        
        # (B) Entity + Venue マッチ (未来レース用)
        df = df.merge(
            final_stats[[entity_col, 'venue', 'final_value']],
            on=[entity_col, 'venue'],
            how='left'
        )
        
        # 【リーク対策】履歴にあるrace_idにはfallbackを適用しない
        # 訓練データの場合、stat_valueがNaNでもfallbackしない（NaNのまま）
        known_race_ids = set(target_hist['race_id'].unique())
        df['_is_known_race'] = df['race_id'].isin(known_race_ids)
        
        # 4. 適用 (履歴にないrace_idの場合のみfallback)
        df[output_col] = np.where(
            df['_is_known_race'],
            df['stat_value'],  # 訓練データ: fallbackなし（NaNならNaN）
            df['stat_value'].fillna(df['final_value'])  # Valid/Test: fallbackあり
        )
        
        # 不要列削除
        df.drop(columns=['stat_value', 'final_value', '_is_known_race'], inplace=True)
        
        return df
    
    def get_feature_columns(self) -> List[str]:
        return self.FEATURE_COLS
