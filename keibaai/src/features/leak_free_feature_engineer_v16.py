"""
リークフリー特徴量エンジニア V16

V15_Fixedをベースに、6年Walk-forward検証で効果確認済みの
6つの新特徴量を追加。

【追加特徴量】（6年Walk-forward検証済み）
1. prev_3f_avg: 過去上がり3F累積平均 (6/6年プラス, +7.6pt)
2. prev_fr_rate: 過去先行率 (6/6年プラス, +8.8pt)  ※V15のhorse_front_runner_rateと統合
3. horse_prev_winrate: 馬累積勝率 (5/6年プラス, +7.9pt)
4. jockey_prev_winrate: 騎手累積勝率 (6/6年プラス, +11.3pt)
5. trainer_prev_winrate: 調教師累積勝率 (追加)
6. is_late_race: R8-12レースフラグ (5/6年プラス, +4.1pt)
7. prev_finish: 前走着順 (5/6年プラス, +3.8pt)
8. prev_finish_cat: 前走着順カテゴリ

【リーク対策（厳格）】
1. すべての累積統計はshift(1)で過去のみ参照
2. 当該レースの結果情報（着順、タイム、通過順等）は使用しない
3. 確定オッズは特徴量として使用しない
4. race_dateでソートしてから処理

【過学習対策】
- 複雑な交互作用特徴量は追加しない（単純な累積統計のみ）
- 年度間で安定効果が確認された特徴量のみ採用

Author: KeibaAI Development Team
Date: 2026-01-07
"""

import pandas as pd
import numpy as np
from typing import Optional, List
from dataclasses import dataclass, field
import logging

from .leak_free_feature_engineer_v15_fixed import (
    LeakFreeFeatureEngineerV15Fixed, 
    FeatureConfigV15Fixed
)

logger = logging.getLogger(__name__)


@dataclass
class FeatureConfigV16(FeatureConfigV15Fixed):
    """V16用設定"""
    # 新特徴量の設定はデフォルト値のみ使用
    pass


class LeakFreeFeatureEngineerV16(LeakFreeFeatureEngineerV15Fixed):
    """
    リークフリー特徴量エンジニア V16
    
    V15_Fixedの全特徴量 + 6年Walk-forward検証済みの新特徴量
    
    【新規追加特徴量】
    - prev_3f_avg: 過去上がり3F累積平均
    - horse_prev_winrate: 馬累積勝率
    - jockey_prev_winrate: 騎手累積勝率
    - trainer_prev_winrate: 調教師累積勝率
    - is_late_race: R8-12レースフラグ
    - prev_finish: 前走着順
    - prev_finish_cat: 前走着順カテゴリ
    
    【リーク対策】
    - すべてshift(1)で過去のみ参照
    - 当該レースの結果は一切使用しない
    """
    
    # V15_Fixed + 新特徴量
    V16_NEW_FEATURES = [
        'prev_3f_avg',
        'horse_prev_winrate',
        'jockey_prev_winrate', 
        'trainer_prev_winrate',
        'is_late_race',
        'prev_finish',
        'prev_finish_cat',
    ]
    
    FEATURE_COLS = LeakFreeFeatureEngineerV15Fixed.FEATURE_COLS + V16_NEW_FEATURES
    
    def __init__(self, config: Optional[FeatureConfigV16] = None):
        super().__init__(config or FeatureConfigV16())
        logger.info("LeakFreeFeatureEngineerV16: 初期化完了")
    
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """V16のtransform"""
        
        # 1. 親クラスのtransform（V15_Fixedまでの全特徴量）
        result = super().transform(df)
        
        logger.info("LeakFreeFeatureEngineerV16: 追加transform開始")
        
        # 2. 新特徴量を追加
        result = self._add_v16_features(result)
        
        logger.info("LeakFreeFeatureEngineerV16: transform完了")
        return result
    
    def _add_v16_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        V16の新特徴量を追加
        
        【リーク対策】
        - すべてshift(1)で過去のみ参照
        - cumsum().shift(1) / cumcount() パターンを厳守
        """
        df = df.copy()
        
        # race_dateが存在することを確認
        if 'race_date' not in df.columns:
            logger.warning("  race_dateがありません。V16特徴量をスキップ。")
            return df
        
        df['race_date'] = pd.to_datetime(df['race_date'])
        
        # ===== 1. 過去上がり3F統計 (prev_3f_avg) =====
        df = self._calc_prev_3f_avg(df)
        
        # ===== 2. 馬累積勝率 (horse_prev_winrate) =====
        df = self._calc_horse_prev_winrate(df)
        
        # ===== 3. 騎手累積勝率 (jockey_prev_winrate) =====
        df = self._calc_jockey_prev_winrate(df)
        
        # ===== 4. 調教師累積勝率 (trainer_prev_winrate) =====
        df = self._calc_trainer_prev_winrate(df)
        
        # ===== 5. R8-12レースフラグ (is_late_race) =====
        df = self._calc_late_race_flag(df)
        
        # ===== 6. 前走着順 (prev_finish, prev_finish_cat) =====
        df = self._calc_prev_finish(df)
        
        return df
    
    def _calc_prev_3f_avg(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        過去上がり3F累積平均
        
        【リーク対策】
        - 当該レースのlast_3f_timeは使用不可（結果情報）
        - 過去レースの累積平均のみ使用
        - shift(1)で当該レースを除外
        - groupby内でshift(1)を適用
        """
        logger.info("  過去上がり3F統計を計算中...")
        
        if 'last_3f_time' not in df.columns:
            logger.warning("    last_3f_timeがありません")
            df['prev_3f_avg'] = np.nan
            return df
        
        # 時系列ソート
        df = df.sort_values(['horse_id', 'race_date']).reset_index(drop=True)
        
        # 累積平均を計算（groupby内でshift(1)を適用）
        def calc_cumulative_avg_shifted(series):
            """shift(1)で当該レースを除外した累積平均を計算"""
            cumsum = series.fillna(0).cumsum()
            cumcount = series.notna().cumsum()
            avg = cumsum / cumcount.replace(0, np.nan)
            # shift(1)で過去のみに限定
            return avg.shift(1)
        
        df['prev_3f_avg'] = df.groupby('horse_id')['last_3f_time'].transform(
            calc_cumulative_avg_shifted
        )
        
        non_null = df['prev_3f_avg'].notna().sum()
        logger.info(f"    prev_3f_avg非NaN: {non_null:,}/{len(df):,} ({non_null/len(df)*100:.1f}%)")
        
        return df
    
    def _calc_horse_prev_winrate(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        馬累積勝率
        
        【リーク対策】
        - cumsum().shift(1) で当該レースの結果を除外
        """
        logger.info("  馬累積勝率を計算中...")
        
        if 'is_win' not in df.columns:
            if 'finish_position' in df.columns:
                df['is_win'] = (df['finish_position'] == 1).astype(int)
            else:
                df['horse_prev_winrate'] = np.nan
                return df
        
        # 時系列ソート
        df = df.sort_values(['horse_id', 'race_date']).reset_index(drop=True)
        
        # 累積勝利数（shift(1)で過去のみ）
        df['_horse_cumwins'] = df.groupby('horse_id')['is_win'].cumsum().shift(1).fillna(0)
        # 累積出走数
        df['_horse_cumraces'] = df.groupby('horse_id').cumcount()
        # 累積勝率
        df['horse_prev_winrate'] = df['_horse_cumwins'] / df['_horse_cumraces'].replace(0, np.nan)
        
        # 一時カラム削除
        df.drop(columns=['_horse_cumwins', '_horse_cumraces'], inplace=True)
        
        non_null = df['horse_prev_winrate'].notna().sum()
        logger.info(f"    horse_prev_winrate非NaN: {non_null:,}/{len(df):,} ({non_null/len(df)*100:.1f}%)")
        
        return df
    
    def _calc_jockey_prev_winrate(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        騎手累積勝率
        
        【リーク対策】
        - 騎手ごとに時系列ソートして cumsum().shift(1)
        """
        logger.info("  騎手累積勝率を計算中...")
        
        if 'jockey_id' not in df.columns:
            df['jockey_prev_winrate'] = np.nan
            return df
        
        # 騎手ごとに時系列ソート
        df = df.sort_values(['jockey_id', 'race_date']).reset_index(drop=True)
        
        # 累積勝利数
        df['_jockey_cumwins'] = df.groupby('jockey_id')['is_win'].cumsum().shift(1).fillna(0)
        df['_jockey_cumraces'] = df.groupby('jockey_id').cumcount()
        df['jockey_prev_winrate'] = df['_jockey_cumwins'] / df['_jockey_cumraces'].replace(0, np.nan)
        
        # 一時カラム削除
        df.drop(columns=['_jockey_cumwins', '_jockey_cumraces'], inplace=True)
        
        # 馬順でソートし直す
        df = df.sort_values(['horse_id', 'race_date']).reset_index(drop=True)
        
        non_null = df['jockey_prev_winrate'].notna().sum()
        logger.info(f"    jockey_prev_winrate非NaN: {non_null:,}/{len(df):,} ({non_null/len(df)*100:.1f}%)")
        
        return df
    
    def _calc_trainer_prev_winrate(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        調教師累積勝率
        """
        logger.info("  調教師累積勝率を計算中...")
        
        if 'trainer_id' not in df.columns:
            df['trainer_prev_winrate'] = np.nan
            return df
        
        df = df.sort_values(['trainer_id', 'race_date']).reset_index(drop=True)
        
        df['_trainer_cumwins'] = df.groupby('trainer_id')['is_win'].cumsum().shift(1).fillna(0)
        df['_trainer_cumraces'] = df.groupby('trainer_id').cumcount()
        df['trainer_prev_winrate'] = df['_trainer_cumwins'] / df['_trainer_cumraces'].replace(0, np.nan)
        
        df.drop(columns=['_trainer_cumwins', '_trainer_cumraces'], inplace=True)
        df = df.sort_values(['horse_id', 'race_date']).reset_index(drop=True)
        
        non_null = df['trainer_prev_winrate'].notna().sum()
        logger.info(f"    trainer_prev_winrate非NaN: {non_null:,}/{len(df):,} ({non_null/len(df)*100:.1f}%)")
        
        return df
    
    def _calc_late_race_flag(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        R8-12レースフラグ
        
        注: これはリークではない（レース番号はレース前に確定）
        """
        logger.info("  R8-12レースフラグを計算中...")
        
        if 'race_id' not in df.columns:
            df['is_late_race'] = 0
            return df
        
        # race_idからレース番号を抽出
        df['_race_number'] = df['race_id'].astype(str).str[-2:].astype(int)
        df['is_late_race'] = ((df['_race_number'] >= 8) & (df['_race_number'] <= 12)).astype(int)
        
        df.drop(columns=['_race_number'], inplace=True)
        
        late_count = df['is_late_race'].sum()
        logger.info(f"    is_late_race=1: {late_count:,}/{len(df):,} ({late_count/len(df)*100:.1f}%)")
        
        return df
    
    def _calc_prev_finish(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        前走着順
        
        【リーク対策】
        - shift(1)で前走の着順のみ参照
        - 当該レースの着順は使用しない
        """
        logger.info("  前走着順を計算中...")
        
        if 'finish_position' not in df.columns:
            df['prev_finish'] = np.nan
            df['prev_finish_cat'] = np.nan
            return df
        
        df = df.sort_values(['horse_id', 'race_date']).reset_index(drop=True)
        
        # 前走着順（shift(1)で過去のみ）
        df['prev_finish'] = df.groupby('horse_id')['finish_position'].shift(1)
        
        # カテゴリ化
        bins = [0, 3, 6, 99]
        labels = ['top3', 'mid', 'bottom']
        df['prev_finish_cat'] = pd.cut(
            df['prev_finish'], 
            bins=bins, 
            labels=labels,
            include_lowest=True
        )
        
        non_null = df['prev_finish'].notna().sum()
        logger.info(f"    prev_finish非NaN: {non_null:,}/{len(df):,} ({non_null/len(df)*100:.1f}%)")
        
        return df
    
    def get_feature_columns(self) -> List[str]:
        """特徴量カラム一覧を返す"""
        return self.FEATURE_COLS


if __name__ == "__main__":
    # テスト実行
    import sys
    from pathlib import Path
    
    logging.basicConfig(level=logging.INFO, format='%(name)s - %(levelname)s - %(message)s')
    
    BASE_PATH = Path(r"c:\Users\zk-ht\Keiba\Keiba_AI_v2\keibaai\data\parsed\parquet")
    
    print("=" * 80)
    print("LeakFreeFeatureEngineerV16 テスト")
    print("=" * 80)
    
    # データ読み込み
    races = pd.read_parquet(BASE_PATH / "races" / "races.parquet")
    corners = pd.read_parquet(BASE_PATH / "corners" / "corner_positions.parquet")
    pedigrees = pd.read_parquet(BASE_PATH / "pedigrees" / "pedigrees.parquet")
    
    print(f"レースデータ: {len(races):,}件")
    
    # 特徴量エンジニアリング
    fe = LeakFreeFeatureEngineerV16()
    fe.fit(races, pedigrees_df=pedigrees, corners_df=corners)
    
    # サンプルデータでtransform
    sample = races.head(10000).copy()
    result = fe.transform(sample)
    
    print("\n" + "=" * 80)
    print("V16新特徴量の統計")
    print("=" * 80)
    
    for col in LeakFreeFeatureEngineerV16.V16_NEW_FEATURES:
        if col in result.columns:
            if result[col].dtype in ['float64', 'int64', 'int32']:
                print(f"{col:25s}: 非欠損 {result[col].notna().sum():,}件, 平均 {result[col].mean():.4f}")
            else:
                print(f"{col:25s}: 非欠損 {result[col].notna().sum():,}件")
    
    print("\n完了")
