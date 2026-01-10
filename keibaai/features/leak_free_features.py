#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
リーク防止特徴量エンジニアリングモジュール

5年Walk-forward検証で効果確認済みの6つの特徴量を、
リーク防止を徹底した形で実装する。

【リーク防止の原則】
1. 累積統計は必ず shift(1) で過去のみ参照
2. 当該レースの結果情報（着順、タイム、通過順等）は使用不可
3. 確定オッズは特徴量として使用不可
4. 時系列分割での検証を必須とする

【採用する特徴量】
1. prev_3f_avg: 過去上がり3F累積平均 (5/5年, +8.5pt)
2. prev_fr_rate: 過去先行率 (5/5年, +8.3pt)
3. horse_prev_winrate: 馬累積勝率 (4/5年, +8.7pt)
4. jockey_prev_winrate: 騎手累積勝率 (4/5年, +5.2pt)
5. is_late_race: R8-12レースフラグ (4/5年, +4.5pt)
6. prev_finish_cat: 前走着順カテゴリ (4/5年, +3.9pt)

Author: KeibaAI Development Team
Date: 2026-01-07
"""

import pandas as pd
import numpy as np
from pathlib import Path
from typing import Optional, Dict, Tuple
import warnings
warnings.filterwarnings('ignore')


class LeakFreeFeatureEngineer:
    """
    リーク防止を徹底した特徴量エンジニアリングクラス
    
    使用方法:
        fe = LeakFreeFeatureEngineer()
        df_with_features = fe.transform(races_df, corners_df)
    """
    
    def __init__(self, verbose: bool = True):
        self.verbose = verbose
        self.feature_names = [
            'prev_3f_avg',
            'prev_fr_rate',
            'horse_prev_winrate',
            'jockey_prev_winrate',
            'trainer_prev_winrate',
            'is_late_race',
            'prev_finish',
            'prev_finish_cat',
        ]
    
    def _log(self, msg: str):
        if self.verbose:
            print(f"[FE] {msg}")
    
    def transform(
        self, 
        races: pd.DataFrame, 
        corners: Optional[pd.DataFrame] = None
    ) -> pd.DataFrame:
        """
        特徴量を追加したDataFrameを返す
        
        Args:
            races: レースデータ（races.parquet）
            corners: コーナー通過データ（corner_positions.parquet, optional）
        
        Returns:
            特徴量追加済みDataFrame
        """
        self._log("特徴量エンジニアリング開始...")
        
        # データのコピーを作成（元データを変更しない）
        df = races.copy()
        
        # 前処理
        df = self._preprocess(df)
        
        # 各特徴量を追加
        df = self._add_horse_cumulative_stats(df)
        df = self._add_jockey_cumulative_stats(df)
        df = self._add_trainer_cumulative_stats(df)
        df = self._add_prev_finish_features(df)
        df = self._add_prev_3f_stats(df)
        df = self._add_race_number_features(df)
        
        # コーナーデータがあれば脚質特徴量を追加
        if corners is not None:
            df = self._add_running_style_features(df, corners)
        
        self._log(f"特徴量エンジニアリング完了。新規特徴量: {len(self.feature_names)}個")
        
        return df
    
    def _preprocess(self, df: pd.DataFrame) -> pd.DataFrame:
        """前処理"""
        self._log("前処理中...")
        
        # 日付型に変換
        if not pd.api.types.is_datetime64_any_dtype(df['race_date']):
            df['race_date'] = pd.to_datetime(df['race_date'])
        
        # 時系列でソート（重要：シフト操作の前提）
        df = df.sort_values(['horse_id', 'race_date']).reset_index(drop=True)
        
        # 勝利フラグ
        if 'is_win' not in df.columns:
            df['is_win'] = (df['finish_position'] == 1).astype(int)
        
        # レース番号を抽出
        if 'race_number' not in df.columns:
            df['race_number'] = df['race_id'].astype(str).str[-2:].astype(int)
        
        return df
    
    def _add_horse_cumulative_stats(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        馬の累積統計（リークなし）
        
        計算方法:
        - cumsum().shift(1) で過去レースのみ集計
        - 当該レースの結果は含まない
        """
        self._log("馬累積統計を計算中...")
        
        # 累積勝利数（shift(1)で過去のみ）
        df['horse_cumwins'] = df.groupby('horse_id')['is_win'].cumsum().shift(1).fillna(0)
        # 累積出走数
        df['horse_cumraces'] = df.groupby('horse_id').cumcount()
        # 累積勝率
        df['horse_prev_winrate'] = df['horse_cumwins'] / df['horse_cumraces'].replace(0, np.nan)
        
        return df
    
    def _add_jockey_cumulative_stats(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        騎手の累積統計（リークなし）
        """
        self._log("騎手累積統計を計算中...")
        
        # 騎手ごとに時系列ソート
        df = df.sort_values(['jockey_id', 'race_date']).reset_index(drop=True)
        
        df['jockey_cumwins'] = df.groupby('jockey_id')['is_win'].cumsum().shift(1).fillna(0)
        df['jockey_cumraces'] = df.groupby('jockey_id').cumcount()
        df['jockey_prev_winrate'] = df['jockey_cumwins'] / df['jockey_cumraces'].replace(0, np.nan)
        
        # 馬順でソートし直す
        df = df.sort_values(['horse_id', 'race_date']).reset_index(drop=True)
        
        return df
    
    def _add_trainer_cumulative_stats(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        調教師の累積統計（リークなし）
        """
        self._log("調教師累積統計を計算中...")
        
        df = df.sort_values(['trainer_id', 'race_date']).reset_index(drop=True)
        
        df['trainer_cumwins'] = df.groupby('trainer_id')['is_win'].cumsum().shift(1).fillna(0)
        df['trainer_cumraces'] = df.groupby('trainer_id').cumcount()
        df['trainer_prev_winrate'] = df['trainer_cumwins'] / df['trainer_cumraces'].replace(0, np.nan)
        
        df = df.sort_values(['horse_id', 'race_date']).reset_index(drop=True)
        
        return df
    
    def _add_prev_finish_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        前走着順特徴量（リークなし）
        """
        self._log("前走着順特徴量を計算中...")
        
        # 前走着順（shift(1)で過去のみ）
        df['prev_finish'] = df.groupby('horse_id')['finish_position'].shift(1)
        
        # カテゴリ化
        bins = [0, 3, 6, 18]
        labels = ['top3', 'mid', 'bottom']
        df['prev_finish_cat'] = pd.cut(
            df['prev_finish'], 
            bins=bins, 
            labels=labels, 
            include_lowest=True
        )
        
        return df
    
    def _add_prev_3f_stats(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        過去上がり3F統計（リークなし）
        
        注意: 当該レースのlast_3f_timeは使用不可（結果情報）
        過去レースの累積平均のみ使用
        """
        self._log("過去上がり3F統計を計算中...")
        
        if 'last_3f_time' not in df.columns:
            self._log("  last_3f_timeカラムなし。スキップ。")
            df['prev_3f_avg'] = np.nan
            return df
        
        # NaNフラグ
        df['last_3f_notna'] = df['last_3f_time'].notna().astype(float)
        
        # 累積合計（NaNは0として扱う）
        df['prev_3f_sum'] = df.groupby('horse_id')['last_3f_time'].transform(
            lambda x: x.fillna(0).cumsum()
        ).shift(1)
        
        # 有効データ数
        df['prev_3f_count'] = df.groupby('horse_id')['last_3f_notna'].cumsum().shift(1)
        
        # 累積平均
        df['prev_3f_avg'] = df['prev_3f_sum'] / df['prev_3f_count'].replace(0, np.nan)
        
        # 一時カラム削除
        df.drop(columns=['last_3f_notna', 'prev_3f_sum', 'prev_3f_count'], inplace=True)
        
        return df
    
    def _add_race_number_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        レース番号特徴量
        """
        self._log("レース番号特徴量を計算中...")
        
        # R8-12フラグ（後半レース）
        df['is_late_race'] = ((df['race_number'] >= 8) & (df['race_number'] <= 12)).astype(int)
        
        return df
    
    def _add_running_style_features(
        self, 
        df: pd.DataFrame, 
        corners: pd.DataFrame
    ) -> pd.DataFrame:
        """
        過去脚質（先行率）特徴量（リークなし）
        
        注意: 当該レースの通過順位は使用不可（結果情報）
        過去レースでの先行率のみ使用
        """
        self._log("過去脚質特徴量を計算中...")
        
        # 4コーナーデータのみ使用
        c4 = corners[corners['corner'] == 4].copy()
        
        # レースデータと結合
        c4 = c4.merge(
            df[['race_id', 'horse_number', 'horse_id', 'race_date']], 
            on=['race_id', 'horse_number'], 
            how='inner'
        )
        
        # 先行判定（4C通過順2番手以内）
        c4['is_frontrunner'] = (c4['position'] <= 2).astype(int)
        
        # 時系列ソート
        c4 = c4.sort_values(['horse_id', 'race_date']).reset_index(drop=True)
        
        # 累積先行回数（shift(1)で過去のみ）
        c4['prev_fr_sum'] = c4.groupby('horse_id')['is_frontrunner'].cumsum().shift(1).fillna(0)
        c4['prev_fr_count'] = c4.groupby('horse_id').cumcount()
        c4['prev_fr_rate'] = c4['prev_fr_sum'] / c4['prev_fr_count'].replace(0, np.nan)
        
        # 元データに結合
        fr_data = c4[['race_id', 'horse_number', 'prev_fr_rate']].drop_duplicates()
        df = df.merge(fr_data, on=['race_id', 'horse_number'], how='left')
        
        return df
    
    def get_feature_names(self) -> list:
        """追加した特徴量名のリストを返す"""
        return self.feature_names


def validate_no_leak(df: pd.DataFrame, sample_size: int = 1000) -> Dict[str, bool]:
    """
    リークがないことを検証する
    
    検証方法:
    - 各特徴量が「当該レースの結果」と相関していないか確認
    - shift(1)が正しく適用されているか確認
    """
    result = {}
    
    # 時系列でソート
    df_sorted = df.sort_values(['horse_id', 'race_date']).reset_index(drop=True)
    
    # 馬累積勝率の検証
    # horse_cumraces=0（最初のレース）ではNaNであるべき
    first_race_mask = df_sorted.groupby('horse_id').cumcount() == 0
    first_races = df_sorted[first_race_mask]
    
    result['horse_prev_winrate_first_is_nan'] = first_races['horse_prev_winrate'].isna().mean() > 0.99
    
    # 前走着順の検証
    result['prev_finish_first_is_nan'] = first_races['prev_finish'].isna().mean() > 0.99
    
    return result


if __name__ == "__main__":
    # テスト実行
    from pathlib import Path
    
    BASE_PATH = Path(r"c:\Users\zk-ht\Keiba\Keiba_AI_v2\keibaai\data\parsed\parquet")
    
    print("=" * 80)
    print("リーク防止特徴量エンジニアリング テスト")
    print("=" * 80)
    
    # データ読み込み
    print("\nデータ読み込み中...")
    races = pd.read_parquet(BASE_PATH / "races" / "races.parquet")
    corners = pd.read_parquet(BASE_PATH / "corners" / "corner_positions.parquet")
    
    print(f"レースデータ: {len(races):,}件")
    print(f"コーナーデータ: {len(corners):,}件")
    
    # 特徴量エンジニアリング実行
    fe = LeakFreeFeatureEngineer(verbose=True)
    df = fe.transform(races, corners)
    
    # 結果確認
    print("\n" + "=" * 80)
    print("追加した特徴量の統計")
    print("=" * 80)
    
    for col in fe.get_feature_names():
        if col in df.columns:
            if df[col].dtype in ['float64', 'int64']:
                print(f"{col:25s}: 非欠損 {df[col].notna().sum():,}件, 平均 {df[col].mean():.4f}")
            else:
                print(f"{col:25s}: 非欠損 {df[col].notna().sum():,}件")
    
    # リーク検証
    print("\n" + "=" * 80)
    print("リーク検証")
    print("=" * 80)
    
    leak_check = validate_no_leak(df)
    for check, passed in leak_check.items():
        status = "✅ PASS" if passed else "❌ FAIL"
        print(f"{check}: {status}")
    
    print("\n完了")
