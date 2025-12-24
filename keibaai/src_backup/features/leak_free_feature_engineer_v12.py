"""
リークフリー特徴量エンジニア V12

V8で混入した「クラス変動特徴量」の重大なリーク（未来のクラス情報を過去に適用）を修正するバージョン。
V10.2をベースにしつつ、問題のメソッドをオーバーライドして修正する。
"""

import pandas as pd
import numpy as np
from typing import Optional, List
from dataclasses import dataclass
import logging

from .leak_free_feature_engineer_v10_2 import LeakFreeFeatureEngineerV10_2, FeatureConfigV10_2

logger = logging.getLogger(__name__)


@dataclass
class FeatureConfigV12(FeatureConfigV10_2):
    """V12用設定"""
    pass


class LeakFreeFeatureEngineerV12(LeakFreeFeatureEngineerV10_2):
    """
    リークフリー特徴量エンジニア V12 (Leak Fix Version)
    
    修正内容:
    - _calc_class_change: V8の実装では、各馬の「履歴データの最後のレース」の前走情報を、
      その馬の全ての行にマージしていたため、未来のクラス情報が過去にリークしていた。
      これを「その時点での前走クラス」を正しく計算するように修正。
    """
    
    def __init__(self, config: Optional[FeatureConfigV12] = None):
        super().__init__(config or FeatureConfigV12())
    
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        transformをオーバーライドして、正しい順序で処理を行う。
        """
        # 親クラスのtransformを呼ぶと、V8のバグありメソッドが呼ばれてしまう可能性があるため、
        # ここで処理フローを再構築するのは複雑。
        # 代わりに、V8の_calc_class_changeが呼ばれた"後"に、正しい値で上書きする戦略をとるか、
        # あるいはV8のメソッド自体をオーバーライドする（Pythonの継承仕様上、これでOK）。
        
        return super().transform(df)
    
    def _calc_class_change(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        クラス変動特徴量（修正版）
        
        Point-in-Timeで正しい「前走クラス」を計算する。
        """
        logger.info("LeakFreeFeatureEngineerV12: クラス変動特徴量を計算（Leak Fix版）")
        
        # 必要なカラムがあるか確認
        if 'race_class' not in df.columns:
            # まだ計算されていない場合は追加（通常は_add_race_classで追加済）
            df = self._add_race_class(df)
            
        # 計算用の一時DataFrameを作成
        # 履歴データと現在のデータを結合して時系列順に並べる
        
        # 1. 履歴データ（Train期間）
        hist = self._full_history.copy()
        hist['dset_type'] = 'hist'
        
        # 2. 現在のデータ
        current = df.copy()
        current['dset_type'] = 'current'
        
        # 重複を除くために、現在のデータに含まれるレースIDは履歴から除外する
        # (Trainデータのtransform時は、histとcurrentが同じになるため)
        current_ids = set(current['race_id'].unique())
        hist = hist[~hist['race_id'].isin(current_ids)]
        
        # 結合
        combined = pd.concat([hist, current], ignore_index=True)
        
        # 時系列ソート
        if 'race_date' not in combined.columns:
             # race_idから日付を推測できないので、元のdfにある必要がある
             logger.warning("race_date not found for class change calculation")
             return df
             
        combined = combined.sort_values(['horse_id', 'race_date', 'race_id'])
        
        # 前走クラスを取得（shift 1）
        combined['correct_prev_race_class'] = combined.groupby('horse_id')['race_class'].shift(1)
        
        # 現在のデータのみを取り出す
        # 元のインデックスや順序を保持するために、マージを使用
        current_with_prev = combined[combined['dset_type'] == 'current'][['race_id', 'horse_id', 'correct_prev_race_class']]
        
        # 元のdfにマージ
        # race_idとhorse_idでマージ
        df = df.merge(current_with_prev, on=['race_id', 'horse_id'], how='left')
        
        # 特徴量計算
        df['prev_race_class'] = df['correct_prev_race_class']
        
        # クラス変動計算
        df['class_change'] = df['race_class'] - df['prev_race_class']
        df['is_class_up'] = (df['class_change'] > 0).astype(float)
        df['is_class_down'] = (df['class_change'] < 0).astype(float)
        
        # NA埋め
        df['class_change'] = df['class_change'].fillna(0)
        df['is_class_up'] = df['is_class_up'].fillna(0)
        df['is_class_down'] = df['is_class_down'].fillna(0)
        
        # 不要カラム削除
        df = df.drop(columns=['correct_prev_race_class', 'dset_type'], errors='ignore')
        
        return df
