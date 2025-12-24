"""
Data Splitters Module (v5.1)

時系列を考慮したデータ分割クラス群。
データリークを防止し、Valid-Test差を最小化するための分割戦略を提供。

【クラス一覧】
- TimeSeriesRaceSplitter: レース単位の時系列クロスバリデーション
- StrictDataSplitter: Train/Valid/Test の厳格分離
- WalkForwardSplitter: ウォークフォワード検証用
"""

import pandas as pd
import numpy as np
from typing import List, Tuple, Generator, Optional
from datetime import datetime, timedelta
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class TimeSeriesRaceSplitter:
    """
    レース単位の時系列クロスバリデーション
    
    通常のTimeSeriesSplitと異なり、レースを途中で分割しない
    （同一レースの全馬が同じfoldに入る）
    """
    
    def __init__(self, n_splits: int = 5, gap_days: int = 0, min_train_races: int = 1000):
        self.n_splits = n_splits
        self.gap_days = gap_days
        self.min_train_races = min_train_races
    
    def split(self, df: pd.DataFrame, date_col: str = 'race_date', 
              race_id_col: str = 'race_id') -> Generator[Tuple[np.ndarray, np.ndarray], None, None]:
        """データフレームを時系列で分割"""
        df = df.copy()
        df[date_col] = pd.to_datetime(df[date_col])
        
        race_dates = df.groupby(race_id_col)[date_col].first().sort_values()
        unique_races = race_dates.index.tolist()
        n_races = len(unique_races)
        
        if n_races < self.min_train_races:
            raise ValueError(f"レース数({n_races})が最小Train期間({self.min_train_races})未満")
        
        valid_size = (n_races - self.min_train_races) // self.n_splits
        
        for fold in range(self.n_splits):
            valid_end = n_races - fold * valid_size
            valid_start = valid_end - valid_size
            train_end = valid_start - 1
            
            if self.gap_days > 0:
                gap_date = race_dates.iloc[valid_start] - timedelta(days=self.gap_days)
                train_mask = race_dates < gap_date
                train_races = race_dates[train_mask].index.tolist()
            else:
                train_races = unique_races[:train_end]
            
            valid_races = unique_races[valid_start:valid_end]
            
            if len(train_races) < self.min_train_races:
                continue
            
            train_idx = df[df[race_id_col].isin(train_races)].index.values
            valid_idx = df[df[race_id_col].isin(valid_races)].index.values
            
            yield train_idx, valid_idx
    
    def get_n_splits(self) -> int:
        return self.n_splits


class StrictDataSplitter:
    """Train/Valid/Test の厳格分離（Test期間データは学習に一切使用不可）"""
    
    def __init__(self, train_end: str = '2022-12-31', 
                 valid_end: str = '2023-12-31',
                 test_start: Optional[str] = None):
        self.train_end = pd.Timestamp(train_end)
        self.valid_end = pd.Timestamp(valid_end)
        self.test_start = pd.Timestamp(test_start) if test_start else self.valid_end + timedelta(days=1)
        
    def split(self, df: pd.DataFrame, date_col: str = 'race_date') -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        """データフレームをTrain/Valid/Testに分割"""
        df = df.copy()
        df[date_col] = pd.to_datetime(df[date_col])
        
        train_df = df[df[date_col] <= self.train_end].copy()
        valid_df = df[(df[date_col] > self.train_end) & (df[date_col] <= self.valid_end)].copy()
        test_df = df[df[date_col] >= self.test_start].copy()
        
        logging.info(f"StrictDataSplitter: Train={len(train_df):,}, Valid={len(valid_df):,}, Test={len(test_df):,}")
        return train_df, valid_df, test_df


class WalkForwardSplitter:
    """ウォークフォワード検証用スプリッター"""
    
    def __init__(self, train_months: int = 24, valid_months: int = 3, step_months: int = 3):
        self.train_months = train_months
        self.valid_months = valid_months
        self.step_months = step_months
    
    def split(self, df: pd.DataFrame, date_col: str = 'race_date',
              start_date: Optional[str] = None, end_date: Optional[str] = None
              ) -> Generator[Tuple[pd.DataFrame, pd.DataFrame, dict], None, None]:
        """ウォークフォワード方式で分割"""
        df = df.copy()
        df[date_col] = pd.to_datetime(df[date_col])
        
        if start_date is None:
            min_date = df[date_col].min()
            start_date = min_date + pd.DateOffset(months=self.train_months)
        else:
            start_date = pd.Timestamp(start_date)
        
        if end_date is None:
            end_date = df[date_col].max()
        else:
            end_date = pd.Timestamp(end_date)
        
        current_date = start_date
        fold = 0
        
        while current_date + pd.DateOffset(months=self.valid_months) <= end_date:
            train_start = df[date_col].min()
            train_end = current_date
            valid_start = current_date + timedelta(days=1)
            valid_end = current_date + pd.DateOffset(months=self.valid_months)
            
            train_df = df[(df[date_col] >= train_start) & (df[date_col] <= train_end)].copy()
            valid_df = df[(df[date_col] >= valid_start) & (df[date_col] <= valid_end)].copy()
            
            period_info = {
                'fold': fold,
                'train_end': train_end,
                'valid_start': valid_start,
                'valid_end': valid_end
            }
            
            if len(train_df) > 0 and len(valid_df) > 0:
                yield train_df, valid_df, period_info
            
            current_date += pd.DateOffset(months=self.step_months)
            fold += 1


def validate_no_leak(train_df: pd.DataFrame, valid_df: pd.DataFrame, 
                     test_df: pd.DataFrame, date_col: str = 'race_date') -> bool:
    """データリークがないことを検証"""
    train_max = train_df[date_col].max()
    valid_min = valid_df[date_col].min()
    valid_max = valid_df[date_col].max()
    test_min = test_df[date_col].min()
    
    assert train_max < valid_min, f"Train/Validが重複"
    assert valid_max < test_min, f"Valid/Testが重複"
    
    logging.info("✅ リーク検証OK: 期間の重複なし")
    return True
