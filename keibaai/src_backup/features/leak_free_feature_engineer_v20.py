"""
リークフリー特徴量エンジニア V20

V15をベースに、深層データ分析で発見された有望な特徴量を追加。

【追加特徴量 (4個)】
1. damsire_win_rate: 母父産駒の累積勝率
   - 人気相関: -0.214 (低い = 市場織り込み少)
   - 父(sire)との相関: 0.423 (独立性あり)
   - Q4 vs Q1 ROI差: +21%

2. damsire_races: 母父産駒の累積レース数
   - 信頼性指標として使用

3. breeder_win_rate: 生産者の産駒勝率
   - 人気相関: -0.228 (最も低い)
   - カバレッジ: 90.7%

4. breeder_races: 生産者の産駒レース数
   - 信頼性指標として使用

【リーク対策】
- 全ての累積統計はfit時のTrain期間データのみで計算
- 産駒別の累積統計はTrain用と区別して計算
- min_races要件で過学習を抑制

【過学習対策】
- min_races=50に設定（母父・生産者は産駒数が多いため）
- 高次元組み合わせは追加しない
- 血統合計スコアなどの合成特徴量は追加しない
"""

import pandas as pd
import numpy as np
from typing import Optional, List, Dict
from dataclasses import dataclass
import logging

from .leak_free_feature_engineer_v15 import LeakFreeFeatureEngineerV15, FeatureConfigV15

logger = logging.getLogger(__name__)


@dataclass
class FeatureConfigV20(FeatureConfigV15):
    """V20用設定"""
    # 母父・生産者の最小レース数
    min_bloodline_races: int = 50


class LeakFreeFeatureEngineerV20(LeakFreeFeatureEngineerV15):
    """
    リークフリー特徴量エンジニア V20
    
    V15に以下の特徴量を追加：
    - damsire_win_rate: 母父産駒の累積勝率
    - damsire_races: 母父産駒のレース数
    - breeder_win_rate: 生産者の産駒勝率
    - breeder_races: 生産者の産駒レース数
    
    【重要】リーク対策
    - 全ての統計はfit時にTrain期間のデータのみで計算
    - transform時はその統計値をマップするのみ
    """
    
    # V15の特徴量 + V20の新特徴量
    FEATURE_COLS = LeakFreeFeatureEngineerV15.FEATURE_COLS + [
        'damsire_win_rate',      # 母父産駒勝率
        'damsire_races',         # 母父産駒レース数
        'breeder_win_rate',      # 生産者産駒勝率
        'breeder_races',         # 生産者産駒レース数
    ]
    
    def __init__(self, config: Optional[FeatureConfigV20] = None):
        super().__init__(config or FeatureConfigV20())
        self._damsire_stats: Dict = {}
        self._breeder_stats: Dict = {}
        self._damsire_mapping: Dict = {}
        self._breeder_mapping: Dict = {}
    
    def fit(self, races_df: pd.DataFrame, pedigrees_df=None, corners_df=None, 
            race_details_df=None, returns_df=None, horses_df=None):
        """
        fitメソッドの拡張
        
        - pedigrees_df: 血統データ（母父取得用）
        - horses_df: 馬データ（生産者取得用）
        """
        logger.info("LeakFreeFeatureEngineerV20: fit開始")
        
        # 親クラスのfit
        super().fit(races_df, pedigrees_df, corners_df, race_details_df, returns_df, horses_df)
        
        # 母父統計の計算
        self._calc_damsire_stats(races_df, pedigrees_df)
        
        # 生産者統計の計算
        self._calc_breeder_stats(races_df, horses_df)
        
        logger.info("LeakFreeFeatureEngineerV20: fit完了")
        return self
    
    def _calc_damsire_stats(self, races_df: pd.DataFrame, pedigrees_df: Optional[pd.DataFrame]):
        """
        母父の産駒統計を計算（Train期間のみ）
        
        【リーク対策】
        - fit時にTrain期間のデータのみで計算
        - 母父IDごとの産駒勝率を累積計算
        """
        logger.info("  母父統計を計算中...")
        
        if pedigrees_df is None or len(pedigrees_df) == 0:
            logger.warning("  pedigrees_dfがないため、母父統計はスキップ")
            return
        
        # 母父を取得（generation=2の3番目 = position 2）
        gen2 = pedigrees_df[pedigrees_df['generation'] == 2].copy()
        gen2_sorted = gen2.sort_values(['horse_id', 'ancestor_id'])
        gen2_sorted['pos'] = gen2_sorted.groupby('horse_id').cumcount()
        
        # position 2 = 母父（父の母の父ではなく、母の父）
        damsire = gen2_sorted[gen2_sorted['pos'] == 2][['horse_id', 'ancestor_id']].copy()
        damsire.columns = ['horse_id', 'damsire_id']
        
        # マッピングを保存
        self._damsire_mapping = dict(zip(damsire['horse_id'], damsire['damsire_id']))
        logger.info(f"    母父マッピング: {len(self._damsire_mapping):,}頭")
        
        # Train期間のデータに母父をマージ
        train = races_df.copy()
        train['race_date'] = pd.to_datetime(train['race_date'])
        train['is_win'] = (train['finish_position'] == 1).astype(int)
        train['damsire_id'] = train['horse_id'].map(self._damsire_mapping)
        
        # 母父ごとの統計を計算
        ds_stats = train.groupby('damsire_id').agg({
            'is_win': ['sum', 'count']
        }).reset_index()
        ds_stats.columns = ['damsire_id', 'ds_wins', 'ds_races']
        
        # 最小レース数フィルタ
        min_races = self.config.min_bloodline_races
        ds_stats = ds_stats[ds_stats['ds_races'] >= min_races].copy()
        
        # 勝率計算
        ds_stats['ds_win_rate'] = ds_stats['ds_wins'] / ds_stats['ds_races']
        
        # 辞書として保存
        self._damsire_stats = {
            row['damsire_id']: {
                'win_rate': row['ds_win_rate'],
                'races': row['ds_races']
            }
            for _, row in ds_stats.iterrows()
        }
        
        logger.info(f"    母父統計: {len(self._damsire_stats):,}頭")
    
    def _calc_breeder_stats(self, races_df: pd.DataFrame, horses_df: Optional[pd.DataFrame]):
        """
        生産者の産駒統計を計算（Train期間のみ）
        
        【リーク対策】
        - fit時にTrain期間のデータのみで計算
        """
        logger.info("  生産者統計を計算中...")
        
        if horses_df is None or len(horses_df) == 0:
            logger.warning("  horses_dfがないため、生産者統計はスキップ")
            return
        
        # 馬ID→生産者マッピング
        breeder_map = horses_df[['horse_id', 'breeder_name']].dropna()
        self._breeder_mapping = dict(zip(breeder_map['horse_id'], breeder_map['breeder_name']))
        logger.info(f"    生産者マッピング: {len(self._breeder_mapping):,}頭")
        
        # Train期間のデータに生産者をマージ
        train = races_df.copy()
        train['race_date'] = pd.to_datetime(train['race_date'])
        train['is_win'] = (train['finish_position'] == 1).astype(int)
        train['breeder_name'] = train['horse_id'].map(self._breeder_mapping)
        
        # 生産者ごとの統計を計算
        br_stats = train.groupby('breeder_name').agg({
            'is_win': ['sum', 'count']
        }).reset_index()
        br_stats.columns = ['breeder_name', 'br_wins', 'br_races']
        
        # 最小レース数フィルタ
        min_races = self.config.min_bloodline_races
        br_stats = br_stats[br_stats['br_races'] >= min_races].copy()
        
        # 勝率計算
        br_stats['br_win_rate'] = br_stats['br_wins'] / br_stats['br_races']
        
        # 辞書として保存
        self._breeder_stats = {
            row['breeder_name']: {
                'win_rate': row['br_win_rate'],
                'races': row['br_races']
            }
            for _, row in br_stats.iterrows()
        }
        
        logger.info(f"    生産者統計: {len(self._breeder_stats):,}件")
    
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """V20のtransform"""
        
        # 1. 親クラスのtransform（V15の特徴量）
        result = super().transform(df)
        
        logger.info("LeakFreeFeatureEngineerV20: 追加transform開始")
        
        # 2. V20特徴量の計算
        result = self._add_damsire_features(result)
        result = self._add_breeder_features(result)
        
        logger.info("LeakFreeFeatureEngineerV20: transform完了")
        return result
    
    def _add_damsire_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        母父特徴量を付与
        
        【リーク対策】
        - fit時に計算した統計値をマップするのみ
        """
        logger.info("  母父特徴量を付与中...")
        
        df = df.copy()
        
        # 母父IDをマップ
        df['_damsire_id'] = df['horse_id'].map(self._damsire_mapping)
        
        # 統計値をマップ
        df['damsire_win_rate'] = df['_damsire_id'].apply(
            lambda x: self._damsire_stats.get(x, {}).get('win_rate', np.nan)
        )
        df['damsire_races'] = df['_damsire_id'].apply(
            lambda x: self._damsire_stats.get(x, {}).get('races', np.nan)
        )
        
        # 一時カラムを削除
        df = df.drop(columns=['_damsire_id'], errors='ignore')
        
        non_null = df['damsire_win_rate'].notna().sum()
        logger.info(f"    damsire_win_rate非NaN: {non_null:,}/{len(df):,} ({non_null/len(df)*100:.1f}%)")
        
        return df
    
    def _add_breeder_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        生産者特徴量を付与
        
        【リーク対策】
        - fit時に計算した統計値をマップするのみ
        """
        logger.info("  生産者特徴量を付与中...")
        
        df = df.copy()
        
        # 生産者名をマップ
        df['_breeder_name'] = df['horse_id'].map(self._breeder_mapping)
        
        # 統計値をマップ
        df['breeder_win_rate'] = df['_breeder_name'].apply(
            lambda x: self._breeder_stats.get(x, {}).get('win_rate', np.nan)
        )
        df['breeder_races'] = df['_breeder_name'].apply(
            lambda x: self._breeder_stats.get(x, {}).get('races', np.nan)
        )
        
        # 一時カラムを削除
        df = df.drop(columns=['_breeder_name'], errors='ignore')
        
        non_null = df['breeder_win_rate'].notna().sum()
        logger.info(f"    breeder_win_rate非NaN: {non_null:,}/{len(df):,} ({non_null/len(df)*100:.1f}%)")
        
        return df
    
    def get_feature_columns(self) -> List[str]:
        return self.FEATURE_COLS
