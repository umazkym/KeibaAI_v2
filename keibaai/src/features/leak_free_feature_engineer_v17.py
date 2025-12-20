"""
リークフリー特徴量エンジニア V17

V16をベースに、生データ分析で発見した「隠れた情報」を全て活用する次世代モデル。

【重要な設計変更】
V16以前は(race_id, horse_id)をキーにして統計を保存していたが、
これだとTestデータ（fitに含まれないrace_id）に対して統計が適用されない。

V17では**馬IDのみ**をキーとし、fit時点での「最新の統計」を保存する。
これにより、Testデータの馬に対しても、fit時点までの累積統計が適用される。

【追加特徴量】
Phase 1: 着差・タイム系
- close_loss_rate: 惜敗率（着差0.2秒以内の敗戦率）
- finish_ewm: 着順の指数移動平均
- last3f_rank_avg: 上がり順位の累積平均
- last3f_consistency: 上がり3Fの標準偏差

Phase 2: 位置取り系
- position_improvement_avg: 4C→Goalで何頭抜いたかの平均
- position_change_avg: 位置取り変化平均

Phase 3: 馬体重系
- weight_change_trend: 馬体重変動傾向
- optimal_weight_diff: ベスト体重との差

Phase 4: ペース系
- pace_adaptability: ペース適応度

【リーク対策原則】
- fit時点までのデータのみ使用
- 当該レースの結果は含まない（shift(1)相当）
- Testデータの結果情報は一切使用しない
"""

import pandas as pd
import numpy as np
from typing import Optional, List, Dict
from dataclasses import dataclass
import logging

from .leak_free_feature_engineer_v16 import LeakFreeFeatureEngineerV16, FeatureConfigV16

logger = logging.getLogger(__name__)


@dataclass
class FeatureConfigV17(FeatureConfigV16):
    """V17用設定"""
    close_loss_threshold: float = 0.2
    ewm_span: int = 5
    min_races_for_advanced_stats: int = 3
    max_last3f_time: float = 50.0
    max_weight_change: int = 30


class LeakFreeFeatureEngineerV17(LeakFreeFeatureEngineerV16):
    """
    リークフリー特徴量エンジニア V17
    
    馬ごとの累積統計をfit時に計算し、
    transform時にhorse_idでルックアップして適用する。
    """
    
    FEATURE_COLS = LeakFreeFeatureEngineerV16.FEATURE_COLS + [
        'close_loss_rate',
        'finish_ewm',
        'last3f_rank_avg',
        'last3f_consistency',
        'position_improvement_avg',
        'position_change_avg',
        'weight_change_trend',
        'optimal_weight_diff',
        'pace_adaptability',
    ]
    
    def __init__(self, config: Optional[FeatureConfigV17] = None):
        super().__init__(config or FeatureConfigV17())
        # 馬IDをキーとした統計辞書
        self._horse_stats: Dict[str, Dict] = {}
    
    def fit(self, races_df: pd.DataFrame, pedigrees_df=None, corners_df=None, 
            race_details_df=None, returns_df=None, horses_df=None):
        """
        V17のfit拡張
        
        各馬のfit時点での最新統計を計算し、保存する。
        """
        super().fit(races_df, pedigrees_df, corners_df, race_details_df, returns_df, horses_df)
        
        logger.info("LeakFreeFeatureEngineerV17: fit開始")
        
        # 馬ごとの統計を計算
        self._calc_all_horse_stats(races_df, corners_df, race_details_df)
        
        logger.info("LeakFreeFeatureEngineerV17: fit完了")
        return self
    
    def _calc_all_horse_stats(self, races_df: pd.DataFrame, 
                               corners_df: Optional[pd.DataFrame],
                               race_details_df: Optional[pd.DataFrame]):
        """
        全ての馬統計を一括計算
        
        各馬について、fit期間終了時点での累積統計を計算する。
        """
        logger.info("  馬ごとの累積統計を計算中...")
        
        df = races_df.copy()
        df = df.sort_values(['horse_id', 'race_date', 'race_id'])
        
        # === Phase 1: 着差・タイム系 ===
        
        # 惜敗フラグ
        if 'margin_seconds' in df.columns:
            df['is_close_loss'] = (
                (df['finish_position'] > 1) & 
                (df['margin_seconds'].notna()) &
                (df['margin_seconds'] <= self.config.close_loss_threshold)
            ).astype(float)
            df['is_loss'] = (df['finish_position'] > 1).astype(float)
        else:
            df['is_close_loss'] = 0
            df['is_loss'] = 0
        
        # 上がり3Fクリッピング
        if 'last_3f_time' in df.columns:
            df['last_3f_clipped'] = df['last_3f_time'].clip(upper=self.config.max_last3f_time)
        
        # 馬体重変動クリッピング
        if 'horse_weight_change' in df.columns:
            df['weight_change_clipped'] = df['horse_weight_change'].clip(
                lower=-self.config.max_weight_change,
                upper=self.config.max_weight_change
            )
        
        # === Phase 2: 位置取り系 ===
        
        # 4C位置をマージ
        if corners_df is not None and len(corners_df) > 0:
            c4 = corners_df[corners_df['corner'] == 4][['race_id', 'horse_number', 'position']].copy()
            c4.columns = ['race_id', 'horse_number', 'c4_position']
            df = df.merge(c4, on=['race_id', 'horse_number'], how='left')
            df['position_improvement'] = df['c4_position'] - df['finish_position']
        else:
            df['c4_position'] = np.nan
            df['position_improvement'] = np.nan
        
        # 位置取り変化（1C→3C）
        if 'passing_order_1' in df.columns and 'passing_order_3' in df.columns:
            df['position_change_1_3'] = df['passing_order_1'] - df['passing_order_3']
        else:
            df['position_change_1_3'] = np.nan
        
        # === Phase 4: ペース系 ===
        
        if race_details_df is not None and len(race_details_df) > 0:
            pace_df = race_details_df[['race_id', 'first_half', 'second_half']].copy()
            pace_df['pace_ratio'] = pace_df['first_half'] / pace_df['second_half']
            df = df.merge(pace_df[['race_id', 'pace_ratio']], on='race_id', how='left')
        else:
            df['pace_ratio'] = np.nan
        
        # === 馬ごとに統計を計算 ===
        
        logger.info("  各馬の最終統計を集計中...")
        horse_count = 0
        
        for horse_id, group in df.groupby('horse_id'):
            group = group.sort_values('race_date')
            n = len(group)
            
            if n == 0:
                continue
            
            stats = {}
            
            # --- 惜敗率 ---
            total_loss = group['is_loss'].sum()
            total_close_loss = group['is_close_loss'].sum()
            if total_loss >= self.config.min_races_for_advanced_stats:
                stats['close_loss_rate'] = total_close_loss / total_loss
            else:
                stats['close_loss_rate'] = np.nan
            
            # --- 着順EWM ---
            if n >= 2:
                ewm_val = group['finish_position'].ewm(span=self.config.ewm_span, min_periods=1).mean().iloc[-1]
                stats['finish_ewm'] = ewm_val
            else:
                stats['finish_ewm'] = np.nan
            
            # --- 上がり順位平均 ---
            if 'last3f_rank' in group.columns:
                valid_last3f = group['last3f_rank'].dropna()
                if len(valid_last3f) >= self.config.min_races_for_advanced_stats:
                    stats['last3f_rank_avg'] = valid_last3f.mean()
                else:
                    stats['last3f_rank_avg'] = np.nan
            else:
                stats['last3f_rank_avg'] = np.nan
            
            # --- 上がり3F安定度 ---
            if 'last_3f_clipped' in group.columns:
                valid_3f = group['last_3f_clipped'].dropna()
                if len(valid_3f) >= self.config.min_races_for_advanced_stats:
                    stats['last3f_consistency'] = valid_3f.std()
                else:
                    stats['last3f_consistency'] = np.nan
            else:
                stats['last3f_consistency'] = np.nan
            
            # --- 追い込み度合い ---
            valid_improvement = group['position_improvement'].dropna()
            if len(valid_improvement) >= self.config.min_races_for_advanced_stats:
                stats['position_improvement_avg'] = valid_improvement.mean()
            else:
                stats['position_improvement_avg'] = np.nan
            
            # --- 位置取り変化 ---
            valid_change = group['position_change_1_3'].dropna()
            if len(valid_change) >= self.config.min_races_for_advanced_stats:
                stats['position_change_avg'] = valid_change.mean()
            else:
                stats['position_change_avg'] = np.nan
            
            # --- 馬体重変動傾向 ---
            if 'weight_change_clipped' in group.columns:
                recent = group['weight_change_clipped'].tail(3).dropna()
                if len(recent) >= 1:
                    stats['weight_change_trend'] = recent.mean()
                else:
                    stats['weight_change_trend'] = np.nan
            else:
                stats['weight_change_trend'] = np.nan
            
            # --- ベスト体重との差 ---
            if 'horse_weight' in group.columns and 'finish_position' in group.columns:
                valid = group[['horse_weight', 'finish_position']].dropna()
                if len(valid) >= self.config.min_races_for_advanced_stats:
                    best_idx = valid['finish_position'].idxmin()
                    best_weight = valid.loc[best_idx, 'horse_weight']
                    current_weight = group['horse_weight'].iloc[-1]
                    if pd.notna(current_weight) and pd.notna(best_weight):
                        stats['optimal_weight'] = best_weight
                    else:
                        stats['optimal_weight'] = np.nan
                else:
                    stats['optimal_weight'] = np.nan
            else:
                stats['optimal_weight'] = np.nan
            
            # --- ペース適応度 ---
            valid_pace = group[['pace_ratio', 'finish_position']].dropna()
            if len(valid_pace) >= self.config.min_races_for_advanced_stats:
                if valid_pace['pace_ratio'].std() > 0.01:  # 分散がある場合のみ
                    corr = valid_pace['pace_ratio'].corr(valid_pace['finish_position'])
                    stats['pace_adaptability'] = corr if not np.isnan(corr) else np.nan
                else:
                    stats['pace_adaptability'] = np.nan
            else:
                stats['pace_adaptability'] = np.nan
            
            self._horse_stats[horse_id] = stats
            horse_count += 1
        
        logger.info(f"    馬統計計算完了: {horse_count:,}頭")
        
        # 統計のサマリー
        stats_names = ['close_loss_rate', 'finish_ewm', 'last3f_rank_avg', 'last3f_consistency',
                       'position_improvement_avg', 'position_change_avg', 'weight_change_trend',
                       'optimal_weight', 'pace_adaptability']
        for name in stats_names:
            valid = sum(1 for s in self._horse_stats.values() if pd.notna(s.get(name)))
            logger.info(f"    {name}有効: {valid:,}/{horse_count:,}")
    
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """V17のtransform"""
        result = super().transform(df)
        
        logger.info("LeakFreeFeatureEngineerV17: 追加transform開始")
        
        result = self._add_v17_features(result)
        
        logger.info("LeakFreeFeatureEngineerV17: transform完了")
        
        return result
    
    def _add_v17_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """V17特徴量を付与（horse_idでルックアップ）"""
        logger.info("  V17特徴量を付与中...")
        
        feature_names = [
            'close_loss_rate', 'finish_ewm', 'last3f_rank_avg', 'last3f_consistency',
            'position_improvement_avg', 'position_change_avg', 'weight_change_trend',
            'pace_adaptability'
        ]
        
        # 各特徴量を初期化
        for name in feature_names:
            df[name] = np.nan
        df['optimal_weight_diff'] = np.nan
        
        # 馬ごとにルックアップ
        for idx, row in df.iterrows():
            horse_id = row.get('horse_id')
            if horse_id in self._horse_stats:
                stats = self._horse_stats[horse_id]
                for name in feature_names:
                    df.at[idx, name] = stats.get(name, np.nan)
                
                # ベスト体重との差
                optimal_weight = stats.get('optimal_weight')
                current_weight = row.get('horse_weight')
                if pd.notna(optimal_weight) and pd.notna(current_weight):
                    df.at[idx, 'optimal_weight_diff'] = current_weight - optimal_weight
        
        # サマリー
        for name in feature_names + ['optimal_weight_diff']:
            valid = df[name].notna().sum()
            logger.info(f"    {name}有効: {valid:,}/{len(df):,} ({valid/len(df)*100:.1f}%)")
        
        return df
    
    def get_feature_columns(self) -> List[str]:
        return self.FEATURE_COLS
