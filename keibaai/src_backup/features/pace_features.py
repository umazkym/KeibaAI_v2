"""
ペース特徴量生成モジュール（μモデル v11用）

【目的】
race_details.parquetを活用し、コースのペース傾向と馬のペース適性を特徴量化する。
穴馬発掘に貢献する「ペースミスマッチ」を検出することがゴール。

【生成する特徴量】
1. venue_surface_pace_tendency: 会場×馬場のペース傾向（Train期間固定）
2. horse_pace_preference: 馬の得意ペース（過去走からの累積）
3. horse_pace_fit: 馬のペース適性とコース傾向の適合度

【リーク防止】
- venue_surface_pace_tendency: Train期間（2022年末まで）の統計を固定使用
- horse_pace_preference: expanding().mean().shift(1)で累積計算
- レース結果（通過順、上がり3F等）は使用しない
"""

import pandas as pd
import numpy as np
import logging
from pathlib import Path


class PaceFeatureEngine:
    """ペース特徴量生成エンジン"""
    
    def __init__(self, train_cutoff: str = '2023-01-01'):
        """
        Args:
            train_cutoff: Train期間の終端日（この日より前のデータでコース統計を固定）
        """
        self.logger = logging.getLogger(__name__)
        self.train_cutoff = pd.Timestamp(train_cutoff)
        self.venue_surface_pace_stats = None  # Train期間で固定
        
    def load_race_details(self, race_details_path: str = 'keibaai/data/parsed/parquet/race_details/race_details.parquet') -> pd.DataFrame:
        """race_details.parquetを読み込み"""
        rd = pd.read_parquet(race_details_path)
        rd['race_id'] = rd['race_id'].astype(str)
        return rd
    
    def generate_features(
        self,
        df: pd.DataFrame,
        races_df: pd.DataFrame,
        race_details_df: pd.DataFrame = None
    ) -> pd.DataFrame:
        """
        ペース関連特徴量を生成
        
        Args:
            df: 特徴量を追加する対象DataFrame（出馬表ベース）
            races_df: レース結果DataFrame（venue, track_surface, distance_m含む）
            race_details_df: race_details.parquet（first_half, second_half含む）
        
        Returns:
            特徴量を追加したDataFrame
        """
        self.logger.info("=" * 60)
        self.logger.info("ペース特徴量の生成開始（v11 リークフリー）")
        self.logger.info("=" * 60)
        
        if race_details_df is None:
            race_details_df = self.load_race_details()
        
        # 型変換
        df = df.copy()
        df['race_id'] = df['race_id'].astype(str)
        races_df = races_df.copy()
        races_df['race_id'] = races_df['race_id'].astype(str)
        races_df['race_date'] = pd.to_datetime(races_df['race_date'])
        
        # race_detailsにレース情報を結合
        race_info = races_df.drop_duplicates('race_id')[['race_id', 'race_date', 'venue', 'track_surface', 'distance_m']]
        rd_merged = race_details_df.merge(race_info, on='race_id', how='left')
        
        # ペース差を計算 (first_half - second_half)
        # 正: 前傾（ハイペース）、負: 後傾（スローペース）
        rd_merged['pace_diff'] = rd_merged['first_half'] - rd_merged['second_half']
        
        # 1. 会場×馬場のペース傾向（Train期間固定）
        df = self._add_venue_surface_pace_tendency(df, rd_merged)
        
        # 2. 馬のペース適性（累積計算）
        df = self._add_horse_pace_preference(df, rd_merged, races_df)
        
        # 3. ペースフィット度
        df = self._add_pace_fit(df)
        
        self.logger.info("=" * 60)
        self.logger.info("ペース特徴量の生成完了")
        self.logger.info("=" * 60)
        
        return df
    
    def _add_venue_surface_pace_tendency(
        self, 
        df: pd.DataFrame, 
        rd_merged: pd.DataFrame
    ) -> pd.DataFrame:
        """
        【特徴量1】会場×馬場のペース傾向（Train期間固定）
        
        リーク防止: Train期間（train_cutoff以前）のデータのみで計算し、全期間に適用
        """
        self.logger.info("  [1/3] 会場×馬場ペース傾向（Train期間固定）...")
        
        # Train期間のデータのみで統計計算
        train_rd = rd_merged[rd_merged['race_date'] < self.train_cutoff].copy()
        
        if len(train_rd) == 0:
            self.logger.warning("  Train期間のrace_detailsがありません。会場ペース傾向をスキップします。")
            df['venue_surface_pace_tendency'] = 0.0
            return df
        
        # 会場×馬場別の平均ペース差
        pace_stats = train_rd.groupby(['venue', 'track_surface'])['pace_diff'].agg(['mean', 'std', 'count']).reset_index()
        pace_stats.columns = ['venue', 'track_surface', 'venue_surface_pace_tendency', 'pace_std', 'pace_count']
        
        # サンプル数が少ない場合はNaN
        pace_stats.loc[pace_stats['pace_count'] < 30, 'venue_surface_pace_tendency'] = np.nan
        
        # 保存（後で参照用）
        self.venue_surface_pace_stats = pace_stats
        
        # dfにマージ
        df = df.merge(
            pace_stats[['venue', 'track_surface', 'venue_surface_pace_tendency']],
            on=['venue', 'track_surface'],
            how='left'
        )
        
        # 欠損値はグローバル平均で埋める
        global_mean = train_rd['pace_diff'].mean()
        df['venue_surface_pace_tendency'] = df['venue_surface_pace_tendency'].fillna(global_mean)
        
        self.logger.info(f"    生成完了: {len(pace_stats)}パターン")
        
        return df
    
    def _add_horse_pace_preference(
        self,
        df: pd.DataFrame,
        rd_merged: pd.DataFrame,
        races_df: pd.DataFrame
    ) -> pd.DataFrame:
        """
        【特徴量2】馬のペース適性（累積計算、リークフリー）
        
        馬が過去に好走したレースのペース傾向を累積平均する。
        expanding().mean().shift(1)で、当該レース前までの情報のみ使用。
        """
        self.logger.info("  [2/3] 馬のペース適性（累積・shift済み）...")
        
        # races_dfからhorse_id, race_date, finish_positionを取得
        perf = races_df[['race_id', 'horse_id', 'race_date', 'finish_position']].copy()
        perf['horse_id'] = perf['horse_id'].astype(str)
        perf['finish_position'] = pd.to_numeric(perf['finish_position'], errors='coerce')
        
        # race_detailsのペース情報を結合
        perf = perf.merge(
            rd_merged[['race_id', 'pace_diff']],
            on='race_id',
            how='left'
        )
        
        # 3着以内のレースのみ考慮（好走時のペース傾向を学習）
        perf['is_good_run'] = (perf['finish_position'] <= 3).fillna(False)
        perf.loc[~perf['is_good_run'], 'pace_diff_good'] = np.nan
        perf.loc[perf['is_good_run'], 'pace_diff_good'] = perf.loc[perf['is_good_run'], 'pace_diff']
        
        # 時系列でソート
        perf = perf.sort_values(['horse_id', 'race_date']).reset_index(drop=True)
        
        # 累積平均（shift(1)で当該レース前まで）
        # expanding().mean()で累積、shift(1)で1行ずらして未来情報をリーク防止
        perf['horse_pace_preference'] = perf.groupby('horse_id')['pace_diff_good'].transform(
            lambda x: x.expanding().mean().shift(1)
        )
        
        # 全レースの平均ペースも計算（好走時に限らない）
        perf['horse_avg_pace_lf'] = perf.groupby('horse_id')['pace_diff'].transform(
            lambda x: x.expanding().mean().shift(1)
        )
        
        # dfにマージ
        merge_cols = ['race_id', 'horse_id', 'horse_pace_preference', 'horse_avg_pace_lf']
        df = df.merge(
            perf[merge_cols].drop_duplicates(subset=['race_id', 'horse_id']),
            on=['race_id', 'horse_id'],
            how='left'
        )
        
        self.logger.info("    生成完了: horse_pace_preference, horse_avg_pace_lf")
        
        return df
    
    def _add_pace_fit(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        【特徴量3】ペースフィット度
        
        馬の得意ペースとコースのペース傾向の適合度。
        差が小さいほど適合度が高い。
        
        pace_fit = -abs(horse_pace_preference - venue_surface_pace_tendency)
        """
        self.logger.info("  [3/3] ペースフィット度...")
        
        if 'horse_pace_preference' in df.columns and 'venue_surface_pace_tendency' in df.columns:
            # 差の絶対値（小さいほど良い）の符号反転（大きいほど良い、に変換）
            df['pace_fit'] = np.abs(
                df['horse_pace_preference'].fillna(0) - 
                df['venue_surface_pace_tendency'].fillna(0)
            ) * (-1)
            
            # 欠損値は0（影響なし）
            df['pace_fit'] = df['pace_fit'].fillna(0)
        else:
            df['pace_fit'] = 0
        
        self.logger.info("    生成完了: pace_fit")
        
        return df


if __name__ == "__main__":
    """テスト実行"""
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    
    # データ読み込み
    races = pd.read_parquet('keibaai/data/parsed/parquet/races/races.parquet')
    rd = pd.read_parquet('keibaai/data/parsed/parquet/race_details/race_details.parquet')
    
    # サンプルデータ作成（2024年1月の出馬）
    races['race_date'] = pd.to_datetime(races['race_date'])
    sample = races[(races['race_date'] >= '2024-01-01') & (races['race_date'] < '2024-02-01')].copy()
    
    print(f"サンプルデータ: {len(sample):,}行")
    
    # 特徴量生成
    engine = PaceFeatureEngine(train_cutoff='2023-01-01')
    result = engine.generate_features(sample, races, rd)
    
    # 結果確認
    pace_cols = ['venue_surface_pace_tendency', 'horse_pace_preference', 'horse_avg_pace_lf', 'pace_fit']
    print("\n生成された特徴量:")
    for col in pace_cols:
        if col in result.columns:
            print(f"  {col}: mean={result[col].mean():.3f}, std={result[col].std():.3f}, non-null={result[col].notna().sum()}")
