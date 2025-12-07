#!/usr/bin/env python3
"""
リークフリー版gap特徴量の計算とROI検証

【問題点】
既存の jockey_win_rate, sire_win_rate, trainer_win_rate 等は
全期間集計されており、テスト期間の情報がリークしている。

【解決策】
expanding().mean().shift(1) を使用して、
「各レース時点での過去情報のみ」で計算する。
"""

import pandas as pd
import numpy as np
import logging
from pathlib import Path

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class LeakFreeGapCalculator:
    """リークフリー版gap特徴量計算クラス"""
    
    def __init__(self):
        self.races_df = None
        self.pedigree_df = None
    
    def load_data(self):
        """データ読み込み"""
        logging.info("データ読み込み中...")
        
        self.races_df = pd.read_parquet('keibaai/data/parsed/parquet/races/races.parquet')
        self.races_df['race_date'] = pd.to_datetime(self.races_df['race_date'])
        
        pedigree_path = Path('keibaai/data/parsed/parquet/pedigrees/pedigrees.parquet')
        if pedigree_path.exists():
            self.pedigree_df = pd.read_parquet(pedigree_path)
        
        logging.info(f"  レース結果: {len(self.races_df):,}件")
        
        # 型変換
        self.races_df['finish_position'] = pd.to_numeric(self.races_df['finish_position'], errors='coerce')
        self.races_df['popularity'] = pd.to_numeric(self.races_df['popularity'], errors='coerce')
        self.races_df['win_odds'] = pd.to_numeric(self.races_df['win_odds'], errors='coerce')
        self.races_df['horse_id'] = self.races_df['horse_id'].astype(str)
        self.races_df['jockey_id'] = self.races_df['jockey_id'].astype(str)
        self.races_df['trainer_id'] = self.races_df['trainer_id'].astype(str)
        
        # 勝利フラグ（NA対応）
        self.races_df['is_win'] = (self.races_df['finish_position'] == 1).fillna(False).astype(int)
        self.races_df['is_place'] = (self.races_df['finish_position'] <= 3).fillna(False).astype(int)
    
    def calculate_leak_free_stats(self):
        """リークフリー統計を計算"""
        logging.info("リークフリー統計を計算中...")
        
        df = self.races_df.sort_values(['race_date', 'race_id']).reset_index(drop=True)
        
        # 騎手の累積勝率（shift(1)で当該レース前まで）
        logging.info("  騎手累積勝率...")
        jockey_stats = df.groupby('jockey_id').apply(
            lambda x: x.assign(
                jockey_win_rate_lf=x['is_win'].expanding().mean().shift(1),
                jockey_race_count=x['is_win'].expanding().count().shift(1)
            )
        ).reset_index(drop=True)
        
        # 調教師の累積勝率
        logging.info("  調教師累積勝率...")
        trainer_stats = df.groupby('trainer_id').apply(
            lambda x: x.assign(
                trainer_win_rate_lf=x['is_win'].expanding().mean().shift(1),
                trainer_race_count=x['is_win'].expanding().count().shift(1)
            )
        ).reset_index(drop=True)
        
        # マージ
        df = df.merge(
            jockey_stats[['race_id', 'horse_id', 'jockey_win_rate_lf', 'jockey_race_count']],
            on=['race_id', 'horse_id'], how='left'
        )
        df = df.merge(
            trainer_stats[['race_id', 'horse_id', 'trainer_win_rate_lf', 'trainer_race_count']],
            on=['race_id', 'horse_id'], how='left'
        )
        
        # 父の累積勝率（全産駒）
        if self.pedigree_df is not None:
            logging.info("  父累積勝率...")
            sire_map = self.pedigree_df[self.pedigree_df['generation'] == 1][['horse_id', 'ancestor_id']].copy()
            sire_map.columns = ['horse_id', 'sire_id']
            sire_map['horse_id'] = sire_map['horse_id'].astype(str)
            sire_map = sire_map.drop_duplicates('horse_id')
            
            df = df.merge(sire_map, on='horse_id', how='left')
            
            sire_stats = df.groupby('sire_id').apply(
                lambda x: x.assign(
                    sire_win_rate_lf=x['is_win'].expanding().mean().shift(1),
                    sire_race_count=x['is_win'].expanding().count().shift(1)
                )
            ).reset_index(drop=True)
            
            df = df.merge(
                sire_stats[['race_id', 'horse_id', 'sire_win_rate_lf', 'sire_race_count']].drop_duplicates(),
                on=['race_id', 'horse_id'], how='left'
            )
        
        self.races_df = df
        logging.info("  統計計算完了")
    
    def calculate_gap_features(self):
        """gap特徴量を計算"""
        logging.info("gap特徴量を計算中...")
        
        df = self.races_df.copy()
        
        # 各レース内で順位を計算
        # 騎手勝率ランク
        df['jockey_rank_lf'] = df.groupby('race_id')['jockey_win_rate_lf'].rank(ascending=False, method='average')
        df['gap_jockey_pop_lf'] = df['popularity'] - df['jockey_rank_lf']
        
        # 調教師勝率ランク
        df['trainer_rank_lf'] = df.groupby('race_id')['trainer_win_rate_lf'].rank(ascending=False, method='average')
        df['gap_trainer_pop_lf'] = df['popularity'] - df['trainer_rank_lf']
        
        # 父勝率ランク
        if 'sire_win_rate_lf' in df.columns:
            df['sire_rank_lf'] = df.groupby('race_id')['sire_win_rate_lf'].rank(ascending=False, method='average')
            df['gap_sire_pop_lf'] = df['popularity'] - df['sire_rank_lf']
        else:
            df['gap_sire_pop_lf'] = 0
        
        self.races_df = df
        logging.info("  gap特徴量計算完了")
    
    def calculate_roi(self, df, score_col, threshold_quantile=None, train_threshold=None):
        """ROI計算"""
        df = df.copy()
        
        if threshold_quantile is not None and train_threshold is None:
            threshold = df[score_col].quantile(threshold_quantile)
            df = df[df[score_col] >= threshold]
        elif train_threshold is not None:
            df = df[df[score_col] >= train_threshold]
        
        if len(df) == 0:
            return 0, 0
        
        df['rank_pred'] = df.groupby('race_id')[score_col].rank(ascending=False, method='first')
        bet = df[df['rank_pred'] == 1]
        hits = bet[bet['finish_position'] == 1]
        roi = hits['win_odds'].sum() / len(bet) if len(bet) > 0 else 0
        return roi, len(bet)
    
    def run_validation(self):
        """リークフリー版での検証"""
        logging.info("=" * 60)
        logging.info("リークフリー版gap特徴量のROI検証")
        logging.info("=" * 60)
        
        df = self.races_df.copy()
        
        # gap特徴量の合成
        gap_cols = ['gap_jockey_pop_lf', 'gap_trainer_pop_lf', 'gap_sire_pop_lf']
        for col in gap_cols:
            if col in df.columns:
                df[f'{col}_norm'] = (df[col] - df[col].mean()) / (df[col].std() + 1e-6)
        
        norm_cols = [f'{col}_norm' for col in gap_cols if col in df.columns]
        df['gap_score_lf'] = df[norm_cols].mean(axis=1)
        
        # データ分割
        train_df = df[df['race_date'] < '2023-01-01'].copy()
        valid_df = df[(df['race_date'] >= '2023-01-01') & (df['race_date'] < '2024-01-01')].copy()
        test_df = df[df['race_date'] >= '2024-01-01'].copy()
        
        # Train統計で正規化
        train_mean = train_df['gap_score_lf'].mean()
        train_std = train_df['gap_score_lf'].std()
        
        for d in [train_df, valid_df, test_df]:
            d['gap_score_lf_norm'] = (d['gap_score_lf'] - train_mean) / (train_std + 1e-6)
        
        # Train閾値を計算
        train_threshold_90 = train_df['gap_score_lf_norm'].quantile(0.9)
        
        logging.info(f"Train閾値（上位10%）: {train_threshold_90:.4f}")
        logging.info("")
        
        # ROI検証
        logging.info("【リークフリー版ROI - 全体】")
        for name, d in [('Train', train_df), ('Valid', valid_df), ('Test', test_df)]:
            roi, n = self.calculate_roi(d, 'gap_score_lf_norm')
            logging.info(f"  {name}: {n:,}レース, ROI={roi:.2%}")
        
        logging.info("")
        logging.info("【リークフリー版ROI - 上位10%フィルター（Train閾値使用）】")
        for name, d in [('Train', train_df), ('Valid', valid_df), ('Test', test_df)]:
            roi, n = self.calculate_roi(d, 'gap_score_lf_norm', train_threshold=train_threshold_90)
            logging.info(f"  {name}: {n:,}レース, ROI={roi:.2%}")
        
        # 比較: 既存版との差
        logging.info("")
        logging.info("=" * 60)
        logging.info("【結論】")
        logging.info("=" * 60)
        
        # 既存版読み込み
        train_data_old = pd.read_parquet('keibaai/models/mu_v3_3/train_data_mu_v3_3.parquet')
        train_data_old['race_date'] = pd.to_datetime(train_data_old['race_date'])
        test_old = train_data_old[train_data_old['race_date'] >= '2024-01-01']
        
        logging.info("  既存版(リークあり): Test ROI ~103%")
        logging.info(f"  リークフリー版: Valid ROI, Test ROIを上記で確認")
    
    def run(self):
        """メイン実行"""
        self.load_data()
        self.calculate_leak_free_stats()
        self.calculate_gap_features()
        self.run_validation()


if __name__ == "__main__":
    calculator = LeakFreeGapCalculator()
    calculator.run()
