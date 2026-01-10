#!/usr/bin/env python3
"""
深層根本原因分析スクリプト (deep_root_cause_analysis.py)

【目的】
2020-2025年の全芝レースを対象に、予測乖離の根本原因を特徴量レベルで分析

【分析項目】
1. 条件別分析（レースクラス、オッズ帯、馬履歴有無、距離など）
2. 特徴量レベル分析（各特徴量値と予測精度の相関）
3. 予測失敗パターンの特定と分類
4. 改善提案の導出

Author: KeibaAI Development Team
Date: 2026-01-10
"""

import sys
from pathlib import Path

project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(project_root))

import pandas as pd
import numpy as np
import lightgbm as lgb
import logging
import json
from datetime import datetime
from typing import List, Dict, Optional, Tuple
from collections import defaultdict
import warnings
warnings.filterwarnings('ignore')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class DeepRootCauseAnalyzer:
    """深層根本原因分析クラス"""
    
    def __init__(self, data_dir: str = 'keibaai/data/parsed/parquet', 
                 model_dir: str = 'keibaai/models/turf_dirt_separate'):
        self.data_dir = Path(data_dir)
        self.model_dir = Path(model_dir)
        self.output_dir = self.model_dir / 'deep_analysis'
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # モデルと特徴量読み込み
        self.model = lgb.Booster(model_file=str(self.model_dir / 'turf_model.txt'))
        with open(self.model_dir / 'features.json', 'r') as f:
            self.feature_cols = json.load(f)
        
        # データ読み込み
        self.races_df = self._load_races()
        
    def _load_races(self) -> pd.DataFrame:
        """レースデータ読み込み"""
        logger.info("レースデータ読み込み中...")
        df = pd.read_parquet(self.data_dir / 'races/races.parquet')
        df['race_date'] = pd.to_datetime(df['race_date'])
        df['year'] = df['race_date'].dt.year
        
        # 2014年以降、芝、障害・新馬除外
        df = df[df['year'] >= 2014]
        df = df[df['track_surface'] == '芝']
        df = df[df['race_class'] != '新馬']
        
        df['target'] = (df['finish_position'] == 1).astype(int)
        
        logger.info(f"レースデータ: {len(df):,}件")
        return df
    
    def _generate_features(self, race_df: pd.DataFrame, history_df: pd.DataFrame) -> Optional[pd.DataFrame]:
        """特徴量生成"""
        df = race_df.copy()
        
        if len(history_df) == 0:
            return None
        
        df['field_size'] = len(df)
        
        track_cond_map = {'良': 0, '稀重': 1, '重': 2, '不良': 3}
        df['track_condition_encoded'] = df['track_condition'].map(track_cond_map).fillna(0)
        
        df['race_number'] = df['race_id'].str[-2:].astype(int)
        df['is_late_race'] = (df['race_number'] >= 8).astype(int)
        
        history_df = history_df.copy()
        history_df['target'] = (history_df['finish_position'] == 1).astype(int)
        
        # 馬の累積勝率
        horse_stats = history_df.groupby('horse_id').agg({
            'target': ['sum', 'count'],
            'finish_position': 'mean'
        }).reset_index()
        horse_stats.columns = ['horse_id', 'horse_wins', 'horse_races', 'horse_avg_finish']
        horse_stats['horse_win_rate'] = np.where(
            horse_stats['horse_races'] >= 3,
            horse_stats['horse_wins'] / horse_stats['horse_races'],
            np.nan
        )
        
        # 騎手の累積勝率
        jockey_stats = history_df.groupby('jockey_id').agg({
            'target': ['sum', 'count']
        }).reset_index()
        jockey_stats.columns = ['jockey_id', 'jockey_wins', 'jockey_races']
        jockey_stats['jockey_win_rate'] = np.where(
            jockey_stats['jockey_races'] >= 10,
            jockey_stats['jockey_wins'] / jockey_stats['jockey_races'],
            np.nan
        )
        
        # 調教師の累積勝率
        trainer_stats = history_df.groupby('trainer_id').agg({
            'target': ['sum', 'count']
        }).reset_index()
        trainer_stats.columns = ['trainer_id', 'trainer_wins', 'trainer_races']
        trainer_stats['trainer_win_rate'] = np.where(
            trainer_stats['trainer_races'] >= 10,
            trainer_stats['trainer_wins'] / trainer_stats['trainer_races'],
            np.nan
        )
        
        # 馬の前走情報
        last_race = history_df.sort_values(['horse_id', 'race_date']).groupby('horse_id').last()[
            ['finish_position', 'race_date', 'horse_weight']
        ].reset_index()
        last_race.columns = ['horse_id', 'prev_finish', 'prev_race_date', 'prev_horse_weight']
        
        # マージ
        df = df.merge(horse_stats[['horse_id', 'horse_win_rate', 'horse_avg_finish', 'horse_races']], on='horse_id', how='left')
        df = df.merge(jockey_stats[['jockey_id', 'jockey_win_rate', 'jockey_races']], on='jockey_id', how='left')
        df = df.merge(trainer_stats[['trainer_id', 'trainer_win_rate', 'trainer_races']], on='trainer_id', how='left')
        df = df.merge(last_race, on='horse_id', how='left')
        
        df['days_since_last'] = (df['race_date'] - df['prev_race_date']).dt.days
        
        if 'horse_weight' in df.columns and 'prev_horse_weight' in df.columns:
            df['weight_change'] = df['horse_weight'] - df['prev_horse_weight']
        else:
            df['weight_change'] = np.nan
        
        df['distance_match'] = 0
        
        all_venues = history_df['venue'].unique()
        venue_encoder = {v: i for i, v in enumerate(all_venues)}
        df['venue_encoded'] = df['venue'].map(venue_encoder).fillna(-1)
        
        # 馬履歴フラグ
        df['has_horse_history'] = (~df['horse_win_rate'].isna()).astype(int)
        
        return df
    
    def analyze_all_races(self, start_year: int = 2020, end_year: int = 2025):
        """
        全レースの深層分析
        """
        logger.info(f"全レース分析開始: {start_year}-{end_year}年")
        
        # 対象レース
        race_ids = self.races_df[
            (self.races_df['year'] >= start_year) & 
            (self.races_df['year'] <= end_year)
        ]['race_id'].unique()
        
        logger.info(f"対象レース数: {len(race_ids):,}")
        
        all_predictions = []
        
        for i, race_id in enumerate(race_ids):
            if i % 500 == 0:
                logger.info(f"  処理中: {i:,}/{len(race_ids):,}")
            
            race_df = self.races_df[self.races_df['race_id'] == race_id].copy()
            if len(race_df) == 0:
                continue
            
            race_date = race_df['race_date'].iloc[0]
            year = race_df['year'].iloc[0]
            race_class = race_df['race_class'].iloc[0] if 'race_class' in race_df.columns else None
            distance = race_df['distance'].iloc[0] if 'distance' in race_df.columns else None
            track_condition = race_df['track_condition'].iloc[0] if 'track_condition' in race_df.columns else None
            
            # リークフリー履歴
            history_df = self.races_df[self.races_df['race_date'] < race_date].copy()
            
            # 特徴量生成
            race_feat = self._generate_features(race_df, history_df)
            if race_feat is None:
                continue
            
            # 欠損特徴量補完
            for col in self.feature_cols:
                if col not in race_feat.columns:
                    race_feat[col] = np.nan
            
            # 予測
            X = race_feat[self.feature_cols]
            preds = self.model.predict(X)
            race_feat['pred_prob'] = preds
            race_feat['pred_rank'] = race_feat['pred_prob'].rank(ascending=False, method='first')
            race_feat['odds_rank'] = race_feat['win_odds'].rank(ascending=True, method='first')
            
            # 各馬の情報を記録
            for _, row in race_feat.iterrows():
                record = {
                    'race_id': race_id,
                    'year': year,
                    'race_class': race_class,
                    'distance': distance,
                    'track_condition': track_condition,
                    'field_size': len(race_feat),
                    'horse_number': row['horse_number'],
                    'finish_position': row['finish_position'],
                    'win_odds': row['win_odds'],
                    'odds_rank': row['odds_rank'] if pd.notna(row['odds_rank']) else None,
                    'pred_prob': row['pred_prob'],
                    'pred_rank': row['pred_rank'],
                    'is_winner': row['finish_position'] == 1,
                    # 特徴量値を記録
                    'horse_win_rate': row.get('horse_win_rate'),
                    'horse_avg_finish': row.get('horse_avg_finish'),
                    'horse_races': row.get('horse_races'),
                    'jockey_win_rate': row.get('jockey_win_rate'),
                    'jockey_races': row.get('jockey_races'),
                    'trainer_win_rate': row.get('trainer_win_rate'),
                    'trainer_races': row.get('trainer_races'),
                    'prev_finish': row.get('prev_finish'),
                    'days_since_last': row.get('days_since_last'),
                    'has_horse_history': row.get('has_horse_history', 0),
                }
                all_predictions.append(record)
        
        df = pd.DataFrame(all_predictions)
        
        # 分析カラム追加
        df['pred_vs_finish_gap'] = df['pred_rank'] - df['finish_position']
        df['is_undervalued'] = (df['pred_rank'] >= 6) & (df['finish_position'] <= 3)
        df['is_overvalued'] = (df['pred_rank'] <= 3) & (df['finish_position'] >= 6)
        
        # オッズ帯
        def categorize_odds(o):
            if pd.isna(o): return 'unknown'
            elif o <= 3: return '1-3'
            elif o <= 10: return '3-10'
            elif o <= 50: return '10-50'
            elif o <= 150: return '50-150'
            else: return '150+'
        df['odds_tier'] = df['win_odds'].apply(categorize_odds)
        
        # 保存
        df.to_parquet(self.output_dir / 'all_predictions.parquet')
        logger.info(f"予測データ保存完了: {len(df):,}件")
        
        return df
    
    def analyze_root_causes(self, df: pd.DataFrame):
        """
        根本原因分析
        """
        logger.info("根本原因分析開始...")
        
        results = {}
        
        # ==================== 1. 条件別分析 ====================
        logger.info("1. 条件別分析")
        
        # 1.1 レースクラス別
        class_analysis = df.groupby('race_class').agg({
            'is_undervalued': ['sum', 'mean'],
            'is_overvalued': ['sum', 'mean'],
            'race_id': 'nunique',
            'horse_number': 'count'
        }).round(4)
        class_analysis.columns = ['undervalued_count', 'undervalued_rate', 
                                  'overvalued_count', 'overvalued_rate',
                                  'n_races', 'n_horses']
        results['by_race_class'] = class_analysis
        
        # 1.2 オッズ帯別
        odds_analysis = df.groupby('odds_tier').agg({
            'is_undervalued': ['sum', 'mean'],
            'is_overvalued': ['sum', 'mean'],
            'is_winner': 'mean',
            'horse_number': 'count'
        }).round(4)
        odds_analysis.columns = ['undervalued_count', 'undervalued_rate',
                                 'overvalued_count', 'overvalued_rate',
                                 'actual_win_rate', 'n_horses']
        results['by_odds_tier'] = odds_analysis
        
        # 1.3 馬履歴有無別
        history_analysis = df.groupby('has_horse_history').agg({
            'is_undervalued': ['sum', 'mean'],
            'is_overvalued': ['sum', 'mean'],
            'is_winner': 'mean',
            'horse_number': 'count'
        }).round(4)
        history_analysis.columns = ['undervalued_count', 'undervalued_rate',
                                    'overvalued_count', 'overvalued_rate',
                                    'actual_win_rate', 'n_horses']
        results['by_horse_history'] = history_analysis
        
        # 1.4 馬場状態別
        track_analysis = df.groupby('track_condition').agg({
            'is_undervalued': ['sum', 'mean'],
            'is_overvalued': ['sum', 'mean'],
            'is_winner': 'mean',
            'horse_number': 'count'
        }).round(4)
        track_analysis.columns = ['undervalued_count', 'undervalued_rate',
                                  'overvalued_count', 'overvalued_rate',
                                  'actual_win_rate', 'n_horses']
        results['by_track_condition'] = track_analysis
        
        # ==================== 2. 特徴量レベル分析 ====================
        logger.info("2. 特徴量レベル分析")
        
        feature_cols_to_analyze = ['horse_win_rate', 'horse_avg_finish', 'jockey_win_rate', 
                                   'trainer_win_rate', 'prev_finish', 'days_since_last']
        
        # 2.1 予測失敗時の特徴量分布
        undervalued_df = df[df['is_undervalued']]
        overvalued_df = df[df['is_overvalued']]
        normal_df = df[~df['is_undervalued'] & ~df['is_overvalued']]
        
        feature_stats = {}
        for col in feature_cols_to_analyze:
            if col in df.columns:
                feature_stats[col] = {
                    'undervalued_mean': undervalued_df[col].mean(),
                    'undervalued_median': undervalued_df[col].median(),
                    'undervalued_missing_rate': undervalued_df[col].isna().mean(),
                    'overvalued_mean': overvalued_df[col].mean(),
                    'overvalued_median': overvalued_df[col].median(),
                    'overvalued_missing_rate': overvalued_df[col].isna().mean(),
                    'normal_mean': normal_df[col].mean(),
                    'normal_median': normal_df[col].median(),
                    'normal_missing_rate': normal_df[col].isna().mean(),
                }
        results['feature_stats'] = pd.DataFrame(feature_stats).T.round(4)
        
        # ==================== 3. 詳細パターン分析 ====================
        logger.info("3. 詳細パターン分析")
        
        # 3.1 低オッズ(3-10倍)で過小評価されたケース
        low_odds_undervalued = df[(df['odds_tier'] == '3-10') & df['is_undervalued']]
        
        # このケースの特徴量分析
        if len(low_odds_undervalued) > 0:
            low_odds_uv_features = {}
            for col in feature_cols_to_analyze:
                if col in low_odds_undervalued.columns:
                    low_odds_uv_features[col] = {
                        'mean': low_odds_undervalued[col].mean(),
                        'median': low_odds_undervalued[col].median(),
                        'missing_rate': low_odds_undervalued[col].isna().mean(),
                    }
            results['low_odds_undervalued_features'] = pd.DataFrame(low_odds_uv_features).T.round(4)
        
        # 3.2 騎手勝率帯別の分析
        df['jockey_win_rate_tier'] = pd.cut(
            df['jockey_win_rate'], 
            bins=[0, 0.05, 0.10, 0.15, 0.20, 1.0],
            labels=['0-5%', '5-10%', '10-15%', '15-20%', '20%+']
        )
        
        jockey_tier_analysis = df.groupby('jockey_win_rate_tier', observed=True).agg({
            'is_undervalued': 'mean',
            'is_overvalued': 'mean',
            'is_winner': 'mean',
            'horse_number': 'count'
        }).round(4)
        jockey_tier_analysis.columns = ['undervalued_rate', 'overvalued_rate', 
                                        'actual_win_rate', 'n_horses']
        results['by_jockey_win_rate_tier'] = jockey_tier_analysis
        
        # ==================== 4. クロス分析 ====================
        logger.info("4. クロス分析")
        
        # 4.1 レースクラス × 馬履歴有無
        cross_analysis = df.groupby(['race_class', 'has_horse_history']).agg({
            'is_undervalued': 'mean',
            'is_overvalued': 'mean',
            'horse_number': 'count'
        }).round(4)
        cross_analysis.columns = ['undervalued_rate', 'overvalued_rate', 'n_horses']
        results['cross_class_history'] = cross_analysis
        
        # 4.2 オッズ帯 × 馬履歴有無
        cross_odds_history = df.groupby(['odds_tier', 'has_horse_history']).agg({
            'is_undervalued': 'mean',
            'is_overvalued': 'mean',
            'is_winner': 'mean',
            'horse_number': 'count'
        }).round(4)
        cross_odds_history.columns = ['undervalued_rate', 'overvalued_rate', 
                                       'actual_win_rate', 'n_horses']
        results['cross_odds_history'] = cross_odds_history
        
        return results
    
    def generate_report(self, df: pd.DataFrame, results: Dict):
        """
        分析レポート生成
        """
        logger.info("レポート生成中...")
        
        report_lines = []
        report_lines.append("=" * 80)
        report_lines.append("深層根本原因分析レポート")
        report_lines.append(f"分析期間: 2020-2025年")
        report_lines.append(f"分析レース数: {df['race_id'].nunique():,}")
        report_lines.append(f"分析馬数: {len(df):,}")
        report_lines.append("=" * 80)
        
        # サマリー
        total = len(df)
        undervalued = df['is_undervalued'].sum()
        overvalued = df['is_overvalued'].sum()
        
        report_lines.append(f"\n【サマリー】")
        report_lines.append(f"過小評価（低オッズ馬を見逃し）: {undervalued:,}件 ({undervalued/total*100:.2f}%)")
        report_lines.append(f"過大評価（高オッズ馬を過信）: {overvalued:,}件 ({overvalued/total*100:.2f}%)")
        
        # レースクラス別
        report_lines.append(f"\n{'='*80}")
        report_lines.append("【レースクラス別分析】")
        report_lines.append("過小評価率 = 低オッズ(3-10倍)馬を予測6位以下にしたが実際3着以内だった率")
        report_lines.append("-" * 80)
        class_df = results['by_race_class']
        for idx, row in class_df.iterrows():
            report_lines.append(f"  {idx:12} | 過小評価: {row['undervalued_rate']*100:.2f}% | 過大評価: {row['overvalued_rate']*100:.2f}% | レース数: {int(row['n_races']):,}")
        
        # オッズ帯別
        report_lines.append(f"\n{'='*80}")
        report_lines.append("【オッズ帯別分析】")
        report_lines.append("-" * 80)
        odds_df = results['by_odds_tier']
        for idx, row in odds_df.iterrows():
            report_lines.append(f"  {idx:8} | 過小評価: {row['undervalued_rate']*100:.2f}% | 過大評価: {row['overvalued_rate']*100:.2f}% | 実勝率: {row['actual_win_rate']*100:.2f}%")
        
        # 馬履歴有無別
        report_lines.append(f"\n{'='*80}")
        report_lines.append("【馬履歴有無別分析】（0=履歴なし, 1=履歴あり）")
        report_lines.append("※これが問題の核心")
        report_lines.append("-" * 80)
        history_df = results['by_horse_history']
        for idx, row in history_df.iterrows():
            label = "履歴なし" if idx == 0 else "履歴あり"
            report_lines.append(f"  {label:12} | 過小評価: {row['undervalued_rate']*100:.2f}% | 過大評価: {row['overvalued_rate']*100:.2f}% | 実勝率: {row['actual_win_rate']*100:.2f}%")
        
        # 特徴量統計
        report_lines.append(f"\n{'='*80}")
        report_lines.append("【特徴量レベル分析】")
        report_lines.append("過小評価ケースと通常ケースで特徴量値がどう違うか")
        report_lines.append("-" * 80)
        feat_df = results['feature_stats']
        for idx, row in feat_df.iterrows():
            underval_miss = row['undervalued_missing_rate'] * 100
            normal_miss = row['normal_missing_rate'] * 100
            report_lines.append(f"  {idx:20}")
            report_lines.append(f"    過小評価時: 平均={row['undervalued_mean']:.3f}, 欠損率={underval_miss:.1f}%")
            report_lines.append(f"    通常時:     平均={row['normal_mean']:.3f}, 欠損率={normal_miss:.1f}%")
        
        # 騎手勝率帯別
        report_lines.append(f"\n{'='*80}")
        report_lines.append("【騎手勝率帯別分析】")
        report_lines.append("-" * 80)
        jockey_df = results['by_jockey_win_rate_tier']
        for idx, row in jockey_df.iterrows():
            report_lines.append(f"  騎手勝率{idx:8} | 過小評価: {row['undervalued_rate']*100:.2f}% | 過大評価: {row['overvalued_rate']*100:.2f}% | 馬数: {int(row['n_horses']):,}")
        
        # クロス分析
        report_lines.append(f"\n{'='*80}")
        report_lines.append("【レースクラス×馬履歴有無 クロス分析】")
        report_lines.append("-" * 80)
        cross_df = results['cross_class_history']
        for (cls, hist), row in cross_df.iterrows():
            label = "履歴なし" if hist == 0 else "履歴あり"
            if cls is not None:
                report_lines.append(f"  {cls:12} / {label:8} | 過小評価: {row['undervalued_rate']*100:.2f}% | 馬数: {int(row['n_horses']):,}")
        
        # 低オッズ過小評価の詳細
        if 'low_odds_undervalued_features' in results:
            report_lines.append(f"\n{'='*80}")
            report_lines.append("【低オッズ(3-10倍)過小評価ケースの特徴量詳細】")
            report_lines.append("この馬たちはなぜ予測6位以下にされたのか？")
            report_lines.append("-" * 80)
            low_df = results['low_odds_undervalued_features']
            for idx, row in low_df.iterrows():
                report_lines.append(f"  {idx:20}: 平均={row['mean']:.3f}, 中央値={row['median']:.3f}, 欠損率={row['missing_rate']*100:.1f}%")
        
        # 結論
        report_lines.append(f"\n{'='*80}")
        report_lines.append("【結論と改善提案】")
        report_lines.append("=" * 80)
        
        # 馬履歴なしの過小評価率を計算
        hist0 = results['by_horse_history'].loc[0] if 0 in results['by_horse_history'].index else None
        hist1 = results['by_horse_history'].loc[1] if 1 in results['by_horse_history'].index else None
        
        if hist0 is not None and hist1 is not None:
            report_lines.append(f"\n1. 馬履歴なしの馬の過小評価率: {hist0['undervalued_rate']*100:.2f}%")
            report_lines.append(f"   馬履歴ありの馬の過小評価率: {hist1['undervalued_rate']*100:.2f}%")
            diff = (hist0['undervalued_rate'] - hist1['undervalued_rate']) * 100
            report_lines.append(f"   → 履歴なしは{diff:.1f}ポイント高い過小評価率")
        
        report_lines.append(f"\n2. 改善提案:")
        report_lines.append(f"   a. 馬履歴がない場合の代替特徴量（血統、同厩舎成績等）の追加")
        report_lines.append(f"   b. 騎手勝率への依存度を下げる（閾値50レース以上）")
        report_lines.append(f"   c. レースクラス別のモデル分岐（未勝利戦専用等）")
        report_lines.append(f"   d. オッズ情報のモデル外での活用（期待値フィルタ）")
        
        report_text = "\n".join(report_lines)
        
        with open(self.output_dir / 'deep_analysis_report.txt', 'w', encoding='utf-8') as f:
            f.write(report_text)
        
        print(report_text)
        
        # CSV保存
        for name, data in results.items():
            if isinstance(data, pd.DataFrame):
                data.to_csv(self.output_dir / f'{name}.csv')
        
        logger.info(f"レポート保存完了: {self.output_dir}")
        
        return report_text
    
    def run(self, start_year: int = 2020, end_year: int = 2025):
        """メイン実行"""
        # 全レース分析
        df = self.analyze_all_races(start_year, end_year)
        
        # 根本原因分析
        results = self.analyze_root_causes(df)
        
        # レポート生成
        report = self.generate_report(df, results)
        
        return df, results, report


if __name__ == "__main__":
    analyzer = DeepRootCauseAnalyzer()
    df, results, report = analyzer.run()
