#!/usr/bin/env python3
"""
モデル予測バイアス診断スクリプト (model_bias_diagnosis.py)

【目的】
人気馬の過小評価・不人気馬の過大評価の原因を特定

【分析項目】
1. 特徴量値の詳細調査（なぜこの馬がこの勝率になったか）
2. 人気別の予測精度（人気1-3位の馬の予測順位分布）
3. オッズと予測勝率の相関分析
4. 複数レースでのパターン確認

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
from typing import List, Dict, Tuple, Optional

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class ModelBiasDiagnostic:
    """モデルバイアス診断クラス"""
    
    def __init__(self, data_dir: str = 'keibaai/data/parsed/parquet', 
                 model_dir: str = 'keibaai/models/turf_dirt_separate'):
        self.data_dir = Path(data_dir)
        self.model_dir = Path(model_dir)
        self.output_dir = self.model_dir / 'diagnosis'
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
    
    def diagnose_single_race(self, race_id: str):
        """単一レースの詳細診断"""
        race_df = self.races_df[self.races_df['race_id'] == race_id].copy()
        
        if len(race_df) == 0:
            logger.error(f"レースが見つかりません: {race_id}")
            return
        
        race_date = race_df['race_date'].iloc[0]
        history_df = self.races_df[self.races_df['race_date'] < race_date].copy()
        
        print(f"\n{'='*100}")
        print(f"【レース診断】{race_id} ({race_date.date()})")
        print(f"{'='*100}")
        
        # 各馬の特徴量と予測を詳細に表示
        for _, row in race_df.sort_values('popularity').iterrows():
            horse_id = row['horse_id']
            horse_name = row.get('horse_name', 'Unknown')
            horse_number = int(row['horse_number'])
            popularity = int(row['popularity']) if pd.notna(row['popularity']) else 'N/A'
            win_odds = row['win_odds']
            finish = int(row['finish_position']) if pd.notna(row['finish_position']) else 'N/A'
            
            # この馬の履歴を取得
            horse_history = history_df[history_df['horse_id'] == horse_id].copy()
            
            print(f"\n--- {horse_number}番 {horse_name} (人気{popularity}位, オッズ{win_odds:.1f}, 実着順{finish}位) ---")
            
            # 馬の過去成績
            if len(horse_history) > 0:
                wins = len(horse_history[horse_history['finish_position'] == 1])
                races = len(horse_history)
                avg_finish = horse_history['finish_position'].mean()
                print(f"  過去成績: {wins}勝/{races}走 (勝率{wins/races:.1%}, 平均着順{avg_finish:.1f})")
                
                # 最近3走
                recent = horse_history.sort_values('race_date', ascending=False).head(3)
                print(f"  最近3走: {recent['finish_position'].tolist()}")
            else:
                print(f"  過去成績: なし（履歴データに該当馬なし）")
            
            # 騎手の過去成績
            jockey_id = row.get('jockey_id')
            if jockey_id and len(history_df) > 0:
                jockey_history = history_df[history_df['jockey_id'] == jockey_id]
                if len(jockey_history) > 0:
                    j_wins = len(jockey_history[jockey_history['finish_position'] == 1])
                    j_races = len(jockey_history)
                    print(f"  騎手成績: {j_wins}勝/{j_races}走 (勝率{j_wins/j_races:.1%})")
            
            # 調教師の過去成績
            trainer_id = row.get('trainer_id')
            if trainer_id and len(history_df) > 0:
                trainer_history = history_df[history_df['trainer_id'] == trainer_id]
                if len(trainer_history) > 0:
                    t_wins = len(trainer_history[trainer_history['finish_position'] == 1])
                    t_races = len(trainer_history)
                    print(f"  調教師成績: {t_wins}勝/{t_races}走 (勝率{t_wins/t_races:.1%})")
    
    def analyze_popularity_bias(self, start_year: int = 2020, end_year: int = 2025, sample_size: int = 1000):
        """
        人気別の予測バイアス分析
        
        【問い】
        - 人気1-3位の馬は、予測でも上位に来ているか？
        - 人気下位の馬が予測上位に来る頻度は？
        """
        logger.info(f"人気バイアス分析: {start_year}-{end_year}年")
        
        df = self.races_df[(self.races_df['year'] >= start_year) & (self.races_df['year'] <= end_year)].copy()
        
        # サンプルレースを選択
        race_ids = df['race_id'].unique()
        if len(race_ids) > sample_size:
            np.random.seed(42)
            sample_ids = np.random.choice(race_ids, sample_size, replace=False)
        else:
            sample_ids = race_ids
        
        results = []
        
        for i, race_id in enumerate(sample_ids):
            if i % 100 == 0:
                logger.info(f"  処理中: {i}/{len(sample_ids)}")
            
            race_df = df[df['race_id'] == race_id].copy()
            race_date = race_df['race_date'].iloc[0]
            
            # リークフリーな履歴
            history_df = df[df['race_date'] < race_date].copy()
            
            # 特徴量生成（簡易版）
            race_feat = self._generate_simple_features(race_df, history_df)
            
            if race_feat is None:
                continue
            
            # 予測
            for col in self.feature_cols:
                if col not in race_feat.columns:
                    race_feat[col] = np.nan
            
            X = race_feat[self.feature_cols]
            preds = self.model.predict(X)
            race_feat['pred_prob'] = preds
            race_feat['pred_rank'] = race_feat['pred_prob'].rank(ascending=False, method='first')
            
            # 各馬の結果を記録
            for _, row in race_feat.iterrows():
                if pd.isna(row['popularity']):
                    continue
                
                results.append({
                    'race_id': race_id,
                    'horse_number': row['horse_number'],
                    'popularity': int(row['popularity']),
                    'win_odds': row['win_odds'],
                    'pred_prob': row['pred_prob'],
                    'pred_rank': int(row['pred_rank']),
                    'finish_position': row['finish_position'],
                    'is_winner': row['finish_position'] == 1,
                })
        
        results_df = pd.DataFrame(results)
        
        # 分析結果
        self._print_popularity_bias_report(results_df)
        
        # 保存
        results_df.to_csv(self.output_dir / 'popularity_bias_analysis.csv', index=False)
        
        return results_df
    
    def _generate_simple_features(self, race_df: pd.DataFrame, history_df: pd.DataFrame) -> Optional[pd.DataFrame]:
        """簡易特徴量生成"""
        df = race_df.copy()
        
        df['field_size'] = len(df)
        
        track_cond_map = {'良': 0, '稍重': 1, '重': 2, '不良': 3}
        df['track_condition_encoded'] = df['track_condition'].map(track_cond_map).fillna(0)
        
        df['race_number'] = df['race_id'].str[-2:].astype(int)
        df['is_late_race'] = (df['race_number'] >= 8).astype(int)
        
        if len(history_df) == 0:
            return None
        
        history_df['target'] = (history_df['finish_position'] == 1).astype(int)
        
        # 馬の累積勝率
        horse_stats = history_df.groupby('horse_id').agg({
            'target': ['sum', 'count'],
            'finish_position': 'mean'
        }).reset_index()
        horse_stats.columns = ['horse_id', 'horse_cum_wins', 'horse_cum_races', 'horse_avg_finish']
        horse_stats['horse_win_rate'] = np.where(
            horse_stats['horse_cum_races'] >= 3,
            horse_stats['horse_cum_wins'] / horse_stats['horse_cum_races'],
            np.nan
        )
        
        # 騎手の累積勝率
        jockey_stats = history_df.groupby('jockey_id').agg({
            'target': ['sum', 'count']
        }).reset_index()
        jockey_stats.columns = ['jockey_id', 'jockey_cum_wins', 'jockey_cum_races']
        jockey_stats['jockey_win_rate'] = np.where(
            jockey_stats['jockey_cum_races'] >= 10,
            jockey_stats['jockey_cum_wins'] / jockey_stats['jockey_cum_races'],
            np.nan
        )
        
        # 調教師の累積勝率
        trainer_stats = history_df.groupby('trainer_id').agg({
            'target': ['sum', 'count']
        }).reset_index()
        trainer_stats.columns = ['trainer_id', 'trainer_cum_wins', 'trainer_cum_races']
        trainer_stats['trainer_win_rate'] = np.where(
            trainer_stats['trainer_cum_races'] >= 10,
            trainer_stats['trainer_cum_wins'] / trainer_stats['trainer_cum_races'],
            np.nan
        )
        
        # 馬の前走情報
        train_last_race = history_df.sort_values(['horse_id', 'race_date']).groupby('horse_id').last()[
            ['finish_position', 'race_date', 'horse_weight']
        ].reset_index()
        train_last_race.columns = ['horse_id', 'prev_finish', 'prev_race_date', 'prev_horse_weight']
        
        # マージ
        df = df.merge(horse_stats[['horse_id', 'horse_win_rate', 'horse_avg_finish']], on='horse_id', how='left')
        df = df.merge(jockey_stats[['jockey_id', 'jockey_win_rate']], on='jockey_id', how='left')
        df = df.merge(trainer_stats[['trainer_id', 'trainer_win_rate']], on='trainer_id', how='left')
        df = df.merge(train_last_race, on='horse_id', how='left')
        
        df['days_since_last'] = (df['race_date'] - df['prev_race_date']).dt.days
        
        if 'horse_weight' in df.columns and 'prev_horse_weight' in df.columns:
            df['weight_change'] = df['horse_weight'] - df['prev_horse_weight']
        else:
            df['weight_change'] = np.nan
        
        df['distance_match'] = 0
        
        all_venues = history_df['venue'].unique()
        venue_encoder = {v: i for i, v in enumerate(all_venues)}
        df['venue_encoded'] = df['venue'].map(venue_encoder).fillna(-1)
        
        return df
    
    def _print_popularity_bias_report(self, df: pd.DataFrame):
        """人気バイアスレポート出力"""
        print(f"\n{'='*100}")
        print(f"【人気バイアス分析レポート】")
        print(f"分析レース数: {df['race_id'].nunique():,}")
        print(f"{'='*100}")
        
        # 人気別の統計
        print(f"\n【人気別の予測順位分布】")
        print(f"{'人気':>6} {'件数':>8} {'予測1位':>8} {'予測Top3':>8} {'予測Top5':>8} {'平均予測順':>10} {'実勝率':>8}")
        print("-" * 70)
        
        for pop in range(1, 11):
            pop_df = df[df['popularity'] == pop]
            if len(pop_df) == 0:
                continue
            
            total = len(pop_df)
            pred1 = len(pop_df[pop_df['pred_rank'] == 1])
            pred_top3 = len(pop_df[pop_df['pred_rank'] <= 3])
            pred_top5 = len(pop_df[pop_df['pred_rank'] <= 5])
            avg_pred_rank = pop_df['pred_rank'].mean()
            actual_win_rate = pop_df['is_winner'].mean()
            
            print(f"{pop:>6} {total:>8,} {pred1/total:>7.1%} {pred_top3/total:>7.1%} {pred_top5/total:>7.1%} {avg_pred_rank:>10.1f} {actual_win_rate:>7.1%}")
        
        # 問題の核心：人気1-3位なのに予測下位の馬
        print(f"\n【問題パターン分析】")
        
        # 人気1-3位なのに予測6位以下
        pop_top3_but_pred_low = df[(df['popularity'] <= 3) & (df['pred_rank'] >= 6)]
        if len(pop_top3_but_pred_low) > 0:
            win_rate = pop_top3_but_pred_low['is_winner'].mean()
            print(f"  人気1-3位 かつ 予測6位以下: {len(pop_top3_but_pred_low):,}件")
            print(f"    → この馬たちの実際の勝率: {win_rate:.1%}")
            print(f"    → 人気1-3位の平均勝率: {df[df['popularity'] <= 3]['is_winner'].mean():.1%}")
        
        # 人気6位以下なのに予測Top3
        pop_low_but_pred_top3 = df[(df['popularity'] >= 6) & (df['pred_rank'] <= 3)]
        if len(pop_low_but_pred_top3) > 0:
            win_rate = pop_low_but_pred_top3['is_winner'].mean()
            print(f"  人気6位以下 かつ 予測Top3: {len(pop_low_but_pred_top3):,}件")
            print(f"    → この馬たちの実際の勝率: {win_rate:.1%}")
            print(f"    → 人気6位以下の平均勝率: {df[df['popularity'] >= 6]['is_winner'].mean():.1%}")
        
        # 予測と人気の相関
        corr = df['popularity'].corr(df['pred_rank'])
        print(f"\n  人気順位と予測順位の相関係数: {corr:.3f}")
        print(f"    (1.0に近いほど市場と一致、0に近いほど独自性が高い)")
        
        # 予測1位の馬のオッズ分布
        pred1_df = df[df['pred_rank'] == 1]
        print(f"\n【予測1位の馬の分析】")
        print(f"  件数: {len(pred1_df):,}")
        print(f"  平均オッズ: {pred1_df['win_odds'].mean():.1f}倍")
        print(f"  中央オッズ: {pred1_df['win_odds'].median():.1f}倍")
        print(f"  平均人気: {pred1_df['popularity'].mean():.1f}位")
        print(f"  実際の勝率: {pred1_df['is_winner'].mean():.1%}")
        
        # オッズ帯別の予測1位勝率
        print(f"\n【予測1位のオッズ帯別勝率】")
        for cat, odds_range in [('1-3倍', (0, 3)), ('3-10倍', (3, 10)), ('10-50倍', (10, 50)), ('50倍超', (50, 9999))]:
            cat_df = pred1_df[(pred1_df['win_odds'] >= odds_range[0]) & (pred1_df['win_odds'] < odds_range[1])]
            if len(cat_df) > 0:
                win_rate = cat_df['is_winner'].mean()
                print(f"  {cat}: {len(cat_df):,}件, 勝率{win_rate:.1%}")


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='モデルバイアス診断')
    parser.add_argument('--race-id', type=str, help='単一レースの診断')
    parser.add_argument('--full', action='store_true', help='全体バイアス分析')
    parser.add_argument('--sample', type=int, default=500, help='サンプルレース数')
    args = parser.parse_args()
    
    diagnostic = ModelBiasDiagnostic()
    
    if args.race_id:
        diagnostic.diagnose_single_race(args.race_id)
    elif args.full:
        diagnostic.analyze_popularity_bias(sample_size=args.sample)
    else:
        # デモ
        print("=== レース202506050803の詳細診断 ===")
        diagnostic.diagnose_single_race('202506050803')
        print("\n\n=== 全体人気バイアス分析（500レースサンプル）===")
        diagnostic.analyze_popularity_bias(sample_size=500)
