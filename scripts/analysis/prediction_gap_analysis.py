#!/usr/bin/env python3
"""
予測乖離パターン分析 (prediction_gap_analysis.py)

【目的】
- 予測勝率と実着順の乖離パターンを複数レースで特定
- オッズ分布（人気ではなく）で市場評価を定量的に分析
- どのような条件で予測が実際と大きく乖離するかを特定

【分析観点】
1. レースごとのオッズ分布（四分位、中央値）と予測勝率の関係
2. 「低オッズ（3-10倍）なのに予測下位」のケース調査
3. 「高オッズ（50倍超）なのに予測上位」のケース調査
4. 予測Top3 vs 実着Top3の一致率

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
from typing import List, Dict, Optional

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class PredictionGapAnalyzer:
    """予測乖離分析クラス"""
    
    def __init__(self, data_dir: str = 'keibaai/data/parsed/parquet', 
                 model_dir: str = 'keibaai/models/turf_dirt_separate'):
        self.data_dir = Path(data_dir)
        self.model_dir = Path(model_dir)
        self.output_dir = self.model_dir / 'gap_analysis'
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # モデルと特徴量読み込み
        self.model = lgb.Booster(model_file=str(self.model_dir / 'turf_model.txt'))
        with open(self.model_dir / 'features.json', 'r') as f:
            self.feature_cols = json.load(f)
        
        # データ読み込み
        self.races_df = self._load_races()
        self.returns_df = self._load_returns()
        
    def _load_races(self) -> pd.DataFrame:
        """レースデータ読み込み"""
        logger.info("レースデータ読み込み中...")
        df = pd.read_parquet(self.data_dir / 'races/races.parquet')
        df['race_date'] = pd.to_datetime(df['race_date'])
        df['year'] = df['race_date'].dt.year
        df = df[df['year'] >= 2014]
        df = df[df['track_surface'] == '芝']
        df = df[df['race_class'] != '新馬']
        df['target'] = (df['finish_position'] == 1).astype(int)
        logger.info(f"レースデータ: {len(df):,}件")
        return df
    
    def _load_returns(self) -> pd.DataFrame:
        """払戻データ読み込み"""
        returns_path = self.data_dir / 'returns/returns.parquet'
        if returns_path.exists():
            return pd.read_parquet(returns_path)
        return pd.DataFrame()
    
    def _generate_features_for_race(self, race_df: pd.DataFrame, history_df: pd.DataFrame) -> Optional[pd.DataFrame]:
        """特定レース用の特徴量生成"""
        df = race_df.copy()
        
        if len(history_df) == 0:
            return None
        
        df['field_size'] = len(df)
        track_cond_map = {'良': 0, '稀重': 1, '重': 2, '不良': 3}
        df['track_condition_encoded'] = df['track_condition'].map(track_cond_map).fillna(0)
        df['race_number'] = df['race_id'].str[-2:].astype(int)
        df['is_late_race'] = (df['race_number'] >= 8).astype(int)
        
        history_df['target'] = (history_df['finish_position'] == 1).astype(int)
        
        # 馬の累積勝率
        horse_stats = history_df.groupby('horse_id').agg({
            'target': ['sum', 'count'],
            'finish_position': 'mean'
        }).reset_index()
        horse_stats.columns = ['horse_id', 'wins', 'races', 'horse_avg_finish']
        horse_stats['horse_win_rate'] = np.where(horse_stats['races'] >= 3, horse_stats['wins'] / horse_stats['races'], np.nan)
        
        # 騎手の累積勝率
        jockey_stats = history_df.groupby('jockey_id').agg({'target': ['sum', 'count']}).reset_index()
        jockey_stats.columns = ['jockey_id', 'wins', 'races']
        jockey_stats['jockey_win_rate'] = np.where(jockey_stats['races'] >= 10, jockey_stats['wins'] / jockey_stats['races'], np.nan)
        
        # 調教師の累積勝率
        trainer_stats = history_df.groupby('trainer_id').agg({'target': ['sum', 'count']}).reset_index()
        trainer_stats.columns = ['trainer_id', 'wins', 'races']
        trainer_stats['trainer_win_rate'] = np.where(trainer_stats['races'] >= 10, trainer_stats['wins'] / trainer_stats['races'], np.nan)
        
        # 馬の前走情報
        last_race = history_df.sort_values(['horse_id', 'race_date']).groupby('horse_id').last()[
            ['finish_position', 'race_date', 'horse_weight']
        ].reset_index()
        last_race.columns = ['horse_id', 'prev_finish', 'prev_race_date', 'prev_horse_weight']
        
        # マージ
        df = df.merge(horse_stats[['horse_id', 'horse_win_rate', 'horse_avg_finish']], on='horse_id', how='left')
        df = df.merge(jockey_stats[['jockey_id', 'jockey_win_rate']], on='jockey_id', how='left')
        df = df.merge(trainer_stats[['trainer_id', 'trainer_win_rate']], on='trainer_id', how='left')
        df = df.merge(last_race, on='horse_id', how='left')
        
        df['days_since_last'] = (df['race_date'] - df['prev_race_date']).dt.days
        df['weight_change'] = df['horse_weight'] - df['prev_horse_weight'] if 'prev_horse_weight' in df.columns else np.nan
        df['distance_match'] = 0
        
        venue_encoder = {v: i for i, v in enumerate(history_df['venue'].unique())}
        df['venue_encoded'] = df['venue'].map(venue_encoder).fillna(-1)
        
        return df
    
    def analyze_race_detailed(self, race_id: str, verbose: bool = True) -> Dict:
        """
        単一レースの詳細分析
        
        【出力】
        - 各馬：オッズ、レース内オッズ順位、予測勝率、予測順位、実着順
        - オッズ分布統計（四分位、中央値）
        - 予測と実着順の乖離指標
        """
        race_df = self.races_df[self.races_df['race_id'] == race_id].copy()
        if len(race_df) == 0:
            return {}
        
        race_date = race_df['race_date'].iloc[0]
        history_df = self.races_df[self.races_df['race_date'] < race_date].copy()
        
        # 特徴量生成と予測
        race_feat = self._generate_features_for_race(race_df, history_df)
        if race_feat is None:
            return {}
        
        for col in self.feature_cols:
            if col not in race_feat.columns:
                race_feat[col] = np.nan
        
        X = race_feat[self.feature_cols]
        preds = self.model.predict(X)
        race_feat['pred_prob'] = preds
        race_feat['pred_rank'] = race_feat['pred_prob'].rank(ascending=False, method='first')
        
        # オッズ順位（市場評価）
        race_feat['odds_rank'] = race_feat['win_odds'].rank(ascending=True, method='first')
        
        # オッズ分布
        odds = race_feat['win_odds'].dropna()
        odds_stats = {
            'min': odds.min(),
            'q1': odds.quantile(0.25),
            'median': odds.median(),
            'q3': odds.quantile(0.75),
            'max': odds.max(),
        }
        
        # 予測乖離指標
        race_feat['pred_vs_finish_gap'] = race_feat['pred_rank'] - race_feat['finish_position']
        race_feat['odds_vs_finish_gap'] = race_feat['odds_rank'] - race_feat['finish_position']
        
        # 期待値
        race_feat['expected_value'] = race_feat['pred_prob'] * race_feat['win_odds']
        
        result = {
            'race_id': race_id,
            'race_date': str(race_date.date()),
            'race_name': race_feat['race_name'].iloc[0] if 'race_name' in race_feat.columns else '',
            'race_class': race_feat['race_class'].iloc[0] if 'race_class' in race_feat.columns else '',
            'field_size': len(race_feat),
            'odds_stats': odds_stats,
            'horses': []
        }
        
        # 各馬の詳細
        for _, row in race_feat.sort_values('finish_position').iterrows():
            horse_data = {
                'horse_number': int(row['horse_number']),
                'horse_name': row.get('horse_name', ''),
                'finish_position': int(row['finish_position']) if pd.notna(row['finish_position']) else None,
                'win_odds': round(row['win_odds'], 1) if pd.notna(row['win_odds']) else None,
                'odds_rank': int(row['odds_rank']) if pd.notna(row['odds_rank']) else None,
                'pred_prob': round(row['pred_prob'] * 100, 2),  # %表示
                'pred_rank': int(row['pred_rank']),
                'expected_value': round(row['expected_value'], 2) if pd.notna(row['expected_value']) else None,
                'pred_vs_finish_gap': int(row['pred_vs_finish_gap']) if pd.notna(row['pred_vs_finish_gap']) else None,
            }
            result['horses'].append(horse_data)
        
        # 払戻情報
        if len(self.returns_df) > 0:
            race_returns = self.returns_df[self.returns_df['race_id'] == race_id]
            if len(race_returns) > 0:
                tansho = race_returns[race_returns['bet_type'] == 'tansho']
                sanrentan = race_returns[race_returns['bet_type'] == 'sanrentan']
                if len(tansho) > 0:
                    result['tansho_payout'] = int(tansho['payout'].iloc[0])
                if len(sanrentan) > 0:
                    result['sanrentan_payout'] = int(sanrentan['payout'].iloc[0])
        
        if verbose:
            self._print_race_analysis(result)
        
        return result
    
    def _print_race_analysis(self, result: Dict):
        """レース分析結果を表示"""
        print(f"\n{'='*100}")
        print(f"【レース】{result['race_id']} ({result['race_date']}) {result['race_name']} [{result['race_class']}]")
        print(f"出走頭数: {result['field_size']}頭")
        print(f"{'='*100}")
        
        # オッズ分布
        stats = result['odds_stats']
        print(f"\n【オッズ分布】最小{stats['min']:.1f} | Q1:{stats['q1']:.1f} | 中央値:{stats['median']:.1f} | Q3:{stats['q3']:.1f} | 最大{stats['max']:.1f}")
        
        # 払戻
        if 'tansho_payout' in result:
            print(f"【単勝配当】{result['tansho_payout']}円")
        if 'sanrentan_payout' in result:
            print(f"【三連単配当】{result['sanrentan_payout']}円")
        
        # 馬データ
        print(f"\n{'着順':>4} {'馬番':>4} {'馬名':>14} {'オッズ':>8} {'O順':>4} {'予測勝率':>8} {'P順':>4} {'乖離':>6} {'期待値':>6}")
        print("-" * 90)
        
        for h in result['horses']:
            finish = h['finish_position']
            odds_str = f"{h['win_odds']:.1f}" if h['win_odds'] else 'N/A'
            gap = h['pred_vs_finish_gap']
            gap_str = f"{gap:+d}" if gap else 'N/A'
            
            # 問題パターンのマーク
            marker = ''
            if h['win_odds'] and h['win_odds'] <= 10 and h['pred_rank'] >= 6:
                marker = '⚠低オッズ高P順'
            elif h['win_odds'] and h['win_odds'] >= 50 and h['pred_rank'] <= 3:
                marker = '⚠高オッズ低P順'
            
            print(f"{finish:>4} {h['horse_number']:>4} {h['horse_name']:>14} {odds_str:>8} {h['odds_rank']:>4} {h['pred_prob']:>7.1f}% {h['pred_rank']:>4} {gap_str:>6} {h['expected_value']:>6.2f} {marker}")
    
    def analyze_multiple_races(self, n_races: int = 20, year: int = 2025):
        """
        複数レースの分析
        
        予測と実着順の乖離パターンを探る
        """
        logger.info(f"{year}年の{n_races}レースを分析")
        
        race_ids = self.races_df[self.races_df['year'] == year]['race_id'].unique()
        
        # ランダムに選択
        np.random.seed(42)
        if len(race_ids) > n_races:
            sample_ids = np.random.choice(race_ids, n_races, replace=False)
        else:
            sample_ids = race_ids
        
        all_results = []
        problem_cases = []
        
        for i, race_id in enumerate(sample_ids):
            result = self.analyze_race_detailed(race_id, verbose=False)
            if not result:
                continue
            
            all_results.append(result)
            
            # 問題パターンを検出
            for h in result['horses']:
                odds = h['win_odds']
                pred_rank = h['pred_rank']
                finish = h['finish_position']
                
                if odds and finish:
                    # 低オッズ（3-10倍）なのに予測6位以下 & 実際上位（3着以内）
                    if 3 <= odds <= 10 and pred_rank >= 6 and finish <= 3:
                        problem_cases.append({
                            'race_id': race_id,
                            'pattern': 'undervalued_favorite',
                            'horse_number': h['horse_number'],
                            'odds': odds,
                            'pred_rank': pred_rank,
                            'finish': finish,
                        })
                    
                    # 高オッズ（50倍超）なのに予測3位以内 & 実際下位（6着以下）
                    if odds >= 50 and pred_rank <= 3 and finish >= 6:
                        problem_cases.append({
                            'race_id': race_id,
                            'pattern': 'overvalued_longshot',
                            'horse_number': h['horse_number'],
                            'odds': odds,
                            'pred_rank': pred_rank,
                            'finish': finish,
                        })
        
        # 集計
        print(f"\n{'='*100}")
        print(f"【{n_races}レース分析サマリー】")
        print(f"{'='*100}")
        
        undervalued = [p for p in problem_cases if p['pattern'] == 'undervalued_favorite']
        overvalued = [p for p in problem_cases if p['pattern'] == 'overvalued_longshot']
        
        print(f"\n問題パターン検出数:")
        print(f"  ▸ 低オッズ(3-10倍)馬が予測6位以下だが実際3着以内: {len(undervalued)}件")
        print(f"  ▸ 高オッズ(50倍超)馬が予測3位以内だが実際6着以下: {len(overvalued)}件")
        
        if undervalued:
            print(f"\n【低オッズ過小評価の具体例（上位5件）】")
            for p in undervalued[:5]:
                print(f"  {p['race_id']}: {p['horse_number']}番 オッズ{p['odds']:.1f}倍 → 予測{p['pred_rank']}位 → 実着{p['finish']}位")
        
        if overvalued:
            print(f"\n【高オッズ過大評価の具体例（上位5件）】")
            for p in overvalued[:5]:
                print(f"  {p['race_id']}: {p['horse_number']}番 オッズ{p['odds']:.1f}倍 → 予測{p['pred_rank']}位 → 実着{p['finish']}位")
        
        # 詳細レースを数件表示
        print(f"\n\n【詳細レース分析（5件）】")
        for result in all_results[:5]:
            self._print_race_analysis(result)
        
        return all_results, problem_cases


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='予測乖離分析')
    parser.add_argument('--race-id', type=str, help='単一レースの分析')
    parser.add_argument('--multi', type=int, default=20, help='複数レース分析（件数）')
    parser.add_argument('--year', type=int, default=2025, help='対象年')
    args = parser.parse_args()
    
    analyzer = PredictionGapAnalyzer()
    
    if args.race_id:
        analyzer.analyze_race_detailed(args.race_id)
    else:
        analyzer.analyze_multiple_races(n_races=args.multi, year=args.year)
