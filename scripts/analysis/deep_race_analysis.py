#!/usr/bin/env python3
"""
深層レース分析スクリプト (deep_race_analysis.py)

【目的】
- レースごとの詳細な期待値分析
- オッズ分布（四分位、中央値、外れ値）の解析
- 予測勝率 vs オッズの相対比較
- 的中時の配当（returns）データとの照合
- 150倍超の穴馬分析

【分析観点】
1. 各レースごとのオッズバラつき（グループ平均ではなく個別分析）
2. 期待値 = 勝率予測 × オッズ
3. 馬単・三連複・三連単の配当可能性
4. オッズ帯別（1-3倍、3-10倍、10-50倍、50-150倍、150倍超）の着順分析

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
from sklearn.metrics import roc_auc_score

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('deep_race_analysis.log', encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)


class DeepRaceAnalyzer:
    """深層レース分析クラス"""
    
    LEAK_FEATURES = [
        'finish_position', 'finish_time_seconds', 'margin_seconds', 'prize_money',
        'finish_time_str', 'margin_str', 'last_3f_time', 'time_except_last3f',
        'last3f_rank', 'popularity', 'win_odds', 'odds', 'win_probability',
        'relative_odds', 'popularity_finish_diff',
        'passing_order', 'passing_order_1', 'passing_order_2',
        'passing_order_3', 'passing_order_4', 'final_corner_to_finish',
        'position_change_1_2', 'position_change_2_3', 'position_change_3_4',
        'pace_index', 'target', 'weight',
        'horse_weight_change', 'horse_weight_deviation',
    ]
    
    META_COLS = [
        'race_id', 'horse_id', 'race_date', 'jockey_id', 'trainer_id', 'owner_id',
        'sire_id', 'damsire_id', 'race_name', 'horse_name', 'jockey_name', 
        'trainer_name', 'scratched', 'year', 'track_surface', 'race_class',
        'venue', 'track_condition', 'distance_category', 'bracket_category',
        'sex', 'weather',
    ]
    
    def __init__(self, data_dir: str = 'keibaai/data/parsed/parquet', 
                 model_dir: str = 'keibaai/models/turf_dirt_separate'):
        self.data_dir = Path(data_dir)
        self.model_dir = Path(model_dir)
        self.output_dir = self.model_dir / 'analysis'
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
        
        # 2014年以降、芝、障害・新馬除外
        df = df[df['year'] >= 2014]
        df = df[df['track_surface'] == '芝']
        df = df[df['race_class'] != '新馬']
        
        df['target'] = (df['finish_position'] == 1).astype(int)
        
        logger.info(f"レースデータ: {len(df):,}件")
        return df
    
    def _load_returns(self) -> pd.DataFrame:
        """払戻データ読み込み"""
        logger.info("払戻データ読み込み中...")
        returns_path = self.data_dir / 'returns/returns.parquet'
        if not returns_path.exists():
            logger.warning("払戻データが見つかりません")
            return pd.DataFrame()
        
        df = pd.read_parquet(returns_path)
        logger.info(f"払戻データ: {len(df):,}件")
        logger.info(f"払戻データカラム: {df.columns.tolist()}")
        return df
    
    def _generate_features_for_race(self, race_df: pd.DataFrame, history_df: pd.DataFrame) -> pd.DataFrame:
        """特定レース用の特徴量生成（リークフリー）"""
        df = race_df.copy()
        
        # 基本特徴量
        df['field_size'] = len(df)
        
        track_cond_map = {'良': 0, '稍重': 1, '重': 2, '不良': 3}
        df['track_condition_encoded'] = df['track_condition'].map(track_cond_map).fillna(0)
        
        df['race_number'] = df['race_id'].str[-2:].astype(int)
        df['is_late_race'] = (df['race_number'] >= 8).astype(int)
        
        if len(history_df) == 0:
            for col in ['horse_win_rate', 'horse_avg_finish', 'jockey_win_rate', 
                       'trainer_win_rate', 'prev_finish', 'days_since_last', 
                       'weight_change', 'distance_match', 'venue_encoded']:
                df[col] = np.nan if col != 'distance_match' else 0
            return df
        
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
    
    def analyze_single_race(self, race_id: str, verbose: bool = True) -> Dict:
        """
        単一レースの詳細分析
        
        【分析内容】
        1. 予測勝率と実際のオッズの比較
        2. 期待値計算（勝率×オッズ）
        3. レース内オッズ分布（四分位、中央値、外れ値）
        4. 実際の着順との比較
        5. 馬単・三連系の配当可能性
        """
        race_df = self.races_df[self.races_df['race_id'] == race_id].copy()
        
        if len(race_df) == 0:
            logger.error(f"レースが見つかりません: {race_id}")
            return {}
        
        race_date = race_df['race_date'].iloc[0]
        
        # リークフリーな履歴データ
        history_df = self.races_df[self.races_df['race_date'] < race_date].copy()
        
        # 特徴量生成
        race_feat = self._generate_features_for_race(race_df, history_df)
        
        # 欠損特徴量の補完
        for col in self.feature_cols:
            if col not in race_feat.columns:
                race_feat[col] = np.nan
        
        # 予測
        X = race_feat[self.feature_cols]
        preds = self.model.predict(X)
        race_feat['pred_prob'] = preds
        race_feat['pred_rank'] = race_feat['pred_prob'].rank(ascending=False, method='first')
        
        # オッズと期待値の計算
        race_feat['expected_value'] = race_feat['pred_prob'] * race_feat['win_odds']
        race_feat['ev_rank'] = race_feat['expected_value'].rank(ascending=False, method='first')
        
        # オッズ分布の計算
        odds = race_feat['win_odds'].dropna()
        odds_stats = {
            'min': odds.min(),
            'q1': odds.quantile(0.25),
            'median': odds.median(),
            'q3': odds.quantile(0.75),
            'max': odds.max(),
            'iqr': odds.quantile(0.75) - odds.quantile(0.25),
        }
        
        # オッズ帯分類
        def categorize_odds(o):
            if pd.isna(o): return 'unknown'
            elif o <= 3: return '1-3'
            elif o <= 10: return '3-10'
            elif o <= 50: return '10-50'
            elif o <= 150: return '50-150'
            else: return '150+'
        
        race_feat['odds_category'] = race_feat['win_odds'].apply(categorize_odds)
        
        # 実際の着順との比較
        winner = race_feat[race_feat['finish_position'] == 1]
        top3 = race_feat[race_feat['finish_position'] <= 3].sort_values('finish_position')
        
        result = {
            'race_id': race_id,
            'race_date': str(race_date.date()),
            'race_name': race_feat['race_name'].iloc[0] if 'race_name' in race_feat.columns else '',
            'field_size': len(race_feat),
            'odds_stats': odds_stats,
            'predictions': [],
            'winner': None,
            'top3': [],
        }
        
        # 各馬の詳細
        for _, row in race_feat.sort_values('pred_rank').iterrows():
            horse_data = {
                'horse_number': int(row['horse_number']),
                'horse_name': row.get('horse_name', ''),
                'pred_prob': round(row['pred_prob'], 4),
                'pred_rank': int(row['pred_rank']),
                'win_odds': row['win_odds'],
                'expected_value': round(row['expected_value'], 4) if pd.notna(row['expected_value']) else None,
                'ev_rank': int(row['ev_rank']) if pd.notna(row['ev_rank']) else None,
                'finish_position': int(row['finish_position']) if pd.notna(row['finish_position']) else None,
                'odds_category': row['odds_category'],
            }
            result['predictions'].append(horse_data)
            
            if row['finish_position'] == 1:
                result['winner'] = horse_data
            if row['finish_position'] <= 3:
                result['top3'].append(horse_data)
        
        # 配当情報（returnsから取得）
        if len(self.returns_df) > 0:
            race_returns = self.returns_df[self.returns_df['race_id'] == race_id]
            if len(race_returns) > 0:
                result['returns'] = race_returns.to_dict('records')
        
        if verbose:
            self._print_race_analysis(result)
        
        return result
    
    def _print_race_analysis(self, result: Dict):
        """レース分析結果を表示"""
        print(f"\n{'='*80}")
        print(f"レースID: {result['race_id']} ({result['race_date']})")
        print(f"レース名: {result['race_name']}")
        print(f"出走頭数: {result['field_size']}頭")
        print(f"{'='*80}")
        
        # オッズ分布
        stats = result['odds_stats']
        print(f"\n【オッズ分布】")
        print(f"  最小: {stats['min']:.1f} | Q1: {stats['q1']:.1f} | 中央値: {stats['median']:.1f} | Q3: {stats['q3']:.1f} | 最大: {stats['max']:.1f}")
        print(f"  四分位範囲(IQR): {stats['iqr']:.1f}")
        
        # 勝ち馬情報
        if result['winner']:
            w = result['winner']
            print(f"\n【勝ち馬】")
            print(f"  {w['horse_number']}番 {w['horse_name']}")
            print(f"  オッズ: {w['win_odds']:.1f}倍 | 予測勝率: {w['pred_prob']:.1%} (予測{w['pred_rank']}位)")
            print(f"  期待値: {w['expected_value']:.2f} (期待値{w['ev_rank']}位)")
        
        # 予測 vs 実際
        print(f"\n{'馬番':>4} {'馬名':>12} {'予測勝率':>8} {'予測順':>4} {'オッズ':>8} {'期待値':>6} {'EV順':>4} {'実着順':>6} {'オッズ帯':>6}")
        print("-" * 80)
        
        for p in result['predictions']:
            marker = ''
            if p['finish_position'] == 1:
                marker = '★1着'
            elif p['finish_position'] and p['finish_position'] <= 3:
                marker = f'★{p["finish_position"]}着'
            
            ev_str = f"{p['expected_value']:.2f}" if p['expected_value'] else 'N/A'
            ev_rank_str = str(p['ev_rank']) if p['ev_rank'] else 'N/A'
            odds_str = f"{p['win_odds']:.1f}" if p['win_odds'] else 'N/A'
            finish_str = str(p['finish_position']) if p['finish_position'] else 'N/A'
            
            print(f"{p['horse_number']:>4} {p['horse_name']:>12} {p['pred_prob']:>7.1%} {p['pred_rank']:>4} {odds_str:>8} {ev_str:>6} {ev_rank_str:>4} {finish_str:>6} {p['odds_category']:>6} {marker}")
        
        # 配当情報
        if 'returns' in result and result['returns']:
            print(f"\n【払戻金】")
            for ret in result['returns']:
                print(f"  {ret}")
    
    def analyze_odds_tier_performance(self, start_year: int = 2020, end_year: int = 2025) -> pd.DataFrame:
        """
        オッズ帯別のパフォーマンス分析
        
        【分析】
        - 各オッズ帯での的中率、平均着順分布
        - 150倍超の穴馬がどこに着くか
        """
        logger.info(f"オッズ帯別分析: {start_year}-{end_year}年")
        
        df = self.races_df[(self.races_df['year'] >= start_year) & (self.races_df['year'] <= end_year)].copy()
        
        # オッズ帯分類
        def categorize_odds(o):
            if pd.isna(o): return 'unknown'
            elif o <= 3: return '1-3'
            elif o <= 10: return '3-10'
            elif o <= 50: return '10-50'
            elif o <= 150: return '50-150'
            else: return '150+'
        
        df['odds_category'] = df['win_odds'].apply(categorize_odds)
        
        # オッズ帯別統計
        results = []
        for cat in ['1-3', '3-10', '10-50', '50-150', '150+']:
            cat_df = df[df['odds_category'] == cat]
            if len(cat_df) == 0:
                continue
            
            # 着順分布
            total = len(cat_df)
            win = len(cat_df[cat_df['finish_position'] == 1])
            top3 = len(cat_df[cat_df['finish_position'] <= 3])
            top5 = len(cat_df[cat_df['finish_position'] <= 5])
            
            # 平均着順
            avg_finish = cat_df['finish_position'].dropna().mean()
            median_finish = cat_df['finish_position'].dropna().median()
            
            # ROI（単勝）
            wins = cat_df[cat_df['finish_position'] == 1]
            roi = wins['win_odds'].sum() / total if total > 0 else 0
            
            results.append({
                'odds_category': cat,
                'count': total,
                'win_rate': win / total,
                'top3_rate': top3 / total,
                'top5_rate': top5 / total,
                'avg_finish': avg_finish,
                'median_finish': median_finish,
                'roi': roi,
            })
        
        results_df = pd.DataFrame(results)
        
        # 表示
        print(f"\n{'='*80}")
        print(f"オッズ帯別パフォーマンス ({start_year}-{end_year}年)")
        print(f"{'='*80}")
        print(f"{'オッズ帯':>8} {'件数':>8} {'勝率':>8} {'複勝率':>8} {'5着内':>8} {'平均着順':>8} {'中央着順':>8} {'ROI':>8}")
        print("-" * 80)
        
        for _, row in results_df.iterrows():
            print(f"{row['odds_category']:>8} {row['count']:>8,} {row['win_rate']:>7.1%} {row['top3_rate']:>7.1%} {row['top5_rate']:>7.1%} {row['avg_finish']:>8.1f} {row['median_finish']:>8.1f} {row['roi']:>7.1%}")
        
        return results_df
    
    def run_full_analysis(self, start_year: int = 2020, end_year: int = 2025, sample_races: int = 100):
        """
        2020-2025年の全レース分析
        
        【出力】
        1. オッズ帯別パフォーマンス
        2. 期待値ベース推奨の検証
        3. サンプルレースの詳細分析
        """
        logger.info(f"全レース分析開始: {start_year}-{end_year}年")
        
        # 1. オッズ帯別分析
        odds_tier_df = self.analyze_odds_tier_performance(start_year, end_year)
        odds_tier_df.to_csv(self.output_dir / 'odds_tier_performance.csv', index=False)
        
        # 2. サンプルレース詳細分析
        race_ids = self.races_df[
            (self.races_df['year'] >= start_year) & 
            (self.races_df['year'] <= end_year)
        ]['race_id'].unique()
        
        logger.info(f"対象レース数: {len(race_ids):,}")
        
        # サンプリング
        if len(race_ids) > sample_races:
            np.random.seed(42)
            sample_ids = np.random.choice(race_ids, sample_races, replace=False)
        else:
            sample_ids = race_ids
        
        all_results = []
        for i, race_id in enumerate(sample_ids):
            if i % 20 == 0:
                logger.info(f"  分析中: {i+1}/{len(sample_ids)}")
            
            result = self.analyze_single_race(race_id, verbose=False)
            if result:
                all_results.append(result)
        
        # 3. 全体統計の計算
        self._calculate_aggregate_stats(all_results)
        
        logger.info(f"分析完了。結果: {self.output_dir}")
        
        return all_results
    
    def _calculate_aggregate_stats(self, results: List[Dict]):
        """集計統計の計算"""
        if not results:
            return
        
        # 予測1位の的中率
        pred1_wins = sum(1 for r in results if r['winner'] and r['winner']['pred_rank'] == 1)
        pred1_total = len(results)
        
        # 期待値1位の的中率
        ev1_wins = sum(1 for r in results if r['winner'] and r['winner']['ev_rank'] == 1)
        
        # 予測Top3に実際のTop3が何頭入っているか
        top3_matches = []
        for r in results:
            pred_top3 = set(p['horse_number'] for p in r['predictions'] if p['pred_rank'] <= 3)
            actual_top3 = set(p['horse_number'] for p in r['top3'])
            matches = len(pred_top3 & actual_top3)
            top3_matches.append(matches)
        
        print(f"\n{'='*80}")
        print(f"集計統計 ({len(results)}レース)")
        print(f"{'='*80}")
        print(f"予測1位の勝率: {pred1_wins}/{pred1_total} ({pred1_wins/pred1_total:.1%})")
        print(f"期待値1位の勝率: {ev1_wins}/{pred1_total} ({ev1_wins/pred1_total:.1%})")
        print(f"予測Top3と実際のTop3の一致: 平均{np.mean(top3_matches):.1f}頭/3頭")


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='深層レース分析')
    parser.add_argument('--race-id', type=str, help='単一レースの分析')
    parser.add_argument('--full', action='store_true', help='2020-2025年全レース分析')
    parser.add_argument('--sample', type=int, default=100, help='サンプルレース数')
    args = parser.parse_args()
    
    analyzer = DeepRaceAnalyzer()
    
    if args.race_id:
        analyzer.analyze_single_race(args.race_id)
    elif args.full:
        analyzer.run_full_analysis(sample_races=args.sample)
    else:
        # デモ: 202506050803の分析
        print("デモ: レース202506050803の詳細分析")
        analyzer.analyze_single_race('202506050803')
