#!/usr/bin/env python3
"""
価値予測品質分析 (value_prediction_analysis.py)

【評価基準】
✅ ポジティブ（高評価）:
   1. 低オッズ(3-10倍)を低予測 & 実着順も低 → 罠回避成功
   2. 高オッズ(10倍超)を高予測 & 実着順も高(3着以内) → 穴馬発掘成功

❌ ネガティブ（低評価）:
   1. 低オッズ(3-10倍)を低予測 & 実着順は高(3着以内) → 機会損失

⚪ 中立:
   1. 高オッズを高予測 & 実着順は低 → リスクテイク（許容範囲）

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
from typing import List, Dict, Optional

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class ValuePredictionAnalyzer:
    """価値予測品質分析クラス"""
    
    def __init__(self, data_dir: str = 'keibaai/data/parsed/parquet', 
                 model_dir: str = 'keibaai/models/turf_dirt_separate'):
        self.data_dir = Path(data_dir)
        self.model_dir = Path(model_dir)
        
        self.model = lgb.Booster(model_file=str(self.model_dir / 'turf_model.txt'))
        with open(self.model_dir / 'features.json', 'r') as f:
            self.feature_cols = json.load(f)
        
        self.races_df = self._load_races()
        self.returns_df = self._load_returns()
        
    def _load_races(self) -> pd.DataFrame:
        df = pd.read_parquet(self.data_dir / 'races/races.parquet')
        df['race_date'] = pd.to_datetime(df['race_date'])
        df['year'] = df['race_date'].dt.year
        df = df[df['year'] >= 2014]
        df = df[df['track_surface'] == '芝']
        df = df[df['race_class'] != '新馬']
        df['target'] = (df['finish_position'] == 1).astype(int)
        return df
    
    def _load_returns(self) -> pd.DataFrame:
        returns_path = self.data_dir / 'returns/returns.parquet'
        if returns_path.exists():
            return pd.read_parquet(returns_path)
        return pd.DataFrame()
    
    def _generate_features(self, race_df, history_df):
        """特徴量生成（簡略版）"""
        df = race_df.copy()
        if len(history_df) == 0:
            return None
        
        df['field_size'] = len(df)
        track_cond_map = {'良': 0, '稀重': 1, '重': 2, '不良': 3}
        df['track_condition_encoded'] = df['track_condition'].map(track_cond_map).fillna(0)
        df['race_number'] = df['race_id'].str[-2:].astype(int)
        df['is_late_race'] = (df['race_number'] >= 8).astype(int)
        
        history_df['target'] = (history_df['finish_position'] == 1).astype(int)
        
        horse_stats = history_df.groupby('horse_id').agg({'target': ['sum', 'count'], 'finish_position': 'mean'}).reset_index()
        horse_stats.columns = ['horse_id', 'wins', 'races', 'horse_avg_finish']
        horse_stats['horse_win_rate'] = np.where(horse_stats['races'] >= 3, horse_stats['wins'] / horse_stats['races'], np.nan)
        
        jockey_stats = history_df.groupby('jockey_id').agg({'target': ['sum', 'count']}).reset_index()
        jockey_stats.columns = ['jockey_id', 'wins', 'races']
        jockey_stats['jockey_win_rate'] = np.where(jockey_stats['races'] >= 10, jockey_stats['wins'] / jockey_stats['races'], np.nan)
        
        trainer_stats = history_df.groupby('trainer_id').agg({'target': ['sum', 'count']}).reset_index()
        trainer_stats.columns = ['trainer_id', 'wins', 'races']
        trainer_stats['trainer_win_rate'] = np.where(trainer_stats['races'] >= 10, trainer_stats['wins'] / trainer_stats['races'], np.nan)
        
        last_race = history_df.sort_values(['horse_id', 'race_date']).groupby('horse_id').last()[['finish_position', 'race_date', 'horse_weight']].reset_index()
        last_race.columns = ['horse_id', 'prev_finish', 'prev_race_date', 'prev_horse_weight']
        
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
    
    def analyze_race_value(self, race_id: str):
        """
        レースの価値予測品質を分析
        """
        race_df = self.races_df[self.races_df['race_id'] == race_id].copy()
        if len(race_df) == 0:
            return None
        
        race_date = race_df['race_date'].iloc[0]
        history_df = self.races_df[self.races_df['race_date'] < race_date].copy()
        
        race_feat = self._generate_features(race_df, history_df)
        if race_feat is None:
            return None
        
        for col in self.feature_cols:
            if col not in race_feat.columns:
                race_feat[col] = np.nan
        
        X = race_feat[self.feature_cols]
        preds = self.model.predict(X)
        race_feat['pred_prob'] = preds
        race_feat['pred_rank'] = race_feat['pred_prob'].rank(ascending=False, method='first')
        race_feat['odds_rank'] = race_feat['win_odds'].rank(ascending=True, method='first')
        
        # 価値評価
        evaluations = []
        for _, row in race_feat.iterrows():
            odds = row['win_odds']
            pred_rank = int(row['pred_rank'])
            finish = int(row['finish_position']) if pd.notna(row['finish_position']) else None
            
            eval_result = self._evaluate_prediction(odds, pred_rank, finish, len(race_feat))
            
            evaluations.append({
                'horse_number': int(row['horse_number']),
                'horse_name': row.get('horse_name', ''),
                'odds': odds if pd.notna(odds) else None,
                'odds_rank': int(row['odds_rank']) if pd.notna(row['odds_rank']) else None,
                'pred_prob': round(row['pred_prob'] * 100, 2),
                'pred_rank': pred_rank,
                'finish': finish,
                'evaluation': eval_result['type'],
                'evaluation_detail': eval_result['detail'],
            })
        
        return {
            'race_id': race_id,
            'race_date': str(race_date.date()),
            'race_name': race_feat['race_name'].iloc[0] if 'race_name' in race_feat.columns else '',
            'race_class': race_feat['race_class'].iloc[0] if 'race_class' in race_feat.columns else '',
            'field_size': len(race_feat),
            'horses': evaluations,
        }
    
    def _evaluate_prediction(self, odds, pred_rank, finish, field_size) -> Dict:
        """
        予測の価値評価
        
        オッズ帯:
        - 低: 3-10倍（人気馬）
        - 中: 10-50倍
        - 高: 50倍超（穴馬）
        
        予測順位:
        - 高評価: Top3
        - 低評価: 6位以下
        
        着順:
        - 好: 3着以内
        - 悪: 6着以下
        """
        if pd.isna(odds) or pd.isna(finish):
            return {'type': 'N/A', 'detail': 'データ不足'}
        
        # オッズ帯判定
        if odds <= 3:
            odds_tier = 'super_low'
        elif odds <= 10:
            odds_tier = 'low'
        elif odds <= 50:
            odds_tier = 'mid'
        else:
            odds_tier = 'high'
        
        # 予測評価判定
        pred_high = pred_rank <= 3
        pred_low = pred_rank >= 6
        
        # 実着順判定
        finish_good = finish <= 3
        finish_bad = finish >= 6
        
        # ========== 評価ロジック ==========
        
        # ✅ 低オッズを低評価 & 実際に着順悪い → 罠回避成功
        if odds_tier in ['low', 'super_low'] and pred_low and finish_bad:
            return {'type': '✅罠回避', 'detail': f'低オッズ{odds:.1f}倍を予測{pred_rank}位(低評価)→実着{finish}位'}
        
        # ✅ 高オッズを高評価 & 実際に好着順 → 穴馬発掘成功
        if odds_tier in ['mid', 'high'] and pred_high and finish_good:
            return {'type': '✅穴発掘', 'detail': f'高オッズ{odds:.1f}倍を予測{pred_rank}位(高評価)→実着{finish}位'}
        
        # ❌ 低オッズを低評価 & 実際は好着順 → 機会損失
        if odds_tier in ['low', 'super_low'] and pred_low and finish_good:
            return {'type': '❌機会損失', 'detail': f'低オッズ{odds:.1f}倍を予測{pred_rank}位(低評価)→実着{finish}位'}
        
        # ❌ 高オッズを高評価 & 実際は着順悪い（ただし許容範囲）
        if odds_tier in ['mid', 'high'] and pred_high and finish_bad:
            return {'type': '⚪リスクテイク', 'detail': f'高オッズ{odds:.1f}倍を予測{pred_rank}位(高評価)→実着{finish}位(許容範囲)'}
        
        # ✅ 低オッズを高評価 & 実際も好着順 → 順当
        if odds_tier in ['low', 'super_low'] and pred_high and finish_good:
            return {'type': '✅順当', 'detail': f'低オッズ{odds:.1f}倍を予測{pred_rank}位→実着{finish}位'}
        
        # その他
        return {'type': '—', 'detail': f'オッズ{odds:.1f}倍, 予測{pred_rank}位, 実着{finish}位'}
    
    def analyze_multiple_races(self, n_races: int = 50, year: int = 2025):
        """複数レースの価値分析"""
        logger.info(f"{year}年の{n_races}レースを分析")
        
        race_ids = self.races_df[self.races_df['year'] == year]['race_id'].unique()
        np.random.seed(42)
        sample_ids = np.random.choice(race_ids, min(n_races, len(race_ids)), replace=False)
        
        all_results = []
        summary = {
            '✅罠回避': 0,
            '✅穴発掘': 0,
            '✅順当': 0,
            '❌機会損失': 0,
            '⚪リスクテイク': 0,
            '—': 0,
            'N/A': 0,
        }
        
        problem_examples = []
        success_examples = []
        
        for race_id in sample_ids:
            result = self.analyze_race_value(race_id)
            if not result:
                continue
            
            all_results.append(result)
            
            for h in result['horses']:
                eval_type = h['evaluation']
                if eval_type in summary:
                    summary[eval_type] += 1
                
                if eval_type == '❌機会損失':
                    problem_examples.append({
                        'race_id': race_id,
                        'race_name': result['race_name'],
                        **h
                    })
                elif eval_type in ['✅罠回避', '✅穴発掘']:
                    success_examples.append({
                        'race_id': race_id,
                        'race_name': result['race_name'],
                        **h
                    })
        
        # サマリー出力
        total = sum(summary.values())
        print(f"\n{'='*100}")
        print(f"【価値予測品質サマリー】{n_races}レース / {year}年")
        print(f"{'='*100}")
        
        print(f"\n評価区分:")
        for k, v in summary.items():
            pct = v / total * 100 if total > 0 else 0
            print(f"  {k}: {v}件 ({pct:.1f}%)")
        
        # 良い例
        if success_examples:
            print(f"\n【成功例（罠回避・穴発掘）上位10件】")
            for ex in success_examples[:10]:
                print(f"  {ex['race_id']} {ex['race_name'][:15]:15} {ex['horse_number']:>2}番{ex['horse_name'][:8]:8} | {ex['evaluation']} | {ex['evaluation_detail']}")
        
        # 問題例
        if problem_examples:
            print(f"\n【機会損失例（低オッズを見逃し）上位10件】")
            for ex in problem_examples[:10]:
                print(f"  {ex['race_id']} {ex['race_name'][:15]:15} {ex['horse_number']:>2}番{ex['horse_name'][:8]:8} | {ex['evaluation']} | {ex['evaluation_detail']}")
        
        # 詳細レース表示
        print(f"\n\n【詳細レース分析（3件）】")
        for result in all_results[:3]:
            self._print_race_detail(result)
        
        return summary, problem_examples, success_examples
    
    def _print_race_detail(self, result):
        """レース詳細表示"""
        print(f"\n{'='*100}")
        print(f"【レース】{result['race_id']} ({result['race_date']}) {result['race_name']} [{result['race_class']}]")
        print(f"{'='*100}")
        
        print(f"\n{'着順':>4} {'馬番':>4} {'馬名':>10} {'オッズ':>8} {'O順':>4} {'予測%':>6} {'P順':>4} {'評価':>10}")
        print("-" * 80)
        
        for h in sorted(result['horses'], key=lambda x: x['finish'] if x['finish'] else 99):
            marker = h['evaluation']
            print(f"{h['finish']:>4} {h['horse_number']:>4} {h['horse_name'][:10]:>10} {h['odds']:>8.1f} {h['odds_rank']:>4} {h['pred_prob']:>5.1f}% {h['pred_rank']:>4} {marker:>10}")


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='価値予測品質分析')
    parser.add_argument('--race-id', type=str, help='単一レース分析')
    parser.add_argument('--multi', type=int, default=50, help='複数レース分析')
    parser.add_argument('--year', type=int, default=2025, help='対象年')
    args = parser.parse_args()
    
    analyzer = ValuePredictionAnalyzer()
    
    if args.race_id:
        result = analyzer.analyze_race_value(args.race_id)
        if result:
            analyzer._print_race_detail(result)
    else:
        analyzer.analyze_multiple_races(n_races=args.multi, year=args.year)
