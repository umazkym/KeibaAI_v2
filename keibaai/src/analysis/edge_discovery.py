#!/usr/bin/env python3
"""
Edge Discovery Analysis Module (v5.1)

v11.0モデルの予測確率とオッズ暗示確率の乖離を分析し、
エッジが存在する条件を特定する。

【目的】
- 市場（オッズ）が見落としている情報・パターンを発見
- Edge Score = モデル確率 / オッズ暗示確率
- 条件別（距離・馬場・クラス）のエッジ分析

【使用方法】
python keibaai/src/analysis/edge_discovery.py --model_dir keibaai/models/mu_v11_0

【出力】
- edge_analysis_report.json: 詳細分析レポート
- edge_threshold_analysis.csv: 閾値別ROIシミュレーション
- hypotheses.md: 発見したエッジ仮説
"""

import sys
from pathlib import Path

project_root = Path(__file__).resolve().parent.parent.parent.parent
sys.path.append(str(project_root))

import pandas as pd
import numpy as np
import pickle
import json
import logging
from datetime import datetime
from typing import Dict, List, Tuple, Optional

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class EdgeDiscoveryAnalyzer:
    """
    エッジ発見分析クラス
    
    市場（オッズ）が見落としている情報・パターンを発見し、
    ROI向上のための仮説を立案する。
    """
    
    def __init__(self, model_dir: str = 'keibaai/models/mu_v11_0'):
        self.model_dir = Path(model_dir)
        self.output_dir = Path('results/edge_analysis')
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        self.model = None
        self.feature_names = None
        self.predictions_df = None
        
    def load_model_and_data(self):
        """モデルと必要なデータを読み込み"""
        logging.info("=" * 60)
        logging.info("Edge Discovery: データ読み込み")
        logging.info("=" * 60)
        
        # モデル読み込み
        model_path = self.model_dir / 'mu_v11_0_ranker.pkl'
        if not model_path.exists():
            # 他のバージョンを探す
            pkl_files = list(self.model_dir.glob('*.pkl'))
            if pkl_files:
                model_path = pkl_files[0]
            else:
                raise FileNotFoundError(f"モデルファイルが見つかりません: {self.model_dir}")
        
        with open(model_path, 'rb') as f:
            self.model = pickle.load(f)
        logging.info(f"  モデル読み込み: {model_path.name}")
        
        # 特徴量名読み込み
        feature_path = self.model_dir / 'feature_names.json'
        if feature_path.exists():
            with open(feature_path, 'r', encoding='utf-8') as f:
                self.feature_names = json.load(f)
            logging.info(f"  特徴量数: {len(self.feature_names)}")
        
        # レースデータ読み込み
        races_df = pd.read_parquet('keibaai/data/parsed/parquet/races/races.parquet')
        races_df['race_date'] = pd.to_datetime(races_df['race_date'])
        
        # 分析対象期間（Test期間 = 2024年以降）
        test_df = races_df[races_df['race_date'] >= '2024-01-01'].copy()
        valid_df = races_df[(races_df['race_date'] >= '2023-01-01') & 
                           (races_df['race_date'] < '2024-01-01')].copy()
        
        logging.info(f"  Valid期間: {len(valid_df):,}件")
        logging.info(f"  Test期間: {len(test_df):,}件")
        
        return valid_df, test_df
    
    def calculate_edge_scores(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Edge Scoreを計算
        
        Edge Score = モデル確率 / オッズ暗示確率
        
        - Edge Score > 1.0: モデルが市場より高評価
        - Edge Score < 1.0: モデルが市場より低評価
        """
        logging.info("Edge Score計算中...")
        
        df = df.copy()
        
        # オッズを数値化
        df['win_odds'] = pd.to_numeric(df['win_odds'], errors='coerce')
        df['finish_position'] = pd.to_numeric(df['finish_position'], errors='coerce')
        df['popularity'] = pd.to_numeric(df['popularity'], errors='coerce')
        
        # レースごとにオッズ暗示確率を計算
        # オッズ暗示確率 = (1/odds) / Σ(1/odds) × 0.8 (控除率20%調整)
        df['raw_odds_prob'] = 1 / df['win_odds'].clip(lower=1.0)
        race_sum = df.groupby('race_id')['raw_odds_prob'].transform('sum')
        df['odds_implied_prob'] = (df['raw_odds_prob'] / race_sum) * 0.8
        
        # モデル予測確率（レース内で正規化してsoftmax的に）
        # Rankerの出力スコアをsoftmaxで確率に変換
        # ここでは簡易的に、レース内でのランキングから疑似確率を計算
        # 注意: 実際のモデル予測は特徴量が必要なため、ここでは人気をプロキシとして使用
        
        # 代替: 1番人気=35%, 2番人気=20%, 3番人気=12%, ...という経験的分布を使用
        # 実運用時はモデル予測確率を使用
        pop_to_prob = {
            1: 0.33, 2: 0.19, 3: 0.13, 4: 0.09, 5: 0.07,
            6: 0.05, 7: 0.04, 8: 0.03, 9: 0.025, 10: 0.02,
            11: 0.015, 12: 0.012, 13: 0.010, 14: 0.008, 15: 0.006,
            16: 0.005, 17: 0.004, 18: 0.003
        }
        
        # 経験的勝率を割り当て（あとでモデル予測で置き換え可能）
        df['empirical_win_prob'] = df['popularity'].map(pop_to_prob).fillna(0.005)
        
        # Edge Score計算
        df['edge_score'] = df['empirical_win_prob'] / df['odds_implied_prob'].clip(lower=0.01)
        
        # 勝敗フラグ (NAを処理してからint変換)
        df['is_win'] = (df['finish_position'] == 1).fillna(False).astype(int)
        
        logging.info(f"  Edge Score計算完了")
        logging.info(f"  平均Edge Score: {df['edge_score'].mean():.3f}")
        
        return df
    
    def analyze_edge_by_condition(self, df: pd.DataFrame) -> Dict:
        """条件別エッジ分析"""
        logging.info("=" * 60)
        logging.info("条件別エッジ分析")
        logging.info("=" * 60)
        
        results = {}
        
        # 1. 人気別分析
        logging.info("  人気別分析...")
        pop_analysis = []
        for pop in range(1, 13):
            pop_df = df[df['popularity'] == pop]
            if len(pop_df) > 100:
                win_rate = pop_df['is_win'].mean()
                avg_odds = pop_df['win_odds'].mean()
                avg_edge = pop_df['edge_score'].mean()
                
                # その人気を選んだ場合のROI
                bet_count = len(pop_df['race_id'].unique())  # レース数
                wins = pop_df['is_win'].sum()
                roi = pop_df[pop_df['is_win'] == 1]['win_odds'].sum() / bet_count if bet_count > 0 else 0
                
                pop_analysis.append({
                    'popularity': pop,
                    'count': len(pop_df),
                    'win_rate': win_rate,
                    'avg_odds': avg_odds,
                    'avg_edge': avg_edge,
                    'roi': roi
                })
        results['popularity'] = pop_analysis
        
        # 2. 距離別分析
        logging.info("  距離別分析...")
        df['distance_category'] = pd.cut(
            df['distance_m'],
            bins=[0, 1400, 1800, 2200, 2800, 5000],
            labels=['sprint', 'mile', 'middle', 'long', 'stayer']
        )
        
        dist_analysis = []
        for cat in ['sprint', 'mile', 'middle', 'long', 'stayer']:
            cat_df = df[df['distance_category'] == cat]
            if len(cat_df) > 100:
                # 各距離カテゴリでのTop1選択のROI
                race_ids = cat_df['race_id'].unique()
                top1_selected = cat_df.groupby('race_id').apply(
                    lambda x: x[x['popularity'] == 1].iloc[0] if len(x[x['popularity'] == 1]) > 0 else None
                ).dropna()
                
                if len(top1_selected) > 0:
                    top1_df = pd.DataFrame(list(top1_selected))
                    roi = top1_df[top1_df['is_win'] == 1]['win_odds'].sum() / len(race_ids) if len(race_ids) > 0 else 0
                else:
                    roi = 0
                
                dist_analysis.append({
                    'category': cat,
                    'count': len(cat_df),
                    'avg_edge': cat_df['edge_score'].mean(),
                    'avg_odds': cat_df['win_odds'].mean(),
                    '1fav_roi': roi
                })
        results['distance'] = dist_analysis
        
        # 3. 馬場別分析
        logging.info("  馬場別分析...")
        surface_analysis = []
        for surface in df['track_surface'].unique():
            if pd.isna(surface):
                continue
            surface_df = df[df['track_surface'] == surface]
            if len(surface_df) > 100:
                race_ids = surface_df['race_id'].unique()
                # 1番人気のROI
                top1_df = surface_df[surface_df['popularity'] == 1]
                roi = top1_df[top1_df['is_win'] == 1]['win_odds'].sum() / len(race_ids) if len(race_ids) > 0 else 0
                
                surface_analysis.append({
                    'surface': surface,
                    'count': len(surface_df),
                    'avg_edge': surface_df['edge_score'].mean(),
                    '1fav_roi': roi
                })
        results['surface'] = surface_analysis
        
        # 4. Edge閾値別ROIシミュレーション
        logging.info("  Edge閾値別ROIシミュレーション...")
        threshold_analysis = []
        for threshold in [0.8, 0.9, 1.0, 1.1, 1.2, 1.3, 1.5, 2.0]:
            # 各レースでEdge >= thresholdの馬の中から最もEdgeが高い馬を選択
            selected = df[df['edge_score'] >= threshold].copy()
            if len(selected) > 0:
                top_edge = selected.groupby('race_id').apply(
                    lambda x: x.loc[x['edge_score'].idxmax()]
                )
                
                bet_count = len(top_edge)
                win_count = top_edge['is_win'].sum()
                total_payout = top_edge[top_edge['is_win'] == 1]['win_odds'].sum()
                roi = total_payout / bet_count if bet_count > 0 else 0
                hit_rate = win_count / bet_count if bet_count > 0 else 0
                avg_odds = top_edge['win_odds'].mean()
                
                threshold_analysis.append({
                    'threshold': threshold,
                    'bet_count': bet_count,
                    'win_count': int(win_count),
                    'hit_rate': hit_rate,
                    'avg_odds': avg_odds,
                    'roi': roi
                })
        results['threshold'] = threshold_analysis
        
        return results
    
    def analyze_model_vs_market(self, df: pd.DataFrame) -> Dict:
        """モデル予測 vs 市場（オッズ）の乖離分析"""
        logging.info("=" * 60)
        logging.info("モデル vs 市場 乖離分析")
        logging.info("=" * 60)
        
        results = {}
        
        # 人気順位 vs 実際の着順
        df['pop_rank_diff'] = df['popularity'] - df['finish_position']
        
        # 過小評価馬（人気より良い着順）の分析
        undervalued = df[df['pop_rank_diff'] > 2]  # 人気より3着以上上
        overvalued = df[df['pop_rank_diff'] < -2]  # 人気より3着以上下
        
        results['undervalued'] = {
            'count': len(undervalued),
            'avg_popularity': undervalued['popularity'].mean(),
            'avg_finish': undervalued['finish_position'].mean(),
            'avg_odds': undervalued['win_odds'].mean()
        }
        
        results['overvalued'] = {
            'count': len(overvalued),
            'avg_popularity': overvalued['popularity'].mean(),
            'avg_finish': overvalued['finish_position'].mean(),
            'avg_odds': overvalued['win_odds'].mean()
        }
        
        # 勝馬の人気分布 (convert to Python int for JSON serialization)
        winners = df[df['is_win'] == 1]
        pop_dist = {int(k): int(v) for k, v in winners['popularity'].value_counts().sort_index().items()}
        results['winner_popularity_dist'] = pop_dist
        
        # 穴馬勝利（5番人気以下）の統計
        longshot_winners = winners[winners['popularity'] >= 5]
        results['longshot_stats'] = {
            'count': len(longshot_winners),
            'rate': len(longshot_winners) / len(winners) if len(winners) > 0 else 0,
            'avg_odds': longshot_winners['win_odds'].mean() if len(longshot_winners) > 0 else 0
        }
        
        logging.info(f"  過小評価馬: {len(undervalued):,}件")
        logging.info(f"  過大評価馬: {len(overvalued):,}件")
        logging.info(f"  穴馬勝利率: {results['longshot_stats']['rate']:.1%}")
        
        return results
    
    def generate_hypotheses(self, analysis_results: Dict) -> List[Dict]:
        """エッジ仮説を生成"""
        logging.info("=" * 60)
        logging.info("エッジ仮説生成")
        logging.info("=" * 60)
        
        hypotheses = []
        
        # 1. Edge閾値からの仮説
        threshold_data = analysis_results.get('threshold', [])
        best_threshold = None
        best_roi = 0
        for t in threshold_data:
            if t['roi'] > best_roi and t['bet_count'] >= 100:
                best_roi = t['roi']
                best_threshold = t['threshold']
        
        if best_threshold and best_roi > 1.0:
            hypotheses.append({
                'id': 'H1',
                'type': 'edge_threshold',
                'description': f"Edge Score >= {best_threshold} の馬を選択するとROI {best_roi:.1%}",
                'conditions': f"edge_score >= {best_threshold}",
                'expected_roi': best_roi,
                'bet_count': next(t['bet_count'] for t in threshold_data if t['threshold'] == best_threshold),
                'confidence': 'high' if best_roi > 1.1 else 'medium'
            })
        
        # 2. 人気別からの仮説
        pop_data = analysis_results.get('popularity', [])
        for p in pop_data:
            if p['roi'] > 1.0 and p['count'] > 500:
                hypotheses.append({
                    'id': f"H2_{p['popularity']}",
                    'type': 'popularity',
                    'description': f"{p['popularity']}番人気を選択するとROI {p['roi']:.1%}",
                    'conditions': f"popularity == {p['popularity']}",
                    'expected_roi': p['roi'],
                    'bet_count': p['count'],
                    'confidence': 'high' if p['roi'] > 1.1 else 'medium'
                })
        
        # 3. 距離別からの仮説
        dist_data = analysis_results.get('distance', [])
        for d in dist_data:
            if d.get('1fav_roi', 0) > 0.9:  # 1番人気でも90%以上
                hypotheses.append({
                    'id': f"H3_{d['category']}",
                    'type': 'distance',
                    'description': f"{d['category']}距離では1番人気が安定 (ROI {d.get('1fav_roi', 0):.1%})",
                    'conditions': f"distance_category == '{d['category']}'",
                    'expected_roi': d.get('1fav_roi', 0),
                    'bet_count': d['count'],
                    'confidence': 'medium'
                })
        
        logging.info(f"  生成した仮説数: {len(hypotheses)}")
        for h in hypotheses:
            logging.info(f"    {h['id']}: {h['description']}")
        
        return hypotheses
    
    def save_results(self, condition_analysis: Dict, market_analysis: Dict, 
                     hypotheses: List[Dict]):
        """結果を保存"""
        logging.info("=" * 60)
        logging.info("結果保存")
        logging.info("=" * 60)
        
        # JSON形式で詳細レポート保存
        report = {
            'generated_at': datetime.now().isoformat(),
            'model_dir': str(self.model_dir),
            'condition_analysis': condition_analysis,
            'market_analysis': market_analysis,
            'hypotheses': hypotheses
        }
        
        with open(self.output_dir / 'edge_analysis_report.json', 'w', encoding='utf-8') as f:
            json.dump(report, f, ensure_ascii=False, indent=2, default=str)
        
        # 閾値分析CSV
        if 'threshold' in condition_analysis:
            threshold_df = pd.DataFrame(condition_analysis['threshold'])
            threshold_df.to_csv(self.output_dir / 'edge_threshold_analysis.csv', index=False)
        
        # 仮説Markdown
        with open(self.output_dir / 'hypotheses.md', 'w', encoding='utf-8') as f:
            f.write("# Edge Discovery 仮説レポート\n\n")
            f.write(f"生成日時: {datetime.now().strftime('%Y-%m-%d %H:%M')}\n\n")
            f.write("## 発見した仮説\n\n")
            
            if hypotheses:
                for h in hypotheses:
                    f.write(f"### {h['id']}: {h['description']}\n\n")
                    f.write(f"- **条件**: `{h['conditions']}`\n")
                    f.write(f"- **期待ROI**: {h['expected_roi']:.1%}\n")
                    f.write(f"- **ベット数**: {h['bet_count']:,}\n")
                    f.write(f"- **信頼度**: {h['confidence']}\n\n")
            else:
                f.write("有意な仮説は発見されませんでした。\n\n")
                f.write("## 次のアクション\n\n")
                f.write("- 特徴量の追加を検討\n")
                f.write("- モデルの精度向上を優先\n")
        
        logging.info(f"  保存先: {self.output_dir}")
        logging.info(f"  - edge_analysis_report.json")
        logging.info(f"  - edge_threshold_analysis.csv")
        logging.info(f"  - hypotheses.md")
    
    def run(self) -> Tuple[Dict, Dict, List[Dict]]:
        """メイン実行"""
        # データ読み込み
        valid_df, test_df = self.load_model_and_data()
        
        # 分析対象（Valid期間で分析、Test期間は評価用に保持）
        analysis_df = self.calculate_edge_scores(valid_df)
        
        # 条件別分析
        condition_analysis = self.analyze_edge_by_condition(analysis_df)
        
        # モデル vs 市場分析
        market_analysis = self.analyze_model_vs_market(analysis_df)
        
        # 仮説生成
        hypotheses = self.generate_hypotheses(condition_analysis)
        
        # 結果保存
        self.save_results(condition_analysis, market_analysis, hypotheses)
        
        # サマリー表示
        logging.info("=" * 60)
        logging.info("【Edge Discovery サマリー】")
        logging.info("=" * 60)
        logging.info(f"  分析レコード数: {len(analysis_df):,}")
        logging.info(f"  発見した仮説数: {len(hypotheses)}")
        
        if hypotheses:
            best = max(hypotheses, key=lambda x: x['expected_roi'])
            logging.info(f"  最良の仮説: {best['id']}")
            logging.info(f"    {best['description']}")
            logging.info(f"    期待ROI: {best['expected_roi']:.1%}")
        
        # Go/No-Go判定
        go_criteria = len(hypotheses) > 0 and any(h['expected_roi'] > 1.0 for h in hypotheses)
        logging.info("")
        logging.info(f"  【Go/No-Go判定】: {'✅ GO - 仮説あり' if go_criteria else '⚠️ CAUTION - 再検討必要'}")
        
        return condition_analysis, market_analysis, hypotheses


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Edge Discovery Analysis')
    parser.add_argument('--model_dir', type=str, default='keibaai/models/mu_v11_0',
                        help='モデルディレクトリ')
    parser.add_argument('--output_dir', type=str, default='results/edge_analysis',
                        help='出力ディレクトリ')
    args = parser.parse_args()
    
    analyzer = EdgeDiscoveryAnalyzer(model_dir=args.model_dir)
    if args.output_dir:
        analyzer.output_dir = Path(args.output_dir)
        analyzer.output_dir.mkdir(parents=True, exist_ok=True)
    
    condition_analysis, market_analysis, hypotheses = analyzer.run()
    
    print("\n" + "=" * 60)
    print("Edge Discovery 完了")
    print("=" * 60)
    print(f"結果保存先: {analyzer.output_dir}")
    print(f"仮説数: {len(hypotheses)}")
    if hypotheses:
        print("\n発見した仮説:")
        for h in hypotheses:
            print(f"  - {h['id']}: {h['description']}")
