#!/usr/bin/env python3
"""
μモデル v3.5 詳細分析スクリプト

【分析項目】
1. オッズ帯別の的中率とROI
2. 人気別の的中率とROI
3. 月別・季節別の傾向
4. 競馬場別の傾向
5. クラス別の傾向
6. 距離別の傾向
7. 馬場別の傾向
8. 配当額の分布

【日本語】
すべてのログ、コメントは日本語で記述。
"""

import sys
from pathlib import Path

project_root = Path(__file__).resolve().parent.parent.parent
sys.path.append(str(project_root))

import pandas as pd
import numpy as np
import pickle
import json
import logging
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)


class ModelPerformanceAnalyzer:
    """モデル性能詳細分析クラス"""
    
    def __init__(self):
        self.models_dir = Path('keibaai/models')
        self.mu_model = None
        self.mu_features = None
        self.results = {}
    
    def load_model_and_data(self):
        """モデルとデータを読み込み"""
        logging.info("モデルとデータを読み込み中...")
        
        # v3.5モデル
        with open(self.models_dir / 'mu_v3_5/mu_v3_5_ranker.pkl', 'rb') as f:
            self.mu_model = pickle.load(f)
        with open(self.models_dir / 'mu_v3_5/feature_names.json', 'r', encoding='utf-8') as f:
            self.mu_features = json.load(f)
        
        # データ
        df = pd.read_parquet(self.models_dir / 'mu_v3_3/train_data_mu_v3_3.parquet')
        df['race_date'] = pd.to_datetime(df['race_date'])
        
        logging.info(f"データ読み込み完了: {len(df):,} rows")
        return df
    
    def prepare_predictions(self, df: pd.DataFrame) -> pd.DataFrame:
        """予測を準備"""
        logging.info("予測を実行中...")
        
        # Gap Features
        if 'past_5_finish_position_mean' in df.columns and 'popularity' in df.columns:
            df['ability_rank'] = df.groupby('race_id')['past_5_finish_position_mean'].rank(ascending=True, method='first')
            df['gap_ability_popularity'] = df['popularity'] - df['ability_rank']
        
        # 欠損特徴量を埋める
        for col in self.mu_features:
            if col not in df.columns:
                df[col] = 0
        
        # 予測
        df['mu_pred'] = self.mu_model.predict(df[self.mu_features])
        df['rank_pred'] = df.groupby('race_id')['mu_pred'].rank(ascending=False, method='first')
        
        # Test期間のみ
        df = df[df['race_date'] >= '2024-01-01'].copy()
        
        logging.info(f"Test期間データ: {len(df):,} rows, {df['race_id'].nunique():,} レース")
        return df
    
    def analyze_by_odds_range(self, df: pd.DataFrame):
        """オッズ帯別分析"""
        logging.info("\n" + "=" * 60)
        logging.info("【オッズ帯別分析】")
        logging.info("=" * 60)
        
        top1 = df[df['rank_pred'] == 1].copy()
        
        # オッズ帯を定義
        odds_bins = [0, 2, 3, 5, 10, 20, 50, 100, 1000]
        odds_labels = ['~2倍', '2-3倍', '3-5倍', '5-10倍', '10-20倍', '20-50倍', '50-100倍', '100倍~']
        top1['odds_range'] = pd.cut(top1['win_odds'], bins=odds_bins, labels=odds_labels, right=False)
        
        results = []
        for odds_range in odds_labels:
            subset = top1[top1['odds_range'] == odds_range]
            if len(subset) > 0:
                hits = subset[subset['finish_position'] == 1]
                hit_rate = len(hits) / len(subset)
                roi = hits['win_odds'].sum() / len(subset) if len(subset) > 0 else 0
                avg_odds = subset['win_odds'].mean()
                results.append({
                    'オッズ帯': odds_range,
                    'ベット数': len(subset),
                    '的中数': len(hits),
                    '的中率': f"{hit_rate:.1%}",
                    '平均オッズ': f"{avg_odds:.1f}",
                    'ROI': f"{roi:.1%}"
                })
        
        result_df = pd.DataFrame(results)
        print(result_df.to_string(index=False))
        self.results['odds_range'] = results
        
        return result_df
    
    def analyze_by_popularity(self, df: pd.DataFrame):
        """人気別分析"""
        logging.info("\n" + "=" * 60)
        logging.info("【予測Top1馬の人気別分析】")
        logging.info("=" * 60)
        
        top1 = df[df['rank_pred'] == 1].copy()
        
        results = []
        for pop in range(1, 11):
            subset = top1[top1['popularity'] == pop]
            if len(subset) > 0:
                hits = subset[subset['finish_position'] == 1]
                hit_rate = len(hits) / len(subset)
                roi = hits['win_odds'].sum() / len(subset) if len(subset) > 0 else 0
                avg_odds = subset['win_odds'].mean()
                results.append({
                    '人気': f"{pop}番人気",
                    'ベット数': len(subset),
                    '的中数': len(hits),
                    '的中率': f"{hit_rate:.1%}",
                    '平均オッズ': f"{avg_odds:.1f}",
                    'ROI': f"{roi:.1%}"
                })
        
        # 11番人気以下をまとめる
        subset = top1[top1['popularity'] >= 11]
        if len(subset) > 0:
            hits = subset[subset['finish_position'] == 1]
            hit_rate = len(hits) / len(subset)
            roi = hits['win_odds'].sum() / len(subset) if len(subset) > 0 else 0
            avg_odds = subset['win_odds'].mean()
            results.append({
                '人気': "11番人気~",
                'ベット数': len(subset),
                '的中数': len(hits),
                '的中率': f"{hit_rate:.1%}",
                '平均オッズ': f"{avg_odds:.1f}",
                'ROI': f"{roi:.1%}"
            })
        
        result_df = pd.DataFrame(results)
        print(result_df.to_string(index=False))
        self.results['popularity'] = results
        
        return result_df
    
    def analyze_by_month(self, df: pd.DataFrame):
        """月別分析"""
        logging.info("\n" + "=" * 60)
        logging.info("【月別分析】")
        logging.info("=" * 60)
        
        top1 = df[df['rank_pred'] == 1].copy()
        top1['month'] = top1['race_date'].dt.month
        
        results = []
        for month in sorted(top1['month'].unique()):
            subset = top1[top1['month'] == month]
            if len(subset) > 0:
                hits = subset[subset['finish_position'] == 1]
                hit_rate = len(hits) / len(subset)
                roi = hits['win_odds'].sum() / len(subset) if len(subset) > 0 else 0
                results.append({
                    '月': f"{month}月",
                    'ベット数': len(subset),
                    '的中数': len(hits),
                    '的中率': f"{hit_rate:.1%}",
                    'ROI': f"{roi:.1%}"
                })
        
        result_df = pd.DataFrame(results)
        print(result_df.to_string(index=False))
        self.results['month'] = results
        
        return result_df
    
    def analyze_by_venue(self, df: pd.DataFrame):
        """競馬場別分析"""
        logging.info("\n" + "=" * 60)
        logging.info("【競馬場別分析】")
        logging.info("=" * 60)
        
        top1 = df[df['rank_pred'] == 1].copy()
        
        results = []
        for venue in top1['venue'].dropna().unique():
            subset = top1[top1['venue'] == venue]
            if len(subset) >= 50:  # サンプル数が少ない場合は除外
                hits = subset[subset['finish_position'] == 1]
                hit_rate = len(hits) / len(subset)
                roi = hits['win_odds'].sum() / len(subset) if len(subset) > 0 else 0
                results.append({
                    '競馬場': venue,
                    'ベット数': len(subset),
                    '的中数': len(hits),
                    '的中率': f"{hit_rate:.1%}",
                    'ROI': f"{roi:.1%}"
                })
        
        result_df = pd.DataFrame(results)
        result_df = result_df.sort_values('ROI', ascending=False)
        print(result_df.to_string(index=False))
        self.results['venue'] = results
        
        return result_df
    
    def analyze_by_distance(self, df: pd.DataFrame):
        """距離別分析"""
        logging.info("\n" + "=" * 60)
        logging.info("【距離別分析】")
        logging.info("=" * 60)
        
        top1 = df[df['rank_pred'] == 1].copy()
        
        # 距離カテゴリ
        bins = [0, 1400, 1800, 2200, 2800, 10000]
        labels = ['短距離(~1400)', 'マイル(1400-1800)', '中距離(1800-2200)', '中長距離(2200-2800)', '長距離(2800~)']
        top1['distance_cat'] = pd.cut(top1['distance_m'], bins=bins, labels=labels, right=False)
        
        results = []
        for dist in labels:
            subset = top1[top1['distance_cat'] == dist]
            if len(subset) > 0:
                hits = subset[subset['finish_position'] == 1]
                hit_rate = len(hits) / len(subset)
                roi = hits['win_odds'].sum() / len(subset) if len(subset) > 0 else 0
                results.append({
                    '距離': dist,
                    'ベット数': len(subset),
                    '的中数': len(hits),
                    '的中率': f"{hit_rate:.1%}",
                    'ROI': f"{roi:.1%}"
                })
        
        result_df = pd.DataFrame(results)
        print(result_df.to_string(index=False))
        self.results['distance'] = results
        
        return result_df
    
    def analyze_by_track_surface(self, df: pd.DataFrame):
        """馬場別分析"""
        logging.info("\n" + "=" * 60)
        logging.info("【馬場別分析】")
        logging.info("=" * 60)
        
        top1 = df[df['rank_pred'] == 1].copy()
        
        results = []
        for surface in ['芝', 'ダート']:
            subset = top1[top1['track_surface'] == surface]
            if len(subset) > 0:
                hits = subset[subset['finish_position'] == 1]
                hit_rate = len(hits) / len(subset)
                roi = hits['win_odds'].sum() / len(subset) if len(subset) > 0 else 0
                results.append({
                    '馬場': surface,
                    'ベット数': len(subset),
                    '的中数': len(hits),
                    '的中率': f"{hit_rate:.1%}",
                    'ROI': f"{roi:.1%}"
                })
        
        result_df = pd.DataFrame(results)
        print(result_df.to_string(index=False))
        self.results['track_surface'] = results
        
        return result_df
    
    def analyze_by_track_condition(self, df: pd.DataFrame):
        """馬場状態別分析"""
        logging.info("\n" + "=" * 60)
        logging.info("【馬場状態別分析】")
        logging.info("=" * 60)
        
        top1 = df[df['rank_pred'] == 1].copy()
        
        results = []
        for condition in ['良', '稍重', '重', '不良']:
            subset = top1[top1['track_condition'] == condition]
            if len(subset) > 0:
                hits = subset[subset['finish_position'] == 1]
                hit_rate = len(hits) / len(subset)
                roi = hits['win_odds'].sum() / len(subset) if len(subset) > 0 else 0
                results.append({
                    '馬場状態': condition,
                    'ベット数': len(subset),
                    '的中数': len(hits),
                    '的中率': f"{hit_rate:.1%}",
                    'ROI': f"{roi:.1%}"
                })
        
        result_df = pd.DataFrame(results)
        print(result_df.to_string(index=False))
        self.results['track_condition'] = results
        
        return result_df
    
    def analyze_winning_pattern(self, df: pd.DataFrame):
        """的中パターン分析"""
        logging.info("\n" + "=" * 60)
        logging.info("【的中時の配当分布】")
        logging.info("=" * 60)
        
        top1 = df[df['rank_pred'] == 1].copy()
        hits = top1[top1['finish_position'] == 1].copy()
        
        # 配当の統計
        odds = hits['win_odds']
        logging.info(f"的中数: {len(hits)}")
        logging.info(f"平均配当: {odds.mean():.1f}倍")
        logging.info(f"中央値配当: {odds.median():.1f}倍")
        logging.info(f"最小配当: {odds.min():.1f}倍")
        logging.info(f"最大配当: {odds.max():.1f}倍")
        logging.info(f"標準偏差: {odds.std():.1f}")
        
        # 配当分布
        logging.info("\n【配当分布】")
        for upper in [2, 3, 5, 10, 20, 50, 100]:
            count = len(odds[odds < upper]) if upper == 2 else len(odds[(odds >= upper/2) & (odds < upper)])
            pct = count / len(odds) * 100
            logging.info(f"  {upper}倍未満: {count}件 ({pct:.1f}%)")
        
        self.results['winning_stats'] = {
            'count': len(hits),
            'mean_odds': odds.mean(),
            'median_odds': odds.median(),
            'min_odds': odds.min(),
            'max_odds': odds.max(),
            'std_odds': odds.std()
        }
        
        return hits
    
    def analyze_losing_pattern(self, df: pd.DataFrame):
        """外れパターン分析"""
        logging.info("\n" + "=" * 60)
        logging.info("【外れ時の分析 - Top1予測馬の実際の着順】")
        logging.info("=" * 60)
        
        top1 = df[df['rank_pred'] == 1].copy()
        misses = top1[top1['finish_position'] != 1].copy()
        
        # 着順分布
        logging.info(f"外れ数: {len(misses)}")
        
        finish_dist = misses['finish_position'].value_counts().sort_index()
        logging.info("\n【着順分布（外れ時）】")
        for pos in range(2, 11):
            if pos in finish_dist.index:
                count = finish_dist[pos]
                pct = count / len(misses) * 100
                logging.info(f"  {pos}着: {count}件 ({pct:.1f}%)")
        
        # 2-3着の率（惜しい外れ）
        close_miss = len(misses[misses['finish_position'].isin([2, 3])])
        logging.info(f"\n2-3着（惜しい外れ）: {close_miss}件 ({close_miss/len(misses)*100:.1f}%)")
        
        # 掲示板外の率
        out_of_board = len(misses[misses['finish_position'] > 5])
        logging.info(f"6着以下: {out_of_board}件 ({out_of_board/len(misses)*100:.1f}%)")
        
        self.results['miss_pattern'] = {
            'count': len(misses),
            'close_miss_rate': close_miss / len(misses),
            'out_of_board_rate': out_of_board / len(misses)
        }
        
        return misses
    
    def run_all_analysis(self):
        """全分析を実行"""
        df = self.load_model_and_data()
        df = self.prepare_predictions(df)
        
        logging.info("\n" + "=" * 80)
        logging.info("μモデル v3.5 詳細分析レポート")
        logging.info("=" * 80)
        
        # 各分析を実行（存在するカラムのみ）
        self.analyze_by_odds_range(df)
        self.analyze_by_popularity(df)
        self.analyze_by_month(df)
        
        # venue, distance_m, track_surface, track_condition があれば分析
        if 'venue' in df.columns:
            self.analyze_by_venue(df)
        if 'distance_m' in df.columns:
            self.analyze_by_distance(df)
        if 'track_surface' in df.columns:
            self.analyze_by_track_surface(df)
        if 'track_condition' in df.columns:
            self.analyze_by_track_condition(df)
        
        self.analyze_winning_pattern(df)
        self.analyze_losing_pattern(df)
        
        # 結果を保存
        with open('keibaai/models/mu_v3_5_analysis.json', 'w', encoding='utf-8') as f:
            # numpy型をPython型に変換
            def convert_to_serializable(obj):
                if isinstance(obj, (np.int64, np.int32)):
                    return int(obj)
                elif isinstance(obj, (np.float64, np.float32)):
                    return float(obj)
                elif isinstance(obj, np.ndarray):
                    return obj.tolist()
                return obj
            
            serializable_results = {}
            for k, v in self.results.items():
                if isinstance(v, list):
                    serializable_results[k] = v
                elif isinstance(v, dict):
                    serializable_results[k] = {kk: convert_to_serializable(vv) for kk, vv in v.items()}
                else:
                    serializable_results[k] = convert_to_serializable(v)
            
            json.dump(serializable_results, f, ensure_ascii=False, indent=2)
        
        logging.info("\n分析結果を保存: keibaai/models/mu_v3_5_analysis.json")
        
        return self.results


if __name__ == "__main__":
    analyzer = ModelPerformanceAnalyzer()
    analyzer.run_all_analysis()
