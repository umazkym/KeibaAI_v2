#!/usr/bin/env python3
"""
ペース適性分析 - 市場が見落としている情報を発見する

【計画書の核心】
「なぜこのモデルは市場より賢いのか？」
→ v11.0: 市場と同じ「過去着順」を見ているだけ
→ 必要: 市場が見落としている「ペース適性」を発見

【race_details.parquetの活用】
- first_half: 前半3Fタイム
- second_half: 後半3Fタイム
- ペース差 = first_half - second_half
  → 正: ハイペース（前傾）
  → 負: スローペース（後傾）

【仮説】
- 馬によって「得意なペース」がある
- ハイペースで好走する馬 vs スローペースで好走する馬
- 当日のペース予測と馬のペース適性の相性がエッジになる
"""

import sys
from pathlib import Path

project_root = Path(__file__).resolve().parent.parent.parent
sys.path.append(str(project_root))

import pandas as pd
import numpy as np
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class PaceAptitudeAnalyzer:
    """ペース適性分析"""
    
    def __init__(self):
        self.output_dir = Path('results/pace_aptitude')
        self.output_dir.mkdir(parents=True, exist_ok=True)
    
    def load_data(self):
        """データ読み込み"""
        logging.info("=" * 60)
        logging.info("データ読み込み")
        logging.info("=" * 60)
        
        races_df = pd.read_parquet('keibaai/data/parsed/parquet/races/races.parquet')
        races_df['race_date'] = pd.to_datetime(races_df['race_date'])
        races_df['race_id'] = races_df['race_id'].astype(str)
        races_df['horse_id'] = races_df['horse_id'].astype(str)
        races_df['finish_position'] = pd.to_numeric(races_df['finish_position'], errors='coerce')
        races_df['win_odds'] = pd.to_numeric(races_df['win_odds'], errors='coerce')
        
        race_details = pd.read_parquet('keibaai/data/parsed/parquet/race_details/race_details.parquet')
        race_details['race_id'] = race_details['race_id'].astype(str)
        
        logging.info(f"  レース結果: {len(races_df):,}件")
        logging.info(f"  ペースデータ: {len(race_details):,}件")
        
        return races_df, race_details
    
    def calculate_pace_features(self, races_df, race_details):
        """ペース特徴量を計算"""
        logging.info("\nペース特徴量計算中...")
        
        # レースごとにペース情報をマージ
        df = races_df.merge(
            race_details[['race_id', 'first_half', 'second_half']],
            on='race_id',
            how='left'
        )
        
        # ペース差（ハイ/スロー指標）
        df['pace_diff'] = df['first_half'] - df['second_half']
        # 正: ハイペース（前傾）、負: スローペース（後傾）
        
        # ペース分類
        df['pace_type'] = pd.cut(
            df['pace_diff'],
            bins=[-np.inf, -1.5, 1.5, np.inf],
            labels=['slow', 'medium', 'fast']
        )
        
        logging.info(f"  ペース情報付きレコード: {df['pace_diff'].notna().sum():,}件")
        
        # ペースタイプ分布
        pace_dist = df['pace_type'].value_counts(dropna=False)
        logging.info(f"  スロー: {pace_dist.get('slow', 0):,}件")
        logging.info(f"  ミディアム: {pace_dist.get('medium', 0):,}件")
        logging.info(f"  ファスト: {pace_dist.get('fast', 0):,}件")
        
        return df
    
    def analyze_pace_aptitude(self, df):
        """馬ごとのペース適性を分析"""
        logging.info("\n馬ごとのペース適性分析...")
        
        # 馬ごと、ペースタイプごとの成績
        aptitude_stats = df.groupby(['horse_id', 'pace_type']).agg({
            'finish_position': ['mean', 'count'],
            'race_id': 'count'
        }).reset_index()
        aptitude_stats.columns = ['horse_id', 'pace_type', 'avg_finish', 'n_races', 'count']
        
        # 最低3レース以上のデータがある馬を分析
        aptitude_stats = aptitude_stats[aptitude_stats['n_races'] >= 3]
        
        logging.info(f"  分析対象馬: {aptitude_stats['horse_id'].nunique():,}頭")
        
        # ペースタイプ別の平均着順
        for pace_type in ['slow', 'medium', 'fast']:
            subset = aptitude_stats[aptitude_stats['pace_type'] == pace_type]
            if len(subset) > 0:
                avg = subset['avg_finish'].mean()
                logging.info(f"  {pace_type}ペース平均着順: {avg:.2f}")
        
        return aptitude_stats
    
    def analyze_edge_by_pace(self, df):
        """ペース適性によるエッジを分析"""
        logging.info("\n" + "=" * 60)
        logging.info("【ペース適性 × ROI分析】")
        logging.info("=" * 60)
        
        # Train期間でペース適性を計算
        train_df = df[df['race_date'] < '2023-01-01'].copy()
        valid_df = df[(df['race_date'] >= '2023-01-01') & (df['race_date'] < '2024-01-01')].copy()
        test_df = df[df['race_date'] >= '2024-01-01'].copy()
        
        # 馬ごとのペースタイプ別着順平均を計算
        horse_pace_stats = train_df.groupby(['horse_id', 'pace_type'])['finish_position'].mean().reset_index()
        horse_pace_stats.columns = ['horse_id', 'pace_type', 'pace_avg_finish']
        
        # ピボットして馬ごとの得意ペースを特定
        pivot = horse_pace_stats.pivot(index='horse_id', columns='pace_type', values='pace_avg_finish')
        pivot.columns = [f'{c}_avg_finish' for c in pivot.columns]
        pivot = pivot.reset_index()
        
        # 得意ペースを判定
        def get_best_pace(row):
            cols = ['slow_avg_finish', 'medium_avg_finish', 'fast_avg_finish']
            valid_cols = [c for c in cols if pd.notna(row.get(c))]
            if not valid_cols:
                return 'unknown'
            best = min(valid_cols, key=lambda c: row.get(c, np.inf))
            return best.replace('_avg_finish', '')
        
        pivot['best_pace'] = pivot.apply(get_best_pace, axis=1)
        
        # Valid/Test期間にマージ
        for period_name, period_df in [('Valid', valid_df), ('Test', test_df)]:
            logging.info(f"\n【{period_name}期間】")
            
            merged = period_df.merge(pivot[['horse_id', 'best_pace']], on='horse_id', how='left')
            merged = merged[merged['best_pace'] != 'unknown']
            
            # ペース予測が当たった場合（馬の得意ペースと実際のペースが一致）
            merged['pace_match'] = merged['best_pace'] == merged['pace_type'].astype(str)
            
            # pace_matchの場合のROI
            for match_status, label in [(True, 'ペース一致'), (False, 'ペース不一致')]:
                subset = merged[(merged['pace_match'] == match_status) & (merged['finish_position'] == 1)]
                all_bets = merged[merged['pace_match'] == match_status]
                
                if len(all_bets) > 100:
                    payout = subset['win_odds'].sum() * 100
                    roi = payout / (len(all_bets) * 100)
                    hit_rate = len(subset) / len(all_bets)
                    
                    logging.info(f"  {label}: ベット{len(all_bets):6,} | 的中率{hit_rate:5.1%} | ROI{roi:6.1%}")
        
        return pivot
    
    def analyze_pace_prediction_accuracy(self, df):
        """展開予測の精度を分析"""
        logging.info("\n" + "=" * 60)
        logging.info("【展開予測精度分析】")
        logging.info("=" * 60)
        
        # 先行馬の数からペースを予測できるか？
        # corner_positionsから脚質を取得
        try:
            corner_df = pd.read_parquet('keibaai/data/parsed/parquet/corners/corner_positions.parquet')
            corner_df['race_id'] = corner_df['race_id'].astype(str)
            corner_df['horse_number'] = pd.to_numeric(corner_df['horse_number'], errors='coerce')
            
            # 4コーナー通過順位で脚質判定
            corner_4 = corner_df[corner_df['corner'] == 4].copy()
            
            # レースごとの先行馬数（通過順位3以内）
            leader_counts = corner_4.groupby('race_id').apply(
                lambda x: (x['position'] <= 3).sum()
            ).reset_index()
            leader_counts.columns = ['race_id', 'n_leaders']
            
            # ペースデータとマージ
            pace_df = df[['race_id', 'pace_type']].drop_duplicates()
            merged = pace_df.merge(leader_counts, on='race_id', how='inner')
            
            # 先行馬数とペースの関係
            logging.info("\n【先行馬数 × 実際のペース】")
            for n_lead in [1, 2, 3, 4, 5]:
                subset = merged[merged['n_leaders'] == n_lead]
                if len(subset) > 100:
                    pace_dist = subset['pace_type'].value_counts(normalize=True)
                    fast_pct = pace_dist.get('fast', 0)
                    slow_pct = pace_dist.get('slow', 0)
                    logging.info(f"  先行馬{n_lead}頭: ファスト{fast_pct:5.1%} | スロー{slow_pct:5.1%}")
            
        except Exception as e:
            logging.warning(f"  コーナーデータ分析失敗: {e}")
    
    def identify_edge_hypothesis(self, df):
        """エッジ仮説を立案"""
        logging.info("\n" + "=" * 60)
        logging.info("【エッジ仮説】")
        logging.info("=" * 60)
        
        # ペース適性がない馬（デビュー戦等）を除く
        train_df = df[df['race_date'] < '2023-01-01'].copy()
        
        # 馬ごとのペースタイプ別成績を計算
        horse_stats = train_df.groupby('horse_id').apply(
            lambda x: pd.Series({
                'fast_win_rate': (x[x['pace_type'] == 'fast']['finish_position'] == 1).mean() if len(x[x['pace_type'] == 'fast']) > 0 else np.nan,
                'slow_win_rate': (x[x['pace_type'] == 'slow']['finish_position'] == 1).mean() if len(x[x['pace_type'] == 'slow']) > 0 else np.nan,
                'n_fast': len(x[x['pace_type'] == 'fast']),
                'n_slow': len(x[x['pace_type'] == 'slow']),
            })
        ).reset_index()
        
        # ペースによる勝率差が大きい馬を特定
        horse_stats['pace_gap'] = horse_stats['fast_win_rate'] - horse_stats['slow_win_rate']
        
        # 差が大きい馬の特徴
        high_gap = horse_stats[(horse_stats['n_fast'] >= 3) & (horse_stats['n_slow'] >= 3) & (horse_stats['pace_gap'].abs() > 0.1)]
        
        logging.info(f"  ペース差が大きい馬: {len(high_gap):,}頭")
        
        if len(high_gap) > 0:
            avg_gap = high_gap['pace_gap'].abs().mean()
            logging.info(f"  平均勝率差: {avg_gap:.1%}")
            
            # ファスト得意 vs スロー得意
            fast_specialists = high_gap[high_gap['pace_gap'] > 0.1]
            slow_specialists = high_gap[high_gap['pace_gap'] < -0.1]
            logging.info(f"  ファスト得意馬: {len(fast_specialists):,}頭")
            logging.info(f"  スロー得意馬: {len(slow_specialists):,}頭")
            
            logging.info("\n【検証すべき仮説】")
            logging.info("  仮説1: ファスト得意馬がハイペースレースに出る時、市場は過小評価している")
            logging.info("  仮説2: スロー得意馬がスローペースレースに出る時、市場は過小評価している")
            logging.info("  仮説3: 展開予測（先行馬数→ペース）と馬のペース適性の組み合わせでエッジが生まれる")
    
    def run(self):
        """メイン実行"""
        races_df, race_details = self.load_data()
        
        df = self.calculate_pace_features(races_df, race_details)
        
        self.analyze_pace_aptitude(df)
        
        pivot = self.analyze_edge_by_pace(df)
        
        self.analyze_pace_prediction_accuracy(df)
        
        self.identify_edge_hypothesis(df)
        
        logging.info("\n" + "=" * 60)
        logging.info("【次のステップ】")
        logging.info("=" * 60)
        logging.info("  1. pace_aptitude_score特徴量を作成してv12.0に追加")
        logging.info("  2. 展開予測（n_leaders → pace_type）をモデルに組み込み")
        logging.info("  3. pace_match（馬のペース適性 × 予測ペース）でフィルタリング")
        
        return df, pivot


if __name__ == "__main__":
    analyzer = PaceAptitudeAnalyzer()
    df, pivot = analyzer.run()
