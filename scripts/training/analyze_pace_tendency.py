#!/usr/bin/env python3
"""
ペース傾向特徴量の開発とROI検証

【新エッジ仮説】
race_details.parquetのfirst_half/second_halfを使用して：
1. コース×距離別のペース傾向を計算
2. 馬の「得意ペース」を過去走から計算
3. 当日のコースペース傾向と馬の適性のマッチング

【期待効果】
- コースのペース傾向と馬の脚質がマッチする場合にROI向上
- 市場は「過去着順」を見るが、モデルは「ペース適性」を見る
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

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class PaceTendencyAnalyzer:
    """ペース傾向特徴量分析"""
    
    def __init__(self):
        self.output_dir = Path('results/pace_tendency')
        self.output_dir.mkdir(parents=True, exist_ok=True)
    
    def load_data(self):
        logging.info("=" * 60)
        logging.info("データ読み込み")
        logging.info("=" * 60)
        
        # race_details（ペースデータ）
        race_details = pd.read_parquet('keibaai/data/parsed/parquet/race_details/race_details.parquet')
        race_details['race_id'] = race_details['race_id'].astype(str)
        
        # races（コース・条件）
        races_df = pd.read_parquet('keibaai/data/parsed/parquet/races/races.parquet')
        races_df['race_date'] = pd.to_datetime(races_df['race_date'])
        races_df['race_id'] = races_df['race_id'].astype(str)
        races_df['horse_id'] = races_df['horse_id'].astype(str)
        races_df['horse_number'] = pd.to_numeric(races_df['horse_number'], errors='coerce')
        races_df['finish_position'] = pd.to_numeric(races_df['finish_position'], errors='coerce')
        races_df['win_odds'] = pd.to_numeric(races_df['win_odds'], errors='coerce')
        
        # corner_positions（脚質判定用）
        corners = pd.read_parquet('keibaai/data/parsed/parquet/corners/corner_positions.parquet')
        corners['race_id'] = corners['race_id'].astype(str)
        
        # returns（ROI計算用）
        returns_df = pd.read_parquet('keibaai/data/parsed/parquet/returns/returns.parquet')
        returns_df['race_id'] = returns_df['race_id'].astype(str)
        
        logging.info(f"  race_details: {len(race_details):,}件")
        logging.info(f"  races: {len(races_df):,}件")
        logging.info(f"  corners: {len(corners):,}件")
        
        return race_details, races_df, corners, returns_df
    
    def calculate_pace_diff(self, race_details):
        """ペース差を計算"""
        race_details['pace_diff'] = race_details['first_half'] - race_details['second_half']
        race_details['is_fast_pace'] = race_details['pace_diff'] > 0
        race_details['is_slow_pace'] = race_details['pace_diff'] < -1.0
        
        logging.info(f"\n【ペース分布】")
        logging.info(f"  ハイペース (pace_diff > 0): {race_details['is_fast_pace'].sum():,}件 ({race_details['is_fast_pace'].mean():.1%})")
        logging.info(f"  スローペース (pace_diff < -1.0): {race_details['is_slow_pace'].sum():,}件 ({race_details['is_slow_pace'].mean():.1%})")
        
        return race_details
    
    def calculate_venue_pace_tendency(self, race_details, races_df):
        """コース×距離別のペース傾向を計算"""
        logging.info("\n【コース×距離別ペース傾向】")
        
        # race_idでレース条件をマージ
        race_info = races_df[['race_id', 'venue', 'distance_m', 'track_surface']].drop_duplicates(subset='race_id')
        rd_with_info = race_details.merge(race_info, on='race_id', how='left')
        
        # 距離カテゴリ
        rd_with_info['dist_cat'] = pd.cut(
            rd_with_info['distance_m'],
            bins=[0, 1400, 1800, 2200, 9999],
            labels=['sprint', 'mile', 'middle', 'long']
        )
        
        # Train期間のみでペース傾向を計算
        train_rd = rd_with_info[rd_with_info['race_id'] < '2023']  # race_id prefix
        
        venue_pace = train_rd.groupby(['venue', 'track_surface', 'dist_cat'])['pace_diff'].agg(['mean', 'std', 'count']).reset_index()
        venue_pace.columns = ['venue', 'track_surface', 'dist_cat', 'pace_mean', 'pace_std', 'n_races']
        
        # 10レース以上のみ
        venue_pace = venue_pace[venue_pace['n_races'] >= 10]
        
        logging.info(f"  有効条件: {len(venue_pace)}パターン")
        
        # ハイペース傾向コース
        fast_pace_courses = venue_pace[venue_pace['pace_mean'] > 0].sort_values('pace_mean', ascending=False)
        logging.info(f"\n  【ハイペース傾向Top 5】")
        for _, row in fast_pace_courses.head(5).iterrows():
            logging.info(f"    {row['venue']} {row['track_surface']} {row['dist_cat']}: pace_diff={row['pace_mean']:.2f}")
        
        # スローペース傾向コース
        slow_pace_courses = venue_pace[venue_pace['pace_mean'] < 0].sort_values('pace_mean')
        logging.info(f"\n  【スローペース傾向Top 5】")
        for _, row in slow_pace_courses.head(5).iterrows():
            logging.info(f"    {row['venue']} {row['track_surface']} {row['dist_cat']}: pace_diff={row['pace_mean']:.2f}")
        
        return venue_pace
    
    def calculate_horse_running_style(self, races_df, corners):
        """馬の脚質を計算"""
        logging.info("\n【馬の脚質判定】")
        
        # 4コーナー通過順位
        c4 = corners[corners['corner'] == 4].copy()
        
        # horse_idマッピング
        horse_mapping = races_df[['race_id', 'horse_number', 'horse_id']].drop_duplicates()
        c4 = c4.merge(horse_mapping, on=['race_id', 'horse_number'], how='left')
        
        # 馬ごとの平均4コーナー通過順位
        horse_style = c4.groupby('horse_id')['position'].mean().reset_index()
        horse_style.columns = ['horse_id', 'avg_c4_position']
        
        # 脚質分類
        def classify_style(pos):
            if pd.isna(pos):
                return 'unknown'
            elif pos <= 3:
                return 'senkou'  # 逃げ/先行
            elif pos <= 6:
                return 'sashi'  # 差し
            else:
                return 'oikomi'  # 追込
        
        horse_style['running_style'] = horse_style['avg_c4_position'].apply(classify_style)
        
        logging.info(f"  脚質分布:")
        logging.info(f"    senkou: {(horse_style['running_style']=='senkou').sum():,}頭")
        logging.info(f"    sashi: {(horse_style['running_style']=='sashi').sum():,}頭")
        logging.info(f"    oikomi: {(horse_style['running_style']=='oikomi').sum():,}頭")
        
        return horse_style
    
    def analyze_pace_aptitude_edge(self, race_details, races_df, corners, returns_df, venue_pace, horse_style):
        """ペース適性マッチングのROI分析"""
        logging.info("\n" + "=" * 60)
        logging.info("【ペース適性マッチングROI分析】")
        logging.info("=" * 60)
        
        # race_idでペースをマージ
        race_info = races_df[['race_id', 'venue', 'distance_m', 'track_surface', 'horse_id', 
                               'horse_number', 'finish_position', 'win_odds', 'race_date']].copy()
        race_info = race_info[race_info['race_date'] >= '2024-01-01']  # Test期間
        
        race_info['dist_cat'] = pd.cut(
            race_info['distance_m'],
            bins=[0, 1400, 1800, 2200, 9999],
            labels=['sprint', 'mile', 'middle', 'long']
        )
        
        # ペース傾向をマージ
        race_info = race_info.merge(
            venue_pace[['venue', 'track_surface', 'dist_cat', 'pace_mean']],
            on=['venue', 'track_surface', 'dist_cat'],
            how='left'
        )
        
        # 脚質をマージ
        race_info = race_info.merge(horse_style, on='horse_id', how='left')
        
        # 適性マッチングスコア
        # スローペース × 逃げ/先行 = 有利
        # ハイペース × 差し/追込 = 有利
        def calc_aptitude_match(row):
            if pd.isna(row['pace_mean']) or pd.isna(row['running_style']):
                return 0.0
            
            pace = row['pace_mean']
            style = row['running_style']
            
            if pace < -0.5:  # スローペース傾向
                if style == 'senkou':
                    return 1.0  # 有利
                elif style == 'oikomi':
                    return -1.0  # 不利
            elif pace > 0.5:  # ハイペース傾向
                if style == 'oikomi':
                    return 1.0  # 有利
                elif style == 'senkou':
                    return -1.0  # 不利
            
            return 0.0
        
        race_info['aptitude_match'] = race_info.apply(calc_aptitude_match, axis=1)
        
        # 配当データ
        tansho = returns_df[returns_df['bet_type'] == 'tansho'][['race_id', 'horse_number', 'payout']]
        
        race_info = race_info.merge(
            tansho.rename(columns={'payout': 'tansho_payout'}),
            on=['race_id', 'horse_number'],
            how='left'
        )
        
        # 勝馬のみでROI計算
        winners = race_info[race_info['finish_position'] == 1].copy()
        
        logging.info(f"\n  Test期間勝馬: {len(winners):,}件")
        
        # 適性マッチング別のROI
        results = []
        
        logging.info(f"\n  【適性マッチング別ROI（勝馬のみ）】")
        for match_score, label in [(1.0, '有利'), (0.0, '中立'), (-1.0, '不利')]:
            subset = winners[winners['aptitude_match'] == match_score]
            
            if len(subset) < 30:
                continue
            
            avg_payout = subset['tansho_payout'].mean()
            avg_odds = subset['win_odds'].mean()
            
            logging.info(f"    {label}: {len(subset):,}件, 平均配当{avg_payout:.0f}円, 平均オッズ{avg_odds:.1f}倍")
            
            results.append({
                'match_type': label,
                'n_wins': len(subset),
                'avg_payout': avg_payout,
                'avg_odds': avg_odds,
            })
        
        # モデル予測1位と適性マッチを組み合わせる
        logging.info(f"\n  【全馬での分析】")
        for match_score, label in [(1.0, '有利'), (0.0, '中立'), (-1.0, '不利')]:
            subset = race_info[race_info['aptitude_match'] == match_score]
            
            if len(subset) < 100:
                continue
            
            win_rate = (subset['finish_position'] == 1).mean()
            avg_odds = subset['win_odds'].mean()
            
            logging.info(f"    {label}: {len(subset):,}件, 勝率{win_rate:.1%}, 平均オッズ{avg_odds:.1f}倍")
        
        return pd.DataFrame(results)
    
    def run(self):
        race_details, races_df, corners, returns_df = self.load_data()
        
        race_details = self.calculate_pace_diff(race_details)
        venue_pace = self.calculate_venue_pace_tendency(race_details, races_df)
        horse_style = self.calculate_horse_running_style(races_df, corners)
        
        results = self.analyze_pace_aptitude_edge(race_details, races_df, corners, returns_df, venue_pace, horse_style)
        
        # 結論
        logging.info("\n" + "=" * 60)
        logging.info("【結論】")
        logging.info("=" * 60)
        
        if len(results) >= 2:
            favorable = results[results['match_type'] == '有利'].iloc[0] if len(results[results['match_type'] == '有利']) > 0 else None
            unfavorable = results[results['match_type'] == '不利'].iloc[0] if len(results[results['match_type'] == '不利']) > 0 else None
            
            if favorable is not None and unfavorable is not None:
                diff = favorable['avg_payout'] - unfavorable['avg_payout']
                logging.info(f"  有利 vs 不利の平均配当差: {diff:.0f}円")
                
                if diff > 0:
                    logging.info(f"  → ペース適性マッチングにエッジあり！")
                else:
                    logging.info(f"  → ペース適性マッチングに明確なエッジなし")
        
        results.to_csv(self.output_dir / 'pace_aptitude_results.csv', index=False)
        
        return results


if __name__ == "__main__":
    analyzer = PaceTendencyAnalyzer()
    analyzer.run()
