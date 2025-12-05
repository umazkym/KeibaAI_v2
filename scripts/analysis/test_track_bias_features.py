#!/usr/bin/env python3
"""
馬場バイアス特徴量の効果検証スクリプト

【目的】
- TrackBiasFeatureGeneratorが正常に動作することを確認
- バイアス特徴量がROI改善に寄与するかを検証
"""

import sys
from pathlib import Path

project_root = Path(__file__).resolve().parent.parent.parent
sys.path.append(str(project_root))

import pandas as pd
import numpy as np
import logging
from datetime import datetime

from keibaai.src.features.track_bias_features import TrackBiasFeatureGenerator

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def test_track_bias_features():
    """馬場バイアス特徴量のテスト"""
    logging.info("=" * 70)
    logging.info("馬場バイアス特徴量テスト")
    logging.info("=" * 70)
    
    # データ読み込み
    logging.info("データ読み込み中...")
    
    base_dir = Path('keibaai/models/mu_v3_3')
    train_data = pd.read_parquet(base_dir / 'train_data_mu_v3_3.parquet')
    train_data['race_date'] = pd.to_datetime(train_data['race_date'])
    
    races_df = pd.read_parquet('keibaai/data/parsed/parquet/races/races.parquet')
    races_df['race_date'] = pd.to_datetime(races_df['race_date'])
    
    train_data['horse_id'] = train_data['horse_id'].astype(str)
    races_df['horse_id'] = races_df['horse_id'].astype(str)
    
    # venueをマージ
    race_info = races_df[['race_id', 'horse_id', 'venue', 'track_surface', 'bracket_number']].drop_duplicates(subset=['race_id', 'horse_id'])
    for col in ['venue', 'track_surface', 'bracket_number']:
        if col not in train_data.columns:
            train_data = train_data.merge(race_info[['race_id', 'horse_id', col]], on=['race_id', 'horse_id'], how='left')
    
    # Test期間
    test_df = train_data[train_data['race_date'] >= '2024-01-01'].copy()
    logging.info(f"  Testデータ: {len(test_df):,} ({test_df['race_id'].nunique():,}レース)")
    
    # 馬場バイアス特徴量生成
    logging.info("\n【特徴量生成】")
    generator = TrackBiasFeatureGenerator(min_races_for_bias=2)
    test_with_bias = generator.generate_bias_features(test_df, races_df)
    
    # 生成結果確認
    logging.info("\n【生成結果】")
    logging.info(f"  bias_inner 分布:")
    logging.info(f"    mean: {test_with_bias['bias_inner'].mean():.4f}")
    logging.info(f"    std:  {test_with_bias['bias_inner'].std():.4f}")
    logging.info(f"    min:  {test_with_bias['bias_inner'].min():.4f}")
    logging.info(f"    max:  {test_with_bias['bias_inner'].max():.4f}")
    
    logging.info(f"\n  bias_front 分布:")
    logging.info(f"    mean: {test_with_bias['bias_front'].mean():.4f}")
    logging.info(f"    std:  {test_with_bias['bias_front'].std():.4f}")
    logging.info(f"    min:  {test_with_bias['bias_front'].min():.4f}")
    logging.info(f"    max:  {test_with_bias['bias_front'].max():.4f}")
    
    logging.info(f"\n  bias_inner_advantage 分布:")
    logging.info(f"    mean: {test_with_bias['bias_inner_advantage'].mean():.4f}")
    logging.info(f"    std:  {test_with_bias['bias_inner_advantage'].std():.4f}")
    
    logging.info(f"\n  bias_front_advantage 分布:")
    logging.info(f"    mean: {test_with_bias['bias_front_advantage'].mean():.4f}")
    logging.info(f"    std:  {test_with_bias['bias_front_advantage'].std():.4f}")
    
    # バイアス有効レース比率
    valid_bias = test_with_bias[test_with_bias['bias_inner'] != 0]
    logging.info(f"\n  バイアス計算可能: {len(valid_bias):,} / {len(test_with_bias):,} ({len(valid_bias)/len(test_with_bias):.1%})")
    
    # バイアスとROIの相関分析
    logging.info("\n【バイアス強度別ROI分析】")
    
    # バイアス強い内枠有利馬場で内枠を買う
    high_inner_bias = test_with_bias[test_with_bias['bias_inner'] > 0.1]
    if len(high_inner_bias) > 0:
        inner_bracket = high_inner_bias[high_inner_bias['bracket_number'] <= 3]
        if len(inner_bracket) > 0:
            inner_roi = inner_bracket[inner_bracket['finish_position'] == 1]['win_odds'].sum() / len(inner_bracket) * (len(inner_bracket) / inner_bracket['race_id'].nunique())
            logging.info(f"  内枠有利馬場(bias>0.1) × 内枠(1-3枠): {len(inner_bracket)}件, 推定寄与: {inner_roi:.2%}")
    
    # 先行有利馬場で先行型を買う
    high_front_bias = test_with_bias[test_with_bias['bias_front'] > 0.1]
    if len(high_front_bias) > 0 and 'horse_avg_position_4c' in high_front_bias.columns:
        front_runners = high_front_bias[high_front_bias['horse_avg_position_4c'] < 0.3]  # 先行型
        if len(front_runners) > 0:
            front_roi = front_runners[front_runners['finish_position'] == 1]['win_odds'].sum() / len(front_runners) * (len(front_runners) / front_runners['race_id'].nunique())
            logging.info(f"  先行有利馬場(bias>0.1) × 先行型: {len(front_runners)}件, 推定寄与: {front_roi:.2%}")
    
    logging.info("\n" + "=" * 70)
    logging.info("テスト完了")
    logging.info("=" * 70)
    
    return test_with_bias


if __name__ == "__main__":
    test_track_bias_features()
