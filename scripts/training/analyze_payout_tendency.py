#!/usr/bin/env python3
"""
騎手/調教師の配当傾向特徴量分析

【仮説】
- 過去に高配当を出しやすい騎手/調教師は「穴馬」を当てる能力がある
- これを特徴量化してベット選択に活用できるか検証
"""

import pandas as pd
import numpy as np
import pickle
import json
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def main():
    logging.info("=" * 60)
    logging.info("騎手/調教師 配当傾向分析")
    logging.info("=" * 60)
    
    # モデル読み込み
    with open('keibaai/models/mu_v11_0/mu_v11_0_ranker.pkl', 'rb') as f:
        model = pickle.load(f)
    with open('keibaai/models/mu_v11_0/feature_names.json', 'r', encoding='utf-8') as f:
        feature_names = json.load(f)
    
    # データ読み込み
    base_data = pd.read_parquet('keibaai/models/mu_v3_3/train_data_mu_v3_3.parquet')
    base_data['race_date'] = pd.to_datetime(base_data['race_date'])
    base_data['race_id'] = base_data['race_id'].astype(str)
    base_data['horse_id'] = base_data['horse_id'].astype(str)
    
    races_df = pd.read_parquet('keibaai/data/parsed/parquet/races/races.parquet')
    races_df['race_date'] = pd.to_datetime(races_df['race_date'])
    races_df['race_id'] = races_df['race_id'].astype(str)
    races_df['horse_id'] = races_df['horse_id'].astype(str)
    races_df['horse_number'] = pd.to_numeric(races_df['horse_number'], errors='coerce')
    races_df['finish_position'] = pd.to_numeric(races_df['finish_position'], errors='coerce')
    races_df['win_odds'] = pd.to_numeric(races_df['win_odds'], errors='coerce')
    
    returns_df = pd.read_parquet('keibaai/data/parsed/parquet/returns/returns.parquet')
    returns_df['race_id'] = returns_df['race_id'].astype(str)
    returns_df['horse_number'] = pd.to_numeric(returns_df['horse_number'], errors='coerce')
    
    logging.info(f"  base_data: {len(base_data):,}件")
    logging.info(f"  races_df: {len(races_df):,}件")
    
    # 単勝配当
    tansho = returns_df[returns_df['bet_type'] == 'tansho'][['race_id', 'horse_number', 'payout']].dropna()
    
    # Train期間で騎手/調教師の平均配当を計算
    train_races = races_df[races_df['race_date'] < '2023-01-01']
    winners = train_races[train_races['finish_position'] == 1][['race_id', 'horse_number', 'jockey_id', 'trainer_id']].copy()
    winners = winners.merge(tansho, on=['race_id', 'horse_number'], how='left')
    
    # 騎手別平均配当
    jockey_payout = winners.groupby('jockey_id')['payout'].agg(['mean', 'count']).reset_index()
    jockey_payout.columns = ['jockey_id', 'jockey_avg_payout', 'jockey_n_wins']
    jockey_payout = jockey_payout[jockey_payout['jockey_n_wins'] >= 10]
    
    # 高配当騎手閾値（上位25%）
    high_payout_jockey_threshold = jockey_payout['jockey_avg_payout'].quantile(0.75)
    high_payout_jockeys = set(jockey_payout[jockey_payout['jockey_avg_payout'] >= high_payout_jockey_threshold]['jockey_id'])
    
    logging.info(f"\n  高配当騎手閾値: {high_payout_jockey_threshold:.0f}円")
    logging.info(f"  高配当騎手数: {len(high_payout_jockeys)}")
    
    # 特徴量準備
    for f in feature_names:
        if f not in base_data.columns:
            base_data[f] = 0
        else:
            base_data[f] = base_data[f].fillna(0)
    
    # Test期間
    test = base_data[base_data['race_date'] >= '2024-01-01'].copy()
    
    # 予測
    logging.info("\n  予測中...")
    preds = model.predict(test[feature_names])
    test['raw_score'] = preds
    test['rank_in_race'] = test.groupby('race_id')['raw_score'].rank(ascending=False, method='first')
    
    # Top1
    top1_ids = test[test['rank_in_race'] == 1][['race_id', 'horse_id']].copy()
    
    # races_dfから情報を取得
    test_races = races_df[races_df['race_date'] >= '2024-01-01'].copy()
    top1_full = top1_ids.merge(
        test_races[['race_id', 'horse_id', 'horse_number', 'jockey_id', 'finish_position', 'win_odds']],
        on=['race_id', 'horse_id'],
        how='left'
    )
    
    # 配当マージ
    top1_full = top1_full.merge(
        tansho.rename(columns={'payout': 'tansho_payout'}),
        on=['race_id', 'horse_number'],
        how='left'
    )
    
    # 高配当騎手フラグ
    top1_full['is_high_payout_jockey'] = top1_full['jockey_id'].isin(high_payout_jockeys)
    
    logging.info(f"\n  Top1件数: {len(top1_full):,}")
    logging.info(f"  高配当騎手のTop1: {top1_full['is_high_payout_jockey'].sum():,}件")
    
    # ROI計算
    logging.info("\n" + "=" * 60)
    logging.info("【条件別ROI（Test期間2024年）】")
    logging.info("=" * 60)
    
    for label, subset in [
        ('全件', top1_full),
        ('高配当騎手', top1_full[top1_full['is_high_payout_jockey'] == True]),
        ('一般騎手', top1_full[top1_full['is_high_payout_jockey'] == False]),
    ]:
        if len(subset) < 50:
            continue
        
        payout = subset['tansho_payout'].fillna(0).sum()
        roi = payout / (len(subset) * 100)
        hits = (subset['tansho_payout'] > 0).sum()
        hit_rate = hits / len(subset)
        
        status = '✅' if roi >= 1.03 else ('⚠️' if roi >= 0.9 else '❌')
        logging.info(f"  {label}: ベット{len(subset):5} | 的中率{hit_rate:5.1%} | ROI{roi:6.1%} {status}")
    
    # 結論
    logging.info("\n" + "=" * 60)
    logging.info("【結論】")
    logging.info("=" * 60)
    
    high_payout_subset = top1_full[top1_full['is_high_payout_jockey'] == True]
    general_subset = top1_full[top1_full['is_high_payout_jockey'] == False]
    
    if len(high_payout_subset) >= 50 and len(general_subset) >= 50:
        hp_roi = high_payout_subset['tansho_payout'].fillna(0).sum() / (len(high_payout_subset) * 100)
        gen_roi = general_subset['tansho_payout'].fillna(0).sum() / (len(general_subset) * 100)
        
        if hp_roi > gen_roi:
            logging.info(f"  高配当騎手: ROI{hp_roi:.1%} > 一般騎手: ROI{gen_roi:.1%}")
            logging.info(f"  → 高配当騎手フィルターにエッジあり！")
        else:
            logging.info(f"  高配当騎手: ROI{hp_roi:.1%} <= 一般騎手: ROI{gen_roi:.1%}")
            logging.info(f"  → 高配当騎手フィルターに明確なエッジなし")


if __name__ == "__main__":
    main()
