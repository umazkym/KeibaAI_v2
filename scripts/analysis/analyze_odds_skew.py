#!/usr/bin/env python3
"""
morning_odds vs win_odds 乖離検証スクリプト

【レビュー指摘対応】
- Training-Serving Skewの定量化
- 運用時のリスク把握
"""

import sys
from pathlib import Path
project_root = Path(__file__).resolve().parent.parent.parent
sys.path.append(str(project_root))

import pandas as pd
import numpy as np
import json
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def main():
    logging.info("="*60)
    logging.info("morning_odds vs win_odds 乖離検証")
    logging.info("="*60)
    
    # データ読み込み
    races_df = pd.read_parquet('keibaai/data/parsed/parquet/races/races.parquet')
    shutuba_df = pd.read_parquet('keibaai/data/parsed/parquet/shutuba/shutuba.parquet')
    
    logging.info(f"レース結果: {len(races_df):,}")
    logging.info(f"出馬表: {len(shutuba_df):,}")
    
    # マージ
    races_df['horse_id'] = races_df['horse_id'].astype(str)
    shutuba_df['horse_id'] = shutuba_df['horse_id'].astype(str)
    
    merged = races_df.merge(
        shutuba_df[['race_id', 'horse_id', 'morning_odds', 'morning_popularity']],
        on=['race_id', 'horse_id'],
        how='left'
    )
    
    # 数値変換
    merged['win_odds'] = pd.to_numeric(merged['win_odds'], errors='coerce')
    merged['morning_odds'] = pd.to_numeric(merged['morning_odds'], errors='coerce')
    merged['popularity'] = pd.to_numeric(merged['popularity'], errors='coerce')
    merged['morning_popularity'] = pd.to_numeric(merged['morning_popularity'], errors='coerce')
    
    # 欠損除外
    valid = merged.dropna(subset=['win_odds', 'morning_odds', 'popularity', 'morning_popularity'])
    logging.info(f"有効データ: {len(valid):,} ({len(valid)/len(merged)*100:.1f}%)")
    
    # === オッズ乖離分析 ===
    logging.info("\n【オッズ乖離分析】")
    
    # 相関
    corr = valid['morning_odds'].corr(valid['win_odds'])
    logging.info(f"  相関係数: {corr:.4f}")
    
    # 絶対誤差
    valid['odds_diff'] = valid['win_odds'] - valid['morning_odds']
    valid['odds_diff_abs'] = valid['odds_diff'].abs()
    valid['odds_diff_pct'] = valid['odds_diff'] / valid['morning_odds'] * 100
    
    mae = valid['odds_diff_abs'].mean()
    mape = valid['odds_diff_pct'].abs().mean()
    logging.info(f"  平均絶対誤差 (MAE): {mae:.2f}")
    logging.info(f"  平均絶対誤差率 (MAPE): {mape:.1f}%")
    
    # オッズ帯別分析
    logging.info("\n【オッズ帯別分析】")
    bins = [(1, 5), (5, 10), (10, 50), (50, 1000)]
    for low, high in bins:
        subset = valid[(valid['morning_odds'] >= low) & (valid['morning_odds'] < high)]
        if len(subset) > 0:
            corr_band = subset['morning_odds'].corr(subset['win_odds'])
            mae_band = subset['odds_diff_abs'].mean()
            mape_band = subset['odds_diff_pct'].abs().mean()
            logging.info(f"  {low}-{high}倍: 件数={len(subset):,}, 相関={corr_band:.3f}, MAE={mae_band:.2f}, MAPE={mape_band:.1f}%")
    
    # === 人気乖離分析 ===
    logging.info("\n【人気乖離分析】")
    
    valid['pop_diff'] = valid['popularity'] - valid['morning_popularity']
    
    # 人気変動なし
    same_pop = (valid['pop_diff'] == 0).mean() * 100
    logging.info(f"  人気変動なし: {same_pop:.1f}%")
    
    # ±1以内
    within_1 = (valid['pop_diff'].abs() <= 1).mean() * 100
    logging.info(f"  ±1位以内: {within_1:.1f}%")
    
    # ±3以内
    within_3 = (valid['pop_diff'].abs() <= 3).mean() * 100
    logging.info(f"  ±3位以内: {within_3:.1f}%")
    
    # 大幅変動（4位以上）
    big_change = (valid['pop_diff'].abs() >= 4).mean() * 100
    logging.info(f"  4位以上変動: {big_change:.1f}%")
    
    # === 勝馬分析 ===
    logging.info("\n【勝馬における乖離】")
    winners = valid[valid['finish_position'] == 1]
    
    winner_pop_same = (winners['pop_diff'] == 0).mean() * 100
    winner_pop_within1 = (winners['pop_diff'].abs() <= 1).mean() * 100
    winner_odds_mape = winners['odds_diff_pct'].abs().mean()
    
    logging.info(f"  人気変動なし: {winner_pop_same:.1f}%")
    logging.info(f"  ±1位以内: {winner_pop_within1:.1f}%")
    logging.info(f"  オッズMAPE: {winner_odds_mape:.1f}%")
    
    # === 結論 ===
    logging.info("\n" + "="*60)
    logging.info("【結論】")
    logging.info("="*60)
    
    if corr > 0.95 and mape < 20:
        logging.info("✅ 乖離は許容範囲。運用時リスクは低い。")
    elif corr > 0.85:
        logging.info("⚠️ 中程度の乖離あり。穴馬狙い時に注意。")
    else:
        logging.info("🔴 乖離が大きい。Gap特徴量は運用時に信頼性低下。")
    
    # 結果保存
    output = {
        'correlation': float(corr),
        'mae': float(mae),
        'mape': float(mape),
        'pop_same': float(same_pop),
        'pop_within_1': float(within_1),
        'pop_within_3': float(within_3),
        'pop_big_change': float(big_change),
        'winner_pop_same': float(winner_pop_same),
        'winner_odds_mape': float(winner_odds_mape),
    }
    
    output_dir = Path('keibaai/models/odds_analysis')
    output_dir.mkdir(parents=True, exist_ok=True)
    with open(output_dir / 'morning_vs_win_odds.json', 'w', encoding='utf-8') as f:
        json.dump(output, f, ensure_ascii=False, indent=2)
    
    logging.info(f"\n保存完了: {output_dir}")


if __name__ == "__main__":
    main()
