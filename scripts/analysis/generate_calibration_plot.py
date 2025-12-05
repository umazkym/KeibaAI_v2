#!/usr/bin/env python3
"""
Calibration Plot生成スクリプト

【レビュー指摘対応】
- 予測確率 vs 実際勝率のプロット
- モデルの確率校正を可視化
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
import matplotlib.pyplot as plt
from scipy.special import softmax

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def main():
    logging.info("="*60)
    logging.info("Calibration Plot生成")
    logging.info("="*60)
    
    # データ読み込み
    base_dir = Path('keibaai/models/mu_v3_3')
    df = pd.read_parquet(base_dir / 'train_data_mu_v3_3.parquet')
    df['race_date'] = pd.to_datetime(df['race_date'])
    
    # Test期間
    test_df = df[df['race_date'] >= '2024-01-01'].copy()
    logging.info(f"Testデータ: {len(test_df):,}")
    
    # 人気による予測確率（ベースライン）
    test_df['popularity'] = pd.to_numeric(test_df['popularity'], errors='coerce')
    test_df['pred_prob'] = 1 / test_df['popularity'].clip(lower=1)  # 簡易確率
    
    # レースごとに正規化
    test_df['pred_prob'] = test_df.groupby('race_id')['pred_prob'].transform(
        lambda x: x / x.sum()
    )
    
    # Calibration計算
    bins = np.linspace(0, 0.5, 11)  # 0-50%を10分割
    calibration_data = []
    
    for i in range(len(bins) - 1):
        low, high = bins[i], bins[i + 1]
        mask = (test_df['pred_prob'] >= low) & (test_df['pred_prob'] < high)
        subset = test_df[mask]
        
        if len(subset) > 0:
            mean_pred = subset['pred_prob'].mean()
            actual_win = (subset['finish_position'] == 1).mean()
            calibration_data.append({
                'bin': f'{low:.2f}-{high:.2f}',
                'mean_pred': mean_pred,
                'actual_win': actual_win,
                'count': len(subset),
            })
    
    # 結果表示
    logging.info("\n【Calibration結果】")
    logging.info("| 予測確率帯 | 予測平均 | 実際勝率 | 件数 |")
    logging.info("|------------|----------|----------|------|")
    for d in calibration_data:
        diff = d['actual_win'] - d['mean_pred']
        marker = "✓" if abs(diff) < 0.03 else ("↑" if diff > 0 else "↓")
        logging.info(f"| {d['bin']} | {d['mean_pred']:.3f} | {d['actual_win']:.3f} | {d['count']:,} | {marker}")
    
    # プロット生成
    plt.figure(figsize=(8, 6))
    mean_preds = [d['mean_pred'] for d in calibration_data]
    actual_wins = [d['actual_win'] for d in calibration_data]
    
    plt.plot([0, 0.5], [0, 0.5], 'k--', label='Perfect Calibration')
    plt.scatter(mean_preds, actual_wins, s=100, c='blue', alpha=0.7)
    plt.plot(mean_preds, actual_wins, 'b-', alpha=0.5)
    
    plt.xlabel('Predicted Probability')
    plt.ylabel('Actual Win Rate')
    plt.title('Calibration Plot (Popularity-based Baseline)')
    plt.legend()
    plt.grid(True, alpha=0.3)
    
    output_dir = Path('keibaai/models/calibration')
    output_dir.mkdir(parents=True, exist_ok=True)
    plt.savefig(output_dir / 'calibration_plot.png', dpi=150, bbox_inches='tight')
    plt.close()
    
    # 結果保存
    with open(output_dir / 'calibration_data.json', 'w', encoding='utf-8') as f:
        json.dump(calibration_data, f, ensure_ascii=False, indent=2)
    
    logging.info(f"\n保存完了: {output_dir}")


if __name__ == "__main__":
    main()
