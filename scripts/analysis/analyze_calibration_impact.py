#!/usr/bin/env python3
"""
Calibration Impact分析スクリプト

【目的】
- Calibration適用前後の効果を可視化
- Reliability Diagram生成
- 確率帯別の較正状況分析
- 中穴帯（10-20%）のEV変化分析
"""

import sys
from pathlib import Path

project_root = Path(__file__).resolve().parent.parent.parent
sys.path.append(str(project_root))

import pandas as pd
import numpy as np
import json
import logging
import matplotlib.pyplot as plt
from scipy.special import softmax
from sklearn.calibration import calibration_curve
from sklearn.metrics import brier_score_loss

plt.rcParams['font.family'] = 'DejaVu Sans'  # Unicode対応フォント

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def analyze_calibration_impact():
    """Calibration効果分析のメイン関数"""
    
    logging.info("=" * 60)
    logging.info("Calibration Impact Analysis")
    logging.info("=" * 60)
    
    output_dir = Path('keibaai/models/mu_v6_0')
    
    # 結果JSONが存在するか確認
    results_path = output_dir / 'calibration_results.json'
    if not results_path.exists():
        logging.error(f"結果ファイルが見つかりません: {results_path}")
        logging.info("先に train_mu_v6_calibrated.py を実行してください")
        return None
    
    with open(results_path, 'r', encoding='utf-8') as f:
        results = json.load(f)
    
    # === 1. サマリー出力 ===
    logging.info("\n【1. 結果サマリー】")
    logging.info(f"  Baseline ROI: {results['baseline_roi']:.2%}")
    logging.info(f"  Brier Score (Before): {results['calibration']['brier_before']:.4f}")
    logging.info(f"  Brier Score (After): {results['calibration']['brier_after']:.4f}")
    
    # === 2. EV閾値別プロット ===
    logging.info("\n【2. EV閾値別ROI】")
    
    ev_data = results['ev_results']
    thresholds = [e['threshold'] for e in ev_data]
    rois = [e['roi'] for e in ev_data]
    ratios = [e['investment_ratio'] for e in ev_data]
    
    fig, ax1 = plt.subplots(figsize=(10, 6))
    
    # ROI
    color1 = '#2196F3'
    ax1.plot(thresholds, rois, 'o-', color=color1, linewidth=2, markersize=8, label='ROI')
    ax1.axhline(y=1.0, color='#4CAF50', linestyle='--', linewidth=2, label='Break-even (100%)')
    ax1.set_xlabel('EV Threshold', fontsize=12)
    ax1.set_ylabel('ROI', fontsize=12, color=color1)
    ax1.tick_params(axis='y', labelcolor=color1)
    ax1.set_ylim(0.5, max(rois) * 1.1)
    
    # 投資比率（右軸）
    ax2 = ax1.twinx()
    color2 = '#FF9800'
    ax2.bar(thresholds, ratios, alpha=0.3, color=color2, width=0.08, label='Investment Ratio')
    ax2.set_ylabel('Investment Ratio', fontsize=12, color=color2)
    ax2.tick_params(axis='y', labelcolor=color2)
    ax2.set_ylim(0, 1.0)
    
    plt.title('ROI vs EV Threshold (with Investment Ratio)', fontsize=14)
    
    # 凡例
    lines1, labels1 = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax1.legend(lines1 + lines2, labels1 + labels2, loc='upper right')
    
    plt.tight_layout()
    plt.savefig(output_dir / 'ev_threshold_analysis.png', dpi=150)
    plt.close()
    logging.info(f"  保存: {output_dir / 'ev_threshold_analysis.png'}")
    
    # === 3. 中穴帯分析 ===
    logging.info("\n【3. 中穴帯（10-20%）分析】")
    mid = results['mid_tier_analysis']
    logging.info(f"  件数: {mid['count']:,}")
    logging.info(f"  EV平均 (Before): {mid['ev_before_mean']:.3f}")
    logging.info(f"  EV平均 (After): {mid['ev_after_mean']:.3f}")
    logging.info(f"  EV上昇: {(mid['ev_after_mean'] - mid['ev_before_mean']) / mid['ev_before_mean']:.1%}")
    logging.info(f"  新規投資機会: {mid['new_opportunities']:,}件")
    
    # === 4. 最適閾値 ===
    logging.info("\n【4. 推奨設定】")
    if results.get('optimal_threshold'):
        logging.info(f"  推奨EV閾値: {results['optimal_threshold']:.1f}")
        logging.info(f"  期待ROI: {results['optimal_roi']:.2%}")
    else:
        # ROIが最大のものを表示
        best = max(ev_data, key=lambda x: x['roi'])
        logging.info(f"  ROI最大の閾値: {best['threshold']:.1f}")
        logging.info(f"  ROI: {best['roi']:.2%} (投資比率: {best['investment_ratio']:.1%})")
    
    logging.info("\n" + "=" * 60)
    logging.info("分析完了")
    logging.info("=" * 60)
    
    return results


if __name__ == "__main__":
    analyze_calibration_impact()
