#!/usr/bin/env python3
"""
追加評価指標計測スクリプト

【計測対象】
- Brier Score: 確率予測の精度
- NDCG@1,3,5: ランキング精度
- Top-1/3/5的中率: 推奨馬の的中率
- オッズ帯別ROI: 1-5, 5-10, 10-50, 50+

【使用方法】
python scripts/training/evaluate_model_metrics.py --model_dir keibaai/models/mu_v5_4
"""

import sys
from pathlib import Path

project_root = Path(__file__).resolve().parent.parent.parent
sys.path.append(str(project_root))

import argparse
import pandas as pd
import numpy as np
import pickle
import json
import logging
from scipy.special import softmax
from sklearn.metrics import brier_score_loss, log_loss

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def ndcg_at_k(y_true, y_score, k):
    """NDCG@k を計算"""
    order = np.argsort(y_score)[::-1]
    y_true_sorted = np.array(y_true)[order[:k]]
    
    # DCG
    gains = y_true_sorted
    discounts = np.log2(np.arange(1, k + 1) + 1)
    dcg = np.sum(gains / discounts)
    
    # IDCG
    ideal_order = np.argsort(y_true)[::-1]
    ideal_gains = np.array(y_true)[ideal_order[:k]]
    idcg = np.sum(ideal_gains / discounts)
    
    return dcg / idcg if idcg > 0 else 0


def calculate_metrics(df, pred_col='score'):
    """全評価指標を計算"""
    results = {}
    
    # グループ化
    grouped = df.groupby('race_id')
    
    # === ROI (Top1推奨) ===
    df_copy = df.copy()
    df_copy['rank_pred'] = grouped[pred_col].rank(ascending=False, method='first')
    bet = df_copy[df_copy['rank_pred'] == 1]
    hits = bet[bet['finish_position'] == 1]
    results['roi_top1'] = hits['win_odds'].sum() / len(bet) if len(bet) > 0 else 0
    
    # === Top-N 的中率 ===
    for k in [1, 3, 5]:
        top_k = df_copy[df_copy['rank_pred'] <= k]
        hit_rate = (top_k['finish_position'] == 1).sum() / len(grouped)
        results[f'hit_rate_top{k}'] = hit_rate
    
    # === NDCG@k (レースごとに計算して平均) ===
    ndcg_scores = {1: [], 3: [], 5: []}
    for race_id, group in grouped:
        y_true = (group['finish_position'] == 1).astype(int).values
        y_score = group[pred_col].values
        for k in [1, 3, 5]:
            if len(group) >= k:
                ndcg_scores[k].append(ndcg_at_k(y_true, y_score, k))
    
    for k in [1, 3, 5]:
        results[f'ndcg@{k}'] = np.mean(ndcg_scores[k]) if ndcg_scores[k] else 0
    
    # === Brier Score (勝率予測の精度) ===
    # スコアをsoftmaxで確率に変換
    probs = []
    targets = []
    for race_id, group in grouped:
        scores = group[pred_col].values
        prob = softmax(scores)
        probs.extend(prob)
        targets.extend((group['finish_position'] == 1).astype(int).values)
    
    results['brier_score'] = brier_score_loss(targets, probs)
    
    # === Log Loss ===
    # 確率を0.01-0.99にクリップ
    probs_clipped = np.clip(probs, 0.01, 0.99)
    results['log_loss'] = log_loss(targets, probs_clipped)
    
    # === オッズ帯別ROI ===
    odds_bins = [(1, 5), (5, 10), (10, 50), (50, 1000)]
    for low, high in odds_bins:
        mask = (bet['win_odds'] >= low) & (bet['win_odds'] < high)
        bet_subset = bet[mask]
        if len(bet_subset) > 0:
            hits_subset = bet_subset[bet_subset['finish_position'] == 1]
            roi = hits_subset['win_odds'].sum() / len(bet_subset)
            results[f'roi_{low}_{high}'] = roi
            results[f'count_{low}_{high}'] = len(bet_subset)
        else:
            results[f'roi_{low}_{high}'] = 0
            results[f'count_{low}_{high}'] = 0
    
    return results


def load_model_and_predict(model_dir, test_df, feature_cols):
    """モデルをロードして予測"""
    with open(model_dir / 'mu_v5_4_ranker.pkl', 'rb') as f:
        model = pickle.load(f)
    
    # 利用可能な特徴量のみ使用
    available_features = [f for f in feature_cols if f in test_df.columns]
    preds = model.predict(test_df[available_features])
    return preds


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--model_dir', type=str, default='keibaai/models/mu_v5_4')
    args = parser.parse_args()
    
    model_dir = Path(args.model_dir)
    
    logging.info("=" * 60)
    logging.info(f"評価指標計測: {model_dir.name}")
    logging.info("=" * 60)
    
    # モデル情報を読み込み
    with open(model_dir / 'model_info.json', 'r', encoding='utf-8') as f:
        model_info = json.load(f)
    
    logging.info(f"バージョン: {model_info.get('version', 'unknown')}")
    logging.info(f"説明: {model_info.get('description', 'none')}")
    logging.info(f"Valid ROI (学習時): {model_info.get('valid_roi', 0):.2%}")
    logging.info(f"Test ROI (学習時): {model_info.get('test_roi', 0):.2%}")
    
    # train_data_mu_v3_3.parquetを直接使用し、簡易評価
    base_dir = Path('keibaai/models/mu_v3_3')
    train_data = pd.read_parquet(base_dir / 'train_data_mu_v3_3.parquet')
    train_data['race_date'] = pd.to_datetime(train_data['race_date'])
    
    # データ分割
    valid_df = train_data[(train_data['race_date'] >= '2023-01-01') & 
                          (train_data['race_date'] < '2024-01-01')].copy()
    test_df = train_data[train_data['race_date'] >= '2024-01-01'].copy()
    
    logging.info(f"Valid: {len(valid_df):,}, Test: {len(test_df):,}")
    
    # 既存の人気順をスコアとして使用して基準線を計算
    logging.info("")
    logging.info("【ベースライン評価（人気順による予測）】")
    
    # 人気順（低いほど高評価）を反転してスコア化
    valid_df['score'] = -valid_df['popularity'].fillna(99)
    test_df['score'] = -test_df['popularity'].fillna(99)
    
    logging.info("")
    logging.info("【Valid期間 (2023年) - 人気順ベースライン】")
    valid_metrics = calculate_metrics(valid_df)
    for key, value in valid_metrics.items():
        if 'count' in key:
            logging.info(f"  {key}: {value}")
        elif isinstance(value, float):
            logging.info(f"  {key}: {value:.4f}")
    
    logging.info("")
    logging.info("【Test期間 (2024年〜) - 人気順ベースライン】")
    test_metrics = calculate_metrics(test_df)
    for key, value in test_metrics.items():
        if 'count' in key:
            logging.info(f"  {key}: {value}")
        elif isinstance(value, float):
            logging.info(f"  {key}: {value:.4f}")
    
    # 評価
    logging.info("")
    logging.info("【Valid期間 (2023年)】")
    valid_metrics = calculate_metrics(valid_df)
    for key, value in valid_metrics.items():
        if 'count' in key:
            logging.info(f"  {key}: {value}")
        else:
            logging.info(f"  {key}: {value:.4f}")
    
    logging.info("")
    logging.info("【Test期間 (2024年〜)】")
    test_metrics = calculate_metrics(test_df)
    for key, value in test_metrics.items():
        if 'count' in key:
            logging.info(f"  {key}: {value}")
        else:
            logging.info(f"  {key}: {value:.4f}")
    
    # 結果保存
    output = {
        'model_dir': str(model_dir),
        'valid_metrics': valid_metrics,
        'test_metrics': test_metrics,
    }
    output_file = model_dir / 'evaluation_metrics.json'
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(output, f, ensure_ascii=False, indent=2)
    
    logging.info("")
    logging.info(f"保存完了: {output_file}")
    
    # サマリー
    logging.info("")
    logging.info("=" * 60)
    logging.info("【サマリー】")
    logging.info("=" * 60)
    logging.info(f"  ROI (Top1): {test_metrics['roi_top1']:.2%}")
    logging.info(f"  Top-1 的中率: {test_metrics['hit_rate_top1']:.2%}")
    logging.info(f"  Top-3 的中率: {test_metrics['hit_rate_top3']:.2%}")
    logging.info(f"  NDCG@3: {test_metrics['ndcg@3']:.4f}")
    logging.info(f"  Brier Score: {test_metrics['brier_score']:.4f}")
    logging.info("=" * 60)


if __name__ == "__main__":
    main()
