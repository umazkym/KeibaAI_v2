#!/usr/bin/env python3
"""Kelly戦略によるROI検証（結果をJSON保存）"""

import sys
from pathlib import Path
project_root = Path(__file__).resolve().parent.parent.parent
sys.path.append(str(project_root))

import pandas as pd
import numpy as np
import pickle
import json
from scipy.special import softmax

def main():
    # v3.8モデルとデータ読み込み
    model_dir = Path('keibaai/models/mu_v3_8')
    data_dir = Path('keibaai/models/mu_v3_3')
    
    with open(model_dir / 'mu_v3_8_ranker.pkl', 'rb') as f:
        model = pickle.load(f)
    
    with open(model_dir / 'feature_names.json', 'r', encoding='utf-8') as f:
        feature_cols = json.load(f)
    
    df = pd.read_parquet(data_dir / 'train_data_mu_v3_3.parquet')
    df['race_date'] = pd.to_datetime(df['race_date'])
    
    # Test期間のみ
    test_mask = df['race_date'] >= '2024-01-01'
    test_df = df[test_mask].copy()
    n_races = test_df['race_id'].nunique()
    
    # 予測
    available_features = [f for f in feature_cols if f in test_df.columns]
    test_df['score'] = model.predict(test_df[available_features])
    test_df['rank_pred'] = test_df.groupby('race_id')['score'].rank(ascending=False, method='first')
    
    # 確率変換
    def score_to_prob(group):
        scores = group['score'].values
        probs = softmax(scores)
        return pd.Series(probs, index=group.index)
    
    test_df['pred_prob'] = test_df.groupby('race_id', group_keys=False).apply(score_to_prob)
    
    # Kelly fraction
    test_df['b'] = test_df['win_odds'] - 1
    test_df['kelly_f'] = test_df['pred_prob'] - (1 - test_df['pred_prob']) / test_df['b'].clip(lower=0.01)
    
    # 閾値探索
    results = []
    for thresh in np.arange(0.0, 0.5, 0.01):
        bet_mask = (test_df['rank_pred'] == 1) & (test_df['kelly_f'] >= thresh)
        bet_df = test_df[bet_mask]
        
        n_bets = len(bet_df)
        if n_bets < 50:
            continue
        
        hits = bet_df[bet_df['finish_position'] == 1]
        roi = hits['win_odds'].sum() / n_bets if n_bets > 0 else 0
        
        results.append({
            'threshold': float(thresh),
            'n_bets': int(n_bets),
            'bet_ratio': float(n_bets / n_races),
            'roi': float(roi)
        })
    
    # 結果保存
    output = {
        'total_races': n_races,
        'baseline_roi': 0.8103,  # v3.8全レースROI
        'results': results
    }
    
    # 100%超え達成条件
    over_100 = [r for r in results if r['roi'] >= 1.0]
    if over_100:
        best = min(over_100, key=lambda x: x['threshold'])
        output['roi_100_achieved'] = {
            'threshold': best['threshold'],
            'roi': best['roi'],
            'n_bets': best['n_bets'],
            'bet_ratio': best['bet_ratio']
        }
    else:
        output['roi_100_achieved'] = None
    
    # 最高ROI
    if results:
        best_roi = max(results, key=lambda x: x['roi'])
        output['best_roi'] = {
            'threshold': best_roi['threshold'],
            'roi': best_roi['roi'],
            'n_bets': best_roi['n_bets'],
            'bet_ratio': best_roi['bet_ratio']
        }
    
    with open('kelly_results.json', 'w', encoding='utf-8') as f:
        json.dump(output, f, indent=2, ensure_ascii=False)
    
    print("結果保存完了: kelly_results.json")

if __name__ == "__main__":
    main()
