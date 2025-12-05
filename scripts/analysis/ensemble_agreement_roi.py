#!/usr/bin/env python3
"""
アンサンブル一致戦略によるROI向上検証

【戦略】
異なるシード/パラメータで複数モデルを訓練し、
1位予測が一致した場合のみ投資。

【目標】
- ROI > 100%
- 投資機会 ≥ 20%（約1,140レース以上）
"""

import sys
from pathlib import Path
project_root = Path(__file__).resolve().parent.parent.parent
sys.path.append(str(project_root))

import pandas as pd
import numpy as np
import lightgbm as lgb
import json
import logging
from collections import Counter

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def train_diverse_models(train_df, valid_df, feature_cols, n_models=5):
    """異なる設定で複数モデルを訓練"""
    models = []
    
    group_train = train_df.groupby('race_id').size().to_list()
    group_valid = valid_df.groupby('race_id').size().to_list()
    
    # 異なる設定のバリエーション
    configs = [
        {'num_leaves': 31, 'feature_fraction': 0.7, 'random_state': 42},
        {'num_leaves': 63, 'feature_fraction': 0.6, 'random_state': 123},
        {'num_leaves': 47, 'feature_fraction': 0.8, 'random_state': 456},
        {'num_leaves': 31, 'feature_fraction': 0.5, 'random_state': 789},
        {'num_leaves': 63, 'feature_fraction': 0.7, 'random_state': 999},
    ]
    
    for i, cfg in enumerate(configs[:n_models]):
        logging.info(f"モデル {i+1}/{n_models} 訓練中...")
        
        params = {
            'objective': 'lambdarank',
            'metric': 'ndcg',
            'eval_at': [1, 3, 5],
            'boosting_type': 'gbdt',
            'num_leaves': cfg['num_leaves'],
            'lambda_l1': 1.0,
            'lambda_l2': 1.0,
            'min_child_samples': 100,
            'learning_rate': 0.05,
            'feature_fraction': cfg['feature_fraction'],
            'bagging_fraction': 0.8,
            'bagging_freq': 5,
            'verbose': -1,
            'random_state': cfg['random_state'],
            'n_estimators': 1500,
            'label_gain': list(range(100))
        }
        
        model = lgb.LGBMRanker(**params)
        model.fit(
            train_df[feature_cols], train_df['target_relevance'],
            group=group_train, sample_weight=train_df['sample_weight'],
            eval_set=[(valid_df[feature_cols], valid_df['target_relevance'])],
            eval_group=[group_valid],
            callbacks=[lgb.early_stopping(stopping_rounds=50, verbose=False)]
        )
        models.append(model)
    
    return models


def evaluate_ensemble_strategy(test_df, models, feature_cols, min_agreement=2):
    """アンサンブル一致戦略を評価"""
    n_races = test_df['race_id'].nunique()
    
    # 各モデルで予測
    predictions = []
    for model in models:
        test_df_copy = test_df.copy()
        test_df_copy['score'] = model.predict(test_df_copy[feature_cols])
        test_df_copy['rank_pred'] = test_df_copy.groupby('race_id')['score'].rank(ascending=False, method='first')
        pred = test_df_copy[test_df_copy['rank_pred'] == 1][['race_id', 'horse_id']].copy()
        pred.columns = ['race_id', 'top_horse']
        predictions.append(pred)
    
    # レースごとに一致度を計算
    results = []
    for race_id in test_df['race_id'].unique():
        votes = [p[p['race_id'] == race_id]['top_horse'].values[0] for p in predictions]
        vote_counts = Counter(votes)
        top_horse, count = vote_counts.most_common(1)[0]
        
        # 基本情報
        horse_row = test_df[(test_df['race_id'] == race_id) & (test_df['horse_id'] == top_horse)]
        if len(horse_row) > 0:
            results.append({
                'race_id': race_id,
                'horse_id': top_horse,
                'agreement': count,
                'finish_position': horse_row['finish_position'].values[0],
                'win_odds': horse_row['win_odds'].values[0]
            })
    
    results_df = pd.DataFrame(results)
    
    # 一致度別のROI
    output = {
        'n_models': len(models),
        'total_races': n_races,
        'agreement_analysis': []
    }
    
    for min_agree in range(2, len(models) + 1):
        bet_df = results_df[results_df['agreement'] >= min_agree]
        n_bets = len(bet_df)
        bet_ratio = n_bets / n_races
        
        if n_bets > 0:
            hits = bet_df[bet_df['finish_position'] == 1]
            roi = hits['win_odds'].sum() / n_bets
        else:
            roi = 0
        
        output['agreement_analysis'].append({
            'min_agreement': min_agree,
            'n_bets': n_bets,
            'bet_ratio': bet_ratio,
            'roi': roi
        })
        
        logging.info(f"一致度≥{min_agree}: {n_bets:,}レース ({bet_ratio:.1%}), ROI: {roi:.2%}")
    
    return output


def main():
    logging.info("=" * 60)
    logging.info("アンサンブル一致戦略 検証開始")
    logging.info("=" * 60)
    
    # データ読み込み
    data_dir = Path('keibaai/models/mu_v3_3')
    
    with open(data_dir / 'feature_names.json', 'r', encoding='utf-8') as f:
        feature_cols = json.load(f)
    
    df = pd.read_parquet(data_dir / 'train_data_mu_v3_3.parquet')
    df['race_date'] = pd.to_datetime(df['race_date'])
    
    # ターゲット準備
    weight_1st, weight_2nd, weight_3rd = 12.74, 6.73, 3.69
    odds = df['win_odds'].fillna(1.0)
    odds_clipped = odds.clip(upper=90)
    log_odds = np.log1p(odds_clipped)
    
    gain = np.zeros(len(df))
    gain[df['finish_position'] == 1] = log_odds[df['finish_position'] == 1] * weight_1st
    gain[df['finish_position'] == 2] = log_odds[df['finish_position'] == 2] * weight_2nd
    gain[df['finish_position'] == 3] = log_odds[df['finish_position'] == 3] * weight_3rd
    
    df['target_relevance'] = gain.astype(int)
    df['sample_weight'] = np.log1p(df['win_odds'].fillna(1.0)).clip(upper=np.log1p(100))
    
    # 分割
    train_mask = df['race_date'] < '2023-01-01'
    valid_mask = (df['race_date'] >= '2023-01-01') & (df['race_date'] < '2024-01-01')
    test_mask = df['race_date'] >= '2024-01-01'
    
    train_df = df[train_mask].copy()
    valid_df = df[valid_mask].copy()
    test_df = df[test_mask].copy()
    
    available_features = [f for f in feature_cols if f in df.columns]
    
    logging.info(f"Train: {len(train_df):,}, Valid: {len(valid_df):,}, Test: {len(test_df):,}")
    
    # 5モデル訓練
    models = train_diverse_models(train_df, valid_df, available_features, n_models=5)
    
    # 評価
    logging.info("\n【アンサンブル一致戦略 結果】")
    results = evaluate_ensemble_strategy(test_df, models, available_features)
    
    # 結果保存
    with open('ensemble_results.json', 'w', encoding='utf-8') as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
    
    logging.info("\n結果保存: ensemble_results.json")
    logging.info("=" * 60)


if __name__ == "__main__":
    main()
