#!/usr/bin/env python3
"""
目的関数比較スクリプト: LambdaRank vs Binary Classification

【レビュー指摘対応】
- LambdaRankは順位最適化、Binary Classificationは勝率予測
- 投資目的には勝率予測の方が適切な可能性
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
import optuna
from scipy.special import softmax
from sklearn.metrics import brier_score_loss, log_loss

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def calculate_roi(df, pred_col='score'):
    """ROI計算"""
    df = df.copy()
    df['rank_pred'] = df.groupby('race_id')[pred_col].rank(ascending=False, method='first')
    bet = df[df['rank_pred'] == 1]
    hits = bet[bet['finish_position'] == 1]
    return hits['win_odds'].sum() / len(bet) if len(bet) > 0 else 0


def calculate_brier_score(df, pred_col='score'):
    """Brier Score計算"""
    probs = []
    targets = []
    for race_id, group in df.groupby('race_id'):
        scores = group[pred_col].values
        prob = softmax(scores)
        probs.extend(prob)
        targets.extend((group['finish_position'] == 1).astype(int).values)
    return brier_score_loss(targets, probs)


def train_lambdarank(train_df, valid_df, feature_cols, n_trials=20):
    """LambdaRank学習"""
    group_train = train_df.groupby('race_id').size().tolist()
    group_valid = valid_df.groupby('race_id').size().tolist()
    
    # ターゲット（順位ベース）
    odds = train_df['win_odds'].fillna(1.0).clip(upper=90)
    log_odds = np.log1p(odds)
    gain = np.zeros(len(train_df))
    gain[train_df['finish_position'] == 1] = log_odds[train_df['finish_position'] == 1] * 12.74
    gain[train_df['finish_position'] == 2] = log_odds[train_df['finish_position'] == 2] * 6.73
    gain[train_df['finish_position'] == 3] = log_odds[train_df['finish_position'] == 3] * 3.69
    train_df = train_df.copy()
    train_df['target'] = gain.astype(int)
    
    valid_df = valid_df.copy()
    odds_v = valid_df['win_odds'].fillna(1.0).clip(upper=90)
    log_odds_v = np.log1p(odds_v)
    gain_v = np.zeros(len(valid_df))
    gain_v[valid_df['finish_position'] == 1] = log_odds_v[valid_df['finish_position'] == 1] * 12.74
    gain_v[valid_df['finish_position'] == 2] = log_odds_v[valid_df['finish_position'] == 2] * 6.73
    gain_v[valid_df['finish_position'] == 3] = log_odds_v[valid_df['finish_position'] == 3] * 3.69
    valid_df['target'] = gain_v.astype(int)
    
    def objective(trial):
        params = {
            'objective': 'lambdarank', 'metric': 'ndcg', 'eval_at': [1, 3, 5],
            'boosting_type': 'gbdt', 'verbose': -1, 'random_state': 42,
            'label_gain': list(range(100)),
            'num_leaves': trial.suggest_int('num_leaves', 30, 100),
            'lambda_l1': trial.suggest_float('lambda_l1', 0.01, 5.0, log=True),
            'lambda_l2': trial.suggest_float('lambda_l2', 0.01, 5.0, log=True),
            'min_child_samples': trial.suggest_int('min_child_samples', 20, 140),
            'learning_rate': trial.suggest_float('learning_rate', 0.02, 0.12, log=True),
        }
        model = lgb.LGBMRanker(**params, n_estimators=500)
        model.fit(train_df[feature_cols], train_df['target'], group=group_train,
                  eval_set=[(valid_df[feature_cols], valid_df['target'])], eval_group=[group_valid],
                  callbacks=[lgb.early_stopping(30, verbose=False)])
        valid_df['score'] = model.predict(valid_df[feature_cols])
        return calculate_roi(valid_df)
    
    optuna.logging.set_verbosity(optuna.logging.WARNING)
    study = optuna.create_study(direction='maximize')
    study.optimize(objective, n_trials=n_trials, show_progress_bar=True)
    
    best_params = study.best_params
    best_params.update({'objective': 'lambdarank', 'metric': 'ndcg', 'eval_at': [1, 3, 5],
                        'boosting_type': 'gbdt', 'verbose': -1, 'random_state': 42,
                        'label_gain': list(range(100))})
    
    model = lgb.LGBMRanker(**best_params, n_estimators=1000)
    model.fit(train_df[feature_cols], train_df['target'], group=group_train,
              eval_set=[(valid_df[feature_cols], valid_df['target'])], eval_group=[group_valid],
              callbacks=[lgb.early_stopping(50, verbose=True)])
    
    return model, best_params


def train_binary(train_df, valid_df, feature_cols, n_trials=20):
    """Binary Classification学習"""
    # ターゲット（勝敗）
    train_df = train_df.copy()
    train_df['target'] = (train_df['finish_position'] == 1).astype(int)
    valid_df = valid_df.copy()
    valid_df['target'] = (valid_df['finish_position'] == 1).astype(int)
    
    def objective(trial):
        params = {
            'objective': 'binary', 'metric': 'binary_logloss',
            'boosting_type': 'gbdt', 'verbose': -1, 'random_state': 42,
            'num_leaves': trial.suggest_int('num_leaves', 30, 100),
            'lambda_l1': trial.suggest_float('lambda_l1', 0.01, 5.0, log=True),
            'lambda_l2': trial.suggest_float('lambda_l2', 0.01, 5.0, log=True),
            'min_child_samples': trial.suggest_int('min_child_samples', 20, 140),
            'learning_rate': trial.suggest_float('learning_rate', 0.02, 0.12, log=True),
        }
        model = lgb.LGBMClassifier(**params, n_estimators=500)
        model.fit(train_df[feature_cols], train_df['target'],
                  eval_set=[(valid_df[feature_cols], valid_df['target'])],
                  callbacks=[lgb.early_stopping(30, verbose=False)])
        valid_df['score'] = model.predict_proba(valid_df[feature_cols])[:, 1]
        return calculate_roi(valid_df)
    
    optuna.logging.set_verbosity(optuna.logging.WARNING)
    study = optuna.create_study(direction='maximize')
    study.optimize(objective, n_trials=n_trials, show_progress_bar=True)
    
    best_params = study.best_params
    best_params.update({'objective': 'binary', 'metric': 'binary_logloss',
                        'boosting_type': 'gbdt', 'verbose': -1, 'random_state': 42})
    
    model = lgb.LGBMClassifier(**best_params, n_estimators=1000)
    model.fit(train_df[feature_cols], train_df['target'],
              eval_set=[(valid_df[feature_cols], valid_df['target'])],
              callbacks=[lgb.early_stopping(50, verbose=True)])
    
    return model, best_params


def main():
    logging.info("="*60)
    logging.info("目的関数比較: LambdaRank vs Binary Classification")
    logging.info("="*60)
    
    # データ読み込み
    base_dir = Path('keibaai/models/mu_v3_3')
    df = pd.read_parquet(base_dir / 'train_data_mu_v3_3.parquet')
    df['race_date'] = pd.to_datetime(df['race_date'])
    
    with open(base_dir / 'feature_names.json', 'r', encoding='utf-8') as f:
        feature_cols = json.load(f)
    feature_cols = [f for f in feature_cols if f in df.columns]
    
    train_df = df[df['race_date'] < '2023-01-01'].copy()
    valid_df = df[(df['race_date'] >= '2023-01-01') & (df['race_date'] < '2024-01-01')].copy()
    test_df = df[df['race_date'] >= '2024-01-01'].copy()
    
    logging.info(f"Train: {len(train_df):,}, Valid: {len(valid_df):,}, Test: {len(test_df):,}")
    
    # === LambdaRank ===
    logging.info("\n【A】LambdaRank学習中...")
    model_lr, params_lr = train_lambdarank(train_df, valid_df, feature_cols, n_trials=15)
    
    valid_df['score_lr'] = model_lr.predict(valid_df[feature_cols])
    test_df['score_lr'] = model_lr.predict(test_df[feature_cols])
    
    lr_valid_roi = calculate_roi(valid_df, 'score_lr')
    lr_test_roi = calculate_roi(test_df, 'score_lr')
    lr_brier = calculate_brier_score(test_df, 'score_lr')
    
    # === Binary Classification ===
    logging.info("\n【B】Binary Classification学習中...")
    model_bc, params_bc = train_binary(train_df, valid_df, feature_cols, n_trials=15)
    
    valid_df['score_bc'] = model_bc.predict_proba(valid_df[feature_cols])[:, 1]
    test_df['score_bc'] = model_bc.predict_proba(test_df[feature_cols])[:, 1]
    
    bc_valid_roi = calculate_roi(valid_df, 'score_bc')
    bc_test_roi = calculate_roi(test_df, 'score_bc')
    bc_brier = calculate_brier_score(test_df, 'score_bc')
    
    # === 結果比較 ===
    logging.info("\n" + "="*60)
    logging.info("【結果比較】")
    logging.info("="*60)
    logging.info(f"| 目的関数 | Valid ROI | Test ROI | Brier Score |")
    logging.info(f"|----------|-----------|----------|-------------|")
    logging.info(f"| LambdaRank | {lr_valid_roi:.2%} | {lr_test_roi:.2%} | {lr_brier:.4f} |")
    logging.info(f"| Binary | {bc_valid_roi:.2%} | {bc_test_roi:.2%} | {bc_brier:.4f} |")
    
    winner = "LambdaRank" if lr_test_roi > bc_test_roi else "Binary"
    logging.info(f"\n勝者: {winner}")
    
    # 結果保存
    output = {
        'lambdarank': {'valid_roi': lr_valid_roi, 'test_roi': lr_test_roi, 'brier_score': lr_brier},
        'binary': {'valid_roi': bc_valid_roi, 'test_roi': bc_test_roi, 'brier_score': bc_brier},
        'winner': winner,
    }
    output_dir = Path('keibaai/models/objective_comparison')
    output_dir.mkdir(parents=True, exist_ok=True)
    with open(output_dir / 'results.json', 'w', encoding='utf-8') as f:
        json.dump(output, f, ensure_ascii=False, indent=2)
    
    logging.info(f"\n保存完了: {output_dir}")


if __name__ == "__main__":
    main()
