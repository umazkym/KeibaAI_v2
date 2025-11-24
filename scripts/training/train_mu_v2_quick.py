# -*- coding: utf-8 -*-
import sys
from pathlib import Path

# プロジェクトルートをsys.pathに追加
project_root = Path(__file__).resolve().parent.parent
sys.path.append(str(project_root))
import pandas as pd
import lightgbm as lgb
import optuna
import pickle
import logging
from pathlib import Path
from sklearn.metrics import roc_auc_score
import matplotlib.pyplot as plt
import sys

# 日本語フォント設定（Windows向け）
plt.rcParams['font.family'] = 'Meiryo'

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def train_quick():
    models_dir = Path('keibaai/models/mu_v2')
    data_path = models_dir / 'train_data_mu_v2.parquet'
    
    if not data_path.exists():
        logging.error("Data not found!")
        return

    logging.info("Loading dataset...")
    df = pd.read_parquet(data_path)
    
    # Sort by date
    df['race_date'] = pd.to_datetime(df['race_date'])
    df = df.sort_values('race_date')
    
    # Re-create target if missing
    if 'is_win' not in df.columns and 'finish_position' in df.columns:
        logging.info("Re-creating 'is_win' target from 'finish_position'...")
        df['is_win'] = (df['finish_position'] == 1).astype(int)
    
    # Split Data
    # Train: 2020-2022
    # Val: 2023
    # Test: 2024-
    train_df = df[(df['race_date'] >= '2020-01-01') & (df['race_date'] <= '2022-12-31')]
    val_df = df[(df['race_date'] >= '2023-01-01') & (df['race_date'] <= '2023-12-31')]
    
    logging.info(f"Train: {len(train_df):,}, Val: {len(val_df):,}")
    
    # Features & Target
    target_col = 'is_win' # 1着予測
    exclude_cols = [
        'race_id', 'horse_id', 'race_date', 'finish_position', 'finish_time_seconds',
        'win_odds', 'prize_money', 'jockey_id', 'trainer_id', 'owner_id', 'sire_id', 'damsire_id',
        'race_name', 'horse_name', 'jockey_name', 'trainer_name', 'is_win',
        # Leakage columns (Future info - Race Results)
        'passing_order_1', 'passing_order_2', 'passing_order_3', 'passing_order_4',
        'last_3f_time', 'time_except_last3f', 'position_change_1_2',
        'passing_order2_std', 'passing_order4_std',
        'win_probability',  # 確定オッズ由来
        'final_corner_to_finish',  # 最終コーナー→ゴールの順位変動（完全リーク）
        'pace_index',  # レースペース指数（完全リーク）
        'finish_time_seconds',  # ゴールタイム（完全リーク）
    ]
    feature_cols = [c for c in df.columns if c not in exclude_cols]
    
    logging.info(f"Features: {len(feature_cols)}")
    
    # Dataset
    train_data = lgb.Dataset(train_df[feature_cols], label=train_df[target_col])
    val_data = lgb.Dataset(val_df[feature_cols], label=val_df[target_col], reference=train_data)
    
    def objective(trial):
        params = {
            'objective': 'binary',
            'metric': 'auc',
            'verbosity': -1,
            'boosting_type': 'gbdt',
            'feature_pre_filter': False, # Fix for Optuna
            'lambda_l1': trial.suggest_float('lambda_l1', 1e-8, 10.0, log=True),
            'lambda_l2': trial.suggest_float('lambda_l2', 1e-8, 10.0, log=True),
            'num_leaves': trial.suggest_int('num_leaves', 2, 256),
            'feature_fraction': trial.suggest_float('feature_fraction', 0.4, 1.0),
            'bagging_fraction': trial.suggest_float('bagging_fraction', 0.4, 1.0),
            'bagging_freq': trial.suggest_int('bagging_freq', 1, 7),
            'min_child_samples': trial.suggest_int('min_child_samples', 5, 100),
        }
        
        callbacks = [
            lgb.early_stopping(stopping_rounds=50),
            lgb.log_evaluation(period=0) # Quiet
        ]
        
        model = lgb.train(
            params,
            train_data,
            valid_sets=[val_data],
            num_boost_round=1000,
            callbacks=callbacks
        )
        
        # Get best score
        return model.best_score['valid_0']['auc']

    logging.info("Starting Optuna tuning...")
    study = optuna.create_study(direction='maximize')
    study.optimize(objective, n_trials=30)
    
    logging.info(f"Best params: {study.best_params}")
    logging.info(f"Best AUC: {study.best_value:.4f}")
    
    # Train Final Model with Best Params
    best_params = study.best_params
    best_params.update({
        'objective': 'binary',
        'metric': 'auc',
        'verbosity': -1,
        'boosting_type': 'gbdt',
        'feature_pre_filter': False
    })
    
    logging.info("Training final model...")
    final_model = lgb.train(
        best_params,
        train_data,
        valid_sets=[val_data],
        num_boost_round=1000,
        callbacks=[lgb.early_stopping(stopping_rounds=50), lgb.log_evaluation(period=100)]
    )
    
    # Save Model
    with open(models_dir / 'mu_v2_model.pkl', 'wb') as f:
        pickle.dump(final_model, f)
    
    # Save Feature Importance
    importance = pd.DataFrame({
        'feature': feature_cols,
        'importance': final_model.feature_importance(importance_type='gain')
    }).sort_values('importance', ascending=False)
    importance.to_csv(models_dir / 'feature_importance.csv', index=False)
    
    logging.info("Done!")

if __name__ == "__main__":
    train_quick()
