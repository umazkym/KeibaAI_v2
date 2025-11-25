#!/usr/bin/env python3
# Ranker-specific hyperparameter optimization with NDCG objective

import argparse
import logging
import sys
import yaml
from pathlib import Path
import pandas as pd
import numpy as np
import lightgbm as lgb
import optuna
from sklearn.model_selection import TimeSeriesSplit
import joblib

# Add project root to path
project_root = Path(__file__).resolve().parent.parent.parent
sys.path.append(str(project_root))

from keibaai.src.utils.data_utils import load_parquet_data_by_date

def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)]
    )

def load_data(data_path: Path, start_date: str = None, end_date: str = None):
    """Load and prepare data for optimization"""
    logging.info(f"Loading data from {data_path}...")
    
    features_dir = data_path / "features" / "parquet"
    
    if not features_dir.exists():
        raise FileNotFoundError(f"Features directory not found at {features_dir}")
        
    start_dt = pd.to_datetime(start_date) if start_date else None
    end_dt = pd.to_datetime(end_date) if end_date else None
    
    try:
        df = load_parquet_data_by_date(features_dir, start_dt, end_dt, date_col='race_date')
        if df.empty:
             raise ValueError("Loaded dataframe is empty")
        return df
    except Exception as e:
        logging.error(f"Failed to load parquet data: {e}")
        raise

def objective_ranker(trial, X, y, group, categorical_features=None):
    """Optuna objective function for LambdaRank (NDCG optimization)"""
    
    params = {
        'objective': 'lambdarank',
        'metric': 'ndcg',
        'verbosity': -1,
        'boosting_type': 'gbdt',
        'random_state': 42,
        # Ranker-specific optimization ranges
        'n_estimators': trial.suggest_int('n_estimators', 500, 3000),
        'learning_rate': trial.suggest_float('learning_rate', 0.005, 0.05, log=True),
        'num_leaves': trial.suggest_int('num_leaves', 31, 200),
        'max_depth': trial.suggest_int('max_depth', 5, 15),
        'min_child_samples': trial.suggest_int('min_child_samples', 10, 100),
        'subsample': trial.suggest_float('subsample', 0.6, 1.0),
        'colsample_bytree': trial.suggest_float('colsample_bytree', 0.6, 1.0),
        'reg_alpha': trial.suggest_float('reg_alpha', 1e-8, 1.0, log=True),
        'reg_lambda': trial.suggest_float('reg_lambda', 1e-8, 1.0, log=True),
    }
    
    # TimeSeriesSplit for validation
    tscv = TimeSeriesSplit(n_splits=5)
    scores = []
    
    for train_idx, valid_idx in tscv.split(X):
        X_train, X_valid = X.iloc[train_idx], X.iloc[valid_idx]
        y_train, y_valid = y.iloc[train_idx], y.iloc[valid_idx]
        group_train = group.iloc[train_idx]
        group_valid = group.iloc[valid_idx]
        
        model = lgb.LGBMRanker(**params)
        
        callbacks = [
            lgb.early_stopping(stopping_rounds=50, verbose=False),
            lgb.log_evaluation(period=0)
        ]
        
        model.fit(
            X_train, y_train,
            group=group_train.values,
            eval_set=[(X_valid, y_valid)],
            eval_group=[group_valid.values],
            eval_metric='ndcg',
            callbacks=callbacks,
            categorical_feature=categorical_features if categorical_features else 'auto'
        )
        
        # Get NDCG score
        # LightGBM's best_score_ contains the validation metric
        ndcg = model.best_score_['valid_0']['ndcg@5']
        scores.append(ndcg)
        
    # We want to MAXIMIZE NDCG, but Optuna minimizes by default
    # So we return negative NDCG
    return -np.mean(scores)

def main():
    parser = argparse.ArgumentParser(description='Optimize Ranker Hyperparameters')
    parser.add_argument('--trials', type=int, default=50, help='Number of trials')
    parser.add_argument('--output', type=str, default='keibaai/configs/optimized_ranker_params.yaml', help='Output config file')
    parser.add_argument('--start_date', type=str, default='2020-01-01', help='Start date')
    parser.add_argument('--end_date', type=str, default='2023-12-31', help='End date')
    args = parser.parse_args()
    
    setup_logging()
    logger = logging.getLogger(__name__)
    
    # Load Config
    config_path = project_root / "keibaai" / "configs" / "default.yaml"
    with open(config_path, 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)
    
    data_path = project_root / config.get('data_path', 'data')
    
    try:
        # Load features
        features_df = load_data(data_path, args.start_date, args.end_date)
        
        # Load races for targets
        races_path = data_path / "parsed" / "parquet" / "races" / "races.parquet"
        if not races_path.exists():
             raise FileNotFoundError(f"Races file not found at {races_path}")
             
        races_df = pd.read_parquet(races_path)
        
        # Merge
        merge_keys = ['race_id', 'horse_id']
        for key in merge_keys:
            if key in features_df.columns:
                features_df[key] = features_df[key].astype(str).str.strip()
                features_df = features_df[features_df[key] != 'nan']
            if key in races_df.columns:
                races_df[key] = races_df[key].astype(str).str.strip()
                races_df = races_df[races_df[key] != 'nan']

        if features_df.duplicated(subset=merge_keys).any():
            logger.warning(f"Found duplicates. Dropping...")
            features_df = features_df.drop_duplicates(subset=merge_keys, keep='first')

        # For ranker, we need finish_position as target
        target_col = 'finish_position'
        races_subset = races_df[merge_keys + [target_col]]
        
        # Drop target from features
        drop_targets = ['finish_position', 'finish_time_seconds']
        features_df = features_df.drop(columns=[c for c in drop_targets if c in features_df.columns], errors='ignore')
        
        df = pd.merge(features_df, races_subset, on=merge_keys, how='inner')
        logger.info(f"Merged data shape: {df.shape}")
        
        # Sort by date and reset index
        if 'race_date' in df.columns:
            df = df.sort_values('race_date').reset_index(drop=True)
            logger.info("Sorted by date and reset index")
        
        # Load feature names
        feature_names_yaml = data_path / "features" / "parquet" / "feature_names.yaml"
        with open(feature_names_yaml, 'r', encoding='utf-8') as f:
            all_feature_names = yaml.safe_load(f)
        
        feature_cols = [c for c in all_feature_names if c in df.columns]
        logger.info(f"Using {len(feature_cols)} features")
        
        # Prepare group (race_id counts for LambdaRank)
        race_id_counts = df.groupby('race_id').size()
        logger.info(f"Number of races: {len(race_id_counts)}")
        
        # Drop NaN
        required_cols = [target_col, 'race_id']
        df = df.dropna(subset=required_cols)
        
        # Convert to numeric
        for col in feature_cols:
            if col in df.columns and df[col].dtype == 'object':
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        logger.info("Preprocessing complete")
        
        X = df[feature_cols]
        y = df[target_col]
        
        # Create group array for LightGBM Ranker
        # Group is the count of samples per race
        group_counts = df.groupby('race_id').size()
        logger.info(f"Group counts shape: {len(group_counts)}")
        
        # Detect categorical features
        categorical_features = [col for col in feature_cols if 
                               col.startswith('sex_') or 
                               col.startswith('trainer_') or
                               col.startswith('jockey_') or
                               '_' in col and col.split('_')[0] in ['surface', 'weather', 'grade']]
        
        if categorical_features:
            logger.info(f"Categorical features: {len(categorical_features)}")
        
        logger.info(f"Starting ranker optimization with {len(X)} samples")
        
        # Suppress Optuna logging
        optuna.logging.set_verbosity(optuna.logging.WARNING)
        
        study = optuna.create_study(direction='minimize')  # Minimize negative NDCG = maximize NDCG
        
        from tqdm import tqdm
        with tqdm(total=args.trials, desc="Ranker Optimization") as pbar:
            def tqdm_callback(study, trial):
                pbar.update(1)
                if study.best_trial:
                    pbar.set_postfix(best_ndcg=f"{-study.best_value:.4f}")

            study.optimize(lambda trial: objective_ranker(trial, X, y, group_counts, categorical_features=categorical_features), 
                          n_trials=args.trials, callbacks=[tqdm_callback])
        
        logger.info("Optimization finished")
        logger.info(f"Best NDCG: {-study.best_value:.4f}")
        logger.info(f"Best params: {study.best_trial.params}")
        
        # Save best params
        best_params = study.best_trial.params
        best_params['objective'] = 'lambdarank'
        best_params['metric'] = 'ndcg'
        
        models_config_path = project_root / args.output
        models_config = {
            'mu_estimator': {
                'ranker_params': best_params,
                'best_ndcg': float(-study.best_value)
            }
        }
    
        with open(models_config_path, 'w', encoding='utf-8') as f:
            yaml.dump(models_config, f, default_flow_style=False)
            
        logger.info(f"Saved best ranker parameters to {models_config_path}")
        
    except Exception as e:
        logger.error(f"Optimization failed: {e}", exc_info=True)

if __name__ == "__main__":
    main()
