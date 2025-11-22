#!/usr/bin/env python3
# keibaai/src/models/optimize_hyperparameters.py

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
from sklearn.metrics import mean_squared_error
import joblib

# Add project root to path
project_root = Path(__file__).resolve().parent.parent.parent
sys.path.append(str(project_root))

from src.features.feature_engine import FeatureEngine
from src.utils.data_utils import load_parquet_data_by_date

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
        raise FileNotFoundError(f"Features directory not found at {features_dir}. Please run feature generation first.")
        
    # Use robust loader
    start_dt = pd.to_datetime(start_date) if start_date else None
    end_dt = pd.to_datetime(end_date) if end_date else None
    
    try:
        df = load_parquet_data_by_date(features_dir, start_dt, end_dt, date_col='race_date')
        if df.empty:
             raise ValueError("Loaded dataframe is empty.")
        return df
    except Exception as e:
        logging.error(f"Failed to load parquet data: {e}")
        raise

def objective(trial, X, y, group=None, categorical_features=None):
    """Optuna objective function"""
    
    params = {
        'objective': 'regression',
        'metric': 'rmse',
        'verbosity': -1,
        'boosting_type': 'gbdt',
        'random_state': 42,  # Reproducibility
        # Optimized search ranges to prevent overfitting and reduce search time
        'n_estimators': trial.suggest_int('n_estimators', 100, 2000),  # Reduced from 5000
        'learning_rate': trial.suggest_float('learning_rate', 0.005, 0.1, log=True),
        'num_leaves': trial.suggest_int('num_leaves', 20, 150),  # Reduced from 300
        'max_depth': trial.suggest_int('max_depth', 3, 12),  # Reduced from 15
        'min_child_samples': trial.suggest_int('min_child_samples', 10, 100),  # Increased min from 5
        'subsample': trial.suggest_float('subsample', 0.6, 1.0),  # Tightened range
        'colsample_bytree': trial.suggest_float('colsample_bytree', 0.6, 1.0),  # Tightened range
        'reg_alpha': trial.suggest_float('reg_alpha', 1e-8, 1.0, log=True),  # Reduced from 10.0
        'reg_lambda': trial.suggest_float('reg_lambda', 1e-8, 1.0, log=True),  # Reduced from 10.0
    }
    
    # TimeSeriesSplit for validation
    tscv = TimeSeriesSplit(n_splits=5)
    scores = []
    
    for train_idx, valid_idx in tscv.split(X):
        X_train, X_valid = X.iloc[train_idx], X.iloc[valid_idx]
        y_train, y_valid = y.iloc[train_idx], y.iloc[valid_idx]
        
        model = lgb.LGBMRegressor(**params)
        
        callbacks = [
            lgb.early_stopping(stopping_rounds=20, verbose=False),  # Reduced from 50 to prevent overfitting
            lgb.log_evaluation(period=0) # Disable logging
        ]
        
        model.fit(
            X_train, y_train,
            eval_set=[(X_valid, y_valid)],
            eval_metric='rmse',
            callbacks=callbacks,
            categorical_feature=categorical_features if categorical_features else 'auto'
        )
        
        preds = model.predict(X_valid)
        rmse = np.sqrt(mean_squared_error(y_valid, preds))
        scores.append(rmse)
        
    return np.mean(scores)

def main():
    parser = argparse.ArgumentParser(description='Optimize Hyperparameters with Optuna')
    parser.add_argument('--trials', type=int, default=50, help='Number of trials')
    parser.add_argument('--output', type=str, default='configs/models.yaml', help='Output config file')
    parser.add_argument('--start_date', type=str, default='2020-01-01', help='Start date')
    parser.add_argument('--end_date', type=str, default='2023-12-31', help='End date')
    args = parser.parse_args()
    
    setup_logging()
    logger = logging.getLogger(__name__)
    
    # Load Config to get data path
    config_path = project_root / "configs" / "default.yaml"
    with open(config_path, 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)
    
    data_path = project_root / config.get('data_path', 'data')
    
    try:
        # Load features
        features_df = load_data(data_path, args.start_date, args.end_date)
        
        # We also need targets! 
        # Features usually don't have targets (unless leaked).
        # We need to load races.parquet to get targets.
        races_path = data_path / "parsed" / "parquet" / "races" / "races.parquet"
        if not races_path.exists():
             raise FileNotFoundError(f"Races file not found at {races_path}")
             
        races_df = pd.read_parquet(races_path)
        
        # Merge
        merge_keys = ['race_id', 'horse_id']
        # Ensure keys are strings and remove invalid values
        for key in merge_keys:
            if key in features_df.columns:
                features_df[key] = features_df[key].astype(str).str.strip()
                # Remove rows where NaN became string 'nan' (critical for data integrity)
                features_df = features_df[features_df[key] != 'nan']
            if key in races_df.columns:
                races_df[key] = races_df[key].astype(str).str.strip()
                races_df = races_df[races_df[key] != 'nan']

        # Deduplicate features (Crucial for memory usage)
        if features_df.duplicated(subset=merge_keys).any():
            logger.warning(f"Found {features_df.duplicated(subset=merge_keys).sum()} duplicates in features. Dropping...")
            features_df = features_df.drop_duplicates(subset=merge_keys, keep='first')

        target_col = 'finish_time_seconds'
        # Select only necessary columns from races
        races_subset = races_df[merge_keys + [target_col]]
        
        # Drop target from features if exists to avoid collision
        drop_targets = ['finish_position', 'finish_time_seconds']
        features_df = features_df.drop(columns=[c for c in drop_targets if c in features_df.columns], errors='ignore')
        
        df = pd.merge(features_df, races_subset, on=merge_keys, how='inner')
        logger.info(f"Merged data shape: {df.shape}")
        
        # Preprocessing
        logger.info("Preprocessing data...")
        # Sort by date for TimeSeriesSplit
        # CRITICAL: Reset index to prevent data leakage in TimeSeriesSplit
        if 'race_date' in df.columns:
            df = df.sort_values('race_date').reset_index(drop=True)
            logger.info("データを日付順にソートし、インデックスをリセットしました（データリーケージ防止）")
        
        # Load feature names from the same YAML file used by train_mu_model.py
        feature_names_yaml = data_path / "features" / "parquet" / "feature_names.yaml"
        if not feature_names_yaml.exists():
            logger.error(f"Feature names file not found: {feature_names_yaml}")
            logger.error("Please run generate_features.py first.")
            raise FileNotFoundError(f"Feature names file not found: {feature_names_yaml}")
        
        with open(feature_names_yaml, 'r', encoding='utf-8') as f:
            all_feature_names = yaml.safe_load(f)
        
        # Use only features that exist in the dataframe
        feature_cols = [c for c in all_feature_names if c in df.columns]
        missing_features = [c for c in all_feature_names if c not in df.columns]
        
        if missing_features:
            logger.warning(f"{len(missing_features)} features from YAML not found in dataframe (will be skipped)")
        
        logger.info(f"Using {len(feature_cols)} features from feature_names.yaml")
        
        target_col = 'finish_time_seconds'
        
        # Drop NaN in critical columns (must match train_mu_model.py)
        # race_id is critical for TimeSeriesSplit and preventing data leakage
        required_cols = [target_col, 'race_id']
        df = df.dropna(subset=required_cols)
        
        # ===== CRITICAL: Same preprocessing as train_mu_model.py =====
        # Convert object columns to numeric (handles encoded categorical features)
        for col in feature_cols:
            if col in df.columns and df[col].dtype == 'object':
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # *** DO NOT fill NaN with 0 - LightGBM handles missing values better natively ***
        # LightGBM will automatically learn the optimal direction for missing values
        logger.info("特徴量を数値型に変換しました。欠損値はLightGBMが自動処理します。")
        # ===== End of preprocessing =====
        
        X = df[feature_cols]
        y = df[target_col]
        
        # Detect categorical features (same logic as train_mu_model.py)
        categorical_features = [col for col in feature_cols if 
                               col.startswith('sex_') or 
                               col.startswith('trainer_') or
                               col.startswith('jockey_') or
                               '_' in col and col.split('_')[0] in ['surface', 'weather', 'grade']]
        
        if categorical_features:
            logger.info(f"カテゴリカル特徴量を検出: {len(categorical_features)}個")
        
        logger.info(f"Starting optimization with {len(X)} samples and {len(feature_cols)} features.")
        
        # Suppress Optuna logging to avoid interfering with tqdm
        optuna.logging.set_verbosity(optuna.logging.WARNING)
        
        study = optuna.create_study(direction='minimize')
        
        from tqdm import tqdm
        with tqdm(total=args.trials, desc="Optimization Progress") as pbar:
            def tqdm_callback(study, trial):
                pbar.update(1)
                if study.best_trial:
                    pbar.set_postfix(best_rmse=f"{study.best_value:.4f}")

            study.optimize(lambda trial: objective(trial, X, y, categorical_features=categorical_features), 
                          n_trials=args.trials, callbacks=[tqdm_callback])
        
        logger.info("Optimization finished.")
        logger.info(f"Best trial: {study.best_trial.value}")
        logger.info(f"Best params: {study.best_trial.params}")
        
        # Save best params
        best_params = study.best_trial.params
        
        # Load existing models.yaml or create new
        models_config_path = project_root / args.output
        if models_config_path.exists():
            with open(models_config_path, 'r', encoding='utf-8') as f:
                models_config = yaml.safe_load(f)
        else:
            models_config = {}
            
        # Update config
        if 'mu_estimator' not in models_config:
            models_config['mu_estimator'] = {}
            
        models_config['mu_estimator']['regressor_params'] = best_params
        # For ranker, we might want to use similar params or optimize separately. 
        # For now, we'll apply the same basic params but keep ranker specific objective
        ranker_params = best_params.copy()
        ranker_params['objective'] = 'lambdarank'
        ranker_params['metric'] = 'ndcg'
        if 'reg_alpha' in ranker_params: del ranker_params['reg_alpha']
        if 'reg_lambda' in ranker_params: del ranker_params['reg_lambda']
        
        models_config['mu_estimator']['ranker_params'] = ranker_params
    
        with open(models_config_path, 'w', encoding='utf-8') as f:
            yaml.dump(models_config, f, default_flow_style=False)
            
        logger.info(f"Saved best parameters to {models_config_path}")
        
    except Exception as e:
        logger.error(f"Optimization failed: {e}", exc_info=True)

if __name__ == "__main__":
    main()
