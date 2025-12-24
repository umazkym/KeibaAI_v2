#!/usr/bin/env python3
# Train calibrator only for existing mu v2.1_new_features model

import sys
import logging
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime
import joblib
import json

# Add project root to path
project_root = Path(__file__).resolve().parent.parent.parent
sys.path.append(str(project_root))

from keibaai.src.models.calibration import IsotonicCalibrator

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def main():
    logger.info("="*60)
    logger.info("Probability Calibration Training")
    logger.info("="*60)
    
    # Load trained model
    model_dir = Path('models/mu_v2.1_new_features')
    logger.info(f"Loading model from {model_dir}")
    
    ranker = joblib.load(model_dir / 'ranker.pkl')
    with open(model_dir / 'feature_names.json', 'r', encoding='utf-8') as f:
        feature_names = json.load(f)
    
    logger.info(f"Loaded ranker with {len(feature_names)} features")
    
    # Load training data (2020-2023)
    features_dir = Path('keibaai/data/features/parquet')
    
    # Load all years
    dfs = []
    for year in [2020, 2021, 2022, 2023]:
        year_path = features_dir / f'year={year}'
        if year_path.exists():
            logger.info(f"Loading {year} data...")
            df_year = pd.read_parquet(year_path)
            dfs.append(df_year)
    
    df = pd.concat(dfs, ignore_index=True)
    logger.info(f"Loaded {len(df)} rows")
    
    # Filter by date
    if 'race_date' in df.columns:
        df['race_date'] = pd.to_datetime(df['race_date'])
        df = df[(df['race_date'] >= '2020-01-01') & (df['race_date'] <= '2023-12-31')]
        logger.info(f"Filtered to {len(df)} rows (2020-2023)")
    
    # Check for target
    if 'finish_position' not in df.columns:
        logger.error("finish_position not found. Need to merge with races.parquet")
        
        # Load races
        races_path = Path('keibaai/data/parsed/parquet/races/races.parquet')
        races_df = pd.read_parquet(races_path)
        
        # Clean keys
        df['race_id'] = df['race_id'].astype(str).str.strip()
        df['horse_id'] = df['horse_id'].astype(str).str.strip()
        races_df['race_id'] = races_df['race_id'].astype(str).str.strip()
        races_df['horse_id'] = races_df['horse_id'].astype(str).str.strip()
        
        # Merge
        races_subset = races_df[['race_id', 'horse_id', 'finish_position']]
        df = df.merge(races_subset, on=['race_id', 'horse_id'], how='inner')
        logger.info(f"Merged with races: {len(df)} rows")
    
    # Sort by date
    df = df.sort_values('race_date').reset_index(drop=True)
    
    # Split for calibration (70:30)
    split_idx = int(len(df) * 0.7)
    train_df = df.iloc[:split_idx]
    val_df = df.iloc[split_idx:]
    
    logger.info(f"Split: Train={len(train_df)}, Validation={len(val_df)}")
    
    # Prepare validation data
    val_features = [col for col in feature_names if col in val_df.columns]
    missing = [col for col in feature_names if col not in val_df.columns]
    
    if missing:
        logger.warning(f"{len(missing)} features missing, filling with 0")
        for col in missing:
            val_df[col] = 0.0
    
    # Convert to numeric
    for col in val_features:
        if col in val_df.columns and val_df[col].dtype == 'object':
            val_df[col] = pd.to_numeric(val_df[col], errors='coerce').fillna(0)
    
    # Drop rows with NA in finish_position
    val_df = val_df.dropna(subset=['finish_position'])
    logger.info(f"After dropping NAs: {len(val_df)} rows")
    
    X_val = val_df[feature_names]
    y_val_is_win = (val_df['finish_position'] == 1).astype(int).values
    
    logger.info("Generating predictions...")
    y_val_pred_score = ranker.predict(X_val)
    
    logger.info(f"Prediction range: [{y_val_pred_score.min():.4f}, {y_val_pred_score.max():.4f}]")
    
    # Train calibrator
    logger.info("Training Isotonic Calibrator...")
    calibrator = IsotonicCalibrator()
    calibrator.fit(
        y_true=y_val_is_win,
        y_pred=y_val_pred_score
    )
    
    logger.info(f"Calibrator trained successfully")
    logger.info(f"  Brier Score Before: {calibrator.brier_score_before:.6f}")
    logger.info(f"  Brier Score After: {calibrator.brier_score_after:.6f}")
    logger.info(f"  Improvement: {(calibrator.brier_score_before - calibrator.brier_score_after):.6f}")
    
    # Save calibrator
    calibrator_path = model_dir / 'calibrator.pkl'
    calibrator.save(calibrator_path)
    logger.info(f"Saved calibrator to {calibrator_path}")
    
    # Update model_info.json
    model_info_path = model_dir / 'model_info.json'
    with open(model_info_path, 'r', encoding='utf-8') as f:
        model_info = json.load(f)
    
    model_info['calibrated'] = True
    model_info['calibration_split'] = '70:30'
    model_info['brier_score_before'] = float(calibrator.brier_score_before)
    model_info['brier_score_after'] = float(calibrator.brier_score_after)
    model_info['calibration_date'] = datetime.now().isoformat()
    
    with open(model_info_path, 'w', encoding='utf-8') as f:
        json.dump(model_info, f, indent=2, ensure_ascii=False)
    
    logger.info("Updated model_info.json")
    logger.info("="*60)
    logger.info("Calibration training complete!")
    logger.info("="*60)

if __name__ == '__main__':
    main()
