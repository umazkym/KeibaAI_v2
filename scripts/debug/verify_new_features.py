import pandas as pd
import numpy as np
import sys
import os
from pathlib import Path
import logging

# Add project root to path
project_root = Path(__file__).resolve().parent.parent
sys.path.append(str(project_root))

from keibaai.src.features.feature_engine import FeatureEngine

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def verify_features():
    logger.info("Starting feature verification...")

    # Load data
    data_dir = project_root / 'keibaai' / 'data' / 'parsed' / 'parquet'
    
    try:
        logger.info("Loading data...")
        shutuba_df = pd.read_parquet(data_dir / 'shutuba' / 'shutuba.parquet')
        results_df = pd.read_parquet(data_dir / 'horses_performance' / 'horses_performance.parquet')
        horses_df = pd.read_parquet(data_dir / 'horses' / 'horses.parquet')
        races_df = pd.read_parquet(data_dir / 'races' / 'races.parquet')
        
        pedigree_path = data_dir / 'pedigrees' / 'pedigrees.parquet'
        if pedigree_path.exists():
            pedigree_df = pd.read_parquet(pedigree_path)
        else:
            logger.warning("pedigrees.parquet not found. Creating dummy.")
            pedigree_df = pd.DataFrame({
                'horse_id': shutuba_df['horse_id'].unique(),
                'generation': 1, 'ancestor_id': 'sire_1', 'ancestor_name': 'Test Sire'
            })

        # --- Data Preparation ---
        logger.info("Preparing data...")

        # 1. Add venue to shutuba from races
        if 'venue' not in shutuba_df.columns:
            race_venue_map = races_df[['race_id', 'venue']].drop_duplicates()
            shutuba_df = shutuba_df.merge(race_venue_map, on='race_id', how='left')
            logger.info(f"Added venue to shutuba. Missing venues: {shutuba_df['venue'].isna().sum()}")

        # 2. Fix trainer_id in results (horses_performance)
        if 'trainer_id' in results_df.columns:
            if results_df['trainer_id'].isna().mean() > 0.5 or (results_df['trainer_id'] == '').mean() > 0.5:
                logger.info("Filling missing trainer_id in results from horses_df...")
                results_df = results_df.drop(columns=['trainer_id'], errors='ignore')
                results_df = results_df.merge(horses_df[['horse_id', 'trainer_id']], on='horse_id', how='left')
        else:
            results_df = results_df.merge(horses_df[['horse_id', 'trainer_id']], on='horse_id', how='left')

        # 3. Clean venue in results
        import re
        def clean_venue(v):
            if not isinstance(v, str): return v
            return re.sub(r'\d+', '', v)

        if 'venue' in results_df.columns:
            results_df['venue'] = results_df['venue'].apply(clean_venue)
            logger.info(f"Cleaned venues in results: {results_df['venue'].unique()[:5]}")

        # 4. Ensure race_date is datetime
        shutuba_df['race_date'] = pd.to_datetime(shutuba_df['race_date'])
        results_df['race_date'] = pd.to_datetime(results_df['race_date'])

    except Exception as e:
        logger.error(f"Failed to load/prepare data: {e}", exc_info=True)
        return

    # Initialize FeatureEngine
    config_path = project_root / 'config' / 'features.yaml'
    if not config_path.exists():
         logger.warning("features.yaml not found. Using default config.")
         config = {'feature_recipes': {}} 
    else:
        import yaml
        with open(config_path, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)

    engine = FeatureEngine(config)

    # Diagnostics
    logger.info(f"Shutuba date range: {shutuba_df['race_date'].min()} to {shutuba_df['race_date'].max()}")
    logger.info(f"Results date range: {results_df['race_date'].min()} to {results_df['race_date'].max()}")

    common_horses = set(shutuba_df['horse_id']).intersection(set(results_df['horse_id']))
    logger.info(f"Common horses: {len(common_horses)} / {shutuba_df['horse_id'].nunique()}")
    
    # Run generation on a subset for speed
    sample_size = 1000
    if len(shutuba_df) > sample_size:
        shutuba_sample = shutuba_df.sort_values('race_date', ascending=False).head(sample_size).copy()
    else:
        shutuba_sample = shutuba_df.copy()

    logger.info(f"Generating features for {len(shutuba_sample)} rows...")
    
    try:
        features_df = engine.generate_features(
            shutuba_df=shutuba_sample,
            results_history_df=results_df,
            horse_profiles_df=horses_df,
            pedigree_df=pedigree_df
        )
    except Exception as e:
        logger.error(f"Feature generation failed: {e}", exc_info=True)
        return

    logger.info("Feature generation successful!")
    logger.info(f"Output shape: {features_df.shape}")

    # Check for new features
    new_features = [
        'sire_avg_finish', 'bms_avg_finish', # Bloodline
        'nicks_avg_finish', 'sire_course_avg_finish', # Deep Pedigree
        'breeder_avg_finish', 'area_avg_finish', # Breeder/Area
        'jockey_recent_win_rate', 'trainer_recent_win_rate', # Recent Form
        'jockey_venue_win_rate', # Jockey Venue Affinity
        'pace_fit_score', # Pace
        'bias_seasonal_score', 'bias_dynamic_score', # Bias
        'bracket_avg_finish' # Course Bias
    ]

    found_features = []
    missing_features = []

    for feat in new_features:
        if feat in features_df.columns:
            found_features.append(feat)
        else:
            missing_features.append(feat)

    logger.info(f"Found {len(found_features)}/{len(new_features)} new features.")
    
    if missing_features:
        logger.warning(f"Missing features: {missing_features}")
    else:
        logger.info("All expected new features are present.")

    # Check for NaNs in found features
    if found_features:
        logger.info("\nNaN counts for new features:")
        print(features_df[found_features].isnull().sum())
        
        logger.info("\nDescriptive statistics for new features:")
        print(features_df[found_features].describe())

if __name__ == "__main__":
    verify_features()
