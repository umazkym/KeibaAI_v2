import sys
from pathlib import Path
import pandas as pd
import logging

# Setup path
project_root = Path(__file__).resolve().parent.parent
sys.path.append(str(project_root))

from keibaai.src.features.roi_features import ROIFeatureEngine

# Setup logging
logging.basicConfig(level=logging.INFO)

def test_roi_features():
    logging.info("Testing ROIFeatureEngine...")
    
    # Load small sample data
    data_dir = project_root / 'keibaai/data/parsed/parquet'
    
    # Load races (for standard time master check)
    # Load horses_performance (for feature generation)
    perf_path = data_dir / 'horses_performance/horses_performance_fixed.parquet'
    if not perf_path.exists():
        logging.error("Performance data not found")
        return

    df = pd.read_parquet(perf_path)
    df['race_date'] = pd.to_datetime(df['race_date'])
    
    # Merge races.parquet to get race_class and race_name
    races_path = data_dir / 'races/races.parquet'
    if races_path.exists():
        races_df = pd.read_parquet(races_path)
        cols_to_add = ['venue', 'track_surface', 'distance_m', 'race_class', 'race_name', 'win_odds', 'popularity']
        cols_to_merge = [c for c in cols_to_add if c not in df.columns]
        if cols_to_merge:
            df = df.merge(races_df[['race_id'] + cols_to_merge], on='race_id', how='left')
    
    # Filter for a small subset (e.g., 1 race or 1 day)
    target_date = '2023-12-24' # Arima Kinen day?
    target_df = df[df['race_date'] == target_date].head(100)
    
    if target_df.empty:
        logging.warning("Target date has no data, using first 100 rows")
        target_df = df.head(100)
        
    # History (past data)
    history_df = df[df['race_date'] < target_df['race_date'].min()].tail(1000)
    
    logging.info(f"Target: {len(target_df)}, History: {len(history_df)}")
    logging.info(f"Available columns: {df.columns.tolist()}")
    
    # Initialize Engine
    engine = ROIFeatureEngine()
    
    # Generate Features
    try:
        result_df = engine.generate_features(target_df, history_df)
        logging.info("Feature generation successful!")
        logging.info(f"Result shape: {result_df.shape}")
        logging.info(f"Columns: {result_df.columns.tolist()}")
        
        # Check specific features
        check_cols = [
            'avg_pci_last5', 'nishida_trend_score', 'corner_loss_proxy', 
            'sire_wet_track_boost', 'pedigree_course_compatibility',
            'prev_race_level_index', 'under_valued_score_avg'
        ]
        
        for col in check_cols:
            if col in result_df.columns:
                logging.info(f"[OK] {col} found. Mean: {result_df[col].mean()}")
            else:
                logging.warning(f"[MISSING] {col} not found")
                
    except Exception as e:
        logging.error(f"Feature generation failed: {e}", exc_info=True)

if __name__ == "__main__":
    test_roi_features()
