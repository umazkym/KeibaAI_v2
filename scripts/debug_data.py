import pandas as pd
import sys
from pathlib import Path
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)

project_root = Path(__file__).resolve().parent.parent
data_dir = project_root / 'keibaai' / 'data' / 'parsed' / 'parquet'

def debug_data():
    logger.info("Debugging data linkage...")
    
    try:
        shutuba_path = data_dir / 'shutuba' / 'shutuba.parquet'
        results_path = data_dir / 'horses_performance' / 'horses_performance.parquet'
        races_path = data_dir / 'races' / 'races.parquet'
        horses_path = data_dir / 'horses' / 'horses.parquet'
        
        if not shutuba_path.exists():
            logger.error(f"Missing: {shutuba_path}")
            return
        if not results_path.exists():
            logger.error(f"Missing: {results_path}")
            return
        if not races_path.exists():
            logger.error(f"Missing: {races_path}")
            return
        if not horses_path.exists():
            logger.error(f"Missing: {horses_path}")
            return
            
        shutuba_df = pd.read_parquet(shutuba_path)
        results_df = pd.read_parquet(results_path)
        races_df = pd.read_parquet(races_path)
        horses_df = pd.read_parquet(horses_path)
        
        logger.info(f"Shutuba shape: {shutuba_df.shape}")
        logger.info(f"Results shape: {results_df.shape}")
        logger.info(f"Races shape: {races_df.shape}")
        logger.info(f"Horses shape: {horses_df.shape}")
        
        # Date ranges
        if 'race_date' in shutuba_df.columns:
            logger.info(f"Shutuba dates: {shutuba_df['race_date'].min()} to {shutuba_df['race_date'].max()}")
        
        if 'race_date' in results_df.columns:
            logger.info(f"Results dates: {results_df['race_date'].min()} to {results_df['race_date'].max()}")

        if 'race_date' in races_df.columns:
            logger.info(f"Races dates: {races_df['race_date'].min()} to {races_df['race_date'].max()}")
            
        # Common IDs
        common_horses = set(shutuba_df['horse_id']).intersection(set(results_df['horse_id']))
        logger.info(f"Common horses: {len(common_horses)} / {shutuba_df['horse_id'].nunique()}")
        
        common_jockeys = set(shutuba_df['jockey_id']).intersection(set(results_df['jockey_id']))
        logger.info(f"Common jockeys: {len(common_jockeys)} / {shutuba_df['jockey_id'].nunique()}")
        
        common_trainers = set(shutuba_df['trainer_id']).intersection(set(results_df['trainer_id']))
        logger.info(f"Common trainers: {len(common_trainers)} / {shutuba_df['trainer_id'].nunique()}")
        
        # Check columns
        logger.info(f"\nShutuba columns: {shutuba_df.columns.tolist()}")
        logger.info(f"Results columns: {results_df.columns.tolist()}")
        
        # Check trainer_id samples
        if 'trainer_id' in shutuba_df.columns:
            logger.info(f"\nShutuba trainer_id sample: {shutuba_df['trainer_id'].dropna().head().tolist()}")
        else:
            logger.warning("Shutuba missing 'trainer_id'")
            
        if 'trainer_id' in results_df.columns:
            logger.info(f"Results trainer_id sample: {results_df['trainer_id'].dropna().head().tolist()}")
        else:
            logger.warning("Results missing 'trainer_id'")
            
        # Check venue
        if 'venue' in shutuba_df.columns:
             logger.info(f"Shutuba venue sample: {shutuba_df['venue'].dropna().head().tolist()}")
        else:
             logger.warning("Shutuba missing 'venue'")
             
        if 'venue' in results_df.columns:
             logger.info(f"Results venue sample: {results_df['venue'].dropna().head().tolist()}")
        else:
             logger.warning("Results missing 'venue'")
             
        # Check races
        logger.info(f"\nRaces columns: {races_df.columns.tolist()}")
        if 'venue' in races_df.columns:
             logger.info(f"Races venue sample: {races_df['venue'].dropna().head().tolist()}")
        if 'trainer_id' in races_df.columns:
             logger.info(f"Races trainer_id sample: {races_df['trainer_id'].dropna().head().tolist()}")
             
    except Exception as e:
        logger.error(f"Error: {e}")

if __name__ == "__main__":
    debug_data()
