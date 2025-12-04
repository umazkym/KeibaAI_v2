import pandas as pd
from pathlib import Path
import logging

logging.basicConfig(level=logging.INFO, format='%(message)s')

def check_integrity():
    data_path = Path('keibaai/models/mu_v3_2/train_data_mu_v3_2.parquet')
    logging.info("Loading data...")
    df = pd.read_parquet(data_path)
    
    logging.info(f"Full Data Size: {len(df)}")
    full_dupes = df.duplicated(subset=['race_id', 'horse_id']).sum()
    logging.info(f"Full Data Duplicates: {full_dupes}")
    
    test_df = df[df['race_date'] >= '2024-01-01'].copy()
    logging.info(f"Test Data Size: {len(test_df)}")
    
    n_unique = test_df['race_id'].nunique()
    logging.info(f"Unique Race IDs: {n_unique}")
    
    if n_unique > 0:
        logging.info(f"Avg Rows per Race: {len(test_df) / n_unique:.2f}")
        
    # Check for duplicates
    dupes = test_df.duplicated(subset=['race_id', 'horse_id']).sum()
    logging.info(f"Duplicates (race_id, horse_id): {dupes}")
    
    # Sample race_id
    logging.info(f"Sample Race IDs: {test_df['race_id'].head(5).tolist()}")

if __name__ == "__main__":
    check_integrity()
