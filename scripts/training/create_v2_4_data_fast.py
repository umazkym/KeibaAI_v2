import pandas as pd
from pathlib import Path
import logging

# ログ設定
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def create_v2_4_data_fast():
    base_dir = Path('keibaai/models/mu_v2')
    target_dir = Path('keibaai/models/mu_v2_4')
    target_dir.mkdir(parents=True, exist_ok=True)

    src_path = base_dir / 'train_data_mu_v2.parquet'
    dst_path = target_dir / 'train_data_mu_v2_4.parquet'

    if not src_path.exists():
        logging.error(f"Source file not found: {src_path}")
        return

    logging.info(f"Loading v2.3 data from {src_path}...")
    df = pd.read_parquet(src_path)
    logging.info(f"Loaded {len(df):,} rows. Columns: {len(df.columns)}")

    # Columns to drop (v2.3 specific features)
    cols_to_drop = [
        # Breeder / Producing Area
        'breeder_avg_finish', 'breeder_win_rate', 
        'area_avg_finish', 'area_win_rate',
        
        # Recent Form
        'jockey_recent_avg_finish', 'jockey_recent_win_rate',
        'trainer_recent_avg_finish', 'trainer_recent_win_rate',
        
        # Jockey x Venue Affinity
        'jockey_venue_avg_finish', 'jockey_venue_win_rate'
    ]

    # Drop existing columns
    existing_cols_to_drop = [c for c in cols_to_drop if c in df.columns]
    
    if existing_cols_to_drop:
        logging.info(f"Dropping {len(existing_cols_to_drop)} columns: {existing_cols_to_drop}")
        df = df.drop(columns=existing_cols_to_drop)
    else:
        logging.warning("No target columns found to drop. Is the source file correct?")

    logging.info(f"Saving v2.4 data to {dst_path}...")
    df.to_parquet(dst_path)
    logging.info("Done. You can now run train_mu_v2_4_model.py immediately.")

if __name__ == "__main__":
    create_v2_4_data_fast()
