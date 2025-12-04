import pandas as pd
import numpy as np
import pickle
from pathlib import Path
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def verify_metrics():
    models_dir = Path('keibaai/models/mu_v3_2')
    data_path = models_dir / 'train_data_mu_v3_2.parquet'
    model_path = models_dir / 'mu_v3_2_ranker.pkl'
    feature_list_path = models_dir / 'feature_names.json'

    logging.info("Loading data...")
    df = pd.read_parquet(data_path)
    
    # Filter Test Data (2024)
    test_df = df[df['race_date'] >= '2024-01-01'].copy()
    logging.info(f"Test Data Size: {len(test_df)} rows")

    # Re-create gap_ability_popularity (as it's created inside train_model, not saved in parquet)
    ability_col = 'avg_finish_last5'
    if ability_col not in test_df.columns:
        if 'past_5_finish_position_mean' in test_df.columns:
            ability_col = 'past_5_finish_position_mean'
    
    if ability_col and 'popularity' in test_df.columns:
        test_df['ability_rank'] = test_df.groupby('race_id')[ability_col].rank(ascending=True)
        test_df['gap_ability_popularity'] = test_df['popularity'] - test_df['ability_rank']
        logging.info("Re-created gap_ability_popularity")
    else:
        logging.warning("Could not re-create gap_ability_popularity")

    logging.info("Loading model...")
    with open(model_path, 'rb') as f:
        model = pickle.load(f)
    
    with open(feature_list_path, 'r') as f:
        import json
        feature_cols = json.load(f)
        
    # Ensure category type for object columns
    for col in feature_cols:
        if col in test_df.columns and test_df[col].dtype == 'object':
             test_df[col] = test_df[col].astype('category')

    # --- Data Integrity Check ---
    n_unique_races = test_df['race_id'].nunique()
    n_rows = len(test_df)
    n_null_race_id = test_df['race_id'].isnull().sum()
    
    logging.info(f"Data Integrity Check:")
    logging.info(f"  Rows: {n_rows}")
    logging.info(f"  Unique Race IDs: {n_unique_races}")
    logging.info(f"  Null Race IDs: {n_null_race_id}")
    logging.info(f"  Avg Rows per Race: {n_rows / n_unique_races if n_unique_races else 0:.2f}")
    
    # Check finish_position
    finish_counts = test_df['finish_position'].value_counts().sort_index().head(5)
    logging.info(f"  Finish Position Counts (Top 5):\n{finish_counts}")
    
    # Check if race_id is string or int
    logging.info(f"  Race ID Dtype: {test_df['race_id'].dtype}")
    logging.info(f"  Sample Race IDs: {test_df['race_id'].head(5).tolist()}")

    logging.info("Predicting...")
    preds = model.predict(test_df[feature_cols])
    test_df['score'] = preds
    
    # Rank Predictions
    test_df['rank_pred'] = test_df.groupby('race_id')['score'].rank(ascending=False, method='first')
    
    # --- Debugging Metrics ---
    
    # 1. Check Winners
    winners = test_df[test_df['finish_position'] == 1]
    logging.info(f"Total Races (Winners): {len(winners)}")
    
    # 2. Check Top 1 Predictions
    top1_preds = test_df[test_df['rank_pred'] == 1]
    logging.info(f"Total Races (Top 1 Preds): {len(top1_preds)}")
    
    # 3. Calculate Accuracy
    # Hits: Predicted Rank 1 AND Actual Rank 1
    hits = top1_preds[top1_preds['finish_position'] == 1]
    accuracy = len(hits) / len(top1_preds)
    logging.info(f"Hits (Rank 1 & Finish 1): {len(hits)}")
    logging.info(f"Top 1 Accuracy: {accuracy:.2%}")
    
    # 4. Calculate Recall @ 5
    # Winners in Top 5: Actual Rank 1 AND Predicted Rank <= 5
    winners_in_top5 = winners[winners['rank_pred'] <= 5]
    recall = len(winners_in_top5) / len(winners)
    logging.info(f"Winners in Top 5 Preds: {len(winners_in_top5)}")
    logging.info(f"Top 5 Recall: {recall:.2%}")
    
    # 5. Deep Dive into Discrepancy
    if len(hits) > len(winners_in_top5):
        logging.error("CRITICAL: Hits > Winners in Top 5. This implies (Rank 1) is NOT a subset of (Rank <= 5).")
        
        # Find the "Impossible" cases
        # Rows where (Rank Pred == 1) AND (Finish == 1) BUT (Rank Pred > 5) -> Impossible
        # Rows where (Rank Pred == 1) AND (Finish == 1) -> These are Hits.
        # Check their rank_pred in winners_in_top5
        
        # Let's look at a sample of hits
        sample_hits = hits.head(5)
        logging.info(f"Sample Hits:\n{sample_hits[['race_id', 'horse_id', 'finish_position', 'rank_pred']]}")
        
        # Let's look at a sample of winners
        sample_winners = winners.head(5)
        logging.info(f"Sample Winners:\n{sample_winners[['race_id', 'horse_id', 'finish_position', 'rank_pred']]}")

if __name__ == "__main__":
    verify_metrics()
