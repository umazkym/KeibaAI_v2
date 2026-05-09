import pandas as pd
import numpy as np
import lightgbm as lgb
import pickle
import logging
from pathlib import Path
from sklearn.metrics import roc_auc_score, log_loss

logging.basicConfig(level=logging.INFO, format='%(message)s')

def evaluate_final():
    models_dir = Path('keibaai/models/mu_v2')
    data_path = models_dir / 'train_data_mu_v2.parquet'
    model_path = models_dir / 'mu_v2_model.pkl'
    
    if not data_path.exists() or not model_path.exists():
        logging.error("Model or Data not found.")
        return

    logging.info("Loading data and model...")
    df = pd.read_parquet(data_path)
    with open(model_path, 'rb') as f:
        model = pickle.load(f)
        
    # Test Data: 2024
    df['race_date'] = pd.to_datetime(df['race_date'])
    test_df = df[df['race_date'] >= '2024-01-01'].copy()
    test_df['target'] = (test_df['finish_position'] == 1).astype(int)
    
    logging.info(f"Test Data: {len(test_df):,} rows")
    
    # Features
    # Use feature names from the model to ensure consistency
    feature_cols = model.feature_name()
    
    # Check if all features exist in test_df
    missing_cols = [c for c in feature_cols if c not in test_df.columns]
    if missing_cols:
        logging.warning(f"Missing features in test data: {missing_cols}")
        # Add missing columns with NaN or 0? LightGBM handles NaN.
        for c in missing_cols:
            test_df[c] = np.nan
            
    # Categorical handling
    
    # Categorical handling
    for col in feature_cols:
        if test_df[col].dtype == 'object':
            test_df[col] = test_df[col].astype('category')
            
    # Predict
    logging.info("Predicting...")
    preds = model.predict(test_df[feature_cols])
    test_df['pred_prob'] = preds
    
    # Metrics
    auc = roc_auc_score(test_df['target'], preds)
    logloss = log_loss(test_df['target'], preds)
    
    logging.info(f"AUC: {auc:.4f}")
    logging.info(f"LogLoss: {logloss:.4f}")
    
    # ROI Simulation
    accuracy = None
    roi = None
    
    # Ensure win_odds exists
    if 'win_odds' not in test_df.columns:
        logging.info("'win_odds' not found in dataset. Attempting to merge from original data...")
        try:
            original_path = Path('horses_performance_fixed.parquet')
            if original_path.exists():
                original_df = pd.read_parquet(original_path)
                # Merge on race_id and horse_number (assuming unique)
                # Or race_id and horse_id
                odds_df = original_df[['race_id', 'horse_id', 'win_odds']]
                test_df = test_df.merge(odds_df, on=['race_id', 'horse_id'], how='left', suffixes=('', '_orig'))
                if 'win_odds' not in test_df.columns and 'win_odds_orig' in test_df.columns:
                    test_df['win_odds'] = test_df['win_odds_orig']
        except Exception as e:
            logging.warning(f"Failed to merge win_odds: {e}")

    if 'win_odds' in test_df.columns:
        test_df['rank_pred'] = test_df.groupby('race_id')['pred_prob'].rank(ascending=False, method='first')
        
        # Top 1 Bet
        bet_df = test_df[test_df['rank_pred'] == 1].copy()
        hits = bet_df[bet_df['target'] == 1]
        accuracy = len(hits) / len(bet_df)
        
        bet_df_valid = bet_df.dropna(subset=['win_odds'])
        hits_valid = bet_df_valid[bet_df_valid['target'] == 1]
        
        return_amount = hits_valid['win_odds'].sum() * 100
        bet_amount = len(bet_df_valid) * 100
        roi = return_amount / bet_amount if bet_amount > 0 else 0
        
        logging.info(f"Accuracy (Top1): {accuracy:.2%}")
        logging.info(f"ROI (Top1): {roi:.2%} ({return_amount:,.0f}/{bet_amount:,.0f})")
        
        # Top 3 Bet (Box? No, just Top 3 horses check)
        # 複勝シミュレーションはデータがないので省略
        
    # Feature Importance
    importance = pd.DataFrame({
        'feature': feature_cols,
        'importance': model.feature_importance(importance_type='gain')
    }).sort_values('importance', ascending=False)
    
    logging.info("\nTop 20 Features:")
    print(importance.head(20).to_string())
    
    # Save Report Data
    with open(models_dir / 'eval_result.txt', 'w') as f:
        f.write(f"AUC: {auc:.4f}\n")
        f.write(f"LogLoss: {logloss:.4f}\n")
        if accuracy is not None:
            f.write(f"Accuracy: {accuracy:.2%}\n")
        if roi is not None:
            f.write(f"ROI: {roi:.2%}\n")

if __name__ == "__main__":
    evaluate_final()
