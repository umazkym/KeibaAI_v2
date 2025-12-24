import pandas as pd
import lightgbm as lgb
import pickle
from pathlib import Path
from sklearn.metrics import roc_auc_score

def calculate_roi():
    print("Loading model and data...")
    
    # Load Model
    model_path = Path('keibaai/models/mu_v2/mu_v2_model.pkl')
    with open(model_path, 'rb') as f:
        model = pickle.load(f)
        
    # Load Training Data (Features)
    feature_df = pd.read_parquet('keibaai/models/mu_v2/train_data_mu_v2.parquet')
    
    # Filter for Test Period (2024)
    test_mask = feature_df['race_date'] >= '2024-01-01'
    test_df = feature_df[test_mask].copy()
    
    # Create target variable
    test_df['target'] = (test_df['finish_position'] == 1).astype(int)
    
    print(f"Test data size: {len(test_df)}")
    
    # Load Odds Data
    perf_path = Path('keibaai/data/parsed/parquet/horses_performance/horses_performance.parquet')
    perf_df = pd.read_parquet(perf_path)
    
    print(f"Performance data columns: {perf_df.columns.tolist()[:10]}...")
    if 'win_odds' not in perf_df.columns:
        print("CRITICAL ERROR: 'win_odds' missing from horses_performance_fixed.parquet")
        return

    # Merge Odds
    print("Merging odds data...")
    # Ensure key columns are correct types
    test_df['race_id'] = test_df['race_id'].astype(str)
    test_df['horse_id'] = test_df['horse_id'].astype(str)
    perf_df['race_id'] = perf_df['race_id'].astype(str)
    perf_df['horse_id'] = perf_df['horse_id'].astype(str)
    
    test_df = test_df.merge(perf_df[['race_id', 'horse_id', 'win_odds']], on=['race_id', 'horse_id'], how='left')
    
    # Predict
    print("Predicting...")
    # Identify feature columns (exclude non-features)
    exclude_cols = [
        'race_id', 'horse_id', 'race_date', 'finish_position', 'target', 
        'win_odds', 'jockey_id', 'trainer_id', 'owner_id', 'sire_id', 'damsire_id',
        'race_name', 'horse_name', 'jockey_name', 'trainer_name',
        # Leakage columns
        'finish_time_seconds', 'margin_seconds', 'prize_money', 'popularity',
        'odds', 'finish_time_str', 'margin_str', 'last_3f_time', 'passing_order',
        'final_corner_to_finish', 'passing_order_1', 'passing_order_2', 
        'passing_order_3', 'passing_order_4', 'position_change_1_2', 
        'scratched', 'time_except_last3f', 'win_probability', 'pace_index'
    ]
    feature_cols = [c for c in test_df.columns if c not in exclude_cols and c != 'win_odds']
    
    # Handle categorical columns if necessary (LightGBM handles them, but ensure types match training)
    for col in feature_cols:
        if test_df[col].dtype == 'object':
            test_df[col] = test_df[col].astype('category')
            
    preds = model.predict(test_df[feature_cols])
    test_df['pred_prob'] = preds
    
    # Calculate AUC
    auc = roc_auc_score(test_df['target'], preds)
    print(f"Test AUC: {auc:.4f}")
    
    # Calculate ROI (Top 1 Strategy)
    print("Calculating ROI...")
    test_df['rank_pred'] = test_df.groupby('race_id')['pred_prob'].rank(ascending=False, method='first')
    
    bet_df = test_df[test_df['rank_pred'] == 1].copy()
    
    # Drop rows with missing odds for ROI calculation
    valid_bet_df = bet_df.dropna(subset=['win_odds'])
    
    hits = valid_bet_df[valid_bet_df['target'] == 1]
    accuracy = len(hits) / len(valid_bet_df)
    
    return_amount = hits['win_odds'].sum() * 100
    bet_amount = len(valid_bet_df) * 100
    roi = return_amount / bet_amount if bet_amount > 0 else 0
    
    print(f"Total Races: {len(bet_df)}")
    print(f"Races with Odds: {len(valid_bet_df)}")
    print(f"Hits: {len(hits)}")
    print(f"Accuracy (Top1): {accuracy:.2%}")
    print(f"Return: {return_amount:,.0f}")
    print(f"Bet: {bet_amount:,.0f}")
    print(f"ROI (Top1): {roi:.2%}")

if __name__ == "__main__":
    calculate_roi()
