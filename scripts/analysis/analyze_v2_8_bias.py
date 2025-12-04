import pandas as pd
import numpy as np
import lightgbm as lgb
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
import sys

# Add project root to path
project_root = Path(__file__).resolve().parent.parent.parent
sys.path.append(str(project_root))

def analyze_model_bias():
    print("Loading data and model...")
    
    # Paths
    models_dir = Path('keibaai/models/mu_v2_8')
    data_path = models_dir / 'train_data_mu_v2_8.parquet'
    model_path = models_dir / 'mu_v2_8_model.txt'
    
    if not data_path.exists() or not model_path.exists():
        print("Data or model not found.")
        return

    # Load Data
    df = pd.read_parquet(data_path)
    
    # Filter for Test Data (2024)
    test_df = df[df['race_date'] >= '2024-01-01'].copy()
    print(f"Test Data: {len(test_df)} rows")
    
    # Load Model
    model = lgb.Booster(model_file=str(model_path))
    
    # Identify feature columns (exclude non-features)
    exclude_cols = [
        'race_id', 'horse_id', 'race_date', 'finish_position', 'target', 
        'win_odds', 'jockey_id', 'trainer_id', 'owner_id', 'sire_id', 'damsire_id',
        'race_name', 'horse_name', 'jockey_name', 'trainer_name',
        'weight', 'popularity'
    ]
    # Also exclude leakage columns if they exist in the parquet
    leakage_cols = [
        'finish_time_seconds', 'margin_seconds', 'prize_money', 
        'odds', 'finish_time_str', 'margin_str', 'last_3f_time', 'passing_order',
        'final_corner_to_finish', 'passing_order_1', 'passing_order_2', 
        'passing_order_3', 'passing_order_4', 'position_change_1_2', 
        'scratched', 'time_except_last3f', 'win_probability', 'pace_index'
    ]
    
    feature_cols = [c for c in test_df.columns if c not in exclude_cols and c not in leakage_cols]
    
    # Ensure categorical
    for col in feature_cols:
        if test_df[col].dtype == 'object':
            test_df[col] = test_df[col].astype('category')
            
    print(f"Using {len(feature_cols)} features.")
    
    # Predict
    print("Predicting...")
    preds = model.predict(test_df[feature_cols])
    test_df['pred_prob'] = preds
    
    # Rank predictions within each race
    test_df['rank_pred'] = test_df.groupby('race_id')['pred_prob'].rank(ascending=False, method='first')
    
    # Select Top 1 Predictions
    top1_df = test_df[test_df['rank_pred'] == 1].copy()
    
    # Calculate Popularity (Rank by Odds)
    # Note: 'popularity' column might be missing, so we calculate it from win_odds
    if 'popularity' not in top1_df.columns:
        # We need to calculate popularity for ALL horses in the race first
        test_df['popularity_calc'] = test_df.groupby('race_id')['win_odds'].rank(ascending=True, method='min')
        top1_df = test_df[test_df['rank_pred'] == 1].copy()
        pop_col = 'popularity_calc'
    else:
        pop_col = 'popularity'

    # --- Analysis ---
    print("\n=== Analysis of Top 1 Predictions (Model's Choice) ===")
    
    # 1. Average Odds
    avg_odds = top1_df['win_odds'].mean()
    median_odds = top1_df['win_odds'].median()
    print(f"Average Win Odds: {avg_odds:.2f}")
    print(f"Median Win Odds:  {median_odds:.2f}")
    
    # 2. Popularity Distribution
    print("\nPopularity Distribution of Selected Horses:")
    pop_counts = top1_df[pop_col].value_counts().sort_index().head(10)
    pop_pct = top1_df[pop_col].value_counts(normalize=True).sort_index().head(10)
    
    for pop, count in pop_counts.items():
        print(f"Popularity {int(pop)}: {count} ({pop_pct[pop]:.1%})")
        
    # 3. ROI by Popularity Bucket (of the selected horse)
    print("\nPerformance by Selected Popularity:")
    print(f"{'Pop':<5} {'Count':<6} {'Hits':<5} {'Acc%':<6} {'ROI%':<6}")
    
    for pop in sorted(top1_df[pop_col].unique())[:10]:
        subset = top1_df[top1_df[pop_col] == pop]
        count = len(subset)
        hits = subset['finish_position'] == 1
        hit_count = hits.sum()
        acc = hit_count / count
        return_amt = subset.loc[hits, 'win_odds'].sum()
        roi = return_amt / count * 100
        
        print(f"{int(pop):<5} {count:<6} {hit_count:<5} {acc:.1%}   {roi:.1f}%")
        
    # 4. Odds Bucket Analysis
    print("\nPerformance by Odds Bucket:")
    bins = [0, 2, 5, 10, 20, 50, 100, 1000]
    labels = ['1.0-1.9', '2.0-4.9', '5.0-9.9', '10.0-19.9', '20.0-49.9', '50.0-99.9', '100+']
    top1_df['odds_bucket'] = pd.cut(top1_df['win_odds'], bins=bins, labels=labels)
    
    print(f"{'Odds':<10} {'Count':<6} {'Hits':<5} {'Acc%':<6} {'ROI%':<6}")
    for label in labels:
        subset = top1_df[top1_df['odds_bucket'] == label]
        count = len(subset)
        if count == 0:
            continue
        hits = subset['finish_position'] == 1
        hit_count = hits.sum()
        acc = hit_count / count
        return_amt = subset.loc[hits, 'win_odds'].sum()
        roi = return_amt / count * 100
        print(f"{label:<10} {count:<6} {hit_count:<5} {acc:.1%}   {roi:.1f}%")

if __name__ == "__main__":
    analyze_model_bias()
