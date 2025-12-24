import pandas as pd

def list_features():
    # Load data columns
    df = pd.read_parquet('keibaai/models/mu_v2/train_data_mu_v2.parquet')
    all_cols = set(df.columns)
    
    # Exclude list from train_mu_v2_model.py
    exclude_cols = {
        'race_id', 'horse_id', 'race_date', 'finish_position', 'target', 
        'win_odds', 'jockey_id', 'trainer_id', 'owner_id', 'sire_id', 'damsire_id',
        'race_name', 'horse_name', 'jockey_name', 'trainer_name',
        # Leakage columns
        'finish_time_seconds', 'margin_seconds', 'prize_money', 'popularity',
        'odds', 'finish_time_str', 'margin_str', 'last_3f_time', 'passing_order',
        'final_corner_to_finish', 'passing_order_1', 'passing_order_2', 
        'passing_order_3', 'passing_order_4', 'position_change_1_2', 
        'scratched', 'time_except_last3f', 'win_probability', 'pace_index'
    }
    
    # Calculate used features
    used_features = sorted(list(all_cols - exclude_cols))
    
    print(f"Total Used Features: {len(used_features)}")
    print("\n--- Feature List ---")
    for f in used_features:
        print(f)

if __name__ == "__main__":
    list_features()
