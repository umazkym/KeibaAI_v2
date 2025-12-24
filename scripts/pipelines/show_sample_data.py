import pandas as pd

def show_sample():
    # Load data
    print("Loading data...")
    df = pd.read_parquet('keibaai/models/mu_v2/train_data_mu_v2.parquet')
    
    # Sort by date desc to get recent races
    df = df.sort_values(['race_date', 'race_id', 'horse_number'], ascending=[False, True, True])
    
    # Get last 2 races
    recent_races = df['race_id'].unique()[:2]
    
    print(f"\n=== Sample Data for {len(recent_races)} Recent Races ===")
    
    cols_to_show = [
        'race_date', 'race_id', 'horse_number', 
        'finish_position', # Target (Result)
        'past_1_finish_position_mean', # Key Feature 1
        'past_5_finish_position_mean', # Key Feature 2
        'jockey_win_rate', # Key Feature 3
        'days_since_last_race' # Key Feature 4
    ]
    
    for race_id in recent_races:
        race_df = df[df['race_id'] == race_id][cols_to_show]
        date = race_df['race_date'].iloc[0].strftime('%Y-%m-%d')
        print(f"\nRace ID: {race_id} ({date})")
        print("-" * 100)
        print(race_df.drop(columns=['race_date', 'race_id']).to_string(index=False, float_format='{:.2f}'.format))

if __name__ == "__main__":
    show_sample()
