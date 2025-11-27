import pandas as pd
import numpy as np
import sys
import os

# Add src to path
sys.path.append(os.path.join(os.getcwd(), 'keibaai'))

from src.features.advanced_features import AdvancedFeatureEngine

def test_seasonal_bias_features():
    print("Testing Seasonal Bias Features...")
    data = {
        'race_id': ['R1', 'R2', 'R3', 'R4', 'R5', 'R6'],
        'race_date': pd.to_datetime(['2020-01-01']*6),
        'venue': ['札幌', '札幌', '札幌', '函館', '函館', '札幌'],
        'round_of_year': [1, 1, 1, 1, 1, 1],
        'track_surface': ['芝', '芝', '芝', '芝', '芝', '芝'],
        'distance_m': [1800, 1800, 1800, 1800, 1800, 1800],
        'bracket_number': [1, 8, 1, 1, 8, 1],
        'finish_position': [1, 10, 1, 10, 1, 2],
        'horse_id': ['H1', 'H2', 'H3', 'H4', 'H5', 'H6']
    }
    df = pd.DataFrame(data)
    df['distance_category'] = pd.cut(
        df['distance_m'],
        bins=[0, 1400, 1800, 2200, 3000, 4000],
        labels=['sprint', 'mile', 'intermediate', 'long', 'extreme_long']
    )
    
    engine = AdvancedFeatureEngine()
    result = engine.generate_seasonal_bias_features(df, df)
    
    if 'bias_seasonal_score' not in result.columns:
        print("FAIL: bias_seasonal_score column missing")
        return

    sapporo_inner = result[(result['venue'] == '札幌') & (result['bracket_number'] == 1)]
    hakodate_inner = result[(result['venue'] == '函館') & (result['bracket_number'] == 1)]
    
    score_sapporo = sapporo_inner['bias_seasonal_score'].iloc[0]
    score_hakodate = hakodate_inner['bias_seasonal_score'].iloc[0]
    
    print(f"Sapporo Inner Score: {score_sapporo}")
    print(f"Hakodate Inner Score: {score_hakodate}")
    
    if score_sapporo > score_hakodate:
         print("PASS: Sapporo Inner score is higher (better win rate)")
    else:
         print("FAIL: Sapporo Inner score should be higher")

def test_dynamic_bias_features():
    print("\nTesting Dynamic Bias Features...")
    data = {
        'race_id': ['R1', 'R2', 'R3', 'R4', 'R5'],
        'race_date': pd.to_datetime([
            '2020-01-01', '2020-01-01', # Day 1
            '2020-01-02', # Day 2
            '2020-02-01', # Next meeting
            '2021-01-01'  # Next year
        ]),
        'venue': ['札幌', '札幌', '札幌', '函館', '札幌'],
        'round_of_year': [1, 1, 1, 1, 1],
        'day_of_meeting': [1, 1, 2, 1, 1],
        'track_surface': ['芝', '芝', '芝', '芝', '芝'],
        'bracket_number': [1, 8, 1, 1, 1],
        'finish_position': [1, 10, 1, 1, 1],
        'horse_id': ['H1', 'H2', 'H3', 'H4', 'H5']
    }
    df = pd.DataFrame(data)
    
    engine = AdvancedFeatureEngine()
    result = engine.generate_dynamic_bias_features(df, df)
    
    # Day 1 check (Should be 0/NaN)
    day1 = result[(result['race_date'] == '2020-01-01') & (result['venue'] == '札幌')]
    score_day1 = day1['bias_dynamic_score'].iloc[0]
    print(f"Day 1 Score: {score_day1}")
    
    if score_day1 == 0:
        print("PASS: Day 1 score is 0 (no leakage)")
    else:
        print(f"FAIL: Day 1 score should be 0, got {score_day1}")

    # Day 2 check (Should reflect Day 1)
    # Day 1 Inner(1) won. So Day 2 Inner(1) should have high score (win rate 1.0)
    day2 = result[(result['race_date'] == '2020-01-02') & (result['venue'] == '札幌')]
    score_day2 = day2['bias_dynamic_score'].iloc[0]
    print(f"Day 2 Score: {score_day2}")
    
    if score_day2 == 1.0:
        print("PASS: Day 2 score reflects Day 1 win")
    else:
        print(f"FAIL: Day 2 score should be 1.0, got {score_day2}")

def test_pace_features():
    print("\nTesting Pace Features...")
    # Mock data
    data = {
        'race_id': ['R1']*5,
        'horse_id': ['H1', 'H2', 'H3', 'H4', 'H5'],
        'past_1_passing_order_1_mean': [1.0, 2.0, 10.0, 12.0, 5.0] # 2 leaders (<=3.0)
    }
    df = pd.DataFrame(data)
    
    engine = AdvancedFeatureEngine()
    result = engine.generate_pace_features(df)
    
    if 'pace_fit_score' not in result.columns:
        print("FAIL: pace_fit_score missing")
        return
        
    n_leaders = result['n_leaders'].iloc[0]
    print(f"n_leaders: {n_leaders}")
    
    if n_leaders == 2:
        print("PASS: n_leaders count is correct")
    else:
        print(f"FAIL: n_leaders should be 2, got {n_leaders}")

if __name__ == "__main__":
    test_seasonal_bias_features()
    test_dynamic_bias_features()
    test_pace_features()
