import pandas as pd
import sys
import os

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

from keibaai.src.features.report_metrics import ReportMetricsFeatureGenerator

def test_metrics_generation():
    print("Loading races.parquet...")
    try:
        df = pd.read_parquet(r"c:\Users\zk-ht\Keiba\Keiba_AI_v2\keibaai\data\parsed\parquet\races\races.parquet")
    except FileNotFoundError:
        print("races.parquet not found. Skipping test.")
        return

    # Use a subset for speed
    print(f"Total rows: {len(df)}")
    # Filter for a specific venue/year to ensure enough data for window functions
    # Tokyo (05), 2023
    df['race_date'] = pd.to_datetime(df['race_date'])
    subset = df[(df['race_date'] >= '2023-01-01') & (df['race_date'] <= '2023-12-31') & (df['venue'] == '東京')].copy()
    
    if len(subset) < 100:
        print("Not enough data in subset. Using full df (slow).")
        subset = df.head(10000).copy()
    else:
        print(f"Subset rows: {len(subset)}")

    print("Initializing Generator...")
    generator = ReportMetricsFeatureGenerator(subset)
    
    print("Annotating metrics...")
    annotated_df = generator.annotate_race_metrics()
    
    print("Verification:")
    cols = ['standard_time', 'track_bias', 'time_deviation_score', 'standard_3f', 'l3f_deviation_score']
    print(annotated_df[cols].describe())
    
    # Check for NaNs
    nan_counts = annotated_df[cols].isna().sum()
    print("\nNaN Counts:")
    print(nan_counts)
    
    # Sample check
    sample = annotated_df.dropna(subset=['time_deviation_score']).iloc[0]
    print("\nSample Row:")
    print(f"Date: {sample['race_date']}")
    print(f"Venue: {sample['venue']} {sample['distance_m']}m {sample['track_surface']}")
    print(f"Time: {sample['finish_time_seconds']}")
    print(f"Standard Time: {sample['standard_time']}")
    print(f"Deviation Score: {sample['time_deviation_score']}")
    
    # Logic check
    # If Time < Standard, Score should be positive
    expected_score = (sample['standard_time'] - sample['finish_time_seconds']) / sample['standard_time']
    print(f"Manual Calc: {expected_score}")
    assert abs(sample['time_deviation_score'] - expected_score) < 1e-6, "Score calculation mismatch"

if __name__ == "__main__":
    test_metrics_generation()
