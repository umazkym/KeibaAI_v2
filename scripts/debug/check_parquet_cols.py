import pandas as pd
import sys

try:
    df = pd.read_parquet('keibaai/models/mu_v2/train_data_mu_v2.parquet')
    print("Columns found:")
    for col in df.columns:
        if 'bias' in col or 'pace' in col or 'leader' in col:
            print(f"  - {col}")
            
    if 'bias_seasonal_score' in df.columns:
        print("SUCCESS: bias_seasonal_score found")
    else:
        print("FAILURE: bias_seasonal_score NOT found")
        
except Exception as e:
    print(f"Error: {e}")
