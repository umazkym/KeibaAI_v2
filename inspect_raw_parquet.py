import pandas as pd
from pathlib import Path

# Check shutuba data
shutuba_path = Path('keibaai/data/parsed/parquet/shutuba/shutuba.parquet')
if shutuba_path.exists():
    print("\nLoading shutuba.parquet...")
    try:
        df_shutuba = pd.read_parquet(shutuba_path)
        print(f"Loaded {len(df_shutuba)} rows from shutuba.parquet")
        
        if 'distance_m' in df_shutuba.columns:
            print("\nshutuba distance_m stats:")
            print(df_shutuba['distance_m'].describe())
            print("\nFirst 5 rows of shutuba distance_m:")
            print(df_shutuba['distance_m'].head())
            
            # Check for zeros
            zeros = (df_shutuba['distance_m'] == 0).sum()
            print(f"\nZeros in distance_m: {zeros}")
            
            # Check for NaNs
            nans = df_shutuba['distance_m'].isna().sum()
            print(f"NaNs in distance_m: {nans}")
            
        else:
            print("distance_m not found in shutuba columns")
            print("Columns found:")
            for col in df_shutuba.columns:
                print(f"  - {col}")
            
    except Exception as e:
        print(f"Failed to load shutuba: {e}")
else:
    print("shutuba.parquet not found")
