import pandas as pd
try:
    df = pd.read_parquet('keibaai/data/parsed/parquet/horses_performance/horses_performance_fixed.parquet')
    print(f"Data Range: {df['race_date'].min()} to {df['race_date'].max()}")
    print(f"Count before 2020: {len(df[df['race_date'] < '2020-01-01'])}")
except Exception as e:
    print(e)
