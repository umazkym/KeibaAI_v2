import pandas as pd
import os

def inspect_data():
    base_dir = r"c:\Users\zk-ht\Keiba\Keiba_AI_v2\keibaai\data\parsed\parquet"
    
    # 1. Inspect Horses
    print("--- Inspecting Horses ---")
    try:
        horses_df = pd.read_parquet(os.path.join(base_dir, "horses", "horses.parquet"))
        print(f"Columns: {horses_df.columns.tolist()}")
        if 'sire_id' in horses_df.columns:
            print("sire_id FOUND in horses.parquet")
        else:
            print("sire_id NOT FOUND in horses.parquet")
    except Exception as e:
        print(f"Error reading horses: {e}")

    # 2. Inspect Pedigrees
    print("\n--- Inspecting Pedigrees ---")
    try:
        ped_df = pd.read_parquet(os.path.join(base_dir, "pedigrees", "pedigrees.parquet"))
        
        # Check counts per generation for a sample horse
        sample_horse = ped_df['horse_id'].iloc[0]
        print(f"Sample Horse ID: {sample_horse}")
        
        subset = ped_df[ped_df['horse_id'] == sample_horse]
        print("Rows per generation for sample horse:")
        print(subset['generation'].value_counts().sort_index())
        
        print("\nSample rows for Generation 1:")
        print(subset[subset['generation'] == 1])
        
        print("\nSample rows for Generation 2:")
        print(subset[subset['generation'] == 2])
        
        # Check global average counts
        print("\nGlobal average rows per generation:")
        counts = ped_df.groupby(['horse_id', 'generation']).size().groupby('generation').mean()
        print(counts)
        
    except Exception as e:
        print(f"Error reading pedigrees: {e}")

if __name__ == "__main__":
    inspect_data()
