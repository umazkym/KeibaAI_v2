import pandas as pd
import numpy as np
import pickle
import lightgbm as lgb
from pathlib import Path
import sys

# Add project root to path
project_root = Path(__file__).resolve().parent.parent.parent
sys.path.append(str(project_root))

from keibaai.src.features.leak_free_feature_engineer_v16 import LeakFreeFeatureEngineerV16

def calc_roi(df, preds):
    df = df.copy()
    df['score'] = preds
    df['rank'] = df.groupby('race_id')['score'].rank(ascending=False, method='first')
    bets = df[df['rank'] == 1]
    if len(bets) == 0: return 0, 0, 0
    hits = bets[bets['finish_position'] == 1]
    roi = hits['win_odds'].sum() / len(bets) * 100
    hit_rate = len(hits) / len(bets) * 100
    return roi, hit_rate, len(bets)

def main():
    print("Evaluating V4.4 Model...")
    
    # Load data
    data_dir = project_root / "keibaai/data/parsed/parquet"
    races = pd.read_parquet(data_dir / "races/races.parquet")
    races = races[races['finish_position'].notna()]
    races['race_date'] = pd.to_datetime(races['race_date'])
    
    # Load V16 engine resources (pedigrees etc)
    pedigrees = pd.read_parquet(data_dir / "pedigrees/pedigrees.parquet")
    corners = pd.read_parquet(data_dir / "corners/corner_positions.parquet")
    race_details = pd.read_parquet(data_dir / "race_details/race_details.parquet")
    horses = pd.read_parquet(data_dir / "horses/horses.parquet")
    
    # 2025 Data
    # 2025-01-01 以降 (Test Set)
    # Note: To be fair, we should fit the engine on data UP TO 2024-12-31, then transform 2025.
    # Just like training.
    
    train_data = races[races['race_date'] <= '2024-12-31'].copy()
    test_data = races[races['race_date'] >= '2025-01-01'].copy()
    
    print(f"Test Data: {len(test_data)} rows ({test_data['race_date'].min().date()} - {test_data['race_date'].max().date()})")
    
    # Init Engine
    engine = LeakFreeFeatureEngineerV16()
    print("Fitting feature engine...")
    engine.fit(train_data, pedigrees, corners, race_details, horses_df=horses)
    
    print("Transforming test data...")
    test_df = engine.transform(test_data)
    
    # Load Model
    model_path = project_root / "keibaai/models/mu_v4_4/model.pkl"
    with open(model_path, "rb") as f:
        model = pickle.load(f)
    
    # Predict
    feature_cols = engine.get_feature_columns()
    valid_features = [c for c in feature_cols if c in test_df.columns]
    print(f"Features: {len(valid_features)}")
    
    X_test = test_df[valid_features].fillna(0)
    preds = model.predict(X_test)
    
    # Calc ROI
    roi, hit_rate, n_bets = calc_roi(test_df, preds)
    
    print("="*40)
    print(f"V4.4 Test Result (2025)")
    print(f"ROI: {roi:.2f}%")
    print(f"Hit Rate: {hit_rate:.2f}%")
    print(f"Bets: {n_bets}")
    print("="*40)
    
    # Segment Analysis (Surface)
    test_df['score'] = preds
    test_df['rank'] = test_df.groupby('race_id')['score'].rank(ascending=False, method='first')
    
    print("\nSegment Analysis:")
    for surf in ['芝', 'ダート']:
        sub = test_df[test_df['track_surface'] == surf]
        bets = sub[sub['rank'] == 1]
        hits = bets[bets['finish_position'] == 1]
        if len(bets) > 0:
            s_roi = hits['win_odds'].sum() / len(bets) * 100
            print(f"{surf}: ROI {s_roi:.2f}% ({len(bets)} bets)")

if __name__ == "__main__":
    main()
