"""
特徴量生成の修正検証スクリプト
features.yamlの修正（venue -> race_course）が反映され、
trainer_中山_win_rateなどが生成されるか確認する。
"""
import pandas as pd
from pathlib import Path
import sys
import yaml
import logging

# プロジェクトルートをパスに追加
project_root = Path('.').resolve()
sys.path.append(str(project_root))

from keibaai.src.features.feature_engine import FeatureEngine
from keibaai.src.utils.data_utils import load_parquet_data_by_date

# ロギング設定
logging.basicConfig(level=logging.INFO)

def test_generation():
    # 設定ロード
    config_path = 'keibaai/configs/features.yaml'
    engine = FeatureEngine(config_path)
    
    # データロード（2024年1月のみ）
    start_date = pd.Timestamp('2024-01-01')
    end_date = pd.Timestamp('2024-01-31')
    
    data_dir = Path('keibaai/data/parsed/parquet')
    
    print("Loading data...")
    shutuba_df = load_parquet_data_by_date(data_dir / 'shutuba', start_date, end_date, 'race_date')
    races_df = load_parquet_data_by_date(data_dir / 'races', None, end_date, 'race_date')
    
    # 簡易的なプロフィールデータ（必須ではないが念のため）
    # horse_profiles_df = load_parquet_data_by_date(data_dir / 'horses', None, None, 'birth_date')
    # 今回は空でもOKか確認
    horse_profiles_df = pd.DataFrame() 
    
    print(f"Shutuba: {len(shutuba_df)}, Races: {len(races_df)}")
    
    if 'race_course' in races_df.columns:
        print("✅ 'race_course' exists in loaded races_df (via load_parquet_data_by_date)")
    else:
        print("❌ 'race_course' MISSING in loaded races_df (via load_parquet_data_by_date)")
        
        # Try loading directly
        print("Trying direct load...")
        races_files = list((data_dir / 'races').glob('**/*.parquet'))
        with open('direct_load_result.txt', 'w', encoding='utf-8') as f:
            if races_files:
                f.write(f"Found file: {races_files[0]}\n")
                direct_df = pd.read_parquet(races_files[0])
                if 'race_course' in direct_df.columns:
                    f.write(f"✅ 'race_course' exists in direct load of {races_files[0].name}\n")
                else:
                    f.write(f"❌ 'race_course' MISSING in direct load of {races_files[0].name}\n")
                    f.write(f"Columns: {direct_df.columns.tolist()}\n")
            else:
                f.write("No parquet files found for direct load.\n")
        print("Direct load result written to direct_load_result.txt")
    
    # 特徴量生成
    print("Generating features...")
    features_df = engine.generate_features(
        shutuba_df,
        races_df,
        horse_profiles_df
    )
    
    print(f"Generated features shape: {features_df.shape}")
    cols = features_df.columns.tolist()
    
    # 確認対象のカラム
    target_cols = [
        'trainer_中山_win_rate',
        'trainer_東京_win_rate',
        'trainer_京都_win_rate',
        'jockey_中山_win_rate'
    ]
    
    found_cols = [c for c in target_cols if c in cols]
    
    with open('test_feature_result.txt', 'w', encoding='utf-8') as f:
        sys.stdout = f
        
        if found_cols:
            print("\n✅ SUCCESS! Found interaction features:")
            for c in found_cols:
                print(f"  - {c}")
        else:
            print("\n❌ FAILED. Interaction features NOT found.")
            print("Sample columns:", cols[:20])
            
        sys.stdout = sys.__stdout__
        print("Test results written to test_feature_result.txt")

if __name__ == "__main__":
    test_generation()
