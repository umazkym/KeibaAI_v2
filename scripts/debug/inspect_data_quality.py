import pandas as pd
import numpy as np
from pathlib import Path
import sys

# プロジェクトルートの設定
project_root = Path(__file__).resolve().parent
sys.path.append(str(project_root))

def inspect_parquet(file_path, name):
    print(f"\n{'='*20} Inspecting {name} {'='*20}")
    print(f"Path: {file_path}")
    
    if not file_path.exists():
        print(f"❌ File not found: {file_path}")
        return None

    try:
        df = pd.read_parquet(file_path)
    except Exception as e:
        print(f"❌ Failed to read parquet: {e}")
        return None
        
    print(f"Shape: {df.shape}")
    print(f"Columns: {df.columns.tolist()}")
    
    # 基本統計と欠損値
    print("\n--- Missing Values ---")
    missing = df.isnull().sum()
    print(missing[missing > 0])
    
    # 重複確認
    print("\n--- Duplicates ---")
    if 'race_id' in df.columns and 'horse_id' in df.columns:
        dup_subset = ['race_id', 'horse_id']
        dups = df.duplicated(subset=dup_subset).sum()
        print(f"Duplicates (race_id, horse_id): {dups}")
        if dups > 0:
            print("Sample duplicates:")
            print(df[df.duplicated(subset=dup_subset, keep=False)].sort_values(by=dup_subset).head(4))
    
    # IDフォーマット確認
    print("\n--- ID Formats ---")
    if 'race_id' in df.columns:
        print(f"race_id dtype: {df['race_id'].dtype}")
        print(f"race_id sample: {df['race_id'].head(3).tolist()}")
        # 文字列長や形式の確認
        if df['race_id'].dtype == 'object':
             lens = df['race_id'].astype(str).map(len).unique()
             print(f"race_id lengths: {lens}")

    if 'horse_id' in df.columns:
        print(f"horse_id dtype: {df['horse_id'].dtype}")
        print(f"horse_id sample: {df['horse_id'].head(3).tolist()}")
    
    if 'horse_number' in df.columns:
        print("\n--- Horse Numbers ---")
        print(f"horse_number unique values: {sorted(df['horse_number'].unique())}")
        zeros = (df['horse_number'] == 0).sum()
        print(f"Count of horse_number == 0: {zeros} ({zeros/len(df):.2%})")

    return df

def main():
    base_dir = Path("data")
    
    # 1. 予測結果データ (出力)
    pred_path = base_dir / "predictions/parquet/predictions_2024_full.parquet"
    pred_df = inspect_parquet(pred_path, "Predictions (Output)")
    
    # 2. 特徴量データ (入力1)
    # features_2024.parquet があると仮定、なければ features ディレクトリを探す
    features_dir = base_dir / "features/parquet/year=2024"
    features_path = None
    if features_dir.exists():
        # 最初のparquetファイルを取得
        for p in features_dir.glob("**/*.parquet"):
            features_path = p
            break
    
    if features_path:
        features_df = inspect_parquet(features_path, "Features 2024 (Input Sample)")
    else:
        print("❌ Features 2024 not found")
        features_df = None
    
    # 3. 出馬表データ (入力2)
    shutuba_path = base_dir / "parsed/parquet/shutuba/shutuba.parquet"
    if not shutuba_path.exists():
         # バックアップ等があるかもしれないので親ディレクトリも確認
         shutuba_path = base_dir / "parsed/parquet/shutuba.parquet"
    
    shutuba_df = inspect_parquet(shutuba_path, "Shutuba (Source)")
    
    # 結合テスト
    if pred_df is not None and shutuba_df is not None:
        print(f"\n{'='*20} Merge Analysis {'='*20}")
        
        # 型合わせ
        p_race_id_dtype = pred_df['race_id'].dtype
        s_race_id_dtype = shutuba_df['race_id'].dtype
        print(f"race_id dtypes: Pred={p_race_id_dtype}, Shutuba={s_race_id_dtype}")
        
        p_horse_id_dtype = pred_df['horse_id'].dtype
        s_horse_id_dtype = shutuba_df['horse_id'].dtype
        print(f"horse_id dtypes: Pred={p_horse_id_dtype}, Shutuba={s_horse_id_dtype}")
        
        # 共通のIDを確認
        p_races = set(pred_df['race_id'].astype(str))
        s_races = set(shutuba_df['race_id'].astype(str))
        common_races = p_races.intersection(s_races)
        print(f"Common race_ids: {len(common_races)} (Pred: {len(p_races)}, Shutuba: {len(s_races)})")
        
        if len(common_races) == 0:
            print("⚠️ No common race_ids found! Check formats.")
            print(f"Pred sample: {list(p_races)[:3]}")
            print(f"Shutuba sample: {list(s_races)[:3]}")
        else:
            # 共通レースでの結合率
            sample_race = list(common_races)[0]
            print(f"Testing merge on race_id: {sample_race}")
            
            p_subset = pred_df[pred_df['race_id'].astype(str) == sample_race].copy()
            s_subset = shutuba_df[shutuba_df['race_id'].astype(str) == sample_race].copy()
            
            print(f"Pred subset shape: {p_subset.shape}")
            print(f"Shutuba subset shape: {s_subset.shape}")
            
            # horse_id の比較
            p_horses = set(p_subset['horse_id'].astype(str))
            s_horses = set(s_subset['horse_id'].astype(str))
            common_horses = p_horses.intersection(s_horses)
            print(f"Common horse_ids in race {sample_race}: {len(common_horses)}")
            print(f"Pred horses: {p_horses}")
            print(f"Shutuba horses: {s_horses}")

if __name__ == "__main__":
    main()
