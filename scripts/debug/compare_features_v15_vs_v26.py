
import logging
import sys
from pathlib import Path

# プロジェクトルートをパスに追加
sys.path.append(str(Path(__file__).resolve().parent.parent.parent))

from keibaai.src.features.leak_free_feature_engineer_v15 import LeakFreeFeatureEngineerV15
from keibaai.src.features.feature_engine import FeatureEngine

def main():
    # v15
    v15 = LeakFreeFeatureEngineerV15()
    v15_cols = set(v15.get_feature_columns())
    
    # v2.6 (FeatureEngine)
    # config経由でロードされる定義を確認（動的生成なので完全なリストは実行しないとわからないが、
    # features.yaml と AdvancedFeatureEngine の実装から推測可能）
    # ここでは、実際に生成された `train_data_mu_v2_6.parquet` のカラムを見るのが確実だが、
    # ファイルがない場合は `features.yaml` から推測するロジックが必要。
    # しかし、先ほどの実行で parquet は生成されているはず。
    
    parquet_path = Path("keibaai/models/mu_v2_6/train_data_mu_v2_6.parquet")
    if not parquet_path.exists():
        print("Error: train_data_mu_v2_6.parquet missing.")
        return

    import pandas as pd
    df_v26 = pd.read_parquet(parquet_path)
    v26_cols = set([c for c in df_v26.columns if c not in ['race_id', 'horse_id', 'race_date', 'finish_position', 'is_win', 'win_odds']])
    
    # Compare
    missing_in_v26 = v15_cols - v26_cols
    extra_in_v26 = v26_cols - v15_cols
    
    print(f"v15 Features: {len(v15_cols)}")
    print(f"v2.6 Features: {len(v26_cols)}")
    print("-" * 30)
    print(f"Missing in v2.6 (Key Candidates for ROI Drop): {len(missing_in_v26)}")
    for c in sorted(missing_in_v26):
        print(f" - {c}")
        
    print("-" * 30)
    print(f"Extra in v2.6 (New Features): {len(extra_in_v26)}")
    # for c in sorted(extra_in_v26):
    #     print(f" + {c}")

if __name__ == "__main__":
    main()
