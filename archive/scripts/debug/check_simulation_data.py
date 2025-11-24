import sys
sys.path.insert(0, 'keibaai/src')

from pathlib import Path
import pandas as pd

print("=" * 80)
print("シミュレーション実行前のデータ整合性チェック")
print("=" * 80)

# 1. モデルファイルの存在確認
print("\n【1. モデルファイル確認】")
models_dir = Path('keibaai/data/models')
required_models = ['mu_model.pkl', 'sigma_model.pkl', 'nu_model.pkl']
required_features = ['feature_names.json', 'sigma_features.json', 'nu_features.json']

for model in required_models:
    model_path = models_dir / model
    if model_path.exists():
        size_kb = model_path.stat().st_size / 1024
        print(f"✓ {model}: {size_kb:.1f} KB")
    else:
        print(f"✗ {model}: 見つかりません")

for feature in required_features:
    feature_path = models_dir / feature
    if feature_path.exists():
        print(f"✓ {feature}")
    else:
        print(f"✗ {feature}: 見つかりません")

# 2. 予測データの確認
print("\n【2. 予測データ確認】")
predictions_path = Path('keibaai/data/predictions/parquet/mu_predictions.parquet')
if predictions_path.exists():
    pred_df = pd.read_parquet(predictions_path)
    print(f"✓ mu_predictions.parquet")
    print(f"  行数: {len(pred_df):,}")
    print(f"  カラム: {list(pred_df.columns)}")
    print(f"  race_id unique: {pred_df['race_id'].nunique()}")
    print(f"  horse_id unique: {pred_df['horse_id'].nunique()}")
    
    # μ/σ/νの値範囲チェック
    print(f"\n  【値範囲チェック】")
    for col in ['mu', 'sigma', 'nu']:
        if col in pred_df.columns:
            print(f"  {col}:")
            print(f"    min: {pred_df[col].min():.4f}")
            print(f"    max: {pred_df[col].max():.4f}")
            print(f"    mean: {pred_df[col].mean():.4f}")
            print(f"    異常値(NaN): {pred_df[col].isna().sum()}")
        else:
            print(f"  {col}: カラムなし")
    
    # サンプルデータ表示
    print(f"\n  【サンプルデータ（最初の3行）】")
    print(pred_df.head(3))
else:
    print(f"✗ mu_predictions.parquet: 見つかりません")

# 3. レース結果データの確認（シミュレーション用）
print("\n【3. レース結果データ確認（参考）】")
results_path = Path('keibaai/data/parsed/parquet/races')
if results_path.exists():
    try:
        from utils.data_utils import load_parquet_data_by_date
        from datetime import datetime
        
        # 2024年1月のデータをサンプル確認
        start_dt = datetime(2024, 1, 1)
        end_dt = datetime(2024, 1, 31)
        results_df = load_parquet_data_by_date(results_path, start_dt, end_dt)
        
        print(f"✓ レース結果データ（2024年1月サンプル）")
        print(f"  行数: {len(results_df):,}")
        if 'race_id' in results_df.columns:
            print(f"  race_id unique: {results_df['race_id'].nunique()}")
    except Exception as e:
        print(f"  警告: データロードエラー: {e}")
else:
    print(f"✗ レース結果ディレクトリ: 見つかりません")

# 4. シミュレータースクリプトの存在確認
print("\n【4. シミュレータースクリプト確認】")
simulator_path = Path('keibaai/src/sim/simulate_daily_races.py')
if simulator_path.exists():
    print(f"✓ simulate_daily_races.py")
else:
    print(f"✗ simulate_daily_races.py: 見つかりません")

simulator_core_path = Path('keibaai/src/sim/simulator.py')
if simulator_core_path.exists():
    size = simulator_core_path.stat().st_size
    print(f"✓ simulator.py ({size} bytes)")
else:
    print(f"✗ simulator.py: 見つかりません")

print("\n" + "=" * 80)
print("データ整合性チェック完了")
print("=" * 80)
