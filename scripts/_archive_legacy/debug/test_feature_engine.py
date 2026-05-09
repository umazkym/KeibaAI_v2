"""
feature_engine.pyの動作をテストするシンプルなスクリプト
"""
import pandas as pd
import sys
from pathlib import Path

# プロジェクトルートを追加
project_root = Path(__file__).resolve().parent / "keibaai"
sys.path.append(str(project_root))

from src.features.feature_engine import FeatureEngine

# 設定
races_path = 'keibaai/data/parsed/parquet/races/races.parquet'
shutuba_path = 'keibaai/data/parsed/parquet/shutuba/shutuba.parquet'

print("=" * 60)
print("FeatureEngineのテスト")
print("=" * 60)

# データ読み込み
races = pd.read_parquet(races_path)
shutuba = pd.read_parquet(shutuba_path)

# 2024年1月のデータだけテスト
races_sample = races[races['race_id'].str.startswith('202401')].copy()
shutuba_sample = shutuba[shutuba['race_id'].str.startswith('202401')].copy()

print(f"\nraces (2024年1月): {len(races_sample)}件")
print(f"shutuba (2024年1月): {len(shutuba_sample)}件")

# 賞金列の確認
prize_cols = [col for col in races_sample.columns if 'prize' in col.lower()]
print(f"\nraces の賞金列: {prize_cols}")

if 'prize_money' in races_sample.columns:
    non_zero = (races_sample['prize_money'] > 0).sum()
    print(f"  prize_money非ゼロ: {non_zero}件")

# FeatureEngineを使って特徴量生成
print("\n" + "=" * 60)
print("FeatureEngineで特徴量生成")
print("=" * 60)

try:
    engine = FeatureEngine(races_df=races_sample, shutuba_df=shutuba_sample)
    
    # 基本統計
    print(f"\n初期化成功")
    print(f"  races_df: {len(engine.races_df)}件")
    print(f"  shutuba_df: {len(engine.shutuba_df)}件")
    
    # generate_featuresメソッドを呼び出す
    features = engine.generate_features()
    
    print(f"\n特徴量生成完了: {len(features)}件")
    print(f"列数: {len(features.columns)}")
    
    # 賞金関連の特徴量を確認
    prize_feature_cols = [col for col in features.columns if 'prize' in col.lower()]
    print(f"\n賞金関連特徴量: {prize_feature_cols}")
    
    for col in prize_feature_cols:
        if pd.api.types.is_numeric_dtype(features[col]):
            non_zero = (features[col] > 0).sum()
            mean_val = features[col].mean()
            print(f"  {col}: 非ゼロ={non_zero}/{len(features)} ({non_zero/len(features)*100:.1f}%), 平均={mean_val:.2f}")
    
    # past関連の特徴量を確認
    past_cols = [col for col in features.columns if col.startswith('past_')]
    if past_cols:
        print(f"\n過去走特徴量（サンプル）:")
        for col in past_cols[:5]:
            if pd.api.types.is_numeric_dtype(features[col]):
                non_zero = (features[col] > 0).sum()
                print(f"  {col}: 非ゼロ={non_zero}/{len(features)}")
    
    # サンプルデータ表示
    if 'prize_total' in features.columns:
        print(f"\nprize_total上位5件:")
        top5 = features.nlargest(5, 'prize_total')[['horse_id', 'prize_total']]
        print(top5)
    
except Exception as e:
    print(f"\nエラー: {e}")
    import traceback
    traceback.print_exc()

print("\n" + "=" * 60)
