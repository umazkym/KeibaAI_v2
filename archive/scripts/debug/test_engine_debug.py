"""
修正したfeature_engine.pyをdirectテスト
"""
import pandas as pd
import yaml
import logging
import sys
from pathlib import Path

# プロジェクトルートを追加
project_root = Path(__file__).resolve().parent / "keibaai"
sys.path.append(str(project_root))

from src.features.feature_engine import FeatureEngine

# ログ設定
logging.basicConfig(level=logging.DEBUG, format='{%(levelname)s} %(message)s')

# データ読み込み
print("=" * 60)
print("データ読み込み中...")
print("=" * 60)

history_df = pd.read_parquet('keibaai/data/parsed/parquet/races/races.parquet')
shutuba_df = pd.read_parquet('keibaai/data/parsed/parquet/shutuba/shutuba.parquet')
horse_profiles_df = pd.read_parquet('keibaai/data/parsed/parquet/horses/horses.parquet')

# 小さいデータセットでテスト（2024年1月1日のみ）
history_df = history_df[history_df['race_id'].str.startswith('20240101')].copy()
shutuba_df = shutuba_df[shutuba_df['race_id'].str.startswith('20240101')].copy()

print(f"history_df: {len(history_df)}件")
print(f"shutuba_df: {len(shutuba_df)}件")

# prize_moneyの確認
if 'prize_money' in history_df.columns:
    non_zero = (history_df['prize_money'] > 0).sum()
    print(f"history_df prize_money非ゼロ: {non_zero}件")

#  設定ファイル
features_config = 'keibaai/configs/features.yaml'

print("\n" + "=" * 60)
print("FeatureEngine初期化")
print("=" * 60)

engine = FeatureEngine(features_config)

# 特徴量生成
print("\n" + "=" * 60)
print("特徴量生成開始")
print("=" * 60)

try:
    features = engine.generate_features(
        shutuba_df=shutuba_df,
        results_history_df=history_df,
        horse_profiles_df=horse_profiles_df,
        pedigree_df=pd.DataFrame()
    )
    
    print(f"\n生成完了: {len(features)}件, {len(features.columns)}列")
    
    # 賞金関連特徴量を確認
    prize_cols = [col for col in features.columns if 'prize' in col.lower()]
    print(f"\n賞金関連特徴量: {prize_cols}")
    
    for col in prize_cols:
        if pd.api.types.is_numeric_dtype(features[col]):
            non_zero = (features[col] > 0).sum()
            mean_val = features[col].mean()
            print(f"  {col}: 非ゼロ={non_zero}/{len(features)} ({non_zero/len(features)*100:.1f}%), 平均={mean_val:.2f}")
    
    # サンプル表示
    if 'prize_total' in features.columns and (features['prize_total'] > 0).any():
        print(f"\nprize_total上位5件:")
        top5 = features.nlargest(5, 'prize_total')[['horse_id', 'prize_total', 'career_starts']]
        print(top5)
    else:
        print(f"\nWARNING: prize_totalが全てゼロです")

except Exception as e:
    print(f"\nエラー: {e}")
    import traceback
    traceback.print_exc()
