import pandas as pd
import numpy as np
from pathlib import Path
from scipy import stats

def compare_feature_distributions():
    """新旧モデルの特徴量分布を比較"""
    
    feature_dir = Path("keibaai/data/features/parquet")
    
    # 2024年1月のデータを読み込み（テストデータの一部）
    files_2024 = list(feature_dir.glob("year=2024/month=1/*.parquet"))
    
    if not files_2024:
        print("2024年のデータが見つかりません")
        return
    
    print(f"ファイル: {files_2024[0]}")
    df = pd.read_parquet(files_2024[0])
    
    print(f"\n=== 特徴量分布分析 (2024年1月データ) ===")
    print(f"総行数: {len(df):,}")
    print(f"総列数: {len(df.columns)}\n")
    
    # 重要な特徴量のリスト
    critical_features = [
        'distance_m',
        'track_ダート',
        'track_芝',
        'past_1_finish_position_mean',
        'past_3_finish_position_mean',
        'past_5_finish_position_mean',
        'horse_weight',
        'age',
        'career_starts',
        'jockey_win_rate',
        'trainer_win_rate'
    ]
    
    print("| 特徴量 | 欠損率 | 平均 | 標準偏差 | ゼロ率 | 分布異常 |")
    print("|--------|--------|------|----------|--------|----------|")
    
    for feature in critical_features:
        if feature not in df.columns:
            print(f"| {feature} | - | - | - | - | ❌ 存在しない |")
            continue
        
        col = df[feature]
        
        # 統計情報
        missing_rate = col.isna().sum() / len(col)
        zero_rate = (col == 0).sum() / len(col) if pd.api.types.is_numeric_dtype(col) else 0
        
        if pd.api.types.is_numeric_dtype(col):
            mean_val = col.mean()
            std_val = col.std()
            
            # 分布異常の検出
            anomaly = ""
            if missing_rate > 0.5:
                anomaly = "⚠️ 高欠損"
            elif zero_rate > 0.9:
                anomaly = "⚠️ ほぼゼロ"
            elif std_val == 0 or pd.isna(std_val):
                anomaly = "⚠️ 分散ゼロ"
            else:
                anomaly = "✅"
            
            print(f"| {feature} | {missing_rate:.2%} | {mean_val:.2f} | {std_val:.2f} | {zero_rate:.2%} | {anomaly} |")
        else:
            print(f"| {feature} | {missing_rate:.2%} | - | - | - | 非数値 |")
    
    # 全特徴量の統計サマリー
    print(f"\n=== 全特徴量の統計 ===")
    numeric_cols = df.select_dtypes(include=[np.number]).columns
    
    high_missing = [col for col in numeric_cols if df[col].isna().sum() / len(df) > 0.5]
    high_zero = [col for col in numeric_cols if (df[col] == 0).sum() / len(df) > 0.9]
    zero_variance = [col for col in numeric_cols if df[col].std() == 0 or pd.isna(df[col].std())]
    
    print(f"数値特徴量数: {len(numeric_cols)}")
    print(f"高欠損特徴量(>50%): {len(high_missing)}")
    print(f"高ゼロ率特徴量(>90%): {len(high_zero)}")
    print(f"分散ゼロ特徴量: {len(zero_variance)}")
    
    if high_missing:
        print(f"\n⚠️ 高欠損特徴量: {high_missing[:10]}")
    if high_zero:
        print(f"\n⚠️ 高ゼロ率特徴量: {high_zero[:10]}")
    if zero_variance:
        print(f"\n⚠️ 分散ゼロ特徴量: {zero_variance[:10]}")

if __name__ == "__main__":
    compare_feature_distributions()
