import pandas as pd
import numpy as np
from pathlib import Path

def check_past_finish_features():
    """過去着順関連特徴量の詳細チェック"""
    
    feature_dir = Path("keibaai/data/features/parquet")
    files_2024 = list(feature_dir.glob("year=2024/month=1/*.parquet"))
    
    if not files_2024:
        print("2024年のデータが見つかりません")
        return
    
    df = pd.read_parquet(files_2024[0])
    
    print(f"=== 過去着順系特徴量の分析 ===")
    print(f"データ行数: {len(df):,}\n")
    
    # 過去着順の平均特徴量
    past_finish_features = [col for col in df.columns if 'past_' in col and 'finish_position' in col and 'mean' in col]
    
    print(f"過去着順平均特徴量: {len(past_finish_features)}個\n")
    
    print("| 特徴量 | 欠損率 | 平均 | 中央値 | 最小 | 最大 | ゼロ率 | 標準偏差 |")
    print("|--------|--------|------|--------|-----|------|--------|----------|")
    
    for col in sorted(past_finish_features):
        if col in df.columns:
            data = df[col]
            missing = data.isna().sum() / len(data)
            zero_rate = (data == 0).sum() / len(data)
            
            if pd.api.types.is_numeric_dtype(data):
                mean_val = data.mean()
                median_val = data.median()
                min_val = data.min()
                max_val = data.max()
                std_val = data.std()
                
                print(f"| {col} | {missing:.2%} | {mean_val:.2f} | {median_val:.2f} | {min_val:.2f} | {max_val:.2f} | {zero_rate:.2%} | {std_val:.2f} |")
    
    # 問題のある特徴量を特定
    print("\n=== 異常検出 ===")
    problem_features = []
    
    for col in past_finish_features:
        if col in df.columns:
            data = df[col]
            zero_rate = (data == 0).sum() / len(data)
            missing_rate = data.isna().sum() / len(data)
            
            if zero_rate > 0.9:
                problem_features.append((col, f"ゼロ率{zero_rate:.1%}"))
            elif missing_rate > 0.9:
                problem_features.append((col, f"欠損率{missing_rate:.1%}"))
            elif data.std() == 0 or pd.isna(data.std()):
                problem_features.append((col, "分散ゼロ"))
    
    if problem_features:
        print(f"\n⚠️ 問題のある特徴量: {len(problem_features)}個")
        for feat, reason in problem_features[:10]:
            print(f"  - {feat}: {reason}")
    else:
        print("✅ 重大な問題は検出されませんでした")
    
    # サンプルデータを確認
    print(f"\n=== サンプルデータ (最初の5行) ===")
    sample_cols = [c for c in past_finish_features[:5] if c in df.columns]
    if sample_cols:
        print(df[sample_cols].head())

if __name__ == "__main__":
    check_past_finish_features()
