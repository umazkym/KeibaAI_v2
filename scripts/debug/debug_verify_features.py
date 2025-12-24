import pandas as pd
import numpy as np

# データ読み込み
df = pd.read_parquet('keibaai/models/mu_v2_6/train_data_mu_v2_6.parquet')

# 検証する新特徴量
new_features = [
    # 騎手×調教師
    'combo_avg_finish', 'combo_win_rate', 'combo_races',
    # 血統
    'sire_avg_finish', 'sire_std_finish', 'sire_avg_distance',
    # コース適性
    'venue_avg_finish', 'venue_races', 'dist_avg_finish', 'surface_avg_finish',
    # 条件変化
    'distance_change', 'venue_change', 'surface_change',
    # 休養
    'days_since_last_race', 'is_rest_return',
    # バイアス
    'bias_seasonal_score', 'bias_dynamic_score',
    # ペース
    'pace_fit_score', 'n_leaders', 'leader_ratio',
    # トレンド
    'avg_finish_last3', 'avg_finish_last5', 'finish_std_last3', 'finish_cv_last3'
]

print("=== 新特徴量のデータ品質検証 ===\n")
print(f"総データ数: {len(df):,}行\n")

missing_features = []
zero_variance_features = []
high_nan_features = []
good_features = []

for feat in new_features:
    if feat not in df.columns:
        missing_features.append(feat)
        continue
    
    nan_count = df[feat].isna().sum()
    nan_rate = nan_count / len(df) * 100
    valid_data = df[feat].dropna()
    
    if len(valid_data) == 0:
        variance = 0
        unique_vals = 0
        min_val = max_val = mean_val = np.nan
    else:
        variance = valid_data.var()
        unique_vals = valid_data.nunique()
        min_val = valid_data.min()
        max_val = valid_data.max()
        mean_val = valid_data.mean()
    
    # カテゴリ分類
    if nan_rate > 90:
        high_nan_features.append((feat, nan_rate))
    elif variance == 0 and len(valid_data) > 0:
        zero_variance_features.append((feat, mean_val))
    else:
        good_features.append({
            'name': feat,
            'nan_rate': nan_rate,
            'variance': variance,
            'unique': unique_vals,
            'min': min_val,
            'max': max_val,
            'mean': mean_val
        })

# レポート出力
print("【1. 正常な特徴量】")
if good_features:
    for f in good_features:
        print(f"  {f['name']:30s} | NaN: {f['nan_rate']:6.2f}% | Var: {f['variance']:10.4f} | Mean: {f['mean']:8.4f} | Range: [{f['min']:.2f}, {f['max']:.2f}] | Unique: {f['unique']:5d}")
else:
    print("  なし")

print(f"\n【2. NaN率が高い特徴量（>90%）】: {len(high_nan_features)}個")
for feat, rate in high_nan_features:
    print(f"  {feat:30s} | NaN率: {rate:.2f}%")

print(f"\n【3. 分散がゼロの特徴量】: {len(zero_variance_features)}個")
for feat, val in zero_variance_features:
    print(f"  {feat:30s} | 値: {val}")

print(f"\n【4. カラムが存在しない特徴量】: {len(missing_features)}個")
for feat in missing_features:
    print(f"  {feat}")

print(f"\n【サマリー】")
print(f"  正常: {len(good_features)}個")
print(f"  高NaN率: {len(high_nan_features)}個")
print(f"  分散ゼロ: {len(zero_variance_features)}個")
print(f"  カラム不在: {len(missing_features)}個")
