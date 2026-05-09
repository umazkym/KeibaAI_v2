import pandas as pd

# Load evaluation results
df_old = pd.read_csv('keibaai/data/evaluation/evaluation_results_mu_v2.1_new_features.csv')
df_new = pd.read_csv('keibaai/data/evaluation/evaluation_results_mu_v2.1_optimized.csv')

print("=== モデル比較 (2024年評価) ===\n")
print("【元のモデル (v2.1_new_features)】")
print(f"  RMSE: {df_old['rmse'].mean():.4f}")
print(f"  Spearman相関: {df_old['spearman_corr'].mean():.4f}")
print(f"  回収率: {df_old['recovery_rate'].mean():.2%}")
print(f"  総損益: ¥{df_old['total_return'].sum() - df_old['total_bet'].sum():,.0f}\n")

print("【最適化モデル (v2.1_optimized)】")
print(f"  RMSE: {df_new['rmse'].mean():.4f}")
print(f"  Spearman相関: {df_new['spearman_corr'].mean():.4f}")
print(f"  回収率: {df_new['recovery_rate'].mean():.2%}")
print(f"  総損益: ¥{df_new['total_return'].sum() - df_new['total_bet'].sum():,.0f}\n")

print("【改善度】")
print(f"  RMSE改善: {df_old['rmse'].mean() - df_new['rmse'].mean():.4f} ({'改善' if df_old['rmse'].mean() > df_new['rmse'].mean() else '悪化'})")
print(f"  相関改善: {df_new['spearman_corr'].mean() - df_old['spearman_corr'].mean():+.4f}")
print(f"  回収率改善: {(df_new['recovery_rate'].mean() - df_old['recovery_rate'].mean()) * 100:+.2f}%")
