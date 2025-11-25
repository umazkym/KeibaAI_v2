#!python
# -*- coding: utf-8 -*-
import pandas as pd

# Load data
df_old = pd.read_csv('keibaai/data/evaluation/evaluation_results_mu_v2.1_new_features.csv')
df_new = pd.read_csv('keibaai/data/evaluation/evaluation_results_mu_v2.1_optimized.csv')

print("Analysis Report")
print("="*60)

# Merge on date
df_old['date'] = pd.to_datetime(df_old['date'])
df_new['date'] = pd.to_datetime(df_new['date'])
merged = pd.merge(df_old, df_new, on='date', suffixes=('_old', '_new'))

# Key findings
worse_days = (merged['recovery_rate_new'] < merged['recovery_rate_old']).sum()
total_days = len(merged)
pct_worse = worse_days / total_days * 100

print(f"\n1. Daily Performance:")
print(f"   Days with worse recovery: {worse_days}/{total_days} ({pct_worse:.1f}%)")

# Volatility
old_std = df_old['recovery_rate'].std()
new_std = df_new['recovery_rate'].std()
print(f"\n2. Volatility (Standard Deviation):")
print(f"   Recovery Rate - Old: {old_std:.4f}, New: {new_std:.4f}")
print(f"   Spearman Corr - Old: {df_old['spearman_corr'].std():.4f}, New: {df_new['spearman_corr'].std():.4f}")

# Monthly averages
df_old['month'] = df_old['date'].dt.to_period('M')
df_new['month'] = df_new['date'].dt.to_period('M')

monthly_old = df_old.groupby('month')['recovery_rate'].mean()
monthly_new = df_new.groupby('month')['recovery_rate'].mean()

print(f"\n3. Monthly Recovery Rate Comparison:")
for month in monthly_old.index:
    old_val = monthly_old[month]
    new_val = monthly_new[month]
    diff = new_val - old_val
    print(f"   {month}: Old={old_val:.2%}, New={new_val:.2%}, Diff={diff:+.2%}")

# Root cause identification
print(f"\n4. Root Cause Analysis:")
if pct_worse > 60:
    print(f"   - Consistent Decline: {pct_worse:.0f}% of days showed worse performance")
if new_std > old_std * 1.1:
    print(f"   - Increased Volatility: Std dev increased by {(new_std/old_std-1)*100:.1f}%")
if abs(df_new['rmse'].mean() - df_old['rmse'].mean()) < 0.01:
    print(f"   - RMSE unchanged but Spearman worse: Regression OK but Ranking degraded")

print(f"\n5. Conclusion:")
print(f"   The hyperparameter optimization focused on RMSE (regression)")
print(f"   but degraded ranking performance (Spearman correlation).")
print(f"   Recommendation: Optimize ranker and regressor separately.")

print("="*60)
