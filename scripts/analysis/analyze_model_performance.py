import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path

# Set style
sns.set_style('whitegrid')
plt.rcParams['font.family'] = 'MS Gothic'

# Load evaluation results
df_old = pd.read_csv('keibaai/data/evaluation/evaluation_results_mu_v2.1_new_features.csv')
df_new = pd.read_csv('keibaai/data/evaluation/evaluation_results_mu_v2.1_optimized.csv')

print("=" * 80)
print("詳細分析: 最適化モデル vs 元のモデル (2024年)")
print("=" * 80)

# 1. 基本統計
print("\n【1. 基本統計】")
print("\n元のモデル:")
print(df_old[['rmse', 'spearman_corr', 'hit_rate', 'recovery_rate']].describe())
print("\n最適化モデル:")
print(df_new[['rmse', 'spearman_corr', 'hit_rate', 'recovery_rate']].describe())

# 2. 日次推移の比較
print("\n【2. 日次推移の統計】")
df_old['date'] = pd.to_datetime(df_old['date'])
df_new['date'] = pd.to_datetime(df_new['date'])

merged = pd.merge(df_old, df_new, on='date', suffixes=('_old', '_new'))

print(f"\n日数: {len(merged)}")
print(f"\n回収率が改善した日数: {(merged['recovery_rate_new'] > merged['recovery_rate_old']).sum()}")
print(f"回収率が悪化した日数: {(merged['recovery_rate_new'] < merged['recovery_rate_old']).sum()}")
print(f"回収率が同じ日数: {(merged['recovery_rate_new'] == merged['recovery_rate_old']).sum()}")

print(f"\nSpearman相関が改善した日数: {(merged['spearman_corr_new'] > merged['spearman_corr_old']).sum()}")
print(f"Spearman相関が悪化した日数: {(merged['spearman_corr_new'] < merged['spearman_corr_old']).sum()}")

# 3. 最悪・最良の日を特定
print("\n【3. 最悪・最良のパフォーマンス日】")
merged['recovery_diff'] = merged['recovery_rate_new'] - merged['recovery_rate_old']
merged['corr_diff'] = merged['spearman_corr_new'] - merged['spearman_corr_old']

print("\n回収率が最も改善した日（Top 3）:")
print(merged.nlargest(3, 'recovery_diff')[['date', 'recovery_rate_old', 'recovery_rate_new', 'recovery_diff']])

print("\n回収率が最も悪化した日（Top 3）:")
print(merged.nsmallest(3, 'recovery_diff')[['date', 'recovery_rate_old', 'recovery_rate_new', 'recovery_diff']])

print("\nSpearman相関が最も改善した日（Top 3）:")
print(merged.nlargest(3, 'corr_diff')[['date', 'spearman_corr_old', 'spearman_corr_new', 'corr_diff']])

print("\nSpearman相関が最も悪化した日（Top 3）:")
print(merged.nsmallest(3, 'corr_diff')[['date', 'spearman_corr_old', 'spearman_corr_new', 'corr_diff']])

# 4. 月次集計
print("\n【4. 月次パフォーマンス比較】")
df_old['month'] = df_old['date'].dt.to_period('M')
df_new['month'] = df_new['date'].dt.to_period('M')

monthly_old = df_old.groupby('month').agg({
    'recovery_rate': 'mean',
    'spearman_corr': 'mean',
    'hit_rate': 'mean',
    'race_count': 'sum'
}).round(4)

monthly_new = df_new.groupby('month').agg({
    'recovery_rate': 'mean',
    'spearman_corr': 'mean',
    'hit_rate': 'mean',
    'race_count': 'sum'
}).round(4)

monthly_comp = pd.DataFrame({
    'recovery_old': monthly_old['recovery_rate'],
    'recovery_new': monthly_new['recovery_rate'],
    'recovery_diff': monthly_new['recovery_rate'] - monthly_old['recovery_rate'],
    'corr_old': monthly_old['spearman_corr'],
    'corr_new': monthly_new['spearman_corr'],
    'corr_diff': monthly_new['spearman_corr'] - monthly_old['spearman_corr']
})

print("\n月次比較:")
print(monthly_comp)

# 5. 分散・安定性の比較
print("\n【5. 安定性の比較（標準偏差）】")
print(f"\n元のモデル:")
print(f"  回収率 std: {df_old['recovery_rate'].std():.4f}")
print(f"  Spearman相関 std: {df_old['spearman_corr'].std():.4f}")
print(f"  RMSE std: {df_old['rmse'].std():.4f}")

print(f"\n最適化モデル:")
print(f"  回収率 std: {df_new['recovery_rate'].std():.4f}")
print(f"  Spearman相関 std: {df_new['spearman_corr'].std():.4f}")
print(f"  RMSE std: {df_new['rmse'].std():.4f}")

# 6. ベット数の比較
print("\n【6. ベット数の比較】")
print(f"\n元のモデル総ベット額: ¥{df_old['total_bet'].sum():,.0f}")
print(f"最適化モデル総ベット額: ¥{df_new['total_bet'].sum():,.0f}")
print(f"差額: ¥{df_new['total_bet'].sum() - df_old['total_bet'].sum():,.0f}")

print(f"\n平均日次ベット額:")
print(f"  元のモデル: ¥{df_old['total_bet'].mean():,.0f}")
print(f"  最適化モデル: ¥{df_new['total_bet'].mean():,.0f}")

# 7. 結論
print("\n" + "=" * 80)
print("【結論】")
print("=" * 80)

if df_new['recovery_rate'].mean() < df_old['recovery_rate'].mean():
    print("\n⚠️ 最適化モデルの回収率が悪化:")
    print(f"  悪化度: {(df_old['recovery_rate'].mean() - df_new['recovery_rate'].mean()) * 100:.2f}%")
    
if df_new['spearman_corr'].mean() < df_old['spearman_corr'].mean():
    print("\n⚠️ 最適化モデルのSpearman相関が悪化:")
    print(f"  悪化度: {df_old['spearman_corr'].mean() - df_new['spearman_corr'].mean():.4f}")

print("\n推定される原因:")
causes = []
if df_new['recovery_rate'].std() > df_old['recovery_rate'].std():
    causes.append("1. 最適化モデルの予測が不安定（高い標準偏差）")
if monthly_comp['recovery_diff'].min() < -0.1:
    causes.append("2. 特定の月で大幅に性能が悪化")
if (merged['recovery_rate_new'] < merged['recovery_rate_old']).sum() > len(merged) * 0.6:
    causes.append("3. 60%以上の日で回収率が悪化（一貫した性能低下）")

for cause in causes:
    print(f"  {cause}")

if not causes:
    print("  明確な原因が特定できませんでした。さらなる分析が必要です。")

print("\n" + "=" * 80)
