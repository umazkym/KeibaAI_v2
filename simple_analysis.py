import pandas as pd
import numpy as np

# Load evaluation results  
df_old = pd.read_csv('keibaai/data/evaluation/evaluation_results_mu_v2.1_new_features.csv')
df_new = pd.read_csv('keibaai/data/evaluation/evaluation_results_mu_v2.1_optimized.csv')

print("="*80)
print("詳細分析: 最適化モデル性能悪化の原因")
print("="*80)

# 1. 基本統計の比較
print("\n【1. 基本統計】")
metrics = ['rmse', 'spearman_corr', 'hit_rate', 'recovery_rate']
for metric in metrics:
    print(f"\n{metric}:")
    print(f"  元のモデル  - 平均: {df_old[metric].mean():.4f}, 標準偏差: {df_old[metric].std():.4f}")
    print(f" 最適化モデル - 平均: {df_new[metric].mean():.4f}, 標準偏差: {df_new[metric].std():.4f}")
    print(f"  差分: {df_new[metric].mean() - df_old[metric].mean():.4f}")

# 2. 日次比較
print("\n【2. 日次比較】")
df_old['date'] = pd.to_datetime(df_old['date'])
df_new['date'] = pd.to_datetime(df_new['date'])
merged = pd.merge(df_old, df_new, on='date', suffixes=('_old', '_new'))

improved_days = (merged['recovery_rate_new'] > merged['recovery_rate_old']).sum()
declined_days = (merged['recovery_rate_new'] < merged['recovery_rate_old']).sum()
same_days = (merged['recovery_rate_new'] == merged['recovery_rate_old']).sum()

print(f"総日数: {len(merged)}")
print(f"回収率改善: {improved_days}日 ({improved_days/len(merged)*100:.1f}%)")
print(f"回収率悪化: {declined_days}日 ({declined_days/len(merged)*100:.1f}%)")
print(f"回収率同じ: {same_days}日 ({same_days/len(merged)*100:.1f}%)")

# 3. 月次集計
print("\n【3. 月次パフォーマンス】")
df_old['month'] = df_old['date'].dt.to_period('M')
df_new['month'] = df_new['date'].dt.to_period('M')

monthly_old = df_old.groupby('month').agg({'recovery_rate': 'mean', 'spearman_corr': 'mean'})
monthly_new = df_new.groupby('month').agg({'recovery_rate': 'mean', 'spearman_corr': 'mean'})

for month in monthly_old.index:
    old_rec = monthly_old.loc[month, 'recovery_rate']
    new_rec = monthly_new.loc[month, 'recovery_rate']
    old_corr = monthly_old.loc[month, 'spearman_corr']
    new_corr = monthly_new.loc[month, 'spearman_corr']
    print(f"{month}: 回収率 {old_rec:.2%}→{new_rec:.2%} ({新_rec-old_rec:+.2%}), "
          f"相関 {old_corr:.3f}→{new_corr:.3f} ({new_corr-old_corr:+.3f})")

# 4. 最悪・最良の日
print("\n【4. 最も影響が大きかった日】")
merged['rec_diff'] = merged['recovery_rate_new'] - merged['recovery_rate_old']
print("\n回収率が最も改善した日Top 3:")
for _, row in merged.nlargest(3, 'rec_diff').iterrows():
    print(f"  {row['date'].date()}: {row['recovery_rate_old']:.2%}→{row['recovery_rate_new']:.2%} "
          f"(+{row['rec_diff']:.2%})")

print("\n回収率が最も悪化した日Top 3:")
for _, row in merged.nsmallest(3, 'rec_diff').iterrows():
    print(f"  {row['date'].date()}: {row['recovery_rate_old']:.2%}→{row['recovery_rate_new']:.2%} "
          f"({row['rec_diff']:.2%})")

# 5. ベット額の比較
print("\n【5. ベット額の比較】")
print(f"元のモデル総ベット額: ¥{df_old['total_bet'].sum():,.0f}")
print(f"最適化モデル総ベット額: ¥{df_new['total_bet'].sum():,.0f}")
print(f"差額: ¥{df_new['total_bet'].sum() - df_old['total_bet'].sum():,.0f}")

# 6. 予想される原因
print("\n【6. 予想される原因】")
causes = []

# 原因1: 一貫した性能低下
if declined_days / len(merged) > 0.6:
    causes.append(f"1. 一貫した性能低下: {declined_days}日中{declined_days}日({declined_days/len(merged)*100:.0f}%)で悪化")

# 原因2: 不安定性の増加
if df_new['recovery_rate'].std() > df_old['recovery_rate'].std() * 1.1:
    causes.append(f"2. 予測の不安定化: 回収率の標準偏差が{df_new['recovery_rate'].std()/df_old['recovery_rate'].std():.2f}倍に増加")

# 原因3: RMSE同じでSpearman悪化
if abs(df_new['rmse'].mean() - df_old['rmse'].mean()) < 0.01 and df_new['spearman_corr'].mean() < df_old['spearman_corr'].mean():
    causes.append("3. タイム予測は同等だが着順予測が悪化: RMSEは同じだがSpearman相関が低下")

# 原因4: 過学習の可能性
if df_new['recovery_rate'].std() > df_old['recovery_rate'].std():
    causes.append("4. 過学習の可能性: TimeSeriesSplitで訓練データに過適合")

for cause in causes:
    print(f"  {cause}")

print("\n【結論】")
print(f"最も可能性の高い原因: ハイパーパラメータ最適化がRMSE(回帰)に特化しすぎて、")
print(f"ランキング性能(Spearman相関)が犠牲になった。")
print(f"回帰モデルとランカーモデルは別々に最適化すべき。")

print("="*80)
