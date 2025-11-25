import pandas as pd

# Load all three evaluation results
df_original = pd.read_csv('keibaai/data/evaluation/evaluation_results_mu_v2.1_new_features.csv')
df_optimized = pd.read_csv('keibaai/data/evaluation/evaluation_results_mu_v2.1_optimized.csv')
df_hybrid = pd.read_csv('keibaai/data/evaluation/evaluation_results_mu_v2.1_hybrid.csv')

print("="*70)
print("Final Model Comparison - All Three Versions (2024 Evaluation)")
print("="*70)

models = {
    'Original (v2.1_new_features)': df_original,
    'Optimized (RMSE-focused)': df_optimized,
    'Hybrid (Ranker manual + Regressor optimized)': df_hybrid
}

# Summary table
print("\nPerformance Metrics:\n")
print(f"{'Model':<50} {'RMSE':<10} {'Spearman':<10} {'RecoveryRate':<15} {'TotalLoss'}")
print("-"*95)

results = {}
for name, df in models.items():
    rmse = df['rmse'].mean()
    spearman = df['spearman_corr'].mean()
    recovery = df['recovery_rate'].mean()
    total_loss = df['total_return'].sum() - df['total_bet'].sum()
    
    results[name] = {
        'rmse': rmse,
        'spearman': spearman,
        'recovery': recovery,
        'loss': total_loss
    }
    
    print(f"{name:<50} {rmse:<10.4f} {spearman:<10.4f} {recovery:<15.2%} {total_loss:,.0f}")

# Find best model
best_recovery_model = max(results.items(), key=lambda x: x[1]['recovery'])
best_spearman_model = max(results.items(), key=lambda x: x[1]['spearman'])
best_loss_model = max(results.items(), key=lambda x: x[1]['loss'])

print("\n" + "="*70)
print("Winner Analysis:")
print("="*70)
print(f"Best Recovery Rate: {best_recovery_model[0]} ({best_recovery_model[1]['recovery']:.2%})")
print(f"Best Spearman Correlation: {best_spearman_model[0]} ({best_spearman_model[1]['spearman']:.4f})")
print(f"Smallest Loss: {best_loss_model[0]} ({best_loss_model[1]['loss']:,.0f})")

# Improvement from optimized to hybrid
if 'Hybrid' in best_recovery_model[0]:
    improvement = results['Hybrid (Ranker manual + Regressor optimized)']['recovery'] - results['Optimized (RMSE-focused)']['recovery']
    print(f"\nHybrid improvement over Optimized: {improvement*100:+.2f}%")
    
    loss_improvement = results['Hybrid (Ranker manual + Regressor optimized)']['loss'] - results['Optimized (RMSE-focused)']['loss']
    print(f"Loss improvement: {loss_improvement:,.0f}")

print("\n" + "="*70)
print("Recommendation:")
print("="*70)

# Determine recommendation
recommendation = max(results.items(), key=lambda x: (x[1]['recovery'], -x[1]['loss']))
print(f"\n✓ RECOMMENDED MODEL: {recommendation[0]}")
print(f"  - Recovery Rate: {recommendation[1]['recovery']:.2%}")
print(f"  - Spearman Correlation: {recommendation[1]['spearman']:.4f}")
print(f"  - Total Loss: {recommendation[1]['loss']:,.0f}")
print("="*70)
