import pandas as pd
from pathlib import Path

def compare_csv_results():
    eval_dir = Path("keibaai/data/evaluation")
    
    # 新旧のCSVファイルを読み込み
    new_file = eval_dir / "evaluation_results_mu_v2.1_fixed.csv"
    old_file = eval_dir / "evaluation_results_mu_v2.1_new_features.csv"
    
    if not new_file.exists():
        print(f"新モデルの評価結果が見つかりません: {new_file}")
        return
    
    if not old_file.exists():
        print(f"旧モデルの評価結果が見つかりません: {old_file}")
        return
    
    new_df = pd.read_csv(new_file)
    old_df = pd.read_csv(old_file)
    
    print("=== μモデル性能比較 (2024年データ) ===\n")
    
    # 主要指標を抽出
    new_recovery = new_df['recovery_rate'].mean()
    old_recovery = old_df['recovery_rate'].mean()
    
    new_spearman = new_df['spearman_corr'].mean()
    old_spearman = old_df['spearman_corr'].mean()
    
    new_rmse = new_df['rmse'].mean()
    old_rmse = old_df['rmse'].mean()
    
    new_hit_rate = new_df['hit_rate'].mean()
    old_hit_rate = old_df['hit_rate'].mean()
    
    new_races = new_df['race_count'].sum()
    old_races = old_df['race_count'].sum()
    
    print(f"モデル: mu_v2.1_fixed (修正後) vs mu_v2.1_new_features (修正前)\n")
    print("| 指標 | 新モデル | 旧モデル | 改善 |")
    print("|------|----------|----------|------|")
    print(f"| 回収率 | {new_recovery:.4f} | {old_recovery:.4f} | {(new_recovery-old_recovery):+.4f} ({(new_recovery-old_recovery)/old_recovery*100:+.2f}%) |")
    print(f"| 的中率 | {new_hit_rate:.4f} | {old_hit_rate:.4f} | {(new_hit_rate-old_hit_rate):+.4f} ({(new_hit_rate-old_hit_rate)/old_hit_rate*100:+.2f}%) |")
    print(f"| Spearman相関 | {new_spearman:.4f} | {old_spearman:.4f} | {(new_spearman-old_spearman):+.4f} ({(new_spearman-old_spearman)/old_spearman*100:+.2f}%) |")
    print(f"| RMSE | {new_rmse:.4f} | {old_rmse:.4f} | {(old_rmse-new_rmse):+.4f} ({(old_rmse-new_rmse)/old_rmse*100:+.2f}%) |")
    print(f"| レース数 | {int(new_races):,} | {int(old_races):,} | - |")
    
    print("\n結論:")
    if new_recovery > old_recovery:
        print(f"✅ 回収率が改善しました ({old_recovery:.4f} → {new_recovery:.4f})")
    else:
        print(f"⚠️ 回収率が低下しました ({old_recovery:.4f} → {new_recovery:.4f})")
    
    if new_spearman > old_spearman:
        print(f"✅ 予測精度(Spearman)が改善しました ({old_spearman:.4f} → {new_spearman:.4f})")
    else:
        print(f"⚠️ 予測精度(Spearman)が低下しました ({old_spearman:.4f} → {new_spearman:.4f})")

if __name__ == "__main__":
    compare_csv_results()
