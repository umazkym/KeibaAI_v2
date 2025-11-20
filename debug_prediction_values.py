"""
予測値の分布を確認するデバッグスクリプト
"""
import pandas as pd
from pathlib import Path

print("="*80)
print("予測値の分布確認")
print("="*80)

# 評価結果CSVを読み込む
eval_path = Path('keibaai/data/evaluation/evaluation_results.csv')
if not eval_path.exists():
    print("評価結果CSVが見つかりません")
    exit(1)

eval_df = pd.read_csv(eval_path)

print(f"\n[1] 評価結果サマリー")
print(f"  日数: {len(eval_df)}")
print(f"  総レース数: {eval_df['race_count'].sum()}")
print(f"  総ベット額: ¥{eval_df['total_bet'].sum():,}")
print(f"  総払戻額: ¥{eval_df['total_return'].sum():,}")
print(f"  平均Hit Rate: {eval_df['hit_rate'].mean():.2%}")
print(f"  平均Recovery Rate: {eval_df['recovery_rate'].mean():.2%}")

print(f"\n[2] 異常な値の確認")
print(f"  RMSE=0の日数: {(eval_df['rmse'] == 0).sum()}/{len(eval_df)}")
print(f"  Spearman Corr=0の日数: {(eval_df['spearman_corr'] == 0).sum()}/{len(eval_df)}")
print(f"  Hit Rate=100%の日数: {(eval_df['hit_rate'] == 1.0).sum()}/{len(eval_df)}")
print(f"  Recovery Rate=0%の日数: {(eval_df['recovery_rate'] == 0.0).sum()}/{len(eval_df)}")

print(f"\n[3] 問題点")
if eval_df['hit_rate'].mean() > 0.5:
    print("  ⚠️ Hit Rateが異常に高い（50%以上）")
    print("     → モデルがwin_oddsを使って予測している可能性（データリーク）")

if eval_df['recovery_rate'].mean() == 0 and eval_df['hit_rate'].mean() > 0:
    print("  ⚠️ Hit Rateは高いのにRecovery Rateが0%")
    print("     → AI本命馬が3着以内に入っているが、1着になっていない")
    print("     → または、predicted_scoreが全て同じで、常に最初の馬を選んでいる")

if (eval_df['spearman_corr'] == 0).sum() > len(eval_df) * 0.9:
    print("  ⚠️ Spearman Correlationが0の日が多い")
    print("     → predicted_scoreが定数になっている可能性")

print(f"\n[4] 推奨対策")
print("  1. モデルの特徴量リストからwin_oddsを削除")
print("  2. モデルを再訓練")
print("  3. 予測値が正しく計算されているか確認")

# 詳細データを確認したい場合
print(f"\n[5] 詳細データ（最初の5日）:")
print(eval_df.head(5).to_string(index=False))

print("\n" + "="*80)
print("デバッグ完了")
print("="*80)
