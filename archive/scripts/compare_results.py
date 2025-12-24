import pandas as pd
import json
from pathlib import Path

def compare_models():
    eval_dir = Path("keibaai/data/evaluation")
    
    # 評価結果ファイルを探す
    result_files = sorted(eval_dir.glob("evaluation_result_*.json"), reverse=True)
    
    if len(result_files) < 2:
        print(f"比較に必要な評価結果が見つかりません。ファイル数: {len(result_files)}")
        return
    
    print("=== μモデル性能比較 ===\n")
    
    # 最新と以前のファイルを読み込み
    with open(result_files[0], 'r', encoding='utf-8') as f:
        new_result = json.load(f)
    
    with open(result_files[1], 'r', encoding='utf-8') as f:
        old_result = json.load(f)
    
    print(f"新モデル: {result_files[0].stem}")
    print(f"旧モデル: {result_files[1].stem}\n")
    
    # 主要指標を比較
    metrics = {
        "回収率(ROI)": "roi",
        "Spearman相関": "spearman_corr",
        "RMSE": "rmse",
        "予測数": "total_predictions"
    }
    
    print("| 指標 | 新モデル | 旧モデル | 改善 |")
    print("|------|----------|----------|------|")
    
    for name, key in metrics.items():
        new_val = new_result.get(key, 0)
        old_val = old_result.get(key, 0)
        
        if key == "rmse":
            # RMSEは低い方が良い
            diff = old_val - new_val
            diff_pct = (diff / old_val * 100) if old_val != 0 else 0
        else:
            diff = new_val - old_val
            diff_pct = (diff / old_val * 100) if old_val != 0 else 0
        
        if isinstance(new_val, float):
            print(f"| {name} | {new_val:.4f} | {old_val:.4f} | {diff:+.4f} ({diff_pct:+.2f}%) |")
        else:
            print(f"| {name} | {new_val:,} | {old_val:,} | {diff:+,} |")

if __name__ == "__main__":
    compare_models()
