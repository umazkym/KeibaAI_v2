import pandas as pd
from pathlib import Path

eval_dir = Path("keibaai/data/evaluation")
new_file = eval_dir / "evaluation_results_mu_v2.1_fixed.csv"

df = pd.read_csv(new_file)
print("カラム名:")
print(df.columns.tolist())
print("\n最初の行:")
print(df.head(1))
