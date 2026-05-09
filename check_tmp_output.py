import pandas as pd

p = r"C:\Users\zk-ht\Keiba\Keiba_AI_v2\tmp_output.parquet"
df = pd.read_parquet(p)

print("shape:", df.shape)
targets = ["t_index","expected_last3f","agari_gap","agari_index","expected_t_index","tenkai_adj_t_index"]
print("index cols:", [c for c in targets if c in df.columns])

show_cols = [c for c in ["race_id","horse_id","t_index","agari_index","tenkai_adj_t_index"] if c in df.columns]
print(df[show_cols].head(10))
