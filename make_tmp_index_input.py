import pandas as pd
from pathlib import Path

src = Path(r"C:\Users\zk-ht\Keiba\Keiba_AI_v2\keibaai\data\features\parquet\year=2024\month=10\fdee5e482eae4742a9fbd7a378ef65ca-0.parquet")
dst = Path(r"C:\Users\zk-ht\Keiba\Keiba_AI_v2\tmp_index_input_2024_10.parquet")

df = pd.read_parquet(src)

# 必須列の補完
if "track_surface" not in df.columns:
    if "track_芝" in df.columns and "track_ダート" in df.columns:
        df["track_surface"] = "不明"
        df.loc[df["track_芝"] == 1, "track_surface"] = "芝"
        df.loc[df["track_ダート"] == 1, "track_surface"] = "ダート"
    else:
        df["track_surface"] = "不明"

for c, v in {
    "track_condition": "不明",
    "venue": "不明",
    "race_class": "不明",
    "course_direction": "不明",
    "is_outer_course": 0,
    "margin": 0.0,
}.items():
    if c not in df.columns:
        df[c] = v

df["is_outer_course"] = pd.to_numeric(df["is_outer_course"], errors="coerce").fillna(0).astype(int)
df["margin"] = pd.to_numeric(df["margin"], errors="coerce").fillna(0.0)

df.to_parquet(dst, index=False)
print("saved:", dst)
print("shape:", df.shape)
print("check cols:", [c for c in ["track_surface","track_condition","venue","race_class","course_direction","is_outer_course","margin"] if c in df.columns])
