from pathlib import Path

p = Path(r"keibaai/analysis/index_modeling.py")
s = p.read_text(encoding="utf-8")

old = '''    metrics = {
        "rows": int(len(out)),
        "t_index_mae_vs_expected": float(mean_absolute_error(out["t_index"], out["expected_t_index"])),
        "t_index_r2_vs_expected": float(r2_score(out["t_index"], out["expected_t_index"])),
    }
    print(json.dumps(metrics, ensure_ascii=False, indent=2))
'''

new = '''    eval_df = out[["t_index", "expected_t_index"]].replace([np.inf, -np.inf], np.nan).dropna()
    if len(eval_df) == 0:
        metrics = {
            "rows": int(len(out)),
            "eval_rows": 0,
            "t_index_mae_vs_expected": None,
            "t_index_r2_vs_expected": None,
            "warning": "No valid rows for metric evaluation (NaN/inf after transforms).",
        }
    else:
        metrics = {
            "rows": int(len(out)),
            "eval_rows": int(len(eval_df)),
            "t_index_mae_vs_expected": float(mean_absolute_error(eval_df["t_index"], eval_df["expected_t_index"])),
            "t_index_r2_vs_expected": float(r2_score(eval_df["t_index"], eval_df["expected_t_index"])),
        }
    print(json.dumps(metrics, ensure_ascii=False, indent=2))
'''

if old not in s:
    raise SystemExit("patch target not found. file content differs.")
s = s.replace(old, new)
p.write_text(s, encoding="utf-8")
print("patched:", p)
