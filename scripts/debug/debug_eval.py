import subprocess
import sys

cmd = [
    sys.executable,
    "keibaai/src/models/evaluate_model.py",
    "--model_dir", "keibaai/data/models",
    "--start_date", "2024-01-01",
    "--end_date", "2024-01-31"
]

print(f"Running command: {' '.join(cmd)}")

try:
    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        encoding='utf-8',
        errors='replace'
    )
    
    with open('eval_full.log', 'w', encoding='utf-8') as f:
        f.write("STDOUT:\n")
        f.write(result.stdout)
        f.write("\nSTDERR:\n")
        f.write(result.stderr)
    
    print(f"Output written to eval_full.log. Exit code: {result.returncode}")

except Exception as e:
    print(f"Error running subprocess: {e}")
