from pathlib import Path
import os

PROJECT_ROOT = Path("C:/Users/zk-ht/Keiba/Keiba_AI_v2")
race_id = "202004030806"
bet_type = "sanrentan"
output_dir = PROJECT_ROOT / "keibaai/data/raw/html/odds" / bet_type
output_file = output_dir / f"{race_id}.bin"

print(f"Target file: {output_file}")
print(f"Exists (Path.exists): {output_file.exists()}")
print(f"Exists (os.path.exists): {os.path.exists(str(output_file))}")

# Check arguments logic simulation
args_force = False
skip_existing = not args_force
print(f"args.force: {args_force}")
print(f"skip_existing: {skip_existing}")

if skip_existing and output_file.exists():
    print("LOGIC CHECK: Should SKIP")
else:
    print("LOGIC CHECK: Should SCRAPE")
