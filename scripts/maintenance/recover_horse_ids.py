import os
from pathlib import Path
from datetime import datetime, timedelta
import re

# Project root
PROJECT_ROOT = Path(os.getcwd())
HTML_RACE_DIR = PROJECT_ROOT / "keibaai/data/raw/html/race"
HORSE_ID_LIST = PROJECT_ROOT / "horse_id_list.txt"

def extract_horse_ids_from_file(filepath):
    """Extract horse IDs from a single HTML file"""
    try:
        with open(filepath, 'r', encoding='utf-8', errors='ignore') as f: # specific encoding might be needed (euc-jp often used by netkeiba)
            content = f.read()
            # Regex for horse urls: /horse/1234567890
            ids = set(re.findall(r'/horse/(\d{10})', content))
            return ids
    except Exception as e:
        print(f"Error reading {filepath}: {e}")
        return set()

def main():
    print("Starting rapid horse ID extraction from recent files...")
    
    # Load existing IDs
    existing_ids = set()
    if HORSE_ID_LIST.exists():
        with open(HORSE_ID_LIST, 'r', encoding='utf-8') as f:
            existing_ids = set(line.strip() for line in f if line.strip())
    print(f"Loaded {len(existing_ids)} existing horse IDs.")
    
    # Find recent files (modified in the last 24 hours)
    cutoff_time = datetime.now() - timedelta(hours=2) # Just the session time
    recent_files = []
    
    if not HTML_RACE_DIR.exists():
        print(f"Directory not found: {HTML_RACE_DIR}")
        return

    print("Scanning for recently modified files...")
    count = 0
    new_ids = set()
    
    for f in HTML_RACE_DIR.glob("*.bin"):
        mtime = datetime.fromtimestamp(f.stat().st_mtime)
        if mtime > cutoff_time:
            recent_files.append(f)
            
    print(f"Found {len(recent_files)} recent files.")
    
    # Extract
    for i, f in enumerate(recent_files):
        ids = extract_horse_ids_from_file(f)
        new_ids.update(ids)
        if i % 100 == 0:
            print(f"Processed {i}/{len(recent_files)} files...")

    # Calculate diff
    added_ids = new_ids - existing_ids
    print(f"Found {len(new_ids)} IDs in recent files.")
    print(f"New IDs to add: {len(added_ids)}")
    
    if added_ids:
        with open(HORSE_ID_LIST, 'a', encoding='utf-8') as f:
            for hid in added_ids:
                f.write(f"{hid}\n")
        print(f"Updated {HORSE_ID_LIST}")
    else:
        print("No new IDs found.")

if __name__ == "__main__":
    main()
