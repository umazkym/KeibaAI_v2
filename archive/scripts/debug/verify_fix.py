import sys
import os
from pathlib import Path
import logging

# Add project root to sys.path
project_root = Path(__file__).resolve().parent
sys.path.append(str(project_root))

from keibaai.src.modules.preparing import _scrape_html
from keibaai.src.modules.constants import LocalPaths

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_pedigree_fetch_requests():
    print("\n--- Testing Pedigree Fetch (requests) ---")
    horse_ids = ["2022105820"] # The one that failed
    updated = _scrape_html.scrape_html_ped(horse_ids, skip=False)
    print(f"Updated paths: {updated}")
    if updated and os.path.exists(updated[0]):
        print("SUCCESS: Pedigree file created.")
    else:
        print("FAILURE: Pedigree file NOT created.")

def test_race_id_caching():
    print("\n--- Testing Race ID Caching ---")
    date_list = ["20251116"]
    
    # 1. First fetch (should use Selenium and cache)
    print("1. First fetch (Selenium)...")
    race_ids_1 = _scrape_html.scrape_race_id_list(date_list)
    print(f"Fetched {len(race_ids_1)} race IDs.")
    
    # Check if cache file exists
    cache_path = LocalPaths.RACE_ID_CACHE_PATH
    if os.path.exists(cache_path):
        print(f"SUCCESS: Cache file created at {cache_path}")
    else:
        print(f"FAILURE: Cache file NOT created at {cache_path}")
        return

    # 2. Second fetch (should use Cache)
    print("2. Second fetch (Cache)...")
    # We can verify this by checking logs or speed, but for now we check if it returns same data
    race_ids_2 = _scrape_html.scrape_race_id_list(date_list)
    print(f"Fetched {len(race_ids_2)} race IDs.")
    
    if race_ids_1 == race_ids_2:
        print("SUCCESS: Cached data matches fetched data.")
    else:
        print("FAILURE: Cached data DOES NOT match.")

if __name__ == "__main__":
    test_pedigree_fetch_requests()
    test_race_id_caching()
