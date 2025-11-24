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

def test_calendar_caching():
    print("\n--- Testing Calendar Caching ---")
    # Use a past month to ensure caching is used
    from_date = "2024-10-01"
    to_date = "2024-10-31"
    
    # 1. First fetch (should fetch from web and cache)
    print("1. First fetch (Web)...")
    dates_1 = _scrape_html.scrape_kaisai_date(from_date, to_date)
    print(f"Fetched {len(dates_1)} dates.")
    
    # Check if cache file exists
    cache_path = LocalPaths.CALENDAR_CACHE_PATH
    if os.path.exists(cache_path):
        print(f"SUCCESS: Cache file created at {cache_path}")
    else:
        print(f"FAILURE: Cache file NOT created at {cache_path}")
        return

    # 2. Second fetch (should use Cache)
    print("2. Second fetch (Cache)...")
    dates_2 = _scrape_html.scrape_kaisai_date(from_date, to_date)
    print(f"Fetched {len(dates_2)} dates.")
    
    if dates_1 == dates_2:
        print("SUCCESS: Cached data matches fetched data.")
    else:
        print("FAILURE: Cached data DOES NOT match.")

if __name__ == "__main__":
    test_calendar_caching()
