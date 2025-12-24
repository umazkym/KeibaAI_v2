import pickle
import os

path = r"keibaai\data\processed\stats_lookup.pkl"

if os.path.exists(path):
    with open(path, 'rb') as f:
        data = pickle.load(f)
    
    print(f"Total keys: {len(data)}")
    print("Sample keys (first 10):")
    for k in list(data.keys())[:10]:
        print(k)
        print(data[k])
        
    # Check for specific keys that failed
    test_keys = [
        ('函館', 1800, '芝', '良'),
        ('札幌', 1000, 'ダート', '良')
    ]
    
    for k in test_keys:
        if k in data:
            print(f"Found key: {k}")
        else:
            print(f"Key {k} NOT found.")
            
    print("\nSearching for Hakodate keys...")
    hako_keys = [k for k in data.keys() if '函館' in str(k)]
    print(f"Found {len(hako_keys)} Hakodate keys.")
    if hako_keys:
        print("Sample Hakodate keys:")
        for k in hako_keys[:5]:
            print(k)
else:
    print("File not found.")
