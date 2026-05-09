import sys
import os

# Add project root to path
sys.path.append(os.getcwd())

try:
    from keibaai.src.features.advanced_features import AdvancedFeatureEngine
    print("SUCCESS: Absolute import worked")
except Exception as e:
    print(f"FAILURE: Absolute import failed: {e}")

try:
    from keibaai.src.features import feature_engine
    print("SUCCESS: feature_engine imported")
except Exception as e:
    print(f"FAILURE: feature_engine import failed: {e}")
