import sys
import os

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), 'keibaai')))

try:
    from src.features.advanced_features import AdvancedFeatureEngine
    print("Successfully imported AdvancedFeatureEngine")
    
    engine = AdvancedFeatureEngine()
    print("Successfully initialized AdvancedFeatureEngine")
    
    # Check if methods exist
    methods = [
        'generate_performance_trend_features',
        'generate_course_affinity_features',
        'generate_jockey_trainer_synergy',
        'generate_bloodline_features',
        'generate_pace_features',
        'generate_deep_pedigree_features',
        'generate_course_bias_features',
        'generate_race_condition_features',
        'calculate_relative_metrics'
    ]
    
    for method in methods:
        if hasattr(engine, method):
            print(f"Method '{method}' exists")
        else:
            print(f"ERROR: Method '{method}' MISSING")
            
except Exception as e:
    print(f"Failed to import or initialize: {e}")
    import traceback
    traceback.print_exc()
