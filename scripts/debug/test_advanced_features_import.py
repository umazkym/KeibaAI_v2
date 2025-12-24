
import sys
import os
import pandas as pd
import logging

# Add project root to path
sys.path.append(os.getcwd())

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_import():
    logger.info("Testing AdvancedFeatureEngine import...")
    try:
        from keibaai.src.features.advanced_features import AdvancedFeatureEngine
        engine = AdvancedFeatureEngine()
        logger.info("Successfully imported and instantiated AdvancedFeatureEngine")
        
        methods = [
            'generate_jockey_trainer_synergy',
            'generate_bloodline_features',
            'generate_deep_pedigree_features',
            'generate_course_bias_features'
        ]
        
        for method in methods:
            if hasattr(engine, method):
                logger.info(f"Method '{method}' exists")
            else:
                logger.error(f"Method '{method}' MISSING")
                
    except ImportError as e:
        logger.error(f"Failed to import AdvancedFeatureEngine: {e}")
    except Exception as e:
        logger.error(f"An error occurred: {e}")

if __name__ == "__main__":
    test_import()
