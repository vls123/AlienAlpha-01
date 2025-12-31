"""
Verification script for Data Engine modules.
Checks if modules can be imported and classes instantiated (even with missing deps).
"""
import sys
import logging

# Configure logging to stdout
logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

def test_imports():
    logger.info("Testing Data Engine Imports...")
    
    try:
        from src.utils.time import now_utc
        from src.data.store import StorageEngine
        from src.data.ingest.historical import HistoricalIngestor
        from src.maintenance.gap_filler import GapFiller
        from src.synthesis.gan_model import MarketGAN
        
        logger.info("Successfully imported all modules.")
        
        # Instantiate to check for runtime errors in __init__
        store = StorageEngine()
        logger.info("Instantiated StorageEngine")
        
        ingestor = HistoricalIngestor()
        logger.info("Instantiated HistoricalIngestor")
        
        gap_filler = GapFiller(store)
        logger.info("Instantiated GapFiller")
        
        gan = MarketGAN()
        logger.info("Instantiated MarketGAN")
        
    except ImportError as e:
        logger.error(f"Import Error: {e}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Runtime Error: {e}")
        sys.exit(1)
        
    logger.info("Data Engine Verification Passed (Modules Valid).")

if __name__ == "__main__":
    test_imports()
