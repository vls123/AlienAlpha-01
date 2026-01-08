"""
Script to verify CTrader Historical Ingestion (1m) into ArcticDB.
"""
import sys
import os
import logging
import pandas as pd
from datetime import datetime, timedelta

# Adjust path to include src
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from src.data.store import StorageEngine
from src.data.ingest.historical import HistoricalIngestor

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def verify_forex_1m():
    # 1. Initialize
    logger.info("Initializing StorageEngine and HistoricalIngestor...")
    try:
        store = StorageEngine()
        store.connect()
        ingestor = HistoricalIngestor()
    except Exception as e:
        logger.error(f"Initialization failed: {e}")
        return

    # 2. Inspect Library
    lib_name = "forex_1m"
    try:
        # Create if missing (we are potentially populating a new structure or verifying existing)
        lib = store.get_library(lib_name, create_if_missing=True)
        logger.info(f"Connected to library: {lib_name}")
    except Exception as e:
        logger.error(f"Failed to access library {lib_name}: {e}")
        return

    # 3. Fetch data
    symbol = 'EURUSD'
    logger.info(f"Fetching {symbol} from CTrader...")
    try:
        # Last 1 day
        end_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        start_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d %H:%M:%S')
        
        df = ingestor.fetch_ctrader(symbol, start_date=start_date, end_date=end_date)
        if df.empty:
            logger.error("Fetched DataFrame is empty.")
            return
        
        logger.info(f"Fetched {len(df)} rows.")
        print(df.tail())
        print(df.index.dtype)
    except Exception as e:
        logger.error(f"Failed to fetch data: {e}")
        # traceback
        import traceback
        traceback.print_exc()
        return

    # 4. Store data
    logger.info(f"Storing data to {lib_name}...")
    try:
        lib.write(symbol, df)
        logger.info(f"Successfully wrote {symbol} to {lib_name}.")
    except Exception as e:
        logger.error(f"Failed to write data: {e}")
        return

    # 5. Read back
    logger.info("Reading back data...")
    try:
        read_item = lib.read(symbol)
        read_df = read_item.data
        if not read_df.empty:
            logger.info(f"Read back {len(read_df)} rows. UTC Index: {read_df.index.dtype}")
        else:
            logger.error("Read back empty dataframe")
    except Exception as e:
         logger.error(f"Failed to read back: {e}")

if __name__ == "__main__":
    verify_forex_1m()
