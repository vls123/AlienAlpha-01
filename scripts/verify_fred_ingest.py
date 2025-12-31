"""
Script to verify data ingestion from FRED to ArcticDB (macro_economic).
"""
import sys
import os
import logging
import pandas as pd
from datetime import timezone

# Adjust path to include src
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from src.data.store import StorageEngine
from src.data.ingest.historical import HistoricalIngestor
import src.utils.time as time_utils

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def verify_fred_ingest():
    # 1. Initialize
    logger.info("Initializing StorageEngine and HistoricalIngestor...")
    try:
        store = StorageEngine()
        store.connect()
        ingestor = HistoricalIngestor()
    except Exception as e:
        logger.error(f"Initialization failed: {e}")
        return

    # Check if FRED client is active
    if not ingestor.fred:
        logger.error("FRED client not initialized! Check FRED_API_KEY in .env")
        return

    # 2. Inspect/Create library
    lib_name = "economics_macro"
    try:
        # Do NOT create if missing
        lib = store.get_library(lib_name, create_if_missing=False)
        logger.info(f"Connected to library: {lib_name}")
    except Exception as e:
        logger.error(f"Failed to access library {lib_name}: {e}")
        return

    # 3. Fetch data
    series_id = 'GDP' # Gross Domestic Product
    logger.info(f"Fetching {series_id} from FRED...")
    try:
        # Fetching last 5 years
        df = ingestor.fetch_fred(series_id, start_date='2020-01-01')
        if df.empty:
            logger.error("Fetched DataFrame is empty.")
            return
        
        logger.info(f"Fetched {len(df)} rows.")
        print(df.tail())
        print(df.index.dtype)
    except Exception as e:
        logger.error(f"Failed to fetch data: {e}")
        return

    # 4. Verify UTC
    if df.index.tz != timezone.utc:
        logger.error("Fetched data is NOT in UTC!")
        return
    logger.info("Verified fetched data is in UTC.")

    # 5. Store data
    logger.info(f"Storing data to {lib_name}...")
    try:
        lib.write(series_id, df)
        logger.info(f"Successfully wrote {series_id} to {lib_name}.")
    except Exception as e:
        logger.error(f"Failed to write data: {e}")
        return

    # 6. Read back and verify
    logger.info("Reading back data...")
    try:
        read_item = lib.read(series_id)
        read_df = read_item.data
        
        if read_df.empty:
            logger.error("Read DataFrame is empty.")
            return

        if read_df.index.tz != timezone.utc:
            logger.error("Read data index is NOT in UTC/Timezone-aware!")
        else:
            logger.info("Read data index is correctly UTC.")
             
        # Simple shape check
        if len(read_df) == len(df):
             logger.info(f"Data length matches: {len(read_df)}")
        else:
             logger.warning(f"Data length mismatch. Wrote: {len(df)}, Read: {len(read_df)}")

        print(read_df.tail())

    except Exception as e:
        logger.error(f"Failed to read back data: {e}")
        return

if __name__ == "__main__":
    verify_fred_ingest()
