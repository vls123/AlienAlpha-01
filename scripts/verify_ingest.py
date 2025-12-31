"""
Script to verify data ingestion from Binance to ArcticDB (crypto_m1).
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
from src.utils.time import to_utc

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def verify_ingest():
    # 1. Initialize
    logger.info("Initializing StorageEngine and HistoricalIngestor...")
    try:
        store = StorageEngine()
        store.connect()
        ingestor = HistoricalIngestor()
    except Exception as e:
        logger.error(f"Initialization failed: {e}")
        return

    # 2. Inspect existing library
    lib_name = "crypto_1m"
    try:
        # Do NOT create if missing, to strictly follow user rules
        lib = store.get_library(lib_name, create_if_missing=False)
        logger.info(f"Connected to library: {lib_name}")
        symbols = lib.list_symbols()
        logger.info(f"Existing symbols count: {len(symbols)}")
    except Exception as e:
        logger.error(f"Failed to access library {lib_name}: {e}")
        return

    # 3. Fetch data
    symbol = 'BTC/USDT'
    target_symbol = 'BTCUSDT' # Format for crypto_1m
    logger.info(f"Fetching 1m data for {symbol} from Binance...")
    try:
        # Fetching a small amount of data for verification
        df = ingestor.fetch_crypto_snapshot(symbol, timeframe='1m', limit=5)
        if df.empty:
            logger.error("Fetched DataFrame is empty.")
            return
        
        logger.info(f"Fetched {len(df)} rows.")
        print(df.head())
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
    logger.info(f"Storing data to {lib_name} as {target_symbol}...")
    try:
        lib.write(target_symbol, df)
        logger.info(f"Successfully wrote {target_symbol} to {lib_name}.")
    except Exception as e:
        logger.error(f"Failed to write data: {e}")
        return

    # 6. Read back and verify
    logger.info("Reading back data...")
    try:
        read_item = lib.read(target_symbol)
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
             logger.info("Data length matches.")
        else:
             logger.warning(f"Data length mismatch. Wrote: {len(df)}, Read: {len(read_df)}")

        print(read_df.head())

    except Exception as e:
        logger.error(f"Failed to read back data: {e}")
        return

if __name__ == "__main__":
    verify_ingest()
