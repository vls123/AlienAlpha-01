"""
Script to verify data ingestion from Yahoo Finance to ArcticDB (stocks_1d).
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

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def verify_stocks_ingest():
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
    lib_name = "stocks_1d"
    try:
        # Do NOT create if missing (it should exist per census)
        lib = store.get_library(lib_name, create_if_missing=False)
        logger.info(f"Connected to library: {lib_name}")
        symbols = lib.list_symbols()
        logger.info(f"Existing symbols count: {len(symbols)}")
    except Exception as e:
        logger.error(f"Failed to access library {lib_name}: {e}")
        return

    # 3. Fetch data
    symbol = 'AAPL'
    logger.info(f"Fetching {symbol} from Yahoo Finance...")
    try:
        # Fetching last 5 days
        import datetime
        end_date = datetime.datetime.now().strftime('%Y-%m-%d')
        start_date = (datetime.datetime.now() - datetime.timedelta(days=7)).strftime('%Y-%m-%d')
        
        df = ingestor.fetch_yahoo(symbol, start_date=start_date, end_date=end_date)
        if df.empty:
            logger.error("Fetched DataFrame is empty.")
            return

        # Flatten MultiIndex columns if present (common in newer yfinance)
        if isinstance(df.columns, pd.MultiIndex):
            logger.info("Flattening MultiIndex columns...")
            # Keep only the Price level (Open, High, Low, Close, Volume)
            # Assuming level 0 is Price and level 1 is Ticker, or vice versa depending on download call.
            # Usually download(..., group_by='ticker') gives Ticker -> Price
            # but default gives Price -> Ticker.
            # Let's inspect the sample output:
            # Price                           Close        High         Low        Open    Volume
            # Ticker                           AAPL        AAPL        AAPL        AAPL      AAPL
            # So level 0 is Price, level 1 is Ticker.
            # We want to drop level 1.
            df.columns = df.columns.get_level_values(0)
        
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
        lib.write(symbol, df)
        logger.info(f"Successfully wrote {symbol} to {lib_name}.")
    except Exception as e:
        logger.error(f"Failed to write data: {e}")
        return

    # 6. Read back and verify
    logger.info("Reading back data...")
    try:
        read_item = lib.read(symbol)
        read_df = read_item.data
        
        if read_df.empty:
            logger.error("Read DataFrame is empty.")
            return

        if read_df.index.tz != timezone.utc:
             logger.error("Read data index is NOT in UTC/Timezone-aware!")
        else:
             logger.info("Read data index is correctly UTC.")
             
        # Simple shape check (read_df might be larger if we appended or if data existed, but here we overwrote)
        # Yahoo fetch might vary slightly in length vs stored depending on how write handles it, 
        # but Arctic write default is overwrite unless append used.
        if len(read_df) >= len(df):
             logger.info(f"Data length valid: {len(read_df)}")
        else:
             logger.warning(f"Data length mismatch. Wrote: {len(df)}, Read: {len(read_df)}")

        print(read_df.tail())

    except Exception as e:
        logger.error(f"Failed to read back data: {e}")
        return

if __name__ == "__main__":
    verify_stocks_ingest()
