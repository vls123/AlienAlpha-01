
import os
import sys
import logging
import time
import pandas as pd
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from src.data.store import StorageEngine
from src.data.ingest.ctrader import CTraderClient

# Setup Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("backfill.log"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

CHUNK_DAYS = 3
SLEEP_BETWEEN_REQS = 0.25 # 4 reqs/sec

def backfill():
    load_dotenv()
    
    # 1. Init Storage
    store = StorageEngine()
    store.connect()
    lib = store.get_library('forex_1m')
    all_symbols = lib.list_symbols()
    
    # 2. Init Client
    client = CTraderClient(
        os.getenv("CTRADER_CLIENT_ID"),
        os.getenv("CTRADER_CLIENT_SECRET"),
        os.getenv("CTRADER_ACCESS_TOKEN"),
        os.getenv("CTRADER_ACCOUNT_ID")
    )

    start_date = datetime(2025, 1, 1, tzinfo=timezone.utc)
    end_date = datetime.now(timezone.utc)
    
    total_symbols = len(all_symbols)
    logger.info(f"Starting Backfill for {total_symbols} symbols from {start_date} to {end_date}")
    
    start_time = time.time()
    
    for i, symbol in enumerate(all_symbols):
        if symbol == "UDXUSD":
            logger.warning(f"Skipping {symbol} (Known Missing)")
            continue
            
        logger.info(f"[{i+1}/{total_symbols}] Processing {symbol}...")
        
        # Chunking Loop
        current_start = start_date
        symbol_data = []
        
        while current_start < end_date:
            current_end = min(current_start + timedelta(days=CHUNK_DAYS), end_date)
            
            try:
                df = client.fetch_history(symbol, current_start, current_end)
                if not df.empty:
                    symbol_data.append(df)
                    # logger.info(f"   Fetched {len(df)} rows for {current_start.date()}")
                else:
                    pass
                    # logger.debug(f"   No data for {current_start.date()}")
            except Exception as e:
                logger.error(f"Error fetching {symbol} window {current_start}: {e}")
            
            time.sleep(SLEEP_BETWEEN_REQS)
            current_start = current_end
        
        # Merge and Write
        if symbol_data:
            full_df = pd.concat(symbol_data)
            full_df = full_df[~full_df.index.duplicated(keep='first')] # Dedup
            full_df.sort_index(inplace=True)
            
            # Write to ArcticDB
            # We use 'overwrite' for the segment or 'append'?
            # Since we are fetching full history from fixed start, overwrite is safer to ensure consistency
            # But we want to keep older history if it exists?
            # User said "Add missing... backfill...".
            # Safest is `lib.write` which versions it.
            
            logger.info(f"   >>> Writing {len(full_df)} rows to {symbol} (Last: {full_df.index[-1]})")
            lib.write(symbol, full_df)
        else:
            logger.warning(f"   No data found for {symbol}")
            
        # Periodic Progress Report (every ~5 mins logic)
        elapsed = time.time() - start_time
        if elapsed > 300: # Every 5 mins
            logger.info(f"--- STATUS REPORT: Processed {i+1}/{total_symbols} symbols in {elapsed/60:.1f} minutes ---")
            # Create a marker file or similar if needed, but logging is enough

    logger.info("Backfill Complete.")
    client.disconnect()

if __name__ == "__main__":
    backfill()
