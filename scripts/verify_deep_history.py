
import os
import sys
import logging
from datetime import datetime, timezone
import pandas as pd
from dotenv import load_dotenv

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from src.data.ingest.ctrader import CTraderClient

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def verify_deep_history():
    load_dotenv()
    client_id = os.getenv("CTRADER_CLIENT_ID")
    client_secret = os.getenv("CTRADER_CLIENT_SECRET")
    access_token = os.getenv("CTRADER_ACCESS_TOKEN")
    account_id = os.getenv("CTRADER_ACCOUNT_ID")

    client = CTraderClient(client_id, client_secret, access_token, account_id)
    
    # 1. Test Mapped Symbol: NSXUSD -> USTEC
    symbol = "NSXUSD"
    # 2. Test Deep History: Jan 1 2025
    start_date = datetime(2025, 1, 1, tzinfo=timezone.utc)
    # Fetch 1 day
    end_date = datetime(2025, 1, 2, tzinfo=timezone.utc)
    
    logger.info(f"Fetching deep history for {symbol} ({start_date} - {end_date})...")
    
    df = client.fetch_history(symbol, start_date, end_date)
    
    if df.empty:
        logger.error("Fetched empty DataFrame. Check logs.")
        return

    logger.info("Successfully fetched data!")
    logger.info(df.head())
    logger.info(df.tail())
    
    # 3. Verify UTC
    logger.info(f"Index Timezone: {df.index.dtype}")
    if str(df.index.dtype) == 'datetime64[ns, UTC]':
        logger.info("CONFIRMED: DataFrame is in UTC.")
    else:
        logger.error(f"FAILED: DataFrame is NOT in UTC. Got {df.index.dtype}")

if __name__ == "__main__":
    verify_deep_history()
