
import os
import sys
import logging
from dotenv import load_dotenv

# Add src to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from src.data.store import StorageEngine
from src.data.ingest.ctrader import CTraderClient

# Configure Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def check_tickers():
    load_dotenv()
    
    # 1. Check ArcticDB
    logger.info("Checking ArcticDB 'forex_1m' library...")
    storage = StorageEngine()
    storage.connect()
    lib = storage.get_library('forex_1m')
    arctic_symbols = lib.list_symbols()
    logger.info(f"ArcticDB has {len(arctic_symbols)} symbols: {arctic_symbols}")

    # 2. Check CTrader
    logger.info("Checking CTrader available symbols...")
    client_id = os.getenv("CTRADER_CLIENT_ID")
    client_secret = os.getenv("CTRADER_CLIENT_SECRET")
    access_token = os.getenv("CTRADER_ACCESS_TOKEN")
    account_id = os.getenv("CTRADER_ACCOUNT_ID")

    if not all([client_id, client_secret, access_token, account_id]):
        logger.error("Missing CTrader credentials in .env")
        return

    client = CTraderClient(client_id, client_secret, access_token, account_id)
    ctrader_symbols = client.fetch_all_symbols()
    
    logger.info(f"CTrader returned {len(ctrader_symbols)} symbols.")
    
    # Extract names
    ctrader_names = set(s['name'] for s in ctrader_symbols)
    
    # 3. Compare
    logger.info("-" * 40)
    logger.info("COMPARISON:")
    
    available = []
    missing = []
    
    for sym in arctic_symbols:
        if sym in ctrader_names:
            available.append(sym)
        else:
            missing.append(sym)
            
    logger.info(f"Available on CTrader: {len(available)}/{len(arctic_symbols)}")
    if missing:
        logger.warning(f"Missing on CTrader: {missing}")
    else:
        logger.info("All ArcticDB symbols are available on CTrader!")

    # Print a sample of CTrader symbols to see format if mismatches
    logger.info(f"Sample CTrader Symbols: {list(ctrader_names)[:10]}")

if __name__ == "__main__":
    check_tickers()
