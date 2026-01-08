
import os
import sys
import logging
import json
from dotenv import load_dotenv

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from src.data.ingest.ctrader import CTraderClient

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def dump_symbols():
    load_dotenv()
    client_id = os.getenv("CTRADER_CLIENT_ID")
    client_secret = os.getenv("CTRADER_CLIENT_SECRET")
    access_token = os.getenv("CTRADER_ACCESS_TOKEN")
    account_id = os.getenv("CTRADER_ACCOUNT_ID")

    client = CTraderClient(client_id, client_secret, access_token, account_id)
    symbols = client.fetch_all_symbols()
    
    # Sort by name
    symbols.sort(key=lambda x: x['name'])
    
    with open("ctrader_symbols_dump.json", "w") as f:
        json.dump(symbols, f, indent=2)
    
    logger.info(f"Dumped {len(symbols)} symbols to ctrader_symbols_dump.json")

    # Search helper
    keywords = ["Oil", "Brent", "WTI", "USD", "Dollar", "Stoxx", "EUR", "UK", "100", "FTSE", "JP", "225", "Nikkei", "NAS", "US100", "US500", "SPX"]
    
    logger.info("Search Results for missing tickers:")
    for kw in keywords:
        matches = [s['name'] for s in symbols if kw.lower() in s['name'].lower()]
        if matches:
            logger.info(f"Keyword '{kw}': {matches[:10]}...")

if __name__ == "__main__":
    dump_symbols()
