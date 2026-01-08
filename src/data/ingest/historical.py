"""
Historical data ingestion module.
Support for Yahoo Finance, Binance (via CCXT), and FRED.
"""
import logging
import pandas as pd
from typing import Optional
from src.utils.time import ensure_utc_index, now_utc

# Conditional imports
try:
    import yfinance as yf
except ImportError:
    yf = None

try:
    import ccxt
except ImportError:
    ccxt = None

try:
    from fredapi import Fred
except ImportError:
    Fred = None

logger = logging.getLogger(__name__)

class HistoricalIngestor:
    def __init__(self):
        self.binance = ccxt.binance() if ccxt else None
        # Initialize FRED client
        import os
        api_key = os.getenv("FRED_API_KEY")
        if Fred and api_key:
            self.fred = Fred(api_key=api_key)
        else:
            self.fred = None
            if not api_key:
                logger.warning("FRED_API_KEY not found in environment variables.")

    def fetch_yahoo(self, symbol: str, start_date: str, end_date: Optional[str] = None) -> pd.DataFrame:
        """Fetches historical data from Yahoo Finance."""
        if not yf:
            logger.error("yfinance library not installed")
            return pd.DataFrame()
            
        logger.info(f"Fetching {symbol} from Yahoo Finance...")
        df = yf.download(symbol, start=start_date, end=end_date)
        if df.empty:
            logger.warning(f"No data found for {symbol}")
            return df
            
        df = ensure_utc_index(df)
        return df

    def fetch_fred(self, series_id: str, start_date: Optional[str] = None, end_date: Optional[str] = None) -> pd.DataFrame:
        """Fetches historical economic data from FRED."""
        if not self.fred:
            logger.error("FRED client not initialized (check API key or dependency)")
            return pd.DataFrame()

        logger.info(f"Fetching {series_id} from FRED...")
        try:
            # get_series returns a Series with datetime index
            series = self.fred.get_series(series_id, observation_start=start_date, observation_end=end_date)
            if series.empty:
                logger.warning(f"No data found for {series_id}")
                return pd.DataFrame()
            
            df = series.to_frame(name='value')
            df.index.name = 'timestamp'
            return ensure_utc_index(df)
        except Exception as e:
            logger.error(f"Failed to fetch {series_id} from FRED: {e}")
            return pd.DataFrame()

    def fetch_crypto_snapshot(self, symbol: str, timeframe: str = '1d', limit: int = 100) -> pd.DataFrame:
        """Fetches historical crypto data from Binance."""
        if not self.binance:
            logger.error("ccxt library not installed")
            return pd.DataFrame()
            
        logger.info(f"Fetching {symbol} from Binance...")
        ohlcv = self.binance.fetch_ohlcv(symbol, timeframe, limit=limit)
        df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
        df.set_index('timestamp', inplace=True)
        return ensure_utc_index(df)

    def fetch_ctrader(self, symbol: str, start_date: str, end_date: str) -> pd.DataFrame:
        """
        Fetches historical data from CTrader Open API.
        """
        from src.data.ingest.ctrader import CTraderClient

        import os
        client_id = os.getenv("CTRADER_APP_CLIENT_ID") or os.getenv("CTRADER_CLIENT_ID")
        client_secret = os.getenv("CTRADER_APP_CLIENT_SECRET") or os.getenv("CTRADER_CLIENT_SECRET")
        access_token = os.getenv("CTRADER_ACCESS_TOKEN")
        account_id = os.getenv("CTRADER_ACCOUNT_ID")
        
        # Instantiate client (Mock/Synthetic for now)
        client = CTraderClient(client_id, client_secret, access_token, account_id)
        
        # Parse dates
        start = pd.Timestamp(start_date).to_pydatetime()
        end = pd.Timestamp(end_date).to_pydatetime()
        
        df = client.fetch_history(symbol, start, end)
        
        # Ensure UTC timezone
        df = ensure_utc_index(df)
        return df
