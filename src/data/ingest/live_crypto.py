"""
Live Crypto data ingestion via CCXT Pro (or standard CCXT polling).
"""
import logging
import asyncio

try:
    import ccxt.pro as ccxtpro
except ImportError:
    ccxtpro = None

logger = logging.getLogger(__name__)

class CryptoStreamer:
    """
    Connects to Crypto exchanges for live ticks/bars.
    """
    def __init__(self, exchange_id: str = 'binance'):
        self.exchange_id = exchange_id
        self.exchange = None

    async def start(self):
        """Starts the websocket stream."""
        if not ccxtpro:
            logger.error("ccxt.pro not available")
            return

        try:
            exchange_class = getattr(ccxtpro, self.exchange_id)
            self.exchange = exchange_class()
            logger.info(f"Initialized {self.exchange_id} stream")
        except AttributeError:
            logger.error(f"Exchange {self.exchange_id} not found in ccxt.pro")

    async def watch_ticker(self, symbol: str):
        """Watches a specific ticker."""
        if not self.exchange:
            return

        while True:
            try:
                ticker = await self.exchange.watch_ticker(symbol)
                logger.debug(f"Tick {symbol}: {ticker['last']}")
                # TODO: Push to Redis
            except Exception as e:
                logger.error(f"Error in ticker stream: {e}")
                break
        
    async def close(self):
        if self.exchange:
            await self.exchange.close()
