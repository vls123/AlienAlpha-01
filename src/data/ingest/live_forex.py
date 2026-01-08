"""
Live Forex data ingestion via CTrader API (Real).
"""
import logging
import asyncio
import os
from datetime import datetime, timezone
import redis
from .ctrader import CTraderClient

logger = logging.getLogger(__name__)

class CTraderConnector:
    """
    Connects to CTrader Open API for live forex ticks using CTraderClient.
    """
    def __init__(self, client_id: str, client_secret: str, redis_host: str = "localhost", redis_port: int = 6379):
        self.client_id = client_id
        self.client_secret = client_secret
        
        # Load tokens from env if not provided or valid
        self.access_token = os.getenv("CTRADER_ACCESS_TOKEN")
        self.account_id = os.getenv("CTRADER_ACCOUNT_ID")
        
        self.client = CTraderClient(client_id, client_secret, self.access_token, self.account_id)
        
        try:
            self._redis = redis.Redis(host=redis_host, port=redis_port, decode_responses=True)
            self._redis.ping()
            logger.info(f"Connected to Redis at {redis_host}:{redis_port}")
        except Exception as e:
            logger.error(f"Failed to connect to Redis: {e}")
            self._redis = None

    async def connect(self):
        """Establishes connection to CTrader."""
        # The client runs its own thread, we just ensure it's connected
        # But Client.connect() is blocking-ish or Future-based
        # We can call it in executor
        await asyncio.to_thread(self.client.connect)

    def _on_spot(self, symbol, bid, ask, ts):
        """Callback from CTrader Thread."""
        # Push to Redis
        if self._redis:
            try:
                stream_key = f"tick:{symbol}"
                entry = {
                    "symbol": symbol,
                    "price": str(bid),
                    "timestamp": ts.isoformat()
                }
                self._redis.xadd(stream_key, entry)
                # logger.debug(f"Pushed {symbol} {bid}")
            except Exception as e:
                logger.error(f"Redis write failed: {e}")

    async def start_ingestion(self, symbols: list = None):
        """Main loop."""
        # Set callback
        self.client.set_spot_callback(self._on_spot)
        
        await self.connect()
        
        # Default to Majors if not specified
        if not symbols:
            symbols = ["EURUSD", "GBPUSD", "USDJPY", "USDCHF", "AUDUSD", "USDCAD", "NZDUSD", "USDZAR"]

        logger.info(f"Subscribing to {symbols}...")
        self.client.subscribe(symbols)
        
        # Keep alive loop with Heartbeat
        while True:
            if self._redis:
                try:
                    # Set heartbeat with 30s expiry
                    self._redis.set("service:ingestor:heartbeat", datetime.now(timezone.utc).isoformat(), ex=30)
                except Exception as e:
                    logger.error(f"Heartbeat failed: {e}")
            await asyncio.sleep(5)

    def stop(self):
        if self.client:
            self.client.disconnect()

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    from dotenv import load_dotenv
    load_dotenv()
    
    # Load Credentials
    cid = os.getenv("CTRADER_CLIENT_ID")
    csec = os.getenv("CTRADER_CLIENT_SECRET")
    
    # Load Redis Config
    redis_host = os.getenv("REDIS_HOST", "localhost")
    redis_port = int(os.getenv("REDIS_PORT", 6379))
    
    if not cid or not csec:
        logger.error("Missing CTRADER_CLIENT_ID or CTRADER_CLIENT_SECRET")
        exit(1)
        
    connector = CTraderConnector(cid, csec, redis_host=redis_host, redis_port=redis_port)
    
    # Run
    try:
        asyncio.run(connector.start_ingestion())
    except KeyboardInterrupt:
        connector.stop()
