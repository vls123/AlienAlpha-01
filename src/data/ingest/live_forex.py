"""
Live Forex data ingestion via CTrader API.
"""
import logging
import asyncio
from datetime import datetime, timezone
import json
import random
try:
    import redis
except ImportError:
    redis = None

logger = logging.getLogger(__name__)

import json
import random
from datetime import datetime
import redis

class CTraderConnector:
    """
    Connects to CTrader Open API for live forex ticks.
    Currently uses SYNTHETIC data for pipeline verification due to missing Protobuf definitions.
    """
    def __init__(self, client_id: str, client_secret: str, redis_host: str = "localhost", redis_port: int = 6379):
        self.client_id = client_id
        self.client_secret = client_secret
        self.connected = False
        self.running = False
        try:
            self._redis = redis.Redis(host=redis_host, port=redis_port, decode_responses=True)
            self._redis.ping()
            logger.info(f"Connected to Redis at {redis_host}:{redis_port}")
        except Exception as e:
            logger.error(f"Failed to connect to Redis: {e}")
            self._redis = None

    async def connect(self):
        """Establishes connection to CTrader (Simulated)."""
        logger.info("Connecting to CTrader...")
        # TODO: Implement actual CTrader Protocol logic (Pros/TCP) using client_id/secret
        await asyncio.sleep(1) # Simulate connection time
        self.connected = True
        logger.info("Connected to CTrader (Simulated)")

    async def subscribe_symbol(self, symbol: str):
        """Subscribes to live ticks for a symbol."""
        if not self.connected:
            logger.warning("Not connected to CTrader")
            return
        logger.info(f"Subscribed to {symbol} (Simulated)")

    async def start_ingestion(self, symbols: list):
        """Main loop for generating and publishing ticks."""
        if not self.connected:
            await self.connect()
        
        self.running = True
        logger.info(f"Starting ingestion for {symbols}...")
        
        while self.running:
            for symbol in symbols:
                # Generate synthetic tick
                tick = {
                    "type": "tick",
                    "symbol": symbol,
                    "price": round(random.uniform(1.05, 1.15), 5),
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "volume": random.randint(1000, 5000)
                }
                
                # Publish to Redis Stream
                if self._redis:
                    try:
                        stream_key = f"tick:{symbol}"
                        # Streams expect dict of strings/bytes. JSON dump not strictly needed for fields if flat, 
                        # but keeping structure is good. Actually XADD takes a dict map.
                        # Let's write fields directly.
                        entry = {
                            "symbol": tick["symbol"],
                            "price": str(tick["price"]),
                            "volume": str(tick["volume"]),
                            "timestamp": tick["timestamp"]
                        }
                        self._redis.xadd(stream_key, entry)
                        
                        # also publish to pub/sub for legacy/livestream (optional, but good for "Hybrid")
                        # self._redis.publish("live_ticks", json.dumps(tick))
                        
                    except Exception as e:
                        logger.error(f"Redis stream add failed: {e}")
            
            await asyncio.sleep(1) # 1 tick per second per symbol

    def stop(self):
        self.running = False
        self.connected = False
