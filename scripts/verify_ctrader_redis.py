"""
Script to verify CTrader (Simulated) ingestion to Redis.
"""
import sys
import os
import asyncio
import logging
import redis
import json

# Adjust path to include src
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from src.data.ingest.live_forex import CTraderConnector

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def verify_redis_stream():
    """Client to listen to Redis Stream."""
    try:
        r = redis.Redis(host='localhost', port=6379, decode_responses=True)
        stream_key = "tick:EURUSD"
        last_id = "$"
        
        logger.info(f"Listening to Stream '{stream_key}'...")
        
        count = 0
        while count < 3:
            # Block for 1000ms waiting for new items
            streams = r.xread({stream_key: last_id}, count=1, block=1000)
            if streams:
                for stream_name, entries in streams:
                    for entry_id, data in entries:
                        logger.info(f"Received from Stream {stream_name} ID {entry_id}: {data}")
                        last_id = entry_id
                        count += 1
            await asyncio.sleep(0.1)
    except Exception as e:
        logger.error(f"Redis stream listener failed: {e}")

async def run_ingestor():
    # Load env (though Connector takes args, we'll dummy them for verification)
    connector = CTraderConnector(
        client_id="dummy_id", 
        client_secret="dummy_secret"
    )
    
    # Start ingestion task
    ingest_task = asyncio.create_task(connector.start_ingestion(['EURUSD']))
    
    # Run listener for a few seconds
    await verify_redis_stream()
    
    # Stop ingestor
    connector.stop()
    await ingest_task
    logger.info("Verification complete.")

if __name__ == "__main__":
    try:
        asyncio.run(run_ingestor())
    except KeyboardInterrupt:
        pass
