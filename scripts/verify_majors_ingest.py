
import sys
import os
import asyncio
import logging
import redis

# Adjust path to include src
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from src.data.ingest.live_forex import CTraderConnector

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def verify_streams():
    try:
        r = redis.Redis(host='localhost', port=6379, decode_responses=True)
        majors = ["EURUSD", "GBPUSD", "USDJPY", "USDCHF", "AUDUSD", "USDCAD", "NZDUSD"]
        
        logger.info("Listening for Majors...")
        received_symbols = set()
        
        last_ids = {f"tick:{sym}": "$" for sym in majors} # Start listening from NOW
        stream_keys_only = {k: last_ids[k] for k in last_ids} # Actually just pass the dict
        
        # Run for 20 seconds to allow for connection setup
        end_time = asyncio.get_event_loop().time() + 20
        while asyncio.get_event_loop().time() < end_time:
            # Check ALL majors streams at once
            res = r.xread(last_ids, count=5, block=100)
            if res:
                for stream_name, entries in res:
                    # Update Last ID for this stream
                     last_ids[stream_name] = entries[-1][0]
                     sym = stream_name.replace("tick:", "")
                     if sym in majors:
                         received_symbols.add(sym)
                         logger.info(f"Received {sym}")
            await asyncio.sleep(0.01)
            
        logger.info(f"Total Unique Majors Received: {len(received_symbols)}")
        if len(received_symbols) > 0:
            print("SUCCESS: Received Majors.")
        else:
            print("FAILURE: No Majors received.")
            
    except Exception as e:
        logger.error(f"Listener failed: {e}")

async def run_test():
    from dotenv import load_dotenv
    load_dotenv()
    
    cid = os.getenv("CTRADER_CLIENT_ID")
    csec = os.getenv("CTRADER_CLIENT_SECRET")
    
    if not cid or not csec:
        logger.error("Missing Credentials in env")
        return

    connector = CTraderConnector(cid, csec)
    # Start WITHOUT arguments to test default behavior
    task = asyncio.create_task(connector.start_ingestion())
    
    await verify_streams()
    
    connector.stop()
    await task

if __name__ == "__main__":
    asyncio.run(run_test())
