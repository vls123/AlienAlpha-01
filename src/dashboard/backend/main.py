from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import psutil
import docker
import redis
import os
import logging
from datetime import datetime

from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse

# Configure Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("dashboard-api")

app = FastAPI(title="AlienAlpha System Monitor")

# CORS (Allow Frontend during dev, strict in prod)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], 
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Clients
try:
    docker_client = docker.from_env()
except Exception as e:
    logger.error(f"Docker client failed: {e}")
    docker_client = None

redis_host = os.getenv("REDIS_HOST", "localhost")
redis_port = int(os.getenv("REDIS_PORT", 6379))
redis_client = redis.Redis(host=redis_host, port=redis_port, decode_responses=True)

@app.get("/")
def health_check():
    return {"status": "ok", "timestamp": datetime.now().isoformat()}

@app.get("/status/system")
def get_system_status():
    """Returns CPU, RAM, and Disk usage."""
    cpu = psutil.cpu_percent(interval=None)
    ram = psutil.virtual_memory().percent
    disk = psutil.disk_usage('/').percent
    
    # Docker Containers
    containers = []
    if docker_client:
        try:
            for c in docker_client.containers.list(all=True):
                containers.append({
                    "name": c.name,
                    "status": c.status,
                    "id": c.short_id
                })
        except Exception as e:
            logger.error(f"Docker list failed: {e}")
            
    return {
        "cpu": cpu,
        "ram": ram,
        "disk": disk,
        "containers": containers
    }

@app.get("/status/services")
def get_service_status():
    """Checks Redis and Backfill status."""
    # Redis Ping
    redis_status = "down"
    try:
        if redis_client.ping():
            redis_status = "up"
    except Exception:
        pass
        
    # Ingestor Heartbeat
    ingestor_status = "down"
    try:
        if redis_client.exists("service:ingestor:heartbeat"):
            ingestor_status = "running"
    except Exception:
        pass
    
    return {
        "redis": redis_status,
        "ingestor": ingestor_status
    }

@app.get("/status/data")
def get_data_status():
    """Returns ingestion metrics."""
    # 1. Redis Streams (Live)
    majors = ["EURUSD", "GBPUSD", "USDJPY", "USDCHF", "AUDUSD", "USDCAD", "NZDUSD", "USDZAR"]
    streams = {}
    try:
        for sym in majors:
            key = f"tick:{sym}"
            length = redis_client.xlen(key)
            streams[sym] = length
    except Exception as e:
        logger.error(f"Redis stream check failed: {e}")

    # 2. ArcticDB (Historical)
    arctic_stats = {}
    try:
        import arcticdb
        import pandas as pd
        # Connection inside container
        ac = arcticdb.Arctic("lmdb:///data/arctic_data")
        lib = ac["forex_1m"]
        symbols = lib.list_symbols()
        
        arctic_stats["symbol_count"] = len(symbols)
        arctic_stats["sample_dates"] = {}
        
        # Sample top 3 majors
        for sym in majors:
            if sym in symbols:
                # Read last row to get latest date
                # ArcticDB head/tail is efficient
                df = lib.read(sym).data
                if not df.empty:
                    last_ts = df.index[-1]
                    arctic_stats["sample_dates"][sym] = str(last_ts)
                else:
                    arctic_stats["sample_dates"][sym] = "Empty"
    except Exception as e:
        logger.error(f"ArcticDB check failed: {e}")
        arctic_stats["error"] = str(e)

    return {
        "streams": streams,
        "arctic": arctic_stats
    }

# Serve Static Files (React App)
# We assume the build is copied to /app/static
if os.path.exists("/app/static"):
    app.mount("/", StaticFiles(directory="/app/static", html=True), name="static")

@app.exception_handler(404)
async def custom_404_handler(request, exc):
    # Fallback to index.html for SPA routing if needed
    if os.path.exists("/app/static/index.html"):
        return FileResponse("/app/static/index.html")
    return {"error": "Not Found"}
