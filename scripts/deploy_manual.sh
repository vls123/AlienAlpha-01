#!/bin/bash
set -e

# 1b. Cleanup
echo "Cleaning up old containers..."
docker rm -f alien_redis alien_alpha_redis alien_ingestor || true

# 1. Network
echo "Creating Network..."
docker network create alien_net || true

# 2. Redis Stack
echo "Deploying Redis Stack..."
docker run -d \
  --name alien_redis \
  --network alien_net \
  -p 6379:6379 \
  -p 8001:8001 \
  -v $(pwd)/data/redis:/data \
  --restart always \
  -e REDIS_ARGS="--appendonly yes" \
  redis/redis-stack:latest

# 3. Build Ingestor
echo "Building Ingestor..."
docker build -t alien_ingestor_img .

# 4. Run Ingestor
echo "Deploying Ingestor..."
docker run -d \
  --name alien_ingestor \
  --network alien_net \
  -v $(pwd)/src:/app/src \
  -v $(pwd)/scripts:/app/scripts \
  --env-file .env \
  -e REDIS_HOST=alien_redis \
  -e REDIS_PORT=6379 \
  -e CTRADER_ACCOUNT_ID=45839947 \
  -e CTRADER_HOST=demo.ctraderapi.com \
  --restart always \
  --memory 512m \
  alien_ingestor_img

echo "Deploying Dashboard..."

# 3. Dashboard
# Stop old if exists
if [ "$(docker ps -a -q -f name=alien_dashboard)" ]; then
    docker rm -f alien_dashboard
fi

echo "Building Dashboard..."
docker build -t alien_dashboard_img -f src/dashboard/Dockerfile .

echo "Running Dashboard..."
docker run -d \
  --name alien_dashboard \
  --network alien_net \
  -p 8000:8000 \
  -e REDIS_HOST=alien_redis \
  -e REDIS_PORT=6379 \
  -v /var/run/docker.sock:/var/run/docker.sock \
  --restart always \
  alien_dashboard_img

echo "Deployment Complete."
docker ps
