# Stage 1: Build Frontend
FROM node:20 as builder

WORKDIR /app/frontend

COPY src/dashboard/frontend/package*.json ./
RUN npm install

COPY src/dashboard/frontend/ ./
RUN npm run build 

# Stage 2: Backend
FROM python:3.12-slim

WORKDIR /app

# Install System deps (curl/ping/docker client deps if needed)
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install Python Deps
COPY src/dashboard/backend/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy Backend Code
COPY src/dashboard/backend/*.py ./

# Copy Frontend Build
COPY --from=builder /app/frontend/dist /app/static

ENV PYTHONPATH=/app
ENV PORT=8000

# Expose
EXPOSE 8000

# CMD
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
