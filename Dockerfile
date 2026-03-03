# BlockID production Docker image
# Python 3.13, FastAPI, pipeline, monitoring

FROM python:3.13-slim

WORKDIR /app

# Install system deps for reportlab/psycopg + healthcheck
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libpq-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY backend_blockid ./backend_blockid

ENV PYTHONPATH=/app

# Production env template (compose injects production.env at runtime)
COPY production.env.example ./production.env.example
ENV DB_PATH=/app/data/blockid.db

EXPOSE 8000

# Init DB on startup, then run API
CMD ["sh", "-c", "mkdir -p /app/data /app/logs && python -m backend_blockid.database.init_tables 2>/dev/null || true && exec uvicorn backend_blockid.api_server.server:app --host 0.0.0.0 --port 8000"]
