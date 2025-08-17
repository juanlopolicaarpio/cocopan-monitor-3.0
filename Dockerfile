# Dockerfile
FROM python:3.11-slim

# ---- Base OS deps (no 'software-properties-common' on slim) ----
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl wget gnupg ca-certificates postgresql-client \
  && rm -rf /var/lib/apt/lists/*

# ---- Runtime env hygiene ----
ENV PIP_NO_CACHE_DIR=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# ---- Python deps ----
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# ---- Playwright (if you scrape with Chromium) ----
RUN pip install --no-cache-dir playwright && \
    python -m playwright install chromium && \
    python -m playwright install-deps

# ---- App code ----
COPY . .

# Optional: a place for logs (mounted in compose)
RUN mkdir -p /app/logs

# Streamlit listens here (dashboard service maps/overrides command)
EXPOSE 8501

# Default command (compose can override for dashboard)
CMD ["python", "monitor_service.py"]
