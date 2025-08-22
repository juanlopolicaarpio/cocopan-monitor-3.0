# Base: Microsoft Playwright (matching your Python package 1.54.0)
FROM mcr.microsoft.com/playwright/python:v1.54.0-jammy

# Root for package install
USER root

# Minimal system deps (psql client, TLS tools)
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      curl wget ca-certificates postgresql-client && \
    rm -rf /var/lib/apt/lists/*

# Runtime env
ENV PIP_NO_CACHE_DIR=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# Python deps
COPY requirements.txt .
# IMPORTANT: playwright in requirements must match the base image (1.54.0) OR be omitted.
RUN pip install --no-cache-dir -r requirements.txt

# Make sure the matching Chromium is present for the installed playwright version
RUN python -m playwright install --with-deps chromium

# App code
COPY . .

# Logs dir + non-root
RUN mkdir -p /app/logs && chmod 755 /app/logs && \
    useradd --create-home --shell /bin/bash app && \
    chown -R app:app /app

USER app
EXPOSE 8501
CMD ["python", "monitor_service.py"]
