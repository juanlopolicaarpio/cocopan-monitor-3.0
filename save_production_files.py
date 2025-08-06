#!/usr/bin/env python3
"""
Save all production files to filesystem
Run this script to create all necessary files for the CocoPan monitoring system
"""
import os
from pathlib import Path

def create_dockerfile():
    """Create Dockerfile"""
    content = '''FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \\
    curl \\
    wget \\
    gnupg \\
    software-properties-common \\
    && apt-get clean \\
    && rm -rf /var/lib/apt/lists/*

# Install Playwright dependencies
RUN apt-get update && apt-get install -y \\
    libnss3 \\
    libnspr4 \\
    libatk-bridge2.0-0 \\
    libdrm2 \\
    libxkbcommon0 \\
    libxcomposite1 \\
    libxdamage1 \\
    libxrandr2 \\
    libgbm1 \\
    libgtk-3-0 \\
    libasound2 \\
    && apt-get clean \\
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for better caching
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Install Playwright browsers
RUN playwright install chromium --with-deps

# Copy application code
COPY . .

# Create log file
RUN touch cocopan_monitor.log

# Expose ports
EXPOSE 8501

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \\
  CMD python -c "import requests; requests.get('http://localhost:8501/_stcore/health', timeout=5)" || exit 1

# Default command (can be overridden in docker-compose)
CMD ["python", "monitor_service.py"]'''
    
    with open('Dockerfile', 'w') as f:
        f.write(content)
    print("✅ Created Dockerfile")

def create_docker_compose():
    """Create docker-compose.yml"""
    content = '''version: '3.8'

services:
  # PostgreSQL Database
  postgres:
    image: postgres:15-alpine
    container_name: cocopan_postgres
    environment:
      POSTGRES_DB: cocopan_monitor
      POSTGRES_USER: cocopan
      POSTGRES_PASSWORD: cocopan123
      POSTGRES_INITDB_ARGS: "--encoding=UTF-8"
    ports:
      - "5432:5432"
    volumes:
      - postgres_data:/var/lib/postgresql/data
      - ./init.sql:/docker-entrypoint-initdb.d/init.sql:ro
    networks:
      - cocopan_network
    restart: unless-stopped
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U cocopan -d cocopan_monitor"]
      interval: 10s
      timeout: 5s
      retries: 5

  # CocoPan Monitor Service (Background scheduler)
  monitor:
    build: .
    container_name: cocopan_monitor
    command: python monitor_service.py
    environment:
      - DATABASE_URL=postgresql://cocopan:cocopan123@postgres:5432/cocopan_monitor
      - TIMEZONE=Asia/Manila
      - LOG_LEVEL=INFO
      - MONITOR_START_HOUR=6
      - MONITOR_END_HOUR=21
      - CHECK_INTERVAL_MINUTES=60
    volumes:
      - ./branch_urls.json:/app/branch_urls.json:ro
      - ./cocopan_monitor.log:/app/cocopan_monitor.log
    networks:
      - cocopan_network
    depends_on:
      postgres:
        condition: service_healthy
    restart: unless-stopped

  # Streamlit Dashboard
  dashboard:
    build: .
    container_name: cocopan_dashboard
    command: streamlit run enhanced_dashboard.py --server.port=8501 --server.address=0.0.0.0 --server.headless=true --server.enableCORS=false --server.enableXsrfProtection=false
    environment:
      - DATABASE_URL=postgresql://cocopan:cocopan123@postgres:5432/cocopan_monitor
      - TIMEZONE=Asia/Manila
      - DASHBOARD_AUTO_REFRESH=300
    ports:
      - "8501:8501"
    networks:
      - cocopan_network
    depends_on:
      postgres:
        condition: service_healthy
    restart: unless-stopped
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8501/_stcore/health"]
      interval: 30s
      timeout: 10s
      retries: 3

  # Optional: pgAdmin for database management
  pgadmin:
    image: dpage/pgadmin4:latest
    container_name: cocopan_pgadmin
    environment:
      PGADMIN_DEFAULT_EMAIL: admin@cocopan.com
      PGADMIN_DEFAULT_PASSWORD: admin123
    ports:
      - "5050:80"
    networks:
      - cocopan_network
    depends_on:
      - postgres
    restart: unless-stopped
    profiles:
      - admin  # Only run when using --profile admin

volumes:
  postgres_data:
    driver: local

networks:
  cocopan_network:
    driver: bridge'''
    
    with open('docker-compose.yml', 'w') as f:
        f.write(content)
    print("✅ Created docker-compose.yml")

def create_init_sql():
    """Create database initialization script"""
    content = '''-- CocoPan Monitor Database Initialization
-- This script sets up the PostgreSQL database with optimal settings

-- Create tables
CREATE TABLE IF NOT EXISTS stores (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    url TEXT NOT NULL UNIQUE,
    platform VARCHAR(50) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS status_checks (
    id SERIAL PRIMARY KEY,
    store_id INTEGER REFERENCES stores(id) ON DELETE CASCADE,
    is_online BOOLEAN NOT NULL,
    checked_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    response_time_ms INTEGER,
    error_message TEXT
);

CREATE TABLE IF NOT EXISTS summary_reports (
    id SERIAL PRIMARY KEY,
    total_stores INTEGER NOT NULL,
    online_stores INTEGER NOT NULL,
    offline_stores INTEGER NOT NULL,
    online_percentage REAL NOT NULL,
    report_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for optimal performance
CREATE INDEX IF NOT EXISTS idx_stores_platform ON stores(platform);
CREATE INDEX IF NOT EXISTS idx_stores_url ON stores(url);

CREATE INDEX IF NOT EXISTS idx_status_checks_store_id ON status_checks(store_id);
CREATE INDEX IF NOT EXISTS idx_status_checks_checked_at ON status_checks(checked_at);
CREATE INDEX IF NOT EXISTS idx_status_checks_is_online ON status_checks(is_online);

CREATE INDEX IF NOT EXISTS idx_summary_reports_time ON summary_reports(report_time);

-- Grant permissions to the cocopan user
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO cocopan;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO cocopan;

COMMIT;'''
    
    with open('init.sql', 'w') as f:
        f.write(content)
    print("✅ Created init.sql")

def update_requirements():
    """Update requirements.txt with all dependencies"""
    content = '''# Core Dependencies
requests>=2.31.0
beautifulsoup4>=4.12.0
python-dotenv>=1.0.0

# Web Scraping
playwright>=1.40.0

# Dashboard
streamlit>=1.28.0
plotly>=5.17.0
pandas>=2.1.0
pytz>=2023.3

# Database
psycopg2-binary>=2.9.0

# Scheduling & Background Tasks
APScheduler>=3.10.0

# Logging & Monitoring
psutil>=5.9.0'''
    
    with open('requirements.txt', 'w') as f:
        f.write(content)
    print("✅ Updated requirements.txt")

def create_env_file():
    """Create .env file if it doesn't exist"""
    if not os.path.exists('.env'):
        content = '''# CocoPan Monitor Configuration
DATABASE_URL=postgresql://cocopan:cocopan123@localhost:5432/cocopan_monitor
TIMEZONE=Asia/Manila
MONITOR_START_HOUR=6
MONITOR_END_HOUR=21
CHECK_INTERVAL_MINUTES=60
LOG_LEVEL=INFO
DASHBOARD_AUTO_REFRESH=300
MAX_RETRIES=3
REQUEST_TIMEOUT=10
STORE_URLS_FILE=branch_urls.json'''
        
        with open('.env', 'w') as f:
            f.write(content)
        print("✅ Created .env file")
    else:
        print("✅ .env file already exists")

def main():
    print("🚀 Setting up CocoPan production files...")
    print("=" * 50)
    
    # Check if branch_urls.json exists
    if not os.path.exists('branch_urls.json'):
        print("❌ branch_urls.json not found!")
        print("💡 Please make sure your store URLs file exists")
        return False
    
    # Create all required files
    create_dockerfile()
    create_docker_compose()
    create_init_sql()
    update_requirements()
    create_env_file()
    
    print("\n" + "=" * 50)
    print("✅ All production files created successfully!")
    print("\n🚀 Next steps:")
    print("   1. docker compose up -d")
    print("   2. Wait 30 seconds for initialization")
    print("   3. Open http://localhost:8501")
    print("\n💡 Or run: python start_cocopan.py (choose option 1)")
    
    return True

if __name__ == "__main__":
    main()