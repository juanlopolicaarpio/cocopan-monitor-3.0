#!/usr/bin/env python3
import os

# Create Dockerfile
dockerfile_content = '''FROM python:3.11-slim
WORKDIR /app
RUN apt-get update && apt-get install -y curl wget gnupg software-properties-common && apt-get clean
RUN apt-get update && apt-get install -y libnss3 libnspr4 libatk-bridge2.0-0 libdrm2 libxkbcommon0 libxcomposite1 libxdamage1 libxrandr2 libgbm1 libgtk-3-0 libasound2 && apt-get clean
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
RUN playwright install chromium --with-deps
COPY . .
RUN touch cocopan_monitor.log
EXPOSE 8501
CMD ["python", "monitor_service.py"]'''

with open('Dockerfile', 'w') as f:
    f.write(dockerfile_content)

# Create docker-compose.yml
compose_content = '''version: '3.8'
services:
  postgres:
    image: postgres:15-alpine
    container_name: cocopan_postgres
    environment:
      POSTGRES_DB: cocopan_monitor
      POSTGRES_USER: cocopan
      POSTGRES_PASSWORD: cocopan123
    ports:
      - "5432:5432"
    volumes:
      - postgres_data:/var/lib/postgresql/data
    networks:
      - cocopan_network
    restart: unless-stopped

  monitor:
    build: .
    container_name: cocopan_monitor
    command: python monitor_service.py
    environment:
      - DATABASE_URL=postgresql://cocopan:cocopan123@postgres:5432/cocopan_monitor
      - TIMEZONE=Asia/Manila
      - LOG_LEVEL=INFO
    volumes:
      - ./branch_urls.json:/app/branch_urls.json:ro
    networks:
      - cocopan_network
    depends_on:
      - postgres
    restart: unless-stopped

  dashboard:
    build: .
    container_name: cocopan_dashboard
    command: streamlit run enhanced_dashboard.py --server.port=8501 --server.address=0.0.0.0 --server.headless=true
    environment:
      - DATABASE_URL=postgresql://cocopan:cocopan123@postgres:5432/cocopan_monitor
      - TIMEZONE=Asia/Manila
    ports:
      - "8501:8501"
    networks:
      - cocopan_network
    depends_on:
      - postgres
    restart: unless-stopped

volumes:
  postgres_data:

networks:
  cocopan_network:'''

with open('docker-compose.yml', 'w') as f:
    f.write(compose_content)

# Update requirements.txt
requirements_content = '''requests>=2.31.0
beautifulsoup4>=4.12.0
python-dotenv>=1.0.0
playwright>=1.40.0
streamlit>=1.28.0
plotly>=5.17.0
pandas>=2.1.0
pytz>=2023.3
psycopg2-binary>=2.9.0
APScheduler>=3.10.0
psutil>=5.9.0'''

with open('requirements.txt', 'w') as f:
    f.write(requirements_content)

print("✅ Created Dockerfile")
print("✅ Created docker-compose.yml") 
print("✅ Updated requirements.txt")
print("\n🚀 Now run: docker compose up -d")
