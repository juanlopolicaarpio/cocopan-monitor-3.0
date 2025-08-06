FROM python:3.11-slim
WORKDIR /app
RUN apt-get update && apt-get install -y curl wget gnupg software-properties-common && apt-get clean
RUN apt-get update && apt-get install -y libnss3 libnspr4 libatk-bridge2.0-0 libdrm2 libxkbcommon0 libxcomposite1 libxdamage1 libxrandr2 libgbm1 libgtk-3-0 libasound2 && apt-get clean
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
RUN playwright install chromium --with-deps
COPY . .
RUN touch cocopan_monitor.log
EXPOSE 8501
CMD ["python", "monitor_service.py"]