#!/usr/bin/env python3
"""
CocoPan Monitor - Configuration Management
- Reads DATABASE_URL strictly from environment (no hard-coded creds)
- Appends sslmode=require automatically for hosted Postgres (Railway)
- Keeps existing timezone validation and other settings
"""

import os
from datetime import datetime
import pytz
from dotenv import load_dotenv

# Load environment variables from a local .env file *only for local/dev*.
# On Railway (prod), Railway injects env vars automatically; .env is ignored there.
load_dotenv()

class Config:
    """Configuration management with environment variables"""

    # ---- Database ----
    # TIP: For dev you can set USE_SQLITE=true to run locally without Postgres.
    USE_SQLITE = os.getenv('USE_SQLITE', 'false').lower() == 'true'
    SQLITE_PATH = os.getenv('SQLITE_PATH', 'store_status.db')

    # We do NOT hard-code DATABASE_URL here. Always read at call time via get_database_url().

    # ---- Monitoring ----
    MONITOR_START_HOUR = int(os.getenv('MONITOR_START_HOUR', '6'))   # 6 
    MONITOR_END_HOUR   = int(os.getenv('MONITOR_END_HOUR', '20'))    # 20 
    CHECK_INTERVAL_MINUTES = int(os.getenv('CHECK_INTERVAL_MINUTES', '60'))
    TIMEZONE = os.getenv('TIMEZONE', 'Asia/Manila')

    # ---- Store / scraping ----
    STORE_URLS_FILE   = os.getenv('STORE_URLS_FILE', 'branch_urls.json')
    REQUEST_TIMEOUT   = int(os.getenv('REQUEST_TIMEOUT', '10'))
    PLAYWRIGHT_TIMEOUT = int(os.getenv('PLAYWRIGHT_TIMEOUT', '60000'))

    # ---- Error handling ----
    MAX_RETRIES = int(os.getenv('MAX_RETRIES', '3'))
    RETRY_DELAY = int(os.getenv('RETRY_DELAY', '5'))  # seconds

    # ---- Dashboard ----
    DASHBOARD_AUTO_REFRESH = int(os.getenv('DASHBOARD_AUTO_REFRESH', '300'))  # seconds
    DASHBOARD_PORT = int(os.getenv('DASHBOARD_PORT', '8501'))

    # ---- Logging ----
    LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
    LOG_FILE  = os.getenv('LOG_FILE', 'cocopan_monitor.log')

    # ---- User-Agent ----
    USER_AGENT = os.getenv(
        'USER_AGENT',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
        'AppleWebKit/537.36 (KHTML, like Gecko) '
        'Chrome/115.0.0.0 Safari/537.36'
    )

    # ---- Email/SMTP (admin alerts) ----
    ALERTS_ENABLED = os.getenv('ALERTS_ENABLED', 'false').lower() == 'true'
    SMTP_SERVER    = os.getenv('SMTP_SERVER', 'smtp.gmail.com')
    SMTP_PORT      = int(os.getenv('SMTP_PORT', '587'))
    SMTP_USE_TLS   = os.getenv('SMTP_USE_TLS', 'true').lower() == 'true'
    SMTP_USERNAME  = os.getenv('SMTP_USERNAME', '')
    SMTP_PASSWORD  = os.getenv('SMTP_PASSWORD', '')
    FROM_EMAIL     = os.getenv('FROM_EMAIL', '')

    # ---------- Helpers ----------

    @classmethod
    def get_database_url(cls) -> str:
        """
        Canonical DB URL (Railway-ready).
        - If USE_SQLITE=true, use local SQLite.
        - Else, read DATABASE_URL from env (Railway injects this in prod).
        - Append sslmode=require for hosted Postgres when missing.
        """
        if cls.USE_SQLITE:
            return f"sqlite:///{cls.SQLITE_PATH}"

        url = os.getenv('DATABASE_URL', '').strip()
        if not url:
            raise RuntimeError(
                "DATABASE_URL is not set (and USE_SQLITE is false). "
                "On Railway, add it in Settings → Variables. "
                "For local dev, put it in a .env file."
            )

        # If it's Postgres and not the local compose hostname (@postgres),
        # enforce SSL for hosted DBs like Railway.
        if url.startswith(('postgres://', 'postgresql://')) \
           and 'sslmode=' not in url \
           and '@postgres:' not in url and '@postgres/' not in url:
            url += ('&' if '?' in url else '?') + 'sslmode=require'

        return url

    @classmethod
    def is_monitor_time(cls, current_hour: int) -> bool:
        return cls.MONITOR_START_HOUR <= current_hour <= cls.MONITOR_END_HOUR

    @classmethod
    def get_timezone(cls):
        return pytz.timezone(cls.TIMEZONE)

    @classmethod
    def get_current_time(cls):
        return datetime.now(cls.get_timezone())

    @classmethod
    def validate_timezone(cls) -> bool:
        """
        Relaxed timezone validation: only check UTC offset for Asia/Manila.
        """
        try:
            tz = cls.get_timezone()
            now = cls.get_current_time()

            print(f"✅ Timezone validation:")
            print(f"   Config: {cls.TIMEZONE}")
            print(f"   Current time: {now.strftime('%Y-%m-%d %H:%M:%S %Z')}")
            print(f"   UTC offset: {now.strftime('%z')}")

            if cls.TIMEZONE == 'Asia/Manila':
                expected_offset = '+0800'
                actual_offset = now.strftime('%z')
                if actual_offset != expected_offset:
                    print(f"❌ ERROR: UTC offset should be {expected_offset} for Manila, got {actual_offset}!")
                    return False
                else:
                    print("✅ UTC offset correct for Manila time")
                    tz_name = now.strftime('%Z')
                    if 'PST' in tz_name or 'PDT' in tz_name:
                        print("ℹ️  Note: PST/PDT name is fine in containers as long as offset is +08:00")
                    return True
            return True
        except Exception as e:
            print(f"❌ Timezone validation error: {e}")
            return False

    @classmethod
    def validate_config(cls):
        """Validate key settings and files."""
        errors = []

        if cls.MONITOR_START_HOUR < 0 or cls.MONITOR_START_HOUR > 23:
            errors.append("MONITOR_START_HOUR must be between 0-23")

        if cls.MONITOR_END_HOUR < 0 or cls.MONITOR_END_HOUR > 23:
            errors.append("MONITOR_END_HOUR must be between 0-23")

        if cls.MONITOR_START_HOUR >= cls.MONITOR_END_HOUR:
            errors.append("MONITOR_START_HOUR must be less than MONITOR_END_HOUR")

        if cls.CHECK_INTERVAL_MINUTES < 1:
            errors.append("CHECK_INTERVAL_MINUTES must be at least 1")

        if not os.path.exists(cls.STORE_URLS_FILE):
            errors.append(f"Store URLs file not found: {cls.STORE_URLS_FILE}")

        if not cls.validate_timezone():
            errors.append("Timezone validation failed")

        return errors

# Global instance (backwards-compat for your other modules)
config = Config()

def print_config() -> bool:
    """Print current configuration for debugging; returns True if valid."""
    print("🔧 CocoPan Monitor Configuration:")
    print(f"   📊 Database: {'SQLite' if config.USE_SQLITE else 'PostgreSQL'}")
    print(f"   ⏰ Monitor Hours: {config.MONITOR_START_HOUR}:00 - {config.MONITOR_END_HOUR}:00 {config.TIMEZONE}")
    print(f"   🔄 Check Interval: {config.CHECK_INTERVAL_MINUTES} minutes")
    print(f"   🏪 Store URLs: {config.STORE_URLS_FILE}")
    print(f"   🔁 Max Retries: {config.MAX_RETRIES}")
    print(f"   📊 Dashboard Port: {config.DASHBOARD_PORT}")
    print(f"   📧 Email Alerts: {'Enabled' if config.ALERTS_ENABLED else 'Disabled'}")
    if config.ALERTS_ENABLED:
        print(f"   📮 SMTP Server: {config.SMTP_SERVER}:{config.SMTP_PORT}")
        print(f"   👤 SMTP User: {config.SMTP_USERNAME}")

    # Validate configuration
    errors = config.validate_config()
    if errors:
        print(f"   ❌ Configuration Errors:")
        for error in errors:
            print(f"      • {error}")
        return False
    else:
        print(f"   ✅ Configuration Valid")
        return True
