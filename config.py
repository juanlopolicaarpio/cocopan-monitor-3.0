#!/usr/bin/env python3
"""
CocoPan Monitor - Configuration Management
Handles all configuration with environment variables and defaults
"""
import os
from datetime import datetime
import pytz
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

class Config:
    """Configuration management with environment variables"""
    
    # Database Configuration
    DATABASE_URL = os.getenv(
        'DATABASE_URL', 
        'postgresql://cocopan:cocopan123@localhost:5432/cocopan_monitor'
    )
    
    # For local development with SQLite fallback
    USE_SQLITE = os.getenv('USE_SQLITE', 'false').lower() == 'true'
    SQLITE_PATH = os.getenv('SQLITE_PATH', 'store_status.db')
    
    # Monitoring Configuration
    MONITOR_START_HOUR = int(os.getenv('MONITOR_START_HOUR', '6'))  # 6 AM Manila
    MONITOR_END_HOUR = int(os.getenv('MONITOR_END_HOUR', '21'))    # 9 PM Manila
    CHECK_INTERVAL_MINUTES = int(os.getenv('CHECK_INTERVAL_MINUTES', '60'))  # Every hour
    TIMEZONE = os.getenv('TIMEZONE', 'Asia/Manila')
    
    # Store Configuration
    STORE_URLS_FILE = os.getenv('STORE_URLS_FILE', 'branch_urls.json')
    REQUEST_TIMEOUT = int(os.getenv('REQUEST_TIMEOUT', '10'))
    PLAYWRIGHT_TIMEOUT = int(os.getenv('PLAYWRIGHT_TIMEOUT', '60000'))
    
    # Error Handling Configuration
    MAX_RETRIES = int(os.getenv('MAX_RETRIES', '3'))
    RETRY_DELAY = int(os.getenv('RETRY_DELAY', '5'))  # seconds
    
    # Dashboard Configuration
    DASHBOARD_AUTO_REFRESH = int(os.getenv('DASHBOARD_AUTO_REFRESH', '300'))  # 5 minutes
    DASHBOARD_PORT = int(os.getenv('DASHBOARD_PORT', '8501'))
    
    # Logging Configuration
    LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
    LOG_FILE = os.getenv('LOG_FILE', 'cocopan_monitor.log')
    
    # User-Agent for requests
    USER_AGENT = os.getenv('USER_AGENT', 
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
        'AppleWebKit/537.36 (KHTML, like Gecko) '
        'Chrome/115.0.0.0 Safari/537.36'
    )
    
    @classmethod
    def get_database_url(cls):
        """Get the appropriate database URL"""
        if cls.USE_SQLITE:
            return f"sqlite:///{cls.SQLITE_PATH}"
        return cls.DATABASE_URL
    
    @classmethod
    def is_monitor_time(cls, current_hour):
        """Check if current hour is within monitoring window"""
        return cls.MONITOR_START_HOUR <= current_hour <= cls.MONITOR_END_HOUR
    
    @classmethod
    def get_timezone(cls):
        """Get timezone object"""
        return pytz.timezone(cls.TIMEZONE)
    
    @classmethod
    def get_current_time(cls):
        """Get current time in configured timezone"""
        return datetime.now(cls.get_timezone())
    
    @classmethod
    def validate_timezone(cls):
        """Validate timezone is correctly configured"""
        try:
            tz = cls.get_timezone()
            now = cls.get_current_time()
            
            print(f"✅ Timezone validation:")
            print(f"   Config: {cls.TIMEZONE}")
            print(f"   Current time: {now.strftime('%Y-%m-%d %H:%M:%S %Z')}")
            print(f"   UTC offset: {now.strftime('%z')}")
            
            # Should show PHT or +0800, NOT PST
            tz_name = now.strftime('%Z')
            if 'PST' in tz_name or 'PDT' in tz_name:
                print("❌ ERROR: Showing Pacific Time instead of Philippine Time!")
                return False
            
            if cls.TIMEZONE == 'Asia/Manila' and '+08' not in now.strftime('%z'):
                print("❌ ERROR: UTC offset should be +0800 for Manila!")
                return False
                
            return True
        except Exception as e:
            print(f"❌ Timezone validation error: {e}")
            return False
    
    @classmethod
    def validate_config(cls):
        """Validate configuration settings"""
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
        
        # Validate timezone
        if not cls.validate_timezone():
            errors.append("Timezone validation failed")
            
        return errors

# Global config instance
config = Config()

def print_config():
    """Print current configuration for debugging"""
    print("🔧 CocoPan Monitor Configuration:")
    print(f"   📊 Database: {'SQLite' if config.USE_SQLITE else 'PostgreSQL'}")
    print(f"   ⏰ Monitor Hours: {config.MONITOR_START_HOUR}:00 - {config.MONITOR_END_HOUR}:00 {config.TIMEZONE}")
    print(f"   🔄 Check Interval: {config.CHECK_INTERVAL_MINUTES} minutes")
    print(f"   🏪 Store URLs: {config.STORE_URLS_FILE}")
    print(f"   🔁 Max Retries: {config.MAX_RETRIES}")
    print(f"   📊 Dashboard Port: {config.DASHBOARD_PORT}")
    
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