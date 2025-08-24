#!/usr/bin/env python3
"""
Debug configuration issues
"""
import os
from config import config

def debug_configuration():
    """Debug all configuration settings"""
    print("🔧 CocoPan Configuration Debugging")
    print("=" * 50)
    
    print("📊 Basic Settings:")
    print(f"   MONITOR_START_HOUR: {config.MONITOR_START_HOUR}")
    print(f"   MONITOR_END_HOUR: {config.MONITOR_END_HOUR}")
    print(f"   CHECK_INTERVAL_MINUTES: {config.CHECK_INTERVAL_MINUTES}")
    print(f"   TIMEZONE: {config.TIMEZONE}")
    
    print("\n📧 Email Settings:")
    print(f"   ALERTS_ENABLED: {config.ALERTS_ENABLED}")
    print(f"   SMTP_SERVER: {config.SMTP_SERVER}")
    print(f"   SMTP_PORT: {config.SMTP_PORT}")
    print(f"   SMTP_USERNAME: {config.SMTP_USERNAME}")
    print(f"   SMTP_PASSWORD: {'***' if config.SMTP_PASSWORD else 'NOT SET'}")
    print(f"   FROM_EMAIL: {config.FROM_EMAIL}")
    
    print("\n📁 File Settings:")
    print(f"   STORE_URLS_FILE: {config.STORE_URLS_FILE}")
    print(f"   File exists: {os.path.exists(config.STORE_URLS_FILE)}")
    print(f"   Current directory: {os.getcwd()}")
    print(f"   Files in current dir: {os.listdir('.')}")
    
    print("\n🔍 Running Validation Checks:")
    
    errors = []
    
    # Check 1: Monitor hours
    if config.MONITOR_START_HOUR < 0 or config.MONITOR_START_HOUR > 23:
        errors.append("MONITOR_START_HOUR must be between 0-23")
    else:
        print("   ✅ MONITOR_START_HOUR valid")
        
    if config.MONITOR_END_HOUR < 0 or config.MONITOR_END_HOUR > 23:
        errors.append("MONITOR_END_HOUR must be between 0-23")
    else:
        print("   ✅ MONITOR_END_HOUR valid")
        
    if config.MONITOR_START_HOUR >= config.MONITOR_END_HOUR:
        errors.append("MONITOR_START_HOUR must be less than MONITOR_END_HOUR")
    else:
        print("   ✅ Monitor hours range valid")
    
    # Check 2: Interval
    if config.CHECK_INTERVAL_MINUTES < 1:
        errors.append("CHECK_INTERVAL_MINUTES must be at least 1")
    else:
        print("   ✅ CHECK_INTERVAL_MINUTES valid")
    
    # Check 3: Store URLs file
    if not os.path.exists(config.STORE_URLS_FILE):
        errors.append(f"Store URLs file not found: {config.STORE_URLS_FILE}")
        print(f"   ❌ Store URLs file missing: {config.STORE_URLS_FILE}")
    else:
        print(f"   ✅ Store URLs file found: {config.STORE_URLS_FILE}")
        
        # Check file content
        try:
            import json
            with open(config.STORE_URLS_FILE) as f:
                data = json.load(f)
                url_count = len(data.get('urls', []))
                print(f"       📊 Contains {url_count} URLs")
        except Exception as e:
            errors.append(f"Store URLs file invalid JSON: {e}")
            print(f"   ❌ JSON error: {e}")
    
    # Check 4: Email config
    print("\n   📧 Email Configuration Checks:")
    if config.ALERTS_ENABLED:
        if not config.SMTP_SERVER:
            errors.append("SMTP_SERVER is required when alerts are enabled")
            print("   ❌ SMTP_SERVER missing")
        else:
            print(f"   ✅ SMTP_SERVER: {config.SMTP_SERVER}")
        
        if not config.SMTP_USERNAME:
            errors.append("SMTP_USERNAME is required when alerts are enabled")
            print("   ❌ SMTP_USERNAME missing")
        else:
            print(f"   ✅ SMTP_USERNAME: {config.SMTP_USERNAME}")
            
        if not config.SMTP_PASSWORD:
            errors.append("SMTP_PASSWORD is required when alerts are enabled")
            print("   ❌ SMTP_PASSWORD missing")
        else:
            print("   ✅ SMTP_PASSWORD set")
            
        if not config.FROM_EMAIL:
            errors.append("FROM_EMAIL is required when alerts are enabled")
            print("   ❌ FROM_EMAIL missing")
        else:
            print(f"   ✅ FROM_EMAIL: {config.FROM_EMAIL}")
    else:
        print("   ℹ️  Email alerts disabled")
    
    # Check 5: Timezone
    try:
        if config.validate_timezone():
            print("   ✅ Timezone validation passed")
        else:
            errors.append("Timezone validation failed")
            print("   ❌ Timezone validation failed")
    except Exception as e:
        errors.append(f"Timezone error: {e}")
        print(f"   ❌ Timezone error: {e}")
    
    print(f"\n📋 Validation Results:")
    if errors:
        print(f"   ❌ Found {len(errors)} errors:")
        for error in errors:
            print(f"      • {error}")
        return False
    else:
        print("   ✅ All validations passed!")
        return True

if __name__ == "__main__":
    debug_configuration()