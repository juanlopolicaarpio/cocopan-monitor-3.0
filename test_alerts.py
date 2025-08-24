#!/usr/bin/env python3
"""
CocoPan Alert Testing Utility
Test email configuration and send sample alerts
"""
import sys
import logging
import argparse
from datetime import datetime
from typing import Optional

from config import config
from alerts import alert_manager, StoreAlert

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_smtp_connection():
    """Test basic SMTP connection"""
    import smtplib
    import ssl
    
    print("🔧 Testing SMTP Connection...")
    print(f"   Server: {config.SMTP_SERVER}:{config.SMTP_PORT}")
    print(f"   Username: {config.SMTP_USERNAME}")
    print(f"   TLS: {config.SMTP_USE_TLS}")
    
    try:
        context = ssl.create_default_context()
        
        with smtplib.SMTP(config.SMTP_SERVER, config.SMTP_PORT) as server:
            if config.SMTP_USE_TLS:
                server.starttls(context=context)
            server.login(config.SMTP_USERNAME, config.SMTP_PASSWORD)
            
        print("✅ SMTP connection successful!")
        return True
        
    except Exception as e:
        print(f"❌ SMTP connection failed: {str(e)}")
        return False

def send_test_offline_alert():
    """Send a test offline alert"""
    print("📧 Sending test offline alert...")
    
    test_store = StoreAlert(
        store_name="Cocopan Test Store",
        store_url="https://food.grab.com/ph/en/restaurant/test-store",
        platform="grabfood",
        status="offline",
        timestamp=datetime.now(),
        error_message="Test alert - store appears to be closed for testing",
        response_time_ms=5000
    )
    
    try:
        alert_manager.send_store_offline_alert(test_store)
        print("✅ Test offline alert sent!")
        return True
    except Exception as e:
        print(f"❌ Failed to send test offline alert: {str(e)}")
        return False

def send_test_online_alert():
    """Send a test recovery alert"""
    print("📧 Sending test recovery alert...")
    
    test_store = StoreAlert(
        store_name="Cocopan Test Store", 
        store_url="https://food.grab.com/ph/en/restaurant/test-store",
        platform="grabfood",
        status="online",
        timestamp=datetime.now(),
        response_time_ms=1200
    )
    
    try:
        alert_manager.send_store_online_alert(test_store)
        print("✅ Test recovery alert sent!")
        return True
    except Exception as e:
        print(f"❌ Failed to send test recovery alert: {str(e)}")
        return False

def send_test_daily_summary():
    """Send a test daily summary"""
    print("📧 Sending test daily summary...")
    
    try:
        alert_manager.send_daily_summary()
        print("✅ Test daily summary sent!")
        return True
    except Exception as e:
        print(f"❌ Failed to send test daily summary: {str(e)}")
        return False

def validate_client_config():
    """Validate client alert configuration"""
    print("🔍 Validating client configuration...")
    
    try:
        clients = alert_manager.clients
        
        if not clients:
            print("❌ No clients configured!")
            return False
        
        print(f"✅ Found {len(clients)} client configurations:")
        
        for client_id, client_config in clients.items():
            enabled = client_config.get('enabled', False)
            emails = client_config.get('emails', [])
            alert_types = client_config.get('alert_types', [])
            
            status = "🟢 Enabled" if enabled else "🔴 Disabled"
            print(f"   • {client_id}: {status}")
            print(f"     Emails: {len(emails)} ({', '.join(emails[:2])}{'...' if len(emails) > 2 else ''})")
            print(f"     Alert Types: {len(alert_types)} ({', '.join(alert_types[:3])}{'...' if len(alert_types) > 3 else ''})")
            
            if enabled and not emails:
                print(f"     ⚠️  Warning: No email addresses configured")
                
        return True
        
    except Exception as e:
        print(f"❌ Error validating client config: {str(e)}")
        return False

def check_alert_cooldowns():
    """Check and display alert cooldown status"""
    print("⏰ Checking alert cooldowns...")
    
    try:
        cooldowns = alert_manager.last_alerts
        
        if not cooldowns:
            print("✅ No active cooldowns")
            return True
            
        current_time = datetime.now()
        
        for key, last_sent in cooldowns.items():
            time_since = (current_time - last_sent).total_seconds() / 60
            print(f"   • {key}: Last sent {time_since:.1f} minutes ago")
            
        return True
        
    except Exception as e:
        print(f"❌ Error checking cooldowns: {str(e)}")
        return False

def reset_alert_cooldowns():
    """Reset all alert cooldowns"""
    print("🔄 Resetting alert cooldowns...")
    
    try:
        alert_manager.last_alerts.clear()
        print("✅ All cooldowns reset!")
        return True
    except Exception as e:
        print(f"❌ Error resetting cooldowns: {str(e)}")
        return False

def run_comprehensive_test():
    """Run comprehensive alert system test"""
    print("🧪 Running Comprehensive Alert System Test")
    print("=" * 50)
    
    tests = [
        ("Configuration Validation", lambda: config.validate_config() == []),
        ("Client Config Validation", validate_client_config),
        ("SMTP Connection Test", test_smtp_connection),
        ("Alert Cooldown Check", check_alert_cooldowns),
        ("Test Offline Alert", send_test_offline_alert),
        ("Test Recovery Alert", send_test_online_alert),
        ("Test Daily Summary", send_test_daily_summary),
    ]
    
    passed = 0
    failed = 0
    
    for test_name, test_func in tests:
        print(f"\n🔬 {test_name}...")
        try:
            if test_func():
                print(f"   ✅ PASSED")
                passed += 1
            else:
                print(f"   ❌ FAILED")
                failed += 1
        except Exception as e:
            print(f"   💥 ERROR: {str(e)}")
            failed += 1
    
    print(f"\n📊 Test Results:")
    print(f"   ✅ Passed: {passed}")
    print(f"   ❌ Failed: {failed}")
    print(f"   📈 Success Rate: {(passed/(passed+failed)*100):.1f}%")
    
    if failed == 0:
        print(f"\n🎉 All tests passed! Alert system is ready for production.")
    else:
        print(f"\n⚠️  Some tests failed. Please fix issues before deploying.")
    
    return failed == 0

def main():
    parser = argparse.ArgumentParser(description="CocoPan Alert Testing Utility")
    parser.add_argument("--test", choices=[
        "smtp", "offline", "online", "summary", "config", 
        "cooldowns", "reset", "all"
    ], help="Type of test to run")
    parser.add_argument("--client", help="Test specific client (use client ID)")
    
    args = parser.parse_args()
    
    if not config.ALERTS_ENABLED:
        print("❌ Alerts are disabled in configuration!")
        print("   Set ALERTS_ENABLED=true in your .env file")
        sys.exit(1)
    
    if not config.SMTP_USERNAME or not config.SMTP_PASSWORD:
        print("❌ SMTP credentials not configured!")
        print("   Please set SMTP_USERNAME and SMTP_PASSWORD in your .env file")
        sys.exit(1)
    
    print("📧 CocoPan Alert Testing Utility")
    print("=" * 40)
    print(f"Environment: {'Production' if 'prod' in config.DATABASE_URL else 'Development'}")
    print(f"SMTP Server: {config.SMTP_SERVER}:{config.SMTP_PORT}")
    print(f"From Email: {config.FROM_EMAIL}")
    print()
    
    success = True
    
    if args.test == "smtp" or not args.test:
        success &= test_smtp_connection()
    
    if args.test == "config" or not args.test:
        success &= validate_client_config()
    
    if args.test == "offline":
        success &= send_test_offline_alert()
    
    if args.test == "online":
        success &= send_test_online_alert()
        
    if args.test == "summary":
        success &= send_test_daily_summary()
        
    if args.test == "cooldowns":
        success &= check_alert_cooldowns()
        
    if args.test == "reset":
        success &= reset_alert_cooldowns()
        
    if args.test == "all":
        success &= run_comprehensive_test()
    
    if not args.test:
        print("\n💡 Usage Examples:")
        print("   python test_alerts.py --test smtp          # Test SMTP connection")
        print("   python test_alerts.py --test offline       # Send test offline alert")
        print("   python test_alerts.py --test summary       # Send test daily summary")
        print("   python test_alerts.py --test all           # Run all tests")
    
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()