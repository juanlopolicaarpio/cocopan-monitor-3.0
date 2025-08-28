#!/usr/bin/env python3
"""
CocoPan Admin Alert Testing Script
Test the admin notification system
"""
import sys
import json
import logging
import argparse
from datetime import datetime
from pathlib import Path

# Import our admin alerts
try:
    from admin_alerts import admin_alerts, ProblemStore
    from config import config
except ImportError as e:
    print(f"❌ Import error: {e}")
    print("Make sure admin_alerts.py and config.py are in the same directory")
    sys.exit(1)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def check_config_exists():
    """Check if admin config file exists"""
    config_file = Path("admin_alerts.json")
    
    if not config_file.exists():
        print("❌ admin_alerts.json not found!")
        print("\nCreating default config file...")
        
        default_config = {
            "admin_team": {
                "name": "CocoPan Operations Admin",
                "emails": [
                    "your-admin@email.com"
                ],
                "enabled": True,
                "timezone": "Asia/Manila"
            },
            "alert_triggers": {
                "manual_verification_needed": {
                    "enabled": True,
                    "min_stores_threshold": 1,
                    "statuses": ["BLOCKED", "UNKNOWN", "ERROR"],
                    "cooldown_minutes": 60,
                    "include_direct_links": True
                },
                "bot_detection_spike": {
                    "enabled": True,
                    "blocked_stores_threshold": 3,
                    "cooldown_minutes": 30
                }
            },
            "email_settings": {
                "from_name": "CocoPan Monitor System",
                "subject_prefix": "🚨 CocoPan Admin Alert",
                "priority": "high",
                "include_dashboard_link": True,
                "dashboard_url": "http://localhost:8501/admin"
            }
        }
        
        with open("admin_alerts.json", "w") as f:
            json.dump(default_config, f, indent=2)
            
        print("✅ Created admin_alerts.json")
        print("📝 Please edit the email address in admin_alerts.json")
        return False
    
    return True

def validate_admin_config():
    """Validate admin configuration"""
    print("🔍 Validating admin configuration...")
    
    if not admin_alerts.config:
        print("❌ Admin config not loaded")
        return False
    
    admin_team = admin_alerts.config.get('admin_team', {})
    
    if not admin_team.get('enabled', False):
        print("❌ Admin alerts disabled")
        print("   Set 'enabled': true in admin_alerts.json")
        return False
    
    emails = admin_team.get('emails', [])
    if not emails or emails == ["your-admin@email.com"]:
        print("❌ No valid admin email addresses configured")
        print("   Update 'emails' in admin_alerts.json")
        return False
    
    print(f"✅ Admin config valid")
    print(f"   Recipients: {', '.join(emails)}")
    print(f"   Alerts enabled: {admin_team['enabled']}")
    
    return True

def check_smtp_config():
    """Check if SMTP is configured for admin alerts"""
    print("📧 Checking SMTP configuration...")
    
    required_settings = [
        ('SMTP_SERVER', config.SMTP_SERVER),
        ('SMTP_PORT', config.SMTP_PORT),
        ('SMTP_USERNAME', config.SMTP_USERNAME),
        ('SMTP_PASSWORD', config.SMTP_PASSWORD),
        ('FROM_EMAIL', config.FROM_EMAIL)
    ]
    
    missing = []
    for name, value in required_settings:
        if not value:
            missing.append(name)
    
    if missing:
        print(f"❌ Missing SMTP settings: {', '.join(missing)}")
        print("   Admin alerts use the same SMTP config as client alerts")
        print("   Set these in your .env file or environment variables")
        return False
    
    print("✅ SMTP configuration found")
    return True

def send_test_manual_verification_alert():
    """Send test manual verification alert"""
    print("📧 Sending test manual verification alert...")
    
    # Create test problem stores
    test_stores = [
        ProblemStore(
            name="Cocopan Sixto Antonio Avenue",
            url="https://www.foodpanda.ph/restaurant/dntx/cocopan-sixto-antonio-avenue",
            status="BLOCKED",
            message="Access denied (403) after retries",
            response_time=3479,
            platform="foodpanda"
        ),
        ProblemStore(
            name="Cocopan Store",
            url="https://foodpanda.page.link/o9eQouUY9YpvhCEQA",
            status="UNKNOWN",
            message="Status unclear, needs manual verification",
            response_time=563,
            platform="foodpanda"
        ),
        ProblemStore(
            name="Cocopan Test Store",
            url="https://food.grab.com/ph/en/restaurant/test-store",
            status="ERROR",
            message="Connection timeout after retries",
            response_time=0,
            platform="grabfood"
        )
    ]
    
    try:
        # Reset cooldown for testing
        if 'manual_verification_needed' in admin_alerts.last_alerts:
            del admin_alerts.last_alerts['manual_verification_needed']
        
        success = admin_alerts.send_manual_verification_alert(test_stores)
        
        if success:
            print("✅ Test manual verification alert sent!")
            print("   Check your admin email for the alert")
            return True
        else:
            print("❌ Failed to send test alert")
            return False
            
    except Exception as e:
        print(f"❌ Error sending test alert: {e}")
        return False

def send_test_bot_detection_alert():
    """Send test bot detection alert"""
    print("📧 Sending test bot detection alert...")
    
    try:
        # Reset cooldown for testing
        if 'bot_detection_spike' in admin_alerts.last_alerts:
            del admin_alerts.last_alerts['bot_detection_spike']
        
        success = admin_alerts.send_bot_detection_alert(5)  # 5 blocked stores
        
        if success:
            print("✅ Test bot detection alert sent!")
            return True
        else:
            print("❌ Failed to send test bot detection alert")
            return False
            
    except Exception as e:
        print(f"❌ Error sending bot detection alert: {e}")
        return False

def check_alert_cooldowns():
    """Check current alert cooldowns"""
    print("⏰ Checking alert cooldowns...")
    
    if not admin_alerts.last_alerts:
        print("✅ No active cooldowns")
        return True
    
    current_time = datetime.now()
    
    for alert_type, last_sent in admin_alerts.last_alerts.items():
        time_since = (current_time - last_sent).total_seconds() / 60
        
        # Get cooldown period for this alert type
        trigger_config = admin_alerts.config.get('alert_triggers', {}).get(alert_type, {})
        cooldown_minutes = trigger_config.get('cooldown_minutes', 60)
        
        if time_since < cooldown_minutes:
            remaining = cooldown_minutes - time_since
            print(f"   🔒 {alert_type}: {remaining:.1f} minutes remaining")
        else:
            print(f"   ✅ {alert_type}: Ready to send")
    
    return True

def reset_cooldowns():
    """Reset all alert cooldowns"""
    print("🔄 Resetting all alert cooldowns...")
    
    admin_alerts.last_alerts.clear()
    print("✅ All cooldowns reset!")
    return True

def simulate_monitor_problems():
    """Simulate what happens when monitor detects problems"""
    print("🎭 Simulating monitor detecting problem stores...")
    
    # This simulates what the monitor would do
    from monitor_service_updated import StoreStatus, CheckResult
    
    # Simulate monitor results with problems
    simulated_results = [
        {
            'url': 'https://www.foodpanda.ph/restaurant/dntx/cocopan-sixto-antonio-avenue',
            'name': 'Cocopan Sixto Antonio Avenue', 
            'result': CheckResult(
                status=StoreStatus.BLOCKED,
                response_time=3479,
                message="Access denied (403) after retries"
            )
        },
        {
            'url': 'https://foodpanda.page.link/o9eQouUY9YpvhCEQA',
            'name': 'Cocopan Store',
            'result': CheckResult(
                status=StoreStatus.UNKNOWN,
                response_time=563,
                message="Status unclear, needs verification"
            )
        }
    ]
    
    # Convert to ProblemStore format
    problem_stores = []
    for result_dict in simulated_results:
        result = result_dict['result']
        if result.status in [StoreStatus.BLOCKED, StoreStatus.UNKNOWN, StoreStatus.ERROR]:
            
            url = result_dict['url']
            if 'foodpanda' in url:
                platform = 'foodpanda'
            elif 'grab.com' in url:
                platform = 'grabfood'
            else:
                platform = 'unknown'
            
            problem_store = ProblemStore(
                name=result_dict['name'],
                url=url,
                status=result.status.value.upper(),
                message=result.message or "No details available",
                response_time=result.response_time,
                platform=platform
            )
            problem_stores.append(problem_store)
    
    if problem_stores:
        print(f"📧 Sending admin alert for {len(problem_stores)} problem stores...")
        
        # Reset cooldown for testing
        if 'manual_verification_needed' in admin_alerts.last_alerts:
            del admin_alerts.last_alerts['manual_verification_needed']
        
        success = admin_alerts.send_manual_verification_alert(problem_stores)
        
        if success:
            print("✅ Simulated monitor alert sent!")
            print("   This is what you'll receive when the real monitor detects problems")
            return True
        else:
            print("❌ Failed to send simulated alert")
            return False
    
    return True

def run_comprehensive_test():
    """Run comprehensive admin alert test"""
    print("🧪 Running Comprehensive Admin Alert Test")
    print("=" * 50)
    
    tests = [
        ("Config File Check", check_config_exists),
        ("Config Validation", validate_admin_config), 
        ("SMTP Configuration", check_smtp_config),
        ("Alert Cooldowns", check_alert_cooldowns),
        ("Test Manual Verification Alert", send_test_manual_verification_alert),
        ("Test Bot Detection Alert", send_test_bot_detection_alert),
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
        print(f"\n🎉 All tests passed! Admin alert system is ready.")
        print(f"📧 You should have received test emails at your configured address")
        print(f"\n📝 Next steps:")
        print(f"   1. Replace monitor_service.py with the updated version")
        print(f"   2. Restart your monitoring service")
        print(f"   3. You'll get alerts when stores need manual verification")
    else:
        print(f"\n⚠️  Some tests failed. Please fix issues before deploying.")
    
    return failed == 0

def main():
    parser = argparse.ArgumentParser(description="CocoPan Admin Alert Testing")
    parser.add_argument("--test", choices=[
        "config", "smtp", "manual", "bot", "cooldowns", 
        "reset", "simulate", "all"
    ], help="Type of test to run")
    
    args = parser.parse_args()
    
    print("🚨 CocoPan Admin Alert Testing")
    print("=" * 40)
    
    if not args.test:
        print("💡 Usage Examples:")
        print("   python test_admin_alerts.py --test config     # Check configuration")
        print("   python test_admin_alerts.py --test manual     # Send test manual alert") 
        print("   python test_admin_alerts.py --test simulate   # Simulate monitor problems")
        print("   python test_admin_alerts.py --test all        # Run all tests")
        return
    
    success = True
    
    if args.test == "config":
        success &= check_config_exists() and validate_admin_config()
    elif args.test == "smtp":
        success &= check_smtp_config()
    elif args.test == "manual":
        success &= send_test_manual_verification_alert()
    elif args.test == "bot":
        success &= send_test_bot_detection_alert()
    elif args.test == "cooldowns":
        success &= check_alert_cooldowns()
    elif args.test == "reset":
        success &= reset_cooldowns()
    elif args.test == "simulate":
        success &= simulate_monitor_problems()
    elif args.test == "all":
        success &= run_comprehensive_test()
    
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()