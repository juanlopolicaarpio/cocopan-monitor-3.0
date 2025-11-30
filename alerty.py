#!/usr/bin/env python3
"""
Quick test for client email alerts - Works at any time
"""
import logging
from datetime import datetime

# Setup logging to see what's happening
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Import the client alerts system
from client_alerts import client_alerts, StoreAlert

def test_client_email():
    """Test client email with sample offline stores"""
    
    logger.info("=" * 70)
    logger.info("🧪 TESTING CLIENT EMAIL ALERTS")
    logger.info("=" * 70)
    
    # Create sample offline stores (matching your actual format)
    offline_stores = [
        # GrabFood stores
        StoreAlert(
            name="Cocopan Citisquare",
            platform="GrabFood",
            status="OFFLINE",
            last_check=datetime.now()
        ),
        StoreAlert(
            name="Cocopan Sierra Madre",
            platform="GrabFood",
            status="OFFLINE",
            last_check=datetime.now()
        ),
        # Foodpanda stores
        StoreAlert(
            name="Cocopan Citisquare Malabon",
            platform="Foodpanda",
            status="OFFLINE",
            last_check=datetime.now()
        ),
        StoreAlert(
            name="Cocopan Moonwalk",
            platform="Foodpanda",
            status="OFFLINE",
            last_check=datetime.now()
        ),
        StoreAlert(
            name="Cocopan Pulang Lupa",
            platform="Foodpanda",
            status="OFFLINE",
            last_check=datetime.now()
        )
    ]
    
    total_stores = 152  # Your actual total from branch_urls.json
    
    logger.info(f"📧 Preparing test email:")
    logger.info(f"   • {len(offline_stores)} offline stores")
    logger.info(f"   • {total_stores} total stores being monitored")
    logger.info(f"   • Recipient: juanlopolicarpio@gmail.com")
    logger.info("")
    logger.info("📨 Sending test email now...")
    logger.info("")
    
    # Send the email
    success = client_alerts.send_hourly_status_alert(offline_stores, total_stores)
    
    logger.info("")
    logger.info("=" * 70)
    if success:
        logger.info("✅ TEST EMAIL SENT SUCCESSFULLY!")
        logger.info("")
        logger.info("📬 Please check your inbox:")
        logger.info("   Email: juanlopolicarpio@gmail.com")
        logger.info("   Subject: EMPORIA UPDATE - 5 Store(s) Offline")
        logger.info("")
        logger.info("📋 The email should contain:")
        logger.info("   • Grabfood (2) Offline stores section")
        logger.info("   • Foodpanda (3) Offline stores section")
        logger.info("   • Formatted with red circles (🔴)")
        logger.info("   • Clean store names (no 'Cocopan' prefix)")
    else:
        logger.error("❌ TEST EMAIL FAILED!")
        logger.error("")
        logger.error("Possible issues:")
        logger.error("   1. SMTP credentials not configured in config.py")
        logger.error("   2. Email cooldown period active (check .last_client_alerts.json)")
        logger.error("   3. Network/firewall blocking SMTP")
        logger.error("")
        logger.error("💡 Check the logs above for specific error messages")
    logger.info("=" * 70)
    
    return success

if __name__ == "__main__":
    try:
        test_client_email()
    except Exception as e:
        logger.error(f"❌ Test script error: {e}")
        import traceback
        traceback.print_exc()