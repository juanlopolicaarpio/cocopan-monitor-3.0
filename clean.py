#!/usr/bin/env python3
"""
CocoPan Production Database Cleanup
WIPES ALL EXISTING DATA and prepares for fresh production deployment
"""
import logging
import sys
from database import db
from config import config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def confirm_cleanup():
    """Confirm with user before wiping data"""
    print("🚨 PRODUCTION DATABASE CLEANUP")
    print("=" * 50)
    print("⚠️  WARNING: This will DELETE ALL existing data:")
    print("   • All store status checks")
    print("   • All summary reports") 
    print("   • All hourly snapshots")
    print("   • All VA check-in history")
    print("   • BUT keeps the 66 store definitions")
    print()
    
    response = input("Are you ABSOLUTELY SURE you want to proceed? (type 'YES' to confirm): ")
    return response.strip() == 'YES'

def cleanup_all_data():
    """Clean up all operational data but keep store definitions"""
    try:
        with db.get_connection() as conn:
            cursor = conn.cursor()
            
            logger.info("🗑️ Cleaning up status_checks table...")
            cursor.execute("DELETE FROM status_checks")
            deleted_checks = cursor.rowcount
            
            logger.info("🗑️ Cleaning up summary_reports table...")
            cursor.execute("DELETE FROM summary_reports") 
            deleted_reports = cursor.rowcount
            
            logger.info("🗑️ Cleaning up store_status_hourly table...")
            cursor.execute("DELETE FROM store_status_hourly")
            deleted_hourly = cursor.rowcount
            
            logger.info("🗑️ Cleaning up status_summary_hourly table...")
            cursor.execute("DELETE FROM status_summary_hourly")
            deleted_summaries = cursor.rowcount
            
            # Reset any manual overrides
            logger.info("🔄 Resetting store overrides...")
            cursor.execute("UPDATE stores SET name_override = NULL, last_manual_check = NULL")
            reset_stores = cursor.rowcount
            
            conn.commit()
            
            logger.info("✅ Cleanup completed:")
            logger.info(f"   📊 Status checks deleted: {deleted_checks}")
            logger.info(f"   📈 Summary reports deleted: {deleted_reports}")
            logger.info(f"   ⏰ Hourly status records deleted: {deleted_hourly}")
            logger.info(f"   📋 Hourly summaries deleted: {deleted_summaries}")
            logger.info(f"   🏪 Store overrides reset: {reset_stores}")
            
            return True
            
    except Exception as e:
        logger.error(f"❌ Cleanup failed: {e}")
        return False

def validate_production_readiness():
    """Validate system is ready for production"""
    logger.info("🔍 Validating production readiness...")
    
    errors = []
    warnings = []
    
    try:
        # Check database connection
        stats = db.get_database_stats()
        logger.info(f"✅ Database connected: {stats['db_type']}")
        
        # Check store count
        if stats['store_count'] != 66:
            errors.append(f"Expected 66 stores, found {stats['store_count']}")
        else:
            logger.info("✅ Exactly 66 stores configured")
            
        # Check platform distribution
        platforms = stats['platforms']
        grab_count = platforms.get('grabfood', 0)
        foodpanda_count = platforms.get('foodpanda', 0)
        
        if grab_count == 0:
            errors.append("No GrabFood stores found")
        if foodpanda_count == 0:
            errors.append("No Foodpanda stores found")
            
        logger.info(f"✅ Platform distribution: GrabFood={grab_count}, Foodpanda={foodpanda_count}")
        
        # Check configuration
        config_errors = config.validate_config()
        if config_errors:
            errors.extend(config_errors)
        else:
            logger.info("✅ Configuration valid")
            
        # Check timezone
        if not config.validate_timezone():
            errors.append("Timezone validation failed")
        else:
            logger.info("✅ Manila timezone configured correctly")
            
        # Check required files
        import os
        required_files = [
            'branch_urls.json',
            'admin_alerts.json', 
            'client_alerts.json'
        ]
        
        for file in required_files:
            if not os.path.exists(file):
                errors.append(f"Required file missing: {file}")
            else:
                logger.info(f"✅ Found: {file}")
                
        # Check email configuration for alerts
        if not config.SMTP_USERNAME or not config.SMTP_PASSWORD:
            warnings.append("Email alerts not configured (SMTP credentials missing)")
        else:
            logger.info("✅ Email alerts configured")
            
        return errors, warnings
        
    except Exception as e:
        errors.append(f"Validation failed: {e}")
        return errors, warnings

def main():
    """Main cleanup and validation"""
    print("🥥 CocoPan Production Deployment Preparation")
    print("=" * 60)
    
    # Step 1: Validation first
    logger.info("Step 1: Pre-cleanup validation...")
    errors, warnings = validate_production_readiness()
    
    if errors:
        logger.error("❌ CRITICAL ERRORS found - fix before deploying:")
        for error in errors:
            logger.error(f"   • {error}")
        sys.exit(1)
        
    if warnings:
        logger.warning("⚠️ WARNINGS found:")
        for warning in warnings:
            logger.warning(f"   • {warning}")
        print()
    
    # Step 2: Confirm cleanup
    if not confirm_cleanup():
        logger.info("❌ Cleanup cancelled by user")
        sys.exit(0)
        
    # Step 3: Clean up data
    logger.info("Step 2: Cleaning up existing data...")
    if not cleanup_all_data():
        logger.error("❌ Cleanup failed!")
        sys.exit(1)
        
    # Step 4: Final validation
    logger.info("Step 3: Post-cleanup validation...")
    errors, warnings = validate_production_readiness()
    
    if errors:
        logger.error("❌ Post-cleanup validation failed:")
        for error in errors:
            logger.error(f"   • {error}")
        sys.exit(1)
    
    # Success!
    print()
    print("🎉 PRODUCTION READY!")
    print("=" * 60)
    print("✅ Database cleaned and validated")
    print("✅ 66 stores configured correctly")
    print("✅ Configuration validated")
    print("✅ All required files present")
    if warnings:
        print("⚠️ Minor warnings (won't block deployment):")
        for warning in warnings:
            print(f"   • {warning}")
    print()
    print("🚀 Ready to deploy with:")
    print("   docker-compose up -d")
    print()

if __name__ == "__main__":
    main()