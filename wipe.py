#!/usr/bin/env python3
"""
🧹 SKU Data Cleanup Script
Safely wipe all SKU-related data from the database while preserving schema.

WHAT THIS DOES:
- Deletes all records from master_skus
- Deletes all records from store_sku_checks
- Deletes all records from sku_compliance_summary
- Preserves table structure (so you can repopulate)

WHAT THIS DOESN'T TOUCH:
- stores table (your store list remains)
- status_checks table (monitoring history preserved)
- All other tables remain intact
"""

import sys
import os
from datetime import datetime
from typing import Dict

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from database import db
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def get_current_sku_counts() -> Dict[str, int]:
    """Get current record counts from all SKU tables"""
    counts = {
        'master_skus': 0,
        'store_sku_checks': 0,
        'sku_compliance_summary': 0
    }
    
    try:
        with db.get_connection() as conn:
            cur = conn.cursor()
            
            for table in counts.keys():
                cur.execute(f"SELECT COUNT(*) FROM {table}")
                row = cur.fetchone()
                if row:
                    counts[table] = row[0] if db.db_type == "postgresql" else row[0]
        
        return counts
    except Exception as e:
        logger.error(f"Error getting counts: {e}")
        return counts


def create_backup() -> bool:
    """Create a backup of SKU data to CSV files"""
    backup_dir = f"sku_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    try:
        os.makedirs(backup_dir, exist_ok=True)
        logger.info(f"📁 Creating backup in: {backup_dir}/")
        
        # Backup master_skus
        with db.get_connection() as conn:
            cur = conn.cursor()
            
            # Master SKUs
            cur.execute("SELECT * FROM master_skus")
            rows = cur.fetchall()
            if rows:
                with open(f"{backup_dir}/master_skus_backup.csv", 'w') as f:
                    if db.db_type == "postgresql":
                        # Write header
                        f.write("id,sku_code,product_name,platform,category,division,flow_category,gmv_q3,is_active,created_at\n")
                        for row in rows:
                            f.write(','.join(str(x) if x is not None else '' for x in row) + '\n')
                    else:
                        # SQLite - use row dict
                        keys = rows[0].keys()
                        f.write(','.join(keys) + '\n')
                        for row in rows:
                            f.write(','.join(str(row[k]) if row[k] is not None else '' for k in keys) + '\n')
                logger.info(f"   ✅ Backed up {len(rows)} master SKUs")
            
            # Store SKU checks
            cur.execute("SELECT * FROM store_sku_checks")
            rows = cur.fetchall()
            if rows:
                with open(f"{backup_dir}/store_sku_checks_backup.csv", 'w') as f:
                    if db.db_type == "postgresql":
                        f.write("id,store_id,platform,check_date,out_of_stock_skus,total_skus_checked,out_of_stock_count,compliance_percentage,checked_by,checked_at,notes\n")
                        for row in rows:
                            f.write(','.join(str(x) if x is not None else '' for x in row) + '\n')
                    else:
                        keys = rows[0].keys()
                        f.write(','.join(keys) + '\n')
                        for row in rows:
                            f.write(','.join(str(row[k]) if row[k] is not None else '' for k in keys) + '\n')
                logger.info(f"   ✅ Backed up {len(rows)} SKU checks")
            
            # SKU compliance summary
            cur.execute("SELECT * FROM sku_compliance_summary")
            rows = cur.fetchall()
            if rows:
                with open(f"{backup_dir}/sku_compliance_summary_backup.csv", 'w') as f:
                    if db.db_type == "postgresql":
                        f.write("id,summary_date,platform,total_stores_checked,stores_100_percent,stores_80_plus_percent,stores_below_80_percent,average_compliance_percentage,total_out_of_stock_items,created_at\n")
                        for row in rows:
                            f.write(','.join(str(x) if x is not None else '' for x in row) + '\n')
                    else:
                        keys = rows[0].keys()
                        f.write(','.join(keys) + '\n')
                        for row in rows:
                            f.write(','.join(str(row[k]) if row[k] is not None else '' for k in keys) + '\n')
                logger.info(f"   ✅ Backed up {len(rows)} compliance summaries")
        
        logger.info(f"✅ Backup completed successfully!")
        return True
        
    except Exception as e:
        logger.error(f"❌ Backup failed: {e}")
        return False


def wipe_sku_data() -> bool:
    """Wipe all SKU data from the database"""
    try:
        with db.get_connection() as conn:
            cur = conn.cursor()
            
            logger.info("🧹 Starting SKU data wipe...")
            
            # Delete in correct order (foreign key constraints)
            
            # 1. Delete compliance summaries (no foreign keys)
            logger.info("   🗑️  Deleting sku_compliance_summary...")
            cur.execute("DELETE FROM sku_compliance_summary")
            deleted_summaries = cur.rowcount
            logger.info(f"      ✅ Deleted {deleted_summaries} records")
            
            # 2. Delete store SKU checks (references master_skus and stores)
            logger.info("   🗑️  Deleting store_sku_checks...")
            cur.execute("DELETE FROM store_sku_checks")
            deleted_checks = cur.rowcount
            logger.info(f"      ✅ Deleted {deleted_checks} records")
            
            # 3. Delete master SKUs (no foreign keys pointing to it now)
            logger.info("   🗑️  Deleting master_skus...")
            cur.execute("DELETE FROM master_skus")
            deleted_skus = cur.rowcount
            logger.info(f"      ✅ Deleted {deleted_skus} records")
            
            # Commit the transaction
            conn.commit()
            
            logger.info("✅ SKU data wipe completed successfully!")
            logger.info(f"📊 Total deleted:")
            logger.info(f"   - {deleted_skus} master SKUs")
            logger.info(f"   - {deleted_checks} store checks")
            logger.info(f"   - {deleted_summaries} compliance summaries")
            
            return True
            
    except Exception as e:
        logger.error(f"❌ Wipe failed: {e}")
        return False


def verify_cleanup() -> bool:
    """Verify that all SKU data has been deleted"""
    logger.info("🔍 Verifying cleanup...")
    
    counts = get_current_sku_counts()
    
    total = sum(counts.values())
    
    logger.info(f"📊 Current counts:")
    for table, count in counts.items():
        status = "✅" if count == 0 else "⚠️"
        logger.info(f"   {status} {table}: {count} records")
    
    if total == 0:
        logger.info("✅ Verification successful - all SKU data deleted!")
        return True
    else:
        logger.warning(f"⚠️ Still have {total} records remaining")
        return False


def main():
    """Main execution"""
    print("=" * 80)
    print("🧹 SKU DATA CLEANUP SCRIPT")
    print("=" * 80)
    print()
    
    # Get current counts
    logger.info("📊 Checking current SKU data...")
    counts = get_current_sku_counts()
    
    total_records = sum(counts.values())
    
    if total_records == 0:
        logger.info("✅ Database already clean - no SKU data found!")
        return
    
    print()
    print("📊 CURRENT SKU DATA:")
    for table, count in counts.items():
        print(f"   - {table}: {count:,} records")
    print(f"   TOTAL: {total_records:,} records")
    print()
    
    # Warning
    print("⚠️  WARNING:")
    print("   This will DELETE ALL SKU-related data:")
    print("   - All master SKU products")
    print("   - All daily compliance checks")
    print("   - All compliance summaries")
    print()
    print("   Your stores and monitoring data will NOT be affected.")
    print()
    
    # Ask for backup
    backup_choice = input("📁 Create backup before wiping? (y/n): ").strip().lower()
    
    if backup_choice == 'y':
        logger.info("Creating backup...")
        if not create_backup():
            logger.error("❌ Backup failed! Aborting wipe for safety.")
            return
        print()
    
    # Final confirmation
    print("🚨 FINAL CONFIRMATION 🚨")
    confirmation = input(f"Type 'DELETE {total_records} RECORDS' to proceed: ").strip()
    
    if confirmation != f"DELETE {total_records} RECORDS":
        logger.info("❌ Confirmation failed - aborting.")
        return
    
    print()
    
    # Perform wipe
    if wipe_sku_data():
        print()
        # Verify
        verify_cleanup()
        print()
        print("=" * 80)
        print("✅ SKU DATA CLEANUP COMPLETED!")
        print("=" * 80)
        print()
        print("🎯 Next steps:")
        print("   1. Prepare your new SKU master list (CSV/Excel)")
        print("   2. Run populate.py to load new SKUs")
        print("   3. Run monitor_service.py to start fresh scraping")
        print()
    else:
        print()
        print("❌ CLEANUP FAILED - Check logs above")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n❌ Interrupted by user")
    except Exception as e:
        logger.error(f"❌ Fatal error: {e}")