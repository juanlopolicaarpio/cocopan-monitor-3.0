#!/usr/bin/env python3
"""
Fixed CocoPan Database Cleanup - Handles Duplicates
"""
import json
import logging
import sys
from typing import Dict, List, Optional, Set
from database import db
from config import config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def check_for_duplicates():
    """Check for duplicate URLs in the data before processing"""
    logger.info("Checking for duplicate URLs in store data...")
    
    try:
        with open('branch_urls.json') as f:
            data = json.load(f)
            urls = data.get('urls', [])
        
        # Find duplicates
        seen = set()
        duplicates = set()
        
        for url in urls:
            clean_url = url.rstrip('?').rstrip('/')
            if clean_url in seen:
                duplicates.add(clean_url)
                logger.warning(f"DUPLICATE URL found: {clean_url}")
            seen.add(clean_url)
        
        if duplicates:
            logger.error(f"Found {len(duplicates)} duplicate URLs:")
            for dup in list(duplicates)[:5]:  # Show first 5
                logger.error(f"  - {dup}")
            
            logger.info("Removing duplicates and creating clean URLs list...")
            
            # Create clean list
            clean_urls = []
            seen_clean = set()
            
            for url in urls:
                clean_url = url.rstrip('?').rstrip('/')
                if clean_url not in seen_clean:
                    clean_urls.append(clean_url)
                    seen_clean.add(clean_url)
            
            # Update branch_urls.json with clean list
            clean_data = {
                "urls": clean_urls,
                "meta": {
                    "total_stores": len(clean_urls),
                    "duplicates_removed": len(urls) - len(clean_urls)
                }
            }
            
            with open('branch_urls.json', 'w') as f:
                json.dump(clean_data, f, indent=2)
            
            logger.info(f"Cleaned URLs: {len(urls)} -> {len(clean_urls)}")
            return clean_urls
        else:
            logger.info("No duplicates found")
            return urls
            
    except Exception as e:
        logger.error(f"Error checking duplicates: {e}")
        return []

def force_cleanup_database():
    """Force cleanup all database tables"""
    logger.info("Force cleaning database...")
    
    try:
        with db.get_connection() as conn:
            cursor = conn.cursor()
            
            # Disable foreign key checks temporarily
            cursor.execute("SET session_replication_role = replica;")
            
            # Delete in reverse dependency order
            tables = ['status_checks', 'store_status_hourly', 'summary_reports', 
                     'status_summary_hourly', 'stores']
            
            for table in tables:
                try:
                    cursor.execute(f"DELETE FROM {table}")
                    deleted = cursor.rowcount
                    logger.info(f"  {table}: deleted {deleted} records")
                except Exception as e:
                    logger.warning(f"  {table}: {e}")
            
            # Re-enable foreign key checks
            cursor.execute("SET session_replication_role = DEFAULT;")
            
            conn.commit()
            logger.info("Force cleanup completed")
            return True
            
    except Exception as e:
        logger.error(f"Force cleanup failed: {e}")
        return False

def setup_unique_stores(urls, store_names_map):
    """Set up stores ensuring no duplicates"""
    logger.info(f"Setting up {len(urls)} unique stores...")
    
    created_count = 0
    skipped_count = 0
    
    try:
        with db.get_connection() as conn:
            cursor = conn.cursor()
            
            for url in urls:
                try:
                    # Get store info
                    clean_url = url.rstrip('?').rstrip('/')
                    
                    if clean_url in store_names_map:
                        store_name = store_names_map[clean_url].get('store_name', 'Cocopan Store')
                        platform = store_names_map[clean_url].get('platform', 'unknown')
                    else:
                        # Fallback name
                        if 'foodpanda' in url:
                            store_name = f"Cocopan Foodpanda Store"
                            platform = 'foodpanda'
                        elif 'grab.com' in url:
                            store_name = f"Cocopan GrabFood Store"
                            platform = 'grabfood'
                        else:
                            store_name = "Cocopan Store"
                            platform = 'unknown'
                        
                        logger.warning(f"No predefined name for: {clean_url}")
                    
                    # Check if already exists
                    cursor.execute("SELECT id FROM stores WHERE url = %s", (clean_url,))
                    if cursor.fetchone():
                        skipped_count += 1
                        continue
                    
                    # Insert new store
                    cursor.execute(
                        "INSERT INTO stores (name, url, platform) VALUES (%s, %s, %s)",
                        (store_name, clean_url, platform)
                    )
                    created_count += 1
                    
                    if created_count <= 3:
                        logger.info(f"  Created: {store_name}")
                    elif created_count == 4:
                        logger.info("  ...")
                
                except Exception as e:
                    logger.error(f"Failed to create store for {url}: {e}")
                    continue
            
            conn.commit()
            logger.info(f"Store setup completed:")
            logger.info(f"  Created: {created_count}")
            logger.info(f"  Skipped: {skipped_count}")
            
            return created_count > 0
            
    except Exception as e:
        logger.error(f"Store setup failed: {e}")
        return False

def validate_database():
    """Validate final database state"""
    logger.info("Validating database...")
    
    try:
        with db.get_connection() as conn:
            cursor = conn.cursor()
            
            # Check total count
            cursor.execute("SELECT COUNT(*) FROM stores")
            total_count = cursor.fetchone()[0]
            
            # Check platforms
            cursor.execute("SELECT platform, COUNT(*) FROM stores GROUP BY platform")
            platforms = dict(cursor.fetchall())
            
            logger.info(f"Database validation:")
            logger.info(f"  Total stores: {total_count}")
            logger.info(f"  Platforms: {platforms}")
            
            # Check for duplicates
            cursor.execute("""
                SELECT url, COUNT(*) as count 
                FROM stores 
                GROUP BY url 
                HAVING COUNT(*) > 1
            """)
            duplicates = cursor.fetchall()
            
            if duplicates:
                logger.error(f"Found {len(duplicates)} duplicate URLs in database!")
                return False
            
            if total_count == 0:
                logger.error("No stores in database!")
                return False
            
            logger.info("Database validation passed")
            return True
            
    except Exception as e:
        logger.error(f"Validation failed: {e}")
        return False

def main():
    """Main process with duplicate handling"""
    print("CocoPan Database Setup - Duplicate-Safe")
    print("=" * 50)
    
    # Check database connection
    try:
        stats = db.get_database_stats()
        logger.info(f"Connected to {stats['db_type']} database")
    except Exception as e:
        logger.error(f"Database connection failed: {e}")
        logger.error("Check your DATABASE_URL")
        sys.exit(1)
    
    # Check required files
    import os
    required_files = ['branch_urls.json', 'store_names.json']
    for file in required_files:
        if not os.path.exists(file):
            logger.error(f"{file} not found! Run convert_stores.py first.")
            sys.exit(1)
    
    # Step 1: Check and clean duplicates
    clean_urls = check_for_duplicates()
    if not clean_urls:
        logger.error("No URLs to process")
        sys.exit(1)
    
    # Load store names
    try:
        with open('store_names.json') as f:
            store_names_map = json.load(f)['store_names']
    except Exception as e:
        logger.error(f"Failed to load store names: {e}")
        store_names_map = {}
    
    # Step 2: Confirm cleanup
    print(f"\nAbout to set up {len(clean_urls)} stores")
    response = input("Continue? (y/n): ")
    if response.lower() != 'y':
        logger.info("Cancelled by user")
        sys.exit(0)
    
    # Step 3: Force cleanup
    if not force_cleanup_database():
        logger.error("Database cleanup failed")
        sys.exit(1)
    
    # Step 4: Setup stores
    if not setup_unique_stores(clean_urls, store_names_map):
        logger.error("Store setup failed")
        sys.exit(1)
    
    # Step 5: Validate
    if not validate_database():
        logger.error("Database validation failed")
        sys.exit(1)
    
    print("\nSUCCESS!")
    print("=" * 50)
    print("Database setup completed successfully")
    print("Ready for monitor service deployment")

if __name__ == "__main__":
    main()