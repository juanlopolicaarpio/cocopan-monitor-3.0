#!/usr/bin/env python3
"""
Debug Foodpanda Database Issues - Why Foodpanda stores don't show in dashboard
"""
import os
import sys
import logging
import json
from typing import List, Dict, Any
from datetime import datetime, timedelta
import pytz
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
log = logging.getLogger("foodpanda_debug")

try:
    from database import db  # your existing connection helper
    from config import config
except Exception as e:
    log.error("Failed to import database.db helper: %s", e)
    sys.exit(1)

def standardize_platform_name(platform_value):
    """Same function used in dashboard"""
    if pd.isna(platform_value) or platform_value is None:
        return "Unknown"
    platform_str = str(platform_value).lower()
    if 'grab' in platform_str:
        return "GrabFood"
    elif 'foodpanda' in platform_str or 'panda' in platform_str:
        return "Foodpanda"
    else:
        return "Unknown"

def get_manila_time():
    """Get current Manila time"""
    manila_tz = pytz.timezone('Asia/Manila')
    return datetime.now(manila_tz)

def debug_foodpanda_database():
    """Comprehensive debug of Foodpanda data in database"""
    
    with db.get_connection() as conn:
        cur = conn.cursor()
        
        current_manila = get_manila_time()
        log.info("=== FOODPANDA DATABASE DEBUG ===")
        log.info(f"Current Manila time: {current_manila}")
        log.info(f"Today's date: {current_manila.date()}")
        
        # 1. Check all platforms in stores table
        log.info("\n=== 1. ALL PLATFORMS IN STORES TABLE ===")
        cur.execute("""
            SELECT 
                platform,
                COUNT(*) as store_count,
                MIN(created_at) as first_created,
                MAX(created_at) as last_created
            FROM stores 
            GROUP BY platform
            ORDER BY store_count DESC
        """)
        
        platforms = cur.fetchall()
        log.info(f"Found {len(platforms)} different platforms:")
        for platform, count, first, last in platforms:
            standardized = standardize_platform_name(platform)
            log.info(f"  '{platform}' -> '{standardized}' ({count} stores, created {first} to {last})")
        
        # 2. Check specific Foodpanda-like platform names
        log.info("\n=== 2. FOODPANDA-LIKE PLATFORM NAMES ===")
        cur.execute("""
            SELECT 
                platform,
                COUNT(*) as count,
                string_agg(name, ', ' ORDER BY name LIMIT 3) as sample_names
            FROM stores 
            WHERE LOWER(platform) LIKE '%panda%' 
               OR LOWER(platform) LIKE '%food%'
            GROUP BY platform
            ORDER BY count DESC
        """)
        
        foodpanda_like = cur.fetchall()
        if foodpanda_like:
            log.info(f"Found {len(foodpanda_like)} Foodpanda-like platforms:")
            for platform, count, samples in foodpanda_like:
                log.info(f"  '{platform}': {count} stores (e.g., {samples})")
        else:
            log.warning("❌ NO Foodpanda-like platforms found!")
        
        # 3. Check recent status checks for all platforms
        log.info("\n=== 3. RECENT STATUS CHECKS (LAST 24 HOURS) ===")
        cur.execute("""
            SELECT 
                s.platform,
                COUNT(*) as total_checks,
                COUNT(DISTINCT s.id) as unique_stores,
                MIN(sc.checked_at) as earliest_check,
                MAX(sc.checked_at) as latest_check,
                COUNT(*) FILTER (WHERE sc.is_online = true) as online_checks,
                COUNT(*) FILTER (WHERE sc.is_online = false) as offline_checks
            FROM stores s
            INNER JOIN status_checks sc ON s.id = sc.store_id
            WHERE sc.checked_at >= NOW() - INTERVAL '24 hours'
            GROUP BY s.platform
            ORDER BY total_checks DESC
        """)
        
        recent_checks = cur.fetchall()
        log.info(f"Status checks in last 24h across {len(recent_checks)} platforms:")
        for platform, total, stores, earliest, latest, online, offline in recent_checks:
            standardized = standardize_platform_name(platform)
            log.info(f"  '{platform}' -> '{standardized}': {total} checks on {stores} stores ({online} online, {offline} offline)")
            log.info(f"    Time range: {earliest} to {latest}")
        
        # 4. Check today's hourly snapshot data
        log.info("\n=== 4. TODAY'S HOURLY SNAPSHOT DATA ===")
        cur.execute("""
            SELECT 
                s.platform,
                COUNT(*) as hourly_records,
                COUNT(DISTINCT s.id) as unique_stores,
                COUNT(*) FILTER (WHERE ssh.status = 'ONLINE') as online_records,
                COUNT(*) FILTER (WHERE ssh.status = 'OFFLINE') as offline_records,
                COUNT(*) FILTER (WHERE ssh.status IN ('BLOCKED','UNKNOWN','ERROR')) as under_review_records,
                MIN(ssh.effective_at) as earliest_record,
                MAX(ssh.effective_at) as latest_record
            FROM stores s
            INNER JOIN store_status_hourly ssh ON s.id = ssh.store_id
            WHERE DATE(ssh.effective_at AT TIME ZONE 'Asia/Manila') 
                  = DATE(timezone('Asia/Manila', now()))
            GROUP BY s.platform
            ORDER BY hourly_records DESC
        """)
        
        today_hourly = cur.fetchall()
        if today_hourly:
            log.info(f"Today's hourly data across {len(today_hourly)} platforms:")
            for platform, records, stores, online, offline, review, earliest, latest in today_hourly:
                standardized = standardize_platform_name(platform)
                log.info(f"  '{platform}' -> '{standardized}': {records} records for {stores} stores")
                log.info(f"    Status: {online} online, {offline} offline, {review} under review")
                log.info(f"    Time range: {earliest} to {latest}")
        else:
            log.warning("❌ NO hourly snapshot data found for today!")
        
        # 5. Test the exact dashboard queries
        log.info("\n=== 5. TESTING EXACT DASHBOARD QUERIES ===")
        
        # Test daily uptime query (Store Uptime Analytics tab)
        log.info("\n--- Daily Uptime Query (Store Uptime Analytics) ---")
        cur.execute("""
            WITH today_hours AS (
              SELECT
                ssh.store_id,
                COUNT(*) FILTER (WHERE ssh.status IN ('ONLINE','OFFLINE')) AS effective_checks,
                SUM(CASE WHEN ssh.status = 'ONLINE'  THEN 1 ELSE 0 END) AS online_checks,
                SUM(CASE WHEN ssh.status = 'OFFLINE' THEN 1 ELSE 0 END) AS downtime_count,
                COUNT(*) FILTER (WHERE ssh.status IN ('BLOCKED','UNKNOWN','ERROR')) AS under_review_checks
              FROM store_status_hourly ssh
              WHERE DATE(ssh.effective_at AT TIME ZONE 'Asia/Manila')
                    = DATE(timezone('Asia/Manila', now()))
              GROUP BY ssh.store_id
            )
            SELECT
              s.id,
              COALESCE(s.name_override, s.name) AS name,
              s.platform,
              COALESCE(th.effective_checks, 0) AS effective_checks,
              COALESCE(th.online_checks, 0)    AS online_checks,
              COALESCE(th.downtime_count, 0)   AS downtime_count,
              COALESCE(th.under_review_checks,0) AS under_review_checks,
              CASE 
                WHEN COALESCE(th.effective_checks,0) = 0 THEN NULL
                ELSE ROUND( (th.online_checks * 100.0 / NULLIF(th.effective_checks,0)) , 1)
              END AS uptime_percentage
            FROM stores s
            LEFT JOIN today_hours th ON th.store_id = s.id
            WHERE COALESCE(th.effective_checks,0) > 0  -- This filters out stores with no data!
            ORDER BY uptime_percentage DESC NULLS LAST, name
        """)
        
        daily_uptime_results = cur.fetchall()
        platform_counts = {}
        for row in daily_uptime_results:
            platform = standardize_platform_name(row[2])
            platform_counts[platform] = platform_counts.get(platform, 0) + 1
        
        log.info(f"Daily uptime query results: {len(daily_uptime_results)} stores")
        for platform, count in platform_counts.items():
            log.info(f"  {platform}: {count} stores")
        
        if 'Foodpanda' not in platform_counts:
            log.warning("❌ NO Foodpanda stores in daily uptime results!")
        
        # Test downtime events query
        log.info("\n--- Downtime Events Query (Downtime Events Analysis) ---")
        cur.execute("""
            SELECT 
                s.name,
                s.platform,
                COUNT(*) FILTER (WHERE ssh.status = 'OFFLINE') AS downtime_events,
                MIN(ssh.effective_at) FILTER (WHERE ssh.status = 'OFFLINE') AS first_downtime,
                MAX(ssh.effective_at) FILTER (WHERE ssh.status = 'OFFLINE') AS last_downtime
            FROM stores s
            JOIN store_status_hourly ssh ON ssh.store_id = s.id
            WHERE DATE(ssh.effective_at AT TIME ZONE 'Asia/Manila')
                  = DATE(timezone('Asia/Manila', now()))
            GROUP BY s.id, s.name, s.platform
            HAVING COUNT(*) FILTER (WHERE ssh.status = 'OFFLINE') > 0
            ORDER BY downtime_events DESC
        """)
        
        downtime_results = cur.fetchall()
        downtime_platform_counts = {}
        for row in downtime_results:
            platform = standardize_platform_name(row[1])
            downtime_platform_counts[platform] = downtime_platform_counts.get(platform, 0) + 1
        
        log.info(f"Downtime events query results: {len(downtime_results)} stores with downtime")
        for platform, count in downtime_platform_counts.items():
            log.info(f"  {platform}: {count} stores with downtime")
        
        # 6. Check what would happen without the filter
        log.info("\n=== 6. DAILY UPTIME WITHOUT FILTER ===")
        cur.execute("""
            WITH today_hours AS (
              SELECT
                ssh.store_id,
                COUNT(*) FILTER (WHERE ssh.status IN ('ONLINE','OFFLINE')) AS effective_checks,
                SUM(CASE WHEN ssh.status = 'ONLINE'  THEN 1 ELSE 0 END) AS online_checks,
                SUM(CASE WHEN ssh.status = 'OFFLINE' THEN 1 ELSE 0 END) AS downtime_count
              FROM store_status_hourly ssh
              WHERE DATE(ssh.effective_at AT TIME ZONE 'Asia/Manila')
                    = DATE(timezone('Asia/Manila', now()))
              GROUP BY ssh.store_id
            )
            SELECT
              s.platform,
              COUNT(*) as total_stores,
              COUNT(*) FILTER (WHERE COALESCE(th.effective_checks,0) > 0) as stores_with_data,
              COUNT(*) FILTER (WHERE COALESCE(th.effective_checks,0) = 0) as stores_without_data
            FROM stores s
            LEFT JOIN today_hours th ON th.store_id = s.id
            GROUP BY s.platform
            ORDER BY total_stores DESC
        """)
        
        unfiltered_results = cur.fetchall()
        log.info("All stores (with and without today's data):")
        total_foodpanda = 0
        foodpanda_with_data = 0
        for platform, total, with_data, without_data in unfiltered_results:
            standardized = standardize_platform_name(platform)
            log.info(f"  '{platform}' -> '{standardized}': {total} total ({with_data} with data, {without_data} without)")
            if standardized == 'Foodpanda':
                total_foodpanda += total
                foodpanda_with_data += with_data
        
        log.info(f"\nFoodpanda summary: {total_foodpanda} total stores, {foodpanda_with_data} with today's data")
        
        # 7. Check specific Foodpanda stores and their latest status
        log.info("\n=== 7. FOODPANDA STORES DETAILED ANALYSIS ===")
        cur.execute("""
            SELECT 
                s.id,
                s.name,
                s.platform,
                s.url,
                s.created_at,
                s.last_checked,
                COALESCE(latest_check.checked_at, 'Never') as last_status_check,
                COALESCE(latest_check.is_online::text, 'Unknown') as last_online_status
            FROM stores s
            LEFT JOIN LATERAL (
                SELECT checked_at, is_online
                FROM status_checks sc
                WHERE sc.store_id = s.id
                ORDER BY checked_at DESC
                LIMIT 1
            ) latest_check ON true
            WHERE LOWER(s.platform) LIKE '%panda%' 
               OR LOWER(s.platform) LIKE '%food%'
            ORDER BY s.name
        """)
        
        foodpanda_stores = cur.fetchall()
        if foodpanda_stores:
            log.info(f"Found {len(foodpanda_stores)} Foodpanda stores:")
            for store_id, name, platform, url, created, last_checked, last_status, online_status in foodpanda_stores[:10]:  # Show first 10
                log.info(f"  ID {store_id}: {name}")
                log.info(f"    Platform: '{platform}' -> '{standardize_platform_name(platform)}'")
                log.info(f"    Created: {created}, Last checked: {last_checked}")
                log.info(f"    Last status check: {last_status} (online: {online_status})")
                log.info(f"    URL: {url}")
        else:
            log.error("❌ NO Foodpanda stores found in database!")
        
        # 8. Check branch_urls.json for comparison
        log.info("\n=== 8. BRANCH_URLS.JSON COMPARISON ===")
        try:
            with open("branch_urls.json", "r") as f:
                urls_data = json.load(f)
                urls = urls_data.get("urls", [])
            
            foodpanda_urls = [url for url in urls if "foodpanda" in url.lower()]
            grabfood_urls = [url for url in urls if "grab" in url.lower()]
            
            log.info(f"branch_urls.json contains:")
            log.info(f"  Foodpanda URLs: {len(foodpanda_urls)}")
            log.info(f"  GrabFood URLs: {len(grabfood_urls)}")
            log.info(f"  Total URLs: {len(urls)}")
            
            # Compare with database
            cur.execute("SELECT COUNT(*) FROM stores WHERE LOWER(platform) LIKE '%panda%'")
            db_foodpanda = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM stores WHERE LOWER(platform) LIKE '%grab%'")
            db_grabfood = cur.fetchone()[0]
            
            log.info(f"Database contains:")
            log.info(f"  Foodpanda stores: {db_foodpanda}")
            log.info(f"  GrabFood stores: {db_grabfood}")
            
            if len(foodpanda_urls) != db_foodpanda:
                log.warning(f"⚠️  MISMATCH: {len(foodpanda_urls)} Foodpanda URLs vs {db_foodpanda} DB records")
            
        except Exception as e:
            log.error(f"Error reading branch_urls.json: {e}")
        
        # 9. Final summary and recommendations
        log.info("\n=== 9. SUMMARY AND DIAGNOSIS ===")
        
        if total_foodpanda == 0:
            log.error("🚨 ROOT CAUSE: No Foodpanda stores exist in database!")
            log.error("   ACTION: Check if stores are being created with correct platform names")
        elif foodpanda_with_data == 0:
            log.error("🚨 ROOT CAUSE: Foodpanda stores exist but have NO monitoring data for today!")
            log.error("   ACTION: Check if monitoring is running for Foodpanda stores")
        elif foodpanda_with_data > 0:
            log.info("✅ Foodpanda stores exist and have today's data")
            log.info("   If they're not showing in dashboard, check the frontend filtering logic")
        
        # Check the exact filter condition
        if foodpanda_with_data == 0 and total_foodpanda > 0:
            log.info("\n🔧 QUICK FIX: Remove the filter 'WHERE COALESCE(th.effective_checks,0) > 0'")
            log.info("   from the daily uptime query to show all stores, even without today's data")

def main():
    log.info("🔍 Debugging Foodpanda Database Issues")
    log.info("=" * 60)
    
    try:
        debug_foodpanda_database()
    except Exception as e:
        log.error(f"Debug failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()