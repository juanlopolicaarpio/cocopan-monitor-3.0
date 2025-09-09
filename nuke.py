#!/usr/bin/env python3
"""
Debug VA Check-in Data - Why admin dashboard doesn't reflect submitted changes
"""
import os
import sys
import logging
from typing import List
from datetime import datetime, timedelta
import pytz

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
log = logging.getLogger("va_debug")

try:
    from database import db  # your existing connection helper
except Exception as e:
    log.error("Failed to import database.db helper: %s", e)
    sys.exit(1)

def get_manila_time():
    """Get current Manila time"""
    manila_tz = pytz.timezone('Asia/Manila')
    return datetime.now(manila_tz)

def debug_va_data():
    """Debug VA check-in data and admin dashboard logic"""
    
    with db.get_connection() as conn:
        cur = conn.cursor()
        
        # 1. Check current Manila time and hour
        current_manila = get_manila_time()
        current_hour_slot = current_manila.replace(minute=0, second=0, microsecond=0)
        
        log.info("=== CURRENT TIME DEBUG ===")
        log.info(f"Current Manila time: {current_manila}")
        log.info(f"Current hour slot: {current_hour_slot}")
        
        # 2. Check what VA data exists for current hour
        log.info("\n=== VA DATA FOR CURRENT HOUR ===")
        cur.execute("""
            SELECT 
                s.name,
                sc.is_online,
                sc.checked_at,
                sc.error_message
            FROM stores s
            INNER JOIN status_checks sc ON s.id = sc.store_id
            WHERE s.platform = 'foodpanda'
              AND sc.error_message LIKE '[VA_CHECKIN]%%'
              AND sc.checked_at >= %s
              AND sc.checked_at < %s + INTERVAL '1 hour'
            ORDER BY s.name
        """, (current_hour_slot, current_hour_slot))
        
        va_current_hour = cur.fetchall()
        log.info(f"VA checks in current hour ({current_hour_slot:%H:00}): {len(va_current_hour)}")
        
        if va_current_hour:
            online_count = sum(1 for row in va_current_hour if row[1])
            offline_count = len(va_current_hour) - online_count
            log.info(f"  Online: {online_count}, Offline: {offline_count}")
            
            # Show first few examples
            for i, (name, is_online, checked_at, error_msg) in enumerate(va_current_hour[:5]):
                status = "ONLINE" if is_online else "OFFLINE"
                log.info(f"  {i+1}. {name}: {status} at {checked_at}")
        else:
            log.warning("  NO VA checks found for current hour!")
        
        # 3. Check the admin dashboard logic for "hour completed"
        log.info("\n=== ADMIN DASHBOARD HOUR COMPLETION CHECK ===")
        cur.execute("""
            SELECT COUNT(*) as va_count
            FROM status_checks
            WHERE error_message LIKE '[VA_CHECKIN]%%'
              AND checked_at >= %s
              AND checked_at < %s + INTERVAL '1 hour'
        """, (current_hour_slot, current_hour_slot))
        
        va_count = cur.fetchone()[0]
        hour_completed = va_count > 0
        
        log.info(f"Hour completion check: {hour_completed} (VA count: {va_count})")
        
        # 4. Check latest status for each Foodpanda store (what main dashboard sees)
        log.info("\n=== LATEST STATUS PER STORE (MAIN DASHBOARD VIEW) ===")
        cur.execute("""
            WITH ranked_checks AS (
                SELECT 
                    s.id,
                    s.name,
                    sc.is_online,
                    sc.checked_at,
                    sc.error_message,
                    CASE 
                        WHEN s.platform = 'foodpanda' AND sc.error_message LIKE '[VA_CHECKIN]%%' 
                             AND sc.checked_at >= NOW() - INTERVAL '2 hours' THEN 1
                        WHEN s.platform = 'foodpanda' AND sc.checked_at >= NOW() - INTERVAL '1 hour' THEN 2
                        ELSE 3
                    END as priority,
                    ROW_NUMBER() OVER (
                        PARTITION BY s.id 
                        ORDER BY 
                            CASE 
                                WHEN s.platform = 'foodpanda' AND sc.error_message LIKE '[VA_CHECKIN]%%' 
                                     AND sc.checked_at >= NOW() - INTERVAL '2 hours' THEN 1
                                WHEN s.platform = 'foodpanda' AND sc.checked_at >= NOW() - INTERVAL '1 hour' THEN 2
                                ELSE 3
                            END,
                            sc.checked_at DESC
                    ) as rn
                FROM stores s
                INNER JOIN status_checks sc ON s.id = sc.store_id
                WHERE s.platform = 'foodpanda'
                  AND sc.checked_at >= NOW() - INTERVAL '24 hours'
            )
            SELECT 
                name,
                is_online,
                checked_at,
                CASE WHEN error_message LIKE '[VA_CHECKIN]%%' THEN 'VA' ELSE 'AUTO' END as check_type,
                priority
            FROM ranked_checks 
            WHERE rn = 1
            ORDER BY name
        """)
        
        latest_status = cur.fetchall()
        va_latest = sum(1 for row in latest_status if row[3] == 'VA')
        auto_latest = len(latest_status) - va_latest
        
        online_latest = sum(1 for row in latest_status if row[1])
        offline_latest = len(latest_status) - online_latest
        
        log.info(f"Latest status for {len(latest_status)} Foodpanda stores:")
        log.info(f"  VA checks: {va_latest}, Auto checks: {auto_latest}")
        log.info(f"  Online: {online_latest}, Offline: {offline_latest}")
        
        # 5. Check what happens when admin dashboard loads stores
        log.info("\n=== ADMIN DASHBOARD STORE LOADING ===")
        try:
            with open("branch_urls.json", "r") as f:
                import json
                data = json.load(f)
                urls = data.get("urls", [])
                
            foodpanda_urls = [url for url in urls if "foodpanda" in url]
            log.info(f"Foodpanda URLs in branch_urls.json: {len(foodpanda_urls)}")
            
            # Check how many of these have store records
            cur.execute("""
                SELECT COUNT(*) 
                FROM stores 
                WHERE platform = 'foodpanda'
            """)
            fp_stores_in_db = cur.fetchone()[0]
            log.info(f"Foodpanda stores in database: {fp_stores_in_db}")
            
            if len(foodpanda_urls) != fp_stores_in_db:
                log.warning(f"MISMATCH: {len(foodpanda_urls)} URLs vs {fp_stores_in_db} DB records")
                
        except Exception as e:
            log.error(f"Error checking branch_urls.json: {e}")
        
        # 6. Check admin dashboard session state simulation
        log.info("\n=== ADMIN DASHBOARD SESSION STATE DEBUG ===")
        
        # Simulate what admin dashboard does to check completion
        cur.execute("""
            SELECT 1
            FROM status_checks
            WHERE error_message LIKE '[VA_CHECKIN]%%'
              AND checked_at >= %s
              AND checked_at < %s + INTERVAL '1 hour'
            LIMIT 1
        """, (current_hour_slot, current_hour_slot))
        
        hour_check_result = cur.fetchone()
        simulated_completion = hour_check_result is not None
        
        log.info(f"Simulated admin dashboard hour completion: {simulated_completion}")
        
        if simulated_completion != hour_completed:
            log.error("INCONSISTENCY in hour completion logic!")
        
        # 7. Final summary
        log.info("\n=== SUMMARY ===")
        if va_current_hour and not simulated_completion:
            log.error("BUG: VA data exists but admin dashboard thinks hour not completed!")
        elif not va_current_hour and simulated_completion:
            log.error("BUG: Admin dashboard thinks completed but no VA data found!")
        elif va_current_hour and simulated_completion:
            log.info("✅ VA data exists AND admin dashboard should show as completed")
            log.info("   If admin UI still shows 'not submitted', the issue is in the UI refresh logic")
        else:
            log.info("❌ No VA data found - hour genuinely not completed")

def main():
    log.info("🔍 Debugging VA Check-in Data vs Admin Dashboard Display")
    log.info("=" * 60)
    
    try:
        debug_va_data()
    except Exception as e:
        log.error(f"Debug failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()