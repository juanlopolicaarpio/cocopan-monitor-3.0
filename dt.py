#!/usr/bin/env python3
"""
Audit All Status Checks in Database
Shows what data exists and identifies any gaps
"""

import os
import sys
import csv
from datetime import datetime, timedelta
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from database import db

print("\n" + "="*80)
print("STATUS CHECKS AUDIT")
print("="*80)

try:
    with db.get_connection() as conn:
        cur = conn.cursor()
        
        # Get total status checks
        cur.execute("SELECT COUNT(*) FROM status_checks")
        total_checks = cur.fetchone()[0]
        
        # Get date range of checks
        if db.db_type == "postgresql":
            cur.execute("""
                SELECT MIN(checked_at), MAX(checked_at), COUNT(DISTINCT DATE(checked_at))
                FROM status_checks
            """)
        else:
            cur.execute("""
                SELECT MIN(checked_at), MAX(checked_at), COUNT(DISTINCT DATE(checked_at))
                FROM status_checks
            """)
        
        min_date, max_date, unique_days = cur.fetchone()
        
        print(f"\nTotal status checks in database: {total_checks:,}")
        print(f"Date range: {min_date} to {max_date}")
        print(f"Unique days with data: {unique_days}")
        
        # Checks per store
        print("\n" + "="*80)
        print("STATUS CHECKS BY STORE")
        print("="*80)
        
        if db.db_type == "postgresql":
            cur.execute("""
                SELECT 
                    s.id,
                    s.name,
                    s.platform,
                    COUNT(sc.id) as check_count,
                    MIN(sc.checked_at) as first_check,
                    MAX(sc.checked_at) as last_check
                FROM stores s
                LEFT JOIN status_checks sc ON s.id = sc.store_id
                GROUP BY s.id, s.name, s.platform
                ORDER BY check_count DESC, s.name
            """)
        else:
            cur.execute("""
                SELECT 
                    s.id,
                    s.name,
                    s.platform,
                    COUNT(sc.id) as check_count,
                    MIN(sc.checked_at) as first_check,
                    MAX(sc.checked_at) as last_check
                FROM stores s
                LEFT JOIN status_checks sc ON s.id = sc.store_id
                GROUP BY s.id, s.name, s.platform
                ORDER BY check_count DESC, s.name
            """)
        
        stores_data = cur.fetchall()
        
        # Export to CSV
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"status_checks_audit_{timestamp}.csv"
        
        csv_data = []
        stores_with_no_checks = []
        
        print(f"\n{'Store ID':<10} {'Store Name':<40} {'Platform':<12} {'Check Count':<12} {'First Check':<20} {'Last Check'}")
        print("-" * 140)
        
        for store in stores_data:
            if db.db_type == "postgresql":
                store_id, name, platform, check_count, first_check, last_check = store
            else:
                store_id = store['id']
                name = store['name']
                platform = store['platform']
                check_count = store['check_count']
                first_check = store['first_check']
                last_check = store['last_check']
            
            # Truncate long names
            display_name = name[:38] + '..' if len(name) > 40 else name
            
            first_str = str(first_check)[:19] if first_check else "Never"
            last_str = str(last_check)[:19] if last_check else "Never"
            
            print(f"{store_id:<10} {display_name:<40} {platform:<12} {check_count:<12} {first_str:<20} {last_str}")
            
            if check_count == 0:
                stores_with_no_checks.append(name)
            
            csv_data.append({
                'Store_ID': store_id,
                'Store_Name': name,
                'Platform': platform,
                'Check_Count': check_count,
                'First_Check': first_str,
                'Last_Check': last_str
            })
        
        # Export to CSV
        with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = ['Store_ID', 'Store_Name', 'Platform', 'Check_Count', 'First_Check', 'Last_Check']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            for row in csv_data:
                writer.writerow(row)
        
        # Summary statistics
        print("\n" + "="*80)
        print("SUMMARY STATISTICS")
        print("="*80)
        
        grabfood_checks = sum(row['Check_Count'] for row in csv_data if row['Platform'] == 'grabfood')
        foodpanda_checks = sum(row['Check_Count'] for row in csv_data if row['Platform'] == 'foodpanda')
        
        grabfood_stores = sum(1 for row in csv_data if row['Platform'] == 'grabfood')
        foodpanda_stores = sum(1 for row in csv_data if row['Platform'] == 'foodpanda')
        
        print(f"\nGrabFood:")
        print(f"  Stores: {grabfood_stores}")
        print(f"  Total checks: {grabfood_checks:,}")
        print(f"  Avg checks per store: {grabfood_checks/max(1, grabfood_stores):.0f}")
        
        print(f"\nFoodpanda:")
        print(f"  Stores: {foodpanda_stores}")
        print(f"  Total checks: {foodpanda_checks:,}")
        print(f"  Avg checks per store: {foodpanda_checks/max(1, foodpanda_stores):.0f}")
        
        if stores_with_no_checks:
            print(f"\nStores with NO status checks: {len(stores_with_no_checks)}")
            for name in stores_with_no_checks[:10]:
                print(f"  - {name}")
            if len(stores_with_no_checks) > 10:
                print(f"  ... and {len(stores_with_no_checks) - 10} more")
        
        # Check for orphaned status_checks (checks pointing to non-existent stores)
        print("\n" + "="*80)
        print("ORPHANED CHECKS (checks with invalid store_id)")
        print("="*80)
        
        if db.db_type == "postgresql":
            cur.execute("""
                SELECT sc.store_id, COUNT(*) as orphan_count
                FROM status_checks sc
                LEFT JOIN stores s ON sc.store_id = s.id
                WHERE s.id IS NULL
                GROUP BY sc.store_id
            """)
        else:
            cur.execute("""
                SELECT sc.store_id, COUNT(*) as orphan_count
                FROM status_checks sc
                LEFT JOIN stores s ON sc.store_id = s.id
                WHERE s.id IS NULL
                GROUP BY sc.store_id
            """)
        
        orphaned = cur.fetchall()
        
        if orphaned:
            print(f"\nWARNING: Found orphaned status checks!")
            for row in orphaned:
                if db.db_type == "postgresql":
                    print(f"  Store ID {row[0]}: {row[1]} orphaned checks")
                else:
                    print(f"  Store ID {row['store_id']}: {row['orphan_count']} orphaned checks")
        else:
            print("\nNo orphaned checks found - all checks link to valid stores")
        
        print(f"\nExported detailed report to: {filename}")

except Exception as e:
    print(f"\nError: {e}")
    import traceback
    traceback.print_exc()

print("\n" + "="*80)
print("Done!")
print("="*80 + "\n")