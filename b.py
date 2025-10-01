#!/usr/bin/env python3
"""
Diagnostic Script: List all stores and their related data counts

- Reads all stores from DB
- For each store, shows:
    - Store ID, Name, URL, Platform
    - Row counts in status_checks, store_sku_checks, store_status_hourly
- Exports to CSV for easy review
- Read-only (no changes made)
"""

import os
import sys
import csv
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from database import db

OUTPUT_FILE = "store_diagnostic.csv"

print("\n" + "="*80)
print("DIAGNOSTIC: STORES + DATA COUNTS (READ ONLY)")
print("="*80)

try:
    with db.get_connection() as conn:
        cur = conn.cursor()

        # Fetch all stores
        cur.execute("""
            SELECT id, name, url, platform, created_at
            FROM stores
            ORDER BY platform, name, created_at
        """)
        stores = cur.fetchall()

        print(f"\n✅ Found {len(stores)} stores in database\n")

        # Open CSV for writing
        with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow([
                "store_id", "name", "platform", "url", "created_at",
                "status_checks_count", "store_sku_checks_count", "store_status_hourly_count"
            ])

            for store in stores:
                store_id, name, url, platform, created_at = store

                # Count related data
                cur.execute("SELECT COUNT(*) FROM status_checks WHERE store_id = %s", (store_id,))
                status_count = cur.fetchone()[0]

                cur.execute("SELECT COUNT(*) FROM store_sku_checks WHERE store_id = %s", (store_id,))
                sku_count = cur.fetchone()[0]

                cur.execute("SELECT COUNT(*) FROM store_status_hourly WHERE store_id = %s", (store_id,))
                hourly_count = cur.fetchone()[0]

                # Write to CSV
                writer.writerow([
                    store_id, name, platform, url, created_at,
                    status_count, sku_count, hourly_count
                ])

                # Print brief summary to console
                print("-"*80)
                print(f"Store ID: {store_id}")
                print(f"Name    : {name}")
                print(f"Platform: {platform}")
                print(f"URL     : {url}")
                print(f"  → {status_count} status_checks")
                print(f"  → {sku_count} store_sku_checks")
                print(f"  → {hourly_count} store_status_hourly")

        print("\n" + "="*80)
        print(f"✅ Done. Results exported to {OUTPUT_FILE}")
        print("="*80 + "\n")

except Exception as e:
    print(f"\n❌ Error: {e}")
    import traceback
    traceback.print_exc()
