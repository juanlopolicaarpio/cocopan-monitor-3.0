#!/usr/bin/env python3
"""
Safely Merge Duplicate Stores
1. Moves all data from duplicate IDs to the kept ID
2. Then deletes the duplicate store entries
3. No data loss!
"""

import os
import sys
from datetime import datetime
from collections import defaultdict
import json

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from database import db

print("\n" + "="*80)
print("SAFE DUPLICATE STORE MERGER")
print("="*80)
print("This will merge all data before deleting duplicates\n")

try:
    # Load current URLs from JSON
    current_urls = set()
    try:
        with open('branch_urls.json', 'r') as f:
            data = json.load(f)
            current_urls = set(data.get('urls', []))
        print(f"✅ Loaded {len(current_urls)} URLs from branch_urls.json\n")
    except Exception as e:
        print(f"❌ Could not load branch_urls.json: {e}")
        sys.exit(1)
    
    with db.get_connection() as conn:
        cur = conn.cursor()
        
        # Get all stores
        if db.db_type == "postgresql":
            cur.execute("""
                SELECT id, name, url, platform, created_at
                FROM stores
                ORDER BY name, created_at
            """)
        else:
            cur.execute("""
                SELECT id, name, url, platform, created_at
                FROM stores
                ORDER BY name, created_at
            """)
        
        all_stores = cur.fetchall()
        
        # Group by normalized name
        stores_by_name = defaultdict(list)
        
        for store in all_stores:
            if db.db_type == "postgresql":
                store_id, name, url, platform, created_at = store
            else:
                store_id = store['id']
                name = store['name']
                url = store['url']
                platform = store['platform']
                created_at = store['created_at']
            
            normalized_name = ' '.join(name.lower().strip().split())
            
            stores_by_name[normalized_name].append({
                'id': store_id,
                'name': name,
                'url': url,
                'platform': platform,
                'created_at': created_at
            })
        
        # Find duplicates
        duplicates = {name: stores for name, stores in stores_by_name.items() if len(stores) > 1}
        
        if not duplicates:
            print("✅ No duplicates found - nothing to merge!")
            sys.exit(0)
        
        print(f"🔍 Found {len(duplicates)} store names with duplicates\n")
        
        # Plan the merges
        merge_plan = []
        
        for name, stores in sorted(duplicates.items()):
            # Determine which to keep
            in_json_stores = [s for s in stores if s['url'] in current_urls]
            
            if len(in_json_stores) == 1:
                keep = in_json_stores[0]
            elif len(in_json_stores) > 1:
                keep = min(in_json_stores, key=lambda s: s['created_at'])
            else:
                keep = min(stores, key=lambda s: s['created_at'])
            
            delete_ids = [s['id'] for s in stores if s['id'] != keep['id']]
            
            merge_plan.append({
                'keep_id': keep['id'],
                'keep_name': keep['name'],
                'keep_url': keep['url'],
                'delete_ids': delete_ids,
                'delete_stores': [s for s in stores if s['id'] in delete_ids]
            })
        
        # Show merge plan
        print("="*80)
        print("MERGE PLAN:")
        print("="*80)
        
        for idx, plan in enumerate(merge_plan, 1):
            print(f"\n{idx}. {plan['keep_name']}")
            print(f"   ✅ KEEP: ID {plan['keep_id']} - {plan['keep_url']}")
            print(f"   🔄 MERGE & DELETE:")
            
            for dup_store in plan['delete_stores']:
                # Check how much data will be merged
                if db.db_type == "postgresql":
                    cur.execute("SELECT COUNT(*) FROM status_checks WHERE store_id = %s", (dup_store['id'],))
                    status_count = cur.fetchone()[0]
                    
                    cur.execute("SELECT COUNT(*) FROM store_sku_checks WHERE store_id = %s", (dup_store['id'],))
                    sku_count = cur.fetchone()[0]
                else:
                    cur.execute("SELECT COUNT(*) FROM status_checks WHERE store_id = ?", (dup_store['id'],))
                    status_count = cur.fetchone()[0]
                    
                    cur.execute("SELECT COUNT(*) FROM store_sku_checks WHERE store_id = ?", (dup_store['id'],))
                    sku_count = cur.fetchone()[0]
                
                print(f"      ID {dup_store['id']}: {status_count} status checks, {sku_count} SKU checks")
        
        total_deletes = sum(len(p['delete_ids']) for p in merge_plan)
        print(f"\n📊 Total: Will merge data from {total_deletes} duplicate entries")
        
        # Ask for confirmation
        print("\n" + "="*80)
        print("READY TO MERGE")
        print("="*80)
        print("\nThis will:")
        print("1. Move all status_checks data to the kept store ID")
        print("2. Move all store_sku_checks data to the kept store ID")
        print("3. Delete the duplicate store entries")
        print("4. NO DATA WILL BE LOST")
        
        confirm = input("\nType 'MERGE' to proceed: ").strip()
        
        if confirm != 'MERGE':
            print("\n❌ Cancelled - no changes made")
            sys.exit(0)
        
        # Execute merges
        print("\n" + "="*80)
        print("MERGING DATA...")
        print("="*80)
        
        total_status_moved = 0
        total_sku_moved = 0
        total_deleted = 0
        
        for plan in merge_plan:
            keep_id = plan['keep_id']
            
            for delete_id in plan['delete_ids']:
                print(f"\n📦 Merging ID {delete_id} → ID {keep_id} ({plan['keep_name']})")
                
                try:
                    # Move status_checks
                    if db.db_type == "postgresql":
                        cur.execute("""
                            UPDATE status_checks 
                            SET store_id = %s 
                            WHERE store_id = %s
                        """, (keep_id, delete_id))
                        status_moved = cur.rowcount
                        
                        # Move store_sku_checks
                        cur.execute("""
                            UPDATE store_sku_checks 
                            SET store_id = %s 
                            WHERE store_id = %s
                        """, (keep_id, delete_id))
                        sku_moved = cur.rowcount
                        
                        # Move store_status_hourly (if exists)
                        cur.execute("""
                            UPDATE store_status_hourly 
                            SET store_id = %s 
                            WHERE store_id = %s
                        """, (keep_id, delete_id))
                        
                    else:
                        cur.execute("""
                            UPDATE status_checks 
                            SET store_id = ? 
                            WHERE store_id = ?
                        """, (keep_id, delete_id))
                        status_moved = cur.rowcount
                        
                        cur.execute("""
                            UPDATE store_sku_checks 
                            SET store_id = ? 
                            WHERE store_id = ?
                        """, (keep_id, delete_id))
                        sku_moved = cur.rowcount
                        
                        cur.execute("""
                            UPDATE store_status_hourly 
                            SET store_id = ? 
                            WHERE store_id = ?
                        """, (keep_id, delete_id))
                    
                    print(f"   ✅ Moved {status_moved} status checks")
                    print(f"   ✅ Moved {sku_moved} SKU checks")
                    
                    total_status_moved += status_moved
                    total_sku_moved += sku_moved
                    
                    # Now safe to delete the duplicate store entry
                    if db.db_type == "postgresql":
                        cur.execute("DELETE FROM stores WHERE id = %s", (delete_id,))
                    else:
                        cur.execute("DELETE FROM stores WHERE id = ?", (delete_id,))
                    
                    print(f"   🗑️  Deleted store entry ID {delete_id}")
                    total_deleted += 1
                    
                except Exception as e:
                    print(f"   ❌ Error merging ID {delete_id}: {e}")
                    raise
        
        conn.commit()
        
        print("\n" + "="*80)
        print("✅ MERGE COMPLETE!")
        print("="*80)
        print(f"\n📊 Summary:")
        print(f"   Status checks moved: {total_status_moved}")
        print(f"   SKU checks moved: {total_sku_moved}")
        print(f"   Duplicate stores deleted: {total_deleted}")
        print(f"\n✅ ALL DATA PRESERVED - No data loss!")
        print(f"✅ Database cleaned - No more duplicates")

except Exception as e:
    print(f"\n❌ Error: {e}")
    import traceback
    traceback.print_exc()
    print("\n⚠️  NO CHANGES MADE - Database rolled back")

print("\n" + "="*80)
print("Done!")
print("="*80 + "\n")