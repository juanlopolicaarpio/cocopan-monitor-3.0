#!/usr/bin/env python3
"""
Merge ONLY GrabFood Duplicates - Leaves Foodpanda Untouched
"""

import os
import sys
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from database import db

print("\n" + "="*80)
print("GRABFOOD-ONLY DUPLICATE MERGER")
print("="*80)
print("This will ONLY merge GrabFood duplicates")
print("Foodpanda stores will NOT be touched\n")

try:
    with db.get_connection() as conn:
        cur = conn.cursor()
        
        # Get ONLY GrabFood stores
        if db.db_type == "postgresql":
            cur.execute("""
                SELECT id, name, url, created_at
                FROM stores
                WHERE platform = 'grabfood'
                ORDER BY name, created_at
            """)
        else:
            cur.execute("""
                SELECT id, name, url, created_at
                FROM stores
                WHERE platform = 'grabfood'
                ORDER BY name, created_at
            """)
        
        grabfood_stores = cur.fetchall()
        
        print(f"Found {len(grabfood_stores)} GrabFood stores\n")
        
        # Group by normalized name
        stores_by_name = defaultdict(list)
        
        for store in grabfood_stores:
            if db.db_type == "postgresql":
                store_data = {
                    'id': store[0],
                    'name': store[1],
                    'url': store[2],
                    'created_at': store[3]
                }
            else:
                store_data = {
                    'id': store['id'],
                    'name': store['name'],
                    'url': store['url'],
                    'created_at': store['created_at']
                }
            
            # Normalize: remove dashes, spaces, lowercase
            normalized = store_data['name'].lower().replace('-', '').replace(' ', '')
            stores_by_name[normalized].append(store_data)
        
        # Find duplicates
        duplicates = {name: stores for name, stores in stores_by_name.items() 
                     if len(stores) > 1}
        
        if not duplicates:
            print("✅ No GrabFood duplicates found!")
            sys.exit(0)
        
        print(f"Found {len(duplicates)} GrabFood duplicate groups\n")
        
        # Show what will be merged
        print("="*80)
        print("MERGE PLAN:")
        print("="*80)
        
        merge_plan = []
        
        for name, stores in duplicates.items():
            # Keep oldest
            keep = min(stores, key=lambda s: s['created_at'])
            delete_ids = [s['id'] for s in stores if s['id'] != keep['id']]
            
            print(f"\nStore: {keep['name']}")
            print(f"  ✅ KEEP: ID {keep['id']} (created {keep['created_at']})")
            
            for dup in stores:
                if dup['id'] in delete_ids:
                    # Count data
                    if db.db_type == "postgresql":
                        cur.execute("SELECT COUNT(*) FROM status_checks WHERE store_id = %s", 
                                  (dup['id'],))
                        status_count = cur.fetchone()[0]
                    else:
                        cur.execute("SELECT COUNT(*) FROM status_checks WHERE store_id = ?", 
                                  (dup['id'],))
                        status_count = cur.fetchone()[0]
                    
                    print(f"  🔄 MERGE: ID {dup['id']} ({status_count} checks)")
            
            merge_plan.append({
                'keep_id': keep['id'],
                'keep_name': keep['name'],
                'delete_ids': delete_ids
            })
        
        # Confirm
        print("\n" + "="*80)
        print("READY TO MERGE GRABFOOD DUPLICATES")
        print("="*80)
        print("\nThis will:")
        print("1. Move all status_checks from duplicate IDs to kept IDs")
        print("2. Move all store_sku_checks from duplicate IDs to kept IDs")
        print("3. Move all store_status_hourly from duplicate IDs to kept IDs")
        print("4. Delete empty duplicate GrabFood store entries")
        print("\n⚠️  FOODPANDA STORES WILL NOT BE TOUCHED")
        
        confirm = input("\nType 'MERGE' to proceed: ").strip()
        
        if confirm != 'MERGE':
            print("\n❌ Cancelled")
            sys.exit(0)
        
        # Execute merges
        print("\n" + "="*80)
        print("MERGING...")
        print("="*80)
        
        total_moved = 0
        total_deleted = 0
        
        for plan in merge_plan:
            keep_id = plan['keep_id']
            
            for delete_id in plan['delete_ids']:
                print(f"\n🔄 Merging ID {delete_id} → ID {keep_id}")
                
                try:
                    # Move status_checks
                    if db.db_type == "postgresql":
                        cur.execute("UPDATE status_checks SET store_id = %s WHERE store_id = %s", 
                                  (keep_id, delete_id))
                        moved = cur.rowcount
                        
                        cur.execute("UPDATE store_sku_checks SET store_id = %s WHERE store_id = %s", 
                                  (keep_id, delete_id))
                        
                        cur.execute("UPDATE store_status_hourly SET store_id = %s WHERE store_id = %s", 
                                  (keep_id, delete_id))
                    else:
                        cur.execute("UPDATE status_checks SET store_id = ? WHERE store_id = ?", 
                                  (keep_id, delete_id))
                        moved = cur.rowcount
                        
                        cur.execute("UPDATE store_sku_checks SET store_id = ? WHERE store_id = ?", 
                                  (keep_id, delete_id))
                        
                        cur.execute("UPDATE store_status_hourly SET store_id = ? WHERE store_id = ?", 
                                  (keep_id, delete_id))
                    
                    print(f"   ✅ Moved {moved} checks")
                    total_moved += moved
                    
                    # Delete empty duplicate
                    if db.db_type == "postgresql":
                        cur.execute("DELETE FROM stores WHERE id = %s", (delete_id,))
                    else:
                        cur.execute("DELETE FROM stores WHERE id = ?", (delete_id,))
                    
                    total_deleted += 1
                    print(f"   🗑️  Deleted store ID {delete_id}")
                    
                except Exception as e:
                    print(f"   ❌ Error: {e}")
                    raise
        
        conn.commit()
        
        print("\n" + "="*80)
        print("✅ MERGE COMPLETE!")
        print("="*80)
        print(f"\nMoved {total_moved} checks")
        print(f"Deleted {total_deleted} duplicate GrabFood stores")
        print("\n⚠️  Foodpanda data was NOT touched")

except Exception as e:
    print(f"\n❌ Error: {e}")
    import traceback
    traceback.print_exc()

print("\n" + "="*80)
print("Done!")
print("="*80 + "\n")