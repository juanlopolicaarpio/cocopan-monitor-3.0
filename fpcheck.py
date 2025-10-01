#!/usr/bin/env python3
"""
Check Foodpanda Stores for Duplicates
Shows both GrabFood and Foodpanda data separately
"""

import os
import sys
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from database import db

print("\n" + "="*80)
print("FOODPANDA & GRABFOOD DUPLICATE CHECK")
print("="*80)

try:
    with db.get_connection() as conn:
        cur = conn.cursor()
        
        # Get all stores grouped by platform
        if db.db_type == "postgresql":
            cur.execute("""
                SELECT id, name, url, platform, created_at
                FROM stores
                ORDER BY platform, name, created_at
            """)
        else:
            cur.execute("""
                SELECT id, name, url, platform, created_at
                FROM stores
                ORDER BY platform, name, created_at
            """)
        
        all_stores = cur.fetchall()
        
        if not all_stores:
            print("No stores found in database.")
            sys.exit(0)
        
        # Separate by platform
        grabfood_stores = []
        foodpanda_stores = []
        
        for store in all_stores:
            if db.db_type == "postgresql":
                store_data = {
                    'id': store[0],
                    'name': store[1],
                    'url': store[2],
                    'platform': store[3],
                    'created_at': store[4]
                }
            else:
                store_data = {
                    'id': store['id'],
                    'name': store['name'],
                    'url': store['url'],
                    'platform': store['platform'],
                    'created_at': store['created_at']
                }
            
            if store_data['platform'] == 'foodpanda':
                foodpanda_stores.append(store_data)
            else:
                grabfood_stores.append(store_data)
        
        print(f"\nTotal stores in database: {len(all_stores)}")
        print(f"  GrabFood: {len(grabfood_stores)}")
        print(f"  Foodpanda: {len(foodpanda_stores)}")
        
        # Check for duplicates in each platform
        def check_duplicates(stores, platform_name):
            print(f"\n{'='*80}")
            print(f"{platform_name.upper()} STORES:")
            print('='*80)
            
            if not stores:
                print(f"No {platform_name} stores found.")
                return
            
            # Group by normalized name
            stores_by_name = defaultdict(list)
            
            for store in stores:
                normalized_name = ' '.join(store['name'].lower().strip().split())
                stores_by_name[normalized_name].append(store)
            
            # Find duplicates
            duplicates = {name: stores for name, stores in stores_by_name.items() 
                         if len(stores) > 1}
            
            print(f"\nTotal {platform_name} stores: {len(stores)}")
            print(f"Unique store names: {len(stores_by_name)}")
            
            if duplicates:
                print(f"Store names with duplicates: {len(duplicates)}")
                print(f"\nDUPLICATE DETAILS:")
                print('-'*80)
                
                for idx, (name, dup_stores) in enumerate(sorted(duplicates.items()), 1):
                    print(f"\n{idx}. {dup_stores[0]['name']}")
                    print(f"   Found {len(dup_stores)} entries:")
                    
                    for i, store in enumerate(dup_stores, 1):
                        # Check data count
                        if db.db_type == "postgresql":
                            cur.execute("SELECT COUNT(*) FROM status_checks WHERE store_id = %s", 
                                      (store['id'],))
                            status_count = cur.fetchone()[0]
                            
                            cur.execute("SELECT COUNT(*) FROM store_sku_checks WHERE store_id = %s", 
                                      (store['id'],))
                            sku_count = cur.fetchone()[0]
                        else:
                            cur.execute("SELECT COUNT(*) FROM status_checks WHERE store_id = ?", 
                                      (store['id'],))
                            status_count = cur.fetchone()[0]
                            
                            cur.execute("SELECT COUNT(*) FROM store_sku_checks WHERE store_id = ?", 
                                      (store['id'],))
                            sku_count = cur.fetchone()[0]
                        
                        print(f"\n   Entry {i}:")
                        print(f"      ID: {store['id']}")
                        print(f"      URL: {store['url']}")
                        print(f"      Created: {store['created_at']}")
                        print(f"      Status checks: {status_count}")
                        print(f"      SKU checks: {sku_count}")
            else:
                print(f"\nNo duplicates found for {platform_name}!")
            
            # Show all stores
            print(f"\n\nALL {platform_name.upper()} STORES:")
            print('-'*80)
            print(f"{'ID':<6} {'Name':<40} {'Status Checks':<15} {'SKU Checks'}")
            print('-'*80)
            
            for store in sorted(stores, key=lambda x: x['name']):
                # Get data counts
                if db.db_type == "postgresql":
                    cur.execute("SELECT COUNT(*) FROM status_checks WHERE store_id = %s", 
                              (store['id'],))
                    status_count = cur.fetchone()[0]
                    
                    cur.execute("SELECT COUNT(*) FROM store_sku_checks WHERE store_id = %s", 
                              (store['id'],))
                    sku_count = cur.fetchone()[0]
                else:
                    cur.execute("SELECT COUNT(*) FROM status_checks WHERE store_id = ?", 
                              (store['id'],))
                    status_count = cur.fetchone()[0]
                    
                    cur.execute("SELECT COUNT(*) FROM store_sku_checks WHERE store_id = ?", 
                              (store['id'],))
                    sku_count = cur.fetchone()[0]
                
                # Truncate long names
                display_name = store['name'][:38] + '..' if len(store['name']) > 40 else store['name']
                
                print(f"{store['id']:<6} {display_name:<40} {status_count:<15} {sku_count}")
        
        # Check both platforms
        check_duplicates(grabfood_stores, "GrabFood")
        check_duplicates(foodpanda_stores, "Foodpanda")
        
        print("\n" + "="*80)
        print("SUMMARY")
        print("="*80)
        
        grabfood_dups = len([name for name, stores in 
                           defaultdict(list, {' '.join(s['name'].lower().strip().split()): s 
                                            for s in grabfood_stores}).items() 
                           if len(stores) > 1])
        
        foodpanda_dups = len([name for name, stores in 
                            defaultdict(list, {' '.join(s['name'].lower().strip().split()): s 
                                             for s in foodpanda_stores}).items() 
                            if len(stores) > 1])
        
        # Recalculate properly
        grabfood_by_name = defaultdict(list)
        for s in grabfood_stores:
            grabfood_by_name[' '.join(s['name'].lower().strip().split())].append(s)
        grabfood_dups = len([name for name, stores in grabfood_by_name.items() if len(stores) > 1])
        
        foodpanda_by_name = defaultdict(list)
        for s in foodpanda_stores:
            foodpanda_by_name[' '.join(s['name'].lower().strip().split())].append(s)
        foodpanda_dups = len([name for name, stores in foodpanda_by_name.items() if len(stores) > 1])
        
        print(f"\nGrabFood:")
        print(f"  Total stores: {len(grabfood_stores)}")
        print(f"  Stores with duplicates: {grabfood_dups}")
        
        print(f"\nFoodpanda:")
        print(f"  Total stores: {len(foodpanda_stores)}")
        print(f"  Stores with duplicates: {foodpanda_dups}")

except Exception as e:
    print(f"\nError: {e}")
    import traceback
    traceback.print_exc()

print("\n" + "="*80)
print("Done!")
print("="*80 + "\n")