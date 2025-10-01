#!/usr/bin/env python3
"""
Fix Duplicate SKU Codes in out_of_stock_skus Arrays
Removes duplicates and recalculates counts to fix discrepancies
"""

import os
import sys
from datetime import datetime, timedelta
from collections import Counter

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from database import db

def find_records_with_duplicates(days_back=30):
    """Find all records that have duplicate SKU codes"""
    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=days_back)
    
    records_to_fix = []
    
    with db.get_connection() as conn:
        cur = conn.cursor()
        
        if db.db_type == "postgresql":
            cur.execute("""
                SELECT 
                    ssc.store_id,
                    s.name as store_name,
                    s.platform,
                    ssc.check_date,
                    ssc.out_of_stock_skus,
                    ssc.out_of_stock_count,
                    ssc.total_skus_checked
                FROM store_sku_checks ssc
                JOIN stores s ON ssc.store_id = s.id
                WHERE ssc.check_date BETWEEN %s AND %s
                ORDER BY ssc.check_date DESC
            """, (start_date, end_date))
        else:
            cur.execute("""
                SELECT 
                    ssc.store_id,
                    s.name as store_name,
                    s.platform,
                    ssc.check_date,
                    ssc.out_of_stock_skus,
                    ssc.out_of_stock_count,
                    ssc.total_skus_checked
                FROM store_sku_checks ssc
                JOIN stores s ON ssc.store_id = s.id
                WHERE ssc.check_date BETWEEN ? AND ?
                ORDER BY ssc.check_date DESC
            """, (start_date.isoformat(), end_date.isoformat()))
        
        records = cur.fetchall()
        
        for record in records:
            if db.db_type == "postgresql":
                store_id, store_name, platform, check_date, oos_skus, stored_count, total_skus = record
            else:
                import json
                store_id = record['store_id']
                store_name = record['store_name']
                platform = record['platform']
                check_date = record['check_date']
                oos_skus = json.loads(record['out_of_stock_skus']) if record['out_of_stock_skus'] else []
                stored_count = record['out_of_stock_count']
                total_skus = record['total_skus_checked']
            
            if oos_skus:
                unique_skus = list(set(oos_skus))  # Remove duplicates
                
                if len(oos_skus) != len(unique_skus):
                    # Found duplicates
                    sku_counts = Counter(oos_skus)
                    duplicates = {sku: count for sku, count in sku_counts.items() if count > 1}
                    
                    records_to_fix.append({
                        'store_id': store_id,
                        'store_name': store_name,
                        'platform': platform,
                        'check_date': check_date,
                        'original_array': oos_skus,
                        'unique_array': unique_skus,
                        'original_count': stored_count,
                        'correct_count': len(unique_skus),
                        'duplicates': duplicates,
                        'total_skus': total_skus
                    })
    
    return records_to_fix

def preview_fixes(records_to_fix):
    """Show what will be fixed"""
    print("\n" + "="*80)
    print("🔍 PREVIEW OF FIXES")
    print("="*80)
    print(f"Found {len(records_to_fix)} records with duplicate SKU codes\n")
    
    for idx, record in enumerate(records_to_fix, 1):
        print(f"{idx}. {record['store_name']}")
        print(f"   Platform: {record['platform']}")
        print(f"   Date: {record['check_date']}")
        print(f"   Current count: {record['original_count']}")
        print(f"   Will become: {record['correct_count']}")
        print(f"   Duplicates found: {dict(record['duplicates'])}")
        
        # Calculate new compliance
        new_compliance = ((record['total_skus'] - record['correct_count']) / max(record['total_skus'], 1)) * 100
        old_compliance = ((record['total_skus'] - record['original_count']) / max(record['total_skus'], 1)) * 100
        
        print(f"   Compliance: {old_compliance:.1f}% → {new_compliance:.1f}%")
        print()
    
    return True

def apply_fixes(records_to_fix, dry_run=True):
    """Apply the fixes to database"""
    if dry_run:
        print("\n" + "="*80)
        print("🧪 DRY RUN MODE - No changes will be made")
        print("="*80)
    else:
        print("\n" + "="*80)
        print("✏️  APPLYING FIXES TO DATABASE")
        print("="*80)
    
    fixed_count = 0
    
    for record in records_to_fix:
        try:
            store_id = record['store_id']
            check_date = record['check_date']
            unique_skus = record['unique_array']
            correct_count = record['correct_count']
            total_skus = record['total_skus']
            
            # Calculate correct compliance percentage
            compliance_pct = ((total_skus - correct_count) / max(total_skus, 1)) * 100.0
            
            if not dry_run:
                with db.get_connection() as conn:
                    cur = conn.cursor()
                    
                    if db.db_type == "postgresql":
                        cur.execute("""
                            UPDATE store_sku_checks
                            SET out_of_stock_skus = %s,
                                out_of_stock_count = %s,
                                compliance_percentage = %s
                            WHERE store_id = %s AND check_date = %s
                        """, (unique_skus, correct_count, compliance_pct, store_id, check_date))
                    else:
                        import json
                        cur.execute("""
                            UPDATE store_sku_checks
                            SET out_of_stock_skus = ?,
                                out_of_stock_count = ?,
                                compliance_percentage = ?
                            WHERE store_id = ? AND check_date = ?
                        """, (json.dumps(unique_skus), correct_count, compliance_pct, 
                              store_id, check_date.isoformat() if hasattr(check_date, 'isoformat') else check_date))
                    
                    conn.commit()
            
            action = "[DRY RUN]" if dry_run else "✓"
            print(f"{action} Fixed {record['store_name']} ({record['check_date']}): "
                  f"{record['original_count']} → {correct_count}")
            
            fixed_count += 1
            
        except Exception as e:
            print(f"✗ Error fixing {record['store_name']}: {e}")
    
    return fixed_count

def update_daily_summaries():
    """Update daily summary after fixing duplicates"""
    print("\n" + "="*80)
    print("🔄 UPDATING DAILY SUMMARIES")
    print("="*80)
    
    try:
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=30)
        
        with db.get_connection() as conn:
            cur = conn.cursor()
            
            # Get all dates that were affected
            if db.db_type == "postgresql":
                cur.execute("""
                    SELECT DISTINCT check_date, platform
                    FROM store_sku_checks
                    WHERE check_date BETWEEN %s AND %s
                    ORDER BY check_date DESC
                """, (start_date, end_date))
            else:
                cur.execute("""
                    SELECT DISTINCT check_date, platform
                    FROM store_sku_checks
                    WHERE check_date BETWEEN ? AND ?
                    ORDER BY check_date DESC
                """, (start_date.isoformat(), end_date.isoformat()))
            
            date_platforms = cur.fetchall()
            
            for record in date_platforms:
                if db.db_type == "postgresql":
                    check_date, platform = record
                else:
                    check_date, platform = record['check_date'], record['platform']
                
                # Recalculate summary for this date/platform
                db._update_daily_sku_summary(platform, check_date, conn)
            
            print(f"✓ Updated summaries for {len(date_platforms)} date/platform combinations")
            
    except Exception as e:
        print(f"✗ Error updating summaries: {e}")

def main():
    """Main execution"""
    print("\n" + "="*80)
    print("CocoPan SKU Duplicate Fixer")
    print("Removes duplicate SKU codes and recalculates counts")
    print("="*80)
    
    # Find records with duplicates
    print("\n🔍 Scanning for duplicate SKU codes...")
    records_to_fix = find_records_with_duplicates(days_back=30)
    
    if not records_to_fix:
        print("\n✅ No duplicates found! All data is clean.")
        return
    
    print(f"\n🔴 Found {len(records_to_fix)} records with duplicates")
    
    # Count total duplicates
    total_duplicate_entries = sum(
        len(r['original_array']) - len(r['unique_array']) 
        for r in records_to_fix
    )
    print(f"🔴 Total duplicate entries: {total_duplicate_entries}")
    
    # Most common duplicated SKUs
    all_duplicates = []
    for r in records_to_fix:
        all_duplicates.extend(r['duplicates'].keys())
    
    from collections import Counter
    duplicate_frequency = Counter(all_duplicates)
    
    print(f"\n📊 Most frequently duplicated SKU codes:")
    for sku, count in duplicate_frequency.most_common(5):
        print(f"   {sku}: appears in {count} records")
    
    # Preview what will be fixed
    preview_fixes(records_to_fix)
    
    # Ask user what to do
    print("="*80)
    print("WHAT WOULD YOU LIKE TO DO?")
    print("="*80)
    print("\n1. DRY RUN (show what would be fixed, no changes)")
    print("2. FIX NOW (remove duplicates and update database)")
    print("3. CANCEL")
    
    try:
        choice = input("\nEnter your choice (1/2/3): ").strip()
        
        if choice == "1":
            print("\n🧪 Running dry run...")
            fixed_count = apply_fixes(records_to_fix, dry_run=True)
            print(f"\n✓ Dry run complete. Would fix {fixed_count} records.")
            print("💡 Run again and choose option 2 to apply fixes.")
            
        elif choice == "2":
            print("\n⚠️  WARNING: This will modify the database!")
            confirm = input("Type 'YES' to confirm: ").strip()
            
            if confirm == "YES":
                print("\n🔧 Applying fixes...")
                fixed_count = apply_fixes(records_to_fix, dry_run=False)
                
                print(f"\n✅ Fixed {fixed_count} records successfully!")
                
                # Update daily summaries
                update_daily_summaries()
                
                print("\n" + "="*80)
                print("✅ ALL FIXES APPLIED SUCCESSFULLY!")
                print("="*80)
                print("\nThe discrepancies have been resolved:")
                print("  ✓ Removed duplicate SKU codes from arrays")
                print("  ✓ Recalculated out_of_stock_count")
                print("  ✓ Updated compliance_percentage")
                print("  ✓ Refreshed daily summaries")
                print("\n💡 Check your dashboard - counts should now match!")
            else:
                print("\n❌ Cancelled. No changes made.")
                
        else:
            print("\n⏭️  Cancelled.")
            
    except KeyboardInterrupt:
        print("\n\n❌ Cancelled by user.")
    
    print("\n" + "="*80)
    print("Done!")
    print("="*80 + "\n")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)