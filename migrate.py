#!/usr/bin/env python3
"""
SKU Migration Script — Sync master_skus with current menu
- Soft-deletes (is_active=FALSE) discontinued items (preserves historical data)
- Adds new menu items
- DRY RUN by default — pass --execute to actually commit
"""
import sys
import logging
from database import db

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# ============================================================================
# DEACTIVATIONS — SKUs in DB but no longer on the menu
# ============================================================================

FOODPANDA_DEACTIVATE = [
    'FB114',  # Cheese Burst Donut
    'FB102',  # Cheesy Ham Roll
    'FB107',  # Chicken Asado Bun
    'FB094',  # Choco Chip Muffin
    'FB004',  # Choco Roll
    'FB108',  # Corn Beef Pandesal
    'FB110',  # Glazed Donut
    'FB097',  # Pinoy Pan Butter Sugar
    'FB098',  # Pinoy Pan Garlic Butter
    'FB115',  # Strawberry Sprinkle Donut
    'FB109',  # Sugar Donut
]

GRABFOOD_DEACTIVATE = [
    'GB114',  # Cheese Burst Donut
    'GB102',  # Cheesy Ham Roll
    'GB107',  # Chicken Asado Bun
    'GB004',  # Choco Roll
    'GB110',  # Glazed Donut
    'GB097',  # Pinoy Pan Butter Sugar
    'GB098',  # Pinoy Pan Garlic Butter
    'GB115',  # Strawberry Sprinkle Donut
    'GB109',  # Sugar Donut
    # NOTE: GB094 Choco Chip Muffin STAYS (still on Grab menu)
    # NOTE: GB108 Corn Beef Pandesal STAYS (still on Grab menu)
]

# ============================================================================
# NEW ITEMS — On the menu but not yet in the DB
# ============================================================================

FOODPANDA_ADD = [
    # Breads / New Items
    {'sku_code': 'FB116', 'product_name': 'Strawberry Jelly Pan',   'platform': 'foodpanda', 'category': 'New Item'},
    {'sku_code': 'FB117', 'product_name': 'Choco Berry Bliss',      'platform': 'foodpanda', 'category': 'New Item'},
    {'sku_code': 'FB118', 'product_name': 'Milky Glazed Donut',     'platform': 'foodpanda', 'category': 'New Item'},
    {'sku_code': 'FB119', 'product_name': 'Snow Sugar Donut',       'platform': 'foodpanda', 'category': 'New Item'},
    # Drinks
    {'sku_code': 'FD118', 'product_name': 'Black Express',          'platform': 'foodpanda', 'category': 'Drinks'},
    {'sku_code': 'FD119', 'product_name': 'Dark Mocha Express',     'platform': 'foodpanda', 'category': 'Drinks'},
    {'sku_code': 'FD120', 'product_name': 'Sweet & Creamy Express',  'platform': 'foodpanda', 'category': 'New Item'},
    {'sku_code': 'FD121', 'product_name': 'Vanilla Latte',          'platform': 'foodpanda', 'category': 'New Item'},
    {'sku_code': 'FD122', 'product_name': 'Caramel Latte',          'platform': 'foodpanda', 'category': 'New Item'},
    {'sku_code': 'FD123', 'product_name': 'Sweet Black Express',    'platform': 'foodpanda', 'category': 'New Item'},
    # Bundles
    {'sku_code': 'FP013', 'product_name': 'Classic Comfort',        'platform': 'foodpanda', 'category': 'Bundles'},
    {'sku_code': 'FP014', 'product_name': 'Mix & Match',            'platform': 'foodpanda', 'category': 'Bundles'},
    {'sku_code': 'FP015', 'product_name': 'Signature Spread',       'platform': 'foodpanda', 'category': 'Bundles'},
]

GRABFOOD_ADD = [
    # Breads / New Items
    {'sku_code': 'GB116', 'product_name': 'Strawberry Jelly Pan',   'platform': 'grabfood', 'category': 'New Item'},
    {'sku_code': 'GB117', 'product_name': 'Choco Berry Bliss',      'platform': 'grabfood', 'category': 'New Item'},
    {'sku_code': 'GB118', 'product_name': 'Snow Sugar Donut',       'platform': 'grabfood', 'category': 'New Item'},
    {'sku_code': 'GB119', 'product_name': 'Milky Glazed Donut',     'platform': 'grabfood', 'category': 'New Item'},
    {'sku_code': 'GB120', 'product_name': 'Double Choco Muffin',    'platform': 'grabfood', 'category': 'Breads'},
    # Drinks
    {'sku_code': 'GD068', 'product_name': 'Barako Black',           'platform': 'grabfood', 'category': 'Drinks'},
    {'sku_code': 'GD069', 'product_name': 'Strong Black',           'platform': 'grabfood', 'category': 'Drinks'},
    {'sku_code': 'GD070', 'product_name': 'Dark Mocha Coffee',      'platform': 'grabfood', 'category': 'Drinks'},
    {'sku_code': 'GD071', 'product_name': 'Vanilla Latte',          'platform': 'grabfood', 'category': 'New Item'},
    {'sku_code': 'GD072', 'product_name': 'Caramel Latte',          'platform': 'grabfood', 'category': 'New Item'},
    {'sku_code': 'GD073', 'product_name': 'Dark Chocolate Express',  'platform': 'grabfood', 'category': 'New Item'},
    {'sku_code': 'GD074', 'product_name': 'Sweet & Creamy Express',  'platform': 'grabfood', 'category': 'New Item'},
    {'sku_code': 'GD075', 'product_name': 'Sweet Black Express',    'platform': 'grabfood', 'category': 'New Item'},
    # Bundles
    {'sku_code': 'GP013', 'product_name': 'Classic Comfort',        'platform': 'grabfood', 'category': 'Bundles'},
    {'sku_code': 'GP014', 'product_name': 'Signature Spread',       'platform': 'grabfood', 'category': 'Bundles'},
    {'sku_code': 'GP015', 'product_name': 'Mix & Match',            'platform': 'grabfood', 'category': 'Bundles'},
]


# ============================================================================
# Migration Logic
# ============================================================================

def deactivate_skus(sku_codes: list, platform: str, dry_run: bool = True):
    """Set is_active = FALSE for the given SKU codes"""
    if not sku_codes:
        return 0

    count = 0
    with db.get_connection() as conn:
        cur = conn.cursor()
        for code in sku_codes:
            if db.db_type == "postgresql":
                # First verify it exists and is currently active
                cur.execute(
                    "SELECT id, product_name, is_active FROM master_skus WHERE sku_code = %s AND platform = %s",
                    (code, platform)
                )
            else:
                cur.execute(
                    "SELECT id, product_name, is_active FROM master_skus WHERE sku_code = ? AND platform = ?",
                    (code, platform)
                )

            row = cur.fetchone()
            if not row:
                logger.warning(f"  ⚠️  {code} — NOT FOUND in DB, skipping")
                continue

            name = row[1] if db.db_type == "postgresql" else row['product_name']
            active = row[2] if db.db_type == "postgresql" else row['is_active']

            if not active:
                logger.info(f"  ⏭️  {code} {name} — already inactive, skipping")
                continue

            if dry_run:
                logger.info(f"  🔸 {code} {name} — WOULD deactivate")
            else:
                if db.db_type == "postgresql":
                    cur.execute(
                        "UPDATE master_skus SET is_active = FALSE WHERE sku_code = %s AND platform = %s",
                        (code, platform)
                    )
                else:
                    cur.execute(
                        "UPDATE master_skus SET is_active = 0 WHERE sku_code = ? AND platform = ?",
                        (code, platform)
                    )
                logger.info(f"  ❌ {code} {name} — DEACTIVATED")
            count += 1

        if not dry_run:
            conn.commit()

    return count


def add_skus(sku_list: list, dry_run: bool = True):
    """Insert new SKUs (skip if sku_code+platform already exists)"""
    if not sku_list:
        return 0

    count = 0
    with db.get_connection() as conn:
        cur = conn.cursor()
        for sku in sku_list:
            code = sku['sku_code']
            platform = sku['platform']

            # Check if already exists (active or inactive)
            if db.db_type == "postgresql":
                cur.execute(
                    "SELECT id, is_active FROM master_skus WHERE sku_code = %s AND platform = %s",
                    (code, platform)
                )
            else:
                cur.execute(
                    "SELECT id, is_active FROM master_skus WHERE sku_code = ? AND platform = ?",
                    (code, platform)
                )

            row = cur.fetchone()
            if row:
                active = row[1] if db.db_type == "postgresql" else row['is_active']
                if active:
                    logger.info(f"  ⏭️  {code} {sku['product_name']} — already exists & active, skipping")
                else:
                    # Re-activate if it was previously deactivated
                    if dry_run:
                        logger.info(f"  🔄 {code} {sku['product_name']} — exists but inactive, WOULD reactivate")
                    else:
                        if db.db_type == "postgresql":
                            cur.execute(
                                "UPDATE master_skus SET is_active = TRUE, product_name = %s, category = %s WHERE sku_code = %s AND platform = %s",
                                (sku['product_name'], sku['category'], code, platform)
                            )
                        else:
                            cur.execute(
                                "UPDATE master_skus SET is_active = 1, product_name = ?, category = ? WHERE sku_code = ? AND platform = ?",
                                (sku['product_name'], sku['category'], code, platform)
                            )
                        logger.info(f"  🔄 {code} {sku['product_name']} — REACTIVATED")
                    count += 1
                continue

            if dry_run:
                logger.info(f"  🔹 {code} {sku['product_name']} ({sku['category']}) — WOULD add")
            else:
                if db.db_type == "postgresql":
                    cur.execute("""
                        INSERT INTO master_skus (sku_code, product_name, platform, category)
                        VALUES (%s, %s, %s, %s)
                    """, (code, sku['product_name'], platform, sku['category']))
                else:
                    cur.execute("""
                        INSERT INTO master_skus (sku_code, product_name, platform, category)
                        VALUES (?, ?, ?, ?)
                    """, (code, sku['product_name'], platform, sku['category']))
                logger.info(f"  ✅ {code} {sku['product_name']} ({sku['category']}) — ADDED")
            count += 1

        if not dry_run:
            conn.commit()

    return count


def main():
    dry_run = '--execute' not in sys.argv

    print()
    print("=" * 70)
    if dry_run:
        print("🧪 DRY RUN — No changes will be made")
        print("   Run with --execute to apply changes")
    else:
        print("🚀 LIVE RUN — Changes WILL be committed to the database")
    print("=" * 70)
    print()

    # --- FOODPANDA ---
    print("=" * 70)
    print("🍕 FOODPANDA — Deactivating discontinued SKUs")
    print("-" * 70)
    fp_deactivated = deactivate_skus(FOODPANDA_DEACTIVATE, 'foodpanda', dry_run)
    print()
    print("🍕 FOODPANDA — Adding new menu items")
    print("-" * 70)
    fp_added = add_skus(FOODPANDA_ADD, dry_run)
    print()

    # --- GRABFOOD ---
    print("=" * 70)
    print("🛵 GRABFOOD — Deactivating discontinued SKUs")
    print("-" * 70)
    gf_deactivated = deactivate_skus(GRABFOOD_DEACTIVATE, 'grabfood', dry_run)
    print()
    print("🛵 GRABFOOD — Adding new menu items")
    print("-" * 70)
    gf_added = add_skus(GRABFOOD_ADD, dry_run)
    print()

    # --- SUMMARY ---
    print("=" * 70)
    print("📊 SUMMARY")
    print("=" * 70)
    print(f"  Foodpanda:  {fp_deactivated} deactivated, {fp_added} added")
    print(f"  GrabFood:   {gf_deactivated} deactivated, {gf_added} added")
    print()

    if dry_run:
        print("👆 This was a DRY RUN. To apply, run:")
        print("   python migrate_skus.py --execute")
    else:
        print("✅ All changes committed to database!")
    print()


if __name__ == "__main__":
    main()