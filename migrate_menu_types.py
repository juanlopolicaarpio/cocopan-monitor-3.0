#!/usr/bin/env python3
"""
Menu Type Migration — Add lite/full menu support
1. Adds menu_type column to stores ('lite' or 'full')
2. Adds menu_type column to master_skus ('both', 'full_only')
3. Tags all stores based on the store list
4. Tags all SKUs based on menu comparison
5. Adds NEW items not yet in DB (Oreo Cream Pan, Almond Glazed Donut, Matcha Melon Milk)

DRY RUN by default — pass --execute to commit
"""
import sys
import logging
from database import db

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# ============================================================================
# STORE CLASSIFICATION (from audit docs)
# ============================================================================

# EXPLICIT OVERRIDES — store IDs that keyword matching gets wrong.
# These are checked FIRST before any keyword matching.
# Format: store_id → 'lite' or 'full'
STORE_ID_OVERRIDES = {
    # --- Wrongly caught by 'galas' lite keyword (SAM = Luzon Galas = FULL) ---
    26:  'full',   # Cocopan Luzon Galas (GrabFood) — SAM is FULL store
    128: 'full',   # Cocopan Luzon (FoodPanda) — SAM is FULL store

    # --- Ever 11th Ave Grab URL has no matching keyword (EME = LITE) ---
    12:  'lite',   # Cocopan Ever 11Th Ave. (GrabFood) — EME is LITE store

    # --- 'sixto-antonio' lite keyword catches SAC which is FULL ---
    127: 'full',   # Cocopan Sixto Antonio Avenue (FoodPanda) — SAC is FULL store

    # --- 'pinagsama' full keyword catches PS2 which is LITE ---
    233: 'lite',   # Cocopan Pinagsama Afp Ii (GrabFood) — PS2 is LITE store

    # --- Grab URLs with no identifiable keywords ---
    203: 'full',   # Cocopan GrabFood Store (Pacita Complex Grab URL)
    221: 'full',   # Cocopan Grab Store (Naga Road Grab URL)
    227: 'full',   # Cocopan Store (Unknown) (Blumentritt Grab URL — store closed)
    226: 'full',   # Cocopan - Pacita Complex (Grab online-delivery URL)
    236: 'full',   # Cocopan Parola (Grab parola URL — not same as parola-cainta)
    243: 'full',   # Cocopan Almeda Santo Rosario (Grab URL) — ASP is FULL
}

# Lite stores — identified by URL fragments
# NOTE: Keywords are checked with substring matching, so be specific
# to avoid colliding with full store URLs
LITE_STORE_KEYWORDS = [
    'malanday',
    'pio-valenzuela',
    'ever-caloocan',
    '11th-avenue',       # EME foodpanda
    'grace-park',        # EME alt name on FP
    'maypajo',
    'naval',
    'santa-quiteria',
    'sta.-quiteria',     # alternate form
    'bf-holy-spirit',
    'congressional',
    'galas',             # LGQ lite (SAM/Luzon Galas is full — handled by override)
    'calderon',
    'blumentritt',
    'pedro-gil',
    'quiapo',
    'san-lazaro',
    'felix-huertas',
    'trabajo',
    'aglipay',
    'barangka',
    'kamuning',
    'kalentong',
    'n-domingo',
    'n.-domingo',
    'pinatubo',
    'pinagbuhatan',
    'binangonan',
    'pag-asa-binangonan',
    'palatiw',
    'market-avenue',
    'san-isidro',
    'sixto-antonio',     # SRP lite (SAC/Sixto Antonio Avenue is full — handled by override)
    'pateros',           # PAT lite (ASP URL is almeda-sto-rosario, no collision)
    'almeda-pateros',    # PAT alt
    'pinagsama-ii',      # PS2 is lite (not PST pinagsama which is full)
    'pinagsama-afp',     # PS2 alternate URL form
    'evacom',
    'victor-medina',
    'merville',
    'caa-las-pinas',
    'caa',               # CAA lite store (no collision with full stores)
    'naga-road',
]

# Full stores — identified by URL fragments
# NOTE: More specific keywords checked second; store ID overrides handle conflicts
FULL_STORE_KEYWORDS = [
    'fortune-marilao',
    'fortune-market',
    'gen-t-de-leon',
    'general-t-de-leon',
    'meycauayan',
    'malinta',
    'marulas',
    'polo-valenzuela',
    'polo-m-h',
    'sta-maria',
    'c-de-guzman',       # Santa Maria alt
    'governor-santiago',
    'wakas-bocaue',
    'vicas-camarin',
    'citisquare',
    'ever-navotas',
    'kanlaon',
    'kaybiga',
    'loreto',
    'morning-breeze',
    'victory-mall',
    'batasan',
    'filinvest-batasan',
    'concepcion-uno',
    'katuparan',
    'litex',
    'munoz',
    'sarmiento',
    'susano',            # Susano Road Novaliches
    'shoe-avenue',
    'villongco',
    'altura',
    'cm-recto',
    'gastambide',
    'old-santa-mesa',
    'old-sta',
    'paco',
    'pureza',
    'anonas',
    'agora',
    'maysilo',
    'milagrosa',
    'mindanao-avenue',
    'tandang-sora',
    'murphy',
    'sierra-madre',
    'dela-paz',
    'imall',
    'i-mall',
    'lifehomes',
    'lifehome-rosario',
    'ortigas',
    'parola-cainta',
    'sixto-antonio-avenue',  # SAC full (not SRP sixto-antonio lite)
    'almeda-sto-rosario',    # ASP full
    'almeda-santo-rosario',  # ASP alt
    'taytay',
    'bayani-road',
    'comembo',
    'evangelista',
    'guadalupe',
    'pembo',
    'pinagsama',         # PST full (pinagsama-ii is lite, handled above)
    'signal',
    'tejeros',
    'tuktukan',
    'edsa-taft',
    'edsa-pasay',
    'buendia',
    'la-huerta',
    'malibay',
    'binakayan',
    'pacita',
    'poblacion-dos',
    'putatan',
    'san-vicente-binan',
    'san-vicente',
    'talon-uno',
    'zapote',
    'moonwalk',
    'landayan',
    'pulang-lupa',
    'luzon',             # Luzon Galas / SAM — full store
]

# ============================================================================
# SKU CLASSIFICATION
# full_only = items that appear on the full menu but NOT on the lite menu
# ============================================================================

# These SKU codes are FULL-ONLY (existing codes in DB after previous migration)
FOODPANDA_FULL_ONLY = [
    # Brewed Coffee category (entire category is full-only)
    'FD057',   # Signature Black Coffee
    'FD028',   # Vietnamese Coffee (Signature Vietnamese Coffee - brewed)
    'FD049',   # Café Espanol
    'FD121',   # Vanilla Latte (new)
    'FD122',   # Caramel Latte (new)

    # Express drinks only on full
    'FD059',   # Express Sweet & Creamy Iced Coffee
    'FD123',   # Sweet Black Express (new)

    # Coolers only on full
    'FD062',   # Lemon Passion Cooler
    'FD065',   # Fruity Melon Milk

    # Bundles only on full
    'FP013',   # Classic Comfort (new)
    'FP014',   # Mix & Match (new)
    'FP015',   # Signature Spread (new)
]

GRABFOOD_FULL_ONLY = [
    # Brewed Coffee category
    'GD057',   # Signature Black Coffee
    'GD028',   # Vietnamese Coffee
    'GD049',   # Café Espanol
    'GD071',   # Vanilla Latte (new)
    'GD072',   # Caramel Latte (new)

    # Express drinks only on full
    'GD059',   # Express Sweet & Creamy Iced Coffee
    'GD075',   # Sweet Black Express (new)

    # Coolers only on full
    'GD062',   # Lemon Passion Cooler
    'GD065',   # Fruity Melon Milk

    # Bundles only on full
    'GP013',   # Classic Comfort (new)
    'GP015',   # Mix & Match (new)
    'GP014',   # Signature Spread (new)
]

# ============================================================================
# NEW ITEMS TO ADD (not in DB yet, discovered from menu docs)
# ============================================================================

NEW_ITEMS_FOODPANDA = [
    {'sku_code': 'FB120', 'product_name': 'Oreo Cream Pan',       'platform': 'foodpanda', 'category': 'Sweet Favorites', 'menu_type': 'both'},
    {'sku_code': 'FB121', 'product_name': 'Almond Glazed Donut',  'platform': 'foodpanda', 'category': 'Donuts',          'menu_type': 'full_only'},
    {'sku_code': 'FD124', 'product_name': 'Matcha Melon Milk',    'platform': 'foodpanda', 'category': 'Matcha',          'menu_type': 'full_only'},
    {'sku_code': 'FD125', 'product_name': 'Vietnamese Express',   'platform': 'foodpanda', 'category': 'Express Coffee',  'menu_type': 'full_only'},
]

NEW_ITEMS_GRABFOOD = [
    {'sku_code': 'GB121', 'product_name': 'Oreo Cream Pan',       'platform': 'grabfood', 'category': 'Sweet Favorites', 'menu_type': 'both'},
    {'sku_code': 'GB122', 'product_name': 'Almond Glazed Donut',  'platform': 'grabfood', 'category': 'Donuts',          'menu_type': 'full_only'},
    {'sku_code': 'GD076', 'product_name': 'Matcha Melon Milk',    'platform': 'grabfood', 'category': 'Matcha',          'menu_type': 'full_only'},
    {'sku_code': 'GD077', 'product_name': 'Vietnamese Express',   'platform': 'grabfood', 'category': 'Express Coffee',  'menu_type': 'full_only'},
]


# ============================================================================
# Migration Logic
# ============================================================================

def step1_add_columns(dry_run=True):
    """Add menu_type columns to stores and master_skus"""
    print("=" * 70)
    print("STEP 1: Add menu_type columns")
    print("=" * 70)

    with db.get_connection() as conn:
        cur = conn.cursor()

        # Check if columns already exist
        if db.db_type == "postgresql":
            cur.execute("""
                SELECT column_name FROM information_schema.columns
                WHERE table_name = 'stores' AND column_name = 'menu_type'
            """)
            stores_has_col = cur.fetchone() is not None

            cur.execute("""
                SELECT column_name FROM information_schema.columns
                WHERE table_name = 'master_skus' AND column_name = 'menu_type'
            """)
            skus_has_col = cur.fetchone() is not None
        else:
            cur.execute("PRAGMA table_info(stores)")
            stores_has_col = any(row[1] == 'menu_type' for row in cur.fetchall())
            cur.execute("PRAGMA table_info(master_skus)")
            skus_has_col = any(row[1] == 'menu_type' for row in cur.fetchall())

        if stores_has_col:
            print("  ⏭️  stores.menu_type already exists")
        elif dry_run:
            print("  🔹 WOULD add stores.menu_type column (default 'full')")
        else:
            if db.db_type == "postgresql":
                cur.execute("ALTER TABLE stores ADD COLUMN menu_type VARCHAR(20) DEFAULT 'full'")
            else:
                cur.execute("ALTER TABLE stores ADD COLUMN menu_type TEXT DEFAULT 'full'")
            print("  ✅ Added stores.menu_type column")

        if skus_has_col:
            print("  ⏭️  master_skus.menu_type already exists")
        elif dry_run:
            print("  🔹 WOULD add master_skus.menu_type column (default 'both')")
        else:
            if db.db_type == "postgresql":
                cur.execute("ALTER TABLE master_skus ADD COLUMN menu_type VARCHAR(20) DEFAULT 'both'")
            else:
                cur.execute("ALTER TABLE master_skus ADD COLUMN menu_type TEXT DEFAULT 'both'")
            print("  ✅ Added master_skus.menu_type column")

        if not dry_run:
            conn.commit()

    print()


def step2_tag_stores(dry_run=True):
    """Tag stores as lite or full based on URL matching"""
    print("=" * 70)
    print("STEP 2: Tag stores as lite/full")
    print("=" * 70)

    with db.get_connection() as conn:
        cur = conn.cursor()

        # Get all stores
        cur.execute("SELECT id, name, url, platform FROM stores ORDER BY name")
        rows = cur.fetchall()

        lite_count = 0
        full_count = 0
        unknown_count = 0
        override_count = 0

        for row in rows:
            if db.db_type == "postgresql":
                store_id, name, url, platform = row[0], row[1], row[2], row[3]
            else:
                store_id, name, url, platform = row['id'], row['name'], row['url'], row['platform']

            url_lower = url.lower()
            menu_type = None
            match_source = ''

            # 1. Check explicit store ID overrides FIRST (handles edge cases)
            if store_id in STORE_ID_OVERRIDES:
                menu_type = STORE_ID_OVERRIDES[store_id]
                match_source = 'override'

            # 2. Check lite keywords
            if not menu_type:
                for kw in LITE_STORE_KEYWORDS:
                    if kw in url_lower:
                        menu_type = 'lite'
                        match_source = f'keyword: {kw}'
                        break

            # 3. Check full keywords
            if not menu_type:
                for kw in FULL_STORE_KEYWORDS:
                    if kw in url_lower:
                        menu_type = 'full'
                        match_source = f'keyword: {kw}'
                        break

            # 4. Default to full if unknown
            if not menu_type:
                menu_type = 'full'
                unknown_count += 1
                match_source = 'DEFAULT'
                logger.warning(f"  ⚠️  [{store_id}] {name} — no match, defaulting to FULL")
                logger.warning(f"       URL: {url}")

            if menu_type == 'lite':
                lite_count += 1
            else:
                full_count += 1
            if match_source == 'override':
                override_count += 1

            if dry_run:
                icon = '🟡' if menu_type == 'lite' else '🟢'
                override_tag = ' ⚡OVERRIDE' if match_source == 'override' else ''
                default_tag = ' ⚠️DEFAULT' if match_source == 'DEFAULT' else ''
                print(f"  {icon} [{store_id}] {name} → {menu_type.upper()}{override_tag}{default_tag}")
            else:
                if db.db_type == "postgresql":
                    cur.execute("UPDATE stores SET menu_type = %s WHERE id = %s", (menu_type, store_id))
                else:
                    cur.execute("UPDATE stores SET menu_type = ? WHERE id = ?", (menu_type, store_id))

        if not dry_run:
            conn.commit()

        print()
        print(f"  Summary: {lite_count} lite, {full_count} full ({override_count} overrides, {unknown_count} defaulted)")

    print()


def step3_tag_skus(dry_run=True):
    """Tag SKUs as 'both' or 'full_only'"""
    print("=" * 70)
    print("STEP 3: Tag SKUs as both/full_only")
    print("=" * 70)

    all_full_only = {
        'foodpanda': FOODPANDA_FULL_ONLY,
        'grabfood': GRABFOOD_FULL_ONLY,
    }

    with db.get_connection() as conn:
        cur = conn.cursor()

        for platform, full_only_codes in all_full_only.items():
            print(f"\n  --- {platform.upper()} ---")

            # First: set ALL active SKUs to 'both' (the default)
            if dry_run:
                print(f"  🔹 WOULD set all {platform} SKUs to 'both' first")
            else:
                if db.db_type == "postgresql":
                    cur.execute(
                        "UPDATE master_skus SET menu_type = 'both' WHERE platform = %s AND is_active = TRUE",
                        (platform,)
                    )
                else:
                    cur.execute(
                        "UPDATE master_skus SET menu_type = 'both' WHERE platform = ? AND is_active = 1",
                        (platform,)
                    )

            # Then: tag full-only SKUs
            tagged = 0
            for code in full_only_codes:
                if db.db_type == "postgresql":
                    cur.execute(
                        "SELECT id, product_name FROM master_skus WHERE sku_code = %s AND platform = %s",
                        (code, platform)
                    )
                else:
                    cur.execute(
                        "SELECT id, product_name FROM master_skus WHERE sku_code = ? AND platform = ?",
                        (code, platform)
                    )

                row = cur.fetchone()
                if not row:
                    logger.warning(f"  ⚠️  {code} — NOT FOUND (might need to run migrate_skus.py first)")
                    continue

                name = row[1] if db.db_type == "postgresql" else row['product_name']

                if dry_run:
                    print(f"  🔸 {code} {name} → FULL_ONLY")
                else:
                    if db.db_type == "postgresql":
                        cur.execute(
                            "UPDATE master_skus SET menu_type = 'full_only' WHERE sku_code = %s AND platform = %s",
                            (code, platform)
                        )
                    else:
                        cur.execute(
                            "UPDATE master_skus SET menu_type = 'full_only' WHERE sku_code = ? AND platform = ?",
                            (code, platform)
                        )
                    print(f"  ✅ {code} {name} → FULL_ONLY")

                tagged += 1

            print(f"\n  {platform}: {tagged} tagged as full_only, rest are 'both'")

        if not dry_run:
            conn.commit()

    print()


def step4_add_new_items(dry_run=True):
    """Add newly discovered menu items"""
    print("=" * 70)
    print("STEP 4: Add new menu items (Oreo Cream Pan, Almond Glazed, etc)")
    print("=" * 70)

    all_new = NEW_ITEMS_FOODPANDA + NEW_ITEMS_GRABFOOD

    with db.get_connection() as conn:
        cur = conn.cursor()

        added = 0
        skipped = 0

        for item in all_new:
            code = item['sku_code']
            platform = item['platform']

            # Check if already exists
            if db.db_type == "postgresql":
                cur.execute(
                    "SELECT id FROM master_skus WHERE sku_code = %s AND platform = %s",
                    (code, platform)
                )
            else:
                cur.execute(
                    "SELECT id FROM master_skus WHERE sku_code = ? AND platform = ?",
                    (code, platform)
                )

            if cur.fetchone():
                print(f"  ⏭️  {code} {item['product_name']} ({platform}) — already exists")
                skipped += 1
                continue

            if dry_run:
                print(f"  🔹 {code} {item['product_name']} ({platform}, {item['menu_type']}) — WOULD add")
            else:
                if db.db_type == "postgresql":
                    cur.execute("""
                        INSERT INTO master_skus (sku_code, product_name, platform, category, menu_type)
                        VALUES (%s, %s, %s, %s, %s)
                    """, (code, item['product_name'], platform, item['category'], item['menu_type']))
                else:
                    cur.execute("""
                        INSERT INTO master_skus (sku_code, product_name, platform, category, menu_type)
                        VALUES (?, ?, ?, ?, ?)
                    """, (code, item['product_name'], platform, item['category'], item['menu_type']))
                print(f"  ✅ {code} {item['product_name']} ({platform}, {item['menu_type']}) — ADDED")

            added += 1

        if not dry_run:
            conn.commit()

        print(f"\n  Added: {added}, Skipped: {skipped}")

    print()


def show_final_summary():
    """Show what the DB looks like after migration"""
    print("=" * 70)
    print("📊 POST-MIGRATION SUMMARY")
    print("=" * 70)

    with db.get_connection() as conn:
        cur = conn.cursor()

        # Store counts
        for menu_type in ['lite', 'full']:
            if db.db_type == "postgresql":
                cur.execute("SELECT COUNT(*) FROM stores WHERE menu_type = %s", (menu_type,))
            else:
                cur.execute("SELECT COUNT(*) FROM stores WHERE menu_type = ?", (menu_type,))
            count = cur.fetchone()[0]
            print(f"  Stores ({menu_type}): {count}")

        print()

        # SKU counts per platform and menu_type
        for platform in ['foodpanda', 'grabfood']:
            if db.db_type == "postgresql":
                cur.execute("""
                    SELECT menu_type, COUNT(*) FROM master_skus
                    WHERE platform = %s AND is_active = TRUE
                    GROUP BY menu_type ORDER BY menu_type
                """, (platform,))
            else:
                cur.execute("""
                    SELECT menu_type, COUNT(*) FROM master_skus
                    WHERE platform = ? AND is_active = 1
                    GROUP BY menu_type ORDER BY menu_type
                """, (platform,))

            rows = cur.fetchall()
            total = 0
            print(f"  {platform.upper()} SKUs:")
            for row in rows:
                mt = row[0] if db.db_type == "postgresql" else row[0]
                cnt = row[1] if db.db_type == "postgresql" else row[1]
                total += cnt
                print(f"    {mt}: {cnt}")
            print(f"    TOTAL active: {total}")

            # For lite stores, how many SKUs they get checked against
            both_count = 0
            for row in rows:
                mt = row[0] if db.db_type == "postgresql" else row[0]
                cnt = row[1] if db.db_type == "postgresql" else row[1]
                if mt == 'both':
                    both_count = cnt
            print(f"    → Lite stores checked against: {both_count} SKUs")
            print(f"    → Full stores checked against: {total} SKUs")
            print()

    print()


def main():
    dry_run = '--execute' not in sys.argv

    print()
    print("=" * 70)
    if dry_run:
        print("🧪 DRY RUN — No changes will be made")
        print("   Run with --execute to apply changes")
    else:
        print("🚀 LIVE RUN — Changes WILL be committed")
    print("=" * 70)
    print()
    print("⚠️  IMPORTANT: Run migrate_skus.py --execute FIRST")
    print("   (to add/deactivate SKUs before tagging menu types)")
    print()

    step1_add_columns(dry_run)
    step2_tag_stores(dry_run)
    step3_tag_skus(dry_run)
    step4_add_new_items(dry_run)

    if not dry_run:
        show_final_summary()
    else:
        print("=" * 70)
        print("👆 This was a DRY RUN. To apply:")
        print("   1. python migrate_skus.py --execute     (add/remove SKUs)")
        print("   2. python migrate_menu_types.py --execute (this script)")
        print("=" * 70)

    print()


if __name__ == "__main__":
    main()