#!/usr/bin/env python3
"""
Master SKUs Full Dump (Postgres via your database.db helper)

- Uses database.db.get_connection() just like your other scripts
- Prints grouped view: platform -> flow_category
- Prints a RAW TABLE section (first N rows) for easy copy-paste
- Exports a CSV file (./master_skus_dump_YYYYMMDD_HHMMSS.csv)
"""

import csv
import sys
from datetime import datetime
from collections import defaultdict
from database import db  # ← your helper

TABLE = "master_skus"
ORDER_BY = "platform, flow_category, product_name, sku_code"
RAW_LIMIT = 500  # how many rows to print in the RAW TABLE section

# Columns we prefer to show; if your table has more, they’ll appear in RAW/CSV
PREFERRED_COLS = [
    "sku_code",
    "product_name",
    "platform",
    "flow_category",
    "category",
    "division",
    "gmv_q3",
]

def fetch_all_rows():
    """
    Fetch all rows from master_skus.
    Returns: (rows: List[Dict[str, Any]], columns: List[str])
    """
    with db.get_connection() as conn:
        cur = conn.cursor()
        # Try to list actual columns from information_schema (Postgres)
        select_cols = None
        try:
            cur.execute("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = %s
                ORDER BY ordinal_position
            """, (TABLE,) if getattr(db, 'db_type', 'postgresql') == 'postgresql' else (TABLE,))
            cols = [r[0] for r in cur.fetchall()]
            if cols:
                select_cols = ", ".join(cols)
        except Exception:
            pass

        if not select_cols:
            select_cols = ", ".join(PREFERRED_COLS)

        # Final select
        cur.execute(f"SELECT {select_cols} FROM {TABLE} ORDER BY {ORDER_BY};")
        colnames = [d[0] for d in cur.description]
        data = [dict(zip(colnames, row)) for row in cur.fetchall()]
        return data, colnames

def export_csv(rows, headers):
    path = f"./master_skus_dump_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=headers)
        w.writeheader()
        for r in rows:
            w.writerow(r)
    return path

def print_grouped(rows):
    by_platform = defaultdict(lambda: defaultdict(list))
    for r in rows:
        plat = (r.get("platform") or "").lower()
        cat = r.get("flow_category") or ""
        by_platform[plat][cat].append(r)

    total = 0
    for plat in sorted(by_platform.keys()):
        print(f"\n========== {plat.upper()} ==========")
        for cat in sorted(by_platform[plat].keys()):
            items = by_platform[plat][cat]
            print(f"\n-- {cat} ({len(items)}) --")
            for r in items:
                sku = r.get("sku_code", "")
                name = r.get("product_name", "")
                div = r.get("division") or r.get("category", "")
                print(f"• {sku:<8}  {name}  [{div}]")
                total += 1
    print(f"\n✅ TOTAL ROWS: {total}")

def print_raw(rows, headers, limit=RAW_LIMIT):
    print("\n==== RAW TABLE (first {} rows) ====".format(min(limit, len(rows))))
    print(" | ".join(headers))
    print("-" * 80)
    for r in rows[:limit]:
        print(" | ".join(str(r.get(h, "")) for h in headers))

def main():
    print("🔎 Dumping full contents of master_skus …")
    try:
        rows, headers = fetch_all_rows()
        if not rows:
            print("⚠️  master_skus is empty or not found.")
            sys.exit(0)

        # Grouped human-readable view
        print_grouped(rows)

        # Raw table for copy-paste back here
        print_raw(rows, headers, RAW_LIMIT)

        # CSV export
        out = export_csv(rows, headers)
        print(f"\n📄 CSV written: {out}")

        # Quick per-platform totals
        gf = sum(1 for r in rows if (r.get("platform") or "").lower() == "grabfood")
        fp = sum(1 for r in rows if (r.get("platform") or "").lower() == "foodpanda")
        print("\n📊 Totals by platform:")
        print(f"   GrabFood:  {gf}")
        print(f"   Foodpanda: {fp}")
        print(f"   Overall:   {len(rows)}")

        print("\n✅ Done. Paste the RAW TABLE (or attach the CSV) and I’ll generate exact UPDATE/INSERTs to align with the October Storefront Flow—without removing any existing data.")

    except Exception as e:
        print(f"❌ Error: {e}")
        raise

if __name__ == "__main__":
    main()
