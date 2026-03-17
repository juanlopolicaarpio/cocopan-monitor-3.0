#!/usr/bin/env python3
"""
Export all SKUs from database (Foodpanda + GrabFood)
"""
from monitor_service import SKUMapper
from database import db
import json

def main():
    print("=" * 60)
    print("📦 ALL SKUs IN DATABASE")
    print("=" * 60)
    print()

    for platform in ['foodpanda', 'grabfood']:
        mapper = SKUMapper(platform=platform)
        skus = mapper.master_skus

        print(f"🏷️  {platform.upper()} — {len(skus)} SKUs")
        print("-" * 60)

        for i, sku in enumerate(skus, 1):
            code = sku.get('sku_code', 'N/A')
            name = sku.get('product_name', 'N/A')
            print(f"  {i:>4}. [{code}] {name}")

        print()

    # Optional: dump to JSON for reference
    output = {}
    for platform in ['foodpanda', 'grabfood']:
        mapper = SKUMapper(platform=platform)
        output[platform] = mapper.master_skus

    with open('all_skus_export.json', 'w') as f:
        json.dump(output, f, indent=2, default=str)

    total = sum(len(v) for v in output.values())
    print(f"💾 Exported {total} total SKUs to all_skus_export.json")

if __name__ == "__main__":
    main()