#!/usr/bin/env python3
"""
🚀 DIRECT DATABASE POPULATION - DIFFERENT SKU CODES PER PLATFORM

GrabFood: GB/GD/GP prefix
Foodpanda: FB/FD/FP prefix

Run: python populate_direct.py
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from database import db
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# ============================================================================
# ALL 60 PRODUCTS WITH DIFFERENT CODES PER PLATFORM
# ============================================================================

PRODUCTS = [
    # BUNDLES (6 products)
    {
        'name': 'Build A Box (Super Box 13 Pcs)',
        'category': 'Bundles',
        'division': 'Value Packs',
        'price': 300.00,
        'grabfood_sku': 'GP007',
        'foodpanda_sku': 'FP007'
    },
    {
        'name': 'Super Box (Assorted 13 Pcs)',
        'category': 'Bundles',
        'division': 'Value Packs',
        'price': 300.00,
        'grabfood_sku': 'GP008',
        'foodpanda_sku': 'FP008'
    },
    {
        'name': 'Build A Box (Snack Box 10 Pcs)',
        'category': 'Bundles',
        'division': 'Value Packs',
        'price': 150.00,
        'grabfood_sku': 'GP009',
        'foodpanda_sku': 'FP009'
    },
    {
        'name': 'Snack Box (Assorted 10 Pcs)',
        'category': 'Bundles',
        'division': 'Value Packs',
        'price': 150.00,
        'grabfood_sku': 'GP010',
        'foodpanda_sku': 'FP010'
    },
    {
        'name': 'Perfect Pair 1',
        'category': 'Bundles',
        'division': 'Promos',
        'price': 69.00,
        'grabfood_sku': 'GP011',
        'foodpanda_sku': 'FP011'
    },
    {
        'name': 'Perfect Pair 2',
        'category': 'Bundles',
        'division': 'Promos',
        'price': 79.00,
        'grabfood_sku': 'GP012',
        'foodpanda_sku': 'FP012'
    },
    
    # DRINKS (24 products)
    {
        'name': 'Vietnamese Coffee',
        'category': 'Drinks',
        'division': 'Beverages',
        'price': 70.00,
        'grabfood_sku': 'GD028',
        'foodpanda_sku': 'FD028'
    },
    {
        'name': 'Milo Overload',
        'category': 'Drinks',
        'division': 'Beverages',
        'price': 85.00,
        'grabfood_sku': 'GD050',
        'foodpanda_sku': 'FD050'
    },
    {
        'name': 'Original Matcha Milk',
        'category': 'Drinks',
        'division': 'Beverages',
        'price': 65.00,
        'grabfood_sku': 'GD051',
        'foodpanda_sku': 'FD051'
    },
    {
        'name': 'Twistea',
        'category': 'Drinks',
        'division': 'Beverages',
        'price': 55.00,
        'grabfood_sku': 'GD117',
        'foodpanda_sku': 'FD117'
    },
    {
        'name': 'Strawberry Matcha Milk',
        'category': 'Drinks',
        'division': 'Matcha',
        'price': 70.00,
        'grabfood_sku': 'GD052',
        'foodpanda_sku': 'FD052'
    },
    {
        'name': 'Viet Coffee Matcha',
        'category': 'Drinks',
        'division': 'Matcha',
        'price': 95.00,
        'grabfood_sku': 'GD053',
        'foodpanda_sku': 'FD053'
    },
    {
        'name': 'Dark Choco Coffee',
        'category': 'Drinks',
        'division': 'Brewed Coffee',
        'price': 85.00,
        'grabfood_sku': 'GD034',
        'foodpanda_sku': 'FD034'
    },
    {
        'name': 'Café Espanol',
        'category': 'Drinks',
        'division': 'Brewed Coffee',
        'price': 80.00,
        'grabfood_sku': 'GD049',
        'foodpanda_sku': 'FD049'
    },
    {
        'name': 'Creamy Vanilla Coffee',
        'category': 'Drinks',
        'division': 'Brewed Coffee',
        'price': 80.00,
        'grabfood_sku': 'GD054',
        'foodpanda_sku': 'FD054'
    },
    {
        'name': 'Salted Caramel Coffee',
        'category': 'Drinks',
        'division': 'Brewed Coffee',
        'price': 80.00,
        'grabfood_sku': 'GD055',
        'foodpanda_sku': 'FD055'
    },
    {
        'name': 'Signature Milky Coffee',
        'category': 'Drinks',
        'division': 'Brewed Coffee',
        'price': 65.00,
        'grabfood_sku': 'GD056',
        'foodpanda_sku': 'FD056'
    },
    {
        'name': 'Signature Black Coffee',
        'category': 'Drinks',
        'division': 'Brewed Coffee',
        'price': 50.00,
        'grabfood_sku': 'GD057',
        'foodpanda_sku': 'FD057'
    },
    {
        'name': 'Express Classic Iced Coffee',
        'category': 'Drinks',
        'division': 'Beverages',
        'price': 45.00,
        'grabfood_sku': 'GD058',
        'foodpanda_sku': 'FD058'
    },
    {
        'name': 'Express Sweet & Creamy Iced Coffee',
        'category': 'Drinks',
        'division': 'Beverages',
        'price': 60.00,
        'grabfood_sku': 'GD059',
        'foodpanda_sku': 'FD059'
    },
    {
        'name': 'Express Caramel Coffee',
        'category': 'Drinks',
        'division': 'Beverages',
        'price': 65.00,
        'grabfood_sku': 'GD060',
        'foodpanda_sku': 'FD060'
    },
    {
        'name': 'Lemon Berry Cooler',
        'category': 'Drinks',
        'division': 'Coolers',
        'price': 65.00,
        'grabfood_sku': 'GD061',
        'foodpanda_sku': 'FD061'
    },
    {
        'name': 'Lemon Passion Cooler',
        'category': 'Drinks',
        'division': 'Coolers',
        'price': 65.00,
        'grabfood_sku': 'GD062',
        'foodpanda_sku': 'FD062'
    },
    {
        'name': 'Mango Sunrise Cooler',
        'category': 'Drinks',
        'division': 'Coolers',
        'price': 55.00,
        'grabfood_sku': 'GD063',
        'foodpanda_sku': 'FD063'
    },
    {
        'name': 'Ruby Twistea',
        'category': 'Drinks',
        'division': 'Coolers',
        'price': 55.00,
        'grabfood_sku': 'GD064',
        'foodpanda_sku': 'FD064'
    },
    {
        'name': 'Fruity Melon Milk',
        'category': 'Drinks',
        'division': 'Coolers',
        'price': 70.00,
        'grabfood_sku': 'GD065',
        'foodpanda_sku': 'FD065'
    },
    {
        'name': 'Fruity Strawberry Milk',
        'category': 'Drinks',
        'division': 'Coolers',
        'price': 70.00,
        'grabfood_sku': 'GD066',
        'foodpanda_sku': 'FD066'
    },
    {
        'name': 'Fruity Milk Mango',
        'category': 'Drinks',
        'division': 'Coolers',
        'price': 70.00,
        'grabfood_sku': 'GD067',
        'foodpanda_sku': 'FD067'
    },
    
    # BREADS & DONUTS (30 products)
    {
        'name': 'Pan de coco',
        'category': 'Breads',
        'division': 'Classic Favorites',
        'price': 15.00,
        'grabfood_sku': 'GB001',
        'foodpanda_sku': 'FB001'
    },
    {
        'name': 'Spanish Bread',
        'category': 'Breads',
        'division': 'Classic Favorites',
        'price': 15.00,
        'grabfood_sku': 'GB002',
        'foodpanda_sku': 'FB002'
    },
    {
        'name': 'Cheese Roll',
        'category': 'Breads',
        'division': 'Classic Favorites',
        'price': 15.00,
        'grabfood_sku': 'GB003',
        'foodpanda_sku': 'FB003'
    },
    {
        'name': 'Choco Roll',
        'category': 'Breads',
        'division': 'Classic Favorites',
        'price': 15.00,
        'grabfood_sku': 'GB004',
        'foodpanda_sku': 'FB004'
    },
    {
        'name': 'Pan De Sal Pack (10pcs)',
        'category': 'Breads',
        'division': 'Classic Favorites',
        'price': 55.00,
        'grabfood_sku': 'GB005',
        'foodpanda_sku': 'FB005'
    },
    {
        'name': 'Cinnamon Roll Deluxe',
        'category': 'Breads',
        'division': 'Sweet Favorites',
        'price': 25.00,
        'grabfood_sku': 'GB089',
        'foodpanda_sku': 'FB089'
    },
    {
        'name': 'Cinnamon Roll Classic',
        'category': 'Breads',
        'division': 'Sweet Favorites',
        'price': 20.00,
        'grabfood_sku': 'GB090',
        'foodpanda_sku': 'FB090'
    },
    {
        'name': 'Coffee Bun',
        'category': 'Breads',
        'division': 'Sweet Favorites',
        'price': 35.00,
        'grabfood_sku': 'GB091',
        'foodpanda_sku': 'FB091'
    },
    {
        'name': 'Banana Bread (Sliced)',
        'category': 'Breads',
        'division': 'Sweet Favorites',
        'price': 25.00,
        'grabfood_sku': 'GB092',
        'foodpanda_sku': 'FB092'
    },
    {
        'name': 'Blueberry Muffin',
        'category': 'Breads',
        'division': 'Sweet Favorites',
        'price': 40.00,
        'grabfood_sku': 'GB093',
        'foodpanda_sku': 'FB093'
    },
    {
        'name': 'Choco Chip Muffin',
        'category': 'Breads',
        'division': 'Sweet Favorites',
        'price': 40.00,
        'grabfood_sku': 'GB094',
        'foodpanda_sku': 'FB094'
    },
    {
        'name': 'Choco Cream Pan',
        'category': 'Breads',
        'division': 'New Offers',
        'price': 25.00,
        'grabfood_sku': 'GB095',
        'foodpanda_sku': 'FB095'
    },
    {
        'name': 'Double Cheese Roll',
        'category': 'Breads',
        'division': 'New Offers',
        'price': 20.00,
        'grabfood_sku': 'GB096',
        'foodpanda_sku': 'FB096'
    },
    {
        'name': 'Pinoy Pan Butter Sugar',
        'category': 'Breads',
        'division': 'New Offers',
        'price': 12.00,
        'grabfood_sku': 'GB097',
        'foodpanda_sku': 'FB097'
    },
    {
        'name': 'Pinoy Pan Garlic Butter',
        'category': 'Breads',
        'division': 'New Offers',
        'price': 12.00,
        'grabfood_sku': 'GB098',
        'foodpanda_sku': 'FB098'
    },
    {
        'name': 'Italian Herb Loaf',
        'category': 'Breads',
        'division': 'Daily Loaf',
        'price': 75.00,
        'grabfood_sku': 'GB099',
        'foodpanda_sku': 'FB099'
    },
    {
        'name': 'Raisin Loaf',
        'category': 'Breads',
        'division': 'Daily Loaf',
        'price': 75.00,
        'grabfood_sku': 'GB100',
        'foodpanda_sku': 'FB100'
    },
    {
        'name': 'Italian Cheese Roll',
        'category': 'Breads',
        'division': 'Savory Favorites',
        'price': 20.00,
        'grabfood_sku': 'GB101',
        'foodpanda_sku': 'FB101'
    },
    {
        'name': 'Cheesy Ham Roll',
        'category': 'Breads',
        'division': 'Savory Favorites',
        'price': 25.00,
        'grabfood_sku': 'GB102',
        'foodpanda_sku': 'FB102'
    },
    {
        'name': 'Pan de Floss (Original)',
        'category': 'Breads',
        'division': 'Savory Favorites',
        'price': 35.00,
        'grabfood_sku': 'GB103',
        'foodpanda_sku': 'FB103'
    },
    {
        'name': 'Pan de Floss (Spicy)',
        'category': 'Breads',
        'division': 'Savory Favorites',
        'price': 35.00,
        'grabfood_sku': 'GB104',
        'foodpanda_sku': 'FB104'
    },
    {
        'name': 'Tuna Bun',
        'category': 'Breads',
        'division': 'Savory Favorites',
        'price': 35.00,
        'grabfood_sku': 'GB105',
        'foodpanda_sku': 'FB105'
    },
    {
        'name': 'Cheesy Sausage Roll',
        'category': 'Breads',
        'division': 'Savory Favorites',
        'price': 45.00,
        'grabfood_sku': 'GB106',
        'foodpanda_sku': 'FB106'
    },
    {
        'name': 'Chicken Asado Bun',
        'category': 'Breads',
        'division': 'Savory Favorites',
        'price': 25.00,
        'grabfood_sku': 'GB107',
        'foodpanda_sku': 'FB107'
    },
    {
        'name': 'Corn Beef Pandesal',
        'category': 'Breads',
        'division': 'New Offers',
        'price': 35.00,
        'grabfood_sku': 'GB108',
        'foodpanda_sku': 'FB108'
    },
    {
        'name': 'Sugar Donut',
        'category': 'Donuts',
        'division': 'Sweet Treats',
        'price': 15.00,
        'grabfood_sku': 'GB109',
        'foodpanda_sku': 'FB109'
    },
    {
        'name': 'Glazed Donut',
        'category': 'Donuts',
        'division': 'Sweet Treats',
        'price': 20.00,
        'grabfood_sku': 'GB110',
        'foodpanda_sku': 'FB110'
    },
    {
        'name': 'Choco Frost Donut',
        'category': 'Donuts',
        'division': 'Sweet Treats',
        'price': 35.00,
        'grabfood_sku': 'GB111',
        'foodpanda_sku': 'FB111'
    },
    {
        'name': 'Choco Cheese Donut',
        'category': 'Donuts',
        'division': 'Sweet Treats',
        'price': 35.00,
        'grabfood_sku': 'GB112',
        'foodpanda_sku': 'FB112'
    },
    {
        'name': 'Milky Cheese Donut',
        'category': 'Donuts',
        'division': 'New Offers',
        'price': 35.00,
        'grabfood_sku': 'GB113',
        'foodpanda_sku': 'FB113'
    },
    {
        'name': 'Cheese Burst Donut',
        'category': 'Donuts',
        'division': 'Sweet Treats',
        'price': 30.00,
        'grabfood_sku': 'GB114',
        'foodpanda_sku': 'FB114'
    },
    {
        'name': 'Strawberry Sprinkle Donut',
        'category': 'Donuts',
        'division': 'Sweet Treats',
        'price': 35.00,
        'grabfood_sku': 'GB115',
        'foodpanda_sku': 'FB115'
    },
]

# Total: 6 + 24 + 30 = 60 products


def main():
    """Direct database population"""
    
    print("=" * 80)
    print("🚀 DIRECT DATABASE POPULATION")
    print("=" * 80)
    print()
    
    logger.info(f"📦 Total products to load: {len(PRODUCTS)}")
    logger.info(f"📊 This will create {len(PRODUCTS) * 2} SKU records (60 × 2 platforms)")
    print()
    
    # Confirmation
    choice = input(f"Load {len(PRODUCTS)} products into database? (y/n): ").strip().lower()
    if choice != 'y':
        logger.info("❌ Cancelled")
        return
    
    print()
    logger.info("💾 Loading into database...")
    
    # Prepare SKU list for bulk insert
    sku_list = []
    
    for product in PRODUCTS:
        # GrabFood version
        sku_list.append({
            'sku_code': product['grabfood_sku'],
            'product_name': product['name'],
            'platform': 'grabfood',
            'category': product['category'],
            'division': product.get('division', ''),
            'flow_category': '',
            'gmv_q3': product.get('price', 0) * 10  # Estimate GMV
        })
        
        # Foodpanda version (different SKU code!)
        sku_list.append({
            'sku_code': product['foodpanda_sku'],
            'product_name': product['name'],
            'platform': 'foodpanda',
            'category': product['category'],
            'division': product.get('division', ''),
            'flow_category': '',
            'gmv_q3': product.get('price', 0) * 10  # Estimate GMV
        })
    
    # Bulk insert
    success = db.bulk_add_master_skus(sku_list)
    
    if success:
        print()
        logger.info(f"✅ Successfully loaded {len(sku_list)} SKU records!")
        
        # Verify
        print()
        logger.info("🔍 Verifying database...")
        
        grabfood_skus = db.get_master_skus_by_platform('grabfood')
        foodpanda_skus = db.get_master_skus_by_platform('foodpanda')
        
        logger.info("📊 Database now has:")
        logger.info(f"   GrabFood:  {len(grabfood_skus)} SKUs (GB/GD/GP prefix)")
        logger.info(f"   Foodpanda: {len(foodpanda_skus)} SKUs (FB/FD/FP prefix)")
        logger.info(f"   TOTAL:     {len(grabfood_skus) + len(foodpanda_skus)} SKU records")
        
        print()
        print("=" * 80)
        print("✅ POPULATION COMPLETED!")
        print("=" * 80)
        print()
        print("🎯 SKU Code Structure:")
        print("   GrabFood:")
        print("      GB001-GB115: Breads & Donuts")
        print("      GD028-GD067: Drinks")
        print("      GP007-GP012: Bundles")
        print()
        print("   Foodpanda:")
        print("      FB001-FB115: Breads & Donuts")
        print("      FD028-FD067: Drinks")
        print("      FP007-FP012: Bundles")
        print()
        print("🎯 Next steps:")
        print("   1. Test scraping: python monitor_service.py --test-sku")
        print("   2. Start monitoring: python monitor_service.py")
        print()
        
    else:
        logger.error("❌ Failed to load SKUs!")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n❌ Interrupted by user")
    except Exception as e:
        logger.error(f"❌ Fatal error: {e}")
        import traceback
        traceback.print_exc()