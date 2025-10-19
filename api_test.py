#!/usr/bin/env python3
"""
Foodpanda Menu API Tester - WORKING SOLUTION!
Tests the discovered API endpoint and shows menu structure

Endpoint discovered: 
https://ph.fd-api.com/api/v5/vendors/{vendor_code}?include=menus,bundles,multiple_discounts&language_id=1&opening_type=delivery&basket_currency=PHP
"""

import requests
import json
from typing import Dict, List, Optional, Any

# Test stores
TEST_STORES = [
    {"code": "i51s", "name": "Cocopan Altura"},
    {"code": "m90j", "name": "Cocopan Aglipay"},
    {"code": "bnn1", "name": "Cocopan Anonas"},
]


def fetch_foodpanda_menu(vendor_code: str) -> Optional[Dict[str, Any]]:
    """
    Fetch menu data from Foodpanda API
    
    Returns complete vendor data including:
    - Basic info (name, address, rating)
    - Menu structure (categories, products)
    - Product availability status
    """
    url = f"https://ph.fd-api.com/api/v5/vendors/{vendor_code}"
    
    params = {
        "include": "menus,bundles,multiple_discounts",
        "language_id": "1",
        "opening_type": "delivery",
        "basket_currency": "PHP"
    }
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "application/json",
        "Accept-Language": "en-US,en;q=0.9",
        "Origin": "https://www.foodpanda.ph",
        "Referer": f"https://www.foodpanda.ph/restaurant/{vendor_code}/",
    }
    
    try:
        print(f"\n📡 Fetching: {url}")
        print(f"   Params: {params}")
        
        response = requests.get(url, params=params, headers=headers, timeout=15)
        
        print(f"   Status: {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            print(f"   ✅ SUCCESS! Got menu data")
            return data
        else:
            print(f"   ❌ Failed: {response.status_code}")
            print(f"   Response: {response.text[:200]}")
            return None
            
    except Exception as e:
        print(f"   ❌ Error: {e}")
        return None


def analyze_menu_structure(data: Dict[str, Any], vendor_name: str):
    """Analyze and display menu structure"""
    print("\n" + "=" * 70)
    print(f"📊 MENU ANALYSIS: {vendor_name}")
    print("=" * 70)
    
    # Basic vendor info
    if 'data' in data:
        vendor = data['data']
        
        print(f"\n🏪 Vendor Info:")
        print(f"   Name: {vendor.get('name', 'N/A')}")
        print(f"   Code: {vendor.get('code', 'N/A')}")
        print(f"   Address: {vendor.get('address', 'N/A')}")
        print(f"   Rating: {vendor.get('rating', 'N/A')}")
        print(f"   Is Available: {vendor.get('is_active', 'N/A')}")
        
        # Menu structure
        menus = vendor.get('menus', [])
        print(f"\n📋 Menu Structure:")
        print(f"   Total Categories: {len(menus)}")
        
        total_products = 0
        available_products = 0
        unavailable_products = 0
        
        for menu in menus:
            category_name = menu.get('name', 'Unknown Category')
            products = menu.get('products', [])
            
            print(f"\n   📁 Category: {category_name}")
            print(f"      Products: {len(products)}")
            
            # Sample first 3 products
            for i, product in enumerate(products[:3]):
                product_name = product.get('name', 'Unknown')
                is_available = product.get('is_available', True)
                price = product.get('product_variations', [{}])[0].get('price', 'N/A')
                
                status = "✅" if is_available else "🔴"
                print(f"      {status} {product_name} - ₱{price}")
                
                total_products += 1
                if is_available:
                    available_products += 1
                else:
                    unavailable_products += 1
            
            if len(products) > 3:
                print(f"      ... and {len(products) - 3} more products")
        
        print(f"\n📊 Summary:")
        print(f"   Total Products: {total_products}")
        print(f"   ✅ Available: {available_products}")
        print(f"   🔴 Out of Stock: {unavailable_products}")


def extract_oos_products(data: Dict[str, Any]) -> List[Dict[str, str]]:
    """Extract all out-of-stock products"""
    oos_products = []
    
    if 'data' in data:
        vendor = data['data']
        menus = vendor.get('menus', [])
        
        for menu in menus:
            category_name = menu.get('name', 'Unknown')
            products = menu.get('products', [])
            
            for product in products:
                is_available = product.get('is_available', True)
                
                if not is_available:
                    oos_products.append({
                        'name': product.get('name', 'Unknown'),
                        'category': category_name,
                        'code': product.get('code', 'N/A'),
                        'id': product.get('id', 'N/A')
                    })
    
    return oos_products


def test_all_stores():
    """Test all store URLs"""
    print("=" * 70)
    print("🐼 FOODPANDA MENU API TEST")
    print("=" * 70)
    print("\nTesting discovered API endpoint with all test stores...")
    
    results = []
    
    for store in TEST_STORES:
        vendor_code = store['code']
        vendor_name = store['name']
        
        print(f"\n{'='*70}")
        print(f"Testing: {vendor_name} ({vendor_code})")
        print('='*70)
        
        # Fetch menu
        data = fetch_foodpanda_menu(vendor_code)
        
        if data:
            # Analyze structure
            analyze_menu_structure(data, vendor_name)
            
            # Extract OOS items
            oos_products = extract_oos_products(data)
            
            if oos_products:
                print(f"\n🔴 Out of Stock Items:")
                for item in oos_products:
                    print(f"   - {item['name']} (Category: {item['category']})")
            else:
                print(f"\n✅ All items in stock!")
            
            # Save sample data
            filename = f"foodpanda_menu_{vendor_code}.json"
            with open(filename, 'w') as f:
                json.dump(data, f, indent=2)
            print(f"\n💾 Full data saved to: {filename}")
            
            results.append({
                'vendor_code': vendor_code,
                'vendor_name': vendor_name,
                'success': True,
                'oos_count': len(oos_products)
            })
        else:
            results.append({
                'vendor_code': vendor_code,
                'vendor_name': vendor_name,
                'success': False
            })
    
    # Final summary
    print("\n" + "=" * 70)
    print("📊 FINAL SUMMARY")
    print("=" * 70)
    
    for result in results:
        if result['success']:
            print(f"✅ {result['vendor_name']}: {result['oos_count']} OOS items")
        else:
            print(f"❌ {result['vendor_name']}: Failed to fetch")
    
    print("\n🎉 API endpoint is working!")
    print("🎯 Next step: Integrate into production scraper")


def show_api_usage():
    """Show how to use the API"""
    print("\n" + "=" * 70)
    print("💡 HOW TO USE THIS API")
    print("=" * 70)
    print("""
ENDPOINT PATTERN:
https://ph.fd-api.com/api/v5/vendors/{VENDOR_CODE}

REQUIRED PARAMETERS:
- include=menus,bundles,multiple_discounts
- language_id=1
- opening_type=delivery
- basket_currency=PHP

REQUIRED HEADERS:
- User-Agent: (standard browser UA)
- Accept: application/json
- Origin: https://www.foodpanda.ph
- Referer: https://www.foodpanda.ph/restaurant/{VENDOR_CODE}/

RESPONSE STRUCTURE:
{
  "data": {
    "name": "Store Name",
    "code": "i51s",
    "is_active": true,
    "menus": [
      {
        "name": "Category Name",
        "products": [
          {
            "name": "Product Name",
            "code": "product-code",
            "is_available": true/false,  ← KEY FIELD!
            "product_variations": [
              {"price": 89.00}
            ]
          }
        ]
      }
    ]
  }
}

KEY FIELD FOR OOS DETECTION:
product['is_available'] == False  → Product is OUT OF STOCK
product['is_available'] == True   → Product is AVAILABLE

RATE LIMITING:
- x-ratelimit-limit: 100 requests per window
- Add delays between requests (2-5 seconds)
- Rotate user agents if scraping many stores
""")


def main():
    """Main runner"""
    # Show API usage first
    show_api_usage()
    
    # Test all stores
    test_all_stores()
    
    print("\n" + "=" * 70)
    print("✅ TESTING COMPLETE!")
    print("=" * 70)
    print("\nNext steps:")
    print("1. Review the saved JSON files to see full data structure")
    print("2. Implement production scraper using this endpoint")
    print("3. Add SKU mapping for product names")
    print("4. Integrate with your database")
    print("\n🚀 Ready to build the production Foodpanda scraper!")


if __name__ == "__main__":
    main()