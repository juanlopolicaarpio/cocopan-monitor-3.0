#!/usr/bin/env python3
"""
GrabFood API Test Script
This will show you ALL the data available from GrabFood's API
including ratings, store status, menu items, etc.
"""
import json
import re
import time
import random
import requests
from urllib.parse import urlparse
from typing import Optional, Dict, Any

# Test with the URL from your screenshot
TEST_URL = "https://food.grab.com/ph/en/restaurant/cocopan-tejeros-delivery/2-C6K2GPUYKYT3LE"

class GrabFoodAPITester:
    def __init__(self):
        self.ph_latlng = "14.5995,120.9842"  # Manila center
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "en-PH,en;q=0.9",
            "Origin": "https://food.grab.com",
            "Referer": "https://food.grab.com/",
            "Connection": "keep-alive",
        }
    
    def extract_merchant_id(self, url: str) -> Optional[str]:
        """Extract merchant ID from GrabFood URL"""
        try:
            parsed = urlparse(url)
            # Extract ID pattern like "2-C6K2GPUYKYT3LE"
            match = re.search(r"/([0-9]-[A-Z0-9]+)$", parsed.path, re.IGNORECASE)
            return match.group(1) if match else None
        except Exception as e:
            print(f"Error extracting merchant ID: {e}")
            return None
    
    def test_api(self, url: str):
        """Test the GrabFood API and display all available data"""
        print("=" * 80)
        print("🔍 GRABFOOD API TEST")
        print("=" * 80)
        print(f"Testing URL: {url}")
        
        # Extract merchant ID
        merchant_id = self.extract_merchant_id(url)
        if not merchant_id:
            print("❌ Could not extract merchant ID from URL")
            return
        
        print(f"✅ Extracted merchant ID: {merchant_id}")
        
        # Test both API endpoints
        api_urls = [
            f"https://portal.grab.com/foodweb/v2/restaurant?merchantCode={merchant_id}&latlng={self.ph_latlng}",
            f"https://portal.grab.com/foodweb/v2/merchants/{merchant_id}?latlng={self.ph_latlng}"
        ]
        
        session = requests.Session()
        session.headers.update(self.headers)
        
        for i, api_url in enumerate(api_urls, 1):
            print(f"\n{'=' * 80}")
            print(f"📡 Testing API Endpoint #{i}")
            print(f"URL: {api_url}")
            print("=" * 80)
            
            try:
                # Add delay to avoid rate limiting
                time.sleep(random.uniform(1, 2))
                
                resp = session.get(api_url, timeout=15)
                print(f"Response Status: {resp.status_code}")
                
                if resp.status_code == 200:
                    data = resp.json()
                    
                    # Save full response to file for analysis
                    filename = f"grabfood_api_response_{i}.json"
                    with open(filename, 'w', encoding='utf-8') as f:
                        json.dump(data, f, indent=2, ensure_ascii=False)
                    print(f"✅ Full response saved to: {filename}")
                    
                    # Extract and display key information
                    self.display_key_data(data)
                    
                else:
                    print(f"❌ API returned status code: {resp.status_code}")
                    print(f"Response: {resp.text[:500]}")
                    
            except Exception as e:
                print(f"❌ Error calling API: {e}")
    
    def display_key_data(self, data: Dict[str, Any]):
        """Extract and display important fields from API response"""
        print("\n📊 KEY DATA EXTRACTED:")
        print("-" * 40)
        
        # Helper function to safely navigate nested dicts
        def get_nested(d, *keys, default="Not found"):
            for key in keys:
                if isinstance(d, dict):
                    d = d.get(key)
                    if d is None:
                        return default
                else:
                    return default
            return d
        
        # Try different paths where data might be
        roots = []
        if isinstance(data, dict):
            roots.append(data)
            if 'data' in data:
                roots.append(data['data'])
            if 'merchant' in data:
                roots.append(data['merchant'])
            if 'restaurant' in data:
                roots.append(data['restaurant'])
        
        for root in roots:
            if not isinstance(root, dict):
                continue
                
            # Store name
            store_name = (root.get('name') or 
                         root.get('displayName') or 
                         root.get('merchantName') or 
                         root.get('restaurantName'))
            if store_name:
                print(f"🏪 Store Name: {store_name}")
            
            # Store status/availability
            is_open = root.get('isOpen')
            is_available = root.get('isAvailable')
            is_accepting_orders = root.get('isAcceptingOrders')
            status = root.get('status')
            business_hours = root.get('businessHours') or root.get('openingHours')
            
            if is_open is not None:
                print(f"🔓 Is Open: {is_open}")
            if is_available is not None:
                print(f"✅ Is Available: {is_available}")
            if is_accepting_orders is not None:
                print(f"📦 Accepting Orders: {is_accepting_orders}")
            if status:
                print(f"📍 Status: {status}")
            if business_hours:
                print(f"🕐 Business Hours: {business_hours}")
            
            # RATING - Try multiple possible locations
            rating_locations = [
                root.get('rating'),
                root.get('averageRating'),
                root.get('avgRating'),
                get_nested(root, 'rating', 'average'),
                get_nested(root, 'rating', 'value'),
                get_nested(root, 'statistics', 'rating'),
                get_nested(root, 'statistics', 'averageRating'),
                get_nested(root, 'reviews', 'rating'),
                get_nested(root, 'reviews', 'averageRating'),
            ]
            
            for rating in rating_locations:
                if rating and rating != "Not found":
                    print(f"⭐ RATING FOUND: {rating}")
                    break
            
            # Review count
            review_count = (root.get('reviewCount') or 
                          root.get('totalReviews') or 
                          get_nested(root, 'rating', 'count') or
                          get_nested(root, 'statistics', 'reviewCount'))
            if review_count and review_count != "Not found":
                print(f"📝 Review Count: {review_count}")
            
            # Additional info
            cuisine = root.get('cuisine') or root.get('cuisineType')
            if cuisine:
                print(f"🍜 Cuisine: {cuisine}")
            
            # Delivery info
            delivery_fee = get_nested(root, 'deliveryFee')
            delivery_time = get_nested(root, 'estimatedDeliveryTime')
            if delivery_fee and delivery_fee != "Not found":
                print(f"🚚 Delivery Fee: {delivery_fee}")
            if delivery_time and delivery_time != "Not found":
                print(f"⏱️ Delivery Time: {delivery_time}")
            
            # Menu sections count
            menu = root.get('menu')
            if isinstance(menu, dict):
                categories = menu.get('categories') or menu.get('sections')
                if isinstance(categories, list):
                    print(f"📜 Menu Categories: {len(categories)}")
                    
                    # Count total items
                    total_items = 0
                    available_items = 0
                    for cat in categories:
                        if isinstance(cat, dict):
                            items = cat.get('items') or cat.get('products')
                            if isinstance(items, list):
                                total_items += len(items)
                                for item in items:
                                    if isinstance(item, dict) and item.get('available') == True:
                                        available_items += 1
                    
                    if total_items > 0:
                        print(f"🍽️ Total Menu Items: {total_items}")
                        print(f"✅ Available Items: {available_items}")
                        print(f"❌ Out of Stock Items: {total_items - available_items}")
        
        print("-" * 40)
        print("\n🔍 To see the complete data structure, check the saved JSON files")
        print("   Look for fields like: rating, averageRating, statistics, reviews")
        print("   The exact location varies based on GrabFood's API version")


def main():
    """Run the API test"""
    tester = GrabFoodAPITester()
    
    # Test with the URL from the screenshot
    tester.test_api(TEST_URL)
    
    print("\n" + "=" * 80)
    print("✅ TEST COMPLETE")
    print("Check the generated JSON files for the complete API response")
    print("Look for rating fields in the JSON to understand the data structure")
    print("=" * 80)
    
    # Test additional URLs if you want
    additional_urls = [
        # Add more URLs here if you want to test multiple stores
        "https://food.grab.com/ph/en/restaurant/cocopan-susano-road-novaliches-delivery/2-C6MEHBN2EX51EA",
         "https://food.grab.com/ph/en/restaurant/cocopan-evangelista-delivery/2-C7K2LTJDNE2VAE",
    ]
    
    for url in additional_urls:
        print(f"\n\n{'#' * 80}")
        print(f"Testing additional URL: {url}")
        tester.test_api(url)


if __name__ == "__main__":
    main()