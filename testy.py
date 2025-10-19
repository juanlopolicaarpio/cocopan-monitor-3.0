#!/usr/bin/env python3
"""
Foodpanda Comprehensive Test - FIXED PARSER with OOS Verification
Loads from branch_urls.json and provides detailed logging

Usage:
    python test_foodpanda_comprehensive.py
    python test_foodpanda_comprehensive.py --save-responses  # Save API responses
    python test_foodpanda_comprehensive.py --visible         # See browser
    python test_foodpanda_comprehensive.py --test-oos        # Test OOS detection only
"""

import json
import time
import logging
import random
import re
import sys
import os
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime

try:
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options
    HAS_SELENIUM = True
except ImportError:
    HAS_SELENIUM = False
    print("❌ Selenium required!")
    print("   Install: pip install selenium")
    exit(1)

# Configure detailed logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(f'foodpanda_test_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')
    ]
)
logger = logging.getLogger(__name__)


class FoodpandaComprehensiveTest:
    """
    Comprehensive tester with detailed logging and FIXED parser
    """
    
    def __init__(self, headless=True, save_responses=False):
        self.headless = headless
        self.save_responses = save_responses
        self.driver = None
        self.test_session_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Create results directory
        self.results_dir = f"foodpanda_test_results_{self.test_session_id}"
        os.makedirs(self.results_dir, exist_ok=True)
        
        logger.info("=" * 80)
        logger.info("🐼 FOODPANDA COMPREHENSIVE TEST - FIXED PARSER v2.0")
        logger.info("=" * 80)
        logger.info(f"Session ID: {self.test_session_id}")
        logger.info(f"Results directory: {self.results_dir}")
        logger.info(f"Headless mode: {headless}")
        logger.info(f"Save API responses: {save_responses}")
        logger.info("=" * 80)
    
    def load_foodpanda_urls(self) -> List[str]:
        """Load Foodpanda URLs from branch_urls.json"""
        try:
            urls_file = 'branch_urls.json'
            logger.info(f"📋 Loading URLs from: {urls_file}")
            
            with open(urls_file) as f:
                data = json.load(f)
                all_urls = data.get('urls', [])
                
                # Filter to only Foodpanda
                foodpanda_urls = [url for url in all_urls if 'foodpanda' in url.lower()]
                grabfood_urls = [url for url in all_urls if 'grab.com' in url.lower()]
                
                logger.info(f"✅ Loaded {len(all_urls)} total URLs")
                logger.info(f"🐼 Found {len(foodpanda_urls)} Foodpanda URLs")
                logger.info(f"🛒 Found {len(grabfood_urls)} GrabFood URLs")
                logger.info("")
                logger.info("📋 Foodpanda URLs to test:")
                for i, url in enumerate(foodpanda_urls, 1):
                    logger.info(f"   {i}. {url}")
                logger.info("")
                
                return foodpanda_urls
                
        except FileNotFoundError:
            logger.error(f"❌ File not found: {urls_file}")
            logger.info("💡 Please ensure branch_urls.json exists in current directory")
            return []
        except Exception as e:
            logger.error(f"❌ Error loading URLs: {e}")
            return []
    
    def _setup_driver(self):
        """Setup Chrome with detailed logging"""
        logger.info("🌐 Setting up Chrome driver...")
        
        chrome_options = Options()
        
        if self.headless:
            chrome_options.add_argument('--headless')
            logger.info("   Mode: Headless (invisible)")
        else:
            logger.info("   Mode: Visible browser")
        
        # Anti-detection
        chrome_options.add_argument('--disable-blink-features=AutomationControlled')
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option('useAutomationExtension', False)
        
        # Enable performance logging
        chrome_options.set_capability('goog:loggingPrefs', {'performance': 'ALL'})
        
        # User agent
        user_agent = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        chrome_options.add_argument(f'user-agent={user_agent}')
        logger.info(f"   User-Agent: {user_agent[:50]}...")
        
        driver = webdriver.Chrome(options=chrome_options)
        driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        
        logger.info("   ✅ Chrome driver ready")
        return driver
    
    def extract_vendor_code(self, url: str) -> Optional[str]:
        """Extract vendor code from URL"""
        try:
            match = re.search(r'/restaurant/([a-z0-9]+)/', url, re.IGNORECASE)
            if match:
                code = match.group(1)
                logger.debug(f"   Vendor code extracted: {code}")
                return code
            else:
                logger.error(f"   ❌ Could not extract vendor code from: {url}")
                return None
        except Exception as e:
            logger.error(f"   ❌ Error extracting vendor code: {e}")
            return None
    
    def _find_menu_api_response(self, logs: List[Dict]) -> Optional[Dict]:
        """Find menu API response with detailed logging"""
        logger.debug(f"   📊 Analyzing {len(logs)} log entries...")
        
        api_calls_found = []
        
        for log in logs:
            try:
                message = json.loads(log['message'])['message']
                
                if message.get('method') == 'Network.responseReceived':
                    response = message.get('params', {}).get('response', {})
                    url = response.get('url', '')
                    status = response.get('status')
                    
                    # Log all API calls for debugging
                    if '/api/' in url and status == 200:
                        api_calls_found.append({
                            'url': url,
                            'status': status
                        })
                    
                    # Check if this is our menu API
                    if ('/api/v5/vendors/' in url and 
                        'include=menus' in url and
                        status == 200):
                        
                        request_id = message['params']['requestId']
                        logger.info(f"   ✅ Found menu API response!")
                        logger.debug(f"      URL: {url}")
                        logger.debug(f"      Status: {status}")
                        logger.debug(f"      Request ID: {request_id}")
                        
                        return {'request_id': request_id, 'url': url}
            except:
                continue
        
        # If not found, log what we did find
        if api_calls_found:
            logger.warning(f"   ⚠️ Menu API not found. Found {len(api_calls_found)} other API calls:")
            for call in api_calls_found[:5]:  # Show first 5
                logger.debug(f"      - {call['url'][:100]}")
        else:
            logger.warning(f"   ⚠️ No API calls found in logs")
        
        return None
    
    def _get_response_body(self, request_id: str) -> Optional[Dict]:
        """Get response body with error handling"""
        try:
            logger.debug(f"   📥 Fetching response body for request: {request_id}")
            
            response_body = self.driver.execute_cdp_cmd(
                'Network.getResponseBody',
                {'requestId': request_id}
            )
            
            if 'body' in response_body:
                data = json.loads(response_body['body'])
                
                # Log structure
                logger.debug(f"   ✅ Response body retrieved")
                logger.debug(f"      Top-level keys: {list(data.keys())}")
                
                if 'data' in data:
                    vendor_data = data['data']
                    logger.debug(f"      Vendor keys: {list(vendor_data.keys())[:10]}")
                
                return data
            else:
                logger.warning(f"   ⚠️ No body in response")
                return None
                
        except Exception as e:
            logger.error(f"   ❌ Error getting response body: {e}")
            return None
    
    def _debug_payload_structure(self, menu_data: Dict):
        """
        Debug helper - call this when total_products = 0 to diagnose schema issues
        """
        logger.info("🔍 DEBUGGING PAYLOAD STRUCTURE:")
        
        try:
            data = menu_data.get('data', {})
            logger.info(f"   data keys: {list(data.keys())}")
            
            # Check menus
            menus = data.get('menus', [])
            logger.info(f"   menus count: {len(menus)}")
            
            if menus:
                menu0 = menus[0]
                logger.info(f"   menus[0] keys: {list(menu0.keys())}")
                logger.info(f"   menus[0].name: {menu0.get('name')}")
                
                # Check for menu_categories (CORRECT KEY)
                cats = menu0.get('menu_categories', [])
                logger.info(f"   menus[0].menu_categories count: {len(cats)}")
                
                if cats:
                    logger.info(f"   categories[0] keys: {list(cats[0].keys())}")
                    logger.info(f"   categories[0].name: {cats[0].get('name')}")
                    logger.info(f"   categories[0].is_popular_category: {cats[0].get('is_popular_category')}")
                    
                    prods = cats[0].get('products', []) or cats[0].get('items', [])
                    logger.info(f"   categories[0].products count: {len(prods)}")
                    
                    if prods:
                        prod0 = prods[0]
                        logger.info(f"   products[0] keys: {list(prod0.keys())}")
                        logger.info(f"   products[0].name: {prod0.get('name')}")
                        logger.info(f"   products[0].is_sold_out: {prod0.get('is_sold_out')}")
                        logger.info(f"   products[0].is_available: {prod0.get('is_available')}")
                        logger.info(f"   products[0].sold_out_options: {prod0.get('sold_out_options')}")
                        
                        # Check variations
                        variations = prod0.get('product_variations', [])
                        logger.info(f"   products[0].product_variations count: {len(variations)}")
                        if variations:
                            logger.info(f"   variation[0] keys: {list(variations[0].keys())}")
                            logger.info(f"   variation[0] has is_sold_out: {'is_sold_out' in variations[0]}")
                
                # Check if menus directly have products (legacy)
                prods = menu0.get('products', [])
                logger.info(f"   menus[0].products count: {len(prods)}")
            
            # Check direct categories
            cats = data.get('categories', [])
            logger.info(f"   data.categories count: {len(cats)}")
            
        except Exception as e:
            logger.error(f"   Debug failed: {e}")
    
    def scrape_store_detailed(self, store_url: str, store_index: int, total_stores: int) -> Dict[str, Any]:
        """
        Scrape single store with comprehensive logging
        """
        logger.info("")
        logger.info("=" * 80)
        logger.info(f"📍 STORE [{store_index}/{total_stores}]: {store_url}")
        logger.info("=" * 80)
        
        result = {
            'store_url': store_url,
            'store_index': store_index,
            'success': False,
            'store_name': None,
            'vendor_code': None,
            'total_products': 0,
            'total_categories': 0,
            'oos_items': [],
            'oos_count': 0,
            'available_count': 0,
            'error': None,
            'timestamp': datetime.now().isoformat(),
            'scrape_duration': 0
        }
        
        start_time = time.time()
        
        try:
            # Extract vendor code
            vendor_code = self.extract_vendor_code(store_url)
            if not vendor_code:
                result['error'] = "Could not extract vendor code"
                logger.error(f"❌ Failed: Could not extract vendor code")
                return result
            
            result['vendor_code'] = vendor_code
            logger.info(f"🔑 Vendor code: {vendor_code}")
            
            # Setup driver if needed
            if not self.driver:
                self.driver = self._setup_driver()
            
            # Load page
            logger.info(f"🌐 Loading page...")
            page_start = time.time()
            self.driver.get(store_url)
            
            # Wait for page to load
            wait_time = 15
            logger.info(f"⏳ Waiting {wait_time} seconds for page to fully load...")
            time.sleep(wait_time)
            page_load_time = time.time() - page_start
            logger.info(f"✅ Page loaded in {page_load_time:.1f} seconds")
            
            # Get logs
            logger.info(f"📊 Analyzing network logs...")
            logs = self.driver.get_log('performance')
            logger.debug(f"   Retrieved {len(logs)} log entries")
            
            # Find menu API response
            api_info = self._find_menu_api_response(logs)
            
            if not api_info:
                result['error'] = "Menu API response not found in logs"
                logger.error(f"❌ Failed: Menu API response not found")
                logger.info(f"💡 Possible reasons:")
                logger.info(f"   - Store is offline")
                logger.info(f"   - Page didn't load completely")
                logger.info(f"   - API endpoint changed")
                return result
            
            # Get response body
            menu_data = self._get_response_body(api_info['request_id'])
            
            if not menu_data:
                result['error'] = "Could not extract menu data"
                logger.error(f"❌ Failed: Could not extract menu data")
                return result
            
            # Debug payload structure if requested
            if self.save_responses:
                self._debug_payload_structure(menu_data)
            
            # Save raw response if requested
            if self.save_responses:
                response_file = os.path.join(
                    self.results_dir,
                    f"response_{vendor_code}.json"
                )
                with open(response_file, 'w') as f:
                    json.dump(menu_data, f, indent=2)
                logger.info(f"💾 Raw API response saved to: {response_file}")
            
            # Extract store info
            store_name = self._extract_store_name(menu_data)
            result['store_name'] = store_name
            logger.info(f"🏪 Store name: {store_name}")
            
            # Extract and analyze menu
            oos_items, stats = self._extract_oos_items_detailed(menu_data)
            
            result['total_categories'] = stats['total_categories']
            result['total_products'] = stats['total_products']
            result['oos_count'] = stats['oos_count']
            result['available_count'] = stats['available_count']
            result['oos_items'] = oos_items
            result['success'] = True
            
            # Log summary
            logger.info("")
            logger.info("📊 SCRAPE SUMMARY:")
            logger.info(f"   Store: {store_name}")
            logger.info(f"   Categories: {stats['total_categories']}")
            logger.info(f"   Total products: {stats['total_products']}")
            logger.info(f"   ✅ Available: {stats['available_count']}")
            logger.info(f"   🔴 Out of stock: {stats['oos_count']}")
            
            if oos_items:
                logger.info("")
                logger.info("🔴 OUT OF STOCK ITEMS:")
                for item in oos_items:
                    logger.info(f"   - {item['name']} (Category: {item['category']})")
                    if 'oos_reason' in item:
                        logger.info(f"     Reason: {item['oos_reason']}")
            else:
                logger.info("")
                logger.info("✅ All items in stock!")
            
        except Exception as e:
            result['error'] = str(e)
            logger.error(f"❌ EXCEPTION: {e}", exc_info=True)
        
        finally:
            result['scrape_duration'] = time.time() - start_time
            logger.info("")
            logger.info(f"⏱️ Scrape completed in {result['scrape_duration']:.1f} seconds")
        
        return result
    
    def _extract_store_name(self, menu_data: Dict) -> str:
        """Extract store name"""
        try:
            if 'data' in menu_data:
                name = menu_data['data'].get('name')
                if name:
                    return name.strip()
        except Exception as e:
            logger.debug(f"Error extracting name: {e}")
        
        return "Unknown Store"
    
    def _extract_oos_items_detailed(self, menu_data: Dict) -> Tuple[List[Dict], Dict]:
        """
        Extract OOS items with CORRECT nesting path
        
        CRITICAL FIX: Use 'menu_categories' instead of 'categories'
        
        Fixed bugs:
        1. Drill down to menus[0].menu_categories[] (CORRECTED KEY!)
        2. No popularity filtering
        3. Check both 'products' and 'items' keys
        4. Strip whitespace from names
        5. Check multiple OOS indicators
        """
        oos_items = []
        stats = {
            'total_categories': 0,
            'total_products': 0,
            'oos_count': 0,
            'available_count': 0,
            'oos_indicators_found': {}  # Track which OOS fields we find
        }
        
        try:
            if 'data' not in menu_data:
                logger.warning("   ⚠️ No 'data' key in menu response")
                return oos_items, stats
            
            vendor = menu_data['data']
            
            # FIX 1: Get categories from the correct path
            # Try multiple paths since schema varies
            categories = []
            
            # Path A: data.menus[0].menu_categories[] (CRITICAL FIX!)
            menus = vendor.get('menus', [])
            if menus and isinstance(menus, list) and len(menus) > 0:
                categories = menus[0].get('menu_categories', [])  # FIXED: was 'categories'
                logger.debug(f"   Found {len(categories)} categories via menus[0].menu_categories")
            
            # Path B: data.categories[] (fallback)
            if not categories:
                categories = vendor.get('categories', [])
                logger.debug(f"   Found {len(categories)} categories via data.categories")
            
            # Path C: Treat menus themselves as categories (legacy)
            if not categories and menus:
                # Check if menus directly have products (old schema)
                if any(m.get('products') or m.get('items') for m in menus):
                    categories = menus
                    logger.debug(f"   Using {len(menus)} menus as categories (legacy schema)")
            
            if not categories:
                logger.error("   ❌ Could not find categories in any expected path")
                logger.debug(f"   Available keys: {list(vendor.keys())}")
                return oos_items, stats
            
            stats['total_categories'] = len(categories)
            logger.info(f"   📁 Processing {len(categories)} categories...")
            
            # FIX 2: NO popularity filtering - process ALL categories
            for category in categories:
                category_name = category.get('name', 'Unknown').strip()
                
                # FIX 3: Check both 'products' and 'items' keys
                products = category.get('products', []) or category.get('items', [])
                
                if not products:
                    logger.debug(f"      Category '{category_name}': 0 products (skipping)")
                    continue
                
                logger.debug(f"      Category '{category_name}': {len(products)} products")
                
                for product in products:
                    stats['total_products'] += 1
                    
                    # FIX 4: Strip whitespace (handles trailing tabs, etc.)
                    product_name = product.get('name', '').strip()
                    if not product_name:
                        logger.debug(f"         ⚠️ Skipping product with empty name")
                        continue
                    
                    product_code = product.get('code', 'N/A')
                    
                    # CRITICAL: Check ALL possible OOS indicators
                    # Primary check: product-level is_sold_out
                    is_sold_out_flag = product.get('is_sold_out', False)
                    is_available_flag = product.get('is_available', True)
                    
                    # Secondary check: nested availability object
                    availability_obj = product.get('availability', {})
                    availability_sold_out = availability_obj.get('is_sold_out', False) if availability_obj else False
                    
                    # Tertiary check: sold_out_options (might indicate partial OOS)
                    sold_out_options = product.get('sold_out_options')
                    has_sold_out_options = sold_out_options is not None
                    
                    # Quaternary check: variation-level sold-out
                    variations = product.get('product_variations', [])
                    all_variations_oos = False
                    if variations:
                        variations_with_oos_flag = [v for v in variations if 'is_sold_out' in v]
                        if variations_with_oos_flag:
                            all_variations_oos = all(v.get('is_sold_out', False) for v in variations_with_oos_flag)
                    
                    # Track which indicators we're finding (for debugging)
                    if is_sold_out_flag:
                        stats['oos_indicators_found']['product_is_sold_out'] = stats['oos_indicators_found'].get('product_is_sold_out', 0) + 1
                    if not is_available_flag:
                        stats['oos_indicators_found']['product_not_available'] = stats['oos_indicators_found'].get('product_not_available', 0) + 1
                    if availability_sold_out:
                        stats['oos_indicators_found']['availability_sold_out'] = stats['oos_indicators_found'].get('availability_sold_out', 0) + 1
                    if has_sold_out_options:
                        stats['oos_indicators_found']['has_sold_out_options'] = stats['oos_indicators_found'].get('has_sold_out_options', 0) + 1
                    if all_variations_oos:
                        stats['oos_indicators_found']['all_variations_oos'] = stats['oos_indicators_found'].get('all_variations_oos', 0) + 1
                    
                    # Determine if product is OOS (any indicator = OOS)
                    is_oos = (
                        is_sold_out_flag or
                        not is_available_flag or
                        availability_sold_out or
                        all_variations_oos
                        # Note: NOT checking sold_out_options as it might just be configuration
                    )
                    
                    if is_oos:
                        stats['oos_count'] += 1
                        oos_reason = []
                        if is_sold_out_flag:
                            oos_reason.append('is_sold_out=true')
                        if not is_available_flag:
                            oos_reason.append('is_available=false')
                        if availability_sold_out:
                            oos_reason.append('availability.is_sold_out=true')
                        if all_variations_oos:
                            oos_reason.append('all_variations_oos')
                        
                        oos_items.append({
                            'name': product_name,
                            'category': category_name,
                            'code': product_code,
                            'is_available': False,
                            'oos_reason': ', '.join(oos_reason)
                        })
                        logger.debug(f"         🔴 OOS: {product_name} ({', '.join(oos_reason)})")
                    else:
                        stats['available_count'] += 1
            
            # Log OOS indicators summary
            if stats['oos_indicators_found']:
                logger.info(f"   🔍 OOS Indicators Found: {stats['oos_indicators_found']}")
            
            # FIX 5: Sanity check - if we found categories with products but total is 0
            if stats['total_products'] == 0 and categories:
                # Check if raw data has products we missed
                sample_products = []
                for cat in categories[:3]:  # Check first 3 categories
                    prods = cat.get('products', []) or cat.get('items', [])
                    if prods:
                        sample_products.extend(prods[:2])  # First 2 products
                
                if sample_products:
                    logger.error("   ❌ PARSE FAILURE: Found categories with products but computed 0!")
                    logger.error(f"   Sample products in raw JSON: {[p.get('name') for p in sample_products]}")
                    logger.error(f"   Category keys: {list(categories[0].keys()) if categories else 'N/A'}")
        
        except Exception as e:
            logger.error(f"   ❌ Error parsing menu: {e}", exc_info=True)
        
        return oos_items, stats
    
    def test_oos_detection(self, test_store_url: str = None):
        """
        Test OOS detection on a single store
        Returns dict with detailed findings
        """
        if not test_store_url:
            # Use first URL from branch_urls.json
            urls = self.load_foodpanda_urls()
            if not urls:
                return {"error": "No URLs found"}
            test_store_url = urls[0]
        
        logger.info("=" * 80)
        logger.info("🧪 TESTING OOS DETECTION")
        logger.info("=" * 80)
        logger.info(f"Test URL: {test_store_url}")
        
        result = self.scrape_store_detailed(test_store_url, 1, 1)
        
        logger.info("")
        logger.info("🔍 OOS DETECTION TEST RESULTS:")
        logger.info(f"   Products found: {result['total_products']}")
        logger.info(f"   OOS items: {result['oos_count']}")
        logger.info(f"   Available: {result['available_count']}")
        
        if result['total_products'] == 0:
            logger.error("   ❌ NO PRODUCTS FOUND - Parser may be broken!")
            return result
        
        if result['oos_count'] == 0:
            logger.warning("   ⚠️ No OOS items found")
            logger.warning("   This could mean:")
            logger.warning("   1. All items are genuinely in stock (good!)")
            logger.warning("   2. OOS detection is not working (bad!)")
            logger.warning("")
            logger.warning("   💡 To verify, manually check the store and look for:")
            logger.warning("      - Grayed out items")
            logger.warning("      - 'Sold out' badges")
            logger.warning("      - Items you can't add to cart")
        else:
            logger.info(f"   ✅ OOS detection is working!")
            logger.info(f"   Found {result['oos_count']} out-of-stock items:")
            for item in result['oos_items']:
                logger.info(f"      - {item['name']} (Category: {item['category']})")
                logger.info(f"        Reason: {item.get('oos_reason', 'N/A')}")
        
        # Always close browser after test
        if self.driver:
            logger.info("🔒 Closing browser...")
            self.driver.quit()
            self.driver = None
        
        return result
    
    def run_comprehensive_test(self):
        """
        Run comprehensive test on all stores
        """
        # Load URLs
        store_urls = self.load_foodpanda_urls()
        
        if not store_urls:
            logger.error("❌ No Foodpanda URLs found!")
            logger.info("💡 Make sure branch_urls.json exists and contains Foodpanda URLs")
            return
        
        logger.info(f"🚀 Starting comprehensive test of {len(store_urls)} stores")
        logger.info("")
        
        # Test all stores
        results = []
        start_time = time.time()
        
        try:
            for i, url in enumerate(store_urls, 1):
                result = self.scrape_store_detailed(url, i, len(store_urls))
                results.append(result)
                
                # Delay between stores
                if i < len(store_urls):
                    delay = random.uniform(2, 4)
                    logger.info(f"⏳ Waiting {delay:.1f} seconds before next store...")
                    time.sleep(delay)
        
        finally:
            # Always close browser
            if self.driver:
                logger.info("")
                logger.info("🔒 Closing browser...")
                self.driver.quit()
                self.driver = None
        
        # Save results
        self._save_final_results(results, time.time() - start_time)
        
        return results
    
    def _save_final_results(self, results: List[Dict], total_time: float):
        """Save final results and generate summary"""
        logger.info("")
        logger.info("=" * 80)
        logger.info("📊 FINAL RESULTS")
        logger.info("=" * 80)
        
        # Calculate statistics
        total_stores = len(results)
        successful = sum(1 for r in results if r['success'])
        failed = total_stores - successful
        total_oos = sum(r['oos_count'] for r in results if r['success'])
        total_products = sum(r['total_products'] for r in results if r['success'])
        total_available = sum(r['available_count'] for r in results if r['success'])
        
        logger.info(f"⏱️ Total time: {total_time/60:.1f} minutes")
        logger.info(f"📋 Total stores tested: {total_stores}")
        logger.info(f"✅ Successful: {successful}")
        logger.info(f"❌ Failed: {failed}")
        logger.info(f"📦 Total products: {total_products}")
        logger.info(f"✅ Available: {total_available}")
        logger.info(f"🔴 Out of stock: {total_oos}")
        
        if total_products > 0:
            oos_percentage = (total_oos / total_products) * 100
            logger.info(f"📊 OOS rate: {oos_percentage:.1f}%")
        
        # Show per-store summary
        logger.info("")
        logger.info("📋 PER-STORE SUMMARY:")
        for r in results:
            if r['success']:
                logger.info(f"   ✅ {r['store_name']}: {r['oos_count']}/{r['total_products']} OOS")
            else:
                logger.info(f"   ❌ {r['store_url']}: {r['error']}")
        
        # Save detailed results
        results_file = os.path.join(self.results_dir, 'test_results.json')
        with open(results_file, 'w') as f:
            json.dump({
                'test_session_id': self.test_session_id,
                'total_time': total_time,
                'total_stores': total_stores,
                'successful': successful,
                'failed': failed,
                'total_products': total_products,
                'total_oos': total_oos,
                'stores': results
            }, f, indent=2)
        
        logger.info("")
        logger.info(f"💾 Detailed results saved to: {results_file}")
        logger.info(f"📁 All files saved in: {self.results_dir}")
        logger.info("")
        logger.info("=" * 80)
        logger.info("✅ COMPREHENSIVE TEST COMPLETE!")
        logger.info("=" * 80)


def main():
    """Main runner"""
    # Parse command line args
    headless = '--visible' not in sys.argv
    save_responses = '--save-responses' in sys.argv
    test_oos_only = '--test-oos' in sys.argv
    
    print("=" * 80)
    print("🐼 FOODPANDA COMPREHENSIVE TEST - FIXED PARSER v2.0")
    print("=" * 80)
    print("")
    
    if test_oos_only:
        print("🧪 OOS DETECTION TEST MODE")
        print("This will test OOS detection on the first store only.")
        print("")
        input("Press ENTER to start OOS test...")
        
        tester = FoodpandaComprehensiveTest(
            headless=False,  # Show browser for testing
            save_responses=True
        )
        
        result = tester.test_oos_detection()
        
        print("")
        print("=" * 80)
        print("🧪 OOS TEST COMPLETE")
        print("=" * 80)
        print(f"Products found: {result.get('total_products', 0)}")
        print(f"OOS items: {result.get('oos_count', 0)}")
        
        if result.get('total_products', 0) > 0 and result.get('oos_count', 0) == 0:
            print("")
            print("⚠️  No OOS items detected!")
            print("Please manually verify by visiting:")
            print(f"   {result.get('store_url', 'N/A')}")
            print("")
            print("Look for:")
            print("   • Grayed out items")
            print("   • 'Sold out' badges")
            print("   • Items that can't be added to cart")
        
        return
    
    print("This will test ALL Foodpanda stores from branch_urls.json")
    print("with comprehensive logging and detailed results.")
    print("")
    print("Options:")
    print(f"  --visible         Show browser window (default: headless)")
    print(f"  --save-responses  Save raw API responses (default: no)")
    print(f"  --test-oos        Test OOS detection only on first store")
    print("")
    
    if not headless:
        print("⚠️  Running in VISIBLE mode - you'll see the browser")
    
    if save_responses:
        print("💾 Will save raw API responses for debugging")
    
    print("")
    input("Press ENTER to start full test...")
    
    # Run test
    tester = FoodpandaComprehensiveTest(
        headless=headless,
        save_responses=save_responses
    )
    
    results = tester.run_comprehensive_test()
    
    print("")
    print("🎉 Test complete! Check the logs and results directory for details.")


if __name__ == "__main__":
    main()