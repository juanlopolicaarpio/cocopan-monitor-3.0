#!/usr/bin/env python3
"""
Foodpanda Headless Scraper - FIXED for Mac
Fixes "Binary Location Must be a String" error

This version:
- ✅ Works in headless mode
- ✅ Fixes Mac Chrome binary location issue
- ✅ Falls back to regular Selenium if needed
- ✅ Longer delays to avoid CAPTCHA
"""

import json
import time
import logging
import random
import re
import sys
import os
import glob
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime

# Try undetected-chromedriver first, fallback to regular selenium
try:
    import undetected_chromedriver as uc
    HAS_UC = True
except ImportError:
    HAS_UC = False

try:
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options as ChromeOptions
    from selenium.webdriver.chrome.service import Service
    HAS_SELENIUM = True
except ImportError:
    HAS_SELENIUM = False
    print("❌ Selenium required!")
    print("   Install: pip install selenium")
    exit(1)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(f'foodpanda_headless_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log')
    ]
)
logger = logging.getLogger(__name__)


def find_chrome_binary():
    """
    Find Chrome binary on Mac
    Fixes the "Binary Location Must be a String" error
    """
    possible_paths = [
        '/Applications/Google Chrome.app/Contents/MacOS/Google Chrome',
        '/Applications/Chromium.app/Contents/MacOS/Chromium',
        '/Applications/Google Chrome Canary.app/Contents/MacOS/Google Chrome Canary',
        os.path.expanduser('~/Applications/Google Chrome.app/Contents/MacOS/Google Chrome'),
    ]
    
    for path in possible_paths:
        if os.path.exists(path):
            logger.info(f"   ✅ Found Chrome at: {path}")
            return path
    
    logger.warning("   ⚠️ Chrome not found in standard locations")
    return None


class FoodpandaHeadlessScraper:
    """
    Headless scraper with binary location fix
    """
    
    def __init__(self, save_responses=False, use_undetected=True):
        self.save_responses = save_responses
        self.use_undetected = use_undetected and HAS_UC
        self.driver = None
        self.test_session_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        self.results_dir = f"foodpanda_headless_{self.test_session_id}"
        os.makedirs(self.results_dir, exist_ok=True)
        
        logger.info("=" * 80)
        logger.info("🐼 FOODPANDA HEADLESS SCRAPER (FIXED)")
        logger.info("=" * 80)
        logger.info(f"Mode: Headless")
        logger.info(f"Using: {'undetected-chromedriver' if self.use_undetected else 'regular Selenium'}")
        logger.info(f"Delays: 15-25 seconds between stores (anti-CAPTCHA)")
        logger.info("=" * 80)
    
    def load_foodpanda_urls(self) -> List[str]:
        """Load URLs"""
        try:
            with open('branch_urls.json') as f:
                data = json.load(f)
                foodpanda_urls = [url for url in data.get('urls', []) if 'foodpanda' in url.lower()]
                logger.info(f"📋 Loaded {len(foodpanda_urls)} Foodpanda URLs")
                return foodpanda_urls
        except Exception as e:
            logger.error(f"❌ Error loading URLs: {e}")
            return []
    
    def _setup_driver_undetected(self):
        """
        Setup undetected-chromedriver with binary location fix
        """
        logger.info("🥷 Setting up undetected-chromedriver (headless)...")
        
        try:
            options = uc.ChromeOptions()
            
            # Find Chrome binary
            chrome_binary = find_chrome_binary()
            if chrome_binary:
                options.binary_location = chrome_binary
            
            # Headless arguments
            options.add_argument('--headless=new')  # New headless mode
            options.add_argument('--disable-gpu')
            options.add_argument('--no-sandbox')
            options.add_argument('--disable-dev-shm-usage')
            options.add_argument('--disable-blink-features=AutomationControlled')
            
            # Window size for headless
            options.add_argument('--window-size=1920,1080')
            
            # Enable performance logging
            options.set_capability('goog:loggingPrefs', {'performance': 'ALL'})
            
            # Create driver
            driver = uc.Chrome(
                options=options,
                version_main=None,
                use_subprocess=True
            )
            
            logger.info("   ✅ Undetected ChromeDriver ready (headless)")
            return driver
            
        except Exception as e:
            logger.error(f"   ❌ Undetected-chromedriver failed: {e}")
            logger.info("   🔄 Falling back to regular Selenium...")
            return None
    
    def _setup_driver_regular(self):
        """
        Setup regular Selenium as fallback
        """
        logger.info("🌐 Setting up regular Selenium (headless)...")
        
        try:
            options = ChromeOptions()
            
            # Find Chrome binary
            chrome_binary = find_chrome_binary()
            if chrome_binary:
                options.binary_location = chrome_binary
            
            # Headless
            options.add_argument('--headless=new')
            options.add_argument('--disable-gpu')
            options.add_argument('--no-sandbox')
            options.add_argument('--disable-dev-shm-usage')
            options.add_argument('--disable-blink-features=AutomationControlled')
            options.add_argument('--window-size=1920,1080')
            
            # Anti-detection
            options.add_experimental_option("excludeSwitches", ["enable-automation"])
            options.add_experimental_option('useAutomationExtension', False)
            
            # Enable logging
            options.set_capability('goog:loggingPrefs', {'performance': 'ALL'})
            
            driver = webdriver.Chrome(options=options)
            
            # Override webdriver property
            driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            
            logger.info("   ✅ Regular Selenium ready (headless)")
            return driver
            
        except Exception as e:
            logger.error(f"   ❌ Regular Selenium also failed: {e}")
            raise
    
    def _setup_driver(self):
        """
        Try undetected first, fallback to regular
        """
        # Try undetected-chromedriver first
        if self.use_undetected:
            driver = self._setup_driver_undetected()
            if driver:
                return driver
        
        # Fallback to regular Selenium
        return self._setup_driver_regular()
    
    def extract_vendor_code(self, url: str) -> Optional[str]:
        """Extract vendor code"""
        match = re.search(r'/restaurant/([a-z0-9]+)/', url, re.IGNORECASE)
        return match.group(1) if match else None
    
    def _find_menu_api_response(self, logs: List[Dict]) -> Optional[Dict]:
        """Find menu API in logs"""
        for log in logs:
            try:
                message = json.loads(log['message'])['message']
                
                if message.get('method') == 'Network.responseReceived':
                    response = message.get('params', {}).get('response', {})
                    url = response.get('url', '')
                    
                    if ('/api/v5/vendors/' in url and 
                        'include=menus' in url and
                        response.get('status') == 200):
                        
                        request_id = message['params']['requestId']
                        return {'request_id': request_id, 'url': url}
            except:
                continue
        
        return None
    
    def _get_response_body(self, request_id: str) -> Optional[Dict]:
        """Get response body"""
        try:
            response_body = self.driver.execute_cdp_cmd(
                'Network.getResponseBody',
                {'requestId': request_id}
            )
            
            if 'body' in response_body:
                return json.loads(response_body['body'])
        except Exception as e:
            logger.debug(f"   Error getting response: {e}")
        
        return None
    
    def _extract_oos_items_FIXED(self, menu_data: Dict) -> Tuple[List[Dict], Dict]:
        """
        FIXED parser - correct JSON path
        """
        oos_items = []
        stats = {
            'total_categories': 0,
            'total_products': 0,
            'oos_count': 0,
            'available_count': 0
        }
        
        try:
            if 'data' not in menu_data:
                return oos_items, stats
            
            vendor = menu_data['data']
            menus = vendor.get('menus', [])
            
            if not menus:
                return oos_items, stats
            
            # Get categories from first menu
            categories = []
            if len(menus) > 0:
                first_menu = menus[0]
                
                if 'categories' in first_menu:
                    categories = first_menu['categories']
                elif 'products' in first_menu:
                    categories = menus
            
            if not categories and 'categories' in vendor:
                categories = vendor['categories']
            
            if not categories:
                return oos_items, stats
            
            stats['total_categories'] = len(categories)
            
            # Process all categories
            for category in categories:
                category_name = category.get('name', 'Unknown')
                products = category.get('products', []) or category.get('items', [])
                
                for product in products:
                    stats['total_products'] += 1
                    
                    product_name = product.get('name', '').strip()
                    if not product_name:
                        continue
                    
                    # Check OOS
                    is_sold_out = product.get('is_sold_out', False)
                    is_available = product.get('is_available', True)
                    is_oos = is_sold_out or (not is_available)
                    
                    if is_oos:
                        stats['oos_count'] += 1
                        oos_items.append({
                            'name': product_name,
                            'category': category_name,
                            'code': product.get('code', 'N/A')
                        })
                    else:
                        stats['available_count'] += 1
        
        except Exception as e:
            logger.error(f"   Parse error: {e}")
        
        return oos_items, stats
    
    def scrape_store(self, store_url: str, store_index: int, total_stores: int) -> Dict[str, Any]:
        """Scrape single store"""
        logger.info("")
        logger.info("=" * 80)
        logger.info(f"📍 STORE [{store_index}/{total_stores}]: {store_url}")
        logger.info("=" * 80)
        
        result = {
            'store_url': store_url,
            'success': False,
            'store_name': None,
            'vendor_code': None,
            'total_products': 0,
            'oos_count': 0,
            'oos_items': [],
            'error': None
        }
        
        try:
            vendor_code = self.extract_vendor_code(store_url)
            if not vendor_code:
                result['error'] = "No vendor code"
                return result
            
            result['vendor_code'] = vendor_code
            logger.info(f"🔑 Vendor: {vendor_code}")
            
            if not self.driver:
                self.driver = self._setup_driver()
            
            logger.info(f"🌐 Loading page...")
            self.driver.get(store_url)
            
            # Wait for page load
            wait_time = 20
            logger.info(f"⏳ Waiting {wait_time} seconds...")
            time.sleep(wait_time)
            
            # Get logs
            logs = self.driver.get_log('performance')
            api_info = self._find_menu_api_response(logs)
            
            if not api_info:
                result['error'] = "API not found"
                logger.warning("   ⚠️ Menu API not found in logs")
                return result
            
            logger.info(f"   ✅ Found API response")
            
            # Get data
            menu_data = self._get_response_body(api_info['request_id'])
            
            if not menu_data:
                result['error'] = "No menu data"
                return result
            
            # Save response
            if self.save_responses:
                response_file = os.path.join(self.results_dir, f"response_{vendor_code}.json")
                with open(response_file, 'w') as f:
                    json.dump(menu_data, f, indent=2)
            
            # Parse
            store_name = menu_data.get('data', {}).get('name', 'Unknown')
            oos_items, stats = self._extract_oos_items_FIXED(menu_data)
            
            result['store_name'] = store_name
            result['total_products'] = stats['total_products']
            result['oos_count'] = stats['oos_count']
            result['oos_items'] = oos_items
            result['success'] = True
            
            logger.info(f"✅ {store_name}")
            logger.info(f"   Products: {stats['total_products']}")
            logger.info(f"   OOS: {stats['oos_count']}")
            
            if oos_items:
                logger.info(f"   🔴 OOS items:")
                for item in oos_items[:3]:  # Show first 3
                    logger.info(f"      - {item['name']}")
                if len(oos_items) > 3:
                    logger.info(f"      ... and {len(oos_items)-3} more")
        
        except Exception as e:
            result['error'] = str(e)
            logger.error(f"❌ Error: {e}")
        
        return result
    
    def run_test(self):
        """Run test"""
        store_urls = self.load_foodpanda_urls()
        
        if not store_urls:
            return []
        
        logger.info(f"🚀 Testing {len(store_urls)} stores (headless mode)")
        logger.info(f"⏱️ Estimated time: {len(store_urls) * 0.5:.1f} minutes")
        logger.info("")
        
        results = []
        
        try:
            for i, url in enumerate(store_urls, 1):
                result = self.scrape_store(url, i, len(store_urls))
                results.append(result)
                
                # Long delays between stores
                if i < len(store_urls):
                    delay = random.uniform(15, 25)
                    logger.info(f"⏳ Cooling down {delay:.1f} seconds...")
                    time.sleep(delay)
        
        except KeyboardInterrupt:
            logger.info("\n⚠️ Interrupted by user")
        
        finally:
            if self.driver:
                logger.info("🔒 Closing browser...")
                try:
                    self.driver.quit()
                except:
                    pass
        
        # Results
        self._save_results(results)
        
        return results
    
    def _save_results(self, results: List[Dict]):
        """Save results"""
        logger.info("")
        logger.info("=" * 80)
        logger.info("📊 RESULTS")
        logger.info("=" * 80)
        
        successful = sum(1 for r in results if r['success'])
        total_products = sum(r['total_products'] for r in results if r['success'])
        total_oos = sum(r['oos_count'] for r in results if r['success'])
        
        logger.info(f"Total stores: {len(results)}")
        logger.info(f"✅ Successful: {successful}")
        logger.info(f"❌ Failed: {len(results) - successful}")
        logger.info(f"📦 Total products: {total_products}")
        logger.info(f"🔴 Total OOS: {total_oos}")
        
        if successful > 0:
            logger.info(f"📊 Success rate: {successful/len(results)*100:.1f}%")
        
        # Per-store summary
        logger.info("")
        logger.info("📋 Per-store:")
        for r in results:
            if r['success']:
                logger.info(f"   ✅ {r['store_name']}: {r['oos_count']}/{r['total_products']} OOS")
            else:
                logger.info(f"   ❌ {r['vendor_code']}: {r['error']}")
        
        # Save to file
        results_file = os.path.join(self.results_dir, 'results.json')
        with open(results_file, 'w') as f:
            json.dump({
                'total_stores': len(results),
                'successful': successful,
                'total_products': total_products,
                'total_oos': total_oos,
                'stores': results
            }, f, indent=2)
        
        logger.info("")
        logger.info(f"💾 Results saved: {results_file}")
        logger.info("=" * 80)


def main():
    """Main"""
    save_responses = '--save-responses' in sys.argv
    use_undetected = '--no-undetected' not in sys.argv
    
    print("=" * 80)
    print("🐼 FOODPANDA HEADLESS SCRAPER (FIXED)")
    print("=" * 80)
    print("")
    print("✅ Fixed: Chrome binary location for Mac")
    print("✅ Mode: Headless")
    print("✅ Delays: 15-25 seconds (anti-CAPTCHA)")
    print("")
    print(f"Using: {'undetected-chromedriver' if use_undetected and HAS_UC else 'regular Selenium'}")
    print("")
    input("Press ENTER to start...")
    
    scraper = FoodpandaHeadlessScraper(
        save_responses=save_responses,
        use_undetected=use_undetected
    )
    
    results = scraper.run_test()
    
    print("")
    print("✅ Complete! Check results folder.")


if __name__ == "__main__":
    main()