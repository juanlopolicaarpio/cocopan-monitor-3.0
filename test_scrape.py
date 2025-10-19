#!/usr/bin/env python3
"""
Foodpanda SKU Scraper - Selenium Version
Bypasses PerimeterX bot detection by using real browser

This version works around the 403 Forbidden issue by:
1. Using Selenium to load pages like a real user
2. Intercepting network requests to capture API responses
3. Extracting menu data from the captured API calls

Trade-off: Slower but reliable (5-10 sec per store vs 2 sec)
"""

import json
import time
import logging
import random
import re
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime

try:
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    HAS_SELENIUM = True
except ImportError:
    HAS_SELENIUM = False
    print("❌ Selenium required!")
    print("   Install: pip install selenium")
    exit(1)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class FoodpandaSeleniumScraper:
    """
    Foodpanda scraper using Selenium to bypass bot detection
    
    How it works:
    1. Opens page in real Chrome browser
    2. Waits for menu to load
    3. Captures API responses from network logs
    4. Extracts OOS items from the response
    """
    
    def __init__(self, sku_mapper=None, headless=True):
        """
        Initialize scraper
        
        Args:
            sku_mapper: Optional SKU mapper for product name mapping
            headless: Run browser in headless mode (True = invisible)
        """
        self.sku_mapper = sku_mapper
        self.headless = headless
        self.driver = None
        
        logger.info(f"🐼 Foodpanda Selenium Scraper initialized")
        logger.info(f"   Mode: {'Headless' if headless else 'Visible browser'}")
    
    def _setup_driver(self):
        """Setup Chrome driver with network logging"""
        chrome_options = Options()
        
        if self.headless:
            chrome_options.add_argument('--headless')
        
        # Anti-detection options
        chrome_options.add_argument('--disable-blink-features=AutomationControlled')
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option('useAutomationExtension', False)
        
        # Enable performance logging to capture network requests
        chrome_options.set_capability('goog:loggingPrefs', {'performance': 'ALL'})
        
        # Random user agent
        user_agents = [
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        ]
        chrome_options.add_argument(f'user-agent={random.choice(user_agents)}')
        
        driver = webdriver.Chrome(options=chrome_options)
        
        # Override navigator.webdriver flag
        driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        
        return driver
    
    def extract_vendor_code(self, url: str) -> Optional[str]:
        """Extract vendor code from Foodpanda URL"""
        try:
            match = re.search(r'/restaurant/([a-z0-9]+)/', url, re.IGNORECASE)
            return match.group(1) if match else None
        except Exception as e:
            logger.error(f"Error extracting vendor code: {e}")
            return None
    
    def _find_menu_api_response(self, logs: List[Dict]) -> Optional[Dict]:
        """
        Find and extract the menu API response from browser logs
        
        Looks for: /api/v5/vendors/{code}?include=menus...
        """
        for log in logs:
            try:
                message = json.loads(log['message'])['message']
                
                # Look for Network.responseReceived with our menu API
                if message.get('method') == 'Network.responseReceived':
                    response = message.get('params', {}).get('response', {})
                    url = response.get('url', '')
                    
                    # Check if this is our menu API endpoint
                    if ('/api/v5/vendors/' in url and 
                        'include=menus' in url and
                        response.get('status') == 200):
                        
                        # Found it! Now get the response body
                        request_id = message['params']['requestId']
                        return {'request_id': request_id, 'url': url}
            except:
                continue
        
        return None
    
    def _get_response_body(self, request_id: str) -> Optional[Dict]:
        """Get response body for a specific request ID"""
        try:
            response_body = self.driver.execute_cdp_cmd(
                'Network.getResponseBody',
                {'requestId': request_id}
            )
            
            if 'body' in response_body:
                return json.loads(response_body['body'])
        except Exception as e:
            logger.debug(f"Could not get response body: {e}")
        
        return None
    
    def scrape_store(self, store_url: str, max_wait: int = 15) -> Tuple[List[str], List[str], str]:
        """
        Scrape a single Foodpanda store
        
        Args:
            store_url: Full Foodpanda restaurant URL
            max_wait: Maximum seconds to wait for page load
            
        Returns:
            Tuple of (oos_sku_codes, unknown_products, store_name)
        """
        vendor_code = self.extract_vendor_code(store_url)
        if not vendor_code:
            logger.error(f"Could not extract vendor code from {store_url}")
            return [], [], "Unknown Store"
        
        try:
            # Setup driver if not already done
            if not self.driver:
                self.driver = self._setup_driver()
            
            logger.info(f"🌐 Loading page: {store_url}")
            
            # Load the page
            self.driver.get(store_url)
            
            # Wait for menu to load
            logger.debug(f"   ⏳ Waiting for menu to load...")
            time.sleep(max_wait)
            
            # Get performance logs (contains network requests)
            logs = self.driver.get_log('performance')
            
            # Find the menu API response
            api_info = self._find_menu_api_response(logs)
            
            if not api_info:
                logger.warning(f"   ❌ Could not find menu API response in logs")
                return [], [], "Error Store"
            
            logger.debug(f"   ✅ Found menu API: {api_info['url']}")
            
            # Get the response body
            menu_data = self._get_response_body(api_info['request_id'])
            
            if not menu_data:
                logger.warning(f"   ❌ Could not extract menu data")
                return [], [], "Error Store"
            
            # Extract store name
            store_name = self._extract_store_name(menu_data, store_url)
            
            # Extract OOS items
            oos_product_names = self._extract_oos_items(menu_data)
            
            if not oos_product_names:
                logger.info(f"   ✅ {store_name}: No OOS items")
                return [], [], store_name
            
            # Map to SKU codes
            if self.sku_mapper:
                oos_sku_codes, unknown_products = self.sku_mapper.map_scraped_names_to_skus(oos_product_names)
            else:
                oos_sku_codes = []
                unknown_products = oos_product_names
            
            logger.info(f"   ✅ {store_name}: {len(oos_sku_codes)} OOS SKUs, {len(unknown_products)} unknown")
            
            return oos_sku_codes, unknown_products, store_name
            
        except Exception as e:
            logger.error(f"   ❌ Error scraping {store_url}: {e}")
            return [], [], "Error Store"
    
    def _extract_store_name(self, menu_data: Dict, url: str) -> str:
        """Extract store name from menu data"""
        try:
            if 'data' in menu_data and isinstance(menu_data['data'], dict):
                name = menu_data['data'].get('name')
                if name:
                    return name.strip()
        except Exception as e:
            logger.debug(f"Error extracting name: {e}")
        
        return "Unknown Store"
    
    def _extract_oos_items(self, menu_data: Dict) -> List[str]:
        """Extract OOS product names from menu data"""
        oos_items = []
        
        try:
            if 'data' not in menu_data:
                return oos_items
            
            vendor = menu_data['data']
            menus = vendor.get('menus', [])
            
            for menu in menus:
                products = menu.get('products', [])
                
                for product in products:
                    product_name = product.get('name')
                    is_available = product.get('is_available', True)
                    
                    if product_name and not is_available:
                        oos_items.append(product_name.strip())
                        logger.debug(f"      🔴 OOS: {product_name}")
        
        except Exception as e:
            logger.error(f"Error parsing menu: {e}")
        
        return oos_items
    
    def scrape_all_stores(self, store_urls: List[str], time_limit_minutes: int = 40) -> Dict[str, Any]:
        """
        Scrape multiple stores with time limit
        
        Args:
            store_urls: List of Foodpanda URLs
            time_limit_minutes: Maximum time to spend
            
        Returns:
            Results dictionary
        """
        logger.info("🐼 Starting Foodpanda SKU scraping (Selenium mode)")
        logger.info(f"⏱️ {time_limit_minutes}-minute time limit")
        logger.info(f"📋 {len(store_urls)} stores to scrape")
        
        start_time = time.time()
        time_limit_seconds = time_limit_minutes * 60
        
        results = {
            'total_stores': len(store_urls),
            'successful_scrapes': 0,
            'failed_scrapes': 0,
            'total_oos_skus': 0,
            'total_unknown_products': 0,
            'store_results': []
        }
        
        try:
            # Setup driver once for all stores
            self.driver = self._setup_driver()
            
            for i, store_url in enumerate(store_urls, 1):
                # Check time limit
                elapsed = time.time() - start_time
                if elapsed >= time_limit_seconds:
                    logger.warning(f"⏰ Hit time limit at store {i}/{len(store_urls)}")
                    results['failed_scrapes'] += len(store_urls) - i + 1
                    break
                
                logger.info(f"📍 [{i}/{len(store_urls)}] Scraping {store_url}")
                
                try:
                    oos_skus, unknown, store_name = self.scrape_store(store_url)
                    
                    if store_name not in ["Unknown Store", "Error Store"]:
                        results['successful_scrapes'] += 1
                        results['total_oos_skus'] += len(oos_skus)
                        results['total_unknown_products'] += len(unknown)
                        
                        results['store_results'].append({
                            'store_name': store_name,
                            'store_url': store_url,
                            'oos_skus': oos_skus,
                            'oos_count': len(oos_skus),
                            'unknown_count': len(unknown),
                            'success': True
                        })
                    else:
                        results['failed_scrapes'] += 1
                        logger.warning(f"   ⚠️ Failed: {store_name}")
                        
                except Exception as e:
                    logger.error(f"   ❌ Error: {e}")
                    results['failed_scrapes'] += 1
                
                # Small delay between stores
                if i < len(store_urls):
                    time.sleep(random.uniform(2, 4))
        
        finally:
            # Always close browser
            if self.driver:
                self.driver.quit()
                self.driver = None
        
        # Summary
        total_elapsed = time.time() - start_time
        logger.info("=" * 70)
        logger.info(f"🐼 Foodpanda scraping completed in {total_elapsed/60:.1f} minutes!")
        logger.info(f"📊 Summary:")
        logger.info(f"   Total: {results['total_stores']}")
        logger.info(f"   ✅ Success: {results['successful_scrapes']}")
        logger.info(f"   ❌ Failed: {results['failed_scrapes']}")
        logger.info(f"   🔴 OOS SKUs: {results['total_oos_skus']}")
        logger.info(f"   ❓ Unknown: {results['total_unknown_products']}")
        
        return results
    
    def close(self):
        """Cleanup - close browser"""
        if self.driver:
            self.driver.quit()
            self.driver = None


# Test runner
if __name__ == "__main__":
    print("=" * 70)
    print("🐼 FOODPANDA SELENIUM SCRAPER TEST")
    print("=" * 70)
    print("\n⚠️  NOTE: This uses Selenium to bypass bot detection")
    print("   Trade-off: Slower but more reliable\n")
    
    # Test URLs
    test_urls = [
        "https://www.foodpanda.ph/restaurant/i51s/cocopan-altura",
        "https://www.foodpanda.ph/restaurant/m90j/cocopan-aglipay",
        "https://www.foodpanda.ph/restaurant/bnn1/cocopan-anonas",
    ]
    
    # Create scraper (headless=False to see browser)
    scraper = FoodpandaSeleniumScraper(headless=True)
    
    try:
        # Run test
        results = scraper.scrape_all_stores(test_urls, time_limit_minutes=10)
        
        # Show results
        print("\n" + "=" * 70)
        print("📊 TEST RESULTS")
        print("=" * 70)
        
        for store in results['store_results']:
            print(f"\n✅ {store['store_name']}")
            print(f"   OOS SKUs: {store['oos_count']}")
            if store['oos_skus']:
                for sku in store['oos_skus'][:5]:  # Show first 5
                    print(f"      - {sku}")
        
        print("\n✅ Test complete!")
        print(f"💾 Success rate: {results['successful_scrapes']}/{results['total_stores']}")
        
    finally:
        scraper.close()
    
    print("\n🎯 Next step: Integrate into your monitor_service.py")