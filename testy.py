#!/usr/bin/env python3
"""
Standalone Foodpanda SKU Scraper
🎭 FRESH BROWSER PER STORE - Anti-Detection Mode!
Run this to scrape all stores RIGHT NOW and save to database
"""
import sys
import logging
from datetime import datetime
import time
import json
import random
import re
from typing import Optional, Dict, List

# Import from existing modules
from monitor_service import SKUMapper
from database import db
from config import config

# Selenium imports
import undetected_chromedriver as uc
from bs4 import BeautifulSoup

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ============================================================================
# Configuration
# ============================================================================

def load_foodpanda_urls():
    """Load Foodpanda URLs from branch_urls.json"""
    try:
        with open(config.STORE_URLS_FILE) as f:
            data = json.load(f)
            all_urls = data.get('urls', [])
            foodpanda_urls = [url for url in all_urls if 'foodpanda' in url.lower()]
            return foodpanda_urls
    except Exception as e:
        logger.error(f"Failed to load URLs: {e}")
        return []

# ============================================================================
# Selenium Scraping Functions - FIXED
# ============================================================================

def find_chrome_binary():
    """Find Chrome binary on the system."""
    import subprocess
    import os
    
    # Common Chrome locations on Mac (YOUR Chrome location first!)
    mac_paths = [
        '/Users/arthur.policarpio/Applications/Google Chrome.app/Contents/MacOS/Google Chrome',  # ← YOUR CHROME
        '/Applications/Google Chrome.app/Contents/MacOS/Google Chrome',
        '/Applications/Chromium.app/Contents/MacOS/Chromium',
        os.path.expanduser('~/Applications/Google Chrome.app/Contents/MacOS/Google Chrome'),
    ]
    
    # Try common paths first
    for path in mac_paths:
        if os.path.exists(path):
            logger.info(f"✅ Found Chrome at: {path}")
            return path
    
    # Try using 'which' command
    try:
        result = subprocess.run(['which', 'google-chrome'], 
                              capture_output=True, text=True, timeout=5)
        if result.returncode == 0 and result.stdout.strip():
            return result.stdout.strip()
    except:
        pass
    
    logger.warning("⚠️ Chrome not found in standard locations")
    return None

def create_driver():
    """Create Chrome driver for scraping - FIXED for Chrome 143"""
    chrome_binary = find_chrome_binary()
    
    if not chrome_binary:
        raise Exception("Chrome not found! Please install Chrome or check the path.")
    
    options = uc.ChromeOptions()
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--window-size=1920,1080')
    options.add_argument('--lang=en-PH')
    
    ua = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36'
    options.add_argument(f'--user-agent={ua}')
    
    # Create driver with explicit Chrome binary and version
    driver = uc.Chrome(
        options=options,
        browser_executable_path=chrome_binary,
        use_subprocess=False
    )
    
    return driver

def extract_next_data(html: str) -> Optional[List[Dict]]:
    """Extract from __NEXT_DATA__ script tag."""
    soup = BeautifulSoup(html, "html.parser")
    script = soup.find("script", id="__NEXT_DATA__", type="application/json")
    
    if not script or not script.string:
        return None
    
    try:
        data = json.loads(script.string)
        items = []
        
        def extract_price(price_obj):
            if isinstance(price_obj, (int, float)):
                return float(price_obj)
            if isinstance(price_obj, dict):
                for field in ['amount', 'value', 'price', 'raw']:
                    if field in price_obj:
                        try:
                            val = price_obj[field]
                            if isinstance(val, str):
                                val = re.sub(r'[^\d.]', '', val)
                            return float(val)
                        except:
                            pass
            if isinstance(price_obj, str):
                match = re.search(r'[\d,]+\.?\d*', price_obj)
                if match:
                    try:
                        return float(match.group(0).replace(',', ''))
                    except:
                        pass
            return None
        
        def walk(obj, depth=0):
            if depth > 15:
                return
            
            if isinstance(obj, dict):
                name = obj.get('name') or obj.get('title') or obj.get('productName')
                price = obj.get('price') or obj.get('product_price') or obj.get('priceDecimalDisplay')
                
                if name and price is not None:
                    price_val = extract_price(price)
                    if price_val and price_val > 0:
                        items.append({
                            'name': str(name),
                            'price': price_val,
                            'description': str(obj.get('description', '')),
                            'category': str(obj.get('category', '')),
                            'available': obj.get('available', obj.get('isAvailable', True)),
                            'source': '__NEXT_DATA__'
                        })
                
                for k, v in obj.items():
                    walk(v, depth + 1)
            
            elif isinstance(obj, list):
                for item in obj:
                    walk(item, depth + 1)
        
        walk(data)
        return items if items else None
            
    except Exception as e:
        logger.debug(f"Error extracting __NEXT_DATA__: {e}")
        return None

def extract_html_parsing(html: str) -> Optional[List[Dict]]:
    """Extract by parsing HTML elements."""
    soup = BeautifulSoup(html, "html.parser")
    items = []
    
    patterns = [
        {'selector': '[data-testid*="menu"]', 'name': 'data-testid menu'},
        {'selector': '[data-testid*="product"]', 'name': 'data-testid product'},
        {'selector': '[class*="menu-item"]', 'name': 'class menu-item'},
        {'selector': '[class*="product-card"]', 'name': 'class product-card'},
    ]
    
    for pattern in patterns:
        try:
            elements = soup.select(pattern['selector'])
            
            for elem in elements:
                # Find name
                name = None
                for heading in ['h3', 'h4', 'h2']:
                    heading_elem = elem.find(heading)
                    if heading_elem:
                        name = heading_elem.get_text(strip=True)
                        break
                
                # Find price
                price = None
                price_elem = elem.find(class_=re.compile(r'price', re.I))
                if price_elem:
                    price_text = price_elem.get_text(strip=True)
                    match = re.search(r'₱?\s*([\d,]+\.?\d*)', price_text)
                    if match:
                        try:
                            price = float(match.group(1).replace(',', ''))
                        except:
                            pass
                
                if name and price and 10 < price < 5000:
                    items.append({
                        'name': name,
                        'price': price,
                        'description': '',
                        'category': '',
                        'available': True,
                        'source': f'HTML:{pattern["name"]}'
                    })
        except Exception as e:
            logger.debug(f"Error with pattern {pattern['name']}: {e}")
    
    # Remove duplicates
    seen = set()
    unique_items = []
    for item in items:
        key = (item['name'].lower().strip(), item['price'])
        if key not in seen:
            seen.add(key)
            unique_items.append(item)
    
    return unique_items if unique_items else None

def scrape_foodpanda_url(driver, url: str) -> Optional[Dict]:
    """Scrape a Foodpanda URL and return results."""
    try:
        # Load page
        driver.get(url)
        time.sleep(random.uniform(4, 6))
        
        # Scroll to load content
        for i in range(3):
            driver.execute_script(f"window.scrollTo(0, {(i + 1) * 500});")
            time.sleep(0.5)
        driver.execute_script("window.scrollTo(0, 0);")
        time.sleep(1)
        
        # Get page source
        html = driver.page_source
        
        # Extract store name from page
        soup = BeautifulSoup(html, "html.parser")
        store_name = "Unknown Store"
        
        # Try to find store name
        title_elem = soup.find('h1')
        if title_elem:
            store_name = title_elem.get_text(strip=True)
        
        # Try extraction methods
        items = None
        
        # Method 1: __NEXT_DATA__
        items = extract_next_data(html)
        
        # Method 2: HTML parsing
        if not items:
            items = extract_html_parsing(html)
        
        if items:
            return {
                'success': True,
                'store_name': store_name,
                'items': items,
                'total_items': len(items)
            }
        else:
            return None
            
    except Exception as e:
        logger.debug(f"Scraping error: {e}")
        return None

# ============================================================================
# Main Scraper Class
# ============================================================================
class StandaloneFoodpandaScraper:
    """Standalone scraper that saves to database"""
    
    def __init__(self, max_retries=2):
        self.sku_mapper = SKUMapper(platform='foodpanda')
        self.store_urls = load_foodpanda_urls()
        self.max_retries = max_retries
        self.problematic_stores = []  # Track stores with 0 items
        
        logger.info("="*80)
        logger.info("🐼 STANDALONE FOODPANDA SKU SCRAPER")
        logger.info("="*80)
        logger.info(f"📦 Loaded {len(self.sku_mapper.master_skus)} master SKU products")
        logger.info(f"📋 Loaded {len(self.store_urls)} Foodpanda store URLs")
        logger.info(f"💾 Will save results to database")
        logger.info(f"🔄 Max retries per store: {self.max_retries}")
        logger.info(f"🎭 Fresh browser per store (anti-detection mode!)")
        logger.info("="*80)
        logger.info("")
    
    def scrape_single_store(self, store_url: str, index: int, total: int):
        """Scrape one store and save to database (with retry logic)"""
        logger.info("="*80)
        logger.info(f"🐼 STORE {index}/{total}")
        logger.info("="*80)
        logger.info(f"URL: {store_url}")
        logger.info("")
        
        result = {
            'url': store_url,
            'store_name': '',
            'success': False,
            'oos_skus': [],
            'unknown_products': [],
            'retry_count': 0,
            'has_zero_items': False
        }
        
        # FRESH DRIVER FOR EACH STORE (anti-detection!)
        driver = None
        
        try:
            # Retry loop (2 retries as requested)
            for attempt in range(self.max_retries + 1):  # 0, 1, 2 = 3 attempts total
                try:
                    # STEP 0: Create fresh browser for this attempt
                    if attempt == 0:
                        logger.info("🌐 Creating fresh browser for this store...")
                    else:
                        logger.info(f"🔄 RETRY {attempt}/{self.max_retries}")
                        logger.info(f"   Creating fresh browser (new session)...")
                    
                    # Close old driver if exists (for retries)
                    if driver:
                        driver.quit()
                        time.sleep(2)  # Wait before creating new driver
                    
                    # Create fresh driver
                    driver = create_driver()
                    logger.info(f"✅ Fresh browser created")
                    logger.info("")
                    
                    # STEP 1: Scrape with Selenium
                    logger.info("📡 Scraping menu...")
                    scrape_result = scrape_foodpanda_url(driver, store_url)
                    
                    if not scrape_result:
                        raise Exception("Failed to scrape menu")
                    
                    store_name = scrape_result['store_name']
                    result['store_name'] = store_name
                    
                    scraped_items = scrape_result['items']
                    total_items = len(scraped_items)
                    
                    logger.info(f"✅ {store_name}")
                    logger.info(f"   Total products scraped: {total_items}")
                    logger.info("")
                    
                    # Check if we got 0 items (problematic)
                    if total_items == 0:
                        result['has_zero_items'] = True
                        result['retry_count'] = attempt
                        
                        if attempt < self.max_retries:
                            logger.warning(f"⚠️ Got 0 products - this looks wrong!")
                            logger.info(f"   Will retry with fresh browser ({attempt}/{self.max_retries} retries so far)")
                            logger.info("")
                            continue  # Retry with fresh browser
                        else:
                            logger.error(f"❌ Still 0 products after {self.max_retries + 1} attempts!")
                            logger.error(f"   Marking as problematic and moving on...")
                            logger.info("")
                            
                            # Add to problematic stores list
                            self.problematic_stores.append({
                                'url': store_url,
                                'store_name': store_name,
                                'index': index
                            })
                            
                            return result
                    
                    # Got valid data, break out of retry loop
                    result['retry_count'] = attempt
                    if attempt > 0:
                        logger.info(f"✅ Success on retry {attempt} with fresh browser!")
                        logger.info("")
                    
                    # STEP 2: Map to SKU codes
                    logger.info("🗺️ Mapping product names to SKU codes...")
                    
                    mapping_result = self.sku_mapper.map_scraped_items(scraped_items)
                    
                    matched = mapping_result['matched']
                    unknown = mapping_result['unknown']
                    matched_skus = mapping_result['matched_skus']
                    
                    logger.info(f"   ✅ Matched: {len(matched)}")
                    logger.info(f"   ❌ Unknown: {len(unknown)}")
                    logger.info("")
                    
                    # STEP 3: Find out-of-stock items (Foodpanda-specific logic)
                    logger.info("🔍 Finding out-of-stock items (missing from menu)...")
                    
                    oos_result = self.sku_mapper.find_out_of_stock_skus(matched_skus)
                    oos_skus = oos_result['out_of_stock_skus']
                    
                    logger.info(f"   📦 Total SKUs in database: {oos_result['total_in_db']}")
                    logger.info(f"   ✅ SKUs found in scraping: {oos_result['total_scraped']}")
                    logger.info(f"   🚫 SKUs out of stock: {oos_result['out_of_stock_count']}")
                    logger.info("")
                    
                    if oos_skus:
                        logger.info(f"   Out-of-stock items:")
                        for i, item in enumerate(oos_result['out_of_stock_details'][:5], 1):
                            logger.info(f"      {i}. {item['sku_code']} - {item['product_name']}")
                        if len(oos_skus) > 5:
                            logger.info(f"      ... and {len(oos_skus) - 5} more")
                        logger.info("")
                    
                    result['oos_skus'] = oos_skus
                    result['unknown_products'] = [u['scraped_name'] for u in unknown]
                    
                    # STEP 4: Save to database
                    logger.info("💾 Saving to database...")
                    
                    store_id = db.get_or_create_store(store_name, store_url)
                    success = db.save_sku_compliance_check(
                        store_id=store_id,
                        platform='foodpanda',
                        out_of_stock_ids=oos_skus,
                        checked_by='manual_scraper'
                    )
                    
                    result['success'] = success
                    
                    if success:
                        compliance_pct = ((oos_result['total_in_db'] - len(oos_skus)) / oos_result['total_in_db']) * 100
                        logger.info(f"✅ Successfully saved to database!")
                        logger.info(f"   Store: {store_name}")
                        logger.info(f"   OOS SKUs: {len(oos_skus)}")
                        logger.info(f"   Compliance: {compliance_pct:.1f}%")
                    else:
                        logger.error(f"❌ Failed to save to database")
                    
                    logger.info("")
                    
                    # Success! Break out of retry loop
                    break
                    
                except Exception as e:
                    logger.error(f"❌ Error on attempt {attempt + 1}: {e}")
                    
                    if attempt < self.max_retries:
                        logger.info(f"   Will retry with fresh browser...")
                        logger.info("")
                        continue
                    else:
                        logger.error(f"   Failed after {self.max_retries + 1} attempts")
                        import traceback
                        logger.debug(traceback.format_exc())
                        logger.info("")
                        return result
            
            return result
            
        finally:
            # ALWAYS close the driver for this store
            if driver:
                logger.info("🔒 Closing browser for this store...")
                try:
                    driver.quit()
                    logger.info("✅ Browser closed")
                except:
                    pass  # Ignore errors when closing
                logger.info("")
    
    def run(self, urls_to_scrape=None):
        """Run scraper on all stores (or specific list)"""
        urls = urls_to_scrape or self.store_urls
        
        if not urls:
            logger.error("❌ No store URLs to scrape!")
            return
        
        start_time = time.time()
        
        logger.info("🚀 STARTING SCRAPE")
        logger.info(f"⏱️ Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"📊 Stores to scrape: {len(urls)}")
        logger.info("")
        
        results = []
        successful = 0
        failed = 0
        total_oos_skus = 0
        total_unknown = 0
        retry_count = 0
        
        for i, url in enumerate(urls, 1):
            result = self.scrape_single_store(url, i, len(urls))
            results.append(result)
            
            if result['success']:
                successful += 1
                total_oos_skus += len(result['oos_skus'])
                total_unknown += len(result['unknown_products'])
            else:
                failed += 1
            
            if result['retry_count'] > 0:
                retry_count += 1
            
            # Pause between stores with RANDOM delay (anti-detection)
            if i < len(urls):
                wait_time = random.uniform(5, 10)  # Random 5-10 seconds
                logger.info(f"⏸️ Waiting {wait_time:.1f} seconds before next store...")
                logger.info("")
                time.sleep(wait_time)
        
        # Final summary
        elapsed = time.time() - start_time
        
        logger.info("="*80)
        logger.info("🎉 SCRAPING COMPLETED!")
        logger.info("="*80)
        logger.info(f"⏱️ Duration: {elapsed/60:.1f} minutes")
        logger.info(f"📊 Results:")
        logger.info(f"   Total stores: {len(urls)}")
        logger.info(f"   ✅ Successful: {successful}")
        logger.info(f"   ❌ Failed: {failed}")
        logger.info(f"   🔄 Needed retries: {retry_count}")
        logger.info(f"   🔴 Total OOS SKUs: {total_oos_skus}")
        logger.info(f"   ❓ Total unknown products: {total_unknown}")
        logger.info("")
        
        # Show problematic stores
        if self.problematic_stores:
            logger.info("="*80)
            logger.info("⚠️ PROBLEMATIC STORES (0 items found):")
            logger.info("="*80)
            for store in self.problematic_stores:
                logger.info(f"   {store['index']}. {store['store_name']}")
                logger.info(f"      URL: {store['url']}")
            logger.info("")
            logger.info(f"Total problematic stores: {len(self.problematic_stores)}")
            logger.info("")
        
        # Show all unknown products
        all_unknown = []
        for result in results:
            all_unknown.extend(result['unknown_products'])
        
        if all_unknown:
            unique_unknown = sorted(set(all_unknown))
            logger.info("="*80)
            logger.info("⚠️ UNKNOWN PRODUCTS (need to be added to master SKUs):")
            logger.info("="*80)
            for product in unique_unknown:
                logger.info(f"   • {product}")
            logger.info("")
        
        # Store-by-store breakdown
        logger.info("="*80)
        logger.info("📋 STORE-BY-STORE RESULTS:")
        logger.info("="*80)
        logger.info("")
        
        for i, result in enumerate(results, 1):
            if result['success']:
                status = "✅"
                details = f"{len(result['oos_skus'])} OOS"
                if result['unknown_products']:
                    details += f", {len(result['unknown_products'])} unknown"
                if result['retry_count'] > 0:
                    details += f" (retry {result['retry_count']})"
            elif result['has_zero_items']:
                status = "⚠️"
                details = "0 items (problematic)"
            else:
                status = "❌"
                details = "Failed"
            
            logger.info(f"   {i}. {status} {result['store_name'] or 'Unknown Store'}")
            logger.info(f"      {details}")
        
        logger.info("")
        logger.info("="*80)
        logger.info("💾 All results saved to database!")
        logger.info("="*80)
        logger.info("")
        
        return results
    
    def retry_problematic_stores(self):
        """Re-scrape only the problematic stores that had 0 items"""
        if not self.problematic_stores:
            logger.info("✅ No problematic stores to retry!")
            return
        
        logger.info("="*80)
        logger.info("🔄 RE-SCRAPING PROBLEMATIC STORES")
        logger.info("="*80)
        logger.info(f"📊 Found {len(self.problematic_stores)} stores with 0 items")
        logger.info("")
        
        # Confirmation prompt
        try:
            response = input(f"Re-scrape these {len(self.problematic_stores)} stores? (y/n): ").strip().lower()
            if response != 'y':
                logger.info("❌ Cancelled by user")
                return
        except KeyboardInterrupt:
            logger.info("\n❌ Cancelled by user")
            return
        
        logger.info("")
        
        # Extract URLs
        problematic_urls = [store['url'] for store in self.problematic_stores]
        
        # Clear the problematic stores list for this new run
        old_problematic = self.problematic_stores.copy()
        self.problematic_stores = []
        
        # Run scraper on just these stores
        results = self.run(urls_to_scrape=problematic_urls)
        
        # Show comparison
        logger.info("="*80)
        logger.info("📊 RETRY RESULTS:")
        logger.info("="*80)
        logger.info(f"   Previously problematic: {len(old_problematic)}")
        logger.info(f"   Still problematic: {len(self.problematic_stores)}")
        logger.info(f"   Fixed: {len(old_problematic) - len(self.problematic_stores)}")
        logger.info("")

# ============================================================================
# Main
# ============================================================================
def main():
    """Main entry point"""
    logger.info("")
    logger.info("="*80)
    logger.info("🐼 FOODPANDA SKU SCRAPER - STANDALONE MODE")
    logger.info("="*80)
    logger.info("")
    
    # Confirmation prompt
    try:
        response = input("⚠️ This will scrape ALL Foodpanda stores and save to database. Continue? (y/n): ").strip().lower()
        if response != 'y':
            logger.info("❌ Cancelled by user")
            return
    except KeyboardInterrupt:
        logger.info("\n❌ Cancelled by user")
        return
    
    logger.info("")
    
    scraper = StandaloneFoodpandaScraper(max_retries=2)
    
    try:
        # Initial scrape
        scraper.run()
        
        # If there are problematic stores, offer to retry them
        if scraper.problematic_stores:
            logger.info("")
            scraper.retry_problematic_stores()
        
        logger.info("✅ Script completed successfully!")
        
    except KeyboardInterrupt:
        logger.info("")
        logger.info("🛑 Interrupted by user")
    except Exception as e:
        logger.error(f"❌ Fatal error: {e}")
        import traceback
        logger.error(traceback.format_exc())
    finally:
        logger.info("👋 Goodbye!")

if __name__ == "__main__":
    main()