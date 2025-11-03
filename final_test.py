#!/usr/bin/env python3
"""
Grab Food Batch Scraper - Loads URLs from branch_urls.json
- Processes multiple GrabFood URLs from branch_urls.json
- Takes screenshots and saves HTML for each
- Tracks success/failure for each URL
- Generates comprehensive summary report
"""

import json
import os
import time
import random
from pathlib import Path
from typing import List, Dict, Optional
from datetime import datetime

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class GrabBatchScraper:
    """Batch scraper for multiple GrabFood URLs"""

    def __init__(self, headless: bool = False, max_retries: int = 3, immediate_retry: bool = True, output_dir: Optional[str] = None):
        self.max_retries = max_retries
        self.headless = headless
        self.immediate_retry = immediate_retry
        self.driver: Optional[webdriver.Chrome] = None
        self.batch_id = int(time.time())  # Unique batch ID

        # ---------- Output directory ----------
        env_dir = os.environ.get("GRAB_OUTPUT_DIR")
        self.output_dir = Path(output_dir or env_dir or (Path.home() / "grab_batch_outputs")).expanduser()
        self.output_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"✓ Output folder: {self.output_dir}")

        # Statistics
        self.stats = {
            'total': 0,
            'success': 0,
            'failed': 0,
            'skipped': 0,
            'results': []
        }

        self._setup_driver()

    def load_grabfood_urls(self, urls_file: str = "branch_urls.json") -> List[str]:
        """Load GrabFood URLs from branch_urls.json"""
        try:
            with open(urls_file, 'r') as f:
                data = json.load(f)
                all_urls = data.get('urls', [])
            
            # Filter to only GrabFood URLs
            grabfood_urls = [
                url for url in all_urls 
                if 'food.grab.com' in url.lower()
            ]
            
            logger.info(f"📋 Loaded {len(grabfood_urls)} GrabFood URLs from {urls_file}")
            return grabfood_urls
            
        except FileNotFoundError:
            logger.error(f"❌ File not found: {urls_file}")
            return []
        except json.JSONDecodeError as e:
            logger.error(f"❌ Invalid JSON in {urls_file}: {e}")
            return []
        except Exception as e:
            logger.error(f"❌ Error loading URLs: {e}")
            return []

    def _file_base(self, url_index: int, attempt: int, stage: str) -> Path:
        """Base filename without extension."""
        safe_stage = "".join(c if c.isalnum() or c in "-_" else "_" for c in stage)
        return self.output_dir / f"batch_{self.batch_id}_url{url_index:03d}_attempt{attempt}_{safe_stage}"

    def _save_text(self, path: Path, text: str) -> None:
        path.write_text(text, encoding="utf-8")

    def _save_json_obj(self, path: Path, obj: dict) -> None:
        path.write_text(json.dumps(obj, indent=2, ensure_ascii=False), encoding="utf-8")

    def _setup_driver(self):
        """Setup Chrome"""
        chrome_options = Options()
        if self.headless:
            chrome_options.add_argument('--headless=new')

        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument('--disable-gpu')
        chrome_options.add_argument('--disable-blink-features=AutomationControlled')
        chrome_options.add_argument('--window-size=1920,1080')

        user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        ]
        chrome_options.add_argument(f'user-agent={random.choice(user_agents)}')
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option('useAutomationExtension', False)

        self.driver = webdriver.Chrome(options=chrome_options)
        self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        logger.info("✓ Browser ready")

    def _check_page_success(self, url_index: int, attempt: int, stage: str) -> bool:
        """Check if page loaded successfully with comprehensive diagnostics"""
        print(f"\n{'='*80}")
        print(f"URL {url_index + 1} - ATTEMPT {attempt} - {stage.upper()}")
        print(f"{'='*80}")

        print(f"\n📍 URL: {self.driver.current_url}")
        print(f"📄 Title: {self.driver.title}")
        html = self.driver.page_source
        print(f"📏 HTML Length: {len(html):,} chars")

        soup = BeautifulSoup(html, 'html.parser')
        
        # Remove scripts, styles, etc for visible text
        soup_copy = BeautifulSoup(html, 'html.parser')
        for tag in soup_copy(["script", "style", "meta", "link"]):
            tag.decompose()

        visible_text = soup_copy.get_text(separator='\n', strip=True)
        lines = [line.strip() for line in visible_text.split('\n') if line.strip()]

        print(f"\n--- VISIBLE TEXT (First 20 lines) ---")
        for i, line in enumerate(lines[:20], 1):
            if len(line) > 75:
                line = line[:72] + "..."
            print(f"{i:2d}. {line}")

        # Check for terminated/closed store in title or visible text
        title_lower = self.driver.title.lower()
        visible_lower = visible_text.lower()
        
        is_terminated = False
        if 'terminated' in title_lower or 'terminated' in visible_lower[:500]:
            is_terminated = True
            print(f"\n🚫 STORE TERMINATED DETECTED")
        elif 'closed permanently' in visible_lower[:500] or 'permanently closed' in visible_lower[:500]:
            is_terminated = True
            print(f"\n🚫 STORE PERMANENTLY CLOSED DETECTED")

        # PRIORITY 1: Check for __NEXT_DATA__ with valid merchant data
        next_data = soup.find('script', {'id': '__NEXT_DATA__'})
        
        if next_data and next_data.string:
            print(f"\n✅ __NEXT_DATA__ FOUND!")
            try:
                data = json.loads(next_data.string)
                props = data.get('props', {}).get('pageProps', {})
                
                if 'merchant' in props:
                    merchant = props['merchant']
                    name = merchant.get('name', 'N/A')
                    rating = merchant.get('rating', 'N/A')
                    merchant_id = merchant.get('id', 'N/A')
                    
                    print(f"   🏪 Name: {name}")
                    print(f"   ⭐ Rating: {rating}")
                    print(f"   🆔 ID: {merchant_id}")
                    
                    # Check if store is closed
                    is_closed = merchant.get('isClosed', False)
                    is_available = merchant.get('available', True)
                    
                    if is_terminated:
                        print(f"   🚫 STORE STATUS: TERMINATED")
                    elif is_closed or not is_available:
                        print(f"   🔒 STORE STATUS: CLOSED")
                    else:
                        print(f"   🟢 STORE STATUS: OPEN")
                    
                    if 'menu' in props:
                        categories = props['menu'].get('categories', [])
                        total_items = sum(len(cat.get('items', [])) for cat in categories)
                        print(f"   🍽️  Menu Items: {total_items}")
                    else:
                        print(f"   ⚠️  No 'menu' data in __NEXT_DATA__")
                    
                    print(f"\n✅ SUCCESS - Valid restaurant data found in __NEXT_DATA__!")
                    return True
                else:
                    print(f"   ⚠️  No 'merchant' data in __NEXT_DATA__")
                    print(f"   📦 Available keys: {list(props.keys())}")
                    print(f"   💡 Will try HTML parsing fallback...")
                    
            except json.JSONDecodeError as e:
                print(f"\n❌ JSON Parse Error: {e}")
            except Exception as e:
                print(f"\n❌ Exception parsing __NEXT_DATA__: {e}")
        else:
            print(f"\n❌ NO __NEXT_DATA__ FOUND")
            print(f"   💡 Will try HTML parsing fallback...")

        # PRIORITY 2: HTML PARSING FALLBACK
        # Parse restaurant data directly from visible HTML
        print(f"\n🔍 Attempting HTML parsing...")
        
        # Try to extract restaurant name and rating from title
        import re
        title = self.driver.title
        
        # Pattern: "Restaurant Name ⭐ Rating"
        match = re.match(r'^(.+?)\s*⭐\s*([\d.]+)', title)
        if match:
            restaurant_name = match.group(1).strip()
            rating = match.group(2)
            
            print(f"   🏪 Name (from title): {restaurant_name}")
            print(f"   ⭐ Rating (from title): {rating}")
            
            # Look for menu categories in visible text
            menu_categories = []
            category_keywords = [
                'Classic Favorites', 'Sweet Favorites', 'Savory Favorites',
                'Donuts', 'Bundle', 'New Offers', 'For You',
                'Daily Loaf', 'Brewed Coffee', 'Breakfast', 'Lunch', 'Dinner',
                'Appetizers', 'Main Course', 'Desserts', 'Beverages', 'Drinks'
            ]
            
            for line in lines:
                for keyword in category_keywords:
                    if keyword.lower() == line.lower():
                        menu_categories.append(keyword)
            
            if menu_categories:
                print(f"   📋 Menu Categories Found: {len(menu_categories)}")
                for cat in menu_categories[:10]:
                    print(f"      • {cat}")
            
            # Check for cuisine/category info
            cuisines = []
            for i, line in enumerate(lines):
                if restaurant_name in line and i + 1 < len(lines):
                    next_line = lines[i + 1]
                    # Check if next line looks like cuisines (contains comma-separated items)
                    if ',' in next_line and len(next_line) < 100:
                        cuisines = [c.strip() for c in next_line.split(',')]
                        print(f"   🍴 Cuisines: {', '.join(cuisines)}")
                        break
            
            if is_terminated:
                print(f"   🚫 STORE STATUS: TERMINATED")
            else:
                print(f"   🟢 STORE STATUS: Based on HTML (appears active)")
            
            print(f"\n✅ SUCCESS - Valid restaurant data found via HTML PARSING!")
            return True
        else:
            print(f"   ⚠️  Could not parse restaurant info from title")

        # PRIORITY 3: Only check for errors if no valid data found
        print(f"\n🔍 Checking for error indicators...")
        errors = []
        
        # Very specific error page detection
        if 'oops' in title_lower and 'something went wrong' in title_lower:
            errors.append("❌ Oops Error Page (in title)")
        if '401' in title_lower or 'unauthorized' in title_lower:
            errors.append("❌ 401 Unauthorized (in title)")
        if '403' in title_lower or 'forbidden' in title_lower:
            errors.append("❌ 403 Forbidden (in title)")
        if '404' in title_lower or 'not found' in title_lower:
            errors.append("❌ 404 Not Found (in title)")
            
        # Check for Cloudflare challenge
        if 'cloudflare' in html.lower() and 'checking your browser' in html.lower():
            errors.append("⚠️  Cloudflare Challenge")
            
        # Check for access denied pages
        if 'access denied' in visible_lower and len(visible_text) < 500:
            errors.append("❌ Access Denied Page")

        if errors:
            print(f"🚨 ERRORS DETECTED:")
            for err in errors:
                print(f"   {err}")
        else:
            print(f"⚠️  No valid data and no clear error detected")

        return False

    def _save_files(self, url_index: int, attempt: int, stage: str):
        """Save screenshot and HTML"""
        base = self._file_base(url_index, attempt, stage)

        # Screenshot
        png_path = base.with_suffix(".png")
        try:
            self.driver.save_screenshot(str(png_path))
        except Exception as e:
            logger.debug(f"Screenshot error: {e}")

        # HTML
        html_path = base.with_suffix(".html")
        try:
            self._save_text(html_path, self.driver.page_source)
        except Exception as e:
            logger.debug(f"HTML save error: {e}")

    def _extract_data(self, url: str, url_index: int) -> dict:
        """Extract restaurant and menu data with HTML parsing fallback"""
        soup = BeautifulSoup(self.driver.page_source, 'html.parser')
        html = self.driver.page_source

        result = {
            'url': url,
            'actual_url': self.driver.current_url,
            'title': self.driver.title,
            'url_index': url_index,
            'batch_id': self.batch_id,
            'timestamp': time.time(),
            'success': False,
            'extraction_method': None,
            'data': {
                'restaurant': {},
                'menu_items': []
            }
        }

        # Check for terminated/closed
        title_lower = self.driver.title.lower()
        visible_text = soup.get_text()
        visible_lower = visible_text.lower()
        
        is_terminated = False
        if 'terminated' in title_lower or 'terminated' in visible_lower[:1000]:
            is_terminated = True
        elif 'closed permanently' in visible_lower[:1000] or 'permanently closed' in visible_lower[:1000]:
            is_terminated = True

        # METHOD 1: Try __NEXT_DATA__ extraction
        script = soup.find('script', {'id': '__NEXT_DATA__'})
        if script and script.string:
            try:
                json_data = json.loads(script.string)
                props = json_data.get('props', {}).get('pageProps', {})

                if 'merchant' in props:
                    merchant = props['merchant']
                    
                    # Check closed status
                    is_closed = merchant.get('isClosed', False)
                    is_available = merchant.get('available', True)
                    
                    if is_terminated:
                        status = 'TERMINATED'
                    elif is_closed or not is_available:
                        status = 'CLOSED'
                    else:
                        status = 'OPEN'
                    
                    result['data']['restaurant'] = {
                        'id': merchant.get('id'),
                        'name': merchant.get('name'),
                        'status': status,
                        'is_closed': is_closed,
                        'is_terminated': is_terminated,
                        'available': is_available,
                        'rating': merchant.get('rating'),
                        'cuisine': merchant.get('cuisines', []),
                        'delivery_fee': merchant.get('deliveryFee'),
                        'delivery_time': merchant.get('estimatedDeliveryTime'),
                        'address': merchant.get('address'),
                        'latitude': merchant.get('latlng', {}).get('latitude'),
                        'longitude': merchant.get('latlng', {}).get('longitude')
                    }

                    if 'menu' in props:
                        for cat in props['menu'].get('categories', []):
                            for item in cat.get('items', []):
                                result['data']['menu_items'].append({
                                    'name': item.get('name'),
                                    'price': item.get('price'),
                                    'description': item.get('description'),
                                    'category': cat.get('name')
                                })

                    result['success'] = True
                    result['extraction_method'] = '__NEXT_DATA__'
                    return result
                    
            except Exception as e:
                result['json_error'] = str(e)

        # METHOD 2: HTML PARSING FALLBACK
        print(f"\n💡 Using HTML parsing fallback...")
        
        import re
        title = self.driver.title
        
        # Extract restaurant name and rating from title
        match = re.match(r'^(.+?)\s*⭐\s*([\d.]+)', title)
        if match:
            restaurant_name = match.group(1).strip()
            rating_str = match.group(2)
            
            try:
                rating = float(rating_str)
            except:
                rating = None
            
            # Extract cuisines from visible text
            lines = [line.strip() for line in visible_text.split('\n') if line.strip()]
            cuisines = []
            
            for i, line in enumerate(lines):
                if restaurant_name in line and i + 1 < len(lines):
                    next_line = lines[i + 1]
                    # Check if next line looks like cuisines
                    if ',' in next_line and len(next_line) < 100 and not any(c.isdigit() for c in next_line):
                        cuisines = [c.strip() for c in next_line.split(',')]
                        break
            
            # Determine status
            if is_terminated:
                status = 'TERMINATED'
            else:
                status = 'OPEN'
            
            result['data']['restaurant'] = {
                'id': None,
                'name': restaurant_name,
                'status': status,
                'is_closed': False,
                'is_terminated': is_terminated,
                'available': not is_terminated,
                'rating': rating,
                'cuisine': cuisines,
                'delivery_fee': None,
                'delivery_time': None,
                'address': None,
                'latitude': None,
                'longitude': None
            }
            
            # Extract menu categories and items from HTML
            # Look for category headings and items under them
            menu_items = []
            
            # Find all text elements that could be menu items
            # Categories are usually in headings or prominent text
            category_keywords = [
                'Classic Favorites', 'Sweet Favorites', 'Savory Favorites',
                'Donuts', 'Bundle', 'Bundles', 'New Offers', 'For You',
                'Daily Loaf', 'Brewed Coffee', 'Breakfast', 'Lunch', 'Dinner',
                'Appetizers', 'Main Course', 'Desserts', 'Beverages', 'Drinks',
                'Pasta', 'Pizza', 'Burgers', 'Sandwiches', 'Salads', 'Sides'
            ]
            
            current_category = 'Uncategorized'
            found_categories = set()
            
            for i, line in enumerate(lines):
                # Check if this line is a category
                for keyword in category_keywords:
                    if keyword.lower() == line.lower():
                        current_category = keyword
                        found_categories.add(keyword)
                        break
                
                # Try to identify menu items (lines that look like items)
                # Usually have prices or descriptions
                if len(line) > 3 and len(line) < 200:
                    # Check if line contains price indicators
                    has_price = bool(re.search(r'₱|PHP|\d+\.?\d*', line))
                    
                    # Skip navigation/header items
                    skip_keywords = ['login', 'sign up', 'home', 'restaurant', 'opening hours', 
                                   'today', 'tomorrow', 'mins', 'km', 'delivery']
                    if any(skip.lower() in line.lower() for skip in skip_keywords):
                        continue
                    
                    # Skip category names themselves
                    if line in found_categories:
                        continue
                    
                    # If we're in a category section and this looks like an item
                    if current_category in found_categories and (has_price or (10 < len(line) < 100)):
                        # Try to extract price
                        price_match = re.search(r'₱\s*([\d,]+\.?\d*)', line)
                        price = None
                        if price_match:
                            try:
                                price = float(price_match.group(1).replace(',', ''))
                            except:
                                pass
                        
                        # Extract item name (remove price if present)
                        item_name = re.sub(r'₱\s*[\d,]+\.?\d*', '', line).strip()
                        
                        if item_name and len(item_name) > 2:
                            menu_items.append({
                                'name': item_name,
                                'price': price,
                                'description': None,
                                'category': current_category
                            })
            
            result['data']['menu_items'] = menu_items
            
            # If we found categories but no items, just record the categories
            if found_categories and not menu_items:
                for cat in found_categories:
                    menu_items.append({
                        'name': f'{cat} (category detected)',
                        'price': None,
                        'description': None,
                        'category': cat
                    })
                result['data']['menu_items'] = menu_items
            
            result['success'] = True
            result['extraction_method'] = 'HTML_PARSING'
            print(f"   ✅ Extracted via HTML: {restaurant_name}, {len(menu_items)} items")
            
            return result

        # If both methods fail
        result['error'] = 'Could not extract data via __NEXT_DATA__ or HTML parsing'
        return result

    def _scroll_page(self):
        """Scroll through page to load dynamic content"""
        try:
            total = self.driver.execute_script("return document.body.scrollHeight")
            viewport = self.driver.execute_script("return window.innerHeight")
            current = 0
            while current < total:
                self.driver.execute_script(f"window.scrollTo(0, {current})")
                current += max(200, viewport // 2)
                time.sleep(0.3)
            self.driver.execute_script("window.scrollTo(0, 0)")
            time.sleep(1)
        except Exception:
            pass

    def scrape_url(self, url: str, url_index: int) -> dict:
        """Scrape a single URL with retries"""
        print(f"\n{'='*80}")
        print(f"🔗 URL {url_index + 1}/{self.stats['total']}: {url}")
        print(f"{'='*80}")

        for attempt in range(1, self.max_retries + 1):
            print(f"\n⏳ Attempt {attempt}/{self.max_retries}...")

            if attempt > 1:
                if self.immediate_retry:
                    print("🚀 IMMEDIATE RETRY (no delay)")
                else:
                    wait = 2 ** attempt
                    print(f"⏳ Waiting {wait}s...")
                    time.sleep(wait)

            try:
                # Load page
                self.driver.get(url)
                time.sleep(3)

                # Check initial load
                success = self._check_page_success(url_index, attempt, 'INITIAL_LOAD')

                if not success:
                    print("\n⚠️  Initial load failed, refreshing...")
                    self.driver.refresh()
                    time.sleep(3)
                    success = self._check_page_success(url_index, attempt, 'AFTER_REFRESH')

                if success:
                    print("\n✅ Page loaded successfully!")
                    
                    # Scroll to load everything
                    self._scroll_page()
                    
                    # Final verification
                    print(f"\n--- FINAL VERIFICATION ---")
                    self._check_page_success(url_index, attempt, 'FINAL_STATE')
                    
                    # Save artifacts
                    self._save_files(url_index, attempt, 'success')
                    
                    # Extract data
                    result = self._extract_data(url, url_index)
                    
                    # Save individual result
                    result_file = self.output_dir / f"batch_{self.batch_id}_url{url_index:03d}_result.json"
                    self._save_json_obj(result_file, result)
                    
                    restaurant_name = result['data']['restaurant'].get('name', 'Unknown')
                    menu_count = len(result['data']['menu_items'])
                    status = result['data']['restaurant'].get('status', 'UNKNOWN')
                    method = result.get('extraction_method', 'UNKNOWN')
                    
                    if status == 'OPEN':
                        status_icon = '🟢'
                    elif status == 'CLOSED':
                        status_icon = '🔒'
                    elif status == 'TERMINATED':
                        status_icon = '🚫'
                    else:
                        status_icon = '❓'
                    
                    method_icon = '📦' if method == '__NEXT_DATA__' else '🌐'
                    
                    print(f"\n✅✅✅ EXTRACTION COMPLETE ✅✅✅")
                    print(f"{status_icon} {restaurant_name} - {status}")
                    print(f"🍽️  {menu_count} menu items extracted")
                    print(f"{method_icon} Method: {method}")
                    print(f"💾 Saved: {result_file.name}")
                    
                    return result
                else:
                    print(f"❌ Attempt {attempt} failed")
                    self._save_files(url_index, attempt, 'failed')

            except Exception as e:
                print(f"❌ Exception: {e}")
                self._save_files(url_index, attempt, 'error')

        # All attempts failed
        return {
            'url': url,
            'url_index': url_index,
            'batch_id': self.batch_id,
            'success': False,
            'error': 'Failed after all attempts',
            'timestamp': time.time()
        }

    def scrape_all(self, urls_file: str = "branch_urls.json"):
        """Scrape all GrabFood URLs from the file"""
        urls = self.load_grabfood_urls(urls_file)
        
        if not urls:
            print("❌ No GrabFood URLs found to scrape!")
            return

        self.stats['total'] = len(urls)

        print("\n" + "="*80)
        print("🍔 GRAB FOOD BATCH SCRAPER")
        print("="*80)
        print(f"📋 Total URLs: {len(urls)}")
        print(f"🔄 Max Retries per URL: {self.max_retries}")
        print(f"⚡ Immediate Retry: {self.immediate_retry}")
        print(f"🆔 Batch ID: {self.batch_id}")
        print(f"📂 Output: {self.output_dir}")
        print("="*80)

        start_time = time.time()

        for idx, url in enumerate(urls):
            try:
                result = self.scrape_url(url, idx)
                
                if result.get('success'):
                    self.stats['success'] += 1
                else:
                    self.stats['failed'] += 1
                
                self.stats['results'].append(result)
                
                # Brief pause between URLs
                if idx < len(urls) - 1:
                    time.sleep(2)
                    
            except Exception as e:
                print(f"\n❌ Unexpected error for URL {idx + 1}: {e}")
                self.stats['failed'] += 1
                self.stats['results'].append({
                    'url': url,
                    'url_index': idx,
                    'success': False,
                    'error': str(e)
                })

        # Save final summary
        elapsed = time.time() - start_time
        self._save_summary(elapsed)

    def _save_summary(self, elapsed_time: float):
        """Save comprehensive summary report"""
        # Count open vs closed vs terminated stores and extraction methods
        open_count = 0
        closed_count = 0
        terminated_count = 0
        json_extraction = 0
        html_extraction = 0
        
        for result in self.stats['results']:
            if result.get('success'):
                status = result.get('data', {}).get('restaurant', {}).get('status', 'UNKNOWN')
                if status == 'OPEN':
                    open_count += 1
                elif status == 'CLOSED':
                    closed_count += 1
                elif status == 'TERMINATED':
                    terminated_count += 1
                
                method = result.get('extraction_method', 'UNKNOWN')
                if method == '__NEXT_DATA__':
                    json_extraction += 1
                elif method == 'HTML_PARSING':
                    html_extraction += 1
        
        summary = {
            'batch_id': self.batch_id,
            'timestamp': time.time(),
            'datetime': datetime.now().isoformat(),
            'elapsed_time_seconds': elapsed_time,
            'statistics': {
                'total': self.stats['total'],
                'success': self.stats['success'],
                'failed': self.stats['failed'],
                'success_rate': f"{(self.stats['success'] / self.stats['total'] * 100):.1f}%" if self.stats['total'] > 0 else "0%",
                'stores_open': open_count,
                'stores_closed': closed_count,
                'stores_terminated': terminated_count,
                'extraction_methods': {
                    'json': json_extraction,
                    'html_parsing': html_extraction
                }
            },
            'results': self.stats['results']
        }

        # Save summary JSON
        summary_file = self.output_dir / f"batch_{self.batch_id}_SUMMARY.json"
        self._save_json_obj(summary_file, summary)

        # Print summary
        print("\n" + "="*80)
        print("📊 BATCH SCRAPING COMPLETE")
        print("="*80)
        print(f"\n⏱️  Total Time: {elapsed_time / 60:.1f} minutes")
        print(f"📋 Total URLs: {self.stats['total']}")
        print(f"✅ Success: {self.stats['success']}")
        print(f"❌ Failed: {self.stats['failed']}")
        print(f"📈 Success Rate: {summary['statistics']['success_rate']}")
        
        if self.stats['success'] > 0:
            print(f"\n🏪 Store Status:")
            print(f"   🟢 Open: {open_count}")
            print(f"   🔒 Closed: {closed_count}")
            if terminated_count > 0:
                print(f"   🚫 Terminated: {terminated_count}")
            
            print(f"\n🔧 Extraction Methods:")
            print(f"   📦 JSON (__NEXT_DATA__): {json_extraction}")
            print(f"   🌐 HTML Parsing: {html_extraction}")

        if self.stats['success'] > 0:
            print(f"\n🎉 Successfully scraped restaurants:")
            for result in self.stats['results']:
                if result.get('success'):
                    name = result['data']['restaurant'].get('name', 'Unknown')
                    items = len(result['data']['menu_items'])
                    status = result['data']['restaurant'].get('status', 'UNKNOWN')
                    method = result.get('extraction_method', '?')
                    
                    if status == 'OPEN':
                        status_icon = '🟢'
                    elif status == 'CLOSED':
                        status_icon = '🔒'
                    elif status == 'TERMINATED':
                        status_icon = '🚫'
                    else:
                        status_icon = '❓'
                    
                    method_icon = '📦' if method == '__NEXT_DATA__' else '🌐'
                    print(f"   {status_icon} {name} ({items} items) - {status} {method_icon}")

        if self.stats['failed'] > 0:
            print(f"\n⚠️  Failed URLs:")
            for result in self.stats['results']:
                if not result.get('success'):
                    url = result.get('url', 'Unknown URL')
                    error = result.get('error', 'Unknown error')
                    print(f"   • {url}")
                    print(f"     Error: {error}")

        print(f"\n📂 Output Directory: {self.output_dir}")
        print(f"📄 Summary File: {summary_file}")
        print(f"\n✨ Batch ID: {self.batch_id}")
        print(f"   All files prefixed with: batch_{self.batch_id}_")

    def close(self):
        """Close browser"""
        if self.driver:
            self.driver.quit()
            print("\n✓ Browser closed")


def main():
    """Main entry point"""
    # Configuration
    scraper = GrabBatchScraper(
        headless=True,           # Automated mode - no browser window
        max_retries=3,           # Number of attempts per URL
        immediate_retry=True     # True = no delays, False = exponential backoff
        # output_dir="~/Desktop/grab_batch"  # (optional) override
    )

    try:
        # Scrape all URLs from branch_urls.json
        scraper.scrape_all("branch_urls.json")
        
    finally:
        scraper.close()


if __name__ == "__main__":
    main()