#!/usr/bin/env python3
"""
GrabFood SKU Scraper - FIXED VERSION v3
✅ Fixed product name extraction (no more concatenated descriptions)
✅ Fixed out-of-stock detection (better add button detection)

✅ Fixed ChromeDriver with undetected-chromedriver (auto version matching)
"""
import os
import json
import time
import logging
import random
import re
from datetime import datetime
from typing import List, Dict, Optional
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
STORE_URLS = [
    "https://food.grab.com/ph/en/restaurant/cocopan-maysilo-delivery/2-C6TATTL2UF2UDA",
    "https://food.grab.com/ph/en/restaurant/cocopan-anonas-delivery/2-C6XVCUDGNXNZNN",
    "https://food.grab.com/ph/en/restaurant/cocopan-altura-santa-mesa-delivery/2-C7EUVP2UEJ43L6"
]

# ============================================================================
# GrabFood Scraper - Fixed Version with undetected-chromedriver
# ============================================================================
class GrabFoodScraper:
    """Fixed scraper with correct name extraction and OOS detection"""
    
    def __init__(self):
        self.driver = None
        self.setup_driver()
    
    def setup_driver(self):
        """Setup Chrome driver with undetected-chromedriver (auto version matching)"""
        try:
            import undetected_chromedriver as uc
            
            logger.info("🔧 Setting up Chrome with undetected-chromedriver...")
            logger.info("   (This will automatically match your Chrome version)")
            chrome_binary = "/Users/arthur.policarpio/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"

            
            # Configure options
            options = uc.ChromeOptions()
            options.add_argument('--headless=new')
            options.add_argument('--no-sandbox')
            options.add_argument('--disable-dev-shm-usage')
            options.add_argument('--disable-gpu')
            options.add_argument('--window-size=1920,1080')
            
            user_agent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
            options.add_argument(f'user-agent={user_agent}')
            
            # Create driver (automatically handles version matching!)
            self.driver = uc.Chrome(options=options,browser_executable_path=chrome_binary, version_main=143)
            
            logger.info("✅ Chrome WebDriver initialized with auto version matching")
            
        except ImportError:
            logger.error("❌ undetected-chromedriver not installed!")
            logger.error("🔧 Install it with: pip install undetected-chromedriver")
            raise
        except Exception as e:
            logger.error(f"❌ Failed to initialize Chrome: {e}")
            logger.error("🔧 Make sure Google Chrome is installed")
            raise
    
    def extract_store_name(self, url: str) -> str:
        """Extract store name from URL"""
        try:
            match = re.search(r'/restaurant/([^/]+)', url)
            if match:
                name = match.group(1).replace('-', ' ').title()
                name = re.sub(r'\s+Delivery$', '', name, flags=re.IGNORECASE)
                return name
        except:
            pass
        return "Unknown Store"
    
    def scrape_menu(self, url: str) -> Dict:
        """Scrape menu from GrabFood store"""
        store_name = self.extract_store_name(url)
        result = {
            'store_name': store_name,
            'url': url,
            'all_items': [],
            'available_items': [],
            'unavailable_items': []
        }
        
        try:
            logger.info("="*80)
            logger.info(f"🌐 Loading: {url}")
            self.driver.get(url)
            
            # Wait for page to load
            logger.info("⏱️ Waiting for page to load...")
            time.sleep(12)
            
            # Scroll to load all items
            logger.info("📜 Scrolling to load all items...")
            for i in range(5):
                scroll_height = self.driver.execute_script("return document.body.scrollHeight")
                self.driver.execute_script(f"window.scrollTo(0, {scroll_height * (i+1) / 5});")
                time.sleep(2)
            
            # Get page source
            html = self.driver.page_source
            soup = BeautifulSoup(html, 'html.parser')
            
            # Save HTML for debugging
            debug_file = f"debug_{store_name.replace(' ', '_')}.html"
            with open(debug_file, 'w', encoding='utf-8') as f:
                f.write(html)
            logger.info(f"💾 Debug HTML saved: {debug_file}")
            
            # Extract store name from title
            if soup.title and soup.title.string:
                title = soup.title.string
                if '⭐' in title:
                    store_name = title.split('⭐')[0].strip()
                    result['store_name'] = store_name
            
            # Parse menu items
            logger.info("🔍 Parsing menu items...")
            all_items = self._parse_menu_items(soup)
            result['all_items'] = all_items
            
            # Separate available and unavailable
            for item in all_items:
                if item['is_available']:
                    result['available_items'].append(item)
                else:
                    result['unavailable_items'].append(item)
            
            # Log summary
            logger.info("")
            logger.info("="*80)
            logger.info(f"📊 {store_name} - RESULTS:")
            logger.info("="*80)
            logger.info(f"  Total items: {len(all_items)}")
            logger.info(f"  🟢 Available: {len(result['available_items'])}")
            logger.info(f"  🔴 Unavailable: {len(result['unavailable_items'])}")
            
            if result['unavailable_items']:
                logger.info("")
                logger.info("🔴 UNAVAILABLE ITEMS:")
                for item in result['unavailable_items']:
                    logger.info(f"  - {item['name']} (₱{item['price']})")
                    logger.info(f"    Reason: {item['reason']}")
            
            if result['available_items']:
                logger.info("")
                logger.info(f"🟢 SAMPLE AVAILABLE ITEMS (first 5):")
                for item in result['available_items'][:5]:
                    logger.info(f"  - {item['name']} (₱{item['price']})")
            
            logger.info("="*80)
            
            return result
            
        except Exception as e:
            logger.error(f"❌ Error scraping {store_name}: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return result
    
    def _parse_menu_items(self, soup: BeautifulSoup) -> List[Dict]:
        """Parse all menu items from the page"""
        all_items = []
        seen_items = set()
        
        # Find INNER wrappers - these are the actual individual item cards
        wrappers = soup.find_all('div', class_=lambda x: x and 'menuItem___' in str(x) and 'menuItemWrapper' not in str(x))
        
        logger.info(f"📋 Found {len(wrappers)} menu item cards")
        
        if not wrappers:
            logger.warning("⚠️ No menu items found!")
            return []
        
        for idx, wrapper in enumerate(wrappers, 1):
            item_data = self._extract_item_info(wrapper, idx)
            if item_data:
                # Create unique key to detect duplicates
                item_key = f"{item_data['name']}_{item_data['price']}"
                
                if item_key not in seen_items:
                    seen_items.add(item_key)
                    all_items.append(item_data)
        
        logger.info(f"✅ Extracted {len(all_items)} unique items (filtered {len(wrappers) - len(all_items)} duplicates)")
        
        return all_items
    
    def _extract_item_info(self, wrapper, item_num: int) -> Optional[Dict]:
        """Extract item information from wrapper"""
        try:
            # Extract product name (FIXED)
            product_name = self._extract_product_name(wrapper)
            
            # Extract description
            description = self._extract_description(wrapper, product_name)
            
            # Extract price
            price = self._extract_price(wrapper)
            
            # Check availability (FIXED)
            is_available, reason = self._check_availability(wrapper)
            
            return {
                'name': product_name,
                'description': description,
                'price': price,
                'is_available': is_available,
                'reason': reason,
                'item_number': item_num
            }
            
        except Exception as e:
            logger.debug(f"Error extracting item {item_num}: {e}")
            return None
    
    def _extract_product_name(self, wrapper) -> str:
        """Extract ONLY the product name (FIXED v2 - Better text extraction)"""
        
        # Method 1: Find the title/heading element with better text extraction
        for tag in ['h3', 'h4', 'h2', 'h5', 'h1']:
            heading = wrapper.find(tag)
            if heading:
                # Get text using NavigableString to avoid concatenation issues
                # First, try to get only direct text children (not nested)
                direct_texts = []
                for child in heading.children:
                    if isinstance(child, str):  # It's a NavigableString
                        text = child.strip()
                        if text:
                            direct_texts.append(text)
                
                if direct_texts:
                    name = ' '.join(direct_texts)
                else:
                    # Fallback: get all text but with newline separator to detect boundaries
                    name = heading.get_text(separator='\n', strip=True)
                    # Take only the first line (usually the name)
                    name = name.split('\n')[0].strip()
                
                # Clean up the name
                name = self._clean_product_name(name)
                
                if len(name) > 2 and len(name) < 80:
                    logger.debug(f"    Found name via heading: '{name}'")
                    return name
        
        # Method 2: Look for elements with name/title classes
        name_element = wrapper.find(class_=lambda x: x and any(
            keyword in str(x).lower() 
            for keyword in ['itemname', 'item-name', 'name___', 'title___']
        ))
        
        if name_element:
            # Try to get only direct text children
            direct_texts = []
            for child in name_element.children:
                if isinstance(child, str):
                    text = child.strip()
                    if text:
                        direct_texts.append(text)
            
            if direct_texts:
                name = ' '.join(direct_texts)
            else:
                # Use newline separator to detect boundaries
                name = name_element.get_text(separator='\n', strip=True)
                name = name.split('\n')[0].strip()
            
            name = self._clean_product_name(name)
            
            if name and len(name) > 2 and len(name) < 80:
                logger.debug(f"    Found name via class: '{name}'")
                return name
        
        # Method 3: Parse using newline separator (better than pipe)
        all_text = wrapper.get_text(separator='\n', strip=True)
        lines = [line.strip() for line in all_text.split('\n') if line.strip()]
        
        if lines:
            # First non-empty line is usually the name
            name = lines[0]
            name = self._clean_product_name(name)
            
            if len(name) > 2 and len(name) < 80:
                logger.debug(f"    Found name via text parse: '{name}'")
                return name
        
        return "[Unknown Product]"
    
    def _clean_product_name(self, name: str) -> str:
        """Clean and extract just the product name"""
        if not name:
            return name
        
        # Remove price patterns (₱123 or 123.00 at the end)
        name = re.sub(r'\s*₱?\s*\d+[\d,.]*\s*$', '', name)
        
        # If there's a period followed by uppercase or description text, split there
        # This catches cases like "Chicken Asado Bun.A soft golden..."
        # or "Chicken Asado Bun. A soft golden..."
        if '.' in name:
            parts = name.split('.')
            # Take the first part which is usually the name
            name = parts[0].strip()
        
        # Remove common description starters if they got concatenated
        # Look for patterns like "NameDescriptionText" and split at capital letter after lowercase
        # This catches "Chicken Asado BunA soft golden..."
        match = re.match(r'^([^.]+?[a-z])([A-Z][a-z].{10,})$', name)
        if match:
            # The name is the first capture group
            name = match.group(1).strip()
        
        # Additional cleanup: remove anything after common description words
        desc_markers = [
            'A soft', 'A golden', 'A delicious', 'A crispy', 'A fluffy',
            'Made with', 'Served with', 'Topped with', 'Filled with',
            'Perfect for', 'Great for', 'Ideal for'
        ]
        for marker in desc_markers:
            if marker in name:
                name = name.split(marker)[0].strip()
        
        return name.strip()
    
    def _extract_description(self, wrapper, product_name: str) -> str:
        """Extract product description (separate from name)"""
        # Get all text
        all_text = wrapper.get_text(separator='|', strip=True)
        parts = [p.strip() for p in all_text.split('|') if p.strip()]
        
        # Description is usually the second part
        if len(parts) > 1:
            # Find the part that comes after the name
            for i, part in enumerate(parts):
                if product_name in part and i + 1 < len(parts):
                    desc = parts[i + 1]
                    # Remove price
                    desc = re.sub(r'\s*[\d,.]+\s*$', '', desc)
                    if len(desc) > 5 and not desc.isdigit():
                        return desc
            
            # Fallback: second part
            desc = parts[1]
            desc = re.sub(r'\s*[\d,.]+\s*$', '', desc)
            if len(desc) > 5 and not desc.isdigit():
                return desc
        
        return ""
    
    def _extract_price(self, wrapper) -> str:
        """Extract price"""
        # Look for price class
        price_element = wrapper.find(class_=lambda x: x and 'price' in str(x).lower())
        if price_element:
            price_text = price_element.get_text().strip()
            price_match = re.search(r'([\d,.]+)', price_text)
            if price_match:
                return price_match.group(1)
        
        # Look for price pattern in all text
        all_text = wrapper.get_text()
        
        # Try decimal price first
        price_match = re.search(r'₱?\s*(\d+\.\d{2})', all_text)
        if price_match:
            return price_match.group(1)
        
        # Try whole number price
        price_match = re.search(r'₱?\s*(\d{2,})\b', all_text)
        if price_match:
            return price_match.group(1)
        
        return "N/A"
    
    def _check_availability(self, wrapper) -> tuple:
        """
        Check if item is available - FIXED v2
        
        Improved logic:
        1. Check for explicit unavailability indicators first
        2. Then look for add button presence
        3. Add more robust detection patterns
        """
        
        # STEP 1: Check for explicit unavailability text FIRST
        text_content = wrapper.get_text().lower()
        unavailable_phrases = [
            'sold out',
            'not available',
            'unavailable',
            'out of stock',
            'currently unavailable'
        ]
        
        for phrase in unavailable_phrases:
            if phrase in text_content:
                logger.debug(f"    ❌ Found unavailable text: '{phrase}'")
                return (False, f"Contains text: '{phrase}'")
        
        # STEP 2: Look for disabled/unavailable class names
        wrapper_classes = ' '.join(wrapper.get('class', [])).lower()
        if any(keyword in wrapper_classes for keyword in ['disabled', 'unavailable', 'soldout', 'sold-out']):
            logger.debug(f"    ❌ Wrapper has disabled class")
            return (False, "Item wrapper has disabled/unavailable class")
        
        # STEP 3: Look for add button within THIS wrapper only
        # Be more specific about what constitutes an "add button"
        found_add_button = False
        
        # Look for clickable elements (buttons, divs with onclick, etc.)
        clickable_elements = wrapper.find_all(['button', 'div', 'a'], recursive=True, limit=30)
        
        for element in clickable_elements:
            # Get classes
            elem_classes = ' '.join(element.get('class', [])).lower()
            
            # Check for button-like indicators
            is_button = False
            
            # Check role
            if element.get('role') == 'button':
                is_button = True
            
            # Check tag name
            if element.name == 'button':
                is_button = True
            
            # Check classes for button/add keywords
            button_keywords = ['add', 'plus', 'increment', 'cart', 'btn', 'button']
            if any(keyword in elem_classes for keyword in button_keywords):
                is_button = True
            
            # Check for click handlers
            if element.get('onclick'):
                is_button = True
            
            # Check inline styles for cursor:pointer
            style = element.get('style', '').lower()
            if 'cursor' in style and 'pointer' in style:
                is_button = True
            
            # If this looks like a button, check if it's enabled
            if is_button:
                # Check if disabled
                is_disabled = (
                    element.get('disabled') is not None or
                    element.get('aria-disabled') == 'true' or
                    'disabled' in elem_classes or
                    'grayed' in elem_classes
                )
                
                if not is_disabled:
                    # Check if this is specifically an ADD button (not quantity controls)
                    elem_text = element.get_text().strip().lower()
                    
                    # Look for SVG (plus icon) or specific text
                    has_svg = element.find('svg') is not None
                    is_add_text = elem_text in ['add', '+', 'add to cart']
                    
                    if has_svg or is_add_text or ('add' in elem_classes):
                        found_add_button = True
                        logger.debug(f"    ✅ Found enabled add button")
                        break
        
        # STEP 4: Additional check - look for SVG plus icons
        if not found_add_button:
            svgs = wrapper.find_all('svg', limit=10)
            for svg in svgs:
                # Check if SVG is in a clickable parent
                parent = svg.parent
                if parent and parent.name in ['button', 'div', 'a']:
                    parent_classes = ' '.join(parent.get('class', [])).lower()
                    
                    # Check if parent is clickable and not disabled
                    is_clickable = (
                        parent.get('role') == 'button' or
                        parent.name == 'button' or
                        'click' in parent_classes or
                        'button' in parent_classes
                    )
                    
                    is_disabled = (
                        parent.get('disabled') is not None or
                        parent.get('aria-disabled') == 'true' or
                        'disabled' in parent_classes
                    )
                    
                    if is_clickable and not is_disabled:
                        found_add_button = True
                        logger.debug(f"    ✅ Found SVG add button")
                        break
        
        # Return result
        if found_add_button:
            return (True, "Has enabled add button")
        else:
            logger.debug(f"    ❌ No add button found")
            return (False, "No enabled add button found")
    
    def scrape_all_stores(self, urls: List[str]) -> Dict:
        """Scrape all stores"""
        results = {
            'scrape_time': datetime.now().isoformat(),
            'total_stores': len(urls),
            'stores': []
        }
        
        for i, url in enumerate(urls, 1):
            logger.info("\n")
            logger.info("#"*80)
            logger.info(f"# STORE {i}/{len(urls)}")
            logger.info("#"*80)
            
            store_result = self.scrape_menu(url)
            results['stores'].append(store_result)
            
            # Delay between stores
            if i < len(urls):
                delay = random.uniform(3, 6)
                logger.info(f"\n⏱️ Waiting {delay:.1f}s...\n")
                time.sleep(delay)
        
        return results
    
    def close(self):
        """Close browser"""
        if self.driver:
            try:
                self.driver.quit()
                logger.info("✅ Browser closed")
            except:
                pass
    
    def __del__(self):
        self.close()

# ============================================================================
# Main
# ============================================================================
def main():
    """Main entry point"""
    logger.info("="*80)
    logger.info("🛒 GrabFood SKU Scraper - FIXED VERSION v3")
    logger.info("✅ Fixed product name extraction (no concatenated descriptions)")
    logger.info("✅ Fixed out-of-stock detection (improved button detection)")
    logger.info("✅ Fixed ChromeDriver with undetected-chromedriver (auto version)")
    logger.info("="*80)
    logger.info("")
    
    scraper = GrabFoodScraper()
    
    try:
        # Scrape all stores
        results = scraper.scrape_all_stores(STORE_URLS)
        
        # Save results
        output_file = f"grabfood_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(results, f, indent=2, ensure_ascii=False)
        
        # Print summary
        logger.info("\n")
        logger.info("="*80)
        logger.info("📊 FINAL SUMMARY")
        logger.info("="*80)
        
        total_items = sum(len(store['all_items']) for store in results['stores'])
        total_available = sum(len(store['available_items']) for store in results['stores'])
        total_unavailable = sum(len(store['unavailable_items']) for store in results['stores'])
        
        logger.info(f"Total stores: {results['total_stores']}")
        logger.info(f"Total items: {total_items}")
        if total_items > 0:
            logger.info(f"  🟢 Available: {total_available} ({total_available/total_items*100:.1f}%)")
            logger.info(f"  🔴 Unavailable: {total_unavailable} ({total_unavailable/total_items*100:.1f}%)")
        
        # List all unavailable items across all stores
        if total_unavailable > 0:
            logger.info("")
            logger.info("🔴 ALL UNAVAILABLE ITEMS:")
            for store in results['stores']:
                if store['unavailable_items']:
                    logger.info(f"\n  {store['store_name']}:")
                    for item in store['unavailable_items']:
                        logger.info(f"    - {item['name']} (₱{item['price']})")
                        logger.info(f"      Reason: {item['reason']}")
        else:
            logger.info("")
            logger.info("✅ All items are currently available!")
        
        logger.info(f"\n💾 Results saved: {output_file}")
        logger.info(f"🐛 Debug HTML: debug_*.html")
        logger.info("="*80)
        
    except KeyboardInterrupt:
        logger.info("\n🛑 Interrupted")
    except Exception as e:
        logger.error(f"\n❌ Error: {e}")
        import traceback
        logger.error(traceback.format_exc())
    finally:
        scraper.close()

if __name__ == "__main__":
    main()