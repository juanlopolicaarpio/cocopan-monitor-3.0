#!/usr/bin/env python3
"""
GrabFood SKU Scraper - FIXED VERSION v3 + SMS ALERTS
- Fixed product name extraction (no more concatenated descriptions)
- Fixed out-of-stock detection (better add button detection)
- Fixed ChromeDriver with undetected-chromedriver (auto version matching)
- SMS alerts to store managers when OOS items found
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
# Chrome Binary Finder
# ============================================================================
def find_chrome_binary():
    """Find Chrome binary on the system."""
    import subprocess
    
    mac_paths = [
        '/Users/arthur.policarpio/Applications/Google Chrome.app/Contents/MacOS/Google Chrome',
        '/Applications/Google Chrome.app/Contents/MacOS/Google Chrome',
        '/Applications/Chromium.app/Contents/MacOS/Chromium',
        os.path.expanduser('~/Applications/Google Chrome.app/Contents/MacOS/Google Chrome'),
    ]
    
    for path in mac_paths:
        if os.path.exists(path):
            logger.info(f"Found Chrome at: {path}")
            return path
    
    try:
        result = subprocess.run(['which', 'google-chrome'], 
                              capture_output=True, text=True, timeout=5)
        if result.returncode == 0 and result.stdout.strip():
            return result.stdout.strip()
    except:
        pass
    
    logger.warning("Chrome not found in standard locations")
    return None

# ============================================================================
# GrabFood Scraper
# ============================================================================
class GrabFoodScraper:
    """Fixed scraper with correct name extraction and OOS detection"""
    
    def __init__(self, send_alerts: bool = True):
        self.driver = None
        self.send_alerts = send_alerts
        self.alert_service = None
        
        # Initialize SMS alert service
        if self.send_alerts:
            try:
                from sms_alerts import SMSAlertService
                self.alert_service = SMSAlertService()
                logger.info("SMS Alert Service initialized")
            except ImportError:
                logger.warning("sms_alerts.py not found - alerts disabled")
                self.send_alerts = False
            except Exception as e:
                logger.warning(f"Failed to initialize SMS alerts: {e}")
                self.send_alerts = False
        
        self.setup_driver()
    
    def setup_driver(self):
        """Setup Chrome driver with undetected-chromedriver"""
        try:
            import undetected_chromedriver as uc
            
            logger.info("Setting up Chrome with undetected-chromedriver...")
            
            chrome_binary = find_chrome_binary()
            if not chrome_binary:
                raise Exception("Chrome not found!")
            
            options = uc.ChromeOptions()
            options.add_argument('--headless=new')
            options.add_argument('--no-sandbox')
            options.add_argument('--disable-dev-shm-usage')
            options.add_argument('--disable-gpu')
            options.add_argument('--window-size=1920,1080')
            options.add_argument('--lang=en-PH')
            
            user_agent = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36'
            options.add_argument(f'--user-agent={user_agent}')
            
            self.driver = uc.Chrome(
                options=options,
                browser_executable_path=chrome_binary,
                version_main=145,
                use_subprocess=True
            )
            
            logger.info("Chrome WebDriver initialized")
            
        except ImportError:
            logger.error("undetected-chromedriver not installed!")
            raise
        except Exception as e:
            logger.error(f"Failed to initialize Chrome: {e}")
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
            'unavailable_items': [],
            'alert_sent': False,
            'alert_result': None
        }
        
        try:
            logger.info("="*80)
            logger.info(f"Loading: {url}")
            self.driver.get(url)
            
            logger.info("Waiting for page to load...")
            time.sleep(12)
            
            logger.info("Scrolling to load all items...")
            for i in range(5):
                scroll_height = self.driver.execute_script("return document.body.scrollHeight")
                self.driver.execute_script(f"window.scrollTo(0, {scroll_height * (i+1) / 5});")
                time.sleep(2)
            
            html = self.driver.page_source
            soup = BeautifulSoup(html, 'html.parser')
            
            debug_file = f"debug_{store_name.replace(' ', '_')}.html"
            with open(debug_file, 'w', encoding='utf-8') as f:
                f.write(html)
            logger.info(f"Debug HTML saved: {debug_file}")
            
            if soup.title and soup.title.string:
                title = soup.title.string
                if '⭐' in title:
                    store_name = title.split('⭐')[0].strip()
                    result['store_name'] = store_name
            
            logger.info("Parsing menu items...")
            all_items = self._parse_menu_items(soup)
            result['all_items'] = all_items
            
            for item in all_items:
                if item['is_available']:
                    result['available_items'].append(item)
                else:
                    result['unavailable_items'].append(item)
            
            total_items = len(all_items)
            unavailable_count = len(result['unavailable_items'])
            compliance_pct = ((total_items - unavailable_count) / total_items * 100) if total_items > 0 else 100.0
            result['compliance_pct'] = compliance_pct
            
            logger.info("")
            logger.info("="*80)
            logger.info(f"{store_name} - RESULTS:")
            logger.info("="*80)
            logger.info(f"  Total items: {len(all_items)}")
            logger.info(f"  Available: {len(result['available_items'])}")
            logger.info(f"  Unavailable: {len(result['unavailable_items'])}")
            logger.info(f"  Compliance: {compliance_pct:.1f}%")
            
            if result['unavailable_items']:
                logger.info("")
                logger.info("UNAVAILABLE ITEMS:")
                for item in result['unavailable_items']:
                    logger.info(f"  - {item['name']} (P{item['price']})")
                
                # Send SMS alert
                if self.send_alerts and self.alert_service:
                    logger.info("")
                    logger.info("Sending SMS alert...")
                    
                    oos_items = [
                        {'product_name': item['name']} 
                        for item in result['unavailable_items']
                    ]
                    
                    alert_result = self.alert_service.send_oos_alert(
                        store_name=store_name,
                        store_url=url,
                        oos_items=oos_items,
                        compliance_pct=compliance_pct
                    )
                    
                    result['alert_sent'] = alert_result['sent'] > 0
                    result['alert_result'] = alert_result
                    
                    if alert_result['sent'] > 0:
                        logger.info(f"[OK] SMS sent to {alert_result['sent']} recipient(s)")
                        for r in alert_result['recipients']:
                            logger.info(f"     - {r['name']} ({r['role']})")
                    elif alert_result['skipped'] > 0:
                        logger.info("[SKIP] Alert skipped (quiet hours or below threshold)")
                    else:
                        logger.info("[FAIL] Failed to send SMS alert")
            else:
                logger.info("")
                logger.info("[OK] All items available - no alert needed")
            
            if result['available_items']:
                logger.info("")
                logger.info(f"SAMPLE AVAILABLE ITEMS (first 5):")
                for item in result['available_items'][:5]:
                    logger.info(f"  - {item['name']} (P{item['price']})")
            
            logger.info("="*80)
            
            return result
            
        except Exception as e:
            logger.error(f"Error scraping {store_name}: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return result
    
    def _parse_menu_items(self, soup: BeautifulSoup) -> List[Dict]:
        """Parse all menu items from the page"""
        all_items = []
        seen_items = set()
        
        wrappers = soup.find_all('div', class_=lambda x: x and 'menuItem___' in str(x) and 'menuItemWrapper' not in str(x))
        
        logger.info(f"Found {len(wrappers)} menu item cards")
        
        if not wrappers:
            logger.warning("No menu items found!")
            return []
        
        for idx, wrapper in enumerate(wrappers, 1):
            item_data = self._extract_item_info(wrapper, idx)
            if item_data:
                item_key = f"{item_data['name']}_{item_data['price']}"
                
                if item_key not in seen_items:
                    seen_items.add(item_key)
                    all_items.append(item_data)
        
        logger.info(f"Extracted {len(all_items)} unique items")
        
        return all_items
    
    def _extract_item_info(self, wrapper, item_num: int) -> Optional[Dict]:
        """Extract item information from wrapper"""
        try:
            product_name = self._extract_product_name(wrapper)
            description = self._extract_description(wrapper, product_name)
            price = self._extract_price(wrapper)
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
        """Extract ONLY the product name"""
        
        for tag in ['h3', 'h4', 'h2', 'h5', 'h1']:
            heading = wrapper.find(tag)
            if heading:
                direct_texts = []
                for child in heading.children:
                    if isinstance(child, str):
                        text = child.strip()
                        if text:
                            direct_texts.append(text)
                
                if direct_texts:
                    name = ' '.join(direct_texts)
                else:
                    name = heading.get_text(separator='\n', strip=True)
                    name = name.split('\n')[0].strip()
                
                name = self._clean_product_name(name)
                
                if len(name) > 2 and len(name) < 80:
                    return name
        
        name_element = wrapper.find(class_=lambda x: x and any(
            keyword in str(x).lower() 
            for keyword in ['itemname', 'item-name', 'name___', 'title___']
        ))
        
        if name_element:
            direct_texts = []
            for child in name_element.children:
                if isinstance(child, str):
                    text = child.strip()
                    if text:
                        direct_texts.append(text)
            
            if direct_texts:
                name = ' '.join(direct_texts)
            else:
                name = name_element.get_text(separator='\n', strip=True)
                name = name.split('\n')[0].strip()
            
            name = self._clean_product_name(name)
            
            if name and len(name) > 2 and len(name) < 80:
                return name
        
        all_text = wrapper.get_text(separator='\n', strip=True)
        lines = [line.strip() for line in all_text.split('\n') if line.strip()]
        
        if lines:
            name = lines[0]
            name = self._clean_product_name(name)
            
            if len(name) > 2 and len(name) < 80:
                return name
        
        return "[Unknown Product]"
    
    def _clean_product_name(self, name: str) -> str:
        """Clean and extract just the product name"""
        if not name:
            return name
        
        name = re.sub(r'\s*P?\s*\d+[\d,.]*\s*$', '', name)
        
        if '.' in name:
            parts = name.split('.')
            name = parts[0].strip()
        
        match = re.match(r'^([^.]+?[a-z])([A-Z][a-z].{10,})$', name)
        if match:
            name = match.group(1).strip()
        
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
        """Extract product description"""
        all_text = wrapper.get_text(separator='|', strip=True)
        parts = [p.strip() for p in all_text.split('|') if p.strip()]
        
        if len(parts) > 1:
            for i, part in enumerate(parts):
                if product_name in part and i + 1 < len(parts):
                    desc = parts[i + 1]
                    desc = re.sub(r'\s*[\d,.]+\s*$', '', desc)
                    if len(desc) > 5 and not desc.isdigit():
                        return desc
            
            desc = parts[1]
            desc = re.sub(r'\s*[\d,.]+\s*$', '', desc)
            if len(desc) > 5 and not desc.isdigit():
                return desc
        
        return ""
    
    def _extract_price(self, wrapper) -> str:
        """Extract price"""
        price_element = wrapper.find(class_=lambda x: x and 'price' in str(x).lower())
        if price_element:
            price_text = price_element.get_text().strip()
            price_match = re.search(r'([\d,.]+)', price_text)
            if price_match:
                return price_match.group(1)
        
        all_text = wrapper.get_text()
        
        price_match = re.search(r'P?\s*(\d+\.\d{2})', all_text)
        if price_match:
            return price_match.group(1)
        
        price_match = re.search(r'P?\s*(\d{2,})\b', all_text)
        if price_match:
            return price_match.group(1)
        
        return "N/A"
    
    def _check_availability(self, wrapper) -> tuple:
        """Check if item is available"""
        
        text_content = wrapper.get_text().lower()
        unavailable_phrases = [
            'sold out', 'not available', 'unavailable',
            'out of stock', 'currently unavailable'
        ]
        
        for phrase in unavailable_phrases:
            if phrase in text_content:
                return (False, f"Contains text: '{phrase}'")
        
        wrapper_classes = ' '.join(wrapper.get('class', [])).lower()
        if any(keyword in wrapper_classes for keyword in ['disabled', 'unavailable', 'soldout', 'sold-out']):
            return (False, "Item wrapper has disabled class")
        
        found_add_button = False
        
        clickable_elements = wrapper.find_all(['button', 'div', 'a'], recursive=True, limit=30)
        
        for element in clickable_elements:
            elem_classes = ' '.join(element.get('class', [])).lower()
            
            is_button = False
            
            if element.get('role') == 'button':
                is_button = True
            if element.name == 'button':
                is_button = True
            
            button_keywords = ['add', 'plus', 'increment', 'cart', 'btn', 'button']
            if any(keyword in elem_classes for keyword in button_keywords):
                is_button = True
            
            if element.get('onclick'):
                is_button = True
            
            style = element.get('style', '').lower()
            if 'cursor' in style and 'pointer' in style:
                is_button = True
            
            if is_button:
                is_disabled = (
                    element.get('disabled') is not None or
                    element.get('aria-disabled') == 'true' or
                    'disabled' in elem_classes or
                    'grayed' in elem_classes
                )
                
                if not is_disabled:
                    elem_text = element.get_text().strip().lower()
                    has_svg = element.find('svg') is not None
                    is_add_text = elem_text in ['add', '+', 'add to cart']
                    
                    if has_svg or is_add_text or ('add' in elem_classes):
                        found_add_button = True
                        break
        
        if not found_add_button:
            svgs = wrapper.find_all('svg', limit=10)
            for svg in svgs:
                parent = svg.parent
                if parent and parent.name in ['button', 'div', 'a']:
                    parent_classes = ' '.join(parent.get('class', [])).lower()
                    
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
                        break
        
        if found_add_button:
            return (True, "Has enabled add button")
        else:
            return (False, "No enabled add button found")
    
    def scrape_all_stores(self, urls: List[str]) -> Dict:
        """Scrape all stores"""
        results = {
            'scrape_time': datetime.now().isoformat(),
            'total_stores': len(urls),
            'stores': [],
            'alerts_sent': 0,
            'alerts_failed': 0
        }
        
        for i, url in enumerate(urls, 1):
            logger.info("\n")
            logger.info("#"*80)
            logger.info(f"# STORE {i}/{len(urls)}")
            logger.info("#"*80)
            
            store_result = self.scrape_menu(url)
            results['stores'].append(store_result)
            
            if store_result.get('alert_result'):
                results['alerts_sent'] += store_result['alert_result'].get('sent', 0)
                results['alerts_failed'] += store_result['alert_result'].get('failed', 0)
            
            if i < len(urls):
                delay = random.uniform(3, 6)
                logger.info(f"\nWaiting {delay:.1f}s...\n")
                time.sleep(delay)
        
        return results
    
    def close(self):
        """Close browser"""
        if self.driver:
            try:
                self.driver.quit()
                logger.info("Browser closed")
            except:
                pass
    
    def __del__(self):
        self.close()

# ============================================================================
# Main
# ============================================================================
def main():
    """Main entry point"""
    import sys
    
    send_alerts = '--no-alerts' not in sys.argv
    
    logger.info("="*80)
    logger.info("GrabFood SKU Scraper - FIXED VERSION v3 + SMS ALERTS")
    logger.info(f"SMS Alerts: {'ENABLED' if send_alerts else 'DISABLED'}")
    logger.info("="*80)
    logger.info("")
    
    scraper = GrabFoodScraper(send_alerts=send_alerts)
    
    try:
        results = scraper.scrape_all_stores(STORE_URLS)
        
        output_file = f"grabfood_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(results, f, indent=2, ensure_ascii=False)
        
        logger.info("\n")
        logger.info("="*80)
        logger.info("FINAL SUMMARY")
        logger.info("="*80)
        
        total_items = sum(len(store['all_items']) for store in results['stores'])
        total_available = sum(len(store['available_items']) for store in results['stores'])
        total_unavailable = sum(len(store['unavailable_items']) for store in results['stores'])
        
        logger.info(f"Total stores: {results['total_stores']}")
        logger.info(f"Total items: {total_items}")
        if total_items > 0:
            logger.info(f"  Available: {total_available} ({total_available/total_items*100:.1f}%)")
            logger.info(f"  Unavailable: {total_unavailable} ({total_unavailable/total_items*100:.1f}%)")
        
        if send_alerts:
            logger.info("")
            logger.info("SMS ALERT STATS:")
            logger.info(f"  Sent: {results['alerts_sent']}")
            logger.info(f"  Failed: {results['alerts_failed']}")
        
        if total_unavailable > 0:
            logger.info("")
            logger.info("ALL UNAVAILABLE ITEMS:")
            for store in results['stores']:
                if store['unavailable_items']:
                    logger.info(f"\n  {store['store_name']}:")
                    for item in store['unavailable_items']:
                        logger.info(f"    - {item['name']} (P{item['price']})")
        else:
            logger.info("")
            logger.info("All items are currently available!")
        
        logger.info(f"\nResults saved: {output_file}")
        logger.info("="*80)
        
    except KeyboardInterrupt:
        logger.info("\nInterrupted")
    except Exception as e:
        logger.error(f"\nError: {e}")
        import traceback
        logger.error(traceback.format_exc())
    finally:
        scraper.close()

if __name__ == "__main__":
    main()