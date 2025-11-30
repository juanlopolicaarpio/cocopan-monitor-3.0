#!/usr/bin/env python3
"""
GrabFood Product & SKU Scraper
Extracts all products and their availability status from GrabFood stores
Reads store URLs from branch_urls.json
"""

import json
import time
import logging
import random
import re
from typing import List, Dict, Any, Optional, Tuple
from urllib.parse import urlparse
from datetime import datetime
import requests
from pathlib import Path

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)


class GrabFoodProductScraper:
    """Scrapes all products and SKUs from GrabFood stores"""
    
    def __init__(self, branch_urls_file: str = "branch_urls.json"):
        self.branch_urls_file = branch_urls_file
        self.store_urls = self._load_grabfood_urls()
        
        # Same headers and config as monitor service
        self.user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        ]
        
        self.ph_latlng = "14.5995,120.9842"  # Manila center
        self.headers = {
            "User-Agent": random.choice(self.user_agents),
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "en-PH,en;q=0.9",
            "Origin": "https://food.grab.com",
            "Referer": "https://food.grab.com/",
            "Connection": "keep-alive",
        }
        
        logger.info(f"🛒 GrabFood Product Scraper initialized")
        logger.info(f"📋 {len(self.store_urls)} GrabFood stores loaded from {branch_urls_file}")
    
    def _load_grabfood_urls(self) -> List[str]:
        """Load ONLY GrabFood URLs from branch_urls.json"""
        try:
            file_path = Path(self.branch_urls_file)
            if not file_path.exists():
                logger.error(f"❌ File not found: {self.branch_urls_file}")
                return []
            
            with open(file_path) as f:
                data = json.load(f)
                all_urls = data.get('urls', [])
                
                # Filter to only GrabFood URLs
                grabfood_urls = [url for url in all_urls if 'grab.com' in url]
                foodpanda_urls = [url for url in all_urls if 'foodpanda' in url]
                
                logger.info(f"📋 Loaded {len(all_urls)} total URLs from {self.branch_urls_file}")
                logger.info(f"🛒 Filtered to {len(grabfood_urls)} GrabFood URLs")
                logger.info(f"🐼 Skipping {len(foodpanda_urls)} Foodpanda URLs")
                
                return grabfood_urls
        
        except Exception as e:
            logger.error(f"Failed to load URLs: {e}")
            return []
    
    def extract_merchant_id(self, url: str) -> Optional[str]:
        """Extract merchant ID from GrabFood URL (e.g., '2-C6TATTL2UF2UDA')"""
        try:
            parsed = urlparse(url)
            if "food.grab.com" not in parsed.netloc or "/ph/" not in parsed.path:
                return None
            
            # Extract ID pattern like "2-C6TATTL2UF2UDA"
            match = re.search(r"/([0-9]-[A-Z0-9]+)$", parsed.path, re.IGNORECASE)
            return match.group(1) if match else None
        except Exception as e:
            logger.error(f"Error extracting merchant ID from {url}: {e}")
            return None
    
    def extract_store_name_from_url(self, url: str) -> str:
        """Extract store name from URL"""
        try:
            match = re.search(r'/restaurant/([^/]+)', url)
            if match:
                raw_name = match.group(1)
                raw_name = re.sub(r'-delivery$', '', raw_name)
                # Clean up the name
                name = raw_name.replace('-', ' ').title()
                return f"Cocopan {name}"
            return "Unknown Store"
        except Exception as e:
            logger.debug(f"Error extracting store name: {e}")
            return "Unknown Store"
    
    def fetch_menu_data(self, session: requests.Session, merchant_id: str, referer_url: str, max_retries: int = 3) -> Optional[Dict[str, Any]]:
        """Fetch menu data from GrabFood API with retry logic"""
        api_urls = [
            f"https://portal.grab.com/foodweb/v2/restaurant?merchantCode={merchant_id}&latlng={self.ph_latlng}",
            f"https://portal.grab.com/foodweb/v2/merchants/{merchant_id}?latlng={self.ph_latlng}"
        ]
        
        # Update referer
        updated_headers = self.headers.copy()
        updated_headers["Referer"] = referer_url
        session.headers.update(updated_headers)
        
        for endpoint_idx, api_url in enumerate(api_urls, 1):
            logger.debug(f"  📡 Trying API endpoint #{endpoint_idx}")
            
            for attempt in range(1, max_retries + 1):
                try:
                    # Pre-request delay (anti-detection)
                    time.sleep(random.uniform(1, 3))
                    
                    try:
                        resp = session.get(api_url, headers=updated_headers, timeout=15)
                    except requests.exceptions.SSLError:
                        resp = session.get(api_url, headers=updated_headers, timeout=15, verify=False)
                    
                    if resp.status_code >= 500:
                        logger.warning(f"  ⚠️ Server error {resp.status_code}, attempt {attempt}/{max_retries}")
                        if attempt < max_retries:
                            time.sleep(2.0 * attempt)
                            continue
                    
                    if resp.status_code == 403:
                        logger.warning(f"  🚫 Blocked (403), attempt {attempt}/{max_retries}")
                        if attempt < max_retries:
                            time.sleep(random.uniform(2, 4))
                            # Rotate user agent
                            updated_headers["User-Agent"] = random.choice(self.user_agents)
                            continue
                    
                    if resp.status_code == 429:
                        logger.warning(f"  ⏱️ Rate limited (429), attempt {attempt}/{max_retries}")
                        if attempt < max_retries:
                            time.sleep(30)
                            continue
                    
                    if resp.status_code == 200:
                        data = resp.json()
                        if data:
                            logger.debug(f"  ✅ API endpoint #{endpoint_idx} succeeded")
                            return data
                    
                    resp.raise_for_status()
                    
                except requests.RequestException as e:
                    logger.warning(f"  ⚠️ Request error: {e}")
                    if attempt < max_retries:
                        time.sleep(2.0 * attempt)
                        continue
                except Exception as e:
                    logger.error(f"  ❌ Unexpected error: {e}")
                    if attempt < max_retries:
                        continue
            
            logger.warning(f"  ❌ API endpoint #{endpoint_idx} failed after {max_retries} attempts")
        
        logger.error(f"  ❌ All API endpoints failed for merchant {merchant_id}")
        return None
    
    def extract_all_products_from_menu(self, menu_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Extract ALL products from menu JSON with their availability status"""
        products = []
        
        try:
            for section in self._iter_menu_sections(menu_data):
                section_name = section.get("name") or section.get("title") or "Uncategorized"
                
                for item in self._iter_section_items(section):
                    name = item.get("name") or item.get("title")
                    if not name:
                        continue
                    
                    # Get product details
                    product_id = item.get("id") or item.get("itemID")
                    price = item.get("price") or item.get("priceV2", {}).get("amount")
                    description = item.get("description", "")
                    
                    # Check availability
                    is_available = item.get("available", True)
                    
                    products.append({
                        "name": name.strip(),
                        "product_id": product_id,
                        "category": section_name,
                        "price": price,
                        "description": description,
                        "available": is_available,
                        "status": "IN_STOCK" if is_available else "OUT_OF_STOCK"
                    })
        
        except Exception as e:
            logger.error(f"Error parsing menu data: {e}")
        
        return products
    
    def _iter_menu_sections(self, data: Dict[str, Any]):
        """Iterate through possible menu section locations"""
        if not data:
            return
        
        roots = [data]
        if "data" in data and isinstance(data["data"], dict):
            roots.append(data["data"])
        
        for root in roots:
            if not isinstance(root, dict):
                continue
            
            # Check menu.categories or menu.sections
            menu = root.get("menu")
            if isinstance(menu, dict):
                categories = menu.get("categories") or menu.get("sections")
                if isinstance(categories, list):
                    for sec in categories:
                        yield sec
            
            # Check merchant.menu
            merchant = root.get("merchant")
            if isinstance(merchant, dict):
                m_menu = merchant.get("menu")
                if isinstance(m_menu, dict):
                    categories = m_menu.get("categories") or m_menu.get("sections")
                    if isinstance(categories, list):
                        for sec in categories:
                            yield sec
                
                # Check merchant.sections
                sections = merchant.get("sections")
                if isinstance(sections, list):
                    for sec in sections:
                        yield sec
    
    def _iter_section_items(self, section: Dict[str, Any]):
        """Iterate through items in a menu section"""
        if not isinstance(section, dict):
            return
        
        # Common item list keys
        for key in ("items", "itemList", "menuItems", "products", "dishes", "dishList"):
            items = section.get(key)
            if isinstance(items, list):
                for item in items:
                    if isinstance(item, dict):
                        yield item
    
    def _extract_store_name(self, menu_data: Dict[str, Any]) -> Optional[str]:
        """Extract store name from menu data"""
        try:
            for root_key in ("merchant", "data", None):
                root = menu_data
                if root_key and isinstance(menu_data, dict):
                    root = menu_data.get(root_key)
                
                if isinstance(root, dict):
                    for name_key in ("name", "displayName", "merchantName", "restaurantName"):
                        name = root.get(name_key)
                        if isinstance(name, str) and name.strip():
                            return name.strip()
        except Exception as e:
            logger.error(f"Error extracting store name: {e}")
        
        return None
    
    def scrape_store_products(self, store_url: str) -> Dict[str, Any]:
        """
        Scrape all products for a single store
        Returns: Dict with store info and products
        """
        try:
            merchant_id = self.extract_merchant_id(store_url)
            if not merchant_id:
                logger.warning(f"Could not extract merchant ID from {store_url}")
                return {
                    'store_url': store_url,
                    'store_name': self.extract_store_name_from_url(store_url),
                    'merchant_id': None,
                    'success': False,
                    'error': 'Invalid merchant ID',
                    'products': []
                }
            
            session = requests.Session()
            
            # Fetch menu data
            menu_data = self.fetch_menu_data(session, merchant_id, store_url)
            if not menu_data:
                logger.warning(f"Could not fetch menu data for merchant {merchant_id}")
                return {
                    'store_url': store_url,
                    'store_name': self.extract_store_name_from_url(store_url),
                    'merchant_id': merchant_id,
                    'success': False,
                    'error': 'Failed to fetch menu data',
                    'products': []
                }
            
            # Extract store name
            store_name = self._extract_store_name(menu_data) or self.extract_store_name_from_url(store_url)
            
            # Extract all products
            products = self.extract_all_products_from_menu(menu_data)
            
            # Count statistics
            total_products = len(products)
            in_stock = sum(1 for p in products if p['available'])
            out_of_stock = sum(1 for p in products if not p['available'])
            
            logger.info(f"✅ {store_name}: {total_products} products ({in_stock} in stock, {out_of_stock} OOS)")
            
            return {
                'store_url': store_url,
                'store_name': store_name,
                'merchant_id': merchant_id,
                'success': True,
                'scraped_at': datetime.now().isoformat(),
                'total_products': total_products,
                'in_stock_count': in_stock,
                'out_of_stock_count': out_of_stock,
                'products': products
            }
            
        except Exception as e:
            logger.error(f"❌ Error scraping {store_url}: {e}")
            return {
                'store_url': store_url,
                'store_name': self.extract_store_name_from_url(store_url),
                'merchant_id': None,
                'success': False,
                'error': str(e),
                'products': []
            }
    
    def scrape_all_stores(self, output_file: str = "grabfood_products.json", delay_between_stores: Tuple[int, int] = (5, 7)) -> Dict[str, Any]:
        """
        Scrape ALL stores from branch_urls.json
        Saves results to JSON file
        """
        logger.info("🛒 Starting GrabFood product scraping for ALL stores...")
        logger.info(f"📁 Results will be saved to: {output_file}")
        
        if not self.store_urls:
            logger.error("❌ No GrabFood URLs loaded - cannot scrape")
            return {
                'success': False,
                'error': 'No URLs loaded',
                'total_stores': 0,
                'stores': []
            }
        
        results = {
            'scraped_at': datetime.now().isoformat(),
            'total_stores': len(self.store_urls),
            'successful_scrapes': 0,
            'failed_scrapes': 0,
            'total_products': 0,
            'total_in_stock': 0,
            'total_out_of_stock': 0,
            'stores': []
        }
        
        for i, store_url in enumerate(self.store_urls, 1):
            logger.info(f"📍 [{i}/{len(self.store_urls)}] Scraping {store_url}")
            
            store_data = self.scrape_store_products(store_url)
            results['stores'].append(store_data)
            
            if store_data['success']:
                results['successful_scrapes'] += 1
                results['total_products'] += store_data['total_products']
                results['total_in_stock'] += store_data['in_stock_count']
                results['total_out_of_stock'] += store_data['out_of_stock_count']
            else:
                results['failed_scrapes'] += 1
            
            # Delay between stores (anti-detection)
            if i < len(self.store_urls):
                delay = random.uniform(delay_between_stores[0], delay_between_stores[1])
                logger.debug(f"⏱️ Waiting {delay:.1f}s before next store...")
                time.sleep(delay)
        
        # Save to JSON file
        try:
            output_path = Path(output_file)
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(results, f, indent=2, ensure_ascii=False)
            logger.info(f"💾 Results saved to: {output_path.absolute()}")
        except Exception as e:
            logger.error(f"❌ Failed to save results: {e}")
        
        # Print summary
        logger.info("=" * 70)
        logger.info(f"✅ GrabFood product scraping completed!")
        logger.info(f"📊 Summary:")
        logger.info(f"   Total stores: {results['total_stores']}")
        logger.info(f"   ✅ Successful: {results['successful_scrapes']}")
        logger.info(f"   ❌ Failed: {results['failed_scrapes']}")
        logger.info(f"   📦 Total products: {results['total_products']}")
        logger.info(f"   🟢 In stock: {results['total_in_stock']}")
        logger.info(f"   🔴 Out of stock: {results['total_out_of_stock']}")
        logger.info("=" * 70)
        
        return results


def main():
    """Main entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Scrape GrabFood products and SKUs')
    parser.add_argument('--urls-file', default='branch_urls.json', help='Path to branch_urls.json file')
    parser.add_argument('--output', default='grabfood_products.json', help='Output JSON file')
    parser.add_argument('--delay-min', type=int, default=5, help='Minimum delay between stores (seconds)')
    parser.add_argument('--delay-max', type=int, default=7, help='Maximum delay between stores (seconds)')
    
    args = parser.parse_args()
    
    logger.info("=" * 80)
    logger.info("🛒 GrabFood Product & SKU Scraper")
    logger.info("📋 Extracts all products and availability status from stores")
    logger.info("=" * 80)
    
    scraper = GrabFoodProductScraper(branch_urls_file=args.urls_file)
    
    if not scraper.store_urls:
        logger.error("❌ No GrabFood URLs found in branch_urls.json!")
        return
    
    results = scraper.scrape_all_stores(
        output_file=args.output,
        delay_between_stores=(args.delay_min, args.delay_max)
    )
    
    logger.info(f"✅ Done! Check {args.output} for full results")


if __name__ == "__main__":
    main()