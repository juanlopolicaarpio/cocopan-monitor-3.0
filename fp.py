#!/usr/bin/env python3
"""
Foodpanda Scraper - Same as GrabFoodMonitor.check_grabfood_store()
Simple HTTP GET + BeautifulSoup - no API
"""
import json
import time
import random
import re
import logging
from typing import List, Tuple
import requests
from bs4 import BeautifulSoup
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class FoodpandaMonitorScraper:
    """Same approach as GrabFoodMonitor - simple HTTP + BeautifulSoup"""
    
    def __init__(self):
        # Same user agents as GrabFoodMonitor
        self.user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        ]
        
        self.store_urls = self._load_foodpanda_urls()
        
        logger.info(f"Foodpanda Monitor Scraper initialized")
        logger.info(f"Loaded {len(self.store_urls)} Foodpanda stores")
    
    def _load_foodpanda_urls(self) -> List[str]:
        """Load Foodpanda URLs from branch_urls.json"""
        try:
            with open('branch_urls.json') as f:
                data = json.load(f)
                all_urls = data.get('urls', [])
                foodpanda_urls = [url for url in all_urls if 'foodpanda' in url]
                logger.info(f"Filtered to {len(foodpanda_urls)} Foodpanda URLs")
                return foodpanda_urls
        except Exception as e:
            logger.error(f"Failed to load URLs: {e}")
            return []
    
    def check_store(self, url: str, retry_count: int = 0) -> Tuple[List[str], str]:
        """
        Check store (EXACT same pattern as GrabFoodMonitor.check_grabfood_store)
        Returns: (oos_items, store_name)
        """
        max_retries = 2
        
        try:
            session = requests.Session()
            session.headers.update({
                'User-Agent': random.choice(self.user_agents),
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.9',
                'Accept-Encoding': 'gzip, deflate',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1',
                'Sec-Fetch-Dest': 'document',
                'Sec-Fetch-Mode': 'navigate',
                'Sec-Fetch-Site': 'none',
                'Cache-Control': 'max-age=0',
            })
            
            # Pre-request delay (same as GrabFoodMonitor)
            time.sleep(random.uniform(1, 3))
            
            try:
                resp = session.get(url, timeout=30, allow_redirects=True)
            except requests.exceptions.SSLError:
                resp = session.get(url, timeout=30, allow_redirects=True, verify=False)
            
            # Handle 403 (same as GrabFoodMonitor)
            if resp.status_code == 403:
                if retry_count < max_retries:
                    time.sleep(random.uniform(2, 4))
                    return self.check_store(url, retry_count + 1)
                return [], "Blocked Store"
            
            if resp.status_code != 200:
                return [], "Error Store"
            
            # Parse HTML (same as GrabFoodMonitor)
            soup = BeautifulSoup(resp.text, 'html.parser')
            
            # Remove script and style (same as GrabFoodMonitor)
            for script in soup(['script', 'style']):
                script.decompose()
            
            # Extract store name
            store_name = "Unknown Store"
            if soup.title and soup.title.string:
                title_text = soup.title.string.strip()
                match = re.search(r'(Cocopan[^|]+)', title_text)
                if match:
                    store_name = match.group(1).strip()
            
            # Get clean text (same as GrabFoodMonitor)
            page_text = ' '.join(soup.get_text().lower().split())
            
            # Find OOS items by looking for specific patterns in the HTML structure
            oos_items = []
            
            # Look for divs/sections that contain "sold out" or "unavailable" text
            # and extract nearby product names
            text_parts = soup.find_all(string=re.compile(r'(sold out|unavailable|out of stock)', re.I))
            
            for text_elem in text_parts:
                # Walk up to find parent container (likely product card)
                parent = text_elem.parent
                depth = 0
                while parent and depth < 8:
                    # Look for product name within this container
                    # Typical structure: div contains both product name and OOS status
                    text_content = parent.get_text()
                    
                    # Extract potential product name (before "sold out" text)
                    lines = [line.strip() for line in text_content.split('\n') if line.strip()]
                    for i, line in enumerate(lines):
                        if re.search(r'(sold out|unavailable|out of stock)', line, re.I):
                            # Product name is likely 1-3 lines before the OOS indicator
                            for j in range(max(0, i-3), i):
                                potential_name = lines[j]
                                if self._looks_like_product(potential_name):
                                    oos_items.append(potential_name)
                                    break
                            break
                    
                    if oos_items:  # Found something, stop walking up
                        break
                    
                    parent = parent.parent
                    depth += 1
            
            # Remove duplicates
            oos_items = list(dict.fromkeys(oos_items))
            
            return oos_items, store_name
            
        except Exception as e:
            logger.error(f"Error: {e}")
            return [], "Error Store"
    
    def _looks_like_product(self, text: str) -> bool:
        """Check if text looks like a product name"""
        if not text or len(text) < 5 or len(text) > 80:
            return False
        
        text_lower = text.lower()
        
        # Exclude store names and sections
        bad_keywords = [
            'cocopan', 'delivery', 'app-only', 'deals', 'popular',
            'sold out', 'unavailable', 'add to', 'order', 'menu',
            'category', 'view', 'select', 'choose'
        ]
        
        if any(bad in text_lower for bad in bad_keywords):
            return False
        
        # Must have letters
        if not re.search(r'[a-zA-Z]', text):
            return False
        
        # Looks reasonable
        return True
    
    def test_all(self):
        """Test all stores"""
        logger.info("=" * 70)
        logger.info("FOODPANDA MONITOR SCRAPER")
        logger.info("(Same as GrabFoodMonitor - simple HTTP + BeautifulSoup)")
        logger.info("=" * 70)
        
        if not self.store_urls:
            logger.error("No URLs loaded!")
            return []
        
        logger.info(f"\nTesting {len(self.store_urls)} stores...\n")
        
        results = []
        
        for i, url in enumerate(self.store_urls, 1):
            logger.info(f"[{i}/{len(self.store_urls)}] Checking {url}")
            
            oos_items, store_name = self.check_store(url)
            
            emoji = "✅" if store_name not in ["Error Store", "Blocked Store"] else "❌"
            logger.info(f"   {emoji} {store_name}: {len(oos_items)} OOS items")
            
            if oos_items:
                for item in oos_items[:5]:
                    logger.info(f"      - {item}")
                if len(oos_items) > 5:
                    logger.info(f"      ... and {len(oos_items) - 5} more")
            
            results.append({
                'url': url,
                'store_name': store_name,
                'oos_count': len(oos_items),
                'oos_items': oos_items,
                'success': store_name not in ["Error Store", "Blocked Store"]
            })
            
            # Delay between stores (same as GrabFoodMonitor)
            if i < len(self.store_urls):
                time.sleep(random.uniform(5, 7))
        
        # Summary
        logger.info("\n" + "=" * 70)
        logger.info("SUMMARY")
        logger.info("=" * 70)
        
        successful = sum(1 for r in results if r['success'])
        total_oos = sum(r['oos_count'] for r in results)
        
        logger.info(f"Total: {len(results)}")
        logger.info(f"Successful: {successful}")
        logger.info(f"Total OOS: {total_oos}")
        
        # Save results
        output_file = f"foodpanda_results_{int(time.time())}.json"
        with open(output_file, 'w') as f:
            json.dump(results, f, indent=2)
        logger.info(f"\nSaved to: {output_file}")
        
        return results

def main():
    print("\nFoodpanda Monitor Scraper")
    print("(Same as GrabFoodMonitor - no API)\n")
    
    scraper = FoodpandaMonitorScraper()
    scraper.test_all()

if __name__ == "__main__":
    main()