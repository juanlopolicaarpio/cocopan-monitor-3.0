#!/usr/bin/env python3
"""
Test Script for GrabFood SKU Scraping
Tests the scraping logic before implementing in main monitor service
"""
import os
import json
import time
import random
import logging
from datetime import datetime
from typing import List, Dict, Optional
from dataclasses import dataclass

import requests
from bs4 import BeautifulSoup
import urllib3

# Use your existing config and database
from config import config
from database import db

# Disable SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@dataclass
class ScrapedProduct:
    name: str
    price: Optional[float]
    is_available: bool
    detection_method: str
    confidence: float
    raw_html_snippet: str

@dataclass
class SKUMatchResult:
    sku_code: str
    expected_name: str
    found_match: bool
    matched_product: Optional[ScrapedProduct]
    match_confidence: float
    status: str  # "available", "out_of_stock", "not_found", "error"

class TestGrabFoodSKUScraper:
    """Test version of GrabFood SKU scraper with detailed logging"""
    
    def __init__(self):
        self.user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        ]
        self.session = requests.Session()
        
    def setup_session(self):
        """Setup realistic session headers"""
        self.session.headers.update({
            'User-Agent': random.choice(self.user_agents),
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Cache-Control': 'no-cache'
        })
        
    def test_single_store(self, store_url: str, store_name: str) -> Dict:
        """Test scraping a single GrabFood store"""
        logger.info(f"🛒 Testing store: {store_name}")
        logger.info(f"🔗 URL: {store_url}")
        
        self.setup_session()
        
        # Step 1: Fetch the page
        try:
            logger.info("📥 Fetching store page...")
            response = self._fetch_page_with_details(store_url)
            if not response:
                return {"error": "Failed to fetch page", "store_url": store_url}
            
            logger.info(f"✅ Page fetched successfully (Status: {response.status_code}, Length: {len(response.text)} chars)")
            
        except Exception as e:
            logger.error(f"❌ Failed to fetch page: {e}")
            return {"error": str(e), "store_url": store_url}
        
        # Step 2: Parse and extract products
        try:
            logger.info("🔍 Parsing page content...")
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Save HTML for debugging (first 5000 chars)
            html_preview = response.text[:5000]
            logger.debug(f"HTML Preview:\n{html_preview}")
            
            scraped_products = self._extract_products_with_details(soup)
            logger.info(f"📦 Found {len(scraped_products)} potential products")
            
            # Show details of found products
            for i, product in enumerate(scraped_products[:10]):  # Show first 10
                logger.info(f"   Product {i+1}: {product.name} - {'✅' if product.is_available else '❌'} - {product.detection_method}")
            
        except Exception as e:
            logger.error(f"❌ Failed to parse products: {e}")
            return {"error": f"Parse error: {e}", "store_url": store_url, "html_preview": response.text[:1000]}
        
        # Step 3: Test SKU matching (get some test SKUs from database)
        try:
            logger.info("🔍 Testing SKU matching...")
            test_skus = self._get_test_skus()
            sku_results = []
            
            for sku in test_skus:
                match_result = self._match_sku_with_scraped_products(sku, scraped_products)
                sku_results.append(match_result)
                
                status_emoji = "✅" if match_result.status == "available" else "❌" if match_result.status == "out_of_stock" else "❓"
                logger.info(f"   SKU {sku['sku_code']} ({sku['product_name']}): {status_emoji} {match_result.status}")
                if match_result.matched_product:
                    logger.info(f"      Matched with: {match_result.matched_product.name} (confidence: {match_result.match_confidence:.2f})")
            
        except Exception as e:
            logger.error(f"❌ SKU matching failed: {e}")
            sku_results = []
        
        # Step 4: Return comprehensive test results
        return {
            "store_name": store_name,
            "store_url": store_url,
            "success": True,
            "page_length": len(response.text),
            "scraped_products_count": len(scraped_products),
            "scraped_products": [
                {
                    "name": p.name,
                    "price": p.price,
                    "is_available": p.is_available,
                    "detection_method": p.detection_method,
                    "confidence": p.confidence
                } for p in scraped_products
            ],
            "sku_matches": [
                {
                    "sku_code": r.sku_code,
                    "expected_name": r.expected_name,
                    "status": r.status,
                    "found_match": r.found_match,
                    "match_confidence": r.match_confidence,
                    "matched_product_name": r.matched_product.name if r.matched_product else None
                } for r in sku_results
            ],
            "html_preview": response.text[:2000]  # For debugging
        }
    
    def _fetch_page_with_details(self, url: str) -> Optional[requests.Response]:
        """Fetch page with detailed logging"""
        try:
            logger.info(f"🌐 Making request to: {url}")
            time.sleep(random.uniform(2, 4))  # Random delay
            
            try:
                response = self.session.get(url, timeout=15, allow_redirects=True)
            except requests.exceptions.SSLError:
                logger.warning("🔓 SSL Error, retrying without verification...")
                response = self.session.get(url, timeout=15, allow_redirects=True, verify=False)
            
            logger.info(f"📊 Response: {response.status_code}, Content-Length: {len(response.text)}")
            
            if response.status_code == 403:
                logger.warning("🚫 Got 403 Forbidden - might be blocked")
            elif response.status_code == 429:
                logger.warning("⏱️ Got 429 Rate Limited")
            elif response.status_code != 200:
                logger.warning(f"⚠️ Unexpected status code: {response.status_code}")
            
            return response if response.status_code == 200 else None
            
        except Exception as e:
            logger.error(f"❌ Request failed: {e}")
            return None
    
    def _extract_products_with_details(self, soup: BeautifulSoup) -> List[ScrapedProduct]:
        """Extract products with detailed detection logging"""
        products = []
        
        logger.info("🔍 Starting product extraction...")
        
        # Method 1: Look for common GrabFood product container patterns
        logger.info("   Method 1: Searching for product containers...")
        
        # Common GrabFood selectors (these may need updating based on current site structure)
        product_selectors = [
            '[data-testid*="menu-item"]',
            '[class*="menu-item"]',
            '[class*="product-card"]',
            '[class*="item-card"]',
            'div[class*="MenuItem"]',
            'div[class*="ProductCard"]'
        ]
        
        containers_found = 0
        for selector in product_selectors:
            containers = soup.select(selector)
            if containers:
                logger.info(f"      Found {len(containers)} containers with selector: {selector}")
                containers_found += len(containers)
                
                for container in containers[:20]:  # Limit to prevent too much processing
                    try:
                        product = self._parse_product_container_detailed(container, f"css_selector:{selector}")
                        if product:
                            products.append(product)
                    except Exception as e:
                        logger.debug(f"Error parsing container: {e}")
                        continue
        
        logger.info(f"   Method 1 result: {containers_found} containers found, {len(products)} products extracted")
        
        # Method 2: Look for text patterns that indicate Cocopan products
        logger.info("   Method 2: Searching for Cocopan-related text...")
        
        cocopan_texts = soup.find_all(text=lambda text: text and any(
            keyword in text.lower() for keyword in ['cocopan', 'spanish bread', 'pan de sal', 'choco roll', 'cheese roll']
        ))
        
        logger.info(f"      Found {len(cocopan_texts)} text elements mentioning Cocopan products")
        
        for text_elem in cocopan_texts[:10]:  # Limit for testing
            try:
                # Find the parent container
                parent = text_elem.parent
                for _ in range(3):  # Go up 3 levels to find product container
                    if parent and parent.name:
                        product = self._parse_product_container_detailed(parent, "text_search")
                        if product and not any(p.name == product.name for p in products):
                            products.append(product)
                            break
                        parent = parent.parent
                    else:
                        break
            except Exception as e:
                logger.debug(f"Error parsing text element: {e}")
        
        # Method 3: Look for structured data
        logger.info("   Method 3: Searching for structured data...")
        
        json_scripts = soup.find_all('script', type='application/ld+json')
        logger.info(f"      Found {len(json_scripts)} JSON-LD scripts")
        
        for script in json_scripts:
            try:
                if script.string:
                    data = json.loads(script.string)
                    structured_products = self._extract_from_structured_data(data)
                    products.extend(structured_products)
                    logger.info(f"      Extracted {len(structured_products)} products from structured data")
            except Exception as e:
                logger.debug(f"Error parsing structured data: {e}")
        
        # Remove duplicates
        unique_products = []
        seen_names = set()
        
        for product in products:
            clean_name = product.name.lower().strip()
            if clean_name not in seen_names:
                seen_names.add(clean_name)
                unique_products.append(product)
        
        logger.info(f"🎯 Final result: {len(unique_products)} unique products after deduplication")
        
        return unique_products
    
    def _parse_product_container_detailed(self, container, detection_method: str) -> Optional[ScrapedProduct]:
        """Parse individual product container with detailed logging"""
        try:
            # Extract product name
            name_candidates = []
            
            # Try different ways to find product name
            for tag in ['h1', 'h2', 'h3', 'h4', 'h5', 'h6']:
                names = container.find_all(tag)
                for name_elem in names:
                    text = name_elem.get_text(strip=True)
                    if text and len(text) > 3:
                        name_candidates.append(text)
            
            # Look for elements with name-like classes
            name_elems = container.find_all(class_=lambda x: x and any(
                term in str(x).lower() for term in ['name', 'title', 'product']
            ))
            
            for elem in name_elems:
                text = elem.get_text(strip=True)
                if text and len(text) > 3:
                    name_candidates.append(text)
            
            # Pick the best name candidate
            product_name = None
            for candidate in name_candidates:
                if any(keyword in candidate.lower() for keyword in ['cocopan', 'bread', 'roll', 'pan']):
                    product_name = candidate
                    break
            
            if not product_name and name_candidates:
                product_name = name_candidates[0]  # Fallback to first candidate
            
            if not product_name:
                return None
            
            # Extract price
            price = None
            price_patterns = [
                r'₱\s*([\d,]+\.?\d*)',
                r'PHP\s*([\d,]+\.?\d*)',
                r'(\d+\.?\d*)\s*peso'
            ]
            
            container_text = container.get_text()
            for pattern in price_patterns:
                import re
                match = re.search(pattern, container_text, re.IGNORECASE)
                if match:
                    try:
                        price = float(match.group(1).replace(',', ''))
                        break
                    except:
                        continue
            
            # Check availability
            is_available = True
            availability_confidence = 0.7  # Default confidence
            availability_method = "default_available"
            
            # Look for out of stock indicators
            container_text_lower = container.get_text().lower()
            
            out_of_stock_phrases = [
                'sold out', 'out of stock', 'unavailable', 'not available',
                'temporarily unavailable', 'currently unavailable'
            ]
            
            for phrase in out_of_stock_phrases:
                if phrase in container_text_lower:
                    is_available = False
                    availability_confidence = 0.9
                    availability_method = f"text_indicator:{phrase}"
                    break
            
            # Check for disabled buttons
            buttons = container.find_all('button')
            for button in buttons:
                button_text = button.get_text().lower()
                if 'add' in button_text or 'order' in button_text:
                    if (button.get('disabled') or 
                        'disabled' in button.get('class', []) or
                        button.get('aria-disabled') == 'true'):
                        is_available = False
                        availability_confidence = 0.95
                        availability_method = "disabled_button"
                        break
                    elif button_text in ['sold out', 'unavailable']:
                        is_available = False
                        availability_confidence = 0.95
                        availability_method = "button_text"
                        break
            
            return ScrapedProduct(
                name=product_name,
                price=price,
                is_available=is_available,
                detection_method=f"{detection_method}|{availability_method}",
                confidence=availability_confidence,
                raw_html_snippet=str(container)[:200] + "..."
            )
            
        except Exception as e:
            logger.debug(f"Error parsing container: {e}")
            return None
    
    def _extract_from_structured_data(self, data) -> List[ScrapedProduct]:
        """Extract from JSON-LD structured data"""
        products = []
        
        def search_recursive(obj):
            if isinstance(obj, dict):
                if obj.get('@type') == 'Product' or 'name' in obj:
                    name = obj.get('name', '')
                    if any(keyword in name.lower() for keyword in ['cocopan', 'bread', 'roll', 'pan']):
                        offer = obj.get('offers', {})
                        availability = offer.get('availability', '').lower()
                        
                        products.append(ScrapedProduct(
                            name=name,
                            price=offer.get('price'),
                            is_available='instock' in availability or 'available' in availability,
                            detection_method=f"structured_data|{availability}",
                            confidence=0.95,
                            raw_html_snippet="structured_data"
                        ))
                
                for value in obj.values():
                    search_recursive(value)
            elif isinstance(obj, list):
                for item in obj:
                    search_recursive(item)
        
        search_recursive(data)
        return products
    
    def _get_test_skus(self) -> List[Dict]:
        """Get test SKUs from database or use hardcoded ones"""
        try:
            # Try to get from database first
            skus = db.get_master_skus_by_platform('grabfood')
            if skus:
                return skus[:10]  # Return first 10 for testing
        except Exception as e:
            logger.warning(f"Couldn't get SKUs from database: {e}")
        
        # Fallback to hardcoded test SKUs
        return [
            {'sku_code': 'CP001', 'product_name': 'Spanish Bread'},
            {'sku_code': 'CP002', 'product_name': 'Choco Roll'},
            {'sku_code': 'CP003', 'product_name': 'Pan De Sal Pack'},
            {'sku_code': 'CP004', 'product_name': 'Cheese Roll'},
            {'sku_code': 'CP005', 'product_name': 'Cocopan Spanish Bread'},
            {'sku_code': 'CP006', 'product_name': 'Cocopan Choco Roll'},
        ]
    
    def _match_sku_with_scraped_products(self, sku: Dict, scraped_products: List[ScrapedProduct]) -> SKUMatchResult:
        """Match expected SKU with scraped products"""
        sku_code = sku['sku_code']
        expected_name = sku['product_name']
        
        best_match = None
        best_score = 0
        
        expected_clean = self._clean_product_name(expected_name)
        
        for product in scraped_products:
            scraped_clean = self._clean_product_name(product.name)
            similarity = self._calculate_similarity(expected_clean, scraped_clean)
            
            if similarity > best_score and similarity > 0.4:  # Lower threshold for testing
                best_match = product
                best_score = similarity
        
        if best_match:
            status = "available" if best_match.is_available else "out_of_stock"
            return SKUMatchResult(
                sku_code=sku_code,
                expected_name=expected_name,
                found_match=True,
                matched_product=best_match,
                match_confidence=best_score,
                status=status
            )
        else:
            return SKUMatchResult(
                sku_code=sku_code,
                expected_name=expected_name,
                found_match=False,
                matched_product=None,
                match_confidence=0.0,
                status="not_found"
            )
    
    def _clean_product_name(self, name: str) -> str:
        """Clean product name for comparison"""
        if not name:
            return ""
        
        import re
        clean = name.lower()
        clean = re.sub(r'\b(cocopan|grab|foodpanda)\b', '', clean)
        clean = re.sub(r'\b(pack|pcs|pieces?|pc)\b', '', clean)
        clean = re.sub(r'\([^)]*\)', '', clean)
        clean = re.sub(r'[^\w\s]', ' ', clean)
        clean = re.sub(r'\s+', ' ', clean).strip()
        
        return clean
    
    def _calculate_similarity(self, name1: str, name2: str) -> float:
        """Calculate similarity between product names"""
        if not name1 or not name2:
            return 0.0
        
        words1 = set(name1.split())
        words2 = set(name2.split())
        
        if not words1 or not words2:
            return 0.0
        
        intersection = len(words1.intersection(words2))
        union = len(words1.union(words2))
        
        return intersection / union if union > 0 else 0.0


def load_test_stores() -> List[Dict[str, str]]:
    """Load test stores from branch_urls.json"""
    try:
        with open('branch_urls.json', 'r') as f:
            data = json.load(f)
        
        urls = data.get('urls', [])
        test_stores = []
        
        # Get GrabFood stores for testing
        for url in urls[:5]:  # Test first 5 stores only
            if 'grab.com' in url and url.strip():
                # Extract store name from URL
                import re
                match = re.search(r'/restaurant/([^/]+)', url)
                if match:
                    raw_name = match.group(1)
                    store_name = raw_name.replace('-', ' ').title()
                    if not store_name.startswith('Cocopan'):
                        store_name = f"Cocopan {store_name}"
                else:
                    store_name = f"Test GrabFood Store {len(test_stores) + 1}"
                
                test_stores.append({
                    'name': store_name,
                    'url': url
                })
        
        return test_stores
        
    except Exception as e:
        logger.error(f"Failed to load test stores: {e}")
        # Fallback test stores
        return [
            {
                'name': 'Test Cocopan Store',
                'url': 'https://food.grab.com/ph/en/restaurant/cocopan-test-delivery'
            }
        ]


def main():
    """Main test function"""
    logger.info("=" * 60)
    logger.info("🧪 GRABFOOD SKU SCRAPING TEST")
    logger.info("=" * 60)
    
    # Initialize scraper
    scraper = TestGrabFoodSKUScraper()
    
    # Load test stores
    test_stores = load_test_stores()
    logger.info(f"📋 Testing {len(test_stores)} stores")
    
    # Test each store
    results = []
    for i, store in enumerate(test_stores, 1):
        logger.info(f"\n{'='*20} STORE {i}/{len(test_stores)} {'='*20}")
        
        try:
            result = scraper.test_single_store(store['url'], store['name'])
            results.append(result)
            
            if result.get('success'):
                logger.info(f"✅ Test completed for {store['name']}")
                logger.info(f"   📦 Found {result['scraped_products_count']} products")
                logger.info(f"   🎯 SKU matches: {len([r for r in result['sku_matches'] if r['found_match']])}")
                
                # Show availability summary
                available_count = len([p for p in result['scraped_products'] if p['is_available']])
                total_count = result['scraped_products_count']
                logger.info(f"   📊 Availability: {available_count}/{total_count} products available")
                
            else:
                logger.error(f"❌ Test failed for {store['name']}: {result.get('error', 'Unknown error')}")
        
        except Exception as e:
            logger.error(f"❌ Exception testing {store['name']}: {e}")
            results.append({'error': str(e), 'store_name': store['name']})
        
        # Delay between stores
        if i < len(test_stores):
            logger.info("⏳ Waiting before next store...")
            time.sleep(random.uniform(5, 10))
    
    # Summary report
    logger.info("\n" + "=" * 60)
    logger.info("📊 TEST SUMMARY REPORT")
    logger.info("=" * 60)
    
    successful_tests = len([r for r in results if r.get('success')])
    logger.info(f"✅ Successful tests: {successful_tests}/{len(results)}")
    
    if successful_tests > 0:
        total_products = sum(r.get('scraped_products_count', 0) for r in results if r.get('success'))
        total_matches = sum(len([m for m in r.get('sku_matches', []) if m['found_match']]) 
                           for r in results if r.get('success'))
        
        logger.info(f"📦 Total products scraped: {total_products}")
        logger.info(f"🎯 Total SKU matches found: {total_matches}")
        
        # Show detailed results for first successful test
        first_success = next((r for r in results if r.get('success')), None)
        if first_success:
            logger.info(f"\n📋 Detailed results for {first_success['store_name']}:")
            for match in first_success['sku_matches'][:5]:  # Show first 5
                status_emoji = "✅" if match['status'] == "available" else "❌" if match['status'] == "out_of_stock" else "❓"
                logger.info(f"   {status_emoji} {match['sku_code']}: {match['status']} (confidence: {match['match_confidence']:.2f})")
    
    # Save results to file for analysis
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"sku_test_results_{timestamp}.json"
    
    try:
        with open(filename, 'w') as f:
            json.dump(results, f, indent=2, default=str)
        logger.info(f"💾 Detailed results saved to: {filename}")
    except Exception as e:
        logger.error(f"❌ Failed to save results: {e}")
    
    # Final recommendations
    logger.info("\n🎯 RECOMMENDATIONS:")
    if successful_tests > 0:
        logger.info("✅ SKU scraping appears to work! You can proceed with integration.")
        logger.info("🔧 Consider adjusting similarity thresholds based on test results.")
        logger.info("⏱️ Add appropriate delays between requests to avoid rate limiting.")
    else:
        logger.info("❌ SKU scraping needs debugging. Check the detailed error logs.")
        logger.info("🔍 Verify store URLs are accessible and have expected structure.")
    
    logger.info(f"\n📁 Check {filename} for complete test data.")


if __name__ == "__main__":
    main()