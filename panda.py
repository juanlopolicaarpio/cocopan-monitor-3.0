#!/usr/bin/env python3
"""
Test Script: SKU Mapping with Foodpanda Selenium Scraping
✅ Tests scraping + mapping on 3 stores
✅ Shows detailed results
❌ Does NOT save to database
"""
import sys
import json
import logging
from typing import List, Dict
from datetime import datetime
from pathlib import Path

# Import from existing files
from testy import SeleniumScraper
from monitor_service import SKUMapper
from database import db

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ============================================================================
# Configuration
# ============================================================================
BRANCH_URLS_FILE = "branch_urls.json"  # File with Foodpanda URLs
NUM_TEST_STORES = 3  # Number of stores to test

# ============================================================================
# Test Runner
# ============================================================================
class FoodpandaSKUMappingTester:
    """Test Foodpanda SKU mapping without database writes"""
    
    def __init__(self):
        self.scraper = SeleniumScraper()
        self.scraper.start_driver()  # Start browser
        self.mapper = SKUMapper()
        
        logger.info("="*80)
        logger.info("🐼 FOODPANDA SKU MAPPING TEST INITIALIZED")
        logger.info("="*80)
        logger.info(f"📦 Loaded {len(self.mapper.grabfood_skus)} master SKU products")
        logger.info(f"🎯 Fuzzy matching: {'ENABLED ✅' if self.mapper.name_to_sku_map else 'DISABLED ❌'}")
        logger.info("")
        logger.info("ℹ️  FOODPANDA OOS LOGIC:")
        logger.info("   • Foodpanda hides out-of-stock items (they don't appear on page)")
        logger.info("   • We scrape all visible items")
        logger.info("   • Compare scraped items to database")
        logger.info("   • Items in DB but NOT scraped = OUT OF STOCK")
        logger.info("")
    
    def get_store_items_from_db(self, store_url: str) -> List[Dict]:
        """Get all items for this store from database"""
        try:
            logger.info(f"   🔍 Fetching items from database for this store...")
            
            # Query database for items at this store
            # This assumes you have a table with store items
            query = """
                SELECT sku_code, product_name 
                FROM foodpanda_products 
                WHERE store_url = %s OR branch_name LIKE %s
                ORDER BY product_name
            """
            
            # Extract store identifier from URL
            # e.g. "https://www.foodpanda.ph/restaurant/v9bb/cocopan-bgc" -> "cocopan-bgc"
            store_id = store_url.rstrip('/').split('/')[-1]
            
            with db.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(query, (store_url, f"%{store_id}%"))
                    rows = cur.fetchall()
                    
                    items = [
                        {'sku_code': row[0], 'product_name': row[1]}
                        for row in rows
                    ]
            
            logger.info(f"   ✅ Found {len(items)} items in database for this store")
            return items
            
        except Exception as e:
            logger.warning(f"   ⚠️  Could not fetch from database: {e}")
            logger.warning(f"   💡 Falling back to using all master SKUs")
            # Fallback: use all master SKUs
            return [
                {'sku_code': sku['sku_code'], 'product_name': sku['product_name']}
                for sku in self.mapper.grabfood_skus
            ]
    
    def load_test_urls(self) -> List[str]:
        """Load test URLs from branch_urls.json (Foodpanda only)"""
        logger.info(f"📂 Loading URLs from {BRANCH_URLS_FILE}...")
        
        try:
            with open(BRANCH_URLS_FILE, 'r') as f:
                data = json.load(f)
            
            # Filter for Foodpanda URLs only (same pattern as foodpanda_selenium.py)
            urls = [url for url in data.get("urls", []) if "foodpanda" in url.lower()]
            
            # Take only first NUM_TEST_STORES
            urls = urls[:NUM_TEST_STORES]
            
            if not urls:
                raise ValueError("No Foodpanda URLs found in branch_urls.json")
            
            logger.info(f"✅ Loaded {len(urls)} Foodpanda test URLs")
            logger.info("")
            return urls
            
        except FileNotFoundError:
            logger.error(f"❌ File not found: {BRANCH_URLS_FILE}")
            logger.error("💡 Create branch_urls.json with Foodpanda URLs")
            logger.error("")
            logger.error("Example format:")
            logger.error('  {"urls": [')
            logger.error('    "https://www.foodpanda.ph/restaurant/v9bb/store1",')
            logger.error('    "https://www.foodpanda.ph/restaurant/abcd/store2"')
            logger.error('  ]}')
            logger.error("")
            raise
        except json.JSONDecodeError as e:
            logger.error(f"❌ Invalid JSON in {BRANCH_URLS_FILE}: {e}")
            raise
        except Exception as e:
            logger.error(f"❌ Error loading URLs: {e}")
            raise
    
    def test_single_store(self, url: str, store_num: int, total: int) -> Dict:
        """Test scraping + mapping for a single store with Foodpanda OOS logic"""
        logger.info("="*80)
        logger.info(f"🐼 FOODPANDA STORE {store_num}/{total}")
        logger.info("="*80)
        logger.info(f"URL: {url}")
        logger.info("")
        
        result = {
            'url': url,
            'store_name': '',
            'scraping_success': False,
            'scraped_items': 0,
            'db_items': 0,
            'missing_items': 0,  # Items in DB but not scraped (OOS)
            'missing_names': [],
            'mapped_skus': [],
            'unknown_products': [],
            'mapping_success_rate': 0.0
        }
        
        try:
            # STEP 1: Scrape all visible items from Foodpanda
            logger.info("📡 STEP 1: Scraping visible items from Foodpanda...")
            scrape_result = self.scraper.scrape_url(url)
            
            if not scrape_result or not scrape_result.get('success'):
                logger.error(f"❌ Scraping failed for {url}")
                logger.error(f"   Error: {scrape_result.get('error', 'Unknown error') if scrape_result else 'No result'}")
                logger.info("")
                return result
            
            result['store_name'] = scrape_result.get('page_title', 'Unknown Store')
            result['scraping_success'] = True
            
            # Get all scraped items (everything visible on page)
            scraped_items = scrape_result.get('items', [])
            scraped_names = {item['name'].lower().strip() for item in scraped_items}
            
            result['scraped_items'] = len(scraped_items)
            
            logger.info(f"✅ Scraping complete: {result['store_name']}")
            logger.info(f"   📦 Scraped {result['scraped_items']} visible items from page")
            logger.info("")
            
            # STEP 2: Get all items from database for this store
            logger.info("💾 STEP 2: Fetching expected items from database...")
            db_items = self.get_store_items_from_db(url)
            result['db_items'] = len(db_items)
            
            if not db_items:
                logger.warning("   ⚠️  No items found in database for this store")
                logger.warning("   💡 Cannot determine OOS items without database reference")
                logger.info("")
                return result
            
            logger.info(f"   📋 Database has {len(db_items)} items for this store")
            logger.info("")
            
            # STEP 3: Compare - find items in DB but NOT scraped (these are OOS)
            logger.info("🔍 STEP 3: Comparing database vs scraped items...")
            logger.info("   Logic: Items in DB but NOT on page = OUT OF STOCK")
            logger.info("")
            
            missing_items = []
            for db_item in db_items:
                db_name = db_item['product_name'].lower().strip()
                
                # Fuzzy match - check if ANY scraped item is similar
                found = False
                for scraped_name in scraped_names:
                    # Simple fuzzy match - you can improve this
                    if db_name == scraped_name or db_name in scraped_name or scraped_name in db_name:
                        found = True
                        break
                
                if not found:
                    missing_items.append(db_item)
            
            result['missing_items'] = len(missing_items)
            result['missing_names'] = [item['product_name'] for item in missing_items]
            
            logger.info(f"   📊 Comparison results:")
            logger.info(f"      Items in database: {len(db_items)}")
            logger.info(f"      Items scraped (visible): {len(scraped_items)}")
            logger.info(f"      🔴 Missing items (OOS): {len(missing_items)}")
            logger.info("")
            
            if not missing_items:
                logger.info("✅ All database items are visible - no out-of-stock items!")
                logger.info("")
                return result
            
            # Show missing items
            logger.info("   🔴 Items in DB but NOT on page (Out of Stock):")
            for item in missing_items[:10]:
                logger.info(f"      • {item['product_name']} (SKU: {item['sku_code']})")
            if len(missing_items) > 10:
                logger.info(f"      ... and {len(missing_items) - 10} more")
            logger.info("")
            
            # STEP 4: Map missing items to SKU codes (they already have SKU from DB)
            logger.info("🗺️  STEP 4: Mapping out-of-stock items to SKU codes...")
            logger.info("")
            
            mapped_skus = []
            unknown_products = []
            
            for i, item in enumerate(missing_items, 1):
                name = item['product_name']
                db_sku = item['sku_code']
                
                logger.info(f"   [{i}/{len(missing_items)}] {name}")
                
                # Verify SKU mapping
                found_sku = self.mapper.find_sku_for_name(name)
                
                if found_sku:
                    mapped_skus.append(found_sku)
                    if found_sku == db_sku:
                        logger.info(f"       ✅ Mapped to: {found_sku} (matches DB)")
                    else:
                        logger.info(f"       ⚠️  Mapped to: {found_sku} (DB has: {db_sku})")
                elif db_sku:
                    mapped_skus.append(db_sku)
                    logger.info(f"       ✅ Using DB SKU: {db_sku}")
                else:
                    unknown_products.append(name)
                    logger.info(f"       ❌ NO SKU FOUND")
                
                logger.info("")
            
            result['mapped_skus'] = mapped_skus
            result['unknown_products'] = unknown_products
            
            # Calculate success rate
            if missing_items:
                result['mapping_success_rate'] = (len(mapped_skus) / len(missing_items)) * 100
            
            # STEP 5: Summary
            logger.info("📊 OUT-OF-STOCK SUMMARY:")
            logger.info(f"   Total missing (OOS): {len(missing_items)}")
            logger.info(f"   ✅ Successfully mapped: {len(mapped_skus)} ({result['mapping_success_rate']:.1f}%)")
            logger.info(f"   ❌ Unknown products: {len(unknown_products)}")
            
            if mapped_skus:
                logger.info("")
                logger.info("   🎯 Out-of-Stock SKU codes:")
                for sku in mapped_skus[:20]:
                    logger.info(f"      • {sku}")
                if len(mapped_skus) > 20:
                    logger.info(f"      ... and {len(mapped_skus) - 20} more")
            
            if unknown_products:
                logger.info("")
                logger.info("   ⚠️  Unknown products (need attention):")
                for product in unknown_products[:10]:
                    logger.info(f"      • {product}")
                if len(unknown_products) > 10:
                    logger.info(f"      ... and {len(unknown_products) - 10} more")
            
            logger.info("")
            
            return result
            
        except Exception as e:
            logger.error(f"❌ Error testing store: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return result
    
    def test_all_stores(self, urls: List[str]):
        """Test all stores and show final summary"""
        logger.info("")
        logger.info("🚀 STARTING TEST ON ALL STORES")
        logger.info("")
        
        results = []
        
        for i, url in enumerate(urls, 1):
            result = self.test_single_store(url, i, len(urls))
            results.append(result)
            
            # Pause between stores
            if i < len(urls):
                logger.info("⏸️  Pausing 5 seconds before next store...")
                logger.info("")
                import time
                time.sleep(5)
        
        # Final Summary
        self._print_final_summary(results)
        
        return results
    
    def _print_final_summary(self, results: List[Dict]):
        """Print comprehensive final summary"""
        logger.info("="*80)
        logger.info("🎯 FINAL TEST SUMMARY")
        logger.info("="*80)
        logger.info("")
        
        total_scraped_success = sum(1 for r in results if r['scraping_success'])
        total_scraped_items = sum(r['scraped_items'] for r in results)
        total_db_items = sum(r['db_items'] for r in results)
        total_missing = sum(r['missing_items'] for r in results)
        total_mapped = sum(len(r['mapped_skus']) for r in results)
        total_unknown = sum(len(r['unknown_products']) for r in results)
        
        logger.info(f"📊 Overall Statistics:")
        logger.info(f"   Stores tested: {len(results)}")
        logger.info(f"   Scraping success: {total_scraped_success}/{len(results)}")
        logger.info(f"   Total items scraped (visible): {total_scraped_items}")
        logger.info(f"   Total items in database: {total_db_items}")
        logger.info(f"   🔴 Total missing (OOS): {total_missing}")
        logger.info("")
        
        logger.info(f"🗺️  Mapping Performance:")
        logger.info(f"   OOS products to map: {total_missing}")
        if total_missing > 0:
            logger.info(f"   ✅ Successfully mapped: {total_mapped} ({total_mapped/total_missing*100:.1f}%)")
            logger.info(f"   ❌ Unknown products: {total_unknown} ({total_unknown/total_missing*100:.1f}%)")
        else:
            logger.info(f"   ✅ Successfully mapped: {total_mapped}")
            logger.info(f"   ❌ Unknown products: {total_unknown}")
        logger.info("")
        
        # Store-by-store breakdown
        logger.info("🐼 Store-by-Store Results:")
        logger.info("")
        
        for i, result in enumerate(results, 1):
            if not result['scraping_success']:
                logger.info(f"   {i}. {result['url']}")
                logger.info(f"      ❌ Scraping failed")
                logger.info("")
                continue
            
            logger.info(f"   {i}. {result['store_name']}")
            logger.info(f"      Scraped: {result['scraped_items']} visible")
            logger.info(f"      Database: {result['db_items']} total")
            logger.info(f"      🔴 Missing (OOS): {result['missing_items']}")
            if result['missing_items'] > 0:
                logger.info(f"      Mapping: {len(result['mapped_skus'])}/{result['missing_items']} ({result['mapping_success_rate']:.1f}%)")
            else:
                logger.info(f"      Mapping: N/A (all items visible)")
            
            if result['mapped_skus']:
                logger.info(f"      ✅ SKUs: {', '.join(result['mapped_skus'][:5])}")
                if len(result['mapped_skus']) > 5:
                    logger.info(f"               ... and {len(result['mapped_skus'])-5} more")
            
            if result['unknown_products']:
                logger.info(f"      ❌ Unknown: {', '.join(result['unknown_products'][:3])}")
                if len(result['unknown_products']) > 3:
                    logger.info(f"                ... and {len(result['unknown_products'])-3} more")
            
            logger.info("")
        
        # All unknown products across all stores
        all_unknown = []
        for result in results:
            all_unknown.extend(result['unknown_products'])
        
        if all_unknown:
            logger.info("⚠️  ALL UNKNOWN PRODUCTS (need to be added to master SKUs):")
            logger.info("")
            unique_unknown = sorted(set(all_unknown))
            for product in unique_unknown[:20]:
                logger.info(f"   • {product}")
            if len(unique_unknown) > 20:
                logger.info(f"   ... and {len(unique_unknown)-20} more")
            logger.info("")
        else:
            logger.info("✅ All out-of-stock products were successfully mapped!")
            logger.info("")
        
        # Overall verdict
        logger.info("="*80)
        if total_missing == 0:
            logger.info("✅ TEST PASSED: No out-of-stock items found (all DB items visible)")
        elif total_unknown == 0:
            logger.info("✅ TEST PASSED: All OOS products mapped successfully!")
        elif total_missing > 0 and total_unknown < total_missing * 0.1:  # Less than 10% unknown
            logger.info("⚠️  TEST PASSED WITH WARNINGS: Most OOS products mapped (>90%)")
        else:
            logger.info("❌ TEST NEEDS ATTENTION: Many unmapped OOS products")
        logger.info("="*80)
        logger.info("")
        
        # Database save confirmation
        logger.info("💾 DATABASE STATUS: NOT SAVED (test mode)")
        logger.info("   To save results, run the full scraper in production mode")
        logger.info("")
    
    def close(self):
        """Cleanup"""
        self.scraper.stop_driver()

# ============================================================================
# Main
# ============================================================================
def main():
    """Run the test"""
    logger.info("="*80)
    logger.info("🐼 FOODPANDA SKU MAPPING TEST SCRIPT")
    logger.info("="*80)
    logger.info("📋 Test Configuration:")
    logger.info(f"   • URL source: {BRANCH_URLS_FILE}")
    logger.info(f"   • Stores to test: {NUM_TEST_STORES}")
    logger.info(f"   • Database writes: DISABLED ❌")
    logger.info(f"   • Fuzzy matching: ENABLED ✅")
    logger.info("="*80)
    logger.info("")
    
    tester = FoodpandaSKUMappingTester()
    
    try:
        # Load test URLs
        urls = tester.load_test_urls()
        
        # Run tests
        results = tester.test_all_stores(urls)
        
        logger.info("🎉 Test completed successfully!")
        logger.info("")
        
    except KeyboardInterrupt:
        logger.info("")
        logger.info("🛑 Test interrupted by user")
    except Exception as e:
        logger.error(f"❌ Test failed: {e}")
        import traceback
        logger.error(traceback.format_exc())
    finally:
        tester.close()
        logger.info("👋 Test script finished")

if __name__ == "__main__":
    main()