

#!/usr/bin/env python3
"""
Test Script: SKU Mapping with Selenium Scraping
✅ Tests scraping + mapping on 3 stores
✅ Shows detailed results
❌ Does NOT save to database
"""
import sys
import logging
from typing import List, Dict
from datetime import datetime

# Import from existing files
from wow import GrabFoodScraper
from monitor_service import SKUMapper
from database import db

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ============================================================================
# Test Configuration
# ============================================================================
TEST_STORES = [
    "https://food.grab.com/ph/en/restaurant/cocopan-maysilo-delivery/2-C6TATTL2UF2UDA",
    "https://food.grab.com/ph/en/restaurant/cocopan-anonas-delivery/2-C6XVCUDGNXNZNN",
    "https://food.grab.com/ph/en/restaurant/cocopan-altura-santa-mesa-delivery/2-C7EUVP2UEJ43L6"
]

# ============================================================================
# Test Runner
# ============================================================================
class SKUMappingTester:
    """Test SKU mapping without database writes"""
    
    def __init__(self):
        self.scraper = GrabFoodScraper()
        self.mapper = SKUMapper()
        
        logger.info("="*80)
        logger.info("🧪 SKU MAPPING TEST INITIALIZED")
        logger.info("="*80)
        logger.info(f"📦 Loaded {len(self.mapper.grabfood_skus)} master SKU products")
        logger.info(f"🎯 Fuzzy matching: {'ENABLED ✅' if self.mapper.name_to_sku_map else 'DISABLED ❌'}")
        logger.info("")
    
    def test_single_store(self, url: str, store_num: int, total: int) -> Dict:
        """Test scraping + mapping for a single store"""
        logger.info("="*80)
        logger.info(f"🏪 STORE {store_num}/{total}")
        logger.info("="*80)
        logger.info(f"URL: {url}")
        logger.info("")
        
        result = {
            'url': url,
            'store_name': '',
            'scraping_success': False,
            'total_items': 0,
            'available_items': 0,
            'unavailable_items': 0,
            'unavailable_names': [],
            'mapped_skus': [],
            'unknown_products': [],
            'mapping_success_rate': 0.0
        }
        
        try:
            # STEP 1: Scrape with wow.py
            logger.info("📡 STEP 1: Scraping with Selenium...")
            scrape_result = self.scraper.scrape_menu(url)
            
            result['store_name'] = scrape_result['store_name']
            result['total_items'] = len(scrape_result['all_items'])
            result['available_items'] = len(scrape_result['available_items'])
            result['unavailable_items'] = len(scrape_result['unavailable_items'])
            result['scraping_success'] = True
            
            logger.info(f"✅ Scraping complete: {result['store_name']}")
            logger.info(f"   Total products: {result['total_items']}")
            logger.info(f"   🟢 Available: {result['available_items']}")
            logger.info(f"   🔴 Unavailable: {result['unavailable_items']}")
            logger.info("")
            
            # Extract unavailable product names
            unavailable_names = [item['name'] for item in scrape_result['unavailable_items']]
            result['unavailable_names'] = unavailable_names
            
            if not unavailable_names:
                logger.info("✅ All items are available - no mapping needed")
                logger.info("")
                return result
            
            # STEP 2: Map to SKU codes
            logger.info("🗺️ STEP 2: Mapping product names to SKU codes...")
            logger.info("")
            
            mapped_skus = []
            unknown_products = []
            
            for i, name in enumerate(unavailable_names, 1):
                logger.info(f"   [{i}/{len(unavailable_names)}] Mapping: '{name}'")
                
                sku_code = self.mapper.find_sku_for_name(name)
                
                if sku_code:
                    mapped_skus.append(sku_code)
                    logger.info(f"       ✅ Mapped to: {sku_code}")
                    
                    # Show what master product it matched to
                    master_product = next(
                        (p for p in self.mapper.grabfood_skus if p['sku_code'] == sku_code),
                        None
                    )
                    if master_product:
                        logger.info(f"       📦 Master product: '{master_product['product_name']}'")
                else:
                    unknown_products.append(name)
                    logger.info(f"       ❌ NO MATCH FOUND")
                
                logger.info("")
            
            result['mapped_skus'] = mapped_skus
            result['unknown_products'] = unknown_products
            
            # Calculate success rate
            if unavailable_names:
                result['mapping_success_rate'] = (len(mapped_skus) / len(unavailable_names)) * 100
            
            # STEP 3: Summary
            logger.info("📊 MAPPING SUMMARY:")
            logger.info(f"   Total unavailable: {len(unavailable_names)}")
            logger.info(f"   ✅ Successfully mapped: {len(mapped_skus)} ({result['mapping_success_rate']:.1f}%)")
            logger.info(f"   ❌ Unknown products: {len(unknown_products)}")
            
            if mapped_skus:
                logger.info("")
                logger.info("   🎯 Mapped SKU codes:")
                for sku in mapped_skus:
                    logger.info(f"      • {sku}")
            
            if unknown_products:
                logger.info("")
                logger.info("   ⚠️ Unknown products (need attention):")
                for product in unknown_products:
                    logger.info(f"      • {product}")
            
            logger.info("")
            
            return result
            
        except Exception as e:
            logger.error(f"❌ Error testing store: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return result
    
    def test_all_stores(self):
        """Test all stores and show final summary"""
        logger.info("")
        logger.info("🚀 STARTING TEST ON ALL STORES")
        logger.info("")
        
        results = []
        
        for i, url in enumerate(TEST_STORES, 1):
            result = self.test_single_store(url, i, len(TEST_STORES))
            results.append(result)
            
            # Pause between stores
            if i < len(TEST_STORES):
                logger.info("⏸️ Pausing 5 seconds before next store...")
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
        
        total_scraped = sum(1 for r in results if r['scraping_success'])
        total_items = sum(r['total_items'] for r in results)
        total_unavailable = sum(r['unavailable_items'] for r in results)
        total_mapped = sum(len(r['mapped_skus']) for r in results)
        total_unknown = sum(len(r['unknown_products']) for r in results)
        
        logger.info(f"📊 Overall Statistics:")
        logger.info(f"   Stores tested: {len(results)}")
        logger.info(f"   Scraping success: {total_scraped}/{len(results)}")
        logger.info(f"   Total products checked: {total_items}")
        logger.info(f"   Unavailable products found: {total_unavailable}")
        logger.info("")
        
        logger.info(f"🗺️ Mapping Performance:")
        logger.info(f"   Products to map: {total_unavailable}")
        logger.info(f"   ✅ Successfully mapped: {total_mapped} ({total_mapped/max(total_unavailable,1)*100:.1f}%)")
        logger.info(f"   ❌ Unknown products: {total_unknown} ({total_unknown/max(total_unavailable,1)*100:.1f}%)")
        logger.info("")
        
        # Store-by-store breakdown
        logger.info("🏪 Store-by-Store Results:")
        logger.info("")
        
        for i, result in enumerate(results, 1):
            if not result['scraping_success']:
                logger.info(f"   {i}. {result['url']}")
                logger.info(f"      ❌ Scraping failed")
                logger.info("")
                continue
            
            logger.info(f"   {i}. {result['store_name']}")
            logger.info(f"      Products: {result['total_items']} total, {result['unavailable_items']} unavailable")
            logger.info(f"      Mapping: {len(result['mapped_skus'])}/{result['unavailable_items']} ({result['mapping_success_rate']:.1f}%)")
            
            if result['mapped_skus']:
                logger.info(f"      ✅ SKUs: {', '.join(result['mapped_skus'])}")
            
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
            logger.info("⚠️ ALL UNKNOWN PRODUCTS (need to be added to master SKUs):")
            logger.info("")
            unique_unknown = sorted(set(all_unknown))
            for product in unique_unknown:
                logger.info(f"   • {product}")
            logger.info("")
        else:
            logger.info("✅ All products were successfully mapped!")
            logger.info("")
        
        # Overall verdict
        logger.info("="*80)
        if total_unknown == 0:
            logger.info("✅ TEST PASSED: All products mapped successfully!")
        elif total_unknown < total_unavailable * 0.1:  # Less than 10% unknown
            logger.info("⚠️ TEST PASSED WITH WARNINGS: Most products mapped (>90%)")
        else:
            logger.info("❌ TEST NEEDS ATTENTION: Many unmapped products")
        logger.info("="*80)
        logger.info("")
        
        # Database save confirmation
        logger.info("💾 DATABASE STATUS: NOT SAVED (test mode)")
        logger.info("   To save results, run the full scraper in production mode")
        logger.info("")
    
    def close(self):
        """Cleanup"""
        self.scraper.close()

# ============================================================================
# Main
# ============================================================================
def main():
    """Run the test"""
    logger.info("="*80)
    logger.info("🧪 SKU MAPPING TEST SCRIPT")
    logger.info("="*80)
    logger.info("📋 Test Configuration:")
    logger.info(f"   • Stores to test: {len(TEST_STORES)}")
    logger.info(f"   • Database writes: DISABLED ❌")
    logger.info(f"   • Fuzzy matching: ENABLED ✅")
    logger.info("="*80)
    logger.info("")
    
    tester = SKUMappingTester()
    
    try:
        results = tester.test_all_stores()
        
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