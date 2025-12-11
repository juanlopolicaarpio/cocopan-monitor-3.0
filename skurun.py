#!/usr/bin/env python3
"""
Standalone GrabFood SKU Scraper
Run this to scrape all stores RIGHT NOW and save to database
"""
import sys
import logging
from datetime import datetime
import time

# Import from existing modules
from wow import GrabFoodScraper
from monitor_service import SKUMapper
from database import db
from config import config

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ============================================================================
# Configuration
# ============================================================================
import json

def load_grabfood_urls():
    """Load GrabFood URLs from branch_urls.json"""
    try:
        with open(config.STORE_URLS_FILE) as f:
            data = json.load(f)
            all_urls = data.get('urls', [])
            grabfood_urls = [url for url in all_urls if 'grab.com' in url]
            return grabfood_urls
    except Exception as e:
        logger.error(f"Failed to load URLs: {e}")
        return []

# ============================================================================
# Main Scraper Class
# ============================================================================
class StandaloneSKUScraper:
    """Standalone scraper that saves to database"""
    
    def __init__(self, max_retries=3):
        self.selenium_scraper = GrabFoodScraper()
        self.sku_mapper = SKUMapper()
        self.store_urls = load_grabfood_urls()
        self.max_retries = max_retries
        self.problematic_stores = []  # Track stores with 0 items
        
        logger.info("="*80)
        logger.info("🛒 STANDALONE GRABFOOD SKU SCRAPER")
        logger.info("="*80)
        logger.info(f"📦 Loaded {len(self.sku_mapper.grabfood_skus)} master SKU products")
        logger.info(f"📋 Loaded {len(self.store_urls)} GrabFood store URLs")
        logger.info(f"💾 Will save results to database")
        logger.info(f"🔄 Max retries per store: {self.max_retries}")
        logger.info("="*80)
        logger.info("")
    
    def scrape_single_store(self, store_url: str, index: int, total: int):
        """Scrape one store and save to database (with retry logic)"""
        logger.info("="*80)
        logger.info(f"🏪 STORE {index}/{total}")
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
        
        # Retry loop
        for attempt in range(self.max_retries):
            try:
                # STEP 1: Scrape with Selenium
                if attempt > 0:
                    logger.info(f"🔄 RETRY {attempt}/{self.max_retries - 1}")
                    logger.info(f"   Refreshing page...")
                    logger.info("")
                
                logger.info("📡 Scraping menu...")
                scrape_result = self.selenium_scraper.scrape_menu(store_url)
                
                store_name = scrape_result['store_name']
                result['store_name'] = store_name
                
                total_items = len(scrape_result['all_items'])
                unavailable_items = scrape_result['unavailable_items']
                
                logger.info(f"✅ {store_name}")
                logger.info(f"   Total products: {total_items}")
                logger.info(f"   Unavailable: {len(unavailable_items)}")
                logger.info("")
                
                # Check if we got 0 items (problematic)
                if total_items == 0:
                    result['has_zero_items'] = True
                    result['retry_count'] = attempt + 1
                    
                    if attempt < self.max_retries - 1:
                        logger.warning(f"⚠️ Got 0 products - this looks wrong!")
                        logger.info(f"   Will retry ({attempt + 1}/{self.max_retries - 1} retries so far)")
                        logger.info("")
                        time.sleep(3)  # Wait before retry
                        continue  # Retry
                    else:
                        logger.error(f"❌ Still 0 products after {self.max_retries} attempts!")
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
                    logger.info(f"✅ Success on retry {attempt}!")
                    logger.info("")
                
                # Extract unavailable names
                unavailable_names = [item['name'] for item in unavailable_items]
                
                if not unavailable_names:
                    logger.info("✅ All items available - saving to database...")
                    
                    # Save to database with empty OOS list
                    store_id = db.get_or_create_store(store_name, store_url)
                    success = db.save_sku_compliance_check(
                        store_id=store_id,
                        platform='grabfood',
                        out_of_stock_ids=[],
                        checked_by='manual_scraper'
                    )
                    
                    result['success'] = success
                    logger.info(f"💾 Saved to database: {'✅' if success else '❌'}")
                    logger.info("")
                    return result
                
                # STEP 2: Map to SKU codes
                logger.info("🗺️ Mapping product names to SKU codes...")
                
                oos_skus = []
                unknown_products = []
                
                for i, name in enumerate(unavailable_names, 1):
                    logger.info(f"   [{i}/{len(unavailable_names)}] '{name}'")
                    
                    sku_code = self.sku_mapper.find_sku_for_name(name)
                    
                    if sku_code:
                        oos_skus.append(sku_code)
                        logger.info(f"       ✅ → {sku_code}")
                    else:
                        unknown_products.append(name)
                        logger.info(f"       ❌ No match")
                
                logger.info("")
                logger.info(f"📊 Mapping Results:")
                logger.info(f"   ✅ Mapped: {len(oos_skus)}")
                logger.info(f"   ❌ Unknown: {len(unknown_products)}")
                logger.info("")
                
                result['oos_skus'] = oos_skus
                result['unknown_products'] = unknown_products
                
                # STEP 3: Save to database
                logger.info("💾 Saving to database...")
                
                store_id = db.get_or_create_store(store_name, store_url)
                success = db.save_sku_compliance_check(
                    store_id=store_id,
                    platform='grabfood',
                    out_of_stock_ids=oos_skus,
                    checked_by='manual_scraper'
                )
                
                result['success'] = success
                
                if success:
                    logger.info(f"✅ Successfully saved to database!")
                    logger.info(f"   Store: {store_name}")
                    logger.info(f"   OOS SKUs: {len(oos_skus)}")
                    logger.info(f"   Compliance: {((total_items - len(oos_skus)) / total_items * 100):.1f}%")
                else:
                    logger.error(f"❌ Failed to save to database")
                
                logger.info("")
                
                return result
                
            except Exception as e:
                logger.error(f"❌ Error on attempt {attempt + 1}: {e}")
                
                if attempt < self.max_retries - 1:
                    logger.info(f"   Will retry...")
                    logger.info("")
                    time.sleep(3)
                    continue
                else:
                    logger.error(f"   Failed after {self.max_retries} attempts")
                    import traceback
                    logger.error(traceback.format_exc())
                    logger.info("")
                    return result
        
        return result
    
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
            
            # Pause between stores
            if i < len(urls):
                wait_time = 5
                logger.info(f"⏸️ Waiting {wait_time} seconds before next store...")
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
    
    def close(self):
        """Cleanup"""
        self.selenium_scraper.close()

# ============================================================================
# Main
# ============================================================================
def main():
    """Main entry point"""
    logger.info("")
    logger.info("="*80)
    logger.info("🛒 GRABFOOD SKU SCRAPER - STANDALONE MODE")
    logger.info("="*80)
    logger.info("")
    
    # Confirmation prompt
    try:
        response = input("⚠️ This will scrape ALL stores and save to database. Continue? (y/n): ").strip().lower()
        if response != 'y':
            logger.info("❌ Cancelled by user")
            return
    except KeyboardInterrupt:
        logger.info("\n❌ Cancelled by user")
        return
    
    logger.info("")
    
    scraper = StandaloneSKUScraper(max_retries=3)
    
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
        scraper.close()
        logger.info("👋 Goodbye!")

if __name__ == "__main__":
    main()