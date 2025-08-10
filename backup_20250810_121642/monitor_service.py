#!/usr/bin/env python3
"""
Fixed CocoPan Monitor Service
Improved error handling and resilience
"""
import os
import json
import time
import logging
import signal
import sys
from datetime import datetime
from typing import List, Tuple
import pytz
import requests
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright
try:
    from apscheduler.schedulers.blocking import BlockingScheduler
    from apscheduler.triggers.cron import CronTrigger
    HAS_SCHEDULER = True
except ImportError:
    HAS_SCHEDULER = False
    
from config import config
from database import db

logging.basicConfig(
    level=getattr(logging, config.LOG_LEVEL),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class StoreMonitor:
    def __init__(self):
        self.store_urls = self._load_store_urls()
        self.headers = {'User-Agent': 'Mozilla/5.0 (compatible; CocoPan-Monitor)'}
        logger.info(f"🏪 StoreMonitor initialized with {len(self.store_urls)} stores")
    
    def _load_store_urls(self):
        try:
            with open(config.STORE_URLS_FILE) as f:
                data = json.load(f)
                urls = data.get('urls', [])
                logger.info(f"📋 Loaded {len(urls)} store URLs")
                
                grabfood_count = sum(1 for url in urls if 'grab.com' in url)
                foodpanda_count = sum(1 for url in urls if 'foodpanda.ph' in url)
                logger.info(f"   🛒 GrabFood: {grabfood_count} stores")
                logger.info(f"   🐼 Foodpanda: {foodpanda_count} stores")
                
                return urls
        except Exception as e:
            logger.error(f"❌ Failed to load store URLs: {e}")
            return []
    
    def check_store_online(self, url: str):
        """Check if store is online with improved error handling"""
        start_time = time.time()
        
        try:
            if 'foodpanda.ph' in url:
                return self._check_foodpanda_store(url, start_time)
            else:
                return self._check_grabfood_store(url, start_time)
        except Exception as e:
            response_time = int((time.time() - start_time) * 1000)
            error_msg = f"Check failed: {str(e)}"
            logger.warning(f"Store check error for {url}: {error_msg}")
            return False, response_time, error_msg
    
    def _check_foodpanda_store(self, url: str, start_time: float):
        """Check Foodpanda store with Playwright"""
        try:
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=True)
                page = browser.new_page()
                
                # Set timeout and navigate
                page.set_default_timeout(60000)
                page.goto(url, timeout=60000, wait_until='domcontentloaded')
                page.wait_for_timeout(3000)
                
                # Check for closed indicators
                closed_indicators = [
                    "text=Temporarily unavailable",
                    "text=Closed for now", 
                    "text=Out of delivery area",
                    "text=Restaurant is closed",
                    ".closed-banner",
                    "[data-testid='closed-banner']"
                ]
                
                for indicator in closed_indicators:
                    if page.query_selector(indicator):
                        browser.close()
                        response_time = int((time.time() - start_time) * 1000)
                        return False, response_time, "Store shows as closed"
                
                browser.close()
                response_time = int((time.time() - start_time) * 1000)
                return True, response_time, None
                
        except Exception as e:
            response_time = int((time.time() - start_time) * 1000)
            return False, response_time, f"Playwright error: {str(e)}"
    
    def _check_grabfood_store(self, url: str, start_time: float):
        """Check GrabFood store with requests"""
        try:
            resp = requests.get(url, headers=self.headers, timeout=config.REQUEST_TIMEOUT)
            resp.raise_for_status()
            
            soup = BeautifulSoup(resp.text, 'html.parser')
            banner = soup.select_one('.status-banner')
            if banner and 'closed' in banner.get_text(strip=True).lower():
                response_time = int((time.time() - start_time) * 1000)
                return False, response_time, "Status banner shows closed"
            
            response_time = int((time.time() - start_time) * 1000)
            return True, response_time, None
            
        except Exception as e:
            response_time = int((time.time() - start_time) * 1000)
            return False, response_time, f"Request error: {str(e)}"
    
    def _get_store_name(self, url: str) -> str:
        """Extract store name from URL with fallback"""
        try:
            r = requests.get(url, headers=self.headers, timeout=config.REQUEST_TIMEOUT)
            r.raise_for_status()
            h1_tag = BeautifulSoup(r.text, 'html.parser').select_one('h1')
            name = h1_tag.get_text(strip=True) if h1_tag else None
            
            if not name or name.lower() == '403 error':
                name = None
        except Exception:
            name = None
            
        if not name:
            slug = url.rstrip('/').split('/')[-1] 
            name = slug.replace('-', ' ').title()
        
        return name
    
    def check_all_stores(self):
        """Check all stores with improved error handling and resilience"""
        manila_tz = pytz.timezone(config.TIMEZONE)
        current_time = datetime.now(manila_tz)
        
        logger.info(f"🔍 Starting store check cycle at {current_time.strftime('%Y-%m-%d %H:%M:%S %Z')}")
        logger.info(f"📋 Checking {len(self.store_urls)} stores...")
        
        results = []
        online_count = 0
        offline_count = 0
        database_errors = 0
        
        for i, url in enumerate(self.store_urls, 1):
            try:
                logger.info(f"🔍 Checking store {i}/{len(self.store_urls)}: {url}")
                
                # Get store name with error handling
                try:
                    store_name = self._get_store_name(url)
                except Exception as e:
                    logger.warning(f"Failed to get store name for {url}: {e}")
                    store_name = f"Store {i}"
                
                # Check store status
                is_online, response_time, error_msg = self.check_store_online(url)
                
                status = "🟢 ONLINE" if is_online else "🔴 OFFLINE"
                logger.info(f"  📊 {store_name}: {status} ({response_time}ms)")
                
                # Save to database with error handling
                try:
                    store_id = db.get_or_create_store(store_name, url)
                    success = db.save_status_check(store_id, is_online, response_time, error_msg)
                    
                    if not success:
                        logger.error(f"  ❌ Failed to save status check for {store_name}")
                        database_errors += 1
                    else:
                        logger.debug(f"  ✅ Saved status check for {store_name}")
                        
                except Exception as db_error:
                    logger.error(f"  ❌ Database error for {store_name}: {str(db_error)}")
                    database_errors += 1
                    # Continue processing other stores instead of failing completely
                
                results.append((store_name, url, is_online, response_time, error_msg))
                if is_online:
                    online_count += 1
                else:
                    offline_count += 1
                
                # Be respectful between checks
                time.sleep(1)
                
            except Exception as e:
                logger.error(f"  ❌ Critical error processing store {i} ({url}): {str(e)}")
                # Continue with next store instead of crashing
                continue
        
        # Save summary report with error handling
        try:
            total_stores = len(results)
            if total_stores > 0:
                success = db.save_summary_report(total_stores, online_count, offline_count)
                
                if not success:
                    logger.error("❌ Failed to save summary report")
                    database_errors += 1
        except Exception as e:
            logger.error(f"❌ Error saving summary report: {str(e)}")
            database_errors += 1
        
        # Log comprehensive summary
        total_stores = len(results)
        online_pct = (online_count / total_stores * 100) if total_stores > 0 else 0
        
        logger.info(f"📊 Check cycle completed:")
        logger.info(f"   ✅ Online: {online_count}/{total_stores} ({online_pct:.1f}%)")
        logger.info(f"   ❌ Offline: {offline_count}/{total_stores}")
        
        if database_errors > 0:
            logger.warning(f"   🔥 Database errors: {database_errors}")
        
        if offline_count > 0:
            offline_stores = [name for name, _, is_online, _, _ in results if not is_online]
            logger.warning(f"🔴 Offline stores: {', '.join(offline_stores[:5])}")
            if len(offline_stores) > 5:
                logger.warning(f"   ... and {len(offline_stores) - 5} more")

def signal_handler(signum, frame):
    """Handle shutdown signals gracefully"""
    logger.info(f"🛑 Received signal {signum}, shutting down gracefully...")
    sys.exit(0)

def main():
    """Main entry point with improved error handling"""
    logger.info("🥥 CocoPan Store Monitor Service")
    logger.info("=" * 50)
    
    # Set up signal handlers
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)
    
    try:
        monitor = StoreMonitor()
        
        if not monitor.store_urls:
            logger.error("❌ No store URLs loaded. Cannot start monitoring.")
            sys.exit(1)
        
        # Test database connection before starting
        try:
            db_stats = db.get_database_stats()
            logger.info(f"📊 Database connection successful: {db_stats['db_type']} with {db_stats['store_count']} stores")
        except Exception as e:
            logger.error(f"❌ Database connection failed: {str(e)}")
            logger.info("💡 Will continue monitoring but data may not be saved")
        
        if HAS_SCHEDULER:
            # Run with scheduler if available
            try:
                scheduler = BlockingScheduler(timezone=pytz.timezone(config.TIMEZONE))
                
                # Schedule monitoring jobs for each hour during business hours
                for hour in range(config.MONITOR_START_HOUR, config.MONITOR_END_HOUR + 1):
                    scheduler.add_job(
                        func=monitor.check_all_stores,
                        trigger=CronTrigger(hour=hour, minute=0, timezone=config.TIMEZONE),
                        id=f'store_check_{hour}',
                        name=f'Store Check at {hour:02d}:00',
                        max_instances=1,
                        coalesce=True
                    )
                
                logger.info(f"⏰ Scheduled monitoring every hour from {config.MONITOR_START_HOUR}:00 to {config.MONITOR_END_HOUR}:00")
                
                # Run initial check
                logger.info("🔍 Running initial store check...")
                try:
                    monitor.check_all_stores()
                except Exception as e:
                    logger.error(f"❌ Initial check failed: {str(e)}")
                    logger.info("🔄 Will continue with scheduled checks")
                
                logger.info("✅ Monitor service started with scheduler")
                scheduler.start()
                
            except KeyboardInterrupt:
                logger.info("🛑 Received KeyboardInterrupt")
            except Exception as e:
                logger.error(f"❌ Scheduler error: {str(e)}")
                logger.info("🔄 Falling back to simple mode")
                # Fall back to simple mode instead of crashing
                simple_monitoring_loop(monitor)
        else:
            # Simple mode without scheduler
            simple_monitoring_loop(monitor)
            
    except Exception as e:
        logger.error(f"❌ Critical error in main: {str(e)}")
        logger.info("🔄 Attempting to restart...")
        time.sleep(30)  # Wait before restart
        # In Docker, the container will restart automatically

def simple_monitoring_loop(monitor):
    """Simple monitoring loop without scheduler"""
    logger.info("🔍 Running in simple monitoring mode...")
    
    try:
        # Run initial check
        monitor.check_all_stores()
        
        # Keep container alive by sleeping and running periodic checks
        logger.info("💤 Entering monitoring loop...")
        while True:
            time.sleep(3600)  # Sleep for 1 hour
            
            # Check if we're in monitoring hours
            manila_tz = pytz.timezone(config.TIMEZONE)
            current_hour = datetime.now(manila_tz).hour
            
            if config.is_monitor_time(current_hour):
                logger.info("🔍 Running scheduled store check...")
                try:
                    monitor.check_all_stores()
                except Exception as e:
                    logger.error(f"❌ Scheduled check failed: {str(e)}")
                    logger.info("🔄 Will retry on next cycle")
            else:
                logger.info(f"😴 Outside monitoring hours ({current_hour}:00), sleeping...")
                
    except KeyboardInterrupt:
        logger.info("🛑 Received KeyboardInterrupt")
    except Exception as e:
        logger.error(f"❌ Error in monitoring loop: {str(e)}")
        time.sleep(60)  # Wait before restart

if __name__ == "__main__":
    main()