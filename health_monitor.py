#!/usr/bin/env python3
"""
FIXED CocoPan Monitor Service
Corrected async/await patterns and improved error handling
"""
import os
import json
import time
import logging
import signal
import sys
import asyncio
import concurrent.futures
from datetime import datetime
from typing import List, Tuple, Dict, Any
import pytz
import requests
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright
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

class CircuitBreaker:
    """Simple circuit breaker for failed stores"""
    def __init__(self, failure_threshold=3, timeout=300):  # 5 min timeout
        self.failure_threshold = failure_threshold
        self.timeout = timeout
        self.failures = {}
        self.last_failure_time = {}
        self.states = {}  # 'closed', 'open', 'half-open'
    
    def is_available(self, store_url: str) -> bool:
        """Check if store should be checked"""
        state = self.states.get(store_url, 'closed')
        
        if state == 'closed':
            return True
        elif state == 'open':
            # Check if timeout period has passed
            if time.time() - self.last_failure_time.get(store_url, 0) > self.timeout:
                self.states[store_url] = 'half-open'
                return True
            return False
        elif state == 'half-open':
            return True
        
        return True
    
    def record_success(self, store_url: str):
        """Record successful check"""
        self.failures[store_url] = 0
        self.states[store_url] = 'closed'
    
    def record_failure(self, store_url: str):
        """Record failed check"""
        self.failures[store_url] = self.failures.get(store_url, 0) + 1
        self.last_failure_time[store_url] = time.time()
        
        if self.failures[store_url] >= self.failure_threshold:
            self.states[store_url] = 'open'
            logger.warning(f"🔴 Circuit breaker OPEN for {store_url} after {self.failures[store_url]} failures")
    
    def get_stats(self) -> Dict[str, int]:
        """Get circuit breaker statistics"""
        stats = {'closed': 0, 'open': 0, 'half-open': 0}
        for state in self.states.values():
            stats[state] = stats.get(state, 0) + 1
        return stats

class StoreMonitor:
    def __init__(self):
        self.store_urls = self._load_store_urls()
        self.headers = {'User-Agent': config.USER_AGENT}
        self.timezone = config.get_timezone()
        self.circuit_breaker = CircuitBreaker()
        self.store_cache = {}  # Cache store names
        logger.info(f"🏪 StoreMonitor initialized with {len(self.store_urls)} stores")
        logger.info(f"🌏 Timezone: {self.timezone} (Current: {config.get_current_time().strftime('%Y-%m-%d %H:%M:%S %Z')})")
    
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
    
    def _get_store_name(self, url: str) -> str:
        """Extract store name from URL with caching"""
        if url in self.store_cache:
            return self.store_cache[url]
        
        try:
            r = requests.get(url, headers=self.headers, timeout=config.REQUEST_TIMEOUT)
            r.raise_for_status()
            soup = BeautifulSoup(r.text, 'html.parser')
            
            # Try multiple selectors for store name
            name_selectors = ['h1', '.restaurant-name', '[data-testid="restaurant-name"]', 'title']
            name = None
            
            for selector in name_selectors:
                element = soup.select_one(selector)
                if element:
                    potential_name = element.get_text(strip=True)
                    if potential_name and len(potential_name) > 3 and 'error' not in potential_name.lower():
                        name = potential_name
                        break
                        
        except Exception as e:
            logger.debug(f"Failed to extract name from {url}: {e}")
            name = None
            
        if not name:
            # Generate name from URL
            slug = url.rstrip('/').split('/')[-1]
            if 'cocopan' in slug.lower():
                name = slug.replace('-', ' ').title()
            else:
                name = f"Cocopan Store ({slug[:20]}...)"
        
        self.store_cache[url] = name
        return name
    
    def check_store_sync(self, url: str) -> Tuple[bool, int, str]:
        """FIXED: Synchronous store checking"""
        start_time = time.time()
        
        # Check circuit breaker
        if not self.circuit_breaker.is_available(url):
            response_time = int((time.time() - start_time) * 1000)
            return False, response_time, "Circuit breaker OPEN"
        
        try:
            if 'foodpanda.ph' in url:
                result = self._check_foodpanda_store_sync(url, start_time)
            else:
                result = self._check_grabfood_store_sync(url, start_time)
            
            # Update circuit breaker
            if result[0]:  # is_online
                self.circuit_breaker.record_success(url)
            else:
                self.circuit_breaker.record_failure(url)
                
            return result
            
        except Exception as e:
            response_time = int((time.time() - start_time) * 1000)
            error_msg = f"Check failed: {str(e)}"
            self.circuit_breaker.record_failure(url)
            logger.warning(f"Store check error for {url}: {error_msg}")
            return False, response_time, error_msg
    
    def _check_foodpanda_store_sync(self, url: str, start_time: float):
        """FIXED: Synchronous Foodpanda store check"""
        try:
            # Use requests for initial check - faster and more reliable
            resp = requests.get(url, headers=self.headers, timeout=config.REQUEST_TIMEOUT)
            resp.raise_for_status()
            
            soup = BeautifulSoup(resp.text, 'html.parser')
            
            # Check for closed indicators in HTML
            closed_indicators = [
                "temporarily unavailable",
                "closed for now", 
                "out of delivery area",
                "restaurant is closed",
                "currently closed"
            ]
            
            page_text = soup.get_text().lower()
            for indicator in closed_indicators:
                if indicator in page_text:
                    response_time = int((time.time() - start_time) * 1000)
                    return False, response_time, f"Store shows as: {indicator}"
            
            # Look for specific closed elements
            closed_elements = soup.find_all(['div', 'span', 'p'], 
                                          class_=lambda x: x and any(word in x.lower() for word in ['closed', 'unavailable']))
            
            if closed_elements:
                response_time = int((time.time() - start_time) * 1000)
                return False, response_time, "Store shows as closed"
            
            response_time = int((time.time() - start_time) * 1000)
            return True, response_time, None
            
        except Exception as e:
            response_time = int((time.time() - start_time) * 1000)
            return False, response_time, f"Foodpanda check error: {str(e)}"
    
    def _check_grabfood_store_sync(self, url: str, start_time: float):
        """FIXED: Synchronous GrabFood store check"""
        try:
            resp = requests.get(url, headers=self.headers, timeout=config.REQUEST_TIMEOUT)
            resp.raise_for_status()
            
            soup = BeautifulSoup(resp.text, 'html.parser')
            
            # Check multiple indicators for closed status
            closed_indicators = [
                '.status-banner',
                '.closed-banner', 
                '.restaurant-closed',
                '[data-testid="closed-banner"]'
            ]
            
            for selector in closed_indicators:
                banner = soup.select_one(selector)
                if banner:
                    banner_text = banner.get_text(strip=True).lower()
                    if any(word in banner_text for word in ['closed', 'unavailable', 'offline']):
                        response_time = int((time.time() - start_time) * 1000)
                        return False, response_time, f"Status banner: {banner_text}"
            
            # Check page text for closed indicators
            page_text = soup.get_text().lower()
            if any(word in page_text for word in ['temporarily closed', 'currently unavailable', 'restaurant closed']):
                response_time = int((time.time() - start_time) * 1000)
                return False, response_time, "Store shows as closed"
            
            response_time = int((time.time() - start_time) * 1000)
            return True, response_time, None
            
        except Exception as e:
            response_time = int((time.time() - start_time) * 1000)
            return False, response_time, f"GrabFood check error: {str(e)}"
    
    def check_store_with_threading(self, url: str) -> Dict[str, Any]:
        """Check single store with proper error handling"""
        try:
            store_name = self._get_store_name(url)
            is_online, response_time, error_msg = self.check_store_sync(url)
            
            return {
                'url': url,
                'name': store_name,
                'is_online': is_online,
                'response_time': response_time,
                'error_msg': error_msg
            }
        except Exception as e:
            return {
                'url': url,
                'name': f"Store ({url.split('/')[-1][:20]})",
                'is_online': False,
                'response_time': 0,
                'error_msg': f"Critical error: {str(e)}"
            }
    
    def check_all_stores(self):
        """FIXED: Check all stores with proper synchronous patterns"""
        current_time = config.get_current_time()
        
        logger.info(f"🔍 Starting store check cycle at {current_time.strftime('%Y-%m-%d %H:%M:%S %Z')}")
        logger.info(f"📋 Checking {len(self.store_urls)} stores...")
        
        results = []
        online_count = 0
        offline_count = 0
        database_errors = 0
        
        # Use ThreadPoolExecutor for concurrent checks
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            # Submit all store checks
            future_to_url = {executor.submit(self.check_store_with_threading, url): url 
                            for url in self.store_urls}
            
            # Process results
            for i, future in enumerate(concurrent.futures.as_completed(future_to_url), 1):
                try:
                    result = future.result(timeout=30)  # 30 second timeout per store
                    
                    store_name = result['name']
                    url = result['url']
                    is_online = result['is_online']
                    response_time = result['response_time']
                    error_msg = result['error_msg']
                    
                    status = "🟢 ONLINE" if is_online else "🔴 OFFLINE"
                    logger.info(f"  📊 {i}/{len(self.store_urls)} {store_name}: {status} ({response_time}ms)")
                    
                    # Save to database
                    try:
                        store_id = db.get_or_create_store(store_name, url)
                        success = db.save_status_check(store_id, is_online, response_time, error_msg)
                        
                        if not success:
                            logger.error(f"  ❌ Failed to save status check for {store_name}")
                            database_errors += 1
                        
                    except Exception as db_error:
                        logger.error(f"  ❌ Database error for {store_name}: {str(db_error)}")
                        database_errors += 1
                    
                    results.append((store_name, url, is_online, response_time, error_msg))
                    if is_online:
                        online_count += 1
                    else:
                        offline_count += 1
                        
                except concurrent.futures.TimeoutError:
                    url = future_to_url[future]
                    logger.error(f"  ❌ Timeout checking store: {url}")
                    offline_count += 1
                except Exception as e:
                    url = future_to_url[future]
                    logger.error(f"  ❌ Error checking store {url}: {str(e)}")
                    offline_count += 1
        
        # Save summary report
        try:
            total_stores = len(results) if results else len(self.store_urls)
            if total_stores > 0:
                success = db.save_summary_report(total_stores, online_count, offline_count)
                if not success:
                    database_errors += 1
        except Exception as e:
            logger.error(f"❌ Error saving summary report: {str(e)}")
            database_errors += 1
        
        # Log comprehensive summary
        total_stores = len(results) if results else len(self.store_urls)
        online_pct = (online_count / total_stores * 100) if total_stores > 0 else 0
        
        logger.info(f"📊 Check cycle completed:")
        logger.info(f"   ✅ Online: {online_count}/{total_stores} ({online_pct:.1f}%)")
        logger.info(f"   ❌ Offline: {offline_count}/{total_stores}")
        
        # Circuit breaker stats
        cb_stats = self.circuit_breaker.get_stats()
        logger.info(f"   🔧 Circuit breaker: {cb_stats}")
        
        if database_errors > 0:
            logger.warning(f"   🔥 Database errors: {database_errors}")
        
        if offline_count > 0:
            offline_stores = [name for name, _, is_online, _, _ in results if not is_online]
            logger.warning(f"🔴 Offline stores: {', '.join(offline_stores[:3])}")
            if len(offline_stores) > 3:
                logger.warning(f"   ... and {len(offline_stores) - 3} more")

def signal_handler(signum, frame):
    """Handle shutdown signals gracefully"""
    logger.info(f"🛑 Received signal {signum}, shutting down gracefully...")
    sys.exit(0)

def main():
    """Main entry point with improved error handling"""
    logger.info("🥥 CocoPan Store Monitor Service - FIXED VERSION")
    logger.info("=" * 50)
    
    # Validate timezone
    if not config.validate_timezone():
        logger.error("❌ Timezone validation failed! Please check configuration.")
        sys.exit(1)
    
    # Set up signal handlers
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)
    
    try:
        monitor = StoreMonitor()
        
        if not monitor.store_urls:
            logger.error("❌ No store URLs loaded. Cannot start monitoring.")
            sys.exit(1)
        
        # Test database connection
        try:
            db_stats = db.get_database_stats()
            logger.info(f"📊 Database connection successful: {db_stats['db_type']} with {db_stats['store_count']} stores")
        except Exception as e:
            logger.error(f"❌ Database connection failed: {str(e)}")
            logger.info("💡 Will continue monitoring but data may not be saved")
        
        if HAS_SCHEDULER:
            try:
                ph_tz = config.get_timezone()
                scheduler = BlockingScheduler(timezone=ph_tz)
                
                # Schedule monitoring jobs
                for hour in range(config.MONITOR_START_HOUR, config.MONITOR_END_HOUR + 1):
                    scheduler.add_job(
                        func=monitor.check_all_stores,
                        trigger=CronTrigger(hour=hour, minute=0, timezone=ph_tz),
                        id=f'store_check_{hour}',
                        name=f'Store Check at {hour:02d}:00',
                        max_instances=1,
                        coalesce=True
                    )
                
                logger.info(f"⏰ Scheduled monitoring every hour from {config.MONITOR_START_HOUR}:00 to {config.MONITOR_END_HOUR}:00 {config.TIMEZONE}")
                
                # Run initial check
                logger.info("🔍 Running initial store check...")
                try:
                    monitor.check_all_stores()
                except Exception as e:
                    logger.error(f"❌ Initial check failed: {str(e)}")
                
                logger.info("✅ Monitor service started successfully")
                scheduler.start()
                
            except KeyboardInterrupt:
                logger.info("🛑 Received KeyboardInterrupt")
            except Exception as e:
                logger.error(f"❌ Scheduler error: {str(e)}")
                simple_monitoring_loop(monitor)
        else:
            simple_monitoring_loop(monitor)
            
    except Exception as e:
        logger.error(f"❌ Critical error in main: {str(e)}")
        time.sleep(30)

def simple_monitoring_loop(monitor):
    """Simple monitoring loop without scheduler"""
    logger.info("🔍 Running in simple monitoring mode...")
    
    try:
        # Run initial check
        monitor.check_all_stores()
        
        while True:
            time.sleep(3600)  # Sleep for 1 hour
            
            current_hour = config.get_current_time().hour
            if config.is_monitor_time(current_hour):
                logger.info("🔍 Running scheduled store check...")
                try:
                    monitor.check_all_stores()
                except Exception as e:
                    logger.error(f"❌ Scheduled check failed: {str(e)}")
            else:
                logger.info(f"😴 Outside monitoring hours ({current_hour}:00), sleeping...")
                
    except KeyboardInterrupt:
        logger.info("🛑 Received KeyboardInterrupt")
    except Exception as e:
        logger.error(f"❌ Error in monitoring loop: {str(e)}")

if __name__ == "__main__":
    main()