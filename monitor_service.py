#!/usr/bin/env python3
"""
BACK TO WORKING - CocoPan Monitor Service
Using simple requests for BOTH platforms (like it was working before)
"""
import os
import json
import time
import logging
import signal
import sys
import concurrent.futures
import random
import re
from datetime import datetime
from typing import List, Tuple, Dict, Any
import pytz
import requests
from bs4 import BeautifulSoup
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
    """Circuit breaker for failed stores"""
    def __init__(self, failure_threshold=3, timeout=300):
        self.failure_threshold = failure_threshold
        self.timeout = timeout
        self.failures = {}
        self.last_failure_time = {}
        self.states = {}
    
    def is_available(self, store_url: str) -> bool:
        state = self.states.get(store_url, 'closed')
        
        if state == 'closed':
            return True
        elif state == 'open':
            if time.time() - self.last_failure_time.get(store_url, 0) > self.timeout:
                self.states[store_url] = 'half-open'
                return True
            return False
        elif state == 'half-open':
            return True
        
        return True
    
    def record_success(self, store_url: str):
        self.failures[store_url] = 0
        self.states[store_url] = 'closed'
    
    def record_failure(self, store_url: str):
        self.failures[store_url] = self.failures.get(store_url, 0) + 1
        self.last_failure_time[store_url] = time.time()
        
        if self.failures[store_url] >= self.failure_threshold:
            self.states[store_url] = 'open'
            logger.warning(f"🔴 Circuit breaker OPEN for {store_url}")
    
    def get_stats(self) -> Dict[str, int]:
        stats = {'closed': 0, 'open': 0, 'half-open': 0}
        for state in self.states.values():
            stats[state] = stats.get(state, 0) + 1
        return stats

class StoreMonitor:
    def __init__(self):
        self.store_urls = self._load_store_urls()
        
        # BACK TO WORKING HEADERS - Rotate user agents
        self.user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/121.0',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15'
        ]
        
        self.timezone = config.get_timezone()
        self.circuit_breaker = CircuitBreaker()
        self.store_cache = {}
        logger.info(f"🏪 StoreMonitor initialized with {len(self.store_urls)} stores")
        logger.info(f"🌏 Timezone: {self.timezone}")
        logger.info("🔄 BACK TO WORKING METHOD: Simple requests for both platforms")
    
    def _load_store_urls(self):
        try:
            with open(config.STORE_URLS_FILE) as f:
                data = json.load(f)
                urls = data.get('urls', [])
                logger.info(f"📋 Loaded {len(urls)} store URLs")
                
                grabfood_count = sum(1 for url in urls if 'grab.com' in url)
                foodpanda_count = sum(1 for url in urls if 'foodpanda.ph' in url)
                logger.info(f"   🛒 GrabFood: {grabfood_count} stores (simple requests)")
                logger.info(f"   🐼 Foodpanda: {foodpanda_count} stores (simple requests - BACK TO WORKING)")
                
                return urls
        except Exception as e:
            logger.error(f"❌ Failed to load store URLs: {e}")
            return []
    
    def _extract_clean_store_name(self, url: str) -> str:
        """Extract proper store names from URLs"""
        if url in self.store_cache:
            return self.store_cache[url]
        
        try:
            if 'grab.com' in url:
                match = re.search(r'/restaurant/([^/]+)/', url)
                if match:
                    slug = match.group(1)
                    name = slug.replace('-', ' ').replace('_', ' ')
                    name = re.sub(r'\s+(delivery|restaurant)$', '', name, flags=re.IGNORECASE)
                    name = ' '.join(word.capitalize() for word in name.split())
                    if not name.lower().startswith('cocopan'):
                        name = f"Cocopan {name}"
                else:
                    name = "Cocopan GrabFood Store"
                    
            elif 'foodpanda.ph' in url:
                match = re.search(r'/restaurant/[^/]+/([^/?]+)', url)
                if match:
                    slug = match.group(1)
                    name = slug.replace('-', ' ').replace('_', ' ')
                    name = ' '.join(word.capitalize() for word in name.split())
                    if not name.lower().startswith('cocopan'):
                        name = f"Cocopan {name}"
                else:
                    name = "Cocopan Foodpanda Store"
            else:
                name = "Cocopan Store"
            
            self.store_cache[url] = name
            return name
            
        except Exception as e:
            logger.debug(f"Error extracting name from {url}: {e}")
            return "Cocopan Store"
    
    def _get_headers(self, url: str):
        """Get headers with rotation for different platforms"""
        base_headers = {
            'User-Agent': random.choice(self.user_agents),
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
        }
        
        # Platform-specific headers
        if 'foodpanda.ph' in url:
            base_headers.update({
                'Cache-Control': 'max-age=0',
                'DNT': '1',
                'Sec-Fetch-User': '?1',
            })
        
        return base_headers
    
    def check_store_simple(self, url: str) -> Tuple[bool, int, str]:
        """BACK TO WORKING: Simple requests for both platforms"""
        start_time = time.time()
        
        # Check circuit breaker
        if not self.circuit_breaker.is_available(url):
            response_time = int((time.time() - start_time) * 1000)
            return False, response_time, "Circuit breaker OPEN"
        
        try:
            if 'foodpanda.ph' in url:
                result = self._check_foodpanda_simple(url, start_time)
            else:
                result = self._check_grabfood_simple(url, start_time)
            
            # Update circuit breaker
            if result[0]:
                self.circuit_breaker.record_success(url)
            else:
                self.circuit_breaker.record_failure(url)
                
            return result
            
        except Exception as e:
            response_time = int((time.time() - start_time) * 1000)
            error_msg = f"Check failed: {str(e)}"
            self.circuit_breaker.record_failure(url)
            return False, response_time, error_msg
    
    def _check_grabfood_simple(self, url: str, start_time: float):
        """GrabFood check with simple requests (WORKING)"""
        try:
            # Random delay to be respectful
            time.sleep(random.uniform(0.5, 2.0))
            
            headers = self._get_headers(url)
            resp = requests.get(url, headers=headers, timeout=config.REQUEST_TIMEOUT)
            resp.raise_for_status()
            
            soup = BeautifulSoup(resp.text, 'html.parser')
            page_text = soup.get_text().lower()
            
            logger.debug(f"🛒 GrabFood check: {resp.status_code}")
            
            # Check for closed indicators in text (FIXED for "Today Closed")
            closed_indicators = [
                "restaurant is closed",
                "currently unavailable", 
                "not accepting orders",
                "temporarily closed",
                "closed for today",
                "currently closed",
                "today closed",  # CRITICAL FIX for GrabFood
                "today  closed",  # Handle extra spaces
            ]
            
            for indicator in closed_indicators:
                if indicator in page_text:
                    response_time = int((time.time() - start_time) * 1000)
                    return False, response_time, f"Closed: {indicator}"
            
            # More conservative closed banner detection
            closed_selectors = [
                '.ant-alert',
                '.restaurant-closed',
                '.status-banner',
                '.closed-banner'
            ]
            
            # FINAL FIX: Smart detection that ignores JavaScript/JSON data
            
            # Approach 1: Only look for "today closed" in visible text (not in script tags)
            # Remove script tags and their content first
            for script in soup(['script', 'style']):
                script.decompose()
            
            # Get clean visible text only
            visible_text = soup.get_text().lower()
            visible_text_clean = ' '.join(visible_text.split())  # Clean whitespace
            
            if 'today closed' in visible_text_clean:
                response_time = int((time.time() - start_time) * 1000)
                return False, response_time, f"Found 'today closed' in visible text"
            
            # Approach 2: Look specifically in opening hours area
            opening_hours_divs = soup.find_all(['div', 'span'], string=lambda x: x and 'opening hours' in x.lower() if x else False)
            for oh_div in opening_hours_divs:
                if oh_div and oh_div.parent:
                    parent_text = oh_div.parent.get_text().lower()
                    if 'today' in parent_text and 'closed' in parent_text:
                        response_time = int((time.time() - start_time) * 1000)
                        return False, response_time, f"Opening hours shows today closed"
            
            # Approach 3: Look for red "Closed" badge (but be conservative)
            for text_node in soup.find_all(string=lambda x: x and x.strip().lower() == 'closed' if x else False):
                if text_node and text_node.parent:
                    parent = text_node.parent
                    # Only trigger if it seems like a status badge (has specific styling or classes)
                    if parent.name in ['span', 'button', 'div'] and ('class' in parent.attrs):
                        classes = ' '.join(parent.attrs.get('class', []))
                        if any(word in classes.lower() for word in ['closed', 'status', 'badge']):
                            response_time = int((time.time() - start_time) * 1000)
                            return False, response_time, f"Found closed status badge"
                elements = soup.select(selector)
                for element in elements:
                    element_text = element.get_text(strip=True).lower()
                    if any(word in element_text for word in ['closed', 'unavailable', 'not accepting']):
                        response_time = int((time.time() - start_time) * 1000)
                        return False, response_time, f"Banner: {element_text[:30]}"
            
            response_time = int((time.time() - start_time) * 1000)
            return True, response_time, None
            
        except requests.RequestException as e:
            response_time = int((time.time() - start_time) * 1000)
            return False, response_time, f"Network error: {str(e)}"
        except Exception as e:
            response_time = int((time.time() - start_time) * 1000)
            return True, response_time, f"Parse warning: {str(e)}"
    
    def _check_foodpanda_simple(self, url: str, start_time: float):
        """BACK TO WORKING: Foodpanda with simple requests (like before)"""
        try:
            # Longer delay for Foodpanda to be more respectful
            time.sleep(random.uniform(1.0, 3.0))
            
            headers = self._get_headers(url)
            
            # Add session to maintain cookies
            session = requests.Session()
            session.headers.update(headers)
            
            resp = session.get(url, timeout=config.REQUEST_TIMEOUT)
            resp.raise_for_status()
            
            soup = BeautifulSoup(resp.text, 'html.parser')
            page_text = soup.get_text().lower()
            
            logger.debug(f"🐼 Foodpanda check: {resp.status_code}")
            
            # BACK TO WORKING: Simple closed detection (like before)
            closed_indicators = [
                "restaurant is closed",
                "currently closed",
                "temporarily unavailable",
                "restaurant temporarily unavailable",
                "out of delivery area",
                "delivery not available",
                "no longer available",
                "restaurant is currently closed",
                "closed for now"
            ]
            
            for indicator in closed_indicators:
                if indicator in page_text:
                    response_time = int((time.time() - start_time) * 1000)
                    return False, response_time, f"Closed: {indicator}"
            
            # Check for closed elements (simple approach)
            closed_selectors = [
                '.restaurant-closed',
                '.temporarily-closed',
                '.unavailable',
                '.closed-banner',
                '[data-testid="closed-banner"]'
            ]
            
            for selector in closed_selectors:
                elements = soup.select(selector)
                for element in elements:
                    element_text = element.get_text(strip=True).lower()
                    if any(word in element_text for word in ['closed', 'unavailable', 'out of delivery']):
                        response_time = int((time.time() - start_time) * 1000)
                        return False, response_time, f"Banner: {element_text[:30]}"
            
            # Look for menu items as positive indicator
            positive_selectors = [
                '.menu-item',
                '.dish-item',
                '.product-item',
                '.food-item',
                '.price',
                '[data-testid="menu-item"]'
            ]
            
            has_positive = any(soup.select(selector) for selector in positive_selectors)
            
            response_time = int((time.time() - start_time) * 1000)
            
            # If we have positive indicators, definitely online
            if has_positive:
                return True, response_time, None
            
            # BACK TO WORKING: If no clear indicators, assume online (to reduce false positives)
            return True, response_time, None
            
        except requests.RequestException as e:
            response_time = int((time.time() - start_time) * 1000)
            
            # Handle specific errors
            if e.response and e.response.status_code == 403:
                return False, response_time, "Access denied (403)"
            elif e.response and e.response.status_code == 429:
                return False, response_time, "Rate limited (429)"
            else:
                return False, response_time, f"Network error: {str(e)}"
                
        except Exception as e:
            response_time = int((time.time() - start_time) * 1000)
            # For parsing errors, assume online (BACK TO WORKING approach)
            return True, response_time, f"Parse warning: {str(e)}"
    
    def check_store_with_threading(self, url: str) -> Dict[str, Any]:
        """Check single store with threading"""
        try:
            store_name = self._extract_clean_store_name(url)
            is_online, response_time, error_msg = self.check_store_simple(url)
            
            return {
                'url': url,
                'name': store_name,
                'is_online': is_online,
                'response_time': response_time,
                'error_msg': error_msg
            }
        except Exception as e:
            store_name = self._extract_clean_store_name(url)
            return {
                'url': url,
                'name': store_name,
                'is_online': False,
                'response_time': 0,
                'error_msg': f"Critical error: {str(e)}"
            }
    
    def check_all_stores(self):
        """Check all stores with BACK TO WORKING method"""
        current_time = config.get_current_time()
        
        logger.info(f"🔍 Starting BACK TO WORKING check at {current_time.strftime('%Y-%m-%d %H:%M:%S %Z')}")
        logger.info(f"📋 Checking {len(self.store_urls)} stores with simple requests (like it was working)...")
        
        results = []
        online_count = 0
        offline_count = 0
        database_errors = 0
        
        # Back to working concurrency (5 workers like before)
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            future_to_url = {executor.submit(self.check_store_with_threading, url): url 
                            for url in self.store_urls}
            
            for i, future in enumerate(concurrent.futures.as_completed(future_to_url), 1):
                try:
                    result = future.result(timeout=30)  # Shorter timeout like before
                    
                    store_name = result['name']
                    url = result['url']
                    is_online = result['is_online']
                    response_time = result['response_time']
                    error_msg = result['error_msg']
                    
                    platform = "🛒" if 'grab.com' in url else "🐼"
                    status = "🟢 ONLINE" if is_online else "🔴 OFFLINE"
                    logger.info(f"  📊 {i}/{len(self.store_urls)} {store_name} ({platform}): {status} ({response_time}ms)")
                    
                    if not is_online and error_msg:
                        logger.info(f"    📝 Reason: {error_msg}")
                    
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
        
        # Summary (like before)
        total_stores = len(results) if results else len(self.store_urls)
        online_pct = (online_count / total_stores * 100) if total_stores > 0 else 0
        
        # Platform breakdown
        grabfood_results = [(name, is_online) for name, url, is_online, _, _ in results if 'grab.com' in url]
        foodpanda_results = [(name, is_online) for name, url, is_online, _, _ in results if 'foodpanda.ph' in url]
        
        grabfood_online = sum(1 for _, is_online in grabfood_results if is_online)
        foodpanda_online = sum(1 for _, is_online in foodpanda_results if is_online)
        
        logger.info(f"📊 BACK TO WORKING CHECK COMPLETED:")
        logger.info(f"   ✅ Total Online: {online_count}/{total_stores} ({online_pct:.1f}%)")
        logger.info(f"   🛒 GrabFood: {grabfood_online}/{len(grabfood_results)} online")
        logger.info(f"   🐼 Foodpanda: {foodpanda_online}/{len(foodpanda_results)} online (BACK TO WORKING)")
        
        cb_stats = self.circuit_breaker.get_stats()
        logger.info(f"   🔧 Circuit breaker: {cb_stats}")
        
        if database_errors > 0:
            logger.warning(f"   🔥 Database errors: {database_errors}")
        
        if offline_count > 0:
            offline_stores = [(name, error) for name, _, is_online, _, error in results if not is_online]
            logger.warning(f"🔴 OFFLINE STORES:")
            for store_name, error in offline_stores[:5]:
                logger.warning(f"   • {store_name}: {error}")
            if len(offline_stores) > 5:
                logger.warning(f"   ... and {len(offline_stores) - 5} more")

# Keep the same main function structure
def signal_handler(signum, frame):
    logger.info(f"🛑 Received signal {signum}, shutting down gracefully...")
    sys.exit(0)

def main():
    logger.info("🥥 CocoPan Store Monitor - BACK TO WORKING VERSION")
    logger.info("=" * 60)
    
    if not config.validate_timezone():
        logger.error("❌ Timezone validation failed! Please check configuration.")
        sys.exit(1)
    
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)
    
    try:
        monitor = StoreMonitor()
        
        if not monitor.store_urls:
            logger.error("❌ No store URLs loaded. Cannot start monitoring.")
            sys.exit(1)
        
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
                
                logger.info("🔍 Running initial BACK TO WORKING check...")
                try:
                    monitor.check_all_stores()
                except Exception as e:
                    logger.error(f"❌ Initial check failed: {str(e)}")
                
                logger.info("✅ BACK TO WORKING monitor service started successfully")
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
    logger.info("🔍 Running in simple monitoring mode...")
    
    try:
        monitor.check_all_stores()
        
        while True:
            time.sleep(3600)
            
            current_hour = config.get_current_time().hour
            if config.is_monitor_time(current_hour):
                logger.info("🔍 Running scheduled BACK TO WORKING check...")
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






