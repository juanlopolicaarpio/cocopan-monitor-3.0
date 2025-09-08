#!/usr/bin/env python3
"""
CocoPan Monitor Service - GRABFOOD ONLY VERSION
✅ ONLY monitors GrabFood stores (~24 stores)
✅ NO Foodpanda scraping (VA handles via admin dashboard)
✅ Filters branch_urls.json to only process grab.com URLs
✅ Simple, reliable monitoring without anti-detection complexity
✅ Integrates with VA check-in data for unified 66-store view
"""
import os
import json
import time
import logging
import signal
import sys
import random
import uuid
from datetime import datetime
from typing import List, Dict, Any, Optional
from dataclasses import dataclass
from enum import Enum

import pytz
import requests
from bs4 import BeautifulSoup
import urllib3

# Disable SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# APScheduler (optional)
try:
    from apscheduler.schedulers.blocking import BlockingScheduler
    from apscheduler.triggers.cron import CronTrigger
    HAS_SCHEDULER = True
except ImportError:
    HAS_SCHEDULER = False

# Local modules
from config import config
from database import db

# Admin alerts (optional)
try:
    from admin_alerts import admin_alerts, ProblemStore
    HAS_ADMIN_ALERTS = True
except ImportError:
    HAS_ADMIN_ALERTS = False

# ------------------------------------------------------------------------------
# Logging
# ------------------------------------------------------------------------------
logging.basicConfig(
    level=getattr(logging, config.LOG_LEVEL),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# ------------------------------------------------------------------------------
# Status + Result
# ------------------------------------------------------------------------------
class StoreStatus(Enum):
    ONLINE = "online"
    OFFLINE = "offline"
    BLOCKED = "blocked"
    ERROR = "error"
    UNKNOWN = "unknown"

@dataclass
class CheckResult:
    status: StoreStatus
    response_time: int
    message: str = None
    confidence: float = 1.0

# ------------------------------------------------------------------------------
# Store Name Management
# ------------------------------------------------------------------------------
class StoreNameManager:
    """Manages proper store name extraction and caching for GrabFood stores"""
    
    def __init__(self):
        self.name_cache = {}
    
    def clean_store_name(self, name: str) -> str:
        """Clean and standardize store names"""
        if not name or name.strip() == '':
            return "Cocopan Store (Unknown)"
        
        # Remove common prefixes
        name = name.replace('Cocopan - ', '').replace('Cocopan ', '').strip()
        
        # Clean up the name
        name = name.replace('-', ' ').replace('_', ' ').title()
        
        # Remove extra whitespace
        name = ' '.join(name.split())
        
        # Add Cocopan prefix if not present
        if not name.lower().startswith('cocopan'):
            name = f"Cocopan {name}"
        
        return name
    
    def extract_store_name_from_url(self, url: str) -> str:
        """Extract proper store name from GrabFood URL with caching"""
        if url in self.name_cache:
            return self.name_cache[url]
        
        try:
            if 'grab.com' in url:
                # Pattern: /restaurant/store-name-delivery/
                import re
                match = re.search(r'/restaurant/([^/]+)', url)
                if match:
                    raw_name = match.group(1)
                    # Remove -delivery suffix if present
                    raw_name = re.sub(r'-delivery$', '', raw_name)
                    name = self.clean_store_name(raw_name)
                    self.name_cache[url] = name
                    return name
                else:
                    logger.warning(f"Could not extract name from GrabFood URL: {url}")
                    name = "Cocopan GrabFood Store"
                    self.name_cache[url] = name
                    return name
            else:
                logger.warning(f"Non-GrabFood URL: {url}")
                name = "Cocopan Store (Unknown)"
                self.name_cache[url] = name
                return name
        
        except Exception as e:
            logger.error(f"Error extracting name from {url}: {e}")
            name = f"Cocopan Store (Error)"
            self.name_cache[url] = name
            return name
    
    def get_platform_from_url(self, url: str) -> str:
        """Determine platform from URL - should only be GrabFood"""
        if 'grab.com' in url:
            return 'grabfood'
        else:
            logger.warning(f"Non-GrabFood URL detected: {url}")
            return 'unknown'

# ------------------------------------------------------------------------------
# Simple GrabFood Monitor
# ------------------------------------------------------------------------------
class GrabFoodMonitor:
    """Simple, reliable GrabFood store monitor without anti-detection complexity"""
    
    def __init__(self):
        self.store_urls = self._load_grabfood_urls()
        self.name_manager = StoreNameManager()
        self.timezone = config.get_timezone()
        self.stats = {}
        
        # User agents for basic rotation
        self.user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        ]

        logger.info(f"🛒 GrabFood Monitor initialized")
        logger.info(f"   📋 {len(self.store_urls)} GrabFood stores to monitor")
        
        if HAS_ADMIN_ALERTS:
            logger.info(f"   📧 Admin alerts enabled")

    def _load_grabfood_urls(self):
        """Load ONLY GrabFood URLs from branch_urls.json"""
        try:
            with open(config.STORE_URLS_FILE) as f:
                data = json.load(f)
                all_urls = data.get('urls', [])
                
                # Filter to only GrabFood URLs
                grabfood_urls = [url for url in all_urls if 'grab.com' in url]
                
                logger.info(f"📋 Loaded {len(all_urls)} total URLs from {config.STORE_URLS_FILE}")
                logger.info(f"🛒 Filtered to {len(grabfood_urls)} GrabFood URLs")
                logger.info(f"🐼 Skipping {len(all_urls) - len(grabfood_urls)} Foodpanda URLs (handled by VA)")
                
                return grabfood_urls
                
        except Exception as e:
            logger.error(f"Failed to load URLs: {e}")
            return []

    def check_grabfood_store(self, url: str, retry_count: int = 0) -> CheckResult:
        """Check a single GrabFood store with simple, reliable method"""
        start_time = time.time()
        max_retries = 2
        
        try:
            # Simple session with rotating user agent
            session = requests.Session()
            session.headers.update({
                'User-Agent': random.choice(self.user_agents),
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.9',
                'Accept-Encoding': 'gzip, deflate',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1',
            })
            
            try:
                resp = session.get(url, timeout=config.REQUEST_TIMEOUT, allow_redirects=True)
            except requests.exceptions.SSLError:
                # Fallback for SSL issues
                resp = session.get(url, timeout=config.REQUEST_TIMEOUT, allow_redirects=True, verify=False)

            response_time = int((time.time() - start_time) * 1000)

            # Handle HTTP errors
            if resp.status_code == 403:
                if retry_count < max_retries:
                    time.sleep(random.uniform(2, 4))
                    return self.check_grabfood_store(url, retry_count + 1)
                return CheckResult(StoreStatus.BLOCKED, response_time, "Access denied (403) after retries", 0.9)
            
            if resp.status_code == 429:
                return CheckResult(StoreStatus.BLOCKED, response_time, "Rate limited (429)", 0.9)
            
            if resp.status_code == 404:
                return CheckResult(StoreStatus.OFFLINE, response_time, "Store page not found (404)", 0.95)

            if resp.status_code == 200:
                try:
                    soup = BeautifulSoup(resp.text, 'html.parser')
                    
                    # Remove script and style elements
                    for script in soup(['script', 'style']):
                        script.decompose()
                    
                    page_text_clean = ' '.join(soup.get_text().lower().split())

                    # Check for specific "closed today" indicator
                    if 'today closed' in page_text_clean:
                        return CheckResult(StoreStatus.OFFLINE, response_time, "Store shows: today closed", 0.98)

                    # Check for offline indicators
                    offline_indicators = [
                        'restaurant is closed',
                        'currently unavailable',
                        'not accepting orders',
                        'temporarily closed',
                        'currently closed',
                        'closed for today',
                        'restaurant closed'
                    ]
                    
                    for indicator in offline_indicators:
                        if indicator in page_text_clean:
                            return CheckResult(StoreStatus.OFFLINE, response_time, f"Store closed: {indicator}", 0.95)

                    # Check for online indicators
                    online_indicators = ['order now', 'add to basket', 'delivery fee', 'menu']
                    if any(indicator in page_text_clean for indicator in online_indicators):
                        return CheckResult(StoreStatus.ONLINE, response_time, "Store page with ordering available", 0.95)

                    # General content check
                    if len(page_text_clean) > 500:
                        if any(word in page_text_clean for word in ['menu', 'order', 'delivery', 'price', 'add']):
                            return CheckResult(StoreStatus.ONLINE, response_time, "Store page loaded", 0.7)
                        else:
                            return CheckResult(StoreStatus.UNKNOWN, response_time, "Page loaded but status unclear", 0.5)
                    else:
                        return CheckResult(StoreStatus.UNKNOWN, response_time, "Minimal page content", 0.3)

                except Exception as parse_error:
                    logger.debug(f"Parse error for {url}: {parse_error}")
                    return CheckResult(StoreStatus.ONLINE, response_time, "Page loaded (parse error ignored)", 0.7)

            return CheckResult(StoreStatus.UNKNOWN, response_time, f"HTTP {resp.status_code}", 0.5)

        except requests.exceptions.Timeout:
            response_time = int((time.time() - start_time) * 1000)
            return CheckResult(StoreStatus.ERROR, response_time, "Request timeout", 0.3)
        except requests.exceptions.ConnectionError as e:
            response_time = int((time.time() - start_time) * 1000)
            return CheckResult(StoreStatus.ERROR, response_time, f"Connection error: {str(e)[:50]}", 0.3)
        except Exception as e:
            response_time = int((time.time() - start_time) * 1000)
            return CheckResult(StoreStatus.ERROR, response_time, f"Error: {str(e)[:50]}", 0.2)

    def check_all_grabfood_stores(self):
        """Check all GrabFood stores and save results"""
        
        # Logical-hour snapshot keys
        tz = self.timezone
        effective_at = datetime.now(tz).replace(minute=0, second=0, microsecond=0)
        run_id = uuid.uuid4()

        self.stats = {
            'cycle_start': datetime.now(),
            'cycle_end': None,
            'total_stores': len(self.store_urls),
            'checked': 0, 'online': 0, 'offline': 0, 'blocked': 0, 'errors': 0, 'unknown': 0
        }

        current_time = config.get_current_time()
        logger.info(f"🛒 GRABFOOD MONITORING at {current_time.strftime('%Y-%m-%d %H:%M:%S %Z')}")
        logger.info(f"📋 Checking {len(self.store_urls)} GrabFood stores only")
        logger.info(f"🐼 Foodpanda stores handled by VA check-in system")

        all_results: List[Dict[str, Any]] = []

        # Check all GrabFood stores
        for i, url in enumerate(self.store_urls, 1):
            result = self._check_single_store_safe(url, i, len(self.store_urls))
            if result:
                all_results.append(result)
            
            # Simple delay between requests
            if i < len(self.store_urls):
                time.sleep(random.uniform(1, 3))

        # Save results
        self._save_all_results(all_results, effective_at, run_id)

        # Admin alerts for blocked/error stores (optional)
        if HAS_ADMIN_ALERTS:
            self._check_and_send_admin_alerts(all_results)

        # Final stats
        self.stats['cycle_end'] = datetime.now()
        duration = (self.stats['cycle_end'] - self.stats['cycle_start']).total_seconds()
        
        logger.info("=" * 70)
        logger.info(f"✅ GRABFOOD MONITORING COMPLETED in {duration/60:.1f} minutes")
        logger.info(f"📊 Final Statistics:")
        logger.info(f"   Total GrabFood Stores: {self.stats['total_stores']}")
        logger.info(f"   ✅ Checked: {self.stats['checked']} ({self.stats['checked']/max(1,self.stats['total_stores'])*100:.1f}%)")
        logger.info(f"   🟢 Online: {self.stats['online']}")
        logger.info(f"   🔴 Offline: {self.stats['offline']}")
        logger.info(f"   🚫 Blocked: {self.stats['blocked']}")
        logger.info(f"   ⚠️ Errors: {self.stats['errors']}")
        logger.info(f"   ❓ Unknown: {self.stats['unknown']}")
        
        # Show blocked stores if any
        blocked_stores = [r for r in all_results if r['result'].status == StoreStatus.BLOCKED]
        if blocked_stores:
            logger.warning(f"🚫 {len(blocked_stores)} GrabFood stores blocked:")
            for store in blocked_stores[:5]:
                logger.warning(f"   • {store['name']}: {store['result'].message}")
            if len(blocked_stores) > 5:
                logger.warning(f"   ... and {len(blocked_stores) - 5} more")
        
        logger.info(f"🐼 Foodpanda stores ({66 - len(self.store_urls)} stores) managed by VA check-in")
        logger.info("📊 Combined data from GrabFood + VA check-in visible in client dashboard")

        return all_results

    def _check_single_store_safe(self, url: str, index: int, total: int) -> Dict[str, Any]:
        """Check single store with proper error handling"""
        store_name = self.name_manager.extract_store_name_from_url(url)
        
        try:
            logger.info(f"   [{index}/{total}] Checking {store_name}...")
            result = self.check_grabfood_store(url)

            self.stats['checked'] += 1
            self._bump_stats(result.status)

            emoji = {
                StoreStatus.ONLINE: "🟢",
                StoreStatus.OFFLINE: "🔴",
                StoreStatus.BLOCKED: "🚫",
                StoreStatus.ERROR: "⚠️",
                StoreStatus.UNKNOWN: "❓"
            }.get(result.status, "❓")
            
            logger.info(f"      {emoji} {result.status.value.upper()} ({result.response_time}ms)")
            if result.message and result.status in [StoreStatus.BLOCKED, StoreStatus.ERROR]:
                logger.debug(f"      📝 {result.message}")
                
            return {'url': url, 'name': store_name, 'result': result}
            
        except Exception as e:
            logger.error(f"   [{index}/{total}] Failed to check {store_name}: {e}")
            self.stats['checked'] += 1
            self.stats['errors'] += 1
            return {'url': url, 'name': store_name,
                    'result': CheckResult(StoreStatus.ERROR, 0, f"Check failed: {str(e)[:100]}", 0.1)}

    def _bump_stats(self, status: StoreStatus):
        """Update statistics counters"""
        if status == StoreStatus.ONLINE:
            self.stats['online'] += 1
        elif status == StoreStatus.OFFLINE:
            self.stats['offline'] += 1
        elif status == StoreStatus.BLOCKED:
            self.stats['blocked'] += 1
        elif status == StoreStatus.ERROR:
            self.stats['errors'] += 1
        elif status == StoreStatus.UNKNOWN:
            self.stats['unknown'] += 1

    def _check_and_send_admin_alerts(self, results: List[Dict[str, Any]]):
        """Send admin alerts for problematic stores"""
        try:
            problem_stores = []
            for rd in results:
                result = rd['result']
                if result.status in [StoreStatus.BLOCKED, StoreStatus.UNKNOWN, StoreStatus.ERROR]:
                    url = rd['url']
                    platform = self.name_manager.get_platform_from_url(url)
                    problem_stores.append(ProblemStore(
                        name=rd['name'], 
                        url=url, 
                        status=result.status.value.upper(),
                        message=result.message or "No details available",
                        response_time=result.response_time, 
                        platform=platform
                    ))
            
            if problem_stores:
                logger.info(f"🚨 Found {len(problem_stores)} GrabFood stores needing admin attention")
                success = admin_alerts.send_manual_verification_alert(problem_stores)
                if success:
                    logger.info("✅ Admin alert sent successfully")
                else:
                    logger.warning("⚠️ Failed to send admin alert")
                    
            # Bot detection alert for too many blocked stores
            blocked_count = sum(1 for r in results if r['result'].status == StoreStatus.BLOCKED)
            if blocked_count >= 3:
                admin_alerts.send_bot_detection_alert(blocked_count)
                
        except Exception as e:
            logger.error(f"Error with admin alerts: {e}")

    def _save_all_results(self, results: List[Dict[str, Any]], effective_at: datetime, run_id: uuid.UUID):
        """Save all results to database with hourly snapshots"""
        logger.info("💾 Saving GrabFood results to database...")

        saved_count = 0
        error_count = 0

        # Per-store hourly upsert
        for rd in results:
            try:
                store_name = rd['name']
                url = rd['url']
                result: CheckResult = rd['result']

                platform = self.name_manager.get_platform_from_url(url)
                store_id = db.get_or_create_store(store_name, url)
                evidence = result.message or ""

                # Save to hourly snapshot table
                db.upsert_store_status_hourly(
                    effective_at=effective_at,
                    platform=platform,
                    store_id=store_id,
                    status=result.status.value.upper(),
                    confidence=result.confidence,
                    response_ms=result.response_time,
                    evidence=evidence,
                    probe_time=datetime.now(self.timezone),
                    run_id=run_id,
                )
                saved_count += 1

                # Backward-compatible status check storage
                try:
                    is_online = (result.status == StoreStatus.ONLINE)
                    msg = result.message or ""
                    if result.status == StoreStatus.BLOCKED:
                        msg = f"[BLOCKED] {msg}"
                    elif result.status == StoreStatus.UNKNOWN:
                        msg = f"[UNKNOWN] {msg}"
                    elif result.status == StoreStatus.ERROR:
                        msg = f"[ERROR] {msg}"
                    elif result.status == StoreStatus.OFFLINE:
                        msg = f"[OFFLINE] {msg}"
                        
                    db.save_status_check(store_id, is_online, result.response_time, msg)
                except Exception as e:
                    logger.debug(f"(Optional) legacy save_status_check failed: {e}")

            except Exception as e:
                logger.error(f"Database error for {rd.get('name','?')}: {e}")
                error_count += 1

        # Hourly summary upsert (only for GrabFood)
        try:
            total = len(results)
            online = sum(1 for r in results if r['result'].status == StoreStatus.ONLINE)
            offline = sum(1 for r in results if r['result'].status == StoreStatus.OFFLINE)
            blocked = sum(1 for r in results if r['result'].status == StoreStatus.BLOCKED)
            errors = sum(1 for r in results if r['result'].status == StoreStatus.ERROR)
            unknown = sum(1 for r in results if r['result'].status == StoreStatus.UNKNOWN)

            # Note: This saves only GrabFood summary. VA check-in saves Foodpanda data separately
            # The client dashboard combines both data sources for unified view
            
            logger.info(f"✅ Saved {saved_count}/{len(results)} GrabFood hourly rows")
            if error_count > 0:
                logger.warning(f"   ⚠️ {error_count} database errors")

            # Backward-compatible summary report
            try:
                db.save_summary_report(total, online, offline)
            except Exception as e:
                logger.debug(f"(Optional) legacy save_summary_report failed: {e}")

        except Exception as e:
            logger.error(f"Error saving hourly summary: {e}")

def signal_handler(signum, frame):
    """Handle shutdown signals gracefully"""
    logger.info(f"🛑 Received signal {signum}, shutting down...")
    sys.exit(0)

def main():
    """Main entry point"""
    logger.info("=" * 80)
    logger.info("🛒 CocoPan GrabFood Monitor - PRODUCTION READY")
    logger.info("🎯 Target: Monitor ~24 GrabFood stores only")
    logger.info("🐼 Foodpanda: Handled by VA hourly check-in system")
    logger.info("📊 Combined: 66-store unified view in client dashboard")
    logger.info("=" * 80)

    if not config.validate_timezone():
        logger.error("❌ Timezone validation failed!")
        sys.exit(1)

    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

    try:
        monitor = GrabFoodMonitor()

        if not monitor.store_urls:
            logger.error("❌ No GrabFood URLs loaded!")
            sys.exit(1)

        # Database sanity check
        try:
            db_stats = db.get_database_stats()
            logger.info(f"✅ Database ready: {db_stats['db_type']}")
            db.ensure_schema()
            logger.info("✅ Ensured hourly snapshot schema")
        except Exception as e:
            logger.warning(f"⚠️ Database issue: {e}")

        # Scheduler
        if HAS_SCHEDULER:
            ph_tz = config.get_timezone()
            scheduler = BlockingScheduler(timezone=ph_tz)

            def job_wrapper():
                now_hour = config.get_current_time().hour
                if config.is_monitor_time(now_hour):
                    monitor.check_all_grabfood_stores()
                else:
                    logger.info(f"😴 Outside monitoring hours ({now_hour}:00)")

            scheduler.add_job(
                func=job_wrapper,
                trigger=CronTrigger(minute=0, timezone=ph_tz),
                id='hourly_grabfood_check',
                max_instances=1,
                coalesce=True,
                misfire_grace_time=900
            )

            logger.info(f"⏰ Scheduled GrabFood checks at the top of every hour ({config.MONITOR_START_HOUR}:00–{config.MONITOR_END_HOUR}:00)")
            logger.info("🔍 Running initial GrabFood check...")
            
            try:
                job_wrapper()
            except Exception as e:
                logger.error(f"Initial check error: {e}")

            logger.info("✅ GrabFood monitoring active!")
            scheduler.start()

        else:
            logger.info("⚠️ Using simple loop (no APScheduler)")
            while True:
                try:
                    now_hour = config.get_current_time().hour
                    if config.is_monitor_time(now_hour):
                        monitor.check_all_grabfood_stores()
                    else:
                        logger.info(f"😴 Outside monitoring hours ({now_hour}:00)")
                    time.sleep(3600)  # Sleep for 1 hour
                except KeyboardInterrupt:
                    break
                except Exception as e:
                    logger.error(f"Loop error: {e}")
                    time.sleep(60)

    except KeyboardInterrupt:
        logger.info("🛑 Interrupted by user")
    except Exception as e:
        logger.error(f"❌ Fatal error: {e}")
    finally:
        logger.info("👋 GrabFood monitor stopped")

if __name__ == "__main__":
    main()