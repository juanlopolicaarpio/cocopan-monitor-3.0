#!/usr/bin/env python3
"""
ANTI-DETECTION MONITOR SERVICE - PRODUCTION READY (FIXED)
✅ Exactly 66 stores monitored every hour
✅ Proper name extraction for ALL store types including foodpanda.page.link
✅ Fixed platform detection (foodpanda.page.link = foodpanda)
✅ Advanced anti-detection for Foodpanda
✅ Expected: 5-15 blocked stores (down from 35+)
✅ Simple for VAs - admin override handles blocked stores
✅ Logical-hour snapshots with idempotent DB upserts
"""
import os
import json
import time
import logging
import signal
import sys
import random
import re
import traceback
import uuid
from datetime import datetime
from typing import List, Dict, Any, Optional
from dataclasses import dataclass
from enum import Enum

import pytz
import requests
from bs4 import BeautifulSoup
import urllib3
import hashlib

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
# Store Name Cache & Utilities
# ------------------------------------------------------------------------------
class StoreNameManager:
    """Manages proper store name extraction and caching"""
    
    def __init__(self):
        self.name_cache = {}
        self.redirect_cache = {}
    
    def clean_store_name(self, name: str) -> str:
        """Clean and standardize store names"""
        if not name or name.strip() == '':
            return "Cocopan Store (Unknown)"
        
        # Remove common prefixes
        name = re.sub(r'^(Cocopan\s*-\s*|Cocopan\s+)', '', name.strip(), flags=re.IGNORECASE)
        
        # Clean up the name
        name = name.replace('-', ' ').replace('_', ' ').title()
        
        # Remove extra whitespace
        name = ' '.join(name.split())
        
        # Add Cocopan prefix if not present
        if not name.lower().startswith('cocopan'):
            name = f"Cocopan {name}"
        
        return name
    
    def resolve_foodpanda_redirect(self, redirect_url: str) -> Optional[str]:
        """Resolve foodpanda.page.link redirects with caching"""
        if redirect_url in self.redirect_cache:
            return self.redirect_cache[redirect_url]
        
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
            }
            
            # Try HEAD first (faster)
            resp = requests.head(redirect_url, allow_redirects=True, timeout=8, headers=headers)
            resolved_url = resp.url
            
            # If HEAD didn't redirect properly, try GET
            if resolved_url == redirect_url or 'page.link' in resolved_url:
                resp = requests.get(redirect_url, allow_redirects=True, timeout=8, headers=headers)
                resolved_url = resp.url
            
            # Cache the result
            if resolved_url and resolved_url != redirect_url and 'foodpanda.ph' in resolved_url:
                self.redirect_cache[redirect_url] = resolved_url
                logger.debug(f"✅ Resolved redirect: {redirect_url} -> {resolved_url}")
                return resolved_url
            else:
                # Cache failed result to avoid repeated attempts
                self.redirect_cache[redirect_url] = None
                logger.warning(f"⚠️  Failed to resolve redirect: {redirect_url}")
                return None
                
        except Exception as e:
            logger.debug(f"Error resolving redirect {redirect_url}: {e}")
            # Cache failed result
            self.redirect_cache[redirect_url] = None
            return None
    
    def extract_store_name_from_url(self, url: str) -> str:
        """Extract proper store name from URL with improved logic"""
        if url in self.name_cache:
            return self.name_cache[url]
        
        try:
            original_url = url
            
            # Handle foodpanda.page.link redirects
            if 'foodpanda.page.link' in url:
                resolved_url = self.resolve_foodpanda_redirect(url)
                if resolved_url:
                    url = resolved_url
                else:
                    # Fallback: create name from redirect URL identifier
                    url_part = original_url.split('/')[-1][:8]
                    name = f"Cocopan {url_part}"
                    self.name_cache[original_url] = name
                    return name
            
            # Handle direct foodpanda URLs
            if 'foodpanda.ph' in url:
                # Pattern: /restaurant/code/store-name
                match = re.search(r'/restaurant/[^/]+/([^/?]+)', url)
                if match:
                    raw_name = match.group(1)
                    name = self.clean_store_name(raw_name)
                    self.name_cache[original_url] = name
                    return name
                else:
                    logger.warning(f"Could not extract name from foodpanda URL: {url}")
                    name = "Cocopan Foodpanda Store"
                    self.name_cache[original_url] = name
                    return name
            
            # Handle GrabFood URLs
            elif 'grab.com' in url:
                # Pattern: /restaurant/store-name-delivery/
                match = re.search(r'/restaurant/([^/]+)', url)
                if match:
                    raw_name = match.group(1)
                    # Remove -delivery suffix if present
                    raw_name = re.sub(r'-delivery$', '', raw_name)
                    name = self.clean_store_name(raw_name)
                    self.name_cache[original_url] = name
                    return name
                else:
                    logger.warning(f"Could not extract name from GrabFood URL: {url}")
                    name = "Cocopan GrabFood Store"
                    self.name_cache[original_url] = name
                    return name
            
            else:
                logger.warning(f"Unknown URL format: {url}")
                name = "Cocopan Store (Unknown)"
                self.name_cache[original_url] = name
                return name
        
        except Exception as e:
            logger.error(f"Error extracting name from {url}: {e}")
            name = f"Cocopan Store (Error)"
            self.name_cache[original_url] = name
            return name
    
    def get_platform_from_url(self, url: str) -> str:
        """Determine platform from URL - FIXED to properly detect foodpanda.page.link"""
        if 'foodpanda' in url:  # This catches both foodpanda.ph and foodpanda.page.link
            return 'foodpanda'
        elif 'grab.com' in url:
            return 'grabfood'
        else:
            return 'unknown'

# ------------------------------------------------------------------------------
# Anti-Detection Manager (Same as before)
# ------------------------------------------------------------------------------
class AntiDetectionManager:
    """Advanced anti-detection techniques for Foodpanda"""
    def __init__(self):
        self.user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/121.0',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/120.0',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:109.0) Gecko/20100101 Firefox/121.0',
            'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/121.0',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0',
        ]
        self.accept_headers = [
            'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
            'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
        ]
        self.accept_languages = [
            'en-US,en;q=0.9',
            'en-US,en;q=0.8',
            'en-GB,en-US;q=0.9,en;q=0.8',
            'en-US,en;q=0.9,fil;q=0.8',
            'en,en-US;q=0.9',
        ]
        self.accept_encodings = [
            'gzip, deflate, br',
            'gzip, deflate',
            'gzip, deflate, br, zstd',
        ]

        self.sessions: Dict[str, requests.Session] = {}
        self.session_request_counts: Dict[str, int] = {}
        self.last_request_times: Dict[str, float] = {}

        self.last_foodpanda_main_visit = 0
        self.request_sequence_counter = 0

        logger.info("🔒 Anti-Detection Manager initialized")

    def get_random_headers(self) -> Dict[str, str]:
        base_headers = {
            'User-Agent': random.choice(self.user_agents),
            'Accept': random.choice(self.accept_headers),
            'Accept-Language': random.choice(self.accept_languages),
            'Accept-Encoding': random.choice(self.accept_encodings),
            'Cache-Control': random.choice(['no-cache', 'max-age=0', 'no-store']),
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        }
        if random.random() < 0.7:
            base_headers['DNT'] = random.choice(['1', '0'])
        if random.random() < 0.8:
            base_headers['Sec-Fetch-Dest'] = 'document'
            base_headers['Sec-Fetch-Mode'] = 'navigate'
            base_headers['Sec-Fetch-Site'] = random.choice(['none', 'same-origin', 'cross-site'])
        if random.random() < 0.6:
            base_headers['Sec-Fetch-User'] = '?1'
        if random.random() < 0.4:
            base_headers['Referer'] = 'https://www.foodpanda.ph/'
        if random.random() < 0.3:
            base_headers['Origin'] = 'https://www.foodpanda.ph'
        return base_headers

    def get_session_key(self, url: str) -> str:
        url_hash = hashlib.md5(url.encode()).hexdigest()[:8]
        time_window = int(time.time() / 600)  # 10-minute buckets
        return f"session_{url_hash}_{time_window}_{random.randint(1, 3)}"

    def get_session(self, url: str) -> requests.Session:
        session_key = self.get_session_key(url)
        if (session_key not in self.sessions or
            self.session_request_counts.get(session_key, 0) >= random.randint(5, 8)):
            # rotate
            if session_key in self.sessions:
                try: self.sessions[session_key].close()
                except: pass
            s = requests.Session()
            s.headers.update(self.get_random_headers())
            self.sessions[session_key] = s
            self.session_request_counts[session_key] = 0
            logger.debug(f"🔄 Created fresh session: {session_key}")
        s = self.sessions[session_key]
        self.session_request_counts[session_key] += 1
        if random.random() < 0.2:
            s.headers.update(self.get_random_headers())
        return s

    def request(self, method: str, url: str, **kw) -> requests.Response:
        """Make a request and record last-use so cleanup works."""
        s = self.get_session(url)
        resp = s.request(method, url, **kw)
        self.last_request_times[self.get_session_key(url)] = time.time()
        return resp

    def should_visit_main_site(self) -> bool:
        if time.time() - self.last_foodpanda_main_visit > random.randint(1800, 3600):
            return random.random() < 0.3
        return False

    def visit_main_site(self):
        try:
            logger.debug("🏠 Visiting foodpanda main site")
            self.request('GET', 'https://www.foodpanda.ph', timeout=10)
            self.last_foodpanda_main_visit = time.time()
            time.sleep(random.uniform(2, 5))
        except: pass

    def get_smart_delay(self) -> float:
        # Bounded so FP sweep fits within an hour
        base = random.uniform(8, 20)  # was 30–120
        self.request_sequence_counter += 1
        if self.request_sequence_counter % 5 == 0:
            base *= random.uniform(1.2, 1.5)
        if random.random() < 0.08:
            base *= random.uniform(1.5, 2.0)
            logger.debug(f"🐌 Taking long human-like break: {base:.1f}s")
        return base

    def cleanup_old_sessions(self):
        if len(self.sessions) <= 10: return
        now = time.time()
        remove = []
        for key in list(self.sessions.keys()):
            if now - self.last_request_times.get(key, 0) > 1800:
                remove.append(key)
        for key in remove:
            try:
                self.sessions[key].close()
            except: pass
            self.sessions.pop(key, None)
            self.session_request_counts.pop(key, None)
            self.last_request_times.pop(key, None)
        if remove:
            logger.debug(f"🧹 Cleaned up {len(remove)} old sessions")

# ------------------------------------------------------------------------------
# Monitor (Updated with fixed naming)
# ------------------------------------------------------------------------------
class StealthStoreMonitor:
    def __init__(self):
        self.store_urls = self._load_store_urls()
        self.anti_detection = AntiDetectionManager()
        self.name_manager = StoreNameManager()  # NEW: Proper name management
        self.timezone = config.get_timezone()
        self.stats = {}
        self.last_store_order: List[str] = []

        logger.info(f"🕵️ Stealth Monitor initialized")
        logger.info(f"   📋 {len(self.store_urls)} stores to monitor")
        
        # Validate we have exactly 66 stores
        if len(self.store_urls) != 66:
            logger.error(f"❌ Expected 66 stores, got {len(self.store_urls)}! Check branch_urls.json")
        else:
            logger.info(f"✅ Exactly 66 stores loaded")
        
        # Count by platform for verification
        grab_count = sum(1 for url in self.store_urls if 'grab.com' in url)
        foodpanda_direct = sum(1 for url in self.store_urls if 'foodpanda.ph' in url)
        foodpanda_redirect = sum(1 for url in self.store_urls if 'foodpanda.page.link' in url)
        
        logger.info(f"   🛒 GrabFood: {grab_count} stores")
        logger.info(f"   🐼 Foodpanda (direct): {foodpanda_direct} stores")
        logger.info(f"   🔗 Foodpanda (redirect): {foodpanda_redirect} stores")
        logger.info(f"   📊 Total: {grab_count + foodpanda_direct + foodpanda_redirect} stores")
        
        if HAS_ADMIN_ALERTS:
            logger.info(f"   📧 Admin alerts enabled")

    def _load_store_urls(self):
        """Load exactly 66 store URLs"""
        try:
            with open(config.STORE_URLS_FILE) as f:
                data = json.load(f)
                urls = data.get('urls', [])
                logger.info(f"📋 Loaded {len(urls)} store URLs from {config.STORE_URLS_FILE}")
                return urls
        except Exception as e:
            logger.error(f"Failed to load URLs: {e}")
            return []

    def randomize_store_order(self, stores: List[str]) -> List[str]:
        """Randomize store checking order"""
        attempts = 0
        while attempts < 10:
            shuffled = stores.copy()
            random.shuffle(shuffled)
            if shuffled != self.last_store_order or attempts > 5:
                self.last_store_order = shuffled.copy()
                return shuffled
            attempts += 1
        random.shuffle(stores)
        return stores

    # Store checking methods (keeping your existing logic)
    def check_store_stealth(self, url: str) -> CheckResult:
        start_time = time.time()
        try:
            if 'foodpanda' in url and self.anti_detection.should_visit_main_site():
                self.anti_detection.visit_main_site()
            response = self.anti_detection.request('GET', url, timeout=15, allow_redirects=True)
            response_time = int((time.time() - start_time) * 1000)

            if response.status_code == 403:
                return CheckResult(StoreStatus.BLOCKED, response_time, "Access denied (403) - stealth failed", 0.9)
            if response.status_code == 429:
                return CheckResult(StoreStatus.BLOCKED, response_time, "Rate limited (429)", 0.9)
            if response.status_code == 404:
                return CheckResult(StoreStatus.OFFLINE, response_time, "Store page not found (404)", 0.95)

            if response.status_code == 200:
                try:
                    soup = BeautifulSoup(response.text, 'html.parser')
                    for script in soup(['script', 'style']):
                        script.decompose()
                    page_text_clean = ' '.join(soup.get_text().lower().split())

                    # Bot guard
                    if any(x in page_text_clean for x in ['cloudflare','checking your browser','captcha','access denied','blocked']):
                        return CheckResult(StoreStatus.BLOCKED, response_time, "Bot detection triggered", 0.8)

                    # Foodpanda specific
                    if 'foodpanda.ph' in url or 'foodpanda.ph' in str(response.url) or 'foodpanda.page.link' in str(response.url):
                        offline_indicators = [
                            'restaurant is closed','currently closed','temporarily unavailable',
                            'restaurant temporarily unavailable','not accepting orders','closed for today',
                            'closed for now','out of delivery area','no longer available','delivery not available'
                        ]
                        for ind in offline_indicators:
                            if ind in page_text_clean:
                                return CheckResult(StoreStatus.OFFLINE, response_time, f"Store closed: {ind}", 0.95)
                        positives = ['add to cart','add to basket','menu','popular items','best sellers','delivery fee','min. order','order now']
                        if any(ind in page_text_clean for ind in positives):
                            return CheckResult(StoreStatus.ONLINE, response_time, "Store page with menu loaded", 0.95)

                    # Generic
                    if len(page_text_clean) > 500:
                        if any(w in page_text_clean for w in ['menu','order','delivery','price','add']):
                            return CheckResult(StoreStatus.ONLINE, response_time, "Store page loaded with content", 0.7)
                        else:
                            return CheckResult(StoreStatus.UNKNOWN, response_time, "Page loaded but status unclear", 0.5)
                    else:
                        return CheckResult(StoreStatus.UNKNOWN, response_time, "Minimal page content", 0.3)

                except Exception:
                    return CheckResult(StoreStatus.ONLINE, response_time, "Page loaded (parse error ignored)", 0.7)

            return CheckResult(StoreStatus.UNKNOWN, response_time, f"HTTP {response.status_code}", 0.5)

        except requests.exceptions.Timeout:
            response_time = int((time.time() - start_time) * 1000)
            return CheckResult(StoreStatus.ERROR, response_time, "Request timeout", 0.3)
        except requests.exceptions.ConnectionError as e:
            response_time = int((time.time() - start_time) * 1000)
            return CheckResult(StoreStatus.ERROR, response_time, f"Connection error: {str(e)[:50]}", 0.3)
        except Exception as e:
            response_time = int((time.time() - start_time) * 1000)
            logger.error(f"Unexpected error checking {url}: {e}")
            return CheckResult(StoreStatus.ERROR, response_time, f"Error: {str(e)[:50]}", 0.2)

    def check_store_simple_grabfood(self, url: str, retry_count: int = 0) -> CheckResult:
        start_time = time.time()
        max_retries = 2
        try:
            session = requests.Session()
            session.headers.update({
                'User-Agent': random.choice(self.anti_detection.user_agents[:3]),
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.9',
                'Accept-Encoding': 'gzip, deflate',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1',
            })
            try:
                resp = session.get(url, timeout=10, allow_redirects=True)
            except requests.exceptions.SSLError:
                resp = session.get(url, timeout=10, allow_redirects=True, verify=False)

            response_time = int((time.time() - start_time) * 1000)

            if resp.status_code == 403:
                if retry_count < max_retries:
                    time.sleep(random.uniform(3, 5))
                    return self.check_store_simple_grabfood(url, retry_count + 1)
                return CheckResult(StoreStatus.BLOCKED, response_time, "Access denied (403) after retries", 0.9)
            if resp.status_code == 429:
                return CheckResult(StoreStatus.BLOCKED, response_time, "Rate limited (429)", 0.9)
            if resp.status_code == 404:
                return CheckResult(StoreStatus.OFFLINE, response_time, "Store page not found (404)", 0.95)

            if resp.status_code == 200:
                try:
                    soup = BeautifulSoup(resp.text, 'html.parser')
                    for script in soup(['script', 'style']):
                        script.decompose()
                    page_text_clean = ' '.join(soup.get_text().lower().split())

                    if 'today closed' in page_text_clean:
                        return CheckResult(StoreStatus.OFFLINE, response_time, "Store shows: today closed", 0.98)

                    grab_offline = [
                        'restaurant is closed','currently unavailable','not accepting orders',
                        'temporarily closed','currently closed','closed for today','restaurant closed'
                    ]
                    for ind in grab_offline:
                        if ind in page_text_clean:
                            return CheckResult(StoreStatus.OFFLINE, response_time, f"Store closed: {ind}", 0.95)

                    if any(ind in page_text_clean for ind in ['order now','add to basket','delivery fee']):
                        return CheckResult(StoreStatus.ONLINE, response_time, "Store page with ordering available", 0.95)

                    if len(page_text_clean) > 500:
                        if any(w in page_text_clean for w in ['menu','order','delivery','price','add']):
                            return CheckResult(StoreStatus.ONLINE, response_time, "Store page loaded", 0.7)
                        else:
                            return CheckResult(StoreStatus.UNKNOWN, response_time, "Page loaded but status unclear", 0.5)
                    else:
                        return CheckResult(StoreStatus.UNKNOWN, response_time, "Minimal page content", 0.3)

                except Exception:
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

    # Master sweep method with proper store naming
    def check_all_stores_stealth(self):
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
        logger.info(f"🕵️ STEALTH CHECKING at {current_time.strftime('%Y-%m-%d %H:%M:%S %Z')}")
        logger.info(f"📋 Will check ALL {len(self.store_urls)} stores with anti-detection")

        grab_stores   = [u for u in self.store_urls if 'grab.com' in u]
        foodp_stores  = [u for u in self.store_urls if ('foodpanda.ph' in u or 'foodpanda.page.link' in u)]

        grab_stores  = self.randomize_store_order(grab_stores)
        foodp_stores = self.randomize_store_order(foodp_stores)

        logger.info(f"   🛒 GrabFood: {len(grab_stores)} stores (randomized order)")
        logger.info(f"   🐼 Foodpanda: {len(foodp_stores)} stores (stealth mode)")

        all_results: List[Dict[str, Any]] = []

        # GrabFood sweep
        if grab_stores:
            logger.info("🛒 Checking GrabFood stores (original method)...")
            for i, url in enumerate(grab_stores, 1):
                res = self._check_single_store_safe_grabfood(url, i, len(grab_stores))
                if res: all_results.append(res)
                if i < len(grab_stores):
                    time.sleep(random.uniform(1, 3))

        # Foodpanda sweep (stealth)
        if foodp_stores:
            logger.info("🐼 Checking Foodpanda stores with stealth mode...")
            logger.info("   🔒 Using randomized headers, sessions, and timing")
            for i, url in enumerate(foodp_stores, 1):
                res = self._check_single_store_safe_foodpanda(url, i, len(foodp_stores))
                if res: all_results.append(res)
                if i < len(foodp_stores):
                    delay = self.anti_detection.get_smart_delay()
                    logger.debug(f"   ⏱️  Anti-detection delay: {delay:.1f}s")
                    time.sleep(delay)

        # Cleanup sessions
        self.anti_detection.cleanup_old_sessions()

        # Save hourly (via db helpers)
        self._save_all_results(all_results, effective_at, run_id)

        # Admin alerts (optional)
        if HAS_ADMIN_ALERTS:
            self._check_and_send_admin_alerts(all_results)

        # Final stats
        self.stats['cycle_end'] = datetime.now()
        dur = (self.stats['cycle_end'] - self.stats['cycle_start']).total_seconds()
        logger.info("=" * 70)
        logger.info(f"✅ STEALTH CHECK COMPLETED in {dur/60:.1f} minutes")
        logger.info(f"📊 Final Statistics:")
        logger.info(f"   Total Stores: {self.stats['total_stores']}")
        logger.info(f"   ✅ Checked: {self.stats['checked']} ({self.stats['checked']/max(1,self.stats['total_stores'])*100:.1f}%)")
        logger.info(f"   🟢 Online: {self.stats['online']}")
        logger.info(f"   🔴 Offline: {self.stats['offline']}")
        logger.info(f"   🚫 Blocked: {self.stats['blocked']}")
        logger.info(f"   ⚠️ Errors: {self.stats['errors']}")
        logger.info(f"   ❓ Unknown: {self.stats['unknown']}")

        blocked_stores = [r for r in all_results if r['result'].status == StoreStatus.BLOCKED]
        if len(blocked_stores) <= 15:
            if len(blocked_stores) <= 5:
                logger.info("🎉 EXCELLENT: ≤5 blocked stores - stealth working perfectly!")
            else:
                logger.info(f"✅ SUCCESS: {len(blocked_stores)} blocked stores - within target range")
        else:
            logger.warning(f"⚠️ Above target: {len(blocked_stores)} blocked stores - may need stronger anti-detection")

        if blocked_stores:
            logger.warning("🚫 Blocked stores for admin override:")
            for store in blocked_stores[:10]:
                logger.warning(f"   • {store['name']}: {store['result'].message}")
            if len(blocked_stores) > 10:
                logger.warning(f"   ... and {len(blocked_stores) - 10} more")

        return all_results

    # Helper methods with proper naming
    def _check_single_store_safe_grabfood(self, url: str, index: int, total: int) -> Dict[str, Any]:
        # Use proper name extraction
        store_name = self.name_manager.extract_store_name_from_url(url)
        try:
            logger.info(f"   [{index}/{total}] Checking {store_name}...")
            result = self.check_store_simple_grabfood(url)

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
            return {'url': url, 'name': store_name, 'result': result}
        except Exception as e:
            logger.error(f"   [{index}/{total}] Failed to check {store_name}: {e}")
            self.stats['checked'] += 1
            self.stats['errors'] += 1
            return {'url': url, 'name': store_name,
                    'result': CheckResult(StoreStatus.ERROR, 0, f"Check failed: {str(e)[:100]}", 0.1)}

    def _check_single_store_safe_foodpanda(self, url: str, index: int, total: int) -> Dict[str, Any]:
        # Use proper name extraction
        store_name = self.name_manager.extract_store_name_from_url(url)
        try:
            logger.info(f"   [{index}/{total}] Checking {store_name}...")
            result = self.check_store_stealth(url)

            self.stats['checked'] += 1
            self._bump_stats(result.status)

            emoji = {
                StoreStatus.ONLINE: "🟢",
                StoreStatus.OFFLINE: "🔴",
                StoreStatus.BLOCKED: "🚫",
                StoreStatus.ERROR: "⚠️",
                StoreStatus.UNKNOWN: "❓"
            }.get(result.status, "❓")
            logger.info(f"      {emoji} {result.status.value.upper()} ({result.response_time}ms) 🔒")
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
        if status == StoreStatus.ONLINE: self.stats['online'] += 1
        elif status == StoreStatus.OFFLINE: self.stats['offline'] += 1
        elif status == StoreStatus.BLOCKED: self.stats['blocked'] += 1
        elif status == StoreStatus.ERROR: self.stats['errors'] += 1
        elif status == StoreStatus.UNKNOWN: self.stats['unknown'] += 1

    def _check_and_send_admin_alerts(self, results: List[Dict[str, Any]]):
        try:
            problem_stores = []
            for rd in results:
                result = rd['result']
                if result.status in [StoreStatus.BLOCKED, StoreStatus.UNKNOWN, StoreStatus.ERROR]:
                    url = rd['url']
                    platform = self.name_manager.get_platform_from_url(url)
                    problem_stores.append(ProblemStore(
                        name=rd['name'], url=url, status=result.status.value.upper(),
                        message=result.message or "No details available",
                        response_time=result.response_time, platform=platform
                    ))
            if problem_stores:
                logger.info(f"🚨 Found {len(problem_stores)} stores needing admin attention")
                ok = admin_alerts.send_manual_verification_alert(problem_stores)
                if ok: logger.info("✅ Admin alert sent successfully")
                else: logger.warning("⚠️ Failed to send admin alert")
            blocked_count = sum(1 for r in results if r['result'].status == StoreStatus.BLOCKED)
            if blocked_count >= 3:
                admin_alerts.send_bot_detection_alert(blocked_count)
        except Exception as e:
            logger.error(f"Error with admin alerts: {e}")

    def _save_all_results(self, results: List[Dict[str, Any]], effective_at: datetime, run_id: uuid.UUID):
        logger.info("💾 Saving results to database (hourly upserts via db)...")

        saved_count = 0
        error_count = 0

        # Per-store hourly upsert
        for rd in results:
            try:
                store_name = rd['name']
                url = rd['url']
                result: CheckResult = rd['result']

                # Use proper platform detection
                platform = self.name_manager.get_platform_from_url(url)
                
                store_id = db.get_or_create_store(store_name, url)
                evidence = result.message or ""

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

                # Backward-compatible raw check storage (optional)
                try:
                    is_online = (result.status == StoreStatus.ONLINE)
                    msg = result.message or ""
                    if result.status == StoreStatus.BLOCKED: msg = f"[BLOCKED] {msg}"
                    elif result.status == StoreStatus.UNKNOWN: msg = f"[UNKNOWN] {msg}"
                    elif result.status == StoreStatus.ERROR: msg = f"[ERROR] {msg}"
                    elif result.status == StoreStatus.OFFLINE: msg = f"[OFFLINE] {msg}"
                    db.save_status_check(store_id, is_online, result.response_time, msg)
                except Exception as e:
                    logger.debug(f"(Optional) legacy save_status_check failed: {e}")

            except Exception as e:
                logger.error(f"Database error for {rd.get('name','?')}: {e}")
                error_count += 1

        # Hourly summary upsert
        try:
            total   = len(results)
            online  = sum(1 for r in results if r['result'].status == StoreStatus.ONLINE)
            offline = sum(1 for r in results if r['result'].status == StoreStatus.OFFLINE)
            blocked = sum(1 for r in results if r['result'].status == StoreStatus.BLOCKED)
            errors  = sum(1 for r in results if r['result'].status == StoreStatus.ERROR)
            unknown = sum(1 for r in results if r['result'].status == StoreStatus.UNKNOWN)

            db.upsert_status_summary_hourly(
                effective_at=effective_at,
                total=total, online=online, offline=offline,
                blocked=blocked, errors=errors, unknown=unknown,
                last_probe_at=datetime.now(self.timezone),
            )

            logger.info(f"✅ Saved {saved_count}/{len(results)} hourly rows")
            if error_count > 0:
                logger.warning(f"   ⚠️ {error_count} database errors")

            # Backward-compatible summary (optional)
            try:
                db.save_summary_report(total, online, offline)
            except Exception as e:
                logger.debug(f"(Optional) legacy save_summary_report failed: {e}")

        except Exception as e:
            logger.error(f"Error saving hourly summary: {e}")

# Signal handling and main (same as before)
def signal_handler(signum, frame):
    logger.info(f"🛑 Received signal {signum}, shutting down...")
    try:
        pass
    finally:
        sys.exit(0)

def main():
    logger.info("=" * 80)
    logger.info("🕵️ CocoPan Anti-Detection Monitor - PRODUCTION READY (FIXED)")
    logger.info("🎯 Target: Exactly 66 stores checked, ≤15 stealth-blocked")
    logger.info("🔧 Fixed: Proper naming for all store types including redirects")
    logger.info("🔒 Advanced anti-detection: Headers, sessions, timing, behavior")
    logger.info("=" * 80)

    if not config.validate_timezone():
        logger.error("❌ Timezone validation failed!")
        sys.exit(1)

    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

    try:
        monitor = StealthStoreMonitor()

        if not monitor.store_urls:
            logger.error("❌ No store URLs loaded!")
            sys.exit(1)
        
        if len(monitor.store_urls) != 66:
            logger.error(f"❌ Expected exactly 66 stores, got {len(monitor.store_urls)}!")
            logger.error("Run the database cleanup script first!")
            sys.exit(1)

        # DB sanity + ensure hourly schema (idempotent)
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
                    monitor.check_all_stores_stealth()
                else:
                    logger.info("😴 Outside monitoring hours")

            scheduler.add_job(
                func=job_wrapper,
                trigger=CronTrigger(minute=0, timezone=ph_tz),
                id='hourly_sweep',
                max_instances=2,
                coalesce=False,
                misfire_grace_time=900
            )

            logger.info(f"⏰ Scheduled stealth checks at the top of every hour ({config.MONITOR_START_HOUR}:00–{config.MONITOR_END_HOUR}:00)")
            logger.info("🔍 Running initial stealth check...")
            try:
                job_wrapper()
            except Exception as e:
                logger.error(f"Initial check error: {e}")
                logger.error(traceback.format_exc())

            logger.info("✅ Stealth monitoring active!")
            scheduler.start()

        else:
            logger.info("⚠️ Using simple loop")
            while True:
                try:
                    now_hour = config.get_current_time().hour
                    if config.is_monitor_time(now_hour):
                        monitor.check_all_stores_stealth()
                    else:
                        logger.info("😴 Outside monitoring hours")
                    time.sleep(3600)
                except KeyboardInterrupt:
                    break
                except Exception as e:
                    logger.error(f"Loop error: {e}")
                    time.sleep(60)

    except KeyboardInterrupt:
        logger.info("🛑 Interrupted by user")
    except Exception as e:
        logger.error(f"❌ Fatal error: {e}")
        logger.error(traceback.format_exc())
    finally:
        logger.info("👋 Stealth monitor stopped")

if __name__ == "__main__":
    main()