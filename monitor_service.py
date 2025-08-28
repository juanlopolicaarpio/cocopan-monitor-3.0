#!/usr/bin/env python3
"""
UPDATED CocoPan Monitor Service with Admin Alerts
Added admin notifications for stores needing manual verification
"""
import os
import json
import time
import logging
import signal
import sys
import random
import re
import subprocess
import traceback
from datetime import datetime, timedelta
from typing import List, Tuple, Dict, Any, Optional
from dataclasses import dataclass
from enum import Enum
import pytz
import requests
from bs4 import BeautifulSoup
import urllib3

# Disable SSL warnings properly
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

try:
    from apscheduler.schedulers.blocking import BlockingScheduler
    from apscheduler.triggers.cron import CronTrigger
    HAS_SCHEDULER = True
except ImportError:
    HAS_SCHEDULER = False
    
from config import config
from database import db

# ADDED: Import admin alerts
from admin_alerts import admin_alerts, ProblemStore

logging.basicConfig(
    level=getattr(logging, config.LOG_LEVEL),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class StoreStatus(Enum):
    """Store status enum"""
    ONLINE = "online"
    OFFLINE = "offline"
    BLOCKED = "blocked"
    ERROR = "error"
    UNKNOWN = "unknown"

@dataclass
class CheckResult:
    """Result from a store check"""
    status: StoreStatus
    response_time: int
    message: str = None
    confidence: float = 1.0

class RateLimiter:
    """Smart rate limiting to avoid detection"""
    def __init__(self):
        self.last_request_time = {}
        self.request_counts = {}
        
        # Platform-specific delays
        self.min_delays = {
            'foodpanda.ph': 2.0,  # 2 seconds between Foodpanda requests
            'grab.com': 1.0,      # 1 second between Grab requests
            'default': 1.0
        }
        
        # Max requests per minute
        self.max_rpm = {
            'foodpanda.ph': 20,  # Increased to 20 to ensure all stores are checked
            'grab.com': 30,      # 30 for Grab
            'default': 25
        }
    
    def wait_if_needed(self, url: str):
        """Wait if necessary to avoid rate limiting"""
        domain = self._get_domain(url)
        
        # Enforce minimum delay
        min_delay = self.min_delays.get(domain, self.min_delays['default'])
        if domain in self.last_request_time:
            elapsed = time.time() - self.last_request_time[domain]
            if elapsed < min_delay:
                wait_time = min_delay - elapsed + random.uniform(0.2, 0.8)
                time.sleep(wait_time)
        
        # Check RPM
        current_minute = datetime.now().replace(second=0, microsecond=0)
        if domain not in self.request_counts:
            self.request_counts[domain] = {}
            
        # Clean old entries
        old_minutes = [m for m in self.request_counts[domain] if m < current_minute - timedelta(minutes=1)]
        for old_minute in old_minutes:
            del self.request_counts[domain][old_minute]
        
        # Count current minute
        current_count = self.request_counts[domain].get(current_minute, 0)
        max_rpm = self.max_rpm.get(domain, self.max_rpm['default'])
        
        if current_count >= max_rpm:
            wait_time = 60 - datetime.now().second + random.uniform(1, 3)
            logger.debug(f"Rate limit reached for {domain}, waiting {wait_time:.1f}s")
            time.sleep(wait_time)
            current_minute = datetime.now().replace(second=0, microsecond=0)
            self.request_counts[domain][current_minute] = 1
        else:
            self.request_counts[domain][current_minute] = current_count + 1
        
        self.last_request_time[domain] = time.time()
    
    def _get_domain(self, url: str) -> str:
        """Extract domain from URL"""
        if 'foodpanda.ph' in url:
            return 'foodpanda.ph'
        elif 'grab.com' in url:
            return 'grab.com'
        return 'default'

class SimpleStoreMonitor:
    """Simple monitor without threading issues"""
    
    def __init__(self):
        self.store_urls = self._load_store_urls()
        self.rate_limiter = RateLimiter()
        self.timezone = config.get_timezone()
        
        # User agents
        self.user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/119.0.0.0',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/120.0.0.0',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Firefox/121.0',
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/120.0.0.0',
        ]
        
        # Statistics
        self.stats = {}
        self.store_names = {}
        
        # Session management for better performance
        self.sessions = {}
        
        logger.info(f"🚀 Simple Monitor initialized")
        logger.info(f"   📋 {len(self.store_urls)} stores to monitor")
        logger.info(f"   ✅ No threading issues - simple sequential checking")
    
    def _load_store_urls(self):
        """Load store URLs from file"""
        try:
            with open(config.STORE_URLS_FILE) as f:
                data = json.load(f)
                urls = data.get('urls', [])
                return urls
        except Exception as e:
            logger.error(f"Failed to load URLs: {e}")
            return []
    
    def _extract_store_name(self, url: str) -> str:
        """IMPROVED: Extract and cache store name with better handling for redirect URLs"""
        if url in self.store_names:
            return self.store_names[url]
        
        try:
            # Handle foodpanda redirect URLs by following them first
            if 'foodpanda.page.link' in url:
                resolved_url = self._resolve_redirect_url(url)
                if resolved_url and resolved_url != url:
                    return self._extract_store_name_from_resolved_url(resolved_url, url)
            
            # Regular URL parsing
            if 'grab.com' in url:
                match = re.search(r'/restaurant/([^/]+)/', url)
                name = match.group(1) if match else "Store"
            elif 'foodpanda.ph' in url:
                match = re.search(r'/restaurant/[^/]+/([^/?]+)', url)
                name = match.group(1) if match else "Store"
            else:
                name = "Store"
                
            name = name.replace('-', ' ').title()
            if not name.lower().startswith('cocopan'):
                name = f"Cocopan {name}"
                
            self.store_names[url] = name
            return name
            
        except Exception as e:
            logger.debug(f"Error extracting store name from {url}: {e}")
            fallback_name = f"Cocopan Store ({len(self.store_names) + 1})"
            self.store_names[url] = fallback_name
            return fallback_name
    
    def _resolve_redirect_url(self, redirect_url: str) -> Optional[str]:
        """ADDED: Resolve foodpanda redirect URLs to get actual store URLs"""
        try:
            session = requests.Session()
            session.headers.update({'User-Agent': random.choice(self.user_agents)})
            
            # Follow redirects but don't load the full page
            response = session.head(redirect_url, allow_redirects=True, timeout=10)
            resolved_url = response.url
            
            logger.debug(f"Resolved {redirect_url} -> {resolved_url}")
            return resolved_url
            
        except Exception as e:
            logger.debug(f"Failed to resolve redirect {redirect_url}: {e}")
            return None
    
    def _extract_store_name_from_resolved_url(self, resolved_url: str, original_url: str) -> str:
        """ADDED: Extract store name from resolved foodpanda URL"""
        try:
            if 'foodpanda.ph' in resolved_url:
                # Try to extract from resolved URL
                match = re.search(r'/restaurant/[^/]+/([^/?]+)', resolved_url)
                if match:
                    name = match.group(1).replace('-', ' ').title()
                    if not name.lower().startswith('cocopan'):
                        name = f"Cocopan {name}"
                    self.store_names[original_url] = name
                    return name
            
            # Fallback to generic name based on URL index
            fallback_name = f"Cocopan Store {len(self.store_names) + 1}"
            self.store_names[original_url] = fallback_name
            return fallback_name
            
        except Exception as e:
            logger.debug(f"Error extracting name from resolved URL: {e}")
            fallback_name = f"Cocopan Store {len(self.store_names) + 1}"
            self.store_names[original_url] = fallback_name
            return fallback_name
    
    def _get_session(self, domain: str):
        """Get or create session for domain"""
        if domain not in self.sessions:
            session = requests.Session()
            session.headers.update({
                'User-Agent': random.choice(self.user_agents),
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.9',
                'Accept-Encoding': 'gzip, deflate',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1',
            })
            
            # Add specific headers for Foodpanda
            if domain == 'foodpanda.ph':
                session.headers.update({
                    'Referer': 'https://www.foodpanda.ph/',
                    'Origin': 'https://www.foodpanda.ph',
                })
            
            self.sessions[domain] = session
        
        return self.sessions[domain]
    
    def check_store_simple(self, url: str, retry_count: int = 0) -> CheckResult:
        """Check a single store with simple requests only"""
        start_time = time.time()
        max_retries = 2
        
        try:
            # Apply rate limiting
            self.rate_limiter.wait_if_needed(url)
            
            # Get domain and session
            domain = 'foodpanda.ph' if 'foodpanda.ph' in url else 'grab.com'
            session = self._get_session(domain)
            
            # Update user agent occasionally
            if random.random() < 0.1:  # 10% chance
                session.headers['User-Agent'] = random.choice(self.user_agents)
            
            # Make request with timeout (with SSL verification)
            try:
                resp = session.get(url, timeout=10, allow_redirects=True)
            except requests.exceptions.SSLError:
                # Retry without SSL verification if needed
                resp = session.get(url, timeout=10, allow_redirects=True, verify=False)
            
            response_time = int((time.time() - start_time) * 1000)
            
            # Check status code
            if resp.status_code == 403:
                # For 403, try with fresh session and different user agent
                if retry_count < max_retries:
                    logger.debug(f"Got 403 for {url}, retrying with fresh session...")
                    # Clear session
                    if domain in self.sessions:
                        del self.sessions[domain]
                    time.sleep(random.uniform(3, 5))
                    return self.check_store_simple(url, retry_count + 1)
                
                return CheckResult(
                    status=StoreStatus.BLOCKED,
                    response_time=response_time,
                    message="Access denied (403) after retries",
                    confidence=0.9
                )
            
            if resp.status_code == 429:
                return CheckResult(
                    status=StoreStatus.BLOCKED,
                    response_time=response_time,
                    message="Rate limited (429)",
                    confidence=0.9
                )
            
            if resp.status_code == 404:
                return CheckResult(
                    status=StoreStatus.OFFLINE,
                    response_time=response_time,
                    message="Store page not found (404)",
                    confidence=0.95
                )
            
            # Parse content for 200 responses
            if resp.status_code == 200:
                try:
                    soup = BeautifulSoup(resp.text, 'html.parser')
                    
                    # CRITICAL: Remove scripts and styles FIRST (like original)
                    for script in soup(['script', 'style']):
                        script.decompose()
                    
                    # Get clean visible text only
                    page_text = soup.get_text().lower()
                    page_text_clean = ' '.join(page_text.split())  # Clean whitespace
                    
                    # Check for bot detection
                    if any(indicator in page_text_clean for indicator in ['cloudflare', 'checking your browser', 'captcha']):
                        return CheckResult(
                            status=StoreStatus.BLOCKED,
                            response_time=response_time,
                            message="Bot detection triggered",
                            confidence=0.8
                        )
                    
                    # PLATFORM-SPECIFIC DETECTION
                    if 'foodpanda.ph' in url or 'foodpanda.page.link' in str(resp.url):
                        # Foodpanda specific checks
                        offline_indicators = [
                            'restaurant is closed',
                            'currently closed',
                            'temporarily unavailable',
                            'restaurant temporarily unavailable',
                            'not accepting orders',
                            'closed for today',
                            'closed for now',
                            'out of delivery area',
                            'no longer available',
                            'delivery not available'
                        ]
                        
                        for indicator in offline_indicators:
                            if indicator in page_text_clean:
                                return CheckResult(
                                    status=StoreStatus.OFFLINE,
                                    response_time=response_time,
                                    message=f"Store closed: {indicator}",
                                    confidence=0.95
                                )
                        
                        # Check for positive indicators
                        positive_indicators = [
                            'add to cart',
                            'add to basket',
                            'menu',
                            'popular items',
                            'best sellers',
                            'delivery fee'
                        ]
                        
                        if any(indicator in page_text_clean for indicator in positive_indicators):
                            return CheckResult(
                                status=StoreStatus.ONLINE,
                                response_time=response_time,
                                message="Store page with menu loaded",
                                confidence=0.95
                            )
                    
                    elif 'grab.com' in url:
                        # GrabFood specific checks - RESTORED ORIGINAL LOGIC
                        
                        # CRITICAL FIX for "Today Closed" detection
                        if 'today closed' in page_text_clean:
                            return CheckResult(
                                status=StoreStatus.OFFLINE,
                                response_time=response_time,
                                message="Store shows: today closed",
                                confidence=0.98
                            )
                        
                        # Check for other closed indicators
                        grab_offline_indicators = [
                            'restaurant is closed',
                            'currently unavailable',
                            'not accepting orders',
                            'temporarily closed',
                            'currently closed',
                            'closed for today',
                            'restaurant closed'
                        ]
                        
                        for indicator in grab_offline_indicators:
                            if indicator in page_text_clean:
                                return CheckResult(
                                    status=StoreStatus.OFFLINE,
                                    response_time=response_time,
                                    message=f"Store closed: {indicator}",
                                    confidence=0.95
                                )
                        
                        # Also check specific HTML elements for Grab
                        closed_elements = soup.find_all(['div', 'span'], 
                                                       class_=lambda x: x and 'closed' in str(x).lower() if x else False)
                        
                        for element in closed_elements:
                            if element and element.get_text(strip=True).lower() in ['closed', 'today closed']:
                                return CheckResult(
                                    status=StoreStatus.OFFLINE,
                                    response_time=response_time,
                                    message="Found closed status element",
                                    confidence=0.95
                                )
                        
                        # Check for positive indicators
                        if any(indicator in page_text_clean for indicator in ['order now', 'add to basket', 'delivery fee']):
                            return CheckResult(
                                status=StoreStatus.ONLINE,
                                response_time=response_time,
                                message="Store page with ordering available",
                                confidence=0.95
                            )
                    
                    # For any platform - only mark as online if we have strong evidence
                    if len(page_text_clean) > 500:  # Reasonable page content
                        # Look for any menu/ordering indicators
                        if any(word in page_text_clean for word in ['menu', 'order', 'delivery', 'price', 'add']):
                            return CheckResult(
                                status=StoreStatus.ONLINE,
                                response_time=response_time,
                                message="Store page loaded",
                                confidence=0.7
                            )
                        else:
                            # Page loaded but no clear indicators
                            return CheckResult(
                                status=StoreStatus.UNKNOWN,
                                response_time=response_time,
                                message="Page loaded but status unclear",
                                confidence=0.5
                            )
                    else:
                        # Very short page content - suspicious
                        return CheckResult(
                            status=StoreStatus.UNKNOWN,
                            response_time=response_time,
                            message="Minimal page content",
                            confidence=0.3
                        )
                    
                except Exception as e:
                    # If parsing fails, but we got 200, assume online
                    return CheckResult(
                        status=StoreStatus.ONLINE,
                        response_time=response_time,
                        message="Page loaded (parse error ignored)",
                        confidence=0.7
                    )
            
            # Other status codes
            return CheckResult(
                status=StoreStatus.UNKNOWN,
                response_time=response_time,
                message=f"HTTP {resp.status_code}",
                confidence=0.5
            )
            
        except requests.exceptions.Timeout:
            response_time = int((time.time() - start_time) * 1000)
            return CheckResult(
                status=StoreStatus.ERROR,
                response_time=response_time,
                message="Request timeout",
                confidence=0.3
            )
            
        except requests.exceptions.ConnectionError as e:
            response_time = int((time.time() - start_time) * 1000)
            return CheckResult(
                status=StoreStatus.ERROR,
                response_time=response_time,
                message=f"Connection error: {str(e)[:50]}",
                confidence=0.3
            )
            
        except Exception as e:
            response_time = int((time.time() - start_time) * 1000)
            logger.error(f"Unexpected error checking {url}: {e}")
            return CheckResult(
                status=StoreStatus.ERROR,
                response_time=response_time,
                message=f"Error: {str(e)[:50]}",
                confidence=0.2
            )
    
    def check_all_stores_guaranteed(self):
        """Check ALL stores - guaranteed completion with ADMIN ALERTS"""
        self.stats = {
            'cycle_start': datetime.now(),
            'cycle_end': None,
            'total_stores': len(self.store_urls),
            'checked': 0,
            'online': 0,
            'offline': 0,
            'blocked': 0,
            'errors': 0,
            'unknown': 0
        }
        
        current_time = config.get_current_time()
        logger.info(f"🚀 CHECKING ALL STORES at {current_time.strftime('%Y-%m-%d %H:%M:%S %Z')}")
        logger.info(f"📋 Will check ALL {len(self.store_urls)} stores sequentially")
        
        # Group by platform
        foodpanda_stores = [url for url in self.store_urls if 'foodpanda.ph' in url or 'foodpanda.page.link' in url]
        grab_stores = [url for url in self.store_urls if 'grab.com' in url]
        other_stores = [url for url in self.store_urls if url not in foodpanda_stores and url not in grab_stores]
        
        logger.info(f"   🐼 Foodpanda: {len(foodpanda_stores)} stores")
        logger.info(f"   🛒 GrabFood: {len(grab_stores)} stores")
        if other_stores:
            logger.info(f"   ❓ Other: {len(other_stores)} stores")
        
        all_results = []
        
        # Check Foodpanda stores
        if foodpanda_stores:
            logger.info("🐼 Checking Foodpanda stores...")
            for i, url in enumerate(foodpanda_stores, 1):
                result = self._check_single_store_safe(url, i, len(foodpanda_stores), "Foodpanda")
                all_results.append(result)
                
                # Small delay every 5 stores
                if i % 5 == 0 and i < len(foodpanda_stores):
                    delay = random.uniform(2, 4)
                    logger.debug(f"   Brief pause ({delay:.1f}s) after {i} stores...")
                    time.sleep(delay)
        
        # Check GrabFood stores
        if grab_stores:
            logger.info("🛒 Checking GrabFood stores...")
            for i, url in enumerate(grab_stores, 1):
                result = self._check_single_store_safe(url, i, len(grab_stores), "GrabFood")
                all_results.append(result)
                
                # Smaller delay for Grab
                if i % 10 == 0 and i < len(grab_stores):
                    delay = random.uniform(1, 2)
                    logger.debug(f"   Brief pause ({delay:.1f}s) after {i} stores...")
                    time.sleep(delay)
        
        # Check other stores
        if other_stores:
            logger.info("❓ Checking other stores...")
            for i, url in enumerate(other_stores, 1):
                result = self._check_single_store_safe(url, i, len(other_stores), "Other")
                all_results.append(result)
        
        # Save all results
        self._save_all_results(all_results)
        
        # ADDED: Check for problematic stores and send admin alerts
        self._check_and_send_admin_alerts(all_results)
        
        # Final statistics
        self.stats['cycle_end'] = datetime.now()
        cycle_duration = (self.stats['cycle_end'] - self.stats['cycle_start']).total_seconds()
        
        logger.info("=" * 60)
        logger.info(f"✅ CHECK COMPLETED in {cycle_duration:.1f} seconds")
        logger.info(f"📊 Final Statistics:")
        logger.info(f"   Total Stores: {self.stats['total_stores']}")
        logger.info(f"   ✅ Checked: {self.stats['checked']} ({self.stats['checked']/self.stats['total_stores']*100:.1f}%)")
        logger.info(f"   🟢 Online: {self.stats['online']} ({self.stats['online']/self.stats['total_stores']*100:.1f}%)")
        logger.info(f"   🔴 Offline: {self.stats['offline']} ({self.stats['offline']/self.stats['total_stores']*100:.1f}%)")
        logger.info(f"   🚫 Blocked: {self.stats['blocked']} ({self.stats['blocked']/self.stats['total_stores']*100:.1f}%)")
        logger.info(f"   ⚠️ Errors: {self.stats['errors']} ({self.stats['errors']/self.stats['total_stores']*100:.1f}%)")
        logger.info(f"   ❓ Unknown: {self.stats['unknown']} ({self.stats['unknown']/self.stats['total_stores']*100:.1f}%)")
        
        # List problem stores
        problem_stores = [r for r in all_results if r['result'].status in [StoreStatus.OFFLINE, StoreStatus.BLOCKED, StoreStatus.ERROR]]
        if problem_stores:
            logger.warning(f"⚠️ Problem stores ({len(problem_stores)}):")
            for store in problem_stores[:10]:  # Show first 10
                logger.warning(f"   • {store['name']}: {store['result'].status.value} - {store['result'].message}")
            if len(problem_stores) > 10:
                logger.warning(f"   ... and {len(problem_stores) - 10} more")
        
        # Clear sessions periodically
        self.sessions.clear()
        
        return all_results
    
    def _check_single_store_safe(self, url: str, index: int, total: int, platform: str) -> Dict:
        """Safely check a single store with error handling"""
        store_name = self._extract_store_name(url)
        
        try:
            logger.info(f"   [{index}/{total}] Checking {store_name}...")
            
            # Check the store
            result = self.check_store_simple(url)
            
            # Update statistics
            self.stats['checked'] += 1
            if result.status == StoreStatus.ONLINE:
                self.stats['online'] += 1
            elif result.status == StoreStatus.OFFLINE:
                self.stats['offline'] += 1
            elif result.status == StoreStatus.BLOCKED:
                self.stats['blocked'] += 1
            elif result.status == StoreStatus.ERROR:
                self.stats['errors'] += 1
            elif result.status == StoreStatus.UNKNOWN:
                self.stats['unknown'] += 1
            
            # Log result
            status_emoji = {
                StoreStatus.ONLINE: "🟢",
                StoreStatus.OFFLINE: "🔴",
                StoreStatus.BLOCKED: "🚫",
                StoreStatus.ERROR: "⚠️",
                StoreStatus.UNKNOWN: "❓"
            }
            
            emoji = status_emoji.get(result.status, "❓")
            logger.info(f"      {emoji} {result.status.value.upper()} ({result.response_time}ms)")
            if result.message:
                logger.debug(f"      📝 {result.message}")
            
            return {
                'url': url,
                'name': store_name,
                'result': result
            }
            
        except Exception as e:
            # Never let an error stop the checking process
            logger.error(f"   [{index}/{total}] Failed to check {store_name}: {e}")
            self.stats['checked'] += 1
            self.stats['errors'] += 1
            
            return {
                'url': url,
                'name': store_name,
                'result': CheckResult(
                    status=StoreStatus.ERROR,
                    response_time=0,
                    message=f"Check failed: {str(e)[:100]}",
                    confidence=0.1
                )
            }
    
    def _check_and_send_admin_alerts(self, results: List[Dict]):
        """ADDED: Check for problematic stores and send admin alerts"""
        try:
            # Find stores that need manual verification
            problem_stores = []
            
            for result_dict in results:
                result = result_dict['result']
                if result.status in [StoreStatus.BLOCKED, StoreStatus.UNKNOWN, StoreStatus.ERROR]:
                    
                    # Determine platform
                    url = result_dict['url']
                    if 'foodpanda' in url:
                        platform = 'foodpanda'
                    elif 'grab.com' in url:
                        platform = 'grabfood' 
                    else:
                        platform = 'unknown'
                    
                    problem_store = ProblemStore(
                        name=result_dict['name'],
                        url=url,
                        status=result.status.value.upper(),
                        message=result.message or "No details available",
                        response_time=result.response_time,
                        platform=platform
                    )
                    problem_stores.append(problem_store)
            
            if problem_stores:
                logger.info(f"🚨 Found {len(problem_stores)} stores needing admin attention")
                
                # Send admin alert
                success = admin_alerts.send_manual_verification_alert(problem_stores)
                if success:
                    logger.info("✅ Admin alert sent successfully")
                else:
                    logger.warning("⚠️ Failed to send admin alert")
            else:
                logger.info("✅ All stores checked successfully - no admin alerts needed")
                
            # Check for bot detection spike
            blocked_count = sum(1 for r in results if r['result'].status == StoreStatus.BLOCKED)
            if blocked_count >= 3:
                admin_alerts.send_bot_detection_alert(blocked_count)
                
            # Check system health 
            cycle_duration = (self.stats['cycle_end'] - self.stats['cycle_start']).total_seconds()
            database_errors = 0  # Track this from _save_all_results if needed
            
            if cycle_duration > 600 or database_errors > 5:  # 10 minutes or 5+ db errors
                admin_alerts.send_system_health_alert(cycle_duration, database_errors)
                
        except Exception as e:
            logger.error(f"Error checking admin alerts: {e}")
    
    def _save_all_results(self, results: List[Dict]):
        """Save all results to database"""
        logger.info("💾 Saving results to database...")
        
        saved_count = 0
        error_count = 0
        
        for result_dict in results:
            try:
                store_name = result_dict['name']
                url = result_dict['url']
                result = result_dict['result']
                
                store_id = db.get_or_create_store(store_name, url)
                
                # Convert to database format
                # IMPORTANT: Blocked/Unknown/Error = Online to avoid false alerts
                is_online = result.status not in [StoreStatus.OFFLINE]
                
                # Add status prefix to message
                message = result.message or ""
                if result.status == StoreStatus.BLOCKED:
                    message = f"[BLOCKED] {message}"
                elif result.status == StoreStatus.UNKNOWN:
                    message = f"[UNKNOWN] {message}"
                elif result.status == StoreStatus.ERROR:
                    message = f"[ERROR] {message}"
                
                db.save_status_check(store_id, is_online, result.response_time, message)
                saved_count += 1
                
            except Exception as e:
                logger.error(f"Database error for {result_dict['name']}: {e}")
                error_count += 1
        
        # Save summary
        try:
            online_count = sum(1 for r in results if r['result'].status == StoreStatus.ONLINE)
            offline_count = sum(1 for r in results if r['result'].status == StoreStatus.OFFLINE)
            total_count = len(results)
            
            db.save_summary_report(total_count, online_count, offline_count)
            logger.info(f"✅ Saved {saved_count}/{len(results)} results to database")
            if error_count > 0:
                logger.warning(f"   ⚠️ {error_count} database errors")
                
        except Exception as e:
            logger.error(f"Error saving summary: {e}")

def signal_handler(signum, frame):
    """Handle shutdown signals"""
    logger.info(f"🛑 Received signal {signum}, shutting down...")
    sys.exit(0)

def main():
    """Main entry point"""
    logger.info("=" * 70)
    logger.info("🚀 CocoPan Monitor Service - FIXED VERSION")
    logger.info("✅ No threading issues, checks ALL stores")
    logger.info("=" * 70)
    
    # Validate configuration
    if not config.validate_timezone():
        logger.error("❌ Timezone validation failed!")
        sys.exit(1)
    
    # Setup signal handlers
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)
    
    monitor = None
    
    try:
        # Initialize monitor
        monitor = SimpleStoreMonitor()
        
        if not monitor.store_urls:
            logger.error("❌ No store URLs loaded!")
            sys.exit(1)
        
        # Test database
        try:
            db_stats = db.get_database_stats()
            logger.info(f"✅ Database ready: {db_stats['db_type']}")
        except Exception as e:
            logger.warning(f"⚠️ Database issue: {e}")
        
        # Setup scheduler
        if HAS_SCHEDULER:
            ph_tz = config.get_timezone()
            scheduler = BlockingScheduler(timezone=ph_tz)
            
            # Schedule hourly checks
            for hour in range(config.MONITOR_START_HOUR, config.MONITOR_END_HOUR + 1):
                scheduler.add_job(
                    func=monitor.check_all_stores_guaranteed,
                    trigger=CronTrigger(hour=hour, minute=0, timezone=ph_tz),
                    id=f'check_{hour}',
                    name=f'Check at {hour:02d}:00',
                    max_instances=1,
                    coalesce=True,
                    misfire_grace_time=300
                )
            
            logger.info(f"⏰ Scheduled checks from {config.MONITOR_START_HOUR}:00 to {config.MONITOR_END_HOUR}:00")
            
            # Run initial check
            logger.info("🔍 Running initial check...")
            try:
                monitor.check_all_stores_guaranteed()
            except Exception as e:
                logger.error(f"Initial check error: {e}")
                logger.error(traceback.format_exc())
            
            # Start scheduler
            logger.info("✅ Monitoring active!")
            scheduler.start()
            
        else:
            # Simple loop
            logger.info("⚠️ Using simple loop")
            while True:
                try:
                    current_hour = config.get_current_time().hour
                    
                    if config.is_monitor_time(current_hour):
                        monitor.check_all_stores_guaranteed()
                    else:
                        logger.info(f"😴 Outside monitoring hours")
                    
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
        logger.info("👋 Monitor stopped")

if __name__ == "__main__":
    main()