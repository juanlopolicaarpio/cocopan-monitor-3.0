#!/usr/bin/env python3
"""
FIXED DEBUG VERSION - GrabFood Store Monitor
🔍 Enhanced debugging with proper rate limit detection
🚀 Improved logic to avoid false positives from error pages
⚡ Better bot detection avoidance
"""
import os
import json
import time
import logging
import random
from datetime import datetime
from typing import List, Dict, Any
from dataclasses import dataclass
from enum import Enum

import requests
from bs4 import BeautifulSoup
import urllib3

# Disable SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Enhanced logging for debugging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# ------------------------------------------------------------------------------
# Status + Result Classes (EXACT SAME AS ORIGINAL)
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
# Store Name Manager (EXACT SAME AS ORIGINAL)
# ------------------------------------------------------------------------------
class StoreNameManager:
    """Manages proper store name extraction and caching for GrabFood stores"""

    def __init__(self):
        self.name_cache: Dict[str, str] = {}
        self.store_cache: Dict[str, str] = {}
        self.headers = {
            'User-Agent': ('Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                           'AppleWebKit/537.36 (KHTML, like Gecko) '
                           'Chrome/120.0.0.0 Safari/537.36'),
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        }

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
                import re
                match = re.search(r'/restaurant/([^/]+)', url)
                if match:
                    raw_name = match.group(1)
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

    def get_store_name(self, url: str) -> str:
        """Get store name from predefined data or extract from URL"""
        # Cache hit
        if url in self.store_cache:
            return self.store_cache[url]

        # Predefined names (store_names.json)
        try:
            with open('store_names.json', 'r') as f:
                payload = json.load(f)
                store_names = payload.get('store_names', {})
            clean_url = url.rstrip('?').rstrip('/')
            if clean_url in store_names:
                name = store_names[clean_url].get('store_name') or ""
                if name:
                    name = self.clean_store_name(name)
                    self.store_cache[url] = name
                    return name
        except Exception as e:
            logger.debug(f"Could not load predefined names: {e}")

        # Fallback to URL extraction
        name = self.extract_store_name_from_url(url)
        self.store_cache[url] = name
        return name

# ------------------------------------------------------------------------------
# IMPROVED Debug GrabFood Monitor 
# ------------------------------------------------------------------------------
class ImprovedDebugGrabFoodMonitor:
    """IMPROVED debug version with better rate limit detection and bot avoidance"""

    def __init__(self):
        self.store_urls = self._load_grabfood_urls()
        self.name_manager = StoreNameManager()
        
        # Enhanced user agents with more variety
        self.user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15',
        ]
        
        # Track rate limiting
        self.rate_limited_count = 0
        self.identical_responses = 0
        self.last_response_hash = None

        print("🔍 IMPROVED DEBUG GrabFood Monitor initialized")
        print(f"📋 Found {len(self.store_urls)} GrabFood stores to check")
        print("🛡️ Enhanced bot detection avoidance enabled")
        print("🚨 Improved rate limit detection enabled")
        print("=" * 80)

    def _load_grabfood_urls(self):
        """Load ONLY GrabFood URLs from branch_urls.json"""
        try:
            with open('branch_urls.json') as f:
                data = json.load(f)
                all_urls = data.get('urls', [])

                # Filter to only GrabFood URLs
                grabfood_urls = [url for url in all_urls if 'grab.com' in url]
                foodpanda_urls = [url for url in all_urls if 'foodpanda' in url]

                logger.info(f"📋 Loaded {len(all_urls)} total URLs from branch_urls.json")
                logger.info(f"🛒 Filtered to {len(grabfood_urls)} GrabFood URLs")
                logger.info(f"🐼 Skipping {len(foodpanda_urls)} Foodpanda URLs")

                return grabfood_urls

        except Exception as e:
            logger.error(f"Failed to load URLs: {e}")
            return []

    def is_rate_limited_response(self, resp, page_text_clean: str) -> bool:
        """Detect if we're getting a rate limited/blocked response"""
        
        # Check for explicit 429 status
        if resp.status_code == 429:
            return True
            
        # Check for 429 in title or content
        try:
            soup = BeautifulSoup(resp.text, 'html.parser')
            title = soup.title.string.strip() if soup.title and soup.title.string else ""
            if "429" in title or "too many requests" in title.lower():
                return True
        except:
            pass
            
        # Check for rate limit indicators in content
        rate_limit_indicators = [
            "429 too many requests",
            "rate limit",
            "too many requests", 
            "request limit exceeded",
            "temporarily blocked",
            "oops, something went wrong",
        ]
        
        for indicator in rate_limit_indicators:
            if indicator in page_text_clean:
                return True
                
        # Check for suspiciously short content that's identical
        content_hash = hash(page_text_clean)
        if self.last_response_hash == content_hash and len(page_text_clean) < 1000:
            self.identical_responses += 1
            if self.identical_responses >= 3:  # 3+ identical short responses = likely rate limited
                return True
        else:
            self.identical_responses = 0
            
        self.last_response_hash = content_hash
        return False

    def check_grabfood_store_improved(self, url: str, retry_count: int = 0) -> CheckResult:
        """IMPROVED check with better rate limit detection and bot avoidance"""
        start_time = time.time()
        max_retries = 2

        try:
            session = requests.Session()
            
            # Enhanced headers to look more human
            user_agent = random.choice(self.user_agents)
            session.headers.update({
                'User-Agent': user_agent,
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.9,fil;q=0.8',
                'Accept-Encoding': 'gzip, deflate, br',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1',
                'Sec-Fetch-Dest': 'document',
                'Sec-Fetch-Mode': 'navigate', 
                'Sec-Fetch-Site': 'none',
                'Cache-Control': 'max-age=0',
            })
            
            # Add some randomness to avoid bot detection
            time.sleep(random.uniform(0.5, 2.0))

            try:
                resp = session.get(url, timeout=30, allow_redirects=True)
            except requests.exceptions.SSLError:
                resp = session.get(url, timeout=30, allow_redirects=True, verify=False)

            response_time = int((time.time() - start_time) * 1000)

            # Handle explicit HTTP errors first
            if resp.status_code == 403:
                if retry_count < max_retries:
                    time.sleep(random.uniform(3, 6))  # Longer delay for 403
                    return self.check_grabfood_store_improved(url, retry_count + 1)
                return CheckResult(StoreStatus.BLOCKED, response_time, "Access denied (403) after retries", 0.9)

            if resp.status_code == 429:
                self.rate_limited_count += 1
                return CheckResult(StoreStatus.BLOCKED, response_time, "Rate limited (429)", 0.95)

            if resp.status_code == 404:
                return CheckResult(StoreStatus.OFFLINE, response_time, "Store page not found (404)", 0.95)

            if resp.status_code == 200:
                try:
                    soup = BeautifulSoup(resp.text, 'html.parser')

                    # Remove script and style elements
                    for script in soup(['script', 'style']):
                        script.decompose()

                    page_text_clean = ' '.join(soup.get_text().lower().split())

                    # 🚨 NEW: Check for rate limiting FIRST before any other logic
                    if self.is_rate_limited_response(resp, page_text_clean):
                        self.rate_limited_count += 1
                        return CheckResult(StoreStatus.BLOCKED, response_time, "Rate limited (detected from content)", 0.95)

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
                        'restaurant closed',
                        'store is currently closed',
                        'no longer available',
                        'permanently closed'
                    ]

                    for indicator in offline_indicators:
                        if indicator in page_text_clean:
                            return CheckResult(StoreStatus.OFFLINE, response_time, f"Store closed: {indicator}", 0.95)

                    # 🚨 IMPROVED: More specific online indicators (not just "menu" which appears in error pages)
                    strong_online_indicators = [
                        'order now',
                        'add to basket', 
                        'add to cart',
                        'delivery fee',
                        'place order',
                        'checkout'
                    ]
                    
                    # Look for strong online indicators first
                    for indicator in strong_online_indicators:
                        if indicator in page_text_clean:
                            return CheckResult(StoreStatus.ONLINE, response_time, f"Store online: found '{indicator}'", 0.95)

                    # 🚨 IMPROVED: Only consider "menu" as online if combined with other indicators
                    if 'menu' in page_text_clean:
                        # Must have additional evidence it's a real store page, not just navigation
                        additional_evidence = ['price', 'php', '₱', 'delivery time', 'rating', 'reviews', 'mins']
                        if any(evidence in page_text_clean for evidence in additional_evidence):
                            return CheckResult(StoreStatus.ONLINE, response_time, "Store page with menu and pricing", 0.85)
                        else:
                            return CheckResult(StoreStatus.UNKNOWN, response_time, "Found 'menu' but no pricing evidence", 0.4)

                    # General content check with higher standards
                    if len(page_text_clean) > 1500:  # Increased threshold
                        store_indicators = ['order', 'delivery', 'price', 'add', 'php', '₱']
                        found_indicators = [ind for ind in store_indicators if ind in page_text_clean]
                        if len(found_indicators) >= 2:  # Need multiple indicators
                            return CheckResult(StoreStatus.ONLINE, response_time, f"Store page (indicators: {found_indicators})", 0.7)
                        else:
                            return CheckResult(StoreStatus.UNKNOWN, response_time, f"Large page but unclear status (found: {found_indicators})", 0.5)
                    else:
                        return CheckResult(StoreStatus.UNKNOWN, response_time, "Minimal page content", 0.3)

                except Exception as parse_error:
                    logger.debug(f"Parse error for {url}: {parse_error}")
                    return CheckResult(StoreStatus.ERROR, response_time, f"Parse error: {str(parse_error)[:50]}", 0.3)

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

    def debug_single_store_improved(self, url: str, index: int, total: int) -> Dict[str, Any]:
        """Enhanced debugging for a single store check"""
        store_name = self.name_manager.get_store_name(url)
        
        print(f"\n{'='*60}")
        print(f"🔍 IMPROVED DEBUG CHECK [{index}/{total}]")
        print(f"🏪 Store: {store_name}")
        print(f"🔗 URL: {url}")
        print(f"⏰ Time: {datetime.now().strftime('%H:%M:%S')}")
        print(f"🚨 Rate Limited Count: {self.rate_limited_count}")
        print(f"{'='*60}")
        
        # Enhanced check with detailed logging
        start_time = time.time()
        
        try:
            # Show which user agent we're using
            user_agent = random.choice(self.user_agents)
            print(f"🕸️ User Agent: {user_agent[:70]}...")
            
            session = requests.Session()
            session.headers.update({
                'User-Agent': user_agent,
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.9,fil;q=0.8',
                'Accept-Encoding': 'gzip, deflate, br',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1',
                'Sec-Fetch-Dest': 'document',
                'Sec-Fetch-Mode': 'navigate',
            })

            # Add pre-request delay
            delay = random.uniform(0.5, 2.0)
            print(f"⏳ Pre-request delay: {delay:.1f}s")
            time.sleep(delay)

            print("📡 Making HTTP request...")
            try:
                resp = session.get(url, timeout=30, allow_redirects=True)
            except requests.exceptions.SSLError:
                print("⚠️ SSL Error - retrying without verification...")
                resp = session.get(url, timeout=30, allow_redirects=True, verify=False)

            response_time = int((time.time() - start_time) * 1000)
            
            print(f"📊 HTTP Status: {resp.status_code}")
            print(f"⏱️ Response Time: {response_time}ms")
            print(f"📏 Content Length: {len(resp.text)} characters")
            
            # Get the actual result using improved logic
            result = self.check_grabfood_store_improved(url)
            
            # Enhanced debugging output
            emoji = {
                StoreStatus.ONLINE: "🟢",
                StoreStatus.OFFLINE: "🔴", 
                StoreStatus.BLOCKED: "🚫",
                StoreStatus.ERROR: "⚠️",
                StoreStatus.UNKNOWN: "❓"
            }.get(result.status, "❓")
            
            print(f"\n🎯 FINAL RESULT: {emoji} {result.status.value.upper()}")
            print(f"🎯 Confidence: {result.confidence:.2f}")
            print(f"🎯 Message: {result.message}")
            
            # Show enhanced page content analysis
            if resp.status_code == 200:
                print(f"\n📄 ENHANCED PAGE ANALYSIS:")
                
                try:
                    soup = BeautifulSoup(resp.text, 'html.parser')
                    
                    # Title analysis
                    title = soup.title.string.strip() if soup.title and soup.title.string else "No title"
                    print(f"📖 Title: {title}")
                    
                    # Check for rate limiting in title
                    if "429" in title or "too many requests" in title.lower():
                        print(f"🚨 RATE LIMIT DETECTED IN TITLE!")
                    
                    # Remove scripts/styles like original
                    for script in soup(['script', 'style']):
                        script.decompose()
                    
                    page_text_clean = ' '.join(soup.get_text().lower().split())
                    print(f"📝 Clean text length: {len(page_text_clean)} characters")
                    
                    # Check if this is a rate limited response
                    is_rate_limited = self.is_rate_limited_response(resp, page_text_clean)
                    if is_rate_limited:
                        print(f"🚨 RATE LIMITED RESPONSE DETECTED!")
                    
                    # Show first 300 characters of cleaned text
                    sample_text = page_text_clean[:300] + "..." if len(page_text_clean) > 300 else page_text_clean
                    print(f"📝 Text sample: {sample_text}")
                    
                    # Enhanced indicator analysis
                    print(f"\n🔍 ENHANCED INDICATOR ANALYSIS:")
                    
                    # Rate limit indicators
                    rate_indicators = ['429 too many requests', 'rate limit', 'too many requests', 'oops, something went wrong']
                    found_rate = [ind for ind in rate_indicators if ind in page_text_clean]
                    if found_rate:
                        print(f"🚨 Found RATE LIMIT indicators: {found_rate}")
                    
                    # Offline indicators
                    offline_indicators = [
                        'restaurant is closed', 'currently unavailable', 'not accepting orders',
                        'temporarily closed', 'currently closed', 'closed for today', 'restaurant closed'
                    ]
                    found_offline = [ind for ind in offline_indicators if ind in page_text_clean]
                    if found_offline:
                        print(f"🔴 Found OFFLINE indicators: {found_offline}")
                    
                    # Strong online indicators  
                    strong_online = ['order now', 'add to basket', 'add to cart', 'delivery fee', 'place order', 'checkout']
                    found_strong_online = [ind for ind in strong_online if ind in page_text_clean]
                    if found_strong_online:
                        print(f"🟢 Found STRONG ONLINE indicators: {found_strong_online}")
                    
                    # Menu + evidence check
                    if 'menu' in page_text_clean:
                        evidence = ['price', 'php', '₱', 'delivery time', 'rating', 'reviews', 'mins']
                        found_evidence = [ev for ev in evidence if ev in page_text_clean]
                        if found_evidence:
                            print(f"🟢 Found MENU with evidence: {found_evidence}")
                        else:
                            print(f"❓ Found MENU but NO pricing evidence (likely navigation/error page)")
                    
                    # Special check for "today closed"
                    if 'today closed' in page_text_clean:
                        print(f"🔴 Found SPECIAL indicator: 'today closed'")
                    
                    # General indicators
                    general_words = ['order', 'delivery', 'price', 'add', 'php', '₱']
                    found_general = [word for word in general_words if word in page_text_clean]
                    if found_general:
                        print(f"📋 Found GENERAL words: {found_general}")
                    
                    if not found_rate and not found_offline and not found_strong_online and not found_general:
                        print(f"❓ No clear indicators found")
                        
                except Exception as parse_error:
                    print(f"⚠️ Parse error: {parse_error}")
            
            print(f"{'='*60}")
            
            return {
                'url': url,
                'name': store_name, 
                'result': result,
                'debug_info': {
                    'http_status': resp.status_code,
                    'response_time': response_time,
                    'content_length': len(resp.text),
                    'rate_limited': self.rate_limited_count
                }
            }
            
        except Exception as e:
            print(f"❌ ERROR: {str(e)}")
            error_result = CheckResult(StoreStatus.ERROR, 0, f"Check failed: {str(e)[:100]}", 0.1)
            return {'url': url, 'name': store_name, 'result': error_result}

    def run_improved_debug_check(self):
        """Run improved debug check with better rate limit handling"""
        if not self.store_urls:
            print("❌ No GrabFood URLs found! Check branch_urls.json file.")
            return
        
        print(f"🚀 Starting IMPROVED DEBUG check of {len(self.store_urls)} GrabFood stores...")
        print(f"⏰ Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        results = []
        stats = {'online': 0, 'offline': 0, 'blocked': 0, 'error': 0, 'unknown': 0}
        
        for i, url in enumerate(self.store_urls, 1):
            result_data = self.debug_single_store_improved(url, i, len(self.store_urls))
            results.append(result_data)
            
            # Update stats
            status = result_data['result'].status.value
            if status in stats:
                stats[status] += 1
            
            # Dynamic delay based on rate limiting
            if result_data['result'].status == StoreStatus.BLOCKED:
                delay = random.uniform(5, 10)  # Longer delay if blocked
                print(f"🚨 Rate limited! Waiting {delay:.1f}s before next check...")
            else:
                delay = random.uniform(2, 4)  # Normal delay
                print(f"⏳ Waiting {delay:.1f}s before next check...")
            
            if i < len(self.store_urls):
                time.sleep(delay)
        
        # Final summary with rate limit analysis
        print(f"\n{'='*80}")
        print(f"✅ IMPROVED DEBUG CHECK COMPLETED")
        print(f"📊 FINAL STATISTICS:")
        print(f"   Total Stores: {len(self.store_urls)}")
        print(f"   🟢 Online: {stats['online']}")
        print(f"   🔴 Offline: {stats['offline']}")
        print(f"   🚫 Blocked/Rate Limited: {stats['blocked']}")
        print(f"   ⚠️ Errors: {stats['error']}")
        print(f"   ❓ Unknown: {stats['unknown']}")
        print(f"   🚨 Total Rate Limited Responses: {self.rate_limited_count}")
        print(f"   ⏰ Completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        # Analysis
        if self.rate_limited_count > len(self.store_urls) * 0.5:
            print(f"\n🚨 HIGH RATE LIMITING DETECTED!")
            print(f"   • {self.rate_limited_count}/{len(self.store_urls)} requests were rate limited")
            print(f"   • Consider using VPN, proxies, or slower request rates")
            print(f"   • Results may not be accurate due to blocking")
        
        # Show stores by status for easy review
        print(f"\n📋 STORES BY STATUS:")
        for status_name, emoji in [('online', '🟢'), ('offline', '🔴'), ('blocked', '🚫'), ('error', '⚠️'), ('unknown', '❓')]:
            stores_with_status = [r for r in results if r['result'].status.value == status_name]
            if stores_with_status:
                print(f"\n{emoji} {status_name.upper()} ({len(stores_with_status)} stores):")
                for store_data in stores_with_status:
                    conf = store_data['result'].confidence
                    msg = store_data['result'].message or "No message"
                    print(f"   • {store_data['name']} (confidence: {conf:.2f}) - {msg}")
        
        print(f"{'='*80}")
        
        return results

def main():
    """Main entry point for improved debug version"""
    print("🔍 CocoPan GrabFood IMPROVED DEBUG Monitor")
    print("📧 NO emails will be sent")
    print("💾 NO database saves") 
    print("🛡️ Enhanced bot detection avoidance")
    print("🚨 Improved rate limit detection")
    print("🎯 Better accuracy with enhanced logic")
    print("=" * 80)
    
    monitor = ImprovedDebugGrabFoodMonitor()
    
    if not monitor.store_urls:
        print("❌ No store URLs loaded! Make sure branch_urls.json exists.")
        return
    
    # Run the improved debug check immediately
    results = monitor.run_improved_debug_check()
    
    print("\n🎉 Improved debug monitoring complete!")
    print("🔍 Review the detailed output above to verify accuracy")
    print("🚨 Pay attention to rate limiting warnings")

if __name__ == "__main__":
    main()