#!/usr/bin/env python3
"""
GrabFood Store Checker with Database Integration
- Uses Selenium for reliable page scraping
- Detects online/offline/terminated/closed stores
- Saves results to database like monitor_service.py
- Processes all GrabFood URLs from branch_urls.json
"""

import json
import os
import time
import random
import uuid
import re
from pathlib import Path
from typing import List, Dict, Optional, Tuple
from datetime import datetime, timedelta
from enum import Enum
from dataclasses import dataclass

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup
import logging

# Import database and config from monitor service
try:
    from config import config
    from database import db
    HAS_DATABASE = True
except ImportError:
    HAS_DATABASE = False
    print("⚠️ WARNING: Could not import config/database modules")

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# ------------------------------------------------------------------------------
# Status Definitions (same as monitor_service.py)
# ------------------------------------------------------------------------------
class StoreStatus(Enum):
    ONLINE = "online"
    OFFLINE = "offline"
    TERMINATED = "terminated"
    CLOSED = "closed"
    BLOCKED = "blocked"
    ERROR = "error"
    UNKNOWN = "unknown"


@dataclass
class StoreCheckResult:
    """Result of a store check"""
    status: StoreStatus
    store_name: str
    store_url: str
    response_time: int
    confidence: float
    message: str = ""
    evidence: str = ""
    store_data: Optional[Dict] = None


# ------------------------------------------------------------------------------
# GrabFood Store Checker with Selenium + Database
# ------------------------------------------------------------------------------
class GrabFoodStoreChecker:
    """Check GrabFood stores using Selenium and save to database"""

    def __init__(self, headless: bool = True, save_artifacts: bool = False, output_dir: Optional[str] = None):
        self.headless = headless
        self.save_artifacts = save_artifacts
        self.driver: Optional[webdriver.Chrome] = None
        self.batch_id = int(time.time())
        
        # Output directory for artifacts (optional)
        if save_artifacts:
            env_dir = os.environ.get("GRAB_OUTPUT_DIR")
            self.output_dir = Path(output_dir or env_dir or (Path.home() / "grab_checker_outputs")).expanduser()
            self.output_dir.mkdir(parents=True, exist_ok=True)
            logger.info(f"✓ Artifacts will be saved to: {self.output_dir}")
        else:
            self.output_dir = None
            logger.info(f"✓ Artifact saving disabled (set save_artifacts=True to enable)")
        
        # Get timezone from config
        if HAS_DATABASE:
            self.timezone = config.get_timezone()
        else:
            import pytz
            self.timezone = pytz.timezone('Asia/Manila')
        
        # Statistics
        self.stats = {
            'total': 0,
            'online': 0,
            'offline': 0,
            'terminated': 0,
            'closed': 0,
            'blocked': 0,
            'error': 0,
            'unknown': 0,
            'results': []
        }

        self._setup_driver()
        logger.info(f"🛒 GrabFood Store Checker initialized")

    def load_grabfood_urls(self, urls_file: str = "branch_urls.json") -> List[str]:
        """Load GrabFood URLs from branch_urls.json"""
        try:
            with open(urls_file, 'r') as f:
                data = json.load(f)
                all_urls = data.get('urls', [])
            
            # Filter to only GrabFood URLs
            grabfood_urls = [
                url for url in all_urls 
                if 'food.grab.com' in url.lower()
            ]
            
            logger.info(f"📋 Loaded {len(grabfood_urls)} GrabFood URLs from {urls_file}")
            return grabfood_urls
            
        except FileNotFoundError:
            logger.error(f"❌ File not found: {urls_file}")
            return []
        except json.JSONDecodeError as e:
            logger.error(f"❌ Invalid JSON in {urls_file}: {e}")
            return []
        except Exception as e:
            logger.error(f"❌ Error loading URLs: {e}")
            return []

    def _setup_driver(self):
        """Setup Chrome WebDriver"""
        chrome_options = Options()
        if self.headless:
            chrome_options.add_argument('--headless=new')

        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument('--disable-gpu')
        chrome_options.add_argument('--disable-blink-features=AutomationControlled')
        chrome_options.add_argument('--window-size=1920,1080')

        user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        ]
        chrome_options.add_argument(f'user-agent={random.choice(user_agents)}')
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option('useAutomationExtension', False)

        self.driver = webdriver.Chrome(options=chrome_options)
        self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        logger.info("✓ Chrome WebDriver ready")

    def extract_store_name_from_url(self, url: str) -> str:
        """Extract store name from URL"""
        try:
            match = re.search(r'/restaurant/([^/]+)', url)
            if match:
                raw_name = match.group(1)
                # Remove delivery suffix
                raw_name = re.sub(r'-delivery$', '', raw_name)
                # Convert to title case and replace dashes
                name = raw_name.replace('-', ' ').title()
                # Add Cocopan prefix if not present
                if not name.lower().startswith('cocopan'):
                    name = f"Cocopan {name}"
                return name
            return "Unknown GrabFood Store"
        except Exception as e:
            logger.error(f"Error extracting name from {url}: {e}")
            return "Unknown Store"

    def check_store(self, url: str, url_index: int, total_urls: int, max_retries: int = 2) -> StoreCheckResult:
        """Check a single GrabFood store and return result"""
        start_time = time.time()
        store_name = self.extract_store_name_from_url(url)
        
        logger.info(f"📍 [{url_index + 1}/{total_urls}] Checking: {store_name}")
        logger.info(f"   URL: {url}")

        for attempt in range(1, max_retries + 1):
            try:
                logger.info(f"   ⏳ Attempt {attempt}/{max_retries}...")
                
                # Load page
                self.driver.get(url)
                time.sleep(3)  # Wait for page to load
                
                # Get page source
                html = self.driver.page_source
                soup = BeautifulSoup(html, 'html.parser')
                
                # Get visible text
                soup_copy = BeautifulSoup(html, 'html.parser')
                for tag in soup_copy(["script", "style", "meta", "link"]):
                    tag.decompose()
                visible_text = soup_copy.get_text(separator='\n', strip=True)
                
                # Calculate response time
                response_time = int((time.time() - start_time) * 1000)
                
                # Check for various statuses
                title_lower = self.driver.title.lower()
                visible_lower = visible_text.lower()
                
                # PRIORITY 1: Check for terminated/permanently closed
                if self._is_terminated(title_lower, visible_lower):
                    result = StoreCheckResult(
                        status=StoreStatus.TERMINATED,
                        store_name=store_name,
                        store_url=url,
                        response_time=response_time,
                        confidence=0.95,
                        message="Store is terminated or permanently closed",
                        evidence="Detected 'terminated' or 'permanently closed' in page content"
                    )
                    logger.info(f"   🚫 TERMINATED/CLOSED")
                    self._save_artifacts_if_enabled(url_index, attempt, 'terminated', html)
                    return result
                
                # PRIORITY 2: Check for error pages
                if self._is_error_page(title_lower, visible_lower, html):
                    if attempt < max_retries:
                        logger.info(f"   ⚠️ Error page detected, retrying...")
                        self.driver.refresh()
                        time.sleep(3)
                        continue
                    
                    result = StoreCheckResult(
                        status=StoreStatus.ERROR,
                        store_name=store_name,
                        store_url=url,
                        response_time=response_time,
                        confidence=0.8,
                        message="Error page or blocked",
                        evidence="Detected error page after retries"
                    )
                    logger.info(f"   ⚠️ ERROR PAGE")
                    self._save_artifacts_if_enabled(url_index, attempt, 'error', html)
                    return result
                
                # PRIORITY 3: Try to extract data from __NEXT_DATA__
                next_data_result = self._check_next_data(soup, store_name, url, response_time)
                if next_data_result:
                    logger.info(f"   ✅ Extracted from __NEXT_DATA__: {next_data_result.status.value.upper()}")
                    self._save_artifacts_if_enabled(url_index, attempt, next_data_result.status.value, html)
                    return next_data_result
                
                # PRIORITY 4: Try HTML parsing
                html_parse_result = self._check_html_parsing(
                    self.driver.title, visible_text, store_name, url, response_time
                )
                if html_parse_result:
                    logger.info(f"   ✅ Extracted from HTML: {html_parse_result.status.value.upper()}")
                    self._save_artifacts_if_enabled(url_index, attempt, html_parse_result.status.value, html)
                    return html_parse_result
                
                # If we get here, we couldn't determine status
                if attempt < max_retries:
                    logger.info(f"   ❓ Unknown status, retrying...")
                    self.driver.refresh()
                    time.sleep(3)
                    continue
                
                # All attempts exhausted
                result = StoreCheckResult(
                    status=StoreStatus.UNKNOWN,
                    store_name=store_name,
                    store_url=url,
                    response_time=response_time,
                    confidence=0.3,
                    message="Could not determine store status",
                    evidence="No valid data found in __NEXT_DATA__ or HTML"
                )
                logger.info(f"   ❓ UNKNOWN STATUS")
                self._save_artifacts_if_enabled(url_index, attempt, 'unknown', html)
                return result
                
            except Exception as e:
                logger.error(f"   ❌ Error checking store: {e}")
                if attempt < max_retries:
                    time.sleep(2)
                    continue
                
                # Final error after all retries
                response_time = int((time.time() - start_time) * 1000)
                return StoreCheckResult(
                    status=StoreStatus.ERROR,
                    store_name=store_name,
                    store_url=url,
                    response_time=response_time,
                    confidence=0.2,
                    message=f"Exception: {str(e)[:100]}",
                    evidence=""
                )

    def _is_terminated(self, title_lower: str, visible_lower: str) -> bool:
        """Check if store is terminated or permanently closed"""
        terminated_keywords = [
            'terminated',
            'permanently closed',
            'closed permanently',
            'no longer available',
            'not available anymore',
            'store has closed',
            "closed"
        ]
        
        # Check in title
        for keyword in terminated_keywords:
            if keyword in title_lower:
                return True
        
        # Check in first 1000 chars of visible text
        first_1000 = visible_lower[:1000]
        for keyword in terminated_keywords:
            if keyword in first_1000:
                return True
        
        return False

    def _is_error_page(self, title_lower: str, visible_lower: str, html: str) -> bool:
        """Check if page is an error page or blocked"""
        error_indicators = [
            ('oops' in title_lower and 'something went wrong' in title_lower),
            ('404' in title_lower or 'not found' in title_lower),
            ('403' in title_lower or 'forbidden' in title_lower),
            ('401' in title_lower or 'unauthorized' in title_lower),
            ('cloudflare' in html.lower() and 'checking your browser' in html.lower()),
            ('access denied' in visible_lower and len(visible_lower) < 500)
        ]
        
        return any(error_indicators)

    def _check_next_data(self, soup: BeautifulSoup, store_name: str, url: str, response_time: int) -> Optional[StoreCheckResult]:
        """Extract status from __NEXT_DATA__ JSON"""
        try:
            next_data = soup.find('script', {'id': '__NEXT_DATA__'})
            if not next_data or not next_data.string:
                return None
            
            data = json.loads(next_data.string)
            props = data.get('props', {}).get('pageProps', {})
            
            if 'merchant' not in props:
                return None
            
            merchant = props['merchant']
            actual_name = merchant.get('name', store_name)
            
            # Check various status indicators
            is_closed = merchant.get('isClosed', False)
            is_available = merchant.get('available', True)
            merchant_status = merchant.get('status', '').upper()
            
            # Determine status
            if merchant_status == 'INACTIVE' or is_closed or not is_available:
                if merchant_status == 'INACTIVE':
                    status = StoreStatus.TERMINATED
                    message = "Store status: INACTIVE (terminated)"
                else:
                    status = StoreStatus.CLOSED
                    message = f"Store is closed (isClosed={is_closed}, available={is_available})"
            else:
                status = StoreStatus.ONLINE
                message = "Store is open and accepting orders"
            
            # Extract additional data
            store_data = {
                'id': merchant.get('id'),
                'name': actual_name,
                'rating': merchant.get('rating'),
                'cuisine': merchant.get('cuisines', []),
                'is_closed': is_closed,
                'available': is_available,
                'status': merchant_status
            }
            
            return StoreCheckResult(
                status=status,
                store_name=actual_name,
                store_url=url,
                response_time=response_time,
                confidence=0.95,
                message=message,
                evidence=f"__NEXT_DATA__: status={merchant_status}, isClosed={is_closed}, available={is_available}",
                store_data=store_data
            )
            
        except json.JSONDecodeError as e:
            logger.debug(f"JSON parse error: {e}")
            return None
        except Exception as e:
            logger.debug(f"Error parsing __NEXT_DATA__: {e}")
            return None

    def _check_html_parsing(self, title: str, visible_text: str, store_name: str, url: str, response_time: int) -> Optional[StoreCheckResult]:
        """Extract status from HTML parsing as fallback"""
        try:
            # Pattern: "Restaurant Name ⭐ Rating"
            match = re.match(r'^(.+?)\s*⭐\s*([\d.]+)', title)
            if not match:
                return None
            
            actual_name = match.group(1).strip()
            rating_str = match.group(2)
            
            try:
                rating = float(rating_str)
            except:
                rating = None
            
            # If we can parse the title with rating, assume store is open
            # (closed/terminated stores usually don't show ratings)
            return StoreCheckResult(
                status=StoreStatus.ONLINE,
                store_name=actual_name,
                store_url=url,
                response_time=response_time,
                confidence=0.75,
                message=f"Store appears active (rating: {rating}★)",
                evidence=f"HTML title format indicates active store",
                store_data={'name': actual_name, 'rating': rating}
            )
            
        except Exception as e:
            logger.debug(f"HTML parsing error: {e}")
            return None

    def _save_artifacts_if_enabled(self, url_index: int, attempt: int, status: str, html: str):
        """Save screenshot and HTML if artifact saving is enabled"""
        if not self.save_artifacts or not self.output_dir:
            return
        
        try:
            # Create base filename
            safe_status = "".join(c if c.isalnum() or c in "-_" else "_" for c in status)
            base = self.output_dir / f"batch_{self.batch_id}_url{url_index:03d}_attempt{attempt}_{safe_status}"
            
            # Save screenshot
            png_path = base.with_suffix(".png")
            self.driver.save_screenshot(str(png_path))
            
            # Save HTML
            html_path = base.with_suffix(".html")
            html_path.write_text(html, encoding="utf-8")
            
            logger.debug(f"   💾 Saved artifacts: {base.name}.*")
        except Exception as e:
            logger.debug(f"   ⚠️ Could not save artifacts: {e}")

    def check_all_stores(self, urls_file: str = "branch_urls.json") -> Dict:
        """Check all GrabFood stores and save to database"""
        urls = self.load_grabfood_urls(urls_file)
        
        if not urls:
            logger.error("❌ No GrabFood URLs found to check!")
            return self.stats

        if not HAS_DATABASE:
            logger.error("❌ Database module not available - cannot save results!")
            return self.stats

        self.stats['total'] = len(urls)
        
        # Calculate effective_at (current hour)
        now = datetime.now(self.timezone)
        effective_at = now.replace(minute=0, second=0, microsecond=0)
        run_id = uuid.uuid4()

        logger.info("=" * 80)
        logger.info("🛒 GRABFOOD STORE CHECKER WITH DATABASE INTEGRATION")
        logger.info("=" * 80)
        logger.info(f"📋 Total URLs: {len(urls)}")
        logger.info(f"🕐 Effective time: {effective_at.strftime('%Y-%m-%d %H:00:00')}")
        logger.info(f"🆔 Run ID: {run_id}")
        logger.info(f"💾 Database: {'ENABLED' if HAS_DATABASE else 'DISABLED'}")
        logger.info(f"📸 Artifacts: {'ENABLED' if self.save_artifacts else 'DISABLED'}")
        logger.info("=" * 80)

        start_time = time.time()
        saved_count = 0
        error_count = 0

        for idx, url in enumerate(urls):
            try:
                # Check store
                result = self.check_store(url, idx, len(urls))
                
                # Update statistics
                self.stats['results'].append({
                    'url': url,
                    'name': result.store_name,
                    'status': result.status.value,
                    'response_time': result.response_time,
                    'confidence': result.confidence,
                    'message': result.message
                })
                
                # Update counters
                if result.status == StoreStatus.ONLINE:
                    self.stats['online'] += 1
                elif result.status == StoreStatus.OFFLINE:
                    self.stats['offline'] += 1
                elif result.status == StoreStatus.TERMINATED:
                    self.stats['terminated'] += 1
                elif result.status == StoreStatus.CLOSED:
                    self.stats['closed'] += 1
                elif result.status == StoreStatus.BLOCKED:
                    self.stats['blocked'] += 1
                elif result.status == StoreStatus.ERROR:
                    self.stats['error'] += 1
                elif result.status == StoreStatus.UNKNOWN:
                    self.stats['unknown'] += 1
                
                # Save to database (same as monitor_service.py)
                try:
                    store_id = db.get_or_create_store(result.store_name, result.store_url)
                    evidence = result.evidence or result.message or ""
                    
                    # Save to hourly snapshot
                    db.upsert_store_status_hourly(
                        effective_at=effective_at,
                        platform='grabfood',
                        store_id=store_id,
                        status=result.status.value.upper(),
                        confidence=result.confidence,
                        response_ms=result.response_time,
                        evidence=evidence,
                        probe_time=datetime.now(self.timezone),
                        run_id=run_id,
                    )
                    
                    # Backward-compatible: save to status_checks table
                    try:
                        is_online = (result.status == StoreStatus.ONLINE)
                        msg = result.message or ""
                        
                        # Add status prefixes (same as monitor_service.py)
                        if result.status == StoreStatus.BLOCKED:
                            msg = f"[BLOCKED] {msg}"
                        elif result.status == StoreStatus.UNKNOWN:
                            msg = f"[UNKNOWN] {msg}"
                        elif result.status == StoreStatus.ERROR:
                            msg = f"[ERROR] {msg}"
                        elif result.status == StoreStatus.OFFLINE:
                            msg = f"[OFFLINE] {msg}"
                        elif result.status == StoreStatus.TERMINATED:
                            msg = f"[TERMINATED] {msg}"
                        elif result.status == StoreStatus.CLOSED:
                            msg = f"[CLOSED] {msg}"

                        db.save_status_check(store_id, is_online, result.response_time, msg)
                    except Exception as e:
                        logger.debug(f"(Optional) legacy save_status_check failed: {e}")
                    
                    saved_count += 1
                    logger.info(f"   💾 Saved to database: {result.store_name}")
                    
                except Exception as e:
                    logger.error(f"   ❌ Database save error: {e}")
                    error_count += 1
                
                # Brief pause between stores
                if idx < len(urls) - 1:
                    time.sleep(random.uniform(3, 5))
                    
            except Exception as e:
                logger.error(f"\n❌ Unexpected error for URL {idx + 1}: {e}")
                self.stats['error'] += 1
                error_count += 1

        # Save summary report (backward compatible)
        try:
            total = len(self.stats['results'])
            online = sum(1 for r in self.stats['results'] if r['status'] == 'online')
            offline = sum(1 for r in self.stats['results'] if r['status'] == 'offline')
            db.save_summary_report(total, online, offline)
        except Exception as e:
            logger.debug(f"(Optional) legacy save_summary_report failed: {e}")

        # Print final summary
        elapsed = time.time() - start_time
        self._print_summary(elapsed, saved_count, error_count, effective_at)
        
        return self.stats

    def _print_summary(self, elapsed_time: float, saved_count: int, error_count: int, effective_at: datetime):
        """Print comprehensive summary"""
        logger.info("\n" + "=" * 80)
        logger.info("📊 STORE CHECKING COMPLETE")
        logger.info("=" * 80)
        logger.info(f"\n⏱️  Total Time: {elapsed_time / 60:.1f} minutes")
        logger.info(f"📋 Total URLs: {self.stats['total']}")
        logger.info(f"\n📈 Status Breakdown:")
        logger.info(f"   🟢 Online: {self.stats['online']}")
        logger.info(f"   🔴 Offline: {self.stats['offline']}")
        logger.info(f"   🚫 Terminated: {self.stats['terminated']}")
        logger.info(f"   🔒 Closed: {self.stats['closed']}")
        logger.info(f"   🚧 Blocked: {self.stats['blocked']}")
        logger.info(f"   ⚠️  Error: {self.stats['error']}")
        logger.info(f"   ❓ Unknown: {self.stats['unknown']}")
        
        if HAS_DATABASE:
            logger.info(f"\n💾 Database:")
            logger.info(f"   ✅ Saved: {saved_count}/{self.stats['total']}")
            if error_count > 0:
                logger.info(f"   ❌ Errors: {error_count}")
            logger.info(f"   🕐 Hour slot: {effective_at.strftime('%Y-%m-%d %H:00:00')}")
        
        if self.save_artifacts and self.output_dir:
            logger.info(f"\n📸 Artifacts saved to: {self.output_dir}")
        
        # List all stores with their status
        if self.stats['results']:
            logger.info(f"\n📋 Store Details:")
            for result in self.stats['results']:
                status_icons = {
                    'online': '🟢',
                    'offline': '🔴',
                    'terminated': '🚫',
                    'closed': '🔒',
                    'blocked': '🚧',
                    'error': '⚠️',
                    'unknown': '❓'
                }
                icon = status_icons.get(result['status'], '❓')
                logger.info(f"   {icon} {result['name']:<40} {result['status'].upper():<12} ({result['response_time']}ms)")

    def close(self):
        """Close browser"""
        if self.driver:
            self.driver.quit()
            logger.info("\n✓ Browser closed")


# ------------------------------------------------------------------------------
# Main Entry Point
# ------------------------------------------------------------------------------
def main():
    """Main entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description='GrabFood Store Checker with Database Integration')
    parser.add_argument('--headless', action='store_true', default=True, 
                       help='Run browser in headless mode (default: True)')
    parser.add_argument('--visible', action='store_true', 
                       help='Run browser in visible mode (overrides --headless)')
    parser.add_argument('--save-artifacts', action='store_true', 
                       help='Save screenshots and HTML for debugging')
    parser.add_argument('--output-dir', type=str, 
                       help='Custom output directory for artifacts')
    
    args = parser.parse_args()
    
    # Handle visible flag
    headless = args.headless and not args.visible
    
    logger.info("=" * 80)
    logger.info("🛒 GRABFOOD STORE CHECKER WITH DATABASE INTEGRATION")
    logger.info("=" * 80)
    logger.info(f"🖥️  Browser mode: {'Headless' if headless else 'Visible'}")
    logger.info(f"📸 Save artifacts: {args.save_artifacts}")
    if not HAS_DATABASE:
        logger.warning("⚠️ Database module not available - results won't be saved!")
    logger.info("=" * 80)
    
    # Create checker
    checker = GrabFoodStoreChecker(
        headless=headless,
        save_artifacts=args.save_artifacts,
        output_dir=args.output_dir
    )

    try:
        # Check all stores
        checker.check_all_stores("branch_urls.json")
        
    finally:
        checker.close()


if __name__ == "__main__":
    main()