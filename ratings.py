#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
CocoPan Rating Scraper (Selenium-based)
- Uses EXACT same scraping methods as SKU scrapers
- GrabFood: headless, shared driver, 12s wait, 5 proportional scrolls (same as grabfood_sku_scraper.py)
- Foodpanda: non-headless, fresh driver per store, random 4-6s wait, 3x500px scrolls (same as foodpanda_sku_scraper.py)
- Extracts ratings from page title, __NEXT_DATA__, JSON-LD, HTML fallbacks
- Writes to DB via database.db (unchanged)
"""

import os
import re
import time
import json
import random
import logging
from typing import Optional, Dict, Any, List
from datetime import datetime
from urllib.parse import urlparse

import undetected_chromedriver as uc
from bs4 import BeautifulSoup

from config import config
from database import db

# ------------------------- Logging -------------------------
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("rating-scraper")

# ------------------------- Chrome Setup --------------------
# EXACT copy from SKU scrapers

def find_chrome_binary():
    """Find Chrome binary on the system. EXACT same as SKU scrapers."""
    import subprocess

    # Common Chrome locations on Mac (YOUR Chrome location first!)
    mac_paths = [
        '/Users/arthur.policarpio/Applications/Google Chrome.app/Contents/MacOS/Google Chrome',  # ← YOUR CHROME
        '/Applications/Google Chrome.app/Contents/MacOS/Google Chrome',
        '/Applications/Chromium.app/Contents/MacOS/Chromium',
        os.path.expanduser('~/Applications/Google Chrome.app/Contents/MacOS/Google Chrome'),
    ]

    # Try common paths first
    for path in mac_paths:
        if os.path.exists(path):
            logger.info(f"✅ Found Chrome at: {path}")
            return path

    # Try using 'which' command
    try:
        result = subprocess.run(['which', 'google-chrome'],
                                capture_output=True, text=True, timeout=5)
        if result.returncode == 0 and result.stdout.strip():
            return result.stdout.strip()
    except:
        pass

    logger.warning("⚠️ Chrome not found in standard locations")
    return None


def create_foodpanda_driver():
    """
    Create Chrome driver for Foodpanda - EXACT copy from foodpanda_sku_scraper.py create_driver()
    NO headless, NO --disable-gpu
    """
    chrome_binary = find_chrome_binary()
    if not chrome_binary:
        raise Exception("Chrome not found! Please install Chrome or check the path.")

    options = uc.ChromeOptions()
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--window-size=1920,1080')
    options.add_argument('--lang=en-PH')

    # IMPORTANT: Your UA should match your installed Chrome major version (145)
    ua = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36'
    options.add_argument(f'--user-agent={ua}')

    driver = uc.Chrome(
        options=options,
        browser_executable_path=chrome_binary,
        version_main=145,              # ✅ force driver major version
        use_subprocess=True            # ✅ more stable on mac for uc
    )
    return driver


def create_grabfood_driver():
    """
    Create Chrome driver for GrabFood - EXACT copy from grabfood_sku_scraper.py setup_driver()
    WITH headless, WITH --disable-gpu
    """
    chrome_binary = find_chrome_binary()
    if not chrome_binary:
        raise Exception("Chrome not found! Please install Chrome or check the path.")

    options = uc.ChromeOptions()
    options.add_argument('--headless=new')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-gpu')
    options.add_argument('--window-size=1920,1080')
    options.add_argument('--lang=en-PH')

    user_agent = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36'
    options.add_argument(f'--user-agent={user_agent}')

    driver = uc.Chrome(
        options=options,
        browser_executable_path=chrome_binary,
        version_main=145,
        use_subprocess=True
    )
    return driver


# ------------------------- Debug Snapshots -----------------

def ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)


def debug_save_snapshot(prefix: str, url: str, body: str):
    """Save response HTML for later inspection."""
    try:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
        host = urlparse(url).netloc.replace(":", "_") or "unknown"
        ensure_dir("debug_snapshots")
        base = f"debug_snapshots/{prefix}_{host}_{ts}"

        with open(base + ".html", "w", encoding="utf-8") as f:
            f.write(body or "")

        with open(base + ".meta.txt", "w", encoding="utf-8") as f:
            f.write(f"URL: {url}\nLength: {len(body or '')}\n")

        logger.debug(f"[dbg] Saved snapshot → {base}.html")
    except Exception as e:
        logger.debug(f"[dbg] Snapshot save failed: {e}")


# ------------------------- Rating Extraction ---------------

def extract_rating_from_page_title(soup: BeautifulSoup) -> Optional[float]:
    """GrabFood puts rating in page title like 'Store Name ⭐ 4.5'
    EXACT same title parsing as grabfood_sku_scraper.py scrape_menu()"""
    if soup.title and soup.title.string:
        title = soup.title.string
        # Match patterns like "⭐ 4.5" or "⭐4.5" or "★ 4.5"
        match = re.search(r'[⭐★]\s*(\d+\.?\d*)', title)
        if match:
            try:
                v = float(match.group(1))
                if 0.0 <= v <= 5.0:
                    return v
            except (ValueError, TypeError):
                pass
    return None


def extract_rating_from_next_data(soup: BeautifulSoup) -> Optional[Dict[str, Any]]:
    """Extract rating from __NEXT_DATA__ script tag.
    EXACT same __NEXT_DATA__ parsing approach as foodpanda_sku_scraper.py extract_next_data()"""
    script = soup.find("script", id="__NEXT_DATA__", type="application/json")
    if not script or not script.string:
        return None

    try:
        data = json.loads(script.string)

        def walk(obj, depth=0):
            if depth > 15:
                return None
            if isinstance(obj, dict):
                # Check for rating keys
                for rating_key in ("rating", "ratingValue", "averageRating", "reviewsRating"):
                    if rating_key in obj:
                        try:
                            v = float(obj[rating_key])
                            if 0.0 <= v <= 5.0:
                                return {
                                    "rating": v,
                                    "vote_count": obj.get("voteCount") or obj.get("reviewCount") or obj.get("ratingCount"),
                                    "status": obj.get("status", "UNKNOWN"),
                                }
                        except (ValueError, TypeError):
                            pass

                # Check nested aggregateRating
                ar = obj.get("aggregateRating")
                if isinstance(ar, dict):
                    try:
                        v = float(ar.get("ratingValue"))
                        if 0.0 <= v <= 5.0:
                            return {
                                "rating": v,
                                "vote_count": ar.get("ratingCount") or ar.get("reviewCount"),
                                "status": "UNKNOWN",
                            }
                    except (ValueError, TypeError):
                        pass

                for value in obj.values():
                    r = walk(value, depth + 1)
                    if r:
                        return r

            elif isinstance(obj, list):
                for item in obj:
                    r = walk(item, depth + 1)
                    if r:
                        return r
            return None

        return walk(data)
    except Exception as e:
        logger.debug(f"Error parsing __NEXT_DATA__ for rating: {e}")
        return None


def extract_rating_from_json_ld(soup: BeautifulSoup) -> Optional[Dict[str, Any]]:
    """Extract rating from JSON-LD structured data."""
    for tag in soup.find_all("script", type="application/ld+json"):
        payload_text = tag.string or ""
        if not payload_text.strip():
            continue
        try:
            payload = json.loads(payload_text)
        except Exception:
            continue

        candidates = payload if isinstance(payload, list) else [payload]
        for obj in candidates:
            if not isinstance(obj, dict):
                continue

            ar = obj.get("aggregateRating")
            if isinstance(ar, dict):
                try:
                    v = float(ar.get("ratingValue"))
                    if 0.0 <= v <= 5.0:
                        return {
                            "rating": v,
                            "vote_count": ar.get("ratingCount") or ar.get("reviewCount"),
                            "status": "UNKNOWN",
                        }
                except (ValueError, TypeError):
                    pass

            # Deep walk
            def walk(o, depth=0):
                if depth > 10:
                    return None
                if isinstance(o, dict):
                    if "aggregateRating" in o and isinstance(o["aggregateRating"], dict):
                        try:
                            rv = float(o["aggregateRating"].get("ratingValue"))
                            if 0.0 <= rv <= 5.0:
                                return {
                                    "rating": rv,
                                    "vote_count": o["aggregateRating"].get("ratingCount"),
                                    "status": "UNKNOWN",
                                }
                        except (ValueError, TypeError):
                            pass
                    for vv in o.values():
                        r = walk(vv, depth + 1)
                        if r:
                            return r
                elif isinstance(o, list):
                    for vv in o:
                        r = walk(vv, depth + 1)
                        if r:
                            return r
                return None

            deep = walk(obj)
            if deep:
                return deep
    return None


def extract_rating_from_html_elements(soup: BeautifulSoup) -> Optional[Dict[str, Any]]:
    """Fallback: extract rating from visible HTML elements."""
    rating_selectors = [
        '[class*="rating"]',
        '[class*="Rating"]',
        '[class*="review"]',
        '[data-testid*="rating"]',
        '[data-testid*="review"]',
        '[class*="stars"]',
        '[class*="score"]',
    ]

    for selector in rating_selectors:
        try:
            elements = soup.select(selector)
            for elem in elements:
                text = elem.get_text(strip=True)
                match = re.search(r'(\d\.\d)\s*(?:/\s*5|out\s+of\s+5)?', text)
                if match:
                    try:
                        v = float(match.group(1))
                        if 0.0 <= v <= 5.0:
                            return {"rating": v, "vote_count": None, "status": "UNKNOWN"}
                    except (ValueError, TypeError):
                        pass
        except Exception:
            continue

    # Last resort: regex across all visible text
    text = soup.get_text(" ", strip=True)
    match = re.search(r'(\d\.\d)\s*(?:/|out of)?\s*5', text, re.I)
    if match:
        try:
            v = float(match.group(1))
            if 0.0 <= v <= 5.0:
                return {"rating": v, "vote_count": None, "status": "UNKNOWN"}
        except (ValueError, TypeError):
            pass

    return None


def extract_all_ratings(html: str) -> Optional[Dict[str, Any]]:
    """Run full extraction chain on HTML. Returns first successful result."""
    soup = BeautifulSoup(html, "html.parser")

    # 1. Page title (GrabFood "Store ⭐ 4.5")
    title_rating = extract_rating_from_page_title(soup)
    if title_rating is not None:
        return {
            "rating": title_rating,
            "status": "UNKNOWN",
            "is_active": None,
            "vote_count": None,
            "success": True,
            "method": "page_title",
        }

    # 2. __NEXT_DATA__
    next_data = extract_rating_from_next_data(soup)
    if next_data and next_data.get("rating") is not None:
        return {
            "rating": next_data["rating"],
            "status": next_data.get("status", "UNKNOWN"),
            "is_active": next_data.get("status") == "ACTIVE" if next_data.get("status") != "UNKNOWN" else None,
            "vote_count": next_data.get("vote_count"),
            "success": True,
            "method": "__NEXT_DATA__",
        }

    # 3. JSON-LD
    json_ld = extract_rating_from_json_ld(soup)
    if json_ld and json_ld.get("rating") is not None:
        return {
            "rating": json_ld["rating"],
            "status": "UNKNOWN",
            "is_active": None,
            "vote_count": json_ld.get("vote_count"),
            "success": True,
            "method": "JSON-LD",
        }

    # 4. HTML elements
    html_rating = extract_rating_from_html_elements(soup)
    if html_rating and html_rating.get("rating") is not None:
        return {
            "rating": html_rating["rating"],
            "status": "UNKNOWN",
            "is_active": None,
            "vote_count": html_rating.get("vote_count"),
            "success": True,
            "method": "HTML_elements",
        }

    return None


# ========================= Scraper Core =====================
# EXACT same scraping flow as SKU scrapers

class RatingScraper:

    def __init__(self):
        self.grabfood_driver = None  # GrabFood: ONE shared driver (same as GrabFoodScraper)

    # ---------- GrabFood ----------
    # EXACT same pattern as grabfood_sku_scraper.py GrabFoodScraper

    def _setup_grabfood_driver(self):
        """Setup shared GrabFood driver - EXACT same as GrabFoodScraper.setup_driver()"""
        logger.info("Setting up Chrome with undetected-chromedriver...")
        self.grabfood_driver = create_grabfood_driver()
        logger.info("Chrome WebDriver initialized")

    def _scrape_grabfood_page(self, url: str) -> Optional[str]:
        """
        Load GrabFood page - EXACT same as GrabFoodScraper.scrape_menu()
        - self.driver.get(url)
        - time.sleep(12)
        - 5 scrolls proportional to scroll height, 2s between each
        - return page_source
        """
        try:
            logger.info(f"Loading: {url}")
            self.grabfood_driver.get(url)

            logger.info("Waiting for page to load...")
            time.sleep(12)

            logger.info("Scrolling to load all items...")
            for i in range(5):
                scroll_height = self.grabfood_driver.execute_script("return document.body.scrollHeight")
                self.grabfood_driver.execute_script(f"window.scrollTo(0, {scroll_height * (i+1) / 5});")
                time.sleep(2)

            html = self.grabfood_driver.page_source
            return html

        except Exception as e:
            logger.error(f"Error loading GrabFood page: {e}")
            return None

    def scrape_grabfood_rating(self, url: str) -> Optional[Dict[str, Any]]:
        """
        Scrape GrabFood rating.
        Uses ONE shared driver for all stores (same as GrabFoodScraper.scrape_all_stores).
        """
        # Setup shared driver if not exists
        if not self.grabfood_driver:
            self._setup_grabfood_driver()

        html = self._scrape_grabfood_page(url)

        if not html:
            logger.warning("   No HTML returned from GrabFood")
            return None

        # Save debug HTML - EXACT same as grabfood_sku_scraper.py
        store_name = self._extract_store_name_from_url(url, "grabfood")
        debug_file = f"debug_{store_name.replace(' ', '_')}.html"
        with open(debug_file, 'w', encoding='utf-8') as f:
            f.write(html)
        logger.info(f"Debug HTML saved: {debug_file}")

        # Extract rating
        result = extract_all_ratings(html)
        if result:
            debug_save_snapshot("grab_success", url, html)
            return result
        else:
            debug_save_snapshot("grab_norating", url, html)
            return None

    def close_grabfood_driver(self):
        """Close shared GrabFood driver - EXACT same as GrabFoodScraper.close()"""
        if self.grabfood_driver:
            try:
                self.grabfood_driver.quit()
                logger.info("Browser closed")
            except:
                pass
            self.grabfood_driver = None

    # ---------- Foodpanda ----------
    # EXACT same pattern as foodpanda_sku_scraper.py StandaloneFoodpandaScraper

    def _scrape_foodpanda_page(self, driver, url: str) -> Optional[str]:
        """
        Load Foodpanda page - EXACT same as foodpanda_sku_scraper.py scrape_foodpanda_url()
        - driver.get(url)
        - time.sleep(random.uniform(4, 6))
        - 3 scrolls at 500px increments, 0.5s between each
        - scroll back to top
        - time.sleep(1)
        - return page_source
        """
        try:
            # Load page
            driver.get(url)
            time.sleep(random.uniform(4, 6))

            # Scroll to load content
            for i in range(3):
                driver.execute_script(f"window.scrollTo(0, {(i + 1) * 500});")
                time.sleep(0.5)
            driver.execute_script("window.scrollTo(0, 0);")
            time.sleep(1)

            # Get page source
            html = driver.page_source
            return html

        except Exception as e:
            logger.error(f"Error loading Foodpanda page: {e}")
            return None

    def scrape_foodpanda_rating(self, url: str) -> Optional[Dict[str, Any]]:
        """
        Scrape Foodpanda rating.
        FRESH driver per store, with retry loop.
        EXACT same pattern as StandaloneFoodpandaScraper.scrape_single_store().
        """
        max_retries = 2  # same as StandaloneFoodpandaScraper(max_retries=2)
        driver = None

        try:
            for attempt in range(max_retries + 1):  # 0, 1, 2 = 3 attempts total (same as SKU scraper)
                try:
                    # STEP 0: Create fresh browser for this attempt
                    # EXACT same as foodpanda_sku_scraper.py scrape_single_store()
                    if attempt == 0:
                        logger.info("🌐 Creating fresh browser for this store...")
                    else:
                        logger.info(f"🔄 RETRY {attempt}/{max_retries}")
                        logger.info(f"   Creating fresh browser (new session)...")

                    # Close old driver if exists (for retries)
                    if driver:
                        driver.quit()
                        time.sleep(2)  # Wait before creating new driver

                    # Create fresh driver
                    driver = create_foodpanda_driver()
                    logger.info(f"✅ Fresh browser created")
                    logger.info("")

                    # Scrape page - EXACT same as scrape_foodpanda_url()
                    logger.info("📡 Scraping page for rating...")
                    html = self._scrape_foodpanda_page(driver, url)

                    if not html:
                        raise Exception("Failed to get page HTML")

                    # Save debug HTML
                    store_name = self._extract_store_name_from_url(url, "foodpanda")
                    debug_file = f"debug_{store_name.replace(' ', '_')}.html"
                    with open(debug_file, 'w', encoding='utf-8') as f:
                        f.write(html)
                    logger.info(f"Debug HTML saved: {debug_file}")

                    # Check if page loaded properly (same zero-content check as SKU scraper)
                    soup = BeautifulSoup(html, "html.parser")
                    page_text = soup.get_text(strip=True)
                    if len(page_text) < 100:
                        # Page basically empty - retry
                        if attempt < max_retries:
                            logger.warning(f"⚠️ Page looks empty ({len(page_text)} chars) - will retry with fresh browser")
                            continue
                        else:
                            logger.error(f"❌ Page still empty after {max_retries + 1} attempts!")
                            debug_save_snapshot("fp_empty", url, html)
                            return None

                    # Extract rating
                    result = extract_all_ratings(html)

                    if result:
                        debug_save_snapshot("fp_success", url, html)
                        if attempt > 0:
                            logger.info(f"✅ Success on retry {attempt} with fresh browser!")
                        return result
                    else:
                        # No rating found - retry
                        if attempt < max_retries:
                            logger.warning(f"⚠️ No rating found - will retry with fresh browser")
                            debug_save_snapshot("fp_norating", url, html)
                            continue
                        else:
                            logger.error(f"❌ No rating found after {max_retries + 1} attempts")
                            debug_save_snapshot("fp_norating", url, html)
                            return None

                except Exception as e:
                    logger.error(f"❌ Error on attempt {attempt + 1}: {e}")

                    if attempt < max_retries:
                        logger.info(f"   Will retry with fresh browser...")
                        logger.info("")
                        continue
                    else:
                        logger.error(f"   Failed after {max_retries + 1} attempts")
                        import traceback
                        logger.debug(traceback.format_exc())
                        return None

        finally:
            # ALWAYS close the driver for this store
            # EXACT same as foodpanda_sku_scraper.py finally block
            if driver:
                logger.info("🔒 Closing browser for this store...")
                try:
                    driver.quit()
                    logger.info("✅ Browser closed")
                except:
                    pass  # Ignore errors when closing
                logger.info("")

        return None

    # ---------- Router ----------

    def scrape_store_rating(self, url: str, platform: str) -> Optional[Dict[str, Any]]:
        if "grab.com" in url or platform == "grabfood":
            return self.scrape_grabfood_rating(url)
        if "foodpanda" in url or platform == "foodpanda":
            return self.scrape_foodpanda_rating(url)
        logger.error(f"Unknown platform for URL: {url}")
        return None

    # ---------- Helpers ----------

    def _extract_store_name_from_url(self, url: str, platform: str) -> str:
        """Extract store name from URL for debug file naming."""
        try:
            if platform == "grabfood" or "grab.com" in url:
                match = re.search(r'/restaurant/([^/]+)', url)
                if match:
                    return match.group(1).replace('-', ' ').title()
            elif platform == "foodpanda" or "foodpanda" in url:
                parts = url.rstrip("/").split("/")
                return (parts[-1] or parts[-2] or "store").replace("-", " ").title()
        except Exception:
            pass
        return "Unknown_Store"

    def close(self):
        """Cleanup all drivers."""
        self.close_grabfood_driver()


# ========================= Orchestrator =====================
# DB logic completely unchanged from original

class RatingMonitor:
    def __init__(self):
        self.scraper = RatingScraper()
        self.timezone = config.get_timezone()

    def should_scrape_today(self) -> bool:
        try:
            with db.get_connection() as conn:
                cur = conn.cursor()
                cur.execute("SELECT MAX(last_scraped_at) FROM current_store_ratings")
                row = cur.fetchone()
                if not row or not row[0]:
                    logger.info("📊 No previous scrape - will scrape today")
                    return True
                last_scrape = row[0]
                if isinstance(last_scrape, str):
                    last_scrape = datetime.fromisoformat(last_scrape.replace('Z', '+00:00'))
                if getattr(last_scrape, "tzinfo", None):
                    last_scrape = last_scrape.replace(tzinfo=None)
                days_since = (datetime.now() - last_scrape).days
                if days_since >= 3:
                    logger.info(f"📊 Last scrape: {days_since} days ago - will scrape")
                    return True
                logger.info(f"📊 Last scrape: {days_since} days ago - skip (next in {3 - days_since} days)")
                return False
        except Exception as e:
            logger.error(f"Error checking schedule: {e}")
            return True

    def load_store_urls(self) -> List[Dict[str, str]]:
        try:
            with open(config.STORE_URLS_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
            stores = []
            for url in data.get("urls", []):
                if "grab.com" in url:
                    platform = "grabfood"
                elif "foodpanda" in url:
                    platform = "foodpanda"
                else:
                    continue
                stores.append({"url": url, "platform": platform})
            logger.info(f"📋 Loaded {len(stores)} stores from {config.STORE_URLS_FILE}")
            logger.info(f"   🛒 GrabFood: {sum(1 for s in stores if s['platform']=='grabfood')}")
            logger.info(f"   🐼 Foodpanda: {sum(1 for s in stores if s['platform']=='foodpanda')}")
            return stores
        except Exception as e:
            logger.error(f"Failed to load store URLs: {e}")
            return []

    def extract_store_name(self, url: str) -> str:
        try:
            if "grab.com" in url:
                m = re.search(r"/restaurant/([^/]+)", url)
                if m:
                    name = m.group(1).replace("-delivery", "").replace("-", " ").title()
                    return f"Cocopan {name}" if not name.lower().startswith("cocopan") else name
            elif "foodpanda" in url:
                parts = (url.rstrip("/").split("/"))
                last = parts[-1] or (len(parts) > 1 and parts[-2]) or "Store"
                name = str(last).replace("-", " ").title()
                return f"Cocopan {name}" if not name.lower().startswith("cocopan") else name
        except Exception:
            pass
        return "Cocopan Store (Unknown)"

    def scrape_all_stores(self) -> Dict[str, Any]:
        logger.info("=" * 70)
        logger.info("🌟 STORE RATING SCRAPING STARTED (Selenium mode)")
        logger.info("=" * 70)

        stores = self.load_store_urls()
        if not stores:
            logger.error("❌ No stores to scrape!")
            return {"success": False, "error": "No stores loaded"}

        results = {
            "total_stores": len(stores),
            "successful": 0,
            "failed": 0,
            "scraper_blocked": 0,
            "alerts_created": 0,
            "store_results": []
        }

        try:
            for i, store in enumerate(stores, 1):
                url, platform = store["url"], store["platform"]
                platform_emoji = "🛒" if platform == "grabfood" else "🐼"
                logger.info(f"\n[{i}/{len(stores)}] {platform_emoji} {platform.upper()}")
                logger.info(f"   {url}")

                try:
                    data = self.scraper.scrape_store_rating(url, platform)
                    if data and data.get("success"):
                        rating = data["rating"]
                        status = data.get("status", "UNKNOWN")
                        is_active = data.get("is_active")
                        vote_count = data.get("vote_count")
                        store_name = self.extract_store_name(url)

                        store_id = db.get_or_create_store(store_name, url)
                        ok = db.save_store_rating(
                            store_id=store_id,
                            platform=platform,
                            rating=rating,
                            manual_entry=False
                        )
                        if ok:
                            results["successful"] += 1
                            status_emoji = "🟢" if is_active else "🔴" if is_active is False else "⚪"
                            status_text = f" | Status: {status_emoji} {status}" if status != "UNKNOWN" else ""
                            vote_text = f" | Votes: {vote_count}" if vote_count else ""
                            logger.info(f"   ✅ {store_name}: {rating:.1f}★{status_text}{vote_text} (method={data.get('method')})")
                            try:
                                alerts = db.get_rating_alerts(acknowledged=False)
                            except Exception:
                                alerts = []
                            store_alerts = [a for a in alerts if a.get("store_name") == store_name]
                            if store_alerts:
                                results["alerts_created"] += len(store_alerts)
                                for alert in store_alerts:
                                    logger.warning(f"   🚨 ALERT: {alert.get('message')}")
                        else:
                            results["failed"] += 1
                            logger.error("   ❌ Failed to save rating to DB")
                    else:
                        results["scraper_blocked"] += 1
                        logger.warning("   ⚠️ Could not scrape rating (blocked or not found)")

                    results["store_results"].append({
                        "url": url,
                        "platform": platform,
                        "success": bool(data and data.get("success")),
                        "rating": (data or {}).get("rating"),
                        "status": (data or {}).get("status"),
                        "is_active": (data or {}).get("is_active"),
                        "method": (data or {}).get("method")
                    })
                except Exception as e:
                    logger.error(f"   ❌ Error: {e}")
                    results["failed"] += 1

                # Delay between stores - EXACT same as each SKU scraper
                if i < len(stores):
                    if platform == "grabfood":
                        # GrabFood SKU scraper uses: random.uniform(3, 6)
                        delay = random.uniform(3, 6)
                    else:
                        # Foodpanda SKU scraper uses: random.uniform(5, 10)
                        delay = random.uniform(5, 10)
                    logger.info(f"   ⏱️ Waiting {delay:.1f}s…")
                    time.sleep(delay)

        finally:
            # Close shared GrabFood driver at the end
            # EXACT same as GrabFoodScraper finally → scraper.close()
            self.scraper.close()

        logger.info("\n" + "=" * 70)
        logger.info("🌟 SCRAPING COMPLETE")
        logger.info("=" * 70)
        logger.info(f"📊 Results:")
        logger.info(f"   Total stores:     {results['total_stores']}")
        logger.info(f"   ✅ Successful:     {results['successful']}")
        logger.info(f"   ❌ Failed:         {results['failed']}")
        logger.info(f"   🚫 Blocked:        {results['scraper_blocked']}")
        logger.info(f"   🚨 Alerts created: {results['alerts_created']}")
        success_rate = (results["successful"] / results["total_stores"] * 100) if results["total_stores"] else 0.0
        logger.info(f"   📈 Success rate:   {success_rate:.1f}%")
        if results["scraper_blocked"] > 0:
            logger.warning(f"\n⚠️ {results['scraper_blocked']} stores couldn't be scraped (blocked/not found)")
            logger.warning("   Check debug_snapshots/* to inspect responses")
        if results["alerts_created"] > 0:
            logger.warning(f"\n🚨 {results['alerts_created']} rating alerts created! Check dashboard.")
        logger.info("=" * 70)
        return results


# ------------------------- Entrypoint -----------------------

def rating_scraper_job():
    monitor = RatingMonitor()
    if not monitor.should_scrape_today():
        logger.info("⏭️ Skipping rating scrape - not scheduled for today")
        return
    return monitor.scrape_all_stores()


if __name__ == "__main__":
    print("🌟 Manual Rating Scraper (Selenium-based)")
    print("=" * 70)
    results = RatingMonitor().scrape_all_stores()
    print(f"\n✅ Manual scrape complete!")
    print(f"Check your database for {results.get('successful', 0)} new ratings")