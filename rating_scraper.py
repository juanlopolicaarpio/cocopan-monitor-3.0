#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
CocoPan Rating Scraper (API-based)
- Fetches ratings from GrabFood API and Foodpanda HTML
- Gets rating + store status (active/online)
- Retries, randomized backoff, UA rotation
- Debug snapshots for failures/blocks/success
- Writes to DB via database.db
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

import requests
from bs4 import BeautifulSoup

import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

from config import config
from database import db

# ------------------------- Logging -------------------------
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("rating-scraper")

# ------------------------- Utils ---------------------------
DEFAULT_UAS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
]

def ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)

def debug_save_snapshot(prefix: str, url: str, body: str, status: int = None, headers: Dict[str, Any] = None):
    """Save response for later inspection."""
    try:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
        host = urlparse(url).netloc.replace(":", "_") or "unknown"
        ensure_dir("debug_snapshots")
        base = f"debug_snapshots/{prefix}_{host}_{ts}"
        
        # Save response body
        ext = ".json" if "application/json" in str(headers.get("Content-Type", "")) else ".html"
        with open(base + ext, "w", encoding="utf-8") as f:
            f.write(body or "")
        
        # Save metadata
        with open(base + ".meta.txt", "w", encoding="utf-8") as f:
            f.write(
                f"URL: {url}\n"
                f"Status: {status}\n"
                f"Headers: {dict(headers or {})}\n"
                f"Length: {len(body or '')}\n"
            )
        logger.debug(f"[dbg] Saved snapshot → {base}{ext}")
    except Exception as e:
        logger.debug(f"[dbg] Snapshot save failed: {e}")

def looks_like_bot_block(response_text: str, headers: Dict[str, Any]) -> Optional[str]:
    """Heuristic check for blocks/gates."""
    try:
        text = (response_text or "").lower()
    except Exception:
        text = ""
    server = (headers or {}).get("Server", "")
    if "captcha" in text or "are you human" in text:
        return "captcha"
    if "cloudflare" in text or server.lower().startswith("cloudflare"):
        if "checking your browser" in text or "just a moment" in text:
            return "cloudflare_challenge"
    if "temporarily blocked" in text or "unusual traffic" in text:
        return "rate_limited"
    return None

def pick_ua(uas: List[str]) -> str:
    return random.choice(uas)

# ------------------------- GrabFood API Extractor ----------
def extract_grabfood_merchant_id(url: str) -> Optional[str]:
    """Extract merchant ID from GrabFood URL (e.g., '2-C6K2GPUYKYT3LE')"""
    try:
        parsed = urlparse(url)
        # Pattern: /restaurant/store-name/2-MERCHANTID or /2-MERCHANTID
        match = re.search(r'/([0-9]-[A-Z0-9]+)$', parsed.path, re.IGNORECASE)
        if match:
            return match.group(1)
        return None
    except Exception as e:
        logger.debug(f"Error extracting GrabFood merchant ID: {e}")
        return None

def fetch_grabfood_api(merchant_id: str, attempt: int = 1) -> Optional[Dict[str, Any]]:
    """Fetch data from GrabFood API endpoint."""
    # Manila coordinates (can be adjusted)
    latlng = "14.5995,120.9842"
    
    # Try both API endpoints
    api_urls = [
        f"https://portal.grab.com/foodweb/v2/restaurant?merchantCode={merchant_id}&latlng={latlng}",
        f"https://portal.grab.com/foodweb/v2/merchants/{merchant_id}?latlng={latlng}"
    ]
    
    session = requests.Session()
    session.headers.update({
        'User-Agent': pick_ua(DEFAULT_UAS),
        'Accept': 'application/json, text/plain, */*',
        'Accept-Language': 'en-PH,en;q=0.9',
        'Origin': 'https://food.grab.com',
        'Referer': 'https://food.grab.com/',
        'Connection': 'keep-alive',
    })
    
    # Add delay to avoid rate limiting
    time.sleep(random.uniform(0.8, 2.2) + (attempt - 1) * random.uniform(0.5, 1.0))
    
    for api_url in api_urls:
        try:
            resp = session.get(api_url, timeout=15, allow_redirects=True)
            
            if resp.status_code == 200:
                try:
                    data = resp.json()
                    return {
                        "status": resp.status_code,
                        "headers": dict(resp.headers),
                        "url": resp.url,
                        "json": data,
                        "text": resp.text
                    }
                except json.JSONDecodeError:
                    logger.debug(f"Non-JSON response from {api_url}")
                    continue
            else:
                logger.debug(f"API returned status {resp.status_code} for {api_url}")
                
        except requests.exceptions.SSLError:
            try:
                resp = session.get(api_url, timeout=15, allow_redirects=True, verify=False)
                if resp.status_code == 200:
                    data = resp.json()
                    return {
                        "status": resp.status_code,
                        "headers": dict(resp.headers),
                        "url": resp.url,
                        "json": data,
                        "text": resp.text
                    }
            except Exception as e:
                logger.debug(f"SSL retry failed: {e}")
                
        except Exception as e:
            logger.debug(f"API request error: {e}")
            continue
    
    return None

def extract_grabfood_rating_from_json(json_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Extract rating and status from GrabFood API JSON response."""
    try:
        # The JSON structure can vary, so we'll check multiple possible paths
        
        # Try direct access first
        if "rating" in json_data:
            rating = json_data.get("rating")
            status = json_data.get("status", "UNKNOWN")
            vote_count = json_data.get("voteCount")
            
            if rating is not None:
                try:
                    rating_float = float(rating)
                    if 0.0 <= rating_float <= 5.0:
                        is_active = status == "ACTIVE"
                        return {
                            "rating": rating_float,
                            "status": status,
                            "is_active": is_active,
                            "vote_count": vote_count
                        }
                except (ValueError, TypeError):
                    pass
        
        # Try nested merchant object
        merchant = json_data.get("merchant") or json_data.get("restaurant")
        if isinstance(merchant, dict):
            rating = merchant.get("rating")
            status = merchant.get("status", "UNKNOWN")
            vote_count = merchant.get("voteCount")
            
            if rating is not None:
                try:
                    rating_float = float(rating)
                    if 0.0 <= rating_float <= 5.0:
                        is_active = status == "ACTIVE"
                        return {
                            "rating": rating_float,
                            "status": status,
                            "is_active": is_active,
                            "vote_count": vote_count
                        }
                except (ValueError, TypeError):
                    pass
        
        # Try deep search through the JSON
        def deep_search(obj, depth=0, max_depth=5):
            if depth > max_depth:
                return None
            
            if isinstance(obj, dict):
                # Check if this dict has rating
                if "rating" in obj:
                    rating = obj.get("rating")
                    status = obj.get("status", "UNKNOWN")
                    vote_count = obj.get("voteCount")
                    
                    try:
                        rating_float = float(rating)
                        if 0.0 <= rating_float <= 5.0:
                            is_active = status == "ACTIVE"
                            return {
                                "rating": rating_float,
                                "status": status,
                                "is_active": is_active,
                                "vote_count": vote_count
                            }
                    except (ValueError, TypeError):
                        pass
                
                # Search nested values
                for value in obj.values():
                    result = deep_search(value, depth + 1, max_depth)
                    if result:
                        return result
            
            elif isinstance(obj, list):
                for item in obj:
                    result = deep_search(item, depth + 1, max_depth)
                    if result:
                        return result
            
            return None
        
        return deep_search(json_data)
        
    except Exception as e:
        logger.debug(f"Error extracting rating from JSON: {e}")
        return None

# ------------------------- Foodpanda HTML Extractors --------
def parse_next_data_for_rating(soup: BeautifulSoup) -> Optional[float]:
    script = soup.find("script", id="__NEXT_DATA__", type="application/json")
    if not script or not script.string:
        return None
    try:
        data = json.loads(script.string)

        def walk(obj):
            if isinstance(obj, dict):
                for k in ("rating", "ratingValue"):
                    if k in obj:
                        try:
                            v = float(obj[k])
                            if 0.0 <= v <= 5.0:
                                return v
                        except Exception:
                            pass
                for v in obj.values():
                    r = walk(v)
                    if r is not None:
                        return r
            elif isinstance(obj, list):
                for v in obj:
                    r = walk(v)
                    if r is not None:
                        return r
            return None

        return walk(data)
    except Exception:
        return None

def parse_json_ld_for_rating(soup: BeautifulSoup) -> Optional[float]:
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
                        return v
                except Exception:
                    pass
            # deep walk
            def walk(o):
                if isinstance(o, dict):
                    if "aggregateRating" in o and isinstance(o["aggregateRating"], dict):
                        try:
                            rv = float(o["aggregateRating"].get("ratingValue"))
                            if 0.0 <= rv <= 5.0:
                                return rv
                        except Exception:
                            pass
                    for vv in o.values():
                        r = walk(vv)
                        if r is not None:
                            return r
                elif isinstance(o, list):
                    for vv in o:
                        r = walk(vv)
                        if r is not None:
                            return r
                return None
            deep = walk(obj)
            if deep is not None:
                return deep
    return None

def extract_rating_from_html(html: str) -> Optional[float]:
    soup = BeautifulSoup(html, "html.parser")
    # __NEXT_DATA__ first
    r = parse_next_data_for_rating(soup)
    if isinstance(r, float):
        return r
    # JSON-LD next
    r = parse_json_ld_for_rating(soup)
    if isinstance(r, float):
        return r
    # text fallback (very loose)
    text = soup.get_text(" ", strip=True)
    m = re.search(r'(\d\.\d)\s*(?:/|out of)?\s*5', text, re.I)
    if m:
        try:
            r = float(m.group(1))
            if 0.0 <= r <= 5.0:
                return r
        except Exception:
            pass
    return None

# ------------------------- Network -------------------------
def fetch_html(url: str, attempt: int = 1) -> Optional[Dict[str, Any]]:
    session = requests.Session()
    session.headers.update({
        'User-Agent': pick_ua(DEFAULT_UAS),
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive',
    })
    time.sleep(random.uniform(0.8, 2.2) + (attempt - 1) * random.uniform(0.5, 1.0))
    try:
        resp = session.get(url, timeout=20, allow_redirects=True)
    except requests.exceptions.SSLError:
        resp = session.get(url, timeout=20, allow_redirects=True, verify=False)
    except Exception as e:
        logger.debug(f"[fetch] requests error: {e}")
        return None
    return {"status": resp.status_code, "headers": dict(resp.headers), "url": resp.url, "text": resp.text}

# ------------------------- Scraper Core ---------------------
class RatingScraper:
    def scrape_grabfood_rating(self, url: str) -> Optional[Dict[str, Any]]:
        """Scrape GrabFood rating using API endpoint."""
        tries = 3
        
        # Extract merchant ID
        merchant_id = extract_grabfood_merchant_id(url)
        if not merchant_id:
            logger.error(f"Could not extract merchant ID from URL: {url}")
            return None
        
        logger.debug(f"   Merchant ID: {merchant_id}")
        
        for attempt in range(1, tries + 1):
            # Fetch from API
            api_response = fetch_grabfood_api(merchant_id, attempt)
            
            if not api_response:
                logger.debug(f"   Attempt {attempt}/{tries}: No API response")
                continue
            
            if api_response["status"] != 200:
                debug_save_snapshot("grab_http", url, api_response.get("text", ""), 
                                  api_response["status"], api_response["headers"])
                logger.debug(f"   Attempt {attempt}/{tries}: HTTP {api_response['status']}")
                continue
            
            # Check for bot blocks
            blocked = looks_like_bot_block(api_response.get("text", ""), api_response["headers"])
            if blocked:
                logger.warning(f"   GrabFood blocked ({blocked})")
                debug_save_snapshot("grab_blocked", url, api_response.get("text", ""), 
                                  api_response["status"], api_response["headers"])
                continue
            
            # Extract rating from JSON
            json_data = api_response.get("json")
            if json_data:
                result = extract_grabfood_rating_from_json(json_data)
                if result and "rating" in result:
                    debug_save_snapshot("grab_success", url, api_response.get("text", ""), 
                                      api_response["status"], api_response["headers"])
                    return {
                        "rating": result["rating"],
                        "status": result.get("status", "UNKNOWN"),
                        "is_active": result.get("is_active", False),
                        "vote_count": result.get("vote_count"),
                        "success": True,
                        "method": "GrabFood_API"
                    }
                else:
                    logger.debug(f"   Attempt {attempt}/{tries}: No rating in JSON")
                    debug_save_snapshot("grab_norating", url, api_response.get("text", ""), 
                                      api_response["status"], api_response["headers"])
            else:
                logger.debug(f"   Attempt {attempt}/{tries}: No JSON data")
        
        return None

    def scrape_foodpanda_rating(self, url: str) -> Optional[Dict[str, Any]]:
        """Scrape Foodpanda rating from HTML."""
        tries = 3
        for attempt in range(1, tries + 1):
            r = fetch_html(url, attempt)
            if not r:
                continue
            if r["status"] != 200:
                debug_save_snapshot("fp_http", url, r.get("text"), r["status"], r["headers"])
                continue
            blocked = looks_like_bot_block(r["text"], r["headers"])
            if blocked:
                logger.warning(f"Foodpanda blocked ({blocked}); snapshot saved.")
                debug_save_snapshot("fp_blocked", url, r["text"], r["status"], r["headers"])
                continue
            rating = extract_rating_from_html(r["text"])
            if isinstance(rating, float):
                debug_save_snapshot("fp_success", url, r["text"], r["status"], r["headers"])
                # Note: Foodpanda HTML scraping doesn't provide status info easily
                return {
                    "rating": rating,
                    "status": "UNKNOWN",
                    "is_active": None,
                    "success": True,
                    "method": "__NEXT_DATA__/JSON-LD"
                }
            debug_save_snapshot("fp_norating", url, r["text"], r["status"], r["headers"])
        return None

    def scrape_store_rating(self, url: str, platform: str) -> Optional[Dict[str, Any]]:
        if "grab.com" in url or platform == "grabfood":
            return self.scrape_grabfood_rating(url)
        if "foodpanda" in url or platform == "foodpanda":
            return self.scrape_foodpanda_rating(url)
        logger.error(f"Unknown platform for URL: {url}")
        return None

# ------------------------- Orchestrator ---------------------
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
        logger.info("🌟 STORE RATING SCRAPING STARTED")
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

            if i < len(stores):
                delay = random.uniform(3, 6)
                logger.debug(f"   ⏱️ Waiting {delay:.1f}s…")
                time.sleep(delay)

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
    print("🌟 Manual Rating Scraper (API-based for GrabFood)")
    print("=" * 70)
    results = RatingMonitor().scrape_all_stores()
    print("\n✅ Manual scrape complete!")
    print(f"Check your database for {results.get('successful', 0)} new ratings")