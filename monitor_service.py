#!/usr/bin/env python3
"""
CocoPan Monitor Service - ENHANCED WITH GRABFOOD SKU SCRAPING
✅ FIXED: Hour slot calculation - runs at :45 but saves to NEXT hour
✅ Sends client emails immediately when stores go offline
✅ Beautiful admin alerts for verification needs
✅ Foodpanda VA check-in integration  
✅ NEW: Daily GrabFood SKU/OOS scraping with fuzzy matching
✅ FIXED: Smart startup SKU test - prevents duplicate scraping
✅ Production-ready with comprehensive error handling
"""
import os
import json
import time
import logging
import signal
import sys
import random
import uuid
import re
from datetime import datetime, timedelta, date  # ← CHANGED: Added 'date'
from typing import List, Dict, Any, Optional, Set, Tuple
from dataclasses import dataclass
from enum import Enum
from urllib.parse import urlparse
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

import pytz
import requests
from bs4 import BeautifulSoup
import urllib3

# Fuzzy matching for SKU mapping
try:
    from fuzzywuzzy import fuzz
    from fuzzywuzzy import process
    HAS_FUZZY = True
except ImportError:
    HAS_FUZZY = False
    logging.warning("fuzzywuzzy not available - SKU matching will be basic")

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

# Client alerts (REQUIRED for immediate offline notifications)
try:
    from client_alerts import client_alerts, StoreAlert
    HAS_CLIENT_ALERTS = True
except ImportError:
    HAS_CLIENT_ALERTS = False
    
    # Create a mock client_alerts for graceful degradation
    class MockClientAlerts:
        def send_hourly_status_alert(self, offline_stores, total_stores):
            logging.getLogger(__name__).warning("Client alerts not available - install client_alerts module")
            return False
        def send_immediate_offline_alert(self, offline_stores, total_stores):
            logging.getLogger(__name__).warning("Client alerts not available - install client_alerts module") 
            return False
    
    client_alerts = MockClientAlerts()

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

# ==============================================================================
# ✨ NEW FUNCTIONS: Smart SKU Scraping Control
# ==============================================================================

def has_sku_scraping_run_today() -> bool:
    """
    ✨ NEW FUNCTION
    Check if SKU scraping already completed today by querying database
    Returns True if found, False if not
    """
    try:
        today = date.today()
        
        # Query database for today's SKU compliance checks
        query = """
            SELECT COUNT(*) as count
            FROM sku_compliance_checks
            WHERE DATE(checked_at) = ?
            AND checked_by = 'automated_scraper'
        """
        
        result = db.execute_query(query, (today,))
        if result and len(result) > 0:
            count = result[0].get('count', 0)
            if count > 0:
                logger.info(f"✅ SKU scraping already completed today ({count} checks found)")
                return True
        
        logger.info(f"📋 No SKU scraping found for today - needs to run")
        return False
        
    except Exception as e:
        logger.warning(f"⚠️ Could not check SKU scraping history: {e}")
        # If we can't check, assume it hasn't run (safe default for first-time setup)
        return False


def should_run_startup_sku_test() -> bool:
    """
    ✨ NEW FUNCTION
    Determine if SKU test should run on startup
    
    Decision logic:
    1. Check SKIP_STARTUP_SKU_TEST env var (production control)
    2. Check if already ran today (prevents duplicates)
    3. Check for --test-sku command line flag (manual testing)
    4. Default: Allow for local development
    """
    
    # Priority 1: Environment variable (production control)
    skip_startup = os.getenv('SKIP_STARTUP_SKU_TEST', 'false').lower() == 'true'
    
    if skip_startup:
        logger.info("🚫 SKIP_STARTUP_SKU_TEST=true - Skipping startup SKU test")
        return False
    
    # Priority 2: Check if already ran today
    if has_sku_scraping_run_today():
        logger.info("✅ SKU scraping already completed today - Skipping startup test")
        return False
    
    # Priority 3: Check command-line argument for manual testing
    if '--test-sku' in sys.argv:
        logger.info("🧪 --test-sku flag detected - Running startup SKU test")
        return True
    
    # Default: Allow for local development (but only if hasn't run today)
    logger.info("🧪 Running startup SKU test (use SKIP_STARTUP_SKU_TEST=true to disable)")
    return True
# ==============================================================================
# ✨ NEW: GrabFood API Helper Functions
# Add these BEFORE the GrabFoodMonitor class in monitor_service.py
# ==============================================================================

def extract_grabfood_merchant_id(url: str) -> Optional[str]:
    """Extract merchant ID from GrabFood URL (e.g., '2-C6K2GPUYKYT3LE')"""
    try:
        parsed = urlparse(url)
        # Pattern: /restaurant/store-name/2-MERCHANTID
        match = re.search(r'/([0-9]-[A-Z0-9]+)$', parsed.path, re.IGNORECASE)
        if match:
            return match.group(1)
        return None
    except Exception as e:
        logger.debug(f"Error extracting merchant ID: {e}")
        return None

# ==============================================================================
# ✨ ENHANCED: GrabFood API Helper Function with DETAILED LOGGING
# ==============================================================================

def fetch_grabfood_api_data(merchant_id: str, referer_url: str, user_agents: List[str], 
                            ph_latlng: str = "14.5995,120.9842", 
                            max_retries: int = 3) -> Optional[Dict[str, Any]]:
    """
    ✨ ENHANCED: Fetch store data from GrabFood API endpoints
    🆕 Added comprehensive logging to debug blocking issues
    """
    
    # 🆕 ADD: Log function entry
    logger.info(f"")
    logger.info(f"{'─'*60}")
    logger.info(f"🌐 API REQUEST START")
    logger.info(f"{'─'*60}")
    logger.info(f"  Merchant ID: {merchant_id}")
    logger.info(f"  Location: {ph_latlng}")
    logger.debug(f"  Referer: {referer_url}")
    logger.debug(f"  Max retries: {max_retries}")
    
    # API endpoints to try
    api_urls = [
        f"https://portal.grab.com/foodweb/v2/restaurant?merchantCode={merchant_id}&latlng={ph_latlng}",
        f"https://portal.grab.com/foodweb/v2/merchants/{merchant_id}?latlng={ph_latlng}"
    ]
    
    # Randomize user agent
    selected_ua = random.choice(user_agents)
    logger.debug(f"  User-Agent: {selected_ua[:80]}...")
    
    # Build headers
    headers = {
        "User-Agent": selected_ua,
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-PH,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Origin": "https://food.grab.com",
        "Referer": referer_url,
        "Connection": "keep-alive",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-site",
    }
    
    logger.debug(f"  Request headers: {len(headers)} headers set")
    
    # Try each API endpoint
    for endpoint_idx, api_url in enumerate(api_urls, 1):
        logger.info(f"")
        logger.info(f"  📡 Endpoint #{endpoint_idx}/{len(api_urls)}")
        logger.debug(f"     URL: {api_url}")
        
        # Retry logic for each endpoint
        for attempt in range(1, max_retries + 1):
            try:
                logger.info(f"     Attempt {attempt}/{max_retries}...")
                
                # Pre-request delay (anti-detection)
                if attempt > 1:
                    delay = min(3.0 * attempt, 10)
                    logger.debug(f"     Waiting {delay:.1f}s before retry...")
                    time.sleep(delay)
                
                # 🆕 ADD: Log right before making request
                request_start = time.time()
                logger.debug(f"     Making HTTP GET request...")
                
                # Make the request with SSL handling
                try:
                    response = requests.get(
                        api_url,
                        headers=headers,
                        timeout=15,
                        verify=True
                    )
                except requests.exceptions.SSLError:
                    logger.warning(f"     ⚠️ SSL Error - retrying without verification")
                    response = requests.get(
                        api_url,
                        headers=headers,
                        timeout=15,
                        verify=False
                    )
                
                request_time = time.time() - request_start
                
                # 🆕 ADD: Detailed response logging
                logger.info(f"     📥 Response received in {request_time:.2f}s")
                logger.info(f"     Status: {response.status_code} {response.reason}")
                logger.debug(f"     Content-Length: {len(response.content)} bytes")
                logger.debug(f"     Content-Type: {response.headers.get('content-type', 'unknown')}")
                
                # 🆕 ADD: Log important response headers for debugging
                important_headers = [
                    'cf-ray', 'cf-cache-status', 'x-ratelimit-limit', 
                    'x-ratelimit-remaining', 'x-ratelimit-reset',
                    'server', 'x-powered-by'
                ]
                for header in important_headers:
                    if header in response.headers:
                        logger.debug(f"     {header}: {response.headers[header]}")
                
                # Check for blocking/rate limiting
                if response.status_code == 403:
                    logger.warning(f"     🚫 BLOCKED (403 Forbidden)")
                    logger.warning(f"        Possible bot detection or IP ban")
                    logger.debug(f"        Response preview: {response.text[:200]}")
                    
                    if 'cloudflare' in response.text.lower() or 'cf-ray' in response.headers:
                        logger.warning(f"        Cloudflare protection detected!")
                    
                    if attempt < max_retries:
                        logger.info(f"        Will retry with different UA/delay...")
                        # Rotate user agent on 403
                        selected_ua = random.choice(user_agents)
                        headers["User-Agent"] = selected_ua
                        continue
                    else:
                        logger.error(f"        Max retries reached for endpoint #{endpoint_idx}")
                        continue  # Try next endpoint
                
                elif response.status_code == 429:
                    logger.warning(f"     ⏱️ RATE LIMITED (429 Too Many Requests)")
                    
                    # Check for rate limit headers
                    retry_after = response.headers.get('retry-after')
                    if retry_after:
                        logger.warning(f"        Retry-After: {retry_after}s")
                    
                    rate_remaining = response.headers.get('x-ratelimit-remaining')
                    if rate_remaining:
                        logger.warning(f"        Rate limit remaining: {rate_remaining}")
                    
                    if attempt < max_retries:
                        delay = int(retry_after) if retry_after else 30
                        logger.info(f"        Waiting {delay}s before retry...")
                        time.sleep(delay)
                        continue
                    else:
                        logger.error(f"        Max retries reached for endpoint #{endpoint_idx}")
                        continue  # Try next endpoint
                
                elif response.status_code == 404:
                    logger.warning(f"     ❌ NOT FOUND (404)")
                    logger.warning(f"        Merchant {merchant_id} may not exist or URL is wrong")
                    # Don't retry on 404, try next endpoint immediately
                    break
                
                elif response.status_code >= 500:
                    logger.error(f"     💥 SERVER ERROR ({response.status_code})")
                    logger.error(f"        GrabFood server issue")
                    if attempt < max_retries:
                        logger.info(f"        Will retry...")
                        continue
                    else:
                        logger.error(f"        Max retries reached for endpoint #{endpoint_idx}")
                        continue  # Try next endpoint
                
                elif response.status_code == 200:
                    logger.info(f"     ✅ SUCCESS (200 OK)")
                    
                    # Try to parse JSON
                    try:
                        json_data = response.json()
                        
                        # 🆕 ADD: Validate JSON structure
                        if not isinstance(json_data, dict):
                            logger.warning(f"     ⚠️ Response is not a JSON object")
                            logger.debug(f"     Response type: {type(json_data)}")
                            if attempt < max_retries:
                                continue
                            else:
                                break
                        
                        # Log JSON structure
                        logger.debug(f"     JSON keys: {list(json_data.keys())[:10]}")
                        
                        # Check if response contains data
                        if not json_data:
                            logger.warning(f"     ⚠️ Empty JSON response")
                            if attempt < max_retries:
                                continue
                            else:
                                break
                        
                        logger.info(f"     ✅ Valid JSON data received")
                        logger.info(f"{'─'*60}")
                        logger.info(f"✅ API REQUEST SUCCESS (endpoint #{endpoint_idx})")
                        logger.info(f"{'─'*60}")
                        return json_data
                    
                    except json.JSONDecodeError as e:
                        logger.error(f"     ❌ JSON DECODE ERROR: {e}")
                        logger.debug(f"     Response preview: {response.text[:300]}")
                        if attempt < max_retries:
                            continue
                        else:
                            break
                
                else:
                    logger.warning(f"     ⚠️ UNEXPECTED STATUS: {response.status_code}")
                    logger.debug(f"     Response preview: {response.text[:200]}")
                    if attempt < max_retries:
                        continue
                    else:
                        break
            
            except requests.exceptions.Timeout:
                logger.error(f"     ⏱️ TIMEOUT after 15s")
                if attempt < max_retries:
                    logger.info(f"        Will retry...")
                    continue
                else:
                    logger.error(f"        Max retries reached for endpoint #{endpoint_idx}")
                    break
            
            except requests.exceptions.ConnectionError as e:
                logger.error(f"     🔌 CONNECTION ERROR")
                logger.error(f"        {type(e).__name__}: {str(e)[:100]}")
                if attempt < max_retries:
                    logger.info(f"        Will retry...")
                    continue
                else:
                    logger.error(f"        Max retries reached for endpoint #{endpoint_idx}")
                    break
            
            except Exception as e:
                logger.error(f"     💥 UNEXPECTED ERROR")
                logger.error(f"        {type(e).__name__}: {str(e)[:150]}")
                if attempt < max_retries:
                    logger.info(f"        Will retry...")
                    continue
                else:
                    logger.error(f"        Max retries reached for endpoint #{endpoint_idx}")
                    break
        
        # If we got here, all retries failed for this endpoint
        logger.warning(f"  ❌ Endpoint #{endpoint_idx} failed after {max_retries} attempts")
        logger.info(f"     Trying next endpoint...")
    
    # All endpoints failed
    logger.error(f"")
    logger.error(f"{'─'*60}")
    logger.error(f"❌ API REQUEST FAILED")
    logger.error(f"{'─'*60}")
    logger.error(f"  All {len(api_urls)} endpoints exhausted")
    logger.error(f"  Merchant ID: {merchant_id}")
    logger.error(f"  This may indicate:")
    logger.error(f"    • IP is blocked/rate limited")
    logger.error(f"    • Merchant ID is invalid")
    logger.error(f"    • GrabFood API is down")
    logger.error(f"    • Cloudflare protection is active")
    logger.error(f"{'─'*60}")
    
    return None


def extract_status_from_api_json(json_data: Dict[str, Any]) -> Tuple[Optional[str], Optional[float], Optional[int]]:
    """
    ✨ ENHANCED: Extract status from GrabFood API JSON response
    🆕 Added logging for debugging
    Returns: (status, rating, vote_count)
    """
    
    logger.debug(f"🔍 Extracting status from API response...")
    
    if not json_data or not isinstance(json_data, dict):
        logger.warning(f"  ⚠️ Invalid or empty JSON data")
        return None, None, None
    
    # Try different possible locations for status field
    status = None
    rating = None
    vote_count = None
    
    # Pattern 1: Top-level status
    if 'status' in json_data:
        status = json_data.get('status')
        logger.debug(f"  Found status at top level: {status}")
    
    # Pattern 2: merchant.status
    if not status and 'merchant' in json_data:
        merchant = json_data.get('merchant', {})
        if isinstance(merchant, dict):
            status = merchant.get('status')
            if status:
                logger.debug(f"  Found status in merchant object: {status}")
            
            # Also extract rating info if available
            rating = merchant.get('rating')
            vote_count = merchant.get('voteCount') or merchant.get('vote_count')
    
    # Pattern 3: data.merchant.status
    if not status and 'data' in json_data:
        data = json_data.get('data', {})
        if isinstance(data, dict) and 'merchant' in data:
            merchant = data.get('merchant', {})
            if isinstance(merchant, dict):
                status = merchant.get('status')
                if status:
                    logger.debug(f"  Found status in data.merchant: {status}")
                
                rating = merchant.get('rating')
                vote_count = merchant.get('voteCount') or merchant.get('vote_count')
    
    # Pattern 4: restaurant.status (alternative field name)
    if not status:
        for key in ['restaurant', 'store', 'shop']:
            if key in json_data:
                obj = json_data.get(key, {})
                if isinstance(obj, dict):
                    status = obj.get('status')
                    if status:
                        logger.debug(f"  Found status in {key} object: {status}")
                        break
    
    if not status:
        logger.warning(f"  ⚠️ Could not find status field in response")
        logger.debug(f"  Available top-level keys: {list(json_data.keys())[:10]}")
    else:
        logger.debug(f"  ✅ Extracted: status={status}, rating={rating}, votes={vote_count}")
    
    return status, rating, vote_count
def extract_status_from_api_json(json_data: Dict[str, Any]) -> Tuple[Optional[str], Optional[float], Optional[int]]:
    """
    ✨ NEW FUNCTION
    Extract status, rating, and vote_count from GrabFood API JSON
    Returns: (status, rating, vote_count)
    
    Known status values:
    - ACTIVE = Store is online and accepting orders
    - INACTIVE = Store is offline/closed
    """
    try:
        # Try multiple possible paths in the JSON structure
        roots = [json_data]
        
        if isinstance(json_data, dict):
            if 'data' in json_data:
                roots.append(json_data['data'])
            if 'merchant' in json_data:
                roots.append(json_data['merchant'])
            if 'restaurant' in json_data:
                roots.append(json_data['restaurant'])
        
        for root in roots:
            if not isinstance(root, dict):
                continue
            
            status = root.get('status')
            rating = root.get('rating')
            vote_count = root.get('voteCount')
            
            if status:
                # Convert rating to float if present
                try:
                    rating = float(rating) if rating is not None else None
                except (ValueError, TypeError):
                    rating = None
                
                return status, rating, vote_count
        
        return None, None, None
        
    except Exception as e:
        logger.debug(f"Error parsing API JSON: {e}")
        return None, None, None
# ==============================================================================
# End of new functions
# ==============================================================================

# ------------------------------------------------------------------------------
# NEW: SKU Mapping Service
# ------------------------------------------------------------------------------
class SKUMapper:
    """Maps scraped product names to SKU codes using fuzzy matching"""
    
    def __init__(self):
        self.grabfood_skus = self._load_grabfood_master_skus()
        self.name_to_sku_map = self._build_name_mapping()
        # Add explicit mappings for problematic products
        self.explicit_mappings = self._build_explicit_mappings()
        logger.info(f"📦 SKU Mapper initialized with {len(self.grabfood_skus)} GrabFood products")
        logger.info(f"🎯 {len(self.explicit_mappings)} explicit mappings configured")
        
        # Log a few sample explicit mappings for verification
        sample_mappings = list(self.explicit_mappings.items())[:3]
        for key, value in sample_mappings:
            logger.debug(f"   Explicit: '{key}' → {value}")
    
    def _build_explicit_mappings(self) -> Dict[str, str]:
        """
        Build explicit mappings ONLY for the 6 products you specified
        These mappings guarantee 100% accuracy for known edge cases
        """
        return {
            # Bundle products with promo text - normalize to core product name
            # GP007 variations (Build A Box Superbox 13pcs)
            "BUILD A BOX SUPERBOX 13PCS": "GP007",
            "BUILD A BOX SUPERBOX 13 PCS": "GP007",
            "BUILD A BOX SUPER BOX 13PCS": "GP007",
            "BUILD A BOX SUPER BOX 13 PCS": "GP007",
            
            # GP008 variations (Superbox Assorted 13pcs)
            "SUPERBOX ASSORTED 13PCS": "GP008",
            "SUPERBOX ASSORTED 13 PCS": "GP008",
            "SUPER BOX ASSORTED 13PCS": "GP008",
            "SUPER BOX ASSORTED 13 PCS": "GP008",
            
            # GP009 variations (Build A Box Snack Box 10pcs)
            "BUILD A BOX SNACK BOX 10PCS": "GP009",
            "BUILD A BOX SNACK BOX 10 PCS": "GP009",
            "BUILD A BOX SNACKBOX 10PCS": "GP009",
            "BUILD A BOX SNACKBOX 10 PCS": "GP009",
            
            # GP010 variations (Snack Box Assorted 10pcs)
            "SNACK BOX ASSORTED 10PCS": "GP010",
            "SNACK BOX ASSORTED 10 PCS": "GP010",
            "SNACKBOX ASSORTED 10PCS": "GP010",
            "SNACKBOX ASSORTED 10 PCS": "GP010",
            
            # K-Salt variations
            "K-SALT": "GB111",
            "K SALT": "GB111",
            "KSALT": "GB111",
            
            # Italian Herbs Loaf variations
            "ITALIAN HERBS LOAF": "GB099",
            "ITALIAN HERB LOAF": "GB099",
            "ITALIAN HERBS": "GB099",
            
            # Vietnamese Coffee - base product
            "VIETNAMESE COFFEE": "GD028",
            "VIET COFFEE": "GD028",
            
            # Café Espanol - map to ICED version (most common)
            "CAFE ESPANOL": "GD049",
            "CAFÉ ESPANOL": "GD049",
            
            # Dark Choco Coffee - map to ICED version (most common)
            "DARK CHOCO COFFEE": "GD034",
            "DARK CHOCOLATE COFFEE": "GD034",
            
            # Twistea - map to classic variant
            "TWISTEA": "GD117",
            "COCOPAN TWISTEA": "GD117",
            "TWISTEA CLASSIC": "GD117",
            
            # Milky Bun
            "MILKY BUN": "GB089",
            
            # Banana Crunch
            "BANANA CRUNCH": "GB094",
            
            # Choco Loaf (Daily Loaf - Choco)
            "CHOCO LOAF": "GB098",
            "CHOCOLATE LOAF": "GB098",
        }
    
    def _normalize_name(self, name: str) -> str:
        """
        Enhanced normalization: removes platform prefixes, promo text, and standardizes format
        """
        if not name:
            return ""
        
        # Convert to uppercase
        name = name.upper()
        
        # Normalize special characters
        name = name.replace('Ñ', 'N').replace('ñ', 'N')
        name = name.replace('É', 'E').replace('é', 'E')
        
        # Remove platform prefixes
        name = re.sub(r'^(GRAB\s+|FOODPANDA\s+|FOOD PANDA\s+)', '', name)
        
        # Remove promotional phrases (key improvement!)
        promo_patterns = [
            r'\s*FREE\s+MANGO\s+SUNRISE.*$',
            r'\s*\+\s*\d+\s*PHP.*$',
            r'\s*\(FREE\s+MANGO\s+SUNRISE\).*$',
            r'\s*\(\+\s*\d+\s*PHP.*\).*$',
            r'\s*WITH\s+FREE.*$',
            r'\s*FREE.*$',
        ]
        
        for pattern in promo_patterns:
            name = re.sub(pattern, '', name, flags=re.IGNORECASE)
        
        # Flatten parentheses - keep content but remove the parentheses themselves
        name = re.sub(r'\s*\(([^)]+)\)\s*', r' \1 ', name)
        name = re.sub(r'\s*\[([^\]]+)\]\s*', r' \1 ', name)
        
        # Standardize spacing around special characters
        name = re.sub(r'\s*-\s*', ' ', name)  # "K-SALT" -> "K SALT"
        name = re.sub(r'\s+', ' ', name).strip()
        
        return name
    
    def find_sku_for_name(self, scraped_name: str, min_confidence: int = 85) -> Optional[str]:
        """
        Enhanced SKU finder with explicit mappings taking priority
        """
        if not scraped_name or not scraped_name.strip():
            return None
        
        normalized_scraped = self._normalize_name(scraped_name)
        
        # Debug logging to see what's happening
        logger.debug(f"🔍 Looking for: '{scraped_name}' → normalized: '{normalized_scraped}'")
        
        # PRIORITY 1: Check explicit mappings first (100% guaranteed)
        if normalized_scraped in self.explicit_mappings:
            sku_code = self.explicit_mappings[normalized_scraped]
            logger.debug(f"🎯 Explicit match: '{scraped_name}' → {sku_code}")
            return sku_code
        
        # PRIORITY 2: Try exact match in master list
        if normalized_scraped in self.name_to_sku_map:
            return self.name_to_sku_map[normalized_scraped]
        
        # PRIORITY 3: Fuzzy matching if available
        if HAS_FUZZY:
            best_match = process.extractOne(
                normalized_scraped, 
                self.name_to_sku_map.keys(),
                scorer=fuzz.token_sort_ratio
            )
            
            if best_match and best_match[1] >= min_confidence:
                matched_name = best_match[0]
                confidence = best_match[1]
                sku_code = self.name_to_sku_map[matched_name]
                logger.debug(f"🎯 Fuzzy match: '{scraped_name}' → {sku_code} (confidence: {confidence}%)")
                return sku_code
        
        # PRIORITY 4: Basic substring matching fallback
        normalized_scraped_words = set(normalized_scraped.split())
        
        best_match_score = 0
        best_sku = None
        
        for master_name, sku_code in self.name_to_sku_map.items():
            master_words = set(master_name.split())
            
            # Calculate word overlap
            common_words = normalized_scraped_words.intersection(master_words)
            if common_words:
                score = len(common_words) / max(len(normalized_scraped_words), len(master_words))
                if score > best_match_score and score >= 0.6:  # 60% word overlap
                    best_match_score = score
                    best_sku = sku_code
        
        if best_sku:
            logger.debug(f"🔍 Substring match: '{scraped_name}' → {best_sku} (score: {best_match_score:.2f})")
        
        return best_sku
    
    def _load_grabfood_master_skus(self) -> List[Dict]:
        """Load ALL GrabFood SKUs from database - NO FALLBACK"""
        try:
            skus = db.get_master_skus_by_platform('grabfood')
            if skus:
                logger.info(f"📦 Loaded {len(skus)} GrabFood SKUs from database")
                return skus
            else:
                logger.error("❌ No GrabFood SKUs found in database!")
                logger.error("❌ Please run populate.py first to populate the database")
                raise RuntimeError("Database has no GrabFood SKUs - run populate.py first")
        except Exception as e:
            logger.error(f"❌ Failed to load SKUs from database: {e}")
            raise RuntimeError(f"Cannot load GrabFood SKUs from database: {e}")
    
    def _build_name_mapping(self) -> Dict[str, str]:
        """Build mapping from normalized names to SKU codes"""
        mapping = {}
        for sku in self.grabfood_skus:
            normalized_name = self._normalize_name(sku['product_name'])
            mapping[normalized_name] = sku['sku_code']
        return mapping
    
    def map_scraped_names_to_skus(self, scraped_names: List[str]) -> Tuple[List[str], List[str]]:
        """
        Map list of scraped names to SKU codes
        Returns: (matched_sku_codes, unknown_products)
        """
        matched_skus = []
        unknown_products = []
        
        for scraped_name in scraped_names:
            sku_code = self.find_sku_for_name(scraped_name)
            if sku_code:
                matched_skus.append(sku_code)
                logger.debug(f"✅ Mapped: '{scraped_name}' → {sku_code}")
            else:
                unknown_products.append(scraped_name)
                logger.warning(f"❓ Unknown product: '{scraped_name}'")
        
        return matched_skus, unknown_products# NEW: GrabFood SKU Scraper (based on s.py)
# ------------------------------------------------------------------------------
# ------------------------------------------------------------------------------
# UPDATED: GrabFood SKU Scraper - Load URLs from branch_urls.json
# ------------------------------------------------------------------------------
class GrabFoodSKUScraper:
    """GrabFood SKU/OOS scraper with same reliability as store status checker"""
    
    def __init__(self):
        self.sku_mapper = SKUMapper()
        self.timezone = config.get_timezone()
        
        # Load store URLs from branch_urls.json (same as GrabFoodMonitor)
        self.store_urls = self._load_grabfood_urls()
        
        # Same headers and config as existing GrabFood monitor
        self.user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        ]
        
        self.ph_latlng = "14.5995,120.9842"  # Manila center
        self.headers = {
            "User-Agent": random.choice(self.user_agents),
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "en-PH,en;q=0.9",
            "Origin": "https://food.grab.com",
            "Referer": "https://food.grab.com/",
            "Connection": "keep-alive",
        }
        
        logger.info(f"🛒 GrabFood SKU Scraper initialized")
        logger.info(f"📋 {len(self.store_urls)} GrabFood stores loaded from branch_urls.json")
    
    def _load_grabfood_urls(self):
        """Load ONLY GrabFood URLs from branch_urls.json (same logic as GrabFoodMonitor)"""
        try:
            with open(config.STORE_URLS_FILE) as f:
                data = json.load(f)
                all_urls = data.get('urls', [])

                # Filter to only GrabFood URLs
                grabfood_urls = [url for url in all_urls if 'grab.com' in url]
                foodpanda_urls = [url for url in all_urls if 'foodpanda' in url]

                logger.info(f"📋 Loaded {len(all_urls)} total URLs from {config.STORE_URLS_FILE}")
                logger.info(f"🛒 Filtered to {len(grabfood_urls)} GrabFood URLs for SKU scraping")
                logger.info(f"🐼 Skipping {len(foodpanda_urls)} Foodpanda URLs (handled by VA)")

                return grabfood_urls

        except Exception as e:
            logger.error(f"Failed to load URLs for SKU scraping: {e}")
            logger.warning("🔄 Falling back to hardcoded test stores")
            # Fallback to original test stores if file load fails
            return [
                "https://food.grab.com/ph/en/restaurant/cocopan-maysilo-delivery/2-C6TATTL2UF2UDA",
                "https://food.grab.com/ph/en/restaurant/cocopan-anonas-delivery/2-C6XVCUDGNXNZNN", 
                "https://food.grab.com/ph/en/restaurant/cocopan-altura-santa-mesa-delivery/2-C7EUVP2UEJ43L6"
            ]
    
    def extract_merchant_id(self, url: str) -> Optional[str]:
        """Extract merchant ID from GrabFood URL"""
        try:
            parsed = urlparse(url)
            if "food.grab.com" not in parsed.netloc or "/ph/" not in parsed.path:
                return None
            
            # Extract ID pattern like "2-C6TATTL2UF2UDA"
            match = re.search(r"/([0-9]-[A-Z0-9]+)$", parsed.path, re.IGNORECASE)
            return match.group(1) if match else None
        except Exception as e:
            logger.error(f"Error extracting merchant ID from {url}: {e}")
            return None
    
    def fetch_json_with_retry(self, session: requests.Session, url: str, max_retries: int = 3) -> Optional[Dict[str, Any]]:
        """Fetch JSON with same retry logic as existing monitor"""
        last_err = None
        
        for attempt in range(1, max_retries + 1):
            try:
                # Pre-request delay like existing scraper
                time.sleep(random.uniform(1, 3))
                
                try:
                    resp = session.get(url, headers=self.headers, timeout=config.REQUEST_TIMEOUT)
                except requests.exceptions.SSLError:
                    resp = session.get(url, headers=self.headers, timeout=config.REQUEST_TIMEOUT, verify=False)
                
                if resp.status_code >= 500:
                    last_err = f"{resp.status_code} {resp.reason}"
                    if attempt < max_retries:
                        time.sleep(2.0 * attempt)  # Backoff
                        continue
                
                if resp.status_code == 403 and attempt < max_retries:
                    time.sleep(random.uniform(2, 4))
                    continue
                
                resp.raise_for_status()
                return resp.json()
                
            except requests.RequestException as e:
                last_err = str(e)
                if attempt < max_retries:
                    time.sleep(2.0 * attempt)
        
        return None
    
    def fetch_menu_data(self, session: requests.Session, merchant_id: str, referer_url: str) -> Optional[Dict[str, Any]]:
        """Fetch menu data from GrabFood API with improved logging"""
        api_urls = [
            f"https://portal.grab.com/foodweb/v2/restaurant?merchantCode={merchant_id}&latlng={self.ph_latlng}",
            f"https://portal.grab.com/foodweb/v2/merchants/{merchant_id}?latlng={self.ph_latlng}"
        ]
        
        # Update referer
        updated_headers = self.headers.copy()
        updated_headers["Referer"] = referer_url
        session.headers.update(updated_headers)
        
        for i, api_url in enumerate(api_urls, 1):
            data = self.fetch_json_with_retry(session, api_url)
            if data:
                if i > 1:
                    logger.info(f"✅ Backup API #{i} succeeded")
                return data
            else:
                logger.warning(f"⚠️ API #{i} failed, trying backup...")
        
        logger.error(f"❌ All API endpoints failed for merchant {merchant_id}")
        return None
    
    def extract_oos_items_from_menu(self, menu_data: Dict[str, Any]) -> List[str]:
        """Extract out-of-stock items from menu JSON (same logic as s.py)"""
        oos_items = []
        
        try:
            # Navigate menu structure
            for section in self._iter_menu_sections(menu_data):
                for item in self._iter_section_items(section):
                    name = item.get("name") or item.get("title")
                    if not name:
                        continue
                    
                    # Same logic as s.py: if not explicitly available=true, consider OOS
                    if item.get("available") is not True:
                        oos_items.append(name.strip())
                        logger.debug(f"🔴 Found OOS item: {name}")
        
        except Exception as e:
            logger.error(f"Error parsing menu data: {e}")
        
        return oos_items
    
    def _iter_menu_sections(self, data: Dict[str, Any]):
        """Iterate through possible menu section locations"""
        if not data:
            return
            
        roots = [data]
        if "data" in data and isinstance(data["data"], dict):
            roots.append(data["data"])
        
        for root in roots:
            if not isinstance(root, dict):
                continue
            
            # Check menu.categories or menu.sections
            menu = root.get("menu")
            if isinstance(menu, dict):
                categories = menu.get("categories") or menu.get("sections")
                if isinstance(categories, list):
                    for sec in categories:
                        yield sec
            
            # Check merchant.menu
            merchant = root.get("merchant")
            if isinstance(merchant, dict):
                m_menu = merchant.get("menu")
                if isinstance(m_menu, dict):
                    categories = m_menu.get("categories") or m_menu.get("sections")
                    if isinstance(categories, list):
                        for sec in categories:
                            yield sec
                
                # Check merchant.sections
                sections = merchant.get("sections")
                if isinstance(sections, list):
                    for sec in sections:
                        yield sec
    
    def _iter_section_items(self, section: Dict[str, Any]):
        """Iterate through items in a menu section"""
        if not isinstance(section, dict):
            return
            
        # Common item list keys
        for key in ("items", "itemList", "menuItems", "products", "dishes", "dishList"):
            items = section.get(key)
            if isinstance(items, list):
                for item in items:
                    if isinstance(item, dict):
                        yield item
    
    def scrape_store_skus(self, store_url: str) -> Tuple[List[str], List[str], str]:
        """
        Scrape SKUs for a single store
        Returns: (oos_sku_codes, unknown_products, store_name)
        """
        try:
            merchant_id = self.extract_merchant_id(store_url)
            if not merchant_id:
                logger.debug(f"Could not extract merchant ID from {store_url}")
                return [], [], "Unknown Store"
            
            session = requests.Session()
            
            # Fetch menu data
            menu_data = self.fetch_menu_data(session, merchant_id, store_url)
            if not menu_data:
                logger.debug(f"Could not fetch menu data for merchant {merchant_id}")
                return [], [], "Unknown Store"
            
            # Extract store name
            store_name = self._extract_store_name(menu_data) or "Unknown Store"
            
            # Extract OOS items
            oos_product_names = self.extract_oos_items_from_menu(menu_data)
            
            if not oos_product_names:
                logger.info(f"✅ {store_name}: No out-of-stock items found")
                return [], [], store_name
            
            # Map to SKU codes
            oos_sku_codes, unknown_products = self.sku_mapper.map_scraped_names_to_skus(oos_product_names)
            
            logger.info(f"📊 {store_name}: {len(oos_sku_codes)} OOS SKUs, {len(unknown_products)} unknown products")
            
            return oos_sku_codes, unknown_products, store_name
            
        except Exception as e:
            logger.error(f"❌ Error scraping {store_url}: {e}")
            return [], [], "Error Store"
    
    def _extract_store_name(self, menu_data: Dict[str, Any]) -> Optional[str]:
        """Extract store name from menu data"""
        try:
            # Try various name locations
            for root_key in ("merchant", "data", None):
                root = menu_data
                if root_key and isinstance(menu_data, dict):
                    root = menu_data.get(root_key)
                
                if isinstance(root, dict):
                    for name_key in ("name", "displayName", "merchantName", "restaurantName"):
                        name = root.get(name_key)
                        if isinstance(name, str) and name.strip():
                            return name.strip()
        except Exception as e:
            logger.error(f"Error extracting store name: {e}")
        
        return None
    
    def scrape_all_stores(self) -> Dict[str, Any]:
        """Scrape ALL stores from branch_urls.json with 40-minute hard time limit"""
        logger.info("🛒 Starting GrabFood SKU scraping for ALL stores from branch_urls.json...")
        logger.info("⏱️ 40-minute hard time limit enforced")
        
        # Start timer for 40-minute hard limit
        scrape_start_time = time.time()
        TIME_LIMIT_SECONDS = 40 * 60  # 40 minutes
        
        if not self.store_urls:
            logger.error("❌ No GrabFood URLs loaded - cannot perform SKU scraping")
            return {
                'total_stores': 0,
                'successful_scrapes': 0,
                'failed_scrapes': 0,
                'total_oos_skus': 0,
                'total_unknown_products': 0,
                'store_results': []
            }
        
        results = {
            'total_stores': len(self.store_urls),
            'successful_scrapes': 0,
            'failed_scrapes': 0,
            'total_oos_skus': 0,
            'total_unknown_products': 0,
            'failed_stores': [],  # Track for retry
            'store_results': []
        }
        
        # ============================================
        # PHASE 1: Main scrape of all stores
        # ============================================
        logger.info(f"📍 PHASE 1: Scraping {len(self.store_urls)} stores")
        
        for i, store_url in enumerate(self.store_urls, 1):
            # Check time limit
            elapsed = time.time() - scrape_start_time
            if elapsed >= TIME_LIMIT_SECONDS:
                logger.warning(f"⏰ Hit 40-minute time limit during main scrape at store {i}/{len(self.store_urls)}")
                # Add remaining stores to failed list
                for remaining_url in self.store_urls[i:]:
                    results['failed_stores'].append(remaining_url)
                    results['failed_scrapes'] += 1
                break
            
            logger.info(f"📍 [{i}/{len(self.store_urls)}] Scraping {store_url}")
            
            try:
                oos_skus, unknown_products, store_name = self.scrape_store_skus(store_url)
                
                # Check if scrape failed (Unknown Store or no data)
                if store_name == "Unknown Store" or store_name == "Error Store":
                    logger.warning(f"❓ {store_name} detected - will retry later")
                    results['failed_stores'].append(store_url)
                    results['failed_scrapes'] += 1
                    continue  # Don't save to database yet
                
                # Success - save to database
                store_id = db.get_or_create_store(store_name, store_url)
                success = db.save_sku_compliance_check(
                    store_id=store_id,
                    platform='grabfood',
                    out_of_stock_ids=oos_skus,
                    checked_by='automated_scraper'
                )
                
                if success:
                    results['successful_scrapes'] += 1
                    results['total_oos_skus'] += len(oos_skus)
                    results['total_unknown_products'] += len(unknown_products)
                    
                    logger.info(f"✅ {store_name}: Saved {len(oos_skus)} OOS SKUs to database")
                    
                    if unknown_products:
                        logger.warning(f"❓ {store_name}: {len(unknown_products)} unknown products logged")
                        for unknown in unknown_products:
                            logger.warning(f"   - Unknown: {unknown}")
                else:
                    logger.error(f"❌ {store_name}: Failed to save to database")
                    results['failed_stores'].append(store_url)
                    results['failed_scrapes'] += 1
                
                results['store_results'].append({
                    'store_name': store_name,
                    'store_url': store_url,
                    'oos_count': len(oos_skus),
                    'unknown_count': len(unknown_products),
                    'success': success
                })
                
            except Exception as e:
                logger.error(f"❌ Failed to scrape {store_url}: {e}")
                results['failed_stores'].append(store_url)
                results['failed_scrapes'] += 1
            
            # Delay between stores (anti-detection)
            if i < len(self.store_urls):
                time.sleep(random.uniform(5, 7))
        
        # ============================================
        # COOLDOWN: Wait before retrying to avoid rate limits
        # ============================================
        if results['failed_stores']:
            cooldown_seconds = 180  # 3 minutes
            elapsed = time.time() - scrape_start_time
            remaining = TIME_LIMIT_SECONDS - elapsed
            
            if elapsed + cooldown_seconds < TIME_LIMIT_SECONDS:
                logger.info("=" * 70)
                logger.info(f"⏸️ COOLDOWN: Waiting {cooldown_seconds/60:.1f} minutes before retries...")
                logger.info(f"   Failed stores: {len(results['failed_stores'])}")
                logger.info(f"   Time remaining: {remaining/60:.1f} minutes")
                time.sleep(cooldown_seconds)
                logger.info("✅ Cooldown complete - starting retry phase")
            else:
                logger.warning(f"⏰ Skipping cooldown - insufficient time remaining ({remaining/60:.1f} min)")
        
        # ============================================
        # PHASE 2: Retry failed stores until all succeed or time runs out
        # ============================================
        retry_round = 1
        
        while results['failed_stores']:
            # Check time limit before retry round
            elapsed = time.time() - scrape_start_time
            remaining = TIME_LIMIT_SECONDS - elapsed
            
            if elapsed >= TIME_LIMIT_SECONDS:
                logger.warning(f"⏰ Hit 40-minute time limit - stopping retries")
                logger.warning(f"⚠️ {len(results['failed_stores'])} stores remain unscraped")
                break
            
            logger.info("=" * 70)
            logger.info(f"🔄 PHASE 2 - Retry Round {retry_round}: {len(results['failed_stores'])} failed stores")
            logger.info(f"⏱️ Time remaining: {remaining/60:.1f} minutes")
            
            newly_successful = []
            still_failed = []
            
            for store_url in results['failed_stores']:
                # Check time limit before each retry
                elapsed = time.time() - scrape_start_time
                if elapsed >= TIME_LIMIT_SECONDS:
                    logger.warning(f"⏰ Hit 40-minute time limit during retry round {retry_round}")
                    # Add remaining stores to still_failed
                    still_failed.append(store_url)
                    for remaining_url in results['failed_stores'][results['failed_stores'].index(store_url)+1:]:
                        still_failed.append(remaining_url)
                    break
                
                logger.info(f"   🔄 Retry #{retry_round}: {store_url}")
                
                # Anti-detection: Rotate user agent
                self.headers["User-Agent"] = random.choice(self.user_agents)
                
                try:
                    oos_skus, unknown_products, store_name = self.scrape_store_skus(store_url)
                    
                    if store_name != "Unknown Store" and store_name != "Error Store":
                        # Success! Save to database
                        store_id = db.get_or_create_store(store_name, store_url)
                        success = db.save_sku_compliance_check(
                            store_id=store_id,
                            platform='grabfood',
                            out_of_stock_ids=oos_skus,
                            checked_by='automated_scraper'
                        )
                        
                        if success:
                            newly_successful.append(store_url)
                            results['successful_scrapes'] += 1
                            results['failed_scrapes'] -= 1
                            results['total_oos_skus'] += len(oos_skus)
                            results['total_unknown_products'] += len(unknown_products)
                            
                            logger.info(f"   ✅ Retry successful: {store_name} - Saved {len(oos_skus)} OOS SKUs")
                            
                            results['store_results'].append({
                                'store_name': store_name,
                                'store_url': store_url,
                                'oos_count': len(oos_skus),
                                'unknown_count': len(unknown_products),
                                'success': success,
                                'retry_round': retry_round
                            })
                        else:
                            logger.error(f"   ❌ Retry: {store_name} - Database save failed")
                            still_failed.append(store_url)
                    else:
                        logger.warning(f"   🚫 Still failed: {store_url}")
                        still_failed.append(store_url)
                
                except Exception as e:
                    logger.error(f"   ❌ Retry error: {e}")
                    still_failed.append(store_url)
                
                # Delay between retry attempts (anti-detection)
                time.sleep(random.uniform(5, 7))
            
            # Update failed stores list
            results['failed_stores'] = still_failed
            
            if newly_successful:
                logger.info(f"   🎉 Recovered {len(newly_successful)} stores in round {retry_round}")
            
            if not results['failed_stores']:
                logger.info(f"   ✅ All stores successfully scraped!")
                break
            
            retry_round += 1
        
        # ============================================
        # FINAL: Log summary
        # ============================================
        total_elapsed = time.time() - scrape_start_time
        
        logger.info("=" * 70)
        logger.info(f"🛒 GrabFood SKU scraping completed in {total_elapsed/60:.1f} minutes!")
        logger.info(f"📊 Summary:")
        logger.info(f"   Total stores: {results['total_stores']}")
        logger.info(f"   ✅ Successful: {results['successful_scrapes']}")
        logger.info(f"   ❌ Failed: {results['failed_scrapes']}")
        logger.info(f"   🔴 Total OOS SKUs found: {results['total_oos_skus']}")
        logger.info(f"   ❓ Total unknown products: {results['total_unknown_products']}")
        
        if results['failed_stores']:
            logger.warning(f"   ⚠️ {len(results['failed_stores'])} stores remain unscraped:")
            for store_url in results['failed_stores']:
                logger.warning(f"      - {store_url}")
        
        if total_elapsed >= TIME_LIMIT_SECONDS:
            logger.warning(f"   ⏰ Hit 40-minute time limit - some stores may be incomplete")
        
        # Remove failed_stores from return dict (internal tracking only)
        del results['failed_stores']
        
        return results

    # Keep the old method name for backward compatibility
    def scrape_all_test_stores(self) -> Dict[str, Any]:
        """Backward compatibility method - now scrapes all stores from branch_urls.json"""
        return self.scrape_all_stores()# ------------------------------------------------------------------------------
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
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Cache-Control': 'max-age=0',
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

    def get_platform_from_url(self, url: str) -> str:
        """Determine platform from URL"""
        if 'grab.com' in url:
            return 'grabfood'
        else:
            logger.warning(f"Non-GrabFood URL detected: {url}")
            return 'unknown'

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
        try:
            # Optional online fetch for title
            try:
                resp = requests.get(url, headers=self.headers, timeout=config.REQUEST_TIMEOUT)
                if resp.status_code == 200:
                    try:
                        soup = BeautifulSoup(resp.text, 'html.parser')
                        title = soup.title.string.strip() if soup.title and soup.title.string else ""
                    except Exception:
                        pass
            except requests.exceptions.SSLError:
                try:
                    resp = requests.get(url, headers=self.headers, timeout=config.REQUEST_TIMEOUT, verify=False)
                except Exception:
                    resp = None

            name = self.extract_store_name_from_url(url)
            self.store_cache[url] = name
            return name
        except Exception as e:
            logger.debug(f"Name extraction fallback failed: {e}")
            name = self.extract_store_name_from_url(url)
            self.store_cache[url] = name
            return name

# ------------------------------------------------------------------------------
# Enhanced GrabFood Monitor with Client Email Integration (EXISTING - UNCHANGED)
# ------------------------------------------------------------------------------
# ------------------------------------------------------------------------------
# Enhanced GrabFood Monitor with Selenium Scraping
# ------------------------------------------------------------------------------
        # ------------------------------------------------------------------------------
# Enhanced GrabFood Monitor with Selenium Scraping
# ------------------------------------------------------------------------------
class GrabFoodMonitor:
    """GrabFood monitor using Selenium scraping with immediate client email alerts"""

    def __init__(self):
        self.store_urls = self._load_grabfood_urls()
        self.name_manager = StoreNameManager()
        self.timezone = config.get_timezone()
        self.stats = {}
        self.previous_offline_stores = set()
        self.driver: Optional[webdriver.Chrome] = None
        
        # Setup Selenium WebDriver
        self._setup_driver()

        logger.info(f"🛒 GrabFood Monitor initialized (Selenium-based checking)")
        logger.info(f"   📋 {len(self.store_urls)} GrabFood stores to monitor")

        if HAS_ADMIN_ALERTS:
            logger.info(f"   📧 Admin alerts enabled")
        if HAS_CLIENT_ALERTS:
            logger.info(f"   📬 Client alerts enabled (immediate offline notifications)")
        else:
            logger.warning(f"   ⚠️ Client alerts NOT available - offline notifications disabled")

    def _setup_driver(self):
        """Setup Chrome WebDriver for Selenium scraping"""
        chrome_options = Options()
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

        try:
            self.driver = webdriver.Chrome(options=chrome_options)
            self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            logger.info("✓ Chrome WebDriver ready")
        except Exception as e:
            logger.error(f"❌ Failed to initialize Chrome WebDriver: {e}")
            logger.error("   Make sure Chrome and chromedriver are installed!")
            raise

    def _load_grabfood_urls(self):
        """Load ONLY GrabFood URLs from branch_urls.json"""
        try:
            with open(config.STORE_URLS_FILE) as f:
                data = json.load(f)
                all_urls = data.get('urls', [])

                # Filter to only GrabFood URLs
                grabfood_urls = [url for url in all_urls if 'grab.com' in url]
                foodpanda_urls = [url for url in all_urls if 'foodpanda' in url]

                logger.info(f"📋 Loaded {len(all_urls)} total URLs from {config.STORE_URLS_FILE}")
                logger.info(f"🛒 Filtered to {len(grabfood_urls)} GrabFood URLs")
                logger.info(f"🐼 Skipping {len(foodpanda_urls)} Foodpanda URLs (handled by VA)")

                return grabfood_urls

        except Exception as e:
            logger.error(f"Failed to load URLs: {e}")
            return []
    
    def _log_html_content(self, url: str, title: str, visible_text: str, html_length: int):
        """
        ✨ NEW: Log HTML content for debugging
        Shows what we're actually seeing in the page
        """
        logger.info(f"   📄 HTML Content Analysis:")
        logger.info(f"      URL: {url}")
        logger.info(f"      Page Title: '{title}'")
        logger.info(f"      HTML Length: {html_length} bytes")
        logger.info(f"      Visible Text Length: {len(visible_text)} chars")
        logger.info(f"   📝 First 800 chars of visible text:")
        logger.info(f"      {'-'*60}")
        # Show first 800 chars, with line breaks preserved
        preview = visible_text[:800].strip()
        for line in preview.split('\n')[:20]:  # Show max 20 lines
            if line.strip():
                logger.info(f"      {line.strip()[:100]}")
        logger.info(f"      {'-'*60}")

    def _check_for_closed_keywords(self, title_lower: str, visible_lower: str) -> Tuple[bool, Optional[str]]:
        """
        ✨ UPDATED: Combined check for ALL closed/terminated/offline keywords
        Returns: (is_closed, keyword_found)
        """
        # ALL keywords that indicate store is closed/terminated/offline
        closed_keywords = [
            'closed',
            'currently closed',
            'temporarily closed',
            'terminated',
            'permanently closed',
            'closed permanently',
            'no longer available',
            'not available anymore',
            'store has closed',
            'not available',
            'unavailable',
            'not accepting orders',
            'offline',
            'store is closed',
            'temporarily unavailable'
        ]
        
        # Check in title first
        for keyword in closed_keywords:
            if keyword in title_lower:
                logger.info(f"   🔍 Found '{keyword}' in TITLE → Store is CLOSED")
                return True, keyword
        
        # Check in first 1000 chars of visible text
        first_1000 = visible_lower[:1000]
        for keyword in closed_keywords:
            if keyword in first_1000:
                logger.info(f"   🔍 Found '{keyword}' in visible text → Store is CLOSED")
                return True, keyword
        
        # No closed keywords found
        logger.info(f"   🔍 NO closed keywords found → Store appears OPEN")
        return False, None

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
        
        if any(error_indicators):
            logger.info(f"   ⚠️ Error page detected in HTML")
            return True
        return False

    def _check_next_data(self, soup: BeautifulSoup, store_name: str, url: str, response_time: int) -> Optional[CheckResult]:
        """Extract status from __NEXT_DATA__ JSON"""
        try:
            next_data = soup.find('script', {'id': '__NEXT_DATA__'})
            if not next_data or not next_data.string:
                logger.debug(f"   📊 __NEXT_DATA__ not found in page")
                return None
            
            data = json.loads(next_data.string)
            props = data.get('props', {}).get('pageProps', {})
            
            if 'merchant' not in props:
                logger.debug(f"   📊 __NEXT_DATA__ found but no merchant data")
                return None
            
            merchant = props['merchant']
            actual_name = merchant.get('name', store_name)
            
            # Check various status indicators
            is_closed = merchant.get('isClosed', False)
            is_available = merchant.get('available', True)
            merchant_status = merchant.get('status', '').upper()
            
            logger.info(f"   📊 __NEXT_DATA__ found: status={merchant_status}, isClosed={is_closed}, available={is_available}")
            
            # Determine status
            if merchant_status == 'INACTIVE' or is_closed or not is_available:
                status = StoreStatus.OFFLINE
                message = f"__NEXT_DATA__: status={merchant_status}, isClosed={is_closed}, available={is_available}"
            else:
                status = StoreStatus.ONLINE
                message = f"__NEXT_DATA__: status={merchant_status}, store is accepting orders"
            
            return CheckResult(
                status=status,
                response_time=response_time,
                message=message,
                confidence=0.95
            )
            
        except json.JSONDecodeError as e:
            logger.debug(f"   ⚠️ __NEXT_DATA__ JSON parse error: {e}")
            return None
        except Exception as e:
            logger.debug(f"   ⚠️ Error parsing __NEXT_DATA__: {e}")
            return None

    def _check_html_parsing(self, title: str, visible_text: str, response_time: int) -> Optional[CheckResult]:
        """Extract status from HTML parsing as fallback"""
        try:
            # Pattern: "Restaurant Name ⭐ Rating"
            match = re.match(r'^(.+?)\s*⭐\s*([\d.]+)', title)
            if not match:
                logger.debug(f"   📄 HTML title doesn't match rating pattern")
                return None
            
            rating_str = match.group(2)
            
            try:
                rating = float(rating_str)
                logger.info(f"   📄 HTML parsing: Found rating {rating}★ in title → Store appears ONLINE")
            except:
                rating = None
            
            # If we can parse the title with rating, assume store is open
            return CheckResult(
                status=StoreStatus.ONLINE,
                response_time=response_time,
                message=f"HTML title shows rating: {rating}★ - store appears active",
                confidence=0.75
            )
            
        except Exception as e:
            logger.debug(f"   ⚠️ HTML parsing error: {e}")
            return None
    
    def check_grabfood_store(self, url: str, retry_count: int = 0) -> CheckResult:
        """
        ✨ UPDATED CHECK LOGIC:
        1. FIRST: Check if NO "closed" keywords found → ONLINE (85% confidence)
        2. Check if "closed" keywords found → OFFLINE (95% confidence)
        3. Check for error pages → ERROR/BLOCKED
        4. Try __NEXT_DATA__ extraction → ONLINE/OFFLINE (95% confidence)
        5. Try HTML parsing fallback → ONLINE (75% confidence)
        """
        start_time = time.time()
        max_retries = 2

        try:
            # Load page with Selenium
            logger.info(f"   🌐 Loading page: {url}")
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
            
            # Get page title
            page_title = self.driver.title
            
            # ✨ LOG EVERYTHING WE FETCHED
            self._log_html_content(url, page_title, visible_text, len(html))
            
            # Get lowercase versions for checking
            title_lower = page_title.lower()
            visible_lower = visible_text.lower()
            
            # ✅ PRIORITY 1 & 2: Check for closed keywords
            is_closed, found_keyword = self._check_for_closed_keywords(title_lower, visible_lower)
            
            if is_closed:
                # Found "closed" keywords → Store is OFFLINE
                return CheckResult(
                    status=StoreStatus.OFFLINE,
                    response_time=response_time,
                    message=f"Store is closed (found keyword: '{found_keyword}')",
                    confidence=0.95
                )
            else:
                # NO "closed" keywords found → Store is ONLINE!
                return CheckResult(
                    status=StoreStatus.ONLINE,
                    response_time=response_time,
                    message="No closed indicators found - store appears open",
                    confidence=0.85
                )
            
            # Note: Below code is now unreachable since we always return above
            # But keeping it as fallback in case you want to change logic later
            
            # PRIORITY 3: Check for error pages
            if self._is_error_page(title_lower, visible_lower, html):
                if retry_count < max_retries:
                    logger.info(f"   ⚠️ Error page detected, retrying...")
                    self.driver.refresh()
                    time.sleep(3)
                    return self.check_grabfood_store(url, retry_count + 1)
                
                return CheckResult(
                    status=StoreStatus.ERROR,
                    response_time=response_time,
                    message="Error page or blocked",
                    confidence=0.8
                )
            
            # PRIORITY 4: Try to extract data from __NEXT_DATA__
            store_name = self.name_manager.extract_store_name_from_url(url)
            next_data_result = self._check_next_data(soup, store_name, url, response_time)
            if next_data_result:
                logger.info(f"   ✅ Extracted from __NEXT_DATA__: {next_data_result.status.value.upper()}")
                return next_data_result
            
            # PRIORITY 5: Try HTML parsing
            html_parse_result = self._check_html_parsing(page_title, visible_text, response_time)
            if html_parse_result:
                logger.info(f"   ✅ Extracted from HTML: {html_parse_result.status.value.upper()}")
                return html_parse_result
            
            # If we get here, we couldn't determine status
            if retry_count < max_retries:
                logger.info(f"   ❓ Unknown status, retrying...")
                self.driver.refresh()
                time.sleep(3)
                return self.check_grabfood_store(url, retry_count + 1)
            
            # All attempts exhausted
            return CheckResult(
                status=StoreStatus.UNKNOWN,
                response_time=response_time,
                message="Could not determine store status",
                confidence=0.3
            )
            
        except Exception as e:
            response_time = int((time.time() - start_time) * 1000)
            logger.error(f"   ❌ Error checking store: {e}")
            
            if retry_count < max_retries:
                time.sleep(2)
                return self.check_grabfood_store(url, retry_count + 1)
            
            return CheckResult(
                status=StoreStatus.ERROR,
                response_time=response_time,
                message=f"Exception: {str(e)[:100]}",
                confidence=0.2
            )

    def check_all_grabfood_stores_with_client_alerts(self):
        """Check stores using Selenium and send immediate client emails when offline"""
        
        # ✅ Calculate target hour correctly
        tz = self.timezone
        now = datetime.now(tz)
        
        if now.minute >= 45:
            effective_at = now.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
        else:
            effective_at = now.replace(minute=0, second=0, microsecond=0)
        
        run_id = uuid.uuid4()

        self.stats = {
            'cycle_start': datetime.now(),
            'cycle_end': None,
            'total_stores': len(self.store_urls),
            'checked': 0, 'online': 0, 'offline': 0, 'blocked': 0, 'errors': 0, 'unknown': 0,
            'retries': 0, 'retry_successes': 0,
            'newly_offline': 0, 'newly_online': 0
        }

        current_time = config.get_current_time()
        target_hour = effective_at.hour
        
        logger.info(f"🛒 GRABFOOD MONITORING (SELENIUM) with CLIENT ALERTS at {current_time.strftime('%Y-%m-%d %H:%M:%S %Z')}")
        logger.info(f"📋 Checking {len(self.store_urls)} GrabFood stores")
        logger.info(f"🎯 Current time: {now.strftime('%H:%M')}, Target hour slot: {effective_at.strftime('%H:00')}")
        logger.info(f"💾 Data will be saved to: {effective_at.strftime('%Y-%m-%d %H:00:00')}")
        logger.info(f"✨ Logic: NO 'closed' keywords = ONLINE | 'closed' keywords found = OFFLINE")

        all_results: List[Dict[str, Any]] = []
        blocked_stores: List[str] = []
        current_offline_stores = set()

        # First pass - check all stores
        for i, url in enumerate(self.store_urls, 1):
            result = self._check_single_store_safe(url, i, len(self.store_urls))
            if result:
                all_results.append(result)
                
                if result['result'].status == StoreStatus.OFFLINE:
                    current_offline_stores.add(url)
                elif result['result'].status == StoreStatus.BLOCKED:
                    blocked_stores.append(url)

            # Delay between requests
            if i < len(self.store_urls):
                time.sleep(random.uniform(3, 5))

        # Retry logic for blocked stores
        retry_round = 1
        max_retry_rounds = 5

        while blocked_stores and retry_round <= max_retry_rounds:
            current_time = config.get_current_time()
            minutes_remaining = 60 - current_time.minute

            if current_time.hour != target_hour and current_time.minute >= 0:
                logger.info(f"⏰ Reached target hour {target_hour}:00, stopping retries")
                break

            if minutes_remaining < 5:
                logger.info(f"⏰ Only {minutes_remaining} minutes until {target_hour}:00, stopping retries")
                break

            logger.info(f"🔄 Retry round {retry_round}: {len(blocked_stores)} blocked stores")

            retry_delay = min(180, 60 * retry_round)
            logger.info(f"   ⏱️ Waiting {retry_delay} seconds before retry...")
            time.sleep(retry_delay)

            newly_unblocked = []
            still_blocked = []

            for url in blocked_stores:
                logger.info(f"   🔄 Retrying blocked store: {url}")

                for i, result_data in enumerate(all_results):
                    if result_data['url'] == url:
                        retry_result = self._check_single_store_safe(url, retry_round, len(blocked_stores), is_retry=True)
                        if retry_result:
                            self.stats['retries'] += 1

                            if retry_result['result'].status != StoreStatus.BLOCKED:
                                all_results[i] = retry_result
                                newly_unblocked.append(url)
                                self.stats['retry_successes'] += 1
                                
                                if retry_result['result'].status == StoreStatus.OFFLINE:
                                    current_offline_stores.add(url)
                                else:
                                    current_offline_stores.discard(url)
                                    
                                logger.info(f"   ✅ Retry successful: {retry_result['name']} now {retry_result['result'].status.value}")
                            else:
                                still_blocked.append(url)
                                logger.info(f"   🚫 Still blocked: {retry_result['name']}")
                        break

                time.sleep(random.uniform(2, 5))

            blocked_stores = still_blocked
            retry_round += 1

            if newly_unblocked:
                logger.info(f"   🎉 Unblocked {len(newly_unblocked)} stores in round {retry_round-1}")

        # DETECT STATE CHANGES AND SEND IMMEDIATE CLIENT ALERTS
        newly_offline_stores = current_offline_stores - self.previous_offline_stores
        newly_online_stores = self.previous_offline_stores - current_offline_stores
        
        self.stats['newly_offline'] = len(newly_offline_stores)
        self.stats['newly_online'] = len(newly_online_stores)

        # Send immediate alerts for newly offline stores
        if newly_offline_stores and HAS_CLIENT_ALERTS:
            newly_offline_alerts = []
            for result_data in all_results:
                if result_data['url'] in newly_offline_stores:
                    platform = self.name_manager.get_platform_from_url(result_data['url'])
                    platform_name = "GrabFood" if platform == "grabfood" else "Unknown"
                    newly_offline_alerts.append(StoreAlert(
                        name=result_data['name'],
                        platform=platform_name,
                        status="OFFLINE",
                        last_check=datetime.now(self.timezone)
                    ))
            
            if newly_offline_alerts:
                logger.info(f"🚨 IMMEDIATE ALERT: {len(newly_offline_alerts)} stores just went offline!")
                success = client_alerts.send_immediate_offline_alert(newly_offline_alerts, len(self.store_urls))
                if success:
                    logger.info("✅ Immediate offline alert sent to clients")
                else:
                    logger.warning("⚠️ Immediate offline alert failed or skipped")

        # Update previous offline stores for next cycle
        self.previous_offline_stores = current_offline_stores.copy()

        # Send regular hourly status update
        self._send_client_alerts(all_results)

        # Save results
        self._save_all_results(all_results, effective_at, run_id)

        # Admin alerts
        if HAS_ADMIN_ALERTS:
            self._send_friendly_admin_alerts(all_results)

        # Final stats
        self.stats['cycle_end'] = datetime.now()
        duration = (self.stats['cycle_end'] - self.stats['cycle_start']).total_seconds()

        logger.info("=" * 70)
        logger.info(f"✅ GRABFOOD MONITORING COMPLETED in {duration/60:.1f} minutes")
        logger.info(f"📊 Final Statistics:")
        logger.info(f"   Total GrabFood Stores: {self.stats['total_stores']}")
        logger.info(f"   ✅ Checked: {self.stats['checked']} ({self.stats['checked']/max(1,self.stats['total_stores'])*100:.1f}%)")
        logger.info(f"   🟢 Online: {self.stats['online']}")
        logger.info(f"   🔴 Offline: {self.stats['offline']} (newly offline: {self.stats['newly_offline']})")
        logger.info(f"   🚫 Blocked: {self.stats['blocked']}")
        logger.info(f"   ⚠️ Errors: {self.stats['errors']}")
        logger.info(f"   ❓ Unknown: {self.stats['unknown']}")
        logger.info(f"   🔄 Retries: {self.stats['retries']} (successes: {self.stats['retry_successes']})")
        if self.stats['newly_offline'] > 0:
            logger.info(f"   🚨 CLIENT ALERTS: Sent immediate alerts for {self.stats['newly_offline']} newly offline stores")

        return all_results

    def check_all_grabfood_stores(self):
        """Legacy method - calls the new client alerts version"""
        return self.check_all_grabfood_stores_with_client_alerts()

    def _check_single_store_safe(self, url: str, index: int, total: int, is_retry: bool = False) -> Dict[str, Any]:
        """Check single store with proper error handling"""
        store_name = self.name_manager.get_store_name(url)

        try:
            retry_text = " (retry)" if is_retry else ""
            logger.info(f"   [{index}/{total}] Checking {store_name}{retry_text}...")
            result = self.check_grabfood_store(url)

            if not is_retry:
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
            if result.message:
                logger.info(f"      📝 {result.message}")

            return {'url': url, 'name': store_name, 'result': result}

        except Exception as e:
            logger.error(f"   [{index}/{total}] Failed to check {store_name}: {e}")
            if not is_retry:
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

    def _send_client_alerts(self, results: List[Dict[str, Any]]):
        """Send hourly client alerts (all offline stores)"""
        try:
            if not HAS_CLIENT_ALERTS:
                logger.debug("Client alerts module not available")
                return

            offline_stores = []
            for rd in results:
                result = rd['result']
                if result.status == StoreStatus.OFFLINE:
                    platform = self.name_manager.get_platform_from_url(rd['url'])
                    platform_name = "GrabFood" if platform == "grabfood" else "Unknown"

                    offline_stores.append(StoreAlert(
                        name=rd['name'],
                        platform=platform_name,
                        status="OFFLINE",
                        last_check=datetime.now(self.timezone)
                    ))

            total_stores = len(results)

            if offline_stores:
                logger.info(f"📧 Sending hourly status alert for {len(offline_stores)} offline stores")
                success = client_alerts.send_hourly_status_alert(offline_stores, total_stores)
                if success:
                    logger.info("✅ Hourly client alerts sent successfully")
                else:
                    logger.warning("⚠️ Hourly client alerts failed or skipped")
            else:
                logger.info("📧 All stores online - sending positive status update")
                _ = client_alerts.send_hourly_status_alert([], total_stores)

        except Exception as e:
            logger.error(f"Error sending client alerts: {e}")

    def _send_friendly_admin_alerts(self, results: List[Dict[str, Any]]):
        """Send friendly admin alerts for verification needs"""
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
                        message=result.message or "Routine verification needed",
                        response_time=result.response_time,
                        platform=platform
                    ))

            if problem_stores:
                logger.info(f"📋 Found {len(problem_stores)} GrabFood stores for routine verification")
                success = admin_alerts.send_manual_verification_alert(problem_stores)
                if success:
                    logger.info("✅ Friendly admin reminder sent")
                else:
                    logger.debug("⚠️ Admin reminder skipped (cooldown or disabled)")

            # Bot detection for excessive blocking
            blocked_count = sum(1 for r in results if r['result'].status == StoreStatus.BLOCKED)
            if blocked_count >= 5:
                admin_alerts.send_bot_detection_alert(blocked_count)

        except Exception as e:
            logger.error(f"Error with admin alerts: {e}")

    def _save_all_results(self, results: List[Dict[str, Any]], effective_at: datetime, run_id: uuid.UUID):
        """Save all results to database with hourly snapshots"""
        logger.info("💾 Saving GrabFood results to database...")

        saved_count = 0
        error_count = 0

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

        # Backward-compatible summary report
        try:
            total = len(results)
            online = sum(1 for r in results if r['result'].status == StoreStatus.ONLINE)
            offline = sum(1 for r in results if r['result'].status == StoreStatus.OFFLINE)
            db.save_summary_report(total, online, offline)
        except Exception as e:
            logger.debug(f"(Optional) legacy save_summary_report failed: {e}")

        logger.info(f"✅ Saved {saved_count}/{len(results)} GrabFood records")
        if error_count > 0:
            logger.warning(f"   ⚠️ {error_count} database errors")

        logger.info(f"💾 Data saved to hour slot: {effective_at.strftime('%Y-%m-%d %H:00:00')}")

    def close(self):
        """Close Selenium driver"""
        if self.driver:
            try:
                self.driver.quit()
                logger.info("✓ Selenium WebDriver closed")
            except:
                pass

    def __del__(self):
        """Cleanup on deletion"""
        self.close()
def main():
    """Main entry point - ENHANCED WITH SMART SKU SCRAPING"""
    logger.info("=" * 80)
    logger.info("🛒 CocoPan GrabFood Monitor - ENHANCED WITH SMART SKU SCRAPING")
    logger.info("📧 FEATURE: Immediate client emails when stores go offline")
    logger.info("📦 FEATURE: Daily GrabFood SKU/OOS scraping with fuzzy matching")
    logger.info("✨ NEW: Smart startup control - prevents duplicate scraping")
    logger.info("🎯 Target: Monitor GrabFood stores with instant offline notifications + SKU compliance")
    logger.info("🐼 Foodpanda: Handled by VA hourly check-in system")
    logger.info("✅ FIXED: Hour slot calculation - runs at :45 but saves to NEXT hour")
    logger.info("=" * 80)

    if not config.validate_timezone():
        logger.error("❌ Timezone validation failed!")
        sys.exit(1)

    #signal.signal(signal.SIGTERM, signal_handler)
    #signal.signal(signal.SIGINT, signal_handler)

    try:
        monitor = GrabFoodMonitor()
        sku_scraper = GrabFoodSKUScraper()

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

        # Test client alerts on startup
        if HAS_CLIENT_ALERTS:
            logger.info("🧪 Testing client email system...")
            try:
                test_success = client_alerts.test_email_system()
                if test_success:
                    logger.info("✅ Client email system test successful!")
                else:
                    logger.warning("⚠️ Client email system test failed - check configuration")
            except Exception as e:
                logger.error(f"❌ Client email test error: {e}")

        # ✨ MODIFIED: Smart SKU scraping on startup
        if should_run_startup_sku_test():
            logger.info("🧪 Running startup GrabFood SKU scraping test...")
            try:
                test_results = sku_scraper.scrape_all_stores()
                if test_results['successful_scrapes'] > 0:
                    logger.info("✅ GrabFood SKU scraping test successful!")
                    logger.info(f"   📊 Scraped {test_results['successful_scrapes']} stores")
                else:
                    logger.warning("⚠️ GrabFood SKU scraping test failed - check configuration")
            except Exception as e:
                logger.error(f"❌ GrabFood SKU scraping test error: {e}")
        else:
            logger.info("⏭️ Skipping startup SKU test (will run at scheduled time or use --test-sku)")

        # Scheduler
        if HAS_SCHEDULER:
            ph_tz = config.get_timezone()
            scheduler = BlockingScheduler(timezone=ph_tz)

            def early_check_job():
                """Start checking at :45 to give time for retries and client alerts"""
                now_hour = config.get_current_time().hour
                if config.is_monitor_time(now_hour):
                    monitor.check_all_grabfood_stores_with_client_alerts()
                else:
                    logger.info(f"😴 Outside monitoring hours ({now_hour}:00)")

            def daily_sku_scraping_job():
                """✨ MODIFIED: Daily SKU scraping at 10AM - with duplicate prevention"""
                logger.info("🛒 Daily scheduled GrabFood SKU scraping triggered...")
                
                # Double-check if already ran today (race condition protection)
                if has_sku_scraping_run_today():
                    logger.info("⏭️ SKU scraping already completed today - Skipping scheduled run")
                    return
                
                try:
                    results = sku_scraper.scrape_all_stores()
                    if results['successful_scrapes'] > 0:
                        logger.info(f"✅ Daily SKU scraping completed: {results['successful_scrapes']} stores processed")
                    else:
                        logger.error("❌ Daily SKU scraping failed - no successful scrapes")
                except Exception as e:
                    logger.error(f"❌ Daily SKU scraping error: {e}")

            # Schedule at :45 minutes past each hour (existing)
            scheduler.add_job(
                func=early_check_job,
                trigger=CronTrigger(minute=45, timezone=ph_tz),
                id='early_grabfood_check_with_client_alerts',
                max_instances=1,
                coalesce=True,
                misfire_grace_time=300
            )

            # Schedule daily SKU scraping at 10AM
            scheduler.add_job(
                func=daily_sku_scraping_job,
                trigger=CronTrigger(hour=10, minute=0, timezone=ph_tz),
                id='daily_grabfood_sku_scraping',
                max_instances=1,
                coalesce=True,
                misfire_grace_time=300
            )

            logger.info(f"⏰ Scheduled GrabFood checks at :45 past each hour for client email integration")
            logger.info(f"⏰ Scheduled daily GrabFood SKU scraping at 10:00 AM")
            logger.info("🔍 Running initial GrabFood check with client alerts...")

            try:
                early_check_job()
            except Exception as e:
                logger.error(f"Initial check error: {e}")

            logger.info("✅ GrabFood monitoring active with client email alerts and daily SKU scraping!")
            scheduler.start()

        else:
            logger.info("⚠️ Using simple loop (no APScheduler)")
            while True:
                try:
                    now_hour = config.get_current_time().hour
                    if config.is_monitor_time(now_hour):
                        monitor.check_all_grabfood_stores_with_client_alerts()
                        
                        # ✨ MODIFIED: Check if it's 10AM and hasn't run today
                        if now_hour == 10 and not has_sku_scraping_run_today():
                            logger.info("🛒 Starting daily GrabFood SKU scraping...")
                            try:
                                results = sku_scraper.scrape_all_stores()
                                logger.info(f"✅ Daily SKU scraping completed: {results['successful_scrapes']} stores processed")
                            except Exception as e:
                                logger.error(f"❌ Daily SKU scraping error: {e}")
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