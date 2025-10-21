#!/usr/bin/env python3
"""
GrabFood Online Status Tester (API-based)
- Tests all GrabFood stores for online/offline status
- Uses GrabFood API (more reliable than HTML scraping)
- Does NOT save to database
- Pretty console output with statistics
"""
import json
import re
import time
import random
from typing import Optional, Dict, Any, List, Tuple
from datetime import datetime
from urllib.parse import urlparse
from dataclasses import dataclass
from enum import Enum

import requests
import urllib3

# Disable SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Constants
STORE_URLS_FILE = "branch_urls.json"
PH_LATLNG = "14.5995,120.9842"  # Manila coordinates
REQUEST_TIMEOUT = 15

# User agents for rotation
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
]

# Status enum
class StoreStatus(Enum):
    ONLINE = "🟢 ONLINE"
    OFFLINE = "🔴 OFFLINE"
    UNKNOWN = "⚪ UNKNOWN"
    ERROR = "⚠️ ERROR"
    BLOCKED = "🚫 BLOCKED"

@dataclass
class StoreResult:
    """Store check result"""
    url: str
    store_name: str
    status: StoreStatus
    api_status: Optional[str]  # Raw API status (ACTIVE, INACTIVE, etc.)
    rating: Optional[float]
    vote_count: Optional[int]
    response_time_ms: int
    message: str

# ==============================================================================
# Helper Functions
# ==============================================================================
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

def fetch_grabfood_api_data(merchant_id: str, referer_url: str, user_agents: List[str], 
                            ph_latlng: str = "14.5995,120.9842", 
                            max_retries: int = 3) -> Optional[Dict[str, Any]]:
    """
    ✨ NEW FUNCTION
    Fetch store data from GrabFood API endpoints
    Returns JSON data if successful, None otherwise
    """
    
    api_urls = [
        f"https://portal.grab.com/foodweb/v2/restaurant?merchantCode={merchant_id}&latlng={ph_latlng}",
        f"https://portal.grab.com/foodweb/v2/merchants/{merchant_id}?latlng={ph_latlng}"
    ]
    
    session = requests.Session()
    
    for api_url in api_urls:
        for attempt in range(1, max_retries + 1):
            try:
                headers = {
                    'User-Agent': random.choice(user_agents),
                    'Accept': 'application/json, text/plain, */*',
                    'Accept-Language': 'en-PH,en;q=0.9',
                    'Origin': 'https://food.grab.com',
                    'Referer': referer_url,
                    'Connection': 'keep-alive',
                }
                
                # Small delay to avoid rate limiting
                time.sleep(random.uniform(0.5, 1.5))
                
                try:
                    resp = session.get(api_url, headers=headers, timeout=15)
                except requests.exceptions.SSLError:
                    resp = session.get(api_url, headers=headers, timeout=15, verify=False)
                
                if resp.status_code == 200:
                    try:
                        return resp.json()
                    except json.JSONDecodeError:
                        logger.debug(f"Non-JSON response from API: {api_url}")
                        continue
                        
                elif resp.status_code >= 500 and attempt < max_retries:
                    # Server error, retry with backoff
                    time.sleep(2.0 * attempt)
                    continue
                    
                elif resp.status_code == 403 and attempt < max_retries:
                    # Forbidden, wait and retry
                    time.sleep(random.uniform(2, 4))
                    continue
                    
            except requests.exceptions.Timeout:
                if attempt == max_retries:
                    logger.debug(f"API timeout after {max_retries} attempts: {api_url}")
                    break
                time.sleep(2.0 * attempt)
                
            except Exception as e:
                logger.debug(f"API request error (attempt {attempt}): {e}")
                if attempt == max_retries:
                    break
                time.sleep(2.0 * attempt)
    
    return None

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
        
def extract_merchant_id(url: str) -> Optional[str]:
    """Extract merchant ID from GrabFood URL (e.g., '2-C6K2GPUYKYT3LE')"""
    try:
        parsed = urlparse(url)
        # Pattern: /restaurant/store-name/2-MERCHANTID
        match = re.search(r'/([0-9]-[A-Z0-9]+)$', parsed.path, re.IGNORECASE)
        if match:
            return match.group(1)
        return None
    except Exception as e:
        print(f"   ❌ Error extracting merchant ID: {e}")
        return None

def extract_store_name_from_url(url: str) -> str:
    """Extract store name from URL"""
    try:
        match = re.search(r'/restaurant/([^/]+)', url)
        if match:
            raw_name = match.group(1)
            raw_name = re.sub(r'-delivery$', '', raw_name)
            # Clean up the name
            name = raw_name.replace('-', ' ').title()
            if not name.lower().startswith('cocopan'):
                name = f"Cocopan {name}"
            return name
        return "Unknown Store"
    except Exception:
        return "Unknown Store"

def load_grabfood_urls() -> List[str]:
    """Load GrabFood URLs from branch_urls.json"""
    try:
        with open(STORE_URLS_FILE, 'r') as f:
            data = json.load(f)
            all_urls = data.get('urls', [])
            
        # Filter to only GrabFood URLs
        grabfood_urls = [url for url in all_urls if 'grab.com' in url]
        
        print(f"📋 Loaded {len(all_urls)} total URLs from {STORE_URLS_FILE}")
        print(f"🛒 Filtered to {len(grabfood_urls)} GrabFood URLs")
        
        return grabfood_urls
        
    except FileNotFoundError:
        print(f"❌ Error: {STORE_URLS_FILE} not found!")
        print("💡 Using hardcoded test URLs instead...")
        return [
            "https://food.grab.com/ph/en/restaurant/cocopan-tejeros-delivery/2-C6K2GPUYKYT3LE",
            "https://food.grab.com/ph/en/restaurant/cocopan-maysilo-delivery/2-C6TATTL2UF2UDA",
            "https://food.grab.com/ph/en/restaurant/cocopan-anonas-delivery/2-C6XVCUDGNXNZNN",
        ]
    except Exception as e:
        print(f"❌ Error loading URLs: {e}")
        return []

# ==============================================================================
# GrabFood API Functions
# ==============================================================================

def fetch_grabfood_api(merchant_id: str, referer_url: str, max_retries: int = 3) -> Optional[Dict[str, Any]]:
    """Fetch store data from GrabFood API"""
    
    api_urls = [
        f"https://portal.grab.com/foodweb/v2/restaurant?merchantCode={merchant_id}&latlng={PH_LATLNG}",
        f"https://portal.grab.com/foodweb/v2/merchants/{merchant_id}?latlng={PH_LATLNG}"
    ]
    
    session = requests.Session()
    
    for api_url in api_urls:
        for attempt in range(1, max_retries + 1):
            try:
                # Rotate user agent
                headers = {
                    'User-Agent': random.choice(USER_AGENTS),
                    'Accept': 'application/json, text/plain, */*',
                    'Accept-Language': 'en-PH,en;q=0.9',
                    'Origin': 'https://food.grab.com',
                    'Referer': referer_url,
                    'Connection': 'keep-alive',
                }
                
                # Add delay to avoid rate limiting
                time.sleep(random.uniform(0.5, 1.5))
                
                resp = session.get(api_url, headers=headers, timeout=REQUEST_TIMEOUT)
                
                if resp.status_code == 200:
                    try:
                        return resp.json()
                    except json.JSONDecodeError:
                        continue
                elif resp.status_code >= 500 and attempt < max_retries:
                    # Server error, retry
                    time.sleep(2.0 * attempt)
                    continue
                elif resp.status_code == 403 and attempt < max_retries:
                    # Forbidden, wait and retry
                    time.sleep(random.uniform(2, 4))
                    continue
                    
            except requests.exceptions.SSLError:
                try:
                    resp = session.get(api_url, headers=headers, timeout=REQUEST_TIMEOUT, verify=False)
                    if resp.status_code == 200:
                        return resp.json()
                except Exception:
                    pass
            except Exception:
                if attempt == max_retries:
                    break
                time.sleep(2.0 * attempt)
    
    return None

def extract_status_from_json(json_data: Dict[str, Any]) -> Tuple[Optional[str], Optional[float], Optional[int]]:
    """
    Extract status, rating, and vote_count from GrabFood API JSON
    Returns: (status, rating, vote_count)
    """
    try:
        # Try multiple possible paths in the JSON
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
        print(f"   ⚠️ Error parsing JSON: {e}")
        return None, None, None

def determine_store_status(api_status: Optional[str]) -> StoreStatus:
    """
    Determine store status from API status field
    
    Known statuses:
    - ACTIVE = Store is online and accepting orders
    - INACTIVE = Store is offline
    - Other statuses may exist
    """
    if not api_status:
        return StoreStatus.UNKNOWN
    
    api_status_upper = api_status.upper()
    
    if api_status_upper == "ACTIVE":
        return StoreStatus.ONLINE
    elif api_status_upper in ["INACTIVE", "CLOSED", "UNAVAILABLE"]:
        return StoreStatus.OFFLINE
    else:
        return StoreStatus.UNKNOWN

# ==============================================================================
# Store Checker
# ==============================================================================

def check_store_status(url: str, index: int, total: int) -> StoreResult:
    """Check a single store's online/offline status"""
    
    start_time = time.time()
    store_name = extract_store_name_from_url(url)
    
    print(f"\n[{index}/{total}] Checking: {store_name}")
    print(f"   URL: {url}")
    
    # Extract merchant ID
    merchant_id = extract_merchant_id(url)
    if not merchant_id:
        response_time = int((time.time() - start_time) * 1000)
        print(f"   ❌ Could not extract merchant ID")
        return StoreResult(
            url=url,
            store_name=store_name,
            status=StoreStatus.ERROR,
            api_status=None,
            rating=None,
            vote_count=None,
            response_time_ms=response_time,
            message="Invalid URL format"
        )
    
    print(f"   Merchant ID: {merchant_id}")
    
    # Fetch from API
    json_data = fetch_grabfood_api(merchant_id, url)
    response_time = int((time.time() - start_time) * 1000)
    
    if not json_data:
        print(f"   ❌ API request failed")
        return StoreResult(
            url=url,
            store_name=store_name,
            status=StoreStatus.ERROR,
            api_status=None,
            rating=None,
            vote_count=None,
            response_time_ms=response_time,
            message="API request failed after retries"
        )
    
    # Extract status info
    api_status, rating, vote_count = extract_status_from_json(json_data)
    status = determine_store_status(api_status)
    
    # Build message
    message_parts = []
    if api_status:
        message_parts.append(f"API Status: {api_status}")
    if rating is not None:
        message_parts.append(f"Rating: {rating:.1f}★")
    if vote_count is not None:
        message_parts.append(f"Votes: {vote_count}")
    
    message = " | ".join(message_parts) if message_parts else "No additional info"
    
    # Print result
    print(f"   {status.value} ({response_time}ms)")
    print(f"   {message}")
    
    return StoreResult(
        url=url,
        store_name=store_name,
        status=status,
        api_status=api_status,
        rating=rating,
        vote_count=vote_count,
        response_time_ms=response_time,
        message=message
    )

# ==============================================================================
# Main Test Function
# ==============================================================================

def test_all_stores():
    """Test all GrabFood stores and display results"""
    
    print("=" * 80)
    print("🛒 GRABFOOD ONLINE STATUS TESTER (API-based)")
    print("=" * 80)
    print(f"⏰ Test started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    # Load URLs
    urls = load_grabfood_urls()
    
    if not urls:
        print("\n❌ No URLs to test!")
        return
    
    print(f"\n📊 Testing {len(urls)} GrabFood stores...")
    print("=" * 80)
    
    # Test all stores
    results: List[StoreResult] = []
    start_time = time.time()
    
    for i, url in enumerate(urls, 1):
        result = check_store_status(url, i, len(urls))
        results.append(result)
        
        # Delay between requests (anti-rate-limiting)
        if i < len(urls):
            delay = random.uniform(2, 4)
            print(f"   ⏱️ Waiting {delay:.1f}s...")
            time.sleep(delay)
    
    total_time = time.time() - start_time
    
    # Display summary
    print("\n" + "=" * 80)
    print("📊 TEST RESULTS SUMMARY")
    print("=" * 80)
    
    # Count by status
    status_counts = {
        StoreStatus.ONLINE: 0,
        StoreStatus.OFFLINE: 0,
        StoreStatus.UNKNOWN: 0,
        StoreStatus.ERROR: 0,
        StoreStatus.BLOCKED: 0,
    }
    
    for result in results:
        status_counts[result.status] += 1
    
    total = len(results)
    
    print(f"\n⏱️ Total time: {total_time:.1f}s ({total_time/60:.1f} minutes)")
    print(f"\n📈 Statistics:")
    print(f"   Total stores tested: {total}")
    print(f"   {StoreStatus.ONLINE.value}: {status_counts[StoreStatus.ONLINE]} ({status_counts[StoreStatus.ONLINE]/total*100:.1f}%)")
    print(f"   {StoreStatus.OFFLINE.value}: {status_counts[StoreStatus.OFFLINE]} ({status_counts[StoreStatus.OFFLINE]/total*100:.1f}%)")
    print(f"   {StoreStatus.UNKNOWN.value}: {status_counts[StoreStatus.UNKNOWN]} ({status_counts[StoreStatus.UNKNOWN]/total*100:.1f}%)")
    print(f"   {StoreStatus.ERROR.value}: {status_counts[StoreStatus.ERROR]} ({status_counts[StoreStatus.ERROR]/total*100:.1f}%)")
    print(f"   {StoreStatus.BLOCKED.value}: {status_counts[StoreStatus.BLOCKED]} ({status_counts[StoreStatus.BLOCKED]/total*100:.1f}%)")
    
    # Average response time
    valid_times = [r.response_time_ms for r in results if r.response_time_ms > 0]
    if valid_times:
        avg_time = sum(valid_times) / len(valid_times)
        print(f"\n⚡ Average response time: {avg_time:.0f}ms")
    
    # List offline stores
    offline_stores = [r for r in results if r.status == StoreStatus.OFFLINE]
    if offline_stores:
        print(f"\n🔴 OFFLINE STORES ({len(offline_stores)}):")
        for result in offline_stores:
            print(f"   • {result.store_name}")
            print(f"     {result.message}")
    
    # List error stores
    error_stores = [r for r in results if r.status in [StoreStatus.ERROR, StoreStatus.BLOCKED]]
    if error_stores:
        print(f"\n⚠️ STORES WITH ERRORS ({len(error_stores)}):")
        for result in error_stores:
            print(f"   • {result.store_name}")
            print(f"     {result.message}")
    
    # Success rate
    successful = status_counts[StoreStatus.ONLINE] + status_counts[StoreStatus.OFFLINE]
    success_rate = (successful / total * 100) if total > 0 else 0
    
    print(f"\n✅ Success rate: {success_rate:.1f}% ({successful}/{total} stores checked successfully)")
    
    # Save detailed results to JSON (optional)
    output_file = f"grabfood_status_test_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    try:
        output_data = {
            'test_time': datetime.now().isoformat(),
            'total_stores': total,
            'total_time_seconds': total_time,
            'statistics': {
                'online': status_counts[StoreStatus.ONLINE],
                'offline': status_counts[StoreStatus.OFFLINE],
                'unknown': status_counts[StoreStatus.UNKNOWN],
                'error': status_counts[StoreStatus.ERROR],
                'blocked': status_counts[StoreStatus.BLOCKED],
            },
            'results': [
                {
                    'url': r.url,
                    'store_name': r.store_name,
                    'status': r.status.name,
                    'api_status': r.api_status,
                    'rating': r.rating,
                    'vote_count': r.vote_count,
                    'response_time_ms': r.response_time_ms,
                    'message': r.message,
                }
                for r in results
            ]
        }
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(output_data, f, indent=2, ensure_ascii=False)
        
        print(f"\n💾 Detailed results saved to: {output_file}")
    except Exception as e:
        print(f"\n⚠️ Could not save results file: {e}")
    
    print("\n" + "=" * 80)
    print("✅ TEST COMPLETE")
    print("=" * 80)

# ==============================================================================
# Entry Point
# ==============================================================================

if __name__ == "__main__":
    try:
        test_all_stores()
    except KeyboardInterrupt:
        print("\n\n⚠️ Test interrupted by user")
    except Exception as e:
        print(f"\n\n❌ Fatal error: {e}")
        import traceback
        traceback.print_exc()