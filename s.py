#!/usr/bin/env python3
"""
GrabFood PH — Out-of-Stock (OOS) menu scraper (public user)
Now includes:
  --debug       show first 4000 chars of store JSON
  --explain     show why an item is flagged OOS
  --dump-items  print raw JSON for every menu item (first 2000 chars each)
"""

import re
import time
import json
import argparse
from datetime import datetime, timezone
from urllib.parse import urlparse
import requests
from typing import Optional, List, Dict, Any

# ===================== CONFIG =====================
PH_LATLNG = "14.5995,120.9842"  # Manila center; good default
REQUEST_TIMEOUT = 12
RETRIES = 3
RETRY_BACKOFF_SEC = 2.0

DEFAULT_URLS = [
    "https://food.grab.com/ph/en/restaurant/cocopan-vicas-camarin-delivery/2-C6TATLDTTE4BT2",
    "https://food.grab.com/ph/en/restaurant/cocopan-caa-delivery/2-C7EUVP2UDB3BRJ",
    "https://food.grab.com/ph/en/restaurant/cocopan-congressional-delivery/2-C7E1CYCVHALWE2",
    "https://food.grab.com/ph/en/restaurant/cocopan-anonas-delivery/2-C6XVCUDGNXNZNN",
]
# ==================================================

UA_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-PH,en;q=0.9",
    "Origin": "https://food.grab.com",
    "Referer": "https://food.grab.com/",
    "Connection": "keep-alive",
}

ID_REGEX = re.compile(r"/([0-9]-[A-Z0-9]+)$", re.IGNORECASE)


def extract_merchant_id(url: str) -> Optional[str]:
    parsed = urlparse(url)
    if "food.grab.com" not in parsed.netloc or "/ph/" not in parsed.path:
        return None
    m = ID_REGEX.search(parsed.path)
    return m.group(1) if m else None


def fetch_json(session: requests.Session, url: str) -> Optional[Dict[str, Any]]:
    last_err = None
    for attempt in range(1, RETRIES + 1):
        try:
            resp = session.get(url, headers=UA_HEADERS, timeout=REQUEST_TIMEOUT)
            if resp.status_code >= 500:
                last_err = f"{resp.status_code} {resp.reason}"
                time.sleep(RETRY_BACKOFF_SEC * attempt)
                continue
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as e:
            last_err = str(e)
            time.sleep(RETRY_BACKOFF_SEC * attempt)
    print(f"⚠️  Fetch failed for {url}: {last_err}")
    return None


def fetch_menu_data(session: requests.Session, merchant_id: str, referer_url: str) -> Optional[Dict[str, Any]]:
    a = f"https://portal.grab.com/foodweb/v2/restaurant?merchantCode={merchant_id}&latlng={PH_LATLNG}"
    b = f"https://portal.grab.com/foodweb/v2/merchants/{merchant_id}?latlng={PH_LATLNG}"
    UA_HEADERS["Referer"] = referer_url

    data = fetch_json(session, a)
    if data:
        return data
    return fetch_json(session, b)


def iter_possible_sections(data: Dict[str, Any]):
    if not data:
        return
    roots = [data]
    if "data" in data and isinstance(data["data"], dict):
        roots.append(data["data"])

    for root in roots:
        if not isinstance(root, dict):
            continue

        menu = root.get("menu")
        if isinstance(menu, dict):
            categories = menu.get("categories") or menu.get("sections")
            if isinstance(categories, list):
                for sec in categories:
                    yield sec

        merchant = root.get("merchant")
        if isinstance(merchant, dict):
            m_menu = merchant.get("menu")
            if isinstance(m_menu, dict):
                categories = m_menu.get("categories") or m_menu.get("sections")
                if isinstance(categories, list):
                    for sec in categories:
                        yield sec

            sections = merchant.get("sections")
            if isinstance(sections, list):
                for sec in sections:
                    yield sec


def iter_items_from_section(section: Dict[str, Any]):
    if not isinstance(section, dict):
        return
    for key in ("items", "itemList", "menuItems", "products", "dishes", "dishList"):
        items = section.get(key)
        if isinstance(items, list):
            for it in items:
                if isinstance(it, dict):
                    yield it


# Simplified detector: only obvious fields
UNAVAILABLE_STRINGS = {
    "UNAVAILABLE", "SOLD_OUT", "OUT_OF_STOCK", "NOT_AVAILABLE",
    "DISABLED", "INACTIVE", "TEMPORARILY_UNAVAILABLE"
}


def is_item_unavailable(item: Dict[str, Any]) -> bool:
    # If Grab explicitly says available: true → trust it
    if item.get("available") is True:
        return False

    # If available is false/missing → check metadata
    meta = item.get("metadata")
    if meta and isinstance(meta, str):
        try:
            meta_obj = json.loads(meta)
            keep_until = meta_obj.get("keep_unavailable_until")
            if keep_until:
                dt = None
                try:
                    dt = datetime.fromisoformat(keep_until.replace("Z", "+00:00"))
                except Exception:
                    pass
                if dt:
                    now = datetime.now(tz=dt.tzinfo) if dt.tzinfo else datetime.now(timezone.utc)
                    if now < dt:
                        return True  # still before the "available again" date
        except Exception:
            pass

    return False


def collect_oos_items(payload: Dict[str, Any]) -> List[str]:
    out = []
    for section in iter_possible_sections(payload):
        for item in iter_items_from_section(section):
            name = item.get("name") or item.get("title")
            if not name:
                continue
            if is_item_unavailable(item):
                out.append(name.strip())
    return out


def store_display_name(payload: Dict[str, Any]) -> Optional[str]:
    for root_key in ("merchant", "data", None):
        root = payload
        if root_key and isinstance(payload, dict):
            root = payload.get(root_key)
        if isinstance(root, dict):
            for k in ("name", "displayName", "merchantName", "restaurantName"):
                val = root.get(k)
                if isinstance(val, str) and val.strip():
                    return val.strip()
    return None


def main(urls: List[str], debug: bool = False, explain: bool = False, dump_items: bool = False):
    session = requests.Session()
    first_dumped = False

    for url in urls:
        merchant_id = extract_merchant_id(url)
        print(f"\n📍 Store URL: {url}")
        if not merchant_id:
            print("⚠️  Skipped (not a PH GrabFood restaurant URL).")
            continue

        payload = fetch_menu_data(session, merchant_id, url)
        if not payload:
            print("⚠️  Could not fetch menu data.")
            continue

        name = store_display_name(payload)
        if name:
            print(f"🏪 {name}")

        if debug and not first_dumped:
            print("— DEBUG JSON (first 4000 chars) —")
            print(json.dumps(payload, indent=2)[:4000])
            first_dumped = True

        if dump_items:
            for section in iter_possible_sections(payload):
                for item in iter_items_from_section(section):
                    if item.get("name"):
                        print(f"\n=== {item.get('name')} ===")
                        print(json.dumps(item, indent=2)[:2000])
            continue

        oos = collect_oos_items(payload)
        if oos:
            for item_name in oos:
                print(f"❌ OUT OF STOCK → {item_name}")
        else:
            print("✅ No out-of-stock items detected")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="GrabFood PH OOS scraper")
    parser.add_argument("urls", nargs="*", help="GrabFood PH restaurant URLs")
    parser.add_argument("--debug", action="store_true", help="Print first store's raw JSON")
    parser.add_argument("--explain", action="store_true", help="(placeholder, not used in this simplified build)")
    parser.add_argument("--dump-items", action="store_true", help="Dump raw JSON for each menu item (first 2000 chars)")
    args = parser.parse_args()

    urls_to_use = args.urls if args.urls else DEFAULT_URLS
    main(urls_to_use, debug=args.debug, explain=args.explain, dump_items=args.dump_items)
