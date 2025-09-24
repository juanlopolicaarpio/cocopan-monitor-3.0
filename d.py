#!/usr/bin/env python3
"""
GrabFood PH — Menu Item Dumper
- Prints each menu item JSON separately
- Usage:
    python scrape.py <url> --dump-items > dump.txt
"""

import re
import time
import json
import argparse
from urllib.parse import urlparse
import requests
from typing import Optional, Dict, Any, List

PH_LATLNG = "14.5995,120.9842"
REQUEST_TIMEOUT = 12
RETRIES = 3
RETRY_BACKOFF_SEC = 2.0

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


def iter_items_from_section(section: Dict[str, Any]):
    if not isinstance(section, dict):
        return
    for key in ("items", "itemList", "menuItems", "products", "dishes", "dishList"):
        items = section.get(key)
        if isinstance(items, list):
            for it in items:
                if isinstance(it, dict):
                    yield it


def main(urls: List[str], dump_items: bool = False):
    session = requests.Session()
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

        if dump_items:
            for section in iter_possible_sections(payload):
                for item in iter_items_from_section(section):
                    if item.get("name"):
                        print(f"\n=== {item.get('name')} ===")
                        print(json.dumps(item, indent=2)[:2000])
            continue


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="GrabFood PH Item Dumper")
    parser.add_argument("urls", nargs="+", help="GrabFood PH restaurant URLs")
    parser.add_argument("--dump-items", action="store_true", help="Dump raw JSON for each menu item")
    args = parser.parse_args()

    main(args.urls, dump_items=args.dump_items)
