# gf_online_check_test.py
import json, random, re, time, argparse
from urllib.parse import urlparse
import requests
from bs4 import BeautifulSoup

UA_POOL = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
]
PH_LATLNG = "14.5995,120.9842"  # Manila center

OFFLINE_HINTS = {
    "today closed", "restaurant is closed", "currently unavailable",
    "not accepting orders", "temporarily closed", "currently closed",
    "closed for today", "restaurant closed"
}
ONLINE_HINTS = {"order now", "add to basket", "delivery fee", "menu"}

def _merchant_code_from_url(url: str):
    try:
        p = urlparse(url)
        if "food.grab.com" not in p.netloc:
            return None
        m = re.search(r"/([0-9]-[A-Z0-9]+)$", p.path, re.I)
        return m.group(1) if m else None
    except Exception:
        return None

def _get_session():
    s = requests.Session()
    s.headers.update({
        "User-Agent": random.choice(UA_POOL),
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-PH,en;q=0.9",
        "Origin": "https://food.grab.com",
        "Referer": "https://food.grab.com/",
        "Connection": "keep-alive",
    })
    return s

def _get_json(s: requests.Session, url: str, timeout=18, retries=2):
    last_err = None
    for i in range(retries + 1):
        try:
            time.sleep(random.uniform(0.8, 1.8))
            r = s.get(url, timeout=timeout)
            if r.status_code in (403, 429) and i < retries:
                # light backoff + rotate UA
                s.headers["User-Agent"] = random.choice(UA_POOL)
                time.sleep(1.5 + i)
                continue
            r.raise_for_status()
            return r.json()
        except Exception as e:
            last_err = e
            if i < retries:
                time.sleep(1.2 + i)
    return None

def _page_text(s: requests.Session, url: str, timeout=18):
    try:
        r = s.get(url, timeout=timeout, allow_redirects=True)
        soup = BeautifulSoup(r.text, "html.parser")
        for tag in soup(["script", "style"]): tag.decompose()
        return r.status_code, " ".join(soup.get_text().lower().split())
    except Exception:
        return None, ""

def check_online(url: str, verbose=False):
    """
    Returns tuple: (status_str, reason)
    status_str: ONLINE | OFFLINE | BLOCKED | ERROR | UNKNOWN
    """
    ses = _get_session()
    mc = _merchant_code_from_url(url)

    # 1) Try JSON portal APIs (preferred & robust)
    if mc:
        api_urls = [
            f"https://portal.grab.com/foodweb/v2/restaurant?merchantCode={mc}&latlng={PH_LATLNG}",
            f"https://portal.grab.com/foodweb/v2/merchants/{mc}?latlng={PH_LATLNG}",
        ]
        for i, api in enumerate(api_urls, 1):
            data = _get_json(ses, api)
            if not data:
                if verbose: print(f"  api#{i}: failed")
                continue

            # Normalize a few possible shapes
            roots = [data]
            if isinstance(data, dict) and isinstance(data.get("data"), dict):
                roots.append(data["data"])

            # Try to read availability
            for root in roots:
                if not isinstance(root, dict): continue
                cand = root.get("merchant") or root
                if isinstance(cand, dict):
                    # any of these sometimes appear depending on region/build
                    flags = [
                        cand.get("isOpen"),
                        cand.get("open"),
                        cand.get("availability", {}).get("isOpen") if isinstance(cand.get("availability"), dict) else None,
                        cand.get("status")  # sometimes "OPEN","CLOSED"
                    ]
                    if verbose: print(f"  api#{i}: flags={flags}")
                    if any(f is True or f == "OPEN" for f in flags):
                        return "ONLINE", "portal api"
                    if any(f is False or f == "CLOSED" for f in flags):
                        return "OFFLINE", "portal api"
            # If we reached here we got JSON but couldn’t determine status—fall through

    # 2) Fallback: HTML text heuristics (less reliable)
    code, text = _page_text(ses, url)
    if code is None:
        return "ERROR", "html fetch failed"
    if code == 403:
        return "BLOCKED", "403"
    if code == 404:
        return "OFFLINE", "404"
    if any(h in text for h in OFFLINE_HINTS):
        return "OFFLINE", "offline-phrase"
    if any(h in text for h in ONLINE_HINTS):
        return "ONLINE", "online-phrase"
    if len(text) > 500 and any(w in text for w in ("menu", "order", "delivery", "add")):
        return "ONLINE", "content-heuristic"
    return "UNKNOWN", f"http {code}"

def load_urls(path: str):
    try:
        with open(path, "r") as f:
            payload = json.load(f)
        return [u for u in payload.get("urls", []) if "grab.com" in u]
    except Exception:
        return []

def main():
    ap = argparse.ArgumentParser(description="DB-free GrabFood online checker")
    ap.add_argument("--urls", default="branch_urls.json", help="path to branch_urls.json")
    ap.add_argument("-v", "--verbose", action="store_true")
    args = ap.parse_args()

    urls = load_urls(args.urls)
    if not urls:
        print("no grabfood urls found in file; add them under { 'urls': [...] }")
        return

    print(f"checking {len(urls)} stores…\n")
    online = offline = blocked = unknown = error = 0

    for i, url in enumerate(urls, 1):
        status, reason = check_online(url, verbose=args.verbose)
        mark = {"ONLINE":"🟢","OFFLINE":"🔴","BLOCKED":"🚫","ERROR":"⚠️","UNKNOWN":"❓"}[status]
        print(f"[{i:02d}] {mark} {status:7} — {url} ({reason})")

        if status == "ONLINE": online += 1
        elif status == "OFFLINE": offline += 1
        elif status == "BLOCKED": blocked += 1
        elif status == "ERROR": error += 1
        else: unknown += 1

        if i < len(urls): time.sleep(random.uniform(0.8, 1.4))  # gentle pacing

    print("\nsummary:")
    print(f"  online : {online}")
    print(f"  offline: {offline}")
    print(f"  blocked: {blocked}")
    print(f"  error  : {error}")
    print(f"  unknown: {unknown}")

if __name__ == "__main__":
    main()
