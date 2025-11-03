#!/usr/bin/env python3
"""
Grab Food Ultimate Debug Scraper - All-in-One (fixed paths)
- Takes screenshots
- Saves HTML
- Prints to console
- Immediate retry option
- Simple file naming (no subdirectories)
"""

import json
import os
import time
import random
from pathlib import Path
from typing import List, Dict, Optional

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class GrabUltimateDebugger:
    """All-in-one debug scraper"""

    def __init__(self, headless: bool = False, max_retries: int = 5, immediate_retry: bool = True, output_dir: Optional[str] = None):
        self.max_retries = max_retries
        self.headless = headless
        self.immediate_retry = immediate_retry
        self.driver: Optional[webdriver.Chrome] = None
        self.session_id = int(time.time())  # Unique session ID

        # ---------- Output directory ----------
        # Priority: explicit arg > env var > ~/grab_debug_outputs
        env_dir = os.environ.get("GRAB_OUTPUT_DIR")
        self.output_dir = Path(output_dir or env_dir or (Path.home() / "grab_debug_outputs")).expanduser()
        self.output_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"✓ Output folder: {self.output_dir}")

        self._setup_driver()

    # ---------- Paths ----------
    def _file_base(self, attempt: int, stage: str) -> Path:
        """Base filename without extension."""
        safe_stage = "".join(c if c.isalnum() or c in "-_" else "_" for c in stage)
        return self.output_dir / f"grab_{self.session_id}_attempt{attempt}_{safe_stage}"

    def _save_text(self, path: Path, text: str) -> None:
        path.write_text(text, encoding="utf-8")
        logger.info(f"💾 Saved: {path}")

    def _save_json_obj(self, path: Path, obj: dict) -> None:
        path.write_text(json.dumps(obj, indent=2, ensure_ascii=False), encoding="utf-8")
        logger.info(f"💾 Saved: {path}")

    # ---------- Browser ----------
    def _setup_driver(self):
        """Setup Chrome"""
        chrome_options = Options()
        if self.headless:
            # Use new headless for modern Chrome
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
        logger.info("✓ Browser ready")

    # ---------- Debug printing ----------
    def _print_page_info(self, attempt: int, stage: str) -> bool:
        print(f"\n{'='*80}")
        print(f"ATTEMPT {attempt} - {stage.upper()}")
        print(f"{'='*80}")

        print(f"\n📍 URL: {self.driver.current_url}")
        print(f"📄 Title: {self.driver.title}")
        html = self.driver.page_source
        print(f"📏 HTML Length: {len(html):,} chars")

        soup = BeautifulSoup(html, 'html.parser')
        for tag in soup(["script", "style", "meta", "link"]):
            tag.decompose()

        visible_text = soup.get_text(separator='\n', strip=True)
        lines = [line.strip() for line in visible_text.split('\n') if line.strip()]

        print(f"\n--- VISIBLE TEXT (First 20 lines) ---")
        for i, line in enumerate(lines[:20], 1):
            if len(line) > 75:
                line = line[:72] + "..."
            print(f"{i:2d}. {line}")

        # Simple error heuristics
        page_lower = html.lower()
        errors = []
        if '401' in page_lower or '401' in self.driver.title:
            errors.append("❌ 401 Unauthorized")
        if 'oops' in page_lower:
            errors.append("❌ Oops Error")
        if 'login to search' in page_lower:
            errors.append("❌ Login Required")
        if 'cloudflare' in page_lower:
            errors.append("⚠️  Cloudflare Challenge")

        if errors:
            print(f"\n🚨 ERRORS FOUND:")
            for err in errors:
                print(f"   {err}")
            return False

        # Check for __NEXT_DATA__
        next_data = soup.find('script', {'id': '__NEXT_DATA__'})
        if next_data and next_data.string:
            print(f"\n✅ __NEXT_DATA__ FOUND!")
            try:
                data = json.loads(next_data.string)
                props = data.get('props', {}).get('pageProps', {})
                if 'merchant' in props:
                    merchant = props['merchant']
                    print(f"   🏪 Name: {merchant.get('name', 'N/A')}")
                    print(f"   ⭐ Rating: {merchant.get('rating', 'N/A')}")
                if 'menu' in props:
                    categories = props['menu'].get('categories', [])
                    total_items = sum(len(cat.get('items', [])) for cat in categories)
                    print(f"   🍽️  Menu Items: {total_items}")
                return True
            except Exception:
                pass
        else:
            print(f"\n❌ NO __NEXT_DATA__ FOUND")

        return False

    # ---------- Save artifacts ----------
    def _save_files(self, attempt: int, stage: str):
        base = self._file_base(attempt, stage)

        # Screenshot
        png_path = base.with_suffix(".png")
        try:
            if self.driver.save_screenshot(str(png_path)):
                logger.info(f"📸 Screenshot: {png_path}")
            else:
                logger.warning("Screenshot returned False (not saved).")
        except Exception as e:
            logger.debug(f"Screenshot error: {e}")

        # HTML
        html_path = base.with_suffix(".html")
        try:
            self._save_text(html_path, self.driver.page_source)
        except Exception as e:
            logger.debug(f"HTML save error: {e}")

        # Info JSON
        info_path = base.with_suffix(".json")
        info = {
            'attempt': attempt,
            'stage': stage,
            'url': self.driver.current_url,
            'title': self.driver.title,
            'timestamp': time.time(),
            'screenshot': str(png_path),
            'html': str(html_path)
        }
        try:
            self._save_json_obj(info_path, info)
        except Exception as e:
            logger.debug(f"Info JSON save error: {e}")

    # ---------- Main ----------
    def scrape(self, url: str) -> dict:
        print("\n" + "="*80)
        print("GRAB FOOD ULTIMATE DEBUG SCRAPER")
        print("="*80)
        print(f"\nTarget: {url}")
        print(f"Max Attempts: {self.max_retries}")
        print(f"Immediate Retry: {self.immediate_retry}")
        print(f"Session ID: {self.session_id}")
        print(f"Output Dir: {self.output_dir}")
        print("="*80)

        for attempt in range(1, self.max_retries + 1):
            print(f"\n\n{'█'*80}")
            print(f"{'█'*80}")
            print(f"█ ATTEMPT {attempt}/{self.max_retries}".ljust(79) + "█")
            print(f"{'█'*80}")
            print(f"{'█'*80}\n")

            if attempt > 1:
                if self.immediate_retry:
                    print("🚀 IMMEDIATE RETRY (no delay)")
                else:
                    wait = 2 ** attempt
                    print(f"⏳ Waiting {wait}s...")
                    time.sleep(wait)

            try:
                # Load page
                print("📥 Loading page...")
                self.driver.get(url)
                time.sleep(3)

                # Initial check
                print("\n--- INITIAL LOAD ---")
                self._save_files(attempt, 'initial')
                success = self._print_page_info(attempt, 'initial')

                if not success:
                    print("\n❌ Error detected - trying refresh...")
                    self.driver.refresh()
                    time.sleep(3)

                    print("\n--- AFTER REFRESH ---")
                    self._save_files(attempt, 'refresh')
                    success = self._print_page_info(attempt, 'refresh')

                if success:
                    print("\n✅✅✅ SUCCESS! ✅✅✅")
                    print("\n📜 Scrolling page...")
                    self._scroll_page()

                    print("\n--- FINAL STATE ---")
                    self._save_files(attempt, 'final')
                    self._print_page_info(attempt, 'final')

                    # Extract data
                    result = self._extract_data()

                    # Save result
                    result_file = self.output_dir / f"grab_{self.session_id}_result.json"
                    self._save_json_obj(result_file, result)

                    print(f"\n✅ Result saved: {result_file}")
                    return result
                else:
                    if attempt < self.max_retries:
                        print(f"\n❌ Failed - moving to attempt {attempt + 1}")
                    else:
                        print("\n❌❌❌ All attempts failed ❌❌❌")
                        self._save_files(attempt, 'failed')

            except Exception as e:
                print(f"\n❌ Exception: {e}")
                self._save_files(attempt, 'error')
                if attempt == self.max_retries:
                    import traceback
                    print(traceback.format_exc())

        return {'error': 'Failed after all attempts'}

    # ---------- Helpers ----------
    def _scroll_page(self):
        try:
            total = self.driver.execute_script("return document.body.scrollHeight")
            viewport = self.driver.execute_script("return window.innerHeight")
            current = 0
            while current < total:
                self.driver.execute_script(f"window.scrollTo(0, {current})")
                current += max(200, viewport // 2)
                time.sleep(0.3)
            self.driver.execute_script("window.scrollTo(0, 0)")
            time.sleep(1)
        except Exception:
            pass

    def _extract_data(self) -> dict:
        soup = BeautifulSoup(self.driver.page_source, 'html.parser')

        result = {
            'url': self.driver.current_url,
            'title': self.driver.title,
            'session_id': self.session_id,
            'timestamp': time.time(),
            'data': {
                'restaurant': {},
                'menu_items': []
            }
        }

        script = soup.find('script', {'id': '__NEXT_DATA__'})
        if script and script.string:
            try:
                json_data = json.loads(script.string)
                props = json_data.get('props', {}).get('pageProps', {})

                if 'merchant' in props:
                    merchant = props['merchant']
                    result['data']['restaurant'] = {
                        'id': merchant.get('id'),
                        'name': merchant.get('name'),
                        'rating': merchant.get('rating'),
                        'cuisine': merchant.get('cuisines', []),
                        'delivery_fee': merchant.get('deliveryFee'),
                        'delivery_time': merchant.get('estimatedDeliveryTime')
                    }

                if 'menu' in props:
                    for cat in props['menu'].get('categories', []):
                        for item in cat.get('items', []):
                            result['data']['menu_items'].append({
                                'name': item.get('name'),
                                'price': item.get('price'),
                                'category': cat.get('name')
                            })

                result['json_extracted'] = True
            except Exception as e:
                result['json_error'] = str(e)

        return result

    def close(self):
        if self.driver:
            self.driver.quit()
            print("\n✓ Browser closed")


def main():
    url = "https://food.grab.com/ph/en/restaurant/cocopan-evangelista-delivery/2-C7K2LTJDNE2VAE"

    scraper = GrabUltimateDebugger(
        headless=False,          # Set to True to hide browser
        max_retries=5,           # Number of attempts
        immediate_retry=True     # True = no delays, False = exponential backoff
        # output_dir="~/Desktop/grab_outputs"  # (optional) override
    )

    try:
        result = scraper.scrape(url)

        print("\n" + "="*80)
        print("FINAL SUMMARY")
        print("="*80)

        if 'error' in result:
            print(f"\n❌ {result['error']}")
        else:
            restaurant = result['data']['restaurant']
            menu_items = result['data']['menu_items']

            print(f"\n✅ Successfully scraped!")
            print(f"\nRestaurant: {restaurant.get('name', 'N/A')}")
            print(f"Rating: {restaurant.get('rating', 'N/A')}")
            print(f"Menu Items: {len(menu_items)}")

            if menu_items:
                print(f"\nSample Items:")
                for item in menu_items[:5]:
                    price = item.get('price')
                    price_str = f"₱{price}" if price is not None else "N/A"
                    print(f"  • {item.get('name','N/A')} - {price_str}")

        print(f"\n📂 All files saved with session ID: {scraper.session_id}")
        print(f"   Format: grab_{scraper.session_id}_attempt*")
        print(f"   Location: {scraper.output_dir}")

    finally:
        scraper.close()


if __name__ == "__main__":
    main()
