#!/usr/bin/env python3
"""
CocoPan Rating Scraper - Complete Implementation
Scrapes store ratings from GrabFood and Foodpanda every 3 days
"""
import re
import time
import random
import logging
import json
import requests
from bs4 import BeautifulSoup
from typing import Optional, Dict, Any, List
from datetime import datetime, timedelta

import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

from config import config
from database import db

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class RatingScraper:
    """Scrapes ratings from GrabFood and Foodpanda pages"""
    
    def __init__(self):
        self.user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        ]
        self.timezone = config.get_timezone()
    
    def scrape_grabfood_rating(self, url: str) -> Optional[Dict[str, Any]]:
        """
        Scrape rating from GrabFood page
        Returns: {'rating': 4.5, 'success': True, 'method': 'exact_class'}
        """
        try:
            session = requests.Session()
            session.headers.update({
                'User-Agent': random.choice(self.user_agents),
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.9',
                'Accept-Encoding': 'gzip, deflate',
                'Connection': 'keep-alive',
            })
            
            # Anti-detection delay
            time.sleep(random.uniform(1, 3))
            
            try:
                resp = session.get(url, timeout=15, allow_redirects=True)
            except requests.exceptions.SSLError:
                resp = session.get(url, timeout=15, allow_redirects=True, verify=False)
            
            if resp.status_code != 200:
                logger.warning(f"GrabFood: HTTP {resp.status_code}")
                return None
            
            soup = BeautifulSoup(resp.text, 'html.parser')
            
            # Method 1: Exact class pattern
            rating_div = soup.find('div', class_=re.compile(r'ratingText'))
            if rating_div:
                rating_text = rating_div.get_text().strip()
                try:
                    rating = float(rating_text)
                    if 0 <= rating <= 5:
                        logger.debug(f"✓ GrabFood rating (exact): {rating}")
                        return {'rating': rating, 'success': True, 'method': 'exact_class'}
                except ValueError:
                    pass
            
            # Method 2: Search for rating-related divs
            rating_divs = soup.find_all('div', class_=re.compile(r'rating', re.IGNORECASE))
            for div in rating_divs[:5]:
                text = div.get_text().strip()
                match = re.search(r'(\d+\.?\d*)', text)
                if match:
                    try:
                        rating = float(match.group(1))
                        if 0 <= rating <= 5:
                            logger.debug(f"✓ GrabFood rating (search): {rating}")
                            return {'rating': rating, 'success': True, 'method': 'rating_search'}
                    except ValueError:
                        pass
            
            # Method 3: Text pattern matching
            text_content = soup.get_text()
            patterns = [
                r'(\d\.\d)\s*(?:out of|\/|\|)?\s*5',
                r'rating[:\s]*(\d\.\d)',
                r'(\d\.\d)\s*stars?',
            ]
            
            for pattern in patterns:
                match = re.search(pattern, text_content, re.IGNORECASE)
                if match:
                    try:
                        rating = float(match.group(1))
                        if 0 <= rating <= 5:
                            logger.debug(f"✓ GrabFood rating (pattern): {rating}")
                            return {'rating': rating, 'success': True, 'method': 'text_pattern'}
                    except ValueError:
                        pass
            
            logger.warning("GrabFood: Rating not found")
            return None
            
        except Exception as e:
            logger.error(f"GrabFood scrape error: {e}")
            return None
    
    def scrape_foodpanda_rating(self, url: str) -> Optional[Dict[str, Any]]:
        """
        Scrape rating from Foodpanda page
        Returns: {'rating': 5.0, 'success': True, 'method': 'sr_only'}
        """
        try:
            session = requests.Session()
            session.headers.update({
                'User-Agent': random.choice(self.user_agents),
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.9',
                'Accept-Encoding': 'gzip, deflate',
                'Connection': 'keep-alive',
            })
            
            # Anti-detection delay
            time.sleep(random.uniform(1, 3))
            
            try:
                resp = session.get(url, timeout=15, allow_redirects=True)
            except requests.exceptions.SSLError:
                resp = session.get(url, timeout=15, allow_redirects=True, verify=False)
            
            if resp.status_code != 200:
                logger.warning(f"Foodpanda: HTTP {resp.status_code}")
                return None
            
            soup = BeautifulSoup(resp.text, 'html.parser')
            
            # Method 1: sr-only span with "Rating X out of 5"
            sr_only_spans = soup.find_all('span', class_='sr-only')
            for span in sr_only_spans:
                text = span.get_text().strip()
                match = re.search(r'rating\s+(\d+\.?\d*)\s+out of\s+5', text, re.IGNORECASE)
                if match:
                    try:
                        rating = float(match.group(1))
                        if 0 <= rating <= 5:
                            logger.debug(f"✓ Foodpanda rating (sr-only): {rating}")
                            return {'rating': rating, 'success': True, 'method': 'sr_only'}
                    except ValueError:
                        pass
            
            # Method 2: Rating label class
            rating_labels = soup.find_all(class_=re.compile(r'rating.*label', re.IGNORECASE))
            for label in rating_labels:
                text = label.get_text().strip()
                match = re.search(r'(\d+\.?\d*)', text)
                if match:
                    try:
                        rating = float(match.group(1))
                        if 0 <= rating <= 5:
                            logger.debug(f"✓ Foodpanda rating (label): {rating}")
                            return {'rating': rating, 'success': True, 'method': 'rating_label'}
                    except ValueError:
                        pass
            
            # Method 3: Any element with 'rating' in class
            rating_elements = soup.find_all(class_=re.compile(r'rating', re.IGNORECASE))
            for elem in rating_elements[:5]:
                text = elem.get_text().strip()
                match = re.search(r'(\d+\.?\d*)', text)
                if match:
                    try:
                        rating = float(match.group(1))
                        if 0 <= rating <= 5:
                            logger.debug(f"✓ Foodpanda rating (search): {rating}")
                            return {'rating': rating, 'success': True, 'method': 'rating_search'}
                    except ValueError:
                        pass
            
            # Method 4: Text patterns
            text_content = soup.get_text()
            patterns = [
                r'rating\s+(\d+\.?\d*)\s+out of\s+5',
                r'(\d+\.?\d*)\s+out of\s+5\s+stars',
                r'(\d+\.?\d*)\/5',
            ]
            
            for pattern in patterns:
                match = re.search(pattern, text_content, re.IGNORECASE)
                if match:
                    try:
                        rating = float(match.group(1))
                        if 0 <= rating <= 5:
                            logger.debug(f"✓ Foodpanda rating (pattern): {rating}")
                            return {'rating': rating, 'success': True, 'method': 'text_pattern'}
                    except ValueError:
                        pass
            
            logger.warning("Foodpanda: Rating not found")
            return None
            
        except Exception as e:
            logger.error(f"Foodpanda scrape error: {e}")
            return None
    
    def scrape_store_rating(self, url: str, platform: str) -> Optional[Dict[str, Any]]:
        """Universal scraper that routes to correct platform"""
        if platform == 'grabfood':
            return self.scrape_grabfood_rating(url)
        elif platform == 'foodpanda':
            return self.scrape_foodpanda_rating(url)
        else:
            logger.error(f"Unknown platform: {platform}")
            return None


class RatingMonitor:
    """Monitors and tracks store ratings every 3 days"""
    
    def __init__(self):
        self.scraper = RatingScraper()
        self.timezone = config.get_timezone()
    
    def should_scrape_today(self) -> bool:
        """Check if we should scrape ratings today (every 3 days)"""
        try:
            with db.get_connection() as conn:
                cur = conn.cursor()
                
                if db.db_type == "postgresql":
                    cur.execute("SELECT MAX(last_scraped_at) FROM current_store_ratings")
                else:
                    cur.execute("SELECT MAX(last_scraped_at) FROM current_store_ratings")
                
                row = cur.fetchone()
                if not row or not row[0]:
                    logger.info("📊 No previous scrape - will scrape today")
                    return True
                
                last_scrape = row[0]
                if isinstance(last_scrape, str):
                    last_scrape = datetime.fromisoformat(last_scrape.replace('Z', '+00:00'))
                
                # Remove timezone info for comparison
                if hasattr(last_scrape, 'tzinfo') and last_scrape.tzinfo:
                    last_scrape = last_scrape.replace(tzinfo=None)
                
                days_since = (datetime.now() - last_scrape).days
                
                if days_since >= 3:
                    logger.info(f"📊 Last scrape: {days_since} days ago - will scrape")
                    return True
                else:
                    logger.info(f"📊 Last scrape: {days_since} days ago - skip (next in {3 - days_since} days)")
                    return False
                
        except Exception as e:
            logger.error(f"Error checking schedule: {e}")
            # If error, scrape anyway
            return True
    
    def load_store_urls(self) -> List[Dict[str, str]]:
        """Load store URLs from branch_urls.json"""
        try:
            with open(config.STORE_URLS_FILE, 'r') as f:
                data = json.load(f)
                all_urls = data.get('urls', [])
            
            stores = []
            for url in all_urls:
                if 'grab.com' in url:
                    platform = 'grabfood'
                elif 'foodpanda' in url:
                    platform = 'foodpanda'
                else:
                    continue
                
                stores.append({'url': url, 'platform': platform})
            
            logger.info(f"📋 Loaded {len(stores)} stores from {config.STORE_URLS_FILE}")
            grabfood_count = sum(1 for s in stores if s['platform'] == 'grabfood')
            foodpanda_count = sum(1 for s in stores if s['platform'] == 'foodpanda')
            logger.info(f"   🛒 GrabFood: {grabfood_count}")
            logger.info(f"   🐼 Foodpanda: {foodpanda_count}")
            
            return stores
            
        except Exception as e:
            logger.error(f"Failed to load store URLs: {e}")
            return []
    
    def extract_store_name(self, url: str) -> str:
        """Extract store name from URL"""
        try:
            if 'grab.com' in url:
                match = re.search(r'/restaurant/([^/]+)', url)
                if match:
                    name = match.group(1).replace('-delivery', '').replace('-', ' ').title()
                    return f"Cocopan {name}" if not name.lower().startswith('cocopan') else name
            elif 'foodpanda' in url:
                parts = url.rstrip('/').split('/')
                if len(parts) > 0:
                    name = parts[-1].replace('-', ' ').title()
                    return f"Cocopan {name}" if not name.lower().startswith('cocopan') else name
        except Exception:
            pass
        return "Cocopan Store (Unknown)"
    
    def scrape_all_stores(self) -> Dict[str, Any]:
        """
        Scrape ratings for ALL stores from branch_urls.json
        Returns summary statistics
        """
        logger.info("=" * 70)
        logger.info("🌟 STORE RATING SCRAPING STARTED")
        logger.info("=" * 70)
        
        # Load stores
        stores = self.load_store_urls()
        if not stores:
            logger.error("❌ No stores to scrape!")
            return {'success': False, 'error': 'No stores loaded'}
        
        results = {
            'total_stores': len(stores),
            'successful': 0,
            'failed': 0,
            'scraper_blocked': 0,
            'alerts_created': 0,
            'store_results': []
        }
        
        for i, store_info in enumerate(stores, 1):
            url = store_info['url']
            platform = store_info['platform']
            
            platform_emoji = "🛒" if platform == "grabfood" else "🐼"
            logger.info(f"\n[{i}/{len(stores)}] {platform_emoji} {platform.upper()}")
            logger.info(f"   {url}")
            
            try:
                # Scrape rating
                rating_data = self.scraper.scrape_store_rating(url, platform)
                
                if rating_data and rating_data['success']:
                    rating = rating_data['rating']
                    
                    # Get store name and ID
                    store_name = self.extract_store_name(url)
                    store_id = db.get_or_create_store(store_name, url)
                    
                    # Save rating
                    success = db.save_store_rating(
                        store_id=store_id,
                        platform=platform,
                        rating=rating,
                        manual_entry=False
                    )
                    
                    if success:
                        results['successful'] += 1
                        logger.info(f"   ✅ {store_name}: {rating:.1f}★")
                        
                        # Check for new alerts
                        alerts = db.get_rating_alerts(acknowledged=False)
                        store_alerts = [a for a in alerts if a['store_name'] == store_name]
                        if store_alerts:
                            results['alerts_created'] += len(store_alerts)
                            for alert in store_alerts:
                                logger.warning(f"   🚨 ALERT: {alert['message']}")
                    else:
                        results['failed'] += 1
                        logger.error(f"   ❌ Failed to save rating")
                else:
                    results['scraper_blocked'] += 1
                    logger.warning(f"   ⚠️ Could not scrape rating (blocked or not found)")
                
                results['store_results'].append({
                    'url': url,
                    'platform': platform,
                    'success': rating_data['success'] if rating_data else False,
                    'rating': rating_data['rating'] if rating_data and rating_data['success'] else None
                })
                
            except Exception as e:
                logger.error(f"   ❌ Error: {e}")
                results['failed'] += 1
            
            # Anti-detection delay
            if i < len(stores):
                delay = random.uniform(3, 6)
                logger.debug(f"   ⏱️ Waiting {delay:.1f}s...")
                time.sleep(delay)
        
        # Summary
        logger.info("\n" + "=" * 70)
        logger.info("🌟 SCRAPING COMPLETE")
        logger.info("=" * 70)
        logger.info(f"📊 Results:")
        logger.info(f"   Total stores:     {results['total_stores']}")
        logger.info(f"   ✅ Successful:     {results['successful']}")
        logger.info(f"   ❌ Failed:         {results['failed']}")
        logger.info(f"   🚫 Blocked:        {results['scraper_blocked']}")
        logger.info(f"   🚨 Alerts created: {results['alerts_created']}")
        
        success_rate = (results['successful'] / results['total_stores'] * 100) if results['total_stores'] > 0 else 0
        logger.info(f"   📈 Success rate:   {success_rate:.1f}%")
        
        if results['scraper_blocked'] > 0:
            logger.warning(f"\n⚠️ {results['scraper_blocked']} stores couldn't be scraped (blocked/not found)")
            logger.warning("   Consider manual entry for these stores in admin dashboard")
        
        if results['alerts_created'] > 0:
            logger.warning(f"\n🚨 {results['alerts_created']} rating alerts created!")
            logger.warning("   Check dashboard for details")
        
        logger.info("=" * 70)
        
        return results


def rating_scraper_job():
    """
    Main job function for scheduler
    Checks if should scrape, then scrapes if needed
    """
    monitor = RatingMonitor()
    
    # Check if we should scrape today
    if not monitor.should_scrape_today():
        logger.info("⏭️ Skipping rating scrape - not scheduled for today")
        return
    
    # Scrape all stores
    results = monitor.scrape_all_stores()
    
    return results


if __name__ == "__main__":
    """
    Run this file directly to manually scrape ratings
    Usage: python rating_scraper.py
    """
    print("🌟 Manual Rating Scraper")
    print("=" * 70)
    print()
    
    monitor = RatingMonitor()
    
    # Always scrape when run manually (ignore 3-day check)
    results = monitor.scrape_all_stores()
    
    print("\n✅ Manual scrape complete!")
    print(f"Check your database for {results['successful']} new ratings")