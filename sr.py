#!/usr/bin/env python3
"""
Rating Scraper Test - Check if we can scrape ratings from GrabFood and Foodpanda
Tests the HTML patterns you provided to see if scraping is viable
"""
import re
import time
import random
import requests
from bs4 import BeautifulSoup
from typing import Optional, Dict, Any
import urllib3

# Disable SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

class RatingScraper:
    """Test scraper for GrabFood and Foodpanda ratings"""
    
    def __init__(self):
        self.user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        ]
    
    def scrape_grabfood_rating(self, url: str) -> Dict[str, Any]:
        """
        Scrape rating from GrabFood page
        Target: <div class="ratingText___1Q08c">4.5</div>
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
                return {
                    'success': False,
                    'error': f'HTTP {resp.status_code}',
                    'rating': None
                }
            
            soup = BeautifulSoup(resp.text, 'html.parser')
            
            # Method 1: Look for the exact class you provided
            rating_div = soup.find('div', class_='ratingText___1Q08c')
            if rating_div:
                rating_text = rating_div.get_text().strip()
                return {
                    'success': True,
                    'method': 'exact_class',
                    'rating': rating_text,
                    'html': str(rating_div)
                }
            
            # Method 2: Look for any div with class containing 'rating'
            rating_divs = soup.find_all('div', class_=re.compile(r'rating', re.IGNORECASE))
            if rating_divs:
                for i, div in enumerate(rating_divs[:5], 1):
                    text = div.get_text().strip()
                    match = re.search(r'(\d+\.?\d*)', text)
                    if match:
                        rating = match.group(1)
                        try:
                            rating_float = float(rating)
                            if 0 <= rating_float <= 5:
                                return {
                                    'success': True,
                                    'method': 'rating_class_search',
                                    'rating': rating,
                                    'html': str(div)
                                }
                        except ValueError:
                            pass
            
            # Method 3: Look for star/rating patterns in text
            text_content = soup.get_text()
            rating_patterns = [
                r'(\d\.\d)\s*(?:out of|\/|\|)?\s*5',
                r'rating[:\s]*(\d\.\d)',
                r'(\d\.\d)\s*stars?',
            ]
            
            for pattern in rating_patterns:
                match = re.search(pattern, text_content, re.IGNORECASE)
                if match:
                    rating = match.group(1)
                    return {
                        'success': True,
                        'method': 'text_pattern',
                        'rating': rating,
                        'pattern': pattern
                    }
            
            return {
                'success': False,
                'error': 'Rating not found in any expected location',
                'rating': None
            }
            
        except Exception as e:
            return {
                'success': False,
                'error': str(e),
                'rating': None
            }
    
    def scrape_foodpanda_rating(self, url: str) -> Dict[str, Any]:
        """
        Scrape rating from Foodpanda page
        Target: <span class="sr-only">Rating 5 out of 5 stars, 500+ reviews</span>
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
                return {
                    'success': False,
                    'error': f'HTTP {resp.status_code}',
                    'rating': None
                }
            
            soup = BeautifulSoup(resp.text, 'html.parser')
            
            # Method 1: Look for sr-only span with rating text
            sr_only_spans = soup.find_all('span', class_='sr-only')
            if sr_only_spans:
                for span in sr_only_spans:
                    text = span.get_text().strip()
                    # Look for "Rating X out of 5" pattern
                    match = re.search(r'rating\s+(\d+\.?\d*)\s+out of\s+5', text, re.IGNORECASE)
                    if match:
                        rating = match.group(1)
                        return {
                            'success': True,
                            'method': 'sr_only_span',
                            'rating': rating,
                            'html': str(span)
                        }
            
            # Method 2: Look for the specific class
            rating_labels = soup.find_all(class_='bds-c-rating__label-primary')
            if rating_labels:
                for label in rating_labels:
                    text = label.get_text().strip()
                    match = re.search(r'(\d+\.?\d*)', text)
                    if match:
                        rating = match.group(1)
                        try:
                            rating_float = float(rating)
                            if 0 <= rating_float <= 5:
                                return {
                                    'success': True,
                                    'method': 'rating_label_class',
                                    'rating': rating,
                                    'html': str(label)
                                }
                        except ValueError:
                            pass
            
            # Method 3: Look for any element with 'rating' in class name
            rating_elements = soup.find_all(class_=re.compile(r'rating', re.IGNORECASE))
            if rating_elements:
                for elem in rating_elements[:5]:
                    text = elem.get_text().strip()
                    match = re.search(r'(\d+\.?\d*)', text)
                    if match:
                        rating = match.group(1)
                        try:
                            rating_float = float(rating)
                            if 0 <= rating_float <= 5:
                                return {
                                    'success': True,
                                    'method': 'rating_class_search',
                                    'rating': rating,
                                    'html': str(elem)
                                }
                        except ValueError:
                            pass
            
            # Method 4: Search for rating patterns in all text
            text_content = soup.get_text()
            rating_patterns = [
                r'rating\s+(\d+\.?\d*)\s+out of\s+5',
                r'(\d+\.?\d*)\s+out of\s+5\s+stars',
                r'(\d+\.?\d*)\/5',
            ]
            
            for pattern in rating_patterns:
                match = re.search(pattern, text_content, re.IGNORECASE)
                if match:
                    rating = match.group(1)
                    return {
                        'success': True,
                        'method': 'text_pattern',
                        'rating': rating,
                        'pattern': pattern
                    }
            
            return {
                'success': False,
                'error': 'Rating not found in any expected location',
                'rating': None
            }
            
        except Exception as e:
            return {
                'success': False,
                'error': str(e),
                'rating': None
            }


def load_urls_from_config():
    """Load URLs from branch_urls.json"""
    try:
        import json
        with open('branch_urls.json', 'r') as f:
            data = json.load(f)
            all_urls = data.get('urls', [])
            
            grabfood_urls = [url for url in all_urls if 'grab.com' in url]
            foodpanda_urls = [url for url in all_urls if 'foodpanda' in url]
            
            print(f"📋 Loaded from branch_urls.json:")
            print(f"   🛒 GrabFood: {len(grabfood_urls)} stores")
            print(f"   🐼 Foodpanda: {len(foodpanda_urls)} stores")
            
            return grabfood_urls, foodpanda_urls
    except Exception as e:
        print(f"⚠️ Could not load branch_urls.json: {e}")
        return [], []


def main():
    """Test rating scraping for both platforms"""
    print("=" * 70)
    print("RATING SCRAPER TEST")
    print("=" * 70)
    print("\nThis script tests if we can scrape ratings from:")
    print("  GrabFood: Target class 'ratingText___1Q08c'")
    print("  Foodpanda: Target 'sr-only' span with rating text")
    print()
    
    scraper = RatingScraper()
    
    # Try to load URLs from config
    grabfood_urls, foodpanda_urls = load_urls_from_config()
    
    # Build test cases from actual config
    test_cases = []
    
    # Test ALL stores
    print(f"\nTest Plan:")
    print(f"  Testing ALL {len(grabfood_urls)} GrabFood stores")
    print(f"  Testing ALL {len(foodpanda_urls)} Foodpanda stores")
    print(f"  Total: {len(grabfood_urls) + len(foodpanda_urls)} stores")
    print()
    
    if grabfood_urls:
        for i, url in enumerate(grabfood_urls, 1):
            store_name = url.split('/')[-2].replace('-delivery', '').replace('-', ' ').title() if '/' in url else 'Unknown'
            test_cases.append({
                'name': f'GrabFood #{i}: {store_name}',
                'platform': 'grabfood',
                'url': url
            })
    
    if foodpanda_urls:
        for i, url in enumerate(foodpanda_urls, 1):
            store_name = url.split('/')[-1].replace('-', ' ').title() if '/' in url else 'Unknown'
            test_cases.append({
                'name': f'Foodpanda #{i}: {store_name}',
                'platform': 'foodpanda',
                'url': url
            })
    
    if not test_cases:
        print("\nERROR: No URLs found to test!")
        print("Please check that branch_urls.json exists and has URLs")
        return
    
    results = []
    
    print("Starting rating scrape...")
    print("=" * 70)
    
    for i, test in enumerate(test_cases, 1):
        # Show what we're testing with full details
        print(f"\n[{i}/{len(test_cases)}] {test['name']}")
        print(f"    {test['url']}")
        print(f"    ", end='', flush=True)
        
        if test['platform'] == 'grabfood':
            result = scraper.scrape_grabfood_rating(test['url'])
        else:
            result = scraper.scrape_foodpanda_rating(test['url'])
        
        results.append({
            'name': test['name'],
            'platform': test['platform'],
            'url': test['url'],
            'result': result
        })
        
        # Show result
        if result['success']:
            print(f"✓ Rating: {result['rating']} stars")
        else:
            print(f"✗ No rating found")
        
        # Delay between requests (be nice to servers)
        if i < len(test_cases):
            time.sleep(random.uniform(2, 4))
    
    print("\n" + "=" * 70)
    
    # Summary by platform
    print("\n" + "=" * 70)
    print("SUMMARY BY PLATFORM")
    print("=" * 70)
    
    grabfood_results = [r for r in results if r['platform'] == 'grabfood']
    foodpanda_results = [r for r in results if r['platform'] == 'foodpanda']
    
    if grabfood_results:
        grab_success = sum(1 for r in grabfood_results if r['result']['success'])
        print(f"\nGrabFood: {grab_success}/{len(grabfood_results)} successful ({grab_success/len(grabfood_results)*100:.0f}%)")
        for r in grabfood_results:
            status = "OK" if r['result']['success'] else "FAIL"
            rating = r['result']['rating'] if r['result']['success'] else "N/A"
            print(f"  [{status}] {r['name']}: {rating}")
    
    if foodpanda_results:
        panda_success = sum(1 for r in foodpanda_results if r['result']['success'])
        print(f"\nFoodpanda: {panda_success}/{len(foodpanda_results)} successful ({panda_success/len(foodpanda_results)*100:.0f}%)")
        for r in foodpanda_results:
            status = "OK" if r['result']['success'] else "FAIL"
            rating = r['result']['rating'] if r['result']['success'] else "N/A"
            print(f"  [{status}] {r['name']}: {rating}")
    
    # Overall conclusion
    print("\n" + "=" * 70)
    print("OVERALL CONCLUSION")
    print("=" * 70)
    
    total_success = sum(1 for r in results if r['result']['success'])
    total = len(results)
    success_rate = (total_success / total * 100) if total > 0 else 0
    
    print(f"\nTotal: {total_success}/{total} successful ({success_rate:.0f}%)")
    
    if success_rate >= 90:
        print("EXCELLENT! Rating scraping is highly reliable.")
    elif success_rate >= 70:
        print("GOOD: Most ratings can be scraped successfully.")
    elif success_rate >= 50:
        print("PARTIAL: Some ratings work, investigate failures.")
    else:
        print("NEEDS WORK: Low success rate, check HTML structure.")
    
    # Show which stores failed for follow-up
    failed_stores = [r for r in results if not r['result']['success']]
    if failed_stores:
        print(f"\nFailed Stores ({len(failed_stores)}):")
        for r in failed_stores:
            print(f"  - {r['name']}")
            print(f"    URL: {r['url']}")
            print(f"    Error: {r['result'].get('error', 'Unknown')}")
    
    print("\n" + "=" * 70)


if __name__ == "__main__":
    main()