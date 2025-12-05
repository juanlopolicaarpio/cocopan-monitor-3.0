#!/usr/bin/env python3
"""
HTML Structure Analyzer - Find actual menu item structure
"""
import time
import logging
import re
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup

logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)

URL = "https://food.grab.com/ph/en/restaurant/cocopan-altura-santa-mesa-delivery/2-C7EUVP2UEJ43L6"

def setup_driver():
    chrome_options = Options()
    chrome_options.add_argument('--headless=new')
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')
    chrome_options.add_argument('--window-size=1920,1080')
    return webdriver.Chrome(options=chrome_options)

def main():
    driver = setup_driver()
    
    try:
        logger.info(f"Loading: {URL}\n")
        driver.get(URL)
        
        logger.info("Waiting 12 seconds...")
        time.sleep(12)
        
        logger.info("Scrolling...")
        for i in range(5):
            driver.execute_script(f"window.scrollTo(0, {1500 * (i+1)});")
            time.sleep(2)
        
        html = driver.page_source
        soup = BeautifulSoup(html, 'html.parser')
        
        # Save
        with open('STRUCTURE_ANALYSIS.html', 'w', encoding='utf-8') as f:
            f.write(html)
        logger.info("Saved: STRUCTURE_ANALYSIS.html\n")
        
        # Strategy: Find elements containing product names we KNOW exist
        known_items = [
            'Chicken Asado Bun',
            'Milky Cheese Donut',
            'Pan de coco',
            'Coffee Bun',
            'Tuna Bun'
        ]
        
        logger.info("="*80)
        logger.info("FINDING MENU ITEMS BY SEARCHING FOR KNOWN PRODUCT NAMES")
        logger.info("="*80)
        
        item_wrappers = []
        
        for item_name in known_items:
            # Find all divs that contain this text
            divs = soup.find_all('div')
            
            for div in divs:
                text = div.get_text()
                if item_name in text:
                    # This div contains the item name
                    # Check if it's a reasonable size (not the whole page)
                    if 100 < len(text) < 1000:
                        item_wrappers.append({
                            'name': item_name,
                            'wrapper': div,
                            'text_length': len(text)
                        })
                        break
        
        logger.info(f"Found {len(item_wrappers)} item wrappers\n")
        
        # Analyze each wrapper
        for item_info in item_wrappers:
            logger.info("="*80)
            logger.info(f"ITEM: {item_info['name']}")
            logger.info("="*80)
            
            wrapper = item_info['wrapper']
            
            # Show classes
            classes = wrapper.get('class', [])
            logger.info(f"Wrapper classes: {classes}")
            
            # Show structure
            logger.info(f"\nWrapper tag: {wrapper.name}")
            logger.info(f"Text length: {item_info['text_length']} chars")
            
            # Show first 300 chars of HTML
            html_snippet = str(wrapper)[:300]
            logger.info(f"\nHTML snippet:\n{html_snippet}...\n")
            
            # Count buttons
            buttons = wrapper.find_all(['button', 'svg'])
            logger.info(f"Found {len(buttons)} button/svg elements")
            
            for i, btn in enumerate(buttons[:3], 1):
                btn_classes = ' '.join(btn.get('class', [])).lower()
                logger.info(f"  Button {i}: {btn.name}, classes: {btn_classes[:80]}")
            
            logger.info("")
        
        # Now let's try to find the common pattern
        logger.info("="*80)
        logger.info("ANALYZING COMMON PATTERNS")
        logger.info("="*80)
        
        if item_wrappers:
            # Get all unique class prefixes
            all_classes = []
            for item_info in item_wrappers:
                classes = item_info['wrapper'].get('class', [])
                all_classes.extend(classes)
            
            logger.info(f"\nAll wrapper classes found:")
            for cls in set(all_classes):
                count = all_classes.count(cls)
                logger.info(f"  '{cls}' - appears {count} times")
        
        # Try to find price elements (they should be near items)
        logger.info("\n" + "="*80)
        logger.info("FINDING PRICE PATTERNS")
        logger.info("="*80)
        
        # Find all elements with price-like text
        all_divs = soup.find_all('div')
        price_pattern = re.compile(r'^\s*\d+\.\d{2}\s*$')
        
        price_elements = []
        for div in all_divs:
            text = div.get_text().strip()
            if price_pattern.match(text):
                price_elements.append(div)
        
        logger.info(f"Found {len(price_elements)} price elements")
        
        if price_elements:
            logger.info("\nSample price element:")
            price_elem = price_elements[0]
            logger.info(f"  Text: {price_elem.get_text().strip()}")
            logger.info(f"  Classes: {price_elem.get('class', [])}")
            
            # Check its parent
            parent = price_elem.parent
            if parent:
                logger.info(f"  Parent tag: {parent.name}")
                logger.info(f"  Parent classes: {parent.get('class', [])}")
                
                # Grand parent
                grandparent = parent.parent
                if grandparent:
                    logger.info(f"  Grandparent tag: {grandparent.name}")
                    logger.info(f"  Grandparent classes: {grandparent.get('class', [])}")
        
        # Try to find the image elements
        logger.info("\n" + "="*80)
        logger.info("FINDING IMAGE PATTERNS")
        logger.info("="*80)
        
        images = soup.find_all('img', alt=re.compile(r'Cocopan.*:'))
        logger.info(f"Found {len(images)} product images")
        
        if images:
            img = images[0]
            logger.info(f"\nSample image:")
            logger.info(f"  Alt: {img.get('alt', '')[:60]}")
            logger.info(f"  Classes: {img.get('class', [])}")
            
            # Find item wrapper by going up the tree
            current = img
            for level in range(10):
                current = current.parent
                if not current:
                    break
                    
                text = current.get_text()
                if '.' in text and len(text) > 100 and len(text) < 1000:
                    logger.info(f"\n  Level {level} up - This looks like an item wrapper:")
                    logger.info(f"    Tag: {current.name}")
                    logger.info(f"    Classes: {current.get('class', [])}")
                    logger.info(f"    Text length: {len(text)} chars")
                    break
        
        logger.info("\n" + "="*80)
        logger.info("RECOMMENDATIONS")
        logger.info("="*80)
        logger.info("\nTo fix the scraper:")
        logger.info("1. Check the classes found above")
        logger.info("2. Use those class patterns to find wrappers")
        logger.info("3. Or use image alt tags to locate items")
        logger.info("4. Check STRUCTURE_ANALYSIS.html for full details")
        
    finally:
        driver.quit()
        logger.info("\n✅ Done!")

if __name__ == "__main__":
    main()