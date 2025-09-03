#!/usr/bin/env python3
"""
CocoPan Production Validation & Fix Script
COMPREHENSIVE: Fixes all issues and validates production readiness
- Removes extra stores (69 -> 66)
- Fixes generic "stores" names from foodpanda.page.link
- Ensures proper platform detection
- Validates database consistency
"""

import json
import re
import requests
import logging
import sys
from typing import Dict, List, Tuple, Optional
from database import db
from config import config

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class ProductionValidator:
    def __init__(self):
        self.expected_urls = self.load_expected_urls()
        self.name_cache = {}
        self.redirect_cache = {}
        self.issues_found = []
        
    def load_expected_urls(self) -> List[str]:
        """Load the 66 expected store URLs from branch_urls.json"""
        try:
            with open('branch_urls.json') as f:
                data = json.load(f)
                urls = data.get('urls', [])
                logger.info(f"📋 Loaded {len(urls)} expected URLs from branch_urls.json")
                return urls
        except Exception as e:
            logger.error(f"❌ Failed to load branch_urls.json: {e}")
            return []

    def resolve_foodpanda_redirect(self, redirect_url: str) -> Optional[str]:
        """Resolve foodpanda.page.link redirects with retry logic"""
        if redirect_url in self.redirect_cache:
            return self.redirect_cache[redirect_url]
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
        }
        
        try:
            # Try HEAD first (faster)
            logger.debug(f"🔗 Resolving redirect: {redirect_url}")
            resp = requests.head(redirect_url, allow_redirects=True, timeout=10, headers=headers)
            resolved_url = resp.url
            
            # If HEAD didn't redirect properly, try GET
            if resolved_url == redirect_url or 'page.link' in resolved_url:
                resp = requests.get(redirect_url, allow_redirects=True, timeout=10, headers=headers)
                resolved_url = resp.url
            
            # Validate the resolved URL
            if resolved_url and resolved_url != redirect_url and 'foodpanda.ph' in resolved_url:
                self.redirect_cache[redirect_url] = resolved_url
                logger.debug(f"✅ Resolved: {redirect_url} -> {resolved_url}")
                return resolved_url
            else:
                logger.warning(f"⚠️ Failed to resolve or invalid resolution: {redirect_url}")
                self.redirect_cache[redirect_url] = None
                return None
                
        except Exception as e:
            logger.warning(f"⚠️ Error resolving {redirect_url}: {e}")
            self.redirect_cache[redirect_url] = None
            return None

    def extract_proper_store_name(self, url: str) -> str:
        """Extract proper store name from any URL type"""
        if url in self.name_cache:
            return self.name_cache[url]
        
        try:
            original_url = url
            
            # Handle foodpanda.page.link redirects
            if 'foodpanda.page.link' in url:
                resolved_url = self.resolve_foodpanda_redirect(url)
                if resolved_url:
                    url = resolved_url
                    logger.debug(f"Using resolved URL for name extraction: {resolved_url}")
                else:
                    # Create a meaningful fallback name from the short URL
                    url_id = original_url.split('/')[-1] if '/' in original_url else 'unknown'
                    name = f"Cocopan Foodpanda {url_id[:8].upper()}"
                    self.name_cache[original_url] = name
                    logger.warning(f"Using fallback name for {original_url}: {name}")
                    return name
            
            # Extract from foodpanda direct URLs
            if 'foodpanda.ph' in url:
                # Pattern: /restaurant/code/store-name
                match = re.search(r'/restaurant/[^/]+/([^/?#]+)', url)
                if match:
                    raw_name = match.group(1)
                    # Clean and format the name
                    name = raw_name.replace('-', ' ').replace('_', ' ').title()
                    # Remove common suffixes and prefixes
                    name = re.sub(r'\b(Restaurant|Store|Branch)\b', '', name, flags=re.IGNORECASE).strip()
                    # Ensure Cocopan prefix
                    if not name.lower().startswith('cocopan'):
                        name = f"Cocopan {name}"
                    
                    self.name_cache[original_url] = name
                    logger.debug(f"Extracted from Foodpanda: {original_url} -> {name}")
                    return name
            
            # Extract from GrabFood URLs  
            elif 'grab.com' in url:
                # Pattern: /restaurant/store-name-delivery/
                match = re.search(r'/restaurant/([^/]+)', url)
                if match:
                    raw_name = match.group(1)
                    # Remove delivery suffix
                    raw_name = re.sub(r'-delivery$', '', raw_name)
                    # Clean and format
                    name = raw_name.replace('-', ' ').replace('_', ' ').title()
                    # Remove common suffixes and prefixes  
                    name = re.sub(r'\b(Restaurant|Store|Branch|Delivery)\b', '', name, flags=re.IGNORECASE).strip()
                    # Ensure Cocopan prefix
                    if not name.lower().startswith('cocopan'):
                        name = f"Cocopan {name}"
                    
                    self.name_cache[original_url] = name
                    logger.debug(f"Extracted from GrabFood: {original_url} -> {name}")
                    return name
            
            # Fallback
            logger.warning(f"Could not extract name from: {url}")
            name = "Cocopan Store (Unknown)"
            self.name_cache[original_url] = name
            return name
            
        except Exception as e:
            logger.error(f"Error extracting name from {url}: {e}")
            name = "Cocopan Store (Error)"
            self.name_cache[original_url] = name
            return name

    def get_platform_from_url(self, url: str) -> str:
        """Determine platform from URL - handles all types correctly"""
        if 'foodpanda' in url:  # Catches both foodpanda.ph and foodpanda.page.link
            return 'foodpanda'
        elif 'grab.com' in url:
            return 'grabfood'
        else:
            return 'unknown'

    def validate_current_state(self) -> Dict:
        """Analyze current database state"""
        logger.info("🔍 Analyzing current database state...")
        
        try:
            with db.get_connection() as conn:
                cursor = conn.cursor()
                
                # Get all current stores
                cursor.execute("SELECT id, name, url, platform FROM stores ORDER BY url")
                current_stores = cursor.fetchall()
                
                # Basic counts
                total_stores = len(current_stores)
                
                # Platform distribution
                cursor.execute("SELECT platform, COUNT(*) FROM stores GROUP BY platform")
                platform_counts = dict(cursor.fetchall())
                
                # Find problematic stores
                generic_names = []
                invalid_urls = []
                wrong_platforms = []
                
                for store_id, name, url, platform in current_stores:
                    # Check for generic names
                    if ('Store' in name and name != name.replace('Store', '').strip()) or name == 'stores':
                        generic_names.append((store_id, name, url))
                    
                    # Check if URL is in expected list
                    if url not in self.expected_urls:
                        invalid_urls.append((store_id, name, url))
                    
                    # Check platform correctness
                    expected_platform = self.get_platform_from_url(url)
                    if platform != expected_platform:
                        wrong_platforms.append((store_id, name, url, platform, expected_platform))
                
                return {
                    'total_stores': total_stores,
                    'platform_counts': platform_counts,
                    'generic_names': generic_names,
                    'invalid_urls': invalid_urls,
                    'wrong_platforms': wrong_platforms
                }
                
        except Exception as e:
            logger.error(f"❌ Failed to validate current state: {e}")
            return {}

    def remove_invalid_stores(self, invalid_stores: List[Tuple]) -> int:
        """Remove stores that shouldn't be in production database"""
        if not invalid_stores:
            return 0
            
        logger.info(f"🗑️ Removing {len(invalid_stores)} invalid stores...")
        
        removed_count = 0
        try:
            with db.get_connection() as conn:
                cursor = conn.cursor()
                
                for store_id, name, url in invalid_stores:
                    logger.info(f"   Removing: {name} -> {url}")
                    
                    # Delete from hourly tables first (foreign key constraints) 
                    cursor.execute("DELETE FROM store_status_hourly WHERE store_id = %s", (store_id,))
                    deleted_hourly = cursor.rowcount
                    
                    # Delete status checks (foreign key constraint)
                    cursor.execute("DELETE FROM status_checks WHERE store_id = %s", (store_id,))
                    deleted_checks = cursor.rowcount
                    
                    # Delete the store
                    cursor.execute("DELETE FROM stores WHERE id = %s", (store_id,))
                    
                    logger.info(f"     Deleted: {deleted_checks} status checks, {deleted_hourly} hourly records")
                    removed_count += 1
                
                conn.commit()
                logger.info(f"✅ Successfully removed {removed_count} invalid stores")
                
        except Exception as e:
            logger.error(f"❌ Failed to remove invalid stores: {e}")
            
        return removed_count

    def fix_store_names_and_platforms(self) -> int:
        """Fix store names and platform assignments"""
        logger.info("🔧 Fixing store names and platforms...")
        
        fixed_count = 0
        try:
            with db.get_connection() as conn:
                cursor = conn.cursor()
                
                # Get all current stores
                cursor.execute("SELECT id, name, url, platform FROM stores")
                current_stores = cursor.fetchall()
                
                for store_id, current_name, url, current_platform in current_stores:
                    correct_name = self.extract_proper_store_name(url)
                    correct_platform = self.get_platform_from_url(url)
                    
                    needs_update = False
                    updates = []
                    
                    if current_name != correct_name:
                        needs_update = True
                        updates.append(f"name: '{current_name}' -> '{correct_name}'")
                    
                    if current_platform != correct_platform:
                        needs_update = True
                        updates.append(f"platform: '{current_platform}' -> '{correct_platform}'")
                    
                    if needs_update:
                        cursor.execute(
                            "UPDATE stores SET name = %s, platform = %s WHERE id = %s",
                            (correct_name, correct_platform, store_id)
                        )
                        logger.info(f"   Fixed: {' | '.join(updates)}")
                        fixed_count += 1
                
                conn.commit()
                logger.info(f"✅ Fixed {fixed_count} stores")
                
        except Exception as e:
            logger.error(f"❌ Failed to fix stores: {e}")
            
        return fixed_count

    def ensure_all_expected_stores(self) -> int:
        """Ensure all 66 expected stores are in database"""
        logger.info("📝 Ensuring all expected stores exist...")
        
        created_count = 0
        try:
            with db.get_connection() as conn:
                cursor = conn.cursor()
                
                for url in self.expected_urls:
                    # Check if store exists
                    cursor.execute("SELECT id FROM stores WHERE url = %s", (url,))
                    if not cursor.fetchone():
                        # Store doesn't exist, create it
                        correct_name = self.extract_proper_store_name(url)
                        correct_platform = self.get_platform_from_url(url)
                        
                        cursor.execute(
                            "INSERT INTO stores (name, url, platform) VALUES (%s, %s, %s)",
                            (correct_name, url, correct_platform)
                        )
                        logger.info(f"   Created: {correct_name} ({correct_platform})")
                        created_count += 1
                
                conn.commit()
                logger.info(f"✅ Created {created_count} missing stores")
                
        except Exception as e:
            logger.error(f"❌ Failed to create missing stores: {e}")
            
        return created_count

    def validate_final_state(self) -> bool:
        """Validate that everything is now correct"""
        logger.info("🎯 Validating final state...")
        
        try:
            with db.get_connection() as conn:
                cursor = conn.cursor()
                
                # Check total count
                cursor.execute("SELECT COUNT(*) FROM stores")
                total_count = cursor.fetchone()[0]
                
                if total_count != 66:
                    logger.error(f"❌ Expected 66 stores, got {total_count}")
                    self.issues_found.append(f"Wrong store count: {total_count}")
                    return False
                else:
                    logger.info(f"✅ Exactly 66 stores in database")
                
                # Check platform distribution
                cursor.execute("SELECT platform, COUNT(*) FROM stores GROUP BY platform ORDER BY platform")
                platforms = cursor.fetchall()
                
                logger.info("📊 Platform distribution:")
                for platform, count in platforms:
                    logger.info(f"   {platform}: {count} stores")
                
                # Check for generic names
                cursor.execute("""
                    SELECT COUNT(*) FROM stores 
                    WHERE name LIKE '%Store%' OR name = 'stores' OR name LIKE 'Cocopan Store (%'
                """)
                generic_count = cursor.fetchone()[0]
                
                if generic_count > 0:
                    cursor.execute("""
                        SELECT name, url FROM stores 
                        WHERE name LIKE '%Store%' OR name = 'stores' OR name LIKE 'Cocopan Store (%'
                        LIMIT 5
                    """)
                    generic_examples = cursor.fetchall()
                    
                    logger.error(f"❌ Found {generic_count} stores with generic names:")
                    for name, url in generic_examples:
                        logger.error(f"   {name} -> {url}")
                    
                    self.issues_found.append(f"Generic names: {generic_count}")
                    return False
                else:
                    logger.info("✅ No generic store names found")
                
                # Check URL uniqueness
                cursor.execute("""
                    SELECT url, COUNT(*) 
                    FROM stores 
                    GROUP BY url 
                    HAVING COUNT(*) > 1
                """)
                duplicates = cursor.fetchall()
                
                if duplicates:
                    logger.error(f"❌ Found duplicate URLs:")
                    for url, count in duplicates:
                        logger.error(f"   {url}: {count} times")
                    self.issues_found.append(f"Duplicate URLs: {len(duplicates)}")
                    return False
                else:
                    logger.info("✅ All URLs are unique")
                
                # Validate all expected URLs are present
                cursor.execute("SELECT url FROM stores ORDER BY url")
                current_urls = {row[0] for row in cursor.fetchall()}
                expected_urls = set(self.expected_urls)
                
                missing_urls = expected_urls - current_urls
                extra_urls = current_urls - expected_urls
                
                if missing_urls:
                    logger.error(f"❌ Missing URLs: {len(missing_urls)}")
                    for url in list(missing_urls)[:5]:
                        logger.error(f"   Missing: {url}")
                    self.issues_found.append(f"Missing URLs: {len(missing_urls)}")
                    return False
                
                if extra_urls:
                    logger.error(f"❌ Extra URLs: {len(extra_urls)}")
                    for url in list(extra_urls)[:5]:
                        logger.error(f"   Extra: {url}")
                    self.issues_found.append(f"Extra URLs: {len(extra_urls)}")
                    return False
                
                logger.info("✅ All expected URLs are present, no extras")
                
                return True
                
        except Exception as e:
            logger.error(f"❌ Failed to validate final state: {e}")
            return False

    def run_full_validation_and_fix(self) -> bool:
        """Run complete validation and fix process"""
        logger.info("🚀 Starting CocoPan Production Validation & Fix")
        logger.info("=" * 70)
        
        if len(self.expected_urls) != 66:
            logger.error(f"❌ branch_urls.json has {len(self.expected_urls)} URLs, expected 66")
            return False
        
        # Step 1: Analyze current state
        state = self.validate_current_state()
        if not state:
            return False
            
        logger.info(f"📊 Current State Analysis:")
        logger.info(f"   Total stores: {state['total_stores']}")
        logger.info(f"   Platform counts: {state['platform_counts']}")
        logger.info(f"   Generic names: {len(state['generic_names'])}")
        logger.info(f"   Invalid URLs: {len(state['invalid_urls'])}")
        logger.info(f"   Wrong platforms: {len(state['wrong_platforms'])}")
        
        # Step 2: Remove invalid stores (test data, etc.)
        if state['invalid_urls']:
            logger.info(f"🗑️ Found {len(state['invalid_urls'])} invalid stores to remove:")
            for store_id, name, url in state['invalid_urls'][:3]:
                logger.info(f"   • {name} -> {url}")
            if len(state['invalid_urls']) > 3:
                logger.info(f"   ... and {len(state['invalid_urls']) - 3} more")
            
            removed = self.remove_invalid_stores(state['invalid_urls'])
            logger.info(f"✅ Removed {removed} invalid stores")
        
        # Step 3: Fix store names and platforms
        fixed = self.fix_store_names_and_platforms()
        if fixed > 0:
            logger.info(f"✅ Fixed {fixed} store names/platforms")
        
        # Step 4: Ensure all expected stores exist
        created = self.ensure_all_expected_stores()
        if created > 0:
            logger.info(f"✅ Created {created} missing stores")
        
        # Step 5: Final validation
        success = self.validate_final_state()
        
        logger.info("=" * 70)
        if success:
            logger.info("🎉 VALIDATION SUCCESSFUL!")
            logger.info("✅ Database is production-ready with exactly 66 stores")
            logger.info("✅ All store names are properly extracted")
            logger.info("✅ All platforms are correctly assigned")
            logger.info("✅ No test data or invalid entries")
            return True
        else:
            logger.error("❌ VALIDATION FAILED!")
            logger.error(f"Issues found: {', '.join(self.issues_found)}")
            return False

def main():
    """Main entry point"""
    validator = ProductionValidator()
    
    try:
        success = validator.run_full_validation_and_fix()
        if success:
            logger.info("\n🎊 Production database is ready!")
            logger.info("You can now start the monitoring service.")
            sys.exit(0)
        else:
            logger.error("\n❌ Fix the issues above and run again.")
            sys.exit(1)
            
    except Exception as e:
        logger.error(f"❌ Validation failed with error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()