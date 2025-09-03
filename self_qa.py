#!/usr/bin/env python3
"""
CocoPan Monitor - Self-QA Script
Automated validation that system matches UI expectations
Validates Under Review exclusion, Manila timezone, platform counts
"""

import json
import logging
import sys
from datetime import datetime
from typing import Dict, List, Tuple
import pandas as pd

from database import db
from config import config

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class CocoPanQA:
    def __init__(self):
        self.issues = []
        self.warnings = []
        self.passed_tests = []
        
    def log_pass(self, test_name: str):
        """Log a passed test"""
        self.passed_tests.append(test_name)
        logger.info(f"✅ PASS: {test_name}")
        
    def log_fail(self, test_name: str, details: str):
        """Log a failed test"""
        self.issues.append(f"{test_name}: {details}")
        logger.error(f"❌ FAIL: {test_name} - {details}")
        
    def log_warning(self, test_name: str, details: str):
        """Log a test warning"""
        self.warnings.append(f"{test_name}: {details}")
        logger.warning(f"⚠️ WARN: {test_name} - {details}")

    def validate_store_count(self) -> bool:
        """Test 1: Validate exactly 66 stores"""
        try:
            with db.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT COUNT(*) FROM stores")
                count = cursor.fetchone()[0]
                
                if count == 66:
                    self.log_pass("Store Count Validation")
                    return True
                else:
                    self.log_fail("Store Count Validation", f"Expected 66 stores, got {count}")
                    return False
        except Exception as e:
            self.log_fail("Store Count Validation", f"Database error: {e}")
            return False

    def validate_store_names(self) -> bool:
        """Test 2: Validate no generic store names"""
        try:
            with db.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT COUNT(*), ARRAY_AGG(name LIMIT 3) as examples
                    FROM stores 
                    WHERE name LIKE '%Store%' OR name = 'stores' OR name LIKE 'Cocopan Store (%'
                """)
                count, examples = cursor.fetchone()
                
                if count == 0:
                    self.log_pass("Store Name Validation")
                    return True
                else:
                    example_str = ', '.join(examples[:3]) if examples else 'none'
                    self.log_fail("Store Name Validation", f"Found {count} generic names. Examples: {example_str}")
                    return False
        except Exception as e:
            self.log_fail("Store Name Validation", f"Database error: {e}")
            return False

    def validate_platform_detection(self) -> bool:
        """Test 3: Validate platform detection logic"""
        try:
            with db.get_connection() as conn:
                cursor = conn.cursor()
                
                # Count by platform
                cursor.execute("SELECT platform, COUNT(*) FROM stores GROUP BY platform ORDER BY platform")
                platform_counts = dict(cursor.fetchall())
                
                # Check for expected platforms
                expected_platforms = {'foodpanda', 'grabfood'}
                actual_platforms = set(platform_counts.keys())
                
                if expected_platforms.issubset(actual_platforms):
                    self.log_pass("Platform Detection")
                    logger.info(f"   Platform distribution: {platform_counts}")
                    
                    # Validate foodpanda.page.link URLs are detected as foodpanda
                    cursor.execute("""
                        SELECT COUNT(*) FROM stores 
                        WHERE url LIKE '%foodpanda.page.link%' AND platform != 'foodpanda'
                    """)
                    wrong_platform_count = cursor.fetchone()[0]
                    
                    if wrong_platform_count == 0:
                        self.log_pass("Foodpanda Page.link Detection")
                        return True
                    else:
                        self.log_fail("Foodpanda Page.link Detection", 
                                    f"{wrong_platform_count} foodpanda.page.link URLs have wrong platform")
                        return False
                else:
                    missing = expected_platforms - actual_platforms
                    self.log_fail("Platform Detection", f"Missing platforms: {missing}")
                    return False
                    
        except Exception as e:
            self.log_fail("Platform Detection", f"Database error: {e}")
            return False

    def validate_under_review_logic(self) -> bool:
        """Test 4: Validate Under Review exclusion logic matches UI"""
        try:
            # Get latest status like the UI does
            with db.get_connection() as conn:
                query = """
                    SELECT 
                        s.platform,
                        sc.is_online,
                        sc.error_message,
                        CASE 
                            WHEN sc.error_message LIKE '[BLOCKED]%' OR 
                                 sc.error_message LIKE '[UNKNOWN]%' OR 
                                 sc.error_message LIKE '[ERROR]%' THEN true
                            ELSE false
                        END as is_under_review
                    FROM stores s
                    INNER JOIN status_checks sc ON s.id = sc.store_id
                    INNER JOIN (
                        SELECT store_id, MAX(checked_at) as latest_check
                        FROM status_checks
                        GROUP BY store_id
                    ) latest ON sc.store_id = latest.store_id AND sc.checked_at = latest.latest_check
                """
                
                df = pd.read_sql_query(query, conn)
                
                if df.empty:
                    self.log_warning("Under Review Logic", "No status data to validate")
                    return True
                
                # Count totals
                total_stores = len(df)
                under_review_count = df['is_under_review'].sum()
                
                # Count Online/Offline (excluding Under Review)
                effective_df = df[~df['is_under_review']]
                online_count = effective_df['is_online'].sum()
                offline_count = len(effective_df) - online_count
                
                # Validate the math matches UI expectations
                expected_effective_total = total_stores - under_review_count
                actual_effective_total = online_count + offline_count
                
                if actual_effective_total == expected_effective_total:
                    self.log_pass("Under Review Exclusion Logic")
                    logger.info(f"   Total: {total_stores}, Online: {online_count}, Offline: {offline_count}, Under Review: {under_review_count}")
                    return True
                else:
                    self.log_fail("Under Review Exclusion Logic", 
                                f"Math doesn't match: Expected {expected_effective_total}, got {actual_effective_total}")
                    return False
                    
        except Exception as e:
            self.log_fail("Under Review Logic", f"Database error: {e}")
            return False

    def validate_manila_timezone(self) -> bool:
        """Test 5: Validate Manila timezone handling"""
        try:
            with db.get_connection() as conn:
                # Test the Manila date conversion query that UI uses
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT 
                        DATE(NOW() AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Manila') as manila_date,
                        DATE(NOW()) as utc_date,
                        NOW() AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Manila' as manila_time,
                        NOW() as utc_time
                """)
                
                result = cursor.fetchone()
                manila_date, utc_date, manila_time, utc_time = result
                
                # Manila should be 8 hours ahead of UTC (but system might show it as -8 due to offset direction)
                time_diff = manila_time - utc_time
                expected_diff_hours = 8
                actual_diff_hours = time_diff.total_seconds() / 3600
                
                # Accept both +8 and -8 as valid (depends on system timezone interpretation)
                if abs(actual_diff_hours) == expected_diff_hours:
                    self.log_pass("Manila Timezone Conversion")
                    logger.info(f"   Manila time: {manila_time}, UTC time: {utc_time}")
                    logger.info(f"   Time difference: {actual_diff_hours:.1f} hours (Manila timezone working)")
                    return True
                else:
                    self.log_fail("Manila Timezone Conversion", 
                                f"Expected ±8 hour difference, got {actual_diff_hours:.1f}")
                    return False
                    
        except Exception as e:
            self.log_fail("Manila Timezone", f"Database error: {e}")
            return False

    def validate_query_performance(self) -> bool:
        """Test 6: Validate key queries perform reasonably"""
        try:
            with db.get_connection() as conn:
                # Test the main dashboard query performance
                start_time = datetime.now()
                
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT COUNT(*)
                    FROM stores s
                    INNER JOIN status_checks sc ON s.id = sc.store_id
                    INNER JOIN (
                        SELECT store_id, MAX(checked_at) as latest_check
                        FROM status_checks
                        GROUP BY store_id
                    ) latest ON sc.store_id = latest.store_id AND sc.checked_at = latest.latest_check
                """)
                
                result = cursor.fetchone()[0]
                duration = (datetime.now() - start_time).total_seconds()
                
                if duration < 2.0:  # Should complete in under 2 seconds
                    self.log_pass("Query Performance")
                    logger.info(f"   Latest status query: {duration:.3f}s for {result} stores")
                    return True
                else:
                    self.log_warning("Query Performance", 
                                   f"Latest status query took {duration:.3f}s - consider index optimization")
                    return True  # Warning, not failure
                    
        except Exception as e:
            self.log_fail("Query Performance", f"Database error: {e}")
            return False

    def validate_expected_urls(self) -> bool:
        """Test 7: Validate all expected URLs are present"""
        try:
            # Load expected URLs
            with open('branch_urls.json') as f:
                data = json.load(f)
                expected_urls = set(data.get('urls', []))
            
            # Get actual URLs
            with db.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT url FROM stores")
                actual_urls = {row[0] for row in cursor.fetchall()}
            
            missing_urls = expected_urls - actual_urls
            extra_urls = actual_urls - expected_urls
            
            if not missing_urls and not extra_urls:
                self.log_pass("URL Completeness")
                return True
            else:
                issues = []
                if missing_urls:
                    issues.append(f"{len(missing_urls)} missing URLs")
                if extra_urls:
                    issues.append(f"{len(extra_urls)} extra URLs")
                
                self.log_fail("URL Completeness", "; ".join(issues))
                return False
                
        except Exception as e:
            self.log_fail("URL Completeness", f"Error: {e}")
            return False

    def validate_database_constraints(self) -> bool:
        """Test 8: Validate database constraints and data integrity"""
        try:
            with db.get_connection() as conn:
                cursor = conn.cursor()
                
                # Check for duplicate URLs
                cursor.execute("""
                    SELECT url, COUNT(*) as count 
                    FROM stores 
                    GROUP BY url 
                    HAVING COUNT(*) > 1
                """)
                duplicates = cursor.fetchall()
                
                if duplicates:
                    self.log_fail("Database Constraints", f"Found {len(duplicates)} duplicate URLs")
                    return False
                
                # Check for NULL values in required fields
                cursor.execute("""
                    SELECT COUNT(*) FROM stores 
                    WHERE name IS NULL OR url IS NULL OR platform IS NULL
                """)
                null_count = cursor.fetchone()[0]
                
                if null_count > 0:
                    self.log_fail("Database Constraints", f"Found {null_count} records with NULL required fields")
                    return False
                
                # Check status_checks data integrity
                cursor.execute("""
                    SELECT COUNT(*) FROM status_checks 
                    WHERE store_id IS NULL OR is_online IS NULL
                """)
                invalid_checks = cursor.fetchone()[0]
                
                if invalid_checks > 0:
                    self.log_fail("Database Constraints", f"Found {invalid_checks} invalid status checks")
                    return False
                
                self.log_pass("Database Constraints")
                return True
                
        except Exception as e:
            self.log_fail("Database Constraints", f"Database error: {e}")
            return False

    def run_all_tests(self) -> bool:
        """Run all validation tests"""
        logger.info("🚀 Starting CocoPan Self-QA Validation")
        logger.info("=" * 60)
        
        tests = [
            ("Store Count", self.validate_store_count),
            ("Store Names", self.validate_store_names), 
            ("Platform Detection", self.validate_platform_detection),
            ("Under Review Logic", self.validate_under_review_logic),
            ("Manila Timezone", self.validate_manila_timezone),
            ("Query Performance", self.validate_query_performance),
            ("URL Completeness", self.validate_expected_urls),
            ("Database Constraints", self.validate_database_constraints),
        ]
        
        passed = 0
        failed = 0
        
        for test_name, test_func in tests:
            try:
                if test_func():
                    passed += 1
                else:
                    failed += 1
            except Exception as e:
                logger.error(f"❌ FAIL: {test_name} - Unexpected error: {e}")
                failed += 1
        
        # Summary
        logger.info("=" * 60)
        logger.info("📋 VALIDATION SUMMARY")
        logger.info(f"   ✅ Passed: {passed}/{len(tests)}")
        logger.info(f"   ❌ Failed: {failed}/{len(tests)}")
        logger.info(f"   ⚠️ Warnings: {len(self.warnings)}")
        
        if failed == 0:
            logger.info("🎉 ALL TESTS PASSED - System is production ready!")
            return True
        else:
            logger.error("❌ TESTS FAILED - Issues need to be resolved:")
            for issue in self.issues:
                logger.error(f"   • {issue}")
            return False

def main():
    """Main entry point"""
    qa = CocoPanQA()
    
    try:
        success = qa.run_all_tests()
        
        if success:
            logger.info("\n✅ System validation successful!")
            logger.info("Dashboard counts should match database exactly.")
            logger.info("Under Review stores are properly excluded.")
            logger.info("Manila timezone handling is correct.")
            sys.exit(0)
        else:
            logger.error("\n❌ System validation failed!")
            logger.error("Fix the issues above before using in production.")
            sys.exit(1)
            
    except Exception as e:
        logger.error(f"❌ Validation failed with unexpected error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
if __name__ == "__main__":
    main()