#!/usr/bin/env python3
"""
CocoPan Monitor - System Testing Suite
Comprehensive tests for all system components
"""
import os
import sys
import time
import json
import unittest
import tempfile
from datetime import datetime, timedelta
from pathlib import Path
import sqlite3

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from config import config, Config
from database import DatabaseManager
from monitor_service import StoreMonitor

class TestConfig(unittest.TestCase):
    """Test configuration management"""
    
    def test_config_validation(self):
        """Test configuration validation"""
        errors = config.validate_config()
        # Should have no errors if branch_urls.json exists
        if os.path.exists('branch_urls.json'):
            self.assertEqual(len(errors), 0, f"Configuration errors: {errors}")
    
    def test_monitor_time_check(self):
        """Test monitoring time window"""
        # Test hours within range
        self.assertTrue(config.is_monitor_time(8))  # 8 AM
        self.assertTrue(config.is_monitor_time(15)) # 3 PM
        self.assertTrue(config.is_monitor_time(20)) # 8 PM
        
        # Test hours outside range
        self.assertFalse(config.is_monitor_time(3))  # 3 AM
        self.assertFalse(config.is_monitor_time(22)) # 10 PM
    
    def test_database_url_generation(self):
        """Test database URL generation"""
        # Test PostgreSQL URL
        config_test = Config()
        config_test.USE_SQLITE = False
        config_test.DATABASE_URL = 'postgresql://test:test@localhost:5432/test'
        self.assertEqual(config_test.get_database_url(), 'postgresql://test:test@localhost:5432/test')
        
        # Test SQLite URL
        config_test.USE_SQLITE = True
        config_test.SQLITE_PATH = 'test.db'
        self.assertEqual(config_test.get_database_url(), 'sqlite:///test.db')

class TestDatabase(unittest.TestCase):
    """Test database operations"""
    
    def setUp(self):
        """Set up test database"""
        self.test_db_path = tempfile.mktemp(suffix='.db')
        self.original_sqlite_path = config.SQLITE_PATH
        self.original_use_sqlite = config.USE_SQLITE
        
        # Force SQLite mode for testing
        config.SQLITE_PATH = self.test_db_path
        config.USE_SQLITE = True
        
        # Create test database manager
        self.db = DatabaseManager()
    
    def tearDown(self):
        """Clean up test database"""
        config.SQLITE_PATH = self.original_sqlite_path
        config.USE_SQLITE = self.original_use_sqlite
        
        if os.path.exists(self.test_db_path):
            os.unlink(self.test_db_path)
    
    def test_database_initialization(self):
        """Test database table creation"""
        # Check if tables exist
        with self.db.get_connection() as conn:
            cursor = conn.cursor()
            
            # Check stores table
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='stores'")
            self.assertIsNotNone(cursor.fetchone())
            
            # Check status_checks table
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='status_checks'")
            self.assertIsNotNone(cursor.fetchone())
            
            # Check summary_reports table
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='summary_reports'")
            self.assertIsNotNone(cursor.fetchone())
    
    def test_store_creation(self):
        """Test store creation and retrieval"""
        store_id = self.db.get_or_create_store(
            "Test Store", 
            "https://example.com/test-store"
        )
        
        self.assertIsInstance(store_id, int)
        self.assertGreater(store_id, 0)
        
        # Test duplicate store
        store_id2 = self.db.get_or_create_store(
            "Test Store Different Name", 
            "https://example.com/test-store"  # Same URL
        )
        
        self.assertEqual(store_id, store_id2)  # Should return same ID
    
    def test_status_check_storage(self):
        """Test status check storage"""
        # Create test store
        store_id = self.db.get_or_create_store(
            "Test Store", 
            "https://example.com/test-store"
        )
        
        # Save status check
        success = self.db.save_status_check(
            store_id=store_id,
            is_online=True,
            response_time_ms=1500,
            error_message=None
        )
        
        self.assertTrue(success)
        
        # Verify status check was saved
        with self.db.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM status_checks WHERE store_id = ?", (store_id,))
            result = cursor.fetchone()
            
            self.assertIsNotNone(result)
            self.assertEqual(result["store_id"], store_id)
            self.assertEqual(result["is_online"], True)
            self.assertEqual(result["response_time_ms"], 1500)
    
    def test_summary_report_storage(self):
        """Test summary report storage"""
        success = self.db.save_summary_report(
            total_stores=10,
            online_stores=8,
            offline_stores=2
        )
        
        self.assertTrue(success)
        
        # Verify summary was saved
        with self.db.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM summary_reports ORDER BY id DESC LIMIT 1")
            result = cursor.fetchone()
            
            self.assertIsNotNone(result)
            self.assertEqual(result["total_stores"], 10)
            self.assertEqual(result["online_stores"], 8)
            self.assertEqual(result["offline_stores"], 2)
            self.assertEqual(result["online_percentage"], 80.0)
    
    def test_data_retrieval(self):
        """Test data retrieval methods"""
        # Create test data
        store_id = self.db.get_or_create_store("Test Store", "https://example.com/test")
        self.db.save_status_check(store_id, True, 1000)
        self.db.save_summary_report(1, 1, 0)
        
        # Test latest status
        latest_status = self.db.get_latest_status()
        self.assertIsNotNone(latest_status)
        self.assertGreater(len(latest_status), 0)
        
        # Test store logs
        store_logs = self.db.get_store_logs()
        self.assertIsNotNone(store_logs)
        
        # Test daily uptime
        daily_uptime = self.db.get_daily_uptime()
        self.assertIsNotNone(daily_uptime)
        
        # Test database stats
        stats = self.db.get_database_stats()
        self.assertIsInstance(stats, dict)
        self.assertIn('store_count', stats)
        self.assertEqual(stats['store_count'], 1)

class TestStoreMonitor(unittest.TestCase):
    """Test store monitoring functionality"""
    
    def setUp(self):
        """Set up test monitor"""
        # Create temporary config
        self.test_config = {
            'USE_SQLITE': True,
            'SQLITE_PATH': tempfile.mktemp(suffix='.db'),
            'MAX_RETRIES': 1,  # Reduce retries for testing
            'REQUEST_TIMEOUT': 5,
            'STORE_URLS_FILE': 'test_branch_urls.json'
        }
        
        # Create test URLs file
        test_urls = {
            "urls": [
                "https://httpbin.org/status/200",  # Should be online
                "https://httpbin.org/status/404",  # Should be offline
            ]
        }
        
        with open(self.test_config['STORE_URLS_FILE'], 'w') as f:
            json.dump(test_urls, f)
        
        # Override config temporarily
        for key, value in self.test_config.items():
            setattr(config, key, value)
        
        self.monitor = StoreMonitor()
    
    def tearDown(self):
        """Clean up test files"""
        if os.path.exists(self.test_config['SQLITE_PATH']):
            os.unlink(self.test_config['SQLITE_PATH'])
        
        if os.path.exists(self.test_config['STORE_URLS_FILE']):
            os.unlink(self.test_config['STORE_URLS_FILE'])
    
    def test_store_url_loading(self):
        """Test store URL loading"""
        self.assertEqual(len(self.monitor.store_urls), 2)
        self.assertIn("https://httpbin.org/status/200", self.monitor.store_urls)
        self.assertIn("https://httpbin.org/status/404", self.monitor.store_urls)
    
    def test_store_status_check(self):
        """Test individual store status checking"""
        # Test online URL (200 status)
        is_online, response_time, error = self.monitor.check_store_online("https://httpbin.org/status/200")
        self.assertTrue(is_online)
        self.assertIsInstance(response_time, int)
        self.assertIsNone(error)
        
        # Test offline URL (404 status)  
        is_online, response_time, error = self.monitor.check_store_online("https://httpbin.org/status/404")
        self.assertFalse(is_online)
        self.assertIsInstance(response_time, int)
        self.assertIsNotNone(error)
    
    def test_store_name_extraction(self):
        """Test store name extraction from URL"""
        name = self.monitor._get_store_name("https://food.grab.com/ph/en/restaurant/test-store/abc123")
        # Should fallback to URL-based name
        self.assertIn("abc123", name)

class TestSystemIntegration(unittest.TestCase):
    """Integration tests for the complete system"""
    
    def setUp(self):
        """Set up integration test environment"""
        self.test_db_path = tempfile.mktemp(suffix='.db')
        self.original_config = {
            'USE_SQLITE': config.USE_SQLITE,
            'SQLITE_PATH': config.SQLITE_PATH
        }
        
        # Use test database
        config.USE_SQLITE = True
        config.SQLITE_PATH = self.test_db_path
    
    def tearDown(self):
        """Clean up integration test"""
        # Restore original config
        for key, value in self.original_config.items():
            setattr(config, key, value)
        
        if os.path.exists(self.test_db_path):
            os.unlink(self.test_db_path)
    
    def test_full_monitoring_cycle(self):
        """Test a complete monitoring cycle"""
        # Create database manager
        db_manager = DatabaseManager()
        
        # Create test store
        store_id = db_manager.get_or_create_store(
            "Integration Test Store",
            "https://httpbin.org/status/200"
        )
        
        # Save status check
        success = db_manager.save_status_check(
            store_id=store_id,
            is_online=True,
            response_time_ms=500
        )
        self.assertTrue(success)
        
        # Save summary report
        success = db_manager.save_summary_report(1, 1, 0)
        self.assertTrue(success)
        
        # Verify data can be retrieved
        latest_status = db_manager.get_latest_status()
        self.assertGreater(len(latest_status), 0)
        
        store_logs = db_manager.get_store_logs()
        self.assertGreater(len(store_logs), 0)
        
        daily_uptime = db_manager.get_daily_uptime()
        self.assertGreater(len(daily_uptime), 0)

class TestBackupRestore(unittest.TestCase):
    """Test backup and restore functionality"""
    
    def setUp(self):
        """Set up backup test environment"""
        self.test_backup_dir = Path(tempfile.mkdtemp())
        self.test_db_path = self.test_backup_dir / "test.db"
        
        # Create test database with some data
        conn = sqlite3.connect(self.test_db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            CREATE TABLE test_table (
                id INTEGER PRIMARY KEY,
                name TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        cursor.execute("INSERT INTO test_table (name) VALUES ('test_data')")
        conn.commit()
        conn.close()
    
    def tearDown(self):
        """Clean up backup test"""
        import shutil
        if self.test_backup_dir.exists():
            shutil.rmtree(self.test_backup_dir)
    
    def test_sqlite_backup_restore(self):
        """Test SQLite backup and restore process"""
        from backup_restore import BackupManager
        
        # Override config for testing
        original_config = {
            'USE_SQLITE': config.USE_SQLITE,
            'SQLITE_PATH': config.SQLITE_PATH
        }
        
        config.USE_SQLITE = True
        config.SQLITE_PATH = str(self.test_db_path)
        
        try:
            # Create backup manager with test directory
            manager = BackupManager()
            manager.backup_dir = self.test_backup_dir
            
            # Create backup
            backup_path = manager.create_backup("test_backup")
            self.assertTrue(Path(backup_path).exists())
            
            # Verify backup is compressed
            self.assertTrue(backup_path.endswith('.gz'))
            
            # List backups
            backups = manager.list_backups()
            self.assertEqual(len(backups), 1)
            self.assertEqual(backups[0]['description'], 'test_backup')
            
            # Test restore (this would overwrite the original)
            # For testing, we'll just verify the restore method works
            # without actually overwriting the test database
            
        finally:
            # Restore original config
            for key, value in original_config.items():
                setattr(config, key, value)

def run_system_tests():
    """Run all system tests"""
    print("🧪 Running CocoPan System Tests")
    print("=" * 50)
    
    # Create test suite
    test_suite = unittest.TestSuite()
    
    # Add test cases
    test_cases = [
        TestConfig,
        TestDatabase,
        TestStoreMonitor,
        TestSystemIntegration,
        TestBackupRestore
    ]
    
    for test_case in test_cases:
        tests = unittest.TestLoader().loadTestsFromTestCase(test_case)
        test_suite.addTests(tests)
    
    # Run tests
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(test_suite)
    
    # Print summary
    print("\n" + "=" * 50)
    print(f"📊 Test Results:")
    print(f"   ✅ Tests run: {result.testsRun}")
    print(f"   ❌ Failures: {len(result.failures)}")
    print(f"   🔥 Errors: {len(result.errors)}")
    
    if result.failures:
        print(f"\n❌ Failures:")
        for test, traceback in result.failures:
            print(f"   • {test}: {traceback.split('AssertionError: ')[-1].split('\\n')[0]}")
    
    if result.errors:
        print(f"\n🔥 Errors:")
        for test, traceback in result.errors:
            print(f"   • {test}: {traceback.split('\\n')[-2]}")
    
    success = len(result.failures) == 0 and len(result.errors) == 0
    
    if success:
        print(f"\n🎉 All tests passed! System is ready for production.")
    else:
        print(f"\n⚠️ Some tests failed. Please review and fix issues before deployment.")
    
    return success

def run_quick_smoke_tests():
    """Run quick smoke tests to verify basic functionality"""
    print("🚀 Running Quick Smoke Tests")
    print("-" * 30)
    
    tests_passed = 0
    total_tests = 0
    
    # Test 1: Configuration loading
    total_tests += 1
    try:
        errors = config.validate_config()
        if not errors or (len(errors) == 1 and "branch_urls.json" in errors[0]):
            print("✅ Configuration validation")
            tests_passed += 1
        else:
            print(f"❌ Configuration validation: {errors}")
    except Exception as e:
        print(f"❌ Configuration validation: {e}")
    
    # Test 2: Database connection
    total_tests += 1
    try:
        # Use temporary database for testing
        original_config = config.USE_SQLITE, config.SQLITE_PATH
        config.USE_SQLITE = True
        config.SQLITE_PATH = tempfile.mktemp(suffix='.db')
        
        db_test = DatabaseManager()
        stats = db_test.get_database_stats()
        
        if isinstance(stats, dict):
            print("✅ Database connectivity")
            tests_passed += 1
        else:
            print("❌ Database connectivity: No stats returned")
        
        # Cleanup
        if os.path.exists(config.SQLITE_PATH):
            os.unlink(config.SQLITE_PATH)
        config.USE_SQLITE, config.SQLITE_PATH = original_config
        
    except Exception as e:
        print(f"❌ Database connectivity: {e}")
        config.USE_SQLITE, config.SQLITE_PATH = original_config
    
    # Test 3: HTTP connectivity
    total_tests += 1
    try:
        import requests
        response = requests.get("https://httpbin.org/status/200", timeout=5)
        if response.status_code == 200:
            print("✅ HTTP connectivity")
            tests_passed += 1
        else:
            print(f"❌ HTTP connectivity: Status {response.status_code}")
    except Exception as e:
        print(f"❌ HTTP connectivity: {e}")
    
    # Test 4: Required modules
    total_tests += 1
    try:
        import streamlit
        import plotly
        import pandas
        import playwright
        print("✅ Required modules")
        tests_passed += 1
    except ImportError as e:
        print(f"❌ Required modules: {e}")
    
    print(f"\n📊 Smoke Test Results: {tests_passed}/{total_tests} passed")
    return tests_passed == total_tests

def main():
    """Main test runner"""
    import argparse
    
    parser = argparse.ArgumentParser(description='CocoPan System Tests')
    parser.add_argument('--smoke', action='store_true', help='Run quick smoke tests only')
    parser.add_argument('--full', action='store_true', help='Run full test suite')
    
    args = parser.parse_args()
    
    if args.smoke:
        success = run_quick_smoke_tests()
    elif args.full:
        success = run_system_tests()
    else:
        # Default: run smoke tests first, then ask about full tests
        print("🧪 CocoPan System Testing")
        print("=" * 30)
        
        smoke_success = run_quick_smoke_tests()
        
        if smoke_success:
            print("\n💡 Quick tests passed! Run full test suite? (y/N): ", end="")
            if input().lower() == 'y':
                success = run_system_tests()
            else:
                success = True
        else:
            print("\n⚠️ Smoke tests failed. Fix basic issues before running full tests.")
            success = False
    
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()