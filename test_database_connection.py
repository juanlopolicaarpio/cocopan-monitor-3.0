#!/usr/bin/env python3
"""
Database Connection Test Script
Test and diagnose database connectivity issues
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from database import db
from config import config
import json

def test_database_connection():
    """Test database connection and operations"""
    print("🔍 Testing database connection...")
    print("=" * 50)
    
    # Test 1: Basic connection
    print("1️⃣ Testing basic connection...")
    conn_test = db.test_connection()
    print(f"   Database type: {conn_test['db_type']}")
    print(f"   Status: {conn_test['status']}")
    if conn_test['connection_time']:
        print(f"   Connection time: {conn_test['connection_time']:.3f}s")
    if conn_test['error']:
        print(f"   Error: {conn_test['error']}")
    
    if conn_test['status'] != 'success':
        print("❌ Basic connection failed!")
        return False
    
    print("✅ Basic connection successful")
    
    # Test 2: Database stats
    print("\n2️⃣ Testing database stats...")
    try:
        stats = db.get_database_stats()
        print(f"   Store count: {stats['store_count']}")
        print(f"   Total checks: {stats['total_checks']}")
        print(f"   Platforms: {stats['platforms']}")
        print("✅ Database stats retrieved")
    except Exception as e:
        print(f"❌ Database stats failed: {e}")
        return False
    
    # Test 3: Store creation
    print("\n3️⃣ Testing store creation...")
    try:
        store_id = db.get_or_create_store("Test Store", "https://example.com/test")
        print(f"   Created store ID: {store_id}")
        print("✅ Store creation successful")
    except Exception as e:
        print(f"❌ Store creation failed: {e}")
        return False
    
    # Test 4: Status check storage
    print("\n4️⃣ Testing status check storage...")
    try:
        success = db.save_status_check(store_id, True, 1000, "Test check")
        if success:
            print("✅ Status check storage successful")
        else:
            print("❌ Status check storage failed")
            return False
    except Exception as e:
        print(f"❌ Status check storage failed: {e}")
        return False
    
    # Test 5: Summary report storage
    print("\n5️⃣ Testing summary report storage...")
    try:
        success = db.save_summary_report(1, 1, 0)
        if success:
            print("✅ Summary report storage successful")
        else:
            print("❌ Summary report storage failed")
            return False
    except Exception as e:
        print(f"❌ Summary report storage failed: {e}")
        return False
    
    print("\n" + "=" * 50)
    print("🎉 All database tests passed!")
    return True

if __name__ == "__main__":
    success = test_database_connection()
    sys.exit(0 if success else 1)
