#!/usr/bin/env python3
"""
CocoPan Monitor - Database Operations
Handles PostgreSQL/SQLite database operations with connection pooling
"""
import os
import time
import logging
from datetime import datetime
from contextlib import contextmanager
from typing import List, Tuple, Optional, Dict, Any

import psycopg2
import sqlite3
from psycopg2 import pool
from psycopg2.extras import RealDictCursor
import pandas as pd

from config import config

# Setup logging
logging.basicConfig(level=getattr(logging, config.LOG_LEVEL))
logger = logging.getLogger(__name__)

class DatabaseManager:
    """Database manager with connection pooling and error handling"""
    
    def __init__(self):
        self.connection_pool = None
        self.db_type = "sqlite" if config.USE_SQLITE else "postgresql"
        self._initialize_database()
    
    def _initialize_database(self):
        """Initialize database connection and create tables"""
        if self.db_type == "postgresql":
            self._init_postgresql()
        else:
            self._init_sqlite()
        
        self._create_tables()
        logger.info(f"✅ Database initialized ({self.db_type})")
    
    def _init_postgresql(self):
        """Initialize PostgreSQL connection pool"""
        try:
            # Parse database URL
            db_url = config.DATABASE_URL
            if db_url.startswith('postgresql://'):
                # Create connection pool
                self.connection_pool = psycopg2.pool.ThreadedConnectionPool(
                    minconn=1,
                    maxconn=20,
                    dsn=db_url,
                    cursor_factory=RealDictCursor
                )
                logger.info("✅ PostgreSQL connection pool created")
            else:
                raise ValueError("Invalid PostgreSQL URL format")
        except Exception as e:
            logger.error(f"❌ PostgreSQL connection failed: {e}")
            logger.info("📝 Falling back to SQLite")
            self.db_type = "sqlite"
            self._init_sqlite()
    
    def _init_sqlite(self):
        """Initialize SQLite (fallback or development)"""
        self.sqlite_path = config.SQLITE_PATH
        # Test SQLite connection
        try:
            conn = sqlite3.connect(self.sqlite_path)
            conn.close()
            logger.info(f"✅ SQLite database: {self.sqlite_path}")
        except Exception as e:
            logger.error(f"❌ SQLite connection failed: {e}")
            raise
    
    @contextmanager
    def get_connection(self):
        """Get database connection from pool or create SQLite connection"""
        if self.db_type == "postgresql":
            conn = None
            try:
                conn = self.connection_pool.getconn()
                yield conn
            except Exception as e:
                if conn:
                    conn.rollback()
                logger.error(f"Database error: {e}")
                raise
            finally:
                if conn:
                    self.connection_pool.putconn(conn)
        else:
            # SQLite
            conn = None
            try:
                conn = sqlite3.connect(self.sqlite_path, timeout=30)
                conn.row_factory = sqlite3.Row  # For dict-like access
                yield conn
            except Exception as e:
                if conn:
                    conn.rollback()
                logger.error(f"SQLite error: {e}")
                raise
            finally:
                if conn:
                    conn.close()
    
    def _create_tables(self):
        """Create database tables if they don't exist"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            # Stores table
            if self.db_type == "postgresql":
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS stores (
                        id SERIAL PRIMARY KEY,
                        name VARCHAR(255) NOT NULL,
                        url TEXT NOT NULL UNIQUE,
                        platform VARCHAR(50) NOT NULL,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                ''')
                
                # Status checks table
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS status_checks (
                        id SERIAL PRIMARY KEY,
                        store_id INTEGER REFERENCES stores(id),
                        is_online BOOLEAN NOT NULL,
                        checked_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        response_time_ms INTEGER,
                        error_message TEXT
                    )
                ''')
                
                # Summary reports table
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS summary_reports (
                        id SERIAL PRIMARY KEY,
                        total_stores INTEGER NOT NULL,
                        online_stores INTEGER NOT NULL,
                        offline_stores INTEGER NOT NULL,
                        online_percentage REAL NOT NULL,
                        report_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                ''')
                
                # Create indexes for performance
                cursor.execute('''
                    CREATE INDEX IF NOT EXISTS idx_status_checks_store_id 
                    ON status_checks(store_id)
                ''')
                cursor.execute('''
                    CREATE INDEX IF NOT EXISTS idx_status_checks_checked_at 
                    ON status_checks(checked_at)
                ''')
                
            else:
                # SQLite (keep existing schema)
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS stores (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        name TEXT NOT NULL,
                        url TEXT NOT NULL UNIQUE,
                        platform TEXT NOT NULL,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                ''')
                
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS status_checks (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        store_id INTEGER,
                        is_online BOOLEAN NOT NULL,
                        checked_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        response_time_ms INTEGER,
                        error_message TEXT,
                        FOREIGN KEY (store_id) REFERENCES stores (id)
                    )
                ''')
                
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS summary_reports (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        total_stores INTEGER NOT NULL,
                        online_stores INTEGER NOT NULL,
                        offline_stores INTEGER NOT NULL,
                        online_percentage REAL NOT NULL,
                        report_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                ''')
            
            conn.commit()
    
    def get_or_create_store(self, name: str, url: str) -> int:
        """Get store ID or create new store record"""
        platform = "foodpanda" if "foodpanda.ph" in url else "grabfood"
        
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            # Try to find existing store
            cursor.execute("SELECT id FROM stores WHERE url = %s", (url,))
            result = cursor.fetchone()
            
            if result:
                return result[0] if self.db_type == "postgresql" else result["id"]
            
            # Create new store
            if self.db_type == "postgresql":
                cursor.execute(
                    "INSERT INTO stores (name, url, platform) VALUES (%s, %s, %s) RETURNING id",
                    (name, url, platform)
                )
                store_id = cursor.fetchone()[0]
            else:
                cursor.execute(
                    "INSERT INTO stores (name, url, platform) VALUES (?, ?, ?)",
                    (name, url, platform)
                )
                store_id = cursor.lastrowid
            
            conn.commit()
            return store_id
    
    def save_status_check(self, store_id: int, is_online: bool, 
                         response_time_ms: Optional[int] = None, 
                         error_message: Optional[str] = None) -> bool:
        """Save status check with retry logic"""
        max_retries = config.MAX_RETRIES
        
        for attempt in range(max_retries):
            try:
                with self.get_connection() as conn:
                    cursor = conn.cursor()
                    
                    if self.db_type == "postgresql":
                        cursor.execute('''
                            INSERT INTO status_checks (store_id, is_online, response_time_ms, error_message)
                            VALUES (%s, %s, %s, %s)
                        ''', (store_id, is_online, response_time_ms, error_message))
                    else:
                        cursor.execute('''
                            INSERT INTO status_checks (store_id, is_online, response_time_ms, error_message)
                            VALUES (?, ?, ?, ?)
                        ''', (store_id, is_online, response_time_ms, error_message))
                    
                    conn.commit()
                    return True
                    
            except Exception as e:
                logger.warning(f"Status check save attempt {attempt + 1} failed: {e}")
                if attempt < max_retries - 1:
                    time.sleep(config.RETRY_DELAY)
                    continue
                else:
                    logger.error(f"Failed to save status check after {max_retries} attempts")
                    return False
        
        return False
    
    def save_summary_report(self, total_stores: int, online_stores: int, 
                           offline_stores: int) -> bool:
        """Save summary report"""
        online_percentage = (online_stores / total_stores * 100) if total_stores > 0 else 0
        
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                if self.db_type == "postgresql":
                    cursor.execute('''
                        INSERT INTO summary_reports (total_stores, online_stores, offline_stores, online_percentage)
                        VALUES (%s, %s, %s, %s)
                    ''', (total_stores, online_stores, offline_stores, online_percentage))
                else:
                    cursor.execute('''
                        INSERT INTO summary_reports (total_stores, online_stores, offline_stores, online_percentage)
                        VALUES (?, ?, ?, ?)
                    ''', (total_stores, online_stores, offline_stores, online_percentage))
                
                conn.commit()
                logger.info(f"📊 Summary saved: {online_stores}/{total_stores} online ({online_percentage:.1f}%)")
                return True
                
        except Exception as e:
            logger.error(f"Failed to save summary report: {e}")
            return False
    
    def get_latest_status(self) -> pd.DataFrame:
        """Get latest status for each store"""
        with self.get_connection() as conn:
            query = '''
                SELECT 
                    s.name,
                    s.url,
                    s.platform,
                    sc.is_online,
                    sc.checked_at,
                    sc.response_time_ms
                FROM stores s
                INNER JOIN status_checks sc ON s.id = sc.store_id
                INNER JOIN (
                    SELECT store_id, MAX(checked_at) as latest_check
                    FROM status_checks
                    GROUP BY store_id
                ) latest ON sc.store_id = latest.store_id AND sc.checked_at = latest.latest_check
                ORDER BY s.name
            '''
            return pd.read_sql_query(query, conn)
    
    def get_hourly_data(self) -> pd.DataFrame:
        """Get hourly summaries for today"""
        with self.get_connection() as conn:
            if self.db_type == "postgresql":
                query = '''
                    SELECT 
                        EXTRACT(HOUR FROM report_time AT TIME ZONE 'Asia/Manila') as hour,
                        ROUND(AVG(online_percentage), 0) as online_pct,
                        ROUND(AVG(100 - online_percentage), 0) as offline_pct,
                        COUNT(*) as data_points
                    FROM summary_reports
                    WHERE DATE(report_time AT TIME ZONE 'Asia/Manila') = CURRENT_DATE
                    GROUP BY EXTRACT(HOUR FROM report_time AT TIME ZONE 'Asia/Manila')
                    ORDER BY hour
                '''
            else:
                query = '''
                    SELECT 
                        strftime('%H', report_time) as hour,
                        ROUND(AVG(online_percentage), 0) as online_pct,
                        ROUND(AVG(100 - online_percentage), 0) as offline_pct,
                        COUNT(*) as data_points
                    FROM summary_reports
                    WHERE DATE(report_time, '+8 hours') = DATE('now', '+8 hours')
                    GROUP BY strftime('%H', report_time)
                    ORDER BY hour
                '''
            return pd.read_sql_query(query, conn)
    
    def get_store_logs(self, limit: int = 50) -> pd.DataFrame:
        """Get recent store logs"""
        with self.get_connection() as conn:
            if self.db_type == "postgresql":
                query = '''
                    SELECT 
                        s.name,
                        s.platform,
                        sc.is_online,
                        sc.checked_at,
                        sc.response_time_ms
                    FROM stores s
                    INNER JOIN status_checks sc ON s.id = sc.store_id
                    WHERE DATE(sc.checked_at AT TIME ZONE 'Asia/Manila') = CURRENT_DATE
                    ORDER BY sc.checked_at DESC
                    LIMIT %s
                '''
                return pd.read_sql_query(query, conn, params=(limit,))
            else:
                query = '''
                    SELECT 
                        s.name,
                        s.platform,
                        sc.is_online,
                        sc.checked_at,
                        sc.response_time_ms
                    FROM stores s
                    INNER JOIN status_checks sc ON s.id = sc.store_id
                    WHERE DATE(sc.checked_at, '+8 hours') = DATE('now', '+8 hours')
                    ORDER BY sc.checked_at DESC
                    LIMIT ?
                '''
                return pd.read_sql_query(query, conn, params=(limit,))
    
    def get_daily_uptime(self) -> pd.DataFrame:
        """Get daily uptime per store"""
        with self.get_connection() as conn:
            if self.db_type == "postgresql":
                query = '''
                    SELECT 
                        s.name,
                        s.platform,
                        COUNT(sc.id) as total_checks,
                        SUM(CASE WHEN sc.is_online = true THEN 1 ELSE 0 END) as online_checks,
                        ROUND(
                            (SUM(CASE WHEN sc.is_online = true THEN 1 ELSE 0 END) * 100.0 / COUNT(sc.id)), 
                            0
                        ) as uptime_percentage
                    FROM stores s
                    INNER JOIN status_checks sc ON s.id = sc.store_id
                    WHERE DATE(sc.checked_at AT TIME ZONE 'Asia/Manila') = CURRENT_DATE
                    GROUP BY s.id, s.name, s.platform
                    ORDER BY uptime_percentage DESC
                '''
            else:
                query = '''
                    SELECT 
                        s.name,
                        s.platform,
                        COUNT(sc.id) as total_checks,
                        SUM(CASE WHEN sc.is_online = 1 THEN 1 ELSE 0 END) as online_checks,
                        ROUND(
                            (SUM(CASE WHEN sc.is_online = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(sc.id)), 
                            0
                        ) as uptime_percentage
                    FROM stores s
                    INNER JOIN status_checks sc ON s.id = sc.store_id
                    WHERE DATE(sc.checked_at, '+8 hours') = DATE('now', '+8 hours')
                    GROUP BY s.id, s.name, s.platform
                    ORDER BY uptime_percentage DESC
                '''
            return pd.read_sql_query(query, conn)
    
    def get_database_stats(self) -> Dict[str, Any]:
        """Get database statistics"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                # Store count
                cursor.execute('SELECT COUNT(*) FROM stores')
                store_count = cursor.fetchone()[0]
                
                # Platform breakdown
                cursor.execute('SELECT platform, COUNT(*) FROM stores GROUP BY platform')
                platforms = dict(cursor.fetchall())
                
                # Total checks
                cursor.execute('SELECT COUNT(*) FROM status_checks')
                total_checks = cursor.fetchone()[0]
                
                # Latest summary
                cursor.execute('SELECT * FROM summary_reports ORDER BY report_time DESC LIMIT 1')
                latest_summary = cursor.fetchone()
                
                return {
                    'store_count': store_count,
                    'platforms': platforms,
                    'total_checks': total_checks,
                    'latest_summary': dict(latest_summary) if latest_summary else None,
                    'db_type': self.db_type
                }
                
        except Exception as e:
            logger.error(f"Failed to get database stats: {e}")
            return {}
    
    def close(self):
        """Close database connections"""
        if self.connection_pool:
            self.connection_pool.closeall()
            logger.info("🔒 Database connections closed")

# Global database manager instance
db = DatabaseManager()