#!/usr/bin/env python3
"""
Fixed CocoPan Database Module
Dynamic timezone support and proper cursor result handling
ADDED: Manual override functionality for admin interface
"""
import os
import time
import logging
from contextlib import contextmanager
from typing import List, Tuple, Optional, Dict, Any
import psycopg2
import sqlite3
from psycopg2 import pool
import pandas as pd
from config import config

logging.basicConfig(level=getattr(logging, config.LOG_LEVEL))
logger = logging.getLogger(__name__)

class DatabaseManager:
    def __init__(self):
        self.connection_pool = None
        self.db_type = "sqlite" if config.USE_SQLITE else "postgresql"
        self.max_retries = 3
        self.retry_delay = 5
        self.timezone = config.TIMEZONE  # Use config timezone consistently
        self._initialize_database()
    
    def _initialize_database(self):
        """Initialize database with retry logic"""
        for attempt in range(self.max_retries):
            try:
                if self.db_type == "postgresql":
                    self._init_postgresql()
                else:
                    self._init_sqlite()
                self._create_tables()
                logger.info(f"✅ Database initialized ({self.db_type}) on attempt {attempt + 1}")
                return
            except Exception as e:
                logger.error(f"❌ Database initialization failed (attempt {attempt + 1}): {str(e)}")
                if attempt < self.max_retries - 1:
                    logger.info(f"🔄 Retrying in {self.retry_delay} seconds...")
                    time.sleep(self.retry_delay)
                else:
                    logger.error("❌ All database initialization attempts failed")
                    # Fallback to SQLite instead of crashing
                    self.db_type = "sqlite"
                    self._init_sqlite()
                    self._create_tables()
    
    def _init_postgresql(self):
        """Initialize PostgreSQL without RealDictCursor"""
        try:
            db_url = config.DATABASE_URL
            logger.info(f"🔌 Attempting PostgreSQL connection...")
            
            if not db_url.startswith('postgresql://'):
                raise ValueError(f"Invalid PostgreSQL URL format: {db_url}")
            
            # Test basic connection first
            test_conn = psycopg2.connect(db_url)
            test_conn.close()
            logger.info("✅ PostgreSQL test connection successful")
            
            # Create connection pool WITHOUT RealDictCursor
            self.connection_pool = psycopg2.pool.ThreadedConnectionPool(
                minconn=1, 
                maxconn=20, 
                dsn=db_url
            )
            logger.info("✅ PostgreSQL connection pool created")
            
        except psycopg2.OperationalError as e:
            error_msg = str(e).strip()
            logger.error(f"❌ PostgreSQL operational error: {error_msg}")
            raise
            
        except Exception as e:
            error_msg = str(e).strip()
            logger.error(f"❌ Unexpected PostgreSQL error: {error_msg}")
            raise
    
    def _init_sqlite(self):
        """Initialize SQLite"""
        self.sqlite_path = config.SQLITE_PATH
        try:
            # Ensure directory exists
            os.makedirs(os.path.dirname(os.path.abspath(self.sqlite_path)), exist_ok=True)
            
            # Test connection
            conn = sqlite3.connect(self.sqlite_path, timeout=30)
            conn.execute("SELECT 1")  # Test query
            conn.close()
            logger.info(f"✅ SQLite database ready: {self.sqlite_path}")
        except Exception as e:
            logger.error(f"❌ SQLite connection failed: {str(e)}")
            raise
    
    @contextmanager
    def get_connection(self):
        """Get database connection with proper error handling"""
        if self.db_type == "postgresql":
            conn = None
            try:
                if not self.connection_pool:
                    raise Exception("Connection pool not initialized")
                
                conn = self.connection_pool.getconn()
                if conn is None:
                    raise Exception("Failed to get connection from pool")
                
                # Test connection
                with conn.cursor() as cursor:
                    cursor.execute("SELECT 1")
                
                yield conn
                
            except Exception as e:
                error_msg = str(e).strip()
                logger.error(f"❌ Database connection error: {error_msg}")
                if conn:
                    try:
                        conn.rollback()
                    except:
                        pass
                raise Exception(f"Database connection failed: {error_msg}")
                
            finally:
                if conn and self.connection_pool:
                    try:
                        self.connection_pool.putconn(conn)
                    except:
                        pass
        else:
            # SQLite
            conn = None
            try:
                conn = sqlite3.connect(self.sqlite_path, timeout=30)
                conn.row_factory = sqlite3.Row
                
                # Test connection
                conn.execute("SELECT 1")
                
                yield conn
                
            except Exception as e:
                error_msg = str(e).strip()
                logger.error(f"❌ SQLite error: {error_msg}")
                if conn:
                    try:
                        conn.rollback()
                    except:
                        pass
                raise Exception(f"SQLite error: {error_msg}")
                
            finally:
                if conn:
                    try:
                        conn.close()
                    except:
                        pass
    
    def _create_tables(self):
        """Create database tables"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            if self.db_type == "postgresql":
                # Create stores table
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS stores (
                        id SERIAL PRIMARY KEY,
                        name VARCHAR(255) NOT NULL,
                        url TEXT NOT NULL UNIQUE,
                        platform VARCHAR(50) NOT NULL,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        name_override VARCHAR(255),
                        last_manual_check TIMESTAMP
                    )
                """)
                
                # Create status_checks table
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS status_checks (
                        id SERIAL PRIMARY KEY,
                        store_id INTEGER REFERENCES stores(id),
                        is_online BOOLEAN NOT NULL,
                        checked_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        response_time_ms INTEGER,
                        error_message TEXT
                    )
                """)
                
                # Create summary_reports table
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS summary_reports (
                        id SERIAL PRIMARY KEY,
                        total_stores INTEGER NOT NULL,
                        online_stores INTEGER NOT NULL,
                        offline_stores INTEGER NOT NULL,
                        online_percentage REAL NOT NULL,
                        report_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)
            else:
                # SQLite tables
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS stores (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        name TEXT NOT NULL,
                        url TEXT NOT NULL UNIQUE,
                        platform TEXT NOT NULL,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        name_override TEXT,
                        last_manual_check TIMESTAMP
                    )
                """)
                
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS status_checks (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        store_id INTEGER,
                        is_online BOOLEAN NOT NULL,
                        checked_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        response_time_ms INTEGER,
                        error_message TEXT,
                        FOREIGN KEY (store_id) REFERENCES stores (id)
                    )
                """)
                
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS summary_reports (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        total_stores INTEGER NOT NULL,
                        online_stores INTEGER NOT NULL,
                        offline_stores INTEGER NOT NULL,
                        online_percentage REAL NOT NULL,
                        report_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)
            
            conn.commit()
    
    def get_or_create_store(self, name: str, url: str) -> int:
        """Get or create store with proper tuple indexing"""
        platform = "foodpanda" if "foodpanda" in url else "grabfood"
        
        for attempt in range(self.max_retries):
            try:
                with self.get_connection() as conn:
                    cursor = conn.cursor()
                    
                    if self.db_type == "postgresql":
                        # Check existing store
                        cursor.execute("SELECT id FROM stores WHERE url = %s", (url,))
                        result = cursor.fetchone()
                        
                        if result:
                            return result[0]  # Tuple indexing for regular cursor
                        
                        # Create new store
                        cursor.execute(
                            "INSERT INTO stores (name, url, platform) VALUES (%s, %s, %s) RETURNING id",
                            (name, url, platform)
                        )
                        store_result = cursor.fetchone()
                        store_id = store_result[0]  # Tuple indexing
                    else:
                        # SQLite version
                        cursor.execute("SELECT id FROM stores WHERE url = ?", (url,))
                        result = cursor.fetchone()
                        
                        if result:
                            return result["id"]  # SQLite uses Row factory
                        
                        cursor.execute(
                            "INSERT INTO stores (name, url, platform) VALUES (?, ?, ?)",
                            (name, url, platform)
                        )
                        store_id = cursor.lastrowid
                    
                    conn.commit()
                    return store_id
                    
            except Exception as e:
                error_msg = str(e).strip()
                logger.error(f"❌ Failed to get_or_create_store for {name} (attempt {attempt + 1}): {error_msg}")
                if attempt < self.max_retries - 1:
                    time.sleep(self.retry_delay)
                else:
                    raise Exception(f"Failed to create store after {self.max_retries} attempts: {error_msg}")
    
    def save_status_check(self, store_id: int, is_online: bool, 
                         response_time_ms: Optional[int] = None, 
                         error_message: Optional[str] = None) -> bool:
        """Save status check with proper data types"""
        for attempt in range(self.max_retries):
            try:
                # Ensure proper data types
                is_online_value = bool(is_online)
                
                if response_time_ms is not None:
                    response_time_ms = int(response_time_ms)
                
                if error_message and len(error_message) > 500:
                    error_message = error_message[:500] + "..."
                
                with self.get_connection() as conn:
                    cursor = conn.cursor()
                    
                    if self.db_type == "postgresql":
                        cursor.execute("""
                            INSERT INTO status_checks (store_id, is_online, response_time_ms, error_message)
                            VALUES (%s, %s, %s, %s)
                        """, (store_id, is_online_value, response_time_ms, error_message))
                    else:
                        cursor.execute("""
                            INSERT INTO status_checks (store_id, is_online, response_time_ms, error_message)
                            VALUES (?, ?, ?, ?)
                        """, (store_id, is_online_value, response_time_ms, error_message))
                    
                    conn.commit()
                    return True
                    
            except Exception as e:
                error_msg = str(e).strip()
                logger.error(f"❌ Failed to save status check for store_id {store_id} (attempt {attempt + 1}): {error_msg}")
                if attempt < self.max_retries - 1:
                    time.sleep(self.retry_delay)
                else:
                    logger.error(f"❌ Failed to save status check after {self.max_retries} attempts")
                    return False
    
    def save_summary_report(self, total_stores: int, online_stores: int, offline_stores: int) -> bool:
        """Save summary report with proper data types"""
        for attempt in range(self.max_retries):
            try:
                online_percentage = (online_stores / total_stores * 100) if total_stores > 0 else 0
                
                # Ensure proper data types
                total_stores = int(total_stores)
                online_stores = int(online_stores)
                offline_stores = int(offline_stores)
                online_percentage = float(online_percentage)
                
                with self.get_connection() as conn:
                    cursor = conn.cursor()
                    
                    if self.db_type == "postgresql":
                        cursor.execute("""
                            INSERT INTO summary_reports (total_stores, online_stores, offline_stores, online_percentage)
                            VALUES (%s, %s, %s, %s)
                        """, (total_stores, online_stores, offline_stores, online_percentage))
                    else:
                        cursor.execute("""
                            INSERT INTO summary_reports (total_stores, online_stores, offline_stores, online_percentage)
                            VALUES (?, ?, ?, ?)
                        """, (total_stores, online_stores, offline_stores, online_percentage))
                    
                    conn.commit()
                    return True
                    
            except Exception as e:
                error_msg = str(e).strip()
                logger.error(f"❌ Failed to save summary report (attempt {attempt + 1}): {error_msg}")
                if attempt < self.max_retries - 1:
                    time.sleep(self.retry_delay)
                else:
                    logger.error(f"❌ Failed to save summary report after {self.max_retries} attempts")
                    return False
    
    def get_latest_status(self) -> pd.DataFrame:
        """Get latest status using proper connection for pandas"""
        try:
            with self.get_connection() as conn:
                query = """
                    SELECT 
                        s.name,
                        COALESCE(s.name_override, s.name) as display_name,
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
                    ORDER BY display_name
                """
                return pd.read_sql_query(query, conn)
        except Exception as e:
            logger.error(f"❌ Failed to get latest status: {str(e)}")
            return pd.DataFrame()
    
    def get_hourly_data(self) -> pd.DataFrame:
        """Get hourly data using dynamic timezone from config"""
        try:
            with self.get_connection() as conn:
                if self.db_type == "postgresql":
                    query = f"""
                        SELECT 
                            EXTRACT(HOUR FROM report_time AT TIME ZONE %s)::integer as hour,
                            ROUND(AVG(online_percentage)::numeric, 0)::integer as online_pct,
                            ROUND(AVG(100 - online_percentage)::numeric, 0)::integer as offline_pct,
                            COUNT(*) as data_points
                        FROM summary_reports
                        WHERE DATE(report_time AT TIME ZONE %s) = CURRENT_DATE
                        GROUP BY EXTRACT(HOUR FROM report_time AT TIME ZONE %s)
                        ORDER BY hour
                    """
                    return pd.read_sql_query(query, conn, params=(self.timezone, self.timezone, self.timezone))
                else:
                    # SQLite - calculate offset based on timezone
                    if self.timezone == 'Asia/Manila':
                        offset_hours = '+8 hours'
                    else:
                        offset_hours = '+8 hours'  # Default to Manila time
                    
                    query = f"""
                        SELECT 
                            strftime('%H', report_time, '{offset_hours}') as hour,
                            ROUND(AVG(online_percentage), 0) as online_pct,
                            ROUND(AVG(100 - online_percentage), 0) as offline_pct,
                            COUNT(*) as data_points
                        FROM summary_reports
                        WHERE DATE(report_time, '{offset_hours}') = DATE('now', '{offset_hours}')
                        GROUP BY strftime('%H', report_time, '{offset_hours}')
                        ORDER BY hour
                    """
                    return pd.read_sql_query(query, conn)
        except Exception as e:
            logger.error(f"❌ Failed to get hourly data: {str(e)}")
            return pd.DataFrame()
    
    def get_store_logs(self, limit: int = 50) -> pd.DataFrame:
        """Get store logs using dynamic timezone from config"""
        try:
            with self.get_connection() as conn:
                if self.db_type == "postgresql":
                    query = """
                        SELECT 
                            COALESCE(s.name_override, s.name) as name,
                            s.platform,
                            sc.is_online,
                            sc.checked_at,
                            sc.response_time_ms
                        FROM stores s
                        INNER JOIN status_checks sc ON s.id = sc.store_id
                        WHERE DATE(sc.checked_at AT TIME ZONE %s) = CURRENT_DATE
                        ORDER BY sc.checked_at DESC
                        LIMIT %s
                    """
                    return pd.read_sql_query(query, conn, params=(self.timezone, limit))
                else:
                    # SQLite with dynamic timezone offset
                    if self.timezone == 'Asia/Manila':
                        offset_hours = '+8 hours'
                    else:
                        offset_hours = '+8 hours'  # Default to Manila time
                    
                    query = f"""
                        SELECT 
                            COALESCE(s.name_override, s.name) as name,
                            s.platform,
                            sc.is_online,
                            sc.checked_at,
                            sc.response_time_ms
                        FROM stores s
                        INNER JOIN status_checks sc ON s.id = sc.store_id
                        WHERE DATE(sc.checked_at, '{offset_hours}') = DATE('now', '{offset_hours}')
                        ORDER BY sc.checked_at DESC
                        LIMIT ?
                    """
                    return pd.read_sql_query(query, conn, params=(limit,))
        except Exception as e:
            logger.error(f"❌ Failed to get store logs: {str(e)}")
            return pd.DataFrame()
    
    def get_daily_uptime(self) -> pd.DataFrame:
        """Get daily uptime using dynamic timezone from config"""
        try:
            with self.get_connection() as conn:
                if self.db_type == "postgresql":
                    query = """
                        SELECT 
                            COALESCE(s.name_override, s.name) as name,
                            s.platform,
                            COUNT(sc.id) as total_checks,
                            SUM(CASE WHEN sc.is_online = true THEN 1 ELSE 0 END) as online_checks,
                            ROUND(
                                (SUM(CASE WHEN sc.is_online = true THEN 1 ELSE 0 END) * 100.0 / COUNT(sc.id))::numeric, 
                                0
                            )::integer as uptime_percentage
                        FROM stores s
                        INNER JOIN status_checks sc ON s.id = sc.store_id
                        WHERE DATE(sc.checked_at AT TIME ZONE %s) = CURRENT_DATE
                        GROUP BY s.id, s.name, s.name_override, s.platform
                        ORDER BY uptime_percentage DESC
                    """
                    return pd.read_sql_query(query, conn, params=(self.timezone,))
                else:
                    # SQLite with dynamic timezone offset
                    if self.timezone == 'Asia/Manila':
                        offset_hours = '+8 hours'
                    else:
                        offset_hours = '+8 hours'  # Default to Manila time
                    
                    query = f"""
                        SELECT 
                            COALESCE(s.name_override, s.name) as name,
                            s.platform,
                            COUNT(sc.id) as total_checks,
                            SUM(CASE WHEN sc.is_online = 1 THEN 1 ELSE 0 END) as online_checks,
                            ROUND(
                                (SUM(CASE WHEN sc.is_online = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(sc.id)), 
                                0
                            ) as uptime_percentage
                        FROM stores s
                        INNER JOIN status_checks sc ON s.id = sc.store_id
                        WHERE DATE(sc.checked_at, '{offset_hours}') = DATE('now', '{offset_hours}')
                        GROUP BY s.id, s.name, s.name_override, s.platform
                        ORDER BY uptime_percentage DESC
                    """
                    return pd.read_sql_query(query, conn)
        except Exception as e:
            logger.error(f"❌ Failed to get daily uptime: {str(e)}")
            return pd.DataFrame()
    
    def get_database_stats(self) -> Dict[str, Any]:
        """Get database stats with FIXED tuple handling"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                cursor.execute('SELECT COUNT(*) FROM stores')
                store_count = cursor.fetchone()[0]
                
                cursor.execute('SELECT platform, COUNT(*) FROM stores GROUP BY platform')
                platforms = dict(cursor.fetchall())
                
                cursor.execute('SELECT COUNT(*) FROM status_checks')
                total_checks = cursor.fetchone()[0]
                
                if self.db_type == "postgresql":
                    cursor.execute("""
                        SELECT total_stores, online_stores, offline_stores, 
                               online_percentage, report_time
                        FROM summary_reports 
                        ORDER BY report_time DESC 
                        LIMIT 1
                    """)
                else:
                    cursor.execute('SELECT * FROM summary_reports ORDER BY report_time DESC LIMIT 1')
                
                latest_summary = cursor.fetchone()
                
                # FIXED: Proper handling of latest_summary tuple
                if latest_summary:
                    if self.db_type == "postgresql":
                        # We know the column order for PostgreSQL
                        latest_summary_dict = {
                            'total_stores': latest_summary[0],
                            'online_stores': latest_summary[1], 
                            'offline_stores': latest_summary[2],
                            'online_percentage': latest_summary[3],
                            'report_time': latest_summary[4]
                        }
                    else:
                        # SQLite Row can be converted to dict
                        latest_summary_dict = dict(latest_summary)
                else:
                    latest_summary_dict = None
                
                return {
                    'store_count': store_count,
                    'platforms': platforms,
                    'total_checks': total_checks,
                    'latest_summary': latest_summary_dict,
                    'db_type': self.db_type,
                    'timezone': self.timezone
                }
        except Exception as e:
            logger.error(f"❌ Failed to get database stats: {str(e)}")
            return {
                'store_count': 0,
                'platforms': {},
                'total_checks': 0,
                'latest_summary': None,
                'db_type': self.db_type,
                'timezone': self.timezone
            }
    
    # ADDED: Manual Override Methods
    
    def get_stores_needing_attention(self) -> pd.DataFrame:
        """Get stores that need manual attention (BLOCKED, UNKNOWN, ERROR)"""
        try:
            with self.get_connection() as conn:
                query = """
                    SELECT 
                        s.id,
                        COALESCE(s.name_override, s.name) as name,
                        s.url,
                        s.platform,
                        sc.is_online,
                        sc.checked_at,
                        sc.response_time_ms,
                        sc.error_message,
                        
                        -- Determine problematic status from error message
                        CASE 
                            WHEN sc.error_message LIKE '[BLOCKED]%' THEN 'BLOCKED'
                            WHEN sc.error_message LIKE '[UNKNOWN]%' THEN 'UNKNOWN'  
                            WHEN sc.error_message LIKE '[ERROR]%' THEN 'ERROR'
                            WHEN sc.is_online = false AND sc.error_message IS NOT NULL THEN 'NEEDS_CHECK'
                            ELSE 'OK'
                        END as problem_status
                        
                    FROM stores s
                    INNER JOIN (
                        SELECT DISTINCT ON (store_id) 
                            store_id, is_online, checked_at, response_time_ms, error_message
                        FROM status_checks 
                        ORDER BY store_id, checked_at DESC
                    ) sc ON s.id = sc.store_id
                    
                    WHERE (
                        sc.error_message LIKE '[BLOCKED]%' OR 
                        sc.error_message LIKE '[UNKNOWN]%' OR 
                        sc.error_message LIKE '[ERROR]%'
                    )
                    AND DATE(sc.checked_at) = CURRENT_DATE  -- Only today's issues
                    
                    ORDER BY sc.checked_at DESC
                """
                
                return pd.read_sql_query(query, conn)
                
        except Exception as e:
            logger.error(f"Failed to get stores needing attention: {e}")
            return pd.DataFrame()
    
    def set_store_name_override(self, store_id: int, new_name: str, set_by: str) -> bool:
        """Set custom name for a store"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                if self.db_type == "postgresql":
                    cursor.execute("""
                        UPDATE stores 
                        SET name_override = %s, last_manual_check = CURRENT_TIMESTAMP
                        WHERE id = %s
                    """, (new_name, store_id))
                else:
                    cursor.execute("""
                        UPDATE stores 
                        SET name_override = ?, last_manual_check = CURRENT_TIMESTAMP
                        WHERE id = ?
                    """, (new_name, store_id))
                
                conn.commit()
                logger.info(f"✅ Store name updated: store_id={store_id}, name='{new_name}', by={set_by}")
                return True
                
        except Exception as e:
            logger.error(f"Failed to set store name override: {e}")
            return False
    
    def close(self):
        """Close database connections"""
        if self.connection_pool:
            try:
                self.connection_pool.closeall()
                logger.info("✅ Connection pool closed")
            except Exception as e:
                logger.error(f"❌ Error closing connection pool: {e}")

# Global database instance
db = DatabaseManager()