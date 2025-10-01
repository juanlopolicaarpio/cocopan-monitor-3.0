#!/usr/bin/env python3
"""
CocoPan Database Module (Railway-ready) — WITH ADMIN HELPERS + SKU COMPLIANCE
- No hard-coded DB URL; always uses config.get_database_url()
- psycopg2 ThreadedConnectionPool for writes/updates
- SQLAlchemy Engine for all pandas reads (fixes pandas warning)
- TCP keepalives + pool_pre_ping + pool_recycle to auto-heal EOF/peer resets
- Keeps your hourly upserts & admin helpers (get_database_stats, get_stores_needing_attention, set_store_name_override)
- NEW: SKU Compliance monitoring tables and methods
"""
import os
import time
import logging
from contextlib import contextmanager
from typing import Optional, Dict, Any, List
from datetime import datetime

import sqlite3
import psycopg2
from psycopg2 import pool as pg_pool
import pandas as pd

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from config import config

logging.basicConfig(level=getattr(logging, config.LOG_LEVEL))
logger = logging.getLogger(__name__)

class DatabaseManager:
    def __init__(self):
        self.connection_pool: Optional[pg_pool.ThreadedConnectionPool] = None
        self.sa_engine: Optional[Engine] = None  # SQLAlchemy engine for pandas reads
        self.db_type = "sqlite" if config.USE_SQLITE else "postgresql"
        self.max_retries = int(getattr(config, "MAX_RETRIES", 3))
        self.retry_delay = int(getattr(config, "RETRY_DELAY", 5))
        self.timezone = config.TIMEZONE
        self.sqlite_path = getattr(config, "SQLITE_PATH", "store_status.db")
        self._initialize_database()

    # ---------- Initialization ----------

    def _initialize_database(self):
        """Initialize DB connections and ensure schema; fall back to SQLite only if all retries fail."""
        for attempt in range(self.max_retries):
            try:
                if self.db_type == "postgresql":
                    self._init_postgresql_pool()
                    self._init_sqlalchemy_engine()  # for pandas reads (resilient)
                else:
                    self._init_sqlite()

                self._create_tables()
                logger.info(f"✅ Database initialized ({self.db_type}) on attempt {attempt + 1}")
                return
            except Exception as e:
                logger.error(f"❌ Database initialization failed (attempt {attempt + 1}): {e}")
                if attempt < self.max_retries - 1:
                    time.sleep(self.retry_delay)
                else:
                    logger.error("⚠️ Falling back to SQLite for safety")
                    self.db_type = "sqlite"
                    self._init_sqlite()
                    self._create_tables()

    def _init_postgresql_pool(self):
        """Create psycopg2 pool using env DATABASE_URL (no hard-coding)."""
        db_url = config.get_database_url()
        logger.info("🔌 Connecting to PostgreSQL via env DATABASE_URL")
        # Quick test connection
        test_conn = psycopg2.connect(db_url)
        test_conn.close()
        # PgBouncer-friendly pool
        self.connection_pool = pg_pool.ThreadedConnectionPool(
            minconn=1,
            maxconn=10,
            dsn=db_url,
        )
        logger.info("✅ PostgreSQL connection pool created")

    def _init_sqlalchemy_engine(self):
        """Small, resilient engine for all pandas reads."""
        url = config.get_database_url()
        self.sa_engine = create_engine(
            url,
            pool_size=3,
            max_overflow=1,
            pool_pre_ping=True,   # swap dead sockets before use
            pool_recycle=120,     # recycle before hosted idle timeouts
            future=True,
            connect_args=dict(
                keepalives=1,
                keepalives_idle=30,
                keepalives_interval=10,
                keepalives_count=5,
            ),
        )

    def _init_sqlite(self):
        os.makedirs(os.path.dirname(os.path.abspath(self.sqlite_path)), exist_ok=True)
        conn = sqlite3.connect(self.sqlite_path, timeout=30)
        conn.execute("SELECT 1")
        conn.close()
        logger.info(f"✅ SQLite database ready: {self.sqlite_path}")

    # ---------- Connection management ----------

    @contextmanager
    def get_connection(self):
        """
        Yield a live connection for write/update operations.
        - Performs a preflight SELECT 1 (heals stale sockets)
        - One quick retry path if the first conn is broken
        """
        if self.db_type == "postgresql":
            if not self.connection_pool:
                raise RuntimeError("Connection pool not initialized")
            conn = None
            try:
                conn = self.connection_pool.getconn()
                with conn.cursor() as cur:
                    cur.execute("SELECT 1")  # preflight ping
                yield conn
            except Exception:
                # try one fresh connection
                try:
                    if conn:
                        conn.close()
                except Exception:
                    pass
                if self.connection_pool:
                    fresh = self.connection_pool.getconn()
                    try:
                        with fresh.cursor() as cur:
                            cur.execute("SELECT 1")
                        yield fresh
                    finally:
                        self.connection_pool.putconn(fresh)
                else:
                    raise
            finally:
                if conn and self.connection_pool:
                    try:
                        self.connection_pool.putconn(conn)
                    except Exception:
                        pass
        else:
            conn = sqlite3.connect(self.sqlite_path, timeout=30)
            conn.row_factory = sqlite3.Row
            try:
                conn.execute("SELECT 1")
                yield conn
            finally:
                try:
                    conn.close()
                except Exception:
                    pass

    def _ensure_sa(self) -> Engine:
        """Ensure a SQLAlchemy engine exists for reads."""
        if self.db_type == "sqlite":
            if not self.sa_engine:
                self.sa_engine = create_engine(f"sqlite:///{self.sqlite_path}", future=True)
            return self.sa_engine
        if not self.sa_engine:
            self._init_sqlalchemy_engine()
        return self.sa_engine

    # ---------- Schema ----------

    def _create_tables(self):
        with self.get_connection() as conn:
            cur = conn.cursor()
            if self.db_type == "postgresql":
                # Original tables
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS stores (
                        id SERIAL PRIMARY KEY,
                        name VARCHAR(255) NOT NULL,
                        url  TEXT NOT NULL UNIQUE,
                        platform VARCHAR(50) NOT NULL,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        name_override VARCHAR(255),
                        last_manual_check TIMESTAMP
                    )
                """)
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS status_checks (
                        id SERIAL PRIMARY KEY,
                        store_id INTEGER REFERENCES stores(id),
                        is_online BOOLEAN NOT NULL,
                        checked_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        response_time_ms INTEGER,
                        error_message TEXT
                    )
                """)
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS summary_reports (
                        id SERIAL PRIMARY KEY,
                        total_stores INTEGER NOT NULL,
                        online_stores INTEGER NOT NULL,
                        offline_stores INTEGER NOT NULL,
                        online_percentage REAL NOT NULL,
                        report_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS store_status_hourly (
                        effective_at  timestamptz NOT NULL,
                        platform      text        NOT NULL,
                        store_id      integer     NOT NULL REFERENCES stores(id),
                        status        text        NOT NULL,
                        confidence    real        NOT NULL,
                        response_ms   integer     NULL,
                        evidence      text        NULL,
                        probe_time    timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
                        run_id        uuid        NOT NULL,
                        PRIMARY KEY (platform, store_id, effective_at)
                    )
                """)
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS status_summary_hourly (
                        effective_at  timestamptz PRIMARY KEY,
                        total         integer NOT NULL,
                        online        integer NOT NULL,
                        offline       integer NOT NULL,
                        blocked       integer NOT NULL,
                        errors        integer NOT NULL,
                        unknown       integer NOT NULL,
                        last_probe_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                
                # NEW: SKU Compliance tables
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS master_skus (
                        id SERIAL PRIMARY KEY,
                        sku_code VARCHAR(50) NOT NULL,
                        product_name VARCHAR(255) NOT NULL,
                        platform VARCHAR(50) NOT NULL CHECK (platform IN ('grabfood', 'foodpanda')),
                        category VARCHAR(100) NOT NULL,
                        division VARCHAR(100),
                        flow_category VARCHAR(100),
                        gmv_q3 DECIMAL(15,2),
                        is_active BOOLEAN DEFAULT TRUE,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        UNIQUE(sku_code, platform)
                    )
                """)
                
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS store_sku_checks (
                        id SERIAL PRIMARY KEY,
                        store_id INTEGER NOT NULL REFERENCES stores(id),
                        platform VARCHAR(50) NOT NULL,
                        check_date DATE NOT NULL,
                        out_of_stock_skus TEXT[], -- Array of SKU codes that are out of stock
                        total_skus_checked INTEGER NOT NULL DEFAULT 0,
                        out_of_stock_count INTEGER NOT NULL DEFAULT 0,
                        compliance_percentage DECIMAL(5,2) NOT NULL DEFAULT 0.0,
                        checked_by VARCHAR(255) NOT NULL,
                        checked_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        notes TEXT,
                        UNIQUE(store_id, platform, check_date)
                    )
                """)
                
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS sku_compliance_summary (
                        id SERIAL PRIMARY KEY,
                        summary_date DATE NOT NULL,
                        platform VARCHAR(50) NOT NULL,
                        total_stores_checked INTEGER NOT NULL DEFAULT 0,
                        stores_100_percent INTEGER NOT NULL DEFAULT 0,
                        stores_80_plus_percent INTEGER NOT NULL DEFAULT 0,
                        stores_below_80_percent INTEGER NOT NULL DEFAULT 0,
                        average_compliance_percentage DECIMAL(5,2) NOT NULL DEFAULT 0.0,
                        total_out_of_stock_items INTEGER NOT NULL DEFAULT 0,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        UNIQUE(summary_date, platform)
                    )
                """)
                
                # Create indexes for SKU tables
                cur.execute("CREATE INDEX IF NOT EXISTS idx_master_skus_platform ON master_skus(platform)")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_master_skus_code ON master_skus(sku_code)")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_store_sku_checks_store_date ON store_sku_checks(store_id, check_date)")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_store_sku_checks_platform ON store_sku_checks(platform)")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_sku_compliance_summary_date ON sku_compliance_summary(summary_date)")
                
            else:
                # SQLite versions
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS stores (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        name TEXT NOT NULL,
                        url  TEXT NOT NULL UNIQUE,
                        platform TEXT NOT NULL,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        name_override TEXT,
                        last_manual_check TIMESTAMP
                    )
                """)
                cur.execute("""
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
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS summary_reports (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        total_stores INTEGER NOT NULL,
                        online_stores INTEGER NOT NULL,
                        offline_stores INTEGER NOT NULL,
                        online_percentage REAL NOT NULL,
                        report_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS store_status_hourly (
                        effective_at  TEXT NOT NULL,
                        platform      TEXT NOT NULL,
                        store_id      INTEGER NOT NULL,
                        status        TEXT NOT NULL,
                        confidence    REAL NOT NULL,
                        response_ms   INTEGER,
                        evidence      TEXT,
                        probe_time    TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                        run_id        TEXT NOT NULL,
                        PRIMARY KEY (platform, store_id, effective_at)
                    )
                """)
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS status_summary_hourly (
                        effective_at  TEXT PRIMARY KEY,
                        total         INTEGER NOT NULL,
                        online        INTEGER NOT NULL,
                        offline       INTEGER NOT NULL,
                        blocked       INTEGER NOT NULL,
                        errors        INTEGER NOT NULL,
                        unknown       INTEGER NOT NULL,
                        last_probe_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
                    )
                """)
                
                # NEW: SKU tables for SQLite
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS master_skus (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        sku_code TEXT NOT NULL,
                        product_name TEXT NOT NULL,
                        platform TEXT NOT NULL,
                        category TEXT NOT NULL,
                        division TEXT,
                        flow_category TEXT,
                        gmv_q3 REAL,
                        is_active BOOLEAN DEFAULT 1,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        UNIQUE(sku_code, platform)
                    )
                """)
                
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS store_sku_checks (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        store_id INTEGER NOT NULL,
                        platform TEXT NOT NULL,
                        check_date TEXT NOT NULL,
                        out_of_stock_skus TEXT, -- JSON string for SQLite
                        total_skus_checked INTEGER NOT NULL DEFAULT 0,
                        out_of_stock_count INTEGER NOT NULL DEFAULT 0,
                        compliance_percentage REAL NOT NULL DEFAULT 0.0,
                        checked_by TEXT NOT NULL,
                        checked_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        notes TEXT,
                        FOREIGN KEY (store_id) REFERENCES stores (id),
                        UNIQUE(store_id, platform, check_date)
                    )
                """)
                
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS sku_compliance_summary (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        summary_date TEXT NOT NULL,
                        platform TEXT NOT NULL,
                        total_stores_checked INTEGER NOT NULL DEFAULT 0,
                        stores_100_percent INTEGER NOT NULL DEFAULT 0,
                        stores_80_plus_percent INTEGER NOT NULL DEFAULT 0,
                        stores_below_80_percent INTEGER NOT NULL DEFAULT 0,
                        average_compliance_percentage REAL NOT NULL DEFAULT 0.0,
                        total_out_of_stock_items INTEGER NOT NULL DEFAULT 0,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        UNIQUE(summary_date, platform)
                    )
                """)
            conn.commit()

    # ---------- CRUD helpers (writes via psycopg2, reads via SQLAlchemy) ----------

    def get_or_create_store(self, name: str, url: str) -> int:
        platform = "foodpanda" if "foodpanda" in url else "grabfood"
        for attempt in range(self.max_retries):
            try:
                with self.get_connection() as conn:
                    cur = conn.cursor()
                    if self.db_type == "postgresql":
                        cur.execute("SELECT id FROM stores WHERE url = %s", (url,))
                        row = cur.fetchone()
                        if row:
                            return row[0]
                        cur.execute(
                            "INSERT INTO stores (name, url, platform) VALUES (%s, %s, %s) RETURNING id",
                            (name, url, platform)
                        )
                        store_id = cur.fetchone()[0]
                    else:
                        cur.execute("SELECT id FROM stores WHERE url = ?", (url,))
                        row = cur.fetchone()
                        if row:
                            return row["id"]
                        cur.execute(
                            "INSERT INTO stores (name, url, platform) VALUES (?, ?, ?)",
                            (name, url, platform)
                        )
                        store_id = cur.lastrowid
                    conn.commit()
                    return store_id
            except Exception as e:
                logger.error(f"❌ get_or_create_store failed (attempt {attempt+1}): {e}")
                if attempt < self.max_retries - 1:
                    time.sleep(self.retry_delay)
                else:
                    raise

    def save_status_check(self, store_id: int, is_online: bool,
                          response_time_ms: Optional[int] = None,
                          error_message: Optional[str] = None) -> bool:
        for attempt in range(self.max_retries):
            try:
                is_online_value = bool(is_online)
                response_time_ms = int(response_time_ms) if response_time_ms is not None else None
                if error_message and len(error_message) > 500:
                    error_message = error_message[:500] + "..."
                with self.get_connection() as conn:
                    cur = conn.cursor()
                    if self.db_type == "postgresql":
                        cur.execute("""
                            INSERT INTO status_checks (store_id, is_online, response_time_ms, error_message)
                            VALUES (%s, %s, %s, %s)
                        """, (store_id, is_online_value, response_time_ms, error_message))
                    else:
                        cur.execute("""
                            INSERT INTO status_checks (store_id, is_online, response_time_ms, error_message)
                            VALUES (?, ?, ?, ?)
                        """, (store_id, is_online_value, response_time_ms, error_message))
                    conn.commit()
                    return True
            except Exception as e:
                logger.error(f"❌ save_status_check failed (attempt {attempt+1}): {e}")
                if attempt < self.max_retries - 1:
                    time.sleep(self.retry_delay)
                else:
                    return False

    def save_summary_report(self, total_stores: int, online_stores: int, offline_stores: int) -> bool:
        for attempt in range(self.max_retries):
            try:
                online_pct = float((online_stores / total_stores * 100) if total_stores > 0 else 0.0)
                with self.get_connection() as conn:
                    cur = conn.cursor()
                    if self.db_type == "postgresql":
                        cur.execute("""
                            INSERT INTO summary_reports (total_stores, online_stores, offline_stores, online_percentage)
                            VALUES (%s, %s, %s, %s)
                        """, (int(total_stores), int(online_stores), int(offline_stores), online_pct))
                    else:
                        cur.execute("""
                            INSERT INTO summary_reports (total_stores, online_stores, offline_stores, online_percentage)
                            VALUES (?, ?, ?, ?)
                        """, (int(total_stores), int(online_stores), int(offline_stores), online_pct))
                    conn.commit()
                    return True
            except Exception as e:
                logger.error(f"❌ save_summary_report failed (attempt {attempt+1}): {e}")
                if attempt < self.max_retries - 1:
                    time.sleep(self.retry_delay)
                else:
                    return False

    # ---------- NEW: SKU Compliance Methods ----------

    def get_master_skus_by_platform(self, platform: str) -> List[Dict]:
        """Get all SKUs for a specific platform (grabfood/foodpanda)"""
        try:
            with self.get_connection() as conn:
                cur = conn.cursor()
                if self.db_type == "postgresql":
                    cur.execute("""
                        SELECT id, sku_code, product_name, category, division, flow_category, gmv_q3
                        FROM master_skus 
                        WHERE platform = %s AND is_active = TRUE
                        ORDER BY product_name
                    """, (platform,))
                else:
                    cur.execute("""
                        SELECT id, sku_code, product_name, category, division, flow_category, gmv_q3
                        FROM master_skus 
                        WHERE platform = ? AND is_active = 1
                        ORDER BY product_name
                    """, (platform,))
                
                rows = cur.fetchall()
                skus = []
                for row in rows:
                    if self.db_type == "postgresql":
                        skus.append({
                            'id': row[0],
                            'sku_code': row[1],
                            'product_name': row[2],
                            'category': row[3],
                            'division': row[4],
                            'flow_category': row[5],
                            'gmv_q3': float(row[6]) if row[6] else 0.0
                        })
                    else:
                        skus.append({
                            'id': row['id'],
                            'sku_code': row['sku_code'],
                            'product_name': row['product_name'],
                            'category': row['category'],
                            'division': row['division'],
                            'flow_category': row['flow_category'],
                            'gmv_q3': float(row['gmv_q3']) if row['gmv_q3'] else 0.0
                        })
                return skus
        except Exception as e:
            logger.error(f"❌ get_master_skus_by_platform failed: {e}")
            return []

    def search_master_skus(self, platform: str, search_term: str) -> List[Dict]:
        """Search products by name for a specific platform"""
        try:
            search_term = f"%{search_term}%"
            with self.get_connection() as conn:
                cur = conn.cursor()
                if self.db_type == "postgresql":
                    cur.execute("""
                        SELECT id, sku_code, product_name, category, division, flow_category, gmv_q3
                        FROM master_skus 
                        WHERE platform = %s AND is_active = TRUE
                          AND (product_name ILIKE %s OR sku_code ILIKE %s)
                        ORDER BY 
                            CASE WHEN product_name ILIKE %s THEN 1 ELSE 2 END,
                            product_name
                    """, (platform, search_term, search_term, f"{search_term}%"))
                else:
                    cur.execute("""
                        SELECT id, sku_code, product_name, category, division, flow_category, gmv_q3
                        FROM master_skus 
                        WHERE platform = ? AND is_active = 1
                          AND (product_name LIKE ? OR sku_code LIKE ?)
                        ORDER BY product_name
                    """, (platform, search_term, search_term))
                
                rows = cur.fetchall()
                skus = []
                for row in rows:
                    if self.db_type == "postgresql":
                        skus.append({
                            'id': row[0],
                            'sku_code': row[1],
                            'product_name': row[2],
                            'category': row[3],
                            'division': row[4],
                            'flow_category': row[5],
                            'gmv_q3': float(row[6]) if row[6] else 0.0
                        })
                    else:
                        skus.append({
                            'id': row['id'],
                            'sku_code': row['sku_code'],
                            'product_name': row['product_name'],
                            'category': row['category'],
                            'division': row['division'],
                            'flow_category': row['flow_category'],
                            'gmv_q3': float(row['gmv_q3']) if row['gmv_q3'] else 0.0
                        })
                return skus
        except Exception as e:
            logger.error(f"❌ search_master_skus failed: {e}")
            return []

    def get_store_sku_status_today(self, store_id: int, platform: str) -> Optional[Dict]:
        """Get current day's SKU check status for a store"""
        try:
            today = datetime.now().date()
            with self.get_connection() as conn:
                cur = conn.cursor()
                if self.db_type == "postgresql":
                    cur.execute("""
                        SELECT out_of_stock_skus, total_skus_checked, out_of_stock_count,
                                compliance_percentage, checked_by, checked_at
                        FROM store_sku_checks 
                        WHERE store_id = %s AND platform = %s AND check_date = %s
                    """, (store_id, platform, today))
                else:
                    cur.execute("""
                        SELECT out_of_stock_skus, total_skus_checked, out_of_stock_count,
                                compliance_percentage, checked_by, checked_at
                        FROM store_sku_checks 
                        WHERE store_id = ? AND platform = ? AND check_date = ?
                    """, (store_id, platform, today.isoformat()))
                
                row = cur.fetchone()
                if row:
                    if self.db_type == "postgresql":
                        return {
                            'out_of_stock_skus': row[0] or [],
                            'total_skus_checked': row[1],
                            'out_of_stock_count': row[2],
                            'compliance_percentage': float(row[3]),
                            'checked_by': row[4],
                            'checked_at': row[5]
                        }
                    else:
                        import json
                        return {
                            'out_of_stock_skus': json.loads(row['out_of_stock_skus']) if row['out_of_stock_skus'] else [],
                            'total_skus_checked': row['total_skus_checked'],
                            'out_of_stock_count': row['out_of_stock_count'],
                            'compliance_percentage': float(row['compliance_percentage']),
                            'checked_by': row['checked_by'],
                            'checked_at': row['checked_at']
                        }
                return None
        except Exception as e:
            logger.error(f"❌ get_store_sku_status_today failed: {e}")
            return None

    def save_sku_compliance_check(self, store_id: int, platform: str,
                                   out_of_stock_ids: List[str], checked_by: str) -> bool:
        """Save complete SKU compliance check - WITH VALIDATION AND DEDUPLICATION"""
        try:
            today = datetime.now().date()
            
            # ✅ STEP 1: REMOVE DUPLICATES FIRST
            original_count = len(out_of_stock_ids)
            out_of_stock_ids = list(set(out_of_stock_ids))  # Remove duplicates
            
            if original_count != len(out_of_stock_ids):
                logger.warning(
                    f"⚠️ Store {store_id}: Removed {original_count - len(out_of_stock_ids)} duplicate SKU codes"
                )
            
            # ✅ STEP 2: VALIDATE - Only include SKU codes that exist in master_skus
            with self.get_connection() as validation_conn:
                cur = validation_conn.cursor()
                if self.db_type == "postgresql":
                    cur.execute("""
                        SELECT sku_code FROM master_skus 
                        WHERE sku_code = ANY(%s) AND platform = %s
                    """, (out_of_stock_ids, platform))
                else:
                    # SQLite version
                    placeholders = ','.join(['?'] * len(out_of_stock_ids))
                    cur.execute(f"""
                        SELECT sku_code FROM master_skus 
                        WHERE sku_code IN ({placeholders}) AND platform = ?
                    """, (*out_of_stock_ids, platform))
                
                # Get only valid SKU codes
                valid_rows = cur.fetchall()
                if self.db_type == "postgresql":
                    valid_sku_codes = [row[0] for row in valid_rows]
                else:
                    valid_sku_codes = [row['sku_code'] for row in valid_rows]
                
                # Log any invalid SKU codes
                invalid_skus = set(out_of_stock_ids) - set(valid_sku_codes)
                if invalid_skus:
                    logger.warning(
                        f"⚠️ Store {store_id}: {len(invalid_skus)} SKU codes not found in master_skus: "
                        f"{list(invalid_skus)}"
                    )
            
            # Get total SKUs for this platform
            total_skus = len(self.get_master_skus_by_platform(platform))
            
            # ✅ USE VALIDATED COUNT - only count SKUs that actually exist (and are unique)
            out_of_stock_count = len(valid_sku_codes)
            compliance_pct = ((total_skus - out_of_stock_count) / max(total_skus, 1)) * 100.0
            
            with self.get_connection() as conn:
                cur = conn.cursor()
                if self.db_type == "postgresql":
                    # ✅ Save only valid SKU codes
                    cur.execute("""
                        INSERT INTO store_sku_checks 
                        (store_id, platform, check_date, out_of_stock_skus, total_skus_checked,
                         out_of_stock_count, compliance_percentage, checked_by)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (store_id, platform, check_date) 
                        DO UPDATE SET
                            out_of_stock_skus = EXCLUDED.out_of_stock_skus,
                            total_skus_checked = EXCLUDED.total_skus_checked,
                            out_of_stock_count = EXCLUDED.out_of_stock_count,
                            compliance_percentage = EXCLUDED.compliance_percentage,
                            checked_by = EXCLUDED.checked_by,
                            checked_at = CURRENT_TIMESTAMP
                    """, (store_id, platform, today, valid_sku_codes, total_skus,
                          out_of_stock_count, compliance_pct, checked_by))
                else:
                    import json
                    cur.execute("""
                        INSERT OR REPLACE INTO store_sku_checks 
                        (store_id, platform, check_date, out_of_stock_skus, total_skus_checked,
                         out_of_stock_count, compliance_percentage, checked_by)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """, (store_id, platform, today.isoformat(), json.dumps(valid_sku_codes),
                          total_skus, out_of_stock_count, compliance_pct, checked_by))
                
                conn.commit()
                
                # Log success with validation info
                if invalid_skus:
                    logger.info(
                        f"✅ Saved SKU check for store {store_id}: "
                        f"{out_of_stock_count} valid OOS items "
                        f"({len(invalid_skus)} invalid SKUs excluded)"
                    )
                else:
                    logger.info(
                        f"✅ Saved SKU check for store {store_id}: "
                        f"{out_of_stock_count} OOS items (all valid)"
                    )
                
                # Update daily summary
                self._update_daily_sku_summary(platform, today, conn)
                return True
            
        except Exception as e:
            logger.error(f"❌ save_sku_compliance_check failed: {e}")
            return False

    def _update_daily_sku_summary(self, platform: str, check_date, conn):
        """Update daily SKU compliance summary"""
        try:
            cur = conn.cursor()
            if self.db_type == "postgresql":
                cur.execute("""
                    WITH daily_stats AS (
                        SELECT 
                            COUNT(*) as total_stores,
                            COUNT(*) FILTER (WHERE compliance_percentage = 100.0) as stores_100,
                            COUNT(*) FILTER (WHERE compliance_percentage >= 80.0) as stores_80_plus,
                            COUNT(*) FILTER (WHERE compliance_percentage < 80.0) as stores_below_80,
                            AVG(compliance_percentage) as avg_compliance,
                            SUM(out_of_stock_count) as total_oos
                        FROM store_sku_checks
                        WHERE platform = %s AND check_date = %s
                    )
                    INSERT INTO sku_compliance_summary 
                    (summary_date, platform, total_stores_checked, stores_100_percent,
                     stores_80_plus_percent, stores_below_80_percent, average_compliance_percentage,
                     total_out_of_stock_items)
                    SELECT %s, %s, total_stores, stores_100, stores_80_plus, stores_below_80,
                           avg_compliance, total_oos
                    FROM daily_stats
                    ON CONFLICT (summary_date, platform)
                    DO UPDATE SET
                        total_stores_checked = EXCLUDED.total_stores_checked,
                        stores_100_percent = EXCLUDED.stores_100_percent,
                        stores_80_plus_percent = EXCLUDED.stores_80_plus_percent,
                        stores_below_80_percent = EXCLUDED.stores_below_80_percent,
                        average_compliance_percentage = EXCLUDED.average_compliance_percentage,
                        total_out_of_stock_items = EXCLUDED.total_out_of_stock_items
                """, (platform, check_date, check_date, platform))
            else:
                # SQLite version - simplified
                cur.execute("""
                    INSERT OR REPLACE INTO sku_compliance_summary
                    (summary_date, platform, total_stores_checked, average_compliance_percentage,
                     total_out_of_stock_items)
                    SELECT ?, ?, COUNT(*), AVG(compliance_percentage), SUM(out_of_stock_count)
                    FROM store_sku_checks
                    WHERE platform = ? AND check_date = ?
                """, (check_date.isoformat(), platform, platform, check_date.isoformat()))
            
            conn.commit()
            
        except Exception as e:
            logger.error(f"❌ _update_daily_sku_summary failed: {e}")

    def get_sku_compliance_dashboard(self) -> List[Dict]:
        """Get today's compliance summary for all stores"""
        try:
            today = datetime.now().date()
            with self.get_connection() as conn:
                cur = conn.cursor()
                if self.db_type == "postgresql":
                    cur.execute("""
                        SELECT s.id, s.name, s.platform, ssc.compliance_percentage,
                               ssc.out_of_stock_count, ssc.checked_by, ssc.checked_at
                        FROM stores s
                        LEFT JOIN store_sku_checks ssc ON s.id = ssc.store_id
                            AND ssc.check_date = %s
                            AND s.platform = ssc.platform
                        WHERE s.platform IN ('grabfood', 'foodpanda')
                        ORDER BY s.platform, ssc.compliance_percentage DESC NULLS LAST, s.name
                    """, (today,))
                else:
                    cur.execute("""
                        SELECT s.id, s.name, s.platform, ssc.compliance_percentage,
                               ssc.out_of_stock_count, ssc.checked_by, ssc.checked_at
                        FROM stores s
                        LEFT JOIN store_sku_checks ssc ON s.id = ssc.store_id
                            AND ssc.check_date = ?
                            AND s.platform = ssc.platform
                        WHERE s.platform IN ('grabfood', 'foodpanda')
                        ORDER BY s.platform, ssc.compliance_percentage DESC, s.name
                    """, (today.isoformat(),))
                
                rows = cur.fetchall()
                dashboard_data = []
                for row in rows:
                    if self.db_type == "postgresql":
                        dashboard_data.append({
                            'store_id': row[0],
                            'store_name': row[1],
                            'platform': row[2],
                            'compliance_percentage': float(row[3]) if row[3] is not None else None,
                            'out_of_stock_count': row[4] or 0,
                            'checked_by': row[5],
                            'checked_at': row[6]
                        })
                    else:
                        dashboard_data.append({
                            'store_id': row['id'],
                            'store_name': row['name'],
                            'platform': row['platform'],
                            'compliance_percentage': float(row['compliance_percentage']) if row['compliance_percentage'] is not None else None,
                            'out_of_stock_count': row['out_of_stock_count'] or 0,
                            'checked_by': row['checked_by'],
                            'checked_at': row['checked_at']
                        })
                
                return dashboard_data
        except Exception as e:
            logger.error(f"❌ get_sku_compliance_dashboard failed: {e}")
            return []

    def get_out_of_stock_details(self, store_id: Optional[int] = None) -> List[Dict]:
        """Get detailed out-of-stock report"""
        try:
            today = datetime.now().date()
            with self.get_connection() as conn:
                cur = conn.cursor()
                base_query = """
                    SELECT s.name as store_name, s.platform, ms.sku_code, ms.product_name,
                           ms.category, ms.division, ssc.checked_by, ssc.checked_at
                    FROM store_sku_checks ssc
                    JOIN stores s ON ssc.store_id = s.id
                    JOIN master_skus ms ON ms.sku_code = ANY(ssc.out_of_stock_skus)
                        AND ms.platform = ssc.platform
                    WHERE ssc.check_date = %s
                """
                
                if self.db_type == "postgresql":
                    if store_id:
                        cur.execute(base_query + " AND s.id = %s ORDER BY s.name, ms.product_name",
                                   (today, store_id))
                    else:
                        cur.execute(base_query + " ORDER BY s.name, ms.product_name", (today,))
                else:
                    # SQLite version - need to handle JSON differently
                    if store_id:
                        cur.execute("""
                            SELECT s.name as store_name, s.platform, ssc.out_of_stock_skus,
                                   ssc.checked_by, ssc.checked_at
                            FROM store_sku_checks ssc
                            JOIN stores s ON ssc.store_id = s.id
                            WHERE ssc.check_date = ? AND s.id = ?
                            ORDER BY s.name
                        """, (today.isoformat(), store_id))
                    else:
                        cur.execute("""
                            SELECT s.name as store_name, s.platform, ssc.out_of_stock_skus,
                                   ssc.checked_by, ssc.checked_at
                            FROM store_sku_checks ssc
                            JOIN stores s ON ssc.store_id = s.id
                            WHERE ssc.check_date = ?
                            ORDER BY s.name
                        """, (today.isoformat(),))
                
                rows = cur.fetchall()
                details = []
                
                if self.db_type == "postgresql":
                    for row in rows:
                        details.append({
                            'store_name': row[0],
                            'platform': row[1],
                            'sku_code': row[2],
                            'product_name': row[3],
                            'category': row[4],
                            'division': row[5],
                            'checked_by': row[6],
                            'checked_at': row[7]
                        })
                else:
                    # For SQLite, we need to expand the JSON array manually
                    import json
                    for row in rows:
                        try:
                            oos_skus = json.loads(row['out_of_stock_skus']) if row['out_of_stock_skus'] else []
                            for sku_code in oos_skus:
                                # Get SKU details
                                cur.execute("""
                                    SELECT sku_code, product_name, category, division
                                    FROM master_skus 
                                    WHERE sku_code = ? AND platform = ?
                                """, (sku_code, row['platform']))
                                sku_row = cur.fetchone()
                                if sku_row:
                                    details.append({
                                        'store_name': row['store_name'],
                                        'platform': row['platform'],
                                        'sku_code': sku_row['sku_code'],
                                        'product_name': sku_row['product_name'],
                                        'category': sku_row['category'],
                                        'division': sku_row['division'],
                                        'checked_by': row['checked_by'],
                                        'checked_at': row['checked_at']
                                    })
                        except Exception as e:
                            logger.error(f"Error processing SQLite OOS details: {e}")
                
                return details
        except Exception as e:
            logger.error(f"❌ get_out_of_stock_details failed: {e}")
            return []

    def bulk_add_master_skus(self, sku_list: List[Dict]) -> bool:
        """Bulk add master SKU data"""
        try:
            with self.get_connection() as conn:
                cur = conn.cursor()
                for sku in sku_list:
                    if self.db_type == "postgresql":
                        cur.execute("""
                            INSERT INTO master_skus 
                            (sku_code, product_name, platform, category, division, flow_category, gmv_q3)
                            VALUES (%s, %s, %s, %s, %s, %s, %s)
                            ON CONFLICT (sku_code, platform) DO UPDATE SET
                                product_name = EXCLUDED.product_name,
                                category = EXCLUDED.category,
                                division = EXCLUDED.division,
                                flow_category = EXCLUDED.flow_category,
                                gmv_q3 = EXCLUDED.gmv_q3
                        """, (
                            sku['sku_code'],
                            sku['product_name'],
                            sku['platform'],
                            sku['category'],
                            sku.get('division'),
                            sku.get('flow_category'),
                            sku.get('gmv_q3')
                        ))
                    else:
                        cur.execute("""
                            INSERT OR REPLACE INTO master_skus 
                            (sku_code, product_name, platform, category, division, flow_category, gmv_q3)
                            VALUES (?, ?, ?, ?, ?, ?, ?)
                        """, (
                            sku['sku_code'],
                            sku['product_name'],
                            sku['platform'],
                            sku['category'],
                            sku.get('division'),
                            sku.get('flow_category'),
                            sku.get('gmv_q3')
                        ))
                
                conn.commit()
                logger.info(f"✅ Bulk added {len(sku_list)} SKUs successfully")
                return True
                
        except Exception as e:
            logger.error(f"❌ bulk_add_master_skus failed: {e}")
            return False

    # ---------- READ APIs (pandas via SQLAlchemy engine; fixes pandas warning) ----------

    def get_latest_status(self) -> pd.DataFrame:
        try:
            sql = """
                SELECT 
                    s.name,
                    COALESCE(s.name_override, s.name) AS display_name,
                    s.url,
                    s.platform,
                    sc.is_online,
                    sc.checked_at,
                    sc.response_time_ms
                FROM stores s
                JOIN status_checks sc ON s.id = sc.store_id
                JOIN (
                    SELECT store_id, MAX(checked_at) AS latest_check
                    FROM status_checks
                    GROUP BY store_id
                ) latest ON sc.store_id = latest.store_id AND sc.checked_at = latest.latest_check
                ORDER BY display_name
            """
            return pd.read_sql_query(text(sql), self._ensure_sa())
        except Exception as e:
            logger.error(f"❌ get_latest_status failed: {e}")
            return pd.DataFrame()

    def get_hourly_data(self) -> pd.DataFrame:
        try:
            if self.db_type == "postgresql":
                sql = """
                    SELECT 
                        EXTRACT(HOUR FROM report_time AT TIME ZONE :tz)::integer AS hour,
                        ROUND(AVG(online_percentage)::numeric, 0)::integer       AS online_pct,
                        ROUND(AVG(100 - online_percentage)::numeric, 0)::integer AS offline_pct,
                        COUNT(*) AS data_points
                    FROM summary_reports
                    WHERE DATE(report_time AT TIME ZONE :tz) = CURRENT_DATE
                    GROUP BY EXTRACT(HOUR FROM report_time AT TIME ZONE :tz)
                    ORDER BY hour
                """
                return pd.read_sql_query(text(sql), self._ensure_sa(), params={"tz": self.timezone})
            else:
                offset = "+8 hours"  # Manila default
                sql = f"""
                    SELECT 
                        CAST(strftime('%H', report_time, '{offset}') AS INTEGER) AS hour,
                        ROUND(AVG(online_percentage), 0) AS online_pct,
                        ROUND(AVG(100 - online_percentage), 0) AS offline_pct,
                        COUNT(*) AS data_points
                    FROM summary_reports
                    WHERE DATE(report_time, '{offset}') = DATE('now', '{offset}')
                    GROUP BY strftime('%H', report_time, '{offset}')
                    ORDER BY hour
                """
                return pd.read_sql_query(text(sql), self._ensure_sa())
        except Exception as e:
            logger.error(f"❌ get_hourly_data failed: {e}")
            return pd.DataFrame()

    def get_store_logs(self, limit: int = 50) -> pd.DataFrame:
        try:
            if self.db_type == "postgresql":
                sql = """
                    SELECT 
                        COALESCE(s.name_override, s.name) AS name,
                        s.platform,
                        sc.is_online,
                        sc.checked_at,
                        sc.response_time_ms
                    FROM stores s
                    JOIN status_checks sc ON s.id = sc.store_id
                    WHERE DATE(sc.checked_at AT TIME ZONE :tz) = CURRENT_DATE
                    ORDER BY sc.checked_at DESC
                    LIMIT :lim
                """
                return pd.read_sql_query(text(sql), self._ensure_sa(), params={"tz": self.timezone, "lim": int(limit)})
            else:
                offset = "+8 hours"
                sql = f"""
                    SELECT 
                        COALESCE(s.name_override, s.name) AS name,
                        s.platform,
                        sc.is_online,
                        sc.checked_at,
                        sc.response_time_ms
                    FROM stores s
                    JOIN status_checks sc ON s.id = sc.store_id
                    WHERE DATE(sc.checked_at, '{offset}') = DATE('now', '{offset}')
                    ORDER BY sc.checked_at DESC
                    LIMIT :lim
                """
                return pd.read_sql_query(text(sql), self._ensure_sa(), params={"lim": int(limit)})
        except Exception as e:
            logger.error(f"❌ get_store_logs failed: {e}")
            return pd.DataFrame()

    def get_daily_uptime(self) -> pd.DataFrame:
        try:
            if self.db_type == "postgresql":
                sql = """
                    SELECT 
                        COALESCE(s.name_override, s.name) AS name,
                        s.platform,
                        COUNT(sc.id)                                         AS total_checks,
                        SUM(CASE WHEN sc.is_online = true THEN 1 ELSE 0 END) AS online_checks,
                        ROUND(
                            (SUM(CASE WHEN sc.is_online = true THEN 1 ELSE 0 END) * 100.0 / COUNT(sc.id))::numeric, 
                            0
                        )::integer AS uptime_percentage
                    FROM stores s
                    JOIN status_checks sc ON s.id = sc.store_id
                    WHERE DATE(sc.checked_at AT TIME ZONE :tz) = CURRENT_DATE
                    GROUP BY s.id, s.name, s.name_override, s.platform
                    ORDER BY uptime_percentage DESC
                """
                return pd.read_sql_query(text(sql), self._ensure_sa(), params={"tz": self.timezone})
            else:
                offset = "+8 hours"
                sql = f"""
                    SELECT 
                        COALESCE(s.name_override, s.name) AS name,
                        s.platform,
                        COUNT(sc.id) AS total_checks,
                        SUM(CASE WHEN sc.is_online = 1 THEN 1 ELSE 0 END) AS online_checks,
                        ROUND(
                            (SUM(CASE WHEN sc.is_online = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(sc.id)), 
                            0
                        ) AS uptime_percentage
                    FROM stores s
                    JOIN status_checks sc ON s.id = sc.store_id
                    WHERE DATE(sc.checked_at, '{offset}') = DATE('now', '{offset}')
                    GROUP BY s.id, s.name, s.name_override, s.platform
                    ORDER BY uptime_percentage DESC
                """
                return pd.read_sql_query(text(sql), self._ensure_sa())
        except Exception as e:
            logger.error(f"❌ get_daily_uptime failed: {e}")
            return pd.DataFrame()

    # ---------- Admin helpers (ported & adapted) ----------

    def get_database_stats(self) -> Dict[str, Any]:
        """Return lightweight stats for health checks / admin widgets."""
        try:
            with self.get_connection() as conn:
                cur = conn.cursor()
                
                # store count
                cur.execute("SELECT COUNT(*) FROM stores")
                store_count = cur.fetchone()[0]
                
                # platform breakdown
                cur.execute("SELECT platform, COUNT(*) FROM stores GROUP BY platform")
                rows = cur.fetchall()
                platforms: Dict[str, int] = {}
                for r in rows:
                    # r can be tuple or Row
                    key = r[0]
                    val = r[1]
                    platforms[str(key)] = int(val)
                
                # total checks
                cur.execute("SELECT COUNT(*) FROM status_checks")
                total_checks = cur.fetchone()[0]
                
                # SKU stats
                cur.execute("SELECT COUNT(*) FROM master_skus")
                total_skus = cur.fetchone()[0]
                
                # latest summary (most recent report_time)
                if self.db_type == "postgresql":
                    cur.execute("""
                        SELECT total_stores, online_stores, offline_stores, online_percentage, report_time
                        FROM summary_reports
                        ORDER BY report_time DESC
                        LIMIT 1
                    """)
                    ls = cur.fetchone()
                    latest_summary = None
                    if ls:
                        latest_summary = {
                            "total_stores": ls[0],
                            "online_stores": ls[1],
                            "offline_stores": ls[2],
                            "online_percentage": ls[3],
                            "report_time": ls[4],
                        }
                else:
                    cur.execute("SELECT total_stores, online_stores, offline_stores, online_percentage, report_time FROM summary_reports ORDER BY report_time DESC LIMIT 1")
                    ls = cur.fetchone()
                    latest_summary = None
                    if ls:
                        # sqlite Row supports keys but we consume positionally for consistency
                        latest_summary = {
                            "total_stores": ls[0],
                            "online_stores": ls[1],
                            "offline_stores": ls[2],
                            "online_percentage": ls[3],
                            "report_time": ls[4],
                        }
                
                return {
                    "store_count": int(store_count),
                    "platforms": platforms,
                    "total_checks": int(total_checks),
                    "total_skus": int(total_skus),
                    "latest_summary": latest_summary,
                    "db_type": self.db_type,
                    "timezone": self.timezone,
                }
        except Exception as e:
            logger.error(f"❌ get_database_stats failed: {e}")
            return {
                "store_count": 0,
                "platforms": {},
                "total_checks": 0,
                "total_skus": 0,
                "latest_summary": None,
                "db_type": self.db_type,
                "timezone": self.timezone,
            }

    def get_stores_needing_attention(self) -> pd.DataFrame:
        """
        Return stores that currently look BLOCKED/UNKNOWN/ERROR today.
        - Postgres: uses DISTINCT ON (fast path)
        - SQLite: emulate with MAX(checked_at) subquery
        """
        try:
            if self.db_type == "postgresql":
                sql = """
                    SELECT 
                        s.id,
                        COALESCE(s.name_override, s.name) AS name,
                        s.url,
                        s.platform,
                        sc.is_online,
                        sc.checked_at,
                        sc.response_time_ms,
                        sc.error_message,
                        CASE 
                            WHEN sc.error_message LIKE '[BLOCKED]%%' THEN 'BLOCKED'
                            WHEN sc.error_message LIKE '[UNKNOWN]%%' THEN 'UNKNOWN'
                            WHEN sc.error_message LIKE '[ERROR]%%'  THEN 'ERROR'
                            WHEN sc.is_online = false AND sc.error_message IS NOT NULL THEN 'NEEDS_CHECK'
                            ELSE 'OK'
                        END AS problem_status
                    FROM stores s
                    JOIN (
                        SELECT DISTINCT ON (store_id)
                               store_id, is_online, checked_at, response_time_ms, error_message
                        FROM status_checks
                        ORDER BY store_id, checked_at DESC
                    ) sc ON s.id = sc.store_id
                    WHERE sc.checked_at >= NOW() - INTERVAL '24 hours'
                      AND  (
                        sc.error_message LIKE '[BLOCKED]%%' OR
                        sc.error_message LIKE '[UNKNOWN]%%' OR
                        sc.error_message LIKE '[ERROR]%%'
                    )
                    AND sc.checked_at >= NOW() - INTERVAL '24 hours' 
                    ORDER BY sc.checked_at DESC
                """
                return pd.read_sql_query(text(sql), self._ensure_sa(), params={"tz": self.timezone})
            else:
                # SQLite: emulate "latest per store" then filter
                offset = "+8 hours"
                sql = f"""
                    WITH latest AS (
                        SELECT sc1.*
                        FROM status_checks sc1
                        JOIN (
                            SELECT store_id, MAX(checked_at) AS max_ts
                            FROM status_checks
                            GROUP BY store_id
                        ) x ON x.store_id = sc1.store_id AND x.max_ts = sc1.checked_at
                    )
                    SELECT 
                        s.id,
                        COALESCE(s.name_override, s.name) AS name,
                        s.url,
                        s.platform,
                        l.is_online,
                        l.checked_at,
                        l.response_time_ms,
                        l.error_message,
                        CASE 
                            WHEN l.error_message LIKE '[BLOCKED]%%' THEN 'BLOCKED'
                            WHEN l.error_message LIKE '[UNKNOWN]%%' THEN 'UNKNOWN'
                            WHEN l.error_message LIKE '[ERROR]%%'  THEN 'ERROR'
                            WHEN l.is_online = 0 AND l.error_message IS NOT NULL THEN 'NEEDS_CHECK'
                            ELSE 'OK'
                        END AS problem_status
                    FROM stores s
                    JOIN latest l ON s.id = l.store_id
                    WHERE (
                        l.error_message LIKE '[BLOCKED]%%' OR
                        l.error_message LIKE '[UNKNOWN]%%' OR
                        l.error_message LIKE '[ERROR]%%'
                    )
                    AND DATE(l.checked_at, '{offset}') = DATE('now', '{offset}')
                    ORDER BY l.checked_at DESC
                """
                return pd.read_sql_query(text(sql), self._ensure_sa())
        except Exception as e:
            logger.error(f"❌ get_stores_needing_attention failed: {e}")
            return pd.DataFrame()

    def set_store_name_override(self, store_id: int, new_name: str, set_by: str) -> bool:
        """Set custom name for a store (records last_manual_check)."""
        try:
            with self.get_connection() as conn:
                cur = conn.cursor()
                if self.db_type == "postgresql":
                    cur.execute("""
                        UPDATE stores
                        SET name_override = %s,
                            last_manual_check = CURRENT_TIMESTAMP
                        WHERE id = %s
                    """, (new_name, store_id))
                else:
                    cur.execute("""
                        UPDATE stores
                        SET name_override = ?,
                            last_manual_check = CURRENT_TIMESTAMP
                        WHERE id = ?
                    """, (new_name, store_id))
                conn.commit()
                logger.info(f"✅ Store name updated: store_id={store_id}, name='{new_name}', by={set_by}")
                return True
        except Exception as e:
            logger.error(f"❌ set_store_name_override failed: {e}")
            return False

    # ---------- Hourly upserts (unchanged logic) ----------
        
    def ensure_schema(self) -> None:
        self._create_tables()

    def upsert_store_status_hourly(self, *, effective_at, platform, store_id, status,
                                   confidence, response_ms, evidence, probe_time, run_id) -> None:
        for attempt in range(self.max_retries):
            try:
                with self.get_connection() as conn:
                    cur = conn.cursor()
                    if self.db_type == "postgresql":
                        cur.execute("""
                            INSERT INTO store_status_hourly
                              (effective_at, platform, store_id, status, confidence, response_ms, evidence, probe_time, run_id)
                            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                            ON CONFLICT (platform, store_id, effective_at)
                            DO UPDATE SET
                              status      = EXCLUDED.status,
                              confidence  = EXCLUDED.confidence,
                              response_ms = EXCLUDED.response_ms,
                              evidence    = EXCLUDED.evidence,
                              probe_time  = EXCLUDED.probe_time,
                              run_id      = EXCLUDED.run_id
                            WHERE store_status_hourly.probe_time <= EXCLUDED.probe_time
                        """, (effective_at, platform, store_id, status, confidence, response_ms, evidence, probe_time, str(run_id)))
                    else:
                        cur.execute("""
                            INSERT INTO store_status_hourly
                              (effective_at, platform, store_id, status, confidence, response_ms, evidence, probe_time, run_id)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                            ON CONFLICT(platform, store_id, effective_at) DO UPDATE SET
                              status      = EXCLUDED.status,
                              confidence  = EXCLUDED.confidence,
                              response_ms = EXCLUDED.response_ms,
                              evidence    = EXCLUDED.evidence,
                              probe_time  = EXCLUDED.probe_time,
                              run_id      = EXCLUDED.run_id
                            WHERE store_status_hourly.probe_time <= EXCLUDED.probe_time
                        """, (str(effective_at), platform, store_id, status, float(confidence),
                              response_ms, evidence, str(probe_time), str(run_id)))
                    conn.commit()
                    return
            except Exception as e:
                logger.error(f"❌ upsert_store_status_hourly failed (attempt {attempt+1}): {e}")
                if attempt < self.max_retries - 1:
                    time.sleep(self.retry_delay)
                else:
                    raise

    def upsert_status_summary_hourly(self, *, effective_at, total, online, offline, blocked, errors, unknown, last_probe_at) -> None:
        for attempt in range(self.max_retries):
            try:
                with self.get_connection() as conn:
                    cur = conn.cursor()
                    if self.db_type == "postgresql":
                        cur.execute("""
                            INSERT INTO status_summary_hourly
                              (effective_at, total, online, offline, blocked, errors, unknown, last_probe_at)
                            VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                            ON CONFLICT (effective_at) DO UPDATE SET
                              total=EXCLUDED.total, online=EXCLUDED.online, offline=EXCLUDED.offline,
                              blocked=EXCLUDED.blocked, errors=EXCLUDED.errors, unknown=EXCLUDED.unknown,
                              last_probe_at=EXCLUDED.last_probe_at
                        """, (effective_at, total, online, offline, blocked, errors, unknown, last_probe_at))
                    else:
                        cur.execute("""
                            INSERT INTO status_summary_hourly
                              (effective_at, total, online, offline, blocked, errors, unknown, last_probe_at)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                            ON CONFLICT(effective_at) DO UPDATE SET
                              total=EXCLUDED.total, online=EXCLUDED.online, offline=EXCLUDED.offline,
                              blocked=EXCLUDED.blocked, errors=EXCLUDED.errors, unknown=EXCLUDED.unknown,
                              last_probe_at=EXCLUDED.last_probe_at
                        """, (str(effective_at), total, online, offline, blocked, errors, unknown, str(last_probe_at)))
                    conn.commit()
                    return
            except Exception as e:
                logger.error(f"❌ upsert_status_summary_hourly failed (attempt {attempt+1}): {e}")
                if attempt < self.max_retries - 1:
                    time.sleep(self.retry_delay)
                else:
                    raise

    def close(self):
        if self.connection_pool:
            try:
                self.connection_pool.closeall()
                logger.info("✅ Connection pool closed")
            except Exception as e:
                logger.error(f"❌ Error closing connection pool: {e}")

# Global instance
db = DatabaseManager()