#!/usr/bin/env python3
"""
CocoPan Database Module (Railway-ready) — WITH ADMIN HELPERS
- No hard-coded DB URL; always uses config.get_database_url()
- psycopg2 ThreadedConnectionPool for writes/updates
- SQLAlchemy Engine for all pandas reads (fixes pandas warning)
- TCP keepalives + pool_pre_ping + pool_recycle to auto-heal EOF/peer resets
- Keeps your hourly upserts & admin helpers (get_database_stats, get_stores_needing_attention, set_store_name_override)
"""
import os
import time
import logging
from contextlib import contextmanager
from typing import Optional, Dict, Any

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
            else:
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
                    WHERE (
                        sc.error_message LIKE '[BLOCKED]%%' OR
                        sc.error_message LIKE '[UNKNOWN]%%' OR
                        sc.error_message LIKE '[ERROR]%%'
                    )
                    AND DATE(sc.checked_at AT TIME ZONE :tz) = CURRENT_DATE
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
