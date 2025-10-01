#!/usr/bin/env python3
"""
Fetch and print all rows from the 'stores' table
"""

import psycopg2
import sqlite3
import os
import logging
from config import config   # reuse your config.py to get DATABASE_URL and flags

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("fetch_stores")

def fetch_all_stores():
    if config.USE_SQLITE:
        # --- SQLite ---
        sqlite_path = getattr(config, "SQLITE_PATH", "store_status.db")
        conn = sqlite3.connect(sqlite_path)
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        cur.execute("SELECT * FROM stores")
        rows = cur.fetchall()
        for row in rows:
            print(dict(row))
        conn.close()
    else:
        # --- PostgreSQL ---
        db_url = config.get_database_url()
        conn = psycopg2.connect(db_url)
        cur = conn.cursor()
        cur.execute("SELECT * FROM stores")
        rows = cur.fetchall()
        colnames = [desc[0] for desc in cur.description]
        for row in rows:
            record = dict(zip(colnames, row))
            print(record)
        cur.close()
        conn.close()

if __name__ == "__main__":
    fetch_all_stores()
