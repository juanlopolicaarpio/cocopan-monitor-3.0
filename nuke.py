#!/usr/bin/env python3
"""
⚠️  DANGER: This script irreversibly deletes ALL data & objects in the `public` schema.
It connects using your existing database helper: `from database import db`.

What it does:
  1) Detect extensions installed in `public`
  2) DROP SCHEMA public CASCADE
  3) CREATE SCHEMA public (owned by current user)
  4) Recreate previously detected extensions in `public`

Usage:
  python nuke_database.py           # prompts "type NUKE"
  python nuke_database.py --yes     # no prompt
  NUKE_YES=1 python nuke_database.py

"""

import os
import sys
import logging
from typing import List

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
log = logging.getLogger("nuke")

try:
    from database import db  # your existing connection helper
except Exception as e:
    log.error("Failed to import database.db helper: %s", e)
    sys.exit(1)


def _get_db_identity(cur):
    cur.execute("SELECT current_database(), current_user, version()")
    dbname, user, version = cur.fetchone()
    return dbname, user, version


def _get_public_extensions(cur) -> List[str]:
    """
    Return list of extensions that are installed with schema = 'public'
    so we can recreate them after recreating the schema.
    """
    cur.execute("""
        SELECT e.extname
        FROM pg_extension e
        JOIN pg_namespace n ON n.oid = e.extnamespace
        WHERE n.nspname = 'public'
        ORDER BY e.extname
    """)
    return [r[0] for r in cur.fetchall()]


def nuke_public_schema():
    """
    Drop and recreate the public schema, restoring any extensions that were in public.
    """
    with db.get_connection() as conn:
        cur = conn.cursor()

        # Identify db/user/version for extra sanity
        dbname, user, version = _get_db_identity(cur)
        log.info("Connected to database: %s (user=%s)", dbname, user)
        log.info("Postgres version: %s", version.splitlines()[0])

        # Capture extensions living in public before we drop it
        public_exts = _get_public_extensions(cur)
        if public_exts:
            log.info("Extensions in public to recreate: %s", ", ".join(public_exts))
        else:
            log.info("No extensions found in public schema.")

        log.warning("About to DROP SCHEMA public CASCADE on database %s", dbname)

        # Do the deed
        try:
            cur.execute("BEGIN;")
            # Relax constraints so we can drop faster (optional but helpful)
            cur.execute("SET session_replication_role = replica;")

            cur.execute("DROP SCHEMA IF EXISTS public CASCADE;")
            cur.execute("CREATE SCHEMA public AUTHORIZATION CURRENT_USER;")

            # reasonable grants
            cur.execute("GRANT ALL ON SCHEMA public TO CURRENT_USER;")
            cur.execute("GRANT ALL ON SCHEMA public TO public;")

            # Recreate any extensions that were previously in public
            for ext in public_exts:
                cur.execute(f"CREATE EXTENSION IF NOT EXISTS {ext} WITH SCHEMA public;")

            cur.execute("RESET session_replication_role;")
            cur.execute("COMMIT;")
            log.info("✅ public schema dropped and recreated successfully.")

        except Exception as e:
            log.error("❌ Failed while nuking schema: %s", e)
            cur.execute("ROLLBACK;")
            raise

        # Small sanity check: list tables should be empty now
        cur.execute("""
            SELECT table_name FROM information_schema.tables
            WHERE table_schema='public' AND table_type='BASE TABLE'
            ORDER BY table_name
        """)
        tables = [r[0] for r in cur.fetchall()]
        if tables:
            log.warning("Schema recreated but tables still found: %s", ", ".join(tables))
        else:
            log.info("No tables remain in public (clean slate).")


def confirm_or_exit():
    """Interactive confirmation unless --yes flag or NUKE_YES=1 is present."""
    if "--yes" in sys.argv or os.getenv("NUKE_YES") == "1":
        return
    print("\n*** DANGER ***")
    print("This will ERASE EVERYTHING in the 'public' schema (all tables, views, data, etc.).")
    print("Type NUKE to continue:")
    resp = input("> ").strip()
    if resp != "NUKE":
        print("Aborted.")
        sys.exit(1)


def main():
    confirm_or_exit()
    try:
        nuke_public_schema()
        print("\nDONE. Database is now reset (clean `public` schema).")
    except Exception as e:
        log.error("Nuke failed: %s", e)
        sys.exit(1)


if __name__ == "__main__":
    main()
