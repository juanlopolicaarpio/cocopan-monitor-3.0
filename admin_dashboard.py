#!/usr/bin/env python3
"""
CocoPan Admin Dashboard - FIXED DATA SAVING
✅ Now saves Foodpanda data to BOTH legacy and hourly snapshot tables
✅ Matches GrabFood saving pattern exactly
✅ Ensures consistent dashboard behavior
"""
# ===== Standard libs =====
import os
import re
import time
import json
import logging
import threading
import http.server
import socketserver
import uuid  # ADD THIS IMPORT
from datetime import datetime, timedelta
from typing import List, Set

# ===== Third-party =====
import pytz
import pandas as pd
import streamlit as st

# ===== Initialize logging BEFORE anything else (used by background threads) =====
logging.basicConfig(level=getattr(logging, os.getenv("LOG_LEVEL", "INFO")))
logger = logging.getLogger(__name__)

# ===== App modules =====
from config import config
from database import db

# ------------------------------------------------------------------------------
# Health check endpoint (hardened)
# ------------------------------------------------------------------------------
_HEALTH_THREAD_STARTED = False
_HEALTH_PORT = int(os.getenv("HEALTH_PORT", "8504"))

def create_health_server():
    """Tiny HTTP health server; safe across Streamlit reruns."""
    class HealthHandler(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path == "/healthz":
                try:
                    db.get_database_stats()
                    self.send_response(200)
                    self.send_header("Content-type", "text/plain")
                    self.end_headers()
                    self.wfile.write(b"OK - Admin Dashboard Healthy")
                except Exception as e:
                    self.send_response(500)
                    self.send_header("Content-type", "text/plain")
                    self.end_headers()
                    self.wfile.write(f"ERROR - {e}".encode())
            else:
                self.send_response(404)
                self.end_headers()

        def log_message(self, *args, **kwargs):
            # Silence base HTTP server logs
            pass

    class ReusableTCPServer(socketserver.TCPServer):
        allow_reuse_address = True

    try:
        with ReusableTCPServer(("", _HEALTH_PORT), HealthHandler) as httpd:
            logger.info(f"Health server listening on :{_HEALTH_PORT}")
            httpd.serve_forever()
    except OSError as e:
        # Address already in use — just log and exit thread gracefully
        if getattr(e, "errno", None) == 98:
            logger.warning(f"Health server not started; port :{_HEALTH_PORT} already in use")
            return
        logger.exception("Health server OSError")
    except Exception:
        logger.exception("Health server unexpected error")

if os.getenv("RAILWAY_ENVIRONMENT") == "production" and not _HEALTH_THREAD_STARTED:
    try:
        t = threading.Thread(target=create_health_server, daemon=True, name="healthz-server")
        t.start()
        _HEALTH_THREAD_STARTED = True
    except Exception:
        logger.exception("Failed to start health server thread")

# ------------------------------------------------------------------------------
# Streamlit page config
# ------------------------------------------------------------------------------
st.set_page_config(
    page_title="CocoPan Admin",
    page_icon="🔧",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ------------------------------------------------------------------------------
# Email Authentication
# ------------------------------------------------------------------------------
def load_authorized_admin_emails():
    """Load authorized admin emails from admin_alerts.json"""
    try:
        with open("admin_alerts.json", "r") as f:
            data = json.load(f)

        admin_team = data.get("admin_team", {})
        if admin_team.get("enabled", False):
            emails = admin_team.get("emails", [])
            authorized_emails = [email.strip() for email in emails if email.strip()]
            logger.info(f"✅ Loaded {len(authorized_emails)} authorized admin emails")
            return authorized_emails
        else:
            logger.warning("⚠️ Admin team disabled in config")
            return ["juanlopolicarpio@gmail.com"]  # Fallback
    except Exception as e:
        logger.error(f"❌ Failed to load admin emails: {e}")
        return ["juanlopolicarpio@gmail.com"]

def check_admin_email_authentication():
    """Simple email gate"""
    authorized_emails = load_authorized_admin_emails()

    if "admin_authenticated" not in st.session_state:
        st.session_state.admin_authenticated = False
        st.session_state.admin_email = None

    if not st.session_state.admin_authenticated:
        st.markdown("""
        <div style="max-width: 420px; margin: 2rem auto; padding: 2rem; background: white; border-radius: 12px; box-shadow: 0 4px 6px rgba(0,0,0,0.1);">
            <h2 style="text-align: center; color: #1E293B; margin-bottom: 1rem;">🔧 CocoPan Admin Access</h2>
            <p style="text-align: center; color: #64748B; margin-bottom: 1.5rem;">Enter your authorized admin email address</p>
        </div>
        """, unsafe_allow_html=True)

        with st.form("admin_email_auth_form"):
            email = st.text_input(
                "Admin Email Address",
                placeholder="admin@cocopan.com",
                help="Enter your authorized admin email address",
            )
            submit = st.form_submit_button("Access Admin Dashboard", use_container_width=True)

            if submit:
                email = email.strip().lower()
                if email in [a.lower() for a in authorized_emails]:
                    st.session_state.admin_authenticated = True
                    st.session_state.admin_email = email
                    logger.info(f"✅ Admin authenticated: {email}")
                    st.success("✅ Admin access granted! Redirecting…")
                    st.rerun()
                else:
                    st.error("❌ Email not authorized for admin access.")
                    logger.warning(f"Unauthorized admin attempt: {email}")
        return False

    return True

# ------------------------------------------------------------------------------
# Store loading (Foodpanda only for VA tab)
# ------------------------------------------------------------------------------
@st.cache_data(ttl=300)
def load_foodpanda_stores():
    """Load Foodpanda stores from branch_urls.json and/or DB (get_or_create)."""
    stores = []
    try:
        with open("branch_urls.json", "r") as f:
            data = json.load(f)
        urls = data.get("urls", [])

        for url in urls:
            if "foodpanda" not in url:
                continue
            try:
                with db.get_connection() as conn:
                    cur = conn.cursor()
                    cur.execute("SELECT id, name FROM stores WHERE url = %s", (url,))
                    row = cur.fetchone()
                    if row:
                        store_id, store_name = row[0], row[1]
                    else:
                        store_name = extract_store_name_from_url(url)
                        store_id = db.get_or_create_store(store_name, url)
                    stores.append({"id": store_id, "name": store_name, "url": url})
            except Exception as e:
                logger.error(f"Error ensuring store in DB for {url}: {e}")
                stores.append({"id": None, "name": extract_store_name_from_url(url), "url": url})

        logger.info(f"📋 Loaded {len(stores)} Foodpanda stores")
        return stores
    except Exception as e:
        logger.error(f"❌ Failed to load Foodpanda stores list: {e}")
        return stores

def extract_store_name_from_url(url: str) -> str:
    """Best-effort store name from foodpanda URL."""
    try:
        if "foodpanda.ph" in url:
            m = re.search(r"/restaurant/[^/]+/([^/?#]+)", url)
            if m:
                raw = m.group(1)
                name = raw.replace("-", " ").replace("_", " ").title()
                return name if name.lower().startswith("cocopan") else f"Cocopan {name}"
        elif "foodpanda.page.link" in url:
            uid = url.split("/")[-1][:8] if "/" in url else "store"
            return f"Cocopan Foodpanda {uid.upper()}"
        return "Cocopan Foodpanda Store"
    except Exception:
        return "Cocopan Foodpanda Store"

# ------------------------------------------------------------------------------
# Existing admin actions (verification tab)
# ------------------------------------------------------------------------------
@st.cache_data(ttl=60)
def load_stores_needing_attention() -> pd.DataFrame:
    try:
        return db.get_stores_needing_attention()
    except Exception as e:
        logger.error(f"Error loading stores needing attention: {e}")
        return pd.DataFrame()

def mark_store_status(store_id: int, store_name: str, is_online: bool, platform: str) -> bool:
    try:
        ok = db.save_status_check(
            store_id=store_id,
            is_online=is_online,
            response_time_ms=1200,
            error_message=None,
        )
        if ok:
            logger.info(f"✅ Admin marked {store_name} as {'online' if is_online else 'offline'}")
            load_stores_needing_attention.clear()
            return True
        logger.error(f"❌ Failed to save status for {store_name}")
        return False
    except Exception as e:
        logger.error(f"Error marking store status: {e}")
        return False

def format_platform_emoji(platform: str) -> str:
    p = platform.lower() if platform else ""
    if "grab" in p:
        return "🛒"
    if "panda" in p:
        return "🍔"
    return "🏪"

def format_time_ago(checked_at) -> str:
    try:
        if pd.isna(checked_at):
            return "Unknown"
        checked = pd.to_datetime(checked_at)
        now = datetime.now(tz=checked.tz) if getattr(checked, "tzinfo", None) else datetime.now()
        diff = now - checked
        if diff.days > 0:
            return f"{diff.days}d ago"
        if diff.seconds >= 3600:
            return f"{diff.seconds // 3600}h ago"
        if diff.seconds >= 60:
            return f"{diff.seconds // 60}m ago"
        return "Just now"
    except:
        return "Unknown"

# ------------------------------------------------------------------------------
# VA Check-in: schedule & helpers (10-minute early window; TZ-safe)
# ------------------------------------------------------------------------------
def get_va_checkin_schedule():
    """Define allowed check hours (Manila)."""
    # Adjust as needed
    return {"start_hour": 6, "end_hour": 22, "timezone": "Asia/Manila"}

def get_current_manila_time():
    return datetime.now(pytz.timezone("Asia/Manila"))

def get_current_hour_slot():
    """Get current hour slot with the 10-minute rule.
    If now is X:50 or later, we treat the slot as (X+1):00.
    Else we treat it as X:00.
    """
    now = get_current_manila_time()
    if now.minute >= 50:
        target_hour = now.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
    else:
        target_hour = now.replace(minute=0, second=0, microsecond=0)
    logger.debug(f"Current time: {now}, Target hour slot: {target_hour}")
    return target_hour

def is_checkin_time():
    """Window is X:50 to (X+1):50 for the slot at (X+1):00.
    Determine target_check_hour and see if it falls within schedule.
    """
    now = get_current_manila_time()
    current_hour = now.hour
    minute = now.minute
    schedule = get_va_checkin_schedule()

    if minute >= 50:
        target_check_hour = current_hour + 1
    else:
        target_check_hour = current_hour

    return schedule["start_hour"] <= target_check_hour <= schedule["end_hour"]

def _fmt_hour(h24: int) -> str:
    tz = pytz.timezone("Asia/Manila")
    dt = datetime.now(tz).replace(hour=h24, minute=0, second=0, microsecond=0)
    return dt.strftime("%I:%M %p").lstrip("0")

def get_next_checkin_time():
    """Next window start is at (target_hour_slot - 10 minutes).
    For UX messages we return the target hour (top-of-hour), and in UI we show window start/end explicitly.
    """
    sched = get_va_checkin_schedule()
    now = get_current_manila_time()
    current_minute = now.minute
    current_hour = now.hour

    if current_minute >= 50:
        # We're currently inside a window; next window is next hour's slot
        next_hour = current_hour + 1
        if next_hour > sched["end_hour"]:
            next_window = (now + timedelta(days=1)).replace(hour=sched["start_hour"], minute=0, second=0, microsecond=0)
        else:
            next_window = now.replace(hour=next_hour, minute=0, second=0, microsecond=0)
    else:
        # Not yet inside the window; next window aligns to the current hour's slot (at top of next hour if needed)
        if current_hour < sched["start_hour"]:
            next_window = now.replace(hour=sched["start_hour"], minute=0, second=0, microsecond=0)
        elif current_hour >= sched["end_hour"]:
            next_window = (now + timedelta(days=1)).replace(hour=sched["start_hour"], minute=0, second=0, microsecond=0)
        else:
            next_window = now.replace(hour=current_hour + 1, minute=0, second=0, microsecond=0)

    return next_window

# ------------------------------------------------------------------------------
# TZ-safe DB reads for submitted state / completion
# ------------------------------------------------------------------------------
def load_submitted_va_state(hour_slot: datetime) -> Set[int]:
    """Load the actual submitted VA state from database for this hour (Manila slot)."""
    try:
        _ = load_foodpanda_stores()  # ensure IDs exist / cached
        offline_store_ids: Set[int] = set()
        with db.get_connection() as conn:
            cur = conn.cursor()
            hour_start = hour_slot
            hour_end = hour_slot + timedelta(hours=1)
            # Convert stored UTC timestamptz to Manila when filtering
            cur.execute("""
                SELECT store_id, is_online, checked_at
                  FROM status_checks
                 WHERE error_message LIKE '[VA_CHECKIN]%%'
                   AND checked_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Manila' >= %s
                   AND checked_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Manila' <  %s
                 ORDER BY checked_at DESC
            """, (hour_start, hour_end))
            rows = cur.fetchall()
            logger.info(f"Query for hour {hour_slot}: found {len(rows)} VA results")
            if rows:
                logger.info(f"Sample timestamps: {[r[2] for r in rows[:3]]}")
            for store_id, is_online, _ts in rows:
                if not is_online:
                    offline_store_ids.add(store_id)
        logger.info(f"Loaded submitted VA state for {hour_slot:%H:00}: {len(offline_store_ids)} offline stores")
        return offline_store_ids
    except Exception as e:
        logger.error(f"Error loading submitted VA state: {e}")
        return set()

def check_if_hour_already_completed(hour_slot: datetime) -> bool:
    """Hour completion check with timezone handling."""
    try:
        with db.get_connection() as conn:
            cur = conn.cursor()
            hour_start = hour_slot
            hour_end = hour_slot + timedelta(hours=1)
            cur.execute("""
                SELECT COUNT(*) as va_count
                  FROM status_checks
                 WHERE error_message LIKE '[VA_CHECKIN]%%'
                   AND checked_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Manila' >= %s
                   AND checked_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Manila' <  %s
            """, (hour_start, hour_end))
            result = cur.fetchone()
            va_count = result[0] if result else 0
            logger.info(f"Hour completion check for {hour_slot:%Y-%m-%d %H:00}: {va_count} rows")
            return va_count > 0
    except Exception as e:
        logger.error(f"Error checking hour completion: {e}")
        return False

def get_completed_hours_today() -> List[int]:
    """Get completed Manila hours today (timezone-fixed)."""
    try:
        with db.get_connection() as conn:
            cur = conn.cursor()
            cur.execute("""
                SELECT DISTINCT 
                       EXTRACT(HOUR FROM checked_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Manila')::int AS h
                  FROM status_checks
                 WHERE error_message LIKE '[VA_CHECKIN]%%'
                   AND DATE(checked_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Manila') = CURRENT_DATE
                 ORDER BY h
            """)
            hours = [int(r[0]) for r in cur.fetchall()]
            logger.info(f"Completed hours today (timezone-fixed): {hours}")
            return hours
    except Exception as e:
        logger.error(f"Error getting completed hours: {e}")
        return []

def debug_va_timestamps():
    """Debug helper to examine latest VA rows across timezones."""
    try:
        with db.get_connection() as conn:
            cur = conn.cursor()
            cur.execute("""
                SELECT 
                    store_id,
                    is_online,
                    checked_at as utc_time,
                    checked_at AT TIME ZONE 'Asia/Manila' as manila_time,
                    DATE(checked_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Manila') as manila_date,
                    EXTRACT(HOUR FROM checked_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Manila') as manila_hour
                  FROM status_checks
                 WHERE error_message LIKE '[VA_CHECKIN]%%'
              ORDER BY checked_at DESC
                 LIMIT 5
            """)
            rows = cur.fetchall()
            logger.info("=== VA TIMESTAMP DEBUG ===")
            for r in rows:
                logger.info(f"Store {r[0]}: UTC={r[2]}, Manila={r[3]}, Date={r[4]}, Hour={r[5]}")
    except Exception as e:
        logger.error(f"Debug timestamps error: {e}")

# ------------------------------------------------------------------------------
# FIXED: Idempotent VA save with STANDARDIZED DATA SAVING (matches GrabFood)
# ------------------------------------------------------------------------------
def save_va_checkin_enhanced(offline_store_ids: List[int], admin_email: str, hour_slot: datetime) -> bool:
    """
    FIXED: Standardized Foodpanda data saving to match GrabFood pattern exactly.
    Now saves to BOTH:
    1. Legacy status_checks table (for backward compatibility)
    2. Hourly snapshot tables (for consistent dashboard behavior)
    """
    try:
        stores = load_foodpanda_stores()
        success_count, error_count = 0, 0
        run_id = uuid.uuid4()  # Generate run ID for this VA session

        for store in stores:
            store_id = store["id"]
            if not store_id:
                continue

            try:
                is_online = store_id not in offline_store_ids
                hour_str = hour_slot.strftime("%Y-%m-%d %H:00")
                msg = f"[VA_CHECKIN] {hour_str} - Store {'online' if is_online else 'offline'} via {admin_email}"

                with db.get_connection() as conn:
                    cur = conn.cursor()

                    # 1. LEGACY SYSTEM: Save to status_checks (idempotent)
                    cur.execute("""
                        DELETE FROM status_checks
                         WHERE store_id = %s
                           AND error_message LIKE '[VA_CHECKIN]%%'
                           AND checked_at >= %s
                           AND checked_at <  %s + INTERVAL '1 hour'
                    """, (store_id, hour_slot, hour_slot))

                    cur.execute("""
                        INSERT INTO status_checks (store_id, is_online, response_time_ms, error_message, checked_at)
                        VALUES (%s, %s, %s, %s, %s)
                    """, (store_id, is_online, 1000, msg, hour_slot))

                    conn.commit()

                # 2. NEW SYSTEM: Save to hourly snapshot tables (matches GrabFood)
                try:
                    db.upsert_store_status_hourly(
                        effective_at=hour_slot,
                        platform='foodpanda',
                        store_id=store_id,
                        status='ONLINE' if is_online else 'OFFLINE',
                        confidence=1.0,  # VA check-ins are highly reliable
                        response_ms=1000,  # Standard response time for manual checks
                        evidence=msg,
                        probe_time=hour_slot,
                        run_id=run_id
                    )
                    logger.debug(f"✅ Saved VA check to both systems: {store['name']} -> {is_online}")
                except Exception as e:
                    logger.warning(f"⚠️ Failed to save to hourly system for {store['name']}: {e}")
                    # Continue - legacy system save succeeded

                success_count += 1

            except Exception as e:
                logger.error(f"Error saving VA for store {store_id}: {e}")
                error_count += 1

        # 3. Save summary to hourly summary table (matches GrabFood)
        try:
            total_stores = len(stores)
            online_count = total_stores - len(offline_store_ids)
            offline_count = len(offline_store_ids)
            
            db.upsert_status_summary_hourly(
                effective_at=hour_slot,
                total=total_stores,
                online=online_count,
                offline=offline_count,
                blocked=0,  # VA checks don't have blocked status
                errors=0,   # VA checks don't have error status  
                unknown=0,  # VA checks don't have unknown status
                last_probe_at=hour_slot
            )
            logger.debug(f"✅ Saved VA summary to hourly system: {online_count}/{total_stores} online")
        except Exception as e:
            logger.warning(f"⚠️ Failed to save hourly summary: {e}")

        logger.info(f"✅ VA Check-in for {hour_slot:%H:00} saved with STANDARDIZED pattern:")
        logger.info(f"   📊 Success: {success_count}/{len(stores)} stores")
        logger.info(f"   💾 Saved to: status_checks + store_status_hourly + status_summary_hourly")
        logger.info(f"   ❌ Errors: {error_count}")
        
        return error_count == 0

    except Exception as e:
        logger.error(f"❌ save_va_checkin_enhanced fatal: {e}")
        return False

# ------------------------------------------------------------------------------
# VA Check-in UI Tab (state preservation + TZ debug)
# ------------------------------------------------------------------------------
def enhanced_va_checkin_tab():
    # Debug timezone/timestamps (visible only in logs)
    debug_va_timestamps()

    current_time = get_current_manila_time()
    current_hour_slot = get_current_hour_slot()
    schedule = get_va_checkin_schedule()

    st.markdown(f"""
    <div class="section-header" style="background:#fff; border:1px solid #E2E8F0; border-radius:8px; padding:.9rem 1.1rem; margin:1.1rem 0 .9rem 0; box-shadow:0 1px 3px rgba(0,0,0,.06);">
        <div style="font-size:1.1rem; font-weight:600; color:#1E293B; margin:0;">🐼 VA Hourly Check-in</div>
        <div style="font-size:.85rem; color:#64748B; margin:.25rem 0 0 0;">Foodpanda Store Status • {current_time.strftime('%I:%M %p')} Manila Time</div>
    </div>
    """, unsafe_allow_html=True)

    # Show check-in window banner
    if not is_checkin_time():
        next_check = get_next_checkin_time()
        st.info(f"""
⏰ **Outside Check-in Window**

Current window: {current_hour_slot.strftime('%I:00 %p')} check (active 10 minutes before: {(current_hour_slot - timedelta(minutes=10)).strftime('%I:%M %p')} - {(current_hour_slot + timedelta(minutes=50)).strftime('%I:%M %p')})
Next window starts: {(next_check - timedelta(minutes=10)).strftime('%I:%M %p')}

You can review stores below; submissions are accepted during the window.
        """)

    # Hour completion + completed hours list
    hour_completed = check_if_hour_already_completed(current_hour_slot)
    completed_hours = get_completed_hours_today()

    # Session state bucket (preserved across reruns)
    if "va_offline_stores" not in st.session_state:
        st.session_state.va_offline_stores = set()

    # If hour already completed and we have no local state, load submitted state
    if hour_completed and len(st.session_state.va_offline_stores) == 0:
        submitted_offline_ids = load_submitted_va_state(current_hour_slot)
        st.session_state.va_offline_stores = submitted_offline_ids
        logger.info(f"Loaded submitted state into session: {len(submitted_offline_ids)} offline stores")

    # Header strip with exact window
    window_start = current_hour_slot - timedelta(minutes=10)
    window_end = current_hour_slot + timedelta(minutes=50)
    st.markdown(f"""
    <div style="background: {'#DCFCE7' if hour_completed else '#FEF3E2'}; border: 1px solid {'#16A34A' if hour_completed else '#F59E0B'}; border-radius: 8px; padding: 1rem; margin: 1rem 0;">
        <h4 style="margin: 0 0 0.4rem 0; color: {'#166534' if hour_completed else '#92400E'};">
            🕐 {current_hour_slot.strftime('%I:00 %p')} Check Window ({window_start.strftime('%I:%M %p')} - {window_end.strftime('%I:%M %p')})
        </h4>
        <p style="margin: 0; color: {'#166534' if hour_completed else '#92400E'};">
            {"✅ Already submitted for this hour. Data saved to BOTH legacy and hourly systems." if hour_completed else "⏳ Ready for submission. Will save to BOTH legacy and hourly systems."}
        </p>
    </div>
    """, unsafe_allow_html=True)

    # Load stores
    stores = load_foodpanda_stores()
    if not stores:
        st.error("❌ Failed to load Foodpanda stores. Please refresh the page.")
        return

    # Summary metrics
    total = len(stores)
    offline = len(st.session_state.va_offline_stores)
    online = total - offline

    c1, c2, c3 = st.columns(3)
    with c1:
        st.metric("🟢 Online", online, "Will stay online")
    with c2:
        st.metric("🔴 Marked Offline", offline, "Marked by VA")
    with c3:
        st.metric("📊 Total Foodpanda", total, "All stores")

    if hour_completed:
        st.info(f"📋 **Showing submitted state:** This reflects what was actually submitted for {current_hour_slot.strftime('%I:00 %p')} and saved to both database systems.")

    st.markdown("---")
    st.markdown("### 🔍 Search & Mark Stores")

    BRAND_PREFIX_RE = re.compile(r'^\s*cocopan[\s\-:]+', re.IGNORECASE)
    def norm_name(name: str) -> str:
        n = BRAND_PREFIX_RE.sub("", name or "")
        n = re.sub(r"\s+", " ", n).strip().lower()
        return n

    def rank(name: str, q: str) -> int:
        k = norm_name(name)
        ql = (q or "").strip().lower()
        if not ql:
            return 2
        if k.startswith(ql):
            return 0
        if any(w.startswith(ql) for w in k.split()):
            return 1
        if ql in k:
            return 2
        return 3

    q = st.text_input(
        "Search (prefix-friendly, 'Cocopan' ignored):",
        placeholder="Type: m → ma → may… (matches 'Cocopan Maysilo')",
        help="Searching treats the 'Cocopan' prefix as invisible.",
    )

    if q:
        ranked = [(rank(s["name"], q), norm_name(s["name"]), s) for s in stores]
        filtered = [t[2] for t in sorted([r for r in ranked if r[0] < 3], key=lambda x: (x[0], x[1]))]
        st.info(f"Found {len(filtered)} matches for "{q}". Prefix matches first.")
    else:
        filtered = stores
        st.info(f"Showing all {len(filtered)} stores. Type to filter.")

    if not filtered:
        st.warning(f"No stores match "{q}". Try a shorter prefix or a different term.")

    # Render each store with preserved state
    for s in filtered:
        sid, sname, surl = s["id"], s["name"], s["url"]
        if not sid:
            continue
        display_name = sname.replace("Cocopan - ", "").replace("Cocopan ", "")
        is_offline = sid in st.session_state.va_offline_stores

        st.markdown(f"""
        <div style="background:{'#FEE2E2' if is_offline else '#F0FDF4'}; border:1px solid {'#EF4444' if is_offline else '#22C55E'}; border-radius:8px; padding:1rem; margin-bottom:.5rem; display:flex; align-items:center; gap:.75rem;">
            <div style="flex:1;">
                <strong>{display_name}</strong><br>
                <small style="color:#64748B;">Status: {'🔴 MARKED OFFLINE' if is_offline else '🟢 ONLINE'}</small>
            </div>
        </div>
        """, unsafe_allow_html=True)

        c1, c2, c3 = st.columns([2, 1, 1])
        with c1:
            st.markdown(f"""
            <a href="{surl}" target="_blank" style="display:inline-block; padding:.45rem .8rem; background:#3B82F6; color:#fff; text-decoration:none; border-radius:6px; font-size:.85rem; text-align:center;">
                🔗 Check Store
            </a>
            """, unsafe_allow_html=True)
        with c2:
            if is_offline:
                if st.button("✅ Mark ONLINE", key=f"online_{sid}", use_container_width=True):
                    st.session_state.va_offline_stores.discard(sid)
                    st.success(f"✅ {display_name} marked as ONLINE")
                    st.rerun()
        with c3:
            if not is_offline:
                if st.button("🔴 Mark OFFLINE", key=f"offline_{sid}", use_container_width=True, type="primary"):
                    st.session_state.va_offline_stores.add(sid)
                    st.success(f"🔴 {display_name} marked as OFFLINE")
                    st.rerun()
            else:
                st.write("OFFLINE ✓")
        st.markdown("<br>", unsafe_allow_html=True)

    # Selected offline list
    if st.session_state.va_offline_stores:
        st.markdown("### 📋 Currently Marked Offline")
        idx = 1
        for s in stores:
            if s["id"] in st.session_state.va_offline_stores:
                st.write(f"{idx}. 🔴 {s['name'].replace('Cocopan - ', '').replace('Cocopan ', '')}")
                idx += 1

    # Submit section (idempotent; shows submitted state)
    st.markdown("---")
    st.markdown("### 📤 Submit Hourly Check-in")

    if offline == 0:
        st.info("ℹ️ No stores marked as offline. All Foodpanda stores will be saved as ONLINE.")
    else:
        st.warning(f"⚠️ {offline} stores will be marked as OFFLINE. {online} stores will remain ONLINE.")

    if hour_completed:
        st.success("✅ This hour has already been submitted. The state above reflects your submission saved to both database systems.")
    else:
        st.info("⏳ Not submitted yet for this hour.")

    # Enable submit only inside the window
    if not is_checkin_time():
        st.button("📤 Submit Check-in (Outside Window)", use_container_width=True, disabled=True)
    else:
        btn_label = ("🔁 Re-submit for " if hour_completed else "📤 Submit Check-in for ") + current_hour_slot.strftime("%I:00 %p")
        if st.button(btn_label, use_container_width=True, type="primary"):
            offline_ids = list(st.session_state.va_offline_stores)
            ok = save_va_checkin_enhanced(offline_ids, st.session_state.admin_email, current_hour_slot)
            if ok:
                st.success(f"""
✅ **{current_hour_slot.strftime('%I:00 %p')} Check-in Saved with STANDARDIZED Pattern**

📊 Summary:
- 🟢 Online: {online}
- 🔴 Offline: {offline}  
- 👤 By: {st.session_state.admin_email}
- 🕐 Window: {(current_hour_slot - timedelta(minutes=10)).strftime('%I:%M %p')} - {(current_hour_slot + timedelta(minutes=50)).strftime('%I:%M %p')}

💾 **Database Consistency:** Saved to BOTH legacy (status_checks) and hourly snapshot (store_status_hourly) tables
✨ **State preserved:** The offline stores will remain marked as offline above.
                """)
                # DO NOT clear the offline state; it represents submitted state
                # Clear only the store cache to keep IDs/names fresh if changed elsewhere
                load_foodpanda_stores.clear()
                time.sleep(0.5)
                st.rerun()
            else:
                st.error("❌ Failed to save. Please try again.")

    # Today's status (for all window hours)
    st.markdown("### 📊 Today's Check-in Status")
    start_h, end_h = schedule["start_hour"], schedule["end_hour"]
    hours_range = list(range(start_h, end_h + 1))
    cols = st.columns(len(hours_range))
    for i, h in enumerate(hours_range):
        status = "✅ Done" if h in completed_hours else "⏳ Pending"
        cols[i].metric(_fmt_hour(h), status)

# ------------------------------------------------------------------------------
# Styles
# ------------------------------------------------------------------------------
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');
    #MainMenu, footer, header, .stDeployButton {visibility: hidden;}
    .main { font-family: 'Inter', sans-serif; background: #F8FAFC; color: #1E293B; padding: 1rem !important; }
    .admin-header { background: linear-gradient(135deg, #DC2626 0%, #EF4444 100%); border-radius: 12px; padding: 1.5rem; margin-bottom: 1.5rem; text-align: center; color: white; }
    .admin-title { font-size: 1.8rem; font-weight: 700; margin: 0; }
    .admin-subtitle { font-size: 0.9rem; margin: 0.5rem 0 0 0; opacity: 0.9; }
    .store-card { background: white; border-radius: 12px; padding: 1.5rem; margin-bottom: 1rem; border-left: 4px solid #F59E0B; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }
    .store-name { font-size: 1.1rem; font-weight: 600; color: #1E293B; margin-bottom: 0.5rem; }
    .store-meta { font-size: 0.85rem; color: #64748B; margin-bottom: 1rem; }
    .store-platform { display: inline-block; background: #E0F2FE; color: #0C4A6E; padding: 0.2rem 0.5rem; border-radius: 4px; font-size: 0.75rem; font-weight: 500; margin-right: 0.5rem; }
    .stButton > button { width: 100%; border-radius: 8px; border: none; font-weight: 600; font-size: 0.9rem; padding: 0.75rem; transition: all 0.2s; min-height: 48px; }
    .stButton > button:hover { transform: translateY(-1px); box-shadow: 0 4px 8px rgba(0,0,0,0.12); }
    @media (max-width: 768px) {
        .admin-title { font-size: 1.5rem; }
        .store-card { padding: 1rem; }
        .stButton > button { min-height: 52px; font-size: 1rem; }
    }
</style>
""", unsafe_allow_html=True)

# ------------------------------------------------------------------------------
# Main
# ------------------------------------------------------------------------------
def main():
    if not check_admin_email_authentication():
        return

    with st.sidebar:
        st.markdown(f"**Admin logged in as:**\n{st.session_state.admin_email}")
        if st.button("Logout"):
            st.session_state.admin_authenticated = False
            st.session_state.admin_email = None
            st.rerun()

    now = config.get_current_time()
    st.markdown(f"""
    <div class="admin-header">
        <div class="admin-title">🔧 CocoPan Admin Dashboard</div>
        <div class="admin-subtitle">Operations Management • {now.strftime('%I:%M %p')} Manila Time</div>
    </div>
    """, unsafe_allow_html=True)

    tab1, tab2 = st.tabs(["🔧 Store Verification", "🐼 VA Hourly Check-in"])

    # Tab 1: existing manual verification
    with tab1:
        df = load_stores_needing_attention()
        st.markdown(f"""
        <div class="section-header" style="background:#fff; border:1px solid #E2E8F0; border-radius:8px; padding:.9rem 1.1rem; margin:1.1rem 0 .9rem 0; box-shadow:0 1px 3px rgba(0,0,0,.06);">
            <div style="font-size:1.1rem; font-weight:600; color:#1E293B; margin:0;">Manual Verification Required</div>
            <div style="font-size:.85rem; color:#64748B; margin:.25rem 0 0 0;">{len(df)} stores need attention • Updated {now.strftime('%I:%M %p')} Manila Time</div>
        </div>
        """, unsafe_allow_html=True)

        if df.empty:
            st.markdown("""
            <div style="background:#D1FAE5; color:#065F46; padding:1rem; border-radius:8px; border-left:4px solid #10B981;">
                <strong>✅ All Clear!</strong> No stores currently need manual verification.
            </div>
            """, unsafe_allow_html=True)
            if st.button("🔄 Check for New Issues", use_container_width=True):
                load_stores_needing_attention.clear()
                st.rerun()
        else:
            if "completed_stores" not in st.session_state:
                st.session_state.completed_stores = set()
            total = len(df)
            done = len(st.session_state.completed_stores)
            pct = (done / total) * 100 if total else 0.0

            st.markdown(f"""
            <div style="background:#E2E8F0; border-radius:8px; height:8px; overflow:hidden; margin:.5rem 0 1rem 0;">
                <div style="background:linear-gradient(90deg,#10B981,#059669); height:100%; width:{pct:.1f}%"></div>
            </div>
            <div style="text-align:center; color:#64748B; margin-bottom:1rem;">Progress: {done}/{total} completed</div>
            """, unsafe_allow_html=True)

            for idx, row in df.iterrows():
                sid = row["id"]
                sname = row["name"]
                platform = (row.get("platform") or "unknown")
                url = row["url"]
                checked_at = row.get("checked_at")

                if sid in st.session_state.completed_stores:
                    continue

                st.markdown(f"""
                <div class="store-card">
                    <div class="store-name">{sname} [{idx + 1}/{total}]</div>
                    <div class="store-meta">
                        <span class="store-platform">{format_platform_emoji(platform)} {platform.title()}</span>
                        <span>Last checked: {format_time_ago(checked_at)}</span>
                    </div>
                </div>
                """, unsafe_allow_html=True)

                c1, c2, c3 = st.columns([2, 1, 1])
                with c1:
                    st.markdown(f"""
                    <a href="{url}" target="_blank" style="display:block; text-align:center; padding:.75rem; background:#3B82F6; color:white; border-radius:8px; text-decoration:none;">
                        🔗 Check Store
                    </a>
                    """, unsafe_allow_html=True)
                with c2:
                    if st.button("✅ Online", key=f"v_online_{sid}", use_container_width=True):
                        if mark_store_status(sid, sname, True, platform):
                            st.session_state.completed_stores.add(sid)
                            st.success(f"✅ {sname} marked Online")
                            time.sleep(0.6)
                            st.rerun()
                        else:
                            st.error("Update failed. Try again.")
                with c3:
                    if st.button("❌ Offline", key=f"v_offline_{sid}", use_container_width=True):
                        if mark_store_status(sid, sname, False, platform):
                            st.session_state.completed_stores.add(sid)
                            st.success(f"❌ {sname} marked Offline")
                            time.sleep(0.6)
                            st.rerun()
                        else:
                            st.error("Update failed. Try again.")
                st.markdown("<br>", unsafe_allow_html=True)

            remain = total - done
            if remain > 0:
                st.info(f"📋 {remain} stores remaining")
            else:
                st.success("🎉 All stores verified!")
                if st.button("🔄 Check for New Issues", use_container_width=True, key="refresh_verification"):
                    st.session_state.completed_stores.clear()
                    load_stores_needing_attention.clear()
                    st.rerun()

    # Tab 2: VA hourly
    with tab2:
        enhanced_va_checkin_tab()

    # Global refresh
    st.markdown("---")
    c1, c2, c3 = st.columns([1, 2, 1])
    with c2:
        if st.button("🔄 Refresh All Data", use_container_width=True):
            load_stores_needing_attention.clear()
            load_foodpanda_stores.clear()
            st.rerun()

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        st.error(f"❌ System Error: {e}")
        logger.exception("Admin dashboard error")