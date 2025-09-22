#!/usr/bin/env python3
"""
CocoPan Admin Dashboard - DOCUMENT 2 + SKU COMPLIANCE
✅ Keeps all Document 2 logic unchanged (PostgreSQL only)
✅ Adds SKU Compliance functionality as new features
✅ Store Verification (from Document 2)
✅ VA Hourly Check-in (from Document 2) 
✅ SKU Compliance Checker (new from Document 1)
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
import uuid
from datetime import datetime, timedelta
from typing import List, Set, Dict

# ===== Third-party =====
import pytz
import pandas as pd
import streamlit as st

# ===== Initialize logging =====
logging.basicConfig(level=getattr(logging, os.getenv("LOG_LEVEL", "INFO")))
logger = logging.getLogger(__name__)

# ===== App modules =====
from config import config
from database import db

# ------------------------------------------------------------------------------
# Health check endpoint (hardened) - FROM DOCUMENT 2
# ------------------------------------------------------------------------------
_HEALTH_THREAD_STARTED = False
_HEALTH_PORT = int(os.getenv("HEALTH_PORT", "8504"))

def create_health_server():
    class HealthHandler(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path == "/healthz":
                try:
                    db.get_database_stats()
                    self.send_response(200); self.send_header("Content-type", "text/plain"); self.end_headers()
                    self.wfile.write(b"OK - Admin Dashboard Healthy")
                except Exception as e:
                    self.send_response(500); self.send_header("Content-type", "text/plain"); self.end_headers()
                    self.wfile.write(f"ERROR - {e}".encode())
            else:
                self.send_response(404); self.end_headers()
        def log_message(self, *args, **kwargs): pass

    class ReusableTCPServer(socketserver.TCPServer):
        allow_reuse_address = True

    try:
        with ReusableTCPServer(("", _HEALTH_PORT), HealthHandler) as httpd:
            logger.info(f"Health server listening on :{_HEALTH_PORT}")
            httpd.serve_forever()
    except OSError as e:
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
# Streamlit page config - FROM DOCUMENT 2
# ------------------------------------------------------------------------------
st.set_page_config(
    page_title="CocoPan Admin",
    page_icon="🔧",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ------------------------------------------------------------------------------
# Email Authentication - FROM DOCUMENT 2
# ------------------------------------------------------------------------------
def load_authorized_admin_emails():
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
            return ["juanlopolicarpio@gmail.com"]
    except Exception as e:
        logger.error(f"❌ Failed to load admin emails: {e}")
        return ["juanlopolicarpio@gmail.com"]

def check_admin_email_authentication():
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
            email = st.text_input("Admin Email Address", placeholder="admin@cocopan.com",
                                  help="Enter your authorized admin email address")
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
# Foodpanda stores - FROM DOCUMENT 2 (UNCHANGED)
# ------------------------------------------------------------------------------
@st.cache_data(ttl=300)
def load_foodpanda_stores():
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
# NEW: Load all stores for SKU compliance (ADDITION FROM DOCUMENT 1)
# ------------------------------------------------------------------------------
@st.cache_data(ttl=300)
def load_all_stores():
    stores = []
    try:
        with open("branch_urls.json", "r") as f:
            data = json.load(f)
        urls = data.get("urls", [])
        for url in urls:
            if not url or url.startswith("Can't find") or url.startswith("foodpanda.ph/restaurant/rusp/cocopan-moonwalk"):
                continue
            try:
                platform = "foodpanda" if "foodpanda" in url else "grabfood"
                with db.get_connection() as conn:
                    cur = conn.cursor()
                    cur.execute("SELECT id, name, platform FROM stores WHERE url = %s", (url,))
                    row = cur.fetchone()
                    if row:
                        store_id, store_name, store_platform = row[0], row[1], row[2]
                    else:
                        store_name = extract_store_name_from_url_extended(url)
                        store_id = db.get_or_create_store(store_name, url)
                        store_platform = platform
                    stores.append({
                        "id": store_id, 
                        "name": store_name, 
                        "url": url, 
                        "platform": store_platform or platform
                    })
            except Exception as e:
                logger.error(f"Error ensuring store in DB for {url}: {e}")
                stores.append({
                    "id": None, 
                    "name": extract_store_name_from_url_extended(url), 
                    "url": url,
                    "platform": "foodpanda" if "foodpanda" in url else "grabfood"
                })
        logger.info(f"📋 Loaded {len(stores)} total stores")
        return stores
    except Exception as e:
        logger.error(f"❌ Failed to load stores list: {e}")
        return stores

def extract_store_name_from_url_extended(url: str) -> str:
    """Extended version for both platforms"""
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
        elif "food.grab.com" in url:
            m = re.search(r"/restaurant/([^/]+)/", url)
            if m:
                raw = m.group(1).replace("-delivery", "").replace("cocopan-", "")
                name = raw.replace("-", " ").replace("_", " ").title()
                return f"Cocopan {name}"
        elif "r.grab.com" in url:
            return "Cocopan Grab Store"
        return "Cocopan Store"
    except Exception:
        return "Cocopan Store"

# ------------------------------------------------------------------------------
# Existing admin actions (verification tab) - FROM DOCUMENT 2 (UNCHANGED)
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
    if "grab" in p: return "🛒"
    if "panda" in p: return "🍔"
    return "🏪"

def format_time_ago(checked_at) -> str:
    try:
        if pd.isna(checked_at): return "Unknown"
        checked = pd.to_datetime(checked_at)
        now = datetime.now(tz=checked.tz) if getattr(checked, "tzinfo", None) else datetime.now()
        diff = now - checked
        if diff.days > 0: return f"{diff.days}d ago"
        if diff.seconds >= 3600: return f"{diff.seconds // 3600}h ago"
        if diff.seconds >= 60: return f"{diff.seconds // 60}m ago"
        return "Just now"
    except:
        return "Unknown"

# ------------------------------------------------------------------------------
# VA Check-in helpers and schedule - FROM DOCUMENT 2 (UNCHANGED)
# ------------------------------------------------------------------------------
def get_va_checkin_schedule():
    return {"start_hour": 7, "end_hour": 21, "timezone": "Asia/Manila"}

def get_current_manila_time():
    return datetime.now(pytz.timezone("Asia/Manila"))

def get_current_hour_slot():
    now = get_current_manila_time()
    if now.minute >= 50:
        target_hour = now.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
    else:
        target_hour = now.replace(minute=0, second=0, microsecond=0)
    logger.debug(f"Current time: {now}, Target hour slot: {target_hour}")
    return target_hour

def is_checkin_time():
    now = get_current_manila_time()
    current_hour = now.hour
    minute = now.minute
    schedule = get_va_checkin_schedule()
    target_check_hour = current_hour + 1 if minute >= 50 else current_hour
    return schedule["start_hour"] <= target_check_hour <= schedule["end_hour"]

def _fmt_hour(h24: int) -> str:
    tz = pytz.timezone("Asia/Manila")
    dt = datetime.now(tz).replace(hour=h24, minute=0, second=0, microsecond=0)
    return dt.strftime("%I:%M %p").lstrip("0")

def get_next_checkin_time():
    sched = get_va_checkin_schedule()
    now = get_current_manila_time()
    m = now.minute; h = now.hour
    if m >= 50:
        nh = h + 1
        if nh > sched["end_hour"]:
            return (now + timedelta(days=1)).replace(hour=sched["start_hour"], minute=0, second=0, microsecond=0)
        return now.replace(hour=nh, minute=0, second=0, microsecond=0)
    else:
        if h < sched["start_hour"]:
            return now.replace(hour=sched["start_hour"], minute=0, second=0, microsecond=0)
        if h >= sched["end_hour"]:
            return (now + timedelta(days=1)).replace(hour=sched["start_hour"], minute=0, second=0, microsecond=0)
        return now.replace(hour=h + 1, minute=0, second=0, microsecond=0)

# NEW: explicit hour tag to avoid TZ confusion - FROM DOCUMENT 2 (UNCHANGED)
def _va_hour_tag(hour_slot: datetime) -> str:
    # hour_slot is Manila-aligned; tag uses Manila wall time text
    return f"[VA_CHECKIN] {hour_slot.strftime('%Y-%m-%d %H:00')}"

# ------------------------------------------------------------------------------
# DB reads using the hour tag - FROM DOCUMENT 2 (UNCHANGED)
# ------------------------------------------------------------------------------
def load_submitted_va_state(hour_slot: datetime) -> Set[int]:
    """Return set of store_ids marked OFFLINE for the exact hour tag."""
    try:
        _ = load_foodpanda_stores()
        tag_prefix = _va_hour_tag(hour_slot) + "%"
        offline_store_ids: Set[int] = set()
        with db.get_connection() as conn:
            cur = conn.cursor()
            cur.execute("""
                SELECT store_id, is_online
                  FROM status_checks
                 WHERE error_message LIKE %s
            """, (tag_prefix,))
            for store_id, is_online in cur.fetchall():
                if not is_online:
                    offline_store_ids.add(store_id)
        logger.info(f"[TAG] Loaded submitted VA state for {tag_prefix}: {len(offline_store_ids)} offline")
        return offline_store_ids
    except Exception as e:
        logger.error(f"[TAG] Error loading submitted VA state: {e}")
        return set()

def check_if_hour_already_completed(hour_slot: datetime) -> bool:
    """True if there exists any VA row for the exact hour tag."""
    try:
        tag_prefix = _va_hour_tag(hour_slot) + "%"
        with db.get_connection() as conn:
            cur = conn.cursor()
            cur.execute("""
                SELECT EXISTS(
                    SELECT 1 FROM status_checks
                     WHERE error_message LIKE %s
                )
            """, (tag_prefix,))
            (exists_row,) = cur.fetchone()
            logger.info(f"[TAG] Completed? {exists_row} for {tag_prefix}")
            return bool(exists_row)
    except Exception as e:
        logger.error(f"[TAG] Error checking hour completion: {e}")
        return False

def get_completed_hours_today() -> List[int]:
    """Still uses timestamp for display; harmless. Optional to keep."""
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
            return [int(r[0]) for r in cur.fetchall()]
    except Exception as e:
        logger.error(f"Error getting completed hours: {e}")
        return []

def debug_va_timestamps():
    try:
        with db.get_connection() as conn:
            cur = conn.cursor()
            cur.execute("""
                SELECT store_id, is_online, error_message, checked_at
                  FROM status_checks
                 WHERE error_message LIKE '[VA_CHECKIN]%%'
              ORDER BY checked_at DESC
                 LIMIT 5
            """)
            for r in cur.fetchall():
                logger.info(f"DEBUG VA ROW: {r}")
    except Exception as e:
        logger.error(f"Debug timestamps error: {e}")

# ------------------------------------------------------------------------------
# Save using the hour tag as idempotency key - FROM DOCUMENT 2 (UNCHANGED)
# ------------------------------------------------------------------------------
def save_va_checkin_enhanced(offline_store_ids: List[int], admin_email: str, hour_slot: datetime) -> bool:
    try:
        stores = load_foodpanda_stores()
        success_count, error_count = 0, 0
        run_id = uuid.uuid4()
        tag = _va_hour_tag(hour_slot)

        for store in stores:
            store_id = store["id"]
            if not store_id:
                continue
            try:
                is_online = store_id not in offline_store_ids
                msg = f"{tag} - Store {'online' if is_online else 'offline'} via {admin_email}"

                with db.get_connection() as conn:
                    cur = conn.cursor()

                    # DELETE by TAG (idempotent)
                    cur.execute("""
                        DELETE FROM status_checks
                         WHERE store_id = %s
                           AND error_message LIKE %s
                    """, (store_id, tag + "%"))

                    # INSERT legacy row
                    cur.execute("""
                        INSERT INTO status_checks (store_id, is_online, response_time_ms, error_message, checked_at)
                        VALUES (%s, %s, %s, %s, %s)
                    """, (store_id, is_online, 1000, msg, hour_slot))

                    conn.commit()

                # Upsert hourly snapshot (unchanged)
                try:
                    db.upsert_store_status_hourly(
                        effective_at=hour_slot,
                        platform='foodpanda',
                        store_id=store_id,
                        status='ONLINE' if is_online else 'OFFLINE',
                        confidence=1.0,
                        response_ms=1000,
                        evidence=msg,
                        probe_time=hour_slot,
                        run_id=run_id
                    )
                except Exception as e:
                    logger.warning(f"Hourly upsert warn for {store['name']}: {e}")

                success_count += 1
            except Exception as e:
                logger.error(f"Error saving VA for store {store_id}: {e}")
                error_count += 1

        # Save hourly summary (unchanged)
        try:
            total_stores = len(stores)
            online_count = total_stores - len(offline_store_ids)
            offline_count = len(offline_store_ids)
            db.upsert_status_summary_hourly(
                effective_at=hour_slot,
                total=total_stores,
                online=online_count,
                offline=offline_count,
                blocked=0,
                errors=0,
                unknown=0,
                last_probe_at=hour_slot
            )
        except Exception as e:
            logger.warning(f"Summary upsert warn: {e}")

        logger.info(f"✅ VA Check-in for {tag} saved. Success {success_count}/{len(stores)}, errors {error_count}")
        return error_count == 0
    except Exception as e:
        logger.error(f"❌ save_va_checkin_enhanced fatal: {e}")
        return False

# ------------------------------------------------------------------------------
# NEW: SKU Compliance Functions (ADDITION FROM DOCUMENT 1)
# ------------------------------------------------------------------------------
def clean_product_name(name: str) -> str:
    if not name:
        return ""
    # Remove platform prefixes
    cleaned = name.replace("GRAB ", "").replace("FOODPANDA ", "")
    return cleaned

def search_skus(platform: str, search_term: str) -> List[Dict]:
    if search_term and len(search_term.strip()) > 0:
        return db.search_master_skus(platform, search_term.strip())
    else:
        return db.get_master_skus_by_platform(platform)

# ------------------------------------------------------------------------------
# NEW: SKU Compliance Tab UI (ADDITION FROM DOCUMENT 1)
# ------------------------------------------------------------------------------
# ------------------------------------------------------------------------------
# NEW: SKU Compliance Tab UI (FIXED - Product checklist right under search)
# ------------------------------------------------------------------------------
def sku_compliance_tab():
    st.markdown(f"""
    <div class="section-header" style="background:#fff; border:1px solid #E2E8F0; border-radius:8px; padding:.9rem 1.1rem; margin:1.1rem 0 .9rem 0; box-shadow:0 1px 3px rgba(0,0,0,.06);">
        <div style="font-size:1.1rem; font-weight:600; color:#1E293B; margin:0;">📦 SKU Compliance Checker</div>
        <div style="font-size:.85rem; color:#64748B; margin:.25rem 0 0 0;">Check product availability at stores • VA Tool</div>
    </div>
    """, unsafe_allow_html=True)

    st.markdown("### 🔍 Store Product Checker")
    st.markdown("Select a store and platform, then check which products are out of stock.")
    
    col1, col2 = st.columns(2)
    
    with col1:
        platform = st.selectbox(
            "Select Platform:",
            ["grabfood", "foodpanda"],
            format_func=lambda x: "🛒 GrabFood" if x == "grabfood" else "🍔 Foodpanda",
            key="sku_platform_select"
        )
    
    with col2:
        # Load stores for selected platform
        all_stores = load_all_stores()
        platform_stores = [s for s in all_stores if s["platform"] == platform and s["id"]]
        
        if not platform_stores:
            st.error(f"No {platform} stores found in database.")
            return
            
        store_options = {f"{s['name']} (ID: {s['id']})": s for s in platform_stores}
        selected_store_display = st.selectbox(
            "Select Store:",
            options=list(store_options.keys()),
            key="sku_store_select"
        )
        
    if selected_store_display:
        selected_store = store_options[selected_store_display]
        store_id = selected_store["id"]
        store_name = selected_store["name"]
        store_url = selected_store["url"]
        
        st.markdown(f"""
        <div style="background:#F0FDF4; border:1px solid #BBF7D0; border-radius:8px; padding:1rem; margin:1rem 0;">
            <strong>📍 Selected Store:</strong> {store_name}<br>
            <strong>🏪 Platform:</strong> {'🛒 GrabFood' if platform == 'grabfood' else '🍔 Foodpanda'}<br>
            <strong>🔗 Store URL:</strong> <a href="{store_url}" target="_blank">Open Store</a>
        </div>
        """, unsafe_allow_html=True)
        
        # Load existing check data for today
        existing_check = db.get_store_sku_status_today(store_id, platform)
        
        # Initialize session state for SKU selections
        session_key = f"sku_check_{store_id}_{platform}"
        if session_key not in st.session_state:
            if existing_check:
                st.session_state[session_key] = set(existing_check.get('out_of_stock_skus', []))
            else:
                st.session_state[session_key] = set()
        
        # Load ALL SKUs first (for calculations)
        all_skus = db.get_master_skus_by_platform(platform)
        if not all_skus:
            st.warning(f"No products found for {platform}. Please run the SKU population script first.")
            return
        
        # Search functionality
        search_term = st.text_input(
            "🔍 Search Products:",
            placeholder="Type product name to search...",
            key=f"sku_search_{store_id}_{platform}"
        )
        
        # Filter SKUs for DISPLAY only
        if search_term:
            display_skus = search_skus(platform, search_term)
            st.info(f"Found {len(display_skus)} products matching '{search_term}' (out of {len(all_skus)} total)")
            if len(display_skus) < len(all_skus):
                st.warning(f"⚠️ Search active - showing {len(display_skus)}/{len(all_skus)} products. Compliance calculated on ALL {len(all_skus)} products.")
        else:
            display_skus = all_skus
            st.info(f"Showing all {len(all_skus)} {platform} products")
        
        # SKU checklist - RIGHT AFTER SEARCH
        st.markdown("### 📋 Product Checklist")
        st.markdown("Check the box next to products that are **OUT OF STOCK**:")
        
        # Group SKUs by category for better organization (use display_skus for UI)
        skus_by_category = {}
        for sku in display_skus:
            category = sku.get('flow_category', 'Other')
            if category not in skus_by_category:
                skus_by_category[category] = []
            skus_by_category[category].append(sku)
        
        # Display SKUs by category (using filtered display_skus)
        for category, category_skus in skus_by_category.items():
            with st.expander(f"📁 {category} ({len(category_skus)} products)", expanded=True):
                for sku in category_skus:
                    sku_code = sku['sku_code']
                    product_name = clean_product_name(sku['product_name'])
                    
                    is_out_of_stock = st.checkbox(
                        f"**{product_name}** ({sku_code})",
                        value=sku_code in st.session_state[session_key],
                        key=f"sku_{sku_code}_{store_id}_{platform}",
                        help=f"GMV Q3: ₱{sku.get('gmv_q3', 0):,.2f}"
                    )
                    
                    if is_out_of_stock:
                        if sku_code not in st.session_state[session_key]:
                            st.session_state[session_key].add(sku_code)
                    else:
                        if sku_code in st.session_state[session_key]:
                            st.session_state[session_key].discard(sku_code)
        
        # Bulk actions - AFTER CHECKLIST
        st.markdown("---")
        st.markdown("### ⚡ Bulk Actions")
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            if st.button("✅ Mark All In Stock", key=f"all_in_stock_{store_id}_{platform}"):
                st.session_state[session_key] = set()
                st.success("All products marked as IN STOCK")
                st.rerun()
        
        with col2:
            if st.button("❌ Mark All Out of Stock", key=f"all_out_stock_{store_id}_{platform}"):
                st.session_state[session_key] = set(sku['sku_code'] for sku in all_skus)
                st.success("All products marked as OUT OF STOCK")
                st.rerun()
        
        with col3:
            if existing_check and st.button("🔄 Reset to Last Saved", key=f"reset_{store_id}_{platform}"):
                st.session_state[session_key] = set(existing_check.get('out_of_stock_skus', []))
                st.success("Reset to last saved state")
                st.rerun()
        
        with col4:
            current_oos_count = len(st.session_state[session_key])
            total_skus = len(all_skus)  # Always use full catalog count
            compliance_pct = ((total_skus - current_oos_count) / max(total_skus, 1)) * 100
            st.metric("Current Compliance", f"{compliance_pct:.1f}%", f"{total_skus - current_oos_count}/{total_skus} in stock")
        
        # Display existing check info - AFTER BULK ACTIONS
        if existing_check:
            st.markdown("---")
            st.markdown(f"""
            <div style="background:#EFF6FF; border:1px solid #BFDBFE; border-radius:8px; padding:1rem; margin:1rem 0;">
                <strong>📋 Previously Saved Check:</strong><br>
                • Compliance: {existing_check['compliance_percentage']:.1f}%<br>
                • Out of Stock: {existing_check['out_of_stock_count']} items<br>
                • Checked by: {existing_check['checked_by']}<br>
                • Checked at: {existing_check['checked_at']}
            </div>
            """, unsafe_allow_html=True)
        
        # Summary before save (always use full catalog for calculations)
        st.markdown("---")
        st.markdown("### 💾 Save Compliance Check")
        
        final_oos_count = len(st.session_state[session_key])
        total_catalog_count = len(all_skus)  # Always use full catalog
        final_compliance = ((total_catalog_count - final_oos_count) / max(total_catalog_count, 1)) * 100
        
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Total Products", total_catalog_count)
        with col2:
            st.metric("In Stock", total_catalog_count - final_oos_count, "✅")
        with col3:
            st.metric("Out of Stock", final_oos_count, "❌")
        
        # Add warning if search is active
        if search_term:
            st.warning(f"""
            ⚠️ **Search Filter Active**: You are viewing {len(display_skus)} filtered products, but compliance 
            calculation includes all {total_catalog_count} products in the catalog. Make sure you've 
            checked all relevant products before saving.
            """)
        
        st.markdown(f"""
        <div style="background:{'#FEF2F2' if final_compliance < 80 else '#F0FDF4'}; border:1px solid {'#FECACA' if final_compliance < 80 else '#BBF7D0'}; border-radius:8px; padding:1rem; margin:1rem 0;">
            <strong>📊 Final Compliance: {final_compliance:.1f}%</strong><br>
            Status: {'🔴 Needs Attention' if final_compliance < 80 else '🟡 Good' if final_compliance < 95 else '🟢 Excellent'}<br>
            <small>Calculated from full catalog of {total_catalog_count} products</small>
        </div>
        """, unsafe_allow_html=True)
        
        # Save button
        if st.button("💾 Save Compliance Check", type="primary", use_container_width=True):
            try:
                out_of_stock_list = list(st.session_state[session_key])
                success = db.save_sku_compliance_check(
                    store_id=store_id,
                    platform=platform,
                    out_of_stock_ids=out_of_stock_list,
                    checked_by=st.session_state.admin_email
                )
                
                if success:
                    st.success(f"""
                    ✅ **Compliance check saved successfully!**
                    
                    📊 **Summary:**
                    - Store: {store_name}
                    - Platform: {platform.title()}
                    - Compliance: {final_compliance:.1f}%
                    - Out of Stock: {final_oos_count} items
                    - Checked by: {st.session_state.admin_email}
                    
                    The compliance data has been saved to the database.
                    """)
                    time.sleep(1)
                    
                else:
                    st.error("❌ Failed to save compliance check. Please try again.")
                    
            except Exception as e:
                st.error(f"❌ Error saving compliance check: {e}")
                logger.error(f"SKU compliance save error: {e}")
# ------------------------------------------------------------------------------
# VA Check-in UI Tab - FROM DOCUMENT 2 (UNCHANGED)
# ------------------------------------------------------------------------------
def enhanced_va_checkin_tab():
    debug_va_timestamps()

    current_time = get_current_manila_time()
    current_hour_slot = get_current_hour_slot()
    schedule = get_va_checkin_schedule()

    window_start = current_hour_slot - timedelta(minutes=10)
    window_end = current_hour_slot + timedelta(minutes=50)
    in_window = window_start <= current_time <= window_end

    st.markdown(f"""
    <div class="section-header" style="background:#fff; border:1px solid #E2E8F0; border-radius:8px; padding:.9rem 1.1rem; margin:1.1rem 0 .9rem 0; box-shadow:0 1px 3px rgba(0,0,0,.06);">
        <div style="font-size:1.1rem; font-weight:600; color:#1E293B; margin:0;">🐼 VA Hourly Check-in</div>
        <div style="font-size:.85rem; color:#64748B; margin:.25rem 0 0 0;">Foodpanda Store Status • {current_time.strftime('%I:%M %p')} Manila Time</div>
    </div>
    """, unsafe_allow_html=True)

    if not is_checkin_time():
        next_check = get_next_checkin_time()
        st.info(f"""
⏰ **Outside Check-in Window**

Current window: {current_hour_slot.strftime('%I:00 %p')} check (active 10 minutes before: {(current_hour_slot - timedelta(minutes=10)).strftime('%I:%M %p')} - {(current_hour_slot + timedelta(minutes=50)).strftime('%I:%M %p')})
Next window starts: {(next_check - timedelta(minutes=10)).strftime('%I:%M %p')}

You can review stores below; submissions are accepted during the window.
        """)

    # Hour completion via TAG
    hour_completed = check_if_hour_already_completed(current_hour_slot)
    completed_hours = get_completed_hours_today()

    # Session state
    if "va_offline_stores" not in st.session_state:
        st.session_state.va_offline_stores = set()

    # Per-slot initializer
    slot_key = f"va_initialized_slot::{current_hour_slot.isoformat()}"
    if slot_key not in st.session_state:
        st.session_state[slot_key] = False

    # Default ALL ONLINE at window start (only if not submitted)
    if in_window and not hour_completed and not st.session_state[slot_key]:
        st.session_state.va_offline_stores = set()
        st.session_state[slot_key] = True
        logger.info(f"🟢 Defaulted to ALL ONLINE for slot {current_hour_slot} at window start")

    # If already submitted and no local state, load submitted set via TAG
    if hour_completed and len(st.session_state.va_offline_stores) == 0:
        st.session_state.va_offline_stores = load_submitted_va_state(current_hour_slot)

    # Header (window info)
    st.markdown(f"""
    <div style="background: {'#DCFCE7' if hour_completed else '#FEF3E2'}; border: 1px solid {'#16A34A' if hour_completed else '#F59E0B'}; border-radius: 8px; padding: 1rem; margin: 1rem 0;">
        <h4 style="margin: 0 0 0.4rem 0; color: {'#166534' if hour_completed else '#92400E'};">
            🕐 {current_hour_slot.strftime('%I:00 %p')} Check Window ({window_start.strftime('%I:%M %p')} - {window_end.strftime('%I:%M %p')})
        </h4>
        <p style="margin: 0; color: {'#166534' if hour_completed else '#92400E'};">
            {"✅ Already submitted for this hour. Data saved to BOTH legacy and hourly systems." if hour_completed else "⏳ Ready for submission. Default state is ALL ONLINE during this window until you mark otherwise."}
        </p>
    </div>
    """, unsafe_allow_html=True)

    # Load stores
    stores = load_foodpanda_stores()
    if not stores:
        st.error("❌ Failed to load Foodpanda stores. Please refresh the page.")
        return

    # Metrics
    total = len(stores)
    offline = len(st.session_state.va_offline_stores)
    online = total - offline

    c1, c2, c3 = st.columns(3)
    with c1: st.metric("🟢 Online", online, "Will stay online")
    with c2: st.metric("🔴 Marked Offline", offline, "Marked by VA")
    with c3: st.metric("📊 Total Foodpanda", total, "All stores")

    if hour_completed:
        st.info(f"📋 **Showing submitted state** for {current_hour_slot.strftime('%I:00 %p')} (tag-based).")

    st.markdown("---")
    st.markdown("### 🔍 Search & Mark Stores")

    BRAND_PREFIX_RE = re.compile(r'^\s*cocopan[\s\-:]+', re.IGNORECASE)
    def norm_name(name: str) -> str:
        n = BRAND_PREFIX_RE.sub("", name or "")
        n = re.sub(r"\s+", " ", n).strip().lower()
        return n

    def rank(name: str, q: str) -> int:
        k = norm_name(name); ql = (q or "").strip().lower()
        if not ql: return 2
        if k.startswith(ql): return 0
        if any(w.startswith(ql) for w in k.split()): return 1
        if ql in k: return 2
        return 3

    # ----------------------------
    # OFFLINE-FIRST FILTERING (NEW)
    # ----------------------------
    offline_set: Set[int] = st.session_state.va_offline_stores

    q = st.text_input(
        "Search (prefix-friendly, 'Cocopan' ignored):",
        placeholder="Type: m → ma → may… (matches 'Cocopan Maysilo')",
        help="Results are ordered with OFFLINE stores first. Searching treats the 'Cocopan' prefix as invisible.",
    )

    if q:
        ranked = []
        for s in stores:
            r = rank(s["name"], q)
            if r < 3:
                # offline-first (0 for offline so it sorts ahead of online=1)
                offline_primary = 0 if (s["id"] in offline_set) else 1
                ranked.append((offline_primary, r, norm_name(s["name"]), s))
        filtered = [t[3] for t in sorted(ranked, key=lambda x: (x[0], x[1], x[2]))]
        st.info(f"Found {len(filtered)} matches for '{q}'. Offline first, then best prefix matches.")
    else:
        # Show all, offline first then alphabetical by normalized name
        filtered = sorted(
            stores,
            key=lambda s: (0 if (s["id"] in offline_set) else 1, norm_name(s["name"]))
        )
        st.info(f"Showing all {len(filtered)} stores. Ordered with OFFLINE first, then A→Z.")

    if not filtered:
        st.warning(f"No stores match '{q}'. Try a shorter prefix or a different term.")

    # Store list
    for s in filtered:
        sid, sname, surl = s["id"], s["name"], s["url"]
        if not sid: continue
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
            if is_offline and st.button("✅ Mark ONLINE", key=f"online_{sid}", use_container_width=True):
                st.session_state.va_offline_stores.discard(sid); st.success(f"✅ {display_name} marked as ONLINE"); st.rerun()
        with c3:
            if not is_offline:
                if st.button("🔴 Mark OFFLINE", key=f"offline_{sid}", use_container_width=True, type="primary"):
                    st.session_state.va_offline_stores.add(sid); st.success(f"🔴 {display_name} marked as OFFLINE"); st.rerun()
            else:
                st.write("OFFLINE ✓")
        st.markdown("<br>", unsafe_allow_html=True)

    if st.session_state.va_offline_stores:
        st.markdown("### 📋 Currently Marked Offline")
        idx = 1
        # show offline list A→Z for readability
        offline_sorted = sorted(
            [s for s in stores if s["id"] in st.session_state.va_offline_stores],
            key=lambda s: norm_name(s["name"])
        )
        for s in offline_sorted:
            st.write(f"{idx}. 🔴 {s['name'].replace('Cocopan - ', '').replace('Cocopan ', '')}")
            idx += 1

    # Submit
    st.markdown("---"); st.markdown("### 📤 Submit Hourly Check-in")
    if offline == 0: st.info("ℹ️ No stores marked as offline. All Foodpanda stores will be saved as ONLINE.")
    else: st.warning(f"⚠️ {offline} stores will be marked as OFFLINE. {online} stores will remain ONLINE.")
    if hour_completed: st.success("✅ Already submitted for this hour (tag-based).")
    else: st.info("⏳ Not submitted yet for this hour.")

    if not is_checkin_time():
        st.button("📤 Submit Check-in (Outside Window)", use_container_width=True, disabled=True)
    else:
        btn_label = ("🔁 Re-submit for " if hour_completed else "📤 Submit Check-in for ") + current_hour_slot.strftime("%I:00 %p")
        if st.button(btn_label, use_container_width=True, type="primary"):
            offline_ids = list(st.session_state.va_offline_stores)
            ok = save_va_checkin_enhanced(offline_ids, st.session_state.admin_email, current_hour_slot)
            if ok:
                st.success(f"""
✅ **{current_hour_slot.strftime('%I:00 %p')} Check-in Saved (tag-based idempotency)**

📊 Summary:
- 🟢 Online: {online}
- 🔴 Offline: {offline}
- 👤 By: {st.session_state.admin_email}
- 🕐 Window: {(current_hour_slot - timedelta(minutes=10)).strftime('%I:%M %p')} - {(current_hour_slot + timedelta(minutes=50)).strftime('%I:%M %p')}

💾 Saved to BOTH legacy (status_checks) and hourly snapshot (store_status_hourly) tables
                """)
                load_foodpanda_stores.clear()
                time.sleep(0.5); st.rerun()
            else:
                st.error("❌ Failed to save. Please try again.")

    # Today status
    st.markdown("### 📊 Today's Check-in Status")
    start_h, end_h = schedule["start_hour"], schedule["end_hour"]
    hours_range = list(range(start_h, end_h + 1))
    cols = st.columns(len(hours_range))
    for i, h in enumerate(hours_range):
        status = "✅ Done" if h in get_completed_hours_today() else "⏳ Pending"
        cols[i].metric(_fmt_hour(h), status)

# ------------------------------------------------------------------------------
# Styles - FROM DOCUMENT 2 (UNCHANGED)
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
# Main - MODIFIED TO ADD SKU TAB
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

    # MODIFIED: Added SKU Compliance tab
    tab1, tab2, tab3 = st.tabs(["🔧 Store Verification", "🐼 VA Hourly Check-in", "📦 SKU Compliance"])

    with tab1:
        df = load_stores_needing_attention()
        st.markdown(f"""
        <div class="section-header" style="background:#fff; border:1px solid #E2E8F0; border-radius:8px; padding:.9rem 1.1rem; margin:1.1rem 0 .9rem 0; box-shadow:0 1px 3px rgba(0,0,0,0.06);">
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
                load_stores_needing_attention.clear(); st.rerun()
        else:
            if "completed_stores" not in st.session_state:
                st.session_state.completed_stores = set()
            total = len(df); done = len(st.session_state.completed_stores)
            pct = (done / total) * 100 if total else 0.0
            st.markdown(f"""
            <div style="background:#E2E8F0; border-radius:8px; height:8px; overflow:hidden; margin:.5rem 0 1rem 0;">
                <div style="background:linear-gradient(90deg,#10B981,#059669); height:100%; width:{pct:.1f}%"></div>
            </div>
            <div style="text-align:center; color:#64748B; margin-bottom:1rem;">Progress: {done}/{total} completed</div>
            """, unsafe_allow_html=True)

            for idx, row in df.iterrows():
                sid = row["id"]; sname = row["name"]; platform = (row.get("platform") or "unknown")
                url = row["url"]; checked_at = row.get("checked_at")
                if sid in st.session_state.completed_stores: continue

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
                            st.session_state.completed_stores.add(sid); st.success(f"✅ {sname} marked Online")
                            time.sleep(0.6); st.rerun()
                        else: st.error("Update failed. Try again.")
                with c3:
                    if st.button("❌ Offline", key=f"v_offline_{sid}", use_container_width=True):
                        if mark_store_status(sid, sname, False, platform):
                            st.session_state.completed_stores.add(sid); st.success(f"❌ {sname} marked Offline")
                            time.sleep(0.6); st.rerun()
                        else: st.error("Update failed. Try again.")
                st.markdown("<br>", unsafe_allow_html=True)

            remain = total - done
            if remain > 0: st.info(f"📋 {remain} stores remaining")
            else:
                st.success("🎉 All stores verified!")
                if st.button("🔄 Check for New Issues", use_container_width=True, key="refresh_verification"):
                    st.session_state.completed_stores.clear(); load_stores_needing_attention.clear(); st.rerun()

    with tab2:
        enhanced_va_checkin_tab()

    # NEW: SKU Compliance Tab
    with tab3:
        sku_compliance_tab()

    st.markdown("---")
    c1, c2, c3 = st.columns([1, 2, 1])
    with c2:
        # MODIFIED: Clear both cache functions
        if st.button("🔄 Refresh All Data", use_container_width=True):
            load_stores_needing_attention.clear()
            load_foodpanda_stores.clear()
            load_all_stores.clear()
            st.rerun()

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        st.error(f"❌ System Error: {e}")
        logger.exception("Admin dashboard error")