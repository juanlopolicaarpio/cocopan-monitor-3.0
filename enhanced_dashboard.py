#!/usr/bin/env python3
"""
CocoPan Watchtower - CLIENT DASHBOARD (SIMPLIFIED, FIXED)
✅ Simple login page - no fancy backgrounds
✅ Just the form and basic styling
✅ Foodpanda status persistence until next hour (live bias to VA check-ins)
✅ Reports starting September 10, 2024
✅ Daily uptime / downtime / reports use store_status_hourly (fixes inflated counts)
"""

import os
import json
import time
import hmac
import base64
import hashlib
import logging
import threading
import http.server
import socketserver
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import pytz

# Production modules
from config import config
from database import db

# =========================
# Optional Cookie Manager
# =========================
try:
    from streamlit_cookies_manager import EncryptedCookieManager as CookieManager
    _COOKIE_LIB_AVAILABLE = True
except Exception:
    CookieManager = None
    _COOKIE_LIB_AVAILABLE = False

# ---------------- Health Check Server (Railway) ----------------
def create_health_server():
    class HealthHandler(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path == '/healthz':
                try:
                    db.get_database_stats()
                    self.send_response(200)
                    self.send_header('Content-type', 'text/plain')
                    self.end_headers()
                    self.wfile.write(b'OK - Client Dashboard Healthy')
                except Exception as e:
                    self.send_response(500)
                    self.send_header('Content-type', 'text/plain')
                    self.end_headers()
                    self.wfile.write(f'ERROR - {str(e)}'.encode())
            else:
                self.send_response(404)
                self.end_headers()
        def log_message(self, format, *args):
            pass

    try:
        port = 8503
        with socketserver.TCPServer(("", port), HealthHandler) as httpd:
            httpd.serve_forever()
    except Exception as e:
        logger = logging.getLogger(__name__)
        logger.debug(f"Health server error: {e}")

if os.getenv('RAILWAY_ENVIRONMENT') == 'production':
    threading.Thread(target=create_health_server, daemon=True).start()

# ---------------- Streamlit Page Config ----------------
st.set_page_config(
    page_title="CocoPan Watchtower",
    page_icon="🏢",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# ---------------- Logging ----------------
logging.basicConfig(level=getattr(logging, config.LOG_LEVEL))
logger = logging.getLogger(__name__)

# ======================================================================
#                            AUTH HELPERS
# ======================================================================
COOKIE_NAME = "cp_client_auth"
COOKIE_PREFIX = "watchtower"
TOKEN_TTL_DAYS = 30
TOKEN_ROLLING_REFRESH_DAYS = 7

def _get_secret_key() -> bytes:
    secret = getattr(config, 'SECRET_KEY', None) or os.getenv('SECRET_KEY')
    if not secret:
        secret = "CHANGE_ME_IN_PROD_please"
    return secret.encode("utf-8")

def _b64url_encode(b: bytes) -> str:
    return base64.urlsafe_b64encode(b).decode('utf-8').rstrip("=")

def _b64url_decode(s: str) -> bytes:
    pad = "=" * (-len(s) % 4)
    return base64.urlsafe_b64decode(s + pad)

def _sign(payload_b64: str) -> str:
    sig = hmac.new(_get_secret_key(), payload_b64.encode('utf-8'), hashlib.sha256).digest()
    return _b64url_encode(sig)

def issue_token(email: str, ttl_days: int = TOKEN_TTL_DAYS) -> str:
    now = int(time.time())
    exp = now + int(ttl_days * 24 * 3600)
    payload = {"email": email, "iat": now, "exp": exp}
    payload_json = json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    payload_b64 = _b64url_encode(payload_json)
    sig_b64 = _sign(payload_b64)
    return f"{payload_b64}.{sig_b64}"

def verify_token(token: str) -> Optional[Dict[str, Any]]:
    try:
        parts = token.split(".")
        if len(parts) != 2:
            return None
        payload_b64, sig_b64 = parts
        expected_sig = _sign(payload_b64)
        if not hmac.compare_digest(sig_b64, expected_sig):
            return None
        payload = json.loads(_b64url_decode(payload_b64).decode("utf-8"))
        if int(payload.get("exp", 0)) < int(time.time()):
            return None
        return payload
    except Exception:
        return None

def days_remaining(exp_unix: int) -> float:
    return max(0.0, (exp_unix - int(time.time())) / 86400.0)

# ---------------- Cookie Abstraction ----------------
class CookieStore:
    def __init__(self):
        self.persistent = False
        self.ready = True
        self._cookies = None

        if _COOKIE_LIB_AVAILABLE:
            try:
                self._cookies = CookieManager(prefix=COOKIE_PREFIX, password=os.getenv("COOKIE_PASSWORD") or "set-a-strong-cookie-password")
                self.ready = self._cookies.ready()
                self.persistent = True
            except Exception as e:
                logger.warning(f"Cookie manager unavailable, using session fallback: {e}")
                self._cookies = None
                self.persistent = False
                self.ready = True
        else:
            if "cookie_fallback" not in st.session_state:
                st.session_state.cookie_fallback = {}
            self._cookies = st.session_state.cookie_fallback
            self.persistent = False
            self.ready = True

    def get(self, key: str) -> Optional[str]:
        if self.persistent:
            return self._cookies.get(key)
        return self._cookies.get(key)

    def set(self, key: str, value: str, max_age_days: int = TOKEN_TTL_DAYS):
        if self.persistent:
            self._cookies[key] = value
            self._cookies.save()
        else:
            self._cookies[key] = value

    def delete(self, key: str):
        if self.persistent:
            try:
                if key in self._cookies:
                    del self._cookies[key]
                    self._cookies.save()
            except Exception:
                pass
        else:
            if key in self._cookies:
                del self._cookies[key]

# ---------------- Allow-list Loader ----------------
def load_authorized_emails():
    try:
        with open('client_alerts.json', 'r') as f:
            data = json.load(f)
        authorized_emails = []
        for group in data.get('clients', {}).values():
            if group.get('enabled', False):
                emails = [e.strip() for e in group.get('emails', []) if str(e).strip()]
                authorized_emails.extend(emails)
        authorized_emails = sorted(set([e.lower() for e in authorized_emails]))
        logger.info(f"✅ Loaded {len(authorized_emails)} authorized client emails")
        return authorized_emails
    except Exception as e:
        logger.error(f"❌ Failed to load client emails: {e}")
        return ["juanlopolicarpio@gmail.com"]

# ======================================================================
#                           SIMPLE STYLES
# ======================================================================
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');
    #MainMenu, footer, .stDeployButton, header {visibility: hidden;}
    .main { 
        font-family: 'Inter', sans-serif; 
        background: #F8FAFC;
        color: #1E293B; 
        padding: 2rem;
    }
    .login-container {
        max-width: 400px;
        margin: 4rem auto;
        background: white;
        padding: 2rem;
        border-radius: 8px;
        border: 1px solid #E2E8F0;
        box-shadow: 0 1px 3px rgba(0,0,0,0.1);
    }
    .login-title {
        font-size: 1.5rem;
        font-weight: 700;
        color: #1E293B;
        text-align: center;
        margin: 0 0 1.5rem 0;
    }
    .header-section { 
        background: linear-gradient(135deg, #1E40AF 0%, #3B82F6 100%); 
        border-radius: 16px; 
        padding: 1.5rem; 
        margin-bottom: 1.25rem; 
        box-shadow: 0 4px 6px -1px rgba(0,0,0,.1);
    }
    h1 { 
        color: #fff !important; 
        font-size: 2.4rem !important; 
        font-weight: 700 !important; 
        text-align: center !important; 
        margin:0 !important; 
        letter-spacing:-.02em;
    }
    h3 { 
        color: rgba(255,255,255,.9) !important; 
        font-weight: 400 !important; 
        font-size: 1rem !important; 
        text-align:center !important; 
        margin:.4rem 0 0 0 !important;
    }
    .section-header { 
        background:#fff; 
        border:1px solid #E2E8F0; 
        border-radius:8px; 
        padding:.9rem 1.1rem; 
        margin:1.1rem 0 .9rem 0; 
        box-shadow:0 1px 3px rgba(0,0,0,.06);
    }
    .section-title { 
        font-size:1.1rem; 
        font-weight:600; 
        color:#1E293B; 
        margin:0;
    }
    .section-subtitle { 
        font-size:.85rem; 
        color:#64748B; 
        margin:.25rem 0 0 0;
    }
    [data-testid="metric-container"] { 
        background:#fff; 
        border:1px solid #E2E8F0; 
        border-radius:12px; 
        padding:1.25rem 1rem; 
        box-shadow:0 1px 3px rgba(0,0,0,.06); 
        text-align:center; 
        transition:.2s; 
    }
    [data-testid="metric-container"]:hover { 
        box-shadow:0 4px 6px -1px rgba(0,0,0,.08); 
        transform: translateY(-1px); 
    }
    [data-testid="metric-value"] { 
        color:#1E293B; 
        font-weight:700; 
        font-size:1.75rem; 
    }
    [data-testid="metric-label"] { 
        color:#64748B; 
        font-weight:600; 
        font-size:.8rem; 
        text-transform:uppercase; 
        letter-spacing:.05em; 
    }
    .stTabs [data-baseweb="tab-list"] { 
        gap:0; 
        background:#F1F5F9; 
        border-radius:8px; 
        padding:.25rem; 
        border:1px solid #E2E8F0;
    }
    .stTabs [data-baseweb="tab"] { 
        background:transparent; 
        border:none; 
        border-radius:6px; 
        color:#64748B; 
        font-weight:500; 
        padding:.65rem 1.2rem; 
        transition:.2s; 
        font-size:.85rem;
    }
    .stTabs [data-baseweb="tab"]:hover { 
        background:#E2E8F0; 
        color:#1E293B; 
    }
    .stTabs [aria-selected="true"] { 
        background:#fff !important; 
        color:#1E293B !important; 
        box-shadow:0 1px 2px rgba(0,0,0,.05); 
        font-weight:600;
    }
    .stDataFrame { 
        background:#fff; 
        border-radius:12px; 
        border:1px solid #E2E8F0; 
        overflow:hidden; 
        box-shadow:0 1px 3px rgba(0,0,0,.06);
    }
    .stDataFrame thead tr th { 
        background:#F8FAFC !important; 
        color:#475569 !important; 
        font-weight:600 !important; 
        text-transform:uppercase; 
        font-size:.72rem; 
        letter-spacing:.05em; 
        border:none !important; 
        border-bottom:1px solid #E2E8F0 !important; 
        padding:.8rem .6rem !important;
    }
    .stDataFrame tbody tr td { 
        background:#fff !important; 
        color:#1E293B !important; 
        border:none !important; 
        border-bottom:1px solid #F1F5F9 !important; 
        padding:.65rem !important;
    }
    .stDataFrame tbody tr:hover td { 
        background:#F8FAFC !important;
    }
    .chart-container { 
        background:#fff; 
        border:1px solid #E2E8F0; 
        border-radius:12px; 
        padding:.75rem; 
        box-shadow:0 1px 3px rgba(0,0,0,.06); 
    }
    .filter-container { 
        background:#F8FAFC; 
        border:1px solid #E2E8F0; 
        border-radius:8px; 
        padding:.9rem; 
        margin-bottom:.9rem; 
    }
    @media (max-width: 768px) {
        .login-container { 
            margin: 2rem auto;
            padding: 1.5rem;
        }
    }
</style>
""", unsafe_allow_html=True)

# ======================================================================
#                       DATA HELPERS (PLATFORM + FLAGS)
# ======================================================================
def standardize_platform_name(platform_value):
    if pd.isna(platform_value):
        return "Unknown"
    platform_str = str(platform_value).lower()
    if 'grab' in platform_str:
        return "GrabFood"
    elif 'foodpanda' in platform_str or 'panda' in platform_str:
        return "Foodpanda"
    else:
        return "Unknown"

def is_under_review(error_message: str) -> bool:
    if pd.isna(error_message):
        return False
    msg = str(error_message).strip()
    return msg.startswith('[BLOCKED]') or msg.startswith('[UNKNOWN]') or msg.startswith('[ERROR]')

# ======================================================================
#                            DATA LOADERS
# ======================================================================
@st.cache_data(ttl=config.DASHBOARD_AUTO_REFRESH)
def load_comprehensive_data():
    """
    LIVE LIST: prefer VA check-ins for Foodpanda (uses status_checks for 'latest' only)
    DAILY UPTIME: use store_status_hourly to avoid inflated counts
    """
    try:
        with db.get_connection() as conn:
            # --- LIVE STATUS (latest) ---
            latest_status_query = """
                WITH ranked_checks AS (
                    SELECT 
                        s.id,
                        COALESCE(s.name_override, s.name) AS name,
                        s.platform,
                        s.url,
                        sc.is_online,
                        sc.checked_at,
                        sc.response_time_ms,
                        sc.error_message,
                        ROW_NUMBER() OVER (
                            PARTITION BY s.id 
                            ORDER BY 
                                CASE 
                                    WHEN s.platform = 'foodpanda' AND sc.error_message LIKE '[VA_CHECKIN]%' THEN 1
                                    ELSE 2 
                                END,
                                sc.checked_at DESC
                        ) as rn
                    FROM stores s
                    INNER JOIN status_checks sc ON s.id = sc.store_id
                    WHERE sc.checked_at >= NOW() - INTERVAL '24 hours'
                )
                SELECT 
                    id, name, platform, url, is_online, checked_at, 
                    response_time_ms, error_message
                FROM ranked_checks 
                WHERE rn = 1
                ORDER BY name
            """
            latest_status = pd.read_sql_query(latest_status_query, conn)
            if not latest_status.empty:
                latest_status['platform'] = latest_status['platform'].apply(standardize_platform_name)

            # --- DAILY UPTIME (SNAPSHOT) ---
            daily_uptime_query = """
                WITH today_hours AS (
                  SELECT
                    ssh.store_id,
                    COUNT(*) FILTER (WHERE ssh.status IN ('ONLINE','OFFLINE')) AS effective_checks,
                    SUM(CASE WHEN ssh.status = 'ONLINE'  THEN 1 ELSE 0 END) AS online_checks,
                    SUM(CASE WHEN ssh.status = 'OFFLINE' THEN 1 ELSE 0 END) AS downtime_count,
                    COUNT(*) FILTER (WHERE ssh.status IN ('BLOCKED','UNKNOWN','ERROR')) AS under_review_checks
                  FROM store_status_hourly ssh
                  WHERE DATE(ssh.effective_at AT TIME ZONE 'Asia/Manila')
                        = DATE(timezone('Asia/Manila', now()))
                  GROUP BY ssh.store_id
                )
                SELECT
                  s.id,
                  COALESCE(s.name_override, s.name) AS name,
                  s.platform,
                  COALESCE(th.effective_checks, 0) AS effective_checks,
                  COALESCE(th.online_checks, 0)    AS online_checks,
                  COALESCE(th.downtime_count, 0)   AS downtime_count,
                  COALESCE(th.under_review_checks,0) AS under_review_checks,
                  CASE 
                    WHEN COALESCE(th.effective_checks,0) = 0 THEN NULL
                    ELSE ROUND( (th.online_checks * 100.0 / NULLIF(th.effective_checks,0)) , 1)
                  END AS uptime_percentage
                FROM stores s
                LEFT JOIN today_hours th ON th.store_id = s.id
                WHERE COALESCE(th.effective_checks,0) > 0
                ORDER BY uptime_percentage DESC NULLS LAST, name
            """
            daily_uptime = pd.read_sql_query(daily_uptime_query, conn)
            if not daily_uptime.empty:
                daily_uptime['platform'] = daily_uptime['platform'].apply(standardize_platform_name)

            return latest_status, daily_uptime, None
    except Exception as e:
        logger.error(f"Error loading data: {e}")
        return None, None, str(e)

@st.cache_data(ttl=300)
def load_reports_data(start_date, end_date):
    """Historical reports from store_status_hourly (range inclusive)"""
    try:
        # Enforce minimum date of September 10, 2024
        min_date = datetime(2024, 9, 10).date()
        if start_date < min_date:
            start_date = min_date
            
        with db.get_connection() as conn:
            reports_query = """
                WITH range_hours AS (
                  SELECT
                    ssh.store_id,
                    COUNT(*)                           AS total_hours,
                    COUNT(*) FILTER (WHERE ssh.status IN ('BLOCKED','UNKNOWN','ERROR')) AS under_review_hours,
                    COUNT(*) FILTER (WHERE ssh.status IN ('ONLINE','OFFLINE')) AS effective_hours,
                    COUNT(*) FILTER (WHERE ssh.status = 'ONLINE')             AS online_hours
                  FROM store_status_hourly ssh
                  WHERE DATE(ssh.effective_at AT TIME ZONE 'Asia/Manila') BETWEEN %s AND %s
                  GROUP BY ssh.store_id
                )
                SELECT
                  s.id,
                  COALESCE(s.name_override, s.name) AS name,
                  s.platform,
                  s.url,
                  COALESCE(rh.total_hours,0)        AS total_checks,
                  COALESCE(rh.under_review_hours,0) AS under_review_checks,
                  COALESCE(rh.effective_hours,0)    AS effective_checks,
                  COALESCE(rh.online_hours,0)       AS effective_online_checks,
                  CASE
                    WHEN COALESCE(rh.effective_hours,0) = 0 THEN NULL
                    ELSE ROUND((rh.online_hours * 100.0 / NULLIF(rh.effective_hours,0)), 1)
                  END AS uptime_percentage
                FROM stores s
                LEFT JOIN range_hours rh ON rh.store_id = s.id
                ORDER BY s.name
            """
            reports_data = pd.read_sql_query(reports_query, conn, params=(start_date, end_date))
            if not reports_data.empty:
                reports_data['platform'] = reports_data['platform'].apply(standardize_platform_name)
            return reports_data, None
    except Exception as e:
        logger.error(f"Error loading reports data: {e}")
        return None, str(e)

def load_downtime_today():
    """Downtime events today (snapshot-based)"""
    try:
        with db.get_connection() as conn:
            downtime_query = """
                SELECT 
                    s.name,
                    s.platform,
                    COUNT(*) FILTER (WHERE ssh.status = 'OFFLINE') AS downtime_events,
                    MIN(ssh.effective_at) FILTER (WHERE ssh.status = 'OFFLINE') AS first_downtime,
                    MAX(ssh.effective_at) FILTER (WHERE ssh.status = 'OFFLINE') AS last_downtime
                FROM stores s
                JOIN store_status_hourly ssh ON ssh.store_id = s.id
                WHERE DATE(ssh.effective_at AT TIME ZONE 'Asia/Manila')
                      = DATE(timezone('Asia/Manila', now()))
                GROUP BY s.id, s.name, s.platform
                HAVING COUNT(*) FILTER (WHERE ssh.status = 'OFFLINE') > 0
                ORDER BY downtime_events DESC
            """
            dt = pd.read_sql_query(downtime_query, conn)
            if not dt.empty:
                dt['platform'] = dt['platform'].apply(standardize_platform_name)
            return dt, None
    except Exception as e:
        return pd.DataFrame(), str(e)

# ======================================================================
#                               UI HELPERS
# ======================================================================
def create_donut(online_count: int, offline_count: int):
    total = max(online_count + offline_count, 1)
    uptime_pct = online_count / total * 100.0
    fig = go.Figure(data=[go.Pie(
        labels=['Online', 'Offline'],
        values=[online_count, offline_count],
        hole=0.65,
        marker=dict(colors=['#059669', '#EF4444'], line=dict(width=2, color='#FFFFFF')),
        textinfo='none',
        hovertemplate='<b>%{label}</b><br>%{value} stores (%{percent})<extra></extra>',
        showlegend=True
    )])
    fig.add_annotation(text=f"<b style='font-size:28px'>{uptime_pct:.0f}%</b>", x=0.5, y=0.5, showarrow=False, font=dict(family="Inter"))
    fig.update_layout(height=250, margin=dict(t=12,b=12,l=12,r=12),
                      paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)',
                      legend=dict(orientation="h", yanchor="bottom", y=-0.08, xanchor="center", x=0.5, font=dict(size=10)))
    return fig

def get_last_check_time(latest_status):
    """Get the most recent check time"""
    if latest_status is None or len(latest_status) == 0:
        return config.get_current_time()
    try:
        latest_time = pd.to_datetime(latest_status['checked_at']).max()
        if latest_time.tz is None:
            latest_time = latest_time.tz_localize('UTC')
        ph_tz = config.get_timezone()
        latest_time = latest_time.tz_convert(ph_tz)
        return latest_time
    except Exception:
        return config.get_current_time()

# ======================================================================
#                         SIMPLE AUTH FLOW
# ======================================================================
def check_email_authentication() -> bool:
    authorized_emails = load_authorized_emails()

    cookies = CookieStore()
    if not cookies.ready:
        st.error("Authentication system initializing. Please refresh.")
        return False

    if 'client_authenticated' not in st.session_state:
        st.session_state.client_authenticated = False
        st.session_state.client_email = None

    # Cookie-based auto-login
    token = cookies.get(COOKIE_NAME)
    if token and not st.session_state.client_authenticated:
        payload = verify_token(token)
        if payload:
            email = str(payload.get("email", "")).lower()
            if email in authorized_emails:
                st.session_state.client_authenticated = True
                st.session_state.client_email = email
                rem = days_remaining(int(payload.get("exp", 0)))
                if rem <= TOKEN_ROLLING_REFRESH_DAYS:
                    new_token = issue_token(email)
                    cookies.set(COOKIE_NAME, new_token, max_age_days=TOKEN_TTL_DAYS)
                return True
        else:
            cookies.delete(COOKIE_NAME)

    if st.session_state.client_authenticated and st.session_state.client_email:
        return True

    # ---- SIMPLE LOGIN UI ----
    st.markdown("""
    <div class="login-container">
        <div class="login-title">CocoPan Watchtower</div>
    </div>
    """, unsafe_allow_html=True)

    with st.form("auth_form", clear_on_submit=False):
        email = st.text_input(
            "Authorized Email Address",
            placeholder="your.email@company.com"
        )
        
        submitted = st.form_submit_button("Access Dashboard", use_container_width=True)
        
        if submitted:
            e = email.strip().lower()
            if e and e in authorized_emails:
                token = issue_token(e)
                cookies.set(COOKIE_NAME, token, max_age_days=TOKEN_TTL_DAYS)
                st.session_state.client_authenticated = True
                st.session_state.client_email = e
                st.success("✅ Access granted")
                st.rerun()
            else:
                st.error("❌ Email not authorized")

    return False

# ======================================================================
#                           MAIN APP
# ======================================================================
def main():
    if not check_email_authentication():
        return

    # Sidebar user info
    with st.sidebar:
        st.markdown(f"**Logged in as:**\n{st.session_state.client_email}")
        if st.button("Logout"):
            CookieStore().delete(COOKIE_NAME)
            st.session_state.client_authenticated = False
            st.session_state.client_email = None
            st.rerun()

    latest_status, daily_uptime, error = load_comprehensive_data()
    last_check_time = get_last_check_time(latest_status)

    st.markdown(f"""
    <div class="header-section">
        <h1>CocoPan Watchtower</h1>
        <h3>Operations Monitoring • Data as of {last_check_time.strftime('%B %d, %Y • %I:%M %p')} Manila Time</h3>
    </div>
    """, unsafe_allow_html=True)

    if error:
        st.error(f"System Error: {error}")
        return

    if latest_status is None or len(latest_status) == 0:
        st.info("Monitoring is running and stores are being checked regularly.")
        return

    # Calculate top-line stats (live list -> exclude 'under review')
    under_review_mask = latest_status['error_message'].apply(is_under_review)
    under_review_count = int(under_review_mask.sum())
    effective = latest_status[~under_review_mask]

    online_stores = int((effective['is_online'] == 1).sum())
    offline_stores = int((effective['is_online'] == 0).sum())

    latest_status['platform'] = latest_status['platform'].fillna('Unknown')
    grab_total = int((latest_status['platform'] == 'GrabFood').sum())
    fp_total   = int((latest_status['platform'] == 'Foodpanda').sum())

    grab_eff   = effective[effective['platform'] == 'GrabFood']
    fp_eff     = effective[effective['platform'] == 'Foodpanda']
    grab_offline = int((grab_eff['is_online'] == 0).sum())
    fp_offline   = int((fp_eff['is_online'] == 0).sum())

    total_effective = max(online_stores + offline_stores, 1)
    online_pct = online_stores / total_effective * 100.0

    # Top layout
    left, right = st.columns([1.6, 1])
    with left:
        st.markdown("### Network Overview")
        m1, m2, m3 = st.columns(3)
        with m1:
            st.metric("Online Stores", f"{online_stores}", f"{online_pct:.0f}% uptime")
        with m2:
            st.metric("Offline Stores", f"{offline_stores}", "being monitored")
        with m3:
            st.metric("Stores Under Review", f"{under_review_count}", "routine checks")
        p1, p2 = st.columns(2)
        with p1:
            st.metric("Grab stores", f"{grab_total}", f"{grab_offline} offline")
        with p2:
            st.metric("Foodpanda stores", f"{fp_total}", f"{fp_offline} offline")
    with right:
        st.markdown('<div class="chart-container">', unsafe_allow_html=True)
        fig = create_donut(online_stores, offline_stores)
        st.plotly_chart(fig, use_container_width=True, config={'displayModeBar': False})
        st.markdown('</div>', unsafe_allow_html=True)

    # Tabs
    tab1, tab2, tab3, tab4 = st.tabs(["🔴 Live Operations Monitor", "📊 Store Uptime Analytics", "📉 Downtime Events", "📈 Reports"])

    # ----- TAB 1: LIVE -----
    with tab1:
        st.markdown(f"""
        <div class="section-header">
            <div class="section-title">Live Operations Monitor</div>
            <div class="section-subtitle">Real-time store status • Updated {last_check_time.strftime('%I:%M %p')} Manila Time</div>
        </div>
        """, unsafe_allow_html=True)

        st.markdown('<div class="filter-container">', unsafe_allow_html=True)
        c1, c2 = st.columns(2)
        with c1:
            available_platforms = sorted(latest_status['platform'].dropna().unique())
            platform_options = ["All Platforms"] + available_platforms
            platform_filter_live = st.selectbox("Filter by Platform:", platform_options, key="live_platform_filter")
        with c2:
            status_filter = st.selectbox(
                "Filter by Status:",
                ["All Statuses", "Online Only", "Offline Only", "Under Review"],
                key="live_status_filter"
            )
        st.markdown('</div>', unsafe_allow_html=True)

        current = latest_status.copy()
        current['under_review'] = current['error_message'].apply(is_under_review)

        if platform_filter_live != "All Platforms":
            current = current[current['platform'] == platform_filter_live]
        if status_filter == "Online Only":
            current = current[(current['is_online'] == 1) & (~current['under_review'])]
        elif status_filter == "Offline Only":
            current = current[(current['is_online'] == 0) & (~current['under_review'])]
        elif status_filter == "Under Review":
            current = current[current['under_review']]

        if len(current) == 0:
            st.info("No stores found for the selected filters.")
        else:
            display = pd.DataFrame()
            display['Branch'] = current['name'].str.replace('Cocopan - ', '', regex=False).str.replace('Cocopan ', '', regex=False)
            display['Platform'] = current['platform']

            status_labels = []
            for _, row in current.iterrows():
                if row['under_review']:
                    status_labels.append("🟡 Under Review")
                else:
                    status_labels.append("🟢 Online" if row['is_online'] else "🔴 Offline")
            display['Status'] = status_labels
            
            try:
                cur = current.copy()
                cur['checked_at'] = pd.to_datetime(cur['checked_at'])
                if cur['checked_at'].dt.tz is None:
                    cur['checked_at'] = cur['checked_at'].dt.tz_localize('UTC')
                ph_tz = config.get_timezone()
                cur['checked_at'] = cur['checked_at'].dt.tz_convert(ph_tz)
                display['Last Checked'] = cur['checked_at'].dt.strftime('%I:%M %p')
            except Exception:
                display['Last Checked'] = '—'

            st.dataframe(display, use_container_width=True, hide_index=True, height=420)

    # ----- TAB 2: DAILY UPTIME (SNAPSHOT) -----
    with tab2:
        st.markdown(f"""
        <div class="section-header">
            <div class="section-title">Store Uptime Analytics</div>
            <div class="section-subtitle">Daily performance metrics and availability statistics (snapshot)</div>
        </div>
        """, unsafe_allow_html=True)

        if daily_uptime is not None and len(daily_uptime) > 0:
            st.markdown('<div class="filter-container">', unsafe_allow_html=True)
            d1, d2 = st.columns(2)
            with d1:
                available_platforms = sorted(daily_uptime['platform'].dropna().unique())
                platform_options = ["All Platforms"] + available_platforms
                platform_filter = st.selectbox("Filter by Platform:", platform_options, key="uptime_platform_filter")
            with d2:
                sort_order = st.selectbox("Sort by Uptime:", ["Highest to Lowest", "Lowest to Highest"], key="uptime_sort_order")
            st.markdown('</div>', unsafe_allow_html=True)

            filt = daily_uptime.copy()
            if platform_filter != "All Platforms":
                filt = filt[filt['platform'] == platform_filter]

            if len(filt) == 0:
                st.info(f"No data available for {platform_filter}")
            else:
                disp = pd.DataFrame()
                disp['Branch'] = filt['name'].str.replace('Cocopan - ', '', regex=False).str.replace('Cocopan ', '', regex=False)
                disp['Platform'] = filt['platform']

                def fmt_u(u):
                    try:
                        u = float(u)
                    except Exception:
                        return "—"
                    if u >= 95:
                        return f"🟢 {u:.1f}%"
                    elif u >= 80:
                        return f"🟡 {u:.1f}%"
                    else:
                        return f"🔴 {u:.1f}%"

                disp['Uptime'] = [fmt_u(u) for u in filt['uptime_percentage']]
                disp['Total Checks'] = filt['effective_checks'].astype(str)
                disp['Times Down'] = filt['downtime_count'].astype(str)

                sort_vals = filt['uptime_percentage'].astype(float)
                idx = sort_vals.sort_values(ascending=(sort_order == "Lowest to Highest")).index
                disp = disp.loc[idx].reset_index(drop=True)

                st.dataframe(disp, use_container_width=True, hide_index=True, height=420)
        else:
            st.info("Performance analytics will appear as new data comes in.")

    # ----- TAB 3: DOWNTIME EVENTS (SNAPSHOT) -----
    with tab3:
        st.markdown(f"""
        <div class="section-header">
            <div class="section-title">Downtime Events Analysis</div>
            <div class="section-subtitle">Overview of offline events and frequency patterns (snapshot)</div>
        </div>
        """, unsafe_allow_html=True)

        downtime, dt_err = load_downtime_today()
        if dt_err:
            st.error(f"Error loading downtime events: {dt_err}")

        if downtime is None or downtime.empty:
            st.success("✅ No downtime events recorded today.")
        else:
            st.markdown('<div class="filter-container">', unsafe_allow_html=True)
            platforms = sorted(downtime['platform'].dropna().unique())
            options = ["All Platforms"] + platforms
            pf = st.selectbox("Filter by Platform:", options, key="down_platform_filter")
            st.markdown('</div>', unsafe_allow_html=True)

            data = downtime.copy()
            if pf != "All Platforms":
                data = data[data['platform'] == pf]

            if len(data) == 0:
                st.info(f"📊 No downtime events for {pf} today ({last_check_time.strftime('%B %d, %Y')}).")
            else:
                disp = pd.DataFrame()
                disp['Branch'] = data['name'].str.replace('Cocopan - ', '', regex=False).str.replace('Cocopan ', '', regex=False)
                disp['Platform'] = data['platform']

                sev = []
                for n in data['downtime_events']:
                    if n >= 5:
                        sev.append(f"🔴 {n} events")
                    elif n >= 3:
                        sev.append(f"🟡 {n} events")
                    else:
                        sev.append(f"🟢 {n} events")
                disp['Offline Events'] = sev

                try:
                    data['first_downtime'] = pd.to_datetime(data['first_downtime'])
                    data['last_downtime']  = pd.to_datetime(data['last_downtime'])
                    ph_tz = config.get_timezone()
                    if data['first_downtime'].dt.tz is None:
                        data['first_downtime'] = data['first_downtime'].dt.tz_localize('UTC')
                        data['last_downtime']  = data['last_downtime'].dt.tz_localize('UTC')
                    data['first_downtime'] = data['first_downtime'].dt.tz_convert(ph_tz)
                    data['last_downtime']  = data['last_downtime'].dt.tz_convert(ph_tz)
                    disp['First Offline'] = data['first_downtime'].dt.strftime('%I:%M %p')
                    disp['Last Offline']  = data['last_downtime'].dt.strftime('%I:%M %p')
                except Exception:
                    disp['First Offline'] = '—'
                    disp['Last Offline'] = '—'

                st.dataframe(disp, use_container_width=True, hide_index=True, height=420)

    # ----- TAB 4: REPORTS (SNAPSHOT) -----
    with tab4:
        st.markdown(f"""
        <div class="section-header">
            <div class="section-title">Store Uptime Reports</div>
            <div class="section-subtitle">Historical uptime analysis • Excludes 'Under Review' periods • Available from September 10, 2024</div>
        </div>
        """, unsafe_allow_html=True)

        st.markdown('<div class="filter-container">', unsafe_allow_html=True)
        col1, col2, col3 = st.columns([2, 2, 1])
        ph_now = config.get_current_time()
        min_date = datetime(2024, 9, 10).date()
        
        with col1:
            default_start = max(min_date, (ph_now - timedelta(days=7)).date())
            start_date = st.date_input(
                "Start Date", 
                value=default_start, 
                min_value=min_date,
                key="reports_start_date"
            )
        with col2:
            default_end = ph_now.date()
            end_date = st.date_input(
                "End Date", 
                value=default_end, 
                min_value=min_date,
                key="reports_end_date"
            )
        with col3:
            st.markdown("<br>", unsafe_allow_html=True)
            generate_report = st.button("📊 Generate Report", use_container_width=True)
        st.markdown('</div>', unsafe_allow_html=True)

        if start_date < min_date:
            st.info(f"ℹ️ Reports are available starting from {min_date.strftime('%B %d, %Y')}. Start date adjusted automatically.")

        if start_date > end_date:
            st.error("❌ Start date must be before end date")
        elif not generate_report:
            st.info("Select a date range and click Generate Report.")
        else:
            reports_data, rep_err = load_reports_data(start_date, end_date)
            if rep_err:
                st.error(f"Error loading reports data: {rep_err}")
            elif reports_data is None or reports_data.empty:
                st.info("📭 No data available for the selected date range")
            else:
                total_stores = len(reports_data)
                stores_with_data = int((reports_data['effective_checks'] > 0).sum())
                stores_no_data = total_stores - stores_with_data
                avg_uptime = reports_data['uptime_percentage'].dropna().mean() if stores_with_data > 0 else 0.0

                st.markdown("### 📊 Report Summary")
                c1, c2, c3, c4 = st.columns(4)
                with c1:
                    st.metric("Date Range", f"{(end_date - start_date).days + 1} days")
                with c2:
                    st.metric("Stores with Data", f"{stores_with_data}/{total_stores}")
                with c3:
                    st.metric("Average Uptime", f"{avg_uptime:.1f}%" if stores_with_data > 0 else "N/A")
                with c4:
                    st.metric("Period", f"{start_date.strftime('%b %d')} - {end_date.strftime('%b %d')}")

                st.markdown('<div class="filter-container">', unsafe_allow_html=True)
                r1, r2 = st.columns([2, 2])
                with r1:
                    available_platforms = sorted(reports_data['platform'].dropna().unique())
                    platform_options = ["All Platforms"] + available_platforms
                    platform_filter_reports = st.selectbox("Filter by Platform:", platform_options, key="reports_platform_filter")
                with r2:
                    sort_options = ["Uptime (High to Low)", "Uptime (Low to High)", "Store Name (A-Z)", "Platform"]
                    sort_order = st.selectbox("Sort by:", sort_options, key="reports_sort_order")
                st.markdown('</div>', unsafe_allow_html=True)

                filtered_data = reports_data.copy()
                if platform_filter_reports != "All Platforms":
                    filtered_data = filtered_data[filtered_data['platform'] == platform_filter_reports]

                display_data = pd.DataFrame()
                display_data['Branch'] = filtered_data['name'].str.replace('Cocopan - ', '', regex=False).str.replace('Cocopan ', '', regex=False)
                display_data['Platform'] = filtered_data['platform']

                uptime_formatted = []
                for _, row in filtered_data.iterrows():
                    if row.get('effective_checks', 0) == 0 or pd.isna(row['uptime_percentage']):
                        uptime_formatted.append("📊 No Data")
                    else:
                        uptime = float(row['uptime_percentage'])
                        if uptime >= 95:
                            uptime_formatted.append(f"🟢 {uptime:.1f}%")
                        elif uptime >= 80:
                            uptime_formatted.append(f"🟡 {uptime:.1f}%")
                        else:
                            uptime_formatted.append(f"🔴 {uptime:.1f}%")
                display_data['Uptime'] = uptime_formatted

                if sort_order == "Uptime (High to Low)":
                    sort_values = []
                    for _, row in filtered_data.iterrows():
                        if row.get('effective_checks', 0) == 0 or pd.isna(row['uptime_percentage']):
                            sort_values.append(-1)
                        else:
                            sort_values.append(float(row['uptime_percentage']))
                    display_data['sort_helper'] = sort_values
                    display_data = display_data.sort_values('sort_helper', ascending=False).drop('sort_helper', axis=1)
                elif sort_order == "Uptime (Low to High)":
                    sort_values = []
                    for _, row in filtered_data.iterrows():
                        if row.get('effective_checks', 0) == 0 or pd.isna(row['uptime_percentage']):
                            sort_values.append(999)
                        else:
                            sort_values.append(float(row['uptime_percentage']))
                    display_data['sort_helper'] = sort_values
                    display_data = display_data.sort_values('sort_helper', ascending=True).drop('sort_helper', axis=1)
                elif sort_order == "Store Name (A-Z)":
                    display_data = display_data.sort_values('Branch')
                elif sort_order == "Platform":
                    display_data = display_data.sort_values(['Platform', 'Branch'])

                st.markdown("### 📈 Store Uptime Report")
                if len(display_data) == 0:
                    st.info(f"No stores found for {platform_filter_reports}")
                else:
                    st.dataframe(display_data, use_container_width=True, hide_index=True, height=420)
                    st.markdown("**Legend:** 🟢 Excellent (≥95%) • 🟡 Good (80–94%) • 🔴 Needs Attention (<80%) • 📊 No Data Available")

                    if stores_with_data > 0:
                        high_performers = int(((filtered_data['uptime_percentage'] >= 95) & filtered_data['uptime_percentage'].notna()).sum())
                        low_performers = int(((filtered_data['uptime_percentage'] < 80) & filtered_data['uptime_percentage'].notna()).sum())
                        st.markdown("### 📋 Quick Insights")
                        ic1, ic2, ic3 = st.columns(3)
                        with ic1:
                            st.markdown(f"**🟢 High Performers:** {high_performers} stores (≥95% uptime)")
                        with ic2:
                            st.markdown(f"**🔴 Need Attention:** {low_performers} stores (<80% uptime)")
                        with ic3:
                            if stores_no_data > 0:
                                st.markdown(f"**📊 No Data:** {stores_no_data} stores (no activity in period)")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        st.error(f"System Error: {e}")
        logger.error(f"Dashboard error: {e}")
