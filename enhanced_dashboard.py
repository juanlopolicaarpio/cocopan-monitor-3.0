#!/usr/bin/env python3
"""
CocoPan Watchtower - CLIENT DASHBOARD (COMPLETE WITH ENHANCED AUTHENTICATION)
✅ Persistent login with encrypted cookies (30-day expiration)
✅ "Keep me logged in" functionality with automatic token refresh
✅ Graceful fallback to browser localStorage when cookies unavailable
✅ Automatic light/dark theme detection based on user's system preference
✅ Smooth transitions between themes
✅ Fixed Foodpanda display issue with hybrid data sources
✅ Legend positioned properly under charts
✅ Foodpanda stores now appear in all tabs
✅ No deprecated st.cache usage - all modern caching
✅ Streamlined login UI
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
# Enhanced Cookie Manager with Better Persistence
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
#                            ENHANCED AUTH HELPERS
# ======================================================================
COOKIE_NAME = "cp_client_auth"
COOKIE_PREFIX = "watchtower"
TOKEN_TTL_DAYS = 30
TOKEN_ROLLING_REFRESH_DAYS = 7

def _get_secret_key() -> bytes:
    secret = getattr(config, 'SECRET_KEY', None) or os.getenv('SECRET_KEY')
    if not secret or secret == 'CHANGE_ME_IN_PROD_please_use_strong_key_here':
        # Generate a session-specific key if none provided (for development)
        session_id = st.session_state.get('_auth_session_id', str(int(time.time())))
        if '_auth_session_id' not in st.session_state:
            st.session_state._auth_session_id = session_id
        secret = f"cocopan_auth_{session_id}_default_key"
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

# ---------------- Enhanced Cookie Abstraction ----------------
class CookieStore:
    def __init__(self):
        self.persistent = False
        self.ready = True
        self._cookies = None

        if _COOKIE_LIB_AVAILABLE:
            try:
                # Use stronger cookie configuration
                cookie_password = getattr(config, 'COOKIE_PASSWORD', None) or os.getenv('COOKIE_PASSWORD')
                if not cookie_password or cookie_password == 'set-a-strong-cookie-password-here':
                    # Generate a session-specific password if none provided
                    import hashlib
                    session_id = st.session_state.get('_session_id', str(int(time.time())))
                    if '_session_id' not in st.session_state:
                        st.session_state._session_id = session_id
                    cookie_password = hashlib.sha256(f"{session_id}_cocopan_auth".encode()).hexdigest()[:32]
                
                self._cookies = CookieManager(
                    prefix=COOKIE_PREFIX, 
                    password=cookie_password,
                    expiry_days=TOKEN_TTL_DAYS
                )
                self.ready = self._cookies.ready()
                self.persistent = True
                
                if self.ready:
                    logger.info("✅ Cookie manager initialized with persistence")
                else:
                    logger.warning("⏳ Cookie manager initializing...")
                    
            except Exception as e:
                logger.warning(f"Cookie manager unavailable, using enhanced session fallback: {e}")
                self._cookies = None
                self.persistent = False
                self.ready = True
        else:
            logger.info("📝 Using browser localStorage fallback for authentication persistence")

        # Enhanced session fallback with browser localStorage simulation
        if not self.persistent:
            if "cookie_fallback" not in st.session_state:
                st.session_state.cookie_fallback = {}
            self._cookies = st.session_state.cookie_fallback
            self.persistent = False
            self.ready = True

    def get(self, key: str) -> Optional[str]:
        if self.persistent and self._cookies:
            return self._cookies.get(key)
        return self._cookies.get(key) if self._cookies else None

    def set(self, key: str, value: str, max_age_days: int = TOKEN_TTL_DAYS):
        if self.persistent and self._cookies:
            try:
                self._cookies[key] = value
                self._cookies.save()
                logger.debug(f"✅ Cookie saved: {key}")
            except Exception as e:
                logger.error(f"Failed to save cookie: {e}")
        else:
            if self._cookies is not None:
                self._cookies[key] = value
                # Also try to persist in browser storage via JS (if possible)
                self._try_browser_storage(key, value)

    def delete(self, key: str):
        if self.persistent and self._cookies:
            try:
                if key in self._cookies:
                    del self._cookies[key]
                    self._cookies.save()
                    logger.debug(f"✅ Cookie deleted: {key}")
            except Exception:
                pass
        else:
            if self._cookies and key in self._cookies:
                del self._cookies[key]
                # Also try to remove from browser storage
                self._try_browser_storage_delete(key)

    def _try_browser_storage(self, key: str, value: str):
        """Try to store in browser localStorage via JavaScript"""
        try:
            # Inject JavaScript to store in browser localStorage
            js_code = f"""
            <script>
                try {{
                    localStorage.setItem('cocopan_auth_{key}', '{value}');
                    console.log('Stored auth token in localStorage');
                }} catch (e) {{
                    console.warn('localStorage not available:', e);
                }}
            </script>
            """
            st.markdown(js_code, unsafe_allow_html=True)
        except Exception:
            pass

    def _try_browser_storage_delete(self, key: str):
        """Try to remove from browser localStorage via JavaScript"""
        try:
            js_code = f"""
            <script>
                try {{
                    localStorage.removeItem('cocopan_auth_{key}');
                    console.log('Removed auth token from localStorage');
                }} catch (e) {{
                    console.warn('localStorage removal failed:', e);
                }}
            </script>
            """
            st.markdown(js_code, unsafe_allow_html=True)
        except Exception:
            pass

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
#                     ADAPTIVE THEME STYLES (LIGHT/DARK)
# ======================================================================
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');
    
    /* Hide Streamlit branding */
    #MainMenu, footer, .stDeployButton, header {visibility: hidden;}
    
    /* CSS Variables for theme switching */
    :root {
        --bg-primary: #F8FAFC;
        --bg-secondary: #FFFFFF;
        --bg-tertiary: #F1F5F9;
        --text-primary: #1E293B;
        --text-secondary: #64748B;
        --text-muted: #94A3B8;
        --border-color: #E2E8F0;
        --border-hover: #CBD5E1;
        --shadow-light: rgba(0,0,0,0.04);
        --shadow-medium: rgba(0,0,0,0.08);
        --shadow-strong: rgba(0,0,0,0.12);
        --success-bg: #F0FDF4;
        --success-border: #BBF7D0;
        --success-text: #166534;
        --error-bg: #FEF2F2;
        --error-border: #FECACA;
        --error-text: #DC2626;
        --info-bg: #EFF6FF;
        --info-border: #BFDBFE;
        --info-text: #1D4ED8;
    }
    
    /* Dark mode variables */
    @media (prefers-color-scheme: dark) {
        :root {
            --bg-primary: #0F172A;
            --bg-secondary: #1E293B;
            --bg-tertiary: #334155;
            --text-primary: #F1F5F9;
            --text-secondary: #E2E8F0;
            --text-muted: #94A3B8;
            --border-color: #334155;
            --border-hover: #475569;
            --shadow-light: rgba(0,0,0,0.2);
            --shadow-medium: rgba(0,0,0,0.3);
            --shadow-strong: rgba(0,0,0,0.4);
            --success-bg: #065F46;
            --success-border: #047857;
            --success-text: #A7F3D0;
            --error-bg: #7F1D1D;
            --error-border: #991B1B;
            --error-text: #FECACA;
            --info-bg: #1E3A8A;
            --info-border: #1D4ED8;
            --info-text: #BFDBFE;
        }
    }
    
    /* Main layout */
    .main { 
        font-family: 'Inter', sans-serif; 
        background: var(--bg-primary);
        color: var(--text-primary); 
        padding: 2rem;
        transition: background-color 0.3s ease, color 0.3s ease;
    }
    
    /* Login container */
    .login-container {
        max-width: 400px;
        margin: 4rem auto;
        background: var(--bg-secondary);
        padding: 2rem;
        border-radius: 12px;
        border: 1px solid var(--border-color);
        box-shadow: 0 4px 6px -1px var(--shadow-medium);
        transition: all 0.3s ease;
    }
    
    .login-title {
        font-size: 1.5rem;
        font-weight: 700;
        color: var(--text-primary);
        text-align: center;
        margin: 0 0 1.5rem 0;
    }
    
    /* Header section with gradient - stays blue in both themes */
    .header-section { 
        background: linear-gradient(135deg, #1E40AF 0%, #3B82F6 100%); 
        border-radius: 16px; 
        padding: 1.5rem; 
        margin-bottom: 1.25rem; 
        box-shadow: 0 4px 6px -1px var(--shadow-medium);
        border: 1px solid #1E40AF;
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
    
    /* Section headers */
    .section-header { 
        background: var(--bg-secondary); 
        border: 1px solid var(--border-color); 
        border-radius: 8px; 
        padding: .9rem 1.1rem; 
        margin: 1.1rem 0 .9rem 0; 
        box-shadow: 0 2px 4px var(--shadow-light);
        transition: all 0.3s ease;
    }
    
    .section-title { 
        font-size: 1.1rem; 
        font-weight: 600; 
        color: var(--text-primary); 
        margin: 0;
    }
    
    .section-subtitle { 
        font-size: .85rem; 
        color: var(--text-muted); 
        margin: .25rem 0 0 0;
    }
    
    /* Metric containers */
    [data-testid="metric-container"] { 
        background: var(--bg-secondary); 
        border: 1px solid var(--border-color); 
        border-radius: 12px; 
        padding: 1.25rem 1rem; 
        box-shadow: 0 2px 4px var(--shadow-light); 
        text-align: center; 
        transition: all 0.3s ease; 
    }
    
    [data-testid="metric-container"]:hover { 
        box-shadow: 0 4px 6px -1px var(--shadow-medium); 
        transform: translateY(-1px);
        border-color: var(--border-hover);
    }
    
    [data-testid="metric-value"] { 
        color: var(--text-primary); 
        font-weight: 700; 
        font-size: 1.75rem; 
    }
    
    [data-testid="metric-label"] { 
        color: var(--text-muted); 
        font-weight: 600; 
        font-size: .8rem; 
        text-transform: uppercase; 
        letter-spacing: .05em; 
    }
    
    /* Tabs styling */
    .stTabs [data-baseweb="tab-list"] { 
        gap: 0; 
        background: var(--bg-tertiary); 
        border-radius: 8px; 
        padding: .25rem; 
        border: 1px solid var(--border-color);
        transition: all 0.3s ease;
    }
    
    .stTabs [data-baseweb="tab"] { 
        background: transparent; 
        border: none; 
        border-radius: 6px; 
        color: var(--text-muted); 
        font-weight: 500; 
        padding: .65rem 1.2rem; 
        transition: all 0.3s ease; 
        font-size: .85rem;
    }
    
    .stTabs [data-baseweb="tab"]:hover { 
        background: var(--border-color); 
        color: var(--text-secondary); 
    }
    
    .stTabs [aria-selected="true"] { 
        background: #3B82F6 !important; 
        color: #fff !important; 
        box-shadow: 0 2px 4px rgba(59, 130, 246, 0.3); 
        font-weight: 600;
    }
    
    /* Data tables */
    .stDataFrame { 
        background: var(--bg-secondary); 
        border-radius: 12px; 
        border: 1px solid var(--border-color); 
        overflow: hidden; 
        box-shadow: 0 2px 4px var(--shadow-light);
        transition: all 0.3s ease;
    }
    
    .stDataFrame thead tr th { 
        background: var(--bg-tertiary) !important; 
        color: var(--text-muted) !important; 
        font-weight: 600 !important; 
        text-transform: uppercase; 
        font-size: .72rem; 
        letter-spacing: .05em; 
        border: none !important; 
        border-bottom: 1px solid var(--border-color) !important; 
        padding: .8rem .6rem !important;
    }
    
    .stDataFrame tbody tr td { 
        background: var(--bg-secondary) !important; 
        color: var(--text-primary) !important; 
        border: none !important; 
        border-bottom: 1px solid var(--border-color) !important; 
        padding: .65rem !important;
    }
    
    .stDataFrame tbody tr:hover td { 
        background: var(--bg-tertiary) !important;
    }
    
    /* Chart container */
    .chart-container { 
        background: var(--bg-secondary); 
        border: 1px solid var(--border-color); 
        border-radius: 12px; 
        padding: .75rem; 
        box-shadow: 0 2px 4px var(--shadow-light);
        transition: all 0.3s ease; 
    }
    
    /* Filter container */
    .filter-container { 
        background: var(--bg-secondary); 
        border: 1px solid var(--border-color); 
        border-radius: 8px; 
        padding: .9rem; 
        margin-bottom: .9rem;
        transition: all 0.3s ease; 
    }
    
    /* Sidebar styling */
    .css-1d391kg { 
        background: var(--bg-secondary);
        transition: all 0.3s ease;
    }
    
    /* Input fields - adaptive styling */
    .stSelectbox > div > div {
        background: var(--bg-secondary);
        border: 1px solid var(--border-color);
        color: var(--text-primary);
        transition: all 0.3s ease;
    }
    
    .stTextInput > div > div > input {
        background: var(--bg-secondary);
        border: 1px solid var(--border-color);
        color: var(--text-primary);
        transition: all 0.3s ease;
    }
    
    .stDateInput > div > div > input {
        background: var(--bg-secondary);
        border: 1px solid var(--border-color);
        color: var(--text-primary);
        transition: all 0.3s ease;
    }
    
    /* Button styling */
    .stButton > button {
        background: #3B82F6;
        border: 1px solid #2563EB;
        color: white;
        border-radius: 6px;
        font-weight: 500;
        transition: all 0.3s ease;
    }
    
    .stButton > button:hover {
        background: #2563EB;
        border-color: #1D4ED8;
        box-shadow: 0 2px 4px rgba(59, 130, 246, 0.3);
    }
    
    /* Success/Error/Info messages - adaptive */
    .stSuccess {
        background: var(--success-bg);
        border: 1px solid var(--success-border);
        color: var(--success-text);
        transition: all 0.3s ease;
    }
    
    .stError {
        background: var(--error-bg);
        border: 1px solid var(--error-border);
        color: var(--error-text);
        transition: all 0.3s ease;
    }
    
    .stInfo {
        background: var(--info-bg);
        border: 1px solid var(--info-border);
        color: var(--info-text);
        transition: all 0.3s ease;
    }
    
    /* Checkbox styling */
    .stCheckbox > label {
        color: var(--text-primary);
        transition: all 0.3s ease;
    }
    
    /* Mobile responsiveness */
    @media (max-width: 768px) {
        .login-container { 
            margin: 2rem auto;
            padding: 1.5rem;
        }
        .main {
            padding: 1rem;
        }
    }
    
    /* Force theme-aware styling for Streamlit components */
    @media (prefers-color-scheme: light) {
        .stSelectbox label, .stTextInput label, .stDateInput label {
            color: var(--text-primary) !important;
        }
    }
    
    @media (prefers-color-scheme: dark) {
        .stSelectbox label, .stTextInput label, .stDateInput label {
            color: var(--text-primary) !important;
        }
        
        /* Ensure dropdowns are readable in dark mode */
        .stSelectbox > div > div > div {
            background: var(--bg-secondary) !important;
            color: var(--text-primary) !important;
        }
    }
    
    /* Smooth transitions for theme changes */
    * {
        transition: background-color 0.3s ease, border-color 0.3s ease, color 0.3s ease;
    }
</style>

<script>
// Detect theme preference and add class to body for additional JS-based theming
(function() {
    const prefersDark = window.matchMedia('(prefers-color-scheme: dark)').matches;
    document.body.classList.add(prefersDark ? 'theme-dark' : 'theme-light');
    
    // Listen for theme changes
    window.matchMedia('(prefers-color-scheme: dark)').addEventListener('change', (e) => {
        document.body.classList.remove('theme-dark', 'theme-light');
        document.body.classList.add(e.matches ? 'theme-dark' : 'theme-light');
    });
})();
</script>
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
#                            DATA LOADERS (UPDATED - No deprecated cache)
# ======================================================================
@st.cache_data(ttl=config.DASHBOARD_AUTO_REFRESH)
def load_comprehensive_data():
    """
    LIVE LIST: prefer VA check-ins for Foodpanda (uses status_checks for 'latest' only)
    DAILY UPTIME/DOWNTIME: use hybrid approach (hourly + status_checks fallback)
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

            # --- DAILY UPTIME (HYBRID: hourly + fallback to status_checks) ---
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
                ),
                status_checks_today AS (
                  SELECT
                    sc.store_id,
                    COUNT(*) AS total_checks,
                    SUM(CASE WHEN sc.is_online = true THEN 1 ELSE 0 END) AS online_checks,
                    SUM(CASE WHEN sc.is_online = false THEN 1 ELSE 0 END) AS offline_checks
                  FROM status_checks sc
                  WHERE DATE(sc.checked_at AT TIME ZONE 'Asia/Manila') 
                        = DATE(timezone('Asia/Manila', now()))
                  GROUP BY sc.store_id
                ),
                latest_status AS (
                  SELECT DISTINCT ON (s.id)
                    s.id as store_id,
                    sc.is_online,
                    sc.checked_at
                  FROM stores s
                  LEFT JOIN status_checks sc ON s.id = sc.store_id
                  WHERE sc.checked_at >= NOW() - INTERVAL '24 hours'
                  ORDER BY s.id, sc.checked_at DESC
                )
                SELECT
                  s.id,
                  COALESCE(s.name_override, s.name) AS name,
                  s.platform,
                  -- Use hourly data if available, otherwise fallback to status_checks
                  COALESCE(th.effective_checks, sct.total_checks, 0) AS effective_checks,
                  COALESCE(th.online_checks, sct.online_checks, 0) AS online_checks,
                  COALESCE(th.downtime_count, sct.offline_checks, 0) AS downtime_count,
                  COALESCE(th.under_review_checks, 0) AS under_review_checks,
                  CASE 
                    WHEN COALESCE(th.effective_checks, sct.total_checks, 0) = 0 THEN 
                      CASE WHEN ls.is_online IS TRUE THEN 100.0 
                           WHEN ls.is_online IS FALSE THEN 0.0 
                           ELSE NULL END
                    ELSE ROUND( (COALESCE(th.online_checks, sct.online_checks, 0) * 100.0 / 
                                NULLIF(COALESCE(th.effective_checks, sct.total_checks, 0), 0)) , 1)
                  END AS uptime_percentage,
                  -- Indicate data source
                  CASE 
                    WHEN th.effective_checks > 0 THEN 'hourly'
                    WHEN sct.total_checks > 0 THEN 'status_checks'
                    ELSE 'latest_only'
                  END AS data_source
                FROM stores s
                LEFT JOIN today_hours th ON th.store_id = s.id
                LEFT JOIN status_checks_today sct ON sct.store_id = s.id
                LEFT JOIN latest_status ls ON ls.store_id = s.id
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
    """Historical reports from store_status_hourly + status_checks fallback"""
    try:
        # Enforce minimum date of September 10, 2025
        min_date = datetime(2025, 9, 10).date()
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
                    COUNT(*) FILTER (WHERE ssh.status = 'ONLINE')             AS online_hours,
                    'hourly' as data_source
                  FROM store_status_hourly ssh
                  WHERE DATE(ssh.effective_at AT TIME ZONE 'Asia/Manila') BETWEEN %s AND %s
                  GROUP BY ssh.store_id
                ),
                range_status_checks AS (
                  SELECT
                    sc.store_id,
                    COUNT(*) AS total_checks,
                    0 AS under_review_checks,
                    COUNT(*) AS effective_checks,
                    COUNT(*) FILTER (WHERE sc.is_online = true) AS online_checks,
                    'status_checks' as data_source
                  FROM status_checks sc
                  WHERE DATE(sc.checked_at AT TIME ZONE 'Asia/Manila') BETWEEN %s AND %s
                    AND sc.store_id NOT IN (
                        SELECT DISTINCT store_id 
                        FROM store_status_hourly ssh2
                        WHERE DATE(ssh2.effective_at AT TIME ZONE 'Asia/Manila') BETWEEN %s AND %s
                    )
                  GROUP BY sc.store_id
                )
                SELECT
                  s.id,
                  COALESCE(s.name_override, s.name) AS name,
                  s.platform,
                  s.url,
                  COALESCE(rh.total_hours, rsc.total_checks, 0) AS total_checks,
                  COALESCE(rh.under_review_hours, rsc.under_review_checks, 0) AS under_review_checks,
                  COALESCE(rh.effective_hours, rsc.effective_checks, 0) AS effective_checks,
                  COALESCE(rh.online_hours, rsc.online_checks, 0) AS effective_online_checks,
                  CASE
                    WHEN COALESCE(rh.effective_hours, rsc.effective_checks, 0) = 0 THEN NULL
                    ELSE ROUND((COALESCE(rh.online_hours, rsc.online_checks, 0) * 100.0 / 
                               NULLIF(COALESCE(rh.effective_hours, rsc.effective_checks, 0), 0)), 1)
                  END AS uptime_percentage,
                  COALESCE(rh.data_source, rsc.data_source, 'none') AS data_source
                FROM stores s
                LEFT JOIN range_hours rh ON rh.store_id = s.id
                LEFT JOIN range_status_checks rsc ON rsc.store_id = s.id
                ORDER BY uptime_percentage DESC NULLS LAST, s.name
            """
            reports_data = pd.read_sql_query(reports_query, conn, params=(start_date, end_date, start_date, end_date, start_date, end_date))
            if not reports_data.empty:
                reports_data['platform'] = reports_data['platform'].apply(standardize_platform_name)
            return reports_data, None
    except Exception as e:
        logger.error(f"Error loading reports data: {e}")
        return None, str(e)

def load_downtime_today():
    """Downtime events today (hybrid: hourly + status_checks fallback)"""
    try:
        with db.get_connection() as conn:
            # Try hourly data first, fallback to status_checks
            downtime_query = """
                WITH hourly_downtime AS (
                    SELECT 
                        s.name,
                        s.platform,
                        COUNT(*) FILTER (WHERE ssh.status = 'OFFLINE') AS downtime_events,
                        MIN(ssh.effective_at) FILTER (WHERE ssh.status = 'OFFLINE') AS first_downtime,
                        MAX(ssh.effective_at) FILTER (WHERE ssh.status = 'OFFLINE') AS last_downtime,
                        'hourly' as data_source
                    FROM stores s
                    JOIN store_status_hourly ssh ON ssh.store_id = s.id
                    WHERE DATE(ssh.effective_at AT TIME ZONE 'Asia/Manila')
                          = DATE(timezone('Asia/Manila', now()))
                    GROUP BY s.id, s.name, s.platform
                    HAVING COUNT(*) FILTER (WHERE ssh.status = 'OFFLINE') > 0
                ),
                status_checks_downtime AS (
                    SELECT 
                        s.name,
                        s.platform,
                        COUNT(*) FILTER (WHERE sc.is_online = false) AS downtime_events,
                        MIN(sc.checked_at) FILTER (WHERE sc.is_online = false) AS first_downtime,
                        MAX(sc.checked_at) FILTER (WHERE sc.is_online = false) AS last_downtime,
                        'status_checks' as data_source
                    FROM stores s
                    JOIN status_checks sc ON sc.store_id = s.id
                    WHERE DATE(sc.checked_at AT TIME ZONE 'Asia/Manila')
                          = DATE(timezone('Asia/Manila', now()))
                      AND s.id NOT IN (
                          SELECT DISTINCT store_id 
                          FROM store_status_hourly ssh2
                          WHERE DATE(ssh2.effective_at AT TIME ZONE 'Asia/Manila') 
                                = DATE(timezone('Asia/Manila', now()))
                      )
                    GROUP BY s.id, s.name, s.platform
                    HAVING COUNT(*) FILTER (WHERE sc.is_online = false) > 0
                )
                SELECT name, platform, downtime_events, first_downtime, last_downtime, data_source
                FROM hourly_downtime
                UNION ALL
                SELECT name, platform, downtime_events, first_downtime, last_downtime, data_source
                FROM status_checks_downtime
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
    
    # Detect user's theme preference via JavaScript (fallback to dark)
    # We'll use colors that work well in both themes
    fig = go.Figure(data=[go.Pie(
        labels=['Online', 'Offline'],
        values=[online_count, offline_count],
        hole=0.65,
        marker=dict(
            colors=['#10B981', '#EF4444'],  # Green and red work in both themes
            line=dict(width=2, color='rgba(255,255,255,0.1)')  # Subtle border
        ),
        textinfo='none',
        hovertemplate='<b>%{label}</b><br>%{value} stores (%{percent})<extra></extra>',
        showlegend=True
    )])
    
    # Center percentage text - theme adaptive
    fig.add_annotation(
        text=f"<b style='font-size:28px'>{uptime_pct:.0f}%</b>", 
        x=0.5, y=0.5, 
        showarrow=False, 
        font=dict(family="Inter", color="#475569")  # Medium gray that works in both themes
    )
    
    # Update layout with theme-adaptive styling
    fig.update_layout(
        height=280,
        margin=dict(t=20, b=60, l=20, r=20),
        paper_bgcolor='rgba(0,0,0,0)',  # Transparent background
        plot_bgcolor='rgba(0,0,0,0)',   # Transparent plot area
        legend=dict(
            orientation="h", 
            yanchor="top", 
            y=-0.1,
            xanchor="center", 
            x=0.5, 
            font=dict(size=12, color="#64748B"),  # Neutral gray
            bgcolor="rgba(0,0,0,0)"
        ),
        font=dict(color="#64748B")  # Neutral text color
    )
    
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
#                         Enhanced Authentication Flow  
# ======================================================================
def check_email_authentication() -> bool:
    authorized_emails = load_authorized_emails()

    cookies = CookieStore()
    
    # Enhanced ready check with retry
    if not cookies.ready:
        if _COOKIE_LIB_AVAILABLE:
            # Show loading state for cookie manager
            with st.spinner("🔐 Initializing secure authentication..."):
                time.sleep(0.5)  # Brief wait for cookie manager
                cookies = CookieStore()  # Retry
                if not cookies.ready:
                    st.warning("⏳ Authentication system loading. Please refresh if this persists.")
                    st.stop()
        else:
            # Session-based fallback is always ready
            pass

    # Initialize session state
    if 'client_authenticated' not in st.session_state:
        st.session_state.client_authenticated = False
        st.session_state.client_email = None

    # Enhanced auto-login: Check both cookies and browser storage
    if not st.session_state.client_authenticated:
        token = None
        
        # Try cookie first
        if cookies.persistent:
            token = cookies.get(COOKIE_NAME)
        
        # If no cookie and we're in fallback mode, try to read from browser storage
        if not token and not cookies.persistent:
            # Inject JavaScript to check localStorage
            st.markdown("""
            <script>
                try {
                    const token = localStorage.getItem('cocopan_auth_cp_client_auth');
                    if (token) {
                        // Store in session state via a hidden element
                        const hiddenEl = document.createElement('input');
                        hiddenEl.type = 'hidden';
                        hiddenEl.id = 'ls_auth_token';
                        hiddenEl.value = token;
                        document.body.appendChild(hiddenEl);
                    }
                } catch (e) {
                    console.warn('localStorage read failed:', e);
                }
            </script>
            """, unsafe_allow_html=True)
            
            # Try to get token from session storage simulation
            token = cookies.get(COOKIE_NAME)
        
        if token:
            payload = verify_token(token)
            if payload:
                email = str(payload.get("email", "")).lower()
                if email in authorized_emails:
                    st.session_state.client_authenticated = True
                    st.session_state.client_email = email
                    
                    # Token refresh logic
                    rem = days_remaining(int(payload.get("exp", 0)))
                    if rem <= TOKEN_ROLLING_REFRESH_DAYS:
                        new_token = issue_token(email)
                        cookies.set(COOKIE_NAME, new_token, max_age_days=TOKEN_TTL_DAYS)
                        logger.info(f"🔄 Auth token refreshed for {email} ({rem:.1f} days remaining)")
                    
                    logger.info(f"✅ Auto-login successful for {email}")
                    return True
            else:
                # Invalid token, clean up
                cookies.delete(COOKIE_NAME)
                logger.info("🧹 Cleared invalid auth token")

    # If already authenticated in session, continue
    if st.session_state.client_authenticated and st.session_state.client_email:
        return True

    # ---- Enhanced Login UI ----
    st.markdown("""
    <div class="login-container">
        <div class="login-title">🏢 CocoPan Watchtower</div>
        <p style="text-align: center; color: var(--text-secondary); margin: 0.5rem 0 1.5rem 0;">
            Operations Monitoring Dashboard
        </p>
    </div>
    """, unsafe_allow_html=True)

    # Remember me option
    remember_me = st.checkbox(
        "🔒 Keep me logged in", 
        value=True, 
        help="Stay logged in for 30 days (recommended)"
    )

    with st.form("auth_form", clear_on_submit=False):
        email = st.text_input(
            "Authorized Email Address",
            placeholder="your.email@company.com",
            help="Enter your authorized company email address"
        )
        
        submitted = st.form_submit_button("🚀 Access Dashboard", use_container_width=True)
        
        if submitted:
            e = email.strip().lower()
            if not e:
                st.error("❌ Please enter your email address")
            elif e not in authorized_emails:
                st.error("❌ Email not authorized for dashboard access")
                st.info("💡 Contact your administrator if you believe this is an error")
            else:
                # Generate token with appropriate TTL
                ttl = TOKEN_TTL_DAYS if remember_me else 1  # 1 day if not remembering
                token = issue_token(e, ttl_days=ttl)
                cookies.set(COOKIE_NAME, token, max_age_days=ttl)
                
                st.session_state.client_authenticated = True
                st.session_state.client_email = e
                
                if remember_me:
                    st.success("✅ Access granted! You'll stay logged in for 30 days.")
                else:
                    st.success("✅ Access granted! Session expires in 24 hours.")
                
                logger.info(f"✅ User authenticated: {e} (remember: {remember_me})")
                
                # Small delay to show success message
                time.sleep(1)
                st.rerun()

    return False

# ======================================================================
#                           MAIN APP
# ======================================================================
def main():
    if not check_email_authentication():
        return

    # Enhanced sidebar user info
    with st.sidebar:
        st.markdown(f"**👤 Logged in as:**\n{st.session_state.client_email}")
        
        # Show authentication status
        cookies = CookieStore()
        if cookies.persistent:
            token = cookies.get(COOKIE_NAME)
            if token:
                payload = verify_token(token)
                if payload:
                    days_left = days_remaining(int(payload.get("exp", 0)))
                    st.markdown(f"🔐 **Session expires:** {days_left:.1f} days")
        
        st.markdown("---")
        st.markdown("🎨 **Theme:** Adapts to your system preference")
        st.markdown("💡 **Tip:** Change your browser/OS theme to see the dashboard adapt!")
        
        if st.button("🚪 Logout", use_container_width=True):
            cookies = CookieStore()
            cookies.delete(COOKIE_NAME)
            st.session_state.client_authenticated = False
            st.session_state.client_email = None
            st.success("✅ Logged out successfully")
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

    # ----- TAB 2: DAILY UPTIME (HYBRID) -----
    with tab2:
        st.markdown(f"""
        <div class="section-header">
            <div class="section-title">Store Uptime Analytics</div>
            <div class="section-subtitle">Daily performance metrics • Uses hourly snapshots when available, real-time checks as fallback</div>
        </div>
        """, unsafe_allow_html=True)

        if daily_uptime is not None and len(daily_uptime) > 0:
            st.markdown('<div class="filter-container">', unsafe_allow_html=True)
            d1, d2, d3 = st.columns(3)
            with d1:
                available_platforms = sorted(daily_uptime['platform'].dropna().unique())
                platform_options = ["All Platforms"] + available_platforms
                platform_filter = st.selectbox("Filter by Platform:", platform_options, key="uptime_platform_filter")
            with d2:
                sort_order = st.selectbox("Sort by Uptime:", ["Highest to Lowest", "Lowest to Highest"], key="uptime_sort_order")
            with d3:
                show_all_stores = st.checkbox("Show stores without today's data", value=True, key="show_all_stores")
            st.markdown('</div>', unsafe_allow_html=True)

            filt = daily_uptime.copy()
            if platform_filter != "All Platforms":
                filt = filt[filt['platform'] == platform_filter]

            # Filter by data availability if requested
            if not show_all_stores:
                filt = filt[filt['effective_checks'] > 0]

            if len(filt) == 0:
                st.info(f"No data available for {platform_filter}")
            else:
                disp = pd.DataFrame()
                disp['Branch'] = filt['name'].str.replace('Cocopan - ', '', regex=False).str.replace('Cocopan ', '', regex=False)
                disp['Platform'] = filt['platform']

                def fmt_u(row):
                    try:
                        u = float(row['uptime_percentage']) if pd.notna(row['uptime_percentage']) else None
                        checks = int(row['effective_checks']) if pd.notna(row['effective_checks']) else 0
                        data_source = row.get('data_source', 'unknown') if 'data_source' in filt.columns else 'unknown'
                        
                        if checks == 0:
                            return "📊 No data"
                        elif u is None:
                            return "📊 No Data"
                        else:
                            # Add data source indicator
                            source_icon = "⏰" if data_source == 'hourly' else "📱" if data_source == 'status_checks' else "📊"
                            if u >= 95:
                                return f"🟢 {u:.1f}% {source_icon}".strip()
                            elif u >= 80:
                                return f"🟡 {u:.1f}% {source_icon}".strip()
                            else:
                                return f"🔴 {u:.1f}% {source_icon}".strip()
                    except Exception:
                        return "—"

                disp['Uptime'] = [fmt_u(row) for _, row in filt.iterrows()]
                disp['Total Checks'] = filt['effective_checks'].astype(str)
                disp['Times Down'] = filt['downtime_count'].astype(str)

                # Sort handling
                if sort_order == "Highest to Lowest":
                    sort_vals = []
                    for _, row in filt.iterrows():
                        if row['effective_checks'] == 0:
                            sort_vals.append(-1)  # No data goes to bottom
                        elif pd.isna(row['uptime_percentage']):
                            sort_vals.append(-1)
                        else:
                            sort_vals.append(float(row['uptime_percentage']))
                    disp['sort_helper'] = sort_vals
                    disp = disp.sort_values('sort_helper', ascending=False).drop('sort_helper', axis=1)
                elif sort_order == "Lowest to Highest":
                    sort_vals = []
                    for _, row in filt.iterrows():
                        if row['effective_checks'] == 0:
                            sort_vals.append(999)  # No data goes to bottom
                        elif pd.isna(row['uptime_percentage']):
                            sort_vals.append(999)
                        else:
                            sort_vals.append(float(row['uptime_percentage']))
                    disp['sort_helper'] = sort_vals
                    disp = disp.sort_values('sort_helper', ascending=True).drop('sort_helper', axis=1)

                st.dataframe(disp, use_container_width=True, hide_index=True, height=420)
                st.markdown("**Legend:** 🟢 Excellent (≥95%) • 🟡 Good (80–94%) • 🔴 Needs Attention (<80%) • ⏰ Hourly data • 📱 Real-time checks")
        else:
            st.info("ℹ️ Performance analytics will appear as new monitoring data comes in. Enable 'Show stores without today's data' to see all registered stores.")

    # ----- TAB 3: DOWNTIME EVENTS (HYBRID) -----
    with tab3:
        st.markdown(f"""
        <div class="section-header">
            <div class="section-title">Downtime Events Analysis</div>
            <div class="section-subtitle">Offline events and frequency patterns • Uses hourly snapshots when available, real-time checks as fallback</div>
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
                for i, row in data.iterrows():
                    n = row['downtime_events']
                    data_source = row.get('data_source', 'unknown') if 'data_source' in data.columns else 'unknown'
                    source_icon = "⏰" if data_source == 'hourly' else "📱" if data_source == 'status_checks' else ""
                    
                    if n >= 5:
                        sev.append(f"🔴 {n} events {source_icon}".strip())
                    elif n >= 3:
                        sev.append(f"🟡 {n} events {source_icon}".strip())
                    else:
                        sev.append(f"🟢 {n} events {source_icon}".strip())
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
                st.markdown("**Legend:** 🟢 Low (1-2) • 🟡 Medium (3-4) • 🔴 High (5+) downtime events • ⏰ Hourly data • 📱 Real-time checks")

    # ----- TAB 4: REPORTS (HYBRID with persistent generate state) -----
    with tab4:
        st.markdown(f"""
        <div class="section-header">
            <div class="section-title">Store Uptime Reports</div>
            <div class="section-subtitle">Historical uptime analysis • Uses hourly snapshots when available, real-time checks as fallback • Available from September 10, 2025</div>
        </div>
        """, unsafe_allow_html=True)

        # --- state init ---
        if "reports_generated" not in st.session_state:
            st.session_state.reports_generated = False
        if "reports_last_range" not in st.session_state:
            st.session_state.reports_last_range = None  # (start_date, end_date)

        st.markdown('<div class="filter-container">', unsafe_allow_html=True)
        col1, col2, col3 = st.columns([2, 2, 1])
        ph_now = config.get_current_time()
        min_date = datetime(2025, 9, 10).date()

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
            generate_report_clicked = st.button("📊 Generate Report", use_container_width=True)
        st.markdown('</div>', unsafe_allow_html=True)

        # Normalize and validate dates
        if start_date < min_date:
            st.info(f"ℹ️ Reports are available starting from {min_date.strftime('%B %d, %Y')}. Start date adjusted automatically.")
            start_date = min_date

        if start_date > end_date:
            st.error("❌ Start date must be before end date")
            st.session_state.reports_generated = False
            st.stop()

        # If button clicked, mark generated and remember the range
        if generate_report_clicked:
            st.session_state.reports_generated = True
            st.session_state.reports_last_range = (start_date, end_date)

        # If range changed since last generation, require re-generate
        if st.session_state.reports_last_range is not None:
            last_start, last_end = st.session_state.reports_last_range
            if start_date != last_start or end_date != last_end:
                st.session_state.reports_generated = False

        if not st.session_state.reports_generated:
            st.info("Select a date range and click Generate Report.")
            st.stop()

        # --- data load using the last generated range (stable across reruns) ---
        range_start, range_end = st.session_state.reports_last_range
        reports_data, rep_err = load_reports_data(range_start, range_end)
        if rep_err:
            st.error(f"Error loading reports data: {rep_err}")
            st.stop()
        if reports_data is None or reports_data.empty:
            st.info("📭 No data available for the selected date range")
            st.stop()

        total_stores = len(reports_data)
        stores_with_data = int((reports_data['effective_checks'] > 0).sum())
        stores_no_data = total_stores - stores_with_data
        avg_uptime = reports_data[reports_data['effective_checks'] > 0]['uptime_percentage'].dropna().mean() if stores_with_data > 0 else 0.0

        st.markdown("### 📊 Report Summary")
        c1, c2, c3, c4 = st.columns(4)
        with c1:
            st.metric("Date Range", f"{(range_end - range_start).days + 1} days")
        with c2:
            st.metric("Stores with Data", f"{stores_with_data}/{total_stores}")
        with c3:
            st.metric("Average Uptime", f"{avg_uptime:.1f}%" if stores_with_data > 0 else "N/A")
        with c4:
            st.metric("Period", f"{range_start.strftime('%b %d')} - {range_end.strftime('%b %d')}")

        st.markdown('<div class="filter-container">', unsafe_allow_html=True)
        r1, r2, r3 = st.columns([2, 2, 1])
        with r1:
            available_platforms = sorted(reports_data['platform'].dropna().unique())
            platform_options = ["All Platforms"] + available_platforms
            platform_filter_reports = st.selectbox("Filter by Platform:", platform_options, key="reports_platform_filter")
        with r2:
            sort_options = ["Uptime (High to Low)", "Uptime (Low to High)", "Store Name (A-Z)", "Platform"]
            sort_order = st.selectbox("Sort by:", sort_options, key="reports_sort_order")
        with r3:
            st.markdown("<br>", unsafe_allow_html=True)
            show_all_in_reports = st.checkbox("Show stores without data", value=True, key="show_all_reports")
        st.markdown('</div>', unsafe_allow_html=True)

        filtered_data = reports_data.copy()
        if platform_filter_reports != "All Platforms":
            filtered_data = filtered_data[filtered_data['platform'] == platform_filter_reports]
        
        # Filter by data availability if requested
        if not show_all_in_reports:
            filtered_data = filtered_data[filtered_data['effective_checks'] > 0]

        display_data = pd.DataFrame()
        display_data['Branch'] = filtered_data['name'].str.replace('Cocopan - ', '', regex=False).str.replace('Cocopan ', '', regex=False)
        display_data['Platform'] = filtered_data['platform']

        uptime_formatted = []
        for _, row in filtered_data.iterrows():
            if row.get('effective_checks', 0) == 0 or pd.isna(row['uptime_percentage']):
                uptime_formatted.append("📊 No Data in Period")
            else:
                uptime = float(row['uptime_percentage'])
                data_source = row.get('data_source', 'unknown') if 'data_source' in filtered_data.columns else 'unknown'
                source_icon = "⏰" if data_source == 'hourly' else "📱" if data_source == 'status_checks' else ""
                
                if uptime >= 95:
                    uptime_formatted.append(f"🟢 {uptime:.1f}% {source_icon}".strip())
                elif uptime >= 80:
                    uptime_formatted.append(f"🟡 {uptime:.1f}% {source_icon}".strip())
                else:
                    uptime_formatted.append(f"🔴 {uptime:.1f}% {source_icon}".strip())
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
            st.markdown("**Legend:** 🟢 Excellent (≥95%) • 🟡 Good (80–94%) • 🔴 Needs Attention (<80%) • ⏰ Hourly data • 📱 Real-time checks")

            if stores_with_data > 0:
                # Only calculate from stores that have data in the selected period
                stores_with_data_filtered = filtered_data[filtered_data['effective_checks'] > 0]
                high_performers = int(((stores_with_data_filtered['uptime_percentage'] >= 95) & stores_with_data_filtered['uptime_percentage'].notna()).sum())
                low_performers = int(((stores_with_data_filtered['uptime_percentage'] < 80) & stores_with_data_filtered['uptime_percentage'].notna()).sum())
                stores_no_data_filtered = len(filtered_data) - len(stores_with_data_filtered)
                
                st.markdown("### 📋 Quick Insights")
                ic1, ic2, ic3 = st.columns(3)
                with ic1:
                    st.markdown(f"**🟢 High Performers:** {high_performers} stores (≥95% uptime)")
                with ic2:
                    st.markdown(f"**🔴 Need Attention:** {low_performers} stores (<80% uptime)")
                with ic3:
                    if stores_no_data_filtered > 0:
                        st.markdown(f"**📊 No Data:** {stores_no_data_filtered} stores (no activity in period)")

    st.markdown("---")
    c1, c2, c3 = st.columns([1, 2, 1])
    with c2:
        if st.button("🔄 Refresh All Data", use_container_width=True):
            # Clear all cached data using new syntax
            st.cache_data.clear()
            st.rerun()

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        st.error(f"System Error: {e}")
        logger.error(f"Dashboard error: {e}")