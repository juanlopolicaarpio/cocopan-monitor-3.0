#!/usr/bin/env python3
"""
CocoPan Watchtower - CLIENT DASHBOARD (ADAPTIVE THEME + CSV EXPORT)
✅ Added comprehensive CSV export functionality to Reports tab
✅ Export format: Store performance with detailed offline events
✅ Automatic light/dark theme detection based on user's system preference
✅ Smooth transitions between themes
✅ Fixed Foodpanda display issue with hybrid data sources
✅ Enhanced downtime analysis with expandable details
✅ Added SKU Dashboard redirect button
✅ Removed specific time displays (kept date and Manila Time)
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
import io

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
    
    /* Navigation link styling */
    .nav-link {
        background: var(--bg-secondary);
        border: 1px solid var(--border-color);
        border-radius: 8px;
        padding: 0.75rem;
        margin-bottom: 0.75rem;
        text-align: center;
        transition: all 0.3s ease;
    }
    
    .nav-link:hover {
        background: var(--bg-tertiary);
        border-color: var(--border-hover);
        transform: translateY(-1px);
        box-shadow: 0 2px 4px var(--shadow-light);
    }
    
    .nav-link a {
        color: var(--text-primary);
        text-decoration: none;
        font-weight: 500;
        font-size: 0.9rem;
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
#                        ENHANCED FORMATTING FUNCTIONS
# ======================================================================
def format_offline_hours(offline_times_array, max_display=5):
    """Format offline times for display with smart truncation - ENHANCED for large displays"""
    
    # Handle None/NaN cases first
    if offline_times_array is None:
        return "—"
    
    # Handle pandas Series/array NaN check properly
    try:
        if hasattr(offline_times_array, '__iter__') and not isinstance(offline_times_array, str):
            # It's an array-like object (pandas Series, numpy array, list)
            if hasattr(offline_times_array, 'isna'):
                # Pandas Series - check if all values are NaN
                if offline_times_array.isna().all():
                    return "—"
            elif hasattr(offline_times_array, '__len__'):
                # List or numpy array - check if empty
                if len(offline_times_array) == 0:
                    return "—"
            
            # Check if it's an empty pandas Series
            if hasattr(offline_times_array, 'empty') and offline_times_array.empty:
                return "—"
                
        else:
            # It's a single value or string
            if pd.isna(offline_times_array):
                return "—"
    except Exception:
        # If any check fails, assume it's a single value
        if pd.isna(offline_times_array):
            return "—"
    
    try:
        # Handle PostgreSQL array format (string like "{time1,time2,time3}")
        if isinstance(offline_times_array, str):
            # Remove curly braces and split
            times_str = offline_times_array.strip('{}')
            if not times_str or times_str.strip() == '':
                return "—"
            time_strings = [t.strip('"\'') for t in times_str.split(',') if t.strip()]
        else:
            # Handle pandas Series, list, or numpy array
            if hasattr(offline_times_array, 'tolist'):
                # Pandas Series or numpy array
                time_strings = offline_times_array.tolist()
            elif hasattr(offline_times_array, '__iter__'):
                # List or other iterable
                time_strings = list(offline_times_array)
            else:
                # Single value
                time_strings = [offline_times_array]
        
        # Filter out None/NaN/empty values
        time_strings = [t for t in time_strings if t is not None and not pd.isna(t) and str(t).strip() != '']
        
        if not time_strings:
            return "—"
        
        # Convert to datetime and format
        ph_tz = config.get_timezone()
        formatted_times = []
        
        for time_str in time_strings:
            try:
                # Skip empty or null values
                if pd.isna(time_str) or str(time_str).strip() == '':
                    continue
                    
                dt = pd.to_datetime(time_str)
                if dt.tz is None:
                    dt = dt.tz_localize('UTC')
                dt_ph = dt.tz_convert(ph_tz)
                formatted_times.append(dt_ph.strftime('%I:%M %p'))
            except Exception as e:
                logger.debug(f"Error parsing time {time_str}: {e}")
                continue
        
        if not formatted_times:
            return "—"
        
        # Remove duplicates while preserving order
        seen = set()
        unique_times = []
        for time in formatted_times:
            if time not in seen:
                seen.add(time)
                unique_times.append(time)
        
        # Handle display based on max_display parameter
        if max_display >= 999:  # Show all times mode
            if len(unique_times) <= 15:
                # For reasonable amounts, show in a single line
                return ", ".join(unique_times)
            else:
                # For many times, organize better
                # Group times and show with line breaks for readability
                chunks = [unique_times[i:i+10] for i in range(0, len(unique_times), 10)]
                formatted_chunks = []
                for i, chunk in enumerate(chunks):
                    if i == 0:
                        formatted_chunks.append(", ".join(chunk))
                    else:
                        formatted_chunks.append(f"      {', '.join(chunk)}")  # Indent continuation lines
                return "\n".join(formatted_chunks)
        
        # Normal truncated display
        if len(unique_times) <= max_display:
            return ", ".join(unique_times)
        else:
            displayed = unique_times[:max_display]
            remaining = len(unique_times) - max_display
            return f"{', '.join(displayed)}... +{remaining} more"
            
    except Exception as e:
        logger.warning(f"Error formatting offline times: {e}")
        return "—"

# ======================================================================
#                        CSV EXPORT FUNCTIONS (NEW)
# ======================================================================
@st.cache_data(ttl=300)
def load_export_data(start_date, end_date):
    """Load comprehensive data for CSV export including downtime details"""
    try:
        # Enforce minimum date of September 10, 2025
        min_date = datetime(2025, 9, 10).date()
        if start_date < min_date:
            start_date = min_date
            
        with db.get_connection() as conn:
            export_query = """
                WITH range_hours AS (
                  SELECT
                    ssh.store_id,
                    COUNT(*)                           AS total_hours,
                    COUNT(*) FILTER (WHERE ssh.status IN ('BLOCKED','UNKNOWN','ERROR')) AS under_review_hours,
                    COUNT(*) FILTER (WHERE ssh.status IN ('ONLINE','OFFLINE')) AS effective_hours,
                    COUNT(*) FILTER (WHERE ssh.status = 'ONLINE')             AS online_hours,
                    COUNT(*) FILTER (WHERE ssh.status = 'OFFLINE')            AS offline_events,
                    ARRAY_AGG(
                        ssh.effective_at ORDER BY ssh.effective_at
                    ) FILTER (WHERE ssh.status = 'OFFLINE') AS offline_times,
                    AVG(ssh.response_ms) FILTER (WHERE ssh.response_ms IS NOT NULL) AS avg_response_time,
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
                    COUNT(*) FILTER (WHERE sc.is_online = false) AS offline_events,
                    ARRAY_AGG(
                        sc.checked_at ORDER BY sc.checked_at
                    ) FILTER (WHERE sc.is_online = false) AS offline_times,
                    AVG(sc.response_time_ms) FILTER (WHERE sc.response_time_ms IS NOT NULL) AS avg_response_time,
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
                  COALESCE(s.name_override, s.name) AS store_name,
                  s.platform,
                  s.url,
                  COALESCE(rh.total_hours, rsc.total_checks, 0) AS total_checks,
                  COALESCE(rh.under_review_hours, rsc.under_review_checks, 0) AS under_review_checks,
                  COALESCE(rh.effective_hours, rsc.effective_checks, 0) AS effective_checks,
                  COALESCE(rh.online_hours, rsc.online_checks, 0) AS effective_online_checks,
                  COALESCE(rh.offline_events, rsc.offline_events, 0) AS offline_events,
                  CASE
                    WHEN COALESCE(rh.effective_hours, rsc.effective_checks, 0) = 0 THEN NULL
                    ELSE ROUND((COALESCE(rh.online_hours, rsc.online_checks, 0) * 100.0 / 
                               NULLIF(COALESCE(rh.effective_hours, rsc.effective_checks, 0), 0)), 1)
                  END AS uptime_percentage,
                  COALESCE(rh.offline_times, rsc.offline_times) AS offline_times,
                  ROUND(COALESCE(rh.avg_response_time, rsc.avg_response_time, 0)::numeric, 0) AS avg_response_time,
                  COALESCE(rh.data_source, rsc.data_source, 'none') AS data_source
                FROM stores s
                LEFT JOIN range_hours rh ON rh.store_id = s.id
                LEFT JOIN range_status_checks rsc ON rsc.store_id = s.id
                ORDER BY uptime_percentage DESC NULLS LAST, s.name
            """
            export_data = pd.read_sql_query(export_query, conn, params=(start_date, end_date, start_date, end_date, start_date, end_date))
            if not export_data.empty:
                export_data['platform'] = export_data['platform'].apply(standardize_platform_name)
            return export_data, None
    except Exception as e:
        logger.error(f"Error loading export data: {e}")
        return None, str(e)

def format_offline_times_for_export(offline_times_array, start_date, end_date):
    """Format offline times specifically for CSV export with proper date handling"""
    if offline_times_array is None:
        return ""
    
    try:
        # Handle PostgreSQL array format
        if isinstance(offline_times_array, str):
            times_str = offline_times_array.strip('{}')
            if not times_str or times_str.strip() == '':
                return ""
            time_strings = [t.strip('"\'') for t in times_str.split(',') if t.strip()]
        else:
            if hasattr(offline_times_array, 'tolist'):
                time_strings = offline_times_array.tolist()
            elif hasattr(offline_times_array, '__iter__'):
                time_strings = list(offline_times_array)
            else:
                time_strings = [offline_times_array]
        
        # Filter and format
        time_strings = [t for t in time_strings if t is not None and not pd.isna(t) and str(t).strip() != '']
        
        if not time_strings:
            return ""
        
        # Determine if we need to show year based on date range
        show_year = start_date.year != end_date.year or start_date.year != datetime.now().year
        
        # Format for CSV export
        ph_tz = config.get_timezone()
        formatted_times = []
        
        for time_str in time_strings:
            try:
                dt = pd.to_datetime(time_str)
                if dt.tz is None:
                    dt = dt.tz_localize('UTC')
                dt_ph = dt.tz_convert(ph_tz)
                
                if show_year:
                    # Show year if spanning multiple years or not current year
                    formatted_time = dt_ph.strftime('%b %d %Y %I:%M%p').replace(' 0', ' ').replace('AM', 'AM').replace('PM', 'PM')
                else:
                    # Same year - don't show year
                    formatted_time = dt_ph.strftime('%b %d %I:%M%p').replace(' 0', ' ').replace('AM', 'AM').replace('PM', 'PM')
                
                formatted_times.append(formatted_time)
            except Exception as e:
                logger.debug(f"Error formatting time {time_str}: {e}")
                continue
        
        if not formatted_times:
            return ""
        
        # Join with pipe separator for clean CSV display
        return " | ".join(formatted_times)
        
    except Exception as e:
        logger.warning(f"Error formatting offline times for export: {e}")
        return ""

def create_export_csv(export_data, start_date, end_date):
    """Create CSV DataFrame for export"""
    if export_data is None or export_data.empty:
        return None
    
    # Create clean CSV DataFrame
    csv_data = pd.DataFrame()
    
    # Clean store names (remove Cocopan prefix)
    csv_data['Store_Name'] = export_data['store_name'].str.replace('Cocopan - ', '', regex=False).str.replace('Cocopan ', '', regex=False)
    csv_data['Platform'] = export_data['platform']
    
    # Format date range
    if start_date.year == end_date.year:
        csv_data['Period_Start'] = start_date.strftime('%b %d %Y').replace(' 0', ' ')
        csv_data['Period_End'] = end_date.strftime('%b %d %Y').replace(' 0', ' ')
    else:
        csv_data['Period_Start'] = start_date.strftime('%b %d %Y').replace(' 0', ' ')
        csv_data['Period_End'] = end_date.strftime('%b %d %Y').replace(' 0', ' ')
    
    # Performance metrics
    csv_data['Uptime_Percent'] = export_data['uptime_percentage'].fillna(0).round(1)
    csv_data['Total_Checks'] = export_data['effective_checks'].fillna(0).astype(int)
    csv_data['Offline_Count'] = export_data['offline_events'].fillna(0).astype(int)
    
    # Format offline events for CSV
    csv_data['All_Offline_Events'] = export_data['offline_times'].apply(
        lambda x: format_offline_times_for_export(x, start_date, end_date)
    )
    
    return csv_data

def generate_csv_content(csv_data):
    """Convert DataFrame to CSV string"""
    if csv_data is None:
        return None
    
    # Convert to CSV string
    output = io.StringIO()
    csv_data.to_csv(output, index=False, encoding='utf-8')
    csv_content = output.getvalue()
    output.close()
    
    return csv_content

def create_export_filename(start_date, end_date):
    """Create standardized filename for export"""
    if start_date == end_date:
        # Single day
        date_str = start_date.strftime('%Y-%m-%d')
    elif start_date.year == end_date.year and start_date.month == end_date.month:
        # Same month
        date_str = f"{start_date.strftime('%Y-%m-%d')}_{end_date.strftime('%d')}"
    else:
        # Different months/years
        date_str = f"{start_date.strftime('%Y-%m-%d')}_{end_date.strftime('%Y-%m-%d')}"
    
    return f"uptime_report_{date_str}.csv"

# ======================================================================
#                            DATA LOADERS
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
    """ENHANCED: Downtime events today with actual offline times (hybrid: hourly + status_checks fallback)"""
    try:
        with db.get_connection() as conn:
            # Try hourly data first, fallback to status_checks
            downtime_query = """
                WITH hourly_downtime AS (
                    SELECT 
                        s.id,
                        s.name,
                        s.platform,
                        COUNT(*) FILTER (WHERE ssh.status = 'OFFLINE') AS downtime_events,
                        ARRAY_AGG(
                            ssh.effective_at ORDER BY ssh.effective_at
                        ) FILTER (WHERE ssh.status = 'OFFLINE') AS offline_times,
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
                        s.id,
                        s.name,
                        s.platform,
                        COUNT(*) FILTER (WHERE sc.is_online = false) AS downtime_events,
                        ARRAY_AGG(
                            sc.checked_at ORDER BY sc.checked_at
                        ) FILTER (WHERE sc.is_online = false) AS offline_times,
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
                SELECT name, platform, downtime_events, offline_times, data_source
                FROM hourly_downtime
                UNION ALL
                SELECT name, platform, downtime_events, offline_times, data_source
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
        st.markdown("---")
        
        # Navigation to SKU Dashboard - NEW
        st.markdown("""
        <div class="nav-link">
            <a href="https://sku.up.railway.app/" target="_blank">
                📊 → SKU Dashboard
            </a>
        </div>
        """, unsafe_allow_html=True)
        
        # Also add as a regular button for better visibility
        if st.button("📊 SKU Dashboard", use_container_width=True):
            st.markdown("""
            <script>
            window.open('https://sku.up.railway.app/', '_blank');
            </script>
            """, unsafe_allow_html=True)
        
        st.markdown("---")
        st.markdown("🎨 **Theme:** Adapts to your system preference")
        st.markdown("💡 **Tip:** Change your browser/OS theme to see the dashboard adapt!")
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

    # Navigation button in main area for better visibility - NEW
    col1, col2, col3 = st.columns([1, 2, 1])
    with col1:
        if st.button("📊 → SKU Dashboard", use_container_width=True):
            st.markdown('<meta http-equiv="refresh" content="0; url=https://sku.up.railway.app/">', unsafe_allow_html=True)
            st.info("Redirecting to SKU Dashboard...")
    with col2:
        pass  # Center space
    with col3:
        pass  # Right space

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
    tab1, tab2, tab3, tab4 = st.tabs(["🔴 Live Operations Monitor", "📊 Store Uptime Analytics", "📉 Downtime Events", "📈 Reports & Export"])

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

    # ----- TAB 3: ENHANCED DOWNTIME EVENTS WITH EXPANDABLE OFFLINE HOURS -----
    with tab3:
        st.markdown(f"""
        <div class="section-header">
            <div class="section-title">Downtime Events Analysis</div>
            <div class="section-subtitle">Detailed offline periods and timing patterns • Click "Show All Times" to see complete offline history</div>
        </div>
        """, unsafe_allow_html=True)

        downtime, dt_err = load_downtime_today()
        if dt_err:
            st.error(f"Error loading downtime events: {dt_err}")

        if downtime is None or downtime.empty:
            st.success("✅ No downtime events recorded today.")
        else:
            st.markdown('<div class="filter-container">', unsafe_allow_html=True)
            col1, col2, col3 = st.columns([2, 2, 1])
            
            with col1:
                platforms = sorted(downtime['platform'].dropna().unique())
                options = ["All Platforms"] + platforms
                pf = st.selectbox("Filter by Platform:", options, key="down_platform_filter")
            
            with col2:
                sort_options = ["Most Events First", "Least Events First", "Store Name (A-Z)"]
                sort_by = st.selectbox("Sort by:", sort_options, key="downtime_sort")
            
            with col3:
                st.markdown("<br>", unsafe_allow_html=True)
                show_full_details = st.toggle("Show All Times", value=False, key="show_all_downtime_details")
            
            st.markdown('</div>', unsafe_allow_html=True)

            data = downtime.copy()
            if pf != "All Platforms":
                data = data[data['platform'] == pf]

            if len(data) == 0:
                st.info(f"📊 No downtime events for {pf} today ({last_check_time.strftime('%B %d, %Y')}).")
            else:
                # Sort data based on selection
                if sort_by == "Most Events First":
                    data = data.sort_values('downtime_events', ascending=False)
                elif sort_by == "Least Events First":
                    data = data.sort_values('downtime_events', ascending=True)
                elif sort_by == "Store Name (A-Z)":
                    data = data.sort_values('name')

                # Create summary table
                disp = pd.DataFrame()
                disp['Branch'] = data['name'].str.replace('Cocopan - ', '', regex=False).str.replace('Cocopan ', '', regex=False)
                disp['Platform'] = data['platform']

                # Enhanced severity with data source indicators
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

                # Format offline hours based on toggle
                offline_hours_formatted = []
                for i, row in data.iterrows():
                    if show_full_details:
                        # Show ALL times when toggle is on
                        formatted_hours = format_offline_hours(row.get('offline_times', None), max_display=999)
                    else:
                        # Show limited times when toggle is off
                        formatted_hours = format_offline_hours(row.get('offline_times', None), max_display=3)
                    offline_hours_formatted.append(formatted_hours)
                disp['Offline Hours'] = offline_hours_formatted

                st.dataframe(disp, use_container_width=True, hide_index=True, height=420)
                
                # Enhanced legend
                st.markdown("""
                **Severity Legend:** 🟢 Low (1-2) • 🟡 Medium (3-4) • 🔴 High (5+) downtime events  
                **Data Sources:** ⏰ Hourly snapshots • 📱 Real-time checks  
                **Offline Hours:** Toggle "Show All Times" above to see complete offline history for each store
                """)

                # DETAILED EXPANDABLE VIEW for stores with many offline events
                if not show_full_details:
                    st.markdown("### 🔍 Detailed View")
                    st.markdown("Click on a store below to see all offline times in detail:")
                    
                    # Filter to stores with 4+ offline events for detailed view
                    detailed_stores = data[data['downtime_events'] >= 4].copy()
                    
                    if len(detailed_stores) > 0:
                        for idx, row in detailed_stores.iterrows():
                            store_name = row['name'].replace('Cocopan - ', '').replace('Cocopan ', '')
                            events_count = row['downtime_events']
                            
                            # Create an expander for each store with many events
                            with st.expander(f"📋 {store_name} - {events_count} offline events (click to expand)"):
                                st.markdown(f"**Store:** {store_name}")
                                st.markdown(f"**Platform:** {row['platform']}")
                                st.markdown(f"**Total Offline Events:** {events_count}")
                                
                                # Show ALL offline times
                                all_times = format_offline_hours(row.get('offline_times', None), max_display=999)
                                if all_times and all_times != "—":
                                    st.markdown(f"**All Offline Times:** {all_times}")
                                    
                                    # Try to parse and show in a more organized way
                                    try:
                                        offline_times_array = row.get('offline_times', None)
                                        if offline_times_array is not None:
                                            # Handle PostgreSQL array format
                                            if isinstance(offline_times_array, str):
                                                times_str = offline_times_array.strip('{}')
                                                if times_str and times_str.strip() != '':
                                                    time_strings = [t.strip('"\'') for t in times_str.split(',') if t.strip()]
                                                else:
                                                    time_strings = []
                                            else:
                                                # Handle pandas Series, list, or numpy array
                                                if hasattr(offline_times_array, 'tolist'):
                                                    time_strings = offline_times_array.tolist()
                                                elif hasattr(offline_times_array, '__iter__'):
                                                    time_strings = list(offline_times_array)
                                                else:
                                                    time_strings = [offline_times_array]
                                            
                                            # Filter and format
                                            time_strings = [t for t in time_strings if t is not None and not pd.isna(t) and str(t).strip() != '']
                                            
                                            if time_strings:
                                                st.markdown("**Offline Timeline:**")
                                                ph_tz = config.get_timezone()
                                                formatted_timeline = []
                                                
                                                for time_str in time_strings:
                                                    try:
                                                        dt = pd.to_datetime(time_str)
                                                        if dt.tz is None:
                                                            dt = dt.tz_localize('UTC')
                                                        dt_ph = dt.tz_convert(ph_tz)
                                                        formatted_timeline.append(f"• {dt_ph.strftime('%I:%M %p')}")
                                                    except Exception:
                                                        formatted_timeline.append(f"• {time_str}")
                                                
                                                # Group times if there are many (e.g., show in columns)
                                                if len(formatted_timeline) > 6:
                                                    # Split into columns for better display
                                                    mid_point = len(formatted_timeline) // 2
                                                    col1, col2 = st.columns(2)
                                                    with col1:
                                                        for time_entry in formatted_timeline[:mid_point]:
                                                            st.markdown(time_entry)
                                                    with col2:
                                                        for time_entry in formatted_timeline[mid_point:]:
                                                            st.markdown(time_entry)
                                                else:
                                                    for time_entry in formatted_timeline:
                                                        st.markdown(time_entry)
                                    except Exception as e:
                                        st.markdown(f"Could not parse detailed timeline: {e}")
                                else:
                                    st.markdown("No detailed offline times available")
                    else:
                        st.info("No stores with 4+ offline events today. Toggle 'Show All Times' above to see complete data for all stores.")

                # Optional: Add insights section (enhanced)
                if len(data) > 0:
                    st.markdown("### 🔍 Quick Insights")
                    total_events = data['downtime_events'].sum()
                    high_freq_stores = len(data[data['downtime_events'] >= 5])
                    medium_freq_stores = len(data[data['downtime_events'].between(3, 4)])
                    
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        st.markdown(f"**Total Offline Events:** {total_events}")
                    with col2:
                        st.markdown(f"**High-Frequency Issues:** {high_freq_stores} stores (5+ events)")
                    with col3:
                        st.markdown(f"**Medium-Frequency Issues:** {medium_freq_stores} stores (3-4 events)")
                    
                    if len(data) > 0:
                        most_affected_idx = data['downtime_events'].idxmax()
                        most_affected = data.loc[most_affected_idx, 'name'].replace('Cocopan - ', '').replace('Cocopan ', '')
                        most_affected_count = data.loc[most_affected_idx, 'downtime_events']
                        st.markdown(f"**Most Affected Store:** {most_affected} ({most_affected_count} offline events)")

                    # Show platform breakdown if multiple platforms
                    if len(data['platform'].unique()) > 1:
                        st.markdown("**Platform Breakdown:**")
                        platform_stats = data.groupby('platform')['downtime_events'].agg(['count', 'sum']).reset_index()
                        platform_stats.columns = ['Platform', 'Stores with Issues', 'Total Events']
                        st.dataframe(platform_stats, use_container_width=True, hide_index=True)

    # ----- TAB 4: REPORTS & CSV EXPORT (ENHANCED) -----
    with tab4:
        st.markdown(f"""
        <div class="section-header">
            <div class="section-title">Store Uptime Reports & Export</div>
            <div class="section-subtitle">Historical analysis with CSV export • Available from September 10, 2025</div>
        </div>
        """, unsafe_allow_html=True)

        # --- state init ---
        if "reports_generated" not in st.session_state:
            st.session_state.reports_generated = False
        if "reports_last_range" not in st.session_state:
            st.session_state.reports_last_range = None

        st.markdown('<div class="filter-container">', unsafe_allow_html=True)
        col1, col2, col3, col4 = st.columns([2, 2, 1, 1])
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
        with col4:
            st.markdown("<br>", unsafe_allow_html=True)
            # Export button - only show after report is generated
            if st.session_state.reports_generated:
                export_clicked = st.button("📥 Export CSV", use_container_width=True, type="primary")
            else:
                st.button("📥 Export CSV", use_container_width=True, disabled=True)
                export_clicked = False
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

        # Handle CSV Export
        if st.session_state.reports_generated and export_clicked:
            range_start, range_end = st.session_state.reports_last_range
            
            with st.spinner("Generating CSV export..."):
                try:
                    # Load export data
                    export_data, export_err = load_export_data(range_start, range_end)
                    
                    if export_err:
                        st.error(f"Error loading export data: {export_err}")
                    elif export_data is None or export_data.empty:
                        st.warning("No data available for export in the selected date range.")
                    else:
                        # Create CSV
                        csv_data = create_export_csv(export_data, range_start, range_end)
                        
                        if csv_data is not None:
                            csv_content = generate_csv_content(csv_data)
                            filename = create_export_filename(range_start, range_end)
                            
                            # Show preview of export
                            st.success(f"✅ Export ready! Contains {len(csv_data)} stores with uptime data and offline events.")
                            
                            # CSV download button
                            st.download_button(
                                label=f"📥 Download {filename}",
                                data=csv_content,
                                file_name=filename,
                                mime="text/csv",
                                use_container_width=True
                            )
                            
                            # Show preview
                            st.markdown("### 📋 Export Preview")
                            st.dataframe(csv_data.head(10), use_container_width=True, hide_index=True)
                            
                            if len(csv_data) > 10:
                                st.info(f"Showing first 10 rows. Full export contains {len(csv_data)} stores.")
                        
                        else:
                            st.error("Failed to create CSV export.")
                
                except Exception as e:
                    st.error(f"Export error: {e}")
                    logger.error(f"CSV export error: {e}")

        if not st.session_state.reports_generated:
            st.info("Select a date range and click Generate Report to view and export data.")
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
            load_comprehensive_data.clear()
            load_reports_data.clear()
            load_export_data.clear()
            st.rerun()

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        st.error(f"System Error: {e}")
        logger.error(f"Dashboard error: {e}")