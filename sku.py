#!/usr/bin/env python3
"""
CocoPan SKU Product Availability Reporting Dashboard
📊 Management/Client view for SKU product availability analytics and reporting
🎯 Displays data collected by VAs through the admin dashboard
✅ Mobile-friendly with adaptive theme (matches enhanced_dashboard.py styling)
✅ Cookie-based authentication (REMOVED)
✅ Navigation back to main uptime dashboard
✅ Fixed sorting issues and added chart legends
✅ Enhanced Store Performance with individual store OOS item details
✅ Removed authentication requirement
✅ Removed specific time displays (kept date and Manila Time)
"""

# ===== Standard libs =====
import os
import re
import json
import time
import hmac
import base64
import hashlib
import logging
import threading
import http.server
import socketserver
from datetime import datetime, timedelta
from typing import List, Dict, Tuple, Optional, Any

# ===== Third-party =====
import pytz
import pandas as pd
import streamlit as st
import plotly.graph_objects as go

# ===== Initialize logging =====
logging.basicConfig(level=getattr(logging, os.getenv("LOG_LEVEL", "INFO")))
logger = logging.getLogger(__name__)

# ===== App modules =====
from config import config   # noqa: F401 (import kept for parity with your project)
from database import db

# ------------------------------------------------------------------------------
# Health check endpoint
# ------------------------------------------------------------------------------
_HEALTH_THREAD_STARTED = False
_HEALTH_PORT = int(os.getenv("HEALTH_PORT", "8505"))


def create_health_server():
    class HealthHandler(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path == "/healthz":
                try:
                    db.get_database_stats()
                    self.send_response(200)
                    self.send_header("Content-type", "text/plain")
                    self.end_headers()
                    self.wfile.write(b"OK - SKU Reporting Dashboard Healthy")
                except Exception as e:
                    self.send_response(500)
                    self.send_header("Content-type", "text/plain")
                    self.end_headers()
                    self.wfile.write(f"ERROR - {e}".encode())
            else:
                self.send_response(404)
                self.end_headers()

        def log_message(self, *args, **kwargs):
            # Silence default request logs
            return

    class ReusableTCPServer(socketserver.TCPServer):
        allow_reuse_address = True

    try:
        with ReusableTCPServer(("", _HEALTH_PORT), HealthHandler) as httpd:
            logger.info(f"SKU Reporting health server listening on :{_HEALTH_PORT}")
            httpd.serve_forever()
    except OSError as e:
        # EADDRINUSE on many platforms
        if getattr(e, "errno", None) == 98:
            logger.warning(f"SKU Reporting health server not started; port :{_HEALTH_PORT} already in use")
            return
        logger.exception("SKU Reporting health server OSError")
    except Exception:
        logger.exception("SKU Reporting health server unexpected error")


if os.getenv("RAILWAY_ENVIRONMENT") == "production" and not _HEALTH_THREAD_STARTED:
    try:
        t = threading.Thread(target=create_health_server, daemon=True, name="sku-reporting-healthz")
        t.start()
        _HEALTH_THREAD_STARTED = True
    except Exception:
        logger.exception("Failed to start SKU reporting health server thread")

# ------------------------------------------------------------------------------
# Streamlit page config
# ------------------------------------------------------------------------------
st.set_page_config(
    page_title="CocoPan SKU Reports",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ======================================================================
#                     ADAPTIVE THEME STYLES (LIGHT/DARK) - EXACT COPY
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
    
    /* Header section with gradient - purple theme for SKU */
    .header-section { 
        background: linear-gradient(135deg, #7C3AED 0%, #A855F7 100%); 
        border-radius: 16px; 
        padding: 1.5rem; 
        margin-bottom: 1.25rem; 
        box-shadow: 0 4px 6px -1px var(--shadow-medium);
        border: 1px solid #7C3AED;
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
        background: #7C3AED !important; 
        color: #fff !important; 
        box-shadow: 0 2px 4px rgba(124, 58, 237, 0.3); 
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
        background: #7C3AED;
        border: 1px solid #6D28D9;
        color: white;
        border-radius: 6px;
        font-weight: 500;
        transition: all 0.3s ease;
    }
    
    .stButton > button:hover {
        background: #6D28D9;
        border-color: #5B21B6;
        box-shadow: 0 2px 4px rgba(124, 58, 237, 0.3);
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

# ------------------------------------------------------------------------------
# Helper functions
# ------------------------------------------------------------------------------
def format_datetime_safe(dt_value) -> str:
    """Safely format datetime objects or strings with Manila timezone - IMPROVED VERSION"""
    if dt_value is None:
        return "—"

    try:
        # Initialize Manila timezone
        ph_tz = pytz.timezone('Asia/Manila')
        
        # Handle different input types
        if isinstance(dt_value, str):
            if not dt_value.strip():
                return "—"
            
            # Parse string datetime
            dt = pd.to_datetime(dt_value, utc=True)  # Force UTC interpretation
            
            # Debug logging
            logger.debug(f"Parsed string '{dt_value}' as UTC datetime: {dt}")
            
        elif isinstance(dt_value, datetime):
            dt = dt_value
            # If datetime is naive (no timezone info), assume it's UTC
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=pytz.UTC)
                logger.debug(f"Added UTC timezone to naive datetime: {dt}")
            
        else:
            # Try to convert other types to datetime
            dt = pd.to_datetime(str(dt_value), utc=True)
            logger.debug(f"Converted '{dt_value}' to UTC datetime: {dt}")
        
        # Convert to Manila time
        dt_manila = dt.astimezone(ph_tz)
        formatted_time = dt_manila.strftime("%I:%M %p").lstrip('0')
        
        logger.debug(f"Converted to Manila time: {dt_manila} -> Display: {formatted_time}")
        
        return formatted_time
        
    except Exception as e:
        logger.error(f"Error formatting datetime '{dt_value}': {e}")
        
        # Enhanced fallback - try to extract time from string if possible
        if isinstance(dt_value, str) and dt_value:
            try:
                # Try to extract just the time part if it looks like a datetime string
                if ' ' in dt_value and ':' in dt_value:
                    # Extract time portion (assumes format like "2025-09-22 03:47:02.239484")
                    time_part = dt_value.split(' ')[1].split('.')[0]  # Get "03:47:02"
                    if ':' in time_part:
                        hour, minute = time_part.split(':')[:2]
                        # Convert to Manila time (add 8 hours to UTC)
                        utc_hour = int(hour)
                        manila_hour = (utc_hour + 8) % 24
                        
                        # Format as 12-hour time
                        if manila_hour == 0:
                            display_hour = 12
                            period = "AM"
                        elif manila_hour < 12:
                            display_hour = manila_hour
                            period = "AM"
                        elif manila_hour == 12:
                            display_hour = 12
                            period = "PM"
                        else:
                            display_hour = manila_hour - 12
                            period = "PM"
                        
                        return f"{display_hour}:{minute} {period}"
            except:
                pass
            
            # Final fallback - return truncated string
            return dt_value[:16] if len(dt_value) > 16 else dt_value
        
        return str(dt_value) if dt_value else "—"


def clean_product_name(name: Optional[str]) -> str:
    """Clean product name for display by removing platform prefixes"""
    if not name:
        return ""
    cleaned = name.replace("GRAB ", "").replace("FOODPANDA ", "")
    return cleaned

# ------------------------------------------------------------------------------
# Data loading (cached)
# ------------------------------------------------------------------------------
@st.cache_data(ttl=300)  # 5 minutes
def get_sku_availability_dashboard_data():
    """Get today's SKU product availability dashboard data"""
    try:
        return db.get_sku_compliance_dashboard()
    except Exception as e:
        logger.error(f"Error loading SKU product availability dashboard: {e}")
        return []


@st.cache_data(ttl=300)
def get_out_of_stock_details_data():
    """Get detailed out-of-stock data"""
    try:
        return db.get_out_of_stock_details()
    except Exception as e:
        logger.error(f"Error loading out-of-stock details: {e}")
        return []

@st.cache_data(ttl=60)  # 1 minute cache - faster refresh for debugging
def get_store_out_of_stock_items(store_id: int):
    """Get specific out-of-stock items for a store - FIXED AND SIMPLIFIED"""
    try:
        with db.get_connection() as conn:
            cur = conn.cursor()
            today = datetime.now().date()
            
            if db.db_type == "postgresql":
                # PostgreSQL version with array handling
                cur.execute("""
                    SELECT ssc.out_of_stock_skus, ssc.platform
                    FROM store_sku_checks ssc
                    WHERE ssc.store_id = %s 
                    AND ssc.check_date = %s
                    AND ssc.out_of_stock_count > 0
                """, (store_id, today))
                
                row = cur.fetchone()
                if row and row[0]:
                    oos_skus = row[0]  # This is already an array in PostgreSQL
                    platform = row[1]
                    
                    # Get product names for the SKU codes
                    if oos_skus:
                        cur.execute("""
                            SELECT product_name 
                            FROM master_skus 
                            WHERE sku_code = ANY(%s) AND platform = %s
                            ORDER BY product_name
                        """, (oos_skus, platform))
                        
                        product_rows = cur.fetchall()
                        return [row[0] for row in product_rows]
                
            else:
                # SQLite version with JSON handling
                cur.execute("""
                    SELECT out_of_stock_skus, platform
                    FROM store_sku_checks 
                    WHERE store_id = ? 
                    AND check_date = ?
                    AND out_of_stock_count > 0
                """, (store_id, today.isoformat()))
                
                row = cur.fetchone()
                if row and row['out_of_stock_skus']:
                    import json
                    try:
                        oos_skus = json.loads(row['out_of_stock_skus'])
                        platform = row['platform']
                        
                        product_names = []
                        for sku_code in oos_skus:
                            cur.execute("""
                                SELECT product_name FROM master_skus 
                                WHERE sku_code = ? AND platform = ?
                            """, (sku_code, platform))
                            sku_row = cur.fetchone()
                            if sku_row:
                                product_names.append(sku_row['product_name'])
                        
                        return sorted(product_names)
                    except Exception as e:
                        logger.error(f"Error parsing JSON for store {store_id}: {e}")
            
            return []  # No out of stock items found
            
    except Exception as e:
        logger.error(f"Error loading out-of-stock items for store {store_id}: {e}")
        return []

# NEW: Functions for historical data queries
@st.cache_data(ttl=600)  # 10 minutes cache
def get_sku_data_by_date_range(start_date, end_date, platform_filter="All Platforms"):
    """Get SKU compliance data for a date range"""
    try:
        with db.get_connection() as conn:
            base_query = """
                SELECT ssc.check_date, s.name as store_name, s.platform, 
                       ssc.compliance_percentage, ssc.out_of_stock_count,
                       ssc.total_skus_checked, ssc.checked_at,
                       ssc.out_of_stock_skus
                FROM store_sku_checks ssc
                JOIN stores s ON ssc.store_id = s.id
                WHERE ssc.check_date BETWEEN %s AND %s
            """
            
            params = [start_date, end_date]
            
            if platform_filter != "All Platforms":
                base_query += " AND s.platform = %s"
                params.append(platform_filter)
            
            base_query += " ORDER BY ssc.check_date DESC, s.name"
            
            if db.db_type == "postgresql":
                result = pd.read_sql_query(base_query, conn, params=params)
            else:
                # Convert date objects to strings for SQLite
                sqlite_params = [d.isoformat() if hasattr(d, 'isoformat') else str(d) for d in params]
                result = pd.read_sql_query(base_query.replace('%s', '?'), conn, params=sqlite_params)
            
            return result.to_dict('records')
    except Exception as e:
        logger.error(f"Error loading SKU data by date range: {e}")
        return []

@st.cache_data(ttl=600)
def get_out_of_stock_by_date_range(start_date, end_date, platform_filter="All Platforms"):
    """Get out of stock items for a date range"""
    try:
        with db.get_connection() as conn:
            base_query = """
                SELECT ssc.check_date, s.name as store_name, s.platform, 
                       ms.sku_code, ms.product_name,
                       ssc.checked_at
                FROM store_sku_checks ssc
                JOIN stores s ON ssc.store_id = s.id
                JOIN master_skus ms ON ms.sku_code = ANY(ssc.out_of_stock_skus) 
                    AND ms.platform = ssc.platform
                WHERE ssc.check_date BETWEEN %s AND %s
                  AND ssc.out_of_stock_count > 0
            """
            
            params = [start_date, end_date]
            
            if platform_filter != "All Platforms":
                base_query += " AND s.platform = %s"
                params.append(platform_filter)
            
            base_query += " ORDER BY ssc.check_date DESC, s.name, ms.product_name"
            
            if db.db_type == "postgresql":
                result = pd.read_sql_query(base_query, conn, params=params)
            else:
                # SQLite version - handle JSON differently
                cur = conn.cursor()
                sqlite_base = """
                    SELECT ssc.check_date, s.name as store_name, s.platform, 
                           ssc.out_of_stock_skus, ssc.checked_at
                    FROM store_sku_checks ssc
                    JOIN stores s ON ssc.store_id = s.id
                    WHERE ssc.check_date BETWEEN ? AND ?
                      AND ssc.out_of_stock_count > 0
                """
                sqlite_params = [start_date.isoformat(), end_date.isoformat()]
                
                if platform_filter != "All Platforms":
                    sqlite_base += " AND s.platform = ?"
                    sqlite_params.append(platform_filter)
                
                sqlite_base += " ORDER BY ssc.check_date DESC, s.name"
                
                cur.execute(sqlite_base, sqlite_params)
                rows = cur.fetchall()
                
                result_data = []
                import json
                for row in rows:
                    try:
                        oos_skus = json.loads(row['out_of_stock_skus']) if row['out_of_stock_skus'] else []
                        for sku_code in oos_skus:
                            cur.execute("""
                                SELECT sku_code, product_name
                                FROM master_skus 
                                WHERE sku_code = ? AND platform = ?
                            """, (sku_code, row['platform']))
                            sku_row = cur.fetchone()
                            if sku_row:
                                result_data.append({
                                    'check_date': row['check_date'],
                                    'store_name': row['store_name'],
                                    'platform': row['platform'],
                                    'sku_code': sku_row['sku_code'],
                                    'product_name': sku_row['product_name'],
                                    'checked_at': row['checked_at']
                                })
                    except Exception as e:
                        logger.error(f"Error processing SQLite OOS data: {e}")
                
                return result_data
            
            return result.to_dict('records')
    except Exception as e:
        logger.error(f"Error loading out of stock by date range: {e}")
        return []

# ------------------------------------------------------------------------------
# Charts - using enhanced dashboard style with legends
# ------------------------------------------------------------------------------
def create_donut(excellent_count: int, good_count: int, attention_count: int, title: str = "Availability"):
    """Create donut chart matching enhanced dashboard style"""
    total = max(excellent_count + good_count + attention_count, 1)
    
    # Only include categories that have data
    labels = []
    values = []
    colors = []
    
    if excellent_count > 0:
        labels.append('Excellent (95%+)')
        values.append(excellent_count)
        colors.append('#10B981')  # Green
    
    if good_count > 0:
        labels.append('Good (80-94%)')
        values.append(good_count)
        colors.append('#F59E0B')  # Amber
    
    if attention_count > 0:
        labels.append('Needs Attention (<80%)')
        values.append(attention_count)
        colors.append('#EF4444')  # Red
    
    if not labels:
        return None
    
    fig = go.Figure(data=[go.Pie(
        labels=labels,
        values=values,
        hole=0.65,
        marker=dict(
            colors=colors,
            line=dict(width=2, color='rgba(255,255,255,0.1)')
        ),
        textinfo='none',
        hovertemplate='<b>%{label}</b><br>%{value} stores (%{percent})<extra></extra>',
        showlegend=True  # Show legend to explain colors
    )])
    
    # Center percentage text - FIXED: Calculate real percentage based on excellent stores
    excellent_pct = (excellent_count / total) * 100.0 if total > 0 else 0.0
    fig.add_annotation(
        text=f"<b style='font-size:28px'>{excellent_pct:.0f}%</b>", 
        x=0.5, y=0.5, 
        showarrow=False, 
        font=dict(family="Inter", color="#475569")
    )
    
    fig.update_layout(
        height=320,  # Increased height for legend
        margin=dict(t=20, b=60, l=20, r=20),
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)',
        legend=dict(
            orientation="h", 
            yanchor="top", 
            y=-0.1,
            xanchor="center", 
            x=0.5, 
            font=dict(size=12, color="#64748B"),
            bgcolor="rgba(0,0,0,0)"
        ),
        font=dict(color="#64748B")
    )
    
    return fig

def create_platform_availability_charts(
    dashboard_data: List[Dict]
) -> Tuple[Optional[go.Figure], Optional[go.Figure]]:
    """Create simple availability donut charts for GrabFood and Foodpanda with platform name titles and legends"""
    if not dashboard_data:
        return None, None

    grabfood_data = [d for d in dashboard_data if d.get('platform') == 'grabfood' and d.get('compliance_percentage') is not None]
    foodpanda_data = [d for d in dashboard_data if d.get('platform') == 'foodpanda' and d.get('compliance_percentage') is not None]

    def create_platform_chart(platform_data: List[Dict], platform_name: str) -> Optional[go.Figure]:
        if not platform_data:
            return None

        excellent_count = sum(1 for store in platform_data if store.get('compliance_percentage', 0) >= 95)
        good_count = sum(1 for store in platform_data if 80 <= store.get('compliance_percentage', 0) < 95)
        attention_count = sum(1 for store in platform_data if store.get('compliance_percentage', 0) < 80)
        
        total = excellent_count + good_count + attention_count
        if total == 0:
            return None

        # Create simple donut with platform colors
        labels = []
        values = []
        colors = []
        
        if excellent_count > 0:
            labels.append('Excellent (95%+)')
            values.append(excellent_count)
            colors.append('#10B981')
        
        if good_count > 0:
            labels.append('Good (80-94%)')
            values.append(good_count)
            colors.append('#F59E0B')
        
        if attention_count > 0:
            labels.append('Needs Attention (<80%)')
            values.append(attention_count)
            colors.append('#EF4444')

        # FIXED: Calculate average availability percentage across all stores for this platform
        avg_availability = sum(store.get('compliance_percentage', 0) for store in platform_data) / len(platform_data) if platform_data else 0.0

        fig = go.Figure(data=[go.Pie(
            labels=labels,
            values=values,
            hole=0.65,
            marker=dict(
                colors=colors,
                line=dict(width=2, color='rgba(255,255,255,0.1)')
            ),
            textinfo='none',
            hovertemplate='<b>%{label}</b><br>%{value} stores (%{percent})<extra></extra>',
            showlegend=True  # CHANGED: Show legend to explain colors
        )])
        
        # Center percentage text with "Availability" - shows average availability of all stores
        fig.add_annotation(
            text=f"<b style='font-size:24px'>{avg_availability:.1f}%</b><br><span style='font-size:14px'>Availability</span>", 
            x=0.5, y=0.5, 
            showarrow=False, 
            font=dict(family="Inter", color="#475569")
        )
        
        # Large platform title
        fig.update_layout(
            title=dict(
                text=f"<b style='font-size:18px'>{platform_name}</b>",
                x=0.5,
                y=0.95,
                xanchor='center',
                yanchor='top',
                font=dict(size=18, color="#64748B")
            ),
            height=320,  # Increased height to accommodate legend
            margin=dict(t=40, b=60, l=20, r=20),  # More bottom margin for legend
            paper_bgcolor='rgba(0,0,0,0)',
            plot_bgcolor='rgba(0,0,0,0)',
            font=dict(color="#64748B"),
            legend=dict(
                orientation="h", 
                yanchor="top", 
                y=-0.1,  # Position below chart
                xanchor="center", 
                x=0.5, 
                font=dict(size=10, color="#64748B"),
                bgcolor="rgba(0,0,0,0)"
            )
        )
        
        return fig

    grabfood_chart = create_platform_chart(grabfood_data, "GrabFood")
    foodpanda_chart = create_platform_chart(foodpanda_data, "Foodpanda")

    return grabfood_chart, foodpanda_chart

# ------------------------------------------------------------------------------
# Report sections
# ------------------------------------------------------------------------------
def availability_dashboard_section():
    """Main product availability dashboard section - ENHANCED WITH STORE OOS ITEM DETAILS"""
    now = datetime.now(pytz.timezone("Asia/Manila"))
    
    st.markdown(f"""
    <div class="section-header">
        <div class="section-title">📊 Store Performance Dashboard</div>
        <div class="section-subtitle">Real-time product availability across all stores • Manila Time</div>
    </div>
    """, unsafe_allow_html=True)

    dashboard_data = get_sku_availability_dashboard_data()

    if not dashboard_data:
        st.info("📭 No product availability data available today.")
        return

    # Platform charts only (removed metrics section)
    st.markdown("### Platform Performance")
    grabfood_chart, foodpanda_chart = create_platform_availability_charts(dashboard_data)
    colA, colB = st.columns(2)
    with colA:
        st.markdown('<div class="chart-container">', unsafe_allow_html=True)
        if grabfood_chart:
            st.plotly_chart(grabfood_chart, use_container_width=True, config={'displayModeBar': False})
        else:
            st.info("No GrabFood data available")
        st.markdown('</div>', unsafe_allow_html=True)
    with colB:
        st.markdown('<div class="chart-container">', unsafe_allow_html=True)
        if foodpanda_chart:
            st.plotly_chart(foodpanda_chart, use_container_width=True, config={'displayModeBar': False})
        else:
            st.info("No Foodpanda data available")
        st.markdown('</div>', unsafe_allow_html=True)

    # Filters
    st.markdown('<div class="filter-container">', unsafe_allow_html=True)
    c1, c2 = st.columns(2)
    with c1:
        platform_filter = st.selectbox(
            "Filter by Platform:",
            ["All Platforms", "grabfood", "foodpanda"],
            format_func=lambda x: "All Platforms" if x == "All Platforms" else ("GrabFood" if x == "grabfood" else "Foodpanda"),
        )
    with c2:
        oos_filter = st.selectbox(
            "Filter by Out of Stock:",
            ["All Stores", "No Out of Stock (0)", "Some Out of Stock (1-5)", "Many Out of Stock (6+)"],
        )
    st.markdown('</div>', unsafe_allow_html=True)

    # Apply filters
    filtered_data = dashboard_data.copy()
    if platform_filter != "All Platforms":
        filtered_data = [d for d in filtered_data if d.get('platform') == platform_filter]

    if oos_filter != "All Stores":
        if oos_filter == "No Out of Stock (0)":
            filtered_data = [d for d in filtered_data if d.get('out_of_stock_count', 0) == 0]
        elif oos_filter == "Some Out of Stock (1-5)":
            filtered_data = [d for d in filtered_data if 1 <= d.get('out_of_stock_count', 0) <= 5]
        elif oos_filter == "Many Out of Stock (6+)":
            filtered_data = [d for d in filtered_data if d.get('out_of_stock_count', 0) >= 6]

    if len(filtered_data) == 0:
        st.info("No stores found for the selected filters.")
        return

    # Create enhanced display table with out-of-stock items
    rows = []
    store_lookup = {}
    
    for store in filtered_data:
        availability_pct = store.get('compliance_percentage')
        store_id = store.get('store_id') or store.get('id')
        store_name_clean = store.get('store_name', "").replace('Cocopan - ', '').replace('Cocopan ', '')
        
        # Add to lookup for store selection
        if store_id:
            store_lookup[f"{store_name_clean} ({store.get('platform', '').title()})"] = {
                'store_id': store_id,
                'store_name': store_name_clean,
                'platform': store.get('platform'),
                'availability': availability_pct,
                'oos_count': store.get('out_of_stock_count', 0)
            }
        
        # Try multiple possible timestamp field names
        checked_at_raw = (
            store.get('checked_at') or 
            store.get('last_checked') or 
            store.get('created_at') or 
            store.get('updated_at') or
            store.get('timestamp')
        )
        
        # Get out-of-stock items for this store
        oos_items = []
        oos_count = store.get('out_of_stock_count', 0)
        if store_id and oos_count > 0:
            try:
                oos_items = get_store_out_of_stock_items(store_id)
            except Exception as e:
                logger.debug(f"Could not load OOS items for store {store_id}: {e}")
        
        # Format out-of-stock items
        if oos_items:
            cleaned_items = [clean_product_name(item) for item in oos_items[:5]]
            oos_display = ", ".join(cleaned_items)
            if len(oos_items) > 5:
                oos_display += f" + {len(oos_items) - 5} more"
        elif oos_count == 0:
            oos_display = "—"
        else:
            oos_display = f"{oos_count} items (details loading...)"
        
        if availability_pct is None:
            status = "Not Checked"
            availability_display = "—"
            availability_sort = -1.0
            availability_numeric = None
            last_check = "—"
            time_sort = datetime.min
        else:
            status = "✅ Checked"
            availability_display = f"{availability_pct:.1f}%"
            availability_sort = availability_pct
            availability_numeric = availability_pct
            # FIXED: Format the actual check time
            if checked_at_raw:
                last_check = format_datetime_safe(checked_at_raw)
                try:
                    if isinstance(checked_at_raw, str):
                        time_sort = pd.to_datetime(checked_at_raw)
                    else:
                        time_sort = checked_at_raw
                except:
                    time_sort = datetime.min
            else:
                last_check = "—"
                time_sort = datetime.min

        rows.append({
            "Branch": store_name_clean,
            "Platform": "GrabFood" if store.get('platform') == 'grabfood' else "Foodpanda",
            "Availability": availability_numeric,
            "Availability Display": availability_display,
            "Status": status,
            "Out of Stock Count": oos_count,
            "Out of Stock Items": oos_display,
            "Last Check": last_check,
            "_availability_sort": availability_sort,
            "_time_sort": time_sort,
            "_oos_count": oos_count,
        })

    # Sort by highest out of stock count first, then by availability
    rows_sorted = sorted(rows, key=lambda r: (-r["_oos_count"], r["_availability_sort"] < 0, -r["_availability_sort"] if r["_availability_sort"] >= 0 else 9999))
    
    # Create display dataframe with proper formatting
    display_data = []
    for r in rows_sorted:
        display_row = {k: v for k, v in r.items() if not k.startswith("_") and k != "Availability Display"}
        if display_row["Availability"] is None:
            display_row["Availability"] = "—"
        else:
            display_row["Availability"] = f"{display_row['Availability']:.1f}%"
        display_data.append(display_row)
    
    df_sorted = pd.DataFrame(display_data)
    
    column_config = {
        "Availability": st.column_config.TextColumn(
            "Availability",
            help="Product availability percentage",
            width="medium"
        ),
        "Out of Stock Count": st.column_config.NumberColumn(
            "Out of Stock Count",
            help="Number of out of stock items",
            format="%d"
        )
    }
    
    st.dataframe(df_sorted, use_container_width=True, hide_index=True, height=420, column_config=column_config)

    # Store selection for OOS item details
    st.markdown("### Store Details")
    st.markdown('<div class="filter-container">', unsafe_allow_html=True)
    
    stores_with_oos = {k: v for k, v in store_lookup.items() if v['oos_count'] > 0}
    
    if stores_with_oos:
        store_options = ["Select a store to view out of stock items..."] + [
            f"{store_name} - {store_data['oos_count']} items out of stock" 
            for store_name, store_data in sorted(stores_with_oos.items(), 
                                               key=lambda x: x[1]['oos_count'], reverse=True)
        ]
    else:
        store_options = ["No stores have out of stock items"]
    
    selected_store_option = st.selectbox(
        "Choose a store to see which products are out of stock:",
        store_options,
        key="selected_store_details"
    )
    st.markdown('</div>', unsafe_allow_html=True)
    
    if selected_store_option not in ["Select a store to view out of stock items...", "No stores have out of stock items"]:
        selected_store_name = selected_store_option.split(" - ")[0]
        selected_store = stores_with_oos.get(selected_store_name)
        
        if selected_store:
            st.markdown(f"""
            **Store Name:** {selected_store['store_name']}  
            **Platform:** {"GrabFood" if selected_store['platform'] == 'grabfood' else "Foodpanda"}  
            **Current Availability:** {selected_store['availability']:.1f}% if {selected_store['availability']} is not None else "Not checked"  
            **Out of Stock Items:** {selected_store['oos_count']}
            """)
            
            try:
                oos_items = get_store_out_of_stock_items(selected_store['store_id'])
                
                if oos_items:
                    st.markdown("**Products currently out of stock at this store:**")
                    
                    oos_display_data = []
                    for idx, item in enumerate(oos_items, 1):
                        oos_display_data.append({
                            "#": idx,
                            "Product Name": clean_product_name(item),
                            "Category": "Food Product"
                        })
                    
                    oos_df = pd.DataFrame(oos_display_data)
                    st.dataframe(
                        oos_df, 
                        use_container_width=True, 
                        hide_index=True,
                        height=min(400, len(oos_items) * 40 + 80)
                    )
                    
                    st.info(f"**Total:** {len(oos_items)} products are currently out of stock at {selected_store['store_name']}")
                    
                else:
                    st.info("No out of stock items found (data may be loading...)")
                    
            except Exception as e:
                logger.error(f"Error loading OOS items for store selection: {e}")
                st.error("Error loading out of stock items for this store.")
        
        else:
            st.error("Store not found in lookup.")

def out_of_stock_items_section():
    """Out of Stock Items section - sortable table of products with store details"""
    now = datetime.now(pytz.timezone("Asia/Manila"))
    
    st.markdown(f"""
    <div class="section-header">
        <div class="section-title">Out of Stock Items</div>
        <div class="section-subtitle">Products currently out of stock - click column headers to sort • Manila Time</div>
    </div>
    """, unsafe_allow_html=True)

    oos_details = get_out_of_stock_details_data()
    if not oos_details:
        st.info("No out-of-stock items recorded today.")
        return

    # Platform filter
    st.markdown('<div class="filter-container">', unsafe_allow_html=True)
    platform_filter = st.selectbox(
        "Filter by Platform:",
        ["All Platforms", "grabfood", "foodpanda"],
        format_func=lambda x: "All Platforms" if x == "All Platforms" else ("GrabFood" if x == "grabfood" else "Foodpanda"),
        key="oos_items_platform_filter",
    )
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Filter data by platform
    filtered_oos = (
        [d for d in oos_details if d.get('platform') == platform_filter]
        if platform_filter != "All Platforms"
        else oos_details
    )

    if not filtered_oos:
        st.info("No out-of-stock items for the selected platform.")
        return

    # Aggregate data by product
    product_frequency = {}
    for item in filtered_oos:
        sku_code = item.get('sku_code')
        product_name = item.get('product_name', 'Unknown Product')
        
        product_key = f"{sku_code}_{product_name}"
        
        if product_key not in product_frequency:
            product_frequency[product_key] = {
                'sku_code': sku_code,
                'product_name': clean_product_name(product_name),
                'stores': [],
                'platforms': set()
            }
        
        product_frequency[product_key]['stores'].append({
            'store_name': item.get('store_name', '').replace('Cocopan - ', '').replace('Cocopan ', ''),
            'platform': item.get('platform'),
            'checked_at': item.get('checked_at')
        })
        product_frequency[product_key]['platforms'].add(item.get('platform'))

    all_products = list(product_frequency.values())

    # Summary metrics
    total_oos_products = len(all_products)

    st.markdown("### Summary Metrics")
    col1, col2 = st.columns(2)
    with col1:
        st.metric("Unique Products OOS", total_oos_products)
    with col2:
        if all_products:
            most_affected = max(all_products, key=lambda x: len(x['stores']))
            st.metric("Most Out of Stock Item", 
                     f"{len(most_affected['stores'])} stores", 
                     most_affected['product_name'][:20] + "..." if len(most_affected['product_name']) > 20 else most_affected['product_name'],
                     delta_color="inverse")

    if not all_products:
        st.info("No products found for the selected filters.")
        return

    # Create products table
    products_table_data = []
    for product in all_products:
        stores_count = len(product['stores'])
        platforms_list = sorted(product['platforms'])
        platforms_text = " + ".join([
            "GrabFood" if p == "grabfood" else "Foodpanda" 
            for p in platforms_list
        ])
        
        store_names = [store['store_name'] for store in product['stores']]
        stores_text = ", ".join(store_names[:3])
        if len(store_names) > 3:
            stores_text += f" + {len(store_names) - 3} more"
        
        products_table_data.append({
            "Product Name": product['product_name'],
            "Out of Stock Count": stores_count,
            "Stores": stores_text,
            "Platforms": platforms_text,
            "_product_key": f"{product['sku_code']}_{product['product_name']}"
        })

    # Display sortable products table
    st.markdown("### Products Out of Stock")
    st.markdown("*Click column headers to sort the table*")
    
    products_df = pd.DataFrame(products_table_data)
    display_df = products_df.drop(columns=['_product_key'])
    
    st.dataframe(display_df, use_container_width=True, hide_index=True, height=400)

    # Product selection for details
    st.markdown("### Product Details")
    st.markdown('<div class="filter-container">', unsafe_allow_html=True)
    
    product_options = ["Select a product to view details..."] + [
        f"{p['product_name']} ({len(p['stores'])} stores)" 
        for p in sorted(all_products, key=lambda x: x['product_name'])
    ]
    
    selected_product_option = st.selectbox(
        "Choose a product to see which stores it's out of stock in:",
        product_options,
        key="selected_product_details"
    )
    st.markdown('</div>', unsafe_allow_html=True)
    
    if selected_product_option != "Select a product to view details...":
        selected_product_name = selected_product_option.split(" (")[0]
        selected_product = next(
            (p for p in all_products if p['product_name'] == selected_product_name), 
            None
        )
        
        if selected_product:
            stores_count = len(selected_product['stores'])
            platforms_text = " + ".join([
                "GrabFood" if p == "grabfood" else "Foodpanda" 
                for p in sorted(selected_product['platforms'])
            ])
            
            st.markdown(f"""
            **Product Code:** `{selected_product['sku_code']}`  
            **Product Name:** {selected_product['product_name']}  
            **Affected Stores:** {stores_count}  
            **Platforms:** {platforms_text}
            """)
            
            # FIXED: Show actual check times
            if selected_product['stores']:
                stores_data = []
                for store in selected_product['stores']:
                    stores_data.append({
                        "Store": store['store_name'],
                        "Platform": "GrabFood" if store['platform'] == 'grabfood' else "Foodpanda",
                        "Last Check": format_datetime_safe(store['checked_at'])
                    })
                
                st.markdown("**Stores where this product is out of stock:**")
                st.dataframe(
                    pd.DataFrame(stores_data), 
                    use_container_width=True, 
                    hide_index=True,
                    height=min(300, len(stores_data) * 40 + 50)
                )
            else:
                st.info("No store details available.")

def reports_export_section():
    """Reports and data export section - UPDATED WITH REQUESTED CHANGES"""
    now = datetime.now(pytz.timezone("Asia/Manila"))
    
    st.markdown(f"""
    <div class="section-header">
        <div class="section-title">📋 Reports & Data Export</div>
        <div class="section-subtitle">Generate and export reports for specific date ranges and platforms • Available from September 20, 2025</div>
    </div>
    """, unsafe_allow_html=True)

    st.markdown('<div class="filter-container">', unsafe_allow_html=True)
    col1, col2, col3, col4 = st.columns([2, 2, 1, 1])
    
    with col1:
        start_date = st.date_input("Start Date", value=datetime(2025, 9, 20).date())
    with col2:
        end_date = st.date_input("End Date", value=datetime.now().date())
    with col3:
        platform_filter = st.selectbox(
            "Platform Filter:",
            ["All Platforms", "grabfood", "foodpanda"],
            format_func=lambda x: "All Platforms" if x == "All Platforms" else ("GrabFood" if x == "grabfood" else "Foodpanda"),
            key="reports_platform_filter",
        )
    with col4:
        report_type = st.selectbox(
            "Report Type:",
            ["Daily Availability Summary", "Out of Stock Items", "Store Performance"],
        )
    st.markdown('</div>', unsafe_allow_html=True)

    if st.button("📊 Generate Report", use_container_width=True):
        try:
            if report_type == "Daily Availability Summary":
                # Use real database data
                data = get_sku_data_by_date_range(start_date, end_date, platform_filter)
                
                if not data:
                    st.info("No data available for the selected date range and platform.")
                    return
                
                # Group by date and calculate summary stats
                date_summary = {}
                for record in data:
                    check_date = record['check_date']
                    if isinstance(check_date, str):
                        check_date = datetime.fromisoformat(check_date).date()
                    
                    if check_date not in date_summary:
                        date_summary[check_date] = {
                            'total_checked': 0,
                            'total_100_percent': 0,
                            'total_80_plus': 0,
                            'total_below_80': 0,
                            'compliance_sum': 0,
                            'total_oos_items': 0,
                            'stores_with_oos': 0
                        }
                    
                    summary = date_summary[check_date]
                    summary['total_checked'] += 1
                    
                    compliance = record['compliance_percentage'] or 0
                    summary['compliance_sum'] += compliance
                    
                    if compliance == 100.0:
                        summary['total_100_percent'] += 1
                    elif compliance >= 80.0:
                        summary['total_80_plus'] += 1
                    else:
                        summary['total_below_80'] += 1
                    
                    oos_count = record['out_of_stock_count'] or 0
                    summary['total_oos_items'] += oos_count
                    if oos_count > 0:
                        summary['stores_with_oos'] += 1
                
                # Convert to table format
                table_data = []
                for date, summary in sorted(date_summary.items()):
                    avg_compliance = summary['compliance_sum'] / max(summary['total_checked'], 1)
                    
                    table_data.append({
                        "Date": date.strftime("%Y-%m-%d"),
                        "Platform": platform_filter if platform_filter != "All Platforms" else "All",
                        "Stores Checked": summary['total_checked'],
                        "Average Availability": f"{avg_compliance:.1f}%",
                        "100% Available": summary['total_100_percent'],
                        "80%+ Available": summary['total_80_plus'],
                        "Below 80%": summary['total_below_80'],
                        "Total OOS Items": summary['total_oos_items'],
                        "Stores with OOS": summary['stores_with_oos']
                    })

                if table_data:
                    df = pd.DataFrame(table_data)
                    st.dataframe(df, use_container_width=True, hide_index=True)
                    st.download_button(
                        label="📥 Download Daily Summary CSV",
                        data=df.to_csv(index=False),
                        file_name=f"daily_availability_summary_{platform_filter}_{start_date}_{end_date}.csv",
                        mime="text/csv",
                        use_container_width=True
                    )
                else:
                    st.info("No data available for the selected criteria.")

            elif report_type == "Out of Stock Items":
                # CHANGED: Make it like out of stock items tab (show count and stores)
                oos_data = get_out_of_stock_by_date_range(start_date, end_date, platform_filter)
                
                if not oos_data:
                    st.info("No out-of-stock items found for the selected date range and platform.")
                    return
                
                # Aggregate by product like the tab does
                product_frequency = {}
                for item in oos_data:
                    sku_code = item['sku_code']
                    product_name = item['product_name']
                    
                    product_key = f"{sku_code}_{product_name}"
                    
                    if product_key not in product_frequency:
                        product_frequency[product_key] = {
                            'sku_code': sku_code,
                            'product_name': clean_product_name(product_name),
                            'stores': [],
                            'platforms': set()
                        }
                    
                    product_frequency[product_key]['stores'].append({
                        'store_name': item['store_name'].replace('Cocopan - ', '').replace('Cocopan ', ''),
                        'platform': item['platform'],
                        'checked_at': item['checked_at'],
                        'check_date': item['check_date']
                    })
                    product_frequency[product_key]['platforms'].add(item['platform'])

                # Create products table like the tab
                products_table_data = []
                for product in product_frequency.values():
                    stores_count = len(product['stores'])
                    platforms_list = sorted(product['platforms'])
                    platforms_text = " + ".join([
                        "GrabFood" if p == "grabfood" else "Foodpanda" 
                        for p in platforms_list
                    ])
                    
                    # Get store names
                    store_names = [store['store_name'] for store in product['stores']]
                    stores_text = ", ".join(store_names[:3])
                    if len(store_names) > 3:
                        stores_text += f" + {len(store_names) - 3} more"
                    
                    products_table_data.append({
                        "Product Name": product['product_name'],
                        "Out of Stock Count": stores_count,
                        "Stores": stores_text,
                        "Platforms": platforms_text,
                    })
                
                if products_table_data:
                    df = pd.DataFrame(products_table_data)
                    df = df.sort_values('Out of Stock Count', ascending=False)
                    st.dataframe(df, use_container_width=True, hide_index=True)
                    st.download_button(
                        label="📥 Download Out of Stock Items Report",
                        data=df.to_csv(index=False),
                        file_name=f"oos_items_report_{platform_filter}_{start_date}_{end_date}.csv",
                        mime="text/csv",
                        use_container_width=True
                    )
                else:
                    st.info("No out of stock data available.")

            elif report_type == "Store Performance":
                # CHANGED: Remove checked_by and total_sku, add OOS items list
                data = get_sku_data_by_date_range(start_date, end_date, platform_filter)
                
                if not data:
                    st.info("No store performance data available for the selected criteria.")
                    return
                
                # Convert to store performance format with OOS items
                performance_data = []
                for record in data:
                    check_date = record['check_date']
                    if isinstance(check_date, str):
                        check_date = datetime.fromisoformat(check_date).date()
                    
                    # Get OOS items for this record
                    oos_items_list = []
                    if record.get('out_of_stock_skus'):
                        if db.db_type == "postgresql":
                            # PostgreSQL - out_of_stock_skus is already a list
                            oos_skus = record['out_of_stock_skus']
                        else:
                            # SQLite - parse JSON
                            import json
                            try:
                                oos_skus = json.loads(record['out_of_stock_skus']) if record['out_of_stock_skus'] else []
                            except:
                                oos_skus = []
                        
                        # Get product names for SKU codes
                        if oos_skus:
                            with db.get_connection() as conn:
                                cur = conn.cursor()
                                if db.db_type == "postgresql":
                                    cur.execute("""
                                        SELECT product_name FROM master_skus 
                                        WHERE sku_code = ANY(%s) AND platform = %s
                                        ORDER BY product_name
                                    """, (oos_skus, record['platform']))
                                    oos_items_list = [row[0] for row in cur.fetchall()]
                                else:
                                    for sku_code in oos_skus:
                                        cur.execute("""
                                            SELECT product_name FROM master_skus 
                                            WHERE sku_code = ? AND platform = ?
                                        """, (sku_code, record['platform']))
                                        sku_row = cur.fetchone()
                                        if sku_row:
                                            oos_items_list.append(sku_row['product_name'])
                    
                    # Format OOS items
                    if oos_items_list:
                        oos_display = ", ".join([clean_product_name(item) for item in oos_items_list[:5]])
                        if len(oos_items_list) > 5:
                            oos_display += f" + {len(oos_items_list) - 5} more"
                    else:
                        oos_display = "—"
                    
                    # FIXED: Use format_datetime_safe to show actual check time
                    performance_data.append({
                        "Date": check_date.strftime("%Y-%m-%d"),
                        "Store": record['store_name'].replace('Cocopan - ', '').replace('Cocopan ', ''),
                        "Platform": "GrabFood" if record['platform'] == 'grabfood' else "Foodpanda",
                        "Availability %": f"{record['compliance_percentage']:.1f}%" if record['compliance_percentage'] is not None else "N/A",
                        "Out of Stock Count": record['out_of_stock_count'] or 0,
                        "Out of Stock Items": oos_display,
                        "Check Time": format_datetime_safe(record.get('checked_at'))  # FIXED HERE
                    })
                
                if performance_data:
                    df = pd.DataFrame(performance_data)
                    # Sort by date (newest first) then by store name
                    df = df.sort_values(['Date', 'Store'], ascending=[False, True])
                    
                    st.dataframe(df, use_container_width=True, hide_index=True)
                    st.download_button(
                        label="📥 Download Store Performance Report",
                        data=df.to_csv(index=False),
                        file_name=f"store_performance_{platform_filter}_{start_date}_{end_date}.csv",
                        mime="text/csv",
                        use_container_width=True
                    )
                else:
                    st.info("No store performance data available.")

        except Exception as e:
            logger.exception("Error generating report")
            st.error(f"Error generating report: {e}")

    st.markdown("---")
    st.markdown("**Available Report Types:**")
    st.markdown("• **Daily Availability Summary**: Aggregated metrics by date showing compliance trends")
    st.markdown("• **Out of Stock Items**: Product-focused view showing which items are OOS and in how many stores")
    st.markdown("• **Store Performance**: Individual store compliance data with out of stock item details")# ------------------------------------------------------------------------------
# Main dashboard
# ------------------------------------------------------------------------------
def main():
    # REMOVED: Authentication check completely

    # Sidebar - matching enhanced dashboard with navigation link
    with st.sidebar:
        # Navigation back to main dashboard - more prominent
        st.markdown("""
        <div class="nav-link">
            <a href="https://cocopanwatchtower.com/" target="_blank">
                🏢 ← Back to Uptime Dashboard
            </a>
        </div>
        """, unsafe_allow_html=True)
        
        # Also add as a regular button for better visibility
        if st.button("🏢 Back to Uptime Dashboard", use_container_width=True):
            st.markdown("""
            <script>
            window.open('https://cocopanwatchtower.com/', '_blank');
            </script>
            """, unsafe_allow_html=True)
        
        st.markdown("---")
        st.markdown("🎨 **Theme:** Adapts to your system preference")
        st.markdown("💡 **Tip:** Change your browser/OS theme to see the dashboard adapt!")

    # Header - matching enhanced dashboard style with purple theme
    now = datetime.now(pytz.timezone("Asia/Manila"))
    st.markdown(f"""
    <div class="header-section">
        <h1>📊 CocoPan SKU Reports</h1>
        <h3>Product Availability Analytics • Data as of {now.strftime('%B %d, %Y')} Manila Time</h3>
    </div>
    """, unsafe_allow_html=True)

    # Navigation link in main area for better visibility
    col1, col2, col3 = st.columns([1, 2, 1])
    with col1:
        if st.button("🏢 ← Back to Uptime Dashboard", use_container_width=True):
            st.markdown('<meta http-equiv="refresh" content="0; url=https://cocopanwatchtower.com/">', unsafe_allow_html=True)
            st.info("Redirecting to Uptime Dashboard...")
    with col2:
        pass  # Center space
    with col3:
        pass  # Right space

    # Main tabs
    tab1, tab2, tab3 = st.tabs([
        "📊 Store Performance", 
        "Out of Stock Items", 
        "📋 Reports & Export"
    ])

    with tab1:
        availability_dashboard_section()
    with tab2:
        out_of_stock_items_section()
    with tab3:
        reports_export_section()

    # Footer - matching enhanced dashboard
    st.markdown("---")
    c1, c2, c3 = st.columns([1, 2, 1])
    with c2:
        if st.button("🔄 Refresh All Data", use_container_width=True):
            get_sku_availability_dashboard_data.clear()
            get_out_of_stock_details_data.clear()
            get_sku_data_by_date_range.clear()
            get_out_of_stock_by_date_range.clear()
            get_store_out_of_stock_items.clear()  # Clear OOS items cache
            st.success("Data refreshed!")
            st.rerun()

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.exception("SKU reporting dashboard error")
        st.error(f"❌ System Error: {e}")