#!/usr/bin/env python3
"""
CocoPan Store Ratings Dashboard
Clean enterprise dashboard — no emojis, semantic color, premium typography
"""
import streamlit as st
import pandas as pd
from datetime import datetime, timedelta, date
from typing import Optional
import pytz
import logging

from database import db

logger = logging.getLogger(__name__)

# Page configuration
st.set_page_config(
    page_title="Store Ratings — CocoPan",
    page_icon="★",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# ─────────────────────────────────────────────────────────────
# DESIGN SYSTEM
# Font: DM Sans (distinctive, readable, not overused)
# Mono: JetBrains Mono for data/numbers
# Palette: Slate neutrals + Indigo accent
# Semantic: Green=excellent, Amber=watch, Red=critical
# ─────────────────────────────────────────────────────────────
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=DM+Sans:ital,opsz,wght@0,9..40,300;0,9..40,400;0,9..40,500;0,9..40,600;0,9..40,700&family=JetBrains+Mono:wght@400;500&display=swap');
    
    /* ── Strip Streamlit chrome ── */
    #MainMenu, footer, .stDeployButton, header {visibility: hidden;}
    [data-testid="stSidebar"] {display: none;}
    
    /* ── Light mode tokens ── */
    :root {
        --surface-0: #FAFBFC;
        --surface-1: #FFFFFF;
        --surface-2: #F4F5F7;
        --surface-3: #EBECF0;
        --text-0: #172B4D;
        --text-1: #44546F;
        --text-2: #626F86;
        --text-3: #8993A4;
        --border: #DFE1E6;
        --border-focus: #B3BAC5;
        --accent: #4F46E5;
        --accent-subtle: #EEF2FF;
        --accent-text: #3730A3;
        --semantic-green: #216E4E;
        --semantic-green-bg: #DCFFF1;
        --semantic-amber: #974F0C;
        --semantic-amber-bg: #FFF7D6;
        --semantic-red: #AE2A19;
        --semantic-red-bg: #FFEDEB;
        --shadow-sm: 0 1px 2px rgba(9,30,66,0.08);
        --shadow-md: 0 1px 3px rgba(9,30,66,0.12), 0 1px 2px rgba(9,30,66,0.08);
        --shadow-lg: 0 4px 8px rgba(9,30,66,0.08), 0 2px 4px rgba(9,30,66,0.06);
        --radius-sm: 6px;
        --radius-md: 8px;
        --radius-lg: 12px;
    }
    
    /* ── Dark mode tokens ── */
    @media (prefers-color-scheme: dark) {
        :root {
            --surface-0: #0D1117;
            --surface-1: #161B22;
            --surface-2: #1C2128;
            --surface-3: #2D333B;
            --text-0: #E6EDF3;
            --text-1: #C9D1D9;
            --text-2: #8B949E;
            --text-3: #6E7681;
            --border: #30363D;
            --border-focus: #484F58;
            --accent: #818CF8;
            --accent-subtle: #1E1B4B;
            --accent-text: #A5B4FC;
            --semantic-green: #56D364;
            --semantic-green-bg: #0D2818;
            --semantic-amber: #E3B341;
            --semantic-amber-bg: #2A1F00;
            --semantic-red: #F85149;
            --semantic-red-bg: #3D0C08;
            --shadow-sm: 0 1px 2px rgba(0,0,0,0.3);
            --shadow-md: 0 1px 3px rgba(0,0,0,0.4), 0 1px 2px rgba(0,0,0,0.3);
            --shadow-lg: 0 4px 8px rgba(0,0,0,0.3), 0 2px 4px rgba(0,0,0,0.2);
        }
    }
    
    /* ── Global ── */
    .block-container {
        padding: 2rem 2rem 3rem;
        max-width: 1200px;
    }
    
    .main {
        font-family: 'DM Sans', -apple-system, BlinkMacSystemFont, sans-serif;
        background: var(--surface-0);
        color: var(--text-0);
    }
    
    h1, h2, h3, h4, h5, h6 {
        font-family: 'DM Sans', sans-serif !important;
        color: var(--text-0) !important;
        font-weight: 600 !important;
        letter-spacing: -0.01em;
    }
    
    /* ── Top bar ── */
    .top-bar {
        display: flex;
        align-items: center;
        justify-content: space-between;
        padding: 0 0 1.5rem;
        border-bottom: 1px solid var(--border);
        margin-bottom: 1.5rem;
        flex-wrap: wrap;
        gap: 0.75rem;
    }
    
    .top-bar-left {
        display: flex;
        flex-direction: column;
        gap: 0.25rem;
    }
    
    .top-bar-title {
        font-size: 1.5rem;
        font-weight: 700;
        color: var(--text-0);
        letter-spacing: -0.02em;
        margin: 0;
        line-height: 1.3;
    }
    
    .top-bar-subtitle {
        font-size: 0.8125rem;
        color: var(--text-2);
        margin: 0;
        font-weight: 400;
    }
    
    /* ── Metric row ── */
    .metric-row {
        display: grid;
        grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
        gap: 1rem;
        margin-bottom: 1.5rem;
    }
    
    .metric-card {
        background: var(--surface-1);
        border: 1px solid var(--border);
        border-radius: var(--radius-md);
        padding: 1.25rem;
        box-shadow: var(--shadow-sm);
    }
    
    .metric-label {
        font-size: 0.6875rem;
        font-weight: 500;
        color: var(--text-3);
        text-transform: uppercase;
        letter-spacing: 0.06em;
        margin-bottom: 0.375rem;
    }
    
    .metric-value {
        font-size: 1.75rem;
        font-weight: 700;
        color: var(--text-0);
        font-family: 'JetBrains Mono', monospace;
        letter-spacing: -0.02em;
        line-height: 1.2;
    }
    
    .metric-sub {
        font-size: 0.6875rem;
        color: var(--text-3);
        margin-top: 0.25rem;
    }
    
    /* ── Store table ── */
    .store-table {
        width: 100%;
        border: 1px solid var(--border);
        border-radius: var(--radius-lg);
        overflow: hidden;
        background: var(--surface-1);
        box-shadow: var(--shadow-sm);
    }
    
    .store-table-header {
        display: grid;
        grid-template-columns: 40px 1fr 90px 100px 72px;
        padding: 0.5rem 1rem;
        background: var(--surface-2);
        border-bottom: 1px solid var(--border);
        font-size: 0.6875rem;
        font-weight: 600;
        color: var(--text-3);
        text-transform: uppercase;
        letter-spacing: 0.06em;
        align-items: center;
    }
    
    .store-row {
        display: grid;
        grid-template-columns: 40px 1fr 90px 100px 72px;
        padding: 0.75rem 1rem;
        border-bottom: 1px solid var(--border);
        align-items: center;
        transition: background 0.12s ease;
    }
    
    .store-row:last-child { border-bottom: none; }
    .store-row:hover { background: var(--surface-2); }
    
    .store-rank {
        font-size: 0.8125rem;
        font-weight: 500;
        color: var(--text-3);
        font-family: 'JetBrains Mono', monospace;
    }
    
    .store-rank-top {
        color: var(--accent);
        font-weight: 700;
    }
    
    .store-info {
        display: flex;
        flex-direction: column;
        gap: 0.0625rem;
        min-width: 0;
    }
    
    .store-name-text {
        font-size: 0.875rem;
        font-weight: 600;
        color: var(--text-0);
        white-space: nowrap;
        overflow: hidden;
        text-overflow: ellipsis;
    }
    
    .store-platform {
        font-size: 0.6875rem;
        font-weight: 500;
        text-transform: uppercase;
        letter-spacing: 0.04em;
    }
    
    .store-platform-gf { color: var(--semantic-green); }
    .store-platform-fp { color: #BE185D; }
    @media (prefers-color-scheme: dark) { .store-platform-fp { color: #F9A8D4; } }
    
    /* ── Rating display ── */
    .rating-cell {
        display: flex;
        align-items: center;
        gap: 0.375rem;
    }
    
    .rating-value {
        font-size: 0.9375rem;
        font-weight: 700;
        font-family: 'JetBrains Mono', monospace;
    }
    
    .rating-stars {
        font-size: 0.6875rem;
        color: #B8860B;
        letter-spacing: 0;
    }
    
    @media (prefers-color-scheme: dark) { .rating-stars { color: #FBBF24; } }
    
    .rating-excellent { color: var(--semantic-green); }
    .rating-watch { color: var(--semantic-amber); }
    .rating-critical { color: var(--semantic-red); }
    
    /* ── Trend chip ── */
    .trend-chip {
        display: inline-flex;
        align-items: center;
        padding: 0.125rem 0.4375rem;
        border-radius: 10px;
        font-size: 0.6875rem;
        font-weight: 600;
        font-family: 'JetBrains Mono', monospace;
        line-height: 1.4;
    }
    
    .trend-up { background: var(--semantic-green-bg); color: var(--semantic-green); }
    .trend-down { background: var(--semantic-red-bg); color: var(--semantic-red); }
    .trend-flat { background: var(--surface-2); color: var(--text-3); }
    
    /* ── Distribution ── */
    .dist-section {
        background: var(--surface-1);
        border: 1px solid var(--border);
        border-radius: var(--radius-md);
        padding: 0.875rem;
        box-shadow: var(--shadow-sm);
    }
    
    .dist-title {
        font-size: 0.75rem;
        font-weight: 600;
        color: var(--text-1);
        margin-bottom: 0.625rem;
        text-transform: uppercase;
        letter-spacing: 0.05em;
    }
    
    .dist-row {
        display: flex;
        align-items: center;
        gap: 0.5rem;
        margin-bottom: 0.375rem;
    }
    
    .dist-row:last-child { margin-bottom: 0; }
    
    .dist-label {
        font-size: 0.6875rem;
        font-weight: 600;
        width: 16px;
        color: var(--text-2);
        text-align: right;
        font-family: 'JetBrains Mono', monospace;
    }
    
    .dist-bar-bg {
        flex: 1;
        height: 5px;
        background: var(--surface-3);
        border-radius: 3px;
        overflow: hidden;
    }
    
    .dist-bar-fill {
        height: 100%;
        background: var(--accent);
        border-radius: 3px;
        transition: width 0.4s ease;
    }
    
    .dist-count {
        font-size: 0.625rem;
        color: var(--text-3);
        min-width: 52px;
        text-align: right;
        font-family: 'JetBrains Mono', monospace;
    }
    
    /* ── Section label ── */
    .section-label {
        font-size: 0.8125rem;
        font-weight: 600;
        color: var(--text-1);
        margin: 0 0 0.625rem;
    }
    
    /* ── Streamlit overrides ── */
    .stSelectbox > div > div {
        background: var(--surface-1);
        border: 1px solid var(--border);
        border-radius: var(--radius-sm);
        color: var(--text-0);
        font-family: 'DM Sans', sans-serif;
        font-size: 0.8125rem;
    }
    
    .stSelectbox label, .stMultiSelect label, .stDateInput label {
        color: var(--text-1) !important;
        font-weight: 500 !important;
        font-size: 0.8125rem !important;
    }
    
    .stButton > button {
        background: var(--accent);
        border: none;
        color: white;
        border-radius: var(--radius-sm);
        font-weight: 600;
        font-family: 'DM Sans', sans-serif;
        font-size: 0.8125rem;
        padding: 0.5rem 1rem;
        transition: opacity 0.15s ease, box-shadow 0.15s ease;
        box-shadow: var(--shadow-sm);
    }
    
    .stButton > button:hover {
        opacity: 0.9;
        box-shadow: var(--shadow-md);
    }
    
    [data-testid="stExpander"] {
        border: 1px solid var(--border) !important;
        border-radius: var(--radius-md) !important;
        background: var(--surface-1);
        box-shadow: var(--shadow-sm);
    }
    
    [data-testid="stExpander"] summary {
        font-weight: 600;
        font-size: 0.875rem;
        color: var(--text-0);
    }
    
    .stTabs [data-baseweb="tab-list"] {
        gap: 0;
        border-bottom: 1px solid var(--border);
    }
    
    .stTabs [data-baseweb="tab"] {
        font-family: 'DM Sans', sans-serif;
        font-weight: 500;
        font-size: 0.8125rem;
        color: var(--text-2);
        padding: 0.5rem 0.875rem;
        border: none;
        background: transparent;
    }
    
    .stTabs [data-baseweb="tab"][aria-selected="true"] {
        color: var(--accent) !important;
        border-bottom: 2px solid var(--accent);
    }
    
    .stCaption, .stCaptionContainer {
        color: var(--text-3) !important;
        font-size: 0.75rem !important;
    }
    
    .stLinkButton > a {
        font-family: 'DM Sans', sans-serif !important;
        font-size: 0.8125rem !important;
        font-weight: 500 !important;
        color: var(--text-2) !important;
        background: var(--surface-1) !important;
        border: 1px solid var(--border) !important;
        border-radius: var(--radius-sm) !important;
    }
    
    .stLinkButton > a:hover {
        color: var(--text-0) !important;
        border-color: var(--border-focus) !important;
        background: var(--surface-2) !important;
    }
    
    /* ── Spacing fixes ── */
    .element-container:has(.store-table),
    .element-container:has(.metric-row),
    .element-container:has(.dist-section) {
        margin-bottom: 0 !important;
    }
    
    /* ── Mobile ── */
    @media (max-width: 768px) {
        .block-container { padding: 1rem; }
        .top-bar-title { font-size: 1.25rem; }
        .metric-row { grid-template-columns: 1fr 1fr; }
        
        .store-table-header, .store-row {
            grid-template-columns: 32px 1fr 68px 80px 56px;
            padding: 0.5rem 0.5rem;
        }
        
        .store-name-text { font-size: 0.8125rem; }
        .rating-value { font-size: 0.8125rem; }
    }
    
    @media (max-width: 480px) {
        .store-table-header, .store-row {
            grid-template-columns: 28px 1fr 56px 56px;
        }
        
        .store-table-header > *:nth-child(5),
        .store-row > *:nth-child(5) { display: none; }
    }
    
    /* ── Transitions ── */
    * { transition: background-color 0.2s ease, border-color 0.2s ease; }
</style>
""", unsafe_allow_html=True)


# ─────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────

def rating_class(r: float) -> str:
    if r >= 4.5: return "rating-excellent"
    if r >= 4.0: return "rating-watch"
    return "rating-critical"


def star_str(r: float) -> str:
    full = int(r)
    return "★" * full + "☆" * (5 - full)


def trend_html(trend: str, val: float) -> str:
    if trend == 'up' and val > 0:
        return f'<span class="trend-chip trend-up">+{val:.1f}</span>'
    elif trend == 'down' and val < 0:
        return f'<span class="trend-chip trend-down">{val:.1f}</span>'
    return '<span class="trend-chip trend-flat">—</span>'


def platform_label(p: str) -> str:
    cls = "store-platform-gf" if p == "grabfood" else "store-platform-fp"
    label = "GrabFood" if p == "grabfood" else "Foodpanda"
    return f'<span class="store-platform {cls}">{label}</span>'


def format_ts_manila(ts_value) -> str:
    manila_tz = pytz.timezone('Asia/Manila')
    try:
        if isinstance(ts_value, str):
            ts_value = datetime.fromisoformat(ts_value.replace('Z', '+00:00'))
        if hasattr(ts_value, 'tzinfo') and ts_value.tzinfo:
            ts_value = ts_value.astimezone(manila_tz)
        else:
            ts_value = pytz.UTC.localize(ts_value).astimezone(manila_tz)
        return ts_value.strftime("%b %d, %Y %-I:%M%p")
    except Exception:
        return str(ts_value) if ts_value else ""


# ─────────────────────────────────────────────────────────────
# REPORT DATA FUNCTIONS
# ─────────────────────────────────────────────────────────────

def get_store_list_for_filter() -> list:
    try:
        with db.get_connection() as conn:
            cur = conn.cursor()
            cur.execute("""
                SELECT DISTINCT COALESCE(s.name_override, s.name) AS store_name
                FROM current_store_ratings csr
                JOIN stores s ON csr.store_id = s.id
                ORDER BY store_name
            """)
            return [row[0] for row in cur.fetchall()]
    except Exception as e:
        logger.error(f"Error loading store list: {e}")
        return []


def get_ratings_history(store_names=None, platform=None, start_date=None, end_date=None) -> list:
    try:
        with db.get_connection() as conn:
            cur = conn.cursor()
            where_clauses = []
            params = []
            ph = "%s" if db.db_type == "postgresql" else "?"

            if platform and platform != 'all':
                where_clauses.append(f"sr.platform = {ph}")
                params.append(platform)

            if store_names:
                placeholders = ", ".join([ph] * len(store_names))
                where_clauses.append(f"COALESCE(s.name_override, s.name) IN ({placeholders})")
                params.extend(store_names)

            if start_date:
                if db.db_type == "postgresql":
                    where_clauses.append(f"DATE(sr.scraped_at AT TIME ZONE 'Asia/Manila') >= {ph}")
                else:
                    where_clauses.append(f"DATE(sr.scraped_at, '+8 hours') >= {ph}")
                params.append(str(start_date))

            if end_date:
                if db.db_type == "postgresql":
                    where_clauses.append(f"DATE(sr.scraped_at AT TIME ZONE 'Asia/Manila') <= {ph}")
                else:
                    where_clauses.append(f"DATE(sr.scraped_at, '+8 hours') <= {ph}")
                params.append(str(end_date))

            where_sql = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""

            cur.execute(f"""
                SELECT
                    COALESCE(s.name_override, s.name) AS store_name,
                    sr.platform, sr.rating, sr.previous_rating,
                    sr.rating_change, sr.scraped_at, sr.manual_entry
                FROM store_ratings sr
                JOIN stores s ON sr.store_id = s.id
                {where_sql}
                ORDER BY COALESCE(s.name_override, s.name), sr.scraped_at DESC
            """, params)

            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, row)) for row in cur.fetchall()]
    except Exception as e:
        logger.error(f"Error loading ratings history: {e}")
        return []


def get_scrape_dates() -> list:
    try:
        with db.get_connection() as conn:
            cur = conn.cursor()
            if db.db_type == "postgresql":
                cur.execute("SELECT DISTINCT DATE(scraped_at AT TIME ZONE 'Asia/Manila') FROM store_ratings ORDER BY 1 DESC")
            else:
                cur.execute("SELECT DISTINCT DATE(scraped_at, '+8 hours') FROM store_ratings ORDER BY 1 DESC")
            return [row[0] for row in cur.fetchall()]
    except Exception:
        return []


def build_snapshot_df(ratings_data):
    rows = []
    for s in ratings_data:
        # Try all possible timestamp field names
        ts = None
        for key in ['last_scraped_at', 'scraped_at', 'last_scraped', 'updated_at', 'checked_at']:
            if s.get(key):
                ts = s[key]
                break
        rows.append({
            'Rank': s['rank'],
            'Store': s['store_name'],
            'Platform': s['platform'].capitalize(),
            'Rating': s['rating'],
            'Last Scraped': format_ts_manila(ts) if ts else ''
        })
    return pd.DataFrame(rows)


def build_history_df(history):
    rows = []
    for r in history:
        change = f"{r['rating_change']:+.1f}" if r.get('rating_change') is not None else ""
        rows.append({
            'Store': r['store_name'],
            'Platform': r['platform'].capitalize() if r.get('platform') else '',
            'Rating': r['rating'],
            'Previous': r.get('previous_rating', ''),
            'Change': change,
            'Scraped': format_ts_manila(r.get('scraped_at', ''))
        })
    return pd.DataFrame(rows)


def build_summary_df(history):
    if not history:
        return pd.DataFrame()
    df = pd.DataFrame(history)
    out = []
    for (store, platform), g in df.groupby(['store_name', 'platform']):
        g = g.sort_values('scraped_at')
        first = g.iloc[0]['rating']
        latest = g.iloc[-1]['rating']
        out.append({
            'Store': store,
            'Platform': platform.capitalize() if platform else '',
            'First': first,
            'First Date': format_ts_manila(g.iloc[0]['scraped_at']),
            'Latest': latest,
            'Latest Date': format_ts_manila(g.iloc[-1]['scraped_at']),
            'High': g['rating'].max(),
            'Low': g['rating'].min(),
            'Net Change': f"{latest - first:+.1f}",
            'Scrapes': len(g)
        })
    return pd.DataFrame(out)


# ─────────────────────────────────────────────────────────────
# RENDER FUNCTIONS
# ─────────────────────────────────────────────────────────────

def _show_last_updated(ratings_data):
    manila_tz = pytz.timezone('Asia/Manila')
    try:
        first = ratings_data[0]
        ts_field = None
        for f in ['scraped_at', 'last_scraped', 'updated_at', 'checked_at', 'created_at', 'timestamp']:
            if f in first and first[f]:
                ts_field = f
                break
        if ts_field:
            latest = max(r[ts_field] for r in ratings_data if r.get(ts_field))
            if isinstance(latest, str):
                latest = datetime.fromisoformat(latest.replace('Z', '+00:00'))
            if hasattr(latest, 'tzinfo') and latest.tzinfo:
                latest = latest.astimezone(manila_tz)
            else:
                latest = pytz.UTC.localize(latest).astimezone(manila_tz)
            st.caption(f"Last updated {latest.strftime('%B %d, %Y at %-I:%M %p')}")
        else:
            st.caption("Timestamp unavailable")
    except Exception:
        st.caption("Timestamp unavailable")


def _render_store_table(ratings_data):
    # Chunked rendering — Streamlit's st.markdown chokes on large HTML.
    # Render header + rows in batches using st.html().
    CHUNK = 30

    for i in range(0, len(ratings_data), CHUNK):
        chunk = ratings_data[i:i + CHUNK]
        is_first = (i == 0)
        is_last = (i + CHUNK >= len(ratings_data))

        html = ""
        if is_first:
            html += """<div class="store-table">
                <div class="store-table-header">
                    <span>#</span><span>Store</span>
                    <span>Rating</span><span>Stars</span><span>Trend</span>
                </div>"""

        for s in chunk:
            rank_cls = "store-rank-top" if s['rank'] <= 3 else "store-rank"
            rc = rating_class(s['rating'])
            html += f"""<div class="store-row">
                <span class="{rank_cls}">{s['rank']}</span>
                <div class="store-info">
                    <span class="store-name-text">{s['store_name']}</span>
                    {platform_label(s['platform'])}
                </div>
                <div class="rating-cell">
                    <span class="rating-value {rc}">{s['rating']:.1f}</span>
                </div>
                <span class="rating-stars">{star_str(s['rating'])}</span>
                {trend_html(s['trend'], s['trend_value'])}
            </div>"""

        if is_last:
            html += "</div>"

        st.markdown(html, unsafe_allow_html=True)


def _render_distribution(ratings_data):
    total = len(ratings_data)
    dist = {
        '5': sum(1 for r in ratings_data if int(r['rating']) == 5),
        '4': sum(1 for r in ratings_data if int(r['rating']) == 4),
        '3': sum(1 for r in ratings_data if int(r['rating']) == 3),
        '2': sum(1 for r in ratings_data if int(r['rating']) == 2),
        '1': sum(1 for r in ratings_data if int(r['rating']) <= 1),
    }

    st.markdown('<div class="dist-section"><div class="dist-title">Distribution</div>', unsafe_allow_html=True)
    for stars, count in dist.items():
        pct = (count / total * 100) if total > 0 else 0
        st.markdown(f'<div class="dist-row"><span class="dist-label">{stars}</span><div class="dist-bar-bg"><div class="dist-bar-fill" style="width:{pct}%"></div></div><span class="dist-count">{count} ({pct:.0f}%)</span></div>', unsafe_allow_html=True)
    st.markdown('</div>', unsafe_allow_html=True)


def _render_reports_section():
    """Reports rendered inside the top-level expander"""

    tab1, tab2, tab3 = st.tabs(["Current Snapshot", "Historical Ratings", "Store Summary"])

    with tab1:
        st.caption("Export the current filtered view.")
        all_data = db.get_store_ratings_dashboard(sort_by='rating_desc')
        if all_data:
            df = build_snapshot_df(all_data)
            st.dataframe(df.head(10), use_container_width=True, hide_index=True)
            if len(df) > 10:
                st.caption(f"Showing 10 of {len(df)} stores.")
            st.download_button(
                "Download Snapshot",
                data=df.to_csv(index=False),
                file_name=f"ratings_snapshot_{datetime.now().strftime('%Y-%m-%d')}.csv",
                mime="text/csv", use_container_width=True
            )
        else:
            st.info("No rating data available.")

    with tab2:
        st.caption("Individual scrape records by store and date range.")
        hc1, hc2, hc3 = st.columns(3)
        with hc1:
            h_plat = st.selectbox("Platform", ['all', 'grabfood', 'foodpanda'],
                format_func=lambda x: {'all': 'All', 'grabfood': 'GrabFood', 'foodpanda': 'Foodpanda'}[x],
                key="rpt_h_plat")
        with hc2:
            scrape_dates = get_scrape_dates()
            earliest = date.today() - timedelta(days=90)
            if scrape_dates:
                e = scrape_dates[-1]
                if isinstance(e, str): e = datetime.strptime(e, '%Y-%m-%d').date()
                earliest = e
            h_start = st.date_input("From", value=earliest, key="rpt_h_start")
        with hc3:
            h_end = st.date_input("To", value=date.today(), key="rpt_h_end")

        all_stores = get_store_list_for_filter()
        h_stores = st.multiselect("Stores (empty = all)", options=all_stores, default=[], key="rpt_h_stores")

        if scrape_dates:
            st.caption(f"Available scrape dates: {', '.join(str(d) for d in scrape_dates[:8])}")

        data = get_ratings_history(store_names=h_stores or None, platform=h_plat, start_date=h_start, end_date=h_end)
        if data:
            df = build_history_df(data)
            st.caption(f"{len(df)} records found.")
            st.dataframe(df.head(20), use_container_width=True, hide_index=True)
            if len(df) > 20:
                st.caption(f"Showing 20 of {len(df)}.")
            st.download_button("Download History", data=df.to_csv(index=False),
                file_name=f"ratings_history_{h_plat}_{h_start}_{h_end}.csv",
                mime="text/csv", use_container_width=True, key="rpt_h_dl")
        else:
            st.warning("No records found for these filters.")

    with tab3:
        st.caption("Per-store aggregation: first vs latest rating, highs, lows, net change.")
        sc1, sc2, sc3 = st.columns(3)
        with sc1:
            s_plat = st.selectbox("Platform", ['all', 'grabfood', 'foodpanda'],
                format_func=lambda x: {'all': 'All', 'grabfood': 'GrabFood', 'foodpanda': 'Foodpanda'}[x],
                key="rpt_s_plat")
        with sc2:
            s_start = st.date_input("From", value=date.today() - timedelta(days=90), key="rpt_s_start")
        with sc3:
            s_end = st.date_input("To", value=date.today(), key="rpt_s_end")

        s_stores = st.multiselect("Stores (empty = all)",
            options=get_store_list_for_filter(),
            default=[], key="rpt_s_stores")

        data = get_ratings_history(store_names=s_stores or None, platform=s_plat, start_date=s_start, end_date=s_end)
        if data:
            df = build_summary_df(data)
            st.caption(f"Summary for {len(df)} stores.")
            st.dataframe(df, use_container_width=True, hide_index=True)
            st.download_button("Download Summary", data=df.to_csv(index=False),
                file_name=f"ratings_summary_{s_plat}_{s_start}_{s_end}.csv",
                mime="text/csv", use_container_width=True, key="rpt_s_dl")
        else:
            st.warning("No records found for these filters.")


# ─────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────

def main():
    col_nav, _, _ = st.columns([1, 2, 1])
    with col_nav:
        st.link_button("Back to Uptime", "https://cocopanwatchtower.com/", use_container_width=True)

    st.markdown("""
    <div class="top-bar">
        <div class="top-bar-left">
            <h1 class="top-bar-title">Store Ratings</h1>
            <p class="top-bar-subtitle">Performance tracking across GrabFood and Foodpanda</p>
        </div>
    </div>
    """, unsafe_allow_html=True)

    # ── Export at top — progressive disclosure ──
    with st.expander("Export Reports"):
        _render_reports_section()

    # ── Filters ──
    st.markdown('<p class="section-label">Filters</p>', unsafe_allow_html=True)
    fc1, fc2, fc3 = st.columns(3)
    with fc1:
        platform_filter = st.selectbox("Platform", ['all', 'grabfood', 'foodpanda'],
            format_func=lambda x: {'all': 'All Platforms', 'grabfood': 'GrabFood', 'foodpanda': 'Foodpanda'}[x])
    with fc2:
        star_filter = st.selectbox("Rating", ['all', '5', '4', '3', '2', '1'],
            format_func=lambda x: 'All Ratings' if x == 'all' else f"{x}-star")
    with fc3:
        sort_filter = st.selectbox("Sort", ['rating_desc', 'rating_asc', 'name_asc'],
            format_func=lambda x: {'rating_desc': 'Highest First', 'rating_asc': 'Lowest First', 'name_asc': 'Name A-Z'}[x])

    # ── Query params ──
    platform_param = None if platform_filter == 'all' else platform_filter
    min_r = max_r = None
    if star_filter != 'all':
        s = int(star_filter)
        if s == 5: min_r = 5.0
        elif s == 1: max_r = 1.99
        else: min_r, max_r = float(s), float(s) + 0.99

    ratings_data = db.get_store_ratings_dashboard(
        platform=platform_param, min_rating=min_r, max_rating=max_r, sort_by=sort_filter)

    if not ratings_data:
        st.info("No stores match the current filters.")
        return

    _show_last_updated(ratings_data)

    # ── Metrics ──
    avg = sum(r['rating'] for r in ratings_data) / len(ratings_data)
    count_5 = sum(1 for r in ratings_data if int(r['rating']) == 5)
    count_below_4 = sum(1 for r in ratings_data if r['rating'] < 4.0)
    improving = sum(1 for r in ratings_data if r['trend'] == 'up' and r['trend_value'] > 0)

    st.markdown(f"""
    <div class="metric-row">
        <div class="metric-card">
            <div class="metric-label">Average Rating</div>
            <div class="metric-value">{avg:.2f}</div>
            <div class="metric-sub">{len(ratings_data)} stores</div>
        </div>
        <div class="metric-card">
            <div class="metric-label">5-Star Stores</div>
            <div class="metric-value">{count_5}</div>
            <div class="metric-sub">perfect score</div>
        </div>
        <div class="metric-card">
            <div class="metric-label">Below 4.0</div>
            <div class="metric-value{' rating-critical' if count_below_4 > 0 else ''}">{count_below_4}</div>
            <div class="metric-sub">need attention</div>
        </div>
        <div class="metric-card">
            <div class="metric-label">Improving</div>
            <div class="metric-value">{improving}</div>
            <div class="metric-sub">trending up</div>
        </div>
    </div>
    """, unsafe_allow_html=True)

    # ── Layout ──
    col_main, col_side = st.columns([5, 1])
    with col_side:
        _render_distribution(ratings_data)
    with col_main:
        _render_store_table(ratings_data)


if __name__ == "__main__":
    main()