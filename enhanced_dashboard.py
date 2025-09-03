#!/usr/bin/env python3
"""
CocoPan Watchtower - CLIENT-SAFE DASHBOARD (v2.1)
- Donut center shows ONLY uptime % (Online vs Offline); 'Stores Under Review' excluded
- 'Stores Under Review' is its own category (neither Online nor Offline)
- No verification/alarm wording
- Platform cards: 'Grab stores' and 'Foodpanda stores' with offline counts
- Platform cards positioned UNDER the Online/Offline/Under Review row
"""
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from datetime import datetime
import logging

# Import our production modules
from config import config
from database import db

# Set page config
st.set_page_config(
    page_title="CocoPan Watchtower",
    page_icon="🏢",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# Setup logging
logging.basicConfig(level=getattr(logging, config.LOG_LEVEL))
logger = logging.getLogger(__name__)

# ----------- STYLES -------------
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800&display=swap');
    #MainMenu, footer, .stDeployButton, header {visibility: hidden;}
    .main { font-family: 'Inter', sans-serif; background: #F8FAFC; color: #1E293B; min-height: 100vh; }
    .main > div { padding: 1.5rem; max-width: 1400px; margin: 0 auto; }

    .header-section { background: linear-gradient(135deg, #1E40AF 0%, #3B82F6 100%); border-radius: 16px; padding: 1.5rem; margin-bottom: 1.25rem; box-shadow: 0 4px 6px -1px rgba(0,0,0,.1);}
    h1 { color: #fff !important; font-size: 2.4rem !important; font-weight: 700 !important; text-align: center !important; margin:0 !important; letter-spacing:-.02em;}
    h3 { color: rgba(255,255,255,.9) !important; font-weight: 400 !important; font-size: 1rem !important; text-align:center !important; margin:.4rem 0 0 0 !important;}

    .section-header { background:#fff; border:1px solid #E2E8F0; border-radius:8px; padding:.9rem 1.1rem; margin:1.1rem 0 .9rem 0; box-shadow:0 1px 3px rgba(0,0,0,.06);}
    .section-title { font-size:1.1rem; font-weight:600; color:#1E293B; margin:0;}
    .section-subtitle { font-size:.85rem; color:#64748B; margin:.25rem 0 0 0;}

    [data-testid="metric-container"] { background:#fff; border:1px solid #E2E8F0; border-radius:12px; padding:1.25rem 1rem; box-shadow:0 1px 3px rgba(0,0,0,.06); text-align:center; transition:.2s; }
    [data-testid="metric-container"]:hover { box-shadow:0 4px 6px -1px rgba(0,0,0,.08); transform: translateY(-1px); }
    [data-testid="metric-value"] { color:#1E293B; font-weight:700; font-size:1.75rem; }
    [data-testid="metric-label"] { color:#64748B; font-weight:600; font-size:.8rem; text-transform:uppercase; letter-spacing:.05em; }

    .stTabs [data-baseweb="tab-list"] { gap:0; background:#F1F5F9; border-radius:8px; padding:.25rem; border:1px solid #E2E8F0;}
    .stTabs [data-baseweb="tab"] { background:transparent; border:none; border-radius:6px; color:#64748B; font-weight:500; padding:.65rem 1.2rem; transition:.2s; font-size:.85rem;}
    .stTabs [data-baseweb="tab"]:hover { background:#E2E8F0; color:#1E293B; }
    .stTabs [aria-selected="true"] { background:#fff !important; color:#1E293B !important; box-shadow:0 1px 2px rgba(0,0,0,.05); font-weight:600;}

    .stDataFrame { background:#fff; border-radius:12px; border:1px solid #E2E8F0; overflow:hidden; box-shadow:0 1px 3px rgba(0,0,0,.06);}
    .stDataFrame thead tr th { background:#F8FAFC !important; color:#475569 !important; font-weight:600 !important; text-transform:uppercase; font-size:.72rem; letter-spacing:.05em; border:none !important; border-bottom:1px solid #E2E8F0 !important; padding:.8rem .6rem !important;}
    .stDataFrame tbody tr td { background:#fff !important; color:#1E293B !important; border:none !important; border-bottom:1px solid #F1F5F9 !important; padding:.65rem !important;}
    .stDataFrame tbody tr:hover td { background:#F8FAFC !important;}

    .chart-container { background:#fff; border:1px solid #E2E8F0; border-radius:12px; padding:.75rem; box-shadow:0 1px 3px rgba(0,0,0,.06); }

    .filter-container { background:#F8FAFC; border:1px solid #E2E8F0; border-radius:8px; padding:.9rem; margin-bottom:.9rem; }
    @media (max-width: 768px){ h1{font-size:1.9rem !important;} .main>div{padding:1rem;} }
</style>
""", unsafe_allow_html=True)

# ----------- HELPERS -------------

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
    """True if this row should be shown as 'Stores Under Review' (neither online nor offline)."""
    if pd.isna(error_message):
        return False
    msg = str(error_message).strip()
    return msg.startswith('[BLOCKED]') or msg.startswith('[UNKNOWN]') or msg.startswith('[ERROR]')

@st.cache_data(ttl=config.DASHBOARD_AUTO_REFRESH)
def load_comprehensive_data():
    """Load data without exposing verification to UI."""
    try:
        with db.get_connection() as conn:
            latest_status_query = """
                SELECT 
                    s.id,
                    s.name,
                    s.platform,
                    sc.is_online,
                    sc.checked_at,
                    sc.response_time_ms,
                    sc.error_message
                FROM stores s
                INNER JOIN status_checks sc ON s.id = sc.store_id
                INNER JOIN (
                    SELECT store_id, MAX(checked_at) AS latest_check
                    FROM status_checks
                    GROUP BY store_id
                ) latest 
                  ON sc.store_id = latest.store_id 
                 AND sc.checked_at = latest.latest_check
                ORDER BY s.name
            """
            latest_status = pd.read_sql_query(latest_status_query, conn)

            if not latest_status.empty:
                latest_status['platform'] = latest_status['platform'].apply(standardize_platform_name)

            daily_uptime_query = """
                SELECT 
                    s.id,
                    s.name,
                    s.platform,
                    COUNT(sc.id) AS total_checks,
                    SUM(CASE WHEN sc.is_online = true THEN 1 ELSE 0 END) AS online_checks,
                    SUM(CASE WHEN sc.is_online = false THEN 1 ELSE 0 END) AS downtime_count,
                    ROUND((SUM(CASE WHEN sc.is_online = true THEN 1 ELSE 0 END) * 100.0 / COUNT(sc.id)), 1) AS uptime_percentage
                FROM stores s
                LEFT JOIN status_checks sc ON s.id = sc.store_id 
                  AND DATE(sc.checked_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Manila') = CURRENT_DATE
                GROUP BY s.id, s.name, s.platform
                HAVING COUNT(sc.id) > 0
                ORDER BY uptime_percentage DESC
            """
            daily_uptime = pd.read_sql_query(daily_uptime_query, conn)
            if not daily_uptime.empty:
                daily_uptime['platform'] = daily_uptime['platform'].apply(standardize_platform_name)

            return latest_status, daily_uptime, None
    except Exception as e:
        logger.error(f"Error loading data: {e}")
        return None, None, str(e)

def create_donut(online_count: int, offline_count: int):
    """Donut with only percentage text inside (no caption)."""
    total = max(online_count + offline_count, 1)
    uptime_pct = online_count / total * 100.0

    fig = go.Figure(data=[go.Pie(
        labels=['Online', 'Offline'],
        values=[online_count, offline_count],
        hole=0.65,
        marker=dict(
            colors=['#059669', '#EF4444'],
            line=dict(width=2, color='#FFFFFF')
        ),
        textinfo='none',
        hovertemplate='<b>%{label}</b><br>%{value} stores (%{percent})<extra></extra>',
        showlegend=True
    )])

    fig.add_annotation(
        text=f"<b style='font-size:28px'>{uptime_pct:.0f}%</b>",
        x=0.5, y=0.5, showarrow=False, font=dict(family="Inter")
    )

    fig.update_layout(
        height=250,
        margin=dict(t=12, b=12, l=12, r=12),
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)',
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=-0.08,
            xanchor="center",
            x=0.5,
            font=dict(size=10)
        )
    )
    return fig

def get_last_check_time(latest_status):
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

# ----------- APP -------------

def main():
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

    # 'Stores Under Review' (neither online nor offline)
    under_review_mask = latest_status['error_message'].apply(is_under_review)
    under_review_count = int(under_review_mask.sum())

    # Use only non-under-review rows for Online/Offline metrics & donut
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

    # --- Compact top layout: metrics left, donut right ---
    left, right = st.columns([1.6, 1])

    with left:
        st.markdown("### Network Overview")

        # Row 1: Online / Offline / Under Review
        m1, m2, m3 = st.columns(3)
        with m1:
            st.metric("Online Stores", f"{online_stores}", f"{online_pct:.0f}% uptime")
        with m2:
            st.metric("Offline Stores", f"{offline_stores}", "being monitored")
        with m3:
            st.metric("Stores Under Review", f"{under_review_count}", "routine checks")

        # Row 2: Platform cards UNDER the metrics row
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

    # Tabs (with 'Under Review' filter)
    tab1, tab2, tab3 = st.tabs(["🔴 Live Operations Monitor", "📊 Store Uptime Analytics", "📉 Downtime Events"])

    with tab1:
        st.markdown(f"""
        <div class="section-header">
            <div class="section-title">Live Operations Monitor</div>
            <div class="section-subtitle">Real-time store status • Updated {last_check_time.strftime('%I:%M %p')} Manila Time</div>
        </div>
        """, unsafe_allow_html=True)

        # Filters
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

        # Apply filters
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
                elif row['is_online']:
                    status_labels.append("🟢 Online")
                else:
                    status_labels.append("🔴 Offline")
            display['Status'] = status_labels

            # Last checked time in Manila
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

    with tab2:
        st.markdown(f"""
        <div class="section-header">
            <div class="section-title">Store Uptime Analytics</div>
            <div class="section-subtitle">Daily performance metrics and availability statistics</div>
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
                disp['Total Checks'] = filt['total_checks'].astype(str)
                disp['Times Down'] = filt['downtime_count'].astype(str)

                sort_vals = filt['uptime_percentage'].astype(float)
                idx = sort_vals.sort_values(ascending=(sort_order == "Lowest to Highest")).index
                disp = disp.loc[idx].reset_index(drop=True)

                st.dataframe(disp, use_container_width=True, hide_index=True, height=420)
        else:
            st.info("Performance analytics will appear as new data comes in.")

    with tab3:
        st.markdown(f"""
        <div class="section-header">
            <div class="section-title">Downtime Events Analysis</div>
            <div class="section-subtitle">Overview of offline events and frequency patterns</div>
        </div>
        """, unsafe_allow_html=True)

        try:
            with db.get_connection() as conn:
                downtime_query = """
                    SELECT 
                        s.name,
                        s.platform,
                        COUNT(sc.id) AS downtime_events,
                        MIN(sc.checked_at) AS first_downtime,
                        MAX(sc.checked_at) AS last_downtime
                    FROM stores s
                    INNER JOIN status_checks sc ON s.id = sc.store_id
                    WHERE sc.is_online = false 
                      AND DATE(sc.checked_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Manila') = CURRENT_DATE
                    GROUP BY s.id, s.name, s.platform
                    ORDER BY downtime_events DESC
                """
                downtime = pd.read_sql_query(downtime_query, conn)
                if not downtime.empty:
                    downtime['platform'] = downtime['platform'].apply(standardize_platform_name)
        except Exception as e:
            downtime = pd.DataFrame()
            st.error(f"Error loading downtime events: {e}")

        if downtime.empty:
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
                st.info(f"No downtime events for {pf}.")
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

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        st.error(f"System Error: {e}")
        logger.error(f"Dashboard error: {e}")
