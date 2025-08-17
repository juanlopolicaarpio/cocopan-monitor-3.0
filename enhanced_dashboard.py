#!/usr/bin/env python3
"""
CocoPan Store Status Dashboard - Complete Fixed Version
Fixed time display formatting and rounding issues
"""
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import pytz
import logging

# Import our production modules
from config import config
from database import db

# Set page config
st.set_page_config(
    page_title="CocoPan Store Status Dashboard",
    page_icon="🥥",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# Setup logging
logging.basicConfig(level=getattr(logging, config.LOG_LEVEL))
logger = logging.getLogger(__name__)

# Professional CSS styling
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');
    
    .main {
        font-family: 'Inter', sans-serif;
        background-color: #f8f9fa;
    }
    
    .main > div {
        padding-top: 2rem;
        padding-bottom: 2rem;
    }
    
    /* Header Styling */
    .dashboard-header {
        background: linear-gradient(90deg, #28a745 0%, #20c997 100%);
        padding: 2rem 1.5rem;
        border-radius: 12px;
        margin-bottom: 2rem;
        color: white;
        text-align: center;
        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
    }
    
    .dashboard-title {
        font-size: 2.5rem;
        font-weight: 700;
        margin-bottom: 0.5rem;
    }
    
    .dashboard-subtitle {
        font-size: 1.1rem;
        opacity: 0.9;
        font-weight: 400;
    }
    
    /* Section Headers */
    .section-header {
        background: white;
        padding: 1rem 1.5rem;
        border-radius: 8px;
        margin-bottom: 1rem;
        border-left: 4px solid #28a745;
        box-shadow: 0 2px 4px rgba(0, 0, 0, 0.05);
    }
    
    .section-title {
        font-size: 1.4rem;
        font-weight: 600;
        color: #2c3e50;
        margin: 0;
    }
    
    .section-subtitle {
        font-size: 0.9rem;
        color: #6c757d;
        margin: 0;
        margin-top: 0.25rem;
    }
    
    /* Cards */
    .metric-card {
        background: white;
        padding: 1.5rem;
        border-radius: 12px;
        box-shadow: 0 2px 8px rgba(0, 0, 0, 0.08);
        border: 1px solid #e9ecef;
        text-align: center;
        margin-bottom: 1rem;
    }
    
    .metric-value {
        font-size: 2.5rem;
        font-weight: 700;
        margin: 0.5rem 0;
    }
    
    .metric-label {
        font-size: 1rem;
        color: #6c757d;
        font-weight: 500;
    }
    
    .status-online { color: #28a745; }
    .status-offline { color: #dc3545; }
    .status-warning { color: #ffc107; }
    
    /* Chart Container */
    .chart-container {
        background: white;
        padding: 1.5rem;
        border-radius: 12px;
        box-shadow: 0 2px 8px rgba(0, 0, 0, 0.08);
        border: 1px solid #e9ecef;
        margin-bottom: 1rem;
    }
    
    /* Table Styling */
    .dataframe {
        font-family: 'Inter', sans-serif !important;
        font-size: 0.9rem;
    }
    
    /* Auto-refresh indicator */
    .refresh-indicator {
        position: fixed;
        top: 20px;
        right: 20px;
        background: #28a745;
        color: white;
        padding: 0.5rem 1rem;
        border-radius: 20px;
        font-size: 0.8rem;
        font-weight: 500;
        z-index: 999;
        box-shadow: 0 2px 8px rgba(0, 0, 0, 0.2);
    }
    
    /* System Status */
    .system-status {
        position: fixed;
        top: 70px;
        right: 20px;
        background: #17a2b8;
        color: white;
        padding: 0.5rem 1rem;
        border-radius: 20px;
        font-size: 0.8rem;
        font-weight: 500;
        z-index: 999;
        box-shadow: 0 2px 8px rgba(0, 0, 0, 0.2);
    }
    
    /* Mobile Responsive */
    @media (max-width: 768px) {
        .dashboard-title {
            font-size: 2rem;
        }
        
        .metric-value {
            font-size: 2rem;
        }
        
        .section-title {
            font-size: 1.2rem;
        }
        
        .chart-container, .metric-card {
            padding: 1rem;
        }
    }
    
    /* Hide Streamlit elements */
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    .stDeployButton {display:none;}
</style>
""", unsafe_allow_html=True)

@st.cache_data(ttl=config.DASHBOARD_AUTO_REFRESH)
def load_data():
    """Load data with restored queries"""
    try:
        with db.get_connection() as conn:
            # 1. Latest status
            latest_status_query = """
                SELECT 
                    s.name,
                    s.url,
                    s.platform,
                    sc.is_online,
                    sc.checked_at,
                    sc.response_time_ms
                FROM stores s
                INNER JOIN status_checks sc ON s.id = sc.store_id
                INNER JOIN (
                    SELECT store_id, MAX(checked_at) as latest_check
                    FROM status_checks
                    GROUP BY store_id
                ) latest ON sc.store_id = latest.store_id AND sc.checked_at = latest.latest_check
                ORDER BY s.name
            """
            latest_status = pd.read_sql_query(latest_status_query, conn)
            
            # 2. Hourly data (KEEP EXISTING QUERY)
            hourly_query = """
                SELECT 
                    EXTRACT(HOUR FROM checked_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Manila') as hour,
                    COUNT(*) as total_checks,
                    SUM(CASE WHEN is_online THEN 1 ELSE 0 END) as online_checks,
                    ROUND(AVG(CASE WHEN is_online THEN 100.0 ELSE 0.0 END), 1) as online_pct,
                    ROUND(AVG(CASE WHEN NOT is_online THEN 100.0 ELSE 0.0 END), 1) as offline_pct
                FROM status_checks 
                WHERE checked_at >= NOW() - INTERVAL '24 hours'
                GROUP BY EXTRACT(HOUR FROM checked_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Manila')
                ORDER BY hour
            """
            hourly_data = pd.read_sql_query(hourly_query, conn)
            
            # 3. Store logs
            logs_query = """
                SELECT 
                    s.name,
                    s.platform,
                    sc.is_online,
                    sc.checked_at,
                    sc.response_time_ms
                FROM stores s
                INNER JOIN status_checks sc ON s.id = sc.store_id
                WHERE sc.checked_at >= NOW() - INTERVAL '24 hours'
                ORDER BY sc.checked_at DESC
                LIMIT 50
            """
            store_logs = pd.read_sql_query(logs_query, conn)
            
            # 4. Daily uptime
            daily_uptime_query = """
                SELECT 
                    s.name,
                    s.platform,
                    COUNT(sc.id) as total_checks,
                    SUM(CASE WHEN sc.is_online THEN 1 ELSE 0 END) as online_checks,
                    SUM(CASE WHEN NOT sc.is_online THEN 1 ELSE 0 END) as offline_checks,
                    ROUND(
                        (SUM(CASE WHEN sc.is_online THEN 1 ELSE 0 END) * 100.0 / COUNT(sc.id)), 
                        1
                    ) as uptime_percentage,
                    MIN(sc.checked_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Manila') as first_check_manila,
                    MAX(sc.checked_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Manila') as last_check_manila
                FROM stores s
                LEFT JOIN status_checks sc ON s.id = sc.store_id 
                    AND DATE(sc.checked_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Manila') = CURRENT_DATE
                GROUP BY s.id, s.name, s.platform
                HAVING COUNT(sc.id) > 0
                ORDER BY uptime_percentage DESC
            """
            daily_uptime = pd.read_sql_query(daily_uptime_query, conn)
            
            return latest_status, hourly_data, store_logs, daily_uptime, None
            
    except Exception as e:
        logger.error(f"Error loading data: {e}")
        return None, None, None, None, str(e)

def get_philippines_time():
    """Get current Philippines time"""
    return config.get_current_time()

def create_status_pie_chart(online_stores, offline_stores, total_stores):
    """Create professional pie chart"""
    online_pct = (online_stores / total_stores * 100) if total_stores > 0 else 0
    offline_pct = 100 - online_pct
    
    fig = go.Figure(data=[go.Pie(
        labels=['Online', 'Offline'],
        values=[online_stores, offline_stores],
        marker_colors=['#28a745', '#dc3545'],
        hole=0.4,
        textinfo='none',
        hovertemplate='<b>%{label}</b><br>Count: %{value}<br>Percentage: %{percent}<extra></extra>'
    )])
    
    center_text = f"Total: {total_stores} stores<br>{online_stores} Online ({online_pct:.0f}%)<br>{offline_stores} Offline ({offline_pct:.0f}%)"
    
    fig.update_layout(
        height=400,
        showlegend=False,
        margin=dict(t=0, b=0, l=0, r=0),
        annotations=[
            dict(
                text=center_text,
                x=0.5, y=0.5,
                font_size=14,
                font_family="Inter",
                font_color="#2c3e50",
                showarrow=False
            )
        ],
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)'
    )
    
    return fig

def display_system_status():
    """Display system status indicator"""
    try:
        stats = db.get_database_stats()
        db_type = stats.get('db_type', 'unknown').upper()
        store_count = stats.get('store_count', 0)
        
        philippines_time = get_philippines_time()
        current_hour = philippines_time.hour
        in_monitoring_hours = config.is_monitor_time(current_hour)
        
        if in_monitoring_hours:
            status_text = f"🟢 {db_type} | {store_count} stores | Monitoring Active"
        else:
            status_text = f"🟡 {db_type} | {store_count} stores | Outside Hours"
            
        st.markdown(f'<div class="system-status">{status_text}</div>', unsafe_allow_html=True)
        
    except Exception as e:
        st.markdown('<div class="system-status">🔴 System Error</div>', unsafe_allow_html=True)

def main():
    # System status indicator
    display_system_status()
    
    # Auto-refresh indicator
    st.markdown('<div class="refresh-indicator">🔄 Auto-refresh: 5min</div>', unsafe_allow_html=True)
    
    # Load data
    latest_status, hourly_data, store_logs, daily_uptime, error = load_data()
    
    # Header
    philippines_time = get_philippines_time()
    timezone_abbr = philippines_time.strftime('%Z')
    if not timezone_abbr or timezone_abbr == philippines_time.strftime('%z'):
        timezone_abbr = 'PHT'
    
    st.markdown(f"""
    <div class="dashboard-header">
        <div class="dashboard-title">🥥 CocoPan Store Status Dashboard</div>
        <div class="dashboard-subtitle">Real-time monitoring across GrabFood and Foodpanda | {philippines_time.strftime('%B %d, %Y • %I:%M %p')} {timezone_abbr}</div>
    </div>
    """, unsafe_allow_html=True)
    
    # Handle errors
    if error:
        st.error(f"❌ Database Error: {error}")
        st.info("💡 Make sure the monitor service is running: `python monitor_service.py`")
        return
    
    if latest_status is None or len(latest_status) == 0:
        st.warning("⚠️ No data available yet.")
        st.info("💡 The monitor service will populate data during business hours (6 AM - 9 PM Philippines time).")
        return
    
    # 1️⃣ LIVE STORE MONITOR PANEL
    st.markdown("""
    <div class="section-header">
        <div class="section-title">1️⃣ Live Store Monitor</div>
        <div class="section-subtitle">Real-time view of all Cocopan stores across platforms</div>
    </div>
    """, unsafe_allow_html=True)
    
    # Platform filter
    col1, col2, col3 = st.columns([2, 1, 1])
    with col2:
        platforms = ['Both'] + list(latest_status['platform'].unique())
        selected_platform = st.selectbox("Platform Filter:", platforms, key="platform_filter")
    
    # Filter data
    if selected_platform != 'Both':
        filtered_data = latest_status[latest_status['platform'] == selected_platform]
    else:
        filtered_data = latest_status
    
    # Calculate metrics
    total_stores = len(filtered_data)
    online_stores = len(filtered_data[filtered_data['is_online'] == 1])
    offline_stores = total_stores - online_stores
    online_pct = (online_stores / total_stores * 100) if total_stores > 0 else 0
    
    # Live monitor layout
    col1, col2 = st.columns([1.2, 0.8])
    
    with col1:
        if total_stores > 0:
            st.markdown('<div class="chart-container">', unsafe_allow_html=True)
            fig = create_status_pie_chart(online_stores, offline_stores, total_stores)
            st.plotly_chart(fig, use_container_width=True, config={'displayModeBar': False})
            st.markdown('</div>', unsafe_allow_html=True)
    
    with col2:
        # Metric cards
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-label">Total Stores</div>
            <div class="metric-value">{total_stores}</div>
        </div>
        """, unsafe_allow_html=True)
        
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-label">Online</div>
            <div class="metric-value status-online">{online_stores}</div>
            <div class="metric-label">{online_pct:.0f}%</div>
        </div>
        """, unsafe_allow_html=True)
        
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-label">Offline</div>
            <div class="metric-value status-offline">{offline_stores}</div>
            <div class="metric-label">{100-online_pct:.0f}%</div>
        </div>
        """, unsafe_allow_html=True)
    
    # 2️⃣ HOURLY SNAPSHOT TRENDS PANEL (FIXED DISPLAY)
    st.markdown("""
    <div class="section-header">
        <div class="section-title">2️⃣ Hourly Snapshot Trends</div>
        <div class="section-subtitle">Track how uptime changes throughout the day</div>
    </div>
    """, unsafe_allow_html=True)
    
    col1, col2 = st.columns([1.5, 1])
    
    with col1:
        st.markdown('<div class="chart-container">', unsafe_allow_html=True)
        if len(hourly_data) > 0:
            # FIXED formatting - keep same data, just display better
            display_hourly = hourly_data.copy()
            
            # Convert 24-hour to 12-hour format (22:00 → 10:00 PM)
            def format_hour(hour):
                hour = int(hour)
                if hour == 0:
                    return "12:00 AM"
                elif hour < 12:
                    return f"{hour}:00 AM"
                elif hour == 12:
                    return "12:00 PM"
                else:
                    return f"{hour-12}:00 PM"
            
            display_hourly['Hour'] = display_hourly['hour'].apply(format_hour)
            
            # FIXED rounding - ensure percentages always add to 100%
            online_pct_rounded = display_hourly['online_pct'].round().astype(int)
            offline_pct_rounded = 100 - online_pct_rounded
            
            display_hourly['Online %'] = online_pct_rounded.astype(str) + '%'
            display_hourly['Offline %'] = offline_pct_rounded.astype(str) + '%'
            
            # Add status flags
            def get_flag(online_pct):
                if online_pct < 60:
                    return '🔴 Low uptime'
                elif online_pct < 80:
                    return '🟡 Monitor'
                else:
                    return '✅ Good'
            
            display_hourly['Status'] = display_hourly['online_pct'].apply(get_flag)
            
            st.subheader("📊 Hourly Tracker Table")
            st.dataframe(
                display_hourly[['Hour', 'Online %', 'Offline %', 'Status']],
                use_container_width=True,
                hide_index=True,
                height=300
            )
        else:
            st.info("⏳ Hourly data will appear as the system collects more data throughout the day.")
        st.markdown('</div>', unsafe_allow_html=True)
    
    with col2:
        st.markdown('<div class="chart-container">', unsafe_allow_html=True)
        if len(hourly_data) > 0:
            avg_online = hourly_data['online_pct'].mean()
            st.subheader("📈 Average Uptime")
            
            # Small donut chart
            fig_avg = go.Figure(data=[go.Pie(
                labels=['Online', 'Offline'],
                values=[avg_online, 100-avg_online],
                marker_colors=['#28a745', '#dc3545'],
                hole=0.6,
                textinfo='none'
            )])
            
            fig_avg.update_layout(
                height=250,
                showlegend=False,
                margin=dict(t=0, b=0, l=0, r=0),
                annotations=[
                    dict(
                        text=f"{avg_online:.0f}%<br>Online",
                        x=0.5, y=0.5,
                        font_size=18,
                        font_family="Inter",
                        font_color="#28a745",
                        showarrow=False
                    )
                ]
            )
            
            st.plotly_chart(fig_avg, use_container_width=True, config={'displayModeBar': False})
        st.markdown('</div>', unsafe_allow_html=True)
    
    # 3️⃣ DAILY STORE SUMMARY PANEL
    st.markdown("""
    <div class="section-header">
        <div class="section-title">3️⃣ Daily Store Summary</div>
        <div class="section-subtitle">Performance ranking by uptime - stores ranked best to worst</div>
    </div>
    """, unsafe_allow_html=True)
    
    st.markdown('<div class="chart-container">', unsafe_allow_html=True)
    if daily_uptime is not None and len(daily_uptime) > 0:
        # Format daily summary
        display_summary = daily_uptime.copy()
        display_summary['Store Name'] = display_summary['name'].str.replace('Cocopan - ', '').str.replace('Cocopan ', '')
        display_summary['Platform'] = display_summary['platform'].str.title()
        display_summary['% Uptime Today'] = display_summary['uptime_percentage'].astype(str) + '%'
        
        # Status with color coding
        def get_status_badge(pct):
            if pct >= 90:
                return '🟢 Excellent'
            elif pct >= 60:
                return '🟡 Monitor'
            else:
                return '🔴 Critical'
        
        display_summary['Current Status'] = display_summary['uptime_percentage'].apply(get_status_badge)
        display_summary['Checks Today'] = display_summary['total_checks'].astype(str)
        
        # Show the full summary table
        st.dataframe(
            display_summary[['Store Name', 'Platform', '% Uptime Today', 'Current Status', 'Checks Today']],
            use_container_width=True,
            hide_index=True,
            height=400
        )
        
        # Show breakdown by status
        excellent_stores = len(display_summary[display_summary['uptime_percentage'] >= 90])
        monitor_stores = len(display_summary[(display_summary['uptime_percentage'] >= 60) & (display_summary['uptime_percentage'] < 90)])
        critical_stores = len(display_summary[display_summary['uptime_percentage'] < 60])
        
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("🟢 Excellent (90%+)", excellent_stores)
        with col2:
            st.metric("🟡 Monitor (60-89%)", monitor_stores)
        with col3:
            st.metric("🔴 Critical (<60%)", critical_stores)
            
    else:
        st.info("⏳ Daily summary will populate as data is collected throughout the day.")
    st.markdown('</div>', unsafe_allow_html=True)
    
    # 4️⃣ DETAILED STORE LOGS PANEL
    st.markdown("""
    <div class="section-header">
        <div class="section-title">4️⃣ Detailed Store Logs</div>
        <div class="section-subtitle">Timestamped log of all store status changes today</div>
    </div>
    """, unsafe_allow_html=True)
    
    st.markdown('<div class="chart-container">', unsafe_allow_html=True)
    if store_logs is not None and len(store_logs) > 0:
        # Format store logs
        display_logs = store_logs.copy()
        
        # Convert timestamps to Philippines time
        try:
            display_logs['checked_at'] = pd.to_datetime(display_logs['checked_at'])
            if display_logs['checked_at'].dt.tz is None:
                display_logs['checked_at'] = display_logs['checked_at'].dt.tz_localize('UTC')
            
            ph_tz = config.get_timezone()
            display_logs['checked_at'] = display_logs['checked_at'].dt.tz_convert(ph_tz)
            display_logs['Time'] = display_logs['checked_at'].dt.strftime('%I:%M %p')
        except Exception as e:
            logger.error(f"Timezone conversion error: {e}")
            display_logs['Time'] = pd.to_datetime(display_logs['checked_at']).dt.strftime('%I:%M %p')
        
        display_logs['Store'] = display_logs['name'].str.replace('Cocopan - ', '').str.replace('Cocopan ', '')
        display_logs['Platform'] = display_logs['platform'].str.title()
        display_logs['Status'] = display_logs['is_online'].apply(lambda x: '✅ Online' if x else '❌ Offline')
        display_logs['Response Time'] = display_logs['response_time_ms'].fillna(0).astype(int).astype(str) + 'ms'
        
        # Show logs table
        st.dataframe(
            display_logs[['Time', 'Store', 'Platform', 'Status', 'Response Time']].head(20),
            use_container_width=True,
            hide_index=True,
            height=400
        )
    else:
        st.info("⏳ Store logs will appear as monitoring data is collected.")
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Footer
    st.markdown(f"""
    <div style="text-align: center; padding: 2rem; color: #6c757d; font-size: 0.9rem;">
        <strong>CocoPan Operations Dashboard</strong> • Last updated: {philippines_time.strftime('%I:%M %p')} {timezone_abbr} • 
        Auto-refresh: 5 minutes • Data timezone: {config.TIMEZONE} • Database: {db.db_type.upper()}
    </div>
    """, unsafe_allow_html=True)

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        st.error(f"❌ Dashboard Error: {e}")
        logger.error(f"Dashboard error: {e}")
        st.info("💡 Check the monitor service and database connection.")