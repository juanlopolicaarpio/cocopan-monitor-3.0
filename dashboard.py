#!/usr/bin/env python3
"""
CocoPan Store Status Dashboard - Enhanced Production Version
Works with both PostgreSQL and SQLite, maintains existing design
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

# Set page config (keep existing design)
st.set_page_config(
    page_title="CocoPan Store Status Dashboard",
    page_icon="🥥",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# Setup logging
logging.basicConfig(level=getattr(logging, config.LOG_LEVEL))
logger = logging.getLogger(__name__)

# Professional CSS styling (keep existing design)
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
    
    /* Status Badges */
    .status-badge {
        padding: 0.25rem 0.75rem;
        border-radius: 20px;
        font-weight: 600;
        font-size: 0.8rem;
        display: inline-block;
    }
    
    .badge-online {
        background-color: #d4edda;
        color: #155724;
    }
    
    .badge-offline {
        background-color: #f8d7da;
        color: #721c24;
    }
    
    .badge-warning {
        background-color: #fff3cd;
        color: #856404;
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
    """Load data from database with caching"""
    try:
        latest_status = db.get_latest_status()
        hourly_data = db.get_hourly_data()
        store_logs = db.get_store_logs()
        daily_uptime = db.get_daily_uptime()
        
        return latest_status, hourly_data, store_logs, daily_uptime, None
        
    except Exception as e:
        logger.error(f"Error loading data: {e}")
        return None, None, None, None, str(e)

def get_philippines_time():
    """Get current Philippines time"""
    ph_tz = pytz.timezone(config.TIMEZONE)
    return datetime.now(ph_tz)

def create_status_pie_chart(online_stores, offline_stores, total_stores):
    """Create professional pie chart (keep existing design)"""
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
    
    # Center text as specified in design
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
        
        # Check if monitor service is running (approximate)
        manila_time = get_philippines_time()
        current_hour = manila_time.hour
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
    
    # Auto-refresh indicator (keep existing)
    st.markdown('<div class="refresh-indicator">🔄 Auto-refresh: 5min</div>', unsafe_allow_html=True)
    
    # Load data
    latest_status, hourly_data, store_logs, daily_uptime, error = load_data()
    
    # Header (keep existing design)
    philippines_time = get_philippines_time()
    st.markdown(f"""
    <div class="dashboard-header">
        <div class="dashboard-title">🥥 CocoPan Store Status Dashboard</div>
        <div class="dashboard-subtitle">Real-time monitoring across GrabFood and Foodpanda | {philippines_time.strftime('%B %d, %Y • %I:%M %p PHT')}</div>
    </div>
    """, unsafe_allow_html=True)
    
    # Handle errors
    if error:
        st.error(f"❌ Database Error: {error}")
        st.info("💡 Make sure the monitor service is running: `python monitor_service.py`")
        return
    
    if latest_status is None or len(latest_status) == 0:
        st.warning("⚠️ No data available yet.")
        st.info("💡 The monitor service will populate data during business hours (6 AM - 9 PM Manila time).")
        
        # Show system info
        try:
            stats = db.get_database_stats()
            if stats:
                st.json(stats)
        except:
            pass
        return
    
    # 1️⃣ LIVE STORE MONITOR PANEL (keep existing design)
    st.markdown("""
    <div class="section-header">
        <div class="section-title">1️⃣ Live Store Monitor</div>
        <div class="section-subtitle">Real-time view of all Cocopan stores across platforms</div>
    </div>
    """, unsafe_allow_html=True)
    
    # Platform filter (keep existing)
    col1, col2, col3 = st.columns([2, 1, 1])
    with col2:
        platforms = ['Both'] + list(latest_status['platform'].unique())
        selected_platform = st.selectbox("Platform Filter:", platforms, key="platform_filter")
    
    # Filter data (keep existing logic)
    if selected_platform != 'Both':
        filtered_data = latest_status[latest_status['platform'] == selected_platform]
    else:
        filtered_data = latest_status
    
    # Calculate metrics (keep existing)
    total_stores = len(filtered_data)
    online_stores = len(filtered_data[filtered_data['is_online'] == 1])
    offline_stores = total_stores - online_stores
    online_pct = (online_stores / total_stores * 100) if total_stores > 0 else 0
    
    # Live monitor layout (keep existing)
    col1, col2 = st.columns([1.2, 0.8])
    
    with col1:
        if total_stores > 0:
            st.markdown('<div class="chart-container">', unsafe_allow_html=True)
            fig = create_status_pie_chart(online_stores, offline_stores, total_stores)
            st.plotly_chart(fig, use_container_width=True, config={'displayModeBar': False})
            st.markdown('</div>', unsafe_allow_html=True)
    
    with col2:
        # Metric cards (keep existing design)
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
    
    # 2️⃣ HOURLY SNAPSHOT TRENDS PANEL (keep existing)
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
            # Format hourly data for display (keep existing logic)
            display_hourly = hourly_data.copy()
            display_hourly['Hour'] = display_hourly['hour'].astype(str).str.zfill(2) + ':00'
            display_hourly['Online %'] = display_hourly['online_pct'].astype(int).astype(str) + '%'
            display_hourly['Offline %'] = display_hourly['offline_pct'].astype(int).astype(str) + '%'
            
            # Add status flags (keep existing)
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
            
            # Small donut chart (keep existing)
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
    
    # 3️⃣ DAILY STORE SUMMARY PANEL (keep existing)
    st.markdown("""
    <div class="section-header">
        <div class="section-title">3️⃣ Daily Store Summary</div>
        <div class="section-subtitle">Performance ranking by uptime - stores ranked best to worst</div>
    </div>
    """, unsafe_allow_html=True)
    
    st.markdown('<div class="chart-container">', unsafe_allow_html=True)
    if len(daily_uptime) > 0:
        # Format daily summary with color coding (keep existing)
        display_summary = daily_uptime.copy()
        display_summary['Store Name'] = display_summary['name'].str.replace('Cocopan - ', '').str.replace('Cocopan ', '')
        display_summary['Platform'] = display_summary['platform'].str.title()
        display_summary['% Uptime Today'] = display_summary['uptime_percentage'].astype(str) + '%'
        
        # Status with color coding (keep existing)
        def get_status_badge(pct):
            if pct >= 90:
                return '🟢 Excellent'
            elif pct >= 60:
                return '🟡 Monitor'
            else:
                return '🔴 Critical'
        
        display_summary['Current Status'] = display_summary['uptime_percentage'].apply(get_status_badge)
        display_summary['Checks Today'] = display_summary['total_checks'].astype(str)
        
        # Show summary table (keep existing)
        st.dataframe(
            display_summary[['Store Name', 'Platform', '% Uptime Today', 'Current Status', 'Checks Today']],
            use_container_width=True,
            hide_index=True,
            height=400
        )
    else:
        st.info("⏳ Daily summary will populate as data is collected throughout the day.")
    st.markdown('</div>', unsafe_allow_html=True)
    
    # 4️⃣ DETAILED STORE LOGS PANEL (keep existing)
    st.markdown("""
    <div class="section-header">
        <div class="section-title">4️⃣ Detailed Store Logs</div>
        <div class="section-subtitle">Timestamped log of all store status changes today</div>
    </div>
    """, unsafe_allow_html=True)
    
    st.markdown('<div class="chart-container">', unsafe_allow_html=True)
    if len(store_logs) > 0:
        # Format store logs (keep existing logic)
        display_logs = store_logs.copy()
        # Convert to Philippines time
        display_logs['Time'] = pd.to_datetime(display_logs['checked_at']).dt.tz_localize('UTC').dt.tz_convert('Asia/Manila').dt.strftime('%I:%M %p')
        display_logs['Store'] = display_logs['name'].str.replace('Cocopan - ', '').str.replace('Cocopan ', '')
        display_logs['Platform'] = display_logs['platform'].str.title()
        display_logs['Status'] = display_logs['is_online'].apply(lambda x: '✅ Online' if x else '❌ Offline')
        display_logs['Response Time'] = display_logs['response_time_ms'].fillna(0).astype(int).astype(str) + 'ms'
        
        # Show logs table (keep existing)
        st.dataframe(
            display_logs[['Time', 'Store', 'Platform', 'Status', 'Response Time']],
            use_container_width=True,
            hide_index=True,
            height=400
        )
    else:
        st.info("⏳ Store logs will appear as monitoring data is collected.")
    st.markdown('</div>', unsafe_allow_html=True)
    
    # Footer (keep existing)
    st.markdown(f"""
    <div style="text-align: center; padding: 2rem; color: #6c757d; font-size: 0.9rem;">
        <strong>CocoPan Operations Dashboard</strong> • Last updated: {philippines_time.strftime('%I:%M %p PHT')} • 
        Auto-refresh: 5 minutes • Data timezone: Philippines (UTC+8) • Database: {db.db_type.upper()}
    </div>
    """, unsafe_allow_html=True)

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        st.error(f"❌ Dashboard Error: {e}")
        logger.error(f"Dashboard error: {e}")
        st.info("💡 Check the monitor service and database connection.")