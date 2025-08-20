#!/usr/bin/env python3
"""
CocoPan Watchtower - Professional Operations Dashboard
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

# PROFESSIONAL CORPORATE CSS
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800&display=swap');
    
    /* Hide Streamlit elements */
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    .stDeployButton {display: none;}
    header {visibility: hidden;}
    
    /* Professional layout */
    .main {
        font-family: 'Inter', sans-serif;
        background: #F8FAFC;
        color: #1E293B;
        min-height: 100vh;
    }
    
    .main > div {
        padding: 2rem;
        max-width: 1400px;
        margin: 0 auto;
    }
    
    /* Professional header section */
    .header-section {
        background: linear-gradient(135deg, #1E40AF 0%, #3B82F6 100%);
        border-radius: 16px;
        padding: 2rem;
        margin-bottom: 2rem;
        box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.1);
    }
    
    /* Professional title */
    h1 {
        color: #FFFFFF !important;
        font-size: 3rem !important;
        font-weight: 700 !important;
        text-align: center !important;
        margin: 0 !important;
        letter-spacing: -0.025em;
    }
    
    h3 {
        color: rgba(255, 255, 255, 0.9) !important;
        font-weight: 400 !important;
        font-size: 1.1rem !important;
        text-align: center !important;
        margin: 0.5rem 0 0 0 !important;
    }
    
    /* Section headers */
    .section-header {
        background: #FFFFFF;
        border: 1px solid #E2E8F0;
        border-radius: 8px;
        padding: 1rem 1.5rem;
        margin: 1.5rem 0 1rem 0;
        box-shadow: 0 1px 3px 0 rgba(0, 0, 0, 0.1);
    }
    
    .section-title {
        font-size: 1.25rem;
        font-weight: 600;
        color: #1E293B;
        margin: 0;
    }
    
    .section-subtitle {
        font-size: 0.875rem;
        color: #64748B;
        margin: 0.25rem 0 0 0;
    }
    
    /* Professional metric cards */
    [data-testid="metric-container"] {
        background: #FFFFFF;
        border: 1px solid #E2E8F0;
        border-radius: 12px;
        padding: 2rem 1.5rem;
        box-shadow: 0 1px 3px 0 rgba(0, 0, 0, 0.1);
        text-align: center;
        transition: all 0.2s ease;
    }
    
    [data-testid="metric-container"]:hover {
        box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.1);
        transform: translateY(-1px);
    }
    
    [data-testid="metric-container"] [data-testid="metric-value"] {
        color: #1E293B;
        font-weight: 700;
        font-size: 2.5rem;
    }
    
    [data-testid="metric-container"] [data-testid="metric-label"] {
        color: #64748B;
        font-weight: 600;
        font-size: 0.875rem;
        text-transform: uppercase;
        letter-spacing: 0.05em;
    }
    
    [data-testid="metric-container"] [data-testid="metric-delta"] {
        color: #059669;
        font-weight: 500;
        font-size: 0.875rem;
    }
    
    /* Professional alerts */
    .stAlert {
        background: #FFFFFF;
        border: 1px solid #D1FAE5;
        border-left: 4px solid #10B981;
        border-radius: 8px;
        color: #065F46;
        font-weight: 500;
        padding: 1rem 1.5rem;
        margin: 2rem 0;
    }
    
    .stAlert[data-baseweb="notification"][kind="warning"] {
        background: #FFFBEB;
        border-color: #FCD34D;
        border-left-color: #F59E0B;
        color: #92400E;
    }
    
    .stAlert[data-baseweb="notification"][kind="error"] {
        background: #FEF2F2;
        border-color: #FECACA;
        border-left-color: #EF4444;
        color: #991B1B;
    }
    
    /* Professional tabs */
    .stTabs [data-baseweb="tab-list"] {
        gap: 0;
        background: #F1F5F9;
        border-radius: 8px;
        padding: 0.25rem;
        border: 1px solid #E2E8F0;
    }
    
    .stTabs [data-baseweb="tab"] {
        background: transparent;
        border: none;
        border-radius: 6px;
        color: #64748B;
        font-weight: 500;
        padding: 0.75rem 1.5rem;
        transition: all 0.2s ease;
        font-size: 0.875rem;
    }
    
    .stTabs [data-baseweb="tab"]:hover {
        background: #E2E8F0;
        color: #1E293B;
    }
    
    .stTabs [aria-selected="true"] {
        background: #FFFFFF !important;
        color: #1E293B !important;
        box-shadow: 0 1px 2px 0 rgba(0, 0, 0, 0.05);
    }
    
    /* Professional dataframe */
    .stDataFrame {
        background: #FFFFFF;
        border-radius: 12px;
        border: 1px solid #E2E8F0;
        overflow: hidden;
        box-shadow: 0 1px 3px 0 rgba(0, 0, 0, 0.1);
    }
    
    .stDataFrame table {
        background: #FFFFFF;
        color: #1E293B;
    }
    
    .stDataFrame thead tr th {
        background: #F8FAFC !important;
        color: #475569 !important;
        font-weight: 600 !important;
        text-transform: uppercase;
        font-size: 0.75rem;
        letter-spacing: 0.05em;
        border: none !important;
        border-bottom: 1px solid #E2E8F0 !important;
        padding: 1rem 0.75rem !important;
    }
    
    .stDataFrame tbody tr td {
        background: #FFFFFF !important;
        color: #1E293B !important;
        border: none !important;
        border-bottom: 1px solid #F1F5F9 !important;
        padding: 0.75rem !important;
    }
    
    .stDataFrame tbody tr:hover td {
        background: #F8FAFC !important;
    }
    
    /* Chart container */
    .chart-container {
        background: #FFFFFF;
        border: 1px solid #E2E8F0;
        border-radius: 12px;
        padding: 1rem;
        box-shadow: 0 1px 3px 0 rgba(0, 0, 0, 0.1);
    }
    
    /* Filter containers */
    .filter-container {
        background: #F8FAFC;
        border: 1px solid #E2E8F0;
        border-radius: 8px;
        padding: 1rem;
        margin-bottom: 1rem;
    }
    
    /* Responsive */
    @media (max-width: 768px) {
        h1 { font-size: 2rem !important; }
        .main > div { padding: 1rem; }
    }
</style>
""", unsafe_allow_html=True)

@st.cache_data(ttl=config.DASHBOARD_AUTO_REFRESH)
def load_data():
    """Load comprehensive operational data"""
    try:
        with db.get_connection() as conn:
            # Latest status
            latest_status_query = """
                SELECT 
                    s.name,
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
            
            # Daily uptime data
            daily_uptime_query = """
                SELECT 
                    s.name,
                    s.platform,
                    COUNT(sc.id) as total_checks,
                    SUM(CASE WHEN sc.is_online THEN 1 ELSE 0 END) as online_checks,
                    ROUND(
                        (SUM(CASE WHEN sc.is_online THEN 1 ELSE 0 END) * 100.0 / COUNT(sc.id)), 
                        1
                    ) as uptime_percentage
                FROM stores s
                LEFT JOIN status_checks sc ON s.id = sc.store_id 
                    AND DATE(sc.checked_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Manila') = CURRENT_DATE
                GROUP BY s.id, s.name, s.platform
                HAVING COUNT(sc.id) > 0
                ORDER BY uptime_percentage DESC
            """
            daily_uptime = pd.read_sql_query(daily_uptime_query, conn)
            
            # Downtime incidents
            downtime_query = """
                SELECT 
                    s.name,
                    s.platform,
                    sc.checked_at,
                    sc.error_message
                FROM stores s
                INNER JOIN status_checks sc ON s.id = sc.store_id
                WHERE sc.is_online = false 
                AND DATE(sc.checked_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Manila') = CURRENT_DATE
                ORDER BY sc.checked_at DESC
                LIMIT 100
            """
            downtime_incidents = pd.read_sql_query(downtime_query, conn)
            
            return latest_status, daily_uptime, downtime_incidents, None
            
    except Exception as e:
        logger.error(f"Error loading data: {e}")
        return None, None, None, str(e)

def create_professional_donut(online_stores, offline_stores, total_stores):
    """Create clean professional donut chart"""
    online_pct = (online_stores / total_stores * 100) if total_stores > 0 else 0
    
    # Professional colors and status
    if online_pct >= 95:
        online_color = '#059669'
        status_text = "Healthy"
    elif online_pct >= 80:
        online_color = '#D97706'
        status_text = "Warning"
    else:
        online_color = '#DC2626'
        status_text = "Alert"
    
    fig = go.Figure(data=[go.Pie(
        labels=['Online', 'Offline'],
        values=[online_stores, offline_stores],
        hole=0.65,
        marker=dict(
            colors=[online_color, '#F1F5F9'],
            line=dict(width=0)
        ),
        textinfo='none',
        hovertemplate='<b>%{label}</b><br>%{value} stores<extra></extra>',
        showlegend=False
    )])
    
    fig.add_annotation(
        text=f"<b style='font-size:32px;color:{online_color}'>{online_pct:.0f}%</b><br><span style='font-size:12px;color:#64748B'>{status_text}</span>",
        x=0.5, y=0.5,
        font=dict(family="Inter"),
        showarrow=False
    )
    
    fig.update_layout(
        height=240,
        margin=dict(t=0, b=0, l=0, r=0),
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)'
    )
    
    return fig

def get_last_check_time(latest_status):
    """Get the most recent check time from the data"""
    if latest_status is None or len(latest_status) == 0:
        return config.get_current_time()
    
    try:
        # Get the most recent check time
        latest_time = pd.to_datetime(latest_status['checked_at']).max()
        if latest_time.tz is None:
            latest_time = latest_time.tz_localize('UTC')
        
        ph_tz = config.get_timezone()
        latest_time = latest_time.tz_convert(ph_tz)
        return latest_time
    except Exception:
        return config.get_current_time()

def main():
    # Load data first to get last check time
    latest_status, daily_uptime, downtime_incidents, error = load_data()
    
    # Get last check time instead of current time
    last_check_time = get_last_check_time(latest_status)
    timezone_abbr = last_check_time.strftime('%Z') or 'PHT'
    
    # PROFESSIONAL HEADER WITH LAST CHECK TIME
    st.markdown(f"""
    <div class="header-section">
        <h1>CocoPan Watchtower</h1>
        <h3>Operations Monitoring System • Data as of {last_check_time.strftime('%B %d, %Y • %I:%M %p')} {timezone_abbr}</h3>
    </div>
    """, unsafe_allow_html=True)
    
    if error:
        st.error(f"System Error: {error}")
        return
    
    if latest_status is None or len(latest_status) == 0:
        st.warning("No operational data available. Monitoring service will collect data during business hours.")
        return
    
    # Calculate metrics
    total_stores = len(latest_status)
    online_stores = len(latest_status[latest_status['is_online'] == 1])
    offline_stores = total_stores - online_stores
    online_pct = (online_stores / total_stores * 100) if total_stores > 0 else 0
    
    # Platform counts
    grabfood_count = len(latest_status[latest_status['platform'] == 'grabfood'])
    foodpanda_count = len(latest_status[latest_status['platform'] == 'foodpanda'])
    
    # BETTER LAYOUT - Two rows of metrics
    st.markdown("### Network Overview")
    
    # First row - Main metrics
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Total Locations", f"{total_stores}", "operational")
    
    with col2:
        st.metric("Online Stores", f"{online_stores}", f"{online_pct:.0f}% active")
    
    with col3:
        st.metric("Offline Stores", f"{offline_stores}", "nominal" if offline_stores == 0 else "requires attention")
    
    with col4:
        # Compact donut chart
        st.markdown('<div class="chart-container">', unsafe_allow_html=True)
        if total_stores > 0:
            fig = create_professional_donut(online_stores, offline_stores, total_stores)
            st.plotly_chart(fig, use_container_width=True, config={'displayModeBar': False})
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Second row - Platform distribution
    st.markdown("### Platform Distribution")
    col1, col2 = st.columns(2)
    
    with col1:
        st.metric("GrabFood Network", f"{grabfood_count}", f"{(grabfood_count/total_stores*100):.0f}% of total")
    
    with col2:
        st.metric("Foodpanda Network", f"{foodpanda_count}", f"{(foodpanda_count/total_stores*100):.0f}% of total")
    
    # Professional status message
    if online_pct == 100:
        st.success("System Status: All locations operational and performing within normal parameters.")
    elif online_pct >= 95:
        st.success(f"System Status: Excellent performance - {online_pct:.0f}% operational capacity maintained.")
    elif online_pct >= 80:
        st.warning(f"System Status: Stable operation - {online_pct:.0f}% capacity, {offline_stores} locations require monitoring.")
    else:
        st.error(f"System Status: Critical performance - {online_pct:.0f}% capacity, {offline_stores} locations offline requiring immediate attention.")
    
    # PROFESSIONAL TABS WITH BETTER NAMES
    tab1, tab2, tab3 = st.tabs(["Store Uptime Analytics", "Live Operations Monitor", "Downtime Incidents"])
    
    with tab1:
        # SECTION HEADER
        st.markdown(f"""
        <div class="section-header">
            <div class="section-title">Store Uptime Report by Branch</div>
            <div class="section-subtitle">Daily performance metrics and availability statistics • Data as of {last_check_time.strftime('%I:%M %p')} {timezone_abbr}</div>
        </div>
        """, unsafe_allow_html=True)
        
        if daily_uptime is not None and len(daily_uptime) > 0:
            # FILTERS
            st.markdown('<div class="filter-container">', unsafe_allow_html=True)
            col1, col2 = st.columns(2)
            
            with col1:
                platform_filter = st.selectbox(
                    "Filter by Platform:",
                    ["All Platforms", "GrabFood", "Foodpanda"],
                    key="uptime_platform_filter"
                )
            
            with col2:
                sort_order = st.selectbox(
                    "Sort by Uptime:",
                    ["Highest to Lowest", "Lowest to Highest"],
                    key="uptime_sort_order"
                )
            st.markdown('</div>', unsafe_allow_html=True)
            
            # Format performance data professionally
            performance_data = daily_uptime.copy()
            performance_data['Branch'] = performance_data['name'].str.replace('Cocopan - ', '').str.replace('Cocopan ', '')
            performance_data['Platform'] = performance_data['platform'].str.title()
            performance_data['Uptime %'] = performance_data['uptime_percentage'].apply(lambda x: f"{x:.1f}%")
            performance_data['Total Checks'] = performance_data['total_checks'].astype(str)  # CHANGED FROM Health Checks
            
            # Better status classification
            def get_health_status(pct):
                if pct >= 95:
                    return "Healthy"
                elif pct >= 85:
                    return "Warning"
                elif pct >= 70:
                    return "Degraded"
                else:
                    return "Alert"
            
            performance_data['Health Status'] = performance_data['uptime_percentage'].apply(get_health_status)
            
            # Apply filters
            if platform_filter != "All Platforms":
                performance_data = performance_data[performance_data['Platform'] == platform_filter]
            
            # Apply sorting
            if sort_order == "Highest to Lowest":
                performance_data = performance_data.sort_values('uptime_percentage', ascending=False)
            else:
                performance_data = performance_data.sort_values('uptime_percentage', ascending=True)
            
            st.dataframe(
                performance_data[['Branch', 'Platform', 'Uptime %', 'Health Status', 'Total Checks']],
                use_container_width=True,
                hide_index=True,
                height=400
            )
        else:
            st.info("Performance analytics will be available as monitoring data accumulates during operational hours.")
    
    with tab2:
        # SECTION HEADER
        st.markdown(f"""
        <div class="section-header">
            <div class="section-title">Live Operations Monitor</div>
            <div class="section-subtitle">Real-time store status and response metrics • Data as of {last_check_time.strftime('%I:%M %p')} {timezone_abbr}</div>
        </div>
        """, unsafe_allow_html=True)
        
        # FILTERS
        st.markdown('<div class="filter-container">', unsafe_allow_html=True)
        col1, col2 = st.columns(2)
        
        with col1:
            platform_filter_live = st.selectbox(
                "Filter by Platform:",
                ["All Platforms", "GrabFood", "Foodpanda"],
                key="live_platform_filter"
            )
        
        with col2:
            status_filter = st.selectbox(
                "Filter by Status:",
                ["All Statuses", "Online Only", "Offline Only"],
                key="live_status_filter"
            )
        st.markdown('</div>', unsafe_allow_html=True)
        
        # Format current status professionally
        current_data = latest_status.copy()
        current_data['Branch'] = current_data['name'].str.replace('Cocopan - ', '').str.replace('Cocopan ', '')
        current_data['Platform'] = current_data['platform'].str.title()
        current_data['Status'] = current_data['is_online'].apply(lambda x: 'Online' if x else 'Offline')
        current_data['Response Time'] = current_data['response_time_ms'].fillna(0).astype(int).astype(str) + 'ms'
        
        # Format timestamp
        try:
            current_data['checked_at'] = pd.to_datetime(current_data['checked_at'])
            if current_data['checked_at'].dt.tz is None:
                current_data['checked_at'] = current_data['checked_at'].dt.tz_localize('UTC')
            
            ph_tz = config.get_timezone()
            current_data['checked_at'] = current_data['checked_at'].dt.tz_convert(ph_tz)
            current_data['Last Verified'] = current_data['checked_at'].dt.strftime('%I:%M %p')
        except Exception:
            current_data['Last Verified'] = pd.to_datetime(current_data['checked_at']).dt.strftime('%I:%M %p')
        
        # Apply filters
        if platform_filter_live != "All Platforms":
            current_data = current_data[current_data['Platform'] == platform_filter_live]
        
        if status_filter == "Online Only":
            current_data = current_data[current_data['is_online'] == 1]
        elif status_filter == "Offline Only":
            current_data = current_data[current_data['is_online'] == 0]
        
        st.dataframe(
            current_data[['Branch', 'Platform', 'Status', 'Response Time', 'Last Verified']],
            use_container_width=True,
            hide_index=True,
            height=400
        )
    
    with tab3:
        # SECTION HEADER
        st.markdown(f"""
        <div class="section-header">
            <div class="section-title">Downtime Incident Log</div>
            <div class="section-subtitle">Historical record of service interruptions and offline events • Data as of {last_check_time.strftime('%I:%M %p')} {timezone_abbr}</div>
        </div>
        """, unsafe_allow_html=True)
        
        if downtime_incidents is not None and len(downtime_incidents) > 0:
            # FILTERS
            st.markdown('<div class="filter-container">', unsafe_allow_html=True)
            platform_filter_down = st.selectbox(
                "Filter by Platform:",
                ["All Platforms", "GrabFood", "Foodpanda"],
                key="down_platform_filter"
            )
            st.markdown('</div>', unsafe_allow_html=True)
            
            # Format downtime data
            downtime_data = downtime_incidents.copy()
            downtime_data['Branch'] = downtime_data['name'].str.replace('Cocopan - ', '').str.replace('Cocopan ', '')
            downtime_data['Platform'] = downtime_data['platform'].str.title()
            
            # Format timestamp
            try:
                downtime_data['checked_at'] = pd.to_datetime(downtime_data['checked_at'])
                if downtime_data['checked_at'].dt.tz is None:
                    downtime_data['checked_at'] = downtime_data['checked_at'].dt.tz_localize('UTC')
                
                ph_tz = config.get_timezone()
                downtime_data['checked_at'] = downtime_data['checked_at'].dt.tz_convert(ph_tz)
                downtime_data['Incident Time'] = downtime_data['checked_at'].dt.strftime('%I:%M %p')
            except Exception:
                downtime_data['Incident Time'] = pd.to_datetime(downtime_data['checked_at']).dt.strftime('%I:%M %p')
            
            downtime_data['Issue Description'] = downtime_data['error_message'].fillna('Connection timeout or service unavailable')
            
            # Apply platform filter
            if platform_filter_down != "All Platforms":
                downtime_data = downtime_data[downtime_data['Platform'] == platform_filter_down]
            
            if len(downtime_data) > 0:
                st.dataframe(
                    downtime_data[['Branch', 'Platform', 'Incident Time', 'Issue Description']],
                    use_container_width=True,
                    hide_index=True,
                    height=400
                )
            else:
                st.success("No downtime incidents recorded for the selected platform filter.")
        else:
            st.success("No downtime incidents recorded today. All systems operating normally.")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        st.error(f"System Error: {e}")
        logger.error(f"Dashboard error: {e}")