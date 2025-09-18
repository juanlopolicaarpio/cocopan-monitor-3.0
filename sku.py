#!/usr/bin/env python3
"""
CocoPan SKU Compliance Reporting Dashboard
📊 Management/Client view for SKU compliance analytics and reporting
🎯 Displays data collected by VAs through the admin dashboard
"""
# ===== Standard libs =====
import os
import re
import json
import logging
import threading
import http.server
import socketserver
from datetime import datetime, timedelta
from typing import List, Dict

# ===== Third-party =====
import pytz
import pandas as pd
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

# ===== Initialize logging =====
logging.basicConfig(level=getattr(logging, os.getenv("LOG_LEVEL", "INFO")))
logger = logging.getLogger(__name__)

# ===== App modules =====
from config import config
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
                    self.send_response(200); self.send_header("Content-type", "text/plain"); self.end_headers()
                    self.wfile.write(b"OK - SKU Reporting Dashboard Healthy")
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
            logger.info(f"SKU Reporting health server listening on :{_HEALTH_PORT}")
            httpd.serve_forever()
    except OSError as e:
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
    initial_sidebar_state="expanded",
)

# ------------------------------------------------------------------------------
# Authentication (simplified for reporting dashboard)
# ------------------------------------------------------------------------------
def load_authorized_report_emails():
    try:
        with open("admin_alerts.json", "r") as f:
            data = json.load(f)
        admin_team = data.get("admin_team", {})
        if admin_team.get("enabled", False):
            emails = admin_team.get("emails", [])
            authorized_emails = [email.strip() for email in emails if email.strip()]
            logger.info(f"✅ Loaded {len(authorized_emails)} authorized report emails")
            return authorized_emails
        else:
            logger.warning("⚠️ Admin team disabled in config")
            return ["juanlopolicarpio@gmail.com"]
    except Exception as e:
        logger.error(f"❌ Failed to load report emails: {e}")
        return ["juanlopolicarpio@gmail.com"]

def check_report_authentication():
    authorized_emails = load_authorized_report_emails()
    if "report_authenticated" not in st.session_state:
        st.session_state.report_authenticated = False
        st.session_state.report_email = None

    if not st.session_state.report_authenticated:
        st.markdown("""
        <div style="max-width: 420px; margin: 2rem auto; padding: 2rem; background: white; border-radius: 12px; box-shadow: 0 4px 6px rgba(0,0,0,0.1);">
            <h2 style="text-align: center; color: #1E293B; margin-bottom: 1rem;">📊 CocoPan SKU Reports Access</h2>
            <p style="text-align: center; color: #64748B; margin-bottom: 1.5rem;">Enter your authorized email address</p>
        </div>
        """, unsafe_allow_html=True)

        with st.form("report_email_auth_form"):
            email = st.text_input("Email Address", placeholder="manager@cocopan.com",
                                  help="Enter your authorized email address")
            submit = st.form_submit_button("Access SKU Reports", use_container_width=True)
            if submit:
                email = email.strip().lower()
                if email in [a.lower() for a in authorized_emails]:
                    st.session_state.report_authenticated = True
                    st.session_state.report_email = email
                    logger.info(f"✅ Report user authenticated: {email}")
                    st.success("✅ Access granted! Redirecting…")
                    st.rerun()
                else:
                    st.error("❌ Email not authorized for report access.")
                    logger.warning(f"Unauthorized report attempt: {email}")
        return False
    return True

# ------------------------------------------------------------------------------
# Data loading functions with caching
# ------------------------------------------------------------------------------
@st.cache_data(ttl=300)  # 5 minute cache
def get_sku_compliance_dashboard_data():
    """Get today's SKU compliance dashboard data"""
    try:
        return db.get_sku_compliance_dashboard()
    except Exception as e:
        logger.error(f"Error loading SKU compliance dashboard: {e}")
        return []

@st.cache_data(ttl=300)
def get_out_of_stock_details_data():
    """Get detailed out-of-stock data"""
    try:
        return db.get_out_of_stock_details()
    except Exception as e:
        logger.error(f"Error loading out-of-stock details: {e}")
        return []

@st.cache_data(ttl=600)  # 10 minute cache for historical data
def get_compliance_trends_data(days_back=7):
    """Get compliance trends over time"""
    try:
        return db.get_compliance_trends(days_back)
    except Exception as e:
        logger.error(f"Error loading compliance trends: {e}")
        return []

@st.cache_data(ttl=300)
def get_product_availability_summary():
    """Get summary of which products are most frequently out of stock"""
    try:
        return db.get_product_availability_summary()
    except Exception as e:
        logger.error(f"Error loading product availability summary: {e}")
        return []

def clean_product_name(name: str) -> str:
    """Clean product name for display"""
    if not name:
        return ""
    # Remove platform prefixes
    cleaned = name.replace("GRAB ", "").replace("FOODPANDA ", "")
    return cleaned

# ------------------------------------------------------------------------------
# Dashboard visualizations
# ------------------------------------------------------------------------------
def create_compliance_overview_chart(dashboard_data):
    """Create compliance overview donut chart"""
    if not dashboard_data:
        return None
    
    compliance_ranges = {
        "Excellent (95%+)": 0,
        "Good (80-94%)": 0,
        "Needs Attention (<80%)": 0,
        "Not Checked": 0
    }
    
    for store in dashboard_data:
        compliance = store.get('compliance_percentage')
        if compliance is None:
            compliance_ranges["Not Checked"] += 1
        elif compliance >= 95:
            compliance_ranges["Excellent (95%+)"] += 1
        elif compliance >= 80:
            compliance_ranges["Good (80-94%)"] += 1
        else:
            compliance_ranges["Needs Attention (<80%)"] += 1
    
    colors = ["#10B981", "#F59E0B", "#EF4444", "#6B7280"]
    
    fig = go.Figure(data=[go.Pie(
        labels=list(compliance_ranges.keys()),
        values=list(compliance_ranges.values()),
        hole=0.4,
        marker_colors=colors
    )])
    
    fig.update_layout(
        title="Store Compliance Distribution",
        height=300,
        showlegend=True,
        margin=dict(t=50, b=0, l=0, r=0)
    )
    
    return fig

def create_platform_comparison_chart(dashboard_data):
    """Create platform comparison chart"""
    if not dashboard_data:
        return None
    
    platform_data = {}
    
    for store in dashboard_data:
        platform = store.get('platform', 'Unknown')
        compliance = store.get('compliance_percentage')
        
        if platform not in platform_data:
            platform_data[platform] = []
        
        if compliance is not None:
            platform_data[platform].append(compliance)
    
    platforms = []
    avg_compliance = []
    
    for platform, compliances in platform_data.items():
        if compliances:  # Only include platforms with data
            platforms.append("🛒 GrabFood" if platform == "grabfood" else "🍔 Foodpanda")
            avg_compliance.append(sum(compliances) / len(compliances))
    
    if not platforms:
        return None
    
    fig = go.Figure(data=[go.Bar(
        x=platforms,
        y=avg_compliance,
        marker_color=['#00A86B' if 'Grab' in p else '#E91E63' for p in platforms],
        text=[f"{val:.1f}%" for val in avg_compliance],
        textposition='auto'
    )])
    
    fig.update_layout(
        title="Average Compliance by Platform",
        yaxis_title="Compliance %",
        height=300,
        margin=dict(t=50, b=50, l=50, r=50)
    )
    
    return fig

def create_trends_chart(trends_data):
    """Create compliance trends over time"""
    if not trends_data:
        return None
    
    df = pd.DataFrame(trends_data)
    df['date'] = pd.to_datetime(df['date'])
    
    fig = go.Figure()
    
    # Overall trend line
    fig.add_trace(go.Scatter(
        x=df['date'],
        y=df['avg_compliance'],
        mode='lines+markers',
        name='Average Compliance',
        line=dict(color='#3B82F6', width=3),
        marker=dict(size=8)
    ))
    
    # Add target line at 95%
    fig.add_hline(y=95, line_dash="dash", line_color="green", 
                  annotation_text="Target (95%)")
    
    # Add warning line at 80%
    fig.add_hline(y=80, line_dash="dash", line_color="orange", 
                  annotation_text="Warning (80%)")
    
    fig.update_layout(
        title="Compliance Trends Over Time",
        xaxis_title="Date",
        yaxis_title="Compliance %",
        height=400,
        margin=dict(t=50, b=50, l=50, r=50)
    )
    
    return fig

# ------------------------------------------------------------------------------
# Report sections
# ------------------------------------------------------------------------------
def compliance_dashboard_section():
    """Main compliance dashboard section"""
    st.markdown("### 📊 SKU Compliance Dashboard")
    st.markdown("Overview of product compliance across all stores.")
    
    # Load dashboard data
    dashboard_data = get_sku_compliance_dashboard_data()
    
    if not dashboard_data:
        st.info("📭 No compliance data available. VAs need to perform store checks first.")
        return
    
    # Calculate summary metrics
    total_stores = len([d for d in dashboard_data if d['platform'] in ['grabfood', 'foodpanda']])
    checked_stores = len([d for d in dashboard_data if d['compliance_percentage'] is not None])
    
    if checked_stores > 0:
        avg_compliance = sum([d['compliance_percentage'] for d in dashboard_data if d['compliance_percentage'] is not None]) / checked_stores
        stores_100_pct = len([d for d in dashboard_data if d['compliance_percentage'] == 100.0])
        stores_need_attention = len([d for d in dashboard_data if d['compliance_percentage'] is not None and d['compliance_percentage'] < 80.0])
    else:
        avg_compliance = 0
        stores_100_pct = 0
        stores_need_attention = 0
    
    # Summary metrics
    col1, col2, col3, col4, col5 = st.columns(5)
    with col1:
        st.metric("Total Stores", total_stores)
    with col2:
        st.metric("Checked Today", checked_stores)
    with col3:
        st.metric("Average Compliance", f"{avg_compliance:.1f}%")
    with col4:
        st.metric("100% Compliant", stores_100_pct, "🟢")
    with col5:
        st.metric("Need Attention", stores_need_attention, "🔴")
    
    # Charts
    col1, col2 = st.columns(2)
    
    with col1:
        overview_chart = create_compliance_overview_chart(dashboard_data)
        if overview_chart:
            st.plotly_chart(overview_chart, use_container_width=True)
    
    with col2:
        comparison_chart = create_platform_comparison_chart(dashboard_data)
        if comparison_chart:
            st.plotly_chart(comparison_chart, use_container_width=True)
    
    # Platform filter
    platform_filter = st.selectbox(
        "Filter by Platform:",
        ["All Platforms", "grabfood", "foodpanda"],
        format_func=lambda x: "All Platforms" if x == "All Platforms" else "🛒 GrabFood" if x == "grabfood" else "🍔 Foodpanda"
    )
    
    # Filter dashboard data
    if platform_filter != "All Platforms":
        filtered_data = [d for d in dashboard_data if d['platform'] == platform_filter]
    else:
        filtered_data = dashboard_data
    
    # Create dashboard table
    if filtered_data:
        dashboard_df = []
        for store_data in filtered_data:
            compliance_pct = store_data['compliance_percentage']
            if compliance_pct is not None:
                if compliance_pct >= 95:
                    status = "🟢 Excellent"
                elif compliance_pct >= 80:
                    status = "🟡 Good"
                else:
                    status = "🔴 Needs Attention"
                compliance_display = f"{compliance_pct:.1f}%"
            else:
                status = "⏳ Not Checked"
                compliance_display = "—"
            
            dashboard_df.append({
                "Store": store_data['store_name'].replace('Cocopan - ', '').replace('Cocopan ', ''),
                "Platform": "🛒 GrabFood" if store_data['platform'] == 'grabfood' else "🍔 Foodpanda",
                "Compliance": compliance_display,
                "Status": status,
                "Out of Stock": store_data['out_of_stock_count'] if store_data['out_of_stock_count'] else 0,
                "Checked By": store_data['checked_by'] if store_data['checked_by'] else "—",
                "Last Check": store_data['checked_at'][:16] if store_data['checked_at'] else "—"
            })
        
        # Sort by compliance (lowest first for attention)
        dashboard_df = sorted(dashboard_df, key=lambda x: (x['Compliance'] == "—", float(x['Compliance'].replace('%', '')) if x['Compliance'] != "—" else 0))
        
        st.dataframe(
            pd.DataFrame(dashboard_df),
            use_container_width=True,
            hide_index=True,
            height=400
        )
    else:
        st.info("No data available for the selected platform filter.")

def out_of_stock_details_section():
    """Detailed out-of-stock reporting section"""
    st.markdown("### 📋 Out of Stock Details")
    st.markdown("Detailed analysis of products that are currently out of stock.")
    
    # Load out of stock details
    oos_details = get_out_of_stock_details_data()
    
    if not oos_details:
        st.info("📭 No out-of-stock items recorded today.")
        return
    
    # Platform filter for details
    platform_filter_details = st.selectbox(
        "Filter by Platform:",
        ["All Platforms", "grabfood", "foodpanda"],
        format_func=lambda x: "All Platforms" if x == "All Platforms" else "🛒 GrabFood" if x == "grabfood" else "🍔 Foodpanda",
        key="oos_platform_filter"
    )
    
    # Filter data
    if platform_filter_details != "All Platforms":
        filtered_oos = [d for d in oos_details if d['platform'] == platform_filter_details]
    else:
        filtered_oos = oos_details
    
    if filtered_oos:
        # Summary metrics for OOS
        total_oos_items = len(filtered_oos)
        unique_products = len(set(item['sku_code'] for item in filtered_oos))
        affected_stores = len(set(item['store_name'] for item in filtered_oos))
        
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Total OOS Items", total_oos_items, "📉")
        with col2:
            st.metric("Unique Products", unique_products, "📦")
        with col3:
            st.metric("Affected Stores", affected_stores, "🏪")
        
        # Create detailed table
        oos_df = []
        for item in filtered_oos:
            oos_df.append({
                "Store": item['store_name'].replace('Cocopan - ', '').replace('Cocopan ', ''),
                "Platform": "🛒 GrabFood" if item['platform'] == 'grabfood' else "🍔 Foodpanda",
                "Product Code": item['sku_code'],
                "Product Name": clean_product_name(item['product_name']),
                "Category": item.get('category', 'Unknown'),
                "Division": item.get('division', 'Unknown'),
                "GMV Impact": f"₱{item.get('gmv_q3', 0):,.2f}",
                "Reported By": item['checked_by']
            })
        
        st.dataframe(
            pd.DataFrame(oos_df),
            use_container_width=True,
            hide_index=True,
            height=400
        )
        
        # Summary by category
        st.markdown("### 📊 Out of Stock Summary by Category")
        category_summary = {}
        gmv_impact = {}
        
        for item in filtered_oos:
            category = item.get('category', 'Unknown')
            gmv = item.get('gmv_q3', 0)
            
            if category not in category_summary:
                category_summary[category] = 0
                gmv_impact[category] = 0
            
            category_summary[category] += 1
            gmv_impact[category] += gmv
        
        if category_summary:
            col1, col2 = st.columns(2)
            
            with col1:
                st.markdown("**Items Out of Stock by Category:**")
                for category, count in category_summary.items():
                    st.metric(f"🍞 {category}", count, f"₱{gmv_impact[category]:,.2f} GMV impact")
            
            with col2:
                # Create category breakdown chart
                if len(category_summary) > 1:
                    fig = go.Figure(data=[go.Pie(
                        labels=list(category_summary.keys()),
                        values=list(category_summary.values()),
                        textinfo='label+percent'
                    )])
                    
                    fig.update_layout(
                        title="OOS Items by Category",
                        height=300
                    )
                    
                    st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No out-of-stock items for the selected platform.")

def trends_analysis_section():
    """Compliance trends analysis section"""
    st.markdown("### 📈 Compliance Trends")
    st.markdown("Historical analysis of compliance performance over time.")
    
    # Date range selector
    col1, col2 = st.columns(2)
    with col1:
        days_back = st.selectbox("Time Period:", 
                                [7, 14, 30], 
                                format_func=lambda x: f"Last {x} days",
                                index=0)
    with col2:
        st.write("")  # Spacer
    
    # Load trends data
    trends_data = get_compliance_trends_data(days_back)
    
    if not trends_data:
        st.info(f"📭 No compliance data available for the last {days_back} days.")
        return
    
    # Create trends chart
    trends_chart = create_trends_chart(trends_data)
    if trends_chart:
        st.plotly_chart(trends_chart, use_container_width=True)
    
    # Trends summary
    if len(trends_data) > 1:
        latest_compliance = trends_data[-1]['avg_compliance']
        previous_compliance = trends_data[-2]['avg_compliance']
        change = latest_compliance - previous_compliance
        
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Latest Compliance", f"{latest_compliance:.1f}%")
        with col2:
            st.metric("Change vs Previous", f"{change:+.1f}%", 
                     "📈" if change > 0 else "📉" if change < 0 else "➡️")
        with col3:
            best_day = max(trends_data, key=lambda x: x['avg_compliance'])
            st.metric("Best Day", f"{best_day['avg_compliance']:.1f}%", 
                     f"{best_day['date']}")

def product_insights_section():
    """Product-level insights section"""
    st.markdown("### 🔍 Product Insights")
    st.markdown("Analysis of which products are most frequently out of stock.")
    
    # Load product availability data
    product_data = get_product_availability_summary()
    
    if not product_data:
        st.info("📭 No product availability data available.")
        return
    
    # Create product insights table
    product_df = []
    for product in product_data:
        product_df.append({
            "Product Name": clean_product_name(product['product_name']),
            "SKU Code": product['sku_code'],
            "Platform": "🛒 GrabFood" if product['platform'] == 'grabfood' else "🍔 Foodpanda",
            "Times Out of Stock": product['oos_frequency'],
            "Affected Stores": product['stores_affected'],
            "Category": product.get('category', 'Unknown'),
            "GMV Impact": f"₱{product.get('gmv_q3', 0):,.2f}"
        })
    
    # Sort by frequency (most problematic first)
    product_df = sorted(product_df, key=lambda x: x['Times Out of Stock'], reverse=True)
    
    st.dataframe(
        pd.DataFrame(product_df),
        use_container_width=True,
        hide_index=True,
        height=400
    )
    
    # Top problematic products chart
    if len(product_df) > 0:
        top_products = product_df[:10]  # Top 10 most problematic
        
        fig = go.Figure(data=[go.Bar(
            x=[p['Times Out of Stock'] for p in top_products],
            y=[p['Product Name'] for p in top_products],
            orientation='h',
            marker_color='#EF4444',
            text=[p['Times Out of Stock'] for p in top_products],
            textposition='auto'
        )])
        
        fig.update_layout(
            title="Top 10 Most Frequently Out-of-Stock Products",
            xaxis_title="Times Reported Out of Stock",
            yaxis_title="Product",
            height=400,
            margin=dict(t=50, b=50, l=200, r=50)
        )
        
        st.plotly_chart(fig, use_container_width=True)

# ------------------------------------------------------------------------------
# Main dashboard
# ------------------------------------------------------------------------------
def main():
    if not check_report_authentication():
        return

    with st.sidebar:
        st.markdown(f"**Logged in as:**\n{st.session_state.report_email}")
        if st.button("Logout"):
            st.session_state.report_authenticated = False
            st.session_state.report_email = None
            st.rerun()
        
        st.markdown("---")
        st.markdown("### 📊 Report Sections")
        st.markdown("Navigate through different analytics views")
        
        # Auto-refresh toggle
        auto_refresh = st.checkbox("Auto-refresh (5 min)", value=False)
        if auto_refresh:
            st.markdown("*Data refreshes every 5 minutes*")

    # Header
    now = datetime.now(pytz.timezone("Asia/Manila"))
    st.markdown(f"""
    <div style="background: linear-gradient(135deg, #7C3AED 0%, #A855F7 100%); border-radius: 12px; padding: 1.5rem; margin-bottom: 1.5rem; text-align: center; color: white;">
        <div style="font-size: 1.8rem; font-weight: 700; margin: 0;">📊 CocoPan SKU Compliance Reports</div>
        <div style="font-size: 0.9rem; margin: 0.5rem 0 0 0; opacity: 0.9;">Analytics Dashboard • {now.strftime('%I:%M %p')} Manila Time</div>
    </div>
    """, unsafe_allow_html=True)

    # Main tabs
    tab1, tab2, tab3, tab4 = st.tabs([
        "📊 Compliance Dashboard", 
        "📋 Out of Stock Details", 
        "📈 Trends Analysis",
        "🔍 Product Insights"
    ])
    
    with tab1:
        compliance_dashboard_section()
    
    with tab2:
        out_of_stock_details_section()
    
    with tab3:
        trends_analysis_section()
        
    with tab4:
        product_insights_section()
    
    # Footer with refresh info
    st.markdown("---")
    col1, col2, col3 = st.columns([1, 2, 1])
    with col2:
        if st.button("🔄 Refresh All Data", use_container_width=True):
            # Clear all caches
            get_sku_compliance_dashboard_data.clear()
            get_out_of_stock_details_data.clear()
            get_compliance_trends_data.clear()
            get_product_availability_summary.clear()
            st.success("Data refreshed!")
            st.rerun()
    
    with col3:
        st.markdown(f"*Last updated: {now.strftime('%H:%M')}*")

# ------------------------------------------------------------------------------
# Styles
# ------------------------------------------------------------------------------
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');
    #MainMenu, footer, header, .stDeployButton {visibility: hidden;}
    .main { 
        font-family: 'Inter', sans-serif; 
        background: #F8FAFC; 
        color: #1E293B; 
        padding: 1rem !important; 
    }
    
    .metric-container {
        background: white;
        border-radius: 8px;
        padding: 1rem;
        box-shadow: 0 1px 3px rgba(0,0,0,0.1);
        text-align: center;
    }
    
    .stMetric > div {
        background: white;
        border-radius: 8px;
        padding: 1rem;
        box-shadow: 0 1px 3px rgba(0,0,0,0.1);
    }
    
    .stDataFrame > div {
        background: white;
        border-radius: 8px;
        box-shadow: 0 1px 3px rgba(0,0,0,0.1);
    }
    
    /* Plotly chart styling */
    .js-plotly-plot {
        background: white !important;
        border-radius: 8px;
        box-shadow: 0 1px 3px rgba(0,0,0,0.1);
    }
</style>
""", unsafe_allow_html=True)

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        st.error(f"❌ System Error: {e}")
        logger.exception("SKU reporting dashboard error")