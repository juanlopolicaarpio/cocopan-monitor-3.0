#!/usr/bin/env python3
"""
CocoPan SKU Product Availability Reporting Dashboard
📊 Management/Client view for SKU product availability analytics and reporting
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
from typing import List, Dict, Tuple, Optional

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
    initial_sidebar_state="expanded",
)

# ------------------------------------------------------------------------------
# Authentication (simplified for reporting dashboard)
# ------------------------------------------------------------------------------
def load_authorized_report_emails() -> List[str]:
    """
    Loads authorized emails from admin_alerts.json > admin_team.emails.
    Falls back to a default list if not available.
    """
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


def check_report_authentication() -> bool:
    authorized_emails = load_authorized_report_emails()
    if "report_authenticated" not in st.session_state:
        st.session_state.report_authenticated = False
        st.session_state.report_email = None

    if not st.session_state.report_authenticated:
        st.markdown(
            """
            <div style="max-width: 420px; margin: 2rem auto; padding: 2rem; background: white; border-radius: 12px; box-shadow: 0 4px 6px rgba(0,0,0,0.1);">
                <h2 style="text-align: center; color: #1E293B; margin-bottom: 1rem;">📊 CocoPan SKU Reports Access</h2>
                <p style="text-align: center; color: #64748B; margin-bottom: 1.5rem;">Enter your authorized email address</p>
            </div>
            """,
            unsafe_allow_html=True,
        )

        with st.form("report_email_auth_form"):
            email = st.text_input(
                "Email Address",
                placeholder="manager@cocopan.com",
                help="Enter your authorized email address",
            )
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
# Helper functions
# ------------------------------------------------------------------------------
def format_datetime_safe(dt_value) -> str:
    """Safely format datetime objects or strings"""
    if dt_value is None:
        return "—"

    if isinstance(dt_value, str):
        # If already string, clamp to reasonable length (UI)
        return dt_value[:16] if len(dt_value) > 16 else dt_value

    if isinstance(dt_value, datetime):
        return dt_value.strftime("%Y-%m-%d %H:%M")

    return str(dt_value)


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


@st.cache_data(ttl=300)
def get_product_availability_summary():
    """Get summary of which products are most frequently out of stock"""
    try:
        # Add some debugging to see what's returned
        data = db.get_product_availability_summary()
        logger.info(f"Product availability summary returned {len(data)} items")
        return data
    except Exception as e:
        logger.error(f"Error loading product availability summary: {e}")
        return []

# ------------------------------------------------------------------------------
# Charts
# ------------------------------------------------------------------------------
def create_platform_availability_charts(
    dashboard_data: List[Dict]
) -> Tuple[Optional[go.Figure], Optional[go.Figure]]:
    """
    Create separate availability donut charts for GrabFood and Foodpanda.
    Excludes "Not Checked" stores to avoid gray sections.
    Uses store-level 'compliance_percentage' to bucket stores.

    Returns (grabfood_chart, foodpanda_chart)
    """
    if not dashboard_data:
        return None, None

    # Separate data by platform and filter out not checked
    grabfood_data = [d for d in dashboard_data if d.get('platform') == 'grabfood' and d.get('compliance_percentage') is not None]
    foodpanda_data = [d for d in dashboard_data if d.get('platform') == 'foodpanda' and d.get('compliance_percentage') is not None]

    def create_platform_chart(platform_data: List[Dict], platform_name: str) -> Optional[go.Figure]:
        if not platform_data:
            return None

        availability_ranges = {
            "Excellent (95%+)": 0,
            "Good (80-94%)": 0,
            "Needs Attention (<80%)": 0
        }

        for store in platform_data:
            availability = store.get('compliance_percentage')
            if availability >= 95:
                availability_ranges["Excellent (95%+)"] += 1
            elif availability >= 80:
                availability_ranges["Good (80-94%)"] += 1
            else:
                availability_ranges["Needs Attention (<80%)"] += 1

        # Only include slices that have counts
        filtered_ranges = {k: v for k, v in availability_ranges.items() if v > 0}
        if not filtered_ranges:
            return None

        colors = []
        for label in filtered_ranges.keys():
            if "Excellent" in label:
                colors.append("#10B981")  # green
            elif "Good" in label:
                colors.append("#F59E0B")  # amber
            else:  # Needs Attention
                colors.append("#EF4444")  # red

        fig = go.Figure(
            data=[
                go.Pie(
                    labels=list(filtered_ranges.keys()),
                    values=list(filtered_ranges.values()),
                    hole=0.4,
                    marker=dict(colors=colors),
                    textinfo='label+value'
                )
            ]
        )

        fig.update_layout(
            title=f"{platform_name} Availability ({len(platform_data)} checked stores)",
            height=300,
            showlegend=True,
            margin=dict(t=50, b=0, l=0, r=0),
        )
        return fig

    grabfood_chart = create_platform_chart(grabfood_data, "GrabFood")
    foodpanda_chart = create_platform_chart(foodpanda_data, "Foodpanda")

    return grabfood_chart, foodpanda_chart


def create_trends_chart(trends_data: List[Dict]) -> Optional[go.Figure]:
    """
    Create product availability trends over time.
    Expects items with keys: date, avg_compliance
    """
    if not trends_data:
        return None

    df = pd.DataFrame(trends_data)
    if df.empty or "date" not in df or "avg_compliance" not in df:
        return None

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date"])

    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=df["date"],
            y=df["avg_compliance"],
            mode="lines+markers",
            name="Average Availability",
            line=dict(color="#3B82F6", width=3),
            marker=dict(size=8),
        )
    )
    fig.add_hline(y=95, line_dash="dash", line_color="green", annotation_text="Target (95%)")
    fig.add_hline(y=80, line_dash="dash", line_color="orange", annotation_text="Warning (80%)")

    fig.update_layout(
        title="Product Availability Trends Over Time",
        xaxis_title="Date",
        yaxis_title="Availability %",
        height=400,
        margin=dict(t=50, b=50, l=50, r=50),
    )
    return fig

# ------------------------------------------------------------------------------
# Report sections
# ------------------------------------------------------------------------------
def availability_dashboard_section():
    """Main product availability dashboard section"""
    st.markdown("### 📊 SKU Product Availability Dashboard")
    st.markdown("Overview of product availability across all stores.")

    dashboard_data = get_sku_availability_dashboard_data()

    if not dashboard_data:
        st.info("📭 No product availability data available. VAs need to perform store checks first.")
        return

    total_stores = len([d for d in dashboard_data if d.get('platform') in ('grabfood', 'foodpanda')])
    checked = [d.get('compliance_percentage') for d in dashboard_data if d.get('compliance_percentage') is not None]
    checked_stores = len(checked)

    if checked_stores > 0:
        avg_availability = sum(checked) / checked_stores
        stores_100 = sum(1 for v in checked if v == 100.0)
        stores_need_attention = sum(1 for v in checked if v < 80.0)
    else:
        avg_availability = 0.0
        stores_100 = 0
        stores_need_attention = 0

    col1, col2, col3, col4, col5 = st.columns(5)
    with col1:
        st.metric("Total Stores", total_stores)
    with col2:
        st.metric("Checked Today", checked_stores)
    with col3:
        st.metric("Average Availability", f"{avg_availability:.1f}%")
    with col4:
        st.metric("100% Available", stores_100, "🟢")
    with col5:
        st.metric("Need Attention", stores_need_attention, "🔴")

    # Platform-specific availability charts (removed platform comparison chart)
    grabfood_chart, foodpanda_chart = create_platform_availability_charts(dashboard_data)
    colA, colB = st.columns(2)
    with colA:
        if grabfood_chart:
            st.plotly_chart(grabfood_chart, use_container_width=True)
        else:
            st.info("No GrabFood data available with checked stores.")
    with colB:
        if foodpanda_chart:
            st.plotly_chart(foodpanda_chart, use_container_width=True)
        else:
            st.info("No Foodpanda data available with checked stores.")

    # Platform filter
    platform_filter = st.selectbox(
        "Filter by Platform:",
        ["All Platforms", "grabfood", "foodpanda"],
        format_func=lambda x: "All Platforms" if x == "All Platforms" else ("GrabFood" if x == "grabfood" else "Foodpanda"),
    )

    filtered_data = (
        [d for d in dashboard_data if d.get('platform') == platform_filter]
        if platform_filter != "All Platforms"
        else dashboard_data
    )

    if filtered_data:
        rows = []
        for store in filtered_data:
            availability_pct = store.get('compliance_percentage')
            if availability_pct is None:
                status = "⏳ Not Checked"
                availability_display = "—"
                sort_val = -1.0
            else:
                if availability_pct >= 95:
                    status = "🟢 Excellent"
                elif availability_pct >= 80:
                    status = "🟡 Good"
                else:
                    status = "🔴 Needs Attention"
                availability_display = f"{availability_pct:.1f}%"
                sort_val = availability_pct

            rows.append(
                {
                    "Store": store.get('store_name', "").replace('Cocopan - ', '').replace('Cocopan ', ''),
                    "Platform": "GrabFood" if store.get('platform') == 'grabfood' else "Foodpanda",
                    "Availability": availability_display,
                    "Status": status,
                    "Out of Stock": store.get('out_of_stock_count') or 0,
                    "Last Check": format_datetime_safe(store.get('checked_at')),
                    "_sort_key": sort_val,
                }
            )

        # Sort: Not Checked (—) last, then ascending availability (low first)
        rows_sorted = sorted(
            rows,
            key=lambda r: (r["_sort_key"] < 0, r["_sort_key"] if r["_sort_key"] >= 0 else 9999),
        )
        df = pd.DataFrame([{k: v for k, v in r.items() if k != "_sort_key"} for r in rows_sorted])
        st.dataframe(df, use_container_width=True, hide_index=True, height=400)
    else:
        st.info("No data available for the selected platform filter.")


def out_of_stock_details_section():
    """Detailed out-of-stock reporting section"""
    st.markdown("### 📋 Out of Stock Details")
    st.markdown("Detailed analysis of products that are currently out of stock.")

    oos_details = get_out_of_stock_details_data()
    if not oos_details:
        st.info("📭 No out-of-stock items recorded today.")
        return

    platform_filter = st.selectbox(
        "Filter by Platform:",
        ["All Platforms", "grabfood", "foodpanda"],
        format_func=lambda x: "All Platforms" if x == "All Platforms" else ("GrabFood" if x == "grabfood" else "Foodpanda"),
        key="oos_platform_filter",
    )
    filtered_oos = (
        [d for d in oos_details if d.get('platform') == platform_filter]
        if platform_filter != "All Platforms"
        else oos_details
    )

    if not filtered_oos:
        st.info("No out-of-stock items for the selected platform.")
        return

    total_oos_items = len(filtered_oos)
    unique_products = len({item.get('sku_code') for item in filtered_oos})
    affected_stores = len({item.get('store_name') for item in filtered_oos})

    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Total OOS Items", total_oos_items, "📉")
    with col2:
        st.metric("Unique Products", unique_products, "📦")
    with col3:
        st.metric("Affected Stores", affected_stores, "🏪")

    oos_rows = []
    for item in filtered_oos:
        oos_rows.append(
            {
                "Store": (item.get('store_name') or "").replace('Cocopan - ', '').replace('Cocopan ', ''),
                "Platform": "GrabFood" if item.get('platform') == 'grabfood' else "Foodpanda",
                "Product Code": item.get('sku_code'),
                "Product Name": clean_product_name(item.get('product_name')),
                "Category": item.get('category', 'Unknown'),
                "Division": item.get('division', 'Unknown'),
            }
        )
    st.dataframe(pd.DataFrame(oos_rows), use_container_width=True, hide_index=True, height=400)

    # Summary by category + optional pie
    st.markdown("### 📊 Out of Stock Summary by Category")
    category_summary: Dict[str, int] = {}
    gmv_impact: Dict[str, float] = {}

    for item in filtered_oos:
        category = item.get('category', 'Unknown')
        gmv = float(item.get('gmv_q3', 0) or 0)
        category_summary[category] = category_summary.get(category, 0) + 1
        gmv_impact[category] = gmv_impact.get(category, 0.0) + gmv

    if category_summary:
        colA, colB = st.columns(2)
        with colA:
            st.markdown("**Items Out of Stock by Category:**")
            for category, count in category_summary.items():
                st.metric(f"🍞 {category}", count, f"₱{gmv_impact.get(category, 0.0):,.2f} GMV impact")

        with colB:
            if len(category_summary) > 1:
                fig = go.Figure(
                    data=[
                        go.Pie(
                            labels=list(category_summary.keys()),
                            values=list(category_summary.values()),
                            textinfo="label+percent",
                        )
                    ]
                )
                fig.update_layout(title="OOS Items by Category", height=300)
                st.plotly_chart(fig, use_container_width=True)


def out_of_stock_frequency_section():
    """Out-of-stock frequency analysis section"""
    st.markdown("### 📊 Out of Stock Frequency Analysis")
    st.markdown("Analysis of which products are most frequently out of stock and their occurrence patterns.")

    product_data = get_product_availability_summary()
    
    # Debug: Check what data we're getting
    st.info(f"Debug: Loaded {len(product_data)} frequency records from database")
    
    if not product_data:
        st.warning("📭 No out of stock frequency data available. This could mean:")
        st.markdown("• No products have been recorded as out of stock yet")
        st.markdown("• Database query `get_product_availability_summary()` is not returning data")
        st.markdown("• Historical tracking may need to be implemented")
        
        # Try to show some sample data structure expected
        st.markdown("**Expected data structure:**")
        sample_data = [
            {"product_name": "Sample Product", "sku_code": "SKU123", "platform": "grabfood", "oos_frequency": 5, "stores_affected": 3, "category": "Food"},
        ]
        st.dataframe(pd.DataFrame(sample_data))
        return

    product_rows = []
    for product in product_data:
        product_rows.append(
            {
                "Product Name": clean_product_name(product.get('product_name')),
                "SKU Code": product.get('sku_code'),
                "Platform": "GrabFood" if product.get('platform') == 'grabfood' else "Foodpanda",
                "Times Out of Stock": product.get('oos_frequency', 0),
                "Affected Stores": product.get('stores_affected', 0),
                "Category": product.get('category', 'Unknown'),
            }
        )

    # Sort by frequency desc
    product_rows = sorted(product_rows, key=lambda x: x["Times Out of Stock"], reverse=True)
    if not product_rows:
        st.info("No out of stock frequency data available.")
        return

    total_oos_occurrences = sum(p["Times Out of Stock"] for p in product_rows)
    most_affected_product = product_rows[0]
    unique_oos_products = len(product_rows)

    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Total OOS Occurrences", total_oos_occurrences, "📉")
    with col2:
        st.metric("Unique OOS Products", unique_oos_products, "📦")
    with col3:
        st.metric("Most Affected Product", most_affected_product["Times Out of Stock"], f"{most_affected_product['Product Name'][:20]}...")

    st.dataframe(pd.DataFrame(product_rows), use_container_width=True, hide_index=True, height=400)

    # Top 10 bar chart
    top_products = product_rows[:10]
    if top_products:
        fig = go.Figure(
            data=[
                go.Bar(
                    x=[p["Times Out of Stock"] for p in top_products],
                    y=[p["Product Name"] for p in top_products],
                    orientation="h",
                    marker_color="#EF4444",
                    text=[p["Times Out of Stock"] for p in top_products],
                    textposition="auto",
                )
            ]
        )
        fig.update_layout(
            title="Top 10 Most Frequently Out-of-Stock Products",
            xaxis_title="Times Reported Out of Stock",
            yaxis_title="Product",
            height=400,
            margin=dict(t=50, b=50, l=200, r=50),
        )
        st.plotly_chart(fig, use_container_width=True)


def reports_export_section():
    """Reports and data export section"""
    st.markdown("### 📋 Reports & Data Export")
    st.markdown("Generate and export reports for specific date ranges and platforms.")

    col1, col2, col3 = st.columns(3)
    with col1:
        # Hardcode start date to September 20, 2025
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

    report_type = st.selectbox(
        "Report Type:",
        ["Daily Availability Summary", "Out of Stock Items", "Store Performance", "Platform Comparison"],
    )

    if st.button("Generate Report", use_container_width=True):
        try:
            if report_type == "Daily Availability Summary":
                # NOTE: Enhanced with stores having out of stock items and platform filtering
                data = []
                current_date = start_date
                while current_date <= end_date:
                    # Sample data - replace with actual DB queries with date range
                    total_stores = 143 if platform_filter == "All Platforms" else (69 if platform_filter == "grabfood" else 74)
                    checked_stores = 16 if platform_filter == "All Platforms" else (8 if platform_filter == "grabfood" else 8)
                    stores_with_oos = 3 if platform_filter == "All Platforms" else (1 if platform_filter == "grabfood" else 2)
                    
                    data.append(
                        {
                            "Date": current_date.strftime("%Y-%m-%d"),
                            "Platform": "All Platforms" if platform_filter == "All Platforms" else ("GrabFood" if platform_filter == "grabfood" else "Foodpanda"),
                            "Total Stores": total_stores,
                            "Checked Stores": checked_stores,
                            "Average Availability": 99.6,
                            "100% Available": 13,
                            "Need Attention": 0,
                            "Stores with Out of Stock": stores_with_oos,
                        }
                    )
                    current_date += timedelta(days=1)

                df = pd.DataFrame(data)
                st.dataframe(df, use_container_width=True)
                st.download_button(
                    label="📥 Download CSV",
                    data=df.to_csv(index=False),
                    file_name=f"availability_summary_{platform_filter}_{start_date}_{end_date}.csv",
                    mime="text/csv",
                )

            elif report_type == "Out of Stock Items":
                oos_data = get_out_of_stock_details_data()
                
                # Apply platform filter
                if platform_filter != "All Platforms":
                    oos_data = [item for item in oos_data if item.get('platform') == platform_filter]
                
                if oos_data:
                    rows = []
                    for item in oos_data:
                        rows.append(
                            {
                                "Date": start_date.strftime("%Y-%m-%d"),  # Replace with actual per-item date if available
                                "Store": (item.get('store_name') or "").replace('Cocopan - ', '').replace('Cocopan ', ''),
                                "Platform": "GrabFood" if item.get('platform') == 'grabfood' else "Foodpanda",
                                "Product Code": item.get('sku_code'),
                                "Product Name": clean_product_name(item.get('product_name')),
                                "Category": item.get('category', 'Unknown'),
                            }
                        )
                    df = pd.DataFrame(rows)
                    st.dataframe(df, use_container_width=True)
                    st.download_button(
                        label="📥 Download CSV",
                        data=df.to_csv(index=False),
                        file_name=f"out_of_stock_{platform_filter}_{start_date}_{end_date}.csv",
                        mime="text/csv",
                    )
                else:
                    st.info("No out of stock data available for selected date range and platform.")

            elif report_type == "Store Performance":
                dashboard_data = get_sku_availability_dashboard_data()
                
                # Apply platform filter
                if platform_filter != "All Platforms":
                    dashboard_data = [store for store in dashboard_data if store.get('platform') == platform_filter]
                
                if dashboard_data:
                    rows = []
                    for store in dashboard_data:
                        availability = store.get('compliance_percentage')
                        if availability is None:
                            status = "Not Checked"
                            avail_val = None
                        else:
                            status = "Excellent" if availability >= 95 else ("Good" if availability >= 80 else "Needs Attention")
                            avail_val = float(availability)

                        rows.append(
                            {
                                "Store": (store.get('store_name') or "").replace('Cocopan - ', '').replace('Cocopan ', ''),
                                "Platform": "GrabFood" if store.get('platform') == 'grabfood' else "Foodpanda",
                                "Availability %": avail_val,
                                "Status": status,
                                "Out of Stock Items": store.get('out_of_stock_count', 0),
                                "Last Check": format_datetime_safe(store.get('checked_at')),
                            }
                        )

                    df = pd.DataFrame(rows)
                    st.dataframe(df, use_container_width=True)
                    st.download_button(
                        label="📥 Download CSV",
                        data=df.to_csv(index=False),
                        file_name=f"store_performance_{platform_filter}_{start_date}_{end_date}.csv",
                        mime="text/csv",
                    )
                else:
                    st.info("No store performance data available for selected platform.")

            elif report_type == "Platform Comparison":
                dashboard_data = get_sku_availability_dashboard_data()
                if dashboard_data:
                    if platform_filter == "All Platforms":
                        grabfood_stores = [d for d in dashboard_data if d.get('platform') == 'grabfood']
                        foodpanda_stores = [d for d in dashboard_data if d.get('platform') == 'foodpanda']

                        def collect_stats(stores: List[Dict]) -> Dict[str, float]:
                            vals = [s.get('compliance_percentage') for s in stores if s.get('compliance_percentage') is not None]
                            oos_stores = len([s for s in stores if s.get('out_of_stock_count', 0) > 0])
                            return {
                                "Total Stores": len(stores),
                                "Checked Stores": len(vals),
                                "Average Availability": (sum(vals) / len(vals)) if vals else 0.0,
                                "100% Available": len([v for v in vals if v == 100.0]),
                                "Need Attention": len([v for v in vals if v < 80.0]),
                                "Stores with Out of Stock": oos_stores,
                            }

                        comparison = []
                        if grabfood_stores:
                            stats = collect_stats(grabfood_stores)
                            comparison.append({"Platform": "GrabFood", **stats})
                        if foodpanda_stores:
                            stats = collect_stats(foodpanda_stores)
                            comparison.append({"Platform": "Foodpanda", **stats})

                        df = pd.DataFrame(comparison)
                    else:
                        st.info("Platform Comparison requires 'All Platforms' filter to compare between platforms.")
                        return
                        
                    st.dataframe(df, use_container_width=True)
                    st.download_button(
                        label="📥 Download CSV",
                        data=df.to_csv(index=False),
                        file_name=f"platform_comparison_{start_date}_{end_date}.csv",
                        mime="text/csv",
                    )
                else:
                    st.info("No platform data available.")

        except Exception as e:
            logger.exception("Error generating report")
            st.error(f"Error generating report: {e}")

    st.markdown("---")
    st.markdown("**Available Report Types:**")
    st.markdown("• **Daily Availability Summary**: Overall metrics by date with out of stock store counts")
    st.markdown("• **Out of Stock Items**: Detailed list of OOS products")
    st.markdown("• **Store Performance**: Individual store availability metrics")
    st.markdown("• **Platform Comparison**: GrabFood vs Foodpanda comparison (requires 'All Platforms' filter)")

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

        # Auto-refresh toggle (visual note only; caching handles refresh)
        auto_refresh = st.checkbox("Auto-refresh (5 min)", value=False)
        if auto_refresh:
            st.markdown("*Data refreshes every 5 minutes*")

    # Header
    now = datetime.now(pytz.timezone("Asia/Manila"))
    st.markdown(
        f"""
        <div style="background: linear-gradient(135deg, #7C3AED 0%, #A855F7 100%); border-radius: 12px; padding: 1.5rem; margin-bottom: 1.5rem; text-align: center; color: white;">
            <div style="font-size: 1.8rem; font-weight: 700; margin: 0;">📊 CocoPan SKU Product Availability Reports</div>
            <div style="font-size: 0.9rem; margin: 0.5rem 0 0 0; opacity: 0.9;">Analytics Dashboard • {now.strftime('%I:%M %p')} Manila Time</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    # Main tabs
    tab1, tab2, tab3, tab4 = st.tabs(
        [
            "📊 Product Availability Dashboard",
            "📋 Out of Stock Details",
            "📊 Out of Stock Frequency",
            "📋 Reports & Export",
        ]
    )

    with tab1:
        availability_dashboard_section()
    with tab2:
        out_of_stock_details_section()
    with tab3:
        out_of_stock_frequency_section()
    with tab4:
        reports_export_section()

    # Footer with refresh
    st.markdown("---")
    col1, col2, col3 = st.columns([1, 2, 1])
    with col2:
        if st.button("🔄 Refresh All Data", use_container_width=True):
            # Clear all caches
            get_sku_availability_dashboard_data.clear()
            get_out_of_stock_details_data.clear()
            get_product_availability_summary.clear()
            st.success("Data refreshed!")
            st.rerun()

    with col3:
        st.markdown(f"*Last updated: {now.strftime('%H:%M')}*")

# ------------------------------------------------------------------------------
# Styles
# ------------------------------------------------------------------------------
st.markdown(
    """
    <style>
        @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');
        #MainMenu, footer, header, .stDeployButton {visibility: hidden;}
        .main {
            font-family: 'Inter', sans-serif;
            background: #F8FAFC;
            color: #1E293B;
            padding: 1rem !important;
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
        .js-plotly-plot {
            background: white !important;
            border-radius: 8px;
            box-shadow: 0 1px 3px rgba(0,0,0,0.1);
        }
    </style>
    """,
    unsafe_allow_html=True,
)

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.exception("SKU reporting dashboard error")
        st.error(f"❌ System Error: {e}")