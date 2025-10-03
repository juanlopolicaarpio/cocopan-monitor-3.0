#!/usr/bin/env python3
"""
CocoPan Store Ratings Dashboard - Mobile-Friendly Redesign
Clean, modern layout optimized for all screen sizes
"""
import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
from typing import Optional
import pytz

from database import db

# Page configuration
st.set_page_config(
    page_title="Store Ratings Dashboard",
    page_icon="⭐",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# Custom CSS - Mobile-First Design
st.markdown("""
<style>
    /* Global resets */
    .block-container {
        padding-top: 3rem;
        padding-bottom: 2rem;
        max-width: 100%;
    }
    
    /* Hide sidebar */
    [data-testid="stSidebar"] {
        display: none;
    }
    
    /* Header */
    .dashboard-header {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white;
        padding: 2rem 1.5rem;
        border-radius: 12px;
        margin-bottom: 1.5rem;
        box-shadow: 0 4px 6px rgba(0,0,0,0.1);
    }
    
    .dashboard-title {
        font-size: 2rem;
        font-weight: 700;
        margin: 0;
        display: flex;
        align-items: center;
        gap: 0.5rem;
    }
    
    .last-updated {
        font-size: 0.9rem;
        opacity: 0.9;
        margin-top: 0.5rem;
    }
    
    /* Filter section */
    .filter-container {
        background: white;
        padding: 1.5rem;
        border-radius: 12px;
        box-shadow: 0 2px 8px rgba(0,0,0,0.08);
        margin-bottom: 1.5rem;
    }
    
    /* Metrics */
    div[data-testid="stMetricValue"] {
        font-size: 1.8rem;
        font-weight: 700;
    }
    
    div[data-testid="stMetricLabel"] {
        font-size: 0.9rem;
        font-weight: 500;
        color: #64748b;
    }
    
    /* Store cards */
    .store-card {
        background: white;
        padding: 1.25rem;
        border-radius: 10px;
        box-shadow: 0 2px 4px rgba(0,0,0,0.06);
        margin-bottom: 1rem;
        transition: all 0.2s ease;
        border-left: 4px solid transparent;
    }
    
    .store-card:hover {
        box-shadow: 0 4px 12px rgba(0,0,0,0.12);
        transform: translateY(-2px);
    }
    
    .store-header {
        display: flex;
        align-items: center;
        gap: 0.75rem;
        margin-bottom: 0.75rem;
        flex-wrap: wrap;
    }
    
    .store-name {
        font-size: 1.1rem;
        font-weight: 700;
        color: #1e293b;
        flex: 1;
        min-width: 200px;
    }
    
    .store-footer {
        display: flex;
        align-items: center;
        justify-content: space-between;
        flex-wrap: wrap;
        gap: 0.75rem;
    }
    
    /* Star display */
    .star-display {
        display: flex;
        align-items: center;
        gap: 0.5rem;
    }
    
    .stars {
        color: #FFD700;
        font-size: 1.3rem;
        letter-spacing: 1px;
    }
    
    .rating-number {
        font-weight: 700;
        font-size: 1.3rem;
    }
    
    .rating-excellent { color: #059669; }
    .rating-good { color: #f59e0b; }
    .rating-fair { color: #ef4444; }
    
    /* Badges */
    .badge {
        display: inline-flex;
        align-items: center;
        padding: 0.35rem 0.75rem;
        border-radius: 20px;
        font-size: 0.85rem;
        font-weight: 600;
        white-space: nowrap;
    }
    
    .platform-grabfood {
        background: #dcfce7;
        color: #15803d;
    }
    
    .platform-foodpanda {
        background: #fce7f3;
        color: #be185d;
    }
    
    .trend-badge {
        padding: 0.25rem 0.6rem;
        border-radius: 12px;
        font-size: 0.8rem;
        font-weight: 600;
    }
    
    .trend-up {
        background: #dcfce7;
        color: #15803d;
    }
    
    .trend-down {
        background: #fee2e2;
        color: #dc2626;
    }
    
    .trend-stable {
        background: #f3f4f6;
        color: #6b7280;
    }
    
    /* Rank badge */
    .rank-badge {
        display: inline-flex;
        align-items: center;
        justify-content: center;
        width: 36px;
        height: 36px;
        border-radius: 50%;
        font-weight: 700;
        font-size: 1rem;
        flex-shrink: 0;
    }
    
    .rank-1 { background: #fbbf24; color: #78350f; }
    .rank-2 { background: #94a3b8; color: white; }
    .rank-3 { background: #fb923c; color: white; }
    .rank-other { background: #e5e7eb; color: #374151; }
    
    /* Distribution chart */
    .dist-row {
        display: flex;
        align-items: center;
        gap: 1rem;
        margin-bottom: 0.75rem;
    }
    
    .dist-label {
        font-weight: 600;
        width: 40px;
        color: #475569;
    }
    
    .dist-bar-container {
        flex: 1;
        height: 24px;
        background: #f1f5f9;
        border-radius: 12px;
        overflow: hidden;
    }
    
    .dist-bar {
        height: 100%;
        background: linear-gradient(90deg, #667eea 0%, #764ba2 100%);
        transition: width 0.3s ease;
    }
    
    .dist-count {
        font-weight: 600;
        color: #475569;
        white-space: nowrap;
        min-width: 80px;
        text-align: right;
    }
    
    /* Mobile responsiveness */
    @media (max-width: 768px) {
        .dashboard-title {
            font-size: 1.5rem;
        }
        
        .store-card {
            padding: 1rem;
        }
        
        .store-name {
            font-size: 1rem;
            min-width: 150px;
        }
        
        .stars {
            font-size: 1.1rem;
        }
        
        .rating-number {
            font-size: 1.1rem;
        }
        
        .store-header {
            gap: 0.5rem;
        }
        
        .store-footer {
            flex-direction: column;
            align-items: flex-start;
        }
        
        div[data-testid="stMetricValue"] {
            font-size: 1.5rem;
        }
    }
    
    /* Streamlit specific adjustments */
    .stSelectbox > div > div {
        border-radius: 8px;
    }
    
    .stExpander {
        border: none;
        box-shadow: none;
    }
</style>
""", unsafe_allow_html=True)


def get_star_display(rating: float) -> str:
    """Simple star display with filled/empty stars - ALWAYS ROUND DOWN"""
    # Always round DOWN to show accurate star representation
    # 4.9 = 4 stars, 5.0 = 5 stars
    full_stars = int(rating)  # Round down
    empty_stars = 5 - full_stars
    
    if rating >= 4.5:
        color_class = "rating-excellent"
    elif rating >= 4.0:
        color_class = "rating-good"
    else:
        color_class = "rating-fair"
    
    stars_html = f'<span class="stars">{"★" * full_stars}{"☆" * empty_stars}</span>'
    number_html = f'<span class="rating-number {color_class}">{rating:.1f}</span>'
    
    return f'<div class="star-display">{stars_html}{number_html}</div>'


def get_trend_badge(trend: str, trend_value: float) -> str:
    """Trend indicator badge"""
    if trend == 'up' and trend_value > 0:
        return f'<span class="trend-badge trend-up">↑ {trend_value:+.1f}</span>'
    elif trend == 'down' and trend_value < 0:
        return f'<span class="trend-badge trend-down">↓ {abs(trend_value):.1f}</span>'
    else:
        return '<span class="trend-badge trend-stable">—</span>'


def get_platform_badge(platform: str) -> str:
    """Platform badge"""
    if platform == 'grabfood':
        return '<span class="badge platform-grabfood">GrabFood</span>'
    else:
        return '<span class="badge platform-foodpanda">Foodpanda</span>'


def get_rank_badge(rank: int) -> str:
    """Rank badge"""
    if rank == 1:
        badge_class = "rank-1"
    elif rank == 2:
        badge_class = "rank-2"
    elif rank == 3:
        badge_class = "rank-3"
    else:
        badge_class = "rank-other"
    
    return f'<span class="rank-badge {badge_class}">{rank}</span>'


def show_distribution_chart(ratings_data):
    """Show rating distribution - matches displayed stars (always rounds DOWN)"""
    if not ratings_data:
        return
    
    # Count stores by their DISPLAYED star rating (floor value)
    dist = {
        '5★': sum(1 for r in ratings_data if int(r['rating']) == 5),
        '4★': sum(1 for r in ratings_data if int(r['rating']) == 4),
        '3★': sum(1 for r in ratings_data if int(r['rating']) == 3),
        '2★': sum(1 for r in ratings_data if int(r['rating']) == 2),
        '1★': sum(1 for r in ratings_data if int(r['rating']) <= 1),
    }
    
    total = len(ratings_data)
    
    st.markdown("### 📊 Rating Distribution")
    st.caption("How many stores fall into each rating category")
    
    for stars, count in dist.items():
        pct = (count / total * 100) if total > 0 else 0
        st.markdown(f"""
        <div class="dist-row">
            <div class="dist-label">{stars}</div>
            <div class="dist-bar-container">
                <div class="dist-bar" style="width: {pct}%"></div>
            </div>
            <div class="dist-count">{count} stores ({pct:.0f}%)</div>
        </div>
        """, unsafe_allow_html=True)


def main():
    """Main dashboard"""
    
    # Header
    st.markdown("""
    <div class="dashboard-header">
        <h1 class="dashboard-title">⭐ Store Ratings Dashboard</h1>
    </div>
    """, unsafe_allow_html=True)
    
    # Filters Section
    with st.container():
        st.markdown('<div class="filter-container">', unsafe_allow_html=True)
        st.markdown("### 🔍 Filters")
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            platform_filter = st.selectbox(
                "Platform",
                options=['all', 'grabfood', 'foodpanda'],
                format_func=lambda x: {
                    'all': 'All Platforms',
                    'grabfood': 'GrabFood',
                    'foodpanda': 'Foodpanda'
                }[x]
            )
        
        with col2:
            star_filter = st.selectbox(
                "Rating",
                options=['all', '5', '4', '3', '2', '1'],
                format_func=lambda x: {
                    'all': 'All Ratings',
                    '5': '⭐⭐⭐⭐⭐ 5 Star',
                    '4': '⭐⭐⭐⭐ 4 Star',
                    '3': '⭐⭐⭐ 3 Star',
                    '2': '⭐⭐ 2 Star',
                    '1': '⭐ 1 Star'
                }[x]
            )
        
        with col3:
            sort_filter = st.selectbox(
                "Sort By",
                options=['rating_desc', 'rating_asc',  'name_asc'],
                format_func=lambda x: {
                    'rating_desc': 'Highest Rating',
                    'rating_asc': 'Lowest Rating',
                    'name_asc': 'Name (A-Z)'
                }[x]
            )
        
        st.markdown('</div>', unsafe_allow_html=True)
    
    # Apply filters - matches displayed stars (floor value)
    platform_param = None if platform_filter == 'all' else platform_filter
    
    min_rating = None
    max_rating = None
    if star_filter == '5':
        min_rating = 5.0  # Only true 5-star ratings
    elif star_filter == '4':
        min_rating = 4.0
        max_rating = 4.99
    elif star_filter == '3':
        min_rating = 3.0
        max_rating = 3.99
    elif star_filter == '2':
        min_rating = 2.0
        max_rating = 2.99
    elif star_filter == '1':
        max_rating = 1.99
    
    # Get data
    ratings_data = db.get_store_ratings_dashboard(
        platform=platform_param,
        min_rating=min_rating,
        max_rating=max_rating,
        sort_by=sort_filter
    )
    
    if not ratings_data:
        st.info("🏪 No stores match your current filters. Try adjusting your selection above.")
        return
    
    # Get last update time from database and convert to Manila timezone
    manila_tz = pytz.timezone('Asia/Manila')
    try:
        if ratings_data and len(ratings_data) > 0:
            # Check what timestamp field exists
            first_record = ratings_data[0]
            timestamp_field = None
            
            for possible_field in ['scraped_at', 'last_scraped', 'updated_at', 'checked_at', 'created_at', 'timestamp']:
                if possible_field in first_record and first_record[possible_field]:
                    timestamp_field = possible_field
                    break
            
            if timestamp_field:
                last_updated = max([r[timestamp_field] for r in ratings_data if r.get(timestamp_field)])
                
                # Parse the timestamp
                if isinstance(last_updated, str):
                    last_updated = datetime.fromisoformat(last_updated.replace('Z', '+00:00'))
                
                # Convert to Manila timezone
                if hasattr(last_updated, 'tzinfo') and last_updated.tzinfo:
                    last_updated = last_updated.astimezone(manila_tz)
                else:
                    # Assume UTC if no timezone info
                    utc_tz = pytz.UTC
                    last_updated = utc_tz.localize(last_updated).astimezone(manila_tz)
                
                # Format as readable datetime
                formatted_date = last_updated.strftime("%B %d, %Y at %I:%M %p")
                st.caption(f"Data as of {formatted_date}")
            else:
                st.caption("Data timestamp unavailable")
    except Exception as e:
        st.caption("Data timestamp unavailable")
    
    # Metrics
    col1, col2, col3 = st.columns(3)
    
    avg_rating = sum(r['rating'] for r in ratings_data) / len(ratings_data)
    perfect_5 = sum(1 for r in ratings_data if r['rating'] >= 5.0)
    below_4 = sum(1 for r in ratings_data if r['rating'] < 4.0)
    
    with col1:
        st.metric("Average Rating", f"{avg_rating:.2f}★")
    with col2:
        st.metric("Perfect 5.0★", perfect_5)
    with col3:
        st.metric("Below 4.0★", below_4)
    
    st.divider()
    
    # Layout: Stores list and distribution
    col_left, col_right = st.columns([2, 1])
    
    with col_right:
        show_distribution_chart(ratings_data)
    
    with col_left:
        st.markdown(f"### 🏪 {len(ratings_data)} Stores")
        
        # Store cards
        for store in ratings_data:
            st.markdown('<div class="store-card">', unsafe_allow_html=True)
            
            # Store header
            st.markdown(f"""
            <div class="store-header">
                {get_rank_badge(store['rank'])}
                <div class="store-name">{store['store_name']}</div>
            </div>
            """, unsafe_allow_html=True)
            
            # Store footer
            st.markdown(f"""
            <div class="store-footer">
                <div>
                    {get_platform_badge(store['platform'])}
                </div>
                <div style="display: flex; gap: 0.75rem; align-items: center;">
                    {get_star_display(store['rating'])}
                    {get_trend_badge(store['trend'], store['trend_value'])}
                </div>
            </div>
            """, unsafe_allow_html=True)
            
            st.markdown('</div>', unsafe_allow_html=True)


if __name__ == "__main__":
    main()