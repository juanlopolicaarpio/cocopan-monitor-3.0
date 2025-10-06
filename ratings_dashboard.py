#!/usr/bin/env python3
"""
CocoPan Store Ratings Dashboard - Mobile-Friendly Redesign
Clean, modern layout optimized for all screen sizes with adaptive dark/light mode
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

# Custom CSS - UPDATED WITH ADAPTIVE THEME (MATCHING sku.py)
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');
    
    /* Hide Streamlit branding */
    #MainMenu, footer, .stDeployButton, header {visibility: hidden;}
    
    /* CSS Variables for theme switching - SAME AS sku.py */
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
    
    /* Dark mode variables - SAME AS sku.py */
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
    
    /* Global resets */
    .block-container {
        padding-top: 3rem;
        padding-bottom: 2rem;
        max-width: 100%;
    }
    
    /* Main layout - ADAPTIVE */
    .main { 
        font-family: 'Inter', sans-serif; 
        background: var(--bg-primary);
        color: var(--text-primary); 
        transition: background-color 0.3s ease, color 0.3s ease;
    }
    
    /* Hide sidebar */
    [data-testid="stSidebar"] {
        display: none;
    }
    
    /* Navigation link styling - ADAPTIVE */
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
    
    /* Header - Gradient stays the same but with better contrast */
    .dashboard-header {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white;
        padding: 2rem 1.5rem;
        border-radius: 12px;
        margin-bottom: 1.5rem;
        box-shadow: 0 4px 6px var(--shadow-medium);
    }
    
    .dashboard-title {
        font-size: 2rem;
        font-weight: 700;
        margin: 0;
        display: flex;
        align-items: center;
        gap: 0.5rem;
        color: white !important;
    }
    
    .last-updated {
        font-size: 0.9rem;
        opacity: 0.9;
        margin-top: 0.5rem;
        color: rgba(255, 255, 255, 0.9);
    }
    
    /* Filter section - ADAPTIVE */
    .filter-container {
        background: var(--bg-secondary);
        padding: 1.5rem;
        border-radius: 12px;
        box-shadow: 0 2px 8px var(--shadow-medium);
        margin-bottom: 1.5rem;
        border: 1px solid var(--border-color);
        transition: all 0.3s ease;
    }
    
    /* Metrics - ADAPTIVE */
    div[data-testid="stMetricValue"] {
        font-size: 1.8rem;
        font-weight: 700;
        color: var(--text-primary);
    }
    
    div[data-testid="stMetricLabel"] {
        font-size: 0.9rem;
        font-weight: 500;
        color: var(--text-muted);
    }
    
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
    
    /* Store cards - ADAPTIVE */
    .store-card {
        background: var(--bg-secondary);
        padding: 1.25rem;
        border-radius: 10px;
        box-shadow: 0 2px 4px var(--shadow-light);
        margin-bottom: 1rem;
        transition: all 0.2s ease;
        border-left: 4px solid transparent;
        border: 1px solid var(--border-color);
    }
    
    .store-card:hover {
        box-shadow: 0 4px 12px var(--shadow-medium);
        transform: translateY(-2px);
        border-color: var(--border-hover);
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
        color: var(--text-primary);
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
    
    /* Badges - ADAPTIVE */
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
    
    /* Dark mode badge adjustments */
    @media (prefers-color-scheme: dark) {
        .platform-grabfood {
            background: #065F46;
            color: #A7F3D0;
        }
        
        .platform-foodpanda {
            background: #831843;
            color: #FBCFE8;
        }
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
        background: var(--bg-tertiary);
        color: var(--text-muted);
    }
    
    /* Dark mode trend adjustments */
    @media (prefers-color-scheme: dark) {
        .trend-up {
            background: #065F46;
            color: #A7F3D0;
        }
        
        .trend-down {
            background: #7F1D1D;
            color: #FECACA;
        }
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
    .rank-other { 
        background: var(--bg-tertiary); 
        color: var(--text-primary);
        border: 2px solid var(--border-color);
    }
    
    /* Distribution chart - ADAPTIVE */
    .dist-row {
        display: flex;
        align-items: center;
        gap: 1rem;
        margin-bottom: 0.75rem;
    }
    
    .dist-label {
        font-weight: 600;
        width: 40px;
        color: var(--text-secondary);
    }
    
    .dist-bar-container {
        flex: 1;
        height: 24px;
        background: var(--bg-tertiary);
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
        color: var(--text-secondary);
        white-space: nowrap;
        min-width: 80px;
        text-align: right;
    }
    
    /* Section headers - ADAPTIVE */
    h3 {
        color: var(--text-primary) !important;
        font-weight: 600 !important;
    }
    
    /* Captions - ADAPTIVE */
    .stCaptionContainer, .stCaption {
        color: var(--text-muted) !important;
    }
    
    /* Input fields - ADAPTIVE */
    .stSelectbox > div > div {
        background: var(--bg-secondary);
        border: 1px solid var(--border-color);
        color: var(--text-primary);
        border-radius: 8px;
        transition: all 0.3s ease;
    }
    
    .stSelectbox label {
        color: var(--text-primary) !important;
    }
    
    /* Button styling - ADAPTIVE */
    .stButton > button {
        background: #667eea;
        border: 1px solid #5a67d8;
        color: white;
        border-radius: 6px;
        font-weight: 500;
        transition: all 0.3s ease;
    }
    
    .stButton > button:hover {
        background: #5a67d8;
        border-color: #4c51bf;
        box-shadow: 0 2px 4px rgba(102, 126, 234, 0.3);
    }
    
    /* Mobile responsiveness */
    @media (max-width: 768px) {
        .main {
            padding: 1rem;
        }
        
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
    
    /* Streamlit specific adjustments - ADAPTIVE */
    .stExpander {
        border: none;
        box-shadow: none;
        background: var(--bg-secondary);
    }
    
    /* Force theme-aware styling for dropdowns in dark mode */
    @media (prefers-color-scheme: dark) {
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
// Detect theme preference and add class to body - SAME AS sku.py
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
    
    # FIXED: Single navigation button (matching sku.py)
    col1, col2, col3 = st.columns([1, 2, 1])
    with col1:
        if st.button("🏢 ← Back to Uptime Dashboard", use_container_width=True):
            st.markdown("""
            <script>
            window.open('https://cocopanwatchtower.com/', '_blank');
            </script>
            """, unsafe_allow_html=True)
    
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
    col1, col2 = st.columns(2)
    
    avg_rating = sum(r['rating'] for r in ratings_data) / len(ratings_data)
    total_stores = len(ratings_data)
    
    with col1:
        st.metric("Average Rating", f"{avg_rating:.2f}★")
    with col2:
        st.metric("Total Stores", total_stores)
    
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