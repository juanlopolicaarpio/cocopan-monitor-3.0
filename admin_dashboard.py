#!/usr/bin/env python3
"""
CocoPan Admin Dashboard - Phase 2
Mobile-first manual verification interface for operations team
Production ready - handles blocked/unknown/error stores from monitoring
"""
import streamlit as st
import pandas as pd
import os
import time
import logging
from datetime import datetime
from typing import List, Dict, Any

# Import production modules
from config import config
from database import db

# Set page config for mobile-first experience
st.set_page_config(
    page_title="CocoPan Admin",
    page_icon="🔧",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# Setup logging
logging.basicConfig(level=getattr(logging, config.LOG_LEVEL))
logger = logging.getLogger(__name__)

# MOBILE-FIRST CSS
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');
    
    /* Hide Streamlit elements */
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    .stDeployButton {display: none;}
    header {visibility: hidden;}
    
    /* Mobile-first design */
    .main {
        font-family: 'Inter', sans-serif;
        background: #F8FAFC;
        color: #1E293B;
        padding: 1rem !important;
    }
    
    /* Header */
    .admin-header {
        background: linear-gradient(135deg, #DC2626 0%, #EF4444 100%);
        border-radius: 12px;
        padding: 1.5rem;
        margin-bottom: 1.5rem;
        text-align: center;
        color: white;
    }
    
    .admin-title {
        font-size: 1.8rem;
        font-weight: 700;
        margin: 0;
    }
    
    .admin-subtitle {
        font-size: 0.9rem;
        margin: 0.5rem 0 0 0;
        opacity: 0.9;
    }
    
    /* Store cards - mobile optimized */
    .store-card {
        background: white;
        border-radius: 12px;
        padding: 1.5rem;
        margin-bottom: 1rem;
        border-left: 4px solid #F59E0B;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
    }
    
    .store-name {
        font-size: 1.1rem;
        font-weight: 600;
        color: #1E293B;
        margin-bottom: 0.5rem;
    }
    
    .store-meta {
        font-size: 0.85rem;
        color: #64748B;
        margin-bottom: 1rem;
    }
    
    .store-platform {
        display: inline-block;
        background: #E0F2FE;
        color: #0C4A6E;
        padding: 0.2rem 0.5rem;
        border-radius: 4px;
        font-size: 0.75rem;
        font-weight: 500;
        margin-right: 0.5rem;
    }
    
    /* Action buttons - large touch targets */
    .action-buttons {
        display: flex;
        gap: 0.75rem;
        margin-top: 1rem;
    }
    
    .btn-check {
        flex: 1;
        background: #3B82F6;
        color: white;
        border: none;
        border-radius: 8px;
        padding: 0.75rem;
        font-weight: 500;
        font-size: 0.9rem;
        cursor: pointer;
        text-decoration: none;
        text-align: center;
        display: block;
        transition: all 0.2s;
    }
    
    .btn-check:hover {
        background: #2563EB;
        transform: translateY(-1px);
    }
    
    /* Status buttons */
    .btn-online, .btn-offline {
        flex: 1;
        border: none;
        border-radius: 8px;
        padding: 0.75rem;
        font-weight: 600;
        font-size: 0.9rem;
        cursor: pointer;
        transition: all 0.2s;
        min-height: 48px; /* Ensure good touch target */
    }
    
    .btn-online {
        background: #10B981;
        color: white;
    }
    
    .btn-online:hover {
        background: #059669;
        transform: translateY(-1px);
    }
    
    .btn-offline {
        background: #EF4444;
        color: white;
    }
    
    .btn-offline:hover {
        background: #DC2626;
        transform: translateY(-1px);
    }
    
    /* Progress bar */
    .progress-container {
        background: #E2E8F0;
        border-radius: 8px;
        height: 8px;
        margin: 1rem 0;
        overflow: hidden;
    }
    
    .progress-bar {
        background: linear-gradient(90deg, #10B981, #059669);
        height: 100%;
        transition: width 0.3s ease;
    }
    
    .progress-text {
        text-align: center;
        font-size: 0.9rem;
        color: #64748B;
        margin: 0.5rem 0;
    }
    
    /* Login form */
    .login-container {
        max-width: 400px;
        margin: 2rem auto;
        padding: 2rem;
        background: white;
        border-radius: 12px;
        box-shadow: 0 4px 6px rgba(0,0,0,0.1);
    }
    
    /* Success/error messages */
    .success-message {
        background: #D1FAE5;
        color: #065F46;
        padding: 1rem;
        border-radius: 8px;
        margin: 1rem 0;
        border-left: 4px solid #10B981;
    }
    
    .error-message {
        background: #FEE2E2;
        color: #991B1B;
        padding: 1rem;
        border-radius: 8px;
        margin: 1rem 0;
        border-left: 4px solid #EF4444;
    }
    
    /* Responsive design */
    @media (max-width: 768px) {
        .admin-title {
            font-size: 1.5rem;
        }
        
        .store-card {
            padding: 1rem;
        }
        
        .action-buttons {
            flex-direction: column;
        }
        
        .btn-check, .btn-online, .btn-offline {
            min-height: 52px;
            font-size: 1rem;
        }
    }
    
    /* Streamlit button overrides */
    .stButton > button {
        width: 100%;
        border-radius: 8px;
        border: none;
        font-weight: 600;
        font-size: 0.9rem;
        padding: 0.75rem;
        transition: all 0.2s;
        min-height: 48px;
    }
    
    .stButton > button:hover {
        transform: translateY(-1px);
        box-shadow: 0 4px 8px rgba(0,0,0,0.2);
    }
    
    /* Hide streamlit padding */
    .block-container {
        padding-top: 1rem;
        padding-bottom: 1rem;
    }
</style>
""", unsafe_allow_html=True)

def check_authentication() -> bool:
    """Check if user is authenticated"""
    # Get admin password from environment
    admin_password = os.getenv('ADMIN_PASSWORD', 'admin123')
    
    if 'authenticated' not in st.session_state:
        st.session_state.authenticated = False
    
    if not st.session_state.authenticated:
        # Show login form
        st.markdown("""
        <div class="login-container">
            <h2 style="text-align: center; color: #1E293B; margin-bottom: 1.5rem;">
                🔧 CocoPan Admin Access
            </h2>
        </div>
        """, unsafe_allow_html=True)
        
        with st.form("login_form"):
            password = st.text_input("Admin Password", type="password", placeholder="Enter admin password")
            submit = st.form_submit_button("Access Admin Dashboard", use_container_width=True)
            
            if submit:
                if password == admin_password:
                    st.session_state.authenticated = True
                    st.rerun()
                else:
                    st.error("❌ Invalid password. Access denied.")
        
        return False
    
    return True

@st.cache_data(ttl=60)  # Cache for 1 minute
def load_stores_needing_attention() -> pd.DataFrame:
    """Load stores that need manual verification"""
    try:
        return db.get_stores_needing_attention()
    except Exception as e:
        logger.error(f"Error loading stores needing attention: {e}")
        return pd.DataFrame()

def mark_store_status(store_id: int, store_name: str, is_online: bool, platform: str) -> bool:
    """Mark store as online/offline - saves as regular status check"""
    try:
        # Save as normal status_check - no special override markers
        success = db.save_status_check(
            store_id=store_id,
            is_online=is_online,
            response_time_ms=1200,  # Reasonable fake response time
            error_message=None      # No error message for manual verification
        )
        
        if success:
            status_text = "online" if is_online else "offline"
            logger.info(f"✅ Admin marked {store_name} as {status_text}")
            
            # Clear cache to force refresh
            load_stores_needing_attention.clear()
            
            return True
        else:
            logger.error(f"❌ Failed to save status for {store_name}")
            return False
            
    except Exception as e:
        logger.error(f"Error marking store status: {e}")
        return False

def format_platform_emoji(platform: str) -> str:
    """Get platform emoji"""
    if 'grab' in platform.lower():
        return "🛒"
    elif 'foodpanda' in platform.lower() or 'panda' in platform.lower():
        return "🐼"
    else:
        return "🏪"

def format_time_ago(checked_at) -> str:
    """Format time ago string"""
    try:
        if pd.isna(checked_at):
            return "Unknown"
        
        checked_time = pd.to_datetime(checked_at)
        now = datetime.now()
        
        # Handle timezone-aware datetime
        if checked_time.tz is not None:
            now = now.replace(tzinfo=checked_time.tz)
        
        diff = now - checked_time
        
        if diff.days > 0:
            return f"{diff.days}d ago"
        elif diff.seconds > 3600:
            hours = diff.seconds // 3600
            return f"{hours}h ago"
        elif diff.seconds > 60:
            minutes = diff.seconds // 60
            return f"{minutes}m ago"
        else:
            return "Just now"
    except:
        return "Unknown"

def main():
    """Main admin dashboard"""
    
    # Check authentication
    if not check_authentication():
        return
    
    # Load stores needing attention
    stores_df = load_stores_needing_attention()
    
    # Header
    current_time = config.get_current_time()
    st.markdown(f"""
    <div class="admin-header">
        <div class="admin-title">🚨 Manual Verification Required</div>
        <div class="admin-subtitle">{len(stores_df)} stores need attention • {current_time.strftime('%I:%M %p')} Manila Time</div>
    </div>
    """, unsafe_allow_html=True)
    
    # Handle empty state
    if stores_df.empty:
        st.markdown("""
        <div class="success-message">
            <h3 style="margin: 0 0 0.5rem 0;">✅ All Clear!</h3>
            <p style="margin: 0;">No stores currently need manual verification. All systems operating normally.</p>
        </div>
        """, unsafe_allow_html=True)
        
        if st.button("🔄 Check for New Issues", use_container_width=True):
            load_stores_needing_attention.clear()
            st.rerun()
        
        return
    
    # Progress tracking
    if 'completed_stores' not in st.session_state:
        st.session_state.completed_stores = set()
    
    total_stores = len(stores_df)
    completed_count = len(st.session_state.completed_stores)
    progress = completed_count / total_stores if total_stores > 0 else 0
    
    # Progress bar
    st.markdown(f"""
    <div class="progress-container">
        <div class="progress-bar" style="width: {progress * 100:.1f}%;"></div>
    </div>
    <div class="progress-text">Progress: {completed_count}/{total_stores} completed</div>
    """, unsafe_allow_html=True)
    
    # Store verification interface
    for idx, store in stores_df.iterrows():
        store_id = store['id']
        store_name = store['name']
        platform = store.get('platform', 'unknown')
        url = store['url']
        checked_at = store.get('checked_at')
        problem_status = store.get('problem_status', 'UNKNOWN')
        
        # Skip already completed stores
        if store_id in st.session_state.completed_stores:
            continue
        
        # Store card
        platform_emoji = format_platform_emoji(platform)
        time_ago = format_time_ago(checked_at)
        card_number = idx + 1
        
        st.markdown(f"""
        <div class="store-card">
            <div class="store-name">{store_name} [{card_number}/{total_stores}]</div>
            <div class="store-meta">
                <span class="store-platform">{platform_emoji} {platform.title()}</span>
                <span>Last checked: {time_ago}</span>
            </div>
        </div>
        """, unsafe_allow_html=True)
        
        # Action buttons
        col1, col2, col3 = st.columns([2, 1, 1])
        
        with col1:
            st.markdown(f"""
            <a href="{url}" target="_blank" class="btn-check">
                🔗 Check Store
            </a>
            """, unsafe_allow_html=True)
        
        with col2:
            if st.button(f"✅ Online", key=f"online_{store_id}", use_container_width=True):
                if mark_store_status(store_id, store_name, True, platform):
                    st.session_state.completed_stores.add(store_id)
                    st.success(f"✅ {store_name} marked as **Online**")
                    time.sleep(1)  # Brief pause for user feedback
                    st.rerun()
                else:
                    st.error("❌ Failed to update store status. Please try again.")
        
        with col3:
            if st.button(f"❌ Offline", key=f"offline_{store_id}", use_container_width=True):
                if mark_store_status(store_id, store_name, False, platform):
                    st.session_state.completed_stores.add(store_id)
                    st.success(f"❌ {store_name} marked as **Offline**")
                    time.sleep(1)  # Brief pause for user feedback
                    st.rerun()
                else:
                    st.error("❌ Failed to update store status. Please try again.")
        
        st.markdown("<br>", unsafe_allow_html=True)
    
    # Footer actions
    if completed_count < total_stores:
        remaining = total_stores - completed_count
        st.info(f"📋 {remaining} stores remaining for verification")
    else:
        st.success("🎉 All stores verified! Great work!")
        if st.button("🔄 Check for New Issues", use_container_width=True):
            st.session_state.completed_stores.clear()
            load_stores_needing_attention.clear()
            st.rerun()
    
    # Refresh button
    st.markdown("---")
    col1, col2, col3 = st.columns([1, 2, 1])
    with col2:
        if st.button("🔄 Refresh Store List", use_container_width=True):
            load_stores_needing_attention.clear()
            st.rerun()

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        st.error(f"❌ System Error: {e}")
        logger.error(f"Admin dashboard error: {e}")