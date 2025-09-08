#!/usr/bin/env python3
"""
CocoPan Admin Dashboard - Enhanced with EMAIL AUTH & VA HOURLY CHECK-IN
- Email-only authentication using admin_alerts.json
- NEW: VA Hourly Check-in tab for manual Foodpanda store status
- Existing admin verification functionality (unchanged)
- Mobile-first design maintained
"""
import streamlit as st
import pandas as pd
import os
import time
import logging
import json
from datetime import datetime
from typing import List, Dict, Any

# Import production modules
from config import config
from database import db

# Health check endpoint for Railway
import threading
import http.server
import socketserver
from urllib.parse import urlparse

def create_health_server():
    """Create a simple health check server"""
    class HealthHandler(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path == '/healthz':
                try:
                    # Quick database check
                    db.get_database_stats()
                    self.send_response(200)
                    self.send_header('Content-type', 'text/plain')
                    self.end_headers()
                    self.wfile.write(b'OK - Admin Dashboard Healthy')
                except Exception as e:
                    self.send_response(500)
                    self.send_header('Content-type', 'text/plain')
                    self.end_headers()
                    self.wfile.write(f'ERROR - {str(e)}'.encode())
            else:
                self.send_response(404)
                self.end_headers()
        
        def log_message(self, format, *args):
            # Suppress health check logs
            pass
    
    try:
        port = 8504  # Different port for health checks
        with socketserver.TCPServer(("", port), HealthHandler) as httpd:
            httpd.serve_forever()
    except Exception as e:
        logger.debug(f"Health server error: {e}")

# Start health check server in background
if os.getenv('RAILWAY_ENVIRONMENT') == 'production':
    health_thread = threading.Thread(target=create_health_server, daemon=True)
    health_thread.start()

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

# ----------- EMAIL AUTHENTICATION -----------
def load_authorized_admin_emails():
    """Load authorized admin emails from admin_alerts.json"""
    try:
        with open('admin_alerts.json', 'r') as f:
            data = json.load(f)
            
        # Extract admin emails
        admin_team = data.get('admin_team', {})
        if admin_team.get('enabled', False):
            emails = admin_team.get('emails', [])
            # Filter out empty strings
            authorized_emails = [email.strip() for email in emails if email.strip()]
            logger.info(f"✅ Loaded {len(authorized_emails)} authorized admin emails")
            return authorized_emails
        else:
            logger.warning("⚠️ Admin team disabled in config")
            return ["juanlopolicarpio@gmail.com"]  # Fallback
            
    except Exception as e:
        logger.error(f"❌ Failed to load admin emails: {e}")
        # Fallback to ensure system works
        return ["juanlopolicarpio@gmail.com"]

def check_admin_email_authentication():
    """Check if admin user is authenticated via email"""
    authorized_emails = load_authorized_admin_emails()
    
    if 'admin_authenticated' not in st.session_state:
        st.session_state.admin_authenticated = False
        st.session_state.admin_email = None
    
    if not st.session_state.admin_authenticated:
        # Show email authentication form
        st.markdown("""
        <div style="max-width: 400px; margin: 2rem auto; padding: 2rem; background: white; border-radius: 12px; box-shadow: 0 4px 6px rgba(0,0,0,0.1);">
            <h2 style="text-align: center; color: #1E293B; margin-bottom: 1.5rem;">
                🔧 CocoPan Admin Access
            </h2>
            <p style="text-align: center; color: #64748B; margin-bottom: 1.5rem;">
                Enter your authorized admin email address
            </p>
        </div>
        """, unsafe_allow_html=True)
        
        with st.form("admin_email_auth_form"):
            email = st.text_input(
                "Admin Email Address", 
                placeholder="admin@cocopan.com",
                help="Enter your authorized admin email address"
            )
            submit = st.form_submit_button("Access Admin Dashboard", use_container_width=True)
            
            if submit:
                email = email.strip().lower()
                
                if email in [auth_email.lower() for auth_email in authorized_emails]:
                    st.session_state.admin_authenticated = True
                    st.session_state.admin_email = email
                    logger.info(f"✅ Admin authenticated: {email}")
                    st.success("✅ Admin access granted! Redirecting...")
                    st.rerun()
                else:
                    st.error("❌ Email not authorized for admin access. Please contact the system administrator.")
                    logger.warning(f"❌ Unauthorized admin access attempt: {email}")
        
        return False
    
    return True

# ----------- FOODPANDA STORE LOADING -----------
@st.cache_data(ttl=300)
def load_foodpanda_stores():
    """Load all Foodpanda stores from branch_urls.json"""
    try:
        with open('branch_urls.json', 'r') as f:
            data = json.load(f)
        
        urls = data.get('urls', [])
        foodpanda_stores = []
        
        for url in urls:
            if 'foodpanda' in url:  # Both foodpanda.ph and foodpanda.page.link
                # Try to get existing store from database
                try:
                    with db.get_connection() as conn:
                        cursor = conn.cursor()
                        cursor.execute("SELECT id, name FROM stores WHERE url = %s", (url,))
                        result = cursor.fetchone()
                        
                        if result:
                            store_id, store_name = result[0], result[1]
                        else:
                            # Create store if it doesn't exist
                            store_name = extract_store_name_from_url(url)
                            store_id = db.get_or_create_store(store_name, url)
                        
                        foodpanda_stores.append({
                            'id': store_id,
                            'name': store_name,
                            'url': url
                        })
                except Exception as e:
                    logger.error(f"Error processing store {url}: {e}")
                    # Add with extracted name as fallback
                    store_name = extract_store_name_from_url(url)
                    foodpanda_stores.append({
                        'id': None,
                        'name': store_name,
                        'url': url
                    })
        
        logger.info(f"📋 Loaded {len(foodpanda_stores)} Foodpanda stores for VA check-in")
        return foodpanda_stores
        
    except Exception as e:
        logger.error(f"❌ Failed to load Foodpanda stores: {e}")
        return []

def extract_store_name_from_url(url: str) -> str:
    """Extract store name from URL (simplified version)"""
    try:
        if 'foodpanda.ph' in url:
            # Pattern: /restaurant/code/store-name
            import re
            match = re.search(r'/restaurant/[^/]+/([^/?]+)', url)
            if match:
                raw_name = match.group(1)
                name = raw_name.replace('-', ' ').replace('_', ' ').title()
                if not name.lower().startswith('cocopan'):
                    name = f"Cocopan {name}"
                return name
        elif 'foodpanda.page.link' in url:
            # For redirect URLs, create a name from the URL ID
            url_id = url.split('/')[-1][:8] if '/' in url else 'unknown'
            return f"Cocopan Foodpanda {url_id.upper()}"
        
        return "Cocopan Foodpanda Store"
        
    except Exception:
        return "Cocopan Foodpanda Store"

def save_va_checkin(offline_store_ids: List[int], admin_email: str) -> bool:
    """Save VA check-in results to database"""
    try:
        current_time = config.get_current_time()
        success_count = 0
        error_count = 0
        
        # Get all Foodpanda stores
        foodpanda_stores = load_foodpanda_stores()
        
        for store in foodpanda_stores:
            store_id = store['id']
            if store_id is None:
                continue
                
            try:
                # Determine if store is offline based on VA selection
                is_online = store_id not in offline_store_ids
                
                # Create error message to mark this as VA check-in
                if is_online:
                    error_message = f"[VA_CHECKIN] Store reported online by {admin_email}"
                else:
                    error_message = f"[VA_CHECKIN] Store reported offline by {admin_email}"
                
                # Save to database
                success = db.save_status_check(
                    store_id=store_id,
                    is_online=is_online,
                    response_time_ms=1000,  # Fake response time for manual checks
                    error_message=error_message
                )
                
                if success:
                    success_count += 1
                else:
                    error_count += 1
                    
            except Exception as e:
                logger.error(f"Error saving VA check-in for store {store_id}: {e}")
                error_count += 1
        
        logger.info(f"✅ VA Check-in saved: {success_count} stores, {error_count} errors")
        return error_count == 0
        
    except Exception as e:
        logger.error(f"❌ Failed to save VA check-in: {e}")
        return False

# MOBILE-FIRST CSS (unchanged from original)
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
        background: white;
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
    
    /* VA Check-in specific styles */
    .va-checkin-container {
        background: white;
        border-radius: 12px;
        padding: 1.5rem;
        margin-bottom: 1rem;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
    }
    
    .foodpanda-store {
        background: #FEF3E2;
        border: 1px solid #F59E0B;
        border-radius: 8px;
        padding: 1rem;
        margin-bottom: 0.5rem;
        display: flex;
        align-items: center;
        gap: 0.75rem;
    }
    
    .foodpanda-store input[type="checkbox"] {
        width: 18px;
        height: 18px;
        accent-color: #EF4444;
    }
    
    .bulk-actions {
        display: flex;
        gap: 0.5rem;
        margin-bottom: 1rem;
        flex-wrap: wrap;
    }
    
    .bulk-actions button {
        padding: 0.5rem 1rem;
        border-radius: 6px;
        border: 1px solid #E2E8F0;
        background: white;
        color: #64748B;
        font-size: 0.85rem;
        cursor: pointer;
        transition: all 0.2s;
    }
    
    .bulk-actions button:hover {
        background: #F8FAFC;
        border-color: #3B82F6;
        color: #3B82F6;
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
        
        .bulk-actions {
            flex-direction: column;
        }
        
        .bulk-actions button {
            width: 100%;
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

@st.cache_data(ttl=60)  # Cache for 1 minute
def load_stores_needing_attention() -> pd.DataFrame:
    """Load stores that need manual verification (existing functionality)"""
    try:
        return db.get_stores_needing_attention()
    except Exception as e:
        logger.error(f"Error loading stores needing attention: {e}")
        return pd.DataFrame()

def mark_store_status(store_id: int, store_name: str, is_online: bool, platform: str) -> bool:
    """Mark store as online/offline - saves as regular status check (existing functionality)"""
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
        return "🍔"
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
    """Main admin dashboard with tabs"""
    
    # Check email authentication
    if not check_admin_email_authentication():
        return
    
    # Show user info in sidebar
    with st.sidebar:
        st.markdown(f"**Admin logged in as:**\n{st.session_state.admin_email}")
        if st.button("Logout"):
            st.session_state.admin_authenticated = False
            st.session_state.admin_email = None
            st.rerun()
    
    current_time = config.get_current_time()
    
    # Header
    st.markdown(f"""
    <div class="admin-header">
        <div class="admin-title">🔧 CocoPan Admin Dashboard</div>
        <div class="admin-subtitle">Operations Management • {current_time.strftime('%I:%M %p')} Manila Time</div>
    </div>
    """, unsafe_allow_html=True)
    
    # Tab navigation
    tab1, tab2 = st.tabs(["🔧 Store Verification", "🐼 VA Hourly Check-in"])
    
    # TAB 1: Store Verification (existing functionality)
    with tab1:
        # Load stores needing attention
        stores_df = load_stores_needing_attention()
        
        st.markdown(f"""
        <div class="section-header" style="background:#fff; border:1px solid #E2E8F0; border-radius:8px; padding:.9rem 1.1rem; margin:1.1rem 0 .9rem 0; box-shadow:0 1px 3px rgba(0,0,0,.06);">
            <div style="font-size:1.1rem; font-weight:600; color:#1E293B; margin:0;">Manual Verification Required</div>
            <div style="font-size:.85rem; color:#64748B; margin:.25rem 0 0 0;">{len(stores_df)} stores need attention • Updated {current_time.strftime('%I:%M %p')} Manila Time</div>
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
        else:
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
                    <a href="{url}" target="_blank" class="btn-check" style="background: #3B82F6; color: white; text-decoration: none; display: block; text-align: center; padding: 0.75rem; border-radius: 8px;">
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
                if st.button("🔄 Check for New Issues", use_container_width=True, key="refresh_verification"):
                    st.session_state.completed_stores.clear()
                    load_stores_needing_attention.clear()
                    st.rerun()
    
    # TAB 2: VA Hourly Check-in (REDESIGNED WORKFLOW)
    with tab2:
        st.markdown(f"""
        <div class="section-header" style="background:#fff; border:1px solid #E2E8F0; border-radius:8px; padding:.9rem 1.1rem; margin:1.1rem 0 .9rem 0; box-shadow:0 1px 3px rgba(0,0,0,.06);">
            <div style="font-size:1.1rem; font-weight:600; color:#1E293B; margin:0;">🐼 VA Hourly Check-in</div>
            <div style="font-size:.85rem; color:#64748B; margin:.25rem 0 0 0;">Search and mark Foodpanda stores that are OFFLINE • {current_time.strftime('%I:%M %p')} Manila Time</div>
        </div>
        """, unsafe_allow_html=True)
        
        # Instructions
        st.markdown("""
        <div class="va-checkin-container">
            <h4 style="margin: 0 0 0.5rem 0; color: #1E293B;">📋 VA Workflow</h4>
            <ol style="margin: 0; color: #64748B; font-size: 0.9rem;">
                <li><strong>All stores start as ONLINE</strong> by default</li>
                <li>Check the <strong>Foodpanda Merchant Portal</strong> for offline stores</li>
                <li><strong>Search</strong> for each offline store using the search box below</li>
                <li>Click <strong>"Mark as OFFLINE"</strong> for stores that are closed</li>
                <li>Click <strong>"Submit Check-in"</strong> when finished</li>
            </ol>
        </div>
        """, unsafe_allow_html=True)
        
        # Load Foodpanda stores
        foodpanda_stores = load_foodpanda_stores()
        
        if not foodpanda_stores:
            st.error("❌ Failed to load Foodpanda stores. Please refresh the page.")
            return
        
        # Initialize session state for marked offline stores
        if 'va_offline_stores' not in st.session_state:
            st.session_state.va_offline_stores = set()
        if 'va_checkin_submitted' not in st.session_state:
            st.session_state.va_checkin_submitted = False
        
        # Status summary
        total_stores = len(foodpanda_stores)
        offline_count = len(st.session_state.va_offline_stores)
        online_count = total_stores - offline_count
        
        # Summary cards
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("🟢 Online", online_count, "Will stay online")
        with col2:
            st.metric("🔴 Marked Offline", offline_count, "Marked by VA")
        with col3:
            st.metric("📊 Total Foodpanda", total_stores, "All stores")
        
        st.markdown("---")
        
        # Search and mark interface
        st.markdown("### 🔍 Search & Mark Stores")
        
        # Search box
        search_term = st.text_input(
            "Search for store name:",
            placeholder="Type store name to search...",
            help="Search for stores by name to quickly find them"
        )
        
        # Filter stores based on search
        if search_term:
            filtered_stores = [
                store for store in foodpanda_stores 
                if search_term.lower() in store['name'].lower()
            ]
        else:
            filtered_stores = foodpanda_stores[:10]  # Show first 10 if no search
        
        if search_term and not filtered_stores:
            st.warning(f"No stores found matching '{search_term}'. Try a different search term.")
        
        # Show stores
        if filtered_stores:
            if search_term:
                st.info(f"Found {len(filtered_stores)} stores matching '{search_term}'")
            else:
                st.info(f"Showing first 10 stores. Use search to find specific stores.")
            
            for store in filtered_stores:
                store_id = store['id']
                store_name = store['name']
                store_url = store['url']
                
                if store_id is None:
                    continue
                
                # Clean store name for display
                display_name = store_name.replace('Cocopan - ', '').replace('Cocopan ', '')
                
                # Check current status
                is_marked_offline = store_id in st.session_state.va_offline_stores
                
                # Store row
                st.markdown(f"""
                <div class="foodpanda-store" style="background: {'#FEE2E2' if is_marked_offline else '#F0FDF4'}; border-color: {'#EF4444' if is_marked_offline else '#22C55E'};">
                    <div style="flex: 1;">
                        <strong>{display_name}</strong>
                        <br>
                        <small style="color: #64748B;">Status: {'🔴 MARKED OFFLINE' if is_marked_offline else '🟢 ONLINE'}</small>
                    </div>
                </div>
                """, unsafe_allow_html=True)
                
                # Action buttons
                col1, col2, col3 = st.columns([2, 1, 1])
                
                with col1:
                    st.markdown(f"""
                    <a href="{store_url}" target="_blank" style="display: inline-block; padding: 0.4rem 0.8rem; background: #3B82F6; color: white; text-decoration: none; border-radius: 6px; font-size: 0.8rem; text-align: center;">
                        🔗 Check Store
                    </a>
                    """, unsafe_allow_html=True)
                
                with col2:
                    if is_marked_offline:
                        if st.button(f"✅ Mark ONLINE", key=f"online_{store_id}", use_container_width=True):
                            st.session_state.va_offline_stores.discard(store_id)
                            st.success(f"✅ {display_name} marked as ONLINE")
                            st.rerun()
                    else:
                        st.write("")  # Empty space when online
                
                with col3:
                    if not is_marked_offline:
                        if st.button(f"🔴 Mark OFFLINE", key=f"offline_{store_id}", use_container_width=True, type="primary"):
                            st.session_state.va_offline_stores.add(store_id)
                            st.success(f"🔴 {display_name} marked as OFFLINE")
                            st.rerun()
                    else:
                        st.write("OFFLINE ✓")
                
                st.markdown("<br>", unsafe_allow_html=True)
        
        # Show currently marked offline stores if any
        if st.session_state.va_offline_stores:
            st.markdown("### 📋 Currently Marked Offline")
            
            offline_stores_info = []
            for store in foodpanda_stores:
                if store['id'] in st.session_state.va_offline_stores:
                    display_name = store['name'].replace('Cocopan - ', '').replace('Cocopan ', '')
                    offline_stores_info.append(display_name)
            
            if offline_stores_info:
                for i, store_name in enumerate(offline_stores_info, 1):
                    st.write(f"{i}. 🔴 {store_name}")
        
        # Submit section
        st.markdown("---")
        st.markdown("### 📤 Submit Hourly Check-in")
        
        if offline_count == 0:
            st.info("ℹ️ No stores marked as offline. All Foodpanda stores will be saved as ONLINE.")
        else:
            st.warning(f"⚠️ {offline_count} stores will be marked as OFFLINE. {online_count} stores will remain ONLINE.")
        
        # Submit button
        if st.button("📤 Submit Hourly Check-in", use_container_width=True, type="primary"):
            offline_store_ids = list(st.session_state.va_offline_stores)
            
            # Save to database
            success = save_va_checkin(offline_store_ids, st.session_state.admin_email)
            
            if success:
                st.success(f"""
                ✅ **Hourly check-in submitted successfully!**
                
                📊 **Summary:**
                - 🟢 Online: {online_count} stores
                - 🔴 Offline: {offline_count} stores
                - 👤 Submitted by: {st.session_state.admin_email}
                - 🕐 Time: {current_time.strftime('%I:%M %p')} Manila Time
                """)
                
                # Clear selections after successful submit
                st.session_state.va_offline_stores = set()
                st.session_state.va_checkin_submitted = True
                
                # Clear cache to refresh data
                load_foodpanda_stores.clear()
                
            else:
                st.error("❌ Failed to submit check-in. Please try again.")
        
        # Reset button
        if st.session_state.va_offline_stores:
            if st.button("🔄 Reset All to Online", use_container_width=True):
                st.session_state.va_offline_stores = set()
                st.info("ℹ️ All stores reset to ONLINE status")
                st.rerun()
    
    # Refresh button (always visible)
    st.markdown("---")
    col1, col2, col3 = st.columns([1, 2, 1])
    with col2:
        if st.button("🔄 Refresh All Data", use_container_width=True):
            load_stores_needing_attention.clear()
            load_foodpanda_stores.clear()
            st.rerun()

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        st.error(f"❌ System Error: {e}")
        logger.error(f"Admin dashboard error: {e}")