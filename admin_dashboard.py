#!/usr/bin/env python3
"""
CocoPan Admin Dashboard - Enhanced with EMAIL AUTH & VA HOURLY CHECK-IN
- Email-only authentication using admin_alerts.json
- NEW: VA Hourly Check-in tab with hour-slot tracking (7–10 AM Manila)
- Existing admin verification functionality (unchanged)
- Mobile-first design maintained
"""
import streamlit as st
import pandas as pd
import os
import time
import logging
import json
from datetime import datetime, timedelta
from typing import List, Dict, Any

# Third-party
import pytz

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

# ---------------------------
# EMAIL AUTHENTICATION
# ---------------------------
def load_authorized_admin_emails():
    """Load authorized admin emails from admin_alerts.json"""
    try:
        with open('admin_alerts.json', 'r') as f:
            data = json.load(f)
            
        # Extract admin emails
        admin_team = data.get('admin_team', {})
        if admin_team.get('enabled', False):
            emails = admin_team.get('emails', [])
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

# ---------------------------
# FOODPANDA STORE LOADING
# ---------------------------
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

# ---------------------------
# EXISTING ADMIN ACTIONS
# ---------------------------
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

# ---------------------------
# ENHANCED VA CHECK-IN (NEW)
# ---------------------------
def get_va_checkin_schedule():
    """Get the VA check-in schedule (7-10 AM Manila)"""
    return {
        'start_hour': 7,
        'end_hour': 10,
        'timezone': 'Asia/Manila',
        'reminder_minutes_before': 5
    }

def get_current_manila_time():
    """Get current Manila time"""
    manila_tz = pytz.timezone('Asia/Manila')
    return datetime.now(manila_tz)

def get_current_hour_slot():
    """Get current hour slot for check-in (e.g., 2025-09-09 08:00:00+08:00)"""
    now = get_current_manila_time()
    return now.replace(minute=0, second=0, microsecond=0)

def is_checkin_time():
    """Check if it's currently check-in time (7-10 AM Manila inclusive)"""
    schedule = get_va_checkin_schedule()
    current_hour = get_current_manila_time().hour
    return schedule['start_hour'] <= current_hour <= schedule['end_hour']

def get_next_checkin_time():
    """Get next check-in time (Manila)"""
    schedule = get_va_checkin_schedule()
    now = get_current_manila_time()
    current_hour = now.hour
    
    # If before 7 AM, next is 7 AM today
    if current_hour < schedule['start_hour']:
        next_checkin = now.replace(hour=schedule['start_hour'], minute=0, second=0, microsecond=0)
    # If during 7-10 AM, next is next hour (or 7 AM tomorrow if after 10)
    elif current_hour <= schedule['end_hour']:
        if current_hour == schedule['end_hour']:
            # After 10 AM, next is 7 AM tomorrow
            next_checkin = (now + timedelta(days=1)).replace(hour=schedule['start_hour'], minute=0, second=0, microsecond=0)
        else:
            # Next hour today
            next_checkin = now.replace(hour=current_hour + 1, minute=0, second=0, microsecond=0)
    else:
        # After 10 AM, next is 7 AM tomorrow
        next_checkin = (now + timedelta(days=1)).replace(hour=schedule['start_hour'], minute=0, second=0, microsecond=0)
    
    return next_checkin

def check_if_hour_already_completed(hour_slot):
    """Check if VA check-in already completed for this hour"""
    try:
        with db.get_connection() as conn:
            cursor = conn.cursor()
            
            # Look for VA check-in entries in this hour slot
            cursor.execute("""
                SELECT COUNT(*) FROM status_checks 
                WHERE error_message LIKE '[VA_CHECKIN]%%' 
                  AND checked_at >= %s
                  AND checked_at < %s + INTERVAL '1 hour'
            """, (hour_slot, hour_slot))
            
            count = cursor.fetchone()[0]
            return count > 0
            
    except Exception as e:
        logger.error(f"Error checking completed hours: {e}")
        return False

def get_completed_hours_today():
    """Get list of hours already completed today (Manila hours as ints)"""
    try:
        with db.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT DISTINCT EXTRACT(HOUR FROM checked_at AT TIME ZONE 'Asia/Manila') AS hour
                FROM status_checks 
                WHERE error_message LIKE '[VA_CHECKIN]%%'
                  AND DATE(checked_at AT TIME ZONE 'Asia/Manila') = CURRENT_DATE
                ORDER BY hour
            """)
            hours = [int(row[0]) for row in cursor.fetchall()]
            return hours
    except Exception as e:
        logger.error(f"Error getting completed hours: {e}")
        return []

def save_va_checkin_enhanced(offline_store_ids: List[int], admin_email: str, hour_slot: datetime) -> bool:
    """Enhanced VA check-in save with hour tracking (sets checked_at to the hour slot)"""
    try:
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
                
                # Create error message to mark this as VA check-in with hour info
                hour_str = hour_slot.strftime('%Y-%m-%d %H:00')
                if is_online:
                    error_message = f"[VA_CHECKIN] {hour_str} - Store online via {admin_email}"
                else:
                    error_message = f"[VA_CHECKIN] {hour_str} - Store offline via {admin_email}"
                
                # Save to database (initial insert with current timestamp)
                success = db.save_status_check(
                    store_id=store_id,
                    is_online=is_online,
                    response_time_ms=1000,  # Fake response time
                    error_message=error_message
                )
                
                # Update the timestamp to the exact hour slot for precise tracking
                if success:
                    with db.get_connection() as conn:
                        cursor = conn.cursor()
                        cursor.execute("""
                            UPDATE status_checks 
                               SET checked_at = %s 
                             WHERE id = (
                                   SELECT id FROM status_checks 
                                    WHERE store_id = %s 
                                 ORDER BY checked_at DESC 
                                    LIMIT 1
                             )
                        """, (hour_slot, store_id))
                        conn.commit()
                    
                    success_count += 1
                else:
                    error_count += 1
                    
            except Exception as e:
                logger.error(f"Error saving VA check-in for store {store_id}: {e}")
                error_count += 1
        
        logger.info(f"✅ VA Check-in for {hour_slot.strftime('%H:00')} saved: {success_count} stores, {error_count} errors")
        return error_count == 0
        
    except Exception as e:
        logger.error(f"❌ Failed to save enhanced VA check-in: {e}")
        return False

def enhanced_va_checkin_tab():
    """Enhanced VA Check-in tab with hour tracking UI"""
    current_time = get_current_manila_time()
    current_hour_slot = get_current_hour_slot()
    schedule = get_va_checkin_schedule()
    
    st.markdown(f"""
    <div class="section-header" style="background:#fff; border:1px solid #E2E8F0; border-radius:8px; padding:.9rem 1.1rem; margin:1.1rem 0 .9rem 0; box-shadow:0 1px 3px rgba(0,0,0,.06);">
        <div style="font-size:1.1rem; font-weight:600; color:#1E293B; margin:0;">🐼 VA Hourly Check-in</div>
        <div style="font-size:.85rem; color:#64748B; margin:.25rem 0 0 0;">Foodpanda Store Status • {current_time.strftime('%I:%M %p')} Manila Time</div>
    </div>
    """, unsafe_allow_html=True)
    
    # Check if it's check-in time
    if not is_checkin_time():
        next_checkin = get_next_checkin_time()
        time_until = next_checkin - current_time
        hours_until = int(time_until.total_seconds() // 3600)
        minutes_until = int((time_until.total_seconds() % 3600) // 60)
        
        st.warning(f"""
⏰ **Not Check-in Time**

Check-in hours: {schedule['start_hour']}:00 AM - {schedule['end_hour']}:00 AM Manila Time  
Next check-in: {next_checkin.strftime('%I:%M %p')} ({hours_until}h {minutes_until}m)
        """)
        return
    
    # Check if current hour already completed
    hour_completed = check_if_hour_already_completed(current_hour_slot)
    completed_hours = get_completed_hours_today()
    
    if hour_completed:
        next_hour = current_hour_slot + timedelta(hours=1)
        if next_hour.hour <= schedule['end_hour']:
            wait_until = next_hour - timedelta(minutes=5)  # Show 5 minutes before
            time_until = wait_until - current_time
            minutes_until = max(0, int(time_until.total_seconds() // 60))
            
            st.success(f"""
✅ **{current_hour_slot.strftime('%I:00 %p')} Check-in Already Completed!**

Next check-in: {next_hour.strftime('%I:00 %p')}  
Available in: {minutes_until} minutes
            """)
        else:
            st.success(f"""
✅ **{current_hour_slot.strftime('%I:00 %p')} Check-in Completed!**

All check-ins done for today. Next check-in: Tomorrow 7:00 AM
            """)
        
        # Show today's completion status
        st.markdown("### 📊 Today's Check-in Status")
        col1, col2, col3, col4 = st.columns(4)
        for i, hour in enumerate(range(7, 11)):
            col = [col1, col2, col3, col4][i]
            status = "✅ Done" if hour in completed_hours else "⏳ Pending"
            col.metric(f"{hour}:00 AM", status)
        return
    
    # Current hour check-in interface
    st.markdown(f"""
    <div style="background: #FEF3E2; border: 1px solid #F59E0B; border-radius: 8px; padding: 1rem; margin: 1rem 0;">
        <h4 style="margin: 0 0 0.5rem 0; color: #92400E;">🕐 {current_hour_slot.strftime('%I:00 %p')} Check-in</h4>
        <p style="margin: 0; color: #92400E;">Please complete the check-in for this hour</p>
    </div>
    """, unsafe_allow_html=True)
    
    # Load Foodpanda stores and session state
    foodpanda_stores = load_foodpanda_stores()
    if not foodpanda_stores:
        st.error("❌ Failed to load Foodpanda stores. Please refresh the page.")
        return
    
    if 'va_offline_stores' not in st.session_state:
        st.session_state.va_offline_stores = set()
    if 'va_checkin_submitted' not in st.session_state:
        st.session_state.va_checkin_submitted = False
    
    # Status summary
    total_stores = len(foodpanda_stores)
    offline_count = len(st.session_state.va_offline_stores)
    online_count = total_stores - offline_count
    
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("🟢 Online", online_count, "Will stay online")
    with col2:
        st.metric("🔴 Marked Offline", offline_count, "Marked by VA")
    with col3:
        st.metric("📊 Total Foodpanda", total_stores, "All stores")
    
    st.markdown("---")
    
    # Search & mark
    st.markdown("### 🔍 Search & Mark Stores")
    search_term = st.text_input(
        "Search for store name:",
        placeholder="Type store name to search...",
        help="Search for stores by name to quickly find them"
    )
    if search_term:
        filtered_stores = [s for s in foodpanda_stores if search_term.lower() in s['name'].lower()]
    else:
        filtered_stores = foodpanda_stores[:10]
    
    if search_term and not filtered_stores:
        st.warning(f"No stores found matching '{search_term}'. Try a different search term.")
    
    if filtered_stores:
        if search_term:
            st.info(f"Found {len(filtered_stores)} stores matching '{search_term}'")
        else:
            st.info("Showing first 10 stores. Use search to find specific stores.")
        
        for store in filtered_stores:
            store_id = store['id']
            store_name = store['name']
            store_url = store['url']
            if store_id is None:
                continue
            
            display_name = store_name.replace('Cocopan - ', '').replace('Cocopan ', '')
            is_marked_offline = store_id in st.session_state.va_offline_stores
            
            st.markdown(f"""
            <div class="foodpanda-store" style="background: {'#FEE2E2' if is_marked_offline else '#F0FDF4'}; border: 1px solid {'#EF4444' if is_marked_offline else '#22C55E'}; border-radius: 8px; padding: 1rem; margin-bottom: 0.5rem; display: flex; align-items: center; gap: 0.75rem;">
                <div style="flex: 1;">
                    <strong>{display_name}</strong><br>
                    <small style="color: #64748B;">Status: {'🔴 MARKED OFFLINE' if is_marked_offline else '🟢 ONLINE'}</small>
                </div>
            </div>
            """, unsafe_allow_html=True)
            
            c1, c2, c3 = st.columns([2, 1, 1])
            with c1:
                st.markdown(f"""
                <a href="{store_url}" target="_blank" style="display: inline-block; padding: 0.4rem 0.8rem; background: #3B82F6; color: white; text-decoration: none; border-radius: 6px; font-size: 0.8rem; text-align: center;">
                    🔗 Check Store
                </a>
                """, unsafe_allow_html=True)
            with c2:
                if is_marked_offline:
                    if st.button("✅ Mark ONLINE", key=f"online_{store_id}", use_container_width=True):
                        st.session_state.va_offline_stores.discard(store_id)
                        st.success(f"✅ {display_name} marked as ONLINE")
                        st.rerun()
                else:
                    st.write("")
            with c3:
                if not is_marked_offline:
                    if st.button("🔴 Mark OFFLINE", key=f"offline_{store_id}", use_container_width=True, type="primary"):
                        st.session_state.va_offline_stores.add(store_id)
                        st.success(f"🔴 {display_name} marked as OFFLINE")
                        st.rerun()
                else:
                    st.write("OFFLINE ✓")
            st.markdown("<br>", unsafe_allow_html=True)
    
    # Currently marked offline list
    if st.session_state.va_offline_stores:
        st.markdown("### 📋 Currently Marked Offline")
        offline_names = []
        for s in foodpanda_stores:
            if s['id'] in st.session_state.va_offline_stores:
                offline_names.append(s['name'].replace('Cocopan - ', '').replace('Cocopan ', ''))
        for i, name in enumerate(offline_names, 1):
            st.write(f"{i}. 🔴 {name}")
    
    # Submit section (ENHANCED: saves with hour_slot)
    st.markdown("---")
    st.markdown("### 📤 Submit Hourly Check-in")
    if offline_count == 0:
        st.info("ℹ️ No stores marked as offline. All Foodpanda stores will be saved as ONLINE.")
    else:
        st.warning(f"⚠️ {offline_count} stores will be marked as OFFLINE. {online_count} stores will remain ONLINE.")
    
    if st.button(f"📤 Submit Check-in for {current_hour_slot.strftime('%I:00 %p')}", use_container_width=True, type="primary"):
        offline_store_ids = list(st.session_state.va_offline_stores)
        success = save_va_checkin_enhanced(offline_store_ids, st.session_state.admin_email, current_hour_slot)
        if success:
            st.success(f"""
✅ **{current_hour_slot.strftime('%I:00 %p')} Check-in Submitted Successfully!**

📊 **Summary:**
- 🟢 Online: {online_count} stores
- 🔴 Offline: {offline_count} stores
- 👤 Submitted by: {st.session_state.admin_email}
- 🕐 Time Slot: {current_hour_slot.strftime('%I:00 %p')} Manila Time

Next check-in will be available at {(current_hour_slot + timedelta(hours=1) - timedelta(minutes=5)).strftime('%I:%M %p')}
            """)
            # Clear selections and refresh
            st.session_state.va_offline_stores = set()
            load_foodpanda_stores.clear()
            st.rerun()
        else:
            st.error("❌ Failed to submit check-in. Please try again.")

# ---------------------------
# STYLES
# ---------------------------
# MOBILE-FIRST CSS (unchanged, plus classes used by enhanced tab)
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    .stDeployButton {display: none;}
    header {visibility: hidden;}
    .main { font-family: 'Inter', sans-serif; background: #F8FAFC; color: #1E293B; padding: 1rem !important; }
    .admin-header { background: linear-gradient(135deg, #DC2626 0%, #EF4444 100%); border-radius: 12px; padding: 1.5rem; margin-bottom: 1.5rem; text-align: center; color: white; }
    .admin-title { font-size: 1.8rem; font-weight: 700; margin: 0; }
    .admin-subtitle { font-size: 0.9rem; margin: 0.5rem 0 0 0; opacity: 0.9; }
    .store-card { background: white; border-radius: 12px; padding: 1.5rem; margin-bottom: 1rem; border-left: 4px solid #F59E0B; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }
    .store-name { font-size: 1.1rem; font-weight: 600; color: #1E293B; margin-bottom: 0.5rem; }
    .store-meta { font-size: 0.85rem; color: #64748B; margin-bottom: 1rem; }
    .store-platform { display: inline-block; background: #E0F2FE; color: #0C4A6E; padding: 0.2rem 0.5rem; border-radius: 4px; font-size: 0.75rem; font-weight: 500; margin-right: 0.5rem; }
    .action-buttons { display: flex; gap: 0.75rem; margin-top: 1rem; }
    .btn-check { flex: 1; background: white; color: white; border: none; border-radius: 8px; padding: 0.75rem; font-weight: 500; font-size: 0.9rem; cursor: pointer; text-decoration: none; text-align: center; display: block; transition: all 0.2s; }
    .btn-check:hover { background: #2563EB; transform: translateY(-1px); }
    .btn-online, .btn-offline { flex: 1; border: none; border-radius: 8px; padding: 0.75rem; font-weight: 600; font-size: 0.9rem; cursor: pointer; transition: all 0.2s; min-height: 48px; }
    .btn-online { background: #10B981; color: white; }
    .btn-online:hover { background: #059669; transform: translateY(-1px); }
    .btn-offline { background: #EF4444; color: white; }
    .btn-offline:hover { background: #DC2626; transform: translateY(-1px); }
    .progress-container { background: #E2E8F0; border-radius: 8px; height: 8px; margin: 1rem 0; overflow: hidden; }
    .progress-bar { background: linear-gradient(90deg, #10B981, #059669); height: 100%; transition: width 0.3s ease; }
    .progress-text { text-align: center; font-size: 0.9rem; color: #64748B; margin: 0.5rem 0; }
    .success-message { background: #D1FAE5; color: #065F46; padding: 1rem; border-radius: 8px; margin: 1rem 0; border-left: 4px solid #10B981; }
    .error-message { background: #FEE2E2; color: #991B1B; padding: 1rem; border-radius: 8px; margin: 1rem 0; border-left: 4px solid #EF4444; }
    .va-checkin-container { background: white; border-radius: 12px; padding: 1.5rem; margin-bottom: 1rem; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }
    .block-container { padding-top: 1rem; padding-bottom: 1rem; }
    .stButton > button { width: 100%; border-radius: 8px; border: none; font-weight: 600; font-size: 0.9rem; padding: 0.75rem; transition: all 0.2s; min-height: 48px; }
    .stButton > button:hover { transform: translateY(-1px); box-shadow: 0 4px 8px rgba(0,0,0,0.2); }
    @media (max-width: 768px) {
        .admin-title { font-size: 1.5rem; }
        .store-card { padding: 1rem; }
        .action-buttons { flex-direction: column; }
        .btn-check, .btn-online, .btn-offline { min-height: 52px; font-size: 1rem; }
    }
</style>
""", unsafe_allow_html=True)

# ---------------------------
# MAIN
# ---------------------------
def main():
    """Main admin dashboard with tabs"""
    # Auth
    if not check_admin_email_authentication():
        return
    
    # Sidebar
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
    
    # Tabs
    tab1, tab2 = st.tabs(["🔧 Store Verification", "🐼 VA Hourly Check-in"])
    
    # TAB 1: Store Verification (existing)
    with tab1:
        stores_df = load_stores_needing_attention()
        st.markdown(f"""
        <div class="section-header" style="background:#fff; border:1px solid #E2E8F0; border-radius:8px; padding:.9rem 1.1rem; margin:1.1rem 0 .9rem 0; box-shadow:0 1px 3px rgba(0,0,0,.06);">
            <div style="font-size:1.1rem; font-weight:600; color:#1E293B; margin:0;">Manual Verification Required</div>
            <div style="font-size:.85rem; color:#64748B; margin:.25rem 0 0 0;">{len(stores_df)} stores need attention • Updated {current_time.strftime('%I:%M %p')} Manila Time</div>
        </div>
        """, unsafe_allow_html=True)
        
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
            if 'completed_stores' not in st.session_state:
                st.session_state.completed_stores = set()
            total_stores = len(stores_df)
            completed_count = len(st.session_state.completed_stores)
            progress = completed_count / total_stores if total_stores > 0 else 0
            
            st.markdown(f"""
            <div class="progress-container"><div class="progress-bar" style="width: {progress * 100:.1f}%;"></div></div>
            <div class="progress-text">Progress: {completed_count}/{total_stores} completed</div>
            """, unsafe_allow_html=True)
            
            for idx, store in stores_df.iterrows():
                store_id = store['id']
                store_name = store['name']
                platform = store.get('platform', 'unknown')
                url = store['url']
                checked_at = store.get('checked_at')
                if store_id in st.session_state.completed_stores:
                    continue
                
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
                
                c1, c2, c3 = st.columns([2, 1, 1])
                with c1:
                    st.markdown(f"""
                    <a href="{url}" target="_blank" class="btn-check" style="background: #3B82F6; color: white; text-decoration: none; display: block; text-align: center; padding: 0.75rem; border-radius: 8px;">
                        🔗 Check Store
                    </a>
                    """, unsafe_allow_html=True)
                with c2:
                    if st.button("✅ Online", key=f"online_{store_id}", use_container_width=True):
                        if mark_store_status(store_id, store_name, True, platform):
                            st.session_state.completed_stores.add(store_id)
                            st.success(f"✅ {store_name} marked as **Online**")
                            time.sleep(1)
                            st.rerun()
                        else:
                            st.error("❌ Failed to update store status. Please try again.")
                with c3:
                    if st.button("❌ Offline", key=f"offline_{store_id}", use_container_width=True):
                        if mark_store_status(store_id, store_name, False, platform):
                            st.session_state.completed_stores.add(store_id)
                            st.success(f"❌ {store_name} marked as **Offline**")
                            time.sleep(1)
                            st.rerun()
                        else:
                            st.error("❌ Failed to update store status. Please try again.")
                st.markdown("<br>", unsafe_allow_html=True)
            
            remaining = total_stores - completed_count
            if remaining > 0:
                st.info(f"📋 {remaining} stores remaining for verification")
            else:
                st.success("🎉 All stores verified! Great work!")
                if st.button("🔄 Check for New Issues", use_container_width=True, key="refresh_verification"):
                    st.session_state.completed_stores.clear()
                    load_stores_needing_attention.clear()
                    st.rerun()
    
    # TAB 2: Enhanced VA Hourly Check-in
    with tab2:
        enhanced_va_checkin_tab()
    
    # Global refresh
    st.markdown("---")
    c1, c2, c3 = st.columns([1, 2, 1])
    with c2:
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
