#!/usr/bin/env python3
"""
CocoPan Client Alert System
Sends email ONLY when stores newly go offline (immediate alerts only).
No hourly updates, no spam - just critical offline notifications.
"""

import json
import logging
import smtplib
import ssl
from email.utils import format_datetime
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from pathlib import Path
from dataclasses import dataclass
from typing import List, Dict, Any, Optional
from datetime import datetime

# Timezone
try:
    from zoneinfo import ZoneInfo
    MANILA_TZ = ZoneInfo("Asia/Manila")
except:
    import pytz
    MANILA_TZ = pytz.timezone("Asia/Manila")

def now_mnl():
    return datetime.now(MANILA_TZ)

def fmt_mnl(dt=None):
    if dt is None:
        dt = now_mnl()
    return dt.strftime('%B %d, %Y • %I:%M %p Manila Time')

from config import config

logging.basicConfig(level=getattr(logging, config.LOG_LEVEL))
logger = logging.getLogger(__name__)

DASHBOARD_URL = "https://cocopanwatchtower.com/"


# ------------------------
# DATA MODEL
# ------------------------
@dataclass
class StoreAlert:
    name: str
    platform: str   # GrabFood / Foodpanda
    status: str     # OFFLINE / ONLINE
    location: str = ""
    last_check: datetime = None


# ------------------------
# ALERT MANAGER
# ------------------------
class ClientAlertManager:

    def __init__(self):
        self.config_file = "client_alerts.json"
        self.config = self._load_config()
        if not self.config:
            logger.warning("⚠️ client_alerts.json missing or empty")
        else:
            logger.info("✅ Client Alert System loaded (immediate alerts only)")

    def _load_config(self):
        try:
            path = Path(self.config_file)
            if not path.exists():
                return {}
            return json.loads(path.read_text())
        except Exception as e:
            logger.error(f"Failed to load config: {e}")
            return {}

    # -----------------------------
    # SEND THE EMAIL
    # -----------------------------
    def _send_email(self, subject, text_body, html_body, recipients: List[str]):
        try:
            msg = MIMEMultipart("alternative")
            msg["Subject"] = subject
            msg["From"] = f"CocoPan Operations <{config.FROM_EMAIL}>"
            msg["To"] = ", ".join(recipients)
            msg["Date"] = format_datetime(now_mnl())

            msg.attach(MIMEText(text_body, "plain"))
            msg.attach(MIMEText(html_body, "html"))

            ctx = ssl.create_default_context()
            with smtplib.SMTP(config.SMTP_SERVER, config.SMTP_PORT) as s:
                if config.SMTP_USE_TLS:
                    s.starttls(context=ctx)
                s.login(config.SMTP_USERNAME, config.SMTP_PASSWORD)
                s.send_message(msg)

            logger.info(f"📧 Sent offline alert to {recipients}")
            return True

        except Exception as e:
            logger.error(f"❌ Email failed: {e}")
            return False

    # -----------------------------
    # MAIN ALERT FUNCTION
    # -----------------------------
    def _send_offline_alert(self, offline_stores: List[StoreAlert]) -> bool:
        """Internal method to send the offline alert email"""
        
        # 1. No offline stores = no email
        if not offline_stores:
            logger.debug("No offline stores - skipping email")
            return False

        # 2. Get recipients from config
        team = self.config.get("clients", {}).get("cocopan_ops", {})
        if not team.get("enabled", False):
            logger.warning("⚠️ cocopan_ops team not enabled in config")
            return False

        recipients = team.get("emails", [])
        if not recipients:
            logger.warning("⚠️ No email recipients configured")
            return False

        # 3. Clean store names
        cleaned = [
            s.name.replace("Cocopan", "").replace("-", "").strip()
            for s in offline_stores
        ]

        timestamp = fmt_mnl()
        count = len(cleaned)

        # 4. Build email - YOUR EXACT FORMAT
        subject = f"CocoPan Alert — {count} Store(s) Offline"
        
        text_body = f"""CocoPan Offline Store Alert
{timestamp}

The following {count} store(s) are currently offline:

""" + "\n".join([f"🔴 {name}" for name in cleaned]) + f"""

Check live status anytime at:
{DASHBOARD_URL}

This is an automated alert from CocoPan WatchTower.
"""

        html_body = f"""
<html>
<body style="font-family: Arial; max-width:600px; margin:auto; padding:20px;">

<h2 style="margin:0; color:#b30000;">CocoPan Offline Store Alert</h2>
<p style="color:#555; margin-top:4px;">{timestamp}</p>

<h3>{count} store(s) currently offline:</h3>
<ul style="padding-left:0; list-style:none;">
"""

        for name in cleaned:
            html_body += f"""
<li style="padding:6px 0; border-bottom:1px solid #eee;">
    <span style="color:#b30000; font-weight:bold;">●</span> {name}
</li>
"""

        html_body += f"""
</ul>

<div style="text-align:center; margin:30px 0;">
    <a href="{DASHBOARD_URL}"
       style="background:#007bff; color:white; padding:12px 22px; text-decoration:none; border-radius:6px;">
       View Live Dashboard
    </a>
</div>

<p style="font-size:12px; color:#999; text-align:center;">
This is an automated alert from CocoPan WatchTower.
</p>

</body>
</html>
"""

        # 5. Send email
        return self._send_email(subject, text_body, html_body, recipients)

    # -----------------------------
    # PUBLIC API (for monitor_service.py)
    # -----------------------------
    
    def test_email_system(self) -> bool:
        """
        Test email system on startup
        Returns True if email system is properly configured
        """
        logger.info("🧪 Testing client email system...")
        
        # Check if config is loaded
        if not self.config:
            logger.warning("⚠️ No client_alerts.json config found")
            return False
        
        # Check if team is configured
        team = self.config.get("clients", {}).get("cocopan_ops", {})
        if not team.get("enabled", False):
            logger.warning("⚠️ cocopan_ops team not enabled")
            return False
        
        recipients = team.get("emails", [])
        if not recipients:
            logger.warning("⚠️ No email recipients configured")
            return False
        
        logger.info(f"✅ Email system configured - recipients: {recipients}")
        return True

    def send_immediate_offline_alert(self, offline_stores: List[StoreAlert], total_stores: int) -> bool:
        """
        ✅ SEND EMAIL - Called when stores NEWLY go offline
        This is the ONLY method that actually sends emails
        """
        if not offline_stores:
            logger.debug("No newly offline stores - skipping immediate alert")
            return False
        
        logger.info(f"🚨 IMMEDIATE ALERT: {len(offline_stores)} store(s) just went offline!")
        return self._send_offline_alert(offline_stores)

    def send_hourly_status_alert(self, offline_stores: List[StoreAlert], total_stores: int) -> bool:
        """
        ❌ NO EMAIL - Called every hour (we ignore this to avoid spam)
        Just log and return True
        """
        if offline_stores:
            logger.info(f"📊 Hourly update: {len(offline_stores)}/{total_stores} stores offline (no email sent - immediate alerts only)")
        else:
            logger.info(f"📊 Hourly update: All {total_stores} stores online ✅")
        
        # Return True so monitor_service thinks it worked, but we don't send email
        return True


# GLOBAL INSTANCE
client_alerts = ClientAlertManager()