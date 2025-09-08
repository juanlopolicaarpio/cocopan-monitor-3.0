#!/usr/bin/env python3
"""
CocoPan Client Alert System
Sends business-friendly notifications to operations teams about store status
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
from datetime import datetime, timedelta

# --- Timezone utilities ---
try:
    from zoneinfo import ZoneInfo
    MANILA_TZ = ZoneInfo("Asia/Manila")
except Exception:
    import pytz
    MANILA_TZ = pytz.timezone("Asia/Manila")

def now_mnl() -> datetime:
    return datetime.now(MANILA_TZ)

def fmt_mnl(dt: Optional[datetime] = None) -> str:
    if dt is None:
        dt = now_mnl()
    return dt.strftime('%B %d, %Y • %I:%M %p Manila Time')

from config import config

# --- Logger setup ---
logging.basicConfig(level=getattr(logging, config.LOG_LEVEL))
logger = logging.getLogger(__name__)

@dataclass
class StoreAlert:
    """Store information for client alerts"""
    name: str
    platform: str
    status: str  # ONLINE/OFFLINE
    location: str = ""
    last_check: datetime = None

class ClientAlertManager:
    """Manages client alerts for store status updates"""

    def __init__(self):
        self.config_file = "client_alerts.json"
        self.last_alerts_file = Path(".last_client_alerts.json")
        self.config = self._load_config()
        self.last_alerts: Dict[str, datetime] = {}
        self._load_last_alerts()

        if not self.config:
            logger.warning("⚠️ Client alerts disabled - no config found")
            return

        logger.info("✅ Client Alert System initialized")

    def _load_config(self) -> Dict[str, Any]:
        """Load client alert configuration"""
        try:
            config_path = Path(self.config_file)
            if not config_path.exists():
                logger.warning(f"Client alert config not found: {self.config_file}")
                return {}
            with open(config_path) as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Failed to load client config: {e}")
            return {}

    def _save_last_alerts(self) -> None:
        try:
            data = {k: v.isoformat() for k, v in self.last_alerts.items()}
            self.last_alerts_file.write_text(json.dumps(data))
        except Exception as e:
            logger.error(f"Failed to persist client alerts: {e}")

    def _load_last_alerts(self) -> None:
        try:
            if self.last_alerts_file.exists():
                raw = json.loads(self.last_alerts_file.read_text())
                for k, iso in raw.items():
                    try:
                        dt = datetime.fromisoformat(iso)
                        if dt.tzinfo is None:
                            dt = dt.replace(tzinfo=MANILA_TZ)
                        else:
                            dt = dt.astimezone(MANILA_TZ)
                        self.last_alerts[k] = dt
                    except Exception:
                        continue
        except Exception as e:
            logger.error(f"Failed to load client alerts: {e}")

    def _should_send_alert(self, alert_type: str, client_id: str = "") -> bool:
        """Check if alert should be sent with cooldown logic"""
        if not self.config:
            return False

        key = f"{client_id}_{alert_type}" if client_id else alert_type
        
        # Default cooldown - for hourly checks, minimum 30 minutes between same alerts
        cooldown_minutes = 30
        
        if alert_type == "hourly_status":
            cooldown_minutes = 45  # Allow one alert per hour cycle
        elif alert_type == "critical_offline":
            cooldown_minutes = 15  # More frequent for critical issues

        last_sent = self.last_alerts.get(key)
        if last_sent:
            time_since = now_mnl() - last_sent
            if time_since < timedelta(minutes=cooldown_minutes):
                return False

        return True

    def _send_email(self, subject: str, body: str, html_body: str, recipients: List[str]) -> bool:
        """Send client email using SMTP config"""
        try:
            if not config.SMTP_USERNAME or not config.SMTP_PASSWORD:
                logger.error("SMTP credentials not configured")
                return False

            if not recipients:
                logger.error("No recipients specified")
                return False

            # Create message
            msg = MIMEMultipart('alternative')
            msg['Subject'] = subject
            msg['From'] = f"CocoPan Operations <{config.FROM_EMAIL}>"
            msg['To'] = ', '.join(recipients)
            msg['Date'] = format_datetime(now_mnl())

            # Add text version
            msg.attach(MIMEText(body, 'plain'))
            
            # Add HTML version
            msg.attach(MIMEText(html_body, 'html'))

            # Send email
            context = ssl.create_default_context()
            with smtplib.SMTP(config.SMTP_SERVER, config.SMTP_PORT) as server:
                if config.SMTP_USE_TLS:
                    server.starttls(context=context)
                server.login(config.SMTP_USERNAME, config.SMTP_PASSWORD)
                server.send_message(msg)

            logger.info(f"✅ Client alert sent to {len(recipients)} recipients: {subject}")
            return True

        except Exception as e:
            logger.error(f"Failed to send client alert: {e}")
            return False

    def send_hourly_status_alert(self, offline_stores: List[StoreAlert], total_stores: int) -> bool:
        """Send hourly status update to clients"""
        if not offline_stores:
            logger.debug("No offline stores - skipping client alert")
            return True

        if not self._should_send_alert('hourly_status'):
            logger.debug("Hourly status alert in cooldown")
            return False

        try:
            current_time = fmt_mnl()
            online_stores = total_stores - len(offline_stores)
            
            # Determine alert level
            offline_pct = len(offline_stores) / max(total_stores, 1) * 100
            if offline_pct > 20:
                alert_level = "🔴 High Priority"
                priority = "high"
            elif offline_pct > 10:
                alert_level = "🟡 Medium Priority"
                priority = "medium"
            else:
                alert_level = "🟢 Low Priority"
                priority = "low"

            # Send to each enabled client group
            sent_count = 0
            for client_id, client_config in self.config.get('clients', {}).items():
                if not client_config.get('enabled', False):
                    continue
                
                # Check if this client wants this type of alert
                alert_types = client_config.get('alert_types', [])
                if 'store_offline' not in alert_types:
                    continue

                # Check priority threshold
                client_threshold = client_config.get('priority_threshold', 'low')
                if client_threshold == 'high' and priority != 'high':
                    continue
                if client_threshold == 'medium' and priority == 'low':
                    continue

                recipients = client_config.get('emails', [])
                if not recipients:
                    continue

                # Compose email
                subject = f"CocoPan Status Update: {len(offline_stores)} Store{'s' if len(offline_stores) != 1 else ''} Temporarily Offline"

                # Text version
                text_body = f"""
CocoPan Operations Status Update
{current_time}

{alert_level} - Store Status Summary:
• Online: {online_stores}/{total_stores} stores ({(online_stores/total_stores*100):.1f}%)
• Temporarily Offline: {len(offline_stores)} stores

Stores Currently Offline:
"""
                
                for store in offline_stores[:10]:  # Limit to first 10
                    platform_emoji = "🛒" if store.platform == "GrabFood" else "🍔"
                    text_body += f"• {platform_emoji} {store.name.replace('Cocopan ', '').replace('Cocopan - ', '')}\n"
                
                if len(offline_stores) > 10:
                    text_body += f"... and {len(offline_stores) - 10} more stores\n"

                text_body += f"""
These stores are being monitored continuously and will automatically return to online status once they resume operations.

Dashboard: https://cocopan-monitor.railway.app
Support: operations@cocopan.com

This is an automated update from CocoPan Operations Monitoring.
"""

                # HTML version
                html_body = f"""
<html><body style="font-family: Arial, sans-serif; line-height: 1.6; color: #333; max-width: 600px; margin: 0 auto;">
<div style="background: linear-gradient(135deg, #1E40AF 0%, #3B82F6 100%); color: white; padding: 20px; border-radius: 8px; margin-bottom: 20px;">
  <h1 style="margin: 0; font-size: 24px;">🏢 CocoPan Operations Update</h1>
  <p style="margin: 5px 0 0 0; opacity: 0.9;">{current_time}</p>
</div>

<div style="background: #F8FAFC; border-left: 4px solid #3B82F6; padding: 15px; margin: 20px 0; border-radius: 4px;">
  <h2 style="color: #1E293B; margin-top: 0;">{alert_level} - Store Status Summary</h2>
  <div style="display: flex; gap: 20px; margin: 15px 0;">
    <div style="text-align: center; padding: 10px; background: white; border-radius: 6px; flex: 1;">
      <div style="font-size: 24px; font-weight: bold; color: #059669;">{online_stores}</div>
      <div style="font-size: 12px; color: #64748B;">ONLINE STORES</div>
    </div>
    <div style="text-align: center; padding: 10px; background: white; border-radius: 6px; flex: 1;">
      <div style="font-size: 24px; font-weight: bold; color: #DC2626;">{len(offline_stores)}</div>
      <div style="font-size: 12px; color: #64748B;">TEMPORARILY OFFLINE</div>
    </div>
  </div>
</div>

<h3 style="color: #1E293B;">Stores Currently Offline:</h3>
<ul style="background: #FEF2F2; padding: 15px; border-radius: 6px; border-left: 4px solid #EF4444;">
"""
                
                for store in offline_stores[:10]:
                    platform_emoji = "🛒" if store.platform == "GrabFood" else "🍔"
                    clean_name = store.name.replace('Cocopan ', '').replace('Cocopan - ', '')
                    html_body += f"<li>{platform_emoji} <strong>{clean_name}</strong></li>"
                
                if len(offline_stores) > 10:
                    html_body += f"<li><em>... and {len(offline_stores) - 10} more stores</em></li>"

                html_body += f"""
</ul>

<div style="background: #EBF8FF; border: 1px solid #3B82F6; border-radius: 6px; padding: 15px; margin: 20px 0;">
  <p style="margin: 0; font-size: 14px; color: #1E40AF;">
    <strong>Note:</strong> These stores are being monitored continuously and will automatically return to online status once they resume operations.
  </p>
</div>

<div style="text-align: center; margin: 30px 0;">
  <a href="https://cocopan-monitor.railway.app" style="background: #3B82F6; color: white; padding: 12px 24px; text-decoration: none; border-radius: 6px; font-weight: bold;">
    View Live Dashboard
  </a>
</div>

<hr style="border: 1px solid #E5E7EB; margin: 20px 0;">
<p style="color: #6B7280; font-size: 12px; text-align: center;">
  This is an automated update from CocoPan Operations Monitoring<br>
  Support: operations@cocopan.com
</p>
</body></html>
"""

                success = self._send_email(subject, text_body, html_body, recipients)
                if success:
                    sent_count += 1

            if sent_count > 0:
                self.last_alerts['hourly_status'] = now_mnl()
                self._save_last_alerts()
                logger.info(f"✅ Sent hourly status alerts to {sent_count} client groups")
                return True
            else:
                logger.debug("No client groups configured for store offline alerts")
                return False

        except Exception as e:
            logger.error(f"Failed to send hourly status alert: {e}")
            return False

    def send_critical_offline_alert(self, critical_stores: List[StoreAlert]) -> bool:
        """Send immediate alert for critical store outages"""
        if not critical_stores:
            return True

        if not self._should_send_alert('critical_offline'):
            return False

        # Implementation for critical alerts (multiple stores down, long outages, etc.)
        # Similar structure to hourly alerts but with urgent messaging
        pass

# Global instance
client_alerts = ClientAlertManager()