#!/usr/bin/env python3
"""
CocoPan Client Alert System - UPDATED WITH CUSTOM TEMPLATE
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
        """
        ✨ UPDATED: Send hourly status update with Dana's custom format
        
        Format:
        EMPORIA UPDATE
        November 24, 2025 (Monday) 9:00 AM
        
        Grabfood (2) Offline stores:
        🔴 Citisquare
        🔴 Sierra Madre
        
        Foodpanda (3) Offline stores:
        🔴 Citisquare Malabon
        🔴 Moonwalk
        🔴 Pulang Lupa
        """
        if not offline_stores:
            logger.debug("No offline stores - skipping client alert")
            return True

        if not self._should_send_alert('hourly_status'):
            logger.debug("Hourly status alert in cooldown")
            return False

        try:
            current_time = now_mnl()
            
            # Format: "November 24, 2025 (Monday) 9:00 AM"
            formatted_date = current_time.strftime('%B %d, %Y (%A) %-I:%M %p')
            
            # Group stores by platform
            grabfood_offline = [s for s in offline_stores if s.platform.lower() == 'grabfood']
            foodpanda_offline = [s for s in offline_stores if s.platform.lower() == 'foodpanda']
            
            # Send to each enabled client group
            sent_count = 0
            for client_id, client_config in self.config.get('clients', {}).items():
                if not client_config.get('enabled', False):
                    continue
                
                # Check if this client wants this type of alert
                alert_types = client_config.get('alert_types', [])
                if 'store_offline' not in alert_types:
                    continue

                recipients = client_config.get('emails', [])
                if not recipients:
                    continue

                # ═══════════════════════════════════════════════════════════
                # ✨ CUSTOM TEMPLATE - Dana's Format
                # ═══════════════════════════════════════════════════════════
                
                # Subject line
                subject = f"EMPORIA UPDATE - {len(offline_stores)} Store(s) Offline"

                # TEXT VERSION (for email clients that don't support HTML)
                text_body = f"""EMPORIA UPDATE
{formatted_date}

"""
                
                if grabfood_offline:
                    text_body += f"Grabfood ({len(grabfood_offline)}) Offline stores:\n"
                    for store in grabfood_offline:
                        # Clean store name (remove "Cocopan" prefix if present)
                        clean_name = store.name.replace('Cocopan ', '').replace('Cocopan - ', '').strip()
                        text_body += f"🔴 {clean_name}\n"
                    text_body += "\n"
                
                if foodpanda_offline:
                    text_body += f"Foodpanda ({len(foodpanda_offline)}) Offline stores:\n"
                    for store in foodpanda_offline:
                        # Clean store name
                        clean_name = store.name.replace('Cocopan ', '').replace('Cocopan - ', '').strip()
                        text_body += f"🔴 {clean_name}\n"
                    text_body += "\n"
                
                text_body += "———\n\n"
                text_body += "This is an automated alert from CocoPan Operations Monitoring.\n"
                text_body += "Dashboard: https://cocopan-monitor.railway.app\n"

                # HTML VERSION (prettier formatting)
                html_body = f"""
<html>
<body style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Arial, sans-serif; line-height: 1.6; color: #333; max-width: 600px; margin: 0 auto; padding: 20px;">

<div style="background: #f8f9fa; border-left: 4px solid #dc3545; padding: 20px; margin-bottom: 20px; border-radius: 4px;">
  <h2 style="margin: 0 0 10px 0; color: #1a1a1a; font-size: 20px; font-weight: 600;">
    EMPORIA UPDATE
  </h2>
  <p style="margin: 0; color: #666; font-size: 14px;">
    {formatted_date}
  </p>
</div>
"""

                if grabfood_offline:
                    html_body += f"""
<div style="margin-bottom: 25px;">
  <h3 style="margin: 0 0 12px 0; color: #1a1a1a; font-size: 16px; font-weight: 600;">
    Grabfood ({len(grabfood_offline)}) Offline stores:
  </h3>
  <ul style="list-style: none; padding: 0; margin: 0;">
"""
                    for store in grabfood_offline:
                        clean_name = store.name.replace('Cocopan ', '').replace('Cocopan - ', '').strip()
                        html_body += f"""
    <li style="padding: 8px 0; border-bottom: 1px solid #eee;">
      <span style="color: #dc3545; font-weight: bold;">🔴</span> {clean_name}
    </li>
"""
                    html_body += """
  </ul>
</div>
"""

                if foodpanda_offline:
                    html_body += f"""
<div style="margin-bottom: 25px;">
  <h3 style="margin: 0 0 12px 0; color: #1a1a1a; font-size: 16px; font-weight: 600;">
    Foodpanda ({len(foodpanda_offline)}) Offline stores:
  </h3>
  <ul style="list-style: none; padding: 0; margin: 0;">
"""
                    for store in foodpanda_offline:
                        clean_name = store.name.replace('Cocopan ', '').replace('Cocopan - ', '').strip()
                        html_body += f"""
    <li style="padding: 8px 0; border-bottom: 1px solid #eee;">
      <span style="color: #dc3545; font-weight: bold;">🔴</span> {clean_name}
    </li>
"""
                    html_body += """
  </ul>
</div>
"""

                html_body += """
<hr style="border: none; border-top: 2px solid #eee; margin: 30px 0;">

<div style="text-align: center; margin: 20px 0;">
  <a href="https://cocopan-monitor.railway.app" 
     style="display: inline-block; background: #007bff; color: white; padding: 12px 30px; text-decoration: none; border-radius: 6px; font-weight: 600;">
    View Live Dashboard
  </a>
</div>

<p style="color: #999; font-size: 12px; text-align: center; margin: 20px 0 0 0;">
  This is an automated alert from CocoPan Operations Monitoring
</p>

</body>
</html>
"""

                # Send email
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