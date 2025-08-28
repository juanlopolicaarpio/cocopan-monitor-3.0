#!/usr/bin/env python3
"""
CocoPan Admin Alert System
Sends technical alerts to operations team when manual intervention needed
"""
import json
import logging
import smtplib
import ssl
from datetime import datetime, timedelta
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from typing import List, Dict, Any
from dataclasses import dataclass
from pathlib import Path

from config import config

logging.basicConfig(level=getattr(logging, config.LOG_LEVEL))
logger = logging.getLogger(__name__)

@dataclass
class ProblemStore:
    """Store that needs admin attention"""
    name: str
    url: str
    status: str  # BLOCKED, UNKNOWN, ERROR
    message: str
    response_time: int
    platform: str
    
class AdminAlertManager:
    """Manages admin alerts for technical issues"""
    
    def __init__(self):
        self.config_file = "admin_alerts.json"
        self.config = self._load_config()
        self.last_alerts = {}  # Cooldown tracking
        
        if not self.config:
            logger.warning("⚠️ Admin alerts disabled - no config found")
            return
            
        logger.info(f"✅ Admin Alert System initialized")
        logger.info(f"   📧 Recipients: {', '.join(self.config['admin_team']['emails'])}")
        
    def _load_config(self) -> Dict:
        """Load admin alert configuration"""
        try:
            config_path = Path(self.config_file)
            if not config_path.exists():
                logger.warning(f"Admin alert config not found: {self.config_file}")
                return {}
                
            with open(config_path) as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Failed to load admin config: {e}")
            return {}
    
    def _should_send_alert(self, alert_type: str) -> bool:
        """Check if alert should be sent (cooldown logic)"""
        if not self.config or not self.config['admin_team']['enabled']:
            return False
            
        trigger_config = self.config['alert_triggers'].get(alert_type, {})
        if not trigger_config.get('enabled', False):
            return False
            
        # Check cooldown
        cooldown_minutes = trigger_config.get('cooldown_minutes', 60)
        last_sent = self.last_alerts.get(alert_type)
        
        if last_sent:
            time_since = datetime.now() - last_sent
            if time_since < timedelta(minutes=cooldown_minutes):
                logger.debug(f"Alert {alert_type} in cooldown for {cooldown_minutes - time_since.total_seconds()/60:.1f} more minutes")
                return False
                
        return True
    
    def _send_email(self, subject: str, body: str, html_body: str = None) -> bool:
        """Send admin email using existing SMTP config"""
        try:
            if not config.SMTP_USERNAME or not config.SMTP_PASSWORD:
                logger.error("SMTP credentials not configured")
                return False
                
            admin_emails = self.config['admin_team']['emails']
            if not admin_emails:
                logger.error("No admin email addresses configured")
                return False
            
            # Create message
            msg = MIMEMultipart('alternative')
            msg['Subject'] = f"{self.config['email_settings']['subject_prefix']}: {subject}"
            msg['From'] = f"{self.config['email_settings']['from_name']} <{config.FROM_EMAIL}>"
            msg['To'] = ', '.join(admin_emails)
            msg['Priority'] = 'High'  # Mark as high priority
            
            # Add text version
            msg.attach(MIMEText(body, 'plain'))
            
            # Add HTML version if provided
            if html_body:
                msg.attach(MIMEText(html_body, 'html'))
            
            # Send email
            context = ssl.create_default_context()
            
            with smtplib.SMTP(config.SMTP_SERVER, config.SMTP_PORT) as server:
                if config.SMTP_USE_TLS:
                    server.starttls(context=context)
                server.login(config.SMTP_USERNAME, config.SMTP_PASSWORD)
                server.send_message(msg)
                
            logger.info(f"✅ Admin alert sent: {subject}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to send admin alert: {e}")
            return False
    
    def send_manual_verification_alert(self, problem_stores: List[ProblemStore]) -> bool:
        """Send alert for stores needing manual verification"""
        if not problem_stores or not self._should_send_alert('manual_verification_needed'):
            return False
            
        try:
            # Group by status
            blocked_stores = [s for s in problem_stores if s.status == 'BLOCKED']
            unknown_stores = [s for s in problem_stores if s.status == 'UNKNOWN'] 
            error_stores = [s for s in problem_stores if s.status == 'ERROR']
            
            current_time = datetime.now().strftime('%B %d, %Y • %I:%M %p Manila Time')
            
            # Text version
            text_body = f"""🚨 CocoPan Monitor - Manual Verification Required

ALERT TIME: {current_time}
ISSUE: {len(problem_stores)} stores showing technical issues (not genuine closures)

"""
            
            # HTML version  
            html_body = f"""
            <html><body style="font-family: Arial, sans-serif; line-height: 1.6; color: #333;">
            <div style="background: #dc2626; color: white; padding: 20px; border-radius: 8px; margin-bottom: 20px;">
                <h1 style="margin: 0; font-size: 24px;">🚨 CocoPan Monitor Alert</h1>
                <p style="margin: 5px 0 0 0; opacity: 0.9;">Manual Verification Required</p>
            </div>
            
            <p><strong>Alert Time:</strong> {current_time}<br>
            <strong>Issue:</strong> {len(problem_stores)} stores showing technical issues (not genuine closures)</p>
            """
            
            # Add blocked stores section
            if blocked_stores:
                text_body += f"🚫 BLOCKED STORES ({len(blocked_stores)} - bot detection/access denied):\n"
                html_body += f"<h2 style='color: #dc2626;'>🚫 Blocked Stores ({len(blocked_stores)})</h2><ul>"
                
                for store in blocked_stores:
                    text_body += f"• {store.name}\n"
                    text_body += f"  URL: {store.url}\n"
                    text_body += f"  Issue: {store.message}\n\n"
                    
                    html_body += f"""
                    <li style="margin-bottom: 10px;">
                        <strong>{store.name}</strong><br>
                        <a href="{store.url}" target="_blank" style="color: #1e40af;">{store.url}</a><br>
                        <em style="color: #dc2626;">Issue: {store.message}</em>
                    </li>
                    """
                html_body += "</ul>"
            
            # Add unknown stores section  
            if unknown_stores:
                text_body += f"❓ UNKNOWN STORES ({len(unknown_stores)} - status unclear):\n"
                html_body += f"<h2 style='color: #f59e0b;'>❓ Unknown Status ({len(unknown_stores)})</h2><ul>"
                
                for store in unknown_stores:
                    text_body += f"• {store.name}\n"
                    text_body += f"  URL: {store.url}\n"
                    text_body += f"  Issue: {store.message}\n\n"
                    
                    html_body += f"""
                    <li style="margin-bottom: 10px;">
                        <strong>{store.name}</strong><br>
                        <a href="{store.url}" target="_blank" style="color: #1e40af;">{store.url}</a><br>
                        <em style="color: #f59e0b;">Issue: {store.message}</em>
                    </li>
                    """
                html_body += "</ul>"
            
            # Add error stores section
            if error_stores:
                text_body += f"⚠️ ERROR STORES ({len(error_stores)} - connection/timeout issues):\n"
                html_body += f"<h2 style='color: #dc2626;'>⚠️ Error Stores ({len(error_stores)})</h2><ul>"
                
                for store in error_stores:
                    text_body += f"• {store.name}\n"
                    text_body += f"  URL: {store.url}\n"
                    text_body += f"  Issue: {store.message}\n\n"
                    
                    html_body += f"""
                    <li style="margin-bottom: 10px;">
                        <strong>{store.name}</strong><br>
                        <a href="{store.url}" target="_blank" style="color: #1e40af;">{store.url}</a><br>
                        <em style="color: #dc2626;">Issue: {store.message}</em>
                    </li>
                    """
                html_body += "</ul>"
            
            # Add action section
            dashboard_url = self.config['email_settings']['dashboard_url']
            
            text_body += f"""
ACTION REQUIRED:
1. Click the links above to manually verify each store status
2. Login to admin dashboard: {dashboard_url}
3. Override status for each verified store
4. Client dashboard will show corrected data

⚠️ TIME SENSITIVE: Clients may see incorrect offline status until fixed

---
This is an automated alert from CocoPan Monitor System
"""
            
            html_body += f"""
            <div style="background: #fef3c7; border-left: 4px solid #f59e0b; padding: 15px; margin: 20px 0;">
                <h3 style="color: #92400e; margin-top: 0;">Action Required:</h3>
                <ol style="color: #92400e;">
                    <li>Click the store links above to manually verify each store status</li>
                    <li><a href="{dashboard_url}" target="_blank" style="color: #1e40af;">Login to admin dashboard</a></li>
                    <li>Override status for each verified store</li>
                    <li>Client dashboard will show corrected data</li>
                </ol>
                <p style="color: #dc2626; font-weight: bold;">⚠️ TIME SENSITIVE: Clients may see incorrect offline status until fixed</p>
            </div>
            
            <hr style="border: 1px solid #e5e7eb; margin: 20px 0;">
            <p style="color: #6b7280; font-size: 12px;">This is an automated alert from CocoPan Monitor System</p>
            </body></html>
            """
            
            # Send the email
            subject = f"Manual Verification Required - {len(problem_stores)} Stores Need Attention"
            success = self._send_email(subject, text_body, html_body)
            
            if success:
                self.last_alerts['manual_verification_needed'] = datetime.now()
                
            return success
            
        except Exception as e:
            logger.error(f"Failed to create manual verification alert: {e}")
            return False
    
    def send_bot_detection_alert(self, blocked_count: int) -> bool:
        """Send alert for bot detection spike"""
        if not self._should_send_alert('bot_detection_spike'):
            return False
            
        threshold = self.config['alert_triggers']['bot_detection_spike']['blocked_stores_threshold']
        if blocked_count < threshold:
            return False
            
        current_time = datetime.now().strftime('%B %d, %Y • %I:%M %p Manila Time')
        
        text_body = f"""🤖 CocoPan Monitor - Bot Detection Alert

ALERT TIME: {current_time}
ISSUE: High number of stores blocked by anti-bot measures
BLOCKED COUNT: {blocked_count} stores

This suggests the monitoring system is being detected as a bot.
Consider adjusting request patterns or user agents.

RECOMMENDED ACTIONS:
1. Check admin dashboard for affected stores
2. Manually verify store status 
3. Consider temporary rate limiting adjustments
4. Monitor for resolution

Admin Dashboard: {self.config['email_settings']['dashboard_url']}
"""
        
        subject = f"Bot Detection Alert - {blocked_count} Stores Blocked"
        success = self._send_email(subject, text_body)
        
        if success:
            self.last_alerts['bot_detection_spike'] = datetime.now()
            
        return success
    
    def send_system_health_alert(self, cycle_time: float, database_errors: int) -> bool:
        """Send system health warning"""
        if not self._should_send_alert('system_health'):
            return False
            
        current_time = datetime.now().strftime('%B %d, %Y • %I:%M %p Manila Time')
        
        issues = []
        if cycle_time > self.config['alert_triggers']['system_health']['max_cycle_time_minutes'] * 60:
            issues.append(f"Monitoring cycle took {cycle_time/60:.1f} minutes (expected < {self.config['alert_triggers']['system_health']['max_cycle_time_minutes']} min)")
            
        if database_errors > self.config['alert_triggers']['system_health']['database_error_threshold']:
            issues.append(f"{database_errors} database errors during cycle")
        
        if not issues:
            return False
            
        text_body = f"""⚠️ CocoPan Monitor - System Health Warning

ALERT TIME: {current_time}
ISSUES DETECTED:

"""
        for issue in issues:
            text_body += f"• {issue}\n"
            
        text_body += f"""
RECOMMENDED ACTIONS:
1. Check system resources (CPU, memory)
2. Review database performance  
3. Monitor next few cycles for improvement
4. Consider scaling if issues persist

System may continue operating but performance is degraded.
"""
        
        subject = "System Health Warning - Performance Issues"
        success = self._send_email(subject, text_body)
        
        if success:
            self.last_alerts['system_health'] = datetime.now()
            
        return success

# Global instance
admin_alerts = AdminAlertManager()