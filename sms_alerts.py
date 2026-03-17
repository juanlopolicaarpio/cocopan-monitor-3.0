#!/usr/bin/env python3
"""
Alert Service - SMS Only (Semaphore)
Sends OOS alerts to store managers via SMS
Routes alerts to SM -> MUM -> OM based on store
"""
import os
import json
import logging
import re
import requests
from datetime import datetime
from typing import List, Dict, Optional

from dotenv import load_dotenv
load_dotenv()

logger = logging.getLogger(__name__)

BRANCH_CONFIG_FILE = "branch_config.json"
SEMAPHORE_API_URL = "https://api.semaphore.co/api/v4/messages"


class BranchConfig:
    """Manages branch and manager configuration"""
    
    def __init__(self, config_file: str = BRANCH_CONFIG_FILE):
        self.config_file = config_file
        self.config = self._load_config()
    
    def _load_config(self) -> Dict:
        try:
            with open(self.config_file, 'r') as f:
                config = json.load(f)
                logger.info(f"Loaded {len(config.get('branches', {}))} branches from config")
                return config
        except FileNotFoundError:
            logger.warning(f"Config file not found: {self.config_file}")
            return {'managers': {}, 'branches': {}, 'url_to_branch': {}, 'alert_settings': {}}
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON in config file: {e}")
            return {'managers': {}, 'branches': {}, 'url_to_branch': {}, 'alert_settings': {}}
    
    def _extract_store_key(self, url: str) -> Optional[str]:
        """Extract store identifier from URL for matching"""
        patterns = [
            r'restaurant/(cocopan-[a-z0-9-]+)',
            r'restaurant/[a-z0-9]+/(cocopan-[a-z0-9-]+)',
        ]
        
        url_lower = url.lower()
        
        for pattern in patterns:
            match = re.search(pattern, url_lower)
            if match:
                return match.group(1)
        
        return None
    
    def get_branch_code(self, url: str) -> Optional[str]:
        """Get branch code from URL"""
        store_key = self._extract_store_key(url)
        if not store_key:
            return None
        
        url_mappings = self.config.get('url_to_branch', {})
        
        if store_key in url_mappings:
            return url_mappings[store_key]
        
        for key, code in url_mappings.items():
            if key in store_key or store_key in key:
                return code
        
        return None
    
    def get_contacts_for_store(self, store_url: str, include_admin: bool = True) -> List[Dict]:
        """Get all contacts that should receive alerts for a store
        
        Args:
            store_url: The store's platform URL
            include_admin: If False, skip admin contacts (used for offline alerts)
        """
        contacts = []
        seen_phones = set()
        settings = self.get_alert_settings()
        
        branch_code = self.get_branch_code(store_url)
        
        if branch_code:
            branch = self.config.get('branches', {}).get(branch_code, {})
            managers = self.config.get('managers', {})
            
            if settings.get('send_to_sm', True) and branch.get('sm'):
                sm = managers.get(branch['sm'])
                if sm and sm.get('phone') not in seen_phones:
                    contacts.append({**sm, 'id': branch['sm'], 'role_for_store': 'SM'})
                    seen_phones.add(sm.get('phone'))
            
            if settings.get('send_to_mum', True) and branch.get('mum'):
                mum = managers.get(branch['mum'])
                if mum and mum.get('phone') not in seen_phones:
                    contacts.append({**mum, 'id': branch['mum'], 'role_for_store': 'MUM'})
                    seen_phones.add(mum.get('phone'))
            
            if settings.get('send_to_om', False) and branch.get('om'):
                om = managers.get(branch['om'])
                if om and om.get('phone') not in seen_phones:
                    contacts.append({**om, 'id': branch['om'], 'role_for_store': 'OM'})
                    seen_phones.add(om.get('phone'))
        
        if include_admin and settings.get('send_to_admin', True):
            admin_ids = settings.get('admin_ids', [])
            managers = self.config.get('managers', {})
            
            for admin_id in admin_ids:
                admin = managers.get(admin_id)
                if admin and admin.get('phone') not in seen_phones:
                    contacts.append({**admin, 'id': admin_id, 'role_for_store': 'ADMIN'})
                    seen_phones.add(admin.get('phone'))
        
        return contacts
    
    def get_alert_settings(self) -> Dict:
        return self.config.get('alert_settings', {
            'min_oos_to_alert': 1,
            'max_items_in_sms': 6,
            'include_compliance_pct': True,
            'send_to_sm': True,
            'send_to_mum': True,
            'send_to_om': False,
            'send_to_admin': True,
            'admin_ids': [],
            'quiet_hours': {'enabled': False},
            'offline_alerts_enabled': True,
        })
    
    def get_branch_name(self, store_url: str) -> Optional[str]:
        """Get friendly branch name from URL"""
        branch_code = self.get_branch_code(store_url)
        if branch_code:
            branch = self.config.get('branches', {}).get(branch_code, {})
            return branch.get('name')
        return None


class SMSAlertService:
    """SMS Alert Service using Semaphore"""
    
    def __init__(self, config_file: str = BRANCH_CONFIG_FILE):
        self.api_key = os.environ.get('SEMAPHORE_API_KEY')
        self.sender_name = os.environ.get('SEMAPHORE_SENDER_NAME', 'Watchtower')
        self.config = BranchConfig(config_file)
        self.stats = {'sent': 0, 'failed': 0, 'skipped': 0}
        
        if self.api_key:
            logger.info(f"Semaphore SMS initialized (Sender: {self.sender_name})")
        else:
            logger.warning("SEMAPHORE_API_KEY not set - SMS alerts disabled")
    
    def _format_phone(self, number: str) -> str:
        """Format to 09XXXXXXXXX"""
        number = ''.join(filter(str.isdigit, number))
        
        if number.startswith('63'):
            return f"0{number[2:]}"
        elif number.startswith('+63'):
            return f"0{number[3:]}"
        elif number.startswith('9') and len(number) == 10:
            return f"0{number}"
        return number
    
    def _send_sms(self, to: str, message: str) -> bool:
        """Send SMS via Semaphore"""
        if not self.api_key:
            logger.error("Cannot send SMS: SEMAPHORE_API_KEY not set")
            return False
        
        try:
            to = self._format_phone(to)
            
            payload = {
                'apikey': self.api_key,
                'number': to,
                'message': message,
                'sendername': self.sender_name
            }
            
            response = requests.post(SEMAPHORE_API_URL, data=payload, timeout=30)
            
            try:
                result = response.json()
            except:
                logger.error(f"Semaphore returned invalid JSON: {response.text}")
                return False
            
            if isinstance(result, list) and len(result) > 0:
                first = result[0]
                if 'message_id' in first:
                    logger.info(f"SMS sent to {to} (ID: {first['message_id']})")
                    return True
                elif 'error' in first:
                    logger.error(f"Semaphore error: {first['error']}")
                    return False
            
            if isinstance(result, dict) and 'error' in result:
                logger.error(f"Semaphore error: {result['error']}")
                return False
            
            logger.error(f"Unexpected Semaphore response: {result}")
            return False
                
        except requests.exceptions.RequestException as e:
            logger.error(f"Network error: {e}")
            return False
        except Exception as e:
            logger.error(f"SMS send error: {type(e).__name__}: {e}")
            return False
    
    def _format_oos_message(self, store_name: str, oos_items: List[Dict], 
                            compliance_pct: Optional[float] = None) -> str:
        """Format the OOS alert message for SMS"""
        settings = self.config.get_alert_settings()
        max_items = settings.get('max_items_in_sms', 6)
        
        lines = [
            f"[OOS ALERT] {store_name} GrabFood",
            "",
            f"{len(oos_items)} item(s) out of stock:"
        ]
        
        for item in oos_items[:max_items]:
            name = item.get('product_name', item.get('name', 'Unknown'))
            if len(name) > 30:
                name = name[:27] + "..."
            lines.append(f"- {name}")
        
        if len(oos_items) > max_items:
            lines.append(f"+{len(oos_items) - max_items} more")
            lines.append("")
            lines.append("See full list:")
            lines.append("https://sku.up.railway.app")
        
        if compliance_pct is not None and settings.get('include_compliance_pct', True):
            lines.append("")
            lines.append(f"Compliance: {compliance_pct:.1f}%")
        
        lines.append(f"Time: {datetime.now().strftime('%b %d, %I:%M %p')}")
        
        return "\n".join(lines)
    
    def _is_quiet_hours(self) -> bool:
        """Check if current time is in quiet hours"""
        settings = self.config.get_alert_settings()
        quiet = settings.get('quiet_hours', {})
        
        if not quiet.get('enabled', False):
            return False
        
        try:
            now = datetime.now().time()
            start = datetime.strptime(quiet['start'], '%H:%M').time()
            end = datetime.strptime(quiet['end'], '%H:%M').time()
            
            if start <= end:
                return start <= now <= end
            else:
                return now >= start or now <= end
        except:
            return False
    
    def send_oos_alert(self, store_name: str, store_url: str, 
                       oos_items: List[Dict], compliance_pct: Optional[float] = None) -> Dict:
        """Send OOS alert via SMS to appropriate managers"""
        result = {'sent': 0, 'failed': 0, 'skipped': 0, 'recipients': []}
        
        settings = self.config.get_alert_settings()
        min_oos = settings.get('min_oos_to_alert', 1)
        
        if len(oos_items) < min_oos:
            logger.info(f"Skipping alert: {len(oos_items)} OOS < minimum {min_oos}")
            result['skipped'] = 1
            return result
        
        if self._is_quiet_hours():
            logger.info(f"Skipping alert: Quiet hours")
            result['skipped'] = 1
            return result
        
        branch_name = self.config.get_branch_name(store_url)
        display_name = branch_name or store_name
        
        contacts = self.config.get_contacts_for_store(store_url)
        
        if not contacts:
            logger.warning(f"No contacts for: {display_name}")
            result['skipped'] = 1
            return result
        
        message = self._format_oos_message(display_name, oos_items, compliance_pct)
        
        logger.info(f"Sending SMS to {len(contacts)} contact(s) for {display_name}...")
        
        for contact in contacts:
            phone = contact.get('phone')
            name = contact.get('name', 'Unknown')
            role = contact.get('role_for_store', 'Unknown')
            
            if not phone:
                continue
            
            logger.info(f"   SMS to {name} ({role}) - {phone}...")
            
            if self._send_sms(phone, message):
                result['sent'] += 1
                result['recipients'].append({
                    'name': name, 
                    'phone': phone, 
                    'role': role,
                    'status': 'sent'
                })
                self.stats['sent'] += 1
            else:
                result['failed'] += 1
                self.stats['failed'] += 1
        
        return result
    
    # ------------------------------------------------------------------
    # OFFLINE / Uptime Alerts
    # ------------------------------------------------------------------
    def _format_offline_message(self, store_name: str, platform: str,
                                reported_by: str, hour_label: str = "") -> str:
        """Format a single-store offline alert message"""
        platform_label = "Foodpanda" if "panda" in platform.lower() else "GrabFood"
        lines = [
            f"[OFFLINE] {store_name}",
            f"Platform: {platform_label}",
            f"Time: {datetime.now().strftime('%b %d, %I:%M %p')}",
        ]
        return "\n".join(lines)

    def send_offline_alert(self, store_name: str, store_url: str,
                           platform: str, reported_by: str,
                           hour_label: str = "") -> Dict:
        """Send a single-store offline alert to the store's SM/MUM/OM chain (no admins)."""
        result = {'sent': 0, 'failed': 0, 'skipped': 0, 'recipients': []}

        settings = self.config.get_alert_settings()
        if not settings.get('offline_alerts_enabled', True):
            logger.info("Offline alerts disabled in settings")
            result['skipped'] = 1
            return result

        if self._is_quiet_hours():
            logger.info("Skipping offline alert: Quiet hours")
            result['skipped'] = 1
            return result

        branch_name = self.config.get_branch_name(store_url)
        display_name = branch_name or store_name

        # include_admin=False → only SM, MUM, OM
        contacts = self.config.get_contacts_for_store(store_url, include_admin=False)
        if not contacts:
            logger.warning(f"No contacts mapped for {display_name} ({store_url})")
            result['skipped'] = 1
            return result

        message = self._format_offline_message(display_name, platform, reported_by, hour_label)

        for contact in contacts:
            phone = contact.get('phone')
            if not phone:
                continue
            name = contact.get('name', 'Unknown')
            role = contact.get('role_for_store', 'Unknown')
            logger.info(f"   Offline SMS to {name} ({role}) - {phone}")

            if self._send_sms(phone, message):
                result['sent'] += 1
                result['recipients'].append({'name': name, 'phone': phone, 'role': role, 'status': 'sent'})
                self.stats['sent'] += 1
            else:
                result['failed'] += 1
                self.stats['failed'] += 1

        return result

    def send_test_sms(self, phone: str) -> bool:
        """Send a test SMS"""
        message = f"""[TEST] SKU Monitor

This is a test message.
If you received this, SMS alerts are working!

Time: {datetime.now().strftime('%b %d, %I:%M %p')}"""
        
        return self._send_sms(phone, message)
    
    def get_stats(self) -> Dict:
        return self.stats.copy()


def test_sms():
    """Test the SMS service"""
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    
    print("="*60)
    print("SMS Alert Test")
    print("="*60)
    
    sms = SMSAlertService()
    
    test_url = "https://food.grab.com/ph/en/restaurant/cocopan-maysilo-delivery/2-C6TATTL2UF2UDA"
    
    oos_items = [
        {'product_name': 'Chicken Asado Bun'},
        {'product_name': 'Pork Siopao Large'},
    ]
    
    result = sms.send_oos_alert(
        store_name="Cocopan Maysilo",
        store_url=test_url,
        oos_items=oos_items,
        compliance_pct=85.7
    )
    
    print(f"\nResult: {result}")


if __name__ == "__main__":
    test_sms()