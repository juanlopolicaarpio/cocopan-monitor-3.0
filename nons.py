#!/usr/bin/env python3
"""
Test SMS - Send sample OOS alert to Shaina and Nons
"""
import os
import requests
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.environ.get('SEMAPHORE_API_KEY')
SENDER_NAME = os.environ.get('SEMAPHORE_SENDER_NAME', 'Watchtower')

# Recipients
RECIPIENTS = [
    {"name": "Shaina", "phone": "09176320151"},
    {"name": "Nons", "phone": "09062258711"},
    {"name": "Juanlo", "phone": "09178005833"},

]

# Sample OOS alert message (realistic example)
MESSAGE = """[OOS ALERT] Cocopan Maysilo

8 item(s) out of stock:
- Chicken Asado Bun
- Pork Siopao Large
- Ube Cheese Pandesal
- Buko Pandan
- Classic Siopao
- Beef Siopao
+2 more

Full list: sku.up.railway.app

Compliance: 75.0%
Time: Feb 25, 10:45 AM"""

def send_sms(phone, message):
    """Send SMS via Semaphore"""
    payload = {
        'apikey': API_KEY,
        'number': phone,
        'message': message,
        'sendername': SENDER_NAME
    }
    
    response = requests.post(
        "https://api.semaphore.co/api/v4/messages",
        data=payload,
        timeout=30
    )
    
    return response.json()

def main():
    print("="*50)
    print("TEST SMS - Sample OOS Alert")
    print("="*50)
    print()
    print("Message to send:")
    print("-"*50)
    print(MESSAGE)
    print("-"*50)
    print()
    print(f"Recipients:")
    for r in RECIPIENTS:
        print(f"  - {r['name']}: {r['phone']}")
    print()
    
    confirm = input("Send to Shaina and Nons? (y/n): ").strip().lower()
    
    if confirm != 'y':
        print("Cancelled.")
        return
    
    print()
    
    for recipient in RECIPIENTS:
        print(f"Sending to {recipient['name']} ({recipient['phone']})...")
        result = send_sms(recipient['phone'], MESSAGE)
        
        if isinstance(result, list) and len(result) > 0 and 'message_id' in result[0]:
            print(f"  [OK] Sent! Message ID: {result[0]['message_id']}")
        else:
            print(f"  [X] Failed: {result}")
        print()
    
    print("="*50)
    print("Done! Check Semaphore USAGE page to confirm delivery.")
    print("="*50)

if __name__ == "__main__":
    main()