#!/usr/bin/env python3
"""
Simple SMS Test - Debug Version
No emojis, minimal message
"""
import os
import requests
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.environ.get('SEMAPHORE_API_KEY')
PHONE = "09178005833"  # Your number

# Test 1: Simple message, NO sender name
print("="*50)
print("TEST 1: Simple message, NO sender name")
print("="*50)

payload = {
    'apikey': API_KEY,
    'number': PHONE,
    'message': 'Test message from SKU Monitor. If you receive this, SMS is working.'
}

print(f"Payload: {payload}")
print()

response = requests.post(
    "https://api.semaphore.co/api/v4/messages",
    data=payload,
    timeout=30
)

print(f"Status: {response.status_code}")
print(f"Response: {response.text}")
print()

# Test 2: With sender name
print("="*50)
print("TEST 2: With sender name 'Watchtower'")
print("="*50)

payload2 = {
    'apikey': API_KEY,
    'number': PHONE,
    'message': 'Test 2 with sender name.',
    'sendername': 'Watchtower'
}

print(f"Payload: {payload2}")
print()

response2 = requests.post(
    "https://api.semaphore.co/api/v4/messages",
    data=payload2,
    timeout=30
)

print(f"Status: {response2.status_code}")
print(f"Response: {response2.text}")
print()

print("="*50)
print("Check your Semaphore USAGE page to see which one worked")
print("="*50)