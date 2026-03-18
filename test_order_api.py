"""
test_order_api.py - Quick test for Order Management API connection
"""
import httpx
import json
import os
from dotenv import load_dotenv

load_dotenv()

BASE_URL  = os.getenv("ORDER_API_BASE_URL")
API_TOKEN = os.getenv("ORDER_API_KEY")

print(f"URL:   {BASE_URL}")
print(f"Token: {API_TOKEN[:20]}..." if API_TOKEN else "Token: NOT SET")

response = httpx.get(
    BASE_URL,
    headers={
        "Authorization": f"Bearer {API_TOKEN}",
        "Content-Type": "application/json",
    },
    params={
        "from_date": "2026-03-10",
        "to_date":   "2026-03-10",
        "ship_type": "ship_date",
    },
    timeout=60,
)

print(f"\nStatus: {response.status_code}")

if response.status_code == 200:
    data   = response.json()
    if isinstance(data, list):
        orders = data
    else:
        orders = data.get("data", [])
        print(f"Orders returned: {len(orders)}")
    if orders:
        print("\nFirst order:")
        print(json.dumps(orders[0], indent=2))
else:
    print(f"Error: {response.text[:200]}")
