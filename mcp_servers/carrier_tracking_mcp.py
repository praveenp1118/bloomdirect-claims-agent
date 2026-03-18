"""
carrier_tracking_mcp.py - MCP Server 1
Carrier Tracking MCP Server for BloomDirect Claims Recovery System.
Wraps UPS (via Shippo) and FedEx tracking APIs.

Tools exposed:
    - get_tracking_status(tracking_id, carrier)
    - get_full_history(tracking_id)
    - get_delivery_proof(tracking_id)
"""

import json
import os
import httpx
from datetime import datetime
from typing import Optional
from database.models import get_session, TrackingCache
from dotenv import load_dotenv

load_dotenv()

SHIPPO_API_KEY  = os.getenv("SHIPPO_API_KEY", "")
FEDEX_API_KEY   = os.getenv("FEDEX_API_KEY", "")
FEDEX_SECRET    = os.getenv("FEDEX_SECRET_KEY", "")
MCP_CACHE_HOURS = int(os.getenv("MCP_CACHE_HOURS", 6))

SHIPPO_TRACK_URL = "https://api.goshippo.com/tracks/{carrier}/{tracking_id}"
FEDEX_TOKEN_URL  = "https://apis.fedex.com/oauth/token"
FEDEX_TRACK_URL  = "https://apis.fedex.com/track/v1/trackingnumbers"


# ── CACHE HELPERS ─────────────────────────────────────────────────

def get_cached(tracking_id: str) -> Optional[dict]:
    """
    Return cached tracking data if fresh enough.
    Returns None if cache miss or stale.
    """
    session = get_session()
    try:
        record = session.query(TrackingCache).filter(
            TrackingCache.tracking_id == tracking_id
        ).first()

        if not record:
            return None

        # Check freshness — don't re-call if status is Delivered
        if record.cached_status and "delivered" in record.cached_status.lower():
            return {
                "tracking_id": tracking_id,
                "status": record.cached_status,
                "status_date": record.cached_status_date,
                "history": json.loads(record.full_history_json) if record.full_history_json else [],
                "source": "cache",
            }

        # For non-delivered — check if cache is still fresh
        if record.last_mcp_call:
            hours_since = (datetime.utcnow() - record.last_mcp_call).total_seconds() / 3600
            if hours_since < MCP_CACHE_HOURS:
                return {
                    "tracking_id": tracking_id,
                    "status": record.cached_status,
                    "status_date": record.cached_status_date,
                    "history": json.loads(record.full_history_json) if record.full_history_json else [],
                    "source": "cache",
                }
        return None
    finally:
        session.close()


def save_to_cache(tracking_id: str, carrier: str, status: str,
                  status_date: str, history: list) -> None:
    """Save or update tracking data in cache."""
    session = get_session()
    try:
        record = session.query(TrackingCache).filter(
            TrackingCache.tracking_id == tracking_id
        ).first()

        if record:
            record.cached_status      = status
            record.cached_status_date = status_date
            record.full_history_json  = json.dumps(history)
            record.last_mcp_call      = datetime.utcnow()
            record.source             = "mcp"
        else:
            record = TrackingCache(
                tracking_id        = tracking_id,
                carrier            = carrier,
                cached_status      = status,
                cached_status_date = status_date,
                full_history_json  = json.dumps(history),
                last_mcp_call      = datetime.utcnow(),
                source             = "mcp",
            )
            session.add(record)
        session.commit()
    finally:
        session.close()


# ── CARRIER DETECTION ─────────────────────────────────────────────

def detect_carrier(tracking_id: str, ship_method: str = "") -> str:
    """Detect carrier from tracking ID or ship_method."""
    if ship_method and "UPS" in ship_method.upper():
        return "UPS"
    if tracking_id.upper().startswith("1Z"):
        return "UPS"
    return "FedEx"


# ── MOCK DATA (for synthetic/test mode) ───────────────────────────

def get_mock_history(tracking_id: str, failure_type: str = "CARRIER_DELAY") -> dict:
    """
    Return realistic mock tracking history for synthetic mode.
    Used when API keys not configured or RUN_MODE=synthetic.
    """
    base_history = [
        {"status": "Picked up", "date": "2026-03-10 18:00", "location": "Origin"},
        {"status": "Departed FedEx location", "date": "2026-03-10 22:00", "location": "Origin Hub"},
        {"status": "Arrived at FedEx hub", "date": "2026-03-11 04:00", "location": "Memphis, TN"},
    ]

    failure_histories = {
        "CARRIER_DELAY": base_history + [
            {"status": "A mechanical failure has caused a delay. We will update the delivery date as soon as possible.",
             "date": "2026-03-11 08:00", "location": "Memphis, TN"},
            {"status": "Departed FedEx hub", "date": "2026-03-11 20:00", "location": "Memphis, TN"},
            {"status": "Arrived at FedEx location", "date": "2026-03-12 06:00", "location": "Destination City"},
            {"status": "On FedEx vehicle for delivery", "date": "2026-03-12 09:00", "location": "Destination City"},
            {"status": "Delivered", "date": "2026-03-12 14:30", "location": "Destination"},
        ],
        "DAMAGE": base_history + [
            {"status": "A damage has been reported and we will notify the sender.",
             "date": "2026-03-11 10:00", "location": "Memphis, TN"},
            {"status": "The package has been damaged and the sender will be notified.",
             "date": "2026-03-11 12:00", "location": "Memphis, TN"},
            {"status": "The package was refused by the receiver and will be returned to the sender.",
             "date": "2026-03-12 15:00", "location": "Destination"},
        ],
        "LOST": base_history + [
            {"status": "In transit", "date": "2026-03-11 06:00", "location": "Memphis, TN"},
            {"status": "Delay", "date": "2026-03-11 18:00", "location": "Unknown"},
        ],
        "LATE": base_history + [
            {"status": "Departed FedEx hub", "date": "2026-03-11 22:00", "location": "Memphis, TN"},
            {"status": "Arrived at FedEx location", "date": "2026-03-12 08:00", "location": "Destination City"},
            {"status": "On FedEx vehicle for delivery", "date": "2026-03-12 10:00", "location": "Destination City"},
            {"status": "Delivered", "date": "2026-03-13 16:00", "location": "Destination"},
        ],
        "ON_TIME": base_history + [
            {"status": "Departed FedEx hub", "date": "2026-03-10 23:00", "location": "Memphis, TN"},
            {"status": "On FedEx vehicle for delivery", "date": "2026-03-11 09:00", "location": "Destination City"},
            {"status": "Delivered", "date": "2026-03-11 14:00", "location": "Destination"},
        ],
    }

    history = failure_histories.get(failure_type, base_history)
    last_event = history[-1]

    return {
        "tracking_id": tracking_id,
        "status": last_event["status"],
        "status_date": last_event["date"],
        "history": history,
        "source": "mock",
    }


# ── FEDEX API ─────────────────────────────────────────────────────

def get_fedex_token() -> Optional[str]:
    """Get FedEx OAuth token."""
    if not FEDEX_API_KEY or not FEDEX_SECRET:
        return None
    try:
        response = httpx.post(
            FEDEX_TOKEN_URL,
            data={
                "grant_type": "client_credentials",
                "client_id": FEDEX_API_KEY,
                "client_secret": FEDEX_SECRET,
            },
            timeout=10,
        )
        if response.status_code == 200:
            return response.json().get("access_token")
    except Exception as e:
        print(f"[FedEx Token Error] {e}")
    return None


def fetch_fedex_history(tracking_id: str) -> Optional[dict]:
    """Fetch full tracking history from FedEx API."""
    token = get_fedex_token()
    if not token:
        return None

    try:
        response = httpx.post(
            FEDEX_TRACK_URL,
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
            },
            json={
                "includeDetailedScans": True,
                "trackingInfo": [{"trackingNumberInfo": {"trackingNumber": tracking_id}}],
            },
            timeout=15,
        )

        if response.status_code != 200:
            return None

        data = response.json()
        track_results = data.get("output", {}).get("completeTrackResults", [])
        if not track_results:
            return None

        track_result = track_results[0].get("trackResults", [{}])[0]
        events = track_result.get("scanEvents", [])

        history = []
        for event in events:
            history.append({
                "status": event.get("eventDescription", ""),
                "date": event.get("date", "") + " " + event.get("derivedStatusCode", ""),
                "location": event.get("scanLocation", {}).get("city", ""),
            })

        latest_status = track_result.get("latestStatusDetail", {})
        current_status = latest_status.get("description", "")
        current_date = events[0].get("date", "") if events else ""

        return {
            "tracking_id": tracking_id,
            "status": current_status,
            "status_date": current_date,
            "history": list(reversed(history)),  # chronological order
            "source": "fedex_api",
        }

    except Exception as e:
        print(f"[FedEx Track Error] {e}")
        return None


# ── SHIPPO / UPS API ──────────────────────────────────────────────

def fetch_ups_history(tracking_id: str) -> Optional[dict]:
    """Fetch UPS tracking via Shippo API."""
    if not SHIPPO_API_KEY:
        return None

    try:
        url = f"https://api.goshippo.com/tracks/ups/{tracking_id}"
        response = httpx.get(
            url,
            headers={"Authorization": f"ShippoToken {SHIPPO_API_KEY}"},
            timeout=15,
        )

        if response.status_code != 200:
            return None

        data = response.json()
        tracking_history = data.get("tracking_history", [])

        history = []
        for event in tracking_history:
            history.append({
                "status": event.get("status_details", ""),
                "date": event.get("status_date", ""),
                "location": event.get("location", {}).get("city", "") if event.get("location") else "",
            })

        current = data.get("tracking_status", {})
        return {
            "tracking_id": tracking_id,
            "status": current.get("status_details", ""),
            "status_date": current.get("status_date", ""),
            "history": history,
            "source": "shippo_api",
        }

    except Exception as e:
        print(f"[UPS/Shippo Track Error] {e}")
        return None


# ── MCP TOOLS ─────────────────────────────────────────────────────

def get_tracking_status(tracking_id: str, carrier: str = "",
                         ship_method: str = "") -> dict:
    """
    MCP Tool: Get current tracking status.
    Checks cache first, calls API if needed.

    Args:
        tracking_id: Carrier tracking number
        carrier: 'UPS' or 'FedEx' (auto-detected if not provided)
        ship_method: ship_method from Order API (helps carrier detection)

    Returns:
        dict with status, status_date, source
    """
    # Detect carrier
    if not carrier:
        carrier = detect_carrier(tracking_id, ship_method)

    # Check cache first
    cached = get_cached(tracking_id)
    if cached:
        print(f"[Cache HIT] {tracking_id}")
        return cached

    print(f"[MCP Call] get_tracking_status: {tracking_id} ({carrier})")

    # Check if we're in synthetic/test mode
    run_mode = os.getenv("RUN_MODE", "synthetic")
    if run_mode == "synthetic" or (not SHIPPO_API_KEY and not FEDEX_API_KEY):
        result = get_mock_history(tracking_id)
        save_to_cache(tracking_id, carrier, result["status"],
                      result["status_date"], result["history"])
        return result

    # Call real API
    result = None
    if carrier == "UPS":
        result = fetch_ups_history(tracking_id)
    else:
        result = fetch_fedex_history(tracking_id)

    if result:
        save_to_cache(tracking_id, carrier, result["status"],
                      result["status_date"], result["history"])
        return result

    # API failed — return error
    return {
        "tracking_id": tracking_id,
        "status": "ERROR",
        "status_date": "",
        "history": [],
        "source": "error",
        "error": "API call failed — check logs",
    }


def get_full_history(tracking_id: str, carrier: str = "",
                      ship_method: str = "") -> dict:
    """
    MCP Tool: Get complete tracking history trail.
    Same as get_tracking_status but emphasizes history.
    """
    return get_tracking_status(tracking_id, carrier, ship_method)


def get_delivery_proof(tracking_id: str, carrier: str = "") -> dict:
    """
    MCP Tool: Get proof of delivery details.
    Returns delivery timestamp, signature info if available.
    """
    history_data = get_tracking_status(tracking_id, carrier)
    history = history_data.get("history", [])

    # Find delivery event
    pod = {
        "tracking_id": tracking_id,
        "delivered": False,
        "delivery_date": None,
        "delivery_location": None,
        "signature": None,
    }

    for event in reversed(history):
        if "delivered" in event.get("status", "").lower():
            pod["delivered"] = True
            pod["delivery_date"] = event.get("date")
            pod["delivery_location"] = event.get("location")
            break

    return pod


# ── TEST ──────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("=" * 60)
    print("TEST 1: FedEx tracking (synthetic mode)")
    result = get_tracking_status("888604130589", "FedEx")
    print(f"Status: {result['status']}")
    print(f"Source: {result['source']}")
    print(f"History events: {len(result['history'])}")
    for event in result["history"]:
        print(f"  [{event['date']}] {event['status'][:60]}")

    print("\n" + "=" * 60)
    print("TEST 2: Same tracking (should hit cache)")
    result2 = get_tracking_status("888604130589", "FedEx")
    print(f"Source: {result2['source']}")

    print("\n" + "=" * 60)
    print("TEST 3: Delivery proof")
    pod = get_delivery_proof("888604130589")
    print(f"Delivered: {pod['delivered']}")
    print(f"Date: {pod['delivery_date']}")
