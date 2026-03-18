"""
failure_classifier.py — Agent 1
Classifies each shipment as on-time, late, damaged, lost, or exception.
Deterministic — zero LLM tokens.
"""

import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

# Load carrier policies
POLICIES = json.loads(Path("config/carrier_policies.json").read_text())

# Status strings that indicate label created but not yet picked up
NOT_PICKED_UP_STATUSES = {
    "Shipment information sent to FedEx",
    "Order Processed: Ready for UPS",
    "Shipper created a label, UPS has not received the package yet.",
    "Invalid tracking number",
    "This tracking number cannot be found. Please check the number or contact the sender.",
}

# Status strings that contain damage indicators
DAMAGE_KEYWORDS = [
    "damage", "damaged", "broken", "missing merchandise",
    "all merchandise missing", "empty carton"
]

# Status strings that indicate carrier delay (golden cases)
CARRIER_DELAY_GOLDEN = [
    "mechanical failure",
    "late ups trailer",
    "late flight",
    "missed flight",
    "railroad mechanical",
    "flight cancellation",
    "incorrectly sorted",
]

# Status strings that indicate weather
WEATHER_KEYWORDS = [
    "weather", "storm", "hurricane", "blizzard", "severe weather",
    "weather delay", "emergency situation"
]

# Status strings that indicate address/receiver fault
NO_CLAIM_KEYWORDS = [
    "receiver refused", "receiver does not want",
    "incorrect address provided by", "address is incomplete",
    "street number is incorrect", "apartment number is either missing",
    "company or receiver name is incorrect",
]


def add_working_days(start_date: datetime, days: int) -> datetime:
    """Add N working days (Mon-Sat) skipping Sundays only."""
    current = start_date
    added = 0
    while added < days:
        current += timedelta(days=1)
        if current.weekday() != 6:  # 6 = Sunday
            added += 1
    return current


def get_promised_date(pickup_date: datetime, ship_method: str) -> datetime:
    """Calculate promised delivery date based on SLA."""
    sla_days = POLICIES.get(ship_method, {}).get("sla_working_days", 1)
    return add_working_days(pickup_date, sla_days)


def is_pickup_confirmed(first_track_status: str) -> bool:
    """Check if carrier has physically picked up the package."""
    return first_track_status.strip() not in NOT_PICKED_UP_STATUSES


def classify_from_status(last_status: str) -> str:
    """
    Quick classification from last_track_status alone.
    Returns: ON_TIME | LATE | DAMAGE | LOST | UNKNOWN | CALL_MCP
    """
    status_lower = last_status.lower()

    if last_status == "Delivered":
        return "DELIVERED"  # Need date comparison next

    # Damage indicators
    if any(kw in status_lower for kw in DAMAGE_KEYWORDS):
        return "DAMAGE"

    # Not yet delivered — need MCP for full history
    return "CALL_MCP"


def classify_shipment(order: dict, mcp_history: Optional[list] = None) -> dict:
    """
    Main classification function.

    Args:
        order: Single order dict from Order API
        mcp_history: List of tracking events from MCP (if already fetched)

    Returns:
        Classification result dict
    """
    ship_method = order["ship_method"]
    ship_date = datetime.strptime(order["ship_date"], "%Y-%m-%d")
    track_id = order["track_id"]
    last_status = order["last_track_status"]
    last_status_date_str = order["last_track_status_date"]
    first_status = order["first_track_status"]
    first_status_date_str = order["first_track_status_date"]
    gift_message = order.get("gift_message", "")

    result = {
        "track_id": track_id,
        "partner_order_id": order["partner_order_id"],
        "ship_method": ship_method,
        "ship_date": order["ship_date"],
        "carrier": POLICIES.get(ship_method, {}).get("carrier", "Unknown"),
        "failure_type": None,
        "claim_eligible": None,
        "needs_mcp": False,
        "occasion_type": infer_occasion(gift_message),
        "delay_days": 0,
        "first_bad_event": None,
        "notes": [],
    }

    # Step 1: Check pickup
    if not is_pickup_confirmed(first_status):
        result["needs_mcp"] = True
        result["notes"].append("Pickup not confirmed — call MCP to verify")
        return result

    # Use first_track_status_date as pickup date
    pickup_date = datetime.strptime(first_status_date_str[:10], "%Y-%m-%d")
    promised_date = get_promised_date(pickup_date, ship_method)
    result["promised_date"] = promised_date.strftime("%Y-%m-%d")

    # Step 2: Check if delivered
    if last_status == "Delivered":
        delivery_date = datetime.strptime(last_status_date_str[:10], "%Y-%m-%d")
        if delivery_date > promised_date:
            delay_days = (delivery_date - promised_date).days
            result["failure_type"] = "LATE"
            result["delay_days"] = delay_days
            result["notes"].append(f"Delivered {delay_days} day(s) late. Promised: {promised_date.date()}, Actual: {delivery_date.date()}")
        else:
            result["failure_type"] = "ON_TIME"
            result["notes"].append("Delivered on time — no claim needed")
        return result

    # Step 3: Not delivered — need MCP history
    result["needs_mcp"] = True
    result["notes"].append(f"Not delivered (status: {last_status}) — call MCP for history")
    return result


def classify_from_mcp_history(result: dict, history: list) -> dict:
    """
    Re-classify using full MCP history trail.
    Finds the FIRST bad event to determine fault.

    Args:
        result: Partial result from classify_shipment()
        history: Ordered list of tracking events from MCP

    Returns:
        Updated result dict
    """
    result["needs_mcp"] = False

    # Check if MCP shows delivered (Order API was stale)
    for event in reversed(history):
        if event.get("status", "").lower() == "delivered":
            delivery_date_str = event.get("date", "")
            delivery_date = datetime.strptime(delivery_date_str[:10], "%Y-%m-%d")
            promised_date = datetime.strptime(result["promised_date"], "%Y-%m-%d")
            if delivery_date > promised_date:
                result["failure_type"] = "LATE"
                result["delay_days"] = (delivery_date - promised_date).days
            else:
                result["failure_type"] = "ON_TIME"
            result["notes"].append("Status updated via MCP — Order API was stale")
            return result

    # Find first bad event in history
    for event in history:
        status = event.get("status", "").lower()
        date = event.get("date", "")

        # Damage
        if any(kw in status for kw in DAMAGE_KEYWORDS):
            result["failure_type"] = "DAMAGE"
            result["first_bad_event"] = event.get("status")
            result["notes"].append(f"Damage found in history on {date}")
            return result

        # Golden carrier delay
        if any(kw in status for kw in CARRIER_DELAY_GOLDEN):
            result["failure_type"] = "CARRIER_DELAY"
            result["first_bad_event"] = event.get("status")
            result["notes"].append(f"Carrier delay (golden case) on {date}: {event.get('status')}")
            return result

        # Weather
        if any(kw in status for kw in WEATHER_KEYWORDS):
            result["failure_type"] = "WEATHER_DELAY"
            result["first_bad_event"] = event.get("status")
            result["notes"].append(f"Weather delay on {date} — low probability but attempt")
            return result

        # Missing merchandise / lost
        if "missing" in status or "lost and found" in status:
            result["failure_type"] = "LOST"
            result["first_bad_event"] = event.get("status")
            result["notes"].append(f"Missing merchandise on {date}")
            return result

        # Address/receiver fault — no claim
        if any(kw in status for kw in NO_CLAIM_KEYWORDS):
            result["failure_type"] = "NO_CLAIM"
            result["first_bad_event"] = event.get("status")
            result["notes"].append(f"No claim — address/receiver issue: {event.get('status')}")
            return result

    # Unknown pattern
    result["failure_type"] = "UNKNOWN"
    result["notes"].append("Unknown failure pattern — no 5yr history match — route to HITL")
    return result


def infer_occasion(gift_message: str) -> str:
    """
    Infer occasion type from gift message.
    NEVER logs or returns the original message.
    """
    if not gift_message:
        return "General"

    msg_lower = gift_message.lower()

    if any(w in msg_lower for w in ["birthday", "cumpleaños", "bday"]):
        return "Birthday"
    if any(w in msg_lower for w in ["funeral", "loss", "passed", "memorial", "rest in peace",
                                      "sympathy", "difficult time", "sorry for your loss",
                                      "remember your", "thinking of you"]):
        return "Funeral"
    if any(w in msg_lower for w in ["valentine", "amor", "love", "sweetheart"]):
        return "Valentine"
    if any(w in msg_lower for w in ["anniversary", "years together", "years of marriage"]):
        return "Anniversary"
    if any(w in msg_lower for w in ["graduation", "graduate", "congratulations", "proud of you"]):
        return "Graduation"

    return "General"
