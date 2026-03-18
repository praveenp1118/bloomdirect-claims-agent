"""
eligibility_assessor.py — Agent 2
Determines claim eligibility based on carrier policies and business rules.
Deterministic — zero LLM tokens.
"""

import json
from datetime import datetime, date
from pathlib import Path
from typing import Optional

POLICIES = json.loads(Path("config/carrier_policies.json").read_text())
PEAK_DATES = json.loads(Path("config/peak_season_dates.json").read_text())
CONFIG = json.loads(Path("config/system_config.json").read_text())

BORDERLINE_THRESHOLD = CONFIG["probability_thresholds"]["borderline_eligibility"]
FILING_WINDOW = CONFIG["filing_window_days"]


def is_within_filing_window(ship_date_str: str) -> tuple[bool, int]:
    """
    Check if shipment is within 15-day filing window.
    Returns (is_within, days_remaining)
    """
    ship_date = datetime.strptime(ship_date_str, "%Y-%m-%d").date()
    today = date.today()
    days_elapsed = (today - ship_date).days
    days_remaining = FILING_WINDOW - days_elapsed
    return days_remaining > 0, days_remaining


def is_guarantee_suspended(carrier: str, ship_date_str: str, failure_type: str) -> bool:
    """
    Check if carrier guarantee is suspended on the ship date.
    Only affects LATE/CARRIER_DELAY claims — not DAMAGE or LOST.
    """
    if failure_type in ("DAMAGE", "LOST"):
        return False  # Guarantee suspension doesn't affect damage/lost claims

    carrier_key = carrier.lower()
    ship_date = datetime.strptime(ship_date_str, "%Y-%m-%d").date()

    for period_key, period in PEAK_DATES.get(carrier_key, {}).items():
        susp_start = datetime.strptime(period["suspension_start"], "%Y-%m-%d").date()
        susp_end = datetime.strptime(period["suspension_end"], "%Y-%m-%d").date()
        if susp_start <= ship_date <= susp_end:
            return True
    return False


def calculate_probability(failure_type: str, carrier: str, attempt_number: int,
                           historical_rate: Optional[float] = None) -> float:
    """
    Rule-based probability scoring.
    Weights: failure_type=40%, carrier=20%, attempt=20%, history=20%
    """
    # Failure type score (40%)
    failure_scores = {
        "CARRIER_DELAY": 0.85,  # Mechanical/flight — golden cases
        "DAMAGE": 0.70,
        "LOST": 0.65,
        "LATE": 0.40,
        "WEATHER_DELAY": 0.30,
        "UNKNOWN": 0.50,
    }
    ft_score = failure_scores.get(failure_type, 0.40)

    # Carrier score (20%)
    carrier_scores = {"FedEx": 0.70, "UPS": 0.55}
    carrier_score = carrier_scores.get(carrier, 0.60)

    # Attempt number score (20%)
    attempt_scores = {1: 0.50, 2: 0.65, 3: 0.45}
    attempt_score = attempt_scores.get(attempt_number, 0.40)

    # Historical outcome score (20%)
    hist_score = historical_rate if historical_rate is not None else 0.50

    probability = (
        ft_score * 0.40 +
        carrier_score * 0.20 +
        attempt_score * 0.20 +
        hist_score * 0.20
    )
    return round(min(max(probability, 0.0), 1.0), 2)


def assess_eligibility(classification: dict, attempt_number: int = 1,
                        historical_rate: Optional[float] = None) -> dict:
    """
    Assess claim eligibility for a classified shipment.

    Args:
        classification: Output from failure_classifier.classify_shipment()
        attempt_number: 1 for first filing, 2+ for resubmissions
        historical_rate: Approval rate from historical_claims.json (0-1)

    Returns:
        Eligibility result dict
    """
    result = {
        "track_id": classification["track_id"],
        "partner_order_id": classification["partner_order_id"],
        "ship_method": classification["ship_method"],
        "carrier": classification["carrier"],
        "failure_type": classification["failure_type"],
        "occasion_type": classification.get("occasion_type", "General"),
        "eligible": False,
        "eligibility_score": 0.0,
        "probability": 0.0,
        "days_remaining": 0,
        "hitl_required": False,
        "hitl_reason": None,
        "auto_file": False,
        "skip_reason": None,
        "notes": classification.get("notes", []),
    }

    failure_type = classification.get("failure_type")

    # No claim cases
    if failure_type in ("ON_TIME", "NO_CLAIM", None):
        result["eligible"] = False
        result["skip_reason"] = failure_type or "Unknown"
        return result

    # Check filing window
    within_window, days_remaining = is_within_filing_window(classification["ship_date"])
    result["days_remaining"] = days_remaining

    if not within_window:
        result["eligible"] = False
        result["skip_reason"] = "Filing window expired"
        result["notes"].append(f"Filing window closed ({FILING_WINDOW} days from ship date)")
        return result

    # Check guarantee suspension
    suspended = is_guarantee_suspended(
        classification["carrier"],
        classification["ship_date"],
        failure_type
    )
    if suspended and failure_type in ("LATE", "CARRIER_DELAY", "WEATHER_DELAY"):
        result["eligibility_score"] = 0.2
        result["notes"].append("Guarantee suspended during peak season — low probability but attempting")

    # Unknown pattern — always HITL
    if failure_type == "UNKNOWN":
        result["eligible"] = True
        result["hitl_required"] = True
        result["hitl_reason"] = "Unknown failure pattern — no historical precedent"
        result["probability"] = calculate_probability(failure_type, classification["carrier"],
                                                       attempt_number, historical_rate)
        return result

    # Calculate probability
    probability = calculate_probability(failure_type, classification["carrier"],
                                        attempt_number, historical_rate)
    result["probability"] = probability
    result["eligible"] = True

    # Check urgency — auto-file if ≤ 2 days left
    if days_remaining <= CONFIG["urgency_threshold_days"]:
        result["auto_file"] = True
        result["hitl_required"] = False
        result["notes"].append(f"URGENT: Only {days_remaining} day(s) left — auto-filing, bypassing HITL")
        return result

    # Check borderline eligibility
    eligibility_score = 1.0 if not suspended else 0.3
    result["eligibility_score"] = eligibility_score

    if eligibility_score <= BORDERLINE_THRESHOLD:
        result["hitl_required"] = True
        result["hitl_reason"] = f"Borderline eligibility score ({eligibility_score:.1f})"

    return result
