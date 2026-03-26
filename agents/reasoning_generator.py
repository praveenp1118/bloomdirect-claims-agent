"""
reasoning_generator.py — Reasoning Generator
Lightweight LLM agent that analyses tracking history and generates:
  - short_label: brief one-line summary (e.g. "Late 2 days — Mechanical delay")
  - narrative: 2-3 sentence explanation of why this is a valid claim

Zero email content — purely analytical reasoning.
Used by:
  - pipeline.py (Auto Generate / Auto Send modes) — node_generate_reasoning
  - dashboard (Manual mode) — user clicks Generate Reasoning button
"""

import os
import json
from typing import Optional
from dotenv import load_dotenv

load_dotenv()


def generate_reasoning(
    tracking_id: str,
    carrier: str,
    ship_method: str,
    ship_date: str,
    failure_type: str,
    delay_days: int,
    first_bad_event: Optional[str],
    promised_date: Optional[str],
    delivered_date: Optional[str],
    tracking_history: Optional[list] = None,
    occasion_type: Optional[str] = None,
) -> dict:
    """
    Generate claim reasoning narrative using LLM.

    Returns:
        {
            "short_label": "Late 2 days — Mechanical delay",
            "narrative": "Full explanation...",
            "success": True/False
        }
    """
    try:
        import anthropic
        client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))

        # Build history summary if available
        history_summary = ""
        if tracking_history:
            sorted_history = sorted(tracking_history, key=lambda x: str(x.get("date", "")))
            events = []
            for e in sorted_history[:10]:  # max 10 events to keep prompt short
                date  = str(e.get("date", ""))[:16]
                status = str(e.get("status", ""))
                loc   = str(e.get("location", "") or "")
                events.append(f"  {date} — {status}" + (f" ({loc})" if loc else ""))
            history_summary = "\n".join(events)

        occasion_line = ""
        if occasion_type and occasion_type != "General":
            occasion_line = f"\nOccasion: {occasion_type} gift — customer impact is high"

        prompt = f"""You are a shipping claims analyst. Analyse this shipment and generate a brief claim reasoning.

Shipment:
- Tracking ID: {tracking_id}
- Carrier: {carrier}
- Ship Method: {ship_method}
- Ship Date: {ship_date}
- Failure Type: {failure_type}
- Delay: {delay_days} day(s)
- First Bad Event: {first_bad_event or 'Unknown'}
- Promised Date: {promised_date or 'Unknown'}
- Delivered Date: {delivered_date or 'Unknown'}{occasion_line}

Tracking History (chronological):
{history_summary or 'Not available'}

Generate a JSON response with exactly these two fields:
1. "short_label": One line, max 50 chars. Format: "Late N day(s) — [reason]" or "Damage — [event]" etc.
2. "narrative": 2-3 sentences explaining why this is a valid claim. Include promised vs actual dates, the first bad event if known, and why the carrier is liable. Be factual and concise.

Respond ONLY with valid JSON, no markdown, no extra text."""

        response = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=300,
            messages=[{"role": "user", "content": prompt}]
        )

        text = response.content[0].text.strip()
        # Strip markdown if present
        if text.startswith("```"):
            text = text.split("```")[1]
            if text.startswith("json"):
                text = text[4:]
        text = text.strip()

        result = json.loads(text)
        return {
            "short_label": result.get("short_label", f"Late {delay_days} day(s)")[:50],
            "narrative":   result.get("narrative", ""),
            "success":     True,
        }

    except Exception as e:
        # Fallback — deterministic reasoning without LLM
        failure_map = {
            "LATE":          f"Late {delay_days} day(s)",
            "CARRIER_DELAY": f"Carrier delay — {first_bad_event[:30] if first_bad_event else 'mechanical/flight issue'}",
            "DAMAGE":        f"Package damaged — {first_bad_event[:30] if first_bad_event else 'in transit'}",
            "LOST":          "Package lost / missing",
            "WEATHER_DELAY": f"Weather delay — {delay_days} day(s)",
        }
        short_label = failure_map.get(failure_type, f"{failure_type} — {delay_days} day(s)")

        narrative_parts = [
            f"Shipment {tracking_id} via {carrier} ({ship_method}) shipped on {ship_date}."
        ]
        if promised_date and delivered_date:
            narrative_parts.append(
                f"Promised delivery: {promised_date}. Actual delivery: {delivered_date} — {delay_days} day(s) late."
            )
        if first_bad_event:
            narrative_parts.append(
                f"First issue detected: '{first_bad_event}'. This constitutes a carrier service failure eligible for a Money-Back Guarantee refund."
            )
        else:
            narrative_parts.append(
                "This constitutes a carrier service failure eligible for a Money-Back Guarantee refund."
            )

        return {
            "short_label": short_label,
            "narrative":   " ".join(narrative_parts),
            "success":     False,  # LLM failed, used fallback
        }


def generate_reasoning_from_claim(claim_dict: dict, tracking_history: Optional[list] = None) -> dict:
    """
    Convenience wrapper — takes a claim dict (from pipeline state or DB row).
    """
    return generate_reasoning(
        tracking_id      = str(claim_dict.get("tracking_id", "") or ""),
        carrier          = str(claim_dict.get("carrier", "") or ""),
        ship_method      = str(claim_dict.get("ship_method", "") or ""),
        ship_date        = str(claim_dict.get("ship_date", "") or ""),
        failure_type     = str(claim_dict.get("failure_type", "LATE") or "LATE"),
        delay_days       = int(claim_dict.get("delay_days", 1) or 1),
        first_bad_event  = claim_dict.get("first_bad_event"),
        promised_date    = claim_dict.get("promised_date"),
        delivered_date   = claim_dict.get("delivered_date"),
        tracking_history = tracking_history,
        occasion_type    = claim_dict.get("occasion_type"),
    )


if __name__ == "__main__":
    test = generate_reasoning(
        tracking_id     = "399539703429",
        carrier         = "FedEx",
        ship_method     = "FEDEX_GROUND",
        ship_date       = "2026-03-12",
        failure_type    = "LATE",
        delay_days      = 2,
        first_bad_event = "Mechanical failure has caused a delay",
        promised_date   = "2026-03-13",
        delivered_date  = "2026-03-14",
        occasion_type   = "Birthday",
    )
    print(f"short_label: {test['short_label']}")
    print(f"narrative:   {test['narrative']}")
    print(f"success:     {test['success']}")
