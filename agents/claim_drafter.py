"""
claim_drafter.py - Agent 3 (LLM-powered)
Drafts persuasive carrier-specific claim emails using Claude/GPT.
~60 LLM calls/week. Uses few-shot examples and chain-of-thought.
"""

import os
import json
from pathlib import Path
from typing import Optional
from anthropic import Anthropic
from dotenv import load_dotenv

load_dotenv()

# Load configs
POLICIES     = json.loads(Path("config/carrier_policies.json").read_text())
CONFIG       = json.loads(Path("config/system_config.json").read_text())
CLAIM_AMOUNT = CONFIG.get("claim_amount", 100.0)

# Load prompt template
PROMPT_PATH = Path("prompts/claim_drafter_prompt.md")
PROMPT_TEMPLATE = PROMPT_PATH.read_text() if PROMPT_PATH.exists() else ""

client = Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))

# ── FEW SHOT EXAMPLES ─────────────────────────────────────────────

FEW_SHOT_EXAMPLES = {
    "FedEx": {
        "CARRIER_DELAY": """
Example approved claim (FedEx - Mechanical Failure):
Subject: Claim Request — Track ID: 888604130589 — Service Failure

Dear FedEx Claims Team,

I am filing a claim under the FedEx Money-Back Guarantee for shipment 888604130589.

Shipment details:
- Tracking number: 888604130589
- Ship date: 2026-02-10
- Service: FedEx International
- Guaranteed delivery: 2026-02-12
- Actual delivery: 2026-02-14 (2 days late)

Root cause per FedEx tracking: "A mechanical failure has caused a delay" — recorded 
on 2026-02-11 at Memphis hub. This is a documented carrier service failure, not a 
weather event or circumstances beyond FedEx's control.

This shipment was intended for a birthday celebration. The 2-day delay rendered the 
delivery meaningless for its intended purpose.

Per the FedEx Money-Back Guarantee policy, we respectfully request a full refund of 
shipping charges for this service failure.

Claim amount: $100.00
Reference: FedEx Money-Back Guarantee

Regards,
BloomDirect Logistics Team
""",
        "DAMAGE": """
Example approved claim (FedEx - Damage):
Subject: Claim Request — Track ID: 398629645141 — Package Damage

Dear FedEx Claims Team,

I am filing a damage claim for shipment 398629645141.

Shipment details:
- Tracking number: 398629645141
- Ship date: 2026-02-11
- Service: FedEx Ground

FedEx tracking shows: "A damage has been reported and we will notify the sender" 
on 2026-02-12. The receiver refused delivery due to visible damage to the package 
contents. This is a memorial arrangement — the occasion has passed and the customer 
cannot be served.

As the carrier responsible for package integrity during transit, FedEx is liable for 
this damage claim.

Claim amount: $100.00

Regards,
BloomDirect Logistics Team
"""
    },
    "UPS": {
        "CARRIER_DELAY": """
Example approved claim (UPS via Shippo - Mechanical Failure):
Subject: Claim Request — Track ID: 1ZK1V6600318414646 — GSR

Dear Shippo Claims Team,

I am requesting a Guaranteed Service Refund for UPS shipment 1ZK1V6600318414646.

Shipment details:
- Tracking number: 1ZK1V6600318414646
- Ship date: 2026-02-10
- Service: UPS Ground
- Guaranteed delivery: 2026-02-11
- Actual delivery: 2026-02-13 (2 days late)

UPS tracking shows: "A mechanical failure has caused a delay. We will update the 
delivery date as soon as possible" — recorded 2026-02-11. This is a clear carrier 
operational failure qualifying for GSR.

Customer ordered flowers for a Valentine's Day celebration. The delay caused complete 
loss of purpose for this time-sensitive shipment.

Per the UPS Service Guarantee / GSR policy, we request a full credit of shipping 
charges.

Claim amount: $100.00

Regards,
BloomDirect Logistics Team
"""
    }
}


# ── TONE INSTRUCTIONS BY ATTEMPT ──────────────────────────────────

TONE_INSTRUCTIONS = {
    1: "Write in a professional, factual tone. State the facts clearly. Make a direct ask.",
    2: "Write in a firm, assertive tone. Explicitly name the carrier's fault. Reference that this is a resubmission after rejection. Add the customer occasion to strengthen the argument.",
    3: "Write in a formal escalation tone. Request supervisor review. Reference all prior claim IDs. Demand a written explanation for the rejection. This is the final attempt before human escalation.",
}


# ── POLICY REFERENCE LOOKUP ───────────────────────────────────────

def get_policy_reference(carrier: str, claim_type: str) -> str:
    """Get the correct policy reference for this carrier and claim type."""
    if carrier == "FedEx":
        return "FedEx Money-Back Guarantee"
    elif carrier == "UPS":
        return "UPS Service Guarantee / Guaranteed Service Refund (GSR)"
    return "Carrier Service Guarantee"


def get_claim_channel(carrier: str) -> str:
    """Get where to address the claim."""
    if carrier == "UPS":
        return "Shippo Claims Team (support@shippo.com) — UPS labels purchased via Shippo"
    return "FedEx Claims Team (file.claim@fedex.com)"


# ── BUILD PROMPT ───────────────────────────────────────────────────

def build_prompt(state: dict) -> str:
    """Build the full prompt for the claim drafter."""
    order          = state["validated_order"]
    classification = state["classification"]
    eligibility    = state.get("eligibility", {})
    mcp_history    = state.get("mcp_history", [])

    carrier        = classification.get("carrier", "FedEx")
    claim_type     = classification.get("failure_type", "UNKNOWN")
    track_id       = order["track_id"]
    ship_date      = order["ship_date"]
    ship_method    = order["ship_method"]
    delay_days     = classification.get("delay_days", 0)
    first_bad_event= classification.get("first_bad_event", "")
    promised_date  = classification.get("promised_date", "")
    occasion       = classification.get("occasion_type", "General")
    attempt_number = state.get("attempt_number", 1)
    probability    = eligibility.get("probability", 0.5)

    # Get few-shot example
    example = FEW_SHOT_EXAMPLES.get(carrier, {}).get(claim_type, "")

    # Build history summary
    history_summary = ""
    if mcp_history:
        history_summary = "\n".join([
            f"  [{e.get('date', '')}] {e.get('status', '')[:80]}"
            for e in mcp_history[:10]
        ])

    # Tone instruction
    tone = TONE_INSTRUCTIONS.get(attempt_number, TONE_INSTRUCTIONS[1])

    # Policy reference
    policy_ref    = get_policy_reference(carrier, claim_type)
    claim_channel = get_claim_channel(carrier)

    prompt = f"""You are a shipping claims specialist for BloomDirect, a premium floral e-commerce company.
Draft a professional claim email for the following shipment failure.

## Claim Details
- Tracking ID: {track_id}
- Carrier: {carrier}
- Ship method: {ship_method}
- Ship date: {ship_date}
- Promised delivery: {promised_date}
- Failure type: {claim_type}
- Delay days: {delay_days}
- First bad event in history: {first_bad_event}
- Customer occasion: {occasion}
- Attempt number: {attempt_number}
- Claim amount: ${CLAIM_AMOUNT:.2f}
- Policy reference: {policy_ref}
- Address to: {claim_channel}
- Approval probability: {probability:.0%}

## Tracking History
{history_summary if history_summary else "Full history available in carrier system"}

## Tone Instructions
{tone}

## Few-Shot Example (approved claim in similar situation)
{example if example else "No example available for this claim type"}

## Chain-of-Thought Instructions
Before writing the email, reason through:
1. What is the specific carrier fault? Which tracking event proves it?
2. Which policy clause exactly applies?
3. What evidence is most compelling from the history?
4. How should the {occasion} occasion be mentioned? (use generic language only, never personal details)
5. What tone is right for attempt {attempt_number}?

## Output Requirements
Return ONLY a valid JSON object with these exact fields:
{{
  "subject": "Claim Request — Track ID: {track_id} — {claim_type}",
  "body": "full email body text here",
  "policy_reference": "{policy_ref}",
  "confidence_score": 0.0
}}

Do not include any text before or after the JSON object.
"""

    return prompt


# ── MAIN DRAFTER FUNCTION ─────────────────────────────────────────

def draft_claim_email(state: dict) -> dict:
    """
    Agent 3: Draft a persuasive claim email using Claude.

    Args:
        state: Pipeline state with validated_order, classification,
               eligibility, mcp_history, attempt_number

    Returns:
        dict with subject, body, policy_reference, confidence_score
    """
    order      = state.get("validated_order", {})
    track_id   = order.get("track_id", "UNKNOWN")
    carrier    = state.get("classification", {}).get("carrier", "FedEx")
    claim_type = state.get("classification", {}).get("failure_type", "UNKNOWN")

    print(f"[Claim Drafter] Drafting for {track_id} | {carrier} | {claim_type} | Attempt {state.get('attempt_number', 1)}")

    prompt = build_prompt(state)

    try:
        response = client.messages.create(
            model      = "claude-sonnet-4-6",
            max_tokens = 1500,
            messages   = [{"role": "user", "content": prompt}]
        )

        raw_text = response.content[0].text.strip()

        # Parse JSON response
        # Strip markdown code blocks if present
        if raw_text.startswith("```"):
            raw_text = raw_text.split("```")[1]
            if raw_text.startswith("json"):
                raw_text = raw_text[4:]
        raw_text = raw_text.strip()

        draft = json.loads(raw_text)

        print(f"[Claim Drafter] Draft generated. Confidence: {draft.get('confidence_score', 0):.0%}")
        return draft

    except json.JSONDecodeError as e:
        print(f"[Claim Drafter] JSON parse error: {e}")
        # Return fallback draft
        return build_fallback_draft(state)

    except Exception as e:
        print(f"[Claim Drafter] LLM error: {e}")
        return build_fallback_draft(state)


def build_fallback_draft(state: dict) -> dict:
    """Fallback draft when LLM fails — basic but complete."""
    order      = state.get("validated_order", {})
    classif    = state.get("classification", {})
    track_id   = order.get("track_id", "UNKNOWN")
    carrier    = classif.get("carrier", "FedEx")
    claim_type = classif.get("failure_type", "CLAIM")
    ship_date  = order.get("ship_date", "")
    ship_method= order.get("ship_method", "")
    delay_days = classif.get("delay_days", 0)
    event      = classif.get("first_bad_event", "")
    occasion   = classif.get("occasion_type", "General")
    policy_ref = get_policy_reference(carrier, claim_type)

    occasion_line = ""
    if occasion and occasion != "General":
        occasion_line = f"\nThis shipment was intended for a {occasion.lower()} occasion, making the delay particularly impactful for our customer.\n"

    body = f"""Dear {carrier} Claims Team,

I am filing a claim for shipment {track_id} under the {policy_ref}.

Shipment Details:
- Tracking number: {track_id}
- Ship date: {ship_date}
- Service: {ship_method}
- Failure type: {claim_type}
{f'- Delay: {delay_days} day(s) past guaranteed delivery' if delay_days else ''}
{f'- Documented failure event: {event}' if event else ''}
{occasion_line}
Per the {policy_ref}, we respectfully request a full refund of shipping charges for this service failure.

Claim amount: ${CLAIM_AMOUNT:.2f}

Regards,
BloomDirect Logistics Team"""

    return {
        "subject":          f"Claim Request — Track ID: {track_id} — {claim_type}",
        "body":             body,
        "policy_reference": policy_ref,
        "confidence_score": 0.5,
    }


# ── TEST ──────────────────────────────────────────────────────────
if __name__ == "__main__":
    from database.models import init_db
    init_db()

    test_state = {
        "validated_order": {
            "partner_order_id":       "TEST-DRAFTER-001",
            "ship_method":            "FEDEX International",
            "ship_date":              "2026-03-10",
            "track_id":               "888604130589",
            "last_track_status":      "Delivered",
            "last_track_status_date": "2026-03-14 23:59",
            "first_track_status":     "Picked up",
            "first_track_status_date":"2026-03-10 18:00",
            "gift_message":           "",
            "days_remaining":         8,
        },
        "classification": {
            "track_id":       "888604130589",
            "carrier":        "FedEx",
            "failure_type":   "CARRIER_DELAY",
            "delay_days":     2,
            "first_bad_event":"A mechanical failure has caused a delay. We will update the delivery date as soon as possible.",
            "promised_date":  "2026-03-12",
            "occasion_type":  "Birthday",
            "notes":          ["Mechanical failure on Day 2"],
        },
        "eligibility": {
            "eligible":    True,
            "probability": 0.85,
            "days_remaining": 8,
        },
        "mcp_history": [
            {"status": "Picked up",              "date": "2026-03-10 18:00", "location": "Origin"},
            {"status": "Arrived at FedEx hub",   "date": "2026-03-11 04:00", "location": "Memphis, TN"},
            {"status": "A mechanical failure has caused a delay.", "date": "2026-03-11 08:00", "location": "Memphis, TN"},
            {"status": "Departed FedEx hub",     "date": "2026-03-11 20:00", "location": "Memphis, TN"},
            {"status": "Delivered",              "date": "2026-03-14 14:30", "location": "Destination"},
        ],
        "attempt_number": 1,
    }

    print("=" * 60)
    print("TEST: Claim Drafter (LLM)")
    draft = draft_claim_email(test_state)

    print(f"\nSubject: {draft['subject']}")
    print(f"\nBody:\n{draft['body']}")
    print(f"\nPolicy: {draft['policy_reference']}")
    print(f"Confidence: {draft['confidence_score']:.0%}")
