"""
followup_escalation.py - Agent 4 (LLM-powered)
Monitors claim responses, analyzes rejections, and resubmits with
adjusted framing. ~20 LLM calls/week.

Handles:
    - Rejection analysis (why was it rejected?)
    - Resubmission drafting (adjusted tone + evidence)
    - Day 14 follow-up (no response yet)
    - Probability recalculation after rejection
    - Escalation to HITL when probability < 0.3
"""

import os
import json
from datetime import datetime, date, timedelta
from pathlib import Path
from typing import Optional
from anthropic import Anthropic
from dotenv import load_dotenv

from database.models import get_session, Claim, ClaimEmailLog, HitlQueue, ErrorLog
from agents.eligibility_assessor import calculate_probability

load_dotenv()

CONFIG       = json.loads(Path("config/system_config.json").read_text())
POLICIES     = json.loads(Path("config/carrier_policies.json").read_text())
CLAIM_AMOUNT = CONFIG.get("claim_amount", 100.0)
FILING_WINDOW= CONFIG.get("filing_window_days", 15)
FOLLOWUP_DAY = CONFIG.get("followup_day", 14)

PROB_HIGH    = CONFIG.get("probability_thresholds", {}).get("auto_resubmit", 0.6)
PROB_LOW     = CONFIG.get("probability_thresholds", {}).get("human_review", 0.3)

client = Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))


# ── RESUBMISSION TONE LADDER ──────────────────────────────────────

RESUBMISSION_TONES = {
    2: """Tone: Firm and assertive. 
- Explicitly challenge the rejection reason if it contradicts tracking evidence
- Cite the specific tracking event that proves carrier fault
- Add customer occasion context to humanize the claim
- Reference the prior rejection politely but directly
- End with a clear, specific ask""",

    3: """Tone: Formal escalation.
- Open by referencing this is the THIRD attempt
- Request escalation to a supervisor or claims manager
- Reference ALL prior claim IDs and rejection dates
- State that the rejection reasoning is unsupported by tracking evidence
- Demand a written explanation if rejecting again
- Professional but firm — this is the final attempt before human escalation"""
}


# ── RESPONSE ANALYSIS PROMPT ──────────────────────────────────────

def build_rejection_analysis_prompt(rejection_reason: str, original_claim: dict) -> str:
    """Build prompt to analyze rejection and plan resubmission strategy."""
    return f"""You are a shipping claims specialist analyzing a carrier rejection.

## Original Claim
- Tracking ID: {original_claim.get('tracking_id')}
- Carrier: {original_claim.get('carrier')}
- Claim type: {original_claim.get('claim_type')}
- Failure event: {original_claim.get('first_bad_event', 'Not specified')}
- Attempt number: {original_claim.get('attempt_number', 1)}

## Carrier Rejection Reason
{rejection_reason}

## Your Task
Analyze this rejection and provide:
1. Is the rejection valid or can it be challenged?
2. What specific counter-argument should we make?
3. What additional evidence should we emphasize?
4. What is your assessment of success probability for resubmission? (0.0 to 1.0)

Return ONLY valid JSON:
{{
  "rejection_valid": false,
  "can_challenge": true,
  "counter_argument": "explanation here",
  "additional_evidence": "what to emphasize",
  "resubmission_probability": 0.0,
  "reasoning": "brief explanation"
}}"""


def build_resubmission_prompt(state: dict, rejection_analysis: dict) -> str:
    """Build prompt for resubmission email."""
    claim         = state.get("claim", {})
    original_body = state.get("original_email_body", "")
    rejection     = state.get("rejection_reason", "")
    attempt       = state.get("attempt_number", 2)
    prior_ids     = state.get("prior_claim_ids", [])
    occasion      = state.get("occasion_type", "General")
    carrier       = claim.get("carrier", "FedEx")
    track_id      = claim.get("tracking_id", "")

    tone = RESUBMISSION_TONES.get(attempt, RESUBMISSION_TONES[2])
    policy_ref = "FedEx Money-Back Guarantee" if carrier == "FedEx" else "UPS Service Guarantee / GSR"

    prior_ids_text = ""
    if prior_ids:
        prior_ids_text = f"Prior claim reference(s): {', '.join(str(i) for i in prior_ids)}"

    occasion_text = ""
    if occasion and occasion != "General":
        occasion_text = f"Customer occasion: {occasion} — time-sensitive delivery"

    return f"""You are a shipping claims specialist. Write a CONCISE resubmission (under 150 words).

Context:
- Tracking ID: {track_id}
- Carrier: {carrier}, Claim type: {claim.get('claim_type', 'UNKNOWN')}
- Attempt #{attempt}. Rejection reason: {rejection}
- Counter-argument: {rejection_analysis.get('counter_argument', '')}
- Additional evidence: {rejection_analysis.get('additional_evidence', '')}

Format EXACTLY like this (one paragraph, no headers, no bullets, no legal language, NO threats of escalation/regulatory/legal action):

Dear {carrier} Claims Team,

[ONE firm paragraph: reference prior rejection, challenge it with tracking evidence, state carrier fault, demand refund under {policy_ref}. Max 150 words.]

Regards,
REBLOOM Logistics

Return ONLY valid JSON:
{{
  "subject": "Re: Service Guarantee Claim — {track_id}",
  "body": "Dear [carrier] Claims Team,\n\n[paragraph]\n\nRegards,\nREBLOOM Logistics",
  "policy_reference": "{policy_ref}",
  "confidence_score": 0.0
}}"""


def build_followup_prompt(claim: dict, days_remaining: int) -> str:
    """Build prompt for Day 14 follow-up when no response received."""
    carrier   = claim.get("carrier", "FedEx")
    track_id  = claim.get("tracking_id", "")
    claim_id  = claim.get("claim_id", "")
    filed_at  = claim.get("filed_at", "")
    policy_ref = "FedEx Money-Back Guarantee" if carrier == "FedEx" else "UPS Service Guarantee / GSR"

    return f"""You are a shipping claims specialist writing a follow-up email.

A claim was filed {filed_at} and has received NO response from the carrier.
The filing window closes in {days_remaining} day(s).

## Claim Details
- Tracking ID: {track_id}
- Carrier: {carrier}
- Claim type: {claim.get('claim_type', 'UNKNOWN')}
- Original claim reference: {claim_id}
- Claim amount: ${CLAIM_AMOUNT:.2f}

## Task
Write a polite but firm follow-up email requesting status of the pending claim.
- Reference the original claim date and tracking ID
- Note the urgency given the filing window
- Request confirmation of receipt and timeline for resolution
- Professional and concise tone

Return ONLY valid JSON:
{{
  "subject": "Follow-Up: Pending Claim — Track ID: {track_id}",
  "body": "full follow-up email body",
  "policy_reference": "{policy_ref}",
  "confidence_score": 0.6
}}"""


# ── CORE FUNCTIONS ────────────────────────────────────────────────

def analyze_rejection(rejection_reason: str, claim: dict) -> dict:
    """Use LLM to analyze rejection and plan counter-strategy."""
    print(f"[Follow-Up] Analyzing rejection for claim {claim.get('claim_id')}")

    prompt = build_rejection_analysis_prompt(rejection_reason, claim)

    try:
        response = client.messages.create(
            model      = "claude-sonnet-4-6",
            max_tokens = 800,
            messages   = [{"role": "user", "content": prompt}]
        )

        raw = response.content[0].text.strip()
        if raw.startswith("```"):
            raw = raw.split("```")[1]
            if raw.startswith("json"):
                raw = raw[4:]
        return json.loads(raw.strip())

    except Exception as e:
        print(f"[Follow-Up] Rejection analysis error: {e}")
        return {
            "rejection_valid":          False,
            "can_challenge":            True,
            "counter_argument":         "The rejection reason does not align with tracking evidence",
            "additional_evidence":      "See tracking history for documented carrier fault",
            "resubmission_probability": 0.5,
            "reasoning":                "Fallback analysis",
        }


def draft_resubmission(state: dict) -> dict:
    """Draft resubmission email after rejection."""
    claim      = state.get("claim", {})
    rejection  = state.get("rejection_reason", "")
    track_id   = claim.get("tracking_id", "UNKNOWN")
    attempt    = state.get("attempt_number", 2)

    print(f"[Follow-Up] Drafting resubmission for {track_id} (attempt {attempt})")

    # First analyze the rejection
    rejection_analysis = analyze_rejection(rejection, claim)
    print(f"[Follow-Up] Can challenge: {rejection_analysis.get('can_challenge')}, "
          f"New prob: {rejection_analysis.get('resubmission_probability', 0):.0%}")

    prompt = build_resubmission_prompt(state, rejection_analysis)

    try:
        response = client.messages.create(
            model      = "claude-sonnet-4-6",
            max_tokens = 1500,
            messages   = [{"role": "user", "content": prompt}]
        )

        raw = response.content[0].text.strip()
        if raw.startswith("```"):
            raw = raw.split("```")[1]
            if raw.startswith("json"):
                raw = raw[4:]

        draft = json.loads(raw.strip())
        draft["rejection_analysis"] = rejection_analysis
        return draft

    except Exception as e:
        print(f"[Follow-Up] Resubmission draft error: {e}")
        # Fallback resubmission
        carrier = claim.get("carrier", "FedEx")
        policy_ref = "FedEx Money-Back Guarantee" if carrier == "FedEx" else "UPS Service Guarantee / GSR"
        return {
            "subject": f"Resubmission — Track ID: {track_id} — Claim Ref: {claim.get('claim_id')}",
            "body": f"""Dear {carrier} Claims Team,

We are resubmitting our claim for shipment {track_id} following your rejection.

We respectfully disagree with the rejection. Our tracking records clearly show a 
documented carrier failure event that directly caused the delay.

We maintain our claim under the {policy_ref} for $100.00.

Prior claim reference: {claim.get('claim_id')}

Please review and reconsider.

Regards,
BloomDirect Logistics Team""",
            "policy_reference": policy_ref,
            "confidence_score": 0.5,
            "rejection_analysis": rejection_analysis,
        }


def draft_followup(claim: dict, days_remaining: int) -> dict:
    """Draft Day 14 follow-up when no response received."""
    track_id = claim.get("tracking_id", "UNKNOWN")
    print(f"[Follow-Up] Drafting Day 14 follow-up for {track_id}")

    prompt = build_followup_prompt(claim, days_remaining)

    try:
        response = client.messages.create(
            model      = "claude-sonnet-4-6",
            max_tokens = 800,
            messages   = [{"role": "user", "content": prompt}]
        )

        raw = response.content[0].text.strip()
        if raw.startswith("```"):
            raw = raw.split("```")[1]
            if raw.startswith("json"):
                raw = raw[4:]
        return json.loads(raw.strip())

    except Exception as e:
        print(f"[Follow-Up] Follow-up draft error: {e}")
        carrier    = claim.get("carrier", "FedEx")
        policy_ref = "FedEx Money-Back Guarantee" if carrier == "FedEx" else "UPS Service Guarantee"
        return {
            "subject": f"Follow-Up: Pending Claim — Track ID: {track_id}",
            "body": f"""Dear {carrier} Claims Team,

We are following up on our pending claim for shipment {track_id} filed on 
{claim.get('filed_at', 'N/A')}.

We have not yet received a response. Please confirm receipt and advise on 
expected resolution timeline. The filing window closes in {days_remaining} day(s).

Claim reference: {claim.get('claim_id')}
Claim amount: ${CLAIM_AMOUNT:.2f}

Thank you,
BloomDirect Logistics Team""",
            "policy_reference": policy_ref,
            "confidence_score": 0.6,
        }


# ── MAIN PROCESSING FUNCTION ──────────────────────────────────────

def process_rejection(claim_id: int, rejection_reason: str) -> dict:
    """
    Main entry point when a rejection is received.
    Recalculates probability and decides: resubmit or HITL.

    Returns:
        dict with action: 'resubmit' / 'hitl' / 'stop'
    """
    session = get_session()
    try:
        claim = session.query(Claim).filter(Claim.claim_id == claim_id).first()
        if not claim:
            return {"action": "error", "reason": f"Claim {claim_id} not found"}

        current_attempt = claim.attempt_number
        next_attempt    = current_attempt + 1

        # Recalculate probability for next attempt
        new_probability = calculate_probability(
            failure_type   = claim.claim_type,
            carrier        = claim.carrier,
            attempt_number = next_attempt,
        )

        print(f"[Follow-Up] Claim {claim_id} rejected. New probability: {new_probability:.0%}")

        # Decision based on probability
        if next_attempt > 3:
            # Max attempts reached
            action = "stop"
            reason = "Maximum attempts (3) reached"

        elif new_probability < PROB_LOW:
            # Low probability — route to HITL
            action = "hitl"
            reason = f"Probability {new_probability:.0%} below threshold after rejection"

        else:
            # Auto-resubmit
            action = "resubmit"
            reason = f"Probability {new_probability:.0%} — auto-resubmitting"

        # Update claim
        claim.attempt_number = next_attempt
        claim.probability    = new_probability
        claim.status         = f"rejected_attempt_{current_attempt}"
        session.commit()

        if action == "hitl":
            # Add to HITL queue
            hitl = HitlQueue(
                claim_id       = claim_id,
                tracking_id    = claim.tracking_id,
                reason         = reason,
                status         = "pending",
                days_remaining = _days_remaining(claim.ship_method, claim_id),
            )
            session.add(hitl)
            session.commit()

        return {
            "action":          action,
            "reason":          reason,
            "new_probability": new_probability,
            "next_attempt":    next_attempt,
            "claim_id":        claim_id,
        }

    finally:
        session.close()


def check_followup_needed(claim_id: int) -> Optional[dict]:
    """
    Check if Day 14 follow-up is needed for a claim.
    Called by hourly scheduler.

    Returns dict if follow-up needed, None otherwise.
    """
    session = get_session()
    try:
        claim = session.query(Claim).filter(Claim.claim_id == claim_id).first()
        if not claim or not claim.filed_at:
            return None

        # Check if already has a response
        if claim.status in ("approved", "rejected", "more_info"):
            return None

        # Calculate days since filed and days remaining in window
        ship_date = datetime.strptime(claim.ship_method, "%Y-%m-%d") if claim.ship_method and len(claim.ship_method) == 10 else None
        today     = date.today()

        # Use filed_at to determine follow-up need
        days_since_filed = (today - claim.filed_at.date()).days
        days_remaining   = _days_remaining(claim.ship_method, claim_id)

        if days_remaining == FILING_WINDOW - FOLLOWUP_DAY or days_remaining <= 1:
            return {
                "claim_id":       claim_id,
                "tracking_id":    claim.tracking_id,
                "carrier":        claim.carrier,
                "claim_type":     claim.claim_type,
                "filed_at":       str(claim.filed_at),
                "days_remaining": days_remaining,
                "first_bad_event":None,
            }
        return None

    finally:
        session.close()


def _days_remaining(ship_method: str, claim_id: int) -> int:
    """Calculate days remaining in filing window."""
    session = get_session()
    try:
        from database.models import Failure
        claim = session.query(Claim).filter(Claim.claim_id == claim_id).first()
        if not claim:
            return 0
        failure = session.query(Failure).filter(
            Failure.failure_id == claim.failure_id
        ).first()
        if not failure or not failure.ship_date:
            return 0
        ship_date     = datetime.strptime(failure.ship_date, "%Y-%m-%d").date()
        days_elapsed  = (date.today() - ship_date).days
        return max(0, FILING_WINDOW - days_elapsed)
    finally:
        session.close()


# ── TEST ──────────────────────────────────────────────────────────
if __name__ == "__main__":
    from database.models import init_db
    init_db()

    print("=" * 60)
    print("TEST 1: Rejection Analysis")
    test_claim = {
        "claim_id":       1,
        "tracking_id":    "888604130589",
        "carrier":        "FedEx",
        "claim_type":     "CARRIER_DELAY",
        "first_bad_event":"A mechanical failure has caused a delay.",
        "attempt_number": 1,
    }
    analysis = analyze_rejection(
        "We are unable to approve this claim as the delay was caused by weather conditions beyond our control.",
        test_claim
    )
    print(f"Can challenge: {analysis['can_challenge']}")
    print(f"Counter: {analysis['counter_argument']}")
    print(f"New probability: {analysis['resubmission_probability']:.0%}")

    print("\n" + "=" * 60)
    print("TEST 2: Resubmission Draft")
    resubmit_state = {
        "claim": {
            **test_claim,
            "ship_date":  "2026-03-10",
            "ship_method":"FEDEX International",
        },
        "original_email_body": "Original claim email body here...",
        "rejection_reason":    "Delay caused by weather conditions beyond our control.",
        "occasion_type":       "Birthday",
        "attempt_number":      2,
        "prior_claim_ids":     [1],
    }
    resubmit = draft_resubmission(resubmit_state)
    print(f"Subject: {resubmit['subject']}")
    print(f"Confidence: {resubmit['confidence_score']:.0%}")
    print(f"\nBody preview:\n{resubmit['body'][:400]}...")

    print("\n" + "=" * 60)
    print("TEST 3: Day 14 Follow-Up Draft")
    followup_claim = {
        "claim_id":   1,
        "tracking_id":"888604130589",
        "carrier":    "FedEx",
        "claim_type": "CARRIER_DELAY",
        "filed_at":   "2026-03-11 10:00",
    }
    followup = draft_followup(followup_claim, days_remaining=1)
    print(f"Subject: {followup['subject']}")
    print(f"\nBody preview:\n{followup['body'][:300]}...")
