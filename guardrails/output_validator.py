"""
output_validator.py - Output Guardrails
Validates LLM-generated claim emails before sending to carriers.
Checks: fact verification, PII, tone, format, policy reference.
"""

import re
import json
from pathlib import Path
from typing import Optional
from pydantic import BaseModel

# Load carrier policies for reference validation
POLICIES_PATH = Path("config/carrier_policies.json")
POLICIES = json.loads(POLICIES_PATH.read_text()) if POLICIES_PATH.exists() else {}

# ── TONE PATTERNS ─────────────────────────────────────────────────
# Aggressive/threatening language to flag
AGGRESSIVE_PATTERNS = [
    r"\bsue\b", r"\blawsuit\b", r"\blegal\s+action\b",
    r"\battorney\b", r"\blawyer\b", r"\bcourt\b",
    r"\bdemand\s+immediately\b", r"\bunacceptable\b",
    r"\bdisgusting\b", r"\boutrageous\b", r"\bterrible\b",
    r"\bworst\b", r"\bincompetent\b", r"\bnegligent\b",
    r"\bidiots?\b", r"\bstupid\b",
]

# Overly passive language to flag
PASSIVE_PATTERNS = [
    r"\bif\s+possible\b",
    r"\bwhenever\s+you\s+get\s+a\s+chance\b",
    r"\bsorry\s+to\s+bother\b",
    r"\bi\s+hope\s+this\s+is\s+okay\b",
    r"\bplease\s+don'?t\s+worry\b",
]

# PII patterns to detect raw personal data
PII_PATTERNS = [
    # Full name pattern removed — too aggressive, redacts carrier names and dates
    r"\b\d{3}[-.\s]?\d{3}[-.\s]?\d{4}\b",  # Phone numbers
    r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b",  # Emails
    r"\b\d{1,5}\s+[A-Za-z\s]+(?:Street|St|Avenue|Ave|Road|Rd|Drive|Dr|Lane|Ln|Blvd)\b",  # Addresses
]

# Required fields in claim email subject
REQUIRED_SUBJECT_KEYWORDS = ["track", "claim"]

# Carrier-specific required body elements
CARRIER_REQUIREMENTS = {
    "UPS": {
        "required_fields": ["tracking number", "ship date", "service guarantee"],
        "claim_policy": "UPS Service Guarantee / Guaranteed Service Refund",
    },
    "FedEx": {
        "required_fields": ["tracking number", "ship date", "money-back guarantee"],
        "claim_policy": "FedEx Money-Back Guarantee",
    }
}


# ── VALIDATION MODELS ─────────────────────────────────────────────
class ClaimEmailDraft(BaseModel):
    """Structure of a claim email draft from Agent 3."""
    subject:           str
    body:              str
    carrier:           str
    tracking_id:       str
    ship_date:         str
    claim_type:        str
    attempt_number:    int = 1
    policy_reference:  Optional[str] = None
    confidence_score:  Optional[float] = None


class OutputValidationResult(BaseModel):
    """Result of output validation."""
    valid:           bool
    action:          str  # send / block / rewrite / flag_human
    issues:          list[str] = []
    warnings:        list[str] = []
    sanitized_body:  Optional[str] = None
    sanitized_subject: Optional[str] = None


# ── VALIDATION FUNCTIONS ──────────────────────────────────────────

def check_fact_accuracy(draft: ClaimEmailDraft) -> list[str]:
    """
    Verify that tracking ID, ship date appear correctly in email.
    Returns list of issues found.
    """
    issues = []
    body_lower = draft.body.lower()
    subject_lower = draft.subject.lower()

    # Check tracking ID appears in email
    if draft.tracking_id.lower() not in body_lower and draft.tracking_id.lower() not in subject_lower:
        issues.append(f"Tracking ID {draft.tracking_id} not found in email")

    # Check ship date appears (at least partial)
    ship_year = draft.ship_date[:4]
    if ship_year not in draft.body:
        issues.append(f"Ship date year {ship_year} not found in email body")

    return issues


def check_pii(body: str, tracking_id: str) -> tuple[bool, str]:
    """
    Detect PII in email body.
    Returns (has_pii, sanitized_body)
    """
    has_pii = False
    sanitized = body

    for pattern in PII_PATTERNS:
        matches = re.findall(pattern, body)
        if matches:
            # Don't flag the tracking ID itself as PII
            real_matches = [m for m in matches if tracking_id not in m]
            if real_matches:
                has_pii = True
                # Redact the matches
                for match in real_matches:
                    sanitized = sanitized.replace(match, "[REDACTED]")

    return has_pii, sanitized


def check_tone(body: str, attempt_number: int) -> tuple[str, list[str]]:
    issues = []
    body_lower = body.lower()

    aggressive_words = [
        "unacceptable", "lawsuit", "legal action", "attorney", "lawyer",
        "court", "incompetent", "negligent", "disgusting", "outrageous",
        "terrible service", "worst service", "idiots", "stupid"
    ]

    for word in aggressive_words:
        if word in body_lower:
            issues.append(f"Aggressive language detected: '{word}'")

    demand_patterns = [r"\bi demand\b", r"\bsue\b", r"\bdemand immediately\b"]
    for pattern in demand_patterns:
        if re.search(pattern, body_lower):
            issues.append(f"Aggressive language: matches '{pattern}'")

    if attempt_number >= 2:
        if "if possible" in body_lower or "sorry to bother" in body_lower:
            issues.append(f"Overly passive for attempt {attempt_number}")

    if issues:
        return "aggressive", issues
    return "good", []

def check_format(draft: ClaimEmailDraft) -> list[str]:
    """
    Check carrier-specific format requirements.
    Returns list of issues.
    """
    issues = []

    # Subject must contain track ID
    if draft.tracking_id not in draft.subject:
        issues.append(f"Subject line missing tracking ID {draft.tracking_id}")

    # Subject should contain claim-related keyword
    subject_lower = draft.subject.lower()
    if not any(kw in subject_lower for kw in REQUIRED_SUBJECT_KEYWORDS):
        issues.append("Subject line missing 'claim' or 'track' keyword")

    # Body minimum length (too short = incomplete)
    if len(draft.body) < 100:
        issues.append(f"Email body too short ({len(draft.body)} chars) — likely incomplete")

    # Body maximum length (too long = unprofessional)
    if len(draft.body) > 3000:
        issues.append(f"Email body too long ({len(draft.body)} chars) — consider trimming")

    return issues


def check_policy_reference(draft: ClaimEmailDraft) -> list[str]:
    """
    Verify that cited policy clause exists in carrier_policies.json.
    Returns list of issues.
    """
    issues = []
    warnings = []

    if not draft.policy_reference:
        issues.append("No policy reference cited in email")
        return issues

    carrier_policy = POLICIES.get(draft.carrier, {})
    if not carrier_policy:
        warnings.append(f"Carrier {draft.carrier} not found in carrier_policies.json")

    # Check that the right claim channel is being used
    claim_channel = carrier_policy.get("claim_channel", "")
    if draft.carrier == "UPS" and "shippo" not in draft.policy_reference.lower():
        pass  # OK — policy ref doesn't need to mention Shippo
    if draft.carrier == "FedEx" and "money-back" not in draft.policy_reference.lower() and \
       "guarantee" not in draft.policy_reference.lower():
        issues.append("FedEx email should reference Money-Back Guarantee")

    return issues


def rewrite_tone(body: str, issues: list[str]) -> str:
    """
    Basic tone rewriting — replace flagged aggressive phrases.
    For serious issues, blocks and routes to human instead.
    """
    rewritten = body

    # Simple replacements for common aggressive phrases
    replacements = {
        r"\bthis is unacceptable\b": "this situation requires resolution",
        r"\bI demand\b": "I respectfully request",
        r"\byou must\b": "we request that you",
        r"\bimmediately\b": "promptly",
        r"\bterrible service\b": "service failure",
        r"\bincompetent\b": "unable to meet service commitments",
    }

    for pattern, replacement in replacements.items():
        rewritten = re.sub(pattern, replacement, rewritten, flags=re.IGNORECASE)

    return rewritten


# ── MAIN VALIDATION ENTRY POINT ───────────────────────────────────

def validate_output(draft: ClaimEmailDraft) -> OutputValidationResult:
    """
    Run all output guardrails on a claim email draft.

    Returns OutputValidationResult with action:
    - 'send'        → passes all checks, ready to send
    - 'rewrite'     → minor issues fixed automatically
    - 'block'       → critical issues, do not send
    - 'flag_human'  → needs human review
    """
    all_issues = []
    all_warnings = []
    sanitized_body = draft.body
    sanitized_subject = draft.subject
    action = "send"

    # 1. Fact verification
    fact_issues = check_fact_accuracy(draft)
    if fact_issues:
        all_issues.extend(fact_issues)
        action = "block"

    # 2. PII check
    has_pii, sanitized_body = check_pii(sanitized_body, draft.tracking_id)
    if has_pii:
        all_warnings.append("PII detected and redacted automatically")
        if action == "send":
            action = "rewrite"

    # 3. Tone check
    tone, tone_issues = check_tone(sanitized_body, draft.attempt_number)
    if tone == "aggressive":
        # Try automatic rewrite first
        sanitized_body = rewrite_tone(sanitized_body, tone_issues)
        # Re-check after rewrite
        tone2, tone_issues2 = check_tone(sanitized_body, draft.attempt_number)
        if tone2 == "aggressive":
            all_issues.extend(tone_issues2)
            action = "flag_human"
        else:
            all_warnings.append("Aggressive tone detected and auto-corrected")
            if action == "send":
                action = "rewrite"
    elif tone == "passive" and draft.attempt_number >= 2:
        all_warnings.extend(tone_issues)

    # 4. Format check
    format_issues = check_format(draft)
    if format_issues:
        all_issues.extend(format_issues)
        if action == "send":
            action = "flag_human"

    # 5. Policy reference check
    policy_issues = check_policy_reference(draft)
    if policy_issues:
        all_issues.extend(policy_issues)
        if action == "send":
            action = "flag_human"

    # Final decision
    valid = action in ("send", "rewrite")

    return OutputValidationResult(
        valid=valid,
        action=action,
        issues=all_issues,
        warnings=all_warnings,
        sanitized_body=sanitized_body,
        sanitized_subject=sanitized_subject,
    )


# ── TEST ──────────────────────────────────────────────────────────
if __name__ == "__main__":
    # Test 1: Good email
    good_draft = ClaimEmailDraft(
        subject="Claim Request — Track ID: 888604130589 — CARRIER_DELAY",
        body="""Dear FedEx Claims Team,

I am writing to file a claim for shipment 888604130589, shipped on 2026-03-10 
via FedEx Priority Overnight, which was delivered 2 days past the guaranteed 
delivery date due to a documented mechanical failure on your network.

Per the FedEx Money-Back Guarantee policy, we are entitled to a full refund 
of shipping charges when delivery misses the committed time due to a carrier 
service failure. The tracking history clearly shows a mechanical failure event 
on March 11, which directly caused the delay.

This shipment was intended for a birthday celebration, and the delay caused 
significant inconvenience to our customer.

Claim amount: $100.00
Tracking number: 888604130589
Ship date: 2026-03-10

We request resolution within 10 business days.

Regards,
BloomDirect Logistics Team""",
        carrier="FedEx",
        tracking_id="888604130589",
        ship_date="2026-03-10",
        claim_type="CARRIER_DELAY",
        attempt_number=1,
        policy_reference="FedEx Money-Back Guarantee",
        confidence_score=0.85
    )

    # Test 2: Email with aggressive tone
    aggressive_draft = ClaimEmailDraft(
        subject="Claim Request — Track ID: 398445223732 — LATE",
        body="""This is completely unacceptable. Your incompetent service destroyed 
our customer's experience. I demand an immediate refund for tracking number 
398445223732 shipped on 2026-03-05. We will take legal action if not resolved.
Ship date: 2026-03-05""",
        carrier="FedEx",
        tracking_id="398445223732",
        ship_date="2026-03-05",
        claim_type="LATE",
        attempt_number=1,
        policy_reference="FedEx Money-Back Guarantee",
    )

    print("=" * 60)
    print("TEST 1: Good email")
    result1 = validate_output(good_draft)
    print(f"Action: {result1.action}")
    print(f"Valid: {result1.valid}")
    print(f"Issues: {result1.issues}")
    print(f"Warnings: {result1.warnings}")

    print("\n" + "=" * 60)
    print("TEST 2: Aggressive tone")
    result2 = validate_output(aggressive_draft)
    print(f"Action: {result2.action}")
    print(f"Valid: {result2.valid}")
    print(f"Issues: {result2.issues}")
    print(f"Warnings: {result2.warnings}")
