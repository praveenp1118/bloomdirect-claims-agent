"""
email_claims_mcp.py - MCP Server 2
Email/Claims MCP Server for BloomDirect Claims Recovery System.
Wraps Gmail API for claim filing and response monitoring.

Tools exposed:
    - send_claim_email(to, subject, body, claim_id)
    - check_email_response(thread_id)
    - get_thread_history(claim_id)
"""

import os
import json
import base64
import re
from datetime import datetime
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from typing import Optional
from dotenv import load_dotenv
from database.models import get_session, Claim, ClaimEmailLog

load_dotenv()

GMAIL_ADDRESS      = os.getenv("GMAIL_ADDRESS", "")
GMAIL_APP_PASSWORD = os.getenv("GMAIL_APP_PASSWORD", "")
RUN_MODE           = os.getenv("RUN_MODE", "synthetic")

# Email targets from config
CONFIG_PATH = "config/system_config.json"
try:
    with open(CONFIG_PATH) as f:
        CONFIG = json.load(f)
    EMAIL_MODE    = CONFIG.get("email", {}).get("mode", "test")
    EMAIL_TARGETS = CONFIG.get("email", {})
except Exception:
    EMAIL_MODE    = "test"
    EMAIL_TARGETS = {}


# ── EMAIL TARGET RESOLUTION ───────────────────────────────────────

def get_target_email(carrier: str) -> str:
    """
    Get the correct email target based on mode (test/production).
    Test mode → personal email
    Production mode → Shippo or FedEx direct
    """
    targets = EMAIL_TARGETS.get(EMAIL_MODE, {})
    if carrier == "UPS":
        return targets.get("ups_claims", GMAIL_ADDRESS)
    return targets.get("fedex_claims", GMAIL_ADDRESS)


# ── GMAIL SMTP SENDER ─────────────────────────────────────────────

def send_via_smtp(to: str, subject: str, body: str, in_reply_to: str = "", cc: str = "") -> dict:
    """Send email via Gmail SMTP using App Password."""
    import smtplib

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"]    = GMAIL_ADDRESS
    msg["To"]      = to
    if cc:
        msg["Cc"] = cc
    import uuid
    msg_id = f"<{uuid.uuid4()}@bloomdirect.claims>"
    msg["Message-ID"] = msg_id
    if in_reply_to:
        msg["In-Reply-To"] = in_reply_to
        msg["References"] = in_reply_to

    msg.attach(MIMEText(body, "plain"))

    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(GMAIL_ADDRESS, GMAIL_APP_PASSWORD)
            recipients = [to] + ([cc] if cc else [])
            server.sendmail(GMAIL_ADDRESS, recipients, msg.as_string())
        return {"success": True, "message": "Email sent via SMTP", "message_id": msg_id}
    except Exception as e:
        return {"success": False, "error": str(e)}


def send_via_gmail_api(to: str, subject: str, body: str) -> dict:
    """Send email via Gmail API (OAuth). Returns thread_id."""
    try:
        from google.oauth2.credentials import Credentials
        from googleapiclient.discovery import build

        creds_path = os.getenv("GMAIL_CREDENTIALS_PATH", "config/gmail_credentials.json")
        if not os.path.exists(creds_path):
            return {"success": False, "error": "Gmail credentials not found"}

        with open(creds_path) as f:
            creds_data = json.load(f)

        creds = Credentials.from_authorized_user_info(creds_data)
        service = build("gmail", "v1", credentials=creds)

        msg = MIMEText(body)
        msg["to"]      = to
        msg["from"]    = GMAIL_ADDRESS
        msg["subject"] = subject

        encoded = base64.urlsafe_b64encode(msg.as_bytes()).decode()
        result = service.users().messages().send(
            userId="me",
            body={"raw": encoded}
        ).execute()

        return {
            "success": True,
            "message_id": result.get("id"),
            "thread_id": result.get("threadId"),
        }
    except Exception as e:
        return {"success": False, "error": str(e)}


# ── MOCK EMAIL (synthetic/test without credentials) ───────────────

def send_mock_email(to: str, subject: str, body: str, claim_id: int) -> dict:
    """
    Mock email send for synthetic mode.
    Logs to DB and prints to console instead of actually sending.
    """
    mock_thread_id = f"mock_thread_{claim_id}_{datetime.now().strftime('%Y%m%d%H%M%S')}"
    print(f"\n[MOCK EMAIL SEND]")
    print(f"To:      {to}")
    print(f"Subject: {subject}")
    print(f"Body preview: {body[:200]}...")
    print(f"Thread ID: {mock_thread_id}")

    return {
        "success": True,
        "thread_id": mock_thread_id,
        "message_id": f"mock_msg_{claim_id}",
        "mode": "mock",
    }


# ── RESPONSE PARSING ──────────────────────────────────────────────

def classify_response(email_body: str) -> dict:
    """
    Classify carrier response using LLM with keyword fallback.
    Returns classification: APPROVED / REJECTED / MORE_INFO / UNKNOWN
    """
    try:
        from anthropic import Anthropic
        client = Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))
        prompt = f"""Classify this carrier claim response email into exactly one category.\n\nCategories:\n- APPROVED: Carrier approved the claim, credit/refund confirmed\n- REJECTED: Carrier denied/rejected the claim\n- MORE_INFO: Carrier needs additional information\n- UNKNOWN: Cannot determine\n\nEmail:\n---\n{email_body[:1500]}\n---\n\nReply with ONLY JSON:\n{{\"classification\": \"APPROVED|REJECTED|MORE_INFO|UNKNOWN\", \"rejection_reason\": \"reason if rejected else null\", \"confidence\": 0.9}}"""
        response = client.messages.create(model="claude-sonnet-4-6", max_tokens=200, messages=[{"role": "user", "content": prompt}])
        raw = response.content[0].text.strip()
        if raw.startswith("```"):
            raw = raw.split("```")[1]
            if raw.startswith("json"): raw = raw[4:]
            raw = raw.strip()
        result = json.loads(raw)
        print(f"[Email MCP] LLM classified: {result['classification']}")
        return result
    except Exception as e:
        print(f"[Email MCP] LLM classify failed ({e}), keyword fallback")
    body_lower = email_body.lower()
    rejection_keywords = ["denied", "rejected", "unable to approve", "not eligible", "does not qualify", "cannot be approved", "claim is denied", "not approved", "guarantee was suspended"]
    approval_keywords = ["credit has been applied", "refund has been processed", "claim has been approved", "we have approved", "credit issued", "refund issued"]
    more_info_keywords = ["additional information", "please provide", "documentation required", "need more details"]
    for kw in rejection_keywords:
        if kw in body_lower:
            return {"classification": "REJECTED", "matched_keyword": kw, "rejection_reason": extract_rejection_reason(email_body)}
    for kw in approval_keywords:
        if kw in body_lower:
            return {"classification": "APPROVED", "matched_keyword": kw}
    for kw in more_info_keywords:
        if kw in body_lower:
            return {"classification": "MORE_INFO", "matched_keyword": kw}
    return {"classification": "UNKNOWN", "matched_keyword": None}

    return {"classification": "UNKNOWN", "matched_keyword": None}


def extract_rejection_reason(email_body: str) -> str:
    """Extract the rejection reason from carrier email."""
    # Look for common reason patterns
    patterns = [
        r"because\s+(.{20,200}?)[\.\n]",
        r"reason[:\s]+(.{20,200}?)[\.\n]",
        r"denied\s+(?:because|due to)\s+(.{20,200}?)[\.\n]",
        r"weather[^\.]{0,100}",
        r"suspended[^\.]{0,100}",
    ]

    for pattern in patterns:
        match = re.search(pattern, email_body, re.IGNORECASE)
        if match:
            return match.group(0)[:200].strip()

    return "Reason not clearly stated in response"


def extract_carrier_case_id(email_body: str, subject: str) -> Optional[str]:
    """Try to extract carrier-assigned case/claim ID from response."""
    patterns = [
        r"case\s*(?:id|number|#)[:\s]*([A-Z0-9\-]{6,20})",
        r"claim\s*(?:id|number|#)[:\s]*([A-Z0-9\-]{6,20})",
        r"reference\s*(?:id|number|#)[:\s]*([A-Z0-9\-]{6,20})",
        r"ticket\s*(?:id|number|#)[:\s]*([A-Z0-9\-]{6,20})",
    ]

    for text in [email_body, subject]:
        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                return match.group(1)
    return None


# ── LOG EMAIL TO DB ───────────────────────────────────────────────

def log_email(claim_id: int, tracking_id: str, direction: str,
              subject: str, body: str, status: str,
              rejection_reason: str = None,
              recovered_amount: float = None) -> None:
    """Log sent or received email to claims_email_log table."""
    session = get_session()
    try:
        log = ClaimEmailLog(
            claim_id         = claim_id,
            tracking_id      = tracking_id,
            direction        = direction,
            timestamp        = datetime.now(),
            subject          = subject,
            body             = body,
            status           = status,
            rejection_reason = rejection_reason,
            recovered_amount = recovered_amount,
        )
        session.add(log)

        # Also update claim status
        claim = session.query(Claim).filter(Claim.claim_id == claim_id).first()
        if claim:
            claim.status = status
            claim.updated_at = datetime.now()
            if status == "filed":
                claim.filed = True
                claim.filed_at = datetime.now()

        session.commit()
    finally:
        session.close()


def update_claim_thread(claim_id: int, thread_id: str,
                         carrier_case_id: str = None) -> None:
    """Update claim with Gmail thread_id and carrier case_id."""
    session = get_session()
    try:
        claim = session.query(Claim).filter(Claim.claim_id == claim_id).first()
        if claim:
            claim.gmail_thread_id = thread_id
            if carrier_case_id:
                claim.carrier_case_id = carrier_case_id
            claim.updated_at = datetime.now()
            session.commit()
    finally:
        session.close()


# ── MCP TOOLS ─────────────────────────────────────────────────────

def send_claim_email(to: str, subject: str, body: str,
                      claim_id: int, carrier: str,
                      tracking_id: str, cc: str = "") -> dict:
    """
    MCP Tool: Send a claim email to carrier.

    Routes to:
    - Mock sender (synthetic mode or no credentials)
    - SMTP sender (Gmail App Password configured)
    - API sender (Gmail OAuth configured)

    Also logs to DB and updates claim status.

    Args:
        to:          Recipient email (resolved by get_target_email)
        subject:     Email subject (must contain tracking ID)
        body:        Email body text
        claim_id:    DB claim ID for logging
        carrier:     'UPS' or 'FedEx'
        tracking_id: Carrier tracking number

    Returns:
        dict with success, thread_id, mode
    """
    # Use the to address passed by dashboard (already resolved by dashboard)
    actual_to = to or get_target_email(carrier)
    print(f"[Email MCP] Sending to: {actual_to}")

    result = None

    # Synthetic or no credentials → mock
    if RUN_MODE == "synthetic" or (not GMAIL_ADDRESS or not GMAIL_APP_PASSWORD):
        result = send_mock_email(actual_to, subject, body, claim_id)

    # App Password available → SMTP
    elif GMAIL_APP_PASSWORD:
        # Check if this is a resubmission — get original Message-ID for threading
        original_msg_id = ""
        try:
            session_db = get_session()
            claim_obj = session_db.query(Claim).filter_by(claim_id=claim_id).first()
            if claim_obj and claim_obj.gmail_thread_id and claim_obj.gmail_thread_id.startswith("<"):
                original_msg_id = claim_obj.gmail_thread_id
            session_db.close()
        except Exception:
            pass
        result = send_via_smtp(actual_to, subject, body, in_reply_to=original_msg_id, cc=cc)
        if result["success"]:
            # Store Message-ID (not smtp_ prefix) for threading
            msg_id = result.get("message_id", f"smtp_{claim_id}_{datetime.now().strftime('%Y%m%d%H%M%S')}")
            result["thread_id"] = msg_id

    # Log to DB
    if result and result.get("success"):
        log_email(
            claim_id    = claim_id,
            tracking_id = tracking_id,
            direction   = "sent",
            subject     = subject,
            body        = body,
            status      = "filed",
        )
        thread_id = result.get("thread_id", "")
        if thread_id:
            update_claim_thread(claim_id, thread_id)

    return result or {"success": False, "error": "No sending method available"}


def check_email_response(thread_id: str, claim_id: int) -> dict:
    """
    MCP Tool: Check Gmail thread for carrier response.
    Called by hourly scheduler for all open claims.

    Args:
        thread_id: Gmail thread ID stored when claim was sent
        claim_id:  DB claim ID

    Returns:
        dict with has_response, classification, rejection_reason
    """
    # Synthetic mode → simulate no response (realistic)
    if RUN_MODE == "synthetic" or thread_id.startswith("mock_"):
        print(f"[Email MCP] check_response: {thread_id} (mock — no response)")
        return {
            "has_response": False,
            "thread_id": thread_id,
            "classification": None,
        }

    # IMAP check (App Password mode)
    if GMAIL_APP_PASSWORD and (thread_id.startswith("smtp_") or thread_id.startswith("<")):
        try:
            import imaplib
            import email as email_lib
            session_db = get_session()
            try:
                claim_obj = session_db.query(Claim).filter_by(claim_id=claim_id).first()
                search_tid = claim_obj.tracking_id if claim_obj else ""
            finally:
                session_db.close()
            if not search_tid:
                return {"has_response": False, "thread_id": thread_id}
            mail = imaplib.IMAP4_SSL("imap.gmail.com")
            mail.login(GMAIL_ADDRESS, GMAIL_APP_PASSWORD)
            mail.select("INBOX")
            status, messages = mail.search(None, f'SUBJECT "{search_tid}"')
            if status != "OK" or not messages[0]:
                mail.logout()
                return {"has_response": False, "thread_id": thread_id}
            msg_ids = messages[0].split()
            reply_msg = None
            for mid in reversed(msg_ids):
                s2, d2 = mail.fetch(mid, "(RFC822)")
                if s2 == "OK":
                    m2 = email_lib.message_from_bytes(d2[0][1])
                    sender = m2.get("From", "").lower()
                    if GMAIL_ADDRESS.lower() not in sender:
                        reply_msg = m2
                        break
            if not reply_msg:
                mail.logout()
                return {"has_response": False, "thread_id": thread_id}
            body = ""
            if reply_msg.is_multipart():
                for part in reply_msg.walk():
                    if part.get_content_type() == "text/plain":
                        body = part.get_payload(decode=True).decode("utf-8", errors="replace")
                        break
            else:
                body = reply_msg.get_payload(decode=True).decode("utf-8", errors="replace")
            subject = reply_msg.get("Subject", "")
            mail.logout()
            classification = classify_response(body)
            carrier_case_id = extract_carrier_case_id(body, subject)
            log_email(
                claim_id=claim_id, tracking_id=search_tid,
                direction="received", subject=subject, body=body,
                status=classification["classification"].lower(),
                rejection_reason=classification.get("rejection_reason"),
            )
            print(f"[Email MCP] IMAP reply found for {search_tid}: {classification['classification']}")
            return {
                "has_response": True, "thread_id": thread_id,
                "classification": classification["classification"],
                "rejection_reason": classification.get("rejection_reason"),
                "carrier_case_id": carrier_case_id,
                "raw_body": body[:500],
            }
        except Exception as e:
            print(f"[Email MCP] IMAP check error: {e}")
            return {"has_response": False, "error": str(e)}



    # Real Gmail API check
    try:
        from google.oauth2.credentials import Credentials
        from googleapiclient.discovery import build

        creds_path = os.getenv("GMAIL_CREDENTIALS_PATH", "config/gmail_credentials.json")
        if not os.path.exists(creds_path):
            return {"has_response": False, "error": "No credentials"}

        with open(creds_path) as f:
            creds_data = json.load(f)

        creds = Credentials.from_authorized_user_info(creds_data)
        service = build("gmail", "v1", credentials=creds)

        thread = service.users().threads().get(
            userId="me", id=thread_id
        ).execute()

        messages = thread.get("messages", [])
        if len(messages) <= 1:
            return {"has_response": False, "thread_id": thread_id}

        # Get the latest reply (not our sent email)
        latest = messages[-1]
        payload = latest.get("payload", {})
        body_data = payload.get("body", {}).get("data", "")
        if body_data:
            body = base64.urlsafe_b64decode(body_data).decode("utf-8", errors="replace")
        else:
            body = ""

        subject = ""
        for header in payload.get("headers", []):
            if header["name"] == "Subject":
                subject = header["value"]
                break

        # Classify response
        classification = classify_response(body)
        carrier_case_id = extract_carrier_case_id(body, subject)

        # Log response to DB
        log_email(
            claim_id         = claim_id,
            tracking_id      = "",
            direction        = "received",
            subject          = subject,
            body             = body,
            status           = classification["classification"].lower(),
            rejection_reason = classification.get("rejection_reason"),
        )

        if carrier_case_id:
            update_claim_thread(claim_id, thread_id, carrier_case_id)

        return {
            "has_response":    True,
            "thread_id":       thread_id,
            "classification":  classification["classification"],
            "rejection_reason":classification.get("rejection_reason"),
            "carrier_case_id": carrier_case_id,
            "raw_body":        body[:500],
        }

    except Exception as e:
        print(f"[Email MCP] check_response error: {e}")
        return {"has_response": False, "error": str(e)}


def get_thread_history(claim_id: int) -> list:
    """
    MCP Tool: Get full email thread history for a claim.
    Used by dashboard L3 mail log.

    Args:
        claim_id: DB claim ID

    Returns:
        List of email log entries for this claim
    """
    session = get_session()
    try:
        logs = session.query(ClaimEmailLog).filter(
            ClaimEmailLog.claim_id == claim_id
        ).order_by(ClaimEmailLog.timestamp).all()

        return [
            {
                "log_id":          log.log_id,
                "direction":       log.direction,
                "timestamp":       str(log.timestamp),
                "subject":         log.subject,
                "body_preview":    (log.body or "")[:200],
                "status":          log.status,
                "rejection_reason":log.rejection_reason,
                "recovered_amount":log.recovered_amount,
            }
            for log in logs
        ]
    finally:
        session.close()


# ── TEST ──────────────────────────────────────────────────────────
if __name__ == "__main__":
    # First create a test claim in DB
    from database.models import init_db, get_session, Order, Failure, Claim
    init_db()

    session = get_session()
    try:
        # Insert test order
        order = session.query(Order).filter(
            Order.partner_order_id == "TEST-001"
        ).first()
        if not order:
            order = Order(
                partner_order_id = "TEST-001",
                tracking_id      = "888604130589",
                ship_method      = "FEDEX International",
                ship_date        = "2026-03-10",
                carrier          = "FedEx",
                occasion_type    = "Birthday",
            )
            session.add(order)
            session.flush()

        # Insert test failure
        failure = Failure(
            partner_order_id = "TEST-001",
            tracking_id      = "888604130589",
            failure_type     = "CARRIER_DELAY",
            delay_days       = 2,
            first_bad_event  = "A mechanical failure has caused a delay.",
            ship_date        = "2026-03-10",
            promised_date    = "2026-03-11",
        )
        session.add(failure)
        session.flush()

        # Insert test claim
        claim = Claim(
            failure_id    = failure.failure_id,
            tracking_id   = "888604130589",
            carrier       = "FedEx",
            ship_method   = "FEDEX International",
            claim_type    = "CARRIER_DELAY",
            claim_amount  = 100.0,
            status        = "pending",
            attempt_number= 1,
            probability   = 0.85,
            occasion_type = "Birthday",
        )
        session.add(claim)
        session.commit()
        claim_id = claim.claim_id
        print(f"Test claim created: claim_id={claim_id}")

    finally:
        session.close()

    print("\n" + "=" * 60)
    print("TEST 1: Send claim email (mock mode)")
    result = send_claim_email(
        to          = "test@example.com",
        subject     = "Claim Request — Track ID: 888604130589 — CARRIER_DELAY",
        body        = """Dear FedEx Claims Team,

I am filing a claim for shipment 888604130589, shipped 2026-03-10 via FedEx 
International, which was delayed 2 days due to a documented mechanical failure.

Per the FedEx Money-Back Guarantee, we request a refund of $100.00.

Tracking: 888604130589
Ship date: 2026-03-10

Regards, BloomDirect""",
        claim_id    = claim_id,
        carrier     = "FedEx",
        tracking_id = "888604130589",
    )
    print(f"Success: {result['success']}")
    print(f"Thread ID: {result.get('thread_id')}")

    print("\n" + "=" * 60)
    print("TEST 2: Check for response (mock — should show no response)")
    response = check_email_response(
        thread_id = result.get("thread_id", ""),
        claim_id  = claim_id,
    )
    print(f"Has response: {response['has_response']}")

    print("\n" + "=" * 60)
    print("TEST 3: Get thread history")
    history = get_thread_history(claim_id)
    print(f"Email log entries: {len(history)}")
    for entry in history:
        print(f"  [{entry['direction']}] {entry['subject'][:50]} | {entry['status']}")

    print("\n" + "=" * 60)
    print("TEST 4: Response classifier")
    test_responses = [
        ("Your claim has been approved and credit will be applied.", "APPROVED"),
        ("We are unable to approve this claim. Weather conditions beyond our control caused the delay.", "REJECTED"),
        ("Please provide additional documentation to process your claim.", "MORE_INFO"),
    ]
    for body, expected in test_responses:
        result_cls = classify_response(body)
        status = "PASS" if result_cls["classification"] == expected else "FAIL"
        print(f"  [{status}] Expected: {expected} | Got: {result_cls['classification']}")
