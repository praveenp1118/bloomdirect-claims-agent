"""
BloomDirect Claims Recovery System — Streamlit Dashboard
=========================================================

Tabs:
  1. 📊 Dashboard   — L1 summary → L2 compact table with email modal
  2. 🧑‍💼 HITL Queue  — Human-in-the-Loop approvals
  3. 📡 MCP 1 Log   — Carrier Tracking MCP activity (UPS/FedEx API calls)
  4. 📧 MCP 2 Log   — Email Claims MCP activity (Gmail send/receive)
  5. ⚠️ Errors      — Pipeline errors
  6. ⚙️ Settings    — System config

Key fixes in this version:
  - No auto-refresh (removed TTL from cache, no automatic reruns)
  - L1 filter stored in session_state, not reset by reruns
  - Email format: brief header + short paragraph, sender=REBLOOM, CC=logistics@arabellabouquets.com
  - MCP 1 Log: filter by ship_method + source + delivery status, Verify button
  - MCP 2 Log: filter by ship_method, full email thread view
  - L1 new columns: MCP 1 Calls (after On Time), MCP 2 Calls (after Avg Prob)

Email format:
  Tracking ID:  {id}
  Ship Date:    {date}
  Delivered:    {date}
  Delay:        {n} days
  Reason:       {type}
  ---
  Dear FedEx/UPS Claims Team,
  [1 short paragraph mentioning occasion if available]
  Regards, REBLOOM Logistics
  CC: logistics@arabellabouquets.com
"""

import hashlib
import json
import os
import subprocess
from datetime import datetime

import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
import streamlit as st
import streamlit.components.v1 as components
from sqlalchemy import create_engine, text


# ─────────────────────────────────────────────────────────────────────────────
# PATHS & CONFIG
# ─────────────────────────────────────────────────────────────────────────────

BASE_DIR    = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CONFIG_PATH = os.path.join(BASE_DIR, "config", "system_config.json")


def _default_config() -> dict:
    return {
        "auth": {
            "username": "Group_05",
            "password_hash": hashlib.sha256("BloomD@2026".encode()).hexdigest()
        },
        "probability": {"auto_resubmit_threshold": 0.6, "human_review_threshold": 0.3},
        "retry": {"max_attempts": 3},
        "filing_windows": {"ups_days": 15, "fedex_days": 15, "auto_file_days_remaining": 2},
        "claim_amount": 100.0,
        "email": {
            "mode": "manual", "env": "test",
            "test_address": "praveen.prakash.82@gmail.com",
            "sender": "praveenp.1118@gmail.com"
        },
        "scheduler": {"weekly_day": "Monday", "daily_enabled": True}
    }


def load_config() -> dict:
    if not os.path.exists(CONFIG_PATH):
        cfg = _default_config()
        os.makedirs(os.path.dirname(CONFIG_PATH), exist_ok=True)
        save_config(cfg)
        return cfg
    with open(CONFIG_PATH) as f:
        return json.load(f)


def save_config(cfg: dict):
    os.makedirs(os.path.dirname(CONFIG_PATH), exist_ok=True)
    with open(CONFIG_PATH, "w") as f:
        json.dump(cfg, f, indent=2)


# ─────────────────────────────────────────────────────────────────────────────
# DATABASE
# ─────────────────────────────────────────────────────────────────────────────

@st.cache_resource
def get_engine():
    db_url = os.getenv("DATABASE_URL", "sqlite:////app/data/bloomdirect.db")
    return create_engine(db_url, connect_args={"check_same_thread": False})


# ─────────────────────────────────────────────────────────────────────────────
# AUTH
# ─────────────────────────────────────────────────────────────────────────────

def _hash(pw: str) -> str:
    return hashlib.sha256(pw.encode()).hexdigest()


def check_credentials(username: str, password: str) -> bool:
    auth = load_config().get("auth", {})
    return (username == auth.get("username", "Group_05") and
            _hash(password) == auth.get("password_hash", ""))


def require_login(label: str = "perform this action") -> bool:
    if st.session_state.get("authenticated"):
        return True
    st.warning(f"🔒 Login required to {label}.")
    with st.form(key=f"login_{label}"):
        c1, c2, c3 = st.columns([2, 2, 1])
        user = c1.text_input("Username")
        pw   = c2.text_input("Password", type="password")
        c3.write(""); c3.write("")
        submit = c3.form_submit_button("Login")
    if submit:
        if check_credentials(user, pw):
            st.session_state["authenticated"] = True
            st.rerun()
        else:
            st.error("Invalid credentials.")
    return False


# ─────────────────────────────────────────────────────────────────────────────
# TRACKING URL HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def is_ups(tracking_id: str, carrier: str = "") -> bool:
    return tracking_id.upper().startswith("1Z") or "UPS" in carrier.upper()


def tracking_url(tracking_id: str, carrier: str = "") -> str:
    if is_ups(tracking_id, carrier):
        return f"https://www.ups.com/track?loc=en_in&tracknum={tracking_id}&requester=WT/trackdetails"
    return f"https://www.fedex.com/wtrk/track/?action=track&trackingnumber={tracking_id}&cntry_code=us&locale=en_US"


def tracking_link_html(tracking_id: str, carrier: str = "") -> str:
    url   = tracking_url(tracking_id, carrier)
    label = tracking_id[:16] + "…" if len(tracking_id) > 16 else tracking_id
    return f'<a href="{url}" target="_blank" style="font-size:12px;">{label}</a>'


# ─────────────────────────────────────────────────────────────────────────────
# DATE RESOLUTION
# ─────────────────────────────────────────────────────────────────────────────

SOURCE_ICON  = {"cache": "📡", "order_api": "🗃️", "mcp": "📡", "unknown": ""}
SOURCE_BADGE = {
    "mcp":       ("🟢 Live API",  "background:#EAF3DE;color:#3B6D11;"),
    "cache":     ("🟡 Cache",     "background:#FAEEDA;color:#854F0B;"),
    "order_api": ("🗃️ Order API", "background:#F1EFE8;color:#5F5E5A;"),
}

BADGE_BASE = "display:inline-block;font-size:10px;padding:2px 7px;border-radius:10px;font-weight:500;white-space:nowrap;"


def source_badge_html(source: str) -> str:
    label, sty = SOURCE_BADGE.get(source, ("❓ Unknown", "background:#F1EFE8;color:#5F5E5A;"))
    return f'<span style="{BADGE_BASE}{sty}">{label}</span>'


def resolve_date(cache_val, order_val) -> tuple:
    if cache_val and str(cache_val).strip() not in ("", "None", "nan"):
        return str(cache_val)[:10], "cache"
    if order_val and str(order_val).strip() not in ("", "None", "nan"):
        return str(order_val)[:10], "order_api"
    return "—", "unknown"


# ─────────────────────────────────────────────────────────────────────────────
# STATUS BADGES
# ─────────────────────────────────────────────────────────────────────────────

STATUS_STYLES  = {
    "approved": "background:#EAF3DE;color:#3B6D11;",
    "filed":    "background:#EAF3DE;color:#3B6D11;",
    "pending":  "background:#EEEDFE;color:#534AB7;",
    "rejected": "background:#FCEBEB;color:#A32D2D;",
    "hitl":     "background:#E6F1FB;color:#185FA5;",
    "hitl pending": "background:#E6F1FB;color:#185FA5;",
    "draft_pending_send": "background:#FAEEDA;color:#854F0B;",
}
FAILURE_STYLES = {
    "late":    "background:#FAEEDA;color:#854F0B;",
    "damage":  "background:#FCEBEB;color:#A32D2D;",
    "lost":    "background:#FCEBEB;color:#A32D2D;",
    "unknown": "background:#F1EFE8;color:#5F5E5A;",
}


def status_badge_html(val: str) -> str:
    v   = str(val or "pending").lower().strip()
    sty = STATUS_STYLES.get(v, "background:#F1EFE8;color:#5F5E5A;")
    return f'<span style="{BADGE_BASE}{sty}">{v.replace("_"," ").title()}</span>'


def failure_badge_html(val: str) -> str:
    v   = str(val or "unknown").lower().strip()
    sty = FAILURE_STYLES.get(v, "background:#F1EFE8;color:#5F5E5A;")
    return f'<span style="{BADGE_BASE}{sty}">{v.upper()}</span>'


def delivery_badge_html(is_late: bool) -> str:
    if is_late:
        return f'<span style="{BADGE_BASE}background:#FCEBEB;color:#A32D2D;">Not On Time</span>'
    return f'<span style="{BADGE_BASE}background:#EAF3DE;color:#3B6D11;">On Time</span>'


# ─────────────────────────────────────────────────────────────────────────────
# SESSION STATE
# ─────────────────────────────────────────────────────────────────────────────

def init_session_state():
    defaults = {
        "authenticated":    False,
        "l2_sm":            None,
        "l2_cat":           None,
        "open_modal_tid":   None,
        "open_reason_tid":  None,
        "show_legend":      False,
        "just_filtered":    False,
        # MCP 1 Log filters
        "mcp1_filter_sm":     None,
        "mcp1_filter_source": "All",
        "mcp1_filter_delivery": "All",
        "mcp1_verify_tid":    None,
        "mcp1_events_tid":    None,
        # MCP 2 Log filters
        "mcp2_filter_sm":     None,
    }
    for k, v in defaults.items():
        if k not in st.session_state:
            st.session_state[k] = v


def set_l2_filter(sm: str, cat: str):
    st.session_state["l2_sm"]        = sm
    st.session_state["l2_cat"]       = cat
    st.session_state["open_modal_tid"] = None
    st.session_state["open_reason_tid"] = None
    st.session_state["just_filtered"] = True


def clear_l2_filter():
    st.session_state["l2_sm"]  = None
    st.session_state["l2_cat"] = None


def set_mcp1_filter(sm: str):
    st.session_state["mcp1_filter_sm"]  = sm
    st.switch_page("pages/1_MCP_1_Log.py")


def set_mcp2_filter(sm: str):
    st.session_state["mcp2_filter_sm"]  = sm
    st.switch_page("pages/2_MCP_2_Log.py")


# ─────────────────────────────────────────────────────────────────────────────
# QUERIES
# ─────────────────────────────────────────────────────────────────────────────

@st.cache_data(ttl=300)
def q_l1() -> pd.DataFrame:
    sql = text("""
        SELECT
            o.ship_method,
            COUNT(DISTINCT o.tracking_id)                                     AS total,
            COUNT(DISTINCT CASE WHEN f.tracking_id IS NULL
                                THEN o.tracking_id END)                       AS on_time,
            COUNT(DISTINCT CASE WHEN f.tracking_id IS NOT NULL
                                THEN o.tracking_id END)                       AS not_on_time,
            COUNT(DISTINCT c.tracking_id)                                     AS eligible,
            ROUND(AVG(c.probability), 2)                                      AS avg_prob,
            COUNT(DISTINCT CASE WHEN tc.source = 'mcp'
                                THEN tc.tracking_id END)                      AS mcp1_calls,
            COUNT(DISTINCT CASE WHEN el2.direction = 'sent'
                                THEN el2.log_id END)                          AS mcp2_calls,
            COUNT(DISTINCT CASE WHEN c.status = 'filed'
                                THEN c.tracking_id END)                       AS filed,
            COUNT(DISTINCT CASE WHEN c.status = 'rejected'
                                THEN c.tracking_id END)                       AS rejected,
            COUNT(DISTINCT CASE WHEN c.status = 'approved'
                                THEN c.tracking_id END)                       AS approved,
            COUNT(DISTINCT CASE WHEN c.status IN ('filed','resubmitted','draft_pending_send')
                                THEN c.tracking_id END)                       AS awaiting
        FROM orders o
        LEFT JOIN failures         f   ON  f.tracking_id = o.tracking_id
        LEFT JOIN claims           c   ON  c.tracking_id = o.tracking_id
        LEFT JOIN tracking_cache   tc  ON tc.tracking_id = o.tracking_id
        LEFT JOIN claims_email_log el2 ON el2.tracking_id = o.tracking_id
        WHERE o.ship_method IS NOT NULL AND o.ship_method != ""
          AND o.ship_date >= date('now', '-20 days')
        GROUP BY o.ship_method
        ORDER BY o.ship_method
    """)
    with get_engine().connect() as conn:
        return pd.read_sql(sql, conn)


@st.cache_data(ttl=300)
def q_l2(ship_method, category) -> pd.DataFrame:
    clauses = ["o.ship_date >= date('now', '-20 days')"]
    params  = {}
    if ship_method:
        clauses.append("o.ship_method = :sm")
        params["sm"] = ship_method
    if category == "not_on_time":
        clauses.append("f.tracking_id IS NOT NULL")
    elif category == "on_time":
        clauses.append("f.tracking_id IS NULL")
    elif category == "eligible":
        clauses.append("c.claim_id IS NOT NULL")
    elif category == "filed":
        clauses.append("c.status = 'filed'")
    elif category == "rejected":
        clauses.append("c.status = 'rejected'")
    elif category == "approved":
        clauses.append("c.status = 'approved'")
    elif category == "awaiting":
        clauses.append("c.status IN ('filed','resubmitted','draft_pending_send')")

    sql = text(f"""
        SELECT
            o.tracking_id, o.ship_method, o.carrier, o.ship_date,
            o.partner_order_id AS order_number,
            tc.cached_status AS last_event_status,
            tc.cached_status_date AS cache_last_event,
            o.ship_date AS order_last_event,
            f.failure_type, f.delay_days,
            c.claim_id, c.status AS claim_status, c.probability,
            c.attempt_number, c.filed, c.short_label,
            c.llm_narrative, c.human_comment, c.draft_email_text,
            c.occasion_type, c.updated_at AS claim_updated_at,
            tc.last_mcp_call, tc.full_history_json,
            f.first_bad_event, f.promised_date,
            COUNT(DISTINCT el.log_id) AS email_count
        FROM orders o
        LEFT JOIN failures         f  ON  f.tracking_id = o.tracking_id
        LEFT JOIN tracking_cache   tc ON tc.tracking_id = o.tracking_id
        LEFT JOIN claims           c  ON  c.tracking_id = o.tracking_id
        LEFT JOIN claims_email_log el ON el.tracking_id = o.tracking_id
        WHERE {" AND ".join(clauses)}
        GROUP BY o.tracking_id
        ORDER BY o.ship_date DESC
    """)
    with get_engine().connect() as conn:
        return pd.read_sql(sql, conn, params=params)


@st.cache_data(ttl=300)
def q_mcp1(ship_method=None, source_filter=None, delivery_filter=None) -> pd.DataFrame:
    clauses = ["1=1"]
    params  = {}
    if ship_method:
        clauses.append("o.ship_method = :sm")
        params["sm"] = ship_method
    if source_filter and source_filter != "All":
        src_map = {"Live API": "mcp", "Cache": "cache", "Order API": "order_api"}
        clauses.append("tc.source = :src")
        params["src"] = src_map.get(source_filter, source_filter)
    if delivery_filter == "Not On Time":
        clauses.append("f.tracking_id IS NOT NULL")
    elif delivery_filter == "On Time":
        clauses.append("f.tracking_id IS NULL")

    sql = text(f"""
        SELECT
            tc.tracking_id, tc.carrier, tc.cached_status,
            tc.cached_status_date, tc.source, tc.last_mcp_call,
            tc.full_history_json,
            o.ship_method, o.ship_date,
            c.probability,
            CASE WHEN f.tracking_id IS NOT NULL THEN 1 ELSE 0 END AS is_late
        FROM tracking_cache tc
        LEFT JOIN orders o ON o.tracking_id = tc.tracking_id
        LEFT JOIN claims c ON c.tracking_id = tc.tracking_id
        LEFT JOIN failures f ON f.tracking_id = tc.tracking_id
        WHERE {" AND ".join(clauses)}
        ORDER BY tc.last_mcp_call DESC
    """)
    with get_engine().connect() as conn:
        return pd.read_sql(sql, conn, params=params)


@st.cache_data(ttl=300)
def q_mcp2(ship_method=None) -> pd.DataFrame:
    clauses = ["1=1"]
    params  = {}
    if ship_method:
        clauses.append("o.ship_method = :sm")
        params["sm"] = ship_method

    sql = text(f"""
        SELECT
            el.log_id, el.tracking_id, el.direction,
            el.timestamp, el.subject, el.body,
            el.status, el.rejection_reason, el.recovered_amount,
            o.ship_method, o.carrier, o.ship_date
        FROM claims_email_log el
        LEFT JOIN orders o ON o.tracking_id = el.tracking_id
        WHERE {" AND ".join(clauses)}
        ORDER BY el.timestamp DESC
    """)
    with get_engine().connect() as conn:
        return pd.read_sql(sql, conn, params=params)


@st.cache_data(ttl=60)
def q_emails(tracking_id: str) -> pd.DataFrame:
    sql = text("""
        SELECT log_id, direction, timestamp, subject, body,
               status, rejection_reason, recovered_amount
        FROM claims_email_log WHERE tracking_id = :tid ORDER BY timestamp ASC
    """)
    with get_engine().connect() as conn:
        return pd.read_sql(sql, conn, params={"tid": tracking_id})


@st.cache_data(ttl=60)
def q_hitl() -> pd.DataFrame:
    sql = text("""
        SELECT h.queue_id, h.claim_id, h.tracking_id, h.reason,
               h.status, h.days_remaining, h.created_at,
               o.ship_method, o.carrier, c.probability, c.attempt_number, f.failure_type
        FROM hitl_queue h
        LEFT JOIN orders   o ON o.tracking_id = h.tracking_id
        LEFT JOIN claims   c ON c.claim_id    = h.claim_id
        LEFT JOIN failures f ON f.tracking_id = h.tracking_id
            AND f.failure_id = (SELECT MIN(f2.failure_id) FROM failures f2 WHERE f2.tracking_id = h.tracking_id)
        WHERE h.status = 'pending'
        GROUP BY h.queue_id
        ORDER BY h.created_at ASC
    """)
    with get_engine().connect() as conn:
        return pd.read_sql(sql, conn)


@st.cache_data(ttl=60)
def q_errors() -> pd.DataFrame:
    sql = text("""
        SELECT error_id, tracking_id, error_type, stage, details, resolved, created_at
        FROM error_log ORDER BY created_at DESC LIMIT 200
    """)
    with get_engine().connect() as conn:
        return pd.read_sql(sql, conn)


# ─────────────────────────────────────────────────────────────────────────────
# SCROLL HELPER
# ─────────────────────────────────────────────────────────────────────────────

def scroll_to_l2():
    components.html("""
        <script>
        setTimeout(function() {
            var els = window.parent.document.querySelectorAll('[data-testid="stMarkdownContainer"]');
            for (var i = 0; i < els.length; i++) {
                if (els[i].textContent.includes('L2 —')) {
                    els[i].scrollIntoView({behavior: 'smooth', block: 'start'});
                    break;
                }
            }
        }, 400);
        </script>
    """, height=0)


# ─────────────────────────────────────────────────────────────────────────────
# EMAIL GENERATION
# ─────────────────────────────────────────────────────────────────────────────

def generate_email_draft(tracking_id: str, row: dict) -> str:
    """Generate evidence-based claim email citing carrier fault events."""
    carrier    = str(row.get("carrier", "FedEx") or "FedEx")
    ship_date  = str(row.get("ship_date", "") or "")
    delay_days = int(row.get("delay_days", 1) or 1)
    occasion   = str(row.get("occasion_type", "General") or "General")
    delivered  = str(row.get("cache_last_event", "") or "")[:10]
    failure    = str(row.get("failure_type", "LATE") or "LATE")
    promised   = str(row.get("promised_date", "") or "")
    first_bad  = str(row.get("first_bad_event", "") or "")
    ship_method= str(row.get("ship_method", "") or "")
    carrier_team = "UPS Claims Team" if "UPS" in carrier.upper() else "FedEx Claims Team"
    guarantee    = "UPS Service Guarantee" if "UPS" in carrier.upper() else "FedEx Money-Back Guarantee"

    # Extract and sort tracking history
    history = []
    try:
        hj = row.get("full_history_json", "")
        if hj:
            history = sorted(json.loads(hj), key=lambda x: str(x.get("date", "")))
    except Exception:
        pass

    # Find carrier-fault events
    FAULT_KEYWORDS = [
        "mechanical failure", "late trailer", "late flight", "missed flight",
        "railroad mechanical", "flight cancellation", "incorrectly sorted",
        "delay", "exception", "damage", "missing merchandise"
    ]
    fault_events = []
    for event in history:
        status = str(event.get("status", "")).lower()
        if any(kw in status for kw in FAULT_KEYWORDS):
            fault_events.append(event)

    # Build history summary (last 8 events)
    history_lines = []
    for e in history[-8:]:
        loc = " (" + str(e.get("location","")) + ")" if e.get("location") else ""
        history_lines.append("  " + str(e.get("date",""))[:16] + " - " + str(e.get("status","")) + loc)
    history_str = chr(10).join(history_lines) if history_lines else "Not available"

    # Build fault evidence string
    evidence_str = ""
    if fault_events:
        evidence_str = "Carrier-fault events:" + chr(10)
        for e in fault_events[:3]:
            loc = " at " + str(e.get("location","")) if e.get("location") else ""
            evidence_str += "  " + str(e.get("date",""))[:16] + loc + ": " + str(e.get("status","")) + chr(10)

    occasion_context = ""
    if occasion and occasion != "General":
        occasion_context = "This was a " + occasion + " gift order - the delay meant the customer missed their occasion entirely."

    try:
        import anthropic
        client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))

        prompt = """You are writing a firm, evidence-based shipping claim email on behalf of REBLOOM.

Shipment details:
- Tracking ID: """ + tracking_id + """
- Carrier: """ + carrier + " (" + ship_method + """)"
- Ship Date: """ + ship_date + """
- Promised Delivery: """ + (promised or "next working day") + """
- Actual Delivery: """ + delivered + """
- Delay: """ + str(delay_days) + """ day(s)
- Failure Type: """ + failure + """
""" + (occasion_context + chr(10) if occasion_context else "") + """
Full tracking history:
""" + history_str + """

""" + evidence_str + """
Write the claim email:

1. Details block:
Tracking ID:   """ + tracking_id + """
Ship Date:     """ + ship_date + """
Delivered:     """ + delivered + """
Delay:         """ + str(delay_days) + """ day(s) past promised date
Reason:        [short reason citing first fault event]
""" + ("Occasion:      " + occasion if occasion and occasion != "General" else "") + """

---

2. ONE firm paragraph (4-5 sentences):
   - State guaranteed vs actual delivery dates
   - Cite specific fault event by date and status text
   - State this is carrier-side failure not weather or shipper error
   - """ + (occasion_context if occasion_context else "State business impact") + """
   - Demand full refund, expect response in 5 business days

3. Sign off:
Regards,
REBLOOM Logistics

Rules: Address to """ + carrier_team + """. Firm not passive. No subject line. No markdown. Under 200 words."""

        response = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=500,
            messages=[{"role": "user", "content": prompt}]
        )
        return response.content[0].text

    except Exception as e:
        # Deterministic fallback using fault events
        if fault_events:
            fe   = fault_events[0]
            floc = " at " + str(fe.get("location","")) if fe.get("location") else ""
            evid = " Your tracking records confirm" + floc + " on " + str(fe.get("date",""))[:16] + ": " + str(fe.get("status","")) + "."
            if len(fault_events) > 1:
                evid += " (" + str(len(fault_events)) + " fault events recorded.)"
        elif first_bad:
            evid = " Your tracking records confirm: " + first_bad + "."
        else:
            evid = ""

        occ_line = " This was a " + occasion + " gift order and the delay meant the customer missed their occasion." if occasion and occasion != "General" else ""
        promised_line = "guaranteed delivery of " + promised if promised else "guaranteed delivery date"

        details = "Tracking ID:   " + tracking_id + chr(10)
        details += "Ship Date:     " + ship_date + chr(10)
        details += "Delivered:     " + delivered + chr(10)
        details += "Delay:         " + str(delay_days) + " day(s) past promised date" + chr(10)
        details += "Reason:        " + failure + (" - " + first_bad[:60] if first_bad else "") + chr(10)
        if occasion and occasion != "General":
            details += "Occasion:      " + occasion + chr(10)

        body = "Dear " + carrier_team + "," + chr(10) + chr(10)
        body += "Shipment " + tracking_id + " was shipped on " + ship_date + " under " + ship_method
        body += " with a " + promised_line + "." + evid
        body += " This constitutes a breach of the " + guarantee + " attributable to " + carrier
        body += " operations, not weather or shipper error." + occ_line
        body += " We formally request a full refund of all shipping charges and expect confirmation within 5 business days."
        body += chr(10) + chr(10) + "Regards," + chr(10) + "REBLOOM Logistics"

        return details + chr(10) + "---" + chr(10) + chr(10) + body


def _send_email(tracking_id: str, claim_id, draft: str, to_addr: str, cc_addr=None, carrier: str = ""):
    try:
        import sys
        if BASE_DIR not in sys.path:
            sys.path.insert(0, BASE_DIR)
        from mcp_servers.email_claims_mcp import send_claim_email
        send_claim_email(
            to=to_addr,
            cc=cc_addr or "",
            subject=f"Service Guarantee Claim — {tracking_id}",
            body=draft,
            claim_id=claim_id,
            carrier=carrier,
            tracking_id=tracking_id,
        )
        cc_info = f" (CC: {cc_addr})" if cc_addr else ""
        st.success(f"✅ Email sent to {to_addr}{cc_info}")
        st.cache_data.clear()
    except Exception as e:
        st.error(f"Failed to send: {e}")


# ─────────────────────────────────────────────────────────────────────────────
# TAB 1 — DASHBOARD
# ─────────────────────────────────────────────────────────────────────────────

def render_dashboard():
    l1 = q_l1()
    if l1.empty:
        st.info("No data yet. Run the pipeline first.")
        return

    # ── L1 ───────────────────────────────────────────────────────────────────
    st.markdown(
        "<div style='font-size:13px;font-weight:500;margin-bottom:2px;'>L1 — Summary by Ship Method (Last 20 Days)</div>"
        "<div style='font-size:11px;color:#888;margin-bottom:10px;'>Click any number to drill into L2. Click MCP columns to view logs.</div>",
        unsafe_allow_html=True
    )

    cols_def = [2, 0.7, 0.7, 1, 1.3, 0.9, 0.9, 0.9, 0.7, 0.7, 0.8, 0.8]
    headers  = ["Ship Method","Total","On Time","Not On Time",
                "Eligible For Claim","Avg Prob (Claim)","MCP 1 Calls","MCP 2 Calls","Filed","Rejected","Approved","Awaiting"]
    h = st.columns(cols_def)
    for col, label in zip(h, headers):
        col.markdown(f"<small><b style='color:#555;font-size:11px;'>{label}</b></small>", unsafe_allow_html=True)
    st.markdown("""

<style>
[data-testid="stSidebar"] {
    min-width: 160px !important;
    max-width: 160px !important;
}
[data-testid="stSidebarNav"] a {
    font-size: 13px !important;
    padding: 4px 8px !important;
}
</style>
""", unsafe_allow_html=True)
    st.markdown("<hr style='margin:4px 0 0;border:none;border-top:0.5px solid #ddd;'>", unsafe_allow_html=True)

    for _, row in l1.iterrows():
        sm   = str(row["ship_method"])
        cols = st.columns(cols_def)

        cols[0].markdown(f"<b style='font-size:12px;'>{sm}</b>", unsafe_allow_html=True)

        with cols[1]:
            if st.button(str(int(row["total"] or 0)), key=f"l1_tot_{sm}"):
                set_l2_filter(sm, "all"); st.rerun()
        with cols[2]:
            if st.button(str(int(row["on_time"] or 0)), key=f"l1_ot_{sm}"):
                set_l2_filter(sm, "on_time"); st.rerun()
        with cols[3]:
            if st.button(str(int(row["not_on_time"] or 0)), key=f"l1_not_{sm}"):
                set_l2_filter(sm, "not_on_time"); st.rerun()
        with cols[4]:
            if st.button(str(int(row["eligible"] or 0)), key=f"l1_elig_{sm}"):
                set_l2_filter(sm, "eligible"); st.rerun()
        prob = row.get("avg_prob")
        cols[5].markdown(
            f"<span style='font-size:12px;'>{f'{float(prob):.0%}' if prob and str(prob) not in ('nan','None') else chr(8212)}</span>",
            unsafe_allow_html=True
        )
        # MCP 1 Calls
        with cols[6]:
            mcp1 = int(row.get("mcp1_calls", 0) or 0)
            if st.button(str(mcp1), key=f"l1_mcp1_{sm}", help=f"View MCP 1 logs for {sm}"):
                set_mcp1_filter(sm); st.rerun()
        # MCP 2 Calls
        with cols[7]:
            mcp2 = int(row.get("mcp2_calls", 0) or 0)
            if st.button(str(mcp2), key=f"l1_mcp2_{sm}", help=f"View MCP 2 logs for {sm}"):
                set_mcp2_filter(sm); st.rerun()

        # Filed
        with cols[8]:
            filed = int(row.get("filed", 0) or 0)
            if st.button(str(filed), key=f"l1_filed_{sm}"):
                set_l2_filter(sm, "filed"); st.rerun()

        # Rejected
        with cols[9]:
            rejected = int(row.get("rejected", 0) or 0)
            if st.button(str(rejected), key=f"l1_rej_{sm}"):
                set_l2_filter(sm, "rejected"); st.rerun()

        # Approved
        with cols[10]:
            approved = int(row.get("approved", 0) or 0)
            if st.button(str(approved), key=f"l1_appr_{sm}"):
                set_l2_filter(sm, "approved"); st.rerun()

        # Awaiting Response
        with cols[11]:
            awaiting = int(row.get("awaiting", 0) or 0)
            if st.button(str(awaiting), key=f"l1_await_{sm}"):
                set_l2_filter(sm, "awaiting"); st.rerun()

    st.markdown("<hr style='margin:4px 0 16px;border:none;border-top:0.5px solid #ddd;'>", unsafe_allow_html=True)

    # ── L2 ───────────────────────────────────────────────────────────────────
    sm_f  = st.session_state["l2_sm"]
    cat_f = st.session_state["l2_cat"]

    if sm_f:
        cat_label = (cat_f or "all").replace("_", " ").title()
        hdr_col, btn_col = st.columns([5, 1])
        hdr_col.markdown(
            f"<div id='l2-section' style='font-size:13px;font-weight:500;'>L2 — {sm_f} → {cat_label}</div>",
            unsafe_allow_html=True
        )
        with btn_col:
            if st.button("✕ Clear"):
                clear_l2_filter(); st.rerun()
    else:
        st.markdown(
            "<div id='l2-section' style='font-size:13px;font-weight:500;margin-bottom:6px;'>L2 — All Shipments</div>",
            unsafe_allow_html=True
        )

    if st.session_state.get("just_filtered"):
        st.session_state["just_filtered"] = False
        scroll_to_l2()

    l2 = q_l2(sm_f, cat_f)
    if l2.empty:
        st.info("No records match this filter.")
        return

    PAGE_SIZE_L2 = 50
    total_l2 = len(l2)
    total_pages_l2 = max(1, (total_l2 + PAGE_SIZE_L2 - 1) // PAGE_SIZE_L2)
    if "l2_page" not in st.session_state:
        st.session_state["l2_page"] = 1
    # Reset page when filter changes
    if st.session_state.get("just_filtered"):
        st.session_state["l2_page"] = 1
    l2_page = max(1, min(st.session_state["l2_page"], total_pages_l2))

    pc1, pc2, pc3 = st.columns([3, 1, 1])
    pc1.caption(f"{total_l2} records — page {l2_page}/{total_pages_l2}")
    with pc2:
        if st.button("← Prev", key="l2_prev", disabled=l2_page<=1):
            st.session_state["l2_page"] = l2_page - 1; st.rerun()
    with pc3:
        if st.button("Next →", key="l2_next", disabled=l2_page>=total_pages_l2):
            st.session_state["l2_page"] = l2_page + 1; st.rerun()

    l2_start = (l2_page-1)*PAGE_SIZE_L2
    render_l2(l2.iloc[l2_start:l2_start+PAGE_SIZE_L2])


def _has_new_events(claim_updated_at, last_mcp_call) -> bool:
    """Check if tracking cache has new events after claim was last updated."""
    if not claim_updated_at or not last_mcp_call:
        return False
    try:
        from datetime import datetime as _dt
        ua = str(claim_updated_at)[:19]
        mc = str(last_mcp_call)[:19]
        ua_dt = _dt.strptime(ua, "%Y-%m-%d %H:%M:%S")
        mc_dt = _dt.strptime(mc, "%Y-%m-%d %H:%M:%S")
        return mc_dt > ua_dt
    except Exception:
        return False


def _save_reasoning_to_db(claim_id, short_label: str, narrative: str):
    """Save generated reasoning to claims table."""
    from sqlalchemy import text as _text
    with get_engine().connect() as conn:
        conn.execute(
            _text("UPDATE claims SET short_label=:sl, llm_narrative=:n, updated_at=:now WHERE claim_id=:cid"),
            {"sl": short_label, "n": narrative, "now": datetime.utcnow(), "cid": claim_id}
        )
        conn.commit()
    st.cache_data.clear()


def _save_draft_to_db(claim_id, draft: str):
    """Save email draft to claims table."""
    from sqlalchemy import text as _text
    with get_engine().connect() as conn:
        conn.execute(
            _text("UPDATE claims SET draft_email_text=:d, updated_at=:now WHERE claim_id=:cid"),
            {"d": draft, "now": datetime.utcnow(), "cid": claim_id}
        )
        conn.commit()
    st.cache_data.clear()


def render_l2(df: pd.DataFrame):
    cfg        = load_config()
    email_mode = cfg.get("email", {}).get("mode", "manual")

    h = st.columns([1.6, 1.1, 0.9, 0.9, 1.6, 0.9, 1.2, 0.6, 1.4, 0.5])
    for col, label in zip(h, ["Tracking ID","Ship Method","Order #","Ship Date",
                               "Last Event","Failure","Claim Status","Prob",
                               "Before Mail Reasoning","✉"]):
        col.markdown(f"<small><b style='color:#888;font-size:11px;'>{label}</b></small>", unsafe_allow_html=True)
    st.markdown("<hr style='margin:3px 0 0;border:none;border-top:0.5px solid #ddd;'>", unsafe_allow_html=True)

    for _, row in df.iterrows():
        tid          = str(row["tracking_id"])
        carrier      = str(row.get("carrier", ""))
        sm           = str(row.get("ship_method", ""))
        ship_date    = str(row.get("ship_date", "—"))[:10]
        order_num    = str(row.get("order_number", "—"))
        failure_type = str(row.get("failure_type", "") or "")
        claim_status = str(row.get("claim_status", "") or "pending")
        prob         = row.get("probability")
        email_count  = int(row.get("email_count", 0) or 0)
        short_label  = str(row.get("short_label", "") or "")
        narrative    = str(row.get("llm_narrative", "") or "")
        human_note   = str(row.get("human_comment", "") or "")
        claim_id     = row.get("claim_id")
        claim_updated= row.get("claim_updated_at") or row.get("updated_at")
        last_mcp     = row.get("last_mcp_call")
        is_on_time   = not failure_type or failure_type.upper() in ("ON_TIME", "")

        last_status = str(row.get("last_event_status", "") or "—")
        last_date, last_src = resolve_date(row.get("cache_last_event"), row.get("order_last_event"))
        last_event_html = (
            f"<div style='font-size:12px;'>{last_status}</div>"
            f"<div style='font-size:10px;color:#888;'>{last_date} {SOURCE_ICON.get(last_src,'')}</div>"
        )

        cols = st.columns([1.6, 1.1, 0.9, 0.9, 1.6, 0.9, 1.2, 0.6, 1.4, 0.5])

        with cols[0]:
            st.markdown(tracking_link_html(tid, carrier), unsafe_allow_html=True)
        cols[1].markdown(f"<span style='font-size:12px;'>{sm}</span>", unsafe_allow_html=True)
        cols[2].markdown(f"<span style='font-size:12px;color:#888;'>{order_num}</span>", unsafe_allow_html=True)
        cols[3].markdown(f"<span style='font-size:12px;'>{ship_date}</span>", unsafe_allow_html=True)
        cols[4].markdown(last_event_html, unsafe_allow_html=True)
        cols[5].markdown(failure_badge_html(failure_type), unsafe_allow_html=True)
        cols[6].markdown(status_badge_html(claim_status), unsafe_allow_html=True)
        cols[7].markdown(
            f"<span style='font-size:12px;'>{f'{float(prob):.0%}' if prob else '—'}</span>",
            unsafe_allow_html=True
        )

        # Before Mail Reasoning button
        with cols[8]:
            if is_on_time:
                st.markdown("<span style='font-size:11px;color:#ccc;' title='Delivered on time'>—</span>", unsafe_allow_html=True)
            elif short_label or narrative:
                display = (short_label[:22] + "…") if len(short_label) > 22 else short_label or "View"
                if st.button(f"📋 {display}", key=f"reason_{tid}"):
                    if st.session_state.get("open_reason_tid") == tid:
                        st.session_state["open_reason_tid"] = None
                    else:
                        st.session_state["open_reason_tid"] = tid
                        st.session_state["open_modal_tid"]  = None
                    st.rerun()
            else:
                # Manual mode — no reasoning yet
                if email_mode == "manual" and claim_id:
                    if st.button("✍ Generate", key=f"gen_reason_{tid}", help="Generate reasoning"):
                        st.session_state["open_reason_tid"] = tid
                        st.session_state["open_modal_tid"]  = None
                        st.rerun()
                else:
                    st.markdown("<span style='font-size:11px;color:#ccc;'>Pending</span>", unsafe_allow_html=True)

        # Email button
        with cols[9]:
            if is_on_time:
                st.markdown("<span style='font-size:18px;color:#ddd;' title='No claim — delivered on time'>✉</span>", unsafe_allow_html=True)
            else:
                btn_label = f"✉{email_count}" if email_count else "✉"
                if st.button(btn_label, key=f"email_{tid}"):
                    if st.session_state.get("open_modal_tid") == tid:
                        st.session_state["open_modal_tid"] = None
                    else:
                        st.session_state["open_modal_tid"]  = tid
                        st.session_state["open_reason_tid"] = None
                    st.rerun()

        st.markdown("<hr style='margin:2px 0;border:none;border-top:0.5px solid #eee;'>", unsafe_allow_html=True)

        # Reasoning popup
        if st.session_state.get("open_reason_tid") == tid:
            has_new = _has_new_events(claim_updated, last_mcp)
            with st.expander(f"📋 {short_label or 'Reasoning — ' + tid}", expanded=True):
                url = tracking_url(tid, carrier)
                st.markdown(f'🔗 <a href="{url}" target="_blank">Open carrier tracking</a>', unsafe_allow_html=True)
                st.divider()

                if narrative:
                    st.markdown(
                        f"""<div style="border-left:3px solid #AFA9EC;padding:10px 14px;
                            background:#f8f8ff;border-radius:0 6px 6px 0;
                            font-size:13px;line-height:1.7;">
                            <div style="font-size:11px;font-weight:600;color:#534AB7;margin-bottom:5px;">LLM REASONING</div>
                            {narrative.replace("<","&lt;").replace(">","&gt;") if narrative else ""}</div>""",
                        unsafe_allow_html=True
                    )
                    if human_note:
                        st.markdown(f"**👤 Human Comment:** {human_note}")
                    st.divider()
                    # Regenerate button
                    regen_disabled = not has_new
                    regen_help = "New events detected — click to regenerate" if has_new else "No new events since last generation"
                    if st.button("↻ Regenerate", key=f"regen_reason_{tid}",
                                 disabled=regen_disabled, help=regen_help):
                        with st.spinner("Regenerating reasoning…"):
                            result = _generate_reasoning_for_row(row)
                        _save_reasoning_to_db(claim_id, result["short_label"], result["narrative"])
                        st.success("Reasoning updated.")
                        st.rerun()
                else:
                    # No narrative yet
                    st.info("No reasoning generated yet.")
                    if claim_id:
                        if st.button("✍ Generate Reasoning", key=f"gen_reason_popup_{tid}", type="primary"):
                            with st.spinner("Generating reasoning…"):
                                result = _generate_reasoning_for_row(row)
                            _save_reasoning_to_db(claim_id, result["short_label"], result["narrative"])
                            st.success("Reasoning generated.")
                            st.rerun()

                if st.button("Close", key=f"close_reason_{tid}"):
                    st.session_state["open_reason_tid"] = None; st.rerun()

        # Email modal
        if st.session_state.get("open_modal_tid") == tid:
            render_email_modal(tid, carrier, dict(row), email_mode)


def _generate_reasoning_for_row(row: dict) -> dict:
    """Call reasoning_generator for a dashboard row."""
    import sys
    BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    if BASE not in sys.path:
        sys.path.insert(0, BASE)
    try:
        from agents.reasoning_generator import generate_reasoning
        # Try to get tracking history from cache
        history = []
        try:
            hj = row.get("full_history_json", "")
            if hj:
                history = json.loads(hj)
        except Exception:
            pass
        return generate_reasoning(
            tracking_id     = str(row.get("tracking_id", "")),
            carrier         = str(row.get("carrier", "")),
            ship_method     = str(row.get("ship_method", "")),
            ship_date       = str(row.get("ship_date", "")),
            failure_type    = str(row.get("failure_type", "LATE") or "LATE"),
            delay_days      = int(row.get("delay_days", 1) or 1),
            first_bad_event = row.get("first_bad_event"),
            promised_date   = row.get("promised_date"),
            delivered_date  = str(row.get("cache_last_event", ""))[:10] or None,
            tracking_history= history,
            occasion_type   = row.get("occasion_type"),
        )
    except Exception as e:
        return {"short_label": "Reasoning failed", "narrative": str(e), "success": False}


def render_email_modal(tracking_id: str, carrier: str, row: dict, email_mode: str):
    short_label  = str(row.get("short_label", "") or "")
    narrative    = str(row.get("llm_narrative", "") or "")
    human_note   = str(row.get("human_comment", "") or "")
    draft_text   = str(row.get("draft_email_text", "") or "")
    claim_id     = row.get("claim_id")
    url          = tracking_url(tracking_id, carrier)
    emails       = q_emails(tracking_id)
    claim_updated= row.get("claim_updated_at") or row.get("updated_at")
    last_mcp     = row.get("last_mcp_call")
    has_new      = _has_new_events(claim_updated, last_mcp)

    cfg       = load_config()
    env       = cfg.get("email", {}).get("env", "test")
    test_addr = cfg.get("email", {}).get("test_address", "praveen.prakash.82@gmail.com")
    prod_to   = "support@shippo.com" if "UPS" in carrier.upper() else "file.claim@fedex.com"
    cc_addr   = "logistics@arabellabouquets.com"

    if env == "test":
        to_display  = f'<span style="background:#FFF3CD;color:#854F0B;padding:2px 8px;border-radius:4px;font-size:12px;">🟠 {test_addr} (TEST)</span>'
        cc_display  = '<span style="font-size:12px;color:#aaa;">— (CC disabled in test mode)</span>'
        to_actual   = test_addr
        cc_actual   = None
    else:
        to_display  = f'<span style="background:#EAF3DE;color:#3B6D11;padding:2px 8px;border-radius:4px;font-size:12px;">🟢 {prod_to}</span>'
        cc_display  = f'<span style="font-size:12px;">{cc_addr}</span>'
        to_actual   = prod_to
        cc_actual   = cc_addr

    subject = f"Service Guarantee Claim — {tracking_id} — {row.get('ship_method','')}"

    with st.expander(f"✉ {tracking_id} — {short_label or 'Email'}", expanded=True):
        st.markdown(f'🔗 <a href="{url}" target="_blank">Open carrier tracking</a>', unsafe_allow_html=True)

        # Email details header
        st.markdown(
            f"""<div style="background:#f8f9fa;border-radius:6px;padding:10px 14px;margin:8px 0;font-size:12px;">
            <div style="margin-bottom:4px;"><b>To:</b>&nbsp;&nbsp;&nbsp;&nbsp;{to_display}</div>
            <div style="margin-bottom:4px;"><b>CC:</b>&nbsp;&nbsp;&nbsp;&nbsp;{cc_display}</div>
            <div><b>Subject:</b>&nbsp;{subject}</div>
            </div>""",
            unsafe_allow_html=True
        )

        st.divider()

        # Brief reasoning
        if narrative:
            st.markdown(
                f"""<div style="border-left:3px solid #AFA9EC;padding:6px 10px;
                    background:#f8f8ff;border-radius:0 6px 6px 0;font-size:12px;margin-bottom:8px;">
                    <b style="font-size:11px;color:#534AB7;">REASONING</b><br>
                    {narrative[:200].replace("<","&lt;").replace(">","&gt;") if narrative else ""}{"…" if len(narrative)>200 else ""}</div>""",
                unsafe_allow_html=True
            )

        # Generate / Regenerate buttons row
        if not emails.empty:
            # Thread exists — show it then follow-up option
            _render_email_thread(emails)
            if email_mode != "auto_send":
                st.divider()
                st.caption("Send follow-up:")
                # Auto-draft resubmission if rejected
                auto_draft = ""
                last_email = emails.iloc[-1] if not emails.empty else None
                if last_email is not None and str(last_email.get("status","")).lower() in ("rejected", "denied"):
                    try:
                        from agents.followup_escalation import draft_resubmission
                        resub = draft_resubmission({
                            "claim_id": claim_id, "tracking_id": tracking_id,
                            "carrier": row.get("carrier",""), "claim_type": row.get("failure_type",""),
                            "rejection_reason": str(last_email.get("rejection_reason","")),
                            "attempt_number": 2, "probability": row.get("probability", 0.5),
                        })
                        auto_draft = resub.get("body", "") if isinstance(resub, dict) else ""
                    except Exception as e:
                        auto_draft = f"[Auto-draft failed: {e}]"
                fu = st.text_area("Draft:", value=auto_draft, height=100, key=f"fu_{tracking_id}")
                if st.button("📤 Send Follow-up", key=f"sendfu_{tracking_id}"):
                    _send_email(tracking_id, claim_id, fu, to_actual, cc_actual)
        else:
            # No thread yet — show draft area
            b1, b2 = st.columns([1, 1])

            # Determine draft source
            gen_key = f"gen_draft_{tracking_id}"
            current_draft = st.session_state.get(gen_key) or draft_text

            with b1:
                if not current_draft:
                    if st.button("✍ Generate Email", key=f"gen_email_{tracking_id}", type="primary"):
                        with st.spinner("Generating email…"):
                            draft = generate_email_draft(tracking_id, row)
                        st.session_state[gen_key] = draft
                        _save_draft_to_db(claim_id, draft)
                        st.rerun()
                else:
                    regen_help = "New events detected — click to regenerate" if has_new else "No new events since last generation"
                    if st.button("↻ Regenerate Email", key=f"regen_email_{tracking_id}",
                                 disabled=not has_new, help=regen_help):
                        with st.spinner("Regenerating email…"):
                            draft = generate_email_draft(tracking_id, row)
                        st.session_state[gen_key] = draft
                        _save_draft_to_db(claim_id, draft)
                        st.rerun()

            if current_draft:
                edited = st.text_area("Email draft (editable):", value=current_draft,
                                      height=250, key=f"draft_{tracking_id}")
                if human_note:
                    st.markdown(f"**👤 Human comment:** {human_note}")
                if st.button("📤 Send", key=f"send_{tracking_id}", type="primary"):
                    _send_email(tracking_id, claim_id, edited, to_actual, cc_actual)
            else:
                st.info("Click Generate Email to draft.")

        if st.button("Close", key=f"close_email_{tracking_id}"):
            st.session_state["open_modal_tid"] = None; st.rerun()


def _render_email_thread(emails: pd.DataFrame):
    if emails.empty:
        st.info("No emails filed yet.")
        return
    st.caption(f"EMAIL THREAD ({len(emails)})")
    for _, email in emails.iterrows():
        direction = str(email.get("direction", "outbound"))
        ts        = str(email.get("timestamp", ""))[:16]
        subject   = str(email.get("subject", "(no subject)"))
        body      = str(email.get("body", "") or "")
        rejection = str(email.get("rejection_reason", "") or "")
        recovered = email.get("recovered_amount")
        icon      = "↑" if direction == "outbound" else "↓"
        color     = "#185FA5" if direction == "outbound" else "#3B6D11"
        label     = f"Sent · {ts}" if direction == "outbound" else f"Carrier reply · {ts}"

        st.markdown(
            f"""<div style="border:0.5px solid #e0e0e0;border-radius:6px;padding:10px 14px;margin-bottom:8px;">
            <div style="font-size:11px;font-weight:600;color:{color};margin-bottom:3px;">{icon} {label}</div>
            <div style="font-size:12px;font-weight:500;margin-bottom:4px;">{subject}</div>
            <div style="font-size:12px;color:#666;line-height:1.5;">{body[:400]}{"…" if len(body)>400 else ""}</div>
            {"<div style='font-size:12px;color:#A32D2D;margin-top:6px;'>❌ " + rejection + "</div>" if rejection else ""}
            {"<div style='font-size:12px;color:#3B6D11;margin-top:6px;'>✅ Recovered: $" + f"{float(recovered):.2f}" + "</div>" if recovered else ""}
            </div>""",
            unsafe_allow_html=True
        )


# ─────────────────────────────────────────────────────────────────────────────
# TAB 3 — MCP 1 LOG
# ─────────────────────────────────────────────────────────────────────────────

def render_mcp1():
    st.markdown(
        "<div style='font-size:13px;font-weight:500;margin-bottom:8px;'>📡 MCP 1 Log — Carrier Tracking API</div>",
        unsafe_allow_html=True
    )
    if st.session_state.get("mcp1_nav_notice"):
        sm = st.session_state["mcp1_nav_notice"]
        st.info(f"🔍 Filtered to: {sm} — clear filter to see all")
        st.session_state["mcp1_nav_notice"] = None

    # Filter bar
    f1, f2, f3, f4 = st.columns([2, 1.5, 1.5, 1])
    sm_filter  = f1.text_input("Ship Method filter", key="mcp1_sm_input", value=st.session_state.get("mcp1_filter_sm", "") or "")
    src_filter = f2.selectbox("Source", ["All", "Live API", "Cache", "Order API"],
                              index=["All","Live API","Cache","Order API"].index(
                                  st.session_state.get("mcp1_filter_source","All")))
    del_filter = f3.selectbox("Delivery", ["All", "On Time", "Not On Time"],
                              index=["All","On Time","Not On Time"].index(
                                  st.session_state.get("mcp1_filter_delivery","All")))
    with f4:
        st.write("")
        if st.button("✕ Clear filters", key="mcp1_clear"):
            st.session_state["mcp1_filter_sm"]       = None
            st.session_state["mcp1_filter_source"]   = "All"
            st.session_state["mcp1_filter_delivery"] = "All"
            st.rerun()

    # Update session state
    st.session_state["mcp1_filter_sm"]       = sm_filter or None
    st.session_state["mcp1_filter_source"]   = src_filter
    st.session_state["mcp1_filter_delivery"] = del_filter

    df = q_mcp1(sm_filter or None, src_filter, del_filter)

    if df.empty:
        st.info("No MCP 1 records match this filter.")
        return

    PAGE_SIZE = 50
    total = len(df)
    total_pages = max(1, (total + PAGE_SIZE - 1) // PAGE_SIZE)
    pg_key = "mcp1_page"
    if pg_key not in st.session_state:
        st.session_state[pg_key] = 1
    # Reset page on filter change
    page = st.session_state[pg_key]
    page = max(1, min(page, total_pages))

    pc1, pc2, pc3 = st.columns([3, 1, 1])
    pc1.caption(f"{total} records — page {page}/{total_pages}")
    with pc2:
        if st.button("← Prev", key="mcp1_prev", disabled=page<=1):
            st.session_state[pg_key] = page - 1; st.rerun()
    with pc3:
        if st.button("Next →", key="mcp1_next", disabled=page>=total_pages):
            st.session_state[pg_key] = page + 1; st.rerun()

    start = (page-1)*PAGE_SIZE
    df_page = df.iloc[start:start+PAGE_SIZE]

    # Headers
    h = st.columns([1.3, 1.5, 0.7, 1.1, 1.4, 0.9, 0.6, 0.7, 0.8, 0.7])
    for col, label in zip(h, ["Last MCP Call","Tracking ID","Carrier","Ship Method",
                               "Cached Status","Source","Events","Prob","Delivery","🔄"]):
        col.markdown(f"<small><b style='color:#888;font-size:11px;'>{label}</b></small>", unsafe_allow_html=True)
    st.markdown("<hr style='margin:3px 0 0;border:none;border-top:0.5px solid #ddd;'>", unsafe_allow_html=True)

    for _, row in df_page.iterrows():
        tid     = str(row.get("tracking_id",""))
        carrier = str(row.get("carrier",""))
        sm      = str(row.get("ship_method","") or "")
        status  = str(row.get("cached_status","") or "—")
        source  = str(row.get("source","") or "")
        ts      = str(row.get("last_mcp_call","") or "—")[:16]
        prob    = row.get("probability")
        is_late = bool(row.get("is_late", 0))

        # Parse cached history
        cached_history = []
        event_count = 0
        hj = row.get("full_history_json","")
        if hj:
            try:
                cached_history = json.loads(hj)
                event_count = len(cached_history)
                cached_history = sorted(cached_history, key=lambda x: str(x.get("date","")))
            except Exception:
                pass

        cols = st.columns([1.3, 1.5, 0.7, 1.1, 1.4, 0.9, 0.6, 0.7, 0.8, 0.7])
        cols[0].markdown(f"<span style='font-size:11px;color:#888;'>{ts}</span>", unsafe_allow_html=True)
        cols[1].markdown(tracking_link_html(tid, carrier), unsafe_allow_html=True)
        cols[2].markdown(f"<span style='font-size:12px;'>{carrier}</span>", unsafe_allow_html=True)
        cols[3].markdown(f"<span style='font-size:11px;'>{sm}</span>", unsafe_allow_html=True)
        cols[4].markdown(f"<span style='font-size:11px;'>{status[:35]}</span>", unsafe_allow_html=True)
        cols[5].markdown(source_badge_html(source), unsafe_allow_html=True)

        # Events — clickable button showing count
        with cols[6]:
            if st.button(str(event_count) if event_count else "—", key=f"ev_{tid}",
                         help="Click to view tracking history"):
                cur = st.session_state.get("mcp1_events_tid")
                st.session_state["mcp1_events_tid"] = None if cur == tid else tid
                st.session_state["mcp1_verify_tid"] = None
                st.rerun()

        cols[7].markdown(
            f"<span style='font-size:12px;'>{f'{float(prob):.0%}' if prob else '—'}</span>",
            unsafe_allow_html=True
        )
        cols[8].markdown(delivery_badge_html(is_late), unsafe_allow_html=True)

        # Verify button — live API call
        with cols[9]:
            if st.button("🔄", key=f"verify_{tid}", help="Call live carrier API now"):
                cur = st.session_state.get("mcp1_verify_tid")
                st.session_state["mcp1_verify_tid"] = None if cur == tid else tid
                st.session_state["mcp1_events_tid"] = None
                st.rerun()

        st.markdown("<hr style='margin:2px 0;border:none;border-top:0.5px solid #eee;'>", unsafe_allow_html=True)

        # Events popup — show cached history sorted by date
        if st.session_state.get("mcp1_events_tid") == tid:
            with st.expander(f"📋 Tracking History — {tid} ({event_count} events)", expanded=True):
                st.markdown(f"**Carrier:** {carrier} &nbsp;|&nbsp; **Status:** {status} &nbsp;|&nbsp; **Last updated:** {ts}")
                st.divider()
                if cached_history:
                    for event in cached_history:
                        ev_date = str(event.get("date",""))[:16]
                        ev_status = str(event.get("status",""))
                        ev_loc = str(event.get("location","") or "")
                        loc_str = f" &nbsp;📍 {ev_loc}" if ev_loc else ""
                        st.markdown(
                            f"<div style='font-size:12px;padding:5px 0;border-bottom:0.5px solid #f0f0f0;'>"
                            f"<span style='color:#888;font-size:11px;'>{ev_date}</span>{loc_str}<br>"
                            f"<span style='font-weight:500;'>{ev_status}</span></div>",
                            unsafe_allow_html=True
                        )
                else:
                    st.info("No cached history available.")
                if st.button("Close", key=f"close_ev_{tid}"):
                    st.session_state["mcp1_events_tid"] = None; st.rerun()

        # Verify popup — live API call
        if st.session_state.get("mcp1_verify_tid") == tid:
            with st.expander(f"🔄 Live API — {tid}", expanded=True):
                with st.spinner(f"Calling {carrier} API…"):
                    try:
                        import sys, os as _os
                        _os.environ.setdefault("DATABASE_URL", "sqlite:////app/data/bloomdirect.db")
                        if BASE_DIR not in sys.path:
                            sys.path.insert(0, BASE_DIR)
                        from mcp_servers.carrier_tracking_mcp import fetch_ups_history, fetch_fedex_history
                        if "UPS" in carrier.upper():
                            result = fetch_ups_history(tid)
                        else:
                            result = fetch_fedex_history(tid)
                        if result:
                            live_history = sorted(result.get("history",[]), key=lambda x: str(x.get("date","")))
                            st.markdown(f"**Live status:** {result.get('status','')} &nbsp;|&nbsp; **{len(live_history)} events**")
                            st.divider()
                            for event in live_history:
                                ev_date = str(event.get("date",""))[:16]
                                ev_status = str(event.get("status",""))
                                ev_loc = str(event.get("location","") or "")
                                loc_str = f" &nbsp;📍 {ev_loc}" if ev_loc else ""
                                st.markdown(
                                    f"<div style='font-size:12px;padding:5px 0;border-bottom:0.5px solid #f0f0f0;'>"
                                    f"<span style='color:#888;font-size:11px;'>{ev_date}</span>{loc_str}<br>"
                                    f"<span style='font-weight:500;'>{ev_status}</span></div>",
                                    unsafe_allow_html=True
                                )
                        else:
                            st.warning("No data returned from carrier API.")
                    except Exception as e:
                        st.error(f"API call failed: {e}")
                if st.button("Close", key=f"close_verify_{tid}"):
                    st.session_state["mcp1_verify_tid"] = None; st.rerun()


# ─────────────────────────────────────────────────────────────────────────────
# TAB 4 — MCP 2 LOG
# ─────────────────────────────────────────────────────────────────────────────

def render_mcp2():
    st.markdown(
        "<div style='font-size:13px;font-weight:500;margin-bottom:8px;'>📧 MCP 2 Log — Email Claims MCP</div>",
        unsafe_allow_html=True
    )
    if st.session_state.get("mcp2_nav_notice"):
        sm = st.session_state["mcp2_nav_notice"]
        st.info(f"🔍 Filtered to: {sm} — clear filter to see all")
        st.session_state["mcp2_nav_notice"] = None

    f1, f2 = st.columns([2, 1])
    sm_filter = f1.text_input("Ship Method filter", key="mcp2_sm_input",
                              value=st.session_state.get("mcp2_filter_sm","") or "")
    with f2:
        st.write("")
        if st.button("✕ Clear", key="mcp2_clear"):
            st.session_state["mcp2_filter_sm"] = None; st.rerun()

    st.session_state["mcp2_filter_sm"] = sm_filter or None

    df = q_mcp2(sm_filter or None)

    if df.empty:
        st.info("No MCP 2 records match this filter.")
        return

    PAGE_SIZE = 50
    total = len(df)
    total_pages = max(1, (total + PAGE_SIZE - 1) // PAGE_SIZE)
    pg_key = "mcp2_page"
    if pg_key not in st.session_state:
        st.session_state[pg_key] = 1
    page = max(1, min(st.session_state[pg_key], total_pages))

    pc1, pc2, pc3 = st.columns([3, 1, 1])
    pc1.caption(f"{total} records — page {page}/{total_pages}")
    with pc2:
        if st.button("← Prev", key="mcp2_prev", disabled=page<=1):
            st.session_state[pg_key] = page - 1; st.rerun()
    with pc3:
        if st.button("Next →", key="mcp2_next", disabled=page>=total_pages):
            st.session_state[pg_key] = page + 1; st.rerun()

    start = (page-1)*PAGE_SIZE
    df_page = df.iloc[start:start+PAGE_SIZE]

    h = st.columns([1.4, 1.6, 0.8, 1.2, 0.8, 2, 1])
    for col, label in zip(h, ["Timestamp","Tracking ID","Carrier","Ship Method",
                               "Direction","Subject","Status"]):
        col.markdown(f"<small><b style='color:#888;font-size:11px;'>{label}</b></small>", unsafe_allow_html=True)
    st.markdown("<hr style='margin:3px 0 0;border:none;border-top:0.5px solid #ddd;'>", unsafe_allow_html=True)

    for _, row in df_page.iterrows():
        tid       = str(row.get("tracking_id",""))
        carrier   = str(row.get("carrier",""))
        sm        = str(row.get("ship_method","") or "")
        direction = str(row.get("direction",""))
        ts        = str(row.get("timestamp",""))[:16]
        subject   = str(row.get("subject","") or "—")
        status    = str(row.get("status","") or "—")

        dir_html = (
            '<span style="font-size:11px;color:#185FA5;font-weight:600;">↑ Sent</span>'
            if direction == "outbound" else
            '<span style="font-size:11px;color:#3B6D11;font-weight:600;">↓ Reply</span>'
        )

        cols = st.columns([1.4, 1.6, 0.8, 1.2, 0.8, 2, 1])
        cols[0].markdown(f"<span style='font-size:11px;color:#888;'>{ts}</span>", unsafe_allow_html=True)
        cols[1].markdown(tracking_link_html(tid, carrier), unsafe_allow_html=True)
        cols[2].markdown(f"<span style='font-size:12px;'>{carrier}</span>", unsafe_allow_html=True)
        cols[3].markdown(f"<span style='font-size:11px;'>{sm}</span>", unsafe_allow_html=True)
        cols[4].markdown(dir_html, unsafe_allow_html=True)
        cols[5].markdown(f"<span style='font-size:11px;'>{subject[:50]}</span>", unsafe_allow_html=True)
        cols[6].markdown(status_badge_html(status), unsafe_allow_html=True)

        st.markdown("<hr style='margin:2px 0;border:none;border-top:0.5px solid #eee;'>", unsafe_allow_html=True)


# ─────────────────────────────────────────────────────────────────────────────
# TAB 2 — HITL QUEUE
# ─────────────────────────────────────────────────────────────────────────────

def render_hitl():
    st.markdown("<div style='font-size:13px;font-weight:500;margin-bottom:8px;'>🧑‍💼 Human-in-the-Loop Queue</div>", unsafe_allow_html=True)
    st.markdown(
        "<div style='font-size:11px;color:#888;margin-bottom:12px;'>"
        "Claims routed here need human review before filing. Generate a draft, review/edit it, then send or skip.</div>",
        unsafe_allow_html=True
    )
    queue = q_hitl()
    if queue.empty:
        st.success("✅ No pending items. All claims are being handled automatically.")
        return
    st.info(f"📋 {len(queue)} item(s) awaiting review.")

    for _idx, (_, row) in enumerate(queue.iterrows()):
        tid     = str(row["tracking_id"])
        carrier = str(row.get("carrier", ""))
        sm      = str(row.get("ship_method", ""))
        prob    = float(row.get("probability", 0) or 0)
        days    = int(row.get("days_remaining", 0) or 0)
        reason  = str(row.get("reason", ""))
        url     = tracking_url(tid, carrier)
        claim_id_val = int(row["claim_id"])

        # Load claim data
        has_draft = False
        draft_text = ""
        reasoning_text = ""
        try:
            with get_engine().connect() as _conn:
                _r = _conn.execute(text(
                    "SELECT draft_email_text, short_label, llm_narrative FROM claims WHERE claim_id=:cid"
                ), {"cid": claim_id_val}).fetchone()
                if _r:
                    if _r[0]:
                        has_draft = True
                        draft_text = str(_r[0])
                    if _r[2]:
                        reasoning_text = str(_r[2])
        except Exception:
            pass

        # Color the expander header based on state
        state_icon = "🟢" if has_draft else "🟡"
        with st.expander(f"{state_icon} {tid}  |  {reason[:50]}  |  Prob: {prob:.0%}  |  {days}d left", expanded=False):

            # ── Row 1: Claim Details ──────────────────────────
            c1, c2 = st.columns(2)
            c1.markdown(f"**Ship Method:** {sm}")
            c1.markdown(f"**Failure Type:** {row.get('failure_type', '—')}")
            c1.markdown(f"**Reason:** {reason}")
            c2.markdown(f"**Probability:** {prob:.0%}")
            c2.markdown(f"**Days Left:** {days}")
            c2.markdown(f'🔗 <a href="{url}" target="_blank">Open Carrier Tracking</a>', unsafe_allow_html=True)

            # ── Row 2: LLM Reasoning ──────────────────────────
            if reasoning_text:
                st.markdown(
                    f"<div style='border-left:3px solid #AFA9EC;padding:8px 12px;background:#f8f8ff;"
                    f"border-radius:0 6px 6px 0;font-size:12px;margin:8px 0;'>"
                    f"<b>LLM Reasoning:</b> {reasoning_text[:400]}</div>",
                    unsafe_allow_html=True
                )

            st.divider()

            # ── Step 1: Generate Draft ────────────────────────
            if not has_draft:
                st.markdown("**Step 1:** Generate an email draft using LLM")
                if st.button("✍ Generate Draft", key=f"hitl_gen_{_idx}", type="primary"):
                    with st.spinner("Generating reasoning + draft via LLM..."):
                        _hitl_generate_draft(claim_id_val, tid)
                    st.rerun()
                st.caption("⚠️ Generate a draft before you can send.")
            else:
                st.markdown("**Step 1:** ✅ Draft generated")

            st.divider()

            # ── Step 2: Review & Edit Draft ───────────────────
            st.markdown("**Step 2:** Review and edit the draft email")
            edited_draft = st.text_area(
                "Email draft:",
                value=draft_text,
                height=200,
                key=f"hitl_draft_{_idx}",
                disabled=not has_draft,
                placeholder="Click 'Generate Draft' first..."
            )

            # Save edits if changed
            if has_draft and edited_draft != draft_text:
                try:
                    with get_engine().connect() as _conn:
                        _conn.execute(text("UPDATE claims SET draft_email_text=:d, updated_at=:now WHERE claim_id=:cid"),
                                      {"d": edited_draft, "now": datetime.utcnow(), "cid": claim_id_val})
                        _conn.commit()
                except Exception:
                    pass

            st.divider()

            # ── Step 3: Action Buttons ────────────────────────
            st.markdown("**Step 3:** Take action")
            b1, b2, b3 = st.columns(3)
            with b1:
                if st.button("📤 Send to Carrier", key=f"hitl_send_{_idx}",
                             disabled=not has_draft, type="primary" if has_draft else "secondary"):
                    _hitl_send(int(row["queue_id"]), claim_id_val, tid, edited_draft or draft_text)
            with b2:
                if st.button("⏭ Skip — No Claim", key=f"hitl_skip_{_idx}"):
                    _hitl_skip(int(row["queue_id"]), claim_id_val, tid)
            with b3:
                if st.button("❌ Reject & Close", key=f"hitl_close_{_idx}"):
                    _hitl_close(int(row["queue_id"]), claim_id_val, tid)


def _hitl_generate_draft(claim_id: int, tracking_id: str):
    """Generate LLM reasoning + draft for a HITL claim."""
    try:
        from database.models import get_session as _gs
        from database.models import Claim as _C, Order as _O, TrackingCache as _TC, Failure as _F
        _s = _gs()
        _claim = _s.query(_C).filter_by(claim_id=claim_id).first()
        _o = _s.query(_O).filter_by(tracking_id=tracking_id).first()
        _tc = _s.query(_TC).filter_by(tracking_id=tracking_id).first()
        _f = _s.query(_F).filter_by(tracking_id=tracking_id).first()

        if not _claim or not _o:
            st.error("Claim or order not found")
            _s.close()
            return

        _state = {
            "validated_order": {
                "partner_order_id": _o.partner_order_id or tracking_id,
                "track_id": tracking_id,
                "ship_method": _o.ship_method or "",
                "ship_date": _o.ship_date or "",
                "carrier": _o.carrier or "FedEx",
                "gift_message": "",
            },
            "classification": {
                "carrier": _o.carrier or "FedEx",
                "failure_type": _f.failure_type if _f else "LATE",
                "delay_days": _f.delay_days if _f else 1,
                "first_bad_event": _f.first_bad_event if _f else "",
                "promised_date": _f.promised_date if _f else "",
                "occasion_type": _claim.occasion_type or "General",
            },
            "eligibility": {"eligible": True, "probability": _claim.probability or 0.5},
            "mcp_history": json.loads(_tc.full_history_json) if _tc and _tc.full_history_json else [],
            "attempt_number": 1,
            "claim_id": claim_id,
        }

        # Generate reasoning
        try:
            from agents.reasoning_generator import generate_reasoning
            reasoning = generate_reasoning(
                tracking_id=tracking_id,
                carrier=_o.carrier or "FedEx",
                ship_method=_o.ship_method or "",
                ship_date=_o.ship_date or "",
                failure_type=_f.failure_type if _f else "LATE",
                delay_days=int(_f.delay_days) if _f and _f.delay_days else 1,
                first_bad_event=_f.first_bad_event if _f else None,
                promised_date=_f.promised_date if _f else None,
                delivered_date=None,
                tracking_history=_state["mcp_history"],
                occasion_type=_claim.occasion_type,
            )
            _claim.short_label = reasoning["short_label"]
            _claim.llm_narrative = reasoning["narrative"]
        except Exception:
            _claim.short_label = "HITL — manual review"
            _claim.llm_narrative = "Claim requires manual review"

        # Generate draft
        from agents.claim_drafter import draft_claim_email
        draft = draft_claim_email(_state)
        _claim.draft_email_text = draft.get("body", "")
        _claim.updated_at = datetime.now()
        _s.commit()
        _s.close()
        st.success("✅ Draft generated successfully!")
    except Exception as e:
        st.error(f"Draft generation failed: {e}")


def _hitl_send(queue_id: int, claim_id: int, tracking_id: str, draft_body: str):
    """Send HITL claim to carrier via paced sender queue."""
    with get_engine().connect() as conn:
        # Update HITL queue → resolved
        conn.execute(text("UPDATE hitl_queue SET status='approved', resolved_at=:now WHERE queue_id=:id"),
                     {"now": datetime.utcnow(), "id": queue_id})
        # Update claim → queued for paced sender
        conn.execute(text("""
            UPDATE claims SET status='queued_to_send', draft_email_text=:body, updated_at=:now
            WHERE claim_id=:cid
        """), {"body": draft_body, "now": datetime.utcnow(), "cid": claim_id})
        conn.commit()
    st.success(f"📤 {tracking_id} — queued for sending. Paced sender will deliver it shortly.")
    st.cache_data.clear()
    st.rerun()


def _hitl_skip(queue_id: int, claim_id: int, tracking_id: str):
    """Skip this claim — no filing needed."""
    with get_engine().connect() as conn:
        conn.execute(text("UPDATE hitl_queue SET status='skipped', resolved_at=:now WHERE queue_id=:id"),
                     {"now": datetime.utcnow(), "id": queue_id})
        conn.execute(text("UPDATE claims SET status='skipped', updated_at=:now WHERE claim_id=:cid"),
                     {"now": datetime.utcnow(), "cid": claim_id})
        conn.commit()
    st.info(f"⏭ {tracking_id} — skipped, no claim filed.")
    st.cache_data.clear()
    st.rerun()


def _hitl_close(queue_id: int, claim_id: int, tracking_id: str):
    """Close/reject this claim."""
    with get_engine().connect() as conn:
        conn.execute(text("UPDATE hitl_queue SET status='closed', resolved_at=:now WHERE queue_id=:id"),
                     {"now": datetime.utcnow(), "id": queue_id})
        conn.execute(text("UPDATE claims SET status='closed', updated_at=:now WHERE claim_id=:cid"),
                     {"now": datetime.utcnow(), "cid": claim_id})
        conn.commit()
    st.info(f"❌ {tracking_id} — closed, no claim filed.")
    st.cache_data.clear()
    st.rerun()


# ─────────────────────────────────────────────────────────────────────────────
# TAB 5 — ERRORS
# ─────────────────────────────────────────────────────────────────────────────


# ─────────────────────────────────────────────────────────────────────────────
# PIPELINE LOG QUERY
# ─────────────────────────────────────────────────────────────────────────────

@st.cache_data(ttl=30)
def q_pipeline_runs() -> pd.DataFrame:
    try:
        sql = text("""
            SELECT run_id, started_at, completed_at, triggered_by,
                   date_from, date_to, status, duration_seconds,
                   orders_fetched, orders_classified, eligible,
                   drafted, filed, skipped, errors, hitl_queued, notes
            FROM pipeline_runs
            ORDER BY started_at DESC
            LIMIT 100
        """)
        with get_engine().connect() as conn:
            return pd.read_sql(sql, conn)
    except Exception:
        return pd.DataFrame()


# ─────────────────────────────────────────────────────────────────────────────
# TAB — PIPELINE LOG
# ─────────────────────────────────────────────────────────────────────────────

# ─────────────────────────────────────────────────────────────────────────────
# ROI IMPACT DASHBOARD
# ─────────────────────────────────────────────────────────────────────────────

def q_roi_all_time() -> dict:
    """Get all-time ROI metrics."""
    sql = text("""
        SELECT
            COUNT(DISTINCT o.tracking_id)                                         AS total_shipments,
            COUNT(DISTINCT f.tracking_id)                                         AS total_failures,
            COUNT(DISTINCT c.tracking_id)                                         AS total_eligible,
            COUNT(DISTINCT CASE WHEN c.filed = 1 THEN c.tracking_id END)          AS total_filed,
            COUNT(DISTINCT CASE WHEN c.status = 'approved' THEN c.tracking_id END) AS total_approved,
            COUNT(DISTINCT CASE WHEN c.status = 'rejected' THEN c.tracking_id END) AS total_rejected,
            COUNT(DISTINCT CASE WHEN c.status IN ('filed','resubmitted','draft_pending_send')
                           THEN c.tracking_id END)                                AS total_awaiting
        FROM orders o
        LEFT JOIN failures f ON f.tracking_id = o.tracking_id
        LEFT JOIN claims   c ON c.tracking_id = o.tracking_id
    """)
    with get_engine().connect() as conn:
        row = pd.read_sql(sql, conn).iloc[0]
    return row.to_dict()


def q_roi_trend(period: str = "weekly") -> pd.DataFrame:
    """Get trend data grouped by period."""
    if period == "weekly":
        group_expr = "strftime('%Y-W%W', o.ship_date)"
        label = "Week"
    elif period == "monthly":
        group_expr = "strftime('%Y-%m', o.ship_date)"
        label = "Month"
    else:
        group_expr = "strftime('%Y', o.ship_date)"
        label = "Year"

    sql = text(f"""
        SELECT
            {group_expr}                                                           AS period,
            COUNT(DISTINCT o.tracking_id)                                          AS shipments,
            COUNT(DISTINCT f.tracking_id)                                          AS failures,
            COUNT(DISTINCT CASE WHEN c.filed = 1 THEN c.tracking_id END)           AS filed,
            COUNT(DISTINCT CASE WHEN c.status = 'approved' THEN c.tracking_id END) AS approved,
            COUNT(DISTINCT CASE WHEN c.status = 'rejected' THEN c.tracking_id END) AS rejected
        FROM orders o
        LEFT JOIN failures f ON f.tracking_id = o.tracking_id
        LEFT JOIN claims   c ON c.tracking_id = o.tracking_id
        WHERE o.ship_date IS NOT NULL
        GROUP BY {group_expr}
        ORDER BY {group_expr}
    """)
    with get_engine().connect() as conn:
        return pd.read_sql(sql, conn)


def q_roi_by_carrier() -> pd.DataFrame:
    """Get ROI breakdown by ship method."""
    sql = text("""
        SELECT
            o.ship_method,
            COUNT(DISTINCT o.tracking_id)                                          AS shipments,
            COUNT(DISTINCT f.tracking_id)                                          AS failures,
            COUNT(DISTINCT CASE WHEN c.filed = 1 THEN c.tracking_id END)           AS filed,
            COUNT(DISTINCT CASE WHEN c.status = 'approved' THEN c.tracking_id END) AS approved,
            COUNT(DISTINCT CASE WHEN c.status = 'rejected' THEN c.tracking_id END) AS rejected
        FROM orders o
        LEFT JOIN failures f ON f.tracking_id = o.tracking_id
        LEFT JOIN claims   c ON c.tracking_id = o.tracking_id
        WHERE o.ship_method IS NOT NULL AND o.ship_method != ''
        GROUP BY o.ship_method
        ORDER BY failures DESC
    """)
    with get_engine().connect() as conn:
        return pd.read_sql(sql, conn)


def render_roi_impact():
    """Render the ROI Impact dashboard — full history, strategic view."""
    claim_amt = load_config().get("claim_amount", 100.0)

    st.markdown(
        "<div style='font-size:15px;font-weight:600;margin-bottom:4px;'>💰 ROI Impact — Claims Recovery Analysis</div>"
        "<div style='font-size:11px;color:#888;margin-bottom:16px;'>Full history — all shipments since system start</div>",
        unsafe_allow_html=True
    )

    # ── All-Time KPIs ────────────────────────────────────────────────
    metrics = q_roi_all_time()
    total_ship   = int(metrics.get("total_shipments", 0))
    total_fail   = int(metrics.get("total_failures", 0))
    total_elig   = int(metrics.get("total_eligible", 0))
    total_filed  = int(metrics.get("total_filed", 0))
    total_appr   = int(metrics.get("total_approved", 0))
    total_rej    = int(metrics.get("total_rejected", 0))
    total_await  = int(metrics.get("total_awaiting", 0))

    fail_rate    = (total_fail / total_ship * 100) if total_ship else 0
    at_stake     = total_fail * claim_amt
    recovered    = total_appr * claim_amt
    recovery_pct = (total_appr / total_filed * 100) if total_filed else 0

    # KPI cards
    k1, k2, k3, k4, k5, k6 = st.columns(6)
    k1.metric("📦 Shipments", f"{total_ship:,}")
    k2.metric("🚨 Failures", f"{total_fail:,}", f"{fail_rate:.1f}%")
    k3.metric("📋 Claims Filed", f"{total_filed:,}")
    k4.metric("💸 $ At Stake", f"${at_stake:,.0f}")
    k5.metric("✅ $ Recovered", f"${recovered:,.0f}")
    k6.metric("📈 Recovery Rate", f"{recovery_pct:.1f}%")

    st.markdown("<hr style='margin:12px 0;border:none;border-top:0.5px solid #eee;'>", unsafe_allow_html=True)

    # ── Period Selector ──────────────────────────────────────────────
    period = st.selectbox("📅 Trend Period", ["Weekly", "Monthly", "Yearly"], index=0)
    period_map = {"Weekly": "weekly", "Monthly": "monthly", "Yearly": "yearly"}
    period_key = period_map.get(period, "weekly")

    # ── Trend Chart ──────────────────────────────────────────────────
    trend = q_roi_trend(period_key)

    if not trend.empty:
        trend["at_stake"]  = trend["failures"] * claim_amt
        trend["recovered"] = trend["approved"] * claim_amt
        trend["recovery_rate"] = trend.apply(
            lambda r: (r["approved"] / r["filed"] * 100) if r["filed"] > 0 else 0, axis=1
        )

        fig = go.Figure()

        # Bar: $ At Stake
        fig.add_trace(go.Bar(
            x=trend["period"], y=trend["at_stake"],
            name="$ At Stake",
            marker_color="#FF6B6B",
            opacity=0.7,
        ))

        # Bar: $ Recovered
        fig.add_trace(go.Bar(
            x=trend["period"], y=trend["recovered"],
            name="$ Recovered",
            marker_color="#51CF66",
            opacity=0.85,
        ))

        # Line: Recovery Rate %
        fig.add_trace(go.Scatter(
            x=trend["period"], y=trend["recovery_rate"],
            name="Recovery Rate %",
            yaxis="y2",
            line=dict(color="#845EF7", width=3),
            mode="lines+markers",
            marker=dict(size=6),
        ))

        fig.update_layout(
            barmode="group",
            yaxis=dict(title="Amount ($)", gridcolor="#f0f0f0"),
            yaxis2=dict(title="Recovery Rate %", overlaying="y", side="right", range=[0, 100], gridcolor="#f0f0f0"),
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
            plot_bgcolor="white",
            height=400,
            margin=dict(l=50, r=50, t=30, b=40),
        )

        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No trend data available yet.")

    st.markdown("<hr style='margin:12px 0;border:none;border-top:0.5px solid #eee;'>", unsafe_allow_html=True)

    # ── Row: Carrier Breakdown + Claims Funnel ───────────────────────
    col_left, col_right = st.columns([3, 2])

    with col_left:
        st.markdown("<div style='font-size:13px;font-weight:500;margin-bottom:8px;'>📊 By Ship Method</div>",
                    unsafe_allow_html=True)
        carrier_df = q_roi_by_carrier()
        if not carrier_df.empty:
            carrier_df["failure_rate"] = carrier_df.apply(
                lambda r: (r["failures"] / r["shipments"] * 100) if r["shipments"] > 0 else 0, axis=1
            )
            carrier_df["recovery_rate"] = carrier_df.apply(
                lambda r: (r["approved"] / r["filed"] * 100) if r["filed"] > 0 else 0, axis=1
            )

            fig2 = go.Figure()
            fig2.add_trace(go.Bar(
                y=carrier_df["ship_method"], x=carrier_df["failure_rate"],
                name="Failure Rate %", orientation="h",
                marker_color="#FF6B6B", opacity=0.7,
            ))
            fig2.add_trace(go.Bar(
                y=carrier_df["ship_method"], x=carrier_df["recovery_rate"],
                name="Recovery Rate %", orientation="h",
                marker_color="#51CF66", opacity=0.85,
            ))
            fig2.update_layout(
                barmode="group",
                xaxis=dict(title="%", gridcolor="#f0f0f0"),
                plot_bgcolor="white",
                height=300,
                margin=dict(l=10, r=10, t=10, b=30),
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
            )
            st.plotly_chart(fig2, use_container_width=True)

    with col_right:
        st.markdown("<div style='font-size:13px;font-weight:500;margin-bottom:8px;'>🎯 Claims Funnel</div>",
                    unsafe_allow_html=True)
        funnel_labels = ["Eligible", "Filed", "Awaiting", "Approved", "Rejected"]
        funnel_values = [total_elig, total_filed, total_await, total_appr, total_rej]
        funnel_colors = ["#4DABF7", "#845EF7", "#FCC419", "#51CF66", "#FF6B6B"]

        fig3 = go.Figure(go.Funnel(
            y=funnel_labels,
            x=funnel_values,
            marker=dict(color=funnel_colors),
            textinfo="value+percent initial",
            textposition="inside",
        ))
        fig3.update_layout(
            plot_bgcolor="white",
            height=300,
            margin=dict(l=10, r=10, t=10, b=10),
        )
        st.plotly_chart(fig3, use_container_width=True)

    st.markdown("<hr style='margin:12px 0;border:none;border-top:0.5px solid #eee;'>", unsafe_allow_html=True)

    # ── Summary Table ────────────────────────────────────────────────
    st.markdown("<div style='font-size:13px;font-weight:500;margin-bottom:8px;'>📋 Period Summary</div>",
                unsafe_allow_html=True)

    if not trend.empty:
        display_df = trend[["period", "shipments", "failures", "filed", "approved", "rejected",
                            "at_stake", "recovered", "recovery_rate"]].copy()
        display_df.columns = ["Period", "Shipments", "Failures", "Filed", "Approved", "Rejected",
                              "$ At Stake", "$ Recovered", "Recovery %"]
        display_df["$ At Stake"]  = display_df["$ At Stake"].apply(lambda x: f"${x:,.0f}")
        display_df["$ Recovered"] = display_df["$ Recovered"].apply(lambda x: f"${x:,.0f}")
        display_df["Recovery %"]  = display_df["Recovery %"].apply(lambda x: f"{x:.1f}%")
        st.dataframe(display_df, use_container_width=True, hide_index=True)
    else:
        st.info("No data available.")


# ─────────────────────────────────────────────────────────────────────────────
# PIPELINE LOG
# ─────────────────────────────────────────────────────────────────────────────
def render_pipeline_log():
    st.markdown(
        "<div style='font-size:13px;font-weight:500;margin-bottom:8px;'>📋 Pipeline Run Log</div>",
        unsafe_allow_html=True
    )

    df = q_pipeline_runs()

    if df.empty:
        st.info("No pipeline runs recorded yet. Run the pipeline first.")
        st.markdown("""
**How to trigger:**
- Click **▶ Run Pipeline** button at top of dashboard
- Or run manually on EC2: 
- Daily scheduler runs automatically at midnight PST
- Weekly full run every Monday
        """)
        return

    # Summary metrics
    total_runs   = len(df)
    complete     = len(df[df["status"].isin(["complete","completed"])])
    failed       = len(df[df["status"].isin(["failed","error"])])
    running      = len(df[df["status"] == "running"])
    total_filed  = int(df["filed"].sum() if "filed" in df.columns else 0)
    total_eligible = int(df["eligible"].sum() if "eligible" in df.columns else 0)

    m1, m2, m3, m4, m5, m6 = st.columns(6)
    m1.metric("Total Runs",     total_runs)
    m2.metric("✅ Complete",    complete)
    m3.metric("❌ Failed",      failed)
    m4.metric("⚡ Running",     running)
    m5.metric("Total Eligible", total_eligible)
    m6.metric("Total Filed",    total_filed)
    st.markdown("<hr style='margin:8px 0;border:none;border-top:0.5px solid #eee;'>", unsafe_allow_html=True)

    # Filter
    f1, f2 = st.columns([2, 2])
    trigger_filter = f1.selectbox("Triggered by",
                                  ["All", "manual", "dashboard", "scheduler_daily", "scheduler_weekly"])
    status_filter  = f2.selectbox("Status", ["All", "complete", "failed", "running"])

    if trigger_filter != "All":
        df = df[df["triggered_by"] == trigger_filter]
    if status_filter != "All":
        df = df[df["status"] == status_filter]

    st.caption(f"{len(df)} runs")

    # Headers
    h = st.columns([1.3, 1.3, 1.3, 1.1, 0.7, 0.7, 0.7, 0.7, 0.7, 0.7, 0.7, 0.7])
    for col, label in zip(h, ["Started","Completed","Triggered By","Date Range",
                               "Status","Fetched","Eligible","Drafted","Filed","Skipped","Errors","Dur(s)"]):
        col.markdown(f"<small><b style='color:#888;font-size:11px;'>{label}</b></small>", unsafe_allow_html=True)
    st.markdown("<hr style='margin:3px 0 0;border:none;border-top:0.5px solid #ddd;'>", unsafe_allow_html=True)

    for _, row in df.iterrows():
        started   = str(row.get("started_at","") or "")[:16]
        completed = str(row.get("completed_at","") or "—")[:16]
        triggered = str(row.get("triggered_by","") or "—")
        date_from = str(row.get("date_from","") or "—")
        date_to   = str(row.get("date_to","") or "—")
        status    = str(row.get("status","") or "—")
        duration  = int(row.get("duration_seconds",0) or 0)

        status_colors = {
            "complete": "background:#EAF3DE;color:#3B6D11;",
            "failed":   "background:#FCEBEB;color:#A32D2D;",
            "running":  "background:#E6F1FB;color:#185FA5;",
        }
        sty = status_colors.get(status, "background:#F1EFE8;color:#5F5E5A;")
        status_html = f'<span style="{BADGE_BASE}{sty}">{status.title()}</span>'

        cols = st.columns([1.3, 1.3, 1.3, 1.1, 0.7, 0.7, 0.7, 0.7, 0.7, 0.7, 0.7, 0.7])
        cols[0].markdown(f"<span style='font-size:11px;'>{started}</span>", unsafe_allow_html=True)
        cols[1].markdown(f"<span style='font-size:11px;color:#888;'>{completed}</span>", unsafe_allow_html=True)
        cols[2].markdown(f"<span style='font-size:11px;'>{triggered}</span>", unsafe_allow_html=True)
        cols[3].markdown(f"<span style='font-size:11px;'>{date_from} → {date_to}</span>", unsafe_allow_html=True)
        cols[4].markdown(status_html, unsafe_allow_html=True)
        cols[5].markdown(f"<span style='font-size:12px;'>{int(row.get('orders_fetched',0) or 0)}</span>", unsafe_allow_html=True)
        cols[6].markdown(f"<span style='font-size:12px;'>{int(row.get('eligible',0) or 0)}</span>", unsafe_allow_html=True)
        cols[7].markdown(f"<span style='font-size:12px;'>{int(row.get('drafted',0) or 0)}</span>", unsafe_allow_html=True)
        cols[8].markdown(f"<span style='font-size:12px;'>{int(row.get('filed',0) or 0)}</span>", unsafe_allow_html=True)
        cols[9].markdown(f"<span style='font-size:12px;'>{int(row.get('skipped',0) or 0)}</span>", unsafe_allow_html=True)
        cols[10].markdown(f"<span style='font-size:12px;'>{int(row.get('errors',0) or 0)}</span>", unsafe_allow_html=True)
        cols[11].markdown(f"<span style='font-size:12px;'>{duration}s</span>", unsafe_allow_html=True)

        st.markdown("<hr style='margin:2px 0;border:none;border-top:0.5px solid #eee;'>", unsafe_allow_html=True)


def render_errors():
    st.markdown("<div style='font-size:13px;font-weight:500;margin-bottom:8px;'>⚠️ Pipeline Errors</div>", unsafe_allow_html=True)
    df = q_errors()
    if df.empty:
        st.success("No errors recorded.")
        return
    unresolved = df[df["resolved"] == 0]
    resolved   = df[df["resolved"] == 1]
    c1, c2 = st.columns(2)
    c1.metric("Unresolved", len(unresolved))
    c2.metric("Resolved",   len(resolved))
    if not unresolved.empty:
        st.markdown("<div style='font-size:12px;font-weight:600;color:#A32D2D;margin:8px 0 4px;'>❌ Unresolved</div>", unsafe_allow_html=True)
        for _, row in unresolved.iterrows():
            tid = str(row.get("tracking_id","") or "")
            with st.expander(f"🔴 {row['error_type']} | {row.get('stage','')} — {str(row['created_at'])[:16]}", expanded=False):
                if tid:
                    url = tracking_url(tid)
                    st.markdown(f'**Tracking ID:** <a href="{url}" target="_blank">{tid}</a>', unsafe_allow_html=True)
                st.markdown(f"**Details:** {row['details']}")
    if not resolved.empty:
        with st.expander(f"✅ Resolved ({len(resolved)})", expanded=False):
            st.dataframe(resolved[["tracking_id","error_type","stage","created_at"]], use_container_width=True)


# ─────────────────────────────────────────────────────────────────────────────
# TAB 6 — SETTINGS
# ─────────────────────────────────────────────────────────────────────────────

def render_settings():
    st.markdown(
        "<div style='font-size:13px;font-weight:500;margin-bottom:2px;'>⚙️ System Configuration</div>"
        "<div style='font-size:11px;color:#888;margin-bottom:12px;'>Visible to all. Login required to save.</div>",
        unsafe_allow_html=True
    )
    cfg = load_config()

    st.markdown("<div style='font-size:12px;font-weight:600;color:#555;margin-bottom:4px;'>🎯 Probability & Retry</div>", unsafe_allow_html=True)
    c1, c2, c3 = st.columns(3)
    auto_thresh  = c1.number_input("Auto-resubmit if prob ≥", 0.0, 1.0, step=0.05, value=float(cfg["probability"]["auto_resubmit_threshold"]))
    human_thresh = c2.number_input("Human review lower bound", 0.0, 1.0, step=0.05, value=float(cfg["probability"]["human_review_threshold"]))
    max_attempts = c3.number_input("Max retry attempts", 1, 10, value=int(cfg["retry"]["max_attempts"]))
    st.markdown(f"<div style='font-size:11px;color:#888;margin-bottom:8px;'>≥{auto_thresh:.0%} auto · {human_thresh:.0%}–{auto_thresh:.0%} review · <{human_thresh:.0%} stop</div>", unsafe_allow_html=True)

    st.markdown("<div style='font-size:12px;font-weight:600;color:#555;margin-bottom:4px;'>📅 Filing Windows & Claim</div>", unsafe_allow_html=True)
    c1, c2, c3, c4 = st.columns(4)
    ups_days  = c1.number_input("UPS (days)",   1, 60, int(cfg["filing_windows"]["ups_days"]))
    fedex_days= c2.number_input("FedEx (days)", 1, 60, int(cfg["filing_windows"]["fedex_days"]))
    auto_days = c3.number_input("Auto-file ≤N days", 1, 10, int(cfg["filing_windows"]["auto_file_days_remaining"]))
    claim_amt = c4.number_input("Claim amount ($)", 1.0, 10000.0, float(cfg["claim_amount"]), step=1.0)

    st.markdown("<div style='font-size:12px;font-weight:600;color:#555;margin-bottom:4px;margin-top:8px;'>📧 Email</div>", unsafe_allow_html=True)
    c1, c2, c3 = st.columns(3)
    email_mode = c1.selectbox("Mode", ["manual","auto_generate","auto_send"],
                              index=["manual","auto_generate","auto_send"].index(cfg["email"].get("mode","manual")))
    env = c2.radio("Environment", ["test","production"],
                   index=0 if cfg["email"].get("env","test")=="test" else 1, horizontal=True)
    c3.write("")

    c1, c2 = st.columns(2)
    test_addr = c1.text_input("Test email", cfg["email"].get("test_address","praveen.prakash.82@gmail.com"))
    sender    = c2.text_input("Sender (claims Gmail)", cfg["email"].get("sender","praveenp.1118@gmail.com"))

    if env == "test":
        st.markdown("<div style='font-size:11px;color:#185FA5;margin-bottom:8px;'>📬 Test mode — emails go to test address only.</div>", unsafe_allow_html=True)
    else:
        st.markdown("<div style='font-size:11px;color:#A32D2D;margin-bottom:8px;'>⚠️ Production — real emails sent to UPS (Shippo) and FedEx. CC: logistics@arabellabouquets.com</div>", unsafe_allow_html=True)

    st.markdown("<div style='font-size:12px;font-weight:600;color:#555;margin-bottom:4px;'>⏰ Scheduler</div>", unsafe_allow_html=True)
    c1, c2 = st.columns(2)
    days = ["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday"]
    weekly_day    = c1.selectbox("Weekly full-run day", days, index=days.index(cfg["scheduler"].get("weekly_day","Monday")))
    daily_enabled = c2.checkbox("Daily not-delivered recheck", cfg["scheduler"].get("daily_enabled", True))

    st.divider()
    if st.button("💾 Save Settings", type="primary"):
        cfg["probability"]["auto_resubmit_threshold"]     = auto_thresh
        cfg["probability"]["human_review_threshold"]      = human_thresh
        cfg["retry"]["max_attempts"]                      = max_attempts
        cfg["filing_windows"]["ups_days"]                 = ups_days
        cfg["filing_windows"]["fedex_days"]               = fedex_days
        cfg["filing_windows"]["auto_file_days_remaining"] = auto_days
        cfg["claim_amount"]                               = claim_amt
        cfg["email"]["mode"]                              = email_mode
        cfg["email"]["env"]                               = env
        cfg["email"]["test_address"]                      = test_addr
        cfg["email"]["sender"]                            = sender
        cfg["scheduler"]["weekly_day"]                    = weekly_day
        cfg["scheduler"]["daily_enabled"]                 = daily_enabled
        save_config(cfg)
        st.success("✅ Saved")
        st.cache_data.clear()


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

def main():
    st.set_page_config(
        page_title="BloomDirect Claims",
        page_icon="🌸",
        layout="wide",
        initial_sidebar_state="expanded"
    )

    init_session_state()

    # ── Compact Header ────────────────────────────────────────────────────────
    h_left, h_right = st.columns([5, 1])
    with h_left:
        st.markdown("## 🌸 BloomDirect — Claims Recovery System")
        refreshed = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        if st.button(f"Refreshed: {refreshed}  ·  Legend ▾", key="legend_toggle"):
            st.session_state["show_legend"] = not st.session_state.get("show_legend", False)

    with h_right:
        st.write("")
        if st.button("▶ Run Pipeline", type="primary", key="run_pipeline"):
            with st.spinner("Running pipeline…"):
                try:
                    res = subprocess.run(
                        ["python", os.path.join(BASE_DIR, "scheduler", "scheduler.py"), "--manual"],
                        capture_output=True, text=True, timeout=300,
                        env={**os.environ, "PYTHONPATH": BASE_DIR,
                             "DATABASE_URL": os.getenv("DATABASE_URL",""),
                             "LANGCHAIN_TRACING_V2": "false"}
                    )
                    if res.returncode == 0:
                        st.success("Pipeline completed.")
                        st.cache_data.clear()
                    else:
                        st.error(f"Error:\n{res.stderr[:300]}")
                except Exception as e:
                    st.error(f"Failed: {e}")

    if st.session_state.get("show_legend"):
        st.markdown("""
| Icon | Meaning |
|------|---------|
| 📡 / 🟢 Live API | Real carrier API call made |
| 🟡 Cache | Used cached tracking data |
| 🗃️ Order API | From order data only |
| 🟢 Approved/Filed | Claim approved or filed |
| 🔴 Rejected | Claim rejected |
| 🔵 HITL | Awaiting human review |
        """)

    st.markdown("<hr style='margin:6px 0 12px;border:none;border-top:0.5px solid #ddd;'>", unsafe_allow_html=True)

    # ── Tabs ─────────────────────────────────────────────────────────────────
    tab_labels = ["📊 Dashboard", "🧑‍💼 HITL Queue", "💰 ROI Impact", "📋 Pipeline Log", "⚠️ Errors", "⚙️ Settings"]
    t1, t2, t6, t3, t4, t5 = st.tabs(tab_labels)

    with t1: render_dashboard()
    with t2: render_hitl()
    with t6: render_roi_impact()
    with t3: render_pipeline_log()
    with t4: render_errors()
    with t5: render_settings()


if __name__ == "__main__":
    main()
