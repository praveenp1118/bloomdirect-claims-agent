"""
BloomDirect Claims Recovery System — Streamlit Dashboard
=========================================================

Changes in this version:
  1. L1: "Eligible" → "Eligible For Claim"
  2. L1: "Avg Prob" → "Avg Prob (Claim)"
  3. L1: New "Before Mail Reasoning" column — short_label inline, llm_narrative in popup
  4. L2: Before Mail Reasoning column with popup
  5. L2: Mode-aware ✉ modal — Manual (Generate+Edit+Send) / Auto Generate (Edit+Send) / Auto Send (read-only)
  6. L1 click: spinner + auto-scroll to L2
  7. Settings: compact layout, tighter spacing
  8. Settings: default emails pre-filled

DB Schema (confirmed):
  orders           : partner_order_id, tracking_id, ship_method, ship_date, carrier, occasion_type, created_at
  failures         : failure_id, partner_order_id, tracking_id, failure_type, delay_days, first_bad_event, severity, ship_date, promised_date, detected_at
  claims           : claim_id, failure_id, tracking_id, carrier, ship_method, claim_type, claim_amount, status, attempt_number, probability, gmail_thread_id, carrier_case_id, draft_email_text, short_label, llm_narrative, human_comment, occasion_type, filed, filed_at, created_at, updated_at
  claims_email_log : log_id, claim_id, tracking_id, direction, timestamp, subject, body, status, rejection_reason, recovered_amount
  hitl_queue       : queue_id, claim_id, tracking_id, reason, status, human_comment, days_remaining, created_at, resolved_at, resolved_by
  error_log        : error_id, tracking_id, error_type, stage, details, resolved, created_at, resolved_at
  tracking_cache   : tracking_id, carrier, cached_status, cached_status_date, full_history_json, last_mcp_call, source, updated_at

Tracking URLs (confirmed):
  UPS  : https://www.ups.com/track?loc=en_in&tracknum={ID}&requester=WT/trackdetails
  FedEx: https://www.fedex.com/wtrk/track/?action=track&trackingnumber={ID}&cntry_code=us&locale=en_US
"""

import hashlib
import json
import os
import subprocess
from datetime import datetime

import pandas as pd
import streamlit as st
import streamlit.components.v1 as components
from sqlalchemy import create_engine, text


# ─────────────────────────────────────────────────────────────────────────────
# PATHS
# ─────────────────────────────────────────────────────────────────────────────

BASE_DIR    = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CONFIG_PATH = os.path.join(BASE_DIR, "config", "system_config.json")
DB_PATH     = os.getenv("DATABASE_URL", "sqlite:////app/data/bloomdirect.db").replace("sqlite:///", "")


# ─────────────────────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────────────────────

def _default_config() -> dict:
    return {
        "auth": {
            "username": "Group_05",
            "password_hash": hashlib.sha256("BloomD@2026".encode()).hexdigest()
        },
        "probability": {
            "auto_resubmit_threshold": 0.6,
            "human_review_threshold":  0.3
        },
        "retry": {"max_attempts": 3},
        "filing_windows": {
            "ups_days":                 15,
            "fedex_days":               15,
            "auto_file_days_remaining":  2
        },
        "claim_amount": 100.0,
        "email": {
            "mode":         "manual",
            "env":          "test",
            "test_address": "praveen.prakash.82@gmail.com",
            "sender":       "praveenp.1118@gmail.com"
        },
        "scheduler": {
            "weekly_day":    "Monday",
            "daily_enabled": True
        }
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
    db_url = os.getenv("DATABASE_URL", f"sqlite:///{DB_PATH}")
    return create_engine(db_url, connect_args={"check_same_thread": False})


# ─────────────────────────────────────────────────────────────────────────────
# AUTH
# ─────────────────────────────────────────────────────────────────────────────

def _hash(pw: str) -> str:
    return hashlib.sha256(pw.encode()).hexdigest()


def check_credentials(username: str, password: str) -> bool:
    auth = load_config().get("auth", {})
    return (
        username == auth.get("username", "Group_05") and
        _hash(password) == auth.get("password_hash", "")
    )


def require_login(label: str = "perform this action") -> bool:
    if st.session_state.get("authenticated"):
        return True
    st.warning(f"🔒 Login required to {label}.")
    with st.form(key=f"login_{label}"):
        c1, c2, c3 = st.columns([2, 2, 1])
        user   = c1.text_input("Username")
        pw     = c2.text_input("Password", type="password")
        c3.write("")
        c3.write("")
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
        return (f"https://www.ups.com/track?loc=en_in"
                f"&tracknum={tracking_id}&requester=WT/trackdetails")
    return (f"https://www.fedex.com/wtrk/track/?action=track"
            f"&trackingnumber={tracking_id}&cntry_code=us&locale=en_US")


def tracking_link_html(tracking_id: str, carrier: str = "") -> str:
    url   = tracking_url(tracking_id, carrier)
    label = tracking_id[:16] + "…" if len(tracking_id) > 16 else tracking_id
    return f'<a href="{url}" target="_blank" style="font-size:12px;">{label}</a>'


# ─────────────────────────────────────────────────────────────────────────────
# DATE RESOLUTION
# ─────────────────────────────────────────────────────────────────────────────

SOURCE_ICON = {"cache": "📡", "order_api": "🗃️", "unknown": ""}


def resolve_date(cache_val, order_val) -> tuple:
    if cache_val and str(cache_val).strip() not in ("", "None", "nan"):
        return str(cache_val)[:10], "cache"
    if order_val and str(order_val).strip() not in ("", "None", "nan"):
        return str(order_val)[:10], "order_api"
    return "—", "unknown"


# ─────────────────────────────────────────────────────────────────────────────
# BADGES
# ─────────────────────────────────────────────────────────────────────────────

BADGE_BASE = "display:inline-block;font-size:10px;padding:2px 7px;border-radius:10px;font-weight:500;white-space:nowrap;"

STATUS_STYLES = {
    "approved":     "background:#EAF3DE;color:#3B6D11;",
    "filed":        "background:#EAF3DE;color:#3B6D11;",
    "pending":      "background:#EEEDFE;color:#534AB7;",
    "rejected":     "background:#FCEBEB;color:#A32D2D;",
    "hitl":         "background:#E6F1FB;color:#185FA5;",
    "hitl pending": "background:#E6F1FB;color:#185FA5;",
    "no_claim":     "background:#F1EFE8;color:#5F5E5A;",
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
        "l2_loading":       False,
        "just_filtered":    False,
    }
    for k, v in defaults.items():
        if k not in st.session_state:
            st.session_state[k] = v


def set_l2_filter(sm: str, cat: str):
    st.session_state["l2_sm"]          = sm
    st.session_state["l2_cat"]         = cat
    st.session_state["open_modal_tid"] = None
    st.session_state["open_reason_tid"]= None
    st.session_state["l2_loading"]     = True
    st.session_state["just_filtered"]  = True


def clear_l2_filter():
    st.session_state["l2_sm"]          = None
    st.session_state["l2_cat"]         = None
    st.session_state["open_modal_tid"] = None
    st.session_state["open_reason_tid"]= None


# ─────────────────────────────────────────────────────────────────────────────
# QUERIES
# ─────────────────────────────────────────────────────────────────────────────

@st.cache_data(ttl=60)
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
            COUNT(DISTINCT el.log_id)                                         AS mail_count
        FROM orders o
        LEFT JOIN failures         f  ON  f.tracking_id = o.tracking_id
        LEFT JOIN claims           c  ON  c.tracking_id = o.tracking_id
        LEFT JOIN claims_email_log el ON el.tracking_id = o.tracking_id
        GROUP BY o.ship_method
        ORDER BY o.ship_method
    """)
    with get_engine().connect() as conn:
        return pd.read_sql(sql, conn)


@st.cache_data(ttl=60)
def q_l2(ship_method, category) -> pd.DataFrame:
    clauses = ["1=1"]
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

    sql = text(f"""
        SELECT
            o.tracking_id,
            o.ship_method,
            o.carrier,
            o.ship_date,
            o.partner_order_id          AS order_number,
            tc.cached_status            AS last_event_status,
            tc.cached_status_date       AS cache_last_event,
            o.ship_date                 AS order_last_event,
            f.failure_type,
            f.delay_days,
            c.claim_id,
            c.status                    AS claim_status,
            c.probability,
            c.attempt_number,
            c.filed,
            c.short_label,
            c.llm_narrative,
            c.human_comment,
            c.draft_email_text,
            COUNT(DISTINCT el.log_id)   AS email_count
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


@st.cache_data(ttl=30)
def q_emails(tracking_id: str) -> pd.DataFrame:
    sql = text("""
        SELECT log_id, direction, timestamp, subject, body,
               status, rejection_reason, recovered_amount
        FROM claims_email_log
        WHERE tracking_id = :tid
        ORDER BY timestamp ASC
    """)
    with get_engine().connect() as conn:
        return pd.read_sql(sql, conn, params={"tid": tracking_id})


@st.cache_data(ttl=30)
def q_hitl() -> pd.DataFrame:
    sql = text("""
        SELECT h.queue_id, h.claim_id, h.tracking_id, h.reason,
               h.status, h.days_remaining, h.created_at,
               o.ship_method, o.carrier,
               c.probability, c.attempt_number,
               f.failure_type
        FROM hitl_queue h
        LEFT JOIN orders   o ON o.tracking_id = h.tracking_id
        LEFT JOIN claims   c ON c.claim_id    = h.claim_id
        LEFT JOIN failures f ON f.tracking_id = h.tracking_id
        WHERE h.status = 'pending'
        ORDER BY h.created_at ASC
    """)
    with get_engine().connect() as conn:
        return pd.read_sql(sql, conn)


@st.cache_data(ttl=30)
def q_errors() -> pd.DataFrame:
    sql = text("""
        SELECT error_id, tracking_id, error_type, stage,
               details, resolved, created_at
        FROM error_log
        ORDER BY created_at DESC
        LIMIT 200
    """)
    with get_engine().connect() as conn:
        return pd.read_sql(sql, conn)


# ─────────────────────────────────────────────────────────────────────────────
# AUTO SCROLL JS
# ─────────────────────────────────────────────────────────────────────────────

def scroll_to_l2():
    """Inject JS to scroll the Streamlit page to the L2 section."""
    components.html("""
        <script>
        setTimeout(function() {
            var els = window.parent.document.querySelectorAll('h3, [data-testid="stMarkdownContainer"] h3');
            for (var i = 0; i < els.length; i++) {
                if (els[i].textContent.includes('L2')) {
                    els[i].scrollIntoView({behavior: 'smooth', block: 'start'});
                    break;
                }
            }
        }, 300);
        </script>
    """, height=0)


# ─────────────────────────────────────────────────────────────────────────────
# LLM EMAIL GENERATION
# ─────────────────────────────────────────────────────────────────────────────

def generate_email_draft(tracking_id: str, claim_id: int, row: dict) -> str:
    """Call Claude to generate a claim email draft."""
    try:
        import anthropic
        client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))

        narrative  = str(row.get("llm_narrative", "") or "")
        failure    = str(row.get("failure_type", "") or "")
        carrier    = str(row.get("carrier", "") or "")
        ship_method= str(row.get("ship_method", "") or "")
        ship_date  = str(row.get("ship_date", "") or "")
        occasion   = str(row.get("occasion_type", "") or "General")

        prompt = f"""You are drafting a shipping claim email for BloomDirect, a floral e-commerce company.

Shipment details:
- Tracking ID: {tracking_id}
- Carrier: {carrier}
- Ship Method: {ship_method}
- Ship Date: {ship_date}
- Failure Type: {failure}
- Occasion: {occasion}
- Analysis: {narrative}

Write a professional claim email requesting a refund under the carrier's service guarantee.
- Be firm but professional
- Reference the tracking ID and specific failure
- Request a full shipping charge refund
- Keep it concise (3-4 paragraphs)

Return only the email body, no subject line."""

        response = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=500,
            messages=[{"role": "user", "content": prompt}]
        )
        return response.content[0].text

    except Exception as e:
        return f"Error generating email: {e}"


# ─────────────────────────────────────────────────────────────────────────────
# TAB 1 — DASHBOARD
# ─────────────────────────────────────────────────────────────────────────────

def render_dashboard():

    # ── L1 ───────────────────────────────────────────────────────────────────
    st.markdown(
        "<div style='font-size:13px;font-weight:500;margin-bottom:2px;'>L1 — Summary by Ship Method</div>"
        "<div style='font-size:11px;color:#888;margin-bottom:10px;'>Click any number to drill into L2.</div>",
        unsafe_allow_html=True
    )

    l1 = q_l1()
    if l1.empty:
        st.info("No data yet. Run the pipeline first.")
        return

    # Headers
    h = st.columns([2, 0.7, 0.7, 1, 1.3, 1.1, 1, 1.3])
    for col, label in zip(h, [
        "Ship Method", "Total", "On Time", "Not On Time",
        "Eligible For Claim", "Avg Prob (Claim)", "Emails", "Before Mail Reasoning"
    ]):
        col.markdown(f"<small><b style='color:#555;'>{label}</b></small>", unsafe_allow_html=True)
    st.markdown("<hr style='margin:4px 0 0;border:none;border-top:0.5px solid #ddd;'>", unsafe_allow_html=True)

    for _, row in l1.iterrows():
        sm   = str(row["ship_method"])
        cols = st.columns([2, 0.7, 0.7, 1, 1.3, 1.1, 1, 1.3])

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
            f"<span style='font-size:12px;'>{f'{float(prob):.0%}' if (prob and str(prob) not in ('nan','None','')) else '—'}</span>",
            unsafe_allow_html=True
        )

        with cols[6]:
            if st.button(str(int(row["mail_count"] or 0)), key=f"l1_mail_{sm}"):
                set_l2_filter(sm, "all"); st.rerun()

        # Before Mail Reasoning — blank at L1, shown per row in L2
        cols[7].markdown("", unsafe_allow_html=True)

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

    # Spinner while loading
    if st.session_state.get("l2_loading"):
        st.session_state["l2_loading"] = False
        with st.spinner("Loading shipments…"):
            l2 = q_l2(sm_f, cat_f)
        scroll_to_l2()
    else:
        l2 = q_l2(sm_f, cat_f)

    if l2.empty:
        st.info("No records match this filter.")
        return

    st.caption(f"{len(l2)} records")
    render_l2(l2)


def render_l2(df: pd.DataFrame):
    cfg        = load_config()
    email_mode = cfg.get("email", {}).get("mode", "manual")

    # Column headers
    h = st.columns([1.6, 1.1, 0.9, 0.9, 1.6, 0.9, 1.2, 0.6, 1.4, 0.5])
    for col, label in zip(h, [
        "Tracking ID", "Ship Method", "Order #", "Ship Date",
        "Last Event", "Failure", "Claim Status", "Prob",
        "Before Mail Reasoning", "✉"
    ]):
        col.markdown(
            f"<small><b style='color:#888;font-size:11px;'>{label}</b></small>",
            unsafe_allow_html=True
        )
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

        last_status = str(row.get("last_event_status", "") or "—")
        last_date, last_src = resolve_date(row.get("cache_last_event"), row.get("order_last_event"))
        last_event_html = (
            f"<div style='font-size:12px;'>{last_status}</div>"
            f"<div style='font-size:10px;color:#888;'>{last_date} {SOURCE_ICON[last_src]}</div>"
        )

        cols = st.columns([1.6, 1.1, 0.9, 0.9, 1.6, 0.9, 1.2, 0.6, 1.4, 0.5])

        with cols[0]:
            st.markdown(tracking_link_html(tid, carrier), unsafe_allow_html=True)

        cols[1].markdown(f"<span style='font-size:12px;'>{sm}</span>",          unsafe_allow_html=True)
        cols[2].markdown(f"<span style='font-size:12px;color:#888;'>{order_num}</span>", unsafe_allow_html=True)
        cols[3].markdown(f"<span style='font-size:12px;'>{ship_date}</span>",   unsafe_allow_html=True)
        cols[4].markdown(last_event_html,                                        unsafe_allow_html=True)
        cols[5].markdown(failure_badge_html(failure_type),                       unsafe_allow_html=True)
        cols[6].markdown(status_badge_html(claim_status),                        unsafe_allow_html=True)
        cols[7].markdown(
            f"<span style='font-size:12px;'>{f'{float(prob):.0%}' if (prob and str(prob) not in ('nan','None','')) else '—'}</span>",
            unsafe_allow_html=True
        )

        # Before Mail Reasoning — short label + click for popup
        with cols[8]:
            if short_label or narrative:
                display_label = (short_label[:25] + "…") if len(short_label) > 25 else short_label or "View reasoning"
                if st.button(f"📋 {display_label}", key=f"reason_btn_{tid}",
                             help="View full reasoning"):
                    if st.session_state.get("open_reason_tid") == tid:
                        st.session_state["open_reason_tid"] = None
                    else:
                        st.session_state["open_reason_tid"] = tid
                        st.session_state["open_modal_tid"]  = None
                    st.rerun()
            else:
                st.markdown("<span style='font-size:11px;color:#ccc;'>—</span>", unsafe_allow_html=True)

        # ✉ Email button
        with cols[9]:
            btn_label = f"✉{email_count}" if email_count else "✉"
            if st.button(btn_label, key=f"email_btn_{tid}"):
                if st.session_state.get("open_modal_tid") == tid:
                    st.session_state["open_modal_tid"] = None
                else:
                    st.session_state["open_modal_tid"]  = tid
                    st.session_state["open_reason_tid"] = None
                st.rerun()

        st.markdown("<hr style='margin:2px 0;border:none;border-top:0.5px solid #eee;'>", unsafe_allow_html=True)

        # ── Reasoning Popup ───────────────────────────────────────────────
        if st.session_state.get("open_reason_tid") == tid:
            with st.expander(f"📋 Reasoning — {short_label or tid}", expanded=True):
                url = tracking_url(tid, carrier)
                st.markdown(
                    f'🔗 <a href="{url}" target="_blank">Open carrier tracking</a>',
                    unsafe_allow_html=True
                )
                st.divider()
                if narrative:
                    st.markdown(
                        f"""<div style="background:var(--background-color,#f8f8ff);
                            border-left:3px solid #AFA9EC;border-radius:0 6px 6px 0;
                            padding:12px 16px;font-size:13px;line-height:1.7;">
                            <div style="font-size:11px;font-weight:600;color:#534AB7;margin-bottom:6px;">
                            LLM REASONING</div>{narrative}</div>""",
                        unsafe_allow_html=True
                    )
                else:
                    st.info("No reasoning available for this shipment yet.")
                if st.button("Close", key=f"close_reason_{tid}"):
                    st.session_state["open_reason_tid"] = None
                    st.rerun()

        # ── Email Modal ───────────────────────────────────────────────────
        if st.session_state.get("open_modal_tid") == tid:
            render_email_modal(tid, carrier, row, email_mode)


def render_email_modal(tracking_id: str, carrier: str, row: dict, email_mode: str):
    """Mode-aware email modal."""
    short_label = str(row.get("short_label", "") or "")
    narrative   = str(row.get("llm_narrative", "") or "")
    human_note  = str(row.get("human_comment", "") or "")
    draft_text  = str(row.get("draft_email_text", "") or "")
    claim_id    = row.get("claim_id")
    url         = tracking_url(tracking_id, carrier)

    emails = q_emails(tracking_id)

    with st.expander(
        f"✉ {tracking_id} — {short_label or 'Email Thread'}",
        expanded=True
    ):
        # Header
        st.markdown(
            f'🔗 <a href="{url}" target="_blank">Open carrier tracking</a>',
            unsafe_allow_html=True
        )

        # LLM Narrative (brief)
        if narrative:
            st.markdown(
                f"""<div style="border-left:3px solid #AFA9EC;padding:8px 12px;
                    background:#f8f8ff;border-radius:0 6px 6px 0;
                    font-size:12px;margin:8px 0;line-height:1.6;">
                    <b style="font-size:11px;color:#534AB7;">REASONING</b><br>{narrative[:300]}{"…" if len(narrative)>300 else ""}
                </div>""",
                unsafe_allow_html=True
            )

        st.divider()

        # ── MODE: AUTO SEND — read only ───────────────────────────────────
        if email_mode == "auto_send":
            st.caption("Auto Send mode — emails sent automatically.")
            _render_email_thread(emails)

        # ── MODE: AUTO GENERATE — show draft + Send button ────────────────
        elif email_mode == "auto_generate":
            if not emails.empty:
                _render_email_thread(emails)
            elif draft_text:
                st.caption("Email drafted automatically. Review and send:")
                edited = st.text_area("Email draft:", value=draft_text,
                                      height=200, key=f"draft_{tracking_id}")
                if human_note:
                    st.markdown(f"**👤 Human comment:** {human_note}")
                col1, col2 = st.columns([1, 4])
                with col1:
                    if st.button("📤 Send", key=f"send_{tracking_id}", type="primary"):
                        _send_email(tracking_id, claim_id, edited)
            else:
                st.info("Email not yet drafted. Pipeline will draft automatically on next run.")

        # ── MODE: MANUAL — Generate → Edit → Send ────────────────────────
        else:
            if not emails.empty:
                # Thread exists — show it
                _render_email_thread(emails)
                # Allow sending follow-up
                st.divider()
                st.caption("Send a follow-up or resubmission:")
                new_draft = st.text_area("Draft:", height=150,
                                         key=f"followup_{tracking_id}")
                if st.button("📤 Send Follow-up", key=f"send_followup_{tracking_id}"):
                    _send_email(tracking_id, claim_id, new_draft)

            elif draft_text:
                # Draft exists, not sent yet
                st.caption("Draft ready. Review and send:")
                edited = st.text_area("Email draft:", value=draft_text,
                                      height=200, key=f"draft_{tracking_id}")
                col1, col2 = st.columns([1, 4])
                with col1:
                    if st.button("📤 Send", key=f"send_{tracking_id}", type="primary"):
                        _send_email(tracking_id, claim_id, edited)

            else:
                # No draft, no thread — Generate button
                st.info("No email drafted yet for this shipment.")
                if claim_id:
                    if st.button("✍ Generate Email", key=f"gen_{tracking_id}", type="primary"):
                        with st.spinner("Generating email draft…"):
                            draft = generate_email_draft(tracking_id, claim_id, dict(row))
                        st.session_state[f"generated_draft_{tracking_id}"] = draft
                        st.rerun()

                    # Show generated draft if just created
                    generated = st.session_state.get(f"generated_draft_{tracking_id}", "")
                    if generated:
                        edited = st.text_area("Email draft (editable):", value=generated,
                                              height=200, key=f"draft_{tracking_id}")
                        if human_note:
                            st.markdown(f"**👤 Human comment:** {human_note}")
                        col1, col2 = st.columns([1, 4])
                        with col1:
                            if st.button("📤 Send", key=f"send_{tracking_id}", type="primary"):
                                _send_email(tracking_id, claim_id, edited)
                else:
                    st.caption("No claim record yet — run pipeline first.")

        if st.button("Close", key=f"close_email_{tracking_id}"):
            st.session_state["open_modal_tid"] = None
            st.rerun()


def _render_email_thread(emails: pd.DataFrame):
    """Render email thread inside modal."""
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

        icon  = "↑" if direction == "outbound" else "↓"
        color = "#185FA5" if direction == "outbound" else "#3B6D11"
        label = f"Sent · {ts}" if direction == "outbound" else f"Carrier reply · {ts}"

        st.markdown(
            f"""<div style="border:0.5px solid #e0e0e0;border-radius:6px;
                padding:10px 14px;margin-bottom:8px;">
                <div style="font-size:11px;font-weight:600;color:{color};margin-bottom:3px;">
                {icon} {label}</div>
                <div style="font-size:12px;font-weight:500;margin-bottom:4px;">{subject}</div>
                <div style="font-size:12px;color:#666;line-height:1.5;">
                {body[:400]}{"…" if len(body)>400 else ""}</div>
                {"<div style='font-size:12px;color:#A32D2D;margin-top:6px;'>❌ " + rejection + "</div>" if rejection else ""}
                {"<div style='font-size:12px;color:#3B6D11;margin-top:6px;'>✅ Recovered: $" + f"{float(recovered):.2f}" + "</div>" if recovered else ""}
            </div>""",
            unsafe_allow_html=True
        )


def _send_email(tracking_id: str, claim_id, draft: str):
    """Send email via MCP."""
    try:
        import sys
        sys.path.insert(0, BASE_DIR)
        from mcp_servers.email_claims_mcp import send_claim_email
        cfg     = load_config()
        env     = cfg.get("email", {}).get("env", "test")
        carrier = ""
        with get_engine().connect() as conn:
            r = conn.execute(
                text("SELECT carrier FROM orders WHERE tracking_id=:tid"),
                {"tid": tracking_id}
            ).fetchone()
            if r:
                carrier = r[0]

        to_addr = cfg["email"]["test_address"] if env == "test" else (
            "support@shippo.com" if "UPS" in carrier.upper() else "file.claim@fedex.com"
        )
        send_claim_email(
            to=to_addr,
            subject=f"Service Guarantee Claim — {tracking_id}",
            body=draft,
            claim_id=claim_id,
            carrier=carrier,
            tracking_id=tracking_id,
        )
        st.success(f"✅ Email sent to {to_addr}")
        st.cache_data.clear()
    except Exception as e:
        st.error(f"Failed to send: {e}")


# ─────────────────────────────────────────────────────────────────────────────
# TAB 2 — HITL QUEUE
# ─────────────────────────────────────────────────────────────────────────────

def render_hitl():
    st.subheader("🧑‍💼 Human-in-the-Loop Queue")
    if not require_login("approve or reject claims"):
        return

    queue = q_hitl()
    if queue.empty:
        st.success("✅ No pending items.")
        return

    st.info(f"{len(queue)} item(s) awaiting review.")
    for _, row in queue.iterrows():
        tid     = str(row["tracking_id"])
        carrier = str(row.get("carrier", ""))
        sm      = str(row.get("ship_method", ""))
        prob    = float(row.get("probability", 0) or 0)
        days    = int(row.get("days_remaining", 0) or 0)
        url     = tracking_url(tid, carrier)

        with st.expander(
            f"🔵 {tid}  |  {row.get('reason','')}  |  Prob: {prob:.0%}  |  {days}d left",
            expanded=True
        ):
            c1, c2 = st.columns(2)
            c1.markdown(f"**Ship Method:** {sm}")
            c1.markdown(f"**Failure Type:** {row.get('failure_type','—')}")
            c1.markdown(f"**Attempt #:** {int(row.get('attempt_number',0) or 0)}")
            c2.markdown(f"**Probability:** {prob:.0%}")
            c2.markdown(f"**Days Remaining:** {days}")
            c2.markdown(f"**Reason:** {row.get('reason','—')}")
            c2.markdown(f'🔗 <a href="{url}" target="_blank">Open Carrier Tracking</a>',
                        unsafe_allow_html=True)
            comment = st.text_area("Human comment:", key=f"hitl_comment_{row['queue_id']}")
            b1, b2, b3 = st.columns(3)
            with b1:
                if st.button("✅ Approve & Send", key=f"hitl_approve_{row['queue_id']}"):
                    _hitl_action(int(row["queue_id"]), int(row["claim_id"]), tid, "approved", comment)
            with b2:
                if st.button("⏭ Skip", key=f"hitl_skip_{row['queue_id']}"):
                    st.toast("Skipped.")
            with b3:
                if st.button("❌ Close Claim", key=f"hitl_close_{row['queue_id']}"):
                    _hitl_action(int(row["queue_id"]), int(row["claim_id"]), tid, "closed", comment)


def _hitl_action(queue_id: int, claim_id: int, tracking_id: str, action: str, comment: str):
    with get_engine().connect() as conn:
        conn.execute(
            text("UPDATE hitl_queue SET status=:a, resolved_at=:now WHERE queue_id=:id"),
            {"a": action, "now": datetime.utcnow(), "id": queue_id}
        )
        if comment.strip():
            conn.execute(
                text("UPDATE claims SET human_comment=:c WHERE claim_id=:cid"),
                {"c": comment.strip(), "cid": claim_id}
            )
        conn.commit()
    st.success(f"Claim {tracking_id} → {action}.")
    st.cache_data.clear()
    st.rerun()


# ─────────────────────────────────────────────────────────────────────────────
# TAB 3 — ERRORS
# ─────────────────────────────────────────────────────────────────────────────

def render_errors():
    st.markdown(
        "<div style='font-size:13px;font-weight:500;margin-bottom:2px;'>⚠️ Pipeline Errors</div>",
        unsafe_allow_html=True
    )
    df = q_errors()
    if df.empty:
        st.success("No errors recorded.")
        return

    unresolved = df[df["resolved"] == 0]
    resolved   = df[df["resolved"] == 1]

    c1, c2 = st.columns(2)
    c1.metric("Unresolved", len(unresolved))
    c2.metric("Resolved", len(resolved))

    if not unresolved.empty:
        st.markdown("<div style='font-size:12px;font-weight:600;color:#A32D2D;margin:8px 0 4px;'>❌ Unresolved</div>", unsafe_allow_html=True)
        for _, row in unresolved.iterrows():
            tid = str(row.get("tracking_id", "") or "")
            with st.expander(
                f"🔴 {row['error_type']} | {row.get('stage','')} — {str(row['created_at'])[:16]}",
                expanded=False
            ):
                if tid:
                    url = tracking_url(tid)
                    st.markdown(f'**Tracking ID:** <a href="{url}" target="_blank">{tid}</a>',
                                unsafe_allow_html=True)
                st.markdown(f"**Details:** {row['details']}")

    if not resolved.empty:
        with st.expander(f"✅ Resolved ({len(resolved)})", expanded=False):
            st.dataframe(resolved[["tracking_id","error_type","stage","created_at"]],
                         use_container_width=True)


# ─────────────────────────────────────────────────────────────────────────────
# TAB 4 — SETTINGS  (compact)
# ─────────────────────────────────────────────────────────────────────────────

def render_settings():
    st.markdown(
        "<div style='font-size:13px;font-weight:500;margin-bottom:2px;'>⚙️ System Configuration</div>"
        "<div style='font-size:11px;color:#888;margin-bottom:12px;'>Visible to all. Login required to save.</div>",
        unsafe_allow_html=True
    )

    cfg = load_config()

    # ── Row 1: Probability + Retry ────────────────────────────────────────
    st.markdown("<div style='font-size:12px;font-weight:600;color:#555;margin-bottom:4px;'>🎯 Probability & Retry</div>", unsafe_allow_html=True)
    c1, c2, c3 = st.columns(3)
    auto_thresh  = c1.number_input("Auto-resubmit if prob ≥", 0.0, 1.0, step=0.05,
                                   value=float(cfg["probability"]["auto_resubmit_threshold"]))
    human_thresh = c2.number_input("Human review lower bound", 0.0, 1.0, step=0.05,
                                   value=float(cfg["probability"]["human_review_threshold"]))
    max_attempts = c3.number_input("Max retry attempts", 1, 10,
                                   value=int(cfg["retry"]["max_attempts"]))
    st.markdown(
        f"<div style='font-size:11px;color:#888;margin-bottom:8px;'>"
        f"≥{auto_thresh:.0%} auto-resubmit · {human_thresh:.0%}–{auto_thresh:.0%} human review · <{human_thresh:.0%} stop</div>",
        unsafe_allow_html=True
    )

    # ── Row 2: Filing Windows + Claim Amount ─────────────────────────────
    st.markdown("<div style='font-size:12px;font-weight:600;color:#555;margin-bottom:4px;'>📅 Filing Windows & Claim</div>", unsafe_allow_html=True)
    c1, c2, c3, c4 = st.columns(4)
    ups_days   = c1.number_input("UPS (days)",   1, 60, int(cfg["filing_windows"]["ups_days"]))
    fedex_days = c2.number_input("FedEx (days)", 1, 60, int(cfg["filing_windows"]["fedex_days"]))
    auto_days  = c3.number_input("Auto-file ≤N days left", 1, 10,
                                  int(cfg["filing_windows"]["auto_file_days_remaining"]))
    claim_amt  = c4.number_input("Claim amount ($)", 1.0, 10000.0,
                                  float(cfg["claim_amount"]), step=1.0)

    # ── Row 3: Email ──────────────────────────────────────────────────────
    st.markdown("<div style='font-size:12px;font-weight:600;color:#555;margin-bottom:4px;margin-top:8px;'>📧 Email</div>", unsafe_allow_html=True)
    c1, c2, c3 = st.columns(3)
    email_mode = c1.selectbox(
        "Mode",
        ["manual", "auto_generate", "auto_send"],
        index=["manual","auto_generate","auto_send"].index(cfg["email"].get("mode","manual")),
        help="manual: Generate+Edit+Send · auto_generate: LLM drafts, user sends · auto_send: fully automatic"
    )
    env = c2.radio("Environment", ["test","production"],
                   index=0 if cfg["email"].get("env","test")=="test" else 1,
                   horizontal=True)
    c3.write("")

    c1, c2 = st.columns(2)
    test_addr = c1.text_input("Test email", cfg["email"].get("test_address","praveen.prakash.82@gmail.com"))
    sender    = c2.text_input("Sender (claims Gmail)", cfg["email"].get("sender","praveenp.1118@gmail.com"))

    if env == "test":
        st.markdown("<div style='font-size:11px;color:#185FA5;margin-bottom:8px;'>📬 Test mode — emails go to test address only.</div>", unsafe_allow_html=True)
    else:
        st.markdown("<div style='font-size:11px;color:#A32D2D;margin-bottom:8px;'>⚠️ Production — real emails sent to UPS (Shippo) and FedEx.</div>", unsafe_allow_html=True)

    # ── Row 4: Scheduler ─────────────────────────────────────────────────
    st.markdown("<div style='font-size:12px;font-weight:600;color:#555;margin-bottom:4px;'>⏰ Scheduler</div>", unsafe_allow_html=True)
    c1, c2 = st.columns(2)
    days = ["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday"]
    weekly_day    = c1.selectbox("Weekly full-run day", days,
                                  index=days.index(cfg["scheduler"].get("weekly_day","Monday")))
    daily_enabled = c2.checkbox("Daily not-delivered recheck",
                                 cfg["scheduler"].get("daily_enabled", True))

    st.markdown("<div style='margin-top:12px;'>", unsafe_allow_html=True)
    if st.button("💾 Save Settings", type="primary"):
        if not require_login("save settings"):
            return
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
        st.success("✅ Saved to system_config.json")
        st.cache_data.clear()
    st.markdown("</div>", unsafe_allow_html=True)


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

def main():
    st.set_page_config(
        page_title="BloomDirect Claims",
        page_icon="🌸",
        layout="wide",
        initial_sidebar_state="collapsed"
    )

    init_session_state()

    # ── Compact Header ────────────────────────────────────────────────────
    h_left, h_right = st.columns([5, 1])
    with h_left:
        st.markdown("## 🌸 BloomDirect — Claims Recovery System")
        refreshed = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        if st.button(f"Refreshed: {refreshed}  ·  Legend ▾", key="legend_toggle"):
            st.session_state["show_legend"] = not st.session_state.get("show_legend", False)

    with h_right:
        st.write("")
        if st.button("▶ Run Pipeline", type="primary", key="run_pipeline_header"):
            with st.spinner("Running pipeline…"):
                try:
                    res = subprocess.run(
                        ["python", os.path.join(BASE_DIR, "scheduler", "scheduler.py"), "--manual"],
                        capture_output=True, text=True, timeout=300,
                        env={**os.environ, "PYTHONPATH": BASE_DIR,
                             "DATABASE_URL": os.getenv("DATABASE_URL", "")}
                    )
                    if res.returncode == 0:
                        st.success("Pipeline completed.")
                        st.cache_data.clear()
                    else:
                        st.error(f"Pipeline error:\n{res.stderr[:300]}")
                except Exception as e:
                    st.error(f"Failed: {e}")

    if st.session_state.get("show_legend"):
        st.markdown("""
| Icon | Meaning |
|------|---------|
| 📡 | Date from MCP carrier tracking |
| 🗃️ | Date from Order API (fallback) |
| 🟢 | Claim approved/filed |
| 🔴 | Claim rejected |
| 🔵 | Awaiting human review (HITL) |
| ⚪ | Not yet filed |
        """)

    st.markdown("<hr style='margin:6px 0 12px;border:none;border-top:0.5px solid #ddd;'>",
                unsafe_allow_html=True)

    t1, t2, t3, t4 = st.tabs(["📊 Dashboard", "🧑‍💼 HITL Queue", "⚠️ Errors", "⚙️ Settings"])
    with t1: render_dashboard()
    with t2: render_hitl()
    with t3: render_errors()
    with t4: render_settings()


if __name__ == "__main__":
    main()
