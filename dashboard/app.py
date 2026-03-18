"""
BloomDirect Claims Recovery System — Streamlit Dashboard
=========================================================

Tabs:
  1. Dashboard  — compact header → L1 summary → L2 compact table with ✉ modal
  2. HITL Queue — Human-in-the-Loop approvals (login required for actions)
  3. Errors     — Pipeline errors and alerts
  4. Settings   — System config (login required to save)

Layout:
  - Compact header: title + refresh time + Legend link + Run Pipeline (right)
  - L1 visible immediately on load, no scrolling
  - L2: compact rows, one line per shipment, ✉ button opens modal popup
  - Modal: LLM narrative + email thread (or "no emails yet")
  - No L3 section — modal replaces it entirely

L2 Columns:
  Tracking ID | Ship Method | Order # | Ship Date | Last Event | Failure | Claim Status | Prob | ✉

Last Event column:
  - Status text (top line)
  - Date + source icon (bottom line): 📡 = tracking_cache (MCP), 🗃️ = order API fallback

Confirmed Tracking URLs:
  UPS  (starts with 1Z or carrier=UPS):
    https://www.ups.com/track?loc=en_in&tracknum={ID}&requester=WT/trackdetails
  FedEx (everything else):
    https://www.fedex.com/wtrk/track/?action=track&trackingnumber={ID}&cntry_code=us&locale=en_US

DB Schema (confirmed):
  orders           : partner_order_id, tracking_id, ship_method, ship_date, carrier, occasion_type, created_at
  failures         : failure_id, partner_order_id, tracking_id, failure_type, delay_days, first_bad_event, severity, ship_date, promised_date, detected_at
  claims           : claim_id, failure_id, tracking_id, carrier, ship_method, claim_type, claim_amount, status, attempt_number, probability, gmail_thread_id, carrier_case_id, draft_email_text, short_label, llm_narrative, human_comment, occasion_type, filed, filed_at, created_at, updated_at
  claims_email_log : log_id, claim_id, tracking_id, direction, timestamp, subject, body, status, rejection_reason, recovered_amount
  hitl_queue       : queue_id, claim_id, tracking_id, reason, status, human_comment, days_remaining, created_at, resolved_at, resolved_by
  error_log        : error_id, tracking_id, error_type, stage, details, resolved, created_at, resolved_at
  tracking_cache   : tracking_id, carrier, cached_status, cached_status_date, full_history_json, last_mcp_call, source, updated_at
  recovery         : recovery_id, claim_id, recovered_amount, credit_date, method, created_at
"""

import hashlib
import json
import os
import subprocess
from datetime import datetime

import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text


# ─────────────────────────────────────────────────────────────────────────────
# PATHS
# ─────────────────────────────────────────────────────────────────────────────

BASE_DIR    = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CONFIG_PATH = os.path.join(BASE_DIR, "system_config.json")
DB_PATH     = os.path.join(BASE_DIR, "bloomdirect.db")


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
            "test_address": "",
            "sender":       ""
        },
        "scheduler": {
            "weekly_day":    "Monday",
            "daily_enabled": True
        }
    }


def load_config() -> dict:
    if not os.path.exists(CONFIG_PATH):
        cfg = _default_config()
        save_config(cfg)
        return cfg
    with open(CONFIG_PATH) as f:
        return json.load(f)


def save_config(cfg: dict):
    with open(CONFIG_PATH, "w") as f:
        json.dump(cfg, f, indent=2)


# ─────────────────────────────────────────────────────────────────────────────
# DATABASE
# ─────────────────────────────────────────────────────────────────────────────

@st.cache_resource
def get_engine():
    return create_engine(
        f"sqlite:///{DB_PATH}",
        connect_args={"check_same_thread": False}
    )


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
        return (
            f"https://www.ups.com/track?loc=en_in"
            f"&tracknum={tracking_id}&requester=WT/trackdetails"
        )
    return (
        f"https://www.fedex.com/wtrk/track/?action=track"
        f"&trackingnumber={tracking_id}&cntry_code=us&locale=en_US"
    )


def tracking_link_html(tracking_id: str, carrier: str = "") -> str:
    url   = tracking_url(tracking_id, carrier)
    label = tracking_id[:16] + "…" if len(tracking_id) > 16 else tracking_id
    return f'<a href="{url}" target="_blank" style="font-size:12px;">{label}</a>'


# ─────────────────────────────────────────────────────────────────────────────
# DATE RESOLUTION  (tracking_cache first → order API fallback)
# ─────────────────────────────────────────────────────────────────────────────

SOURCE_ICON = {"cache": "📡", "order_api": "🗃️", "unknown": ""}


def resolve_date(cache_val, order_val) -> tuple:
    if cache_val and str(cache_val).strip() not in ("", "None", "nan"):
        return str(cache_val)[:10], "cache"
    if order_val and str(order_val).strip() not in ("", "None", "nan"):
        return str(order_val)[:10], "order_api"
    return "—", "unknown"


# ─────────────────────────────────────────────────────────────────────────────
# STATUS BADGE HTML
# ─────────────────────────────────────────────────────────────────────────────

BADGE_STYLES = {
    "approved":     "background:#EAF3DE;color:#3B6D11;",
    "filed":        "background:#EAF3DE;color:#3B6D11;",
    "pending":      "background:#EEEDFE;color:#534AB7;",
    "rejected":     "background:#FCEBEB;color:#A32D2D;",
    "hitl":         "background:#E6F1FB;color:#185FA5;",
    "hitl pending": "background:#E6F1FB;color:#185FA5;",
    "no_claim":     "background:#F1EFE8;color:#5F5E5A;",
}

FAILURE_BADGE_STYLES = {
    "late":    "background:#FAEEDA;color:#854F0B;",
    "damage":  "background:#FCEBEB;color:#A32D2D;",
    "lost":    "background:#FCEBEB;color:#A32D2D;",
    "unknown": "background:#F1EFE8;color:#5F5E5A;",
}

BADGE_BASE = (
    "display:inline-block;font-size:10px;padding:2px 7px;"
    "border-radius:10px;font-weight:500;white-space:nowrap;"
)


def status_badge_html(val: str) -> str:
    v   = str(val or "pending").lower().strip()
    sty = BADGE_STYLES.get(v, "background:#F1EFE8;color:#5F5E5A;")
    return f'<span style="{BADGE_BASE}{sty}">{v.replace("_", " ").title()}</span>'


def failure_badge_html(val: str) -> str:
    v   = str(val or "unknown").lower().strip()
    sty = FAILURE_BADGE_STYLES.get(v, "background:#F1EFE8;color:#5F5E5A;")
    return f'<span style="{BADGE_BASE}{sty}">{v.upper()}</span>'


# ─────────────────────────────────────────────────────────────────────────────
# SESSION STATE
# ─────────────────────────────────────────────────────────────────────────────

def init_session_state():
    defaults = {
        "authenticated": False,
        "l2_sm":         None,
        "l2_cat":        None,
        "show_legend":   False,
    }
    for k, v in defaults.items():
        if k not in st.session_state:
            st.session_state[k] = v


def set_l2_filter(sm: str, cat: str):
    st.session_state["l2_sm"]  = sm
    st.session_state["l2_cat"] = cat


def clear_l2_filter():
    st.session_state["l2_sm"]  = None
    st.session_state["l2_cat"] = None


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
            c.short_label,
            c.llm_narrative,
            c.human_comment,
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
# EMAIL MODAL  (rendered as HTML component via st.components)
# ─────────────────────────────────────────────────────────────────────────────

def show_email_modal(tracking_id: str, carrier: str, short_label: str,
                     narrative: str, human_comment: str):
    """Renders email thread + narrative for one tracking_id in an expander (modal substitute)."""
    emails = q_emails(tracking_id)
    url    = tracking_url(tracking_id, carrier)

    with st.expander(
        f"✉ {tracking_id} — {short_label or 'Email Thread'}",
        expanded=True
    ):
        # Tracking link
        st.markdown(
            f'🔗 <a href="{url}" target="_blank">Open carrier tracking</a>',
            unsafe_allow_html=True
        )
        st.divider()

        # LLM Narrative
        if narrative and narrative.strip():
            st.markdown(
                f"""<div style="background:var(--background-color,#f8f8ff);
                    border-left:3px solid #AFA9EC;border-radius:0 6px 6px 0;
                    padding:10px 14px;margin-bottom:12px;font-size:13px;
                    line-height:1.6;">
                    <div style="font-size:11px;font-weight:600;color:#534AB7;
                    margin-bottom:4px;">LLM NARRATIVE</div>
                    {narrative}
                </div>""",
                unsafe_allow_html=True
            )

        # Human comment
        if human_comment and human_comment.strip():
            st.markdown(
                f"""<div style="background:var(--background-color,#fffdf0);
                    border-left:3px solid #EF9F27;border-radius:0 6px 6px 0;
                    padding:10px 14px;margin-bottom:12px;font-size:13px;">
                    <div style="font-size:11px;font-weight:600;color:#854F0B;
                    margin-bottom:4px;">HUMAN COMMENT</div>
                    {human_comment}
                </div>""",
                unsafe_allow_html=True
            )

        # Email thread
        st.markdown(
            f"<div style='font-size:11px;font-weight:600;color:#888;margin-bottom:8px;'>"
            f"EMAIL THREAD ({len(emails)})</div>",
            unsafe_allow_html=True
        )

        if emails.empty:
            st.info("No emails filed yet for this shipment.")
        else:
            for _, email in emails.iterrows():
                direction = str(email.get("direction", "outbound"))
                ts        = str(email.get("timestamp", ""))[:16]
                subject   = str(email.get("subject", "(no subject)"))
                body      = str(email.get("body", "") or "")
                rejection = str(email.get("rejection_reason", "") or "")
                recovered = email.get("recovered_amount")

                if direction == "outbound":
                    icon  = "↑"
                    color = "#185FA5"
                    label = f"Sent · {ts}"
                else:
                    icon  = "↓"
                    color = "#3B6D11"
                    label = f"Carrier reply · {ts}"

                st.markdown(
                    f"""<div style="border:0.5px solid var(--border-color,#e0e0e0);
                        border-radius:6px;padding:10px 14px;margin-bottom:8px;">
                        <div style="font-size:11px;font-weight:600;color:{color};
                        margin-bottom:3px;">{icon} {label}</div>
                        <div style="font-size:12px;font-weight:500;margin-bottom:4px;">{subject}</div>
                        <div style="font-size:12px;color:#666;line-height:1.5;">{body[:400]}{"…" if len(body)>400 else ""}</div>
                        {"<div style='font-size:12px;color:#A32D2D;margin-top:6px;'>❌ Rejection: " + rejection + "</div>" if rejection else ""}
                        {"<div style='font-size:12px;color:#3B6D11;margin-top:6px;'>✅ Recovered: $" + f"{float(recovered):.2f}" + "</div>" if recovered else ""}
                    </div>""",
                    unsafe_allow_html=True
                )


# ─────────────────────────────────────────────────────────────────────────────
# TAB 1 — DASHBOARD
# ─────────────────────────────────────────────────────────────────────────────

def render_dashboard():

    # ── L1 ───────────────────────────────────────────────────────────────────
    st.markdown(
        "<div style='font-size:13px;font-weight:500;color:var(--text-color);margin-bottom:2px;'>"
        "L1 — Summary by Ship Method</div>"
        "<div style='font-size:11px;color:#888;margin-bottom:10px;'>"
        "Click any number to drill into L2 for that ship method + category.</div>",
        unsafe_allow_html=True
    )

    l1 = q_l1()
    if l1.empty:
        st.info("No data yet. Run the pipeline first.")
        return

    # Header row
    cols = st.columns([2, 0.8, 0.8, 1.1, 1.1, 1, 1])
    for col, label in zip(cols, [
        "Ship Method", "Total", "On Time", "Not On Time", "Eligible", "Avg Prob", "Emails"
    ]):
        col.markdown(f"<small><b>{label}</b></small>", unsafe_allow_html=True)
    st.markdown(
        "<hr style='margin:4px 0 0 0;border:none;border-top:0.5px solid #ddd;'>",
        unsafe_allow_html=True
    )

    for _, row in l1.iterrows():
        sm   = str(row["ship_method"])
        cols = st.columns([2, 0.8, 0.8, 1.1, 1.1, 1, 1])

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
            f"<span style='font-size:12px;'>{f'{float(prob):.0%}' if prob else '—'}</span>",
            unsafe_allow_html=True
        )

        with cols[6]:
            if st.button(str(int(row["mail_count"] or 0)), key=f"l1_mail_{sm}"):
                set_l2_filter(sm, "all"); st.rerun()

    st.markdown(
        "<hr style='margin:4px 0 16px 0;border:none;border-top:0.5px solid #ddd;'>",
        unsafe_allow_html=True
    )

    # ── L2 ───────────────────────────────────────────────────────────────────
    sm_f  = st.session_state["l2_sm"]
    cat_f = st.session_state["l2_cat"]

    if sm_f:
        cat_label = (cat_f or "all").replace("_", " ").title()
        hdr_col, btn_col = st.columns([5, 1])
        hdr_col.markdown(
            f"<div style='font-size:13px;font-weight:500;'>"
            f"L2 — {sm_f} → {cat_label}</div>",
            unsafe_allow_html=True
        )
        with btn_col:
            if st.button("✕ Clear"):
                clear_l2_filter(); st.rerun()
    else:
        st.markdown(
            "<div style='font-size:13px;font-weight:500;margin-bottom:6px;'>"
            "L2 — All Shipments</div>",
            unsafe_allow_html=True
        )

    l2 = q_l2(sm_f, cat_f)
    if l2.empty:
        st.info("No records match this filter.")
        return

    st.caption(f"{len(l2)} records")
    render_l2(l2)


def render_l2(df: pd.DataFrame):
    # Column headers
    h = st.columns([1.6, 1.2, 0.9, 0.9, 1.8, 1, 1.3, 0.6, 0.5])
    for col, label in zip(h, [
        "Tracking ID", "Ship Method", "Order #", "Ship Date",
        "Last Event", "Failure", "Claim Status", "Prob", "✉"
    ]):
        col.markdown(
            f"<small><b style='color:#888;font-size:11px;'>{label}</b></small>",
            unsafe_allow_html=True
        )
    st.markdown(
        "<hr style='margin:3px 0 0 0;border:none;border-top:0.5px solid #ddd;'>",
        unsafe_allow_html=True
    )

    # Track which row has email modal open
    if "open_modal_tid" not in st.session_state:
        st.session_state["open_modal_tid"] = None

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
        narrative    = str(row.get("llm_narrative", "") or "")
        human_note   = str(row.get("human_comment", "") or "")
        short_label  = str(row.get("short_label", "") or "")

        # Last event: status text + date + source icon
        last_status = str(row.get("last_event_status", "") or "—")
        last_date, last_src = resolve_date(
            row.get("cache_last_event"),
            row.get("order_last_event")
        )
        last_event_html = (
            f"<div style='font-size:12px;'>{last_status}</div>"
            f"<div style='font-size:10px;color:#888;'>"
            f"{last_date} {SOURCE_ICON[last_src]}</div>"
        )

        cols = st.columns([1.6, 1.2, 0.9, 0.9, 1.8, 1, 1.3, 0.6, 0.5])

        with cols[0]:
            st.markdown(tracking_link_html(tid, carrier), unsafe_allow_html=True)

        cols[1].markdown(f"<span style='font-size:12px;'>{sm}</span>",       unsafe_allow_html=True)
        cols[2].markdown(f"<span style='font-size:12px;color:#888;'>{order_num}</span>", unsafe_allow_html=True)
        cols[3].markdown(f"<span style='font-size:12px;'>{ship_date}</span>", unsafe_allow_html=True)
        cols[4].markdown(last_event_html,                                     unsafe_allow_html=True)
        cols[5].markdown(failure_badge_html(failure_type),                    unsafe_allow_html=True)
        cols[6].markdown(status_badge_html(claim_status),                     unsafe_allow_html=True)
        cols[7].markdown(
            f"<span style='font-size:12px;'>{f'{float(prob):.0%}' if prob else '—'}</span>",
            unsafe_allow_html=True
        )

        with cols[8]:
            btn_label = f"✉ {email_count}" if email_count else "✉"
            if st.button(btn_label, key=f"email_btn_{tid}"):
                if st.session_state["open_modal_tid"] == tid:
                    st.session_state["open_modal_tid"] = None
                else:
                    st.session_state["open_modal_tid"] = tid
                st.rerun()

        st.markdown(
            "<hr style='margin:2px 0;border:none;border-top:0.5px solid #eee;'>",
            unsafe_allow_html=True
        )

        # Email modal — inline below the clicked row
        if st.session_state.get("open_modal_tid") == tid:
            show_email_modal(tid, carrier, short_label, narrative, human_note)


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
            f"🔵 {tid}  |  {row.get('reason', '')}  |  Prob: {prob:.0%}  |  {days}d left",
            expanded=True
        ):
            c1, c2 = st.columns(2)
            c1.markdown(f"**Ship Method:** {sm}")
            c1.markdown(f"**Failure Type:** {row.get('failure_type', '—')}")
            c1.markdown(f"**Attempt #:** {int(row.get('attempt_number', 0) or 0)}")
            c2.markdown(f"**Probability:** {prob:.0%}")
            c2.markdown(f"**Days Remaining:** {days}")
            c2.markdown(f"**Reason:** {row.get('reason', '—')}")
            c2.markdown(
                f'🔗 <a href="{url}" target="_blank">Open Carrier Tracking</a>',
                unsafe_allow_html=True
            )

            comment = st.text_area(
                "Human comment (included in next email draft):",
                key=f"hitl_comment_{row['queue_id']}"
            )

            b1, b2, b3 = st.columns(3)
            with b1:
                if st.button("✅ Approve & Send", key=f"hitl_approve_{row['queue_id']}"):
                    _hitl_action(int(row["queue_id"]), int(row["claim_id"]),
                                 tid, "approved", comment)
            with b2:
                if st.button("⏭ Skip", key=f"hitl_skip_{row['queue_id']}"):
                    st.toast("Skipped for now.")
            with b3:
                if st.button("❌ Close Claim", key=f"hitl_close_{row['queue_id']}"):
                    _hitl_action(int(row["queue_id"]), int(row["claim_id"]),
                                 tid, "closed", comment)


def _hitl_action(queue_id: int, claim_id: int, tracking_id: str,
                 action: str, comment: str):
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
    st.subheader("⚠️ Pipeline Errors & Alerts")

    df = q_errors()
    if df.empty:
        st.success("No errors recorded.")
        return

    unresolved = df[df["resolved"] == 0]
    resolved   = df[df["resolved"] == 1]

    st.metric("Unresolved Errors", len(unresolved))

    if not unresolved.empty:
        st.markdown("#### ❌ Unresolved")
        for _, row in unresolved.iterrows():
            tid = str(row.get("tracking_id", "") or "")
            with st.expander(
                f"🔴 {row['error_type']} | {row.get('stage','')} — {str(row['created_at'])[:16]}",
                expanded=True
            ):
                if tid:
                    url = tracking_url(tid)
                    st.markdown(
                        f'**Tracking ID:** <a href="{url}" target="_blank">{tid}</a>',
                        unsafe_allow_html=True
                    )
                st.markdown(f"**Details:** {row['details']}")

    if not resolved.empty:
        with st.expander(f"✅ Resolved ({len(resolved)})"):
            st.dataframe(
                resolved[["tracking_id", "error_type", "stage", "created_at"]],
                use_container_width=True
            )


# ─────────────────────────────────────────────────────────────────────────────
# TAB 4 — SETTINGS
# ─────────────────────────────────────────────────────────────────────────────

def render_settings():
    st.subheader("⚙️ System Configuration")
    st.caption("Visible to all. Login required to save.")

    cfg = load_config()

    st.markdown("#### 🎯 Probability Thresholds")
    c1, c2 = st.columns(2)
    auto_thresh  = c1.number_input("Auto-resubmit if prob ≥", 0.0, 1.0, step=0.05,
                                   value=float(cfg["probability"]["auto_resubmit_threshold"]))
    human_thresh = c2.number_input("Human review lower bound", 0.0, 1.0, step=0.05,
                                   value=float(cfg["probability"]["human_review_threshold"]))
    st.caption(
        f"prob ≥ {auto_thresh:.0%} → auto-resubmit  |  "
        f"{human_thresh:.0%}–{auto_thresh:.0%} → human review  |  "
        f"< {human_thresh:.0%} → stop & HITL"
    )

    st.markdown("#### 🔁 Retry")
    max_attempts = st.number_input("Max retry attempts", 1, 10,
                                   value=int(cfg["retry"]["max_attempts"]))

    st.markdown("#### 📅 Filing Windows")
    c1, c2, c3 = st.columns(3)
    ups_days   = c1.number_input("UPS (days)",   1, 60, int(cfg["filing_windows"]["ups_days"]))
    fedex_days = c2.number_input("FedEx (days)", 1, 60, int(cfg["filing_windows"]["fedex_days"]))
    auto_days  = c3.number_input("Auto-file if ≤ N days left", 1, 10,
                                  int(cfg["filing_windows"]["auto_file_days_remaining"]),
                                  help="Bypass HITL when window is almost closed.")

    st.markdown("#### 💰 Claim Amount")
    claim_amt = st.number_input("Fixed claim amount per shipment ($)", 1.0, 10000.0,
                                float(cfg["claim_amount"]), step=1.0)

    st.markdown("#### 📧 Email Configuration")
    email_mode = st.selectbox(
        "Email Mode",
        ["manual", "auto_generate", "auto_send"],
        index=["manual", "auto_generate", "auto_send"].index(cfg["email"].get("mode", "manual")),
        help="manual: Generate per claim, preview, send  |  "
             "auto_generate: LLM drafts all, user reviews  |  "
             "auto_send: fully automatic"
    )
    env = st.radio("Environment", ["test", "production"],
                   index=0 if cfg["email"].get("env", "test") == "test" else 1,
                   horizontal=True)
    if env == "test":
        st.info("📬 Emails go to test address, not to carriers.")
    else:
        st.warning("⚠️ Production — real emails sent to UPS (Shippo) and FedEx.")

    c1, c2 = st.columns(2)
    test_addr = c1.text_input("Test email address",    cfg["email"].get("test_address", ""))
    sender    = c2.text_input("Sender (claims Gmail)", cfg["email"].get("sender", ""))

    st.markdown("#### ⏰ Scheduler")
    c1, c2 = st.columns(2)
    days = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"]
    weekly_day    = c1.selectbox("Weekly full-run day", days,
                                 index=days.index(cfg["scheduler"].get("weekly_day", "Monday")))
    daily_enabled = c2.checkbox("Daily not-delivered recheck",
                                cfg["scheduler"].get("daily_enabled", True))

    st.divider()

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
        st.success("✅ Settings saved to system_config.json")
        st.cache_data.clear()


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

    # ── Compact Header ────────────────────────────────────────────────────────
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
                        ["python",
                         os.path.join(BASE_DIR, "scheduler", "scheduler.py"),
                         "--manual"],
                        capture_output=True, text=True, timeout=300
                    )
                    if res.returncode == 0:
                        st.success("Pipeline completed.")
                        st.cache_data.clear()
                    else:
                        st.error(f"Pipeline error:\n{res.stderr[:300]}")
                except Exception as e:
                    st.error(f"Failed: {e}")

    # Legend (toggle)
    if st.session_state.get("show_legend"):
        st.markdown("""
| Icon | Meaning |
|------|---------|
| 📡 | Date from MCP carrier tracking (most accurate) |
| 🗃️ | Date from Order API (fallback) |
| 🟢 | Claim approved / filed |
| 🟡 | Claim filed / in progress |
| 🔴 | Claim rejected |
| 🔵 | Awaiting human review (HITL) |
| ⚪ | Not yet filed |
| ⬛ | Not eligible / no claim |
        """)

    st.markdown(
        "<hr style='margin:6px 0 12px 0;border:none;border-top:0.5px solid #ddd;'>",
        unsafe_allow_html=True
    )

    # ── Tabs ─────────────────────────────────────────────────────────────────
    t1, t2, t3, t4 = st.tabs([
        "📊 Dashboard", "🧑‍💼 HITL Queue", "⚠️ Errors", "⚙️ Settings"
    ])

    with t1: render_dashboard()
    with t2: render_hitl()
    with t3: render_errors()
    with t4: render_settings()


if __name__ == "__main__":
    main()
