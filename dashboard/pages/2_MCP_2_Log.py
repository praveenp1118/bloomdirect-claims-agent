"""
pages/2_MCP_2_Log.py
MCP 2 Log — Email Claims MCP
Enhanced email claims activity log with summary, urgency indicators, full body view.
"""

import os
import sys
from datetime import datetime, timedelta

import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text
from email.header import decode_header as email_decode_header

def decode_subject(raw_subject: str) -> str:
    try:
        if not raw_subject or '=?' not in raw_subject:
            return raw_subject
        decoded_parts = email_decode_header(raw_subject)
        result = []
        for part, encoding in decoded_parts:
            if isinstance(part, bytes):
                result.append(part.decode(encoding or 'utf-8', errors='replace'))
            else:
                result.append(str(part))
        return ' '.join(result)
    except Exception:
        return raw_subject

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

st.set_page_config(
    page_title="MCP 2 Log — BloomDirect",
    page_icon="📧",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.markdown("""
<style>
section[data-testid="stSidebar"] { min-width: 160px !important; max-width: 160px !important; }
section[data-testid="stSidebar"] a p { font-size: 13px !important; }
</style>
""", unsafe_allow_html=True)


@st.cache_resource
def get_engine():
    db_url = os.getenv("DATABASE_URL", "sqlite:////app/data/bloomdirect.db")
    return create_engine(db_url, connect_args={"check_same_thread": False})

BADGE_BASE = "display:inline-block;font-size:10px;padding:2px 7px;border-radius:10px;font-weight:500;white-space:nowrap;"

def tracking_url(tracking_id: str, carrier: str = "") -> str:
    if tracking_id.upper().startswith("1Z") or "UPS" in carrier.upper():
        return f"https://www.ups.com/track?loc=en_in&tracknum={tracking_id}&requester=WT/trackdetails"
    return f"https://www.fedex.com/wtrk/track/?action=track&trackingnumber={tracking_id}&cntry_code=us&locale=en_US"

def tracking_link_html(tid: str, carrier: str = "") -> str:
    url   = tracking_url(tid, carrier)
    label = tid[:16] + "…" if len(tid) > 16 else tid
    return f'<a href="{url}" target="_blank" style="font-size:12px;">{label}</a>'

def days_since(ts_str: str) -> int:
    try:
        ts = datetime.strptime(str(ts_str)[:19], "%Y-%m-%d %H:%M:%S")
        return (datetime.utcnow() - ts).days
    except Exception:
        return 0

def urgency_badge(days: int, direction: str) -> str:
    if direction not in ("outbound", "sent"):
        return ""
    if days >= 14:
        return f'<span style="{BADGE_BASE}background:#FCEBEB;color:#A32D2D;">🔴 {days}d — Overdue</span>'
    elif days >= 7:
        return f'<span style="{BADGE_BASE}background:#FAEEDA;color:#854F0B;">🟡 {days}d — Follow up</span>'
    return f'<span style="{BADGE_BASE}background:#EAF3DE;color:#3B6D11;">🟢 {days}d</span>'

def load_config():
    import json
    config_path = os.path.join(BASE_DIR, "config", "system_config.json")
    if os.path.exists(config_path):
        with open(config_path) as f:
            return json.load(f)
    return {}

@st.cache_data(ttl=300)
def q_mcp2(ship_method=None, direction_filter=None, status_filter=None, tracking_id_search=None) -> pd.DataFrame:
    clauses = ["1=1"]
    params  = {}
    if tracking_id_search:
        clauses.append("el.tracking_id LIKE :tid")
        params["tid"] = f"%{tracking_id_search}%"
    if ship_method:
        clauses.append("o.ship_method = :sm")
        params["sm"] = ship_method
    if direction_filter and direction_filter != "All":
        clauses.append("el.direction = :dir")
        params["dir"] = "sent" if direction_filter == "Sent" else "received"
    if status_filter and status_filter != "All":
        clauses.append("el.status = :st")
        params["st"] = status_filter.lower()

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


# ── Session state ─────────────────────────────────────────────────
if "mcp2_expand_tid" not in st.session_state:
    st.session_state["mcp2_expand_tid"] = None
if "mcp2_page" not in st.session_state:
    st.session_state["mcp2_page"] = 1

# ── Header ────────────────────────────────────────────────────────
c1, c2 = st.columns([5, 1])
c1.markdown("## 📧 MCP 2 Log — Email Claims")
with c2:
    st.write("")
    if st.button("← Dashboard"):
        st.switch_page("app.py")

st.markdown("<hr style='margin:6px 0 12px;border:none;border-top:0.5px solid #ddd;'>",
            unsafe_allow_html=True)

# ── Config ────────────────────────────────────────────────────────
cfg       = load_config()
env       = cfg.get("email", {}).get("env", "test")
email_mode= cfg.get("email", {}).get("mode", "manual")

mode_labels = {"manual": "🖐 Manual", "auto_generate": "🤖 Auto Generate", "auto_send": "⚡ Auto Send"}
env_label   = "🟠 TEST MODE" if env == "test" else "🟢 PRODUCTION"

st.markdown(
    f"<div style='background:#f8f9fa;border-radius:6px;padding:8px 14px;margin-bottom:10px;font-size:12px;'>"
    f"<b>Mode:</b> {mode_labels.get(email_mode, email_mode)} &nbsp;&nbsp; "
    f"<b>Environment:</b> {env_label}</div>",
    unsafe_allow_html=True
)

# ── Summary metrics ───────────────────────────────────────────────
all_df = q_mcp2()
if not all_df.empty:
    sent     = all_df[all_df["direction"].isin(["outbound","sent"])]
    received = all_df[all_df["direction"].isin(["inbound","received"])]
    failed   = all_df[all_df["status"].str.lower().isin(["failed","error"]) if "status" in all_df.columns else [False]*len(all_df)]

    # Pending reply = sent with no corresponding inbound
    sent_tids     = set(sent["tracking_id"].unique())
    received_tids = set(received["tracking_id"].unique())
    pending       = len(sent_tids - received_tids)

    # Overdue = sent >14 days ago with no reply
    overdue = 0
    for _, r in sent.iterrows():
        if r["tracking_id"] not in received_tids:
            if days_since(str(r.get("timestamp", ""))) >= 14:
                overdue += 1

    m1, m2, m3, m4, m5 = st.columns(5)
    m1.metric("📤 Total Sent",     len(sent))
    m2.metric("📬 Replies",        len(received))
    m3.metric("⏳ Pending Reply",  pending)
    m4.metric("❌ Failed",         len(failed))
    m5.metric("🔴 Overdue (>14d)", overdue)
    st.markdown("<hr style='margin:8px 0;border:none;border-top:0.5px solid #eee;'>",
                unsafe_allow_html=True)

# ── Filters ───────────────────────────────────────────────────────
pre_sm = st.session_state.get("mcp2_filter_sm", "")

f1, f2, f3, f4, f5 = st.columns([1.5, 1.5, 1, 1, 0.8])
tid_search = f1.text_input("🔍 Tracking ID", key="mcp2_tid_search",
                            placeholder="Enter tracking ID...")
sm_filter  = f2.text_input("Ship Method", value=pre_sm or "", key="mcp2_sm_input",
                            placeholder="e.g. FEDEX_GROUND")
dir_filter = f3.selectbox("Direction", ["All", "Sent", "Received"])
sts_filter = f4.selectbox("Status", ["All", "filed", "rejected", "approved", "sent", "failed", "pending"])
with f5:
    st.write("")
    if st.button("✕ Clear", key="mcp2_clear"):
        st.session_state["mcp2_filter_sm"] = None
        st.session_state["mcp2_page"]      = 1
        st.rerun()

if pre_sm:
    st.info(f"🔍 Filtered to: **{pre_sm}** (from Dashboard)")

# ── Data ──────────────────────────────────────────────────────────
df = q_mcp2(sm_filter or None, dir_filter, sts_filter, tid_search or None)

if df.empty:
    st.info("No email records match this filter.")
    st.stop()

PAGE_SIZE   = 50
total       = len(df)
total_pages = max(1, (total + PAGE_SIZE - 1) // PAGE_SIZE)
page        = max(1, min(st.session_state["mcp2_page"], total_pages))

pc1, pc2, pc3 = st.columns([3, 1, 1])
pc1.caption(f"{total} records — page {page}/{total_pages}")
with pc2:
    if st.button("← Prev", disabled=page <= 1, key="mcp2_prev"):
        st.session_state["mcp2_page"] = page - 1; st.rerun()
with pc3:
    if st.button("Next →", disabled=page >= total_pages, key="mcp2_next"):
        st.session_state["mcp2_page"] = page + 1; st.rerun()

df_page = df.iloc[(page-1)*PAGE_SIZE : page*PAGE_SIZE]

# ── Headers ───────────────────────────────────────────────────────
h = st.columns([1.2, 1.5, 0.7, 1.1, 0.7, 1.8, 0.9, 1.0])
for col, label in zip(h, ["Timestamp","Tracking ID","Carrier","Ship Method",
                           "Direction","Subject","Status","Urgency"]):
    col.markdown(f"<small><b style='color:#888;font-size:11px;'>{label}</b></small>",
                 unsafe_allow_html=True)
st.markdown("<hr style='margin:3px 0 0;border:none;border-top:0.5px solid #ddd;'>",
            unsafe_allow_html=True)

# ── Rows ──────────────────────────────────────────────────────────
for _, row in df_page.iterrows():
    tid       = str(row.get("tracking_id", ""))
    carrier   = str(row.get("carrier", ""))
    sm        = str(row.get("ship_method", "") or "")
    direction = str(row.get("direction", ""))
    ts        = str(row.get("timestamp", ""))[:16]
    subject   = decode_subject(str(row.get("subject", "") or "—"))
    body      = str(row.get("body", "") or "")
    status    = str(row.get("status", "") or "—")
    rejection = str(row.get("rejection_reason", "") or "")
    recovered = row.get("recovered_amount")
    days      = days_since(str(row.get("timestamp", "")))

    dir_html = (
        '<span style="font-size:11px;color:#185FA5;font-weight:600;">↑ Sent</span>'
        if direction in ("outbound", "sent") else
        '<span style="font-size:11px;color:#3B6D11;font-weight:600;">↓ Reply</span>'
    )

    status_colors = {
        "sent":     "background:#EAF3DE;color:#3B6D11;",
        "filed":    "background:#EAF3DE;color:#3B6D11;",
        "approved": "background:#D4EDDA;color:#155724;",
        "rejected": "background:#FCEBEB;color:#A32D2D;",
        "failed":   "background:#FCEBEB;color:#A32D2D;",
        "pending":  "background:#EEEDFE;color:#534AB7;",
    }
    sty = status_colors.get(status.lower(), "background:#F1EFE8;color:#5F5E5A;")
    status_html = f'<span style="{BADGE_BASE}{sty}">{status.title()}</span>'

    cols = st.columns([1.2, 1.5, 0.7, 1.1, 0.7, 1.8, 0.9, 1.0])
    cols[0].markdown(f"<span style='font-size:11px;color:#888;'>{ts}</span>", unsafe_allow_html=True)
    cols[1].markdown(tracking_link_html(tid, carrier), unsafe_allow_html=True)
    cols[2].markdown(f"<span style='font-size:12px;'>{carrier}</span>", unsafe_allow_html=True)
    cols[3].markdown(f"<span style='font-size:11px;'>{sm}</span>", unsafe_allow_html=True)
    cols[4].markdown(dir_html, unsafe_allow_html=True)

    # Subject — clickable to expand body
    with cols[5]:
        subj_display = (subject[:35] + "…") if len(subject) > 35 else subject
        if st.button(subj_display, key=f"expand_{row['log_id']}", help="Click to view full email"):
            cur = st.session_state.get("mcp2_expand_tid")
            st.session_state["mcp2_expand_tid"] = None if cur == str(row['log_id']) else str(row['log_id'])
            st.rerun()

    cols[6].markdown(status_html, unsafe_allow_html=True)
    cols[7].markdown(urgency_badge(days, direction), unsafe_allow_html=True)

    st.markdown("<hr style='margin:2px 0;border:none;border-top:0.5px solid #eee;'>",
                unsafe_allow_html=True)

    # Expanded email body
    if st.session_state.get("mcp2_expand_tid") == str(row['log_id']):
        with st.expander(f"📧 {subject}", expanded=True):
            url = tracking_url(tid, carrier)
            st.markdown(f'🔗 <a href="{url}" target="_blank">Open carrier tracking</a>',
                        unsafe_allow_html=True)
            st.divider()

            c1, c2 = st.columns(2)
            c1.markdown(f"**Direction:** {'↑ Sent' if direction in ('outbound','sent') else '↓ Received'}")
            c1.markdown(f"**Timestamp:** {ts}")
            c1.markdown(f"**Days since:** {days}")
            c2.markdown(f"**Carrier:** {carrier}")
            c2.markdown(f"**Ship Method:** {sm}")
            c2.markdown(f"**Status:** {status}")

            if rejection:
                st.markdown(f"**❌ Rejection reason:** {rejection}")
            if recovered:
                st.markdown(f"**✅ Recovered:** ${float(recovered):.2f}")

            st.divider()
            st.markdown("**Email Body:**")
            st.markdown(
                f"<div style='background:#f8f9fa;border-radius:6px;padding:12px 16px;"
                f"font-size:13px;line-height:1.7;white-space:pre-wrap;'>{body}</div>",
                unsafe_allow_html=True
            )

            if st.button("Close", key=f"close_expand_{row['log_id']}"):
                st.session_state["mcp2_expand_tid"] = None; st.rerun()
