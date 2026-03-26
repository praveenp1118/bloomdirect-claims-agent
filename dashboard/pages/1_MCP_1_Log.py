"""
pages/1_MCP_1_Log.py
MCP 1 Log — Carrier Tracking API
Dedicated page for carrier tracking MCP activity.
Navigated to from L1 Dashboard MCP 1 Calls column.
"""

import json
import os
import sys

import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text

# ── Path setup ────────────────────────────────────────────────────
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

st.set_page_config(
    page_title="MCP 1 Log — BloomDirect",
    page_icon="📡",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.markdown("""
<style>
section[data-testid="stSidebar"] { min-width: 160px !important; max-width: 160px !important; }
section[data-testid="stSidebar"] a p { font-size: 13px !important; }
</style>
""", unsafe_allow_html=True)


# ── DB ────────────────────────────────────────────────────────────
@st.cache_resource
def get_engine():
    db_url = os.getenv("DATABASE_URL", "sqlite:////app/data/bloomdirect.db")
    return create_engine(db_url, connect_args={"check_same_thread": False})


# ── Helpers ───────────────────────────────────────────────────────
BADGE_BASE = "display:inline-block;font-size:10px;padding:2px 7px;border-radius:10px;font-weight:500;white-space:nowrap;"

SOURCE_BADGE = {
    "mcp":       ("🟢 Live API",  "background:#EAF3DE;color:#3B6D11;"),
    "cache":     ("🟡 Cache",     "background:#FAEEDA;color:#854F0B;"),
    "order_api": ("🗃️ Order API", "background:#F1EFE8;color:#5F5E5A;"),
}

def source_badge_html(source: str) -> str:
    label, sty = SOURCE_BADGE.get(source, ("❓ Unknown", "background:#F1EFE8;color:#5F5E5A;"))
    return f'<span style="{BADGE_BASE}{sty}">{label}</span>'

def delivery_badge_html(is_late: bool) -> str:
    if is_late:
        return f'<span style="{BADGE_BASE}background:#FCEBEB;color:#A32D2D;">Not On Time</span>'
    return f'<span style="{BADGE_BASE}background:#EAF3DE;color:#3B6D11;">On Time</span>'

def tracking_url(tracking_id: str, carrier: str = "") -> str:
    if tracking_id.upper().startswith("1Z") or "UPS" in carrier.upper():
        return f"https://www.ups.com/track?loc=en_in&tracknum={tracking_id}&requester=WT/trackdetails"
    return f"https://www.fedex.com/wtrk/track/?action=track&trackingnumber={tracking_id}&cntry_code=us&locale=en_US"

def tracking_link_html(tid: str, carrier: str = "") -> str:
    url   = tracking_url(tid, carrier)
    label = tid[:16] + "…" if len(tid) > 16 else tid
    return f'<a href="{url}" target="_blank" style="font-size:12px;">{label}</a>'


# ── Query ─────────────────────────────────────────────────────────
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
        LEFT JOIN orders   o ON o.tracking_id = tc.tracking_id
        LEFT JOIN claims   c ON c.tracking_id = tc.tracking_id
        LEFT JOIN failures f ON f.tracking_id = tc.tracking_id
        WHERE {" AND ".join(clauses)}
        ORDER BY tc.last_mcp_call DESC
    """)
    with get_engine().connect() as conn:
        return pd.read_sql(sql, conn, params=params)


# ── Session state ─────────────────────────────────────────────────
if "mcp1_verify_tid" not in st.session_state:
    st.session_state["mcp1_verify_tid"] = None
if "mcp1_events_tid" not in st.session_state:
    st.session_state["mcp1_events_tid"] = None
if "mcp1_page" not in st.session_state:
    st.session_state["mcp1_page"] = 1


# ── Header ────────────────────────────────────────────────────────
c1, c2 = st.columns([5, 1])
c1.markdown("## 📡 MCP 1 Log — Carrier Tracking API")
with c2:
    st.write("")
    if st.button("← Dashboard", key="back_dashboard"):
        st.switch_page("app.py")

st.markdown("<hr style='margin:6px 0 12px;border:none;border-top:0.5px solid #ddd;'>", unsafe_allow_html=True)

# ── Summary metrics ───────────────────────────────────────────────
all_df = q_mcp1()
if not all_df.empty:
    total      = len(all_df)
    live_calls = len(all_df[all_df["source"] == "mcp"])
    cached     = len(all_df[all_df["source"] == "cache"])
    not_on_time= len(all_df[all_df["is_late"] == 1])

    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Total Tracked", total)
    m2.metric("🟢 Live API Calls", live_calls)
    m3.metric("🟡 Cache Hits", cached)
    m4.metric("Not On Time", not_on_time)
    st.markdown("<hr style='margin:8px 0;border:none;border-top:0.5px solid #eee;'>", unsafe_allow_html=True)

# ── Filter bar ────────────────────────────────────────────────────
# Check if navigated from L1 with pre-filter
pre_sm = st.session_state.get("mcp1_filter_sm", "")

f1, f2, f3, f4 = st.columns([2, 1.5, 1.5, 1])
sm_filter  = f1.text_input("Ship Method", value=pre_sm or "", key="mcp1_sm_input",
                            placeholder="e.g. UPS Ground")
src_filter = f2.selectbox("Source", ["All", "Live API", "Cache", "Order API"])
del_filter = f3.selectbox("Delivery", ["All", "On Time", "Not On Time"])
with f4:
    st.write("")
    if st.button("✕ Clear", key="mcp1_clear"):
        st.session_state["mcp1_filter_sm"] = None
        st.session_state["mcp1_page"]      = 1
        st.rerun()

if pre_sm:
    st.info(f"🔍 Filtered to: **{pre_sm}** (from Dashboard)")

# ── Data ──────────────────────────────────────────────────────────
df = q_mcp1(sm_filter or None, src_filter, del_filter)

if df.empty:
    st.info("No records match this filter.")
    st.stop()

PAGE_SIZE   = 50
total       = len(df)
total_pages = max(1, (total + PAGE_SIZE - 1) // PAGE_SIZE)
page        = max(1, min(st.session_state["mcp1_page"], total_pages))

pc1, pc2, pc3 = st.columns([3, 1, 1])
pc1.caption(f"{total} records — page {page}/{total_pages}")
with pc2:
    if st.button("← Prev", key="mcp1_prev", disabled=page <= 1):
        st.session_state["mcp1_page"] = page - 1; st.rerun()
with pc3:
    if st.button("Next →", key="mcp1_next", disabled=page >= total_pages):
        st.session_state["mcp1_page"] = page + 1; st.rerun()

df_page = df.iloc[(page-1)*PAGE_SIZE : page*PAGE_SIZE]

# ── Headers ───────────────────────────────────────────────────────
h = st.columns([1.3, 1.5, 0.7, 1.1, 1.4, 0.9, 0.6, 0.7, 0.8, 0.7])
for col, label in zip(h, ["Last MCP Call","Tracking ID","Carrier","Ship Method",
                           "Cached Status","Source","Events","Prob","Delivery","🔄"]):
    col.markdown(f"<small><b style='color:#888;font-size:11px;'>{label}</b></small>",
                 unsafe_allow_html=True)
st.markdown("<hr style='margin:3px 0 0;border:none;border-top:0.5px solid #ddd;'>",
            unsafe_allow_html=True)

# ── Rows ──────────────────────────────────────────────────────────
for _idx, (_, row) in enumerate(df_page.iterrows()):
    tid     = str(row.get("tracking_id", ""))
    carrier = str(row.get("carrier", ""))
    sm      = str(row.get("ship_method", "") or "")
    status  = str(row.get("cached_status", "") or "—")
    source  = str(row.get("source", "") or "")
    ts      = str(row.get("last_mcp_call", "") or "—")[:16]
    prob    = row.get("probability")
    is_late = bool(row.get("is_late", 0))

    # Parse history
    cached_history = []
    event_count    = 0
    hj = row.get("full_history_json", "")
    if hj:
        try:
            cached_history = json.loads(hj)
            event_count    = len(cached_history)
            cached_history = sorted(cached_history, key=lambda x: str(x.get("date", "")))
        except Exception:
            pass

    cols = st.columns([1.3, 1.5, 0.7, 1.1, 1.4, 0.9, 0.6, 0.7, 0.8, 0.7])
    cols[0].markdown(f"<span style='font-size:11px;color:#888;'>{ts}</span>", unsafe_allow_html=True)
    cols[1].markdown(tracking_link_html(tid, carrier), unsafe_allow_html=True)
    cols[2].markdown(f"<span style='font-size:12px;'>{carrier}</span>", unsafe_allow_html=True)
    cols[3].markdown(f"<span style='font-size:11px;'>{sm}</span>", unsafe_allow_html=True)
    cols[4].markdown(f"<span style='font-size:11px;'>{status[:35]}</span>", unsafe_allow_html=True)
    cols[5].markdown(source_badge_html(source), unsafe_allow_html=True)

    # Events — clickable
    with cols[6]:
        if st.button(str(event_count) if event_count else "—", key=f"ev_{tid}_{_idx}",
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

    with cols[9]:
        if st.button("🔄", key=f"verify_{tid}_{_idx}", help="Call live carrier API"):
            cur = st.session_state.get("mcp1_verify_tid")
            st.session_state["mcp1_verify_tid"] = None if cur == tid else tid
            st.session_state["mcp1_events_tid"] = None
            st.rerun()

    st.markdown("<hr style='margin:2px 0;border:none;border-top:0.5px solid #eee;'>",
                unsafe_allow_html=True)

    # Events popup
    if st.session_state.get("mcp1_events_tid") == tid:
        with st.expander(f"📋 Tracking History — {tid} ({event_count} events)", expanded=True):
            st.markdown(f"**Carrier:** {carrier} &nbsp;|&nbsp; **Status:** {status} &nbsp;|&nbsp; **Updated:** {ts}")
            st.divider()
            if cached_history:
                for event in cached_history:
                    ev_date   = str(event.get("date", ""))[:16]
                    ev_status = str(event.get("status", ""))
                    ev_loc    = str(event.get("location", "") or "")
                    loc_str   = f" &nbsp;📍 {ev_loc}" if ev_loc else ""
                    st.markdown(
                        f"<div style='font-size:12px;padding:5px 0;border-bottom:0.5px solid #f0f0f0;'>"
                        f"<span style='color:#888;font-size:11px;'>{ev_date}</span>{loc_str}<br>"
                        f"<span style='font-weight:500;'>{ev_status}</span></div>",
                        unsafe_allow_html=True
                    )
            else:
                st.info("No cached history.")
            if st.button("Close", key=f"close_ev_{tid}_{_idx}"):
                st.session_state["mcp1_events_tid"] = None; st.rerun()

    # Verify popup — live API
    if st.session_state.get("mcp1_verify_tid") == tid:
        with st.expander(f"🔄 Live API — {tid}", expanded=True):
            with st.spinner(f"Calling {carrier} API…"):
                try:
                    from mcp_servers.carrier_tracking_mcp import fetch_ups_history, fetch_fedex_history
                    if "UPS" in carrier.upper():
                        result = fetch_ups_history(tid)
                    else:
                        result = fetch_fedex_history(tid)
                    if result:
                        live_history = sorted(result.get("history", []),
                                              key=lambda x: str(x.get("date", "")))
                        st.markdown(f"**Live status:** {result.get('status','')} &nbsp;|&nbsp; **{len(live_history)} events**")
                        st.divider()
                        for event in live_history:
                            ev_date   = str(event.get("date", ""))[:16]
                            ev_status = str(event.get("status", ""))
                            ev_loc    = str(event.get("location", "") or "")
                            loc_str   = f" &nbsp;📍 {ev_loc}" if ev_loc else ""
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
            if st.button("Close", key=f"close_verify_{tid}_{_idx}"):
                st.session_state["mcp1_verify_tid"] = None; st.rerun()
