"""
scheduler.py - APScheduler for BloomDirect Claims Recovery System

Scheduler modes:
    Daily (midnight PST): Fetch 10 days of orders sequentially (today-3 to today-12)
                          30 second sleep between each date
                          MCP called only for non-delivered shipments
    Hourly:               Poll Gmail for carrier responses on filed claims
    Manual:               User-triggered from dashboard for any date range
"""

import json
import os
import time
import pandas as pd
from datetime import datetime, date, timedelta
from pathlib import Path
from typing import Optional
from dotenv import load_dotenv
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger

from database.models import (
    init_db, get_session, Order, Claim, Failure,
    SchedulerState, ErrorLog, TrackingCache
)

load_dotenv()

CONFIG   = json.loads(Path("config/system_config.json").read_text())
RUN_MODE = os.getenv("RUN_MODE", "synthetic")

ORDER_API_BASE = os.getenv("ORDER_API_BASE_URL", "")
ORDER_API_KEY  = os.getenv("ORDER_API_KEY", "")

DAY_OFFSETS   = list(range(3, 13))  # today-3 through today-12
SLEEP_BETWEEN = 30                   # seconds between date fetches
FILING_WINDOW = CONFIG.get("filing_window_days", 15)


# ── LOGGING HELPERS ───────────────────────────────────────────────

def log_error(error_type: str, stage: str, details: str,
              tracking_id: str = "") -> None:
    session = get_session()
    try:
        session.add(ErrorLog(
            tracking_id=tracking_id, error_type=error_type,
            stage=stage, details=details,
        ))
        session.commit()
    finally:
        session.close()


def log_scheduler_run(run_type: str, start_date: str, end_date: str,
                       status: str, records: int = 0) -> None:
    session = get_session()
    try:
        session.add(SchedulerState(
            run_type=run_type, start_date=start_date, end_date=end_date,
            status=status, records_processed=records,
            completed_at=datetime.now() if status == "completed" else None,
        ))
        session.commit()
    finally:
        session.close()


# ── CARRIER + OCCASION HELPERS ────────────────────────────────────

def detect_carrier(track_id: str, ship_method: str = "") -> str:
    if "UPS" in ship_method.upper() or str(track_id).upper().startswith("1Z"):
        return "UPS"
    return "FedEx"


def infer_occasion(gift_message: str) -> str:
    if not gift_message:
        return "General"
    msg = gift_message.lower()
    if any(w in msg for w in ["birthday", "cumpleaños", "bday"]):
        return "Birthday"
    if any(w in msg for w in ["funeral", "loss", "passed", "memorial",
                                "sorry for your loss", "difficult time",
                                "thinking of you", "remember"]):
        return "Funeral"
    if any(w in msg for w in ["valentine", "amor"]):
        return "Valentine"
    if "anniversary" in msg:
        return "Anniversary"
    if any(w in msg for w in ["graduation", "congratulations", "proud"]):
        return "Graduation"
    return "General"


# ── ORDER API FETCH ───────────────────────────────────────────────

def fetch_orders_for_date(fetch_date: str) -> Optional[list]:
    """Fetch orders from Order API for a single date. Returns list or None."""
    if RUN_MODE == "synthetic" or not ORDER_API_BASE:
        return load_synthetic_data(fetch_date)

    try:
        import httpx
        print(f"[Scheduler] Fetching API: {fetch_date}...")
        response = httpx.get(
            ORDER_API_BASE,
            headers={"Authorization": f"Bearer {ORDER_API_KEY}",
                     "Content-Type": "application/json"},
            params={"from_date": fetch_date, "to_date": fetch_date,
                    "ship_type": "ship_date"},
            timeout=60,
        )
        if response.status_code == 200:
            data   = response.json()
            orders = data if isinstance(data, list) else data.get("data", [])
            print(f"[Scheduler] {fetch_date}: {len(orders)} orders")
            return orders
        else:
            print(f"[Scheduler] API error {response.status_code} for {fetch_date}")
            log_error("API_ERROR", "fetch_orders", f"Status {response.status_code}")
            return None
    except Exception as e:
        print(f"[Scheduler] API error {fetch_date}: {e}")
        log_error("API_DOWN", "fetch_orders", str(e))
        return None


def load_synthetic_data(fetch_date: str = None) -> list:
    csv_path = Path("data/sample_shipments.csv")
    if not csv_path.exists():
        print("[Scheduler] No synthetic data. Run: python data/generate_synthetic_data.py")
        return []
    df = pd.read_csv(csv_path).fillna("")
    if fetch_date and "ship_date" in df.columns:
        filtered = df[df["ship_date"] == fetch_date]
        return filtered.to_dict("records") if len(filtered) > 0 else df.head(10).to_dict("records")
    return df.to_dict("records")


# ── DB OVERWRITE ──────────────────────────────────────────────────

def overwrite_orders_for_date(orders: list, fetch_date: str) -> int:
    """Delete existing orders for date and insert fresh. Never touches tracking_cache."""
    session = get_session()
    try:
        existing = session.query(Order).filter(Order.ship_date == fetch_date).all()
        for o in existing:
            session.delete(o)
        session.flush()

        inserted = 0
        for o in orders:
            session.add(Order(
                partner_order_id = str(o.get("partner_order_id", "")),
                tracking_id      = str(o.get("track_id", "")),
                ship_method      = str(o.get("ship_method", "")),
                ship_date        = str(o.get("ship_date", fetch_date)),
                carrier          = detect_carrier(str(o.get("track_id", "")),
                                                  str(o.get("ship_method", ""))),
                occasion_type    = infer_occasion(str(o.get("gift_message", ""))),
            ))
            inserted += 1

        session.commit()
        print(f"[Scheduler] {fetch_date}: {inserted} orders stored")
        return inserted
    except Exception as e:
        session.rollback()
        log_error("DB_ERROR", "overwrite_orders", str(e))
        return 0
    finally:
        session.close()


# ── MCP REFRESH ───────────────────────────────────────────────────

def refresh_mcp_for_date(fetch_date: str, orders: list) -> int:
    """
    For each order:
    - cache shows delivered → skip forever
    - order_api says delivered → update cache, skip MCP
    - not delivered → call MCP sequentially, update cache
    Returns MCP call count.
    """
    from mcp_servers.carrier_tracking_mcp import get_full_history

    session  = get_session()
    mcp_calls= 0

    try:
        for order in orders:
            track_id    = str(order.get("track_id", ""))
            last_status = str(order.get("last_track_status", "")).lower()
            ship_method = str(order.get("ship_method", ""))

            if not track_id:
                continue

            cache = session.query(TrackingCache).filter(
                TrackingCache.tracking_id == track_id
            ).first()

            # Cache says delivered → skip forever
            if cache and cache.cached_status and \
               "delivered" in cache.cached_status.lower():
                continue

            # Order API says delivered → update cache, no MCP needed
            if "delivered" in last_status:
                if cache:
                    cache.cached_status      = order.get("last_track_status", "Delivered")
                    cache.cached_status_date = order.get("last_track_status_date", "")
                    cache.source             = "order_api"
                else:
                    session.add(TrackingCache(
                        tracking_id        = track_id,
                        carrier            = detect_carrier(track_id, ship_method),
                        cached_status      = order.get("last_track_status", "Delivered"),
                        cached_status_date = order.get("last_track_status_date", ""),
                        source             = "order_api",
                    ))
                session.commit()
                continue

            # Not delivered → call MCP
            try:
                print(f"[Scheduler] MCP: {track_id}")
                result     = get_full_history(track_id, ship_method=ship_method)
                status     = result.get("status", "")
                status_date= result.get("status_date", "")
                history    = result.get("history", [])
                mcp_calls += 1

                if cache:
                    cache.cached_status      = status
                    cache.cached_status_date = status_date
                    cache.full_history_json  = json.dumps(history)
                    cache.last_mcp_call      = datetime.now()
                    cache.source             = "mcp"
                else:
                    session.add(TrackingCache(
                        tracking_id        = track_id,
                        carrier            = detect_carrier(track_id, ship_method),
                        cached_status      = status,
                        cached_status_date = status_date,
                        full_history_json  = json.dumps(history),
                        last_mcp_call      = datetime.now(),
                        source             = "mcp",
                    ))
                session.commit()

            except Exception as e:
                print(f"[Scheduler] MCP error {track_id}: {e}")
                log_error("MCP_TIMEOUT", "refresh_mcp", str(e), track_id)

        return mcp_calls
    finally:
        session.close()


# ── PROCESS ELIGIBLE ──────────────────────────────────────────────

def process_eligible_for_date(orders: list) -> dict:
    from guardrails.input_validator import validate_batch
    from orchestrator.pipeline import run_batch

    valid_orders, skipped = validate_batch(orders)
    if not valid_orders:
        return {"filed": 0, "skipped": len(skipped), "hitl": 0}

    results = run_batch(valid_orders)
    return {
        "filed":   sum(1 for r in results if r.get("filed")),
        "skipped": len(skipped),
        "hitl":    sum(1 for r in results if r.get("needs_hitl") and not r.get("filed")),
    }


# ── MAIN DAILY JOB ────────────────────────────────────────────────

def run_daily_pipeline():
    """
    Daily — midnight PST.
    Fetches today-3 through today-12 sequentially.
    30s sleep between dates. MCP only for non-delivered.
    """
    today = date.today()
    print(f"\n[Daily] Starting {datetime.now().strftime('%Y-%m-%d %H:%M')} — {len(DAY_OFFSETS)} dates")

    total_orders = 0
    total_filed  = 0
    total_mcp    = 0

    for i, offset in enumerate(DAY_OFFSETS):
        fetch_date = (today - timedelta(days=offset)).strftime("%Y-%m-%d")
        print(f"\n[Daily] [{i+1}/{len(DAY_OFFSETS)}] {fetch_date}")

        orders = fetch_orders_for_date(fetch_date)
        if orders is None:
            print(f"[Daily] {fetch_date}: API failed — skipping")
        else:
            overwrite_orders_for_date(orders, fetch_date)
            mcp_calls    = refresh_mcp_for_date(fetch_date, orders)
            result       = process_eligible_for_date(orders)
            total_orders += len(orders)
            total_mcp    += mcp_calls
            total_filed  += result.get("filed", 0)
            print(f"[Daily] {fetch_date}: {len(orders)} orders, {mcp_calls} MCP, {result['filed']} filed")

        if i < len(DAY_OFFSETS) - 1:
            print(f"[Daily] Sleeping 30s...")
            time.sleep(SLEEP_BETWEEN)

    print(f"\n[Daily] Done — Orders: {total_orders} | MCP: {total_mcp} | Filed: {total_filed}")
    log_scheduler_run(
        "daily",
        (today - timedelta(days=DAY_OFFSETS[-1])).strftime("%Y-%m-%d"),
        (today - timedelta(days=DAY_OFFSETS[0])).strftime("%Y-%m-%d"),
        "completed", total_orders,
    )


# ── HOURLY POLL ───────────────────────────────────────────────────

def run_hourly_response_poll():
    print(f"\n[Hourly] Polling Gmail...")
    session = get_session()
    try:
        filed_claims = session.query(Claim).filter(
            Claim.filed == True,
            Claim.status.in_(["filed", "resubmitted"]),
        ).all()

        if not filed_claims:
            print("[Hourly] No filed claims")
            return

        from mcp_servers.email_claims_mcp import check_email_response, send_claim_email
        from agents.followup_escalation import process_rejection, draft_resubmission, draft_followup
        from database.models import Recovery, HitlQueue

        for claim in filed_claims:
            if not claim.gmail_thread_id:
                continue
            try:
                response       = check_email_response(claim.gmail_thread_id, claim.claim_id)
                classification = response.get("classification") if response.get("has_response") else None

                if not response.get("has_response"):
                    failure = session.query(Failure).filter(
                        Failure.failure_id == claim.failure_id
                    ).first()
                    if failure and failure.ship_date:
                        ship_d        = datetime.strptime(failure.ship_date, "%Y-%m-%d").date()
                        days_elapsed  = (date.today() - ship_d).days
                        days_remaining= FILING_WINDOW - days_elapsed
                        if days_elapsed >= CONFIG.get("followup_day", 14) and claim.attempt_number == 1:
                            followup = draft_followup(
                                {"claim_id": claim.claim_id, "tracking_id": claim.tracking_id,
                                 "carrier": claim.carrier, "claim_type": claim.claim_type,
                                 "filed_at": str(claim.filed_at)},
                                days_remaining
                            )
                            send_claim_email(
                                to="", subject=followup["subject"], body=followup["body"],
                                claim_id=claim.claim_id, carrier=claim.carrier,
                                tracking_id=claim.tracking_id,
                            )
                    continue

                if classification == "APPROVED":
                    claim.status = "approved"
                    session.add(Recovery(
                        claim_id=claim.claim_id,
                        recovered_amount=claim.claim_amount,
                        credit_date=date.today().strftime("%Y-%m-%d"),
                    ))

                elif classification == "REJECTED":
                    rejection_reason = response.get("rejection_reason", "")
                    decision = process_rejection(claim.claim_id, rejection_reason)
                    if decision["action"] == "resubmit":
                        failure = session.query(Failure).filter(
                            Failure.failure_id == claim.failure_id
                        ).first()
                        resubmit = draft_resubmission({
                            "claim": {"claim_id": claim.claim_id, "tracking_id": claim.tracking_id,
                                      "carrier": claim.carrier, "claim_type": claim.claim_type,
                                      "first_bad_event": failure.first_bad_event if failure else "",
                                      "attempt_number": claim.attempt_number},
                            "original_email_body": claim.draft_email_text or "",
                            "rejection_reason": rejection_reason,
                            "occasion_type": claim.occasion_type,
                            "attempt_number": claim.attempt_number,
                            "prior_claim_ids": [claim.claim_id],
                        })
                        send_claim_email(
                            to="", subject=resubmit["subject"], body=resubmit["body"],
                            claim_id=claim.claim_id, carrier=claim.carrier,
                            tracking_id=claim.tracking_id,
                        )

                elif classification == "MORE_INFO":
                    claim.status = "more_info_needed"
                    session.add(HitlQueue(
                        claim_id=claim.claim_id, tracking_id=claim.tracking_id,
                        reason="Carrier requested additional information", status="pending",
                    ))

                claim.updated_at = datetime.now()
                session.commit()

            except Exception as e:
                print(f"[Hourly] Error {claim.tracking_id}: {e}")
                log_error("RESPONSE_ERROR", "hourly_poll", str(e), claim.tracking_id)

        print("[Hourly] Poll complete")
    finally:
        session.close()


# ── MANUAL TRIGGER ────────────────────────────────────────────────

def run_manual(start_date: str, end_date: str) -> dict:
    """Manual trigger from dashboard for a specific date range."""
    print(f"\n[Manual] {start_date} to {end_date}")
    log_scheduler_run("manual", start_date, end_date, "running")

    try:
        from datetime import datetime as dt
        start      = dt.strptime(start_date, "%Y-%m-%d").date()
        end        = dt.strptime(end_date,   "%Y-%m-%d").date()
        all_orders = []
        current    = start

        while current <= end:
            date_str = current.strftime("%Y-%m-%d")
            orders   = fetch_orders_for_date(date_str)
            if orders:
                overwrite_orders_for_date(orders, date_str)
                refresh_mcp_for_date(date_str, orders)
                all_orders.extend(orders)
            current += timedelta(days=1)

        if not all_orders:
            log_scheduler_run("manual", start_date, end_date, "completed", 0)
            return {"status": "no_orders", "total": 0, "filed": 0, "skipped": 0, "hitl": 0}

        result = process_eligible_for_date(all_orders)
        log_scheduler_run("manual", start_date, end_date, "completed", len(all_orders))

        return {
            "status":  "complete",
            "total":   len(all_orders),
            "filed":   result.get("filed", 0),
            "skipped": result.get("skipped", 0),
            "hitl":    result.get("hitl", 0),
        }

    except Exception as e:
        log_scheduler_run("manual", start_date, end_date, "error")
        return {"status": "error", "message": str(e)}


# ── SCHEDULER SETUP ───────────────────────────────────────────────

def create_scheduler() -> BackgroundScheduler:
    scheduler = BackgroundScheduler()

    scheduler.add_job(
        run_daily_pipeline,
        CronTrigger(hour=0, minute=0, timezone="America/Los_Angeles"),
        id="daily_pipeline", name="Daily Pipeline (midnight PST)",
        replace_existing=True,
    )
    scheduler.add_job(
        run_hourly_response_poll,
        IntervalTrigger(hours=1),
        id="hourly_response_poll", name="Hourly Response Poll",
        replace_existing=True,
    )
    return scheduler


# ── ENTRYPOINT ────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys
    init_db()

    args = sys.argv[1:]

    # ── --manual [start_date] [end_date] ──────────────────────────
    # Called by dashboard "Run Pipeline" button.
    # Defaults to last 14 days (within 15-day filing window).
    if "--manual" in args:
        today     = date.today()
        idx       = args.index("--manual")
        try:
            start_date = args[idx + 1]
        except IndexError:
            start_date = (today - timedelta(days=14)).strftime("%Y-%m-%d")
        try:
            end_date = args[idx + 2]
        except IndexError:
            end_date = today.strftime("%Y-%m-%d")

        print(f"\n[Manual] Triggered from dashboard: {start_date} → {end_date}")
        result = run_manual(start_date, end_date)
        print(f"[Manual] Result: {result}")
        sys.exit(0 if result.get("status") in ("complete", "no_orders") else 1)

    # ── --daily ───────────────────────────────────────────────────
    # Force a single daily pipeline run immediately.
    elif "--daily" in args:
        print("\n[CLI] Forcing daily pipeline run...")
        run_daily_pipeline()
        sys.exit(0)

    # ── --hourly ──────────────────────────────────────────────────
    # Force a single hourly Gmail poll immediately.
    elif "--hourly" in args:
        print("\n[CLI] Forcing hourly Gmail poll...")
        run_hourly_response_poll()
        sys.exit(0)

    # ── --test ────────────────────────────────────────────────────
    # Run smoke tests (old behaviour).
    elif "--test" in args:
        today      = date.today()
        fetch_date = (today - timedelta(days=3)).strftime("%Y-%m-%d")

        print("=" * 60)
        print(f"TEST 1: Fetch single date ({fetch_date})")
        orders = fetch_orders_for_date(fetch_date)
        print(f"Orders: {len(orders) if orders else 0}")

        print("\n" + "=" * 60)
        print("TEST 2: Manual run")
        result = run_manual(fetch_date, fetch_date)
        print(f"Result: {result}")

        print("\n" + "=" * 60)
        print("TEST 3: Hourly poll")
        run_hourly_response_poll()

        print("\n" + "=" * 60)
        print("TEST 4: Scheduler jobs")
        sched = create_scheduler()
        for job in sched.get_jobs():
            print(f"  - {job.name}")
        sys.exit(0)

    # ── default: run as background scheduler daemon ────────────────
    else:
        print("\n[Scheduler] Starting background scheduler daemon...")
        scheduler = create_scheduler()
        scheduler.start()
        print("[Scheduler] Running. Press Ctrl+C to stop.")
        try:
            while True:
                time.sleep(60)
        except (KeyboardInterrupt, SystemExit):
            scheduler.shutdown()
            print("[Scheduler] Stopped.")
