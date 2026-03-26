"""
Reclassify helper — Steps 4, 5, 6 for the daily scheduler.
Step 4: Enrich orders with fresh cache data after MCP refresh
Step 5: Find old unresolved shipments, call MCP again
Step 6: Reclassify those old ones
"""
import json
from datetime import datetime, date, timedelta
from database.models import (
    get_session, TrackingCache, Failure, Order, Claim, HitlQueue
)


def enrich_orders_with_cache(orders: list) -> list:
    """
    Step 4: After MCP refresh, update order dicts with fresh cache data.
    This ensures the classifier sees 'Delivered' even if the Order API was stale.
    """
    session = get_session()
    try:
        for order in orders:
            track_id = str(order.get("track_id", ""))
            if not track_id:
                continue
            cache = session.query(TrackingCache).filter_by(tracking_id=track_id).first()
            if cache and cache.cached_status:
                order["last_track_status"] = cache.cached_status
                order["last_track_status_date"] = cache.cached_status_date or ""
        return orders
    finally:
        session.close()


def reclassify_old_unresolved():
    """
    Steps 5 & 6:
    - Find tracking IDs in failures table with UNKNOWN or not-delivered status
    - Check tracking_cache for updated status
    - If now delivered → reclassify (update failure record)
    - If still not delivered → call MCP, update cache, reclassify
    """
    from mcp_servers.carrier_tracking_mcp import get_full_history
    from agents.failure_classifier import classify_shipment

    session = get_session()
    updated = 0
    mcp_calls = 0

    try:
        # Find all UNKNOWN failures
        unknown_failures = session.query(Failure).filter(
            Failure.failure_type == "UNKNOWN"
        ).all()

        if not unknown_failures:
            print("[Reclassify] No UNKNOWN failures to reclassify")
            return {"updated": 0, "mcp_calls": 0}

        print(f"[Reclassify] Found {len(unknown_failures)} UNKNOWN failures to check")

        for failure in unknown_failures:
            track_id = failure.tracking_id
            cache = session.query(TrackingCache).filter_by(tracking_id=track_id).first()

            # Step 5: If cache doesn't show delivered, call MCP again
            if cache and cache.cached_status and \
               "delivered" not in cache.cached_status.lower():
                try:
                    print(f"[Reclassify] MCP refresh: {track_id}")
                    result = get_full_history(track_id, ship_method=failure.carrier or "")
                    status = result.get("status", "")
                    status_date = result.get("status_date", "")
                    history = result.get("history", [])
                    mcp_calls += 1

                    session.merge(TrackingCache(
                        tracking_id=track_id,
                        carrier=failure.carrier or "",
                        cached_status=status,
                        cached_status_date=status_date,
                        full_history_json=json.dumps(history),
                        last_mcp_call=datetime.now(),
                        source="mcp",
                    ))
                    session.commit()

                    # Update cache reference for reclassification
                    cache = session.query(TrackingCache).filter_by(tracking_id=track_id).first()

                except Exception as e:
                    session.rollback()
                    print(f"[Reclassify] MCP error {track_id}: {e}")
                    continue

            # Step 6: Reclassify with updated cache data
            if cache and cache.cached_status and \
               "delivered" in cache.cached_status.lower():

                # Get the order record to build the classification input
                order = session.query(Order).filter_by(tracking_id=track_id).first()
                if not order:
                    continue

                order_dict = {
                    "partner_order_id": order.partner_order_id,
                    "track_id": track_id,
                    "ship_method": order.ship_method,
                    "ship_date": order.ship_date,
                    "carrier": order.carrier,
                    "last_track_status": cache.cached_status,
                    "last_track_status_date": cache.cached_status_date or "",
                    "first_track_status": "Picked up",
                    "first_track_status_date": order.ship_date,
                    "gift_message": "",
                }

                # Reclassify
                classification = classify_shipment(order_dict)
                new_type = classification.get("failure_type", "UNKNOWN")

                if new_type != "UNKNOWN":
                    old_type = failure.failure_type
                    failure.failure_type = new_type
                    failure.delay_days = classification.get("delay_days", 0)
                    failure.notes = "; ".join(classification.get("notes", []))

                    # If now ON_TIME, remove from HITL queue and claims
                    if new_type == "ON_TIME":
                        session.query(HitlQueue).filter_by(tracking_id=track_id).delete()
                        session.query(Claim).filter_by(tracking_id=track_id).delete()

                    session.commit()
                    updated += 1
                    print(f"[Reclassify] {track_id}: {old_type} → {new_type}")

        print(f"[Reclassify] Done — {updated} updated, {mcp_calls} MCP calls")
        return {"updated": updated, "mcp_calls": mcp_calls}

    except Exception as e:
        session.rollback()
        print(f"[Reclassify] Error: {e}")
        return {"updated": 0, "mcp_calls": 0}
    finally:
        session.close()