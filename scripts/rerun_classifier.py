"""
rerun_classifier.py
====================
Reruns the full pipeline on all existing orders in the DB.
- Does NOT re-fetch from Order API
- Does NOT make new MCP calls (uses tracking_cache)
- Clears failures, claims, hitl, errors before rerun
- Keeps orders and tracking_cache intact

Usage (on EC2):
  docker exec -it bloomdirect_dashboard bash -c \
    "cd /app && PYTHONPATH=/app DATABASE_URL=sqlite:////app/data/bloomdirect.db python3 scripts/rerun_classifier.py"
"""

import sys
import os

os.environ.setdefault("DATABASE_URL", "sqlite:////app/data/bloomdirect.db")
sys.path.insert(0, "/app")

from database.models import (
    init_db, get_session, Order, Failure, Claim,
    ClaimEmailLog, HitlQueue, ErrorLog, TrackingCache
)
from orchestrator.pipeline import run_batch

init_db()
session = get_session()

# ── Step 1: Clear derived tables, keep orders + tracking_cache ──────────────
print("Clearing failures, claims, hitl, errors...")
session.query(ClaimEmailLog).delete()
session.query(HitlQueue).delete()
session.query(Claim).delete()
session.query(Failure).delete()
session.query(ErrorLog).delete()
session.commit()
print("Cleared.")

# ── Step 2: Build order dicts from DB + tracking_cache ──────────────────────
orders = session.query(Order).all()
print(f"\nBuilding order dicts for {len(orders)} orders...")

# Build cache lookup for fast access
cache_lookup = {}
cache_records = session.query(TrackingCache).all()
for c in cache_records:
    cache_lookup[c.tracking_id] = c

order_dicts = []
for o in orders:
    cache = cache_lookup.get(o.tracking_id)

    # Use cached status if available, else assume needs MCP
    last_status      = cache.cached_status if cache else "Unknown"
    last_status_date = cache.cached_status_date if cache else ""

    order_dicts.append({
        "partner_order_id":        o.partner_order_id,
        "track_id":                o.tracking_id,
        "ship_method":             o.ship_method,
        "ship_date":               o.ship_date,
        "carrier":                 o.carrier,
        "last_track_status":       last_status,
        "last_track_status_date":  last_status_date,
        "first_track_status":      "Picked up",
        "first_track_status_date": o.ship_date,
        "gift_message":            "",
    })

session.close()

# ── Step 3: Run pipeline ─────────────────────────────────────────────────────
print(f"\nRunning pipeline on {len(order_dicts)} orders...")
print("(MCP cache will be used — no new carrier API calls for cached shipments)\n")

results = run_batch(order_dicts)

# ── Step 4: Summary ──────────────────────────────────────────────────────────
filed   = sum(1 for r in results if r.get("filed"))
drafts  = sum(1 for r in results if r.get("draft_body") and not r.get("filed"))
hitl    = sum(1 for r in results if r.get("needs_hitl") and not r.get("filed"))
skipped = sum(1 for r in results if r.get("skip_reason"))
errors  = sum(1 for r in results if r.get("error"))

print(f"\n{'='*50}")
print(f"RERUN COMPLETE")
print(f"{'='*50}")
print(f"  Total orders : {len(order_dicts)}")
print(f"  Drafted      : {drafts}  (saved, not sent — manual mode)")
print(f"  Filed        : {filed}")
print(f"  HITL queue   : {hitl}")
print(f"  Skipped      : {skipped}")
print(f"  Errors       : {errors}")
print(f"{'='*50}")
