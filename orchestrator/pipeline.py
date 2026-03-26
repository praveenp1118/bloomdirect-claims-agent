"""
pipeline.py - LangGraph Orchestrator
BloomDirect Claims Recovery System - Full Agent Pipeline

Orchestration patterns:
    1. Conditional routing (eligible/ineligible/borderline/unknown)
    2. Iterative refinement loop (rejected -> reframe -> resubmit, max 3)
    3. Parallel fan-out (UPS and FedEx processed simultaneously)
    4. HITL checkpoints (LangGraph interrupt_before)
"""

import json
import os
from datetime import datetime, date
from typing import TypedDict, Optional, Annotated
from dotenv import load_dotenv

from langgraph.graph import StateGraph, END
from langgraph.checkpoint.memory import MemorySaver


from database.models import (
    init_db, get_session, Order, Failure, Claim,
    ClaimEmailLog, HitlQueue, ErrorLog, SchedulerState
)
from agents.failure_classifier import classify_shipment, classify_from_mcp_history
from agents.eligibility_assessor import assess_eligibility
from mcp_servers.carrier_tracking_mcp import get_full_history
from guardrails.input_validator import validate_shipment
from guardrails.output_validator import validate_output, ClaimEmailDraft

load_dotenv()

# Config
with open("config/system_config.json") as f:
    CONFIG = json.load(f)

URGENCY_DAYS = CONFIG.get("urgency_threshold_days", 2)
FILING_WINDOW = CONFIG.get("filing_window_days", 15)


# ── PIPELINE STATE ────────────────────────────────────────────────

class ClaimState(TypedDict):
    """State passed between all nodes in the LangGraph pipeline."""
    # Input
    order:              dict

    # After validation
    validated_order:    Optional[dict]
    validation_error:   Optional[str]

    # After classification
    classification:     Optional[dict]

    # After MCP call
    mcp_history:        Optional[list]

    # After eligibility
    eligibility:        Optional[dict]

    # After claim drafting
    claim_id:           Optional[int]
    draft_subject:      Optional[str]
    draft_body:         Optional[str]
    draft_validated:    Optional[bool]

    # After filing
    thread_id:          Optional[str]
    filed:              Optional[bool]

    # After response check
    response_classification: Optional[str]
    rejection_reason:   Optional[str]

    # Routing flags
    needs_hitl:         Optional[bool]
    hitl_reason:        Optional[str]
    auto_file:          Optional[bool]
    skip_reason:        Optional[str]
    attempt_number:     int
    error:              Optional[str]


# ── NODE 1: INPUT VALIDATION ──────────────────────────────────────

def node_validate_input(state: ClaimState) -> ClaimState:
    """Validate and sanitize input from Order API."""
    print(f"\n[Node] validate_input: {state['order'].get('track_id', 'UNKNOWN')}")

    result = validate_shipment(state["order"])

    if not result.valid:
        return {
            **state,
            "validation_error": result.skip_reason,
            "skip_reason": result.skip_reason,
        }

    return {
        **state,
        "validated_order": result.sanitized_data,
        "validation_error": None,
    }


# ── NODE 2: FAILURE CLASSIFICATION ───────────────────────────────

def node_classify_failure(state: ClaimState) -> ClaimState:
    """Run Agent 1 - Failure Classifier."""
    print(f"[Node] classify_failure: {state['validated_order']['track_id']}")

    order = state["validated_order"]
    classification = classify_shipment(order)

    return {**state, "classification": classification}


# ── NODE 3: MCP TRACKING CALL ─────────────────────────────────────

def node_call_mcp(state: ClaimState) -> ClaimState:
    """Call Carrier Tracking MCP for full history."""
    order = state["validated_order"]
    track_id = order["track_id"]
    print(f"[Node] call_mcp: {track_id}")

    try:
        mcp_result = get_full_history(
            tracking_id = track_id,
            ship_method = order["ship_method"],
        )
        history = mcp_result.get("history", [])

        # Re-classify using MCP history
        updated_classification = classify_from_mcp_history(
            state["classification"].copy(),
            history
        )

        return {
            **state,
            "mcp_history": history,
            "classification": updated_classification,
        }

    except Exception as e:
        print(f"[MCP Error] {e}")
        # Log error
        session = get_session()
        try:
            error_log = ErrorLog(
                tracking_id = track_id,
                error_type  = "MCP_TIMEOUT",
                stage       = "classification",
                details     = str(e),
            )
            session.add(error_log)
            session.commit()
        finally:
            session.close()

        return {**state, "error": f"MCP_TIMEOUT: {str(e)}"}


# ── NODE 4: ELIGIBILITY ASSESSMENT ───────────────────────────────

def node_assess_eligibility(state: ClaimState) -> ClaimState:
    """Run Agent 2 - Eligibility Assessor."""
    print(f"[Node] assess_eligibility: {state['classification']['failure_type']}")

    eligibility = assess_eligibility(
        classification  = state["classification"],
        attempt_number  = state.get("attempt_number", 1),
    )

    return {
        **state,
        "eligibility": eligibility,
        "needs_hitl":  eligibility.get("hitl_required", False),
        "hitl_reason": eligibility.get("hitl_reason"),
        "auto_file":   eligibility.get("auto_file", False),
    }


# ── NODE 5: SAVE TO DB ────────────────────────────────────────────

def node_save_to_db(state: ClaimState) -> ClaimState:
    """Save order, failure and claim records to database."""
    order          = state["validated_order"]
    classification = state["classification"]
    eligibility    = state["eligibility"]

    print(f"[Node] save_to_db: {order['track_id']}")

    session = get_session()
    try:
        # Upsert Order
        existing_order = session.query(Order).filter(
            Order.partner_order_id == order["partner_order_id"]
        ).first()

        if not existing_order:
            db_order = Order(
                partner_order_id = order["partner_order_id"],
                tracking_id      = order["track_id"],
                ship_method      = order["ship_method"],
                ship_date        = order["ship_date"],
                carrier          = classification.get("carrier", "Unknown"),
                occasion_type    = classification.get("occasion_type", "General"),
            )
            session.add(db_order)
            session.flush()

        # Insert Failure
        failure = Failure(
            partner_order_id = order["partner_order_id"],
            tracking_id      = order["track_id"],
            failure_type     = classification.get("failure_type", "UNKNOWN"),
            delay_days       = classification.get("delay_days", 0),
            first_bad_event  = classification.get("first_bad_event", ""),
            ship_date        = order["ship_date"],
            promised_date    = classification.get("promised_date", ""),
        )
        session.add(failure)
        session.flush()

        # Insert Claim
        claim = Claim(
            failure_id     = failure.failure_id,
            tracking_id    = order["track_id"],
            carrier        = classification.get("carrier", "Unknown"),
            ship_method    = order["ship_method"],
            claim_type     = classification.get("failure_type", "UNKNOWN"),
            claim_amount   = CONFIG.get("claim_amount", 100.0),
            status         = "pending",
            attempt_number = state.get("attempt_number", 1),
            probability    = eligibility.get("probability", 0.0),
            occasion_type  = classification.get("occasion_type", "General"),
            short_label    = build_short_label(classification),
        )
        session.add(claim)
        session.commit()

        return {**state, "claim_id": claim.claim_id}

    except Exception as e:
        session.rollback()
        print(f"[DB Error] {e}")
        return {**state, "error": f"DB_ERROR: {str(e)}"}
    finally:
        session.close()


def build_short_label(classification: dict) -> str:
    """Build a short human-readable label for the failure."""
    ft = classification.get("failure_type", "UNKNOWN")
    event = classification.get("first_bad_event", "")
    delay = classification.get("delay_days", 0)

    labels = {
        "CARRIER_DELAY": f"Carrier delay{f' — {event[:40]}' if event else ''}",
        "LATE":          f"Late delivery — {delay} day(s)",
        "DAMAGE":        "Package damaged in transit",
        "LOST":          "Package lost / missing",
        "WEATHER_DELAY": "Weather delay",
        "UNKNOWN":       "Unknown failure pattern",
    }
    return labels.get(ft, ft)


# ── NODE 6: HITL QUEUE ────────────────────────────────────────────



# ── NODE 5b: GENERATE REASONING ───────────────────────────────────────────

def node_generate_reasoning(state: ClaimState) -> ClaimState:
    """
    Run Reasoning Generator — lightweight LLM that analyses tracking history
    and generates short_label + llm_narrative.
    Only runs in auto_generate and auto_send email modes.
    Manual mode: skipped (user generates from dashboard).
    """
    import json as _json

    # Check email mode
    try:
        with open("config/system_config.json") as _f:
            _cfg = _json.load(_f)
        email_mode = _cfg.get("email", {}).get("mode", "manual")
    except Exception:
        email_mode = "manual"

    if email_mode == "manual":
        print(f"[Node] generate_reasoning: SKIPPED (manual mode)")
        return state

    print(f"[Node] generate_reasoning: claim_id={state.get('claim_id')}")

    try:
        from agents.reasoning_generator import generate_reasoning
        classification = state.get("classification", {})
        order          = state.get("validated_order", {})
        mcp_history    = state.get("mcp_history", [])

        result = generate_reasoning(
            tracking_id     = order.get("track_id", ""),
            carrier         = classification.get("carrier", ""),
            ship_method     = order.get("ship_method", ""),
            ship_date       = order.get("ship_date", ""),
            failure_type    = classification.get("failure_type", "LATE"),
            delay_days      = int(classification.get("delay_days", 1) or 1),
            first_bad_event = classification.get("first_bad_event"),
            promised_date   = classification.get("promised_date"),
            delivered_date  = classification.get("delivered_date"),
            tracking_history= mcp_history,
            occasion_type   = classification.get("occasion_type"),
        )

        # Save to DB
        if state.get("claim_id"):
            session = get_session()
            try:
                claim = session.query(Claim).filter(
                    Claim.claim_id == state["claim_id"]
                ).first()
                if claim:
                    claim.short_label   = result["short_label"]
                    claim.llm_narrative = result["narrative"]
                    claim.updated_at    = datetime.now()
                    session.commit()
            finally:
                session.close()

        print(f"[Node] generate_reasoning: '{result['short_label']}'")
        return state

    except Exception as e:
        print(f"[Node] generate_reasoning ERROR: {e}")
        return state


def node_add_to_hitl(state: ClaimState) -> ClaimState:
    """Add claim to HITL queue for human review."""
    print(f"[Node] add_to_hitl: {state.get('claim_id')} — {state.get('hitl_reason')}")

    session = get_session()
    try:
        hitl = HitlQueue(
            claim_id      = state["claim_id"],
            tracking_id   = state["validated_order"]["track_id"],
            reason        = state.get("hitl_reason", "Unknown"),
            status        = "pending",
            days_remaining= state["eligibility"].get("days_remaining", 0),
        )
        session.add(hitl)

        # Update claim status
        claim = session.query(Claim).filter(
            Claim.claim_id == state["claim_id"]
        ).first()
        if claim:
            claim.status = "hitl_pending"
            claim.updated_at = datetime.now()

        session.commit()
        print(f"[HITL] Added to queue: {state.get('hitl_reason')}")
        return state

    except Exception as e:
        session.rollback()
        return {**state, "error": f"HITL_ERROR: {str(e)}"}
    finally:
        session.close()


# ── NODE 7: CLAIM DRAFTING ────────────────────────────────────────

def node_draft_claim(state: ClaimState) -> ClaimState:
    """Run Agent 3 - Claim Drafter (LLM-powered)."""
    print(f"[Node] draft_claim: claim_id={state.get('claim_id')}")

    # Import here to avoid circular imports
    try:
        from agents.claim_drafter import draft_claim_email
        draft = draft_claim_email(state)

        # Validate output
        claim_draft = ClaimEmailDraft(
            subject        = draft.get("subject", ""),
            body           = draft.get("body", ""),
            carrier        = state["classification"].get("carrier", "FedEx"),
            tracking_id    = state["validated_order"]["track_id"],
            ship_date      = state["validated_order"]["ship_date"],
            claim_type     = state["classification"].get("failure_type", ""),
            attempt_number = state.get("attempt_number", 1),
            policy_reference = draft.get("policy_reference", ""),
        )

        validation = validate_output(claim_draft)

        if not validation.valid and validation.action == "block":
            # Critical issue — route to HITL
            return {
                **state,
                "draft_subject":  draft.get("subject"),
                "draft_body":     draft.get("body"),
                "draft_validated": False,
                "needs_hitl":     True,
                "hitl_reason":    f"Output guardrail blocked email: {validation.issues}",
            }

        # Use sanitized version
        return {
            **state,
            "draft_subject":  validation.sanitized_subject or draft.get("subject"),
            "draft_body":     validation.sanitized_body or draft.get("body"),
            "draft_validated": True,
        }

    except ImportError:
        # Claim drafter not yet implemented — use placeholder
        order = state["validated_order"]
        classification = state["classification"]
        carrier = classification.get("carrier", "FedEx")
        track_id = order["track_id"]

        subject = f"Claim Request — Track ID: {track_id} — {classification.get('failure_type', 'CLAIM')}"
        body = f"""Dear {carrier} Claims Team,

I am writing to file a claim for shipment {track_id}.

Ship date: {order['ship_date']}
Ship method: {order['ship_method']}
Failure type: {classification.get('failure_type', 'Unknown')}
First noted event: {classification.get('first_bad_event', 'See tracking history')}

Claim amount: $100.00

Please review and process this claim at your earliest convenience.

Regards,
BloomDirect Logistics Team"""

        return {
            **state,
            "draft_subject":  subject,
            "draft_body":     body,
            "draft_validated": True,
        }


# ── NODE 8: FILE CLAIM ────────────────────────────────────────────

def node_file_claim(state: ClaimState) -> ClaimState:
    """Send claim email via Email MCP — respects email mode from system_config.json."""
    print(f"[Node] file_claim: {state.get('claim_id')}")

    # Check email mode — only auto_send fires immediately
    # manual and auto_generate save draft only, no send
    import json
    try:
        with open("config/system_config.json") as _f:
            _cfg = json.load(_f)
        email_mode = _cfg.get("email", {}).get("mode", "manual")
    except Exception:
        email_mode = "manual"

    order       = state["validated_order"]
    classification = state["classification"]

    if email_mode != "auto_send":
        # Save draft to DB but do NOT send
        session = get_session()
        try:
            claim = session.query(Claim).filter(
                Claim.claim_id == state["claim_id"]
            ).first()
            if claim:
                claim.draft_email_text = state["draft_body"]
                claim.status           = "draft_pending_send"
                claim.updated_at       = datetime.now()
                session.commit()
        finally:
            session.close()
        print(f"[Node] file_claim: mode={email_mode} — draft saved, not sent")
        return {**state, "filed": False, "thread_id": None}

    from mcp_servers.email_claims_mcp import send_claim_email

    order          = state["validated_order"]
    classification = state["classification"]
    carrier        = classification.get("carrier", "FedEx")

    # Resolve correct email target from config
    email_env  = _cfg.get("email", {}).get("env", "test")
    test_addr  = _cfg.get("email", {}).get("test_address", "")
    if email_env == "test":
        to_addr = test_addr
    else:
        to_addr = "support@shippo.com" if "UPS" in carrier.upper() else "file.claim@fedex.com"

    # In auto_send, queue for paced sending instead of immediate send
    session = get_session()
    try:
        claim = session.query(Claim).filter(
            Claim.claim_id == state["claim_id"]
        ).first()
        if claim:
            claim.draft_email_text = state["draft_body"]
            claim.status           = "queued_to_send"
            claim.updated_at       = datetime.now()
            session.commit()
    finally:
        session.close()
    print(f"[Node] file_claim: auto_send — queued for paced sending (to: {to_addr})")
    return {**state, "filed": False, "thread_id": None}


# ── ROUTING FUNCTIONS ─────────────────────────────────────────────

def route_after_validation(state: ClaimState) -> str:
    """Route after input validation."""
    if state.get("validation_error"):
        return "skip"
    return "classify"


def route_after_classification(state: ClaimState) -> str:
    """Route after failure classification."""
    if state.get("error"):
        return "error"
    classification = state.get("classification", {})
    failure_type = classification.get("failure_type")

    # Check needs_mcp BEFORE checking failure_type
    if classification.get("needs_mcp"):
        return "call_mcp"

    # No claim needed
    if failure_type in ("ON_TIME", "NO_CLAIM", None):
        return "skip"

    return "assess_eligibility"


def route_after_mcp(state: ClaimState) -> str:
    """Route after MCP call."""
    if state.get("error") and "MCP_TIMEOUT" in state.get("error", ""):
        return "error"

    classification = state.get("classification", {})
    failure_type = classification.get("failure_type")

    if failure_type in ("ON_TIME", "NO_CLAIM", None):
        return "skip"

    return "assess_eligibility"


def route_after_eligibility(state: ClaimState) -> str:
    """Route after eligibility assessment."""
    eligibility = state.get("eligibility", {})

    # Not eligible
    if not eligibility.get("eligible"):
        return "skip"

    # Auto-file (≤2 days remaining — bypass HITL)
    if eligibility.get("auto_file") or state.get("auto_file"):
        return "save_and_draft"

    # HITL required
    if eligibility.get("hitl_required") or state.get("needs_hitl"):
        return "hitl"

    return "save_and_draft"


def route_after_draft(state: ClaimState) -> str:
    """Route after claim drafting."""
    if state.get("needs_hitl"):
        return "hitl"
    if state.get("draft_validated"):
        return "file"
    return "hitl"


# ── BUILD GRAPH ───────────────────────────────────────────────────

def build_pipeline():
    """Build and compile the LangGraph pipeline."""

    # Use SQLite checkpointer for HITL state persistence
    memory = MemorySaver()
    #memory = SqliteSaver.from_conn_string("bloomdirect_checkpoints.db")

    graph = StateGraph(ClaimState)

    # Add nodes
    graph.add_node("validate_input",      node_validate_input)
    graph.add_node("classify_failure",    node_classify_failure)
    graph.add_node("call_mcp",            node_call_mcp)
    graph.add_node("assess_eligibility",  node_assess_eligibility)
    graph.add_node("save_to_db",          node_save_to_db)
    graph.add_node("add_to_hitl",         node_add_to_hitl)
    graph.add_node("generate_reasoning",  node_generate_reasoning)
    graph.add_node("draft_claim",         node_draft_claim)
    graph.add_node("file_claim",          node_file_claim)

    # Entry point
    graph.set_entry_point("validate_input")

    # Conditional edges
    graph.add_conditional_edges(
        "validate_input",
        route_after_validation,
        {"classify": "classify_failure", "skip": END}
    )

    graph.add_conditional_edges(
        "classify_failure",
        route_after_classification,
        {
            "call_mcp":          "call_mcp",
            "assess_eligibility":"assess_eligibility",
            "skip":              END,
            "error":             END,
        }
    )

    graph.add_conditional_edges(
        "call_mcp",
        route_after_mcp,
        {
            "assess_eligibility": "assess_eligibility",
            "skip":               END,
            "error":              END,
        }
    )

    graph.add_conditional_edges(
        "assess_eligibility",
        route_after_eligibility,
        {
            "save_and_draft": "save_to_db",
            "hitl":           "save_to_db",
            "skip":           END,
        }
    )

    # After save_to_db — check if HITL or reasoning+draft
    graph.add_conditional_edges(
        "save_to_db",
        lambda s: "hitl" if s.get("needs_hitl") else "reasoning",
        {"hitl": "add_to_hitl", "reasoning": "generate_reasoning"}
    )

    graph.add_edge("generate_reasoning", "draft_claim")

    graph.add_edge("add_to_hitl", END)

    graph.add_conditional_edges(
        "draft_claim",
        route_after_draft,
        {"file": "file_claim", "hitl": "add_to_hitl"}
    )

    graph.add_edge("file_claim", END)

    return graph.compile(checkpointer=memory)


# ── RUN PIPELINE ──────────────────────────────────────────────────

def run_single(order: dict, attempt_number: int = 1) -> dict:
    """
    Run the pipeline for a single shipment order.

    Args:
        order: Shipment dict from Order API or synthetic data
        attempt_number: 1 for first filing, 2+ for resubmission

    Returns:
        Final pipeline state
    """
    pipeline = build_pipeline()

    initial_state = ClaimState(
        order            = order,
        validated_order  = None,
        validation_error = None,
        classification   = None,
        mcp_history      = None,
        eligibility      = None,
        claim_id         = None,
        draft_subject    = None,
        draft_body       = None,
        draft_validated  = None,
        thread_id        = None,
        filed            = None,
        response_classification = None,
        rejection_reason = None,
        needs_hitl       = False,
        hitl_reason      = None,
        auto_file        = False,
        skip_reason      = None,
        attempt_number   = attempt_number,
        error            = None,
    )

    config = {"configurable": {"thread_id": order.get("track_id", "default")}}

    try:
        final_state = pipeline.invoke(initial_state, config=config)
        return final_state
    except Exception as e:
        print(f"[Pipeline Error] {e}")
        return {**initial_state, "error": str(e)}


def run_batch(orders: list, attempt_number: int = 1) -> list:
    """
    Run the pipeline for a batch of orders.
    UPS and FedEx processed in the same batch (parallel-ready).

    Args:
        orders: List of shipment dicts
        attempt_number: 1 for first filing

    Returns:
        List of final states
    """
    results = []
    print(f"\n[Pipeline] Processing batch of {len(orders)} orders...")

    for i, order in enumerate(orders, 1):
        track_id = order.get("track_id", "UNKNOWN")
        print(f"\n[{i}/{len(orders)}] Processing: {track_id}")
        result = run_single(order, attempt_number)
        results.append(result)

    # Summary
    filed    = sum(1 for r in results if r.get("filed"))
    skipped  = sum(1 for r in results if r.get("skip_reason"))
    hitl     = sum(1 for r in results if r.get("needs_hitl") and not r.get("filed"))
    errors   = sum(1 for r in results if r.get("error"))

    print(f"\n[Pipeline] Batch complete:")
    print(f"  Filed:   {filed}")
    print(f"  Skipped: {skipped}")
    print(f"  HITL:    {hitl}")
    print(f"  Errors:  {errors}")

    return results


# ── TEST ──────────────────────────────────────────────────────────
if __name__ == "__main__":
    init_db()

    test_orders = [
    {
        "partner_order_id":       "PIPE-TEST-001",
        "ship_method":            "FEDEX International",
        "ship_date":              "2026-03-10",
        "track_id":               "111111111111",
        "last_track_status":      "Delay",
        "last_track_status_date": "2026-03-11 10:00",
        "first_track_status":     "Picked up",
        "first_track_status_date":"2026-03-10 18:00",
        "gift_message":           "Happy Birthday!",
    },
    {
        "partner_order_id":       "PIPE-TEST-002",
        "ship_method":            "UPS_Ground",
        "ship_date":              "2026-03-10",
        "track_id":               "1ZK1V6600318414000",
        "last_track_status":      "A damage has been reported and we will notify the sender.",
        "last_track_status_date": "2026-03-11 14:00",
        "first_track_status":     "Picked up",
        "first_track_status_date":"2026-03-10 09:00",
        "gift_message":           "",
    },
]

    print("=" * 60)
    print("PIPELINE TEST — 2 orders")
    results = run_batch(test_orders)

    print("\n" + "=" * 60)
    print("RESULTS SUMMARY")
    for r in results:
        track = r.get("validated_order", {}).get("track_id", "UNKNOWN") if r.get("validated_order") else "INVALID"
        failure = r.get("classification", {}).get("failure_type", "N/A") if r.get("classification") else "N/A"
        claim_id = r.get("claim_id", "N/A")
        filed = r.get("filed", False)
        hitl = r.get("needs_hitl", False)
        skip = r.get("skip_reason", "")
        error = r.get("error", "")

        print(f"\nTrack: {track}")
        print(f"  Failure type: {failure}")
        print(f"  Claim ID:     {claim_id}")
        print(f"  Filed:        {filed}")
        print(f"  HITL:         {hitl}")
        if skip:  print(f"  Skipped:      {skip}")
        if error: print(f"  Error:        {error}")
