"""
patch_pipeline.py
Adds node_generate_reasoning to the existing pipeline.py on EC2.
Run from /app directory:
  PYTHONPATH=/app DATABASE_URL=sqlite:////app/data/bloomdirect.db python3 /tmp/patch_pipeline.py
"""

with open('/app/orchestrator/pipeline.py', 'r') as f:
    content = f.read()

# ── 1. Add reasoning node function after node_save_to_db ──────────────────

reasoning_node = '''

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

'''

# Insert after node_save_to_db function
insert_after = "def node_add_to_hitl"
if reasoning_node.strip() not in content:
    content = content.replace(
        f"def node_add_to_hitl",
        reasoning_node + "\ndef node_add_to_hitl"
    )
    print("Reasoning node function added")
else:
    print("Reasoning node already exists")

# ── 2. Add node to graph ──────────────────────────────────────────────────

old_graph_nodes = '''    graph.add_node("validate_input",      node_validate_input)
    graph.add_node("classify_failure",    node_classify_failure)
    graph.add_node("call_mcp",            node_call_mcp)
    graph.add_node("assess_eligibility",  node_assess_eligibility)
    graph.add_node("save_to_db",          node_save_to_db)
    graph.add_node("add_to_hitl",         node_add_to_hitl)
    graph.add_node("draft_claim",         node_draft_claim)
    graph.add_node("file_claim",          node_file_claim)'''

new_graph_nodes = '''    graph.add_node("validate_input",      node_validate_input)
    graph.add_node("classify_failure",    node_classify_failure)
    graph.add_node("call_mcp",            node_call_mcp)
    graph.add_node("assess_eligibility",  node_assess_eligibility)
    graph.add_node("save_to_db",          node_save_to_db)
    graph.add_node("add_to_hitl",         node_add_to_hitl)
    graph.add_node("generate_reasoning",  node_generate_reasoning)
    graph.add_node("draft_claim",         node_draft_claim)
    graph.add_node("file_claim",          node_file_claim)'''

if old_graph_nodes in content:
    content = content.replace(old_graph_nodes, new_graph_nodes)
    print("Graph node added")
else:
    print("Graph node pattern not found")

# ── 3. Route save_to_db → generate_reasoning → draft ─────────────────────

old_route = '''    # After save_to_db — check if HITL or draft
    graph.add_conditional_edges(
        "save_to_db",
        lambda s: "hitl" if s.get("needs_hitl") else "draft",
        {"hitl": "add_to_hitl", "draft": "draft_claim"}
    )'''

new_route = '''    # After save_to_db — check if HITL or reasoning+draft
    graph.add_conditional_edges(
        "save_to_db",
        lambda s: "hitl" if s.get("needs_hitl") else "reasoning",
        {"hitl": "add_to_hitl", "reasoning": "generate_reasoning"}
    )

    graph.add_edge("generate_reasoning", "draft_claim")'''

if old_route in content:
    content = content.replace(old_route, new_route)
    print("Route updated")
else:
    print("Route pattern not found")

with open('/app/orchestrator/pipeline.py', 'w') as f:
    f.write(content)

print("\nPipeline patched successfully.")
