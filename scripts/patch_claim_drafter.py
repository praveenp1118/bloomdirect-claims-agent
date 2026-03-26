"""
patch_claim_drafter.py
Patches claim_drafter.py:
  1. REBLOOM branding
  2. Evidence-based strong prompt citing carrier fault events
  3. Firm tone
"""

with open('/app/agents/claim_drafter.py', 'r') as f:
    content = f.read()

print(f"Original size: {len(content)} chars")

# Fix 1: Replace all BloomDirect
content = content.replace("BloomDirect Logistics Team", "REBLOOM Logistics")
content = content.replace("BloomDirect, a premium floral e-commerce company", "REBLOOM, a floral e-commerce company")
content = content.replace("BloomDirect", "REBLOOM")
print("Branding fixed")

# Fix 2: Replace build_prompt
start_marker = "def build_prompt(state: dict) -> str:"
end_marker   = "\ndef draft_claim_email"
start_idx = content.find(start_marker)
end_idx   = content.find(end_marker, start_idx)

if start_idx == -1 or end_idx == -1:
    print(f"build_prompt bounds not found")
else:
    new_func = r'''def build_prompt(state: dict) -> str:
    """Build evidence-based claim prompt citing carrier fault events."""
    order          = state.get("validated_order", {})
    classification = state.get("classification", {})
    eligibility    = state.get("eligibility", {})
    mcp_history    = state.get("mcp_history", [])
    attempt        = state.get("attempt_number", 1)

    track_id     = order.get("track_id", "")
    ship_method  = order.get("ship_method", "")
    ship_date    = order.get("ship_date", "")
    carrier      = classification.get("carrier", "FedEx")
    failure_type = classification.get("failure_type", "LATE")
    delay_days   = int(classification.get("delay_days", 1) or 1)
    promised     = classification.get("promised_date", "")
    occasion     = classification.get("occasion_type", "General")
    probability  = eligibility.get("probability", 0.5)

    sorted_history = sorted(mcp_history, key=lambda x: str(x.get("date", "")))

    FAULT_KEYWORDS = [
        "mechanical failure", "late trailer", "late flight", "missed flight",
        "railroad mechanical", "flight cancellation", "incorrectly sorted",
        "delay", "exception", "damage", "missing merchandise"
    ]
    fault_events = []
    for event in sorted_history:
        status = str(event.get("status", "")).lower()
        if any(kw in status for kw in FAULT_KEYWORDS):
            fault_events.append({
                "date":     str(event.get("date", ""))[:16],
                "status":   str(event.get("status", "")),
                "location": str(event.get("location", "") or "")
            })

    history_lines = []
    for e in sorted_history[-8:]:
        loc = f" ({e.get('location','')})" if e.get("location") else ""
        history_lines.append(f"  {str(e.get('date',''))[:16]} — {e.get('status','')}{loc}")
    history_str = "\n".join(history_lines) if history_lines else "Not available"

    fault_str = ""
    if fault_events:
        fault_str = "Carrier-fault events:\n"
        for e in fault_events[:3]:
            loc = f" at {e['location']}" if e['location'] else ""
            fault_str += f"  {e['date']}{loc}: \"{e['status']}\"\n"

    occasion_context = ""
    if occasion and occasion != "General":
        occasion_context = f"This was a {occasion} gift order — the delay meant the customer missed their occasion entirely."

    tone = "firm and assertive" if probability >= 0.6 else "professional but persistent"
    resubmit = f"\nNote: Attempt #{attempt} — previous claim rejected. Strengthen the argument." if attempt > 1 else ""
    carrier_team = "UPS Claims Team" if "UPS" in carrier.upper() else f"{carrier} Claims Team"
    guarantee    = "UPS Service Guarantee" if "UPS" in carrier.upper() else "FedEx Money-Back Guarantee"

    prompt = f"""You are a shipping claims specialist writing on behalf of REBLOOM.{resubmit}

Shipment:
- Tracking ID: {track_id}
- Carrier: {carrier} ({ship_method})
- Ship Date: {ship_date}
- Promised Delivery: {promised or "next working day"}
- Failure: {failure_type} — {delay_days} day(s) late
- Tone: {tone}
{occasion_context}

Tracking history:
{history_str}

{fault_str}

Write the claim email:

1. Details block:
Tracking ID:   {track_id}
Ship Date:     {ship_date}
Delay:         {delay_days} day(s) past promised date
Reason:        [short reason citing first fault event]
{"Occasion:      " + occasion if occasion and occasion != "General" else ""}

---

2. ONE firm paragraph (4-5 sentences):
   - State guaranteed vs actual delivery dates explicitly
   - Cite specific fault event by date, location, exact status text (if available)
   - State this is carrier-side failure — not weather or shipper error
   - {occasion_context if occasion_context else "State the business impact"}
   - Demand full refund under {guarantee}, expect response within 5 business days

3. Sign off:
Regards,
REBLOOM Logistics

Rules: Address to {carrier_team}. Cite their own data. Firm not passive. No subject line. No markdown. Under 200 words."""

    return prompt

'''
    content = content[:start_idx] + new_func + content[end_idx:]
    print("build_prompt replaced")

# Fix 3: Remaining branding
content = content.replace("Regards,\nBloomDirect Logistics Team", "Regards,\nREBLOOM Logistics")
content = content.replace("Regards,\nREBLOOM Logistics Team", "Regards,\nREBLOOM Logistics")

with open('/app/agents/claim_drafter.py', 'w') as f:
    f.write(content)

remaining = content.count("BloomDirect")
print(f"Remaining BloomDirect refs: {remaining}")
print("Done")
