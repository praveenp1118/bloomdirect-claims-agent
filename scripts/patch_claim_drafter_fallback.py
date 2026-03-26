"""
patch_claim_drafter_fallback.py
Makes the fallback email in claim_drafter.py smarter by reading
full mcp_history to find ALL carrier-fault events, not just first_bad_event.
Run: PYTHONPATH=/app python3 /tmp/patch_claim_drafter_fallback.py
"""

with open('/app/agents/claim_drafter.py', 'r') as f:
    lines = f.readlines()

# Find the fallback function that builds the body (after LLM fails)
# Look for the def that contains "except" and then builds body = f"""
start = None
end   = None

for i, line in enumerate(lines):
    if 'def draft_claim_email' in line:
        start = i
    if start and i > start and 'body = f"""Tracking ID' in line:
        body_start = i
    if start and i > start and 'return {' in line and '"body"' in line and i > start + 50:
        end = i
        break

print(f"draft_claim_email: start={start}, body_start={body_start}, return={end}")

# Replace fallback body with smarter version
new_fallback = '''
    # ── Smarter deterministic fallback ───────────────────────────
    # Read full mcp_history to find ALL carrier-fault events
    FAULT_KEYWORDS = [
        "mechanical failure", "late trailer", "late flight", "missed flight",
        "railroad mechanical", "flight cancellation", "incorrectly sorted",
        "delay", "exception", "damage", "missing merchandise"
    ]

    sorted_history = sorted(mcp_history, key=lambda x: str(x.get("date", "")))
    fault_events = []
    for h_event in sorted_history:
        h_status = str(h_event.get("status", "")).lower()
        if any(kw in h_status for kw in FAULT_KEYWORDS):
            fault_events.append(h_event)

    # Build evidence paragraph
    if fault_events:
        first_fault = fault_events[0]
        fault_date  = str(first_fault.get("date", ""))[:16]
        fault_status= str(first_fault.get("status", ""))
        fault_loc   = str(first_fault.get("location", "") or "")
        loc_str     = f" at {fault_loc}" if fault_loc else ""
        evidence    = (f" Your tracking records confirm a carrier-side failure{loc_str} on {fault_date}: "
                      f"{fault_status}.")
        fault_count = len(fault_events)
        if fault_count > 1:
            evidence += f" ({fault_count} fault events recorded in total.)"
    elif event:
        evidence = f" Your tracking records confirm: {event}."
    else:
        evidence = ""

    occasion_line = ""
    if occasion_type and occasion_type != "General":
        occasion_line = (f" This was a {occasion_type} gift order and the delay "
                        f"meant the customer missed their occasion entirely.")

'''

# Replace old fallback lines
if body_start and end:
    # Get the existing return statement
    return_line = lines[end].rstrip()

    new_body_lines = new_fallback.split('\n')
    new_body_lines += [
        f'    body = f"""Tracking ID:   {"{track_id}"}\n',
        f'Ship Date:     {"{ship_date}"}\n',
        f'Delay:         {"{delay_days}"} day(s) past promised date\n',
        f'Reason:        {"{claim_type}"}{"{f\\' — {event[:60]}\\' if event else \\'\\'}"}\n',
        f'{"{f\\\"Occasion:      {occasion_type}\\\" if occasion_type and occasion_type != \\\"General\\\" else \\\"\\\"}"}\n',
        '\n',
        '---\n',
        '\n',
        f'Dear {"{carrier}"} Claims Team,\n',
        '\n',
        f'Shipment {"{track_id}"} was shipped on {"{ship_date}"} under {"{ship_method}"} '
        f'with a guaranteed delivery date that was missed by {"{delay_days}"} day(s).'
        f'{"{evidence}"}'
        f' This constitutes a breach of the {"{policy_ref}"} attributable to {"{carrier}"} '
        f'operations, not weather or shipper error.'
        f'{"{occasion_line}"}'
        f' We formally request a full refund of all shipping charges and expect '
        f'confirmation within 5 business days.\n',
        '\n',
        'Regards,\n',
        'REBLOOM Logistics"""\n',
    ]

    lines = lines[:body_start] + ['\n'.join(new_body_lines) + '\n'] + lines[end:]

    with open('/app/agents/claim_drafter.py', 'w') as f:
        f.writelines(lines)
    print("Smarter fallback written")
else:
    print("Could not find fallback boundaries - using simpler fix")

    # Simpler approach: just add fault_events detection before the body
    with open('/app/agents/claim_drafter.py', 'r') as f:
        content = f.read()

    old = '''    body = f"""Tracking ID:   {track_id}
Ship Date:     {ship_date}
Delay:         {delay_days} day(s) past promised date
Reason:        {claim_type}{f" — {event[:60]}" if event else ""}

---

Dear {carrier} Claims Team,

Shipment {track_id} was shipped on {ship_date} under {ship_method} with a guaranteed delivery that was missed by {delay_days} day(s). Your tracking records confirm a carrier-side failure that directly caused this delay: {event} This constitutes a breach of the {policy_ref} attributable to {carrier} operations, not weather or shipper error. We formally request a full refund of all shipping charges and expect confirmation within 5 business days.

Regards,
REBLOOM Logistics"""'''

    new = '''    # Build evidence from full mcp_history
    FAULT_KWS = ["mechanical failure","late trailer","late flight","missed flight",
                  "delay","exception","damage","missing merchandise"]
    sorted_h  = sorted(mcp_history, key=lambda x: str(x.get("date","")))
    faults    = [e for e in sorted_h if any(k in str(e.get("status","")).lower() for k in FAULT_KWS)]
    if faults:
        fe    = faults[0]
        floc  = f" at {fe.get('location','')}" if fe.get("location") else ""
        evid  = f" Your tracking records confirm{floc} on {str(fe.get('date',''))[:16]}: {fe.get('status','')}."
        if len(faults) > 1: evid += f" ({len(faults)} fault events total.)"
    elif event:
        evid  = f" Your tracking records confirm: {event}."
    else:
        evid  = ""
    occ_line = (f" This was a {occasion_type} gift order and the delay meant the customer missed their occasion."
                if occasion_type and occasion_type != "General" else "")

    body = f"""Tracking ID:   {track_id}
Ship Date:     {ship_date}
Delay:         {delay_days} day(s) past promised date
Reason:        {claim_type}{f" — {event[:60]}" if event else ""}
{"Occasion:      " + (occasion_type or "") if occasion_type and occasion_type != "General" else ""}

---

Dear {carrier} Claims Team,

Shipment {track_id} was shipped on {ship_date} under {ship_method} with a guaranteed delivery that was missed by {delay_days} day(s).{evid} This constitutes a breach of the {policy_ref} attributable to {carrier} operations, not weather or shipper error.{occ_line} We formally request a full refund of all shipping charges and expect confirmation within 5 business days.

Regards,
REBLOOM Logistics"""'''

    if old in content:
        content = content.replace(old, new)
        print("Simpler fallback fix applied")
    else:
        print("Could not find pattern - manual fix needed")
        idx = content.find("body = f\"\"\"Tracking ID")
        if idx != -1:
            print("Current body at:")
            print(content[idx:idx+500])

    with open('/app/agents/claim_drafter.py', 'w') as f:
        f.write(content)

print("Done")
