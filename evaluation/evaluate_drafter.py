#!/usr/bin/env python3
"""
BloomDirect Claims Recovery System
===================================
Sub-Agent Evaluation: Claim Drafter (LLM-as-Judge)
ISB AMPBA Batch 24 — Group 8

Evaluates the Claim Drafter agent across 15 curated scenarios using
Claude claude-sonnet-4-6 as the judge. Scores each draft on 4 dimensions:
  - Tone Appropriateness (1-5)
  - Factual Accuracy (1-5)
  - Completeness (1-5)
  - Actionability (1-5)

Usage:
    python evaluation/evaluate_drafter.py

Output:
    - Prints scores per scenario
    - Saves evaluation_results.json
    - Prints summary table at end
"""

import json
import os
import sys
import time
from datetime import datetime
from pathlib import Path

try:
    import anthropic
except ImportError:
    print("ERROR: anthropic package not found. Run: pip install anthropic")
    sys.exit(1)

# ── Config ────────────────────────────────────────────────────────────────────
ROOT = Path(__file__).parent.parent
EVAL_FILE = ROOT / "evaluation" / "eval_scenarios.json"
OUTPUT_FILE = ROOT / "evaluation" / "evaluation_results.json"

API_KEY = os.environ.get("ANTHROPIC_API_KEY")
if not API_KEY:
    print("ERROR: ANTHROPIC_API_KEY not set in environment")
    sys.exit(1)

client = anthropic.Anthropic(api_key=API_KEY)

# ── Judge Prompt ──────────────────────────────────────────────────────────────
JUDGE_SYSTEM = """You are an expert evaluator for shipping carrier claim emails.
You assess claim drafts on 4 dimensions. Always respond in valid JSON only — 
no preamble, no markdown, no explanation outside the JSON."""

JUDGE_PROMPT = """Evaluate this shipping claim email draft on 4 dimensions.

## Claim Context
- Carrier: {carrier}
- Ship Method: {ship_method}
- Failure Type: {failure_type}
- Delay Days: {delay_days}
- Claim Amount: ${claim_amount}
- Attempt Number: {attempt_number}
- Tone Requested: {tone_requested}

## Draft Subject
{subject}

## Draft Body
{body}

## Scoring Rubric

**Tone Appropriateness (1-5)**
1 = Aggressive/robotic/demanding ("hereby formally demand", "within 5 business days")
3 = Professional but generic
5 = Warm, accountable, carrier-specific, appropriate for attempt number

**Factual Accuracy (1-5)**  
1 = Wrong dates, amounts, or carrier names
3 = Mostly correct with minor gaps
5 = All facts precisely match the context provided

**Completeness (1-5)**
1 = Missing key fields (tracking ID, dates, amount)
3 = Core info present
5 = All required fields + policy reference + supporting evidence

**Actionability (1-5)**
1 = No clear ask or resolution request
3 = Vague resolution request
5 = Specific dollar amount stated + clear next step requested

Respond ONLY with this JSON (no other text):
{{
  "tone_appropriateness": <1-5>,
  "factual_accuracy": <1-5>,
  "completeness": <1-5>,
  "actionability": <1-5>,
  "overall": <average of above>,
  "reasoning": "<2-3 sentence explanation>",
  "pass": <true if overall >= 3.5, else false>,
  "failure_mode": "<describe if pass=false, else null>"
}}"""

# ── Load Scenarios ────────────────────────────────────────────────────────────
print("=" * 65)
print("🌸 BloomDirect — Claim Drafter Evaluation (LLM-as-Judge)")
print("=" * 65)
print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print()

with open(EVAL_FILE) as f:
    data = json.load(f)

scenarios = data["scenarios"]
print(f"Loaded {len(scenarios)} scenarios from {EVAL_FILE.name}")
print(f"Judge model: claude-sonnet-4-6")
print()

# ── Run Evaluation ────────────────────────────────────────────────────────────
results = []
total_scores = {"tone_appropriateness": 0, "factual_accuracy": 0,
                "completeness": 0, "actionability": 0, "overall": 0}
passed = 0
failed = 0

for i, scenario in enumerate(scenarios, 1):
    sid = scenario["id"]
    cat = scenario["category"]
    inp = scenario["input"]
    draft = scenario["generated_draft"]

    print(f"[{i:02d}/15] {sid} — {cat[:40]}")

    prompt = JUDGE_PROMPT.format(
        carrier=inp.get("carrier", "UPS"),
        ship_method=inp.get("ship_method", ""),
        failure_type=inp.get("failure_type", ""),
        delay_days=inp.get("delay_days", 0),
        claim_amount=inp.get("claim_amount", 0),
        attempt_number=inp.get("attempt_number", 1),
        tone_requested=inp.get("tone_requested", "professional"),
        subject=draft.get("subject", ""),
        body=draft.get("body", "")
    )

    try:
        response = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=512,
            system=JUDGE_SYSTEM,
            messages=[{"role": "user", "content": prompt}]
        )

        raw = response.content[0].text.strip()
        # Strip markdown fences if present
        raw = raw.replace("```json", "").replace("```", "").strip()
        scores = json.loads(raw)

        # Accumulate totals
        for dim in ["tone_appropriateness", "factual_accuracy", "completeness", "actionability", "overall"]:
            total_scores[dim] += scores.get(dim, 0)

        if scores.get("pass", False):
            passed += 1
            status = "✅"
        else:
            failed += 1
            status = "❌"

        print(f"       {status} Overall: {scores.get('overall', 0):.2f} | "
              f"Tone: {scores.get('tone_appropriateness')} | "
              f"Accuracy: {scores.get('factual_accuracy')} | "
              f"Complete: {scores.get('completeness')} | "
              f"Action: {scores.get('actionability')}")
        print(f"       💬 {scores.get('reasoning', '')[:100]}")

        results.append({
            "id": sid,
            "category": cat,
            "scores": scores,
            "input_summary": {
                "carrier": inp.get("carrier"),
                "failure_type": inp.get("failure_type"),
                "attempt_number": inp.get("attempt_number"),
                "tone_requested": inp.get("tone_requested")
            }
        })

    except Exception as e:
        print(f"       ⚠️  Error: {e}")
        results.append({"id": sid, "category": cat, "error": str(e)})
        failed += 1

    print()
    time.sleep(1)  # Rate limit buffer

# ── Summary ───────────────────────────────────────────────────────────────────
n = len(scenarios)
print("=" * 65)
print("EVALUATION SUMMARY")
print("=" * 65)
print(f"Total Scenarios  : {n}")
print(f"Passed (≥3.5)    : {passed}")
print(f"Failed (<3.5)    : {failed}")
print(f"Pass Rate        : {passed/n*100:.0f}%")
print()
print("Dimension Averages:")
print(f"  Tone Appropriateness : {total_scores['tone_appropriateness']/n:.2f} / 5.0")
print(f"  Factual Accuracy     : {total_scores['factual_accuracy']/n:.2f} / 5.0")
print(f"  Completeness         : {total_scores['completeness']/n:.2f} / 5.0")
print(f"  Actionability        : {total_scores['actionability']/n:.2f} / 5.0")
print(f"  ─────────────────────────────────")
print(f"  Overall Average      : {total_scores['overall']/n:.2f} / 5.0")
print()

# Category breakdown
categories = {}
for r in results:
    cat = r.get("category", "Unknown")
    if cat not in categories:
        categories[cat] = []
    if "scores" in r:
        categories[cat].append(r["scores"].get("overall", 0))

print("Category Breakdown:")
for cat, scores_list in categories.items():
    avg = sum(scores_list) / len(scores_list) if scores_list else 0
    print(f"  {cat[:40]:<40} : {avg:.2f}")

print()

# ── Save Results ──────────────────────────────────────────────────────────────
output = {
    "run_timestamp": datetime.now().isoformat(),
    "model_judge": "claude-sonnet-4-6",
    "total_scenarios": n,
    "passed": passed,
    "failed": failed,
    "pass_rate": f"{passed/n*100:.0f}%",
    "dimension_averages": {
        dim: round(total_scores[dim] / n, 2)
        for dim in ["tone_appropriateness", "factual_accuracy",
                    "completeness", "actionability", "overall"]
    },
    "results": results
}

with open(OUTPUT_FILE, "w") as f:
    json.dump(output, f, indent=2)

print(f"Results saved to: {OUTPUT_FILE}")
print(f"Completed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print("=" * 65)
