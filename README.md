# 🌸 BloomDirect — Automated Shipping Claims Recovery System

> ISB AMPBA Batch 24 · CT2 Group Assignment · Group 8  
> Praveen Prakash · Sanskar Jain · Siddharth Kolli · Suparna Dhumale

---

## What This Does

BloomDirect automatically detects, classifies, drafts, files, and follows up on shipping claims for a perishable floral e-commerce business. Carriers (UPS via Shippo, FedEx direct) offer service guarantees but reject ~99% of first-attempt claims. This system files every eligible failure and persistently resubmits until approved or the filing window closes.

**~98% deterministic** — LLM tokens only used for email drafting (~60 claims/week).

---

## Architecture

```
Order API → Failure Classifier → Eligibility Assessor
                                        ↓
                              Claim Drafter (LLM)
                                        ↓
                         Email MCP → UPS / FedEx
                                        ↓
                         Gmail Poll → Rejection?
                                        ↓
                    Prob > 0.6 → Auto Resubmit (firm tone)
                    Prob 0.3–0.6 → Auto Resubmit (balanced)
                    Prob < 0.3 → HITL Queue → Human Review
```

**4 Agents:** Failure Classifier · Eligibility Assessor · Claim Drafter · Follow-Up & Escalation  
**2 MCP Servers:** Carrier Tracking · Gmail Claims  
**Framework:** LangGraph  
**Dashboard:** Streamlit

---

## Quick Start (Local)

```bash
# 1. Clone
git clone https://github.com/praveenp1118/bloomdirect-claims-agent.git
cd bloomdirect-claims-agent

# 2. Virtual environment
python -m venv venv
venv\Scripts\activate        # Windows
source venv/bin/activate     # Mac/Linux

# 3. Install dependencies
pip install -r requirements.txt

# 4. Configure environment
cp .env.example .env
# Edit .env — add your API keys

# 5. Initialise database
python -c "from database.models import init_db; init_db()"

# 6. Generate synthetic data (for testing)
python data/generate_synthetic_data.py

# 7. Run dashboard
streamlit run dashboard/app.py
```

Open `http://localhost:8501`

---

## Project Structure

```
bloomdirect-claims-agent/
├── agents/
│   ├── claim_drafter.py          # LLM email drafting (Claude Sonnet)
│   └── followup_escalation.py   # Rejection analysis + resubmission
├── config/
│   └── system_config.json        # All thresholds, configurable via UI
├── dashboard/
│   └── app.py                    # Streamlit dashboard (4 tabs)
├── data/
│   ├── generate_synthetic_data.py
│   └── sample_shipments.csv
├── database/
│   └── models.py                 # SQLAlchemy models (SQLite / MySQL)
├── guardrails/
│   ├── input_validator.py
│   └── output_validator.py
├── mcp_servers/
│   ├── carrier_tracking_mcp.py   # UPS (Shippo) + FedEx tracking
│   └── email_claims_mcp.py       # Gmail send + response polling
├── orchestrator/
│   └── pipeline.py               # LangGraph pipeline
├── prompts/
│   ├── claim_drafter.md
│   └── followup_escalation.md
├── scheduler/
│   └── scheduler.py              # APScheduler (daily + hourly)
├── evaluation/
│   └── evaluate_drafter.py       # LLM-as-a-Judge eval (15 scenarios)
├── .env.example
├── .gitignore
├── docker-compose.yml
├── Dockerfile
├── requirements.txt
└── README.md
```

---

## Running the Pipeline

### From Dashboard
Click **▶ Run Pipeline** — runs the last 14 days of orders.

### From CLI

```bash
# Manual run (last 14 days)
python scheduler/scheduler.py --manual

# Manual run (custom date range)
python scheduler/scheduler.py --manual 2026-03-01 2026-03-15

# Force daily run now
python scheduler/scheduler.py --daily

# Force Gmail poll now
python scheduler/scheduler.py --hourly

# Run smoke tests
python scheduler/scheduler.py --test

# Start background scheduler daemon
python scheduler/scheduler.py
```

---

## Scheduler

| Job | Schedule | What it does |
|-----|----------|-------------|
| Daily pipeline | Midnight PST (Mon–Sat) | Fetches orders for last 14 days, runs MCP, files eligible claims |
| Hourly Gmail poll | Every hour | Checks for carrier replies, processes approvals/rejections |

---

## Key Business Rules

| Rule | Value | Configurable |
|------|-------|-------------|
| Filing window | 15 days from ship date | ✅ Settings tab |
| Auto-file if window ≤ N days | 2 days | ✅ Settings tab |
| Auto-resubmit threshold | Prob ≥ 60% | ✅ Settings tab |
| Human review threshold | Prob 30–60% | ✅ Settings tab |
| Stop pursuing | Prob < 30% | ✅ Settings tab |
| Claim amount (fixed) | $100 per shipment | ✅ Settings tab |
| Max retry attempts | 3 | ✅ Settings tab |

All rules configurable from the Streamlit Settings tab — no code changes needed.

---

## HITL Triggers

1. FedEx portal claims (technical necessity — no API)
2. Borderline/high-value claims (prob 30–60%)
3. Unknown failure pattern (no 5-year history match)
4. Probability drops below 30% after rejection

---

## Docker Deployment

```bash
# Build and run
docker-compose up --build -d

# View logs
docker-compose logs -f dashboard
docker-compose logs -f scheduler

# Stop
docker-compose down
```

---

## AWS Deployment

See `docs/aws_deployment.md` for full EC2 + RDS setup guide.

Quick version:
```bash
# EC2: Amazon Linux 2, t3.small minimum
# Install Docker + Docker Compose
# Clone repo, add .env, run docker-compose up -d
# Open port 8501 in security group
```

---

## Evaluation

```bash
# Run LLM-as-a-Judge evaluation on Claim Drafter (15 scenarios)
python evaluation/evaluate_drafter.py
```

Target: run by Mar 31, 2026. Submit Apr 12, 2026.

---

## Environment Variables

Copy `.env.example` to `.env` and fill in:

| Variable | Description |
|----------|-------------|
| `RUN_MODE` | `synthetic` (dev) or `production` |
| `ORDER_API_KEY` | Arabella Bouquets Bearer token |
| `ANTHROPIC_API_KEY` | Claude API key |
| `GMAIL_*` | OAuth credentials for claims Gmail |
| `SHIPPO_API_KEY` | UPS tracking via Shippo |
| `FEDEX_*` | FedEx tracking credentials |

---

## Default Login

| Field | Value |
|-------|-------|
| Username | `Group_05` |
| Password | `BloomD@2026` |

Required only for HITL approvals and saving Settings.

---

## Group 8

| Name | Role |
|------|------|
| Praveen Prakash | Lead |
| Sanskar Jain | |
| Siddharth Kolli | |
| Suparna Dhumale | |

---

*Due: 29 March 2026 · ISB AMPBA Batch 24 · CT2 Group Assignment*
