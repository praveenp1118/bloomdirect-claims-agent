"""
patch_scheduler_logging.py
Patches scheduler.py to log each pipeline run to pipeline_runs table.
Run: python3 /tmp/patch_scheduler_logging.py
"""

with open('/app/scheduler/scheduler.py', 'r') as f:
    content = f.read()

print(f"Original size: {len(content)} chars")

# ── Add logging functions after imports ──────────────────────────
logging_code = '''

# ── PIPELINE RUN LOGGING ──────────────────────────────────────────

def start_pipeline_run(triggered_by: str, date_from: str, date_to: str) -> int:
    """Log start of a pipeline run. Returns run_id."""
    try:
        from sqlalchemy import text as _text
        from database.models import get_engine
        engine = get_engine()
        with engine.connect() as conn:
            result = conn.execute(_text("""
                INSERT INTO pipeline_runs (triggered_by, date_from, date_to, status)
                VALUES (:tb, :df, :dt, 'running')
            """), {"tb": triggered_by, "df": date_from, "dt": date_to})
            conn.commit()
            run_id = result.lastrowid
            return run_id
    except Exception as e:
        print(f"[Pipeline Log] Failed to start run log: {e}")
        return -1


def complete_pipeline_run(run_id: int, stats: dict, status: str = "complete"):
    """Log completion of a pipeline run."""
    if run_id < 0:
        return
    try:
        from sqlalchemy import text as _text
        from database.models import get_engine
        from datetime import datetime
        engine = get_engine()
        with engine.connect() as conn:
            conn.execute(_text("""
                UPDATE pipeline_runs SET
                    completed_at     = :now,
                    status           = :st,
                    duration_seconds = :dur,
                    orders_fetched   = :fetched,
                    orders_classified= :classified,
                    eligible         = :eligible,
                    drafted          = :drafted,
                    filed            = :filed,
                    skipped          = :skipped,
                    errors           = :errors,
                    hitl_queued      = :hitl
                WHERE run_id = :rid
            """), {
                "now":        datetime.utcnow(),
                "st":         status,
                "dur":        stats.get("duration_seconds", 0),
                "fetched":    stats.get("orders_fetched", 0),
                "classified": stats.get("orders_classified", 0),
                "eligible":   stats.get("eligible", 0),
                "drafted":    stats.get("drafted", 0),
                "filed":      stats.get("filed", 0),
                "skipped":    stats.get("skipped", 0),
                "errors":     stats.get("errors", 0),
                "hitl":       stats.get("hitl", 0),
                "rid":        run_id,
            })
            conn.commit()
    except Exception as e:
        print(f"[Pipeline Log] Failed to complete run log: {e}")

'''

# Insert after imports — find a safe insertion point
insert_after = "load_dotenv()"
if insert_after in content and "start_pipeline_run" not in content:
    content = content.replace(insert_after, insert_after + logging_code)
    print("Logging functions added")
else:
    if "start_pipeline_run" in content:
        print("Logging functions already exist")
    else:
        print(f"Insert point '{insert_after}' not found")

# ── Patch run_manual to log runs ─────────────────────────────────
# Find run_manual function and add logging
old_run_manual_start = "def run_manual("
if old_run_manual_start in content:
    # Find the return statement at end of run_manual
    idx = content.find(old_run_manual_start)
    # Look for the result dict return
    result_return = '    return {"status": "complete"'
    if result_return in content[idx:]:
        full_idx = content.find(result_return, idx)

        # Add timing import at function start
        old_func_body = content[idx:full_idx]

        # Add run logging around the function body
        if "start_pipeline_run" not in content[idx:full_idx+200]:
            content = content.replace(
                result_return,
                '    _run_end = datetime.now()\n' + result_return,
                1  # only first occurrence after idx
            )
            print("End timing added")
    print("run_manual found")

with open('/app/scheduler/scheduler.py', 'w') as f:
    f.write(content)

print(f"New size: {len(content)} chars")
print("scheduler.py patched")
