"""
patch_models_pipeline.py
Adds pipeline_runs table to the BloomDirect database.
Run: PYTHONPATH=/app DATABASE_URL=sqlite:////app/data/bloomdirect.db python3 /tmp/patch_models_pipeline.py
"""

import os, sys
os.environ.setdefault("DATABASE_URL", "sqlite:////app/data/bloomdirect.db")
sys.path.insert(0, '/app')

from sqlalchemy import create_engine, text

engine = create_engine(os.environ["DATABASE_URL"])

with engine.connect() as conn:
    # Check if table already exists
    result = conn.execute(text(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='pipeline_runs'"
    )).fetchone()

    if result:
        print("pipeline_runs table already exists")
    else:
        conn.execute(text("""
            CREATE TABLE pipeline_runs (
                run_id          INTEGER PRIMARY KEY AUTOINCREMENT,
                started_at      DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                completed_at    DATETIME,
                triggered_by    VARCHAR(50),   -- manual / scheduler_daily / scheduler_weekly / dashboard
                date_from       VARCHAR(10),
                date_to         VARCHAR(10),
                status          VARCHAR(20) DEFAULT 'running',  -- running / complete / failed
                duration_seconds INTEGER,
                orders_fetched  INTEGER DEFAULT 0,
                orders_classified INTEGER DEFAULT 0,
                eligible        INTEGER DEFAULT 0,
                drafted         INTEGER DEFAULT 0,
                filed           INTEGER DEFAULT 0,
                skipped         INTEGER DEFAULT 0,
                errors          INTEGER DEFAULT 0,
                hitl_queued     INTEGER DEFAULT 0,
                notes           TEXT
            )
        """))
        conn.commit()
        print("pipeline_runs table created successfully")

# Verify
with engine.connect() as conn:
    count = conn.execute(text("SELECT COUNT(*) FROM pipeline_runs")).scalar()
    print(f"Current pipeline_runs rows: {count}")
