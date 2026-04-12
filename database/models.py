"""
models.py - SQLAlchemy Database Schema
All tables for BloomDirect Claims Recovery System.
SQLite for local dev. MySQL-compatible for production.
"""

from datetime import datetime
from sqlalchemy import (
    create_engine, Column, String, Integer, Float,
    DateTime, Boolean, Text, ForeignKey
)
from sqlalchemy.orm import declarative_base, relationship, Session
from sqlalchemy.sql import func
import os
from dotenv import load_dotenv

load_dotenv()

Base = declarative_base()
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///bloomdirect.db")


# ── TABLE 1: ORDERS ──────────────────────────────────────────────
class Order(Base):
    __tablename__ = "orders"

    partner_order_id = Column(String(50), primary_key=True)
    tracking_id      = Column(String(50), nullable=False, index=True)
    ship_method      = Column(String(30), nullable=False)
    ship_date        = Column(String(10), nullable=False)
    carrier          = Column(String(10), nullable=False)
    occasion_type    = Column(String(20), default="General")
    created_at       = Column(DateTime, default=func.now())

    failures = relationship("Failure", back_populates="order")


# ── TABLE 2: TRACKING CACHE ───────────────────────────────────────
class TrackingCache(Base):
    __tablename__ = "tracking_cache"

    tracking_id        = Column(String(50), primary_key=True)
    carrier            = Column(String(10), nullable=False)
    cached_status      = Column(String(200))
    cached_status_date = Column(String(20))
    full_history_json  = Column(Text)
    last_mcp_call      = Column(DateTime)
    source             = Column(String(20), default="order_api")
    updated_at         = Column(DateTime, default=func.now(), onupdate=func.now())


# ── TABLE 3: FAILURES ─────────────────────────────────────────────
class Failure(Base):
    __tablename__ = "failures"

    failure_id       = Column(Integer, primary_key=True, autoincrement=True)
    partner_order_id = Column(String(50), ForeignKey("orders.partner_order_id"), nullable=False)
    tracking_id      = Column(String(50), nullable=False, index=True)
    failure_type     = Column(String(30), nullable=False)
    delay_days       = Column(Integer, default=0)
    first_bad_event  = Column(Text)
    severity         = Column(String(10), default="medium")
    ship_date        = Column(String(10))
    promised_date    = Column(String(10))
    detected_at      = Column(DateTime, default=func.now())

    order  = relationship("Order", back_populates="failures")
    claims = relationship("Claim", back_populates="failure")


# ── TABLE 4: CLAIMS ───────────────────────────────────────────────
class Claim(Base):
    __tablename__ = "claims"

    claim_id         = Column(Integer, primary_key=True, autoincrement=True)
    failure_id       = Column(Integer, ForeignKey("failures.failure_id"), nullable=False)
    tracking_id      = Column(String(50), nullable=False, index=True)
    carrier          = Column(String(10), nullable=False)
    ship_method      = Column(String(30))
    claim_type       = Column(String(20))
    claim_amount     = Column(Float, default=100.0)
    status           = Column(String(20), default="pending")
    attempt_number   = Column(Integer, default=1)
    probability      = Column(Float, default=0.0)
    gmail_thread_id  = Column(String(100))
    carrier_case_id  = Column(String(100))
    draft_email_text = Column(Text)
    short_label      = Column(String(200))
    llm_narrative    = Column(Text)
    human_comment    = Column(Text)
    occasion_type    = Column(String(20), default="General")
    filed            = Column(Boolean, default=False)
    filed_at         = Column(DateTime)
    fedex_batch_id   = Column(String(30))
    created_at       = Column(DateTime, default=func.now())
    updated_at       = Column(DateTime, default=func.now(), onupdate=func.now())

    failure    = relationship("Failure", back_populates="claims")
    email_logs = relationship("ClaimEmailLog", back_populates="claim")
    recovery   = relationship("Recovery", back_populates="claim", uselist=False)
    hitl       = relationship("HitlQueue", back_populates="claim", uselist=False)


# ── TABLE 5: CLAIMS EMAIL LOG ─────────────────────────────────────
class ClaimEmailLog(Base):
    __tablename__ = "claims_email_log"

    log_id           = Column(Integer, primary_key=True, autoincrement=True)
    claim_id         = Column(Integer, ForeignKey("claims.claim_id"), nullable=False)
    tracking_id      = Column(String(50), index=True)
    direction        = Column(String(10), nullable=False)
    timestamp        = Column(DateTime, default=func.now())
    subject          = Column(String(300))
    body             = Column(Text)
    status           = Column(String(20))
    rejection_reason = Column(Text)
    recovered_amount = Column(Float)

    claim = relationship("Claim", back_populates="email_logs")


# ── TABLE 6: RECOVERY ─────────────────────────────────────────────
class Recovery(Base):
    __tablename__ = "recovery"

    recovery_id      = Column(Integer, primary_key=True, autoincrement=True)
    claim_id         = Column(Integer, ForeignKey("claims.claim_id"), nullable=False)
    recovered_amount = Column(Float, nullable=False)
    credit_date      = Column(String(10))
    method           = Column(String(20), default="credit")
    created_at       = Column(DateTime, default=func.now())

    claim = relationship("Claim", back_populates="recovery")


# ── TABLE 7: HITL QUEUE ───────────────────────────────────────────
class HitlQueue(Base):
    __tablename__ = "hitl_queue"

    queue_id      = Column(Integer, primary_key=True, autoincrement=True)
    claim_id      = Column(Integer, ForeignKey("claims.claim_id"), nullable=False)
    tracking_id   = Column(String(50), index=True)
    reason        = Column(String(100), nullable=False)
    status        = Column(String(20), default="pending")
    human_comment = Column(Text)
    days_remaining= Column(Integer, default=0)
    created_at    = Column(DateTime, default=func.now())
    resolved_at   = Column(DateTime)
    resolved_by   = Column(String(50))

    claim = relationship("Claim", back_populates="hitl")


# ── TABLE 8: ERROR LOG ────────────────────────────────────────────
class ErrorLog(Base):
    __tablename__ = "error_log"

    error_id    = Column(Integer, primary_key=True, autoincrement=True)
    tracking_id = Column(String(50), index=True)
    error_type  = Column(String(50), nullable=False)
    stage       = Column(String(50))
    details     = Column(Text)
    resolved    = Column(Boolean, default=False)
    created_at  = Column(DateTime, default=func.now())
    resolved_at = Column(DateTime)


# ── TABLE 9: SCHEDULER STATE ──────────────────────────────────────
class SchedulerState(Base):
    __tablename__ = "scheduler_state"

    id                = Column(Integer, primary_key=True, autoincrement=True)
    run_type          = Column(String(20), nullable=False)
    start_date        = Column(String(10))
    end_date          = Column(String(10))
    status            = Column(String(20), default="pending")
    records_processed = Column(Integer, default=0)
    completed_at      = Column(DateTime)
    created_at        = Column(DateTime, default=func.now())


# ── TABLE 10: FEDEX BATCHES ────────────────────────────────────────
class FedExBatch(Base):
    __tablename__ = "fedex_batches"

    batch_id     = Column(String(30), primary_key=True)
    created_at   = Column(DateTime, default=func.now())
    claim_count  = Column(Integer, default=0)
    status       = Column(String(20), default="ready")    # ready / filed / discarded
    fedex_ref_id = Column(String(100))
    notes        = Column(Text)


# ── DB UTILITIES ──────────────────────────────────────────────────
def get_engine():
    """Create database engine."""
    return create_engine(DATABASE_URL, echo=False)


def init_db():
    """Create all tables if they don't exist."""
    engine = get_engine()
    Base.metadata.create_all(engine)
    print(f"Database initialized: {DATABASE_URL}")
    return engine


def get_session():
    """Get a database session."""
    engine = get_engine()
    return Session(engine)


if __name__ == "__main__":
    init_db()
    print("All tables created successfully.")
