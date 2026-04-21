#!/usr/bin/env python3
"""
Extraction pipeline: MongoDB events/clients/guestStateHistory + Mixpanel exports.
All raw data is cached to disk so subsequent runs skip network I/O.

Usage:
    python -m analysis.extract
"""

import pickle
from pathlib import Path

import pandas as pd

from src.config import (
    CACHE_DIR, A_START, A_END, B_START, B_END, B_LOOKBACK_START,
    MP_TZ_OFFSET_H,
)
from src.db import mongo_tunnel, mp_export
from src.utils import oid_from_dt, print_section


# ── Cache helpers ────────────────────────────────────────────────────────────

def _save(obj, name: str):
    path = CACHE_DIR / f"{name}.pkl"
    with open(path, "wb") as f:
        pickle.dump(obj, f, protocol=pickle.HIGHEST_PROTOCOL)
    print(f"  [cache] Saved {name} -> {path}")


def _load(name: str):
    path = CACHE_DIR / f"{name}.pkl"
    if path.exists():
        print(f"  [cache] Loading {name} from {path}")
        with open(path, "rb") as f:
            return pickle.load(f)
    return None


# ── Mixpanel date helpers ────────────────────────────────────────────────────

def _utc_to_mp_date(dt) -> str:
    """Convert UTC datetime to Mixpanel project-timezone date string.
    Mixpanel uses Europe/Moscow (UTC+3)."""
    from datetime import timedelta
    mp_dt = dt + timedelta(hours=MP_TZ_OFFSET_H)
    return mp_dt.strftime("%Y-%m-%d")


# ── MongoDB extraction ───────────────────────────────────────────────────────

def extract_events_problem_a(db) -> pd.DataFrame:
    """
    Extract per-event data for Problem A period from MongoDB events.
    Returns DataFrame with one row per event:
        guest_id, url, product_id, query_sk, created_ts
    """
    cached = _load("events_a")
    if cached is not None:
        return cached

    print_section("Extracting events for Problem A")
    oid_start = oid_from_dt(A_START)
    oid_end = oid_from_dt(A_END)

    pipeline = [
        {"$match": {"_id": {"$gte": oid_start, "$lte": oid_end}}},
        {"$project": {
            "guest_id": 1,
            "payload.url": 1,
            "payload.productId": 1,
            "payload.querySk": 1,
        }},
    ]
    print("  Running events aggregation (Problem A)...")
    cursor = db["events"].aggregate(pipeline, allowDiskUse=True, batchSize=50000)

    rows = []
    for i, doc in enumerate(cursor):
        p = doc.get("payload", {})
        rows.append({
            "guest_id": str(doc.get("guest_id", "")),
            "url": p.get("url", ""),
            "product_id": p.get("productId"),
            "query_sk": p.get("querySk", ""),
            "created_ts": doc["_id"].generation_time,
        })
        if (i + 1) % 500_000 == 0:
            print(f"    ... {i + 1:,} events processed")

    df = pd.DataFrame(rows)
    print(f"  Total events extracted: {len(df):,}")
    _save(df, "events_a")
    return df


def extract_events_problem_b(db) -> pd.DataFrame:
    """
    Extract per-event data for Problem B attribution window (72h before B_START to B_END).
    Same fields as Problem A.
    """
    cached = _load("events_b")
    if cached is not None:
        return cached

    print_section("Extracting events for Problem B (72h lookback)")
    oid_start = oid_from_dt(B_LOOKBACK_START)
    oid_end = oid_from_dt(B_END)

    pipeline = [
        {"$match": {"_id": {"$gte": oid_start, "$lte": oid_end}}},
        {"$project": {
            "guest_id": 1,
            "payload.url": 1,
            "payload.productId": 1,
            "payload.querySk": 1,
        }},
    ]
    print("  Running events aggregation (Problem B lookback)...")
    cursor = db["events"].aggregate(pipeline, allowDiskUse=True, batchSize=50000)

    rows = []
    for i, doc in enumerate(cursor):
        p = doc.get("payload", {})
        rows.append({
            "guest_id": str(doc.get("guest_id", "")),
            "url": p.get("url", ""),
            "product_id": p.get("productId"),
            "query_sk": p.get("querySk", ""),
            "created_ts": doc["_id"].generation_time,
        })
        if (i + 1) % 500_000 == 0:
            print(f"    ... {i + 1:,} events processed")

    df = pd.DataFrame(rows)
    print(f"  Total events extracted: {len(df):,}")
    _save(df, "events_b")
    return df


def extract_clients(db) -> pd.DataFrame:
    """Extract client enrichment: browser, country, version per guest_id."""
    cached = _load("clients")
    if cached is not None:
        return cached

    print_section("Extracting clients")
    pipeline = [
        {"$project": {
            "guest_id": 1,
            "browser": 1,
            "country": 1,
            "client_version": 1,
            "os": 1,
        }},
    ]
    cursor = db["clients"].aggregate(pipeline, allowDiskUse=True, batchSize=50000)

    rows = []
    for doc in cursor:
        rows.append({
            "guest_id": str(doc.get("guest_id", "")),
            "browser": doc.get("browser", ""),
            "country": str(doc.get("country", "")).upper(),
            "client_version": doc.get("client_version", ""),
            "os": doc.get("os", ""),
        })

    df = pd.DataFrame(rows)
    print(f"  Total client records: {len(df):,}")
    _save(df, "clients")
    return df


def extract_guest_state_history(db) -> pd.DataFrame:
    """
    Extract guestStateHistory for the widest analysis window.
    Each record = config snapshot delivered to client.
    """
    cached = _load("gsh")
    if cached is not None:
        return cached

    print_section("Extracting guestStateHistory")
    # Cover the widest window: from B_LOOKBACK_START to A_END
    widest_start = min(B_LOOKBACK_START, A_START)
    widest_end = max(A_END, B_END)
    oid_start = oid_from_dt(widest_start)
    oid_end = oid_from_dt(widest_end)

    pipeline = [
        {"$match": {"_id": {"$gte": oid_start, "$lte": oid_end}}},
        {"$project": {
            "guest_id": 1,
            "domain": 1,
            "value": 1,
        }},
    ]
    cursor = db["guestStateHistory"].aggregate(pipeline, allowDiskUse=True, batchSize=50000)

    rows = []
    for doc in cursor:
        rows.append({
            "guest_id": str(doc.get("guest_id", "")),
            "domain": doc.get("domain", ""),
            "value": doc.get("value"),
            "config_ts": doc["_id"].generation_time,
        })

    df = pd.DataFrame(rows)
    print(f"  Total GSH records: {len(df):,}")
    _save(df, "gsh")
    return df


# ── Mixpanel extraction ─────────────────────────────────────────────────────

def extract_mixpanel_affiliate_click() -> list[dict]:
    """Download Affiliate Click events for Problem A period."""
    from_d = _utc_to_mp_date(A_START)
    to_d = _utc_to_mp_date(A_END)
    return mp_export("Affiliate Click", from_d, to_d, "aff_click_a")


def extract_mixpanel_purchase() -> list[dict]:
    """Download Purchase events for Problem B period."""
    from_d = _utc_to_mp_date(B_START)
    to_d = _utc_to_mp_date(B_END)
    return mp_export("Purchase", from_d, to_d, "purchase_b")


def extract_mixpanel_purchase_completed() -> list[dict]:
    """Download Purchase Completed events for Problem B period."""
    from_d = _utc_to_mp_date(B_START)
    to_d = _utc_to_mp_date(B_END)
    return mp_export("Purchase Completed", from_d, to_d, "pc_b")


# ── Main ─────────────────────────────────────────────────────────────────────

def run():
    """Run full extraction pipeline."""
    print_section("Starting extraction pipeline")

    # Mixpanel (no tunnel needed)
    print("\n── Mixpanel exports ──")
    ac_raw = extract_mixpanel_affiliate_click()
    print(f"  Affiliate Click: {len(ac_raw):,} records")

    p_raw = extract_mixpanel_purchase()
    print(f"  Purchase: {len(p_raw):,} records")

    pc_raw = extract_mixpanel_purchase_completed()
    print(f"  Purchase Completed: {len(pc_raw):,} records")

    # MongoDB
    print("\n── MongoDB exports ──")
    with mongo_tunnel() as db:
        events_a = extract_events_problem_a(db)
        events_b = extract_events_problem_b(db)
        clients = extract_clients(db)
        gsh = extract_guest_state_history(db)

    print_section("Extraction complete")
    print(f"  Events A:       {len(events_a):,}")
    print(f"  Events B:       {len(events_b):,}")
    print(f"  Clients:        {len(clients):,}")
    print(f"  GSH:            {len(gsh):,}")
    print(f"  Affiliate Click:{len(ac_raw):,}")
    print(f"  Purchase:       {len(p_raw):,}")
    print(f"  Purchase Compl: {len(pc_raw):,}")


if __name__ == "__main__":
    run()
