#!/usr/bin/env python3
"""
Extraction pipeline: MongoDB events/clients/guestStateHistory + Mixpanel exports.

All raw data is cached to disk (namespaced by REPORT_MODE + window) so repeat
runs skip network I/O. Coverage of new instrumentation fields
(events.params, clients.build_app, Purchase Completed client-side fields) is
reported at the end of extraction.

Usage:
    REPORT_MODE=oneoff|pulse|deep python -m analysis.extract
"""

import pickle
from datetime import timedelta
from pathlib import Path

import pandas as pd

from src.config import (
    CACHE_DIR, CACHE_SUFFIX, REPORT_MODE, PROBLEM_B_ENABLED,
    A_START, A_END, B_START, B_END, B_LOOKBACK_START,
    MP_TZ_OFFSET_H,
)
from src.db import mongo_tunnel, mp_export
from src.utils import (
    oid_from_dt, print_section, pct,
    classify_event, lineage_segment,
)


# ── Cache helpers ────────────────────────────────────────────────────────────

def _key(name: str) -> str:
    """Cache key namespaced by mode+window so pulse/deep/oneoff don't collide."""
    return f"{name}__{CACHE_SUFFIX}"


def _save(obj, name: str):
    path = CACHE_DIR / f"{_key(name)}.pkl"
    with open(path, "wb") as f:
        pickle.dump(obj, f, protocol=pickle.HIGHEST_PROTOCOL)
    print(f"  [cache] Saved {name} -> {path}")


def _load(name: str):
    path = CACHE_DIR / f"{_key(name)}.pkl"
    if path.exists():
        print(f"  [cache] Loading {name} from {path}")
        with open(path, "rb") as f:
            return pickle.load(f)
    return None


# ── Mixpanel date helpers ────────────────────────────────────────────────────

def _utc_to_mp_date(dt) -> str:
    """UTC datetime -> Mixpanel project-timezone (Europe/Moscow UTC+3) date."""
    mp_dt = dt + timedelta(hours=MP_TZ_OFFSET_H)
    return mp_dt.strftime("%Y-%m-%d")


# ── MongoDB extraction ───────────────────────────────────────────────────────

def _project_events():
    return {
        "guest_id": 1,
        "payload.url": 1,
        "payload.productId": 1,
        "payload.querySk": 1,
        "params": 1,
    }


def _event_row(doc) -> dict:
    p = doc.get("payload", {})
    params = doc.get("params")
    return {
        "guest_id": str(doc.get("guest_id", "")),
        "url": p.get("url", ""),
        "product_id": p.get("productId"),
        "query_sk": p.get("querySk", ""),
        "params": params if isinstance(params, dict) else None,
        "created_ts": doc["_id"].generation_time,
    }


def extract_events(db, start, end, cache_name: str, label: str) -> pd.DataFrame:
    cached = _load(cache_name)
    if cached is not None:
        return cached

    print_section(f"Extracting events ({label})")
    oid_start = oid_from_dt(start)
    oid_end = oid_from_dt(end)

    pipeline = [
        {"$match": {"_id": {"$gte": oid_start, "$lte": oid_end}}},
        {"$project": _project_events()},
    ]
    print(f"  Running events aggregation ({label})...")
    cursor = db["events"].aggregate(pipeline, allowDiskUse=True, batchSize=50000)

    rows = []
    for i, doc in enumerate(cursor):
        rows.append(_event_row(doc))
        if (i + 1) % 500_000 == 0:
            print(f"    ... {i + 1:,} events processed")

    df = pd.DataFrame(rows)
    print(f"  Total events extracted: {len(df):,}")
    _save(df, cache_name)
    return df


def extract_events_problem_a(db) -> pd.DataFrame:
    return extract_events(db, A_START, A_END, "events_a", "Problem A")


def extract_events_problem_b(db) -> pd.DataFrame:
    if not PROBLEM_B_ENABLED:
        return pd.DataFrame()
    return extract_events(db, B_LOOKBACK_START, B_END, "events_b",
                          "Problem B (72h lookback)")


CLIENTS_MASTER_PATH = CACHE_DIR / "clients.pkl"


def _client_row(doc) -> dict:
    return {
        "_id_hex": str(doc["_id"]),
        "guest_id": str(doc.get("guest_id", "")),
        "browser": doc.get("browser", ""),
        "country": str(doc.get("country", "")).upper(),
        "client_version": doc.get("client_version", ""),
        "os": doc.get("os", ""),
        "build_app": doc.get("build_app"),
        "city": doc.get("city", ""),
        "user_agent": doc.get("user_agent", ""),
    }


def extract_clients(db) -> pd.DataFrame:
    """Client enrichment: browser/country/version/build_app per guest_id.

    Slowly-changing, full-collection table. Cached once at
    `cache/clients.pkl` (not namespaced by report window) and extended
    incrementally on subsequent runs via `_id > max(_id)` — `_id` is an
    ObjectId, chronological by leading timestamp bytes, and the `_id`
    field is indexed in MongoDB so the incremental fetch is cheap.
    """
    print_section("Extracting clients (incremental)")

    master = None
    if CLIENTS_MASTER_PATH.exists():
        try:
            master = pd.read_pickle(CLIENTS_MASTER_PATH)
        except Exception as e:
            print(f"  [warn] failed to load master {CLIENTS_MASTER_PATH}: {e}")
            master = None
    # Migration: legacy master without `_id_hex` column can't be extended
    # incrementally — force a full rescan.
    if master is not None and (len(master) == 0 or "_id_hex" not in master.columns):
        print("  [migrate] legacy clients cache has no _id_hex — full rescan")
        master = None

    pipeline = [{"$project": {
        "guest_id": 1, "browser": 1, "country": 1,
        "client_version": 1, "os": 1, "build_app": 1,
        "city": 1, "user_agent": 1,
    }}]

    if master is not None and len(master):
        from bson import ObjectId
        max_hex = master["_id_hex"].max()
        print(f"  Master cache: {len(master):,} rows, max _id={max_hex}")
        pipeline.insert(0, {"$match": {"_id": {"$gt": ObjectId(max_hex)}}})
    else:
        print("  No master cache — full collection scan")

    # Sort by _id ASC so checkpoints are monotonic — on crash, the saved
    # master is a valid prefix and the next run resumes via `_id > max`.
    pipeline.append({"$sort": {"_id": 1}})

    cursor = db["clients"].aggregate(pipeline, allowDiskUse=True, batchSize=50000)
    new_rows = []
    CHECKPOINT_EVERY = 1_000_000

    def _flush(extra_rows, reason):
        if not extra_rows:
            return master
        new_df = pd.DataFrame(extra_rows)
        if master is not None and len(master):
            keep = new_df[~new_df["_id_hex"].isin(master["_id_hex"])]
            merged = pd.concat([master, keep], ignore_index=True)
        else:
            merged = new_df
        merged.to_pickle(CLIENTS_MASTER_PATH)
        print(f"  [checkpoint:{reason}] master -> {CLIENTS_MASTER_PATH} "
              f"({len(merged):,} rows)")
        return merged

    try:
        for i, doc in enumerate(cursor):
            new_rows.append(_client_row(doc))
            if (i + 1) % 500_000 == 0:
                print(f"    ... {i + 1:,} new clients processed")
            if (i + 1) % CHECKPOINT_EVERY == 0:
                master = _flush(new_rows, f"{i + 1:,}")
                new_rows = []
    except Exception as e:
        # Save partial progress before re-raising so the next run can
        # resume from `_id > max(saved._id_hex)`.
        print(f"  [error] cursor failed at {len(new_rows):,} buffered "
              f"+ {len(master) if master is not None else 0:,} master: {e}")
        _flush(new_rows, "crash")
        raise

    df = _flush(new_rows, "final") if new_rows else master
    if df is None:
        df = pd.DataFrame()
    print(f"  Master total: {len(df):,} rows")
    return df


def extract_guest_state_history(db) -> pd.DataFrame:
    cached = _load("gsh")
    if cached is not None:
        return cached

    print_section("Extracting guestStateHistory")
    widest_start = min(B_LOOKBACK_START, A_START) if B_LOOKBACK_START else A_START
    widest_end = max(A_END, B_END) if B_END else A_END
    oid_start = oid_from_dt(widest_start)
    oid_end = oid_from_dt(widest_end)

    pipeline = [
        {"$match": {"_id": {"$gte": oid_start, "$lte": oid_end}}},
        {"$project": {"guest_id": 1, "domain": 1, "value": 1}},
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
    from_d = _utc_to_mp_date(A_START)
    to_d = _utc_to_mp_date(A_END)
    return mp_export("Affiliate Click", from_d, to_d, _key("aff_click_a"))


def extract_mixpanel_purchase() -> list[dict]:
    if not PROBLEM_B_ENABLED:
        return []
    from_d = _utc_to_mp_date(B_START)
    to_d = _utc_to_mp_date(B_END)
    return mp_export("Purchase", from_d, to_d, _key("purchase_b"))


def extract_mixpanel_purchase_completed() -> list[dict]:
    if not PROBLEM_B_ENABLED:
        return []
    from_d = _utc_to_mp_date(B_START)
    to_d = _utc_to_mp_date(B_END)
    return mp_export("Purchase Completed", from_d, to_d, _key("pc_b"))


# ── Coverage reporting ──────────────────────────────────────────────────────

def _source_tier_counts(events_a: pd.DataFrame, sample_size: int = 200_000) -> dict:
    """
    Tally which source tier produced sk/af/utm across events.
    Tiers: params / querySk (sk only) / url_parse / none.
    Samples for large datasets — exact counts are unnecessary for coverage.
    """
    if len(events_a) == 0:
        return {}
    sample = events_a if len(events_a) <= sample_size else events_a.sample(
        sample_size, random_state=42)

    tally = {
        "sk":  {"params": 0, "querySk": 0, "url_parse": 0, "none": 0},
        "af":  {"params": 0, "url_parse": 0, "none": 0},
        "utm": {"params": 0, "url_parse": 0, "none": 0},
    }
    for _, row in sample.iterrows():
        ev = classify_event({
            "url": row.get("url", "") or "",
            "query_sk": row.get("query_sk", "") or "",
            "params": row.get("params"),
        })
        tally["sk"][ev["sk_source"]] = tally["sk"].get(ev["sk_source"], 0) + 1
        tally["af"][ev["af_source"]] = tally["af"].get(ev["af_source"], 0) + 1
        tally["utm"][ev["utm_source_tier"]] = tally["utm"].get(ev["utm_source_tier"], 0) + 1
    return {
        "sample_size": int(len(sample)),
        "total_events": int(len(events_a)),
        "by_kind": tally,
    }


def _lineage_split(clients: pd.DataFrame) -> dict:
    """Distribution of flow lineage across clients (build_app first, UA fallback)."""
    if len(clients) == 0:
        return {}
    counts = {"dogi": 0, "auto_redirect": 0,
              "edge_ambiguous_build": 0, "unknown_build": 0}
    for _, row in clients.iterrows():
        seg = lineage_segment(row.get("build_app"), row.get("browser", ""))
        counts[seg] = counts.get(seg, 0) + 1
    total = len(clients)
    return {
        "total_clients": int(total),
        "counts": counts,
        "pcts": {k: (100 * v / total if total else 0.0) for k, v in counts.items()},
    }


def report_coverage(events_a: pd.DataFrame, clients: pd.DataFrame,
                    pc_raw: list[dict]) -> dict:
    """Coverage snapshot for the report (section 2 of report_structure.md).

    `build_app` coverage and lineage split are scoped to **active** clients
    only — i.e. those whose `guest_id` appears in `events_a` for the window.
    Old inactive clients can't be backfilled with `build_app`, so including
    them in the denominator understates the true rollout progress.
    """
    print_section("Coverage snapshot")

    coverage = {}

    # events.params
    if len(events_a):
        has_params = events_a["params"].apply(
            lambda v: isinstance(v, dict) and len(v) > 0).sum()
        total = len(events_a)
        coverage["events_params_pct"] = 100 * has_params / total if total else 0.0
        coverage["events_params_count"] = int(has_params)
        coverage["events_total"] = int(total)
        print(f"  events.params: {has_params:,}/{total:,} "
              f"({pct(has_params, total)})")

    # Active clients = those that appear in events for this window. Use one
    # row per guest (last seen in clients), so multi-client guests don't
    # inflate the denominator.
    active_guests = set(events_a["guest_id"].dropna().astype(str).unique()) \
        if len(events_a) else set()
    if len(clients):
        cl_dedup = clients.drop_duplicates(subset="guest_id", keep="last")
        active_clients = cl_dedup[cl_dedup["guest_id"].astype(str).isin(active_guests)] \
            if active_guests else cl_dedup.iloc[0:0]
    else:
        active_clients = clients

    # clients.build_app — scoped to active clients in window
    if len(active_clients):
        has_build = active_clients["build_app"].apply(
            lambda v: isinstance(v, str) and v.strip() != "").sum()
        total = len(active_clients)
        coverage["build_app_pct"] = 100 * has_build / total if total else 0.0
        coverage["build_app_count"] = int(has_build)
        coverage["clients_total"] = int(total)
        coverage["clients_scope"] = "active_in_window"

        build_counts = (active_clients["build_app"]
                        .fillna("").astype(str).str.lower().value_counts())
        coverage["build_app_breakdown"] = build_counts.to_dict()

        print(f"  clients.build_app (active in window): "
              f"{has_build:,}/{total:,} ({pct(has_build, total)})")
        for b, c in build_counts.head(10).items():
            print(f"    {b or '<missing>':<20} {c:>10,}")

    # Purchase Completed new fields
    if pc_raw:
        pc_rows = [r.get("properties", {}) for r in pc_raw]
        total = len(pc_rows)
        with_any = sum(
            1 for p in pc_rows
            if any(p.get(k) is not None for k in
                   ("last_sk", "last_af", "last_utm_campaign", "is_CIS"))
        )
        coverage["pc_new_fields_pct"] = 100 * with_any / total if total else 0.0
        coverage["pc_new_fields_count"] = int(with_any)
        coverage["pc_total"] = int(total)
        print(f"  Purchase Completed new fields: {with_any:,}/{total:,} "
              f"({pct(with_any, total)})")

    # Attribution source tiers (sampled for large event corpora)
    print("\n  -- Attribution source tiers --")
    tiers = _source_tier_counts(events_a)
    if tiers:
        coverage["source_tiers"] = tiers
        for kind, dist in tiers["by_kind"].items():
            total_k = sum(dist.values()) or 1
            print(f"    {kind}: " + ", ".join(
                f"{k}={v:,} ({100*v/total_k:.1f}%)" for k, v in dist.items()))

    # Flow lineage split — same scope as build_app coverage
    print("\n  -- Flow lineage split (active in window) --")
    ls = _lineage_split(active_clients)
    if ls:
        ls["scope"] = "active_in_window"
        coverage["lineage_split"] = ls
        for seg, cnt in ls["counts"].items():
            print(f"    {seg:<22} {cnt:>10,} ({ls['pcts'][seg]:.1f}%)")

    _save(coverage, "coverage")
    return coverage


# ── Main ─────────────────────────────────────────────────────────────────────

def run():
    print_section(f"Starting extraction pipeline — mode={REPORT_MODE}")
    print(f"  Problem A window: {A_START} → {A_END}")
    if PROBLEM_B_ENABLED:
        print(f"  Problem B window: {B_START} → {B_END} "
              f"(lookback from {B_LOOKBACK_START})")
    else:
        print("  Problem B: SKIPPED (mode=pulse)")

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
        events_b = extract_events_problem_b(db) if PROBLEM_B_ENABLED else pd.DataFrame()
        clients = extract_clients(db)
        gsh = extract_guest_state_history(db)

    # Coverage
    report_coverage(events_a, clients, pc_raw)

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
