"""
AliHelper — Full Reproducible Investigation
Problem A: Missing Affiliate Click
Problem B: Purchase Completed without Purchase

Run: python3 analysis_full.py
Caches Mixpanel exports to ./cache/ so re-runs are fast.
"""

# ─────────────────────────────────────────────────────────────
# SECTION 0: Setup & Constants
# ─────────────────────────────────────────────────────────────
import os, json, time, re, pickle
from pathlib import Path
from datetime import datetime, timezone, timedelta
from urllib.parse import urlparse

import pandas as pd
import numpy as np
from dotenv import load_dotenv
import sshtunnel
import pymongo
from bson import ObjectId
import requests
from requests.auth import HTTPBasicAuth
from tabulate import tabulate

load_dotenv()

CACHE_DIR = Path("./cache")
CACHE_DIR.mkdir(exist_ok=True)

# MongoDB
SSH_HOST   = os.getenv("MONGO_SSH_HOST")
SSH_USER   = os.getenv("MONGO_SSH_USER")
DB_HOST    = os.getenv("MONGO_DB_HOST")
DB_PORT    = int(os.getenv("MONGO_DB_PORT", 27017))
LOCAL_PORT = int(os.getenv("MONGO_LOCAL_PORT", 27018))
DB_NAME    = os.getenv("MONGO_DB_NAME")
MONGO_USER = os.getenv("MONGO_USER")
MONGO_PASS = os.getenv("MONGO_PASSWORD")
AUTH_DB    = os.getenv("MONGO_AUTH_DB", "admin")

# Mixpanel
MP_ACCOUNT = os.getenv("MIXPANEL_SERVICE_ACCOUNT")
MP_SECRET  = os.getenv("MIXPANEL_SECRET")
MP_PROJECT = os.getenv("MIXPANEL_PROJECT_ID")

# Analysis windows (UTC)
A_START = datetime(2026, 3,  6,  0,  0,  0, tzinfo=timezone.utc)
A_END   = datetime(2026, 4,  2, 23, 59, 59, tzinfo=timezone.utc)
B_START = datetime(2026, 2, 27,  0,  0,  0, tzinfo=timezone.utc)
B_END   = datetime(2026, 3, 26, 23, 59, 59, tzinfo=timezone.utc)

# AliHelper-owned sk whitelist
OUR_SKS = {"_c36PoUEj", "_d6jWDbY", "_AnTGXs", "_olPBn9X", "_dVh6yw5"}

# CIS countries (commonly used in the product)
CIS_COUNTRIES = {"RU", "BY", "KZ", "UA", "UZ", "AZ", "AM", "GE", "KG", "MD", "TJ", "TM"}

# Auto-redirect browsers (Firefox/Edge) vs DOGI (Chrome-like)
AUTO_REDIRECT_BROWSERS = {"firefox", "edge"}


def oid_from_dt(dt: datetime) -> ObjectId:
    ts = int(dt.timestamp())
    return ObjectId(f"{ts:08x}0000000000000000")


def pct(num, denom, decimals=1):
    if denom == 0:
        return "N/A"
    return f"{100*num/denom:.{decimals}f}%"


def print_section(title):
    print(f"\n{'='*70}")
    print(f"  {title}")
    print(f"{'='*70}")


def browser_family(browser_str: str) -> str:
    """Normalize browser string to family."""
    if not browser_str:
        return "unknown"
    b = str(browser_str).lower()
    if "firefox" in b:
        return "firefox"
    if "edge" in b or "edg/" in b:
        return "edge"
    if "yandex" in b or "yabrowser" in b:
        return "yandex"
    if "opera" in b or "opr/" in b:
        return "opera"
    if "chrome" in b or "chromium" in b:
        return "chrome"
    if "safari" in b:
        return "safari"
    return "other"


def lineage(bf: str) -> str:
    if bf in AUTO_REDIRECT_BROWSERS:
        return "auto-redirect"
    return "dogi"


def is_cis(country: str) -> bool:
    return str(country).upper() in CIS_COUNTRIES


# ─────────────────────────────────────────────────────────────
# SECTION 1: Mixpanel data download (cached)
# ─────────────────────────────────────────────────────────────

def mp_export(event_name: str, from_date: str, to_date: str, cache_key: str) -> list[dict]:
    """Download Mixpanel event export, caching to disk."""
    cache_file = CACHE_DIR / f"{cache_key}.json"
    if cache_file.exists():
        print(f"  [cache] Loading {event_name} from {cache_file}")
        with open(cache_file) as f:
            return json.load(f)

    print(f"  [download] Exporting {event_name} {from_date}→{to_date} ...")
    export_url = "https://data-eu.mixpanel.com/api/2.0/export"
    resp = requests.get(
        export_url,
        auth=HTTPBasicAuth(MP_ACCOUNT, MP_SECRET),
        params={
            "project_id": MP_PROJECT,
            "from_date": from_date,
            "to_date": to_date,
            "event": json.dumps([event_name]),
        },
        timeout=300, stream=True
    )
    resp.raise_for_status()
    records = []
    for line in resp.iter_lines():
        if line:
            records.append(json.loads(line))

    with open(cache_file, "w") as f:
        json.dump(records, f)
    print(f"    → {len(records):,} records")
    return records


def download_mixpanel_data():
    print_section("Downloading Mixpanel data (cached)")

    # Problem A window in MSK dates (Mixpanel uses project timezone)
    # UTC+3, so 2026-03-06 00:00 UTC = 2026-03-06 03:00 MSK → use from_date=2026-03-06
    # 2026-04-02 23:59 UTC = 2026-04-03 02:59 MSK → use to_date=2026-04-03
    # But to stay conservative and match exactly, use to_date=2026-04-02 (last full MSK day that overlaps)
    # We'll filter by Unix timestamp after download anyway.

    aff_clicks = mp_export("Affiliate Click", "2026-03-06", "2026-04-03", "aff_click_a")
    purchases   = mp_export("Purchase",          "2026-02-27", "2026-03-27", "purchase_b")
    pc_events   = mp_export("Purchase Completed","2026-02-27", "2026-03-27", "pc_b")

    return aff_clicks, purchases, pc_events


def to_df(records: list[dict]) -> pd.DataFrame:
    """Flatten Mixpanel NDJSON records to DataFrame."""
    rows = []
    for r in records:
        props = r.get("properties", {})
        rows.append(props)
    return pd.DataFrame(rows)


# ─────────────────────────────────────────────────────────────
# SECTION 2: MongoDB aggregations for Problem A
# ─────────────────────────────────────────────────────────────

def run_mongo_problem_a(db) -> dict:
    """Run all MongoDB aggregations needed for Problem A. Results are cached to disk."""
    cache_file = CACHE_DIR / "mongo_problem_a.pkl"
    if cache_file.exists():
        print("  [cache] Loading MongoDB Problem A data from cache")
        with open(cache_file, "rb") as f:
            return pickle.load(f)

    events = db["events"]
    clients_col = db["clients"]
    gsh = db["guestStateHistory"]

    oid_start = oid_from_dt(A_START)
    oid_end   = oid_from_dt(A_END)
    # Look-back 14 days before window for config history
    oid_lookback = oid_from_dt(A_START - timedelta(days=14))

    # ── 2a. Per-user event summary ─────────────────────────────
    print("  [mongo] Aggregating events per user (Problem A window)...")
    t0 = time.time()
    pipeline_users = [
        {"$match": {"_id": {"$gte": oid_start, "$lte": oid_end}}},
        {"$group": {
            "_id": "$guest_id",
            "total_events": {"$sum": 1},
            "country": {"$last": "$country"},
            # Has product page
            "product_events": {"$sum": {
                "$cond": [{"$ne": ["$payload.productId", None]}, 1, 0]
            }},
            # Has our sk in querySk
            "our_sk_events": {"$sum": {
                "$cond": [{"$in": ["$payload.querySk", list(OUR_SKS)]}, 1, 0]
            }},
            # Any non-null querySk
            "any_sk_events": {"$sum": {
                "$cond": [{"$ne": ["$payload.querySk", None]}, 1, 0]
            }},
            # Foreign sk (non-null, not ours)
            "foreign_sk_events": {"$sum": {
                "$cond": [
                    {"$and": [
                        {"$ne": ["$payload.querySk", None]},
                        {"$not": {"$in": ["$payload.querySk", list(OUR_SKS)]}}
                    ]},
                    1, 0
                ]
            }},
            # First event time (for config lookback reference)
            "first_event_oid": {"$min": "$_id"},
        }},
    ]
    user_events = list(events.aggregate(pipeline_users, allowDiskUse=True))
    print(f"    → {len(user_events):,} distinct users in {time.time()-t0:.1f}s")

    # Now classify homepage visits — need URL path check
    # Run separately for homepage detection
    print("  [mongo] Counting homepage events per user...")
    t0 = time.time()
    pipeline_hp = [
        {"$match": {
            "_id": {"$gte": oid_start, "$lte": oid_end},
            "payload.productId": None,
            "payload.url": {"$regex": r"^https?://[^/]*aliexpress\.[^/]*(/(#.*)?)?$"}
        }},
        {"$group": {"_id": "$guest_id", "homepage_events": {"$sum": 1}}},
    ]
    homepage_data = {str(r["_id"]): r["homepage_events"]
                     for r in events.aggregate(pipeline_hp, allowDiskUse=True)}
    print(f"    → {len(homepage_data):,} users with homepage visits in {time.time()-t0:.1f}s")

    # ── 2b. Client enrichment via server-side aggregation ─────────
    # Use $lookup aggregation (runs entirely on MongoDB server).
    # Avoids transferring large $in lists over SSH tunnel.
    # Use expressive $lookup with pipeline + $limit:1 to avoid 100MB limit.
    # Only fetches one client doc per guest_id and only two fields.
    print("  [mongo] Client enrichment via pipeline $lookup (limit 1 per user)...")
    t0 = time.time()
    pipeline_clients = [
        {"$match": {"_id": {"$gte": oid_start, "$lte": oid_end}}},
        {"$group": {"_id": "$guest_id"}},
        {"$lookup": {
            "from": "clients",
            "let": {"gid": "$_id"},
            "pipeline": [
                {"$match": {"$expr": {"$eq": ["$guest_id", "$$gid"]}}},
                {"$project": {"browser": 1, "client_version": 1, "_id": 0}},
                {"$limit": 1},
            ],
            "as": "client_docs",
        }},
        {"$project": {
            "browser": {"$arrayElemAt": ["$client_docs.browser", 0]},
            "client_version": {"$arrayElemAt": ["$client_docs.client_version", 0]},
        }},
    ]
    client_map = {}
    for r in events.aggregate(pipeline_clients, allowDiskUse=True):
        gid = str(r["_id"])
        client_map[gid] = {
            "browser": r.get("browser") or "",
            "client_version": r.get("client_version") or "",
        }
    print(f"    → enriched {len(client_map):,} users in {time.time()-t0:.1f}s")

    # ── 2c. guestStateHistory — latest config per user ─────────
    print("  [mongo] Aggregating guestStateHistory (window + lookback)...")
    t0 = time.time()
    # In-window: latest config snapshot
    pipeline_gsh_in = [
        {"$match": {"_id": {"$gte": oid_start, "$lte": oid_end}}},
        {"$sort": {"_id": -1}},
        {"$group": {
            "_id": "$guest_id",
            "latest_value": {"$first": "$value"},
            "latest_domain": {"$first": "$domain"},
            "latest_region": {"$first": "$region"},
        }},
    ]
    gsh_in_window = {r["_id"]: r for r in gsh.aggregate(pipeline_gsh_in, allowDiskUse=True)}
    print(f"    → {len(gsh_in_window):,} users with config in window in {time.time()-t0:.1f}s")

    # Pre-window lookback for users not in window
    print("  [mongo] guestStateHistory lookback (14 days before window)...")
    t0 = time.time()
    pipeline_gsh_pre = [
        {"$match": {"_id": {"$gte": oid_lookback, "$lt": oid_start}}},
        {"$sort": {"_id": -1}},
        {"$group": {
            "_id": "$guest_id",
            "latest_value": {"$first": "$value"},
            "latest_domain": {"$first": "$domain"},
            "latest_region": {"$first": "$region"},
        }},
    ]
    gsh_pre_window = {r["_id"]: r for r in gsh.aggregate(pipeline_gsh_pre, allowDiskUse=True)}
    print(f"    → {len(gsh_pre_window):,} users with config in lookback in {time.time()-t0:.1f}s")

    result = {
        "user_events": user_events,
        "homepage_data": homepage_data,
        "client_map": client_map,
        "gsh_in_window": gsh_in_window,
        "gsh_pre_window": gsh_pre_window,
    }
    with open(CACHE_DIR / "mongo_problem_a.pkl", "wb") as f:
        pickle.dump(result, f)
    print("  [cache] MongoDB Problem A data saved to cache")
    return result


# ─────────────────────────────────────────────────────────────
# SECTION 3: Build Problem A dataframe
# ─────────────────────────────────────────────────────────────

def build_problem_a_df(mongo_data, aff_click_df: pd.DataFrame) -> pd.DataFrame:
    """Join all sources into per-user Problem A dataframe."""
    print_section("Building Problem A user-level dataframe")

    user_events   = mongo_data["user_events"]
    homepage_data = mongo_data["homepage_data"]
    client_map    = mongo_data["client_map"]
    gsh_in        = mongo_data["gsh_in_window"]
    gsh_pre       = mongo_data["gsh_pre_window"]

    # Filter Affiliate Click to Problem A UTC window
    ac = aff_click_df.copy()
    ac["time_utc"] = pd.to_datetime(ac["time"], unit="s", utc=True)
    ac_window = ac[
        (ac["time_utc"] >= A_START) & (ac["time_utc"] <= A_END)
    ]
    # Users who had Affiliate Click in window
    ac_users = set(ac_window["$user_id"].dropna().astype(str))
    print(f"  Affiliate Click users in window: {len(ac_users):,}")

    # Build rows
    rows = []
    for ue in user_events:
        guest_id = str(ue["_id"])

        # Eligible: product page OR homepage
        hp_count = homepage_data.get(guest_id, 0)
        prod_count = ue.get("product_events", 0)
        has_product = prod_count > 0
        has_homepage = hp_count > 0
        is_eligible = has_product or has_homepage

        # Client info
        cli = client_map.get(guest_id, {})
        raw_browser = cli.get("browser", "")
        bf = browser_family(raw_browser)
        lg = lineage(bf)
        cv = cli.get("client_version", "")

        # Config: prefer in-window, fall back to pre-window
        conf = gsh_in.get(guest_id) or gsh_pre.get(guest_id)
        if conf:
            cfg_value  = conf.get("latest_value", False)
            cfg_domain = conf.get("latest_domain", "")
            cfg_region = conf.get("latest_region", "")
        else:
            cfg_value  = None  # unknown
            cfg_domain = ""
            cfg_region = ""

        # Return to AliExpress with our sk
        has_our_sk_return = ue.get("our_sk_events", 0) > 0

        # Reached hub
        reached_hub = guest_id in ac_users

        country = ue.get("country", "") or ""

        rows.append({
            "guest_id": guest_id,
            "country": country.upper() if country else "",
            "is_cis": is_cis(country),
            "total_events": ue.get("total_events", 0),
            "product_events": prod_count,
            "homepage_events": hp_count,
            "has_product": has_product,
            "has_homepage": has_homepage,
            "is_eligible": is_eligible,
            "browser_raw": raw_browser,
            "browser_family": bf,
            "lineage": lg,
            "client_version": cv,
            "cfg_value": cfg_value,    # True/False/None
            "cfg_domain": cfg_domain,
            "cfg_region": cfg_region,
            "has_cfg": conf is not None,
            "has_usable_cfg": cfg_value is True,
            "reached_hub": reached_hub,
            "has_our_sk_return": has_our_sk_return,
            "has_any_sk_return": ue.get("any_sk_events", 0) > 0,
            "has_foreign_sk_return": ue.get("foreign_sk_events", 0) > 0,
        })

    df = pd.DataFrame(rows)

    # A5: Missing click tracking = has our sk return but NO Affiliate Click
    df["a5_missing_tracking"] = df["has_our_sk_return"] & ~df["reached_hub"]

    # A6: Reached hub but no our sk return
    df["a6_hub_no_return"] = df["reached_hub"] & ~df["has_our_sk_return"]

    print(f"  Total users: {len(df):,}")
    print(f"  Eligible users: {df['is_eligible'].sum():,}")
    print(f"  Reached hub: {df['reached_hub'].sum():,}")
    print(f"  Has our sk return: {df['has_our_sk_return'].sum():,}")

    return df


# ─────────────────────────────────────────────────────────────
# SECTION 4: Problem A — Funnel & Segmentation
# ─────────────────────────────────────────────────────────────

def analyze_problem_a(df: pd.DataFrame):
    print_section("PROBLEM A — Missing Affiliate Click Analysis")

    n_total        = len(df)
    n_eligible     = df["is_eligible"].sum()
    n_elig_product = (df["is_eligible"] & df["has_product"]).sum()
    n_elig_hp      = (df["is_eligible"] & df["has_homepage"] & ~df["has_product"]).sum()
    n_has_cfg      = (df["is_eligible"] & df["has_cfg"]).sum()
    n_usable_cfg   = (df["is_eligible"] & df["has_usable_cfg"]).sum()
    n_reached_hub  = (df["is_eligible"] & df["reached_hub"]).sum()
    n_our_sk_ret   = (df["is_eligible"] & df["has_our_sk_return"]).sum()
    n_a5           = (df["is_eligible"] & df["a5_missing_tracking"]).sum()
    n_a6           = (df["is_eligible"] & df["a6_hub_no_return"]).sum()

    funnel = [
        ["1. Raw AliExpress activity",              n_total,       pct(n_total, n_total)],
        ["2. Eligible (product or homepage)",        n_eligible,    pct(n_eligible, n_total)],
        ["  2a. Product page only/also",             n_elig_product,pct(n_elig_product, n_eligible)],
        ["  2b. Homepage only",                      n_elig_hp,     pct(n_elig_hp, n_eligible)],
        ["3. Has ANY config snapshot",               n_has_cfg,     pct(n_has_cfg, n_eligible)],
        ["4. Has USABLE config (value=True)",        n_usable_cfg,  pct(n_usable_cfg, n_eligible)],
        ["5. Reached hub (Affiliate Click)",         n_reached_hub, pct(n_reached_hub, n_eligible)],
        ["6. Returned with our sk",                  n_our_sk_ret,  pct(n_our_sk_ret, n_eligible)],
        ["A5. Our sk return, NO Affiliate Click",    n_a5,          pct(n_a5, n_eligible)],
        ["A6. Affiliate Click, NO our sk return",    n_a6,          pct(n_a6, n_eligible)],
    ]
    print("\n── A1. Funnel decomposition ──")
    print(tabulate(funnel, headers=["Stage", "Users", "% of eligible"], tablefmt="pipe"))

    # Gap analysis
    n_gap = n_eligible - n_reached_hub
    print(f"\n  Eligible users without Affiliate Click (gap): {n_gap:,} ({pct(n_gap, n_eligible)} of eligible)")
    print(f"  Of gap:")
    n_no_cfg     = (df["is_eligible"] & ~df["has_cfg"]).sum()
    n_bad_cfg    = (df["is_eligible"] & df["has_cfg"] & ~df["has_usable_cfg"]).sum()
    n_good_cfg_no_hub = (df["is_eligible"] & df["has_usable_cfg"] & ~df["reached_hub"]).sum()
    gap_table = [
        ["No config snapshot found",                n_no_cfg,          pct(n_no_cfg, n_gap)],
        ["Config found but value=False",            n_bad_cfg,         pct(n_bad_cfg, n_gap)],
        ["Usable config but never reached hub",     n_good_cfg_no_hub, pct(n_good_cfg_no_hub, n_gap)],
        ["A5: Silent redirect (sk return, no click)",n_a5,             pct(n_a5, n_gap)],
    ]
    print(tabulate(gap_table, headers=["Gap bucket", "Users", "% of gap"], tablefmt="pipe"))

    # ── A2. By browser lineage ─────────────────────────────────
    print("\n── A2. By browser lineage (eligible users) ──")
    elig = df[df["is_eligible"]].copy()
    lineage_stats = []
    for lg, g in elig.groupby("lineage"):
        n = len(g)
        lineage_stats.append([
            lg,
            n,
            pct(n, n_eligible),
            g["reached_hub"].sum(),
            pct(g["reached_hub"].sum(), n),
            g["has_our_sk_return"].sum(),
            pct(g["has_our_sk_return"].sum(), n),
            g["a5_missing_tracking"].sum(),
            g["a6_hub_no_return"].sum(),
        ])
    lineage_stats.sort(key=lambda x: -x[1])
    print(tabulate(lineage_stats,
        headers=["Lineage","Users","% eligible","Reached hub","% reach","Our sk return","% return","A5 silent","A6 hub-no-ret"],
        tablefmt="pipe"))

    # ── A3. By browser family ──────────────────────────────────
    print("\n── A3. By browser family (eligible users) ──")
    browser_stats = []
    for bf, g in elig.groupby("browser_family"):
        n = len(g)
        browser_stats.append([
            bf, n, pct(n, n_eligible),
            g["reached_hub"].sum(), pct(g["reached_hub"].sum(), n),
            g["has_our_sk_return"].sum(), pct(g["has_our_sk_return"].sum(), n),
        ])
    browser_stats.sort(key=lambda x: -x[1])
    print(tabulate(browser_stats,
        headers=["Browser","Users","% eligible","Reached hub","% reach","Our sk return","% return"],
        tablefmt="pipe"))

    # ── A4. Auto-redirect: Firefox vs Edge ────────────────────
    print("\n── A4. Auto-redirect: Firefox vs Edge (eligible) ──")
    auto = elig[elig["lineage"] == "auto-redirect"]
    ff_stats = []
    for bf, g in auto.groupby("browser_family"):
        n = len(g)
        ff_stats.append([
            bf, n,
            g["reached_hub"].sum(), pct(g["reached_hub"].sum(), n),
            g["has_our_sk_return"].sum(), pct(g["has_our_sk_return"].sum(), n),
        ])
    if ff_stats:
        print(tabulate(ff_stats,
            headers=["Browser","Users","Reached hub","% reach","Our sk return","% return"],
            tablefmt="pipe"))
    else:
        print("  No auto-redirect users found (Firefox/Edge).")

    # ── A5. By hub (cfg_domain, eligible with usable config) ───
    print("\n── A5. By hub (eligible users with usable config) ──")
    usable = elig[elig["has_usable_cfg"]].copy()
    hub_stats = []
    for domain, g in usable.groupby("cfg_domain"):
        n = len(g)
        hub_stats.append([
            domain, n,
            g["reached_hub"].sum(), pct(g["reached_hub"].sum(), n),
            g["has_our_sk_return"].sum(), pct(g["has_our_sk_return"].sum(), n),
        ])
    hub_stats.sort(key=lambda x: -x[1])
    print(tabulate(hub_stats,
        headers=["Hub domain","Users","Reached hub","% reach","Our sk return","% return"],
        tablefmt="pipe"))

    # ── A6. CIS vs Global ─────────────────────────────────────
    print("\n── A6. CIS vs Global (eligible) ──")
    for label, mask in [("CIS", elig["is_cis"]), ("Global", ~elig["is_cis"])]:
        g = elig[mask]
        n = len(g)
        print(f"  {label}: {n:,} users | "
              f"reached hub: {g['reached_hub'].sum():,} ({pct(g['reached_hub'].sum(), n)}) | "
              f"our sk return: {g['has_our_sk_return'].sum():,} ({pct(g['has_our_sk_return'].sum(), n)})")

    # ── A7. Top countries (eligible) ──────────────────────────
    print("\n── A7. Top countries (eligible, min 50 users) ──")
    country_stats = []
    for c, g in elig.groupby("country"):
        n = len(g)
        if n < 50:
            continue
        country_stats.append([
            c, n,
            g["reached_hub"].sum(), pct(g["reached_hub"].sum(), n),
            g["has_our_sk_return"].sum(), pct(g["has_our_sk_return"].sum(), n),
        ])
    country_stats.sort(key=lambda x: -x[1])
    print(tabulate(country_stats[:20],
        headers=["Country","Users","Reached hub","% reach","Our sk return","% return"],
        tablefmt="pipe"))

    # ── A8. Client version (top 15, eligible) ─────────────────
    print("\n── A8. Top client versions (eligible, min 30 users) ──")
    ver_stats = []
    for v, g in elig.groupby("client_version"):
        n = len(g)
        if n < 30 or not v:
            continue
        ver_stats.append([
            v, n,
            g["reached_hub"].sum(), pct(g["reached_hub"].sum(), n),
        ])
    ver_stats.sort(key=lambda x: -x[1])
    print(tabulate(ver_stats[:15],
        headers=["Version","Users","Reached hub","% reach"],
        tablefmt="pipe"))

    # ── A9. Multi-client users ─────────────────────────────────
    # Users with multiple client records were enriched only with the first found
    # We can approximate "multi-client" as users found in client_map vs not
    # (client_map deduplication is per guest_id, not multiple clients)
    print("\n── A9. Config coverage by lineage ──")
    for lg, g in elig.groupby("lineage"):
        print(f"  {lg}:")
        print(f"    Has config:         {g['has_cfg'].sum():,} / {len(g):,} ({pct(g['has_cfg'].sum(), len(g))})")
        print(f"    Has usable config:  {g['has_usable_cfg'].sum():,} / {len(g):,} ({pct(g['has_usable_cfg'].sum(), len(g))})")

    # ── A10. Page type breakdown ───────────────────────────────
    print("\n── A10. Eligible by page type ──")
    prod_only  = elig[elig["has_product"] & ~elig["has_homepage"]]
    hp_only    = elig[~elig["has_product"] & elig["has_homepage"]]
    both       = elig[elig["has_product"] & elig["has_homepage"]]
    for label, g in [("Product only", prod_only), ("Homepage only", hp_only), ("Both", both)]:
        n = len(g)
        if n == 0:
            continue
        print(f"  {label}: {n:,} | reached hub: {g['reached_hub'].sum():,} ({pct(g['reached_hub'].sum(), n)})")

    return df


# ─────────────────────────────────────────────────────────────
# SECTION 5: Problem B — Purchase Completed vs Purchase
# ─────────────────────────────────────────────────────────────

def build_problem_b_df(purchases: list[dict], pc_events: list[dict]) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Build Purchase and Purchase Completed DataFrames for Problem B window."""
    print_section("Building Problem B dataframes")

    pur_df = to_df(purchases)
    pc_df  = to_df(pc_events)

    # Filter to UTC window
    pur_df["time_utc"] = pd.to_datetime(pur_df["time"], unit="s", utc=True)
    pc_df["time_utc"]  = pd.to_datetime(pc_df["time"],  unit="s", utc=True)

    pur_b = pur_df[(pur_df["time_utc"] >= B_START) & (pur_df["time_utc"] <= B_END)].copy()
    pc_b  = pc_df[ (pc_df["time_utc"]  >= B_START) & (pc_df["time_utc"]  <= B_END)].copy()

    print(f"  Purchase Completed in window: {len(pc_b):,}")
    print(f"  Purchase in window:           {len(pur_b):,}")
    print(f"  Raw gap:                      {len(pc_b) - len(pur_b):,} "
          f"({pct(len(pc_b)-len(pur_b), len(pc_b))} of PC)")

    return pur_b, pc_b


def match_purchases(pc_b: pd.DataFrame, pur_b: pd.DataFrame,
                    window_minutes: int = 10) -> pd.DataFrame:
    """
    Match Purchase Completed → Purchase by $user_id + time proximity.
    Returns pc_b with 'matched' column.
    """
    print(f"\n  Matching PC → Purchase (±{window_minutes} min)...")

    # Build Purchase lookup: user_id → sorted list of timestamps
    pur_by_user = {}
    for _, row in pur_b.iterrows():
        uid = str(row.get("$user_id", "") or "")
        if not uid:
            continue
        ts = row["time_utc"]
        oid = str(row.get("order_id", "") or "")
        pur_by_user.setdefault(uid, []).append((ts, oid))

    window = timedelta(minutes=window_minutes)
    pc_b = pc_b.copy()
    pc_b["user_id"]  = pc_b["$user_id"].astype(str)
    pc_b["matched"]  = False
    pc_b["match_order_id"] = None

    for idx, row in pc_b.iterrows():
        uid = row["user_id"]
        pc_ts = row["time_utc"]
        candidates = pur_by_user.get(uid, [])
        for (pur_ts, oid) in candidates:
            if abs((pc_ts - pur_ts).total_seconds()) <= window.total_seconds():
                pc_b.at[idx, "matched"] = True
                pc_b.at[idx, "match_order_id"] = oid
                break

    n_matched   = pc_b["matched"].sum()
    n_unmatched = (~pc_b["matched"]).sum()
    print(f"  Matched:   {n_matched:,} ({pct(n_matched, len(pc_b))})")
    print(f"  Unmatched: {n_unmatched:,} ({pct(n_unmatched, len(pc_b))})")

    return pc_b


def assign_reason_codes(pc_b: pd.DataFrame) -> pd.DataFrame:
    """
    For unmatched Purchase Completed, assign primary reason code.
    Uses fields already present in the Purchase Completed event.
    """
    pc_b = pc_b.copy()

    # Normalize fields
    pc_b["last_sk"]   = pc_b.get("last_sk", pd.Series(dtype=str)).fillna("").astype(str)
    pc_b["last_af"]   = pc_b.get("last_af", pd.Series(dtype=str)).fillna("").astype(str)
    pc_b["sk"]        = pc_b.get("sk", pd.Series(dtype=str)).fillna("").astype(str)
    pc_b["af"]        = pc_b.get("af", pd.Series(dtype=str)).fillna("").astype(str)
    pc_b["cashback_list"] = pc_b.get("cashback_list", pd.Series(dtype=object))

    pc_b["has_our_last_sk"]     = pc_b["last_sk"].isin(OUR_SKS)
    pc_b["has_our_current_sk"]  = pc_b["sk"].isin(OUR_SKS)
    pc_b["has_any_our_sk"]      = pc_b["has_our_last_sk"] | pc_b["has_our_current_sk"]
    pc_b["has_foreign_last_sk"] = (pc_b["last_sk"] != "") & ~pc_b["has_our_last_sk"]
    pc_b["has_foreign_sk"]      = (pc_b["sk"] != "") & ~pc_b["has_our_current_sk"]
    pc_b["has_af"]              = pc_b["af"] != ""
    pc_b["has_last_af"]         = pc_b["last_af"] != ""
    pc_b["has_cashback"]        = pc_b["cashback_list"].notna() & (pc_b["cashback_list"] != "")

    def assign_code(row):
        if row["matched"]:
            return "MATCHED"
        # Priority order
        if not row["has_any_our_sk"]:
            return "NO_OUR_SK_IN_72H"
        if row["has_foreign_sk"] or row["has_foreign_last_sk"]:
            return "FOREIGN_SK_AFTER_OUR_SK"
        if row["has_af"] or row["has_last_af"]:
            return "AF_AFTER_OUR_SK"
        if row["has_cashback"]:
            return "CASHBACK_TRACE"
        return "UNKNOWN"

    pc_b["reason_code"] = pc_b.apply(assign_code, axis=1)
    return pc_b


def analyze_problem_b(pc_b: pd.DataFrame, pur_b: pd.DataFrame):
    print_section("PROBLEM B — Purchase Completed without Purchase Analysis")

    n_pc  = len(pc_b)
    n_pur = len(pur_b)
    n_gap = n_pc - n_pur

    print(f"\n  Purchase Completed: {n_pc:,}")
    print(f"  Purchase:           {n_pur:,}")
    print(f"  Raw event gap:      {n_gap:,} ({pct(n_gap, n_pc)} of PC)")

    unmatched = pc_b[~pc_b["matched"]]
    matched   = pc_b[pc_b["matched"]]
    n_unmatched = len(unmatched)
    n_matched   = len(matched)

    print(f"\n  After 10-min user+time matching:")
    print(f"    Matched PC→Purchase: {n_matched:,} ({pct(n_matched, n_pc)})")
    print(f"    Unmatched (gap):     {n_unmatched:,} ({pct(n_unmatched, n_pc)})")

    # ── B1. Reason code distribution ──────────────────────────
    print("\n── B1. Reason code distribution (unmatched PC) ──")
    rc_stats = []
    for code, g in unmatched.groupby("reason_code"):
        rc_stats.append([code, len(g), pct(len(g), n_unmatched), pct(len(g), n_pc)])
    rc_stats.sort(key=lambda x: -x[1])
    print(tabulate(rc_stats,
        headers=["Reason code", "PC events", "% of unmatched", "% of all PC"],
        tablefmt="pipe"))

    # ── B2. Attribution state breakdown ───────────────────────
    print("\n── B2. Attribution state in ALL unmatched PC ──")
    attr_table = [
        ["Has our last_sk (OUR_SKS)",      unmatched["has_our_last_sk"].sum(),     pct(unmatched["has_our_last_sk"].sum(), n_unmatched)],
        ["Has foreign last_sk",            unmatched["has_foreign_last_sk"].sum(), pct(unmatched["has_foreign_last_sk"].sum(), n_unmatched)],
        ["Has any our sk (last or current)",unmatched["has_any_our_sk"].sum(),     pct(unmatched["has_any_our_sk"].sum(), n_unmatched)],
        ["Has foreign sk (current)",       unmatched["has_foreign_sk"].sum(),      pct(unmatched["has_foreign_sk"].sum(), n_unmatched)],
        ["Has af",                         unmatched["has_af"].sum(),              pct(unmatched["has_af"].sum(), n_unmatched)],
        ["Has last_af",                    unmatched["has_last_af"].sum(),         pct(unmatched["has_last_af"].sum(), n_unmatched)],
        ["Has cashback trace",             unmatched["has_cashback"].sum(),        pct(unmatched["has_cashback"].sum(), n_unmatched)],
    ]
    print(tabulate(attr_table, headers=["Attribute", "Count", "% of unmatched"], tablefmt="pipe"))

    # ── B3. Matching sensitivity ───────────────────────────────
    print("\n── B3. Matching sensitivity (different windows) ──")
    for win_min in [5, 10, 20, 30, 60]:
        # Rebuild match counts quickly
        pur_by_user = {}
        for _, row in pur_b.iterrows():
            uid = str(row.get("$user_id", "") or "")
            if uid:
                pur_by_user.setdefault(uid, []).append(row["time_utc"])

        win = timedelta(minutes=win_min)
        n_match = 0
        for _, row in pc_b.iterrows():
            uid = row["user_id"]
            pc_ts = row["time_utc"]
            for pur_ts in pur_by_user.get(uid, []):
                if abs((pc_ts - pur_ts).total_seconds()) <= win.total_seconds():
                    n_match += 1
                    break
        print(f"  ±{win_min:2d} min: matched={n_match:,} ({pct(n_match, n_pc)})  unmatched={n_pc-n_match:,} ({pct(n_pc-n_match, n_pc)})")

    # ── B4. Segment: CIS vs Global ─────────────────────────────
    print("\n── B4. CIS vs Global (unmatched PC) ──")
    unmatched_c = unmatched.copy()
    unmatched_c["is_cis_flag"] = unmatched_c["mp_country_code"].apply(is_cis)
    for label, mask in [("CIS", unmatched_c["is_cis_flag"]), ("Global", ~unmatched_c["is_cis_flag"])]:
        g = unmatched_c[mask]
        n = len(g)
        top_codes = g["reason_code"].value_counts().head(3).to_dict()
        print(f"  {label}: {n:,} ({pct(n, n_unmatched)})")
        for code, cnt in top_codes.items():
            print(f"    {code}: {cnt:,} ({pct(cnt, n)})")

    # ── B5. By browser (from PC event) ────────────────────────
    print("\n── B5. Unmatched PC by browser (from $browser) ──")
    browser_stats_b = []
    for bf, g in unmatched.groupby("$browser"):
        n_g = len(g)
        if n_g < 30:
            continue
        # all PC for this browser
        total_for_browser = len(pc_b[pc_b["$browser"] == bf])
        browser_stats_b.append([
            bf, n_g, total_for_browser,
            pct(n_g, total_for_browser),
            g["reason_code"].value_counts().index[0] if len(g) > 0 else "",
        ])
    browser_stats_b.sort(key=lambda x: -x[1])
    print(tabulate(browser_stats_b,
        headers=["Browser","Unmatched","All PC","Loss %","Top reason"],
        tablefmt="pipe"))

    # ── B6. By alihelper_version ───────────────────────────────
    print("\n── B6. Unmatched PC by alihelper_version (top 10) ──")
    ver_b = []
    for v, g in unmatched.groupby("alihelper_version"):
        n_g = len(g)
        if n_g < 30 or not v:
            continue
        total_for_ver = len(pc_b[pc_b["alihelper_version"] == v])
        ver_b.append([v, n_g, total_for_ver, pct(n_g, total_for_ver)])
    ver_b.sort(key=lambda x: -x[1])
    print(tabulate(ver_b[:10],
        headers=["Version","Unmatched","All PC","Loss %"],
        tablefmt="pipe"))

    # ── B7. last_sk distribution in unmatched with our sk ─────
    print("\n── B7. our last_sk distribution in unmatched (with our sk) ──")
    ours = unmatched[unmatched["has_our_last_sk"]]
    for sk, cnt in ours["last_sk"].value_counts().items():
        print(f"  {sk}: {cnt:,} ({pct(cnt, len(ours))})")

    # ── B8. Top countries (unmatched, min 30) ─────────────────
    print("\n── B8. Top countries by loss rate (unmatched, min 30 total PC) ──")
    c_stats = []
    for c, g in pc_b.groupby("mp_country_code"):
        n_g = len(g)
        if n_g < 30:
            continue
        n_un = (~g["matched"]).sum()
        c_stats.append([c, n_g, n_un, pct(n_un, n_g)])
    c_stats.sort(key=lambda x: -float(x[3].replace("%","").replace("N/A","0")))
    print(tabulate(c_stats[:20],
        headers=["Country","Total PC","Unmatched","Loss %"],
        tablefmt="pipe"))

    # ── B9. is_new_buyer, is_hot_product in matched Purchase ──
    print("\n── B9. Purchase attributes (in matched events) ──")
    matched_purs = pur_b[pur_b["$user_id"].isin(matched["user_id"])]
    if len(matched_purs) > 0:
        print(f"  is_new_buyer=true: {(matched_purs['is_new_buyer']=='true').sum():,} / {len(matched_purs):,}")
        print(f"  is_hot_product=true: {(matched_purs['is_hot_product']=='true').sum():,} / {len(matched_purs):,}")


# ─────────────────────────────────────────────────────────────
# SECTION 6: Ranked root causes & recommendations
# ─────────────────────────────────────────────────────────────

def print_ranked_causes():
    print_section("RANKED ROOT CAUSES (to be filled after analysis)")
    print("""
See analysis output above. Causes are ranked by impact in each problem.

Problem A — most likely rank (update after seeing numbers):
  1. Ineligible traffic in denominator (non-product, non-homepage)
  2. Config not covering all users (value=False or missing)
  3. DOGI flow gap (users don't interact with DOGI coin)
  4. Hub reached but no return to AliExpress with our sk (A6)
  5. Missing Mixpanel click tracking (A5)
  6. Auto-redirect suppressed (30-min cooldown / cashback cooldown)
  7. Version underperformance

Problem B — most likely rank (update after seeing numbers):
  1. NO_OUR_SK_IN_72H — user never had our sk before purchase
  2. FOREIGN_SK_AFTER_OUR_SK — overwrite by third-party
  3. UNKNOWN — no clear signal (possible delayed postback)
  4. CASHBACK_TRACE
  5. AF_AFTER_OUR_SK
""")


def print_data_quality_caveats():
    print_section("DATA QUALITY & OBSERVABILITY CAVEATS")
    print("""
1. PARTIAL CASHBACK OBSERVABILITY
   cashback_list in Purchase Completed is client-reported.
   Cashback site visits are NOT logged to backend. Treat as partial evidence.

2. MISSING order_id IN PURCHASE COMPLETED
   Most PC events have order_id=None. Primary matching is user+time proximity.
   Ambiguous matches (same user, multiple purchases within window) treated as matched
   if any Purchase is within 10 min.

3. EXCLUDED noLogUrls
   AliExpress checkout/order paths may not be logged in events due to config-level
   URL exclusions. Absence of checkout events ≠ no user activity at checkout.

4. guestStateHistory IS CONFIG DELIVERY, NOT USAGE
   guestStateHistory records confirm config was delivered to client.
   They do NOT confirm the client executed a redirect using that config.
   "Latest delivered config" is a proxy for "what config was active at that time".

5. NO DIRECT LOG OF AUTO-REDIRECT ATTEMPTS
   Client-side webNavigation.onBeforeNavigate is not logged to backend.
   Auto-redirect opportunity reconstruction uses: eligible visit + browser lineage
   + 30-min rule approximation (not enforceable from backend logs).

6. TIME MATCHING UNCERTAINTY
   Mixpanel timestamps are in Moscow timezone project settings.
   All times converted to UTC for cross-source matching.
   Sub-minute precision is available in both sources.

7. ONLY _id INDEX ON events AND guestStateHistory
   All queries use _id-based date range. No field-level index on guest_id in events.
   Aggregation pipeline scans 5M events; may take several minutes.

8. CLIENT ENRICHMENT IS SPARSE
   Not all guest_id values in events have a matching record in clients.
   Browser/version data is missing for users who never sent client state.
   clients.guest_id is ObjectId; guestStateHistory.guest_id is string — different types.

9. SINGLE EVENT TYPE
   All events in the collection are of type 'watcher'. This is the only behavioral
   signal in MongoDB for this analysis. No session boundaries are defined natively.

10. PROBLEM B WINDOW EXCLUDES INCIDENT DATES
    Problem B analysis window (Feb 27 – Mar 26) avoids the Apr 1 CIS postback incident.
    Any residual delayed postbacks from the incident window are not included.
""")


# ─────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────

def main():
    print("=" * 70)
    print("  AliHelper Affiliate Investigation — Full Analysis")
    print(f"  Run date: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    print("=" * 70)

    print_data_quality_caveats()

    # Download / load Mixpanel data
    print_section("Downloading Mixpanel data")
    aff_click_raw, purchase_raw, pc_raw = download_mixpanel_data()

    aff_click_df = to_df(aff_click_raw)
    pur_df_raw   = to_df(purchase_raw)
    pc_df_raw    = to_df(pc_raw)

    # MongoDB
    print_section("Connecting to MongoDB via SSH tunnel")
    with sshtunnel.SSHTunnelForwarder(
        SSH_HOST, ssh_username=SSH_USER,
        remote_bind_address=(DB_HOST, DB_PORT),
        local_bind_address=("127.0.0.1", LOCAL_PORT),
    ) as tunnel:
        mongo_client = pymongo.MongoClient(
            f"mongodb://{MONGO_USER}:{MONGO_PASS}@127.0.0.1:{LOCAL_PORT}/{AUTH_DB}",
            serverSelectionTimeoutMS=15000, directConnection=True,
        )
        db = mongo_client[DB_NAME]
        print("  Connected.")

        # Problem A — MongoDB aggregations
        print_section("Running MongoDB aggregations for Problem A")
        mongo_data = run_mongo_problem_a(db)

        mongo_client.close()

    # Build Problem A dataframe
    df_a = build_problem_a_df(mongo_data, aff_click_df)

    # Analyze Problem A
    analyze_problem_a(df_a)

    # Problem B
    pur_b, pc_b = build_problem_b_df(purchase_raw, pc_raw)
    pc_b = match_purchases(pc_b, pur_b, window_minutes=10)
    pc_b = assign_reason_codes(pc_b)
    analyze_problem_b(pc_b, pur_b)

    print_ranked_causes()

    print_section("Analysis complete")
    print("  Cache files saved to ./cache/")
    print("  Re-run without re-downloading: python3 analysis_full.py")


if __name__ == "__main__":
    main()
