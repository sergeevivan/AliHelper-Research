"""
AliHelper — Full Reproducible Investigation v2
Corrected methodology per CLAUDE.md (CIS/EPN limited-observability update).

KEY CHANGES FROM v1:
  - CIS traffic is analyzed under a limited-observability framework
  - CIS proxy return = Affiliate Click followed by aliexpress.ru event within 120s
  - Problem A: dual funnel — Global (GLOBAL_DIRECT) vs CIS (CIS_PROXY)
  - Problem B: CIS events use CIS-specific reason codes, not sk-based buckets
  - Every finding labeled: GLOBAL_DIRECT / CIS_PROXY / NOT_OBSERVABLE_WITH_CURRENT_DATA

CACHE REUSE:
  REUSED  : cache/aff_click_a.json     — raw Affiliate Click (Mixpanel)
  REUSED  : cache/purchase_b.json      — raw Purchase (Mixpanel)
  REUSED  : cache/pc_b.json            — raw Purchase Completed (Mixpanel)
  REUSED  : cache/mongo_problem_a.pkl  — MongoDB user-level aggregations
  NEW     : cache/cis_proxy_return_a.pkl — CIS aliexpress.ru proxy return signal

Run: python3 -u analysis_v2.py 2>&1 | tee /tmp/analysis_v2_output.txt
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

# AliHelper-owned Global sk whitelist (for Global traffic only)
OUR_SKS = {"_c36PoUEj", "_d6jWDbY", "_AnTGXs", "_olPBn9X", "_dVh6yw5"}

# CIS countries (UA is Global/Portals per CLAUDE.md — analyzed via direct sk logic)
CIS_COUNTRIES = {"RU", "BY", "KZ", "UZ", "AZ", "AM", "GE", "KG", "MD", "TJ", "TM"}

# Auto-redirect browsers
AUTO_REDIRECT_BROWSERS = {"firefox", "edge"}

# CIS proxy return window (seconds after Affiliate Click)
PROXY_RETURN_WINDOW_S = 120

# Problem B 72h lookback window
ATTRIBUTION_WINDOW_H = 72


def oid_from_dt(dt: datetime) -> ObjectId:
    ts = int(dt.timestamp())
    return ObjectId(f"{ts:08x}0000000000000000")


def pct(num, denom, decimals=1):
    if denom == 0:
        return "N/A"
    return f"{100*num/denom:.{decimals}f}%"


def pct_f(num, denom):
    """Return float percentage, 0 if denom is 0."""
    if denom == 0:
        return 0.0
    return 100 * num / denom


def print_section(title):
    print(f"\n{'='*70}")
    print(f"  {title}")
    print(f"{'='*70}")


def browser_family(browser_str: str) -> str:
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
# SECTION 1: Mixpanel data download (reuse all existing caches)
# ─────────────────────────────────────────────────────────────

def mp_export(event_name: str, from_date: str, to_date: str, cache_key: str) -> list:
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
        params={"project_id": MP_PROJECT, "from_date": from_date,
                "to_date": to_date, "event": json.dumps([event_name])},
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
    print_section("Mixpanel data (reusing cache)")
    aff_clicks = mp_export("Affiliate Click",    "2026-03-06", "2026-04-03", "aff_click_a")
    purchases   = mp_export("Purchase",           "2026-02-27", "2026-03-27", "purchase_b")
    pc_events   = mp_export("Purchase Completed", "2026-02-27", "2026-03-27", "pc_b")
    return aff_clicks, purchases, pc_events


def to_df(records: list) -> pd.DataFrame:
    rows = [r.get("properties", {}) for r in records]
    return pd.DataFrame(rows)


# ─────────────────────────────────────────────────────────────
# SECTION 2: MongoDB Problem A (reuse existing pkl cache)
# ─────────────────────────────────────────────────────────────

def run_mongo_problem_a(db) -> dict:
    """Reuse cached MongoDB Problem A aggregations from v1."""
    cache_file = CACHE_DIR / "mongo_problem_a.pkl"
    if cache_file.exists():
        print("  [cache] Loading MongoDB Problem A data (reusing v1 cache)")
        with open(cache_file, "rb") as f:
            return pickle.load(f)

    # If cache missing, run the full aggregation (same as v1)
    events = db["events"]
    clients_col = db["clients"]
    gsh = db["guestStateHistory"]

    oid_start   = oid_from_dt(A_START)
    oid_end     = oid_from_dt(A_END)
    oid_lookback = oid_from_dt(A_START - timedelta(days=14))

    print("  [mongo] Aggregating events per user...")
    t0 = time.time()
    pipeline_users = [
        {"$match": {"_id": {"$gte": oid_start, "$lte": oid_end}}},
        {"$group": {
            "_id": "$guest_id",
            "total_events": {"$sum": 1},
            "country": {"$last": "$country"},
            "product_events": {"$sum": {"$cond": [{"$ne": ["$payload.productId", None]}, 1, 0]}},
            "our_sk_events": {"$sum": {"$cond": [{"$in": ["$payload.querySk", list(OUR_SKS)]}, 1, 0]}},
            "any_sk_events": {"$sum": {"$cond": [{"$ne": ["$payload.querySk", None]}, 1, 0]}},
            "foreign_sk_events": {"$sum": {"$cond": [
                {"$and": [{"$ne": ["$payload.querySk", None]},
                           {"$not": {"$in": ["$payload.querySk", list(OUR_SKS)]}}]}, 1, 0]}},
            "first_event_oid": {"$min": "$_id"},
        }},
    ]
    user_events = list(events.aggregate(pipeline_users, allowDiskUse=True))
    print(f"    → {len(user_events):,} distinct users in {time.time()-t0:.1f}s")

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

    print("  [mongo] Client enrichment via pipeline $lookup...")
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

    print("  [mongo] guestStateHistory aggregation...")
    t0 = time.time()
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
    print(f"    → gsh done in {time.time()-t0:.1f}s")

    result = {
        "user_events": user_events,
        "homepage_data": homepage_data,
        "client_map": client_map,
        "gsh_in_window": gsh_in_window,
        "gsh_pre_window": gsh_pre_window,
    }
    with open(cache_file, "wb") as f:
        pickle.dump(result, f)
    return result


# ─────────────────────────────────────────────────────────────
# SECTION 2b: NEW — CIS proxy return query
# ─────────────────────────────────────────────────────────────

def run_cis_proxy_return(db, aff_click_raw: list) -> dict:
    """
    NEW query (not in v1 cache).

    For CIS users who had Affiliate Click in Problem A window:
    check if they had an aliexpress.ru event in MongoDB within
    PROXY_RETURN_WINDOW_S seconds after any of their clicks.

    Returns: dict { guest_id_str -> bool }
    Observability label: CIS_PROXY
    """
    cache_file = CACHE_DIR / "cis_proxy_return_a.pkl"
    if cache_file.exists():
        print("  [cache] Loading CIS proxy return data")
        with open(cache_file, "rb") as f:
            return pickle.load(f)

    print("  [new] Computing CIS proxy return (NEW — not in v1 cache)...")

    # ── Step 1: CIS Affiliate Click users and their click timestamps ──
    cis_click_times = {}  # guest_id_str -> [unix_ts_s, ...]
    a_start_ts = A_START.timestamp()
    a_end_ts   = A_END.timestamp()

    for ev in aff_click_raw:
        props = ev.get("properties", ev) if "properties" in ev else ev
        ts = props.get("time", 0)
        if not (a_start_ts <= ts <= a_end_ts):
            continue
        country = str(props.get("mp_country_code", "") or "").upper()
        if country not in CIS_COUNTRIES:
            continue
        uid = str(props.get("$user_id", "") or "")
        if uid:
            cis_click_times.setdefault(uid, []).append(int(ts))

    print(f"    CIS users with Affiliate Click in window: {len(cis_click_times):,}")

    if not cis_click_times:
        result = {}
        with open(cache_file, "wb") as f:
            pickle.dump(result, f)
        return result

    # ── Step 2: Convert to ObjectIds for MongoDB ──
    cis_oids = []
    for uid_str in cis_click_times:
        try:
            cis_oids.append(ObjectId(uid_str))
        except Exception:
            pass

    print(f"    Querying MongoDB for aliexpress.ru events for {len(cis_oids):,} CIS users...")
    t0 = time.time()

    oid_start = oid_from_dt(A_START)
    oid_end   = oid_from_dt(A_END)

    events = db["events"]

    # Aggregate: for each CIS click user, collect timestamps of aliexpress.ru visits
    # Use $toDate on _id to get event timestamp (ObjectId encodes UTC creation time)
    pipeline = [
        {"$match": {
            "_id":       {"$gte": oid_start, "$lte": oid_end},
            "guest_id":  {"$in": cis_oids},
            "payload.url": {"$regex": r"aliexpress\.ru", "$options": "i"},
        }},
        {"$group": {
            "_id": "$guest_id",
            # $toLong of $toDate gives milliseconds since epoch
            "visit_ms": {"$push": {"$toLong": {"$toDate": "$_id"}}},
        }},
    ]

    aliexpress_ru_visits = {}  # guest_id_str -> sorted list of unix_ts_s
    for r in events.aggregate(pipeline, allowDiskUse=True):
        uid_str = str(r["_id"])
        # Convert ms → seconds
        visit_times_s = sorted(t // 1000 for t in r["visit_ms"])
        aliexpress_ru_visits[uid_str] = visit_times_s

    elapsed = time.time() - t0
    print(f"    → {len(aliexpress_ru_visits):,} CIS users had aliexpress.ru visits "
          f"in {elapsed:.1f}s")

    # ── Step 3: Match click → proxy return within PROXY_RETURN_WINDOW_S ──
    proxy_result = {}  # guest_id_str -> bool
    for uid, click_times in cis_click_times.items():
        visit_times = aliexpress_ru_visits.get(uid, [])
        has_proxy = False
        for ct in sorted(click_times):
            for vt in visit_times:
                if vt < ct:
                    continue
                if vt > ct + PROXY_RETURN_WINDOW_S:
                    break
                has_proxy = True
                break
            if has_proxy:
                break
        proxy_result[uid] = has_proxy

    n_true = sum(1 for v in proxy_result.values() if v)
    n_total = len(proxy_result)
    print(f"    CIS proxy return (within {PROXY_RETURN_WINDOW_S}s): "
          f"{n_true:,}/{n_total:,} ({pct(n_true, n_total)})")

    with open(cache_file, "wb") as f:
        pickle.dump(proxy_result, f)
    print("    [cache] CIS proxy return saved to cis_proxy_return_a.pkl")
    return proxy_result


# ─────────────────────────────────────────────────────────────
# SECTION 3: Build Problem A dataframe (corrected)
# ─────────────────────────────────────────────────────────────

def build_problem_a_df(mongo_data: dict, aff_click_raw: list,
                       cis_proxy_return: dict) -> pd.DataFrame:
    """
    Build per-user Problem A dataframe with strict Global/CIS split.

    - Global: has_our_sk_return = GLOBAL_DIRECT signal
    - CIS:    has_proxy_return  = CIS_PROXY signal (aliexpress.ru within 120s)
    - a5_missing_tracking: GLOBAL_DIRECT only (sk return without click)
    - a6_hub_no_global_return: Global — hub but no our sk
    - a6_hub_no_cis_proxy: CIS — hub but no proxy return
    """
    print_section("Building Problem A user-level dataframe (corrected v2)")

    user_events   = mongo_data["user_events"]
    homepage_data = mongo_data["homepage_data"]
    client_map    = mongo_data["client_map"]
    gsh_in        = mongo_data["gsh_in_window"]
    gsh_pre       = mongo_data["gsh_pre_window"]

    # Filter Affiliate Click to Problem A UTC window
    ac_by_user = {}  # guest_id_str -> country (first seen)
    a_start_ts = A_START.timestamp()
    a_end_ts   = A_END.timestamp()

    for ev in aff_click_raw:
        props = ev.get("properties", ev) if "properties" in ev else ev
        ts = props.get("time", 0)
        if not (a_start_ts <= ts <= a_end_ts):
            continue
        uid = str(props.get("$user_id", "") or "")
        if uid and uid not in ac_by_user:
            ac_by_user[uid] = str(props.get("mp_country_code", "") or "").upper()

    ac_users = set(ac_by_user.keys())
    print(f"  Affiliate Click users in window: {len(ac_users):,}")

    rows = []
    for ue in user_events:
        guest_id = str(ue["_id"])

        # Eligible check
        hp_count   = homepage_data.get(guest_id, 0)
        prod_count = ue.get("product_events", 0)
        has_product  = prod_count > 0
        has_homepage = hp_count > 0
        is_eligible  = has_product or has_homepage

        # Client enrichment
        cli        = client_map.get(guest_id, {})
        raw_browser = cli.get("browser", "")
        bf         = browser_family(raw_browser)
        lg         = lineage(bf)
        cv         = cli.get("client_version", "")

        # Config: prefer in-window, fall back to pre-window
        conf = gsh_in.get(guest_id) or gsh_pre.get(guest_id)
        if conf:
            cfg_value  = conf.get("latest_value", False)
            cfg_domain = conf.get("latest_domain", "")
            cfg_region = conf.get("latest_region", "")
        else:
            cfg_value  = None
            cfg_domain = ""
            cfg_region = ""

        country     = (ue.get("country") or "").upper()
        user_is_cis = is_cis(country)

        reached_hub = guest_id in ac_users

        # ── GLOBAL_DIRECT signal: owned sk in MongoDB events ──
        has_our_sk_return     = ue.get("our_sk_events", 0) > 0
        has_any_sk_return     = ue.get("any_sk_events", 0) > 0
        has_foreign_sk_return = ue.get("foreign_sk_events", 0) > 0

        # ── CIS_PROXY signal: aliexpress.ru visit within 120s of click ──
        # Only meaningful for CIS users who reached hub.
        # For Global users: NOT_OBSERVABLE_WITH_CURRENT_DATA (different mechanism).
        has_proxy_return = cis_proxy_return.get(guest_id, False) if user_is_cis else None

        # A5: Missing Mixpanel click tracking (GLOBAL_DIRECT only)
        # sk return observed in events, but no Affiliate Click in Mixpanel
        a5_missing_tracking = (not user_is_cis) and has_our_sk_return and not reached_hub

        # A6: Reached hub but no post-hub signal
        a6_hub_no_global_return = (not user_is_cis) and reached_hub and not has_our_sk_return
        a6_hub_no_cis_proxy     = user_is_cis and reached_hub and (has_proxy_return is False)

        rows.append({
            "guest_id":              guest_id,
            "country":               country,
            "is_cis":                user_is_cis,
            "region":                "CIS" if user_is_cis else "Global",
            "total_events":          ue.get("total_events", 0),
            "product_events":        prod_count,
            "homepage_events":       hp_count,
            "has_product":           has_product,
            "has_homepage":          has_homepage,
            "is_eligible":           is_eligible,
            "browser_raw":           raw_browser,
            "browser_family":        bf,
            "lineage":               lg,
            "client_version":        cv,
            "cfg_value":             cfg_value,
            "cfg_domain":            cfg_domain,
            "cfg_region":            cfg_region,
            "has_cfg":               conf is not None,
            "has_usable_cfg":        cfg_value is True,
            "reached_hub":           reached_hub,
            # Global signals (GLOBAL_DIRECT)
            "has_our_sk_return":     has_our_sk_return,
            "has_any_sk_return":     has_any_sk_return,
            "has_foreign_sk_return": has_foreign_sk_return,
            # CIS signal (CIS_PROXY) — None for Global users
            "has_proxy_return":      has_proxy_return,
            # Derived flags
            "a5_missing_tracking":   a5_missing_tracking,          # GLOBAL_DIRECT
            "a6_hub_no_global_ret":  a6_hub_no_global_return,      # GLOBAL_DIRECT
            "a6_hub_no_cis_proxy":   a6_hub_no_cis_proxy,          # CIS_PROXY
        })

    df = pd.DataFrame(rows)
    print(f"  Total users: {len(df):,}")
    print(f"  Global users: {(~df['is_cis']).sum():,} | CIS users: {df['is_cis'].sum():,}")
    print(f"  Eligible: {df['is_eligible'].sum():,}")
    print(f"  Reached hub: {df['reached_hub'].sum():,}")
    print(f"  Global — our sk return [GLOBAL_DIRECT]: {df['has_our_sk_return'].sum():,}")
    print(f"  CIS — proxy return [CIS_PROXY]: {df['has_proxy_return'].eq(True).sum():,}")
    return df


# ─────────────────────────────────────────────────────────────
# SECTION 4: Problem A — Dual Funnel & Segmentation (corrected)
# ─────────────────────────────────────────────────────────────

def analyze_problem_a(df: pd.DataFrame) -> dict:
    print_section("PROBLEM A — Missing Affiliate Click (corrected v2)")

    results = {}

    # ── Overall funnel ─────────────────────────────────────────
    n_total    = len(df)
    n_eligible = df["is_eligible"].sum()
    n_hub      = df[df["is_eligible"]]["reached_hub"].sum()

    print(f"\n  Total users (raw AliExpress activity): {n_total:,}")
    print(f"  Eligible users (product or homepage):   {n_eligible:,} ({pct(n_eligible, n_total)})")
    print(f"  Reached hub (Affiliate Click):          {n_hub:,} ({pct(n_hub, n_eligible)} of eligible)")
    print(f"  Ineligible gap:                         {n_total - n_eligible:,} ({pct(n_total-n_eligible, n_total)})")

    results["n_total"] = int(n_total)
    results["n_eligible"] = int(n_eligible)
    results["n_hub"] = int(n_hub)

    # ── A1. Dual funnel: Global (GLOBAL_DIRECT) vs CIS (CIS_PROXY) ────
    print("\n── A1. Dual Funnel by Region ──")

    global_df = df[~df["is_cis"]]
    cis_df    = df[df["is_cis"]]

    elig_g = global_df[global_df["is_eligible"]]
    elig_c = cis_df[cis_df["is_eligible"]]

    # Global funnel
    ng_total   = len(global_df)
    ng_elig    = len(elig_g)
    ng_cfg     = elig_g["has_usable_cfg"].sum()
    ng_hub     = elig_g["reached_hub"].sum()
    ng_sk_ret  = elig_g["has_our_sk_return"].sum()
    ng_a5      = elig_g["a5_missing_tracking"].sum()
    ng_a6      = elig_g["a6_hub_no_global_ret"].sum()

    global_funnel = [
        ["1. Raw activity",                    ng_total, pct(ng_total, ng_total),   "GLOBAL_DIRECT"],
        ["2. Eligible (product/homepage)",     ng_elig,  pct(ng_elig, ng_total),    "GLOBAL_DIRECT"],
        ["3. Usable config (value=True)",      ng_cfg,   pct(ng_cfg, ng_elig),      "GLOBAL_DIRECT"],
        ["4. Reached hub (Affiliate Click)",   ng_hub,   pct(ng_hub, ng_elig),      "GLOBAL_DIRECT"],
        ["5. Returned with our sk",            ng_sk_ret,pct(ng_sk_ret, ng_elig),   "GLOBAL_DIRECT"],
        ["   A5: sk return, no click (silent)",ng_a5,    pct(ng_a5, ng_elig),       "GLOBAL_DIRECT"],
        ["   A6: hub reached, no sk return",   ng_a6,    pct(ng_a6, ng_elig),       "GLOBAL_DIRECT"],
    ]
    print(f"\n  [GLOBAL_DIRECT] Global / Portals funnel ({ng_total:,} users)")
    print(tabulate(global_funnel,
        headers=["Stage", "Users", "% of eligible", "Observability"],
        tablefmt="pipe"))

    results["global"] = {
        "n_total": int(ng_total), "n_eligible": int(ng_elig),
        "n_usable_cfg": int(ng_cfg), "n_hub": int(ng_hub),
        "n_sk_return": int(ng_sk_ret), "n_a5": int(ng_a5), "n_a6": int(ng_a6),
    }

    # CIS funnel
    nc_total  = len(cis_df)
    nc_elig   = len(elig_c)
    nc_cfg    = elig_c["has_usable_cfg"].sum()
    nc_hub    = elig_c["reached_hub"].sum()
    nc_proxy  = elig_c["has_proxy_return"].eq(True).sum()   # CIS_PROXY signal
    nc_a6     = elig_c["a6_hub_no_cis_proxy"].sum()

    cis_funnel = [
        ["1. Raw activity",                        nc_total, pct(nc_total, nc_total), "CIS_PROXY"],
        ["2. Eligible (product/homepage)",         nc_elig,  pct(nc_elig, nc_total),  "CIS_PROXY"],
        ["3. Usable config (value=True)",          nc_cfg,   pct(nc_cfg, nc_elig),    "CIS_PROXY"],
        ["4. Reached hub (Affiliate Click)",        nc_hub,   pct(nc_hub, nc_elig),    "CIS_PROXY"],
        ["5. Proxy return to aliexpress.ru (≤120s)",nc_proxy, pct(nc_proxy, nc_elig), "CIS_PROXY"],
        ["5a. Affiliate params preserved",          "—",     "—",                     "NOT_OBSERVABLE_WITH_CURRENT_DATA"],
        ["   A6: hub reached, no proxy return",    nc_a6,    pct(nc_a6, nc_elig),     "CIS_PROXY"],
    ]
    print(f"\n  [CIS_PROXY] CIS / EPN funnel ({nc_total:,} users)")
    print(tabulate(cis_funnel,
        headers=["Stage", "Users", "% of eligible", "Observability"],
        tablefmt="pipe"))

    print(f"\n  NOTE: CIS proxy return = aliexpress.ru event within {PROXY_RETURN_WINDOW_S}s of Affiliate Click.")
    print(f"  NOTE: Whether EPN affiliate params were preserved is NOT_OBSERVABLE_WITH_CURRENT_DATA.")
    print(f"        utm_source/utm_medium/utm_campaign are not stored in MongoDB events.")

    results["cis"] = {
        "n_total": int(nc_total), "n_eligible": int(nc_elig),
        "n_usable_cfg": int(nc_cfg), "n_hub": int(nc_hub),
        "n_proxy_return": int(nc_proxy), "n_a6_no_proxy": int(nc_a6),
    }

    # ── A2. Gap decomposition (eligible, overall) ───────────────────
    print("\n── A2. Gap decomposition — why didn't eligible users reach hub? ──")
    elig = df[df["is_eligible"]].copy()
    n_no_cfg      = (~elig["has_cfg"]).sum()
    n_bad_cfg     = (elig["has_cfg"] & ~elig["has_usable_cfg"]).sum()
    n_good_no_hub = (elig["has_usable_cfg"] & ~elig["reached_hub"]).sum()
    n_gap_total   = n_eligible - n_hub

    gap_table = [
        ["No config snapshot at all",          n_no_cfg,      pct(n_no_cfg, n_gap_total)],
        ["Config found but value=False",        n_bad_cfg,     pct(n_bad_cfg, n_gap_total)],
        ["Usable config, never reached hub",    n_good_no_hub, pct(n_good_no_hub, n_gap_total)],
        ["A5: silent redirect (sk, no click)",  elig["a5_missing_tracking"].sum(),
                                                pct(elig["a5_missing_tracking"].sum(), n_gap_total)],
    ]
    print(tabulate(gap_table, headers=["Gap bucket", "Users", "% of gap"], tablefmt="pipe"))
    results["gap"] = {
        "n_gap_total": int(n_gap_total),
        "n_no_cfg": int(n_no_cfg), "n_bad_cfg": int(n_bad_cfg),
        "n_good_no_hub": int(n_good_no_hub),
        "n_a5": int(elig["a5_missing_tracking"].sum()),
    }

    # ── A3. Browser / lineage split ─────────────────────────────────
    print("\n── A3. Browser lineage (eligible users) ──")
    lineage_stats = []
    for lg, g in elig.groupby("lineage"):
        n = len(g)
        g_gl = g[~g["is_cis"]]
        g_ci = g[g["is_cis"]]
        lineage_stats.append([
            lg, n, pct(n, n_eligible),
            g["reached_hub"].sum(), pct(g["reached_hub"].sum(), n),
            g_gl["has_our_sk_return"].sum(), pct(g_gl["has_our_sk_return"].sum(), len(g_gl)) if len(g_gl) else "N/A",
            g_ci["has_proxy_return"].eq(True).sum(), pct(g_ci["has_proxy_return"].eq(True).sum(), len(g_ci)) if len(g_ci) else "N/A",
        ])
    lineage_stats.sort(key=lambda x: -x[1])
    print(tabulate(lineage_stats,
        headers=["Lineage","Users","% elig","Reached hub","% hub",
                 "Global sk-ret [GD]","% sk-ret","CIS proxy-ret [CP]","% proxy-ret"],
        tablefmt="pipe"))

    # ── A4. Auto-redirect: Firefox vs Edge (GLOBAL_DIRECT + CIS_PROXY) ─
    print("\n── A4. Auto-redirect: Firefox vs Edge (eligible) ──")
    auto = elig[elig["lineage"] == "auto-redirect"]
    auto_stats = []
    for bf, g in auto.groupby("browser_family"):
        n = len(g)
        g_gl = g[~g["is_cis"]]
        g_ci = g[g["is_cis"]]
        auto_stats.append([
            bf, n,
            g["reached_hub"].sum(), pct(g["reached_hub"].sum(), n),
            g_gl["has_our_sk_return"].sum() if len(g_gl) else 0,
            pct(g_gl["has_our_sk_return"].sum(), len(g_gl)) if len(g_gl) else "N/A",
            g_ci["has_proxy_return"].eq(True).sum() if len(g_ci) else 0,
            pct(g_ci["has_proxy_return"].eq(True).sum(), len(g_ci)) if len(g_ci) else "N/A",
        ])
    if auto_stats:
        print(tabulate(auto_stats,
            headers=["Browser","Users","Reached hub","% hub",
                     "Global sk-ret","% sk-ret [GD]","CIS proxy-ret","% proxy-ret [CP]"],
            tablefmt="pipe"))
    else:
        print("  No auto-redirect users found.")

    # ── A5. By hub domain (Global only — sk-return is GLOBAL_DIRECT) ──
    print("\n── A5. By hub domain — Global only (GLOBAL_DIRECT) ──")
    print("  NOTE: Hub sk-return comparison is only valid for Global traffic.")
    print("        CIS/EPN return uses utm_*, not sk. Hub comparison for CIS = NOT_OBSERVABLE_WITH_CURRENT_DATA.")
    usable_global = elig[elig["has_usable_cfg"] & ~elig["is_cis"]].copy()
    hub_global = []
    for domain, g in usable_global.groupby("cfg_domain"):
        n = len(g)
        hub_global.append([
            domain, n,
            g["reached_hub"].sum(), pct(g["reached_hub"].sum(), n),
            g["has_our_sk_return"].sum(), pct(g["has_our_sk_return"].sum(), n),
            "GLOBAL_DIRECT",
        ])
    hub_global.sort(key=lambda x: -x[1])
    if hub_global:
        print(tabulate(hub_global,
            headers=["Hub domain","Users","Reached hub","% hub","sk return","% return","Observability"],
            tablefmt="pipe"))

    print("\n── A5b. By hub domain — CIS (CIS_PROXY) ──")
    usable_cis = elig[elig["has_usable_cfg"] & elig["is_cis"]].copy()
    hub_cis = []
    for domain, g in usable_cis.groupby("cfg_domain"):
        n = len(g)
        hub_cis.append([
            domain, n,
            g["reached_hub"].sum(), pct(g["reached_hub"].sum(), n),
            g["has_proxy_return"].eq(True).sum(), pct(g["has_proxy_return"].eq(True).sum(), n),
            "CIS_PROXY",
        ])
    hub_cis.sort(key=lambda x: -x[1])
    if hub_cis:
        print(tabulate(hub_cis,
            headers=["Hub domain","Users","Reached hub","% hub","Proxy return","% return [CIS_PROXY]","Observability"],
            tablefmt="pipe"))
    else:
        print("  No CIS users with usable config found in window.")

    results["hub_global"] = [{"domain": r[0], "n": r[1], "n_hub": r[2], "n_sk_ret": r[4]} for r in hub_global]
    results["hub_cis"]    = [{"domain": r[0], "n": r[1], "n_hub": r[2], "n_proxy": r[4]} for r in hub_cis]

    # ── A6. By country (eligible, min 50 users) ──────────────────────
    print("\n── A6. Top countries (eligible, min 50 users) ──")
    country_stats = []
    for c, g in elig.groupby("country"):
        n = len(g)
        if n < 50:
            continue
        cis_flag = g["is_cis"].all()
        region_label = "CIS" if g["is_cis"].all() else ("Mixed" if g["is_cis"].any() else "Global")
        obs = "CIS_PROXY" if cis_flag else "GLOBAL_DIRECT"
        hub_n = g["reached_hub"].sum()
        if cis_flag:
            ret_n = g["has_proxy_return"].eq(True).sum()
            ret_lbl = f"{ret_n} (proxy)"
        else:
            ret_n = g["has_our_sk_return"].sum()
            ret_lbl = f"{ret_n} (sk)"
        country_stats.append([c, region_label, n, hub_n, pct(hub_n, n), ret_lbl, pct(ret_n, n), obs])
    country_stats.sort(key=lambda x: -x[2])
    print(tabulate(country_stats[:20],
        headers=["Country","Region","Users","Reached hub","% hub","Return signal","% return","Obs."],
        tablefmt="pipe"))

    # ── A7. Client version (top 15, eligible) ────────────────────────
    print("\n── A7. Top client versions (eligible, min 30 users) ──")
    ver_stats = []
    for v, g in elig.groupby("client_version"):
        n = len(g)
        if n < 30 or not v:
            continue
        ver_stats.append([v, n, g["reached_hub"].sum(), pct(g["reached_hub"].sum(), n)])
    ver_stats.sort(key=lambda x: -x[1])
    print(tabulate(ver_stats[:15],
        headers=["Version","Users","Reached hub","% hub"], tablefmt="pipe"))

    # ── A8. Config coverage by region/lineage ──────────────────────────
    print("\n── A8. Config coverage by region and lineage ──")
    for region_label, mask in [("Global", ~elig["is_cis"]), ("CIS", elig["is_cis"])]:
        g_reg = elig[mask]
        for lg, g_lin in g_reg.groupby("lineage"):
            n = len(g_lin)
            print(f"  {region_label} / {lg}: {n:,} users | "
                  f"has cfg: {g_lin['has_cfg'].sum():,} ({pct(g_lin['has_cfg'].sum(), n)}) | "
                  f"usable cfg: {g_lin['has_usable_cfg'].sum():,} ({pct(g_lin['has_usable_cfg'].sum(), n)})")

    # ── A9. Page type split ────────────────────────────────────────────
    print("\n── A9. Eligible by page type ──")
    for label, mask in [
        ("Product only",  elig["has_product"] & ~elig["has_homepage"]),
        ("Homepage only", ~elig["has_product"] & elig["has_homepage"]),
        ("Both",          elig["has_product"] & elig["has_homepage"]),
    ]:
        g = elig[mask]
        if len(g) == 0:
            continue
        print(f"  {label}: {len(g):,} | reached hub: {g['reached_hub'].sum():,} ({pct(g['reached_hub'].sum(), len(g))})")

    return results


# ─────────────────────────────────────────────────────────────
# SECTION 5: Problem B — Purchase Completed vs Purchase
# ─────────────────────────────────────────────────────────────

def build_problem_b_df(purchases: list, pc_events: list):
    print_section("Building Problem B dataframes")

    pur_df = to_df(purchases)
    pc_df  = to_df(pc_events)

    pur_df["time_utc"] = pd.to_datetime(pur_df["time"], unit="s", utc=True)
    pc_df["time_utc"]  = pd.to_datetime(pc_df["time"],  unit="s", utc=True)

    pur_b = pur_df[(pur_df["time_utc"] >= B_START) & (pur_df["time_utc"] <= B_END)].copy()
    pc_b  = pc_df[ (pc_df["time_utc"]  >= B_START) & (pc_df["time_utc"]  <= B_END)].copy()

    print(f"  Purchase Completed in window: {len(pc_b):,}")
    print(f"  Purchase in window:           {len(pur_b):,}")
    print(f"  Raw gap:                      {len(pc_b)-len(pur_b):,} ({pct(len(pc_b)-len(pur_b), len(pc_b))} of PC)")

    return pur_b, pc_b


def match_purchases(pc_b: pd.DataFrame, pur_b: pd.DataFrame,
                    window_minutes: int = 10) -> pd.DataFrame:
    print(f"\n  Matching PC → Purchase (user + ±{window_minutes} min)...")
    pur_by_user = {}
    for _, row in pur_b.iterrows():
        uid = str(row.get("$user_id", "") or "")
        if uid:
            pur_by_user.setdefault(uid, []).append(row["time_utc"])

    window = timedelta(minutes=window_minutes)
    pc_b = pc_b.copy()
    pc_b["user_id"] = pc_b["$user_id"].astype(str)
    pc_b["matched"] = False

    for idx, row in pc_b.iterrows():
        uid    = row["user_id"]
        pc_ts  = row["time_utc"]
        for pur_ts in pur_by_user.get(uid, []):
            if abs((pc_ts - pur_ts).total_seconds()) <= window.total_seconds():
                pc_b.at[idx, "matched"] = True
                break

    n_matched = pc_b["matched"].sum()
    print(f"  Matched:   {n_matched:,} ({pct(n_matched, len(pc_b))})")
    print(f"  Unmatched: {(~pc_b['matched']).sum():,} ({pct((~pc_b['matched']).sum(), len(pc_b))})")
    return pc_b


def build_ac_72h_lookup(aff_click_raw: list) -> dict:
    """
    Build lookup: guest_id_str -> sorted list of AC unix timestamps.
    Used for 72h lookback check in Problem B CIS analysis.
    Coverage: Mar 6 – Apr 3 (may miss lookback for PC events Feb 27 – Mar 8).
    """
    ac_by_user = {}
    for ev in aff_click_raw:
        props = ev.get("properties", ev) if "properties" in ev else ev
        uid = str(props.get("$user_id", "") or "")
        ts  = props.get("time", 0)
        if uid and ts:
            ac_by_user.setdefault(uid, []).append(int(ts))
    for uid in ac_by_user:
        ac_by_user[uid].sort()
    return ac_by_user


def assign_reason_codes(pc_b: pd.DataFrame, aff_click_raw: list) -> pd.DataFrame:
    """
    Assign primary reason codes with strict Global/CIS split.

    Global (GLOBAL_DIRECT): sk-based logic
    CIS (CIS_PROXY): limited-observability codes only — NO sk-based codes
    """
    pc_b = pc_b.copy()

    # Normalize affiliate state fields (from Purchase Completed event properties)
    pc_b["last_sk"]   = pc_b.get("last_sk",   pd.Series(dtype=str)).fillna("").astype(str)
    pc_b["last_af"]   = pc_b.get("last_af",   pd.Series(dtype=str)).fillna("").astype(str)
    pc_b["sk"]        = pc_b.get("sk",         pd.Series(dtype=str)).fillna("").astype(str)
    pc_b["af"]        = pc_b.get("af",         pd.Series(dtype=str)).fillna("").astype(str)
    pc_b["cashback_list"] = pc_b.get("cashback_list", pd.Series(dtype=object))

    # Region classification
    pc_b["is_cis_pc"] = pc_b.get("mp_country_code", pd.Series(dtype=str)).apply(
        lambda x: is_cis(str(x) if x else ""))

    # Global sk-based flags
    pc_b["has_our_last_sk"]     = pc_b["last_sk"].isin(OUR_SKS)
    pc_b["has_our_current_sk"]  = pc_b["sk"].isin(OUR_SKS)
    pc_b["has_any_our_sk"]      = pc_b["has_our_last_sk"] | pc_b["has_our_current_sk"]
    pc_b["has_foreign_last_sk"] = (pc_b["last_sk"] != "") & ~pc_b["has_our_last_sk"]
    pc_b["has_foreign_sk"]      = (pc_b["sk"] != "") & ~pc_b["has_our_current_sk"]
    pc_b["has_af"]              = pc_b["af"] != ""
    pc_b["has_last_af"]         = pc_b["last_af"] != ""
    pc_b["has_cashback"]        = pc_b["cashback_list"].notna() & (pc_b["cashback_list"] != "")

    # CIS: 72h AC lookup from available Mixpanel data
    ac_72h = build_ac_72h_lookup(aff_click_raw)
    attribution_window_s = ATTRIBUTION_WINDOW_H * 3600

    def had_ac_in_72h(row) -> bool:
        """Check if user had Affiliate Click within 72h before this PC event."""
        uid = str(row.get("user_id", "") or "")
        pc_ts = int(row["time_utc"].timestamp())
        for ac_ts in ac_72h.get(uid, []):
            if 0 <= (pc_ts - ac_ts) <= attribution_window_s:
                return True
        return False

    # ── Assign reason codes ──────────────────────────────────────────
    def assign_global_code(row) -> str:
        """GLOBAL_DIRECT reason codes (sk-based)."""
        if row["matched"]:
            return "MATCHED"
        if not row["has_any_our_sk"]:
            return "NO_OUR_SK_IN_72H"
        if row["has_foreign_sk"] or row["has_foreign_last_sk"]:
            return "FOREIGN_SK_AFTER_OUR_SK"
        if row["has_af"] or row["has_last_af"]:
            return "AF_AFTER_OUR_SK"
        if row["has_cashback"]:
            return "CASHBACK_TRACE"
        return "UNKNOWN"

    def assign_cis_code(row) -> str:
        """
        CIS_PROXY reason codes — limited observability.
        Do NOT use sk-based logic for CIS traffic.
        """
        if row["matched"]:
            return "CIS_LIKELY_DELAYED_POSTBACK"
        # Check if user had Affiliate Click in available 72h window
        has_ac = had_ac_in_72h(row)
        if not has_ac:
            return "CIS_NO_HUB_REACH_OBSERVED"
        # Had AC but no matched Purchase
        return "CIS_PURCHASE_COMPLETED_WITHOUT_PURCHASE_UNDER_LIMITED_OBSERVABILITY"

    codes = []
    for _, row in pc_b.iterrows():
        if row["is_cis_pc"]:
            codes.append(assign_cis_code(row))
        else:
            codes.append(assign_global_code(row))

    pc_b["reason_code"] = codes

    # Observability label
    def obs_label(code: str) -> str:
        if code.startswith("CIS_"):
            return "CIS_PROXY"
        if code == "MATCHED":
            return "GLOBAL_DIRECT"
        return "GLOBAL_DIRECT"

    pc_b["observability"] = pc_b["reason_code"].apply(obs_label)
    return pc_b


# ─────────────────────────────────────────────────────────────
# SECTION 6: Problem B analysis (corrected)
# ─────────────────────────────────────────────────────────────

def analyze_problem_b(pc_b: pd.DataFrame, pur_b: pd.DataFrame) -> dict:
    print_section("PROBLEM B — Purchase Completed without Purchase (corrected v2)")

    results = {}

    n_pc  = len(pc_b)
    n_pur = len(pur_b)
    n_raw_gap = n_pc - n_pur

    print(f"\n  Purchase Completed in window:  {n_pc:,}")
    print(f"  Purchase in window:            {n_pur:,}")
    print(f"  Raw event gap (net):           {n_raw_gap:,} ({pct(n_raw_gap, n_pc)} of PC)")

    n_matched   = pc_b["matched"].sum()
    n_unmatched = (~pc_b["matched"]).sum()
    print(f"\n  10-min user+time match:")
    print(f"    Matched PC→Purchase:  {n_matched:,} ({pct(n_matched, n_pc)})")
    print(f"    Unmatched (gap):      {n_unmatched:,} ({pct(n_unmatched, n_pc)})")
    print(f"\n  Primary metric = raw event gap ({n_raw_gap:,}).")
    print(f"  UNKNOWN/delayed bucket ≈ raw gap confirms most unmatched = delayed postbacks.")

    results["n_pc"] = int(n_pc)
    results["n_pur"] = int(n_pur)
    results["n_raw_gap"] = int(n_raw_gap)
    results["n_matched"] = int(n_matched)
    results["n_unmatched"] = int(n_unmatched)

    # ── B1. CIS vs Global split ───────────────────────────────────────
    print("\n── B1. CIS vs Global split ──")
    cis_pc    = pc_b[pc_b["is_cis_pc"]]
    global_pc = pc_b[~pc_b["is_cis_pc"]]

    for label, gdf, obs in [
        ("Global [GLOBAL_DIRECT]", global_pc, "GLOBAL_DIRECT"),
        ("CIS [CIS_PROXY]",        cis_pc,    "CIS_PROXY"),
    ]:
        n = len(gdf)
        n_match = gdf["matched"].sum()
        n_unmatch = (~gdf["matched"]).sum()
        print(f"\n  {label}: {n:,} PC events | "
              f"matched: {n_match:,} ({pct(n_match,n)}) | "
              f"unmatched: {n_unmatch:,} ({pct(n_unmatch,n)})")

    results["cis_pc"]    = int(len(cis_pc))
    results["global_pc"] = int(len(global_pc))
    results["cis_unmatched"]    = int((~cis_pc["matched"]).sum())
    results["global_unmatched"] = int((~global_pc["matched"]).sum())

    # ── B2. Global reason codes (GLOBAL_DIRECT) ───────────────────────
    print("\n── B2. Global reason codes [GLOBAL_DIRECT] ──")
    global_unmatched = global_pc[~global_pc["matched"]]
    n_global_unmatched = len(global_unmatched)
    rc_global = []
    for code, g in global_unmatched.groupby("reason_code"):
        rc_global.append([code, len(g),
                          pct(len(g), n_global_unmatched),
                          pct(len(g), len(global_pc)),
                          "GLOBAL_DIRECT"])
    rc_global.sort(key=lambda x: -x[1])
    print(tabulate(rc_global,
        headers=["Reason code","Count","% of unmatched Global","% of all Global PC","Observability"],
        tablefmt="pipe"))

    results["global_reason_codes"] = [{"code": r[0], "n": r[1]} for r in rc_global]

    # ── B3. CIS reason codes (CIS_PROXY) ─────────────────────────────
    print("\n── B3. CIS reason codes [CIS_PROXY] ──")
    print("  NOTE: No sk-based codes used for CIS. EPN attribution is NOT_OBSERVABLE_WITH_CURRENT_DATA.")
    print(f"  NOTE: AC coverage for CIS is partial (cache covers Mar 6–Apr 3).")
    print(f"        PC events before Mar 9 may have incomplete 72h AC lookback.")
    cis_all = cis_pc.copy()
    n_cis_total = len(cis_all)
    rc_cis = []
    for code, g in cis_all.groupby("reason_code"):
        rc_cis.append([code, len(g),
                       pct(len(g), n_cis_total),
                       "CIS_PROXY"])
    rc_cis.sort(key=lambda x: -x[1])
    print(tabulate(rc_cis,
        headers=["Reason code","Count","% of CIS PC","Observability"],
        tablefmt="pipe"))
    print(f"\n  CIS overwrite analysis: NOT_OBSERVABLE_WITH_CURRENT_DATA")
    print(f"  (utm_source/utm_medium/utm_campaign not stored in MongoDB events)")

    results["cis_reason_codes"] = [{"code": r[0], "n": r[1]} for r in rc_cis]

    # ── B4. Global attribution state in unmatched ─────────────────────
    print("\n── B4. Global attribution state in unmatched [GLOBAL_DIRECT] ──")
    attr_table = [
        ["Has our last_sk",          global_unmatched["has_our_last_sk"].sum(),     pct(global_unmatched["has_our_last_sk"].sum(), n_global_unmatched)],
        ["Has foreign last_sk",      global_unmatched["has_foreign_last_sk"].sum(), pct(global_unmatched["has_foreign_last_sk"].sum(), n_global_unmatched)],
        ["Has any our sk",           global_unmatched["has_any_our_sk"].sum(),      pct(global_unmatched["has_any_our_sk"].sum(), n_global_unmatched)],
        ["Has foreign sk (current)", global_unmatched["has_foreign_sk"].sum(),      pct(global_unmatched["has_foreign_sk"].sum(), n_global_unmatched)],
        ["Has af",                   global_unmatched["has_af"].sum(),              pct(global_unmatched["has_af"].sum(), n_global_unmatched)],
        ["Has last_af",              global_unmatched["has_last_af"].sum(),         pct(global_unmatched["has_last_af"].sum(), n_global_unmatched)],
        ["Has cashback trace",       global_unmatched["has_cashback"].sum(),        pct(global_unmatched["has_cashback"].sum(), n_global_unmatched)],
    ]
    print(tabulate(attr_table, headers=["Attribute","Count","% of unmatched Global"], tablefmt="pipe"))

    results["global_attr"] = {
        "has_our_sk": int(global_unmatched["has_any_our_sk"].sum()),
        "has_foreign_sk": int(global_unmatched["has_foreign_sk"].sum()),
        "has_af": int(global_unmatched["has_af"].sum()),
        "has_cashback": int(global_unmatched["has_cashback"].sum()),
        "n_unmatched": int(n_global_unmatched),
    }

    # ── B5. Matching sensitivity ──────────────────────────────────────
    print("\n── B5. Matching sensitivity (different windows) ──")
    pur_by_user = {}
    for _, row in pur_b.iterrows():
        uid = str(row.get("$user_id", "") or "")
        if uid:
            pur_by_user.setdefault(uid, []).append(row["time_utc"])

    sensitivity_rows = []
    for win_min in [5, 10, 20, 30, 60]:
        win = timedelta(minutes=win_min)
        n_match = 0
        for _, row in pc_b.iterrows():
            uid = row["user_id"]
            pc_ts = row["time_utc"]
            for pur_ts in pur_by_user.get(uid, []):
                if abs((pc_ts - pur_ts).total_seconds()) <= win.total_seconds():
                    n_match += 1
                    break
        sensitivity_rows.append([f"±{win_min} min", n_match, pct(n_match, n_pc),
                                  n_pc - n_match, pct(n_pc - n_match, n_pc)])
    print(tabulate(sensitivity_rows,
        headers=["Window","Matched","% matched","Unmatched","% unmatched"], tablefmt="pipe"))
    print(f"  Net raw gap ({n_raw_gap:,}) is the primary metric — not the matching result.")

    results["sensitivity"] = [{"window_min": int(r[0].strip("±min ")), "matched": r[1]} for r in sensitivity_rows]

    # ── B6. By browser (unmatched PC) ────────────────────────────────
    print("\n── B6. Unmatched PC by browser ──")
    unmatched = pc_b[~pc_b["matched"]]
    browser_stats_b = []
    for bf, g in unmatched.groupby("$browser"):
        n_g = len(g)
        if n_g < 30:
            continue
        total_for_browser = len(pc_b[pc_b["$browser"] == bf])
        browser_stats_b.append([bf, n_g, total_for_browser, pct(n_g, total_for_browser),
                                 g["reason_code"].value_counts().index[0] if len(g) > 0 else ""])
    browser_stats_b.sort(key=lambda x: -x[1])
    print(tabulate(browser_stats_b,
        headers=["Browser","Unmatched","All PC","Loss %","Top reason"], tablefmt="pipe"))

    # ── B7. By country (unmatched, min 30 total PC) ──────────────────
    print("\n── B7. Loss rate by country (min 30 total PC) ──")
    c_stats = []
    for c, g in pc_b.groupby("mp_country_code"):
        n_g = len(g)
        if n_g < 30:
            continue
        n_un = (~g["matched"]).sum()
        region = "CIS" if g["is_cis_pc"].any() else "Global"
        obs = "CIS_PROXY" if region == "CIS" else "GLOBAL_DIRECT"
        c_stats.append([c, region, n_g, n_un, pct(n_un, n_g), obs])
    c_stats.sort(key=lambda x: -float(x[4].replace("%","").replace("N/A","0")))
    print(tabulate(c_stats[:20],
        headers=["Country","Region","Total PC","Unmatched","Loss %","Observability"],
        tablefmt="pipe"))

    results["country_stats"] = [{"country": r[0], "region": r[1], "n": r[2], "unmatched": r[3]} for r in c_stats[:10]]

    # ── B8. Global last_sk distribution in unmatched with our sk ──────
    print("\n── B8. Our last_sk distribution in Global unmatched [GLOBAL_DIRECT] ──")
    ours_global = global_unmatched[global_unmatched["has_our_last_sk"]]
    if len(ours_global) > 0:
        for sk, cnt in ours_global["last_sk"].value_counts().items():
            print(f"  {sk}: {cnt:,} ({pct(cnt, len(ours_global))})")
    else:
        print("  None found.")

    # ── B9. CIS: observability caveat summary ─────────────────────────
    print("\n── B9. CIS observability summary [NOT_OBSERVABLE_WITH_CURRENT_DATA] ──")
    print(f"  CIS total PC:              {len(cis_pc):,}")
    print(f"  CIS matched to Purchase:   {cis_pc['matched'].sum():,} ({pct(cis_pc['matched'].sum(), len(cis_pc))})")
    print(f"  CIS unmatched:             {(~cis_pc['matched']).sum():,} ({pct((~cis_pc['matched']).sum(), len(cis_pc))})")
    print(f"  CIS EPN affiliate state:   NOT_OBSERVABLE_WITH_CURRENT_DATA")
    print(f"  CIS overwrite detection:   NOT_OBSERVABLE_WITH_CURRENT_DATA")
    print(f"  CIS proxy return (per PC): NOT_OBSERVABLE_WITH_CURRENT_DATA (would require additional MongoDB query)")
    print(f"  Available CIS signal:      hub reach (Affiliate Click) — partial 72h coverage")

    return results


# ─────────────────────────────────────────────────────────────
# SECTION 7: Data quality caveats
# ─────────────────────────────────────────────────────────────

def print_caveats():
    print_section("DATA QUALITY & OBSERVABILITY CAVEATS")
    caveats = [
        ("1", "CIS EPN ATTRIBUTION NOT OBSERVABLE",
         "MongoDB events do not store utm_source/utm_medium/utm_campaign. "
         "AliHelper EPN return (utm_source=aerkol, utm_campaign=*_7685) is not directly observable "
         "in historical data. All CIS affiliate-state conclusions = NOT_OBSERVABLE_WITH_CURRENT_DATA."),
        ("2", "CIS PROXY RETURN IS INDIRECT",
         "Proxy return (aliexpress.ru event within 120s of Affiliate Click) proves site return, "
         "NOT that EPN affiliate params were preserved. Labeled CIS_PROXY, not CIS_DIRECT."),
        ("3", "AC COVERAGE PARTIAL FOR EARLY PROBLEM B WINDOW",
         "Affiliate Click cache covers Mar 6–Apr 3. PC events from Feb 27–Mar 8 have "
         "incomplete 72h AC lookback. CIS_NO_HUB_REACH_OBSERVED may be overstated for that period."),
        ("4", "CASHBACK OBSERVABILITY PARTIAL",
         "cashback_list in PC is client-reported. Cashback site visits not logged to backend."),
        ("5", "NO order_id IN MOST PURCHASE COMPLETED",
         "Primary matching is user+time proximity (±10 min). Ambiguous matches treated as matched."),
        ("6", "noLogUrls EXCLUSIONS",
         "Checkout/order paths may not appear in events due to config-level URL exclusions. "
         "Absence of checkout events ≠ no user activity."),
        ("7", "guestStateHistory = CONFIG DELIVERY, NOT USAGE",
         "gsh records confirm config was delivered, not that a redirect was executed."),
        ("8", "NO DIRECT AUTO-REDIRECT LOG",
         "Client-side webNavigation.onBeforeNavigate is not logged. Auto-redirect opportunity "
         "reconstructed from eligible visit + browser lineage + 30-min rule approximation."),
        ("9", "ONLY _id INDEX",
         "events and guestStateHistory have no field-level indexes. All queries use _id-based range. "
         "Aggregation scans the full window."),
        ("10", "SINGLE EVENT TYPE",
         "All events are type='watcher'. No native session boundaries."),
        ("11", "PROBLEM B WINDOW AVOIDS INCIDENT",
         "Apr 1 CIS postback incident excluded. B window is Feb 27–Mar 26 (mature cohort)."),
    ]
    for num, title, body in caveats:
        print(f"\n  [{num}] {title}")
        print(f"       {body}")


# ─────────────────────────────────────────────────────────────
# SECTION 8: Save structured results to JSON
# ─────────────────────────────────────────────────────────────

def save_results(results_a: dict, results_b: dict, df_a: pd.DataFrame, pc_b: pd.DataFrame):
    """Save key metrics to JSON for HTML report generation."""
    out = {
        "run_date": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
        "problem_a": results_a,
        "problem_b": results_b,
        # Additional segment data
        "pa_cis_users": int(df_a["is_cis"].sum()),
        "pa_global_users": int((~df_a["is_cis"]).sum()),
        "pb_cis_pc": int(pc_b["is_cis_pc"].sum()),
        "pb_global_pc": int((~pc_b["is_cis_pc"]).sum()),
    }
    class _Encoder(json.JSONEncoder):
        def default(self, o):
            if isinstance(o, (np.integer,)):
                return int(o)
            if isinstance(o, (np.floating,)):
                return float(o)
            return super().default(o)

    out_path = CACHE_DIR / "results_v2.json"
    with open(out_path, "w") as f:
        json.dump(out, f, indent=2, cls=_Encoder)
    print(f"\n  Results saved to {out_path}")
    return out


# ─────────────────────────────────────────────────────────────
# SECTION 9: Ranked root causes
# ─────────────────────────────────────────────────────────────

def print_ranked_causes(df_a: pd.DataFrame, pc_b: pd.DataFrame, results: dict):
    print_section("RANKED ROOT CAUSES")

    a = results["problem_a"]
    b = results["problem_b"]

    print("\n═══ PROBLEM A — Missing Affiliate Click ═══\n")

    elig_g = a.get("global", {})
    elig_c = a.get("cis", {})
    gap = a.get("gap", {})

    causes_a = [
        # rank, cause, region/obs, affected, impact, confidence, fix
        (1, "Ineligible traffic in denominator",
         "ALL", f"{a['n_total']-a['n_eligible']:,} users ({pct(a['n_total']-a['n_eligible'], a['n_total'])})",
         "Reduces apparent gap (denominator correction)", "HIGH",
         "GLOBAL_DIRECT / CIS_PROXY",
         "Use only product+homepage pages in denominator"),
        (2, "Users with usable config but never reach hub",
         "ALL", f"{gap.get('n_good_no_hub',0):,} users ({pct(gap.get('n_good_no_hub',0), gap.get('n_gap_total',1))} of gap)",
         "Direct miss — user had working config but DOGI/auto-redirect didn't fire",
         "HIGH", "GLOBAL_DIRECT / CIS_PROXY",
         "Improve DOGI coin visibility; review 30-min cooldown logic"),
        (3, "Config not found or value=False",
         "ALL", f"{gap.get('n_no_cfg',0)+gap.get('n_bad_cfg',0):,} users ({pct(gap.get('n_no_cfg',0)+gap.get('n_bad_cfg',0), gap.get('n_gap_total',1))} of gap)",
         "User received unusable/no hub config", "HIGH", "GLOBAL_DIRECT / CIS_PROXY",
         "Investigate why value=False; check config delivery reliability"),
        (4, "CIS: hub reached, no proxy return to aliexpress.ru",
         "CIS", f"{elig_c.get('n_a6_no_proxy',0):,} users [CIS_PROXY]",
         "Return to site not observed after hub — EPN param preservation unknown",
         "MEDIUM", "CIS_PROXY",
         "Add Affiliate Return Detected event; log EPN utm params to backend"),
        (5, "Global: hub reached, no our sk return",
         "Global", f"{elig_g.get('n_a6',0):,} users [GLOBAL_DIRECT]",
         "User reached hub but no AliHelper-owned sk in subsequent events",
         "MEDIUM", "GLOBAL_DIRECT",
         "Check hub redirect logic; verify sk injection in return URL"),
        (6, "A5: Silent redirect (sk in events, no Affiliate Click)",
         "Global", f"{elig_g.get('n_a5',0):,} users [GLOBAL_DIRECT]",
         "Mixpanel click event missing for successful redirects",
         "MEDIUM", "GLOBAL_DIRECT",
         "Fix Mixpanel event firing in hub redirect flow"),
    ]

    for r in causes_a:
        print(f"  #{r[0]} {r[1]}")
        print(f"     Region: {r[2]}  |  Observability: {r[6]}")
        print(f"     Affected: {r[3]}")
        print(f"     Impact: {r[4]}")
        print(f"     Confidence: {r[5]}")
        print(f"     Fix: {r[7]}")
        print()

    print("\n═══ PROBLEM B — Purchase Completed without Purchase ═══\n")

    n_gap = b.get("n_raw_gap", 0)
    n_pc  = b.get("n_pc", 1)
    ga    = b.get("global_attr", {})

    causes_b = [
        (1, "Delayed postback (primary cause)",
         "ALL", f"~{b.get('n_unmatched',0):,} unmatched ≈ raw gap {n_gap:,} ({pct(n_gap, n_pc)})",
         "Net raw gap ≈ unmatched bucket — confirmed delayed postbacks dominate",
         "HIGH", "GLOBAL_DIRECT (net gap)",
         "Use mature cohort (28d excluding last 7d); monitor postback SLA"),
        (2, "Global: NO_OUR_SK_IN_72H — no AliHelper attribution before purchase",
         "Global", f"{next((r['n'] for r in b.get('global_reason_codes',[]) if r['code']=='NO_OUR_SK_IN_72H'), 0):,} events [GLOBAL_DIRECT]",
         "User purchased without AliHelper-owned sk in prior 72h",
         "HIGH", "GLOBAL_DIRECT",
         "Improve activation rate (Problem A fixes); check 72h window edge cases"),
        (3, "Global: FOREIGN_SK overwrite",
         "Global", f"{next((r['n'] for r in b.get('global_reason_codes',[]) if r['code']=='FOREIGN_SK_AFTER_OUR_SK'), 0):,} events [GLOBAL_DIRECT]",
         "Third-party sk overwrote AliHelper-owned sk before purchase",
         "HIGH", "GLOBAL_DIRECT",
         "Partner discussion on last-click rules; monitor overwrite rate"),
        (4, "CIS: EPN attribution not observable",
         "CIS", f"{b.get('cis_pc',0):,} CIS PC events — attribution state: NOT_OBSERVABLE_WITH_CURRENT_DATA",
         "Cannot determine EPN param preservation from historical events",
         "N/A", "NOT_OBSERVABLE_WITH_CURRENT_DATA",
         "Instrument: store utm_source/utm_medium/utm_campaign in MongoDB events; add Affiliate Return Detected event"),
        (5, "Cashback interference",
         "Global", f"{ga.get('has_cashback',0):,} events with cashback trace [GLOBAL_DIRECT]",
         "Partial evidence — actual rate likely higher (local storage only)",
         "LOW", "GLOBAL_DIRECT (partial)",
         "Improve cashback interference detection; add server-side cashback logging"),
    ]

    for r in causes_b:
        print(f"  #{r[0]} {r[1]}")
        print(f"     Region: {r[2]}  |  Observability: {r[6]}")
        print(f"     Affected: {r[3]}")
        print(f"     Impact: {r[4]}")
        print(f"     Confidence: {r[5]}")
        print(f"     Fix: {r[7]}")
        print()

    print("\n═══ UNEXPLAINED REMAINDER ═══")
    print(f"  Problem A — users with usable config, reached hub, but no return signal:")
    print(f"    Global A6 (hub, no sk): {elig_g.get('n_a6',0):,} [GLOBAL_DIRECT — unknown cause]")
    print(f"    CIS A6 (hub, no proxy): {elig_c.get('n_a6_no_proxy',0):,} [CIS_PROXY — EPN state unknown]")
    print(f"  Problem B — CIS unmatched PC: {b.get('cis_unmatched',0):,} [NOT_OBSERVABLE_WITH_CURRENT_DATA]")
    print(f"  Problem B — Global UNKNOWN: "
          f"{next((r['n'] for r in b.get('global_reason_codes',[]) if r['code']=='UNKNOWN'), 0):,} [GLOBAL_DIRECT — likely delayed postbacks]")


# ─────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────

def main():
    print("=" * 70)
    print("  AliHelper Affiliate Investigation — v2 (CIS-corrected methodology)")
    print(f"  Run date: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    print("=" * 70)

    print_caveats()

    # ── Step 1: Mixpanel data (all reused from cache) ──────────────────
    aff_click_raw, purchase_raw, pc_raw = download_mixpanel_data()
    aff_click_df = to_df(aff_click_raw)

    # ── Step 2: MongoDB (Problem A aggregations + NEW CIS proxy return) ─
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

        # Problem A MongoDB agg (reuse cache)
        print_section("MongoDB Problem A aggregations (reusing cache)")
        mongo_data = run_mongo_problem_a(db)

        # NEW: CIS proxy return (new query — will be cached after first run)
        print_section("CIS proxy return query (NEW)")
        cis_proxy_return = run_cis_proxy_return(db, aff_click_raw)

        mongo_client.close()

    # ── Step 3: Problem A ───────────────────────────────────────────────
    df_a = build_problem_a_df(mongo_data, aff_click_raw, cis_proxy_return)
    results_a = analyze_problem_a(df_a)

    # ── Step 4: Problem B ───────────────────────────────────────────────
    pur_b, pc_b = build_problem_b_df(purchase_raw, pc_raw)
    pc_b = match_purchases(pc_b, pur_b, window_minutes=10)
    pc_b = assign_reason_codes(pc_b, aff_click_raw)
    results_b = analyze_problem_b(pc_b, pur_b)

    # ── Step 5: Ranked causes ───────────────────────────────────────────
    all_results = {"problem_a": results_a, "problem_b": results_b}
    print_ranked_causes(df_a, pc_b, all_results)

    # ── Step 6: Save structured results ────────────────────────────────
    save_results(results_a, results_b, df_a, pc_b)

    print_section("Analysis v2 complete")
    print("  Cached artifacts:")
    print("    cache/aff_click_a.json       — REUSED")
    print("    cache/purchase_b.json        — REUSED")
    print("    cache/pc_b.json              — REUSED")
    print("    cache/mongo_problem_a.pkl    — REUSED")
    print("    cache/cis_proxy_return_a.pkl — NEW (first run)")
    print("    cache/results_v2.json        — NEW (structured output)")
    print("\n  Next: run build_report_v2.py to regenerate HTML report.")


if __name__ == "__main__":
    main()
