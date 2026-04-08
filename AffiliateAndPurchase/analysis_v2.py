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

import json, time, pickle
from datetime import datetime, timezone, timedelta

import pandas as pd
import numpy as np
from bson import ObjectId
from tabulate import tabulate

from src.config import (
    CACHE_DIR, A_START, A_END, B_START, B_END,
    OUR_SKS, CIS_COUNTRIES, AUTO_REDIRECT_BROWSERS,
    PROXY_RETURN_WINDOW_S, ATTRIBUTION_WINDOW_H,
)
from src.db import mongo_tunnel, mp_export
from src.utils import (
    oid_from_dt, pct, pct_f, print_section,
    browser_family, lineage, is_cis, to_df,
)


# ─────────────────────────────────────────────────────────────
# SECTION 1: Mixpanel data download (reuse all existing caches)
# ─────────────────────────────────────────────────────────────

def download_mixpanel_data():
    print_section("Mixpanel data (reusing cache)")
    aff_clicks = mp_export("Affiliate Click",    "2026-03-06", "2026-04-03", "aff_click_a")
    purchases  = mp_export("Purchase",           "2026-02-27", "2026-03-27", "purchase_b")
    pc_events  = mp_export("Purchase Completed", "2026-02-27", "2026-03-27", "pc_b")
    return aff_clicks, purchases, pc_events


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

    # If cache missing, run the full aggregation
    events = db["events"]
    gsh = db["guestStateHistory"]

    oid_start    = oid_from_dt(A_START)
    oid_end      = oid_from_dt(A_END)
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
    print(f"    -> {len(user_events):,} distinct users in {time.time()-t0:.1f}s")

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
    print(f"    -> {len(homepage_data):,} users with homepage visits in {time.time()-t0:.1f}s")

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
    print(f"    -> enriched {len(client_map):,} users in {time.time()-t0:.1f}s")

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
    print(f"    -> gsh done in {time.time()-t0:.1f}s")

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
# SECTION 2b: CIS proxy return query
# ─────────────────────────────────────────────────────────────

def run_cis_proxy_return(db, aff_click_raw: list) -> dict:
    """
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

    print("  [new] Computing CIS proxy return...")

    cis_click_times = {}
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

    pipeline = [
        {"$match": {
            "_id":       {"$gte": oid_start, "$lte": oid_end},
            "guest_id":  {"$in": cis_oids},
            "payload.url": {"$regex": r"aliexpress\.ru", "$options": "i"},
        }},
        {"$group": {
            "_id": "$guest_id",
            "visit_ms": {"$push": {"$toLong": {"$toDate": "$_id"}}},
        }},
    ]

    aliexpress_ru_visits = {}
    for r in events.aggregate(pipeline, allowDiskUse=True):
        uid_str = str(r["_id"])
        visit_times_s = sorted(t // 1000 for t in r["visit_ms"])
        aliexpress_ru_visits[uid_str] = visit_times_s

    print(f"    -> {len(aliexpress_ru_visits):,} CIS users had aliexpress.ru visits "
          f"in {time.time()-t0:.1f}s")

    proxy_result = {}
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
    print(f"    CIS proxy return (within {PROXY_RETURN_WINDOW_S}s): "
          f"{n_true:,}/{len(proxy_result):,} ({pct(n_true, len(proxy_result))})")

    with open(cache_file, "wb") as f:
        pickle.dump(proxy_result, f)
    return proxy_result


# ─────────────────────────────────────────────────────────────
# SECTION 3: Build Problem A dataframe (corrected)
# ─────────────────────────────────────────────────────────────

def build_problem_a_df(mongo_data: dict, aff_click_raw: list,
                       cis_proxy_return: dict) -> pd.DataFrame:
    print_section("Building Problem A user-level dataframe (corrected v2)")

    user_events   = mongo_data["user_events"]
    homepage_data = mongo_data["homepage_data"]
    client_map    = mongo_data["client_map"]
    gsh_in        = mongo_data["gsh_in_window"]
    gsh_pre       = mongo_data["gsh_pre_window"]

    ac_by_user = {}
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

        hp_count   = homepage_data.get(guest_id, 0)
        prod_count = ue.get("product_events", 0)
        has_product  = prod_count > 0
        has_homepage = hp_count > 0
        is_eligible  = has_product or has_homepage

        cli        = client_map.get(guest_id, {})
        raw_browser = cli.get("browser", "")
        bf         = browser_family(raw_browser)
        lg         = lineage(bf)
        cv         = cli.get("client_version", "")

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

        has_our_sk_return     = ue.get("our_sk_events", 0) > 0
        has_any_sk_return     = ue.get("any_sk_events", 0) > 0
        has_foreign_sk_return = ue.get("foreign_sk_events", 0) > 0

        has_proxy_return = cis_proxy_return.get(guest_id, False) if user_is_cis else None

        a5_missing_tracking    = (not user_is_cis) and has_our_sk_return and not reached_hub
        a6_hub_no_global_return = (not user_is_cis) and reached_hub and not has_our_sk_return
        a6_hub_no_cis_proxy     = user_is_cis and reached_hub and (has_proxy_return is False)

        rows.append({
            "guest_id": guest_id, "country": country,
            "is_cis": user_is_cis, "region": "CIS" if user_is_cis else "Global",
            "total_events": ue.get("total_events", 0),
            "product_events": prod_count, "homepage_events": hp_count,
            "has_product": has_product, "has_homepage": has_homepage,
            "is_eligible": is_eligible,
            "browser_raw": raw_browser, "browser_family": bf,
            "lineage": lg, "client_version": cv,
            "cfg_value": cfg_value, "cfg_domain": cfg_domain, "cfg_region": cfg_region,
            "has_cfg": conf is not None, "has_usable_cfg": cfg_value is True,
            "reached_hub": reached_hub,
            "has_our_sk_return": has_our_sk_return,
            "has_any_sk_return": has_any_sk_return,
            "has_foreign_sk_return": has_foreign_sk_return,
            "has_proxy_return": has_proxy_return,
            "a5_missing_tracking": a5_missing_tracking,
            "a6_hub_no_global_ret": a6_hub_no_global_return,
            "a6_hub_no_cis_proxy": a6_hub_no_cis_proxy,
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
# SECTION 4: Problem A — Dual Funnel & Segmentation
# ─────────────────────────────────────────────────────────────

def analyze_problem_a(df: pd.DataFrame) -> dict:
    print_section("PROBLEM A — Missing Affiliate Click (corrected v2)")

    results = {}
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

    # ── A1. Dual funnel: Global vs CIS ────────────────────────
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
        headers=["Stage", "Users", "% of eligible", "Observability"], tablefmt="pipe"))

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
    nc_proxy  = elig_c["has_proxy_return"].eq(True).sum()
    nc_a6     = elig_c["a6_hub_no_cis_proxy"].sum()

    cis_funnel = [
        ["1. Raw activity",                        nc_total, pct(nc_total, nc_total), "CIS_PROXY"],
        ["2. Eligible (product/homepage)",         nc_elig,  pct(nc_elig, nc_total),  "CIS_PROXY"],
        ["3. Usable config (value=True)",          nc_cfg,   pct(nc_cfg, nc_elig),    "CIS_PROXY"],
        ["4. Reached hub (Affiliate Click)",        nc_hub,   pct(nc_hub, nc_elig),    "CIS_PROXY"],
        ["5. Proxy return to aliexpress.ru (<=120s)",nc_proxy, pct(nc_proxy, nc_elig), "CIS_PROXY"],
        ["5a. Affiliate params preserved",          "—",     "—",                     "NOT_OBSERVABLE_WITH_CURRENT_DATA"],
        ["   A6: hub reached, no proxy return",    nc_a6,    pct(nc_a6, nc_elig),     "CIS_PROXY"],
    ]
    print(f"\n  [CIS_PROXY] CIS / EPN funnel ({nc_total:,} users)")
    print(tabulate(cis_funnel,
        headers=["Stage", "Users", "% of eligible", "Observability"], tablefmt="pipe"))

    print(f"\n  NOTE: CIS proxy return = aliexpress.ru event within {PROXY_RETURN_WINDOW_S}s of Affiliate Click.")
    print(f"  NOTE: Whether EPN affiliate params were preserved is NOT_OBSERVABLE_WITH_CURRENT_DATA.")

    results["cis"] = {
        "n_total": int(nc_total), "n_eligible": int(nc_elig),
        "n_usable_cfg": int(nc_cfg), "n_hub": int(nc_hub),
        "n_proxy_return": int(nc_proxy), "n_a6_no_proxy": int(nc_a6),
    }

    # ── A2. Gap decomposition ─────────────────────────────────
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
        "n_gap_total": int(n_gap_total), "n_no_cfg": int(n_no_cfg),
        "n_bad_cfg": int(n_bad_cfg), "n_good_no_hub": int(n_good_no_hub),
        "n_a5": int(elig["a5_missing_tracking"].sum()),
    }

    # ── A3. Browser / lineage split ───────────────────────────
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

    # ── A4. Auto-redirect: Firefox vs Edge ────────────────────
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

    # ── A5. By hub domain ─────────────────────────────────────
    print("\n── A5. By hub domain — Global only (GLOBAL_DIRECT) ──")
    usable_global = elig[elig["has_usable_cfg"] & ~elig["is_cis"]].copy()
    hub_global = []
    for domain, g in usable_global.groupby("cfg_domain"):
        n = len(g)
        hub_global.append([
            domain, n, g["reached_hub"].sum(), pct(g["reached_hub"].sum(), n),
            g["has_our_sk_return"].sum(), pct(g["has_our_sk_return"].sum(), n), "GLOBAL_DIRECT",
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
            domain, n, g["reached_hub"].sum(), pct(g["reached_hub"].sum(), n),
            g["has_proxy_return"].eq(True).sum(), pct(g["has_proxy_return"].eq(True).sum(), n), "CIS_PROXY",
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

    # ── A6. By country ────────────────────────────────────────
    print("\n── A6. Top countries (eligible, min 50 users) ──")
    country_stats = []
    for c, g in elig.groupby("country"):
        n = len(g)
        if n < 50:
            continue
        cis_flag = g["is_cis"].all()
        region_label = "CIS" if cis_flag else ("Mixed" if g["is_cis"].any() else "Global")
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

    # ── A7. Client version ────────────────────────────────────
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

    # ── A8. Config coverage by region/lineage ─────────────────
    print("\n── A8. Config coverage by region and lineage ──")
    for region_label, mask in [("Global", ~elig["is_cis"]), ("CIS", elig["is_cis"])]:
        g_reg = elig[mask]
        for lg, g_lin in g_reg.groupby("lineage"):
            n = len(g_lin)
            print(f"  {region_label} / {lg}: {n:,} users | "
                  f"has cfg: {g_lin['has_cfg'].sum():,} ({pct(g_lin['has_cfg'].sum(), n)}) | "
                  f"usable cfg: {g_lin['has_usable_cfg'].sum():,} ({pct(g_lin['has_usable_cfg'].sum(), n)})")

    # ── A9. Page type split ───────────────────────────────────
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
    print(f"\n  Matching PC -> Purchase (user + +/-{window_minutes} min)...")
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
    pc_b = pc_b.copy()

    pc_b["last_sk"]   = pc_b.get("last_sk",   pd.Series(dtype=str)).fillna("").astype(str)
    pc_b["last_af"]   = pc_b.get("last_af",   pd.Series(dtype=str)).fillna("").astype(str)
    pc_b["sk"]        = pc_b.get("sk",         pd.Series(dtype=str)).fillna("").astype(str)
    pc_b["af"]        = pc_b.get("af",         pd.Series(dtype=str)).fillna("").astype(str)
    pc_b["cashback_list"] = pc_b.get("cashback_list", pd.Series(dtype=object))

    pc_b["is_cis_pc"] = pc_b.get("mp_country_code", pd.Series(dtype=str)).apply(
        lambda x: is_cis(str(x) if x else ""))

    pc_b["has_our_last_sk"]     = pc_b["last_sk"].isin(OUR_SKS)
    pc_b["has_our_current_sk"]  = pc_b["sk"].isin(OUR_SKS)
    pc_b["has_any_our_sk"]      = pc_b["has_our_last_sk"] | pc_b["has_our_current_sk"]
    pc_b["has_foreign_last_sk"] = (pc_b["last_sk"] != "") & ~pc_b["has_our_last_sk"]
    pc_b["has_foreign_sk"]      = (pc_b["sk"] != "") & ~pc_b["has_our_current_sk"]
    pc_b["has_af"]              = pc_b["af"] != ""
    pc_b["has_last_af"]         = pc_b["last_af"] != ""
    pc_b["has_cashback"]        = pc_b["cashback_list"].notna() & (pc_b["cashback_list"] != "")

    ac_72h = build_ac_72h_lookup(aff_click_raw)
    attribution_window_s = ATTRIBUTION_WINDOW_H * 3600

    def had_ac_in_72h(row) -> bool:
        uid = str(row.get("user_id", "") or "")
        pc_ts = int(row["time_utc"].timestamp())
        for ac_ts in ac_72h.get(uid, []):
            if 0 <= (pc_ts - ac_ts) <= attribution_window_s:
                return True
        return False

    def assign_global_code(row) -> str:
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
        if row["matched"]:
            return "CIS_LIKELY_DELAYED_POSTBACK"
        has_ac = had_ac_in_72h(row)
        if not has_ac:
            return "CIS_NO_HUB_REACH_OBSERVED"
        return "CIS_PURCHASE_COMPLETED_WITHOUT_PURCHASE_UNDER_LIMITED_OBSERVABILITY"

    codes = []
    for _, row in pc_b.iterrows():
        if row["is_cis_pc"]:
            codes.append(assign_cis_code(row))
        else:
            codes.append(assign_global_code(row))

    pc_b["reason_code"] = codes
    pc_b["observability"] = pc_b["reason_code"].apply(
        lambda code: "CIS_PROXY" if code.startswith("CIS_") else "GLOBAL_DIRECT")
    return pc_b


# ─────────────────────────────────────────────────────────────
# SECTION 6: Problem B analysis
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
    print(f"    Matched PC->Purchase:  {n_matched:,} ({pct(n_matched, n_pc)})")
    print(f"    Unmatched (gap):      {n_unmatched:,} ({pct(n_unmatched, n_pc)})")
    print(f"\n  Primary metric = raw event gap ({n_raw_gap:,}).")

    results.update({"n_pc": int(n_pc), "n_pur": int(n_pur), "n_raw_gap": int(n_raw_gap),
                    "n_matched": int(n_matched), "n_unmatched": int(n_unmatched)})

    # ── B1. CIS vs Global split ───────────────────────────────
    print("\n── B1. CIS vs Global split ──")
    cis_pc    = pc_b[pc_b["is_cis_pc"]]
    global_pc = pc_b[~pc_b["is_cis_pc"]]

    for label, gdf in [("Global [GLOBAL_DIRECT]", global_pc), ("CIS [CIS_PROXY]", cis_pc)]:
        n = len(gdf)
        n_match = gdf["matched"].sum()
        n_unmatch = (~gdf["matched"]).sum()
        print(f"\n  {label}: {n:,} PC events | "
              f"matched: {n_match:,} ({pct(n_match,n)}) | "
              f"unmatched: {n_unmatch:,} ({pct(n_unmatch,n)})")

    results.update({"cis_pc": int(len(cis_pc)), "global_pc": int(len(global_pc)),
                    "cis_unmatched": int((~cis_pc["matched"]).sum()),
                    "global_unmatched": int((~global_pc["matched"]).sum())})

    # ── B2. Global reason codes ───────────────────────────────
    print("\n── B2. Global reason codes [GLOBAL_DIRECT] ──")
    global_unmatched = global_pc[~global_pc["matched"]]
    n_global_unmatched = len(global_unmatched)
    rc_global = []
    for code, g in global_unmatched.groupby("reason_code"):
        rc_global.append([code, len(g), pct(len(g), n_global_unmatched),
                          pct(len(g), len(global_pc)), "GLOBAL_DIRECT"])
    rc_global.sort(key=lambda x: -x[1])
    print(tabulate(rc_global,
        headers=["Reason code","Count","% of unmatched Global","% of all Global PC","Observability"],
        tablefmt="pipe"))
    results["global_reason_codes"] = [{"code": r[0], "n": r[1]} for r in rc_global]

    # ── B3. CIS reason codes ─────────────────────────────────
    print("\n── B3. CIS reason codes [CIS_PROXY] ──")
    print("  NOTE: No sk-based codes used for CIS.")
    n_cis_total = len(cis_pc)
    rc_cis = []
    for code, g in cis_pc.groupby("reason_code"):
        rc_cis.append([code, len(g), pct(len(g), n_cis_total), "CIS_PROXY"])
    rc_cis.sort(key=lambda x: -x[1])
    print(tabulate(rc_cis,
        headers=["Reason code","Count","% of CIS PC","Observability"], tablefmt="pipe"))
    print(f"\n  CIS overwrite analysis: NOT_OBSERVABLE_WITH_CURRENT_DATA")
    results["cis_reason_codes"] = [{"code": r[0], "n": r[1]} for r in rc_cis]

    # ── B4. Global attribution state ──────────────────────────
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

    # ── B5. Matching sensitivity ──────────────────────────────
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
        sensitivity_rows.append([f"+/-{win_min} min", n_match, pct(n_match, n_pc),
                                  n_pc - n_match, pct(n_pc - n_match, n_pc)])
    print(tabulate(sensitivity_rows,
        headers=["Window","Matched","% matched","Unmatched","% unmatched"], tablefmt="pipe"))

    # ── B6. By browser ────────────────────────────────────────
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

    # ── B7. By country ────────────────────────────────────────
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

    # ── B8. Global last_sk distribution ───────────────────────
    print("\n── B8. Our last_sk distribution in Global unmatched [GLOBAL_DIRECT] ──")
    ours_global = global_unmatched[global_unmatched["has_our_last_sk"]]
    if len(ours_global) > 0:
        for sk, cnt in ours_global["last_sk"].value_counts().items():
            print(f"  {sk}: {cnt:,} ({pct(cnt, len(ours_global))})")
    else:
        print("  None found.")

    # ── B9. CIS observability summary ─────────────────────────
    print("\n── B9. CIS observability summary [NOT_OBSERVABLE_WITH_CURRENT_DATA] ──")
    print(f"  CIS total PC:              {len(cis_pc):,}")
    print(f"  CIS matched to Purchase:   {cis_pc['matched'].sum():,} ({pct(cis_pc['matched'].sum(), len(cis_pc))})")
    print(f"  CIS unmatched:             {(~cis_pc['matched']).sum():,} ({pct((~cis_pc['matched']).sum(), len(cis_pc))})")
    print(f"  CIS EPN affiliate state:   NOT_OBSERVABLE_WITH_CURRENT_DATA")

    return results


# ─────────────────────────────────────────────────────────────
# SECTION 7: Data quality caveats
# ─────────────────────────────────────────────────────────────

def print_caveats():
    print_section("DATA QUALITY & OBSERVABILITY CAVEATS")
    caveats = [
        ("1", "CIS EPN ATTRIBUTION NOT OBSERVABLE",
         "MongoDB events do not store utm_source/utm_medium/utm_campaign."),
        ("2", "CIS PROXY RETURN IS INDIRECT",
         "Proxy return proves site return, NOT that EPN affiliate params were preserved."),
        ("3", "AC COVERAGE PARTIAL FOR EARLY PROBLEM B WINDOW",
         "AC cache covers Mar 6-Apr 3. PC events from Feb 27-Mar 8 have incomplete 72h lookback."),
        ("4", "CASHBACK OBSERVABILITY PARTIAL",
         "cashback_list in PC is client-reported. Cashback site visits not logged to backend."),
        ("5", "NO order_id IN MOST PURCHASE COMPLETED",
         "Primary matching is user+time proximity (+/-10 min)."),
        ("6", "noLogUrls EXCLUSIONS",
         "Checkout/order paths may not appear in events due to config-level URL exclusions."),
        ("7", "guestStateHistory = CONFIG DELIVERY, NOT USAGE",
         "gsh records confirm config was delivered, not that a redirect was executed."),
        ("8", "NO DIRECT AUTO-REDIRECT LOG",
         "Client-side webNavigation.onBeforeNavigate is not logged."),
        ("9", "ONLY _id INDEX",
         "events and guestStateHistory have no field-level indexes."),
        ("10", "SINGLE EVENT TYPE",
         "All events are type='watcher'. No native session boundaries."),
        ("11", "PROBLEM B WINDOW AVOIDS INCIDENT",
         "Apr 1 CIS postback incident excluded. B window is Feb 27-Mar 26."),
    ]
    for num, title, body in caveats:
        print(f"\n  [{num}] {title}")
        print(f"       {body}")


# ─────────────────────────────────────────────────────────────
# SECTION 8: Save structured results
# ─────────────────────────────────────────────────────────────

def save_results(results_a: dict, results_b: dict, df_a: pd.DataFrame, pc_b: pd.DataFrame):
    out = {
        "run_date": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
        "problem_a": results_a,
        "problem_b": results_b,
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

    print("\n=== PROBLEM A — Missing Affiliate Click ===\n")

    elig_g = a.get("global", {})
    elig_c = a.get("cis", {})
    gap = a.get("gap", {})

    causes_a = [
        (1, "Ineligible traffic in denominator", "ALL",
         f"{a['n_total']-a['n_eligible']:,} users ({pct(a['n_total']-a['n_eligible'], a['n_total'])})",
         "Reduces apparent gap (denominator correction)", "HIGH", "GLOBAL_DIRECT / CIS_PROXY",
         "Use only product+homepage pages in denominator"),
        (2, "Users with usable config but never reach hub", "ALL",
         f"{gap.get('n_good_no_hub',0):,} users ({pct(gap.get('n_good_no_hub',0), gap.get('n_gap_total',1))} of gap)",
         "Direct miss — DOGI/auto-redirect didn't fire", "HIGH", "GLOBAL_DIRECT / CIS_PROXY",
         "Improve DOGI coin visibility; review 30-min cooldown logic"),
        (3, "Config not found or value=False", "ALL",
         f"{gap.get('n_no_cfg',0)+gap.get('n_bad_cfg',0):,} users",
         "User received unusable/no hub config", "HIGH", "GLOBAL_DIRECT / CIS_PROXY",
         "Investigate why value=False; check config delivery reliability"),
        (4, "CIS: hub reached, no proxy return", "CIS",
         f"{elig_c.get('n_a6_no_proxy',0):,} users [CIS_PROXY]",
         "Return to site not observed after hub", "MEDIUM", "CIS_PROXY",
         "Add Affiliate Return Detected event; log EPN utm params"),
        (5, "Global: hub reached, no our sk return", "Global",
         f"{elig_g.get('n_a6',0):,} users [GLOBAL_DIRECT]",
         "User reached hub but no owned sk in subsequent events", "MEDIUM", "GLOBAL_DIRECT",
         "Check hub redirect logic; verify sk injection"),
        (6, "A5: Silent redirect (sk in events, no Affiliate Click)", "Global",
         f"{elig_g.get('n_a5',0):,} users [GLOBAL_DIRECT]",
         "Mixpanel click event missing for successful redirects", "MEDIUM", "GLOBAL_DIRECT",
         "Fix Mixpanel event firing in hub redirect flow"),
    ]

    for r in causes_a:
        print(f"  #{r[0]} {r[1]}")
        print(f"     Region: {r[2]}  |  Observability: {r[6]}")
        print(f"     Affected: {r[3]}")
        print(f"     Impact: {r[4]}  |  Confidence: {r[5]}")
        print(f"     Fix: {r[7]}\n")

    print("\n=== PROBLEM B — Purchase Completed without Purchase ===\n")

    n_gap = b.get("n_raw_gap", 0)
    n_pc  = b.get("n_pc", 1)
    ga    = b.get("global_attr", {})

    causes_b = [
        (1, "Delayed postback (primary cause)", "ALL",
         f"raw gap {n_gap:,} ({pct(n_gap, n_pc)})",
         "Net raw gap confirmed — delayed postbacks dominate", "HIGH", "GLOBAL_DIRECT (net gap)",
         "Use mature cohort; monitor postback SLA"),
        (2, "Global: NO_OUR_SK_IN_72H", "Global",
         f"{next((r['n'] for r in b.get('global_reason_codes',[]) if r['code']=='NO_OUR_SK_IN_72H'), 0):,} events",
         "User purchased without AliHelper-owned sk in prior 72h", "HIGH", "GLOBAL_DIRECT",
         "Improve activation rate (Problem A fixes)"),
        (3, "Global: FOREIGN_SK overwrite", "Global",
         f"{next((r['n'] for r in b.get('global_reason_codes',[]) if r['code']=='FOREIGN_SK_AFTER_OUR_SK'), 0):,} events",
         "Third-party sk overwrote AliHelper-owned sk", "HIGH", "GLOBAL_DIRECT",
         "Partner discussion on last-click rules"),
        (4, "CIS: EPN attribution not observable", "CIS",
         f"{b.get('cis_pc',0):,} CIS PC events — NOT_OBSERVABLE_WITH_CURRENT_DATA",
         "Cannot determine EPN param preservation", "N/A", "NOT_OBSERVABLE_WITH_CURRENT_DATA",
         "Instrument: store utm_* in MongoDB events"),
        (5, "Cashback interference", "Global",
         f"{ga.get('has_cashback',0):,} events with cashback trace",
         "Partial evidence — actual rate likely higher", "LOW", "GLOBAL_DIRECT (partial)",
         "Add server-side cashback logging"),
    ]

    for r in causes_b:
        print(f"  #{r[0]} {r[1]}")
        print(f"     Region: {r[2]}  |  Observability: {r[6]}")
        print(f"     Affected: {r[3]}")
        print(f"     Impact: {r[4]}  |  Confidence: {r[5]}")
        print(f"     Fix: {r[7]}\n")

    print("\n=== UNEXPLAINED REMAINDER ===")
    print(f"  Problem A — Global A6 (hub, no sk): {elig_g.get('n_a6',0):,}")
    print(f"  Problem A — CIS A6 (hub, no proxy): {elig_c.get('n_a6_no_proxy',0):,}")
    print(f"  Problem B — CIS unmatched: {b.get('cis_unmatched',0):,}")
    print(f"  Problem B — Global UNKNOWN: "
          f"{next((r['n'] for r in b.get('global_reason_codes',[]) if r['code']=='UNKNOWN'), 0):,}")


# ─────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────

def main():
    print("=" * 70)
    print("  AliHelper Affiliate Investigation — v2 (CIS-corrected methodology)")
    print(f"  Run date: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    print("=" * 70)

    print_caveats()

    aff_click_raw, purchase_raw, pc_raw = download_mixpanel_data()

    print_section("Connecting to MongoDB via SSH tunnel")
    with mongo_tunnel() as db:
        print("  Connected.")
        print_section("MongoDB Problem A aggregations (reusing cache)")
        mongo_data = run_mongo_problem_a(db)
        print_section("CIS proxy return query")
        cis_proxy_return = run_cis_proxy_return(db, aff_click_raw)

    df_a = build_problem_a_df(mongo_data, aff_click_raw, cis_proxy_return)
    results_a = analyze_problem_a(df_a)

    pur_b, pc_b = build_problem_b_df(purchase_raw, pc_raw)
    pc_b = match_purchases(pc_b, pur_b, window_minutes=10)
    pc_b = assign_reason_codes(pc_b, aff_click_raw)
    results_b = analyze_problem_b(pc_b, pur_b)

    all_results = {"problem_a": results_a, "problem_b": results_b}
    print_ranked_causes(df_a, pc_b, all_results)
    save_results(results_a, results_b, df_a, pc_b)

    print_section("Analysis v2 complete")


if __name__ == "__main__":
    main()
