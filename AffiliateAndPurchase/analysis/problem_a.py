#!/usr/bin/env python3
"""
Problem A — Missing Affiliate Click.

Why do many AliExpress users not generate Affiliate Click?

Funnel:
  1. Raw AliExpress activity
  2. Eligible product pages (per flow-specific rules)
  3. Eligible with usable latest config
  4. Reached hub (Affiliate Click)
  5. Returned with affiliate markers (Global: sk, CIS: UTM)

Usage:
    python -m analysis.problem_a
"""

import pickle
from collections import defaultdict
from datetime import timedelta

import pandas as pd
from tabulate import tabulate

from src.config import (
    CACHE_DIR, A_START, A_END,
    PROXY_RETURN_WINDOW_S, MP_TZ_OFFSET_H,
)
from src.utils import (
    print_section, pct, pct_f, fmt,
    browser_family, lineage, is_cis, region_label,
    is_our_sk, has_foreign_sk, has_af,
    is_alihelper_utm, is_foreign_utm,
    matches_check_list_urls, is_eligible_product_page,
    mp_to_df, is_aliexpress_ru,
)


# ── Data loading ─────────────────────────────────────────────────────────────

def _load_pkl(name):
    with open(CACHE_DIR / f"{name}.pkl", "rb") as f:
        return pickle.load(f)


def _load_json(name):
    import json
    with open(CACHE_DIR / f"{name}.json") as f:
        return json.load(f)


# ── Client enrichment ────────────────────────────────────────────────────────

def enrich_with_clients(events: pd.DataFrame, clients: pd.DataFrame) -> pd.DataFrame:
    """
    Enrich events with client info. One user may have multiple clients;
    we take the latest client per guest_id (by last record).
    """
    # Deduplicate clients: keep last per guest_id
    cl = clients.drop_duplicates(subset="guest_id", keep="last")
    cl = cl.set_index("guest_id")[["browser", "country", "client_version", "os"]]
    return events.join(cl, on="guest_id", how="left")


# ── Latest config lookup ─────────────────────────────────────────────────────

def build_latest_config(gsh: pd.DataFrame, events: pd.DataFrame) -> dict:
    """
    For each user-day, find the latest guestStateHistory snapshot before
    the user's first eligible event that day.
    Returns dict: guest_id -> {domain, value, config_ts}
    """
    # For simplicity in the user-level analysis, find the latest config
    # for each guest_id before A_END.
    gsh_sorted = gsh.sort_values("config_ts")
    latest = gsh_sorted.drop_duplicates(subset="guest_id", keep="last")
    return latest.set_index("guest_id")[["domain", "value", "config_ts"]].to_dict("index")


# ── Core analysis ────────────────────────────────────────────────────────────

def analyze(events_a: pd.DataFrame, clients: pd.DataFrame,
            gsh: pd.DataFrame, ac_raw: list[dict]) -> dict:
    """Run Problem A analysis. Returns results dict."""

    print_section("Problem A — Missing Affiliate Click")

    # ── Step 1: Enrich events ───────────────────────────────────────────
    print("\n[1] Enriching events with client data...")
    ev = enrich_with_clients(events_a, clients)
    ev["browser_fam"] = ev["browser"].apply(browser_family)
    ev["lineage"] = ev["browser_fam"].apply(lineage)
    ev["region"] = ev["country"].apply(region_label)
    ev["is_cis"] = ev["country"].apply(is_cis)

    total_events = len(ev)
    total_users = ev["guest_id"].nunique()
    print(f"  Total events: {fmt(total_events)}")
    print(f"  Total users:  {fmt(total_users)}")

    # ── Step 2: Eligible pages ──────────────────────────────────────────
    print("\n[2] Classifying eligible pages...")
    ev["eligible"] = ev.apply(
        lambda r: (matches_check_list_urls(r["url"])
                   if r["lineage"] == "auto-redirect"
                   else is_eligible_product_page(r["product_id"])),
        axis=1,
    )
    eligible_events = ev["eligible"].sum()
    print(f"  Eligible events: {fmt(eligible_events)} ({pct(eligible_events, total_events)})")

    # ── Step 3: User-level aggregation ──────────────────────────────────
    print("\n[3] Building user-level aggregation...")

    # Global affiliate return: our sk found in querySk
    ev["has_our_sk"] = ev["query_sk"].apply(is_our_sk)
    # CIS affiliate return: our UTM in URL on aliexpress.ru
    ev["has_our_utm"] = ev.apply(
        lambda r: is_alihelper_utm(r["url"]) if r["is_cis"] else False, axis=1)
    # Foreign affiliate markers
    ev["has_foreign_sk"] = ev["query_sk"].apply(has_foreign_sk)
    ev["has_foreign_utm"] = ev.apply(
        lambda r: is_foreign_utm(r["url"]) if r["is_cis"] else False, axis=1)
    ev["has_af"] = ev["query_sk"].apply(has_af)

    user_agg = ev.groupby("guest_id").agg(
        total_events=("eligible", "count"),
        eligible_events=("eligible", "sum"),
        has_our_sk=("has_our_sk", "any"),
        has_our_utm=("has_our_utm", "any"),
        has_foreign_sk=("has_foreign_sk", "any"),
        has_foreign_utm=("has_foreign_utm", "any"),
        has_af=("has_af", "any"),
        country=("country", "first"),
        browser_fam=("browser_fam", "first"),
        lineage=("lineage", "first"),
        region=("region", "first"),
        is_cis=("is_cis", "first"),
        client_version=("client_version", "first"),
    ).reset_index()

    user_agg["is_eligible"] = user_agg["eligible_events"] > 0

    # ── Step 4: Latest config ───────────────────────────────────────────
    print("\n[4] Matching latest config per user...")
    config_map = build_latest_config(gsh, ev)
    user_agg["cfg_domain"] = user_agg["guest_id"].map(
        lambda g: config_map.get(g, {}).get("domain", ""))
    user_agg["cfg_value"] = user_agg["guest_id"].map(
        lambda g: config_map.get(g, {}).get("value"))
    user_agg["has_usable_config"] = user_agg["cfg_value"] == True  # noqa: E712

    # ── Step 5: Affiliate Click (Mixpanel) ──────────────────────────────
    print("\n[5] Processing Affiliate Click from Mixpanel...")
    ac_df = mp_to_df(ac_raw)
    if len(ac_df) > 0:
        ac_df["user_id"] = ac_df.get("$user_id", ac_df.get("distinct_id", ""))
        ac_users = set(ac_df["user_id"].dropna().unique())
    else:
        ac_users = set()
    print(f"  Users with Affiliate Click: {fmt(len(ac_users))}")

    user_agg["reached_hub"] = user_agg["guest_id"].isin(ac_users)

    # ── Step 6: Affiliate return signal ─────────────────────────────────
    print("\n[6] Detecting affiliate return signals...")
    # Global: our sk seen after any point
    # CIS: our UTM seen
    user_agg["has_return_signal"] = user_agg.apply(
        lambda r: r["has_our_sk"] if r["region"] == "Global" else r["has_our_utm"],
        axis=1,
    )

    # ── Step 7: CIS proxy return (fallback) ─────────────────────────────
    print("\n[7] Building CIS proxy return (time-based fallback)...")
    if len(ac_df) > 0 and "time" in ac_df.columns:
        ac_df["ac_ts"] = pd.to_datetime(ac_df["time"], unit="s", utc=True)
        ac_times = defaultdict(list)
        for _, row in ac_df.iterrows():
            uid = row.get("user_id", "")
            if uid and pd.notna(row.get("ac_ts")):
                ac_times[uid].append(row["ac_ts"])

        # For CIS users who reached hub but have no UTM return,
        # check if they returned to aliexpress.ru within PROXY_RETURN_WINDOW_S
        cis_events = ev[ev["is_cis"]].copy()
        cis_events["is_ali_ru"] = cis_events["url"].apply(is_aliexpress_ru)
        cis_ru_events = cis_events[cis_events["is_ali_ru"]]

        proxy_return_users = set()
        for guest_id, grp in cis_ru_events.groupby("guest_id"):
            if guest_id not in ac_times:
                continue
            for ac_ts in ac_times[guest_id]:
                mask = (grp["created_ts"] >= ac_ts) & (
                    grp["created_ts"] <= ac_ts + timedelta(seconds=PROXY_RETURN_WINDOW_S))
                if mask.any():
                    proxy_return_users.add(guest_id)
                    break

        user_agg["has_proxy_return"] = user_agg["guest_id"].isin(proxy_return_users)
    else:
        user_agg["has_proxy_return"] = False

    # Combined return: direct or proxy
    user_agg["has_any_return"] = user_agg["has_return_signal"] | user_agg["has_proxy_return"]

    # ── Step 8: Funnel ──────────────────────────────────────────────────
    print_section("Problem A — Funnel")

    results = {"funnel": {}, "segments": {}, "hypotheses": {}}

    for reg in ["Global", "CIS", "All"]:
        if reg == "All":
            uu = user_agg
        else:
            uu = user_agg[user_agg["region"] == reg]

        total = len(uu)
        eligible = uu["is_eligible"].sum()
        has_cfg = uu[uu["is_eligible"]]["has_usable_config"].sum()
        hub = uu["reached_hub"].sum()
        ret = uu["has_return_signal"].sum()
        proxy = uu["has_proxy_return"].sum()
        any_ret = uu["has_any_return"].sum()

        funnel = {
            "total_users": int(total),
            "eligible_users": int(eligible),
            "with_usable_config": int(has_cfg),
            "reached_hub": int(hub),
            "direct_return": int(ret),
            "proxy_return": int(proxy),
            "any_return": int(any_ret),
        }
        results["funnel"][reg] = funnel

        print(f"\n  ── {reg} ({fmt(total)} users) ──")
        print(f"  1. Total users:            {fmt(total)}")
        print(f"  2. Eligible (product page): {fmt(eligible)}  ({pct(eligible, total)})")
        print(f"  3. + usable config:         {fmt(has_cfg)}  ({pct(has_cfg, eligible)})")
        print(f"  4. Reached hub (AC):        {fmt(hub)}  ({pct(hub, eligible)})")
        print(f"  5. Direct return signal:    {fmt(ret)}  ({pct(ret, hub)})")
        if reg in ("CIS", "All"):
            print(f"     Proxy return (≤120s):    {fmt(proxy)}")
        print(f"  6. Any return:              {fmt(any_ret)}  ({pct(any_ret, hub)})")

    # ── Step 9: A5 — Missing Mixpanel click tracking ────────────────────
    print_section("A5 — Missing Mixpanel click tracking")
    # Users with return signal but NO Affiliate Click
    missing_ac_global = user_agg[
        (user_agg["region"] == "Global") &
        (~user_agg["reached_hub"]) &
        (user_agg["has_our_sk"])
    ]
    missing_ac_cis = user_agg[
        (user_agg["region"] == "CIS") &
        (~user_agg["reached_hub"]) &
        (user_agg["has_our_utm"])
    ]
    print(f"  Global: {fmt(len(missing_ac_global))} users have our sk but no Affiliate Click")
    print(f"  CIS:    {fmt(len(missing_ac_cis))} users have our UTM but no Affiliate Click")
    results["missing_ac"] = {
        "global": len(missing_ac_global),
        "cis": len(missing_ac_cis),
    }

    # ── Step 10: A6 — Hub reached, no return ────────────────────────────
    print_section("A6 — Hub reached but no return signal")
    hub_no_return_g = user_agg[
        (user_agg["region"] == "Global") &
        (user_agg["reached_hub"]) &
        (~user_agg["has_our_sk"])
    ]
    hub_no_return_c = user_agg[
        (user_agg["region"] == "CIS") &
        (user_agg["reached_hub"]) &
        (~user_agg["has_our_utm"]) &
        (~user_agg["has_proxy_return"])
    ]
    print(f"  Global: {fmt(len(hub_no_return_g))} users reached hub, no our sk return")
    print(f"  CIS:    {fmt(len(hub_no_return_c))} users reached hub, no UTM or proxy return")
    results["hub_no_return"] = {
        "global": len(hub_no_return_g),
        "cis": len(hub_no_return_c),
    }

    # ── Step 11: Segmentation ───────────────────────────────────────────
    print_section("Segmentation")

    seg_dims = {
        "region": "region",
        "browser": "browser_fam",
        "lineage": "lineage",
        "country": "country",
        "hub": "cfg_domain",
        "version": "client_version",
    }

    for seg_name, col in seg_dims.items():
        print(f"\n  ── By {seg_name} ──")
        seg = user_agg.groupby(col).agg(
            users=("guest_id", "count"),
            eligible=("is_eligible", "sum"),
            has_config=("has_usable_config", "sum"),
            reached_hub=("reached_hub", "sum"),
            has_return=("has_any_return", "sum"),
        ).reset_index()
        seg["elig_rate"] = seg.apply(lambda r: pct_f(r["eligible"], r["users"]), axis=1)
        seg["hub_rate"] = seg.apply(lambda r: pct_f(r["reached_hub"], r["eligible"]), axis=1)
        seg["return_rate"] = seg.apply(lambda r: pct_f(r["has_return"], r["reached_hub"]), axis=1)
        seg = seg.sort_values("users", ascending=False).head(20)

        print(tabulate(seg, headers="keys", tablefmt="simple", floatfmt=".1f", showindex=False))
        results["segments"][seg_name] = seg.to_dict("records")

    # ── Save results ────────────────────────────────────────────────────
    with open(CACHE_DIR / "results_a.pkl", "wb") as f:
        pickle.dump(results, f)
    print(f"\n  Results saved to {CACHE_DIR / 'results_a.pkl'}")

    return results


# ── Main ─────────────────────────────────────────────────────────────────────

def run():
    events_a = _load_pkl("events_a")
    clients = _load_pkl("clients")
    gsh = _load_pkl("gsh")
    ac_raw = _load_json("aff_click_a")

    analyze(events_a, clients, gsh, ac_raw)


if __name__ == "__main__":
    run()
