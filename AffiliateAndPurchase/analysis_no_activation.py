"""
AliHelper — "Purchases without AliHelper activation" follow-up
Focus: Problem B cohort; Global and CIS analyzed separately.

CACHE REUSE:
  REUSED : cache/pc_b.json          — Purchase Completed (Feb 27 – Mar 26)
  REUSED : cache/purchase_b.json    — Purchase events for match flag
  REUSED : cache/aff_click_a.json   — Affiliate Click lookup (Mar 6 – Apr 3)

NO new extraction needed.

Run: python3 -u analysis_no_activation.py 2>&1 | tee /tmp/no_activation_output.txt
"""

import json, time, pickle
from pathlib import Path
from datetime import datetime, timezone, timedelta
from collections import Counter

import pandas as pd
import numpy as np
from tabulate import tabulate

from src.config import (
    CACHE_DIR, B_START, B_END, OUR_SKS, CIS_COUNTRIES,
    ATTRIBUTION_WINDOW_H, MATCH_WINDOW_S, MP_TZ_OFFSET_H,
    AC_COVERAGE_START_UTC,
)
from src.utils import pct, print_section, is_cis, get_lineage


# ── Data loading ─────────────────────────────────────────────────────────────

def load_json(path: str) -> list:
    print(f"  [load] {path} ...", end=" ", flush=True)
    t0 = time.time()
    with open(path) as f:
        data = json.load(f)
    print(f"{len(data):,} records in {time.time()-t0:.1f}s")
    return data


def load_pc(path="cache/pc_b.json") -> pd.DataFrame:
    raw = load_json(path)
    rows = []
    for ev in raw:
        p = ev.get("properties", ev)
        rows.append({
            "user_id":           str(p.get("$user_id", "") or ""),
            "time_unix":         int(p.get("time", 0)),
            "country":           str(p.get("mp_country_code", "") or "").upper(),
            "browser":           str(p.get("$browser", "") or ""),
            "browser_lc":        str(p.get("$browser", "") or "").lower(),
            "version":           str(p.get("alihelper_version", "") or ""),
            "last_sk":           str(p.get("last_sk", "") or ""),
            "last_sk_datetime":  p.get("last_sk_datetime"),
            "sk":                str(p.get("sk", "") or ""),
            "af":                str(p.get("af", "") or ""),
            "last_af":           str(p.get("last_af", "") or ""),
            "cashback_list":     p.get("cashback_list"),
        })
    df = pd.DataFrame(rows)
    df["time_utc"] = pd.to_datetime(df["time_unix"], unit="s", utc=True)
    df = df[(df["time_utc"] >= B_START) & (df["time_utc"] <= B_END)].copy()
    print(f"  PC in Problem B window: {len(df):,}")
    return df


def load_purchase(path="cache/purchase_b.json") -> dict:
    raw = load_json(path)
    by_user = {}
    for ev in raw:
        p = ev.get("properties", ev)
        uid = str(p.get("$user_id", "") or "")
        ts  = int(p.get("time", 0))
        t   = datetime.utcfromtimestamp(ts).replace(tzinfo=timezone.utc)
        if uid and B_START <= t <= B_END:
            by_user.setdefault(uid, []).append(ts)
    for uid in by_user:
        by_user[uid].sort()
    print(f"  Purchase users in window: {len(by_user):,}")
    return by_user


def load_ac(path="cache/aff_click_a.json") -> dict:
    raw = load_json(path)
    by_user = {}
    for ev in raw:
        p = ev.get("properties", ev)
        uid = str(p.get("$user_id", "") or "")
        ts  = int(p.get("time", 0))
        if uid and ts:
            by_user.setdefault(uid, []).append(ts)
    for uid in by_user:
        by_user[uid].sort()
    print(f"  AC users in cache: {len(by_user):,}")
    return by_user


# ── Feature engineering ───────────────────────────────────────────────────────

def parse_sk_datetime(dt_str) -> datetime | None:
    if not dt_str or pd.isna(dt_str):
        return None
    try:
        dt = datetime.fromisoformat(str(dt_str))
        dt_utc = dt.replace(tzinfo=timezone.utc) - timedelta(hours=MP_TZ_OFFSET_H)
        return dt_utc
    except Exception:
        return None


def add_features(df: pd.DataFrame, pur_by_user: dict, ac_by_user: dict) -> pd.DataFrame:
    df = df.copy()

    df["is_cis"] = df["country"].apply(is_cis)
    df["region"] = df["is_cis"].map({True: "CIS", False: "Global"})
    df["lineage"] = df["browser_lc"].apply(get_lineage)

    attr_window_s = ATTRIBUTION_WINDOW_H * 3600

    def has_sk_in_72h(row) -> bool:
        if row["sk"] in OUR_SKS:
            return True
        if row["last_sk"] in OUR_SKS:
            dt_utc = parse_sk_datetime(row["last_sk_datetime"])
            if dt_utc is None:
                return True
            delta_s = (row["time_utc"] - dt_utc).total_seconds()
            return 0 <= delta_s <= attr_window_s
        return False

    print("  Computing has_our_sk_in_72h...", end=" ", flush=True)
    t0 = time.time()
    df["has_our_sk_in_72h"] = df.apply(has_sk_in_72h, axis=1)
    print(f"{time.time()-t0:.1f}s")

    print("  Matching Purchase Completed -> Purchase...", end=" ", flush=True)
    t0 = time.time()

    def is_matched(row) -> bool:
        uid = row["user_id"]
        pc_ts = int(row["time_utc"].timestamp())
        for pur_ts in pur_by_user.get(uid, []):
            if abs(pc_ts - pur_ts) <= MATCH_WINDOW_S:
                return True
        return False

    df["matched"] = df.apply(is_matched, axis=1)
    print(f"matched={df['matched'].sum():,}  {time.time()-t0:.1f}s")

    print("  Checking Affiliate Click in 72h...", end=" ", flush=True)
    t0 = time.time()
    ac_cov_start_ts = int(AC_COVERAGE_START_UTC.timestamp())

    def had_ac(row) -> tuple[bool, str]:
        uid = row["user_id"]
        pc_ts = int(row["time_utc"].timestamp())
        lookback_start = pc_ts - attr_window_s

        if lookback_start >= ac_cov_start_ts:
            coverage = "full"
        elif pc_ts >= ac_cov_start_ts:
            coverage = "partial"
        else:
            coverage = "none"

        for ac_ts in ac_by_user.get(uid, []):
            if lookback_start <= ac_ts <= pc_ts:
                return True, coverage
        return False, coverage

    results = df.apply(had_ac, axis=1)
    df["had_ac_in_72h"]  = results.apply(lambda x: x[0])
    df["ac_coverage"]    = results.apply(lambda x: x[1])
    print(f"had_ac={df['had_ac_in_72h'].sum():,}  {time.time()-t0:.1f}s")

    df["global_no_activation"] = (~df["has_our_sk_in_72h"]) & (~df["had_ac_in_72h"])
    df["global_no_sk"] = ~df["has_our_sk_in_72h"]

    def cis_cohort(row) -> str:
        if not row["is_cis"]:
            return "N/A"
        if row["matched"]:
            return "CIS_LIKELY_DELAYED_POSTBACK"
        if not row["had_ac_in_72h"]:
            return "CIS_NO_HUB_REACH_OBSERVED"
        return "CIS_HUB_REACHED_NO_PURCHASE"

    df["cis_cohort"] = df.apply(cis_cohort, axis=1)
    return df


# ── Reporting ────────────────────────────────────────────────────────────────

def section_global(df: pd.DataFrame) -> None:
    print_section("GLOBAL / PORTALS — Purchases without AliHelper activation")

    g = df[~df["is_cis"]].copy()
    total_pc     = len(g)
    total_users  = g["user_id"].nunique()

    no_act = g[g["global_no_activation"]]
    no_act_pc    = len(no_act)
    no_act_users = no_act["user_id"].nunique()

    no_sk = g[g["global_no_sk"]]
    no_sk_pc    = len(no_sk)
    no_sk_users = no_sk["user_id"].nunique()

    print(f"\n  Global PC total:                {total_pc:,}")
    print(f"  Global unique users (PC):       {total_users:,}")
    print(f"\n  -- Strict: no owned sk AND no AC in 72h --")
    print(f"  PC: {no_act_pc:,}  ({pct(no_act_pc, total_pc)})")
    print(f"  Users: {no_act_users:,}  ({pct(no_act_users, total_users)})")
    print(f"\n  -- Broader: no owned sk in 72h --")
    print(f"  PC: {no_sk_pc:,}  ({pct(no_sk_pc, total_pc)})")
    print(f"  Users: {no_sk_users:,}  ({pct(no_sk_users, total_users)})")

    cov = g["ac_coverage"].value_counts()
    print(f"\n  AC coverage breakdown (Global PC):")
    for k, v in cov.items():
        print(f"    {k:10s}: {v:,} ({pct(v, total_pc)})")

    n_matched   = g["matched"].sum()
    n_unmatched = (~g["matched"]).sum()
    print(f"\n  Matched:    {n_matched:,}  ({pct(n_matched, total_pc)})")
    print(f"  Unmatched:  {n_unmatched:,}  ({pct(n_unmatched, total_pc)})")

    u = g[~g["matched"]]
    print(f"\n  Among unmatched Global PC:")
    print(f"    No owned sk:      {u['global_no_sk'].sum():,}  ({pct(u['global_no_sk'].sum(), len(u))})")
    print(f"    No sk AND no AC:  {u['global_no_activation'].sum():,}  ({pct(u['global_no_activation'].sum(), len(u))})")
    print(f"    Had owned sk:     {(~u['global_no_sk']).sum():,}  ({pct((~u['global_no_sk']).sum(), len(u))})")

    for label, cohort in [("strict (no sk + no AC)", no_act), ("broader (no sk)", no_sk)]:
        print(f"\n  -- Segmentation: {label} --")
        if len(cohort) == 0:
            print("    (empty cohort)")
            continue

        print(f"\n    Top countries:")
        cdf = cohort.groupby("country").agg(
            pc=("user_id", "count"), users=("user_id", "nunique"),
        ).sort_values("pc", ascending=False).head(15)
        cdf["pct_of_cohort"] = (cdf["pc"] / len(cohort) * 100).round(1)
        print(tabulate(cdf.reset_index(), headers=["Country","PC","Users","% cohort"],
                       tablefmt="simple", intfmt=",", showindex=False))

        print(f"\n    Top browsers:")
        bdf = cohort.groupby("browser").agg(
            pc=("user_id", "count"), users=("user_id", "nunique"),
        ).sort_values("pc", ascending=False).head(10)
        bdf["pct"] = (bdf["pc"] / len(cohort) * 100).round(1)
        print(tabulate(bdf.reset_index(), headers=["Browser","PC","Users","% cohort"],
                       tablefmt="simple", intfmt=",", showindex=False))

        print(f"\n    By lineage:")
        ldf = cohort.groupby("lineage").agg(
            pc=("user_id", "count"), users=("user_id", "nunique"),
        ).sort_values("pc", ascending=False)
        ldf["pct"] = (ldf["pc"] / len(cohort) * 100).round(1)
        print(tabulate(ldf.reset_index(), headers=["Lineage","PC","Users","% cohort"],
                       tablefmt="simple", intfmt=",", showindex=False))

        print(f"\n    Top extension versions (top 10):")
        vdf = cohort.groupby("version").agg(
            pc=("user_id", "count"), users=("user_id", "nunique"),
        ).sort_values("pc", ascending=False).head(10)
        vdf["pct"] = (vdf["pc"] / len(cohort) * 100).round(1)
        print(tabulate(vdf.reset_index(), headers=["Version","PC","Users","% cohort"],
                       tablefmt="simple", intfmt=",", showindex=False))


def section_cis(df: pd.DataFrame) -> None:
    print_section("CIS / EPN — Limited-observability cohorts")

    c = df[df["is_cis"]].copy()
    total_pc    = len(c)
    total_users = c["user_id"].nunique()

    print(f"\n  CIS PC total:             {total_pc:,}")
    print(f"  CIS unique users (PC):    {total_users:,}")

    cohort_counts = c["cis_cohort"].value_counts()
    print(f"\n  CIS cohort breakdown:")
    for code, n in cohort_counts.items():
        print(f"    {code:55s}: {n:6,}  ({pct(n, total_pc)})")

    no_hub = c[c["cis_cohort"] == "CIS_NO_HUB_REACH_OBSERVED"]
    hub_no_pur = c[c["cis_cohort"] == "CIS_HUB_REACHED_NO_PURCHASE"]

    cov = c["ac_coverage"].value_counts()
    print(f"\n  AC coverage breakdown (CIS PC):")
    for k, v in cov.items():
        print(f"    {k:10s}: {v:,} ({pct(v, total_pc)})")

    for label, cohort in [("CIS_NO_HUB_REACH_OBSERVED", no_hub),
                          ("CIS_HUB_REACHED_NO_PURCHASE", hub_no_pur)]:
        print(f"\n  -- Segmentation: {label} ({len(cohort):,} PC) --")
        if len(cohort) == 0:
            print("    (empty cohort)")
            continue

        for seg_name, seg_col in [("countries", "country"), ("browsers", "browser"),
                                   ("lineage", "lineage"), ("versions", "version")]:
            print(f"\n    Top {seg_name}:")
            sdf = cohort.groupby(seg_col).agg(
                pc=("user_id", "count"), users=("user_id", "nunique"),
            ).sort_values("pc", ascending=False).head(10 if seg_name != "lineage" else 5)
            sdf["pct"] = (sdf["pc"] / len(cohort) * 100).round(1)
            print(tabulate(sdf.reset_index(), headers=[seg_col.title(),"PC","Users","% cohort"],
                           tablefmt="simple", intfmt=",", showindex=False))


def section_comparison(df: pd.DataFrame) -> None:
    print_section("COMPARATIVE SUMMARY")

    g = df[~df["is_cis"]]
    c = df[df["is_cis"]]

    g_no_act = g[g["global_no_activation"]]
    g_no_sk  = g[g["global_no_sk"]]
    c_no_hub = c[c["cis_cohort"] == "CIS_NO_HUB_REACH_OBSERVED"]

    rows = [
        ["Global — no sk + no AC (strict)", len(g_no_act), g_no_act["user_id"].nunique(),
         pct(len(g_no_act), len(g)), pct(g_no_act["user_id"].nunique(), g["user_id"].nunique()), "GLOBAL_DIRECT"],
        ["Global — no owned sk (broader)",  len(g_no_sk),  g_no_sk["user_id"].nunique(),
         pct(len(g_no_sk), len(g)),   pct(g_no_sk["user_id"].nunique(), g["user_id"].nunique()),  "GLOBAL_DIRECT"],
        ["CIS — no hub reach observed",     len(c_no_hub), c_no_hub["user_id"].nunique(),
         pct(len(c_no_hub), len(c)),   pct(c_no_hub["user_id"].nunique(), c["user_id"].nunique()), "CIS_PROXY"],
    ]
    print(tabulate(rows, headers=["Cohort","PC","Users","% region PC","% region users","Observability"],
                   tablefmt="simple", intfmt=","))


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    print_section("Loading cache")
    df = load_pc()
    pur_by_user = load_purchase()
    ac_by_user  = load_ac()

    print_section("Feature engineering")
    df = add_features(df, pur_by_user, ac_by_user)

    print(f"\n  Summary flags:")
    print(f"    has_our_sk_in_72h:    {df['has_our_sk_in_72h'].sum():,}  ({pct(df['has_our_sk_in_72h'].sum(), len(df))})")
    print(f"    had_ac_in_72h:        {df['had_ac_in_72h'].sum():,}  ({pct(df['had_ac_in_72h'].sum(), len(df))})")
    print(f"    matched:              {df['matched'].sum():,}  ({pct(df['matched'].sum(), len(df))})")
    print(f"    global_no_activation: {df['global_no_activation'].sum():,}")

    df.to_pickle(CACHE_DIR / "no_activation_b.pkl")

    section_global(df)
    section_cis(df)
    section_comparison(df)

    print_section("Done")


if __name__ == "__main__":
    main()
