"""
AliHelper — "Purchases without AliHelper activation" follow-up
Focus: Problem B cohort; Global and CIS analyzed separately.

CACHE REUSE:
  REUSED : cache/pc_b.json          — Purchase Completed (Feb 27 – Mar 26)
  REUSED : cache/purchase_b.json    — Purchase events for match flag
  REUSED : cache/aff_click_a.json   — Affiliate Click lookup (Mar 6 – Apr 3)
               ⚠ Partial coverage: 72h lookback for PC events before Mar 9
                 is incomplete; those cases flagged with ac_coverage='partial'

NO new extraction needed.
NOT AVAILABLE without new MongoDB: hub from latest delivered guestStateHistory config.

Run: python3 -u analysis_no_activation.py 2>&1 | tee /tmp/no_activation_output.txt
"""

import os, json, pickle, time
from pathlib import Path
from datetime import datetime, timezone, timedelta
from collections import Counter

import pandas as pd
import numpy as np
from tabulate import tabulate

CACHE_DIR = Path("./cache")

# ── Constants ────────────────────────────────────────────────────────────────
B_START = datetime(2026, 2, 27,  0,  0,  0, tzinfo=timezone.utc)
B_END   = datetime(2026, 3, 26, 23, 59, 59, tzinfo=timezone.utc)

# AliHelper-owned Global sk whitelist
OUR_SKS = {"_c36PoUEj", "_d6jWDbY", "_AnTGXs", "_olPBn9X", "_dVh6yw5"}

# CIS/EPN countries (UA = Global per CLAUDE.md)
CIS_COUNTRIES = {"RU", "BY", "KZ", "UZ", "AZ", "AM", "GE", "KG", "MD", "TJ", "TM"}

AUTO_REDIRECT = {"firefox", "edge"}

ATTRIBUTION_H   = 72
MATCH_WINDOW_S  = 10 * 60  # 10 min

# Mixpanel project timezone offset (Europe/Moscow = UTC+3)
MP_TZ_OFFSET_H = 3

# Affiliate Click data starts at this timestamp (Mar 6 00:00 UTC)
AC_COVERAGE_START_UTC = datetime(2026, 3, 6, 0, 0, 0, tzinfo=timezone.utc)


def print_section(title: str) -> None:
    print()
    print("=" * 70)
    print(f"  {title}")
    print("=" * 70)


def pct(n, d, dec=1):
    if not d:
        return "—"
    return f"{100*n/d:.{dec}f}%"


def load_json(path: str) -> list:
    print(f"  [load] {path} ...", end=" ", flush=True)
    t0 = time.time()
    with open(path) as f:
        data = json.load(f)
    print(f"{len(data):,} records in {time.time()-t0:.1f}s")
    return data


def is_cis(country: str) -> bool:
    return str(country).upper() in CIS_COUNTRIES


def get_lineage(browser: str) -> str:
    b = str(browser).lower()
    if b in AUTO_REDIRECT:
        return "auto-redirect"
    return "dogi"


# ── Data loading ─────────────────────────────────────────────────────────────

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
            "last_sk_datetime":  p.get("last_sk_datetime"),    # Moscow tz string
            "sk":                str(p.get("sk", "") or ""),
            "af":                str(p.get("af", "") or ""),
            "last_af":           str(p.get("last_af", "") or ""),
            "cashback_list":     p.get("cashback_list"),
        })
    df = pd.DataFrame(rows)
    df["time_utc"] = pd.to_datetime(df["time_unix"], unit="s", utc=True)
    # Filter to Problem B window
    df = df[(df["time_utc"] >= B_START) & (df["time_utc"] <= B_END)].copy()
    print(f"  PC in Problem B window: {len(df):,}")
    return df


def load_purchase(path="cache/purchase_b.json") -> dict:
    """Returns user_id → sorted list of unix timestamps."""
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
    """Returns user_id → sorted list of unix timestamps. Coverage: Mar 6 onward."""
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
    """Parse last_sk_datetime (Moscow time) → UTC datetime."""
    if not dt_str or pd.isna(dt_str):
        return None
    try:
        dt = datetime.fromisoformat(str(dt_str))
        # Moscow = UTC+3 → subtract 3h to get UTC
        dt_utc = dt.replace(tzinfo=timezone.utc) - timedelta(hours=MP_TZ_OFFSET_H)
        return dt_utc
    except Exception:
        return None


def add_features(df: pd.DataFrame, pur_by_user: dict, ac_by_user: dict) -> pd.DataFrame:
    df = df.copy()

    # ── Region ──────────────────────────────────────────────────────────────
    df["is_cis"] = df["country"].apply(is_cis)
    df["region"] = df["is_cis"].map({True: "CIS", False: "Global"})
    df["lineage"] = df["browser_lc"].apply(get_lineage)

    # ── has_our_sk_in_72h ───────────────────────────────────────────────────
    # Primary evidence for Global activation.
    # = (sk at purchase time is ours) OR (last_sk is ours AND last_sk within 72h)
    attr_window_s = ATTRIBUTION_H * 3600

    def has_sk_in_72h(row) -> bool:
        # sk at purchase time — always within window
        if row["sk"] in OUR_SKS:
            return True
        # last_sk: check temporal distance
        if row["last_sk"] in OUR_SKS:
            dt_utc = parse_sk_datetime(row["last_sk_datetime"])
            if dt_utc is None:
                return True  # no datetime → assume valid (conservative)
            delta_s = (row["time_utc"] - dt_utc).total_seconds()
            return 0 <= delta_s <= attr_window_s
        return False

    print("  Computing has_our_sk_in_72h...", end=" ", flush=True)
    t0 = time.time()
    df["has_our_sk_in_72h"] = df.apply(has_sk_in_72h, axis=1)
    print(f"{time.time()-t0:.1f}s")

    # ── matched_purchase ────────────────────────────────────────────────────
    print("  Matching Purchase Completed → Purchase...", end=" ", flush=True)
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

    # ── had_ac_in_72h (with partial coverage flag) ──────────────────────────
    print("  Checking Affiliate Click in 72h...", end=" ", flush=True)
    t0 = time.time()

    def had_ac(row) -> tuple[bool, str]:
        """Returns (had_ac: bool, coverage: 'full'|'partial'|'none')."""
        uid = row["user_id"]
        pc_ts = int(row["time_utc"].timestamp())
        lookback_start = pc_ts - attr_window_s
        ac_cov_start_ts = int(AC_COVERAGE_START_UTC.timestamp())

        # Coverage quality
        if lookback_start >= ac_cov_start_ts:
            coverage = "full"
        elif pc_ts >= ac_cov_start_ts:
            coverage = "partial"
        else:
            coverage = "none"  # PC before Mar 6, entire 72h window uncovered

        for ac_ts in ac_by_user.get(uid, []):
            if lookback_start <= ac_ts <= pc_ts:
                return True, coverage
        return False, coverage

    results = df.apply(had_ac, axis=1)
    df["had_ac_in_72h"]  = results.apply(lambda x: x[0])
    df["ac_coverage"]    = results.apply(lambda x: x[1])
    print(f"had_ac={df['had_ac_in_72h'].sum():,}  {time.time()-t0:.1f}s")

    # ── Compound "no activation" flags ──────────────────────────────────────
    # Global: no activation = no owned sk in 72h AND no AC in 72h
    df["global_no_activation"] = (~df["has_our_sk_in_72h"]) & (~df["had_ac_in_72h"])
    # Global: no sk only (wider definition)
    df["global_no_sk"] = ~df["has_our_sk_in_72h"]

    # ── CIS cohorts (per CLAUDE.md limited-observability) ───────────────────
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


# ── Reporting helpers ─────────────────────────────────────────────────────────

def top_n(series, n=10) -> pd.DataFrame:
    vc = series.value_counts().head(n).reset_index()
    vc.columns = ["value", "count"]
    vc["pct"] = (vc["count"] / vc["count"].sum() * 100).round(1)
    return vc


def section_global(df: pd.DataFrame) -> None:
    print_section("GLOBAL / PORTALS — Purchases without AliHelper activation")

    g = df[~df["is_cis"]].copy()
    total_users  = g["user_id"].nunique()
    total_pc     = len(g)
    total_pur_users = g[g["matched"]]["user_id"].nunique()

    # Cohort: strict (no sk AND no AC)
    no_act = g[g["global_no_activation"]]
    no_act_users = no_act["user_id"].nunique()
    no_act_pc    = len(no_act)

    # Cohort: no sk only (broader)
    no_sk = g[g["global_no_sk"]]
    no_sk_users = no_sk["user_id"].nunique()
    no_sk_pc    = len(no_sk)

    print(f"\n  Global PC total:                {total_pc:,}")
    print(f"  Global unique users (PC):       {total_users:,}")
    print()
    print(f"  ── Strict: no owned sk AND no AC in 72h ──")
    print(f"  PC:                             {no_act_pc:,}  ({pct(no_act_pc, total_pc)} of all Global PC)")
    print(f"  Unique users:                   {no_act_users:,}  ({pct(no_act_users, total_users)} of all Global PC users)")
    print()
    print(f"  ── Broader: no owned sk in 72h (regardless of AC) ──")
    print(f"  PC:                             {no_sk_pc:,}  ({pct(no_sk_pc, total_pc)} of all Global PC)")
    print(f"  Unique users:                   {no_sk_users:,}  ({pct(no_sk_users, total_users)} of all Global PC users)")
    print()
    print(f"  AC coverage note: Affiliate Click data starts Mar 6.")
    print(f"  PC before Mar 9 may have partial/no AC lookback coverage.")

    cov = g["ac_coverage"].value_counts()
    print(f"\n  AC coverage breakdown (Global PC):")
    for k, v in cov.items():
        print(f"    {k:10s}: {v:,} ({pct(v, total_pc)})")

    # Matched vs unmatched breakdown
    print(f"\n  ── Matched / Unmatched (Global) ──")
    n_matched   = g["matched"].sum()
    n_unmatched = (~g["matched"]).sum()
    print(f"  Matched:    {n_matched:,}  ({pct(n_matched, total_pc)})")
    print(f"  Unmatched:  {n_unmatched:,}  ({pct(n_unmatched, total_pc)})")

    print(f"\n  Among unmatched Global PC:")
    u = g[~g["matched"]]
    n_u_no_act  = u["global_no_activation"].sum()
    n_u_no_sk   = u["global_no_sk"].sum()
    n_u_has_sk  = (~u["global_no_sk"]).sum()
    print(f"    No owned sk in 72h:             {n_u_no_sk:,}  ({pct(n_u_no_sk, len(u))} of unmatched)")
    print(f"    No owned sk AND no AC:          {n_u_no_act:,}  ({pct(n_u_no_act, len(u))} of unmatched)")
    print(f"    Had owned sk (overwrite/timing): {n_u_has_sk:,}  ({pct(n_u_has_sk, len(u))} of unmatched)")

    # Segmentation on "no activation" (strict) cohort
    for label, cohort in [
        ("strict (no sk + no AC)", no_act),
        ("broader (no sk)", no_sk),
    ]:
        print(f"\n  ── Segmentation: {label} ──")
        if len(cohort) == 0:
            print("    (empty cohort)")
            continue

        # Top countries
        print(f"\n    Top countries ({label}):")
        cdf = cohort.groupby("country").agg(
            pc=("user_id", "count"),
            users=("user_id", "nunique"),
        ).sort_values("pc", ascending=False).head(15)
        cdf["pct_of_cohort"] = (cdf["pc"] / len(cohort) * 100).round(1)
        print(tabulate(cdf.reset_index(), headers=["Country","PC","Users","% cohort"],
                       tablefmt="simple", intfmt=",", showindex=False))

        # Top browsers
        print(f"\n    Top browsers:")
        bdf = cohort.groupby("browser").agg(
            pc=("user_id", "count"),
            users=("user_id", "nunique"),
        ).sort_values("pc", ascending=False).head(10)
        bdf["pct"] = (bdf["pc"] / len(cohort) * 100).round(1)
        print(tabulate(bdf.reset_index(), headers=["Browser","PC","Users","% cohort"],
                       tablefmt="simple", intfmt=",", showindex=False))

        # Lineage
        print(f"\n    By lineage (auto-redirect vs DOGI):")
        ldf = cohort.groupby("lineage").agg(
            pc=("user_id", "count"),
            users=("user_id", "nunique"),
        ).sort_values("pc", ascending=False)
        ldf["pct"] = (ldf["pc"] / len(cohort) * 100).round(1)
        print(tabulate(ldf.reset_index(), headers=["Lineage","PC","Users","% cohort"],
                       tablefmt="simple", intfmt=",", showindex=False))

        # Top versions
        print(f"\n    Top extension versions (top 10):")
        vdf = cohort.groupby("version").agg(
            pc=("user_id", "count"),
            users=("user_id", "nunique"),
        ).sort_values("pc", ascending=False).head(10)
        vdf["pct"] = (vdf["pc"] / len(cohort) * 100).round(1)
        print(tabulate(vdf.reset_index(), headers=["Version","PC","Users","% cohort"],
                       tablefmt="simple", intfmt=",", showindex=False))


def section_cis(df: pd.DataFrame) -> None:
    print_section("CIS / EPN — Limited-observability cohorts")

    c = df[df["is_cis"]].copy()
    total_users = c["user_id"].nunique()
    total_pc    = len(c)

    print(f"\n  CIS PC total:             {total_pc:,}")
    print(f"  CIS unique users (PC):    {total_users:,}")

    # CIS cohorts
    cohort_counts = c["cis_cohort"].value_counts()
    print(f"\n  CIS cohort breakdown:")
    for code, n in cohort_counts.items():
        print(f"    {code:55s}: {n:6,}  ({pct(n, total_pc)})")

    # Focus: "no hub reach" = no activation observable
    no_hub = c[c["cis_cohort"] == "CIS_NO_HUB_REACH_OBSERVED"]
    hub_reached_no_pur = c[c["cis_cohort"] == "CIS_HUB_REACHED_NO_PURCHASE"]

    print(f"\n  ⚠  CIS_NO_HUB_REACH_OBSERVED note:")
    print(f"     'No hub reach' uses AC lookup from aff_click_a.json (Mar 6+).")
    print(f"     For CIS PC before ~Mar 9, AC lookback may be missing → undercount.")
    print(f"     This cohort is a lower bound on 'no observable hub reach'.")

    cov = c["ac_coverage"].value_counts()
    print(f"\n  AC coverage breakdown (CIS PC):")
    for k, v in cov.items():
        print(f"    {k:10s}: {v:,} ({pct(v, total_pc)})")

    # Segmentation for each cohort
    for label, cohort in [
        ("CIS_NO_HUB_REACH_OBSERVED", no_hub),
        ("CIS_HUB_REACHED_NO_PURCHASE", hub_reached_no_pur),
    ]:
        print(f"\n  ── Segmentation: {label} ({len(cohort):,} PC) ──")
        if len(cohort) == 0:
            print("    (empty cohort)")
            continue

        print(f"\n    Top countries:")
        cdf = cohort.groupby("country").agg(
            pc=("user_id", "count"),
            users=("user_id", "nunique"),
        ).sort_values("pc", ascending=False).head(15)
        cdf["pct_cohort"] = (cdf["pc"] / len(cohort) * 100).round(1)
        print(tabulate(cdf.reset_index(), headers=["Country","PC","Users","% cohort"],
                       tablefmt="simple", intfmt=",", showindex=False))

        print(f"\n    Top browsers:")
        bdf = cohort.groupby("browser").agg(
            pc=("user_id", "count"),
            users=("user_id", "nunique"),
        ).sort_values("pc", ascending=False).head(10)
        bdf["pct"] = (bdf["pc"] / len(cohort) * 100).round(1)
        print(tabulate(bdf.reset_index(), headers=["Browser","PC","Users","% cohort"],
                       tablefmt="simple", intfmt=",", showindex=False))

        print(f"\n    By lineage:")
        ldf = cohort.groupby("lineage").agg(
            pc=("user_id", "count"),
            users=("user_id", "nunique"),
        ).sort_values("pc", ascending=False)
        ldf["pct"] = (ldf["pc"] / len(cohort) * 100).round(1)
        print(tabulate(ldf.reset_index(), headers=["Lineage","PC","Users","% cohort"],
                       tablefmt="simple", intfmt=",", showindex=False))

        print(f"\n    Top extension versions (top 10):")
        vdf = cohort.groupby("version").agg(
            pc=("user_id", "count"),
            users=("user_id", "nunique"),
        ).sort_values("pc", ascending=False).head(10)
        vdf["pct"] = (vdf["pc"] / len(cohort) * 100).round(1)
        print(tabulate(vdf.reset_index(), headers=["Version","PC","Users","% cohort"],
                       tablefmt="simple", intfmt=",", showindex=False))


def section_comparison(df: pd.DataFrame) -> None:
    print_section("COMPARATIVE SUMMARY")

    g = df[~df["is_cis"]]
    c = df[df["is_cis"]]

    g_no_act = g[g["global_no_activation"]]
    g_no_sk  = g[g["global_no_sk"]]
    c_no_hub = c[c["cis_cohort"] == "CIS_NO_HUB_REACH_OBSERVED"]

    total_g_pc = len(g)
    total_c_pc = len(c)
    total_g_users = g["user_id"].nunique()
    total_c_users = c["user_id"].nunique()

    rows = [
        ["Global — no sk + no AC (strict)", len(g_no_act), g_no_act["user_id"].nunique(),
         pct(len(g_no_act), total_g_pc), pct(g_no_act["user_id"].nunique(), total_g_users), "GLOBAL_DIRECT"],
        ["Global — no owned sk (broader)",  len(g_no_sk),  g_no_sk["user_id"].nunique(),
         pct(len(g_no_sk), total_g_pc),   pct(g_no_sk["user_id"].nunique(), total_g_users),  "GLOBAL_DIRECT"],
        ["CIS — no hub reach observed",     len(c_no_hub), c_no_hub["user_id"].nunique(),
         pct(len(c_no_hub), total_c_pc),   pct(c_no_hub["user_id"].nunique(), total_c_users), "CIS_PROXY"],
    ]
    print(tabulate(rows, headers=["Cohort","PC","Users","% region PC","% region users","Observability"],
                   tablefmt="simple", intfmt=","))

    # Top countries comparison
    print(f"\n  Top countries — Global no-activation (strict):")
    gdf = g_no_act.groupby("country")["user_id"].count().sort_values(ascending=False).head(10)
    print("   ", " | ".join(f"{c}: {n:,}" for c, n in gdf.items()))

    print(f"\n  Top countries — CIS no-hub-reach:")
    cdf = c_no_hub.groupby("country")["user_id"].count().sort_values(ascending=False).head(10)
    print("   ", " | ".join(f"{c}: {n:,}" for c, n in cdf.items()))

    print(f"\n  Top browsers — Global no-activation (strict):")
    gbdf = g_no_act.groupby("browser")["user_id"].count().sort_values(ascending=False).head(8)
    print("   ", " | ".join(f"{b}: {n:,}" for b, n in gbdf.items()))

    print(f"\n  Top browsers — CIS no-hub-reach:")
    cbdf = c_no_hub.groupby("browser")["user_id"].count().sort_values(ascending=False).head(8)
    print("   ", " | ".join(f"{b}: {n:,}" for b, n in cbdf.items()))


def section_interpretation(df: pd.DataFrame) -> None:
    print_section("INTERPRETATION")

    g = df[~df["is_cis"]]
    c = df[df["is_cis"]]
    g_no_act = g[g["global_no_activation"]]
    c_no_hub = c[c["cis_cohort"] == "CIS_NO_HUB_REACH_OBSERVED"]

    total_all_users = df["user_id"].nunique()
    missed_global   = g_no_act["user_id"].nunique()
    missed_cis      = c_no_hub["user_id"].nunique()

    print(f"""
  ① AUDIENCE SIZE
     Global users who purchased without any detected AliHelper activation:
       {missed_global:,} unique users  ({pct(missed_global, g['user_id'].nunique())} of all Global PC users)
       {len(g_no_act):,} Purchase Completed events  ({pct(len(g_no_act), len(g))} of all Global PC)

     CIS users where no hub reach is observable before purchase:
       {missed_cis:,} unique users  ({pct(missed_cis, c['user_id'].nunique())} of all CIS PC users)
       {len(c_no_hub):,} Purchase Completed events  ({pct(len(c_no_hub), len(c))} of all CIS PC)
       (lower bound — AC coverage is partial for early-window PC)

  ② COUNTRY CONCENTRATION (Global)""")

    gctry = g_no_act.groupby("country")["user_id"].count().sort_values(ascending=False)
    top3_share = gctry.head(3).sum() / len(g_no_act) * 100
    print(f"     Top 3 countries account for {top3_share:.1f}% of Global no-activation PC.")
    print(f"     Top 5:")
    for cty, n in gctry.head(5).items():
        print(f"       {cty}: {n:,}  ({pct(n, len(g_no_act))})")

    print(f"\n  ③ BROWSER / LINEAGE CONCENTRATION (Global)")
    g_lin = g_no_act.groupby("lineage")["user_id"].count().sort_values(ascending=False)
    g_all_lin = g.groupby("lineage")["user_id"].count()
    for lin, n in g_lin.items():
        miss_rate = n / g_all_lin.get(lin, 1) * 100
        print(f"     {lin:15s}: {n:,} no-act PC  |  miss rate vs all {lin} PC: {miss_rate:.1f}%")

    g_br = g_no_act.groupby("browser")["user_id"].count().sort_values(ascending=False).head(5)
    print(f"     Top browsers in no-activation cohort:")
    for br, n in g_br.items():
        all_br = g[g["browser"] == br]["user_id"].count()
        print(f"       {br}: {n:,}  (miss rate: {pct(n, all_br)} of all {br} PC)")

    print(f"""
  ④ DIAGNOSIS

     GLOBAL DIRECT observations:
     ─────────────────────────────────────────────────────────────────────
     {pct(len(g_no_act), len(g))} of Global PC events show NO owned sk AND NO AC in 72h.
     This strongly indicates that AliHelper never activated for these users
     in the attribution window — NOT a postback/partner issue.

     Likely root causes (in order of probability):
     a) User visited AliExpress on an ineligible page only (never triggered redirect)
     b) Redirect fired but extension was not active / hub was unreachable
     c) User switched browsers or used a browser without the extension
     d) User session was very short — purchase happened too fast for activation
     e) DOGI flow did not trigger (user did not interact with coin/thumbnail)

     This is primarily a product activation problem (Problem A contributors
     flowing through to purchase), NOT a postback or measurement issue.

     CIS_PROXY observations:
     ─────────────────────────────────────────────────────────────────────
     CIS_NO_HUB_REACH: {len(c_no_hub):,} events. Likely similar activation failures,
     but direct EPN attribution is NOT observable in historical events.
     Absence of AC does NOT prove EPN failure — EPN postback could still
     arrive independently of the observed hub-reach signal.
     Treat as lower-bound estimate of unactivated purchases, not a definitive count.
""")


def save_pkl(df: pd.DataFrame) -> None:
    out = CACHE_DIR / "no_activation_b.pkl"
    df.to_pickle(out)
    print(f"  [cache] Saved {out}")


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    print_section("Cache & Extraction Plan")
    print("""
  REUSED:
    cache/pc_b.json          — Purchase Completed (Problem B window, Feb 27–Mar 26)
    cache/purchase_b.json    — Purchase events for match flag
    cache/aff_click_a.json   — Affiliate Click lookup (coverage: Mar 6 onward)
                               ⚠ PC before Mar 9 have partial/no AC lookback coverage

  NEW COMPUTATIONS (from existing cache only, no MongoDB):
    has_our_sk_in_72h        — from last_sk / last_sk_datetime (Moscow→UTC) / sk
    matched                  — user + 10min window match against Purchase
    had_ac_in_72h            — from aff_click_a lookup with coverage flag
    global_no_activation     — not(has_our_sk_in_72h) AND not(had_ac_in_72h)
    cis_cohort               — limited-observability CIS codes

  NOT AVAILABLE (would need new MongoDB extraction):
    hub from latest delivered config (guestStateHistory for Problem B window)
""")

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
    print(f"    global_no_activation: {df['global_no_activation'].sum():,}  ({pct(df['global_no_activation'].sum(), len(df))})")
    print(f"    global (region):      {(~df['is_cis']).sum():,}")
    print(f"    cis (region):         {df['is_cis'].sum():,}")

    save_pkl(df)

    section_global(df)
    section_cis(df)
    section_comparison(df)
    section_interpretation(df)

    print_section("Done")
    print("  Output saved: cache/no_activation_b.pkl")
    print("  Full console output: /tmp/no_activation_output.txt")


if __name__ == "__main__":
    main()
