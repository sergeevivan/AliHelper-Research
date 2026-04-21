#!/usr/bin/env python3
"""
Problem A — Missing Affiliate Click.

Why do many AliExpress users not generate `Affiliate Click`?

Funnel:
  1. Raw AliExpress activity
  2. Eligible product pages (per flow-specific rules)
  3. Eligible with usable latest config
  4. Reached hub (Affiliate Click)
  5. Returned with affiliate markers
     - Global: `sk` (whitelist)
     - CIS:    Pattern A (`af=*_7685`) or Pattern B (full-UTM with `_7685`)
                or Pattern partial (`utm_campaign=*_7685` without `source+medium`)
                or CIS_PROXY fallback (return to aliexpress.ru ≤120s after AC)

Per-event CIS classification is URL-domain-based (`aliexpress.ru`), NOT country.
Flow lineage uses `clients.build_app` when present; UA fallback otherwise.
`edge_ambiguous_build` / `unknown_build` are kept as separate segments.

Includes A7 — non-activator deep-dive (cohort, profile, behaviour, hypotheses).

Usage:
    REPORT_MODE=oneoff|pulse|deep python -m analysis.problem_a
"""

import pickle
from collections import defaultdict
from datetime import timedelta

import pandas as pd
from tabulate import tabulate

from src.config import (
    CACHE_DIR, CACHE_SUFFIX, REPORT_MODE,
    A_START, A_END, PROXY_RETURN_WINDOW_S, SESSION_GAP_S,
)
from src.utils import (
    print_section, pct, pct_f, fmt,
    browser_family, lineage_segment, lineage_from_build, region_label,
    is_cis_country, is_aliexpress_ru, matches_check_list_urls,
    is_eligible_product_page, mp_to_df, classify_event,
    product_page_subtype,
)


# ── Data loading ─────────────────────────────────────────────────────────────

def _key(name: str) -> str:
    return f"{name}__{CACHE_SUFFIX}"


def _load_pkl(name):
    path = CACHE_DIR / f"{_key(name)}.pkl"
    # Fall back to legacy (un-suffixed) name if the suffixed one doesn't exist
    if not path.exists():
        legacy = CACHE_DIR / f"{name}.pkl"
        if legacy.exists():
            path = legacy
    with open(path, "rb") as f:
        return pickle.load(f)


def _load_json(name):
    import json
    path = CACHE_DIR / f"{_key(name)}.json"
    if not path.exists():
        legacy = CACHE_DIR / f"{name}.json"
        if legacy.exists():
            path = legacy
    with open(path) as f:
        return json.load(f)


# ── Client enrichment ────────────────────────────────────────────────────────

def enrich_with_clients(events: pd.DataFrame, clients: pd.DataFrame) -> pd.DataFrame:
    cl = clients.drop_duplicates(subset="guest_id", keep="last")
    cols = ["browser", "country", "client_version", "os", "build_app"]
    for c in cols:
        if c not in cl.columns:
            cl[c] = None
    cl = cl.set_index("guest_id")[cols]
    return events.join(cl, on="guest_id", how="left")


def build_client_multiplicity(clients: pd.DataFrame) -> pd.Series:
    """guest_id -> number of distinct client records."""
    if "guest_id" not in clients.columns or len(clients) == 0:
        return pd.Series(dtype="int64")
    return clients.groupby("guest_id").size()


def build_latest_config(gsh: pd.DataFrame) -> dict:
    gsh_sorted = gsh.sort_values("config_ts")
    latest = gsh_sorted.drop_duplicates(subset="guest_id", keep="last")
    return latest.set_index("guest_id")[["domain", "value", "config_ts"]].to_dict("index")


# ── Per-event classification ─────────────────────────────────────────────────

def classify_events(events: pd.DataFrame) -> pd.DataFrame:
    """Attach owned/foreign markers and attribution-source tiers per event."""
    cls = events.apply(
        lambda r: classify_event({
            "url": r.get("url", ""),
            "query_sk": r.get("query_sk", ""),
            "params": r.get("params"),
        }),
        axis=1,
    )
    events["label"] = cls.map(lambda c: c["label"])
    events["is_owned"] = cls.map(lambda c: c["is_owned"])
    events["foreign_kind"] = cls.map(lambda c: c["foreign_kind"])
    events["is_ali_ru"] = cls.map(lambda c: c["is_ali_ru"])
    events["sk_source"] = cls.map(lambda c: c["sk_source"])
    events["af_source"] = cls.map(lambda c: c["af_source"])
    events["utm_source_tier"] = cls.map(lambda c: c["utm_source_tier"])
    events["epn_on_global"] = cls.map(lambda c: c["epn_on_global"])
    events["subtype"] = events["url"].apply(product_page_subtype)
    return events


# ── Core analysis ────────────────────────────────────────────────────────────

def analyze(events_a: pd.DataFrame, clients: pd.DataFrame,
            gsh: pd.DataFrame, ac_raw: list[dict]) -> dict:

    print_section(f"Problem A — Missing Affiliate Click (mode={REPORT_MODE})")
    print(f"  Window: {A_START} → {A_END}")

    # ── Step 1: Enrichment ─────────────────────────────────────────────
    print("\n[1] Enriching events with client data...")
    ev = enrich_with_clients(events_a, clients)
    ev["browser_fam"] = ev["browser"].apply(browser_family)
    ev["lineage"] = ev.apply(
        lambda r: lineage_segment(r.get("build_app"), r.get("browser")), axis=1)
    ev["build_app_present"] = ev["build_app"].apply(
        lambda v: isinstance(v, str) and v.strip() != "")
    ev["region"] = ev["country"].apply(region_label)
    ev["is_cis_user"] = ev["country"].apply(is_cis_country)

    total_events = len(ev)
    total_users = ev["guest_id"].nunique()
    print(f"  Total events: {fmt(total_events)}")
    print(f"  Total users:  {fmt(total_users)}")

    # ── Step 2: Eligibility ────────────────────────────────────────────
    print("\n[2] Classifying eligible pages...")
    ev["eligible"] = ev.apply(
        lambda r: (matches_check_list_urls(r["url"])
                   if r["lineage"] == "auto_redirect"
                   else (is_eligible_product_page(r["product_id"])
                         if r["lineage"] == "dogi" else False)),
        axis=1,
    )
    eligible_events = ev["eligible"].sum()
    print(f"  Eligible events: {fmt(eligible_events)} ({pct(eligible_events, total_events)})")

    # ── Step 3: Per-event attribution classification ───────────────────
    print("\n[3] Classifying per-event attribution (sk/af/utm)...")
    ev = classify_events(ev)

    ev["has_owned_sk"]        = (ev["label"] == "GLOBAL_DIRECT")
    ev["has_owned_af"]        = (ev["label"] == "CIS_DIRECT_AF")
    ev["has_owned_utm_full"]  = (ev["label"] == "CIS_DIRECT_UTM")
    ev["has_owned_utm_partial"] = (ev["label"] == "CIS_PARTIAL_UTM")
    ev["has_foreign_sk"]      = (ev["foreign_kind"] == "sk")
    ev["has_foreign_af"]      = (ev["foreign_kind"] == "af")
    ev["has_foreign_utm"]     = (ev["foreign_kind"] == "utm")

    # ── Step 4: User-level aggregation ─────────────────────────────────
    print("\n[4] Building user-level aggregation...")
    user_agg = ev.groupby("guest_id").agg(
        total_events=("eligible", "count"),
        eligible_events=("eligible", "sum"),
        has_owned_sk=("has_owned_sk", "any"),
        has_owned_af=("has_owned_af", "any"),
        has_owned_utm_full=("has_owned_utm_full", "any"),
        has_owned_utm_partial=("has_owned_utm_partial", "any"),
        has_foreign_sk=("has_foreign_sk", "any"),
        has_foreign_af=("has_foreign_af", "any"),
        has_foreign_utm=("has_foreign_utm", "any"),
        any_ali_ru=("is_ali_ru", "any"),
        country=("country", "first"),
        browser_fam=("browser_fam", "first"),
        lineage=("lineage", "first"),
        build_app=("build_app", "first"),
        build_app_present=("build_app_present", "first"),
        region=("region", "first"),
        is_cis_user=("is_cis_user", "first"),
        client_version=("client_version", "first"),
    ).reset_index()

    user_agg["is_eligible"] = user_agg["eligible_events"] > 0
    # A user with any aliexpress.ru event is treated as a CIS user for attribution
    user_agg["cis_by_url"] = user_agg["any_ali_ru"]

    # Multi-client tag (single / multi) per guest_id
    mult = build_client_multiplicity(clients)
    user_agg["client_count"] = user_agg["guest_id"].map(mult).fillna(0).astype(int)
    user_agg["multiclient"] = user_agg["client_count"].apply(
        lambda n: "multi" if n >= 2 else ("single" if n == 1 else "no_client"))

    # Dominant product-page subtype per user (most frequent non-null)
    subtypes_per_user = (
        ev[ev["subtype"].notna()]
        .groupby("guest_id")["subtype"]
        .agg(lambda s: s.value_counts().index[0] if len(s) else None)
    )
    user_agg["dominant_subtype"] = user_agg["guest_id"].map(subtypes_per_user).fillna("none")

    # ── Step 5: Latest config ──────────────────────────────────────────
    print("\n[5] Matching latest config per user...")
    config_map = build_latest_config(gsh)
    user_agg["cfg_domain"] = user_agg["guest_id"].map(
        lambda g: config_map.get(g, {}).get("domain", ""))
    user_agg["cfg_value"] = user_agg["guest_id"].map(
        lambda g: config_map.get(g, {}).get("value"))
    user_agg["has_usable_config"] = user_agg["cfg_value"] == True  # noqa: E712

    # ── Step 6: Affiliate Click (Mixpanel) ─────────────────────────────
    print("\n[6] Processing Affiliate Click from Mixpanel...")
    ac_df = mp_to_df(ac_raw)
    if len(ac_df) > 0:
        ac_df["user_id"] = ac_df.get("$user_id", ac_df.get("distinct_id", ""))
        ac_users = set(ac_df["user_id"].dropna().unique())
    else:
        ac_users = set()
    print(f"  Users with Affiliate Click: {fmt(len(ac_users))}")
    user_agg["reached_hub"] = user_agg["guest_id"].isin(ac_users)

    # ── Step 7: Return signal ──────────────────────────────────────────
    print("\n[7] Detecting affiliate return signals...")
    # Global: owned sk
    # CIS   : owned af (Pattern A) OR owned full-UTM (Pattern B) OR partial-UTM
    user_agg["has_return_signal"] = user_agg.apply(
        lambda r: (
            r["has_owned_sk"] if not r["cis_by_url"]
            else (r["has_owned_af"] or r["has_owned_utm_full"]
                  or r["has_owned_utm_partial"])
        ),
        axis=1,
    )

    # Primary user-level label (strongest owned signal seen)
    def _primary_label(r) -> str | None:
        if r["has_owned_sk"] and not r["cis_by_url"]:
            return "GLOBAL_DIRECT"
        if r["has_owned_af"]:
            return "CIS_DIRECT_AF"
        if r["has_owned_utm_full"]:
            return "CIS_DIRECT_UTM"
        if r["has_owned_utm_partial"]:
            return "CIS_PARTIAL_UTM"
        return None
    user_agg["primary_label"] = user_agg.apply(_primary_label, axis=1)

    # ── Step 8: CIS proxy return ───────────────────────────────────────
    print("\n[8] Building CIS proxy return (time-based fallback)...")
    if len(ac_df) > 0 and "time" in ac_df.columns:
        ac_df["ac_ts"] = pd.to_datetime(ac_df["time"], unit="s", utc=True)
        ac_times = defaultdict(list)
        for _, row in ac_df.iterrows():
            uid = row.get("user_id", "")
            if uid and pd.notna(row.get("ac_ts")):
                ac_times[uid].append(row["ac_ts"])

        cis_events = ev[ev["is_ali_ru"]].copy()

        proxy_return_users = set()
        for guest_id, grp in cis_events.groupby("guest_id"):
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

    # CIS_PROXY label takes effect only when there's no direct owned signal
    user_agg["primary_label"] = user_agg.apply(
        lambda r: r["primary_label"] or ("CIS_PROXY" if r["has_proxy_return"] else None),
        axis=1,
    )
    user_agg["has_any_return"] = user_agg["has_return_signal"] | user_agg["has_proxy_return"]

    # ── Step 9: Funnel by region (+ label breakdown) ───────────────────
    print_section("Problem A — Funnel")
    results = {
        "meta": {
            "report_mode": REPORT_MODE,
            "period_start": A_START.isoformat(),
            "period_end": A_END.isoformat(),
        },
        "funnel": {},
        "label_breakdown": {},
        "segments": {},
        "hypotheses": {},
    }

    # Region split is purely URL-domain-based (specs/domain/regional_routing.md
    # — "routing evidence wins"). A RU user who only visited aliexpress.com
    # belongs to Global; a UA user on aliexpress.ru belongs to CIS.
    for reg in ["Global", "CIS", "All"]:
        if reg == "All":
            uu = user_agg
        elif reg == "CIS":
            uu = user_agg[user_agg["cis_by_url"]]
        else:
            uu = user_agg[~user_agg["cis_by_url"]]

        total = len(uu)
        eligible = uu["is_eligible"].sum()
        has_cfg = uu[uu["is_eligible"]]["has_usable_config"].sum()
        hub = uu["reached_hub"].sum()
        ret = uu["has_return_signal"].sum()
        proxy = uu["has_proxy_return"].sum()
        any_ret = uu["has_any_return"].sum()

        results["funnel"][reg] = {
            "total_users": int(total),
            "eligible_users": int(eligible),
            "with_usable_config": int(has_cfg),
            "reached_hub": int(hub),
            "direct_return": int(ret),
            "proxy_return": int(proxy),
            "any_return": int(any_ret),
        }

        print(f"\n  ── {reg} ({fmt(total)} users) ──")
        print(f"  1. Total users:              {fmt(total)}")
        print(f"  2. Eligible (product page):  {fmt(eligible)}  ({pct(eligible, total)})")
        print(f"  3. + usable config:          {fmt(has_cfg)}  ({pct(has_cfg, eligible)})")
        print(f"  4. Reached hub (AC):         {fmt(hub)}  ({pct(hub, eligible)})")
        print(f"  5. Direct return signal:     {fmt(ret)}  ({pct(ret, hub)})")
        if reg in ("CIS", "All"):
            print(f"     Proxy return (≤120s):    {fmt(proxy)}")
        print(f"  6. Any return:               {fmt(any_ret)}  ({pct(any_ret, hub)})")

    # Label breakdown (owned users by primary label)
    lbl_counts = user_agg["primary_label"].value_counts(dropna=False).to_dict()
    results["label_breakdown"] = {str(k): int(v) for k, v in lbl_counts.items()}
    print("\n  ── Label breakdown (users) ──")
    for k, v in lbl_counts.items():
        print(f"    {str(k):<20} {fmt(int(v))}")

    # ── A5: Missing Mixpanel click tracking ────────────────────────────
    print_section("A5 — Missing Mixpanel click tracking")
    missing_ac_global = user_agg[
        (~user_agg["cis_by_url"]) & (~user_agg["reached_hub"]) & (user_agg["has_owned_sk"])
    ]
    missing_ac_cis = user_agg[
        (user_agg["cis_by_url"]) & (~user_agg["reached_hub"]) &
        (user_agg["has_owned_af"] | user_agg["has_owned_utm_full"]
         | user_agg["has_owned_utm_partial"])
    ]
    print(f"  Global: {fmt(len(missing_ac_global))} users have our sk but no Affiliate Click")
    print(f"  CIS:    {fmt(len(missing_ac_cis))} users have our af/UTM but no Affiliate Click")
    results["missing_ac"] = {
        "global": int(len(missing_ac_global)),
        "cis": int(len(missing_ac_cis)),
    }

    # ── A6: Hub reached, no return ─────────────────────────────────────
    print_section("A6 — Hub reached but no return signal")
    hub_no_return_g = user_agg[
        (~user_agg["cis_by_url"]) & (user_agg["reached_hub"]) & (~user_agg["has_owned_sk"])
    ]
    hub_no_return_c = user_agg[
        (user_agg["cis_by_url"]) & (user_agg["reached_hub"]) &
        (~user_agg["has_owned_af"]) & (~user_agg["has_owned_utm_full"])
        & (~user_agg["has_owned_utm_partial"]) & (~user_agg["has_proxy_return"])
    ]
    print(f"  Global: {fmt(len(hub_no_return_g))} users reached hub, no owned sk")
    print(f"  CIS:    {fmt(len(hub_no_return_c))} users reached hub, no af/UTM or proxy return")
    results["hub_no_return"] = {
        "global": int(len(hub_no_return_g)),
        "cis": int(len(hub_no_return_c)),
    }

    # ── Segmentation ──────────────────────────────────────────────────
    print_section("Segmentation")
    seg_dims = {
        "region": "region",
        "browser": "browser_fam",
        "lineage": "lineage",
        "country": "country",
        "hub": "cfg_domain",
        "version": "client_version",
        "build_app": "build_app",
        "subtype": "dominant_subtype",
        "multiclient": "multiclient",
    }
    for seg_name, col in seg_dims.items():
        print(f"\n  ── By {seg_name} ──")
        seg = user_agg.groupby(col, dropna=False).agg(
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

    # ── UA / _7685-on-Global anomaly count ─────────────────────────────
    # EPN suffix is CIS-only; its appearance on a Global AliExpress host is
    # a data-integrity flag (routing mis-hit or creative misconfiguration).
    print_section("Anomaly — _7685 on Global host")
    ua_anom_events = int(ev["epn_on_global"].sum())
    ua_anom_users = int(ev[ev["epn_on_global"]]["guest_id"].nunique())
    print(f"  Events: {fmt(ua_anom_events)} | Users: {fmt(ua_anom_users)}")
    results["ua_anomaly"] = {
        "events": ua_anom_events,
        "users": ua_anom_users,
    }

    # ── A7: Non-activator deep-dive ────────────────────────────────────
    results["a7"] = analyze_non_activators(ev, user_agg, ac_users)

    # ── Save ───────────────────────────────────────────────────────────
    out_path = CACHE_DIR / f"results_a__{CACHE_SUFFIX}.pkl"
    with open(out_path, "wb") as f:
        pickle.dump(results, f)
    print(f"\n  Results saved to {out_path}")
    return results


# ── A7: Non-activator deep-dive ──────────────────────────────────────────────

def _sessionize(ts_series: pd.Series) -> list[int]:
    """Assign session ids based on 30-min inactivity gap."""
    ts_sorted = ts_series.sort_values()
    session_ids = []
    sid = 0
    prev = None
    for t in ts_sorted:
        if prev is None or (t - prev).total_seconds() > SESSION_GAP_S:
            sid += 1
        session_ids.append(sid)
        prev = t
    return session_ids


def analyze_non_activators(ev: pd.DataFrame, user_agg: pd.DataFrame,
                           ac_users: set) -> dict:
    """A7: who are the non-activators and why."""
    print_section("A7 — Non-activator deep-dive")

    user_agg["is_activator"] = user_agg["reached_hub"]
    non_act = user_agg[~user_agg["is_activator"]].copy()
    act = user_agg[user_agg["is_activator"]].copy()

    # Table 1 — cohort sizing
    total = len(user_agg)
    non_with_elig = int(non_act["is_eligible"].sum())
    non_no_elig = int((~non_act["is_eligible"]).sum())
    t1 = {
        "total_users": int(total),
        "activators": int(len(act)),
        "non_activators": int(len(non_act)),
        "non_with_eligible": non_with_elig,
        "non_no_eligible": non_no_elig,
        "non_pct_of_total": pct_f(len(non_act), total),
    }
    print(f"  Non-activators: {fmt(t1['non_activators'])} "
          f"({pct(t1['non_activators'], total)})")
    print(f"    with eligible opportunities: {fmt(non_with_elig)}")
    print(f"    no eligible opportunities:   {fmt(non_no_elig)}")

    # Never-activator vs partial-activator split is available only via
    # AC cohort beyond the period; with only in-window data we approximate:
    #   never-activator  ≈ non_activator AND no Affiliate Click in this period
    #   partial-activator cannot be determined without lifetime data — mark TODO
    t1["never_activator_in_period"] = int((~non_act["guest_id"].isin(ac_users)).sum())

    # Table 2 — profile distribution (non-activator vs activator)
    def dist(df: pd.DataFrame, col: str, top: int = 10) -> list[dict]:
        s = df[col].fillna("<missing>").astype(str).value_counts().head(top)
        total_n = len(df) or 1
        return [
            {"value": k, "count": int(v), "pct": pct_f(v, total_n)}
            for k, v in s.items()
        ]

    t2 = {}
    for dim in ("country", "browser_fam", "build_app", "lineage",
                "client_version", "region"):
        t2[dim] = {
            "non_activator": dist(non_act, dim),
            "activator": dist(act, dim),
        }

    # Table 3 — non-activator rate by segment
    t3 = {}
    for dim in ("browser_fam", "country", "lineage", "client_version",
                "cfg_domain", "build_app"):
        grp = user_agg.groupby(dim, dropna=False).agg(
            users=("guest_id", "count"),
            activators=("is_activator", "sum"),
        ).reset_index()
        grp["non_activators"] = grp["users"] - grp["activators"]
        grp["non_activator_rate"] = grp.apply(
            lambda r: pct_f(r["non_activators"], r["users"]), axis=1)
        grp = grp.sort_values("users", ascending=False).head(20)
        t3[dim] = grp.to_dict("records")

    # Table 4 — session metrics (non-activator vs activator)
    # Build per-session stats only for the non-activator cohort (cheapest cut)
    non_ids = set(non_act["guest_id"])
    act_ids = set(act["guest_id"])

    def _session_stats(ids: set) -> dict:
        sub = ev[ev["guest_id"].isin(ids)][["guest_id", "created_ts", "eligible",
                                             "product_id", "url"]]
        if len(sub) == 0:
            return {"sessions": 0}
        sub = sub.sort_values(["guest_id", "created_ts"])
        sub["session_id"] = (
            sub.groupby("guest_id")["created_ts"]
            .transform(lambda s: _sessionize(s))
        )
        sess = sub.groupby(["guest_id", "session_id"]).agg(
            events=("created_ts", "count"),
            duration_s=("created_ts", lambda s: (s.max() - s.min()).total_seconds()),
            eligible_hits=("eligible", "sum"),
            product_page_hits=("product_id",
                               lambda s: s.apply(lambda v: v is not None and v != "").sum()),
        ).reset_index()
        return {
            "sessions": int(len(sess)),
            "median_events": float(sess["events"].median()) if len(sess) else 0.0,
            "median_duration_s": float(sess["duration_s"].median()) if len(sess) else 0.0,
            "median_eligible_hits": float(sess["eligible_hits"].median()) if len(sess) else 0.0,
            "median_product_hits": float(sess["product_page_hits"].median()) if len(sess) else 0.0,
            "bounce_rate_pct": pct_f(int((sess["events"] == 1).sum()), len(sess)),
        }

    t4 = {
        "non_activator": _session_stats(non_ids),
        "activator": _session_stats(act_ids),
    }

    # Table 5 — top-N non-activator cohorts (browser x country)
    nov = non_act.copy()
    nov["cohort_key"] = nov["browser_fam"].astype(str) + " / " + nov["country"].astype(str)
    top_cohorts = (nov.groupby("cohort_key")
                    .size().sort_values(ascending=False).head(10))
    t5 = [{"cohort": k, "users": int(v), "share_pct": pct_f(v, len(non_act))}
          for k, v in top_cohorts.items()]

    # Table 6 — hypothesis proxies
    t6 = {
        "no_usable_hub": int(((~user_agg["has_usable_config"]) &
                              (~user_agg["is_activator"])).sum()),
        "ineligible_only": non_no_elig,
        "edge_ambiguous_non_activators": int(
            (non_act["lineage"] == "edge_ambiguous_build").sum()),
        "unknown_build_non_activators": int(
            (non_act["lineage"] == "unknown_build").sum()),
    }

    print("  A7 Table 1 cohort sizing computed.")
    print("  A7 Table 3 rates computed.")
    print(f"  A7 top cohort: {t5[0] if t5 else '—'}")

    return {
        "table1_cohort_sizing": t1,
        "table2_profile": t2,
        "table3_non_activator_rate": t3,
        "table4_session_metrics": t4,
        "table5_top_cohorts": t5,
        "table6_hypothesis_proxies": t6,
    }


# ── Main ─────────────────────────────────────────────────────────────────────

def run():
    events_a = _load_pkl("events_a")
    clients = _load_pkl("clients")
    gsh = _load_pkl("gsh")
    ac_raw = _load_json("aff_click_a")
    analyze(events_a, clients, gsh, ac_raw)


if __name__ == "__main__":
    run()
