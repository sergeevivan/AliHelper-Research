#!/usr/bin/env python3
"""
Problem B — Purchase Completed without Purchase.

Why do we see more Purchase Completed than commission-bearing Purchase?

For each Purchase Completed:
  - Reconstruct 72h pre-purchase attribution window from MongoDB events
  - Match to Purchase by user + time proximity (10-min window)
  - Assign reason code

Usage:
    python -m analysis.problem_b
"""

import pickle
from collections import defaultdict
from datetime import timedelta

import pandas as pd
from tabulate import tabulate

from src.config import (
    CACHE_DIR, B_START, B_END,
    ATTRIBUTION_WINDOW_H, MATCH_WINDOW_S, MP_TZ_OFFSET_H,
    PROXY_RETURN_WINDOW_S,
)
from src.utils import (
    print_section, pct, pct_f, fmt,
    browser_family, lineage, is_cis, region_label,
    is_our_sk, has_foreign_sk, has_af,
    is_alihelper_utm, is_foreign_utm,
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


# ── Purchase Completed preparation ───────────────────────────────────────────

def prepare_pc(pc_raw: list[dict]) -> pd.DataFrame:
    """Parse Purchase Completed events into DataFrame with UTC timestamps."""
    df = mp_to_df(pc_raw)
    if len(df) == 0:
        return pd.DataFrame()

    df["user_id"] = df.get("$user_id", df.get("distinct_id", ""))
    df["time_utc"] = pd.to_datetime(df["time"], unit="s", utc=True)
    df["country"] = df.get("mp_country_code", df.get("country", "")).str.upper()
    df["browser"] = df.get("$browser", "")
    df["version"] = df.get("version", "")
    df["region"] = df["country"].apply(region_label)
    df["is_cis"] = df["country"].apply(is_cis)

    # Cashback traces (partial observability)
    df["cashback_list"] = df.get("cashback_list", "")

    # Existing sk/af from Mixpanel properties
    df["mp_sk"] = df.get("sk", "")
    df["mp_last_sk"] = df.get("last_sk", "")
    df["mp_af"] = df.get("af", "")
    df["mp_last_af"] = df.get("last_af", "")

    return df


def prepare_purchase(p_raw: list[dict]) -> pd.DataFrame:
    """Parse Purchase events into DataFrame."""
    df = mp_to_df(p_raw)
    if len(df) == 0:
        return pd.DataFrame()

    df["user_id"] = df.get("$user_id", df.get("distinct_id", ""))
    df["time_utc"] = pd.to_datetime(df["time"], unit="s", utc=True)
    return df


# ── 72h window reconstruction ────────────────────────────────────────────────

def build_user_events_index(events_b: pd.DataFrame) -> dict:
    """
    Build inverted index: guest_id -> sorted list of (ts, url, query_sk, product_id).
    For O(1) user lookup during attribution window reconstruction.
    """
    idx = defaultdict(list)
    for _, row in events_b.iterrows():
        idx[row["guest_id"]].append((
            row["created_ts"],
            row.get("url", ""),
            row.get("query_sk", ""),
            row.get("product_id"),
        ))
    # Sort each user's events by timestamp
    for uid in idx:
        idx[uid].sort(key=lambda x: x[0])
    return idx


def reconstruct_attribution_window(user_events: list, pc_time, is_cis_user: bool) -> dict:
    """
    For a single Purchase Completed, reconstruct the 72h attribution window.

    Returns dict with:
        has_our_marker, last_our_marker_ts,
        has_foreign_marker_after, has_cashback_trace,
        marker_type (sk/utm/none)
    """
    window_start = pc_time - timedelta(hours=ATTRIBUTION_WINDOW_H)

    our_marker_ts = None
    foreign_after_our = False
    has_any_af = False

    # Filter events in the 72h window
    window_events = [e for e in user_events if window_start <= e[0] <= pc_time]

    if is_cis_user:
        # CIS: check UTM in URL
        for ts, url, query_sk, pid in window_events:
            if is_alihelper_utm(url):
                our_marker_ts = ts

        if our_marker_ts:
            # Check for foreign UTM after our last marker
            for ts, url, query_sk, pid in window_events:
                if ts > our_marker_ts and is_foreign_utm(url):
                    foreign_after_our = True
                    break
    else:
        # Global: check sk in querySk
        for ts, url, query_sk, pid in window_events:
            if is_our_sk(query_sk):
                our_marker_ts = ts

        if our_marker_ts:
            # Check for foreign sk or af after our last marker
            for ts, url, query_sk, pid in window_events:
                if ts > our_marker_ts:
                    if has_foreign_sk(query_sk):
                        foreign_after_our = True
                        break
                    if has_af(query_sk):
                        has_any_af = True

    return {
        "has_our_marker": our_marker_ts is not None,
        "last_our_marker_ts": our_marker_ts,
        "has_foreign_after": foreign_after_our,
        "has_af_after": has_any_af,
    }


# ── Purchase matching ────────────────────────────────────────────────────────

def build_purchase_index(purchase_df: pd.DataFrame) -> dict:
    """Build user_id -> sorted list of purchase UTC timestamps."""
    idx = defaultdict(list)
    for _, row in purchase_df.iterrows():
        uid = row["user_id"]
        ts = row["time_utc"]
        if uid and pd.notna(ts):
            idx[uid].append(ts)
    for uid in idx:
        idx[uid].sort()
    return idx


def match_purchase(user_id: str, pc_time, purchase_idx: dict) -> bool:
    """Check if there's a Purchase within MATCH_WINDOW_S of this Purchase Completed."""
    candidates = purchase_idx.get(user_id, [])
    window = timedelta(seconds=MATCH_WINDOW_S)
    for p_time in candidates:
        if abs((pc_time - p_time).total_seconds()) <= MATCH_WINDOW_S:
            return True
    return False


# ── Reason code assignment ───────────────────────────────────────────────────

def assign_reason_code(row: dict, attr: dict, matched: bool, is_cis_user: bool) -> str:
    """
    Assign primary reason code for a Purchase Completed without matching Purchase.
    """
    if matched:
        return "MATCHED"

    if is_cis_user:
        if not attr["has_our_marker"]:
            return "CIS_NO_OUR_UTM_IN_72H"
        if attr["has_foreign_after"]:
            return "CIS_FOREIGN_UTM_AFTER_OURS"
        cashback = row.get("cashback_list", "")
        if cashback and str(cashback).strip() not in ("", "[]", "nan"):
            return "CIS_CASHBACK_TRACE"
        return "CIS_UNKNOWN"
    else:
        if not attr["has_our_marker"]:
            return "NO_OUR_SK_IN_72H"
        if attr["has_foreign_after"]:
            return "FOREIGN_SK_AFTER_OUR_SK"
        if attr["has_af_after"]:
            return "AF_AFTER_OUR_SK"
        cashback = row.get("cashback_list", "")
        if cashback and str(cashback).strip() not in ("", "[]", "nan"):
            return "CASHBACK_TRACE"
        return "UNKNOWN"


# ── Core analysis ────────────────────────────────────────────────────────────

def analyze(events_b: pd.DataFrame, pc_raw: list[dict],
            p_raw: list[dict], ac_raw: list[dict]) -> dict:
    """Run Problem B analysis. Returns results dict."""

    print_section("Problem B — Purchase Completed without Purchase")

    # ── Prepare data ────────────────────────────────────────────────────
    print("\n[1] Preparing Purchase Completed & Purchase...")
    pc_df = prepare_pc(pc_raw)
    p_df = prepare_purchase(p_raw)
    print(f"  Purchase Completed: {fmt(len(pc_df))}")
    print(f"  Purchase:           {fmt(len(p_df))}")

    # ── Build indexes ───────────────────────────────────────────────────
    print("\n[2] Building event indexes...")
    user_events_idx = build_user_events_index(events_b)
    purchase_idx = build_purchase_index(p_df)
    print(f"  Users with events:    {fmt(len(user_events_idx))}")
    print(f"  Users with purchases: {fmt(len(purchase_idx))}")

    # ── Affiliate Click index ───────────────────────────────────────────
    ac_df = mp_to_df(ac_raw)
    ac_users = set()
    if len(ac_df) > 0:
        ac_df["user_id"] = ac_df.get("$user_id", ac_df.get("distinct_id", ""))
        ac_users = set(ac_df["user_id"].dropna().unique())

    # ── Process each Purchase Completed ─────────────────────────────────
    print("\n[3] Reconstructing attribution windows & matching...")
    results_rows = []

    for i, (_, row) in enumerate(pc_df.iterrows()):
        uid = row["user_id"]
        pc_time = row["time_utc"]
        is_cis_user = row["is_cis"]

        # Reconstruct 72h window
        user_ev = user_events_idx.get(uid, [])
        attr = reconstruct_attribution_window(user_ev, pc_time, is_cis_user)

        # Match to Purchase
        matched = match_purchase(uid, pc_time, purchase_idx)

        # Assign reason code
        reason = assign_reason_code(row.to_dict(), attr, matched, is_cis_user)

        results_rows.append({
            "user_id": uid,
            "pc_time": pc_time,
            "country": row["country"],
            "region": row["region"],
            "is_cis": is_cis_user,
            "browser": row["browser"],
            "version": row["version"],
            "matched": matched,
            "has_our_marker": attr["has_our_marker"],
            "has_foreign_after": attr["has_foreign_after"],
            "has_af_after": attr.get("has_af_after", False),
            "had_ac": uid in ac_users,
            "reason_code": reason,
            "cashback_list": row.get("cashback_list", ""),
        })

        if (i + 1) % 5000 == 0:
            print(f"    ... {i + 1:,} / {len(pc_df):,} processed")

    res_df = pd.DataFrame(results_rows)
    print(f"  Processed: {fmt(len(res_df))}")

    # ── Summary ─────────────────────────────────────────────────────────
    print_section("Problem B — Summary")
    results = {"summary": {}, "reason_codes": {}, "segments": {}}

    total_pc = len(res_df)
    matched = res_df["matched"].sum()
    unmatched = total_pc - matched
    print(f"  Total Purchase Completed: {fmt(total_pc)}")
    print(f"  Matched to Purchase:      {fmt(matched)} ({pct(matched, total_pc)})")
    print(f"  Unmatched (gap):          {fmt(unmatched)} ({pct(unmatched, total_pc)})")
    results["summary"]["total_pc"] = int(total_pc)
    results["summary"]["matched"] = int(matched)
    results["summary"]["unmatched"] = int(unmatched)

    # ── B1: Attribution evidence ────────────────────────────────────────
    print_section("B1 — Attribution evidence")
    for reg in ["Global", "CIS"]:
        sub = res_df[res_df["region"] == reg]
        with_marker = sub["has_our_marker"].sum()
        print(f"  {reg}: {fmt(with_marker)}/{fmt(len(sub))} "
              f"({pct(with_marker, len(sub))}) had our affiliate marker in 72h")
    results["attribution"] = {
        "global_with_marker": int(res_df[(res_df["region"] == "Global")]["has_our_marker"].sum()),
        "cis_with_marker": int(res_df[(res_df["region"] == "CIS")]["has_our_marker"].sum()),
    }

    # ── B2: Overwrite analysis ──────────────────────────────────────────
    print_section("B2 — Overwrite analysis")
    for reg in ["Global", "CIS"]:
        sub = res_df[(res_df["region"] == reg) & (res_df["has_our_marker"])]
        overwritten = sub["has_foreign_after"].sum()
        print(f"  {reg}: {fmt(overwritten)}/{fmt(len(sub))} "
              f"({pct(overwritten, len(sub))}) overwritten by foreign affiliate")

    # ── Reason code breakdown ───────────────────────────────────────────
    print_section("Reason code breakdown")
    unmatched_df = res_df[~res_df["matched"]]

    for reg in ["Global", "CIS", "All"]:
        if reg == "All":
            sub = unmatched_df
        else:
            sub = unmatched_df[unmatched_df["region"] == reg]

        if len(sub) == 0:
            continue

        print(f"\n  ── {reg} (unmatched: {fmt(len(sub))}) ──")
        rc = sub["reason_code"].value_counts()
        rc_pct = sub["reason_code"].value_counts(normalize=True) * 100
        rc_table = pd.DataFrame({"count": rc, "pct": rc_pct}).reset_index()
        rc_table.columns = ["reason_code", "count", "pct"]
        print(tabulate(rc_table, headers="keys", tablefmt="simple",
                       floatfmt=".1f", showindex=False))
        results["reason_codes"][reg] = rc_table.to_dict("records")

    # ── B4: Matching sensitivity ────────────────────────────────────────
    print_section("B4 — Matching sensitivity check")
    for window_s in [5 * 60, 10 * 60, 15 * 60, 20 * 60]:
        matched_count = 0
        for _, row in pc_df.iterrows():
            uid = row["user_id"]
            pc_time = row["time_utc"]
            candidates = purchase_idx.get(uid, [])
            for p_time in candidates:
                if abs((pc_time - p_time).total_seconds()) <= window_s:
                    matched_count += 1
                    break
        print(f"  Window {window_s // 60}min: {fmt(matched_count)}/{fmt(len(pc_df))} "
              f"({pct(matched_count, len(pc_df))})")

    # ── B5: Segment-level loss rate ─────────────────────────────────────
    print_section("B5 — Segment-level loss rate")
    res_df["browser_fam"] = res_df["browser"].apply(browser_family)
    res_df["lineage"] = res_df["browser_fam"].apply(lineage)

    seg_dims = {
        "region": "region",
        "browser": "browser_fam",
        "lineage": "lineage",
        "country": "country",
    }

    for seg_name, col in seg_dims.items():
        print(f"\n  ── By {seg_name} ──")
        seg = res_df.groupby(col).agg(
            total=("matched", "count"),
            matched=("matched", "sum"),
        ).reset_index()
        seg["unmatched"] = seg["total"] - seg["matched"]
        seg["loss_rate"] = seg.apply(lambda r: pct_f(r["unmatched"], r["total"]), axis=1)
        seg = seg.sort_values("total", ascending=False).head(20)
        print(tabulate(seg, headers="keys", tablefmt="simple",
                       floatfmt=".1f", showindex=False))
        results["segments"][seg_name] = seg.to_dict("records")

    # ── Save ────────────────────────────────────────────────────────────
    with open(CACHE_DIR / "results_b.pkl", "wb") as f:
        pickle.dump(results, f)
    with open(CACHE_DIR / "results_b_detail.pkl", "wb") as f:
        pickle.dump(res_df, f)
    print(f"\n  Results saved to {CACHE_DIR / 'results_b.pkl'}")

    return results


# ── Main ─────────────────────────────────────────────────────────────────────

def run():
    events_b = _load_pkl("events_b")
    pc_raw = _load_json("pc_b")
    p_raw = _load_json("purchase_b")
    ac_raw = _load_json("aff_click_a")

    analyze(events_b, pc_raw, p_raw, ac_raw)


if __name__ == "__main__":
    run()
