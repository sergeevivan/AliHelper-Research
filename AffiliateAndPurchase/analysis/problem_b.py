#!/usr/bin/env python3
"""
Problem B — Purchase Completed without Purchase.

Why do we see more Purchase Completed than commission-bearing Purchase?

For each Purchase Completed:
  - Reconstruct 72h pre-purchase attribution window from MongoDB events
    - Global: owned `sk` (whitelist); foreign overwrite = later foreign sk OR
              later `af` on an aliexpress.* host (third-party on Global)
    - CIS   : Pattern A (`af=*_7685`) or Pattern B (full-UTM); separately
              track foreign-af vs foreign-utm overwrite; partial-UTM flagged
  - Match to Purchase by user + time proximity (10-min window)
  - Assign reason code

B6 — validate events-based reconstruction against Purchase Completed
client-side fields (`last_sk`, `last_af`, `last_utm_*`). Report agreement.

CIS-ness of each event is URL-domain-based (aliexpress.ru), not country.

Usage:
    REPORT_MODE=oneoff|deep python -m analysis.problem_b
"""

import pickle
from collections import defaultdict
from datetime import timedelta

import pandas as pd
from tabulate import tabulate

from src.config import (
    CACHE_DIR, CACHE_SUFFIX, REPORT_MODE, PROBLEM_B_ENABLED,
    B_START, B_END, ATTRIBUTION_WINDOW_H, MATCH_WINDOW_S, OUR_SKS, EPN_SUFFIX,
    PROXY_RETURN_WINDOW_S,
)
from src.utils import (
    print_section, pct, pct_f, fmt,
    browser_family, lineage_from_build, lineage_segment, region_label,
    is_cis_country, is_aliexpress_ru, is_aliexpress_host, mp_to_df,
    classify_event, classify_cis_utm, is_our_af_value, is_foreign_af_value,
    is_our_sk_value, is_foreign_sk_value,
)


# ── Data loading ─────────────────────────────────────────────────────────────

def _key(name: str) -> str:
    return f"{name}__{CACHE_SUFFIX}"


def _load_pkl(name):
    path = CACHE_DIR / f"{_key(name)}.pkl"
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


# ── Purchase Completed preparation ───────────────────────────────────────────

def prepare_pc(pc_raw: list[dict]) -> pd.DataFrame:
    """Parse Purchase Completed events into DataFrame with UTC timestamps."""
    df = mp_to_df(pc_raw)
    if len(df) == 0:
        return pd.DataFrame()

    df["user_id"] = df.get("$user_id", df.get("distinct_id", ""))
    df["time_utc"] = pd.to_datetime(df["time"], unit="s", utc=True)
    df["country"] = df.get("mp_country_code", df.get("country", "")).fillna("").str.upper()
    df["browser"] = df.get("$browser", "")
    df["version"] = df.get("version", "")
    df["region"] = df["country"].apply(region_label)
    df["is_cis_user"] = df["country"].apply(is_cis_country)

    # Client-side attribution state (validation / fallback only, NOT authoritative)
    df["pc_sk"]            = df.get("sk", "")
    df["pc_last_sk"]       = df.get("last_sk", "")
    df["pc_af"]            = df.get("af", "")
    df["pc_last_af"]       = df.get("last_af", "")
    df["pc_last_utm_campaign"] = df.get("last_utm_campaign", "")
    df["pc_last_utm_source"]   = df.get("last_utm_source", "")
    df["pc_last_utm_medium"]   = df.get("last_utm_medium", "")
    df["pc_is_cis"]        = df.get("is_CIS", None)
    df["cashback_list"]    = df.get("cashback_list", "")

    return df


def prepare_purchase(p_raw: list[dict]) -> pd.DataFrame:
    df = mp_to_df(p_raw)
    if len(df) == 0:
        return pd.DataFrame()
    df["user_id"] = df.get("$user_id", df.get("distinct_id", ""))
    df["time_utc"] = pd.to_datetime(df["time"], unit="s", utc=True)
    return df


# ── 72h window reconstruction ────────────────────────────────────────────────

def build_user_events_index(events_b: pd.DataFrame) -> dict:
    """
    Build inverted index: guest_id -> sorted list of classified event records.
    Each record is a dict produced by classify_event() plus ts.
    """
    idx = defaultdict(list)
    for _, row in events_b.iterrows():
        ev = classify_event({
            "url": row.get("url", ""),
            "query_sk": row.get("query_sk", ""),
            "params": row.get("params"),
        })
        ev["ts"] = row["created_ts"]
        idx[row["guest_id"]].append(ev)
    for uid in idx:
        idx[uid].sort(key=lambda x: x["ts"])
    return idx


def reconstruct_attribution_window(user_events: list, pc_time,
                                   ac_times: list | None = None) -> dict:
    """
    For a single Purchase Completed, reconstruct 72h window.

    CIS-ness of the trail is per-event (URL-based), not per-user. A user
    can have both Global and CIS activity in the same window.

    Returns dict with:
      - Global side: has_owned_sk, owned_sk_ts, foreign_sk_after, af_on_global_after
      - CIS side   : has_owned_af, owned_af_ts, has_owned_utm_full, utm_full_ts,
                     has_owned_utm_partial, utm_partial_ts,
                     foreign_af_after, foreign_utm_after
      - has_any_ali_ru (any event on aliexpress.ru in window)
      - has_proxy_return (ali_ru event within ≤120s after any Affiliate Click
                          that itself falls inside the 72h window)
    """
    window_start = pc_time - timedelta(hours=ATTRIBUTION_WINDOW_H)
    window = [e for e in user_events if window_start <= e["ts"] <= pc_time]

    out = {
        # Global
        "has_owned_sk": False, "owned_sk_ts": None,
        "foreign_sk_after": False, "af_on_global_after": False,
        # CIS
        "has_owned_af": False, "owned_af_ts": None,
        "has_owned_utm_full": False, "utm_full_ts": None,
        "has_owned_utm_partial": False, "utm_partial_ts": None,
        "foreign_af_after": False, "foreign_utm_after": False,
        "has_any_ali_ru": False,
        "has_proxy_return": False,
    }

    # First pass: record latest owned markers by type
    for e in window:
        if e["is_ali_ru"]:
            out["has_any_ali_ru"] = True
        label = e["label"]
        if label == "GLOBAL_DIRECT":
            out["has_owned_sk"] = True
            out["owned_sk_ts"] = e["ts"]
        elif label == "CIS_DIRECT_AF":
            out["has_owned_af"] = True
            out["owned_af_ts"] = e["ts"]
        elif label == "CIS_DIRECT_UTM":
            out["has_owned_utm_full"] = True
            out["utm_full_ts"] = e["ts"]
        elif label == "CIS_PARTIAL_UTM":
            out["has_owned_utm_partial"] = True
            out["utm_partial_ts"] = e["ts"]

    # Second pass: foreign-after for each owned marker
    last_owned_sk = out["owned_sk_ts"]
    last_owned_cis = max(
        (t for t in [out["owned_af_ts"], out["utm_full_ts"], out["utm_partial_ts"]] if t),
        default=None,
    )

    for e in window:
        fk = e["foreign_kind"]
        if fk is None:
            continue
        t = e["ts"]
        if last_owned_sk and t > last_owned_sk:
            if fk == "sk":
                out["foreign_sk_after"] = True
            elif fk == "af_on_global":
                out["af_on_global_after"] = True
        if last_owned_cis and t > last_owned_cis:
            if fk == "af":
                out["foreign_af_after"] = True
            elif fk == "utm":
                out["foreign_utm_after"] = True

    # Proxy-return: ali_ru event within PROXY_RETURN_WINDOW_S after an
    # Affiliate Click that itself falls inside the 72h window.
    if ac_times:
        for ac_ts in ac_times:
            if not (window_start <= ac_ts <= pc_time):
                continue
            deadline = ac_ts + timedelta(seconds=PROXY_RETURN_WINDOW_S)
            for e in window:
                if e["is_ali_ru"] and ac_ts <= e["ts"] <= deadline:
                    out["has_proxy_return"] = True
                    break
            if out["has_proxy_return"]:
                break

    return out


# ── Purchase matching ────────────────────────────────────────────────────────

def build_purchase_index(purchase_df: pd.DataFrame) -> dict:
    idx = defaultdict(list)
    for _, row in purchase_df.iterrows():
        uid = row["user_id"]
        ts = row["time_utc"]
        if uid and pd.notna(ts):
            idx[uid].append(ts)
    for uid in idx:
        idx[uid].sort()
    return idx


def match_purchase(user_id: str, pc_time, purchase_idx: dict, window_s: int = MATCH_WINDOW_S) -> bool:
    for p_time in purchase_idx.get(user_id, []):
        if abs((pc_time - p_time).total_seconds()) <= window_s:
            return True
    return False


# ── Reason code assignment ───────────────────────────────────────────────────

def assign_reason_code(row: dict, attr: dict, matched: bool,
                       had_ac: bool, likely_delayed: bool = False) -> str:
    """
    Decide primary reason code. Uses per-event trail (`attr`) rather than
    user country to decide CIS vs Global branch — defined by what actually
    happened in the 72h window.

    `likely_delayed` is set by B3 (delayed-postback heuristic) — when a
    Purchase for this user exists within ±24h of PC but outside the 10-min
    match window, the gap is most likely a postback arriving late.
    """
    if matched:
        return "MATCHED"

    any_cis_owned = (attr["has_owned_af"] or attr["has_owned_utm_full"]
                     or attr["has_owned_utm_partial"])
    has_cashback = False
    cb = row.get("cashback_list", "")
    if cb and str(cb).strip() not in ("", "[]", "nan", "None"):
        has_cashback = True

    # CIS branch: any aliexpress.ru activity in window indicates CIS purchase
    if attr["has_any_ali_ru"]:
        # Delayed postback takes precedence when strong evidence exists
        if likely_delayed:
            return "CIS_LIKELY_DELAYED_POSTBACK"

        if not any_cis_owned and not attr["has_owned_sk"]:
            # Proxy-only trail: hub reached + return ≤120s but no owned marker
            if had_ac and attr["has_proxy_return"]:
                return "CIS_PROXY_ONLY"
            if not had_ac:
                return "CIS_NO_HUB_REACH_OBSERVED"
            return "CIS_NO_OUR_SIGNAL_IN_72H"

        if any_cis_owned:
            # Report foreign-af and foreign-utm separately
            if attr["foreign_af_after"]:
                return "CIS_FOREIGN_AF_AFTER_OURS"
            if attr["foreign_utm_after"]:
                return "CIS_FOREIGN_UTM_AFTER_OURS"
            # Only partial UTM evidence and no foreign overwrite?
            if (attr["has_owned_utm_partial"] and not attr["has_owned_af"]
                    and not attr["has_owned_utm_full"]):
                return "CIS_PARTIAL_UTM_ONLY"
            if had_ac:
                return "CIS_HUB_REACHED_NO_RETURN"
            if has_cashback:
                return "CIS_CASHBACK_TRACE"
            return "CIS_UNKNOWN"

        # No CIS owned, but maybe proxy-only trail available — encoded upstream
        if has_cashback:
            return "CIS_CASHBACK_TRACE"
        return "CIS_UNKNOWN"

    # Global branch
    if likely_delayed:
        return "LIKELY_DELAYED_POSTBACK"
    if not attr["has_owned_sk"]:
        return "NO_OUR_SK_IN_72H"
    if attr["foreign_sk_after"]:
        return "FOREIGN_SK_AFTER_OUR_SK"
    if attr["af_on_global_after"]:
        return "AF_AFTER_OUR_SK"
    if has_cashback:
        return "CASHBACK_TRACE"
    return "UNKNOWN"


# ── B6: PC field vs events reconstruction ────────────────────────────────────

def validate_pc_fields(row: dict, attr: dict) -> dict:
    """
    Compare events-based reconstruction (attr) to client-side PC fields.
    Agreement is counted only when PC field is present (non-empty).
    """
    out = {"sk_checked": False, "sk_agree": None,
           "af_checked": False, "af_agree": None,
           "utm_checked": False, "utm_agree": None}

    pc_last_sk = (row.get("pc_last_sk") or "").strip()
    if pc_last_sk:
        out["sk_checked"] = True
        pc_owned = is_our_sk_value(pc_last_sk)
        out["sk_agree"] = (pc_owned == attr["has_owned_sk"])

    pc_last_af = (row.get("pc_last_af") or "").strip()
    if pc_last_af:
        out["af_checked"] = True
        pc_owned_af = is_our_af_value(pc_last_af)
        out["af_agree"] = (pc_owned_af == attr["has_owned_af"])

    pc_cam = (row.get("pc_last_utm_campaign") or "").strip()
    if pc_cam:
        out["utm_checked"] = True
        pc_utm_owned = (
            pc_cam.endswith(EPN_SUFFIX)
            and (row.get("pc_last_utm_source") == "aerkol")
            and (row.get("pc_last_utm_medium") == "cpa")
        )
        out["utm_agree"] = (pc_utm_owned == attr["has_owned_utm_full"])

    return out


# ── Core analysis ────────────────────────────────────────────────────────────

def analyze(events_b: pd.DataFrame, pc_raw: list[dict],
            p_raw: list[dict], ac_raw: list[dict],
            clients: pd.DataFrame | None = None,
            gsh: pd.DataFrame | None = None) -> dict:

    if not PROBLEM_B_ENABLED:
        print("Problem B is disabled in this mode (pulse). Nothing to do.")
        return {}

    print_section(f"Problem B — Purchase Completed without Purchase (mode={REPORT_MODE})")
    print(f"  Window: {B_START} → {B_END}")

    # ── Prepare ────────────────────────────────────────────────────────
    print("\n[1] Preparing Purchase Completed & Purchase...")
    pc_df = prepare_pc(pc_raw)
    p_df = prepare_purchase(p_raw)
    print(f"  Purchase Completed: {fmt(len(pc_df))}")
    print(f"  Purchase:           {fmt(len(p_df))}")

    # ── Indexes ────────────────────────────────────────────────────────
    print("\n[2] Building event indexes...")
    user_events_idx = build_user_events_index(events_b)
    purchase_idx = build_purchase_index(p_df)
    print(f"  Users with events:    {fmt(len(user_events_idx))}")
    print(f"  Users with purchases: {fmt(len(purchase_idx))}")

    ac_df = mp_to_df(ac_raw)
    ac_users = set()
    ac_times_by_user = defaultdict(list)
    if len(ac_df) > 0:
        ac_df["user_id"] = ac_df.get("$user_id", ac_df.get("distinct_id", ""))
        ac_users = set(ac_df["user_id"].dropna().unique())
        if "time" in ac_df.columns:
            ac_df["ac_ts"] = pd.to_datetime(ac_df["time"], unit="s", utc=True)
            for _, r in ac_df.iterrows():
                uid = r.get("user_id", "")
                ts = r.get("ac_ts")
                if uid and pd.notna(ts):
                    ac_times_by_user[uid].append(ts)
            for uid in ac_times_by_user:
                ac_times_by_user[uid].sort()

    # B3 helper: does user have any Purchase within ±24h of pc_time?
    def _has_purchase_nearby(uid: str, pc_time, radius_s: int = 24 * 3600) -> bool:
        for p_time in purchase_idx.get(uid, []):
            if abs((pc_time - p_time).total_seconds()) <= radius_s:
                return True
        return False

    # ── Iterate PCs ────────────────────────────────────────────────────
    print("\n[3] Reconstructing attribution windows & matching...")
    rows = []
    pc_validation = []
    match_deltas_s = []  # B3: (Purchase.time - PC.time) seconds for matched pairs

    for i, (_, row) in enumerate(pc_df.iterrows()):
        uid = row["user_id"]
        pc_time = row["time_utc"]

        user_ev = user_events_idx.get(uid, [])
        user_ac_times = ac_times_by_user.get(uid, [])
        attr = reconstruct_attribution_window(user_ev, pc_time,
                                              ac_times=user_ac_times)
        matched = match_purchase(uid, pc_time, purchase_idx)

        # B3: record closest Purchase delta for matched pairs
        if matched:
            closest = min(
                ((p - pc_time).total_seconds()
                 for p in purchase_idx.get(uid, [])),
                key=abs,
                default=None,
            )
            if closest is not None:
                match_deltas_s.append(closest)

        # B3: "likely delayed" when unmatched but Purchase exists within ±24h
        likely_delayed = False
        if not matched and _has_purchase_nearby(uid, pc_time):
            likely_delayed = True

        had_ac = uid in ac_users
        reason = assign_reason_code(row.to_dict(), attr, matched, had_ac,
                                    likely_delayed=likely_delayed)

        # B6 validation
        validation = validate_pc_fields(row.to_dict(), attr)
        pc_validation.append(validation)

        # Effective region: if CIS activity in window → CIS; else Global
        eff_region = "CIS" if attr["has_any_ali_ru"] else "Global"

        rows.append({
            "user_id": uid,
            "pc_time": pc_time,
            "country": row["country"],
            "user_region": row["region"],
            "eff_region": eff_region,
            "browser": row["browser"],
            "version": row["version"],
            "matched": matched,
            "likely_delayed": likely_delayed,
            "had_ac": had_ac,
            "has_owned_sk": attr["has_owned_sk"],
            "has_owned_af": attr["has_owned_af"],
            "has_owned_utm_full": attr["has_owned_utm_full"],
            "has_owned_utm_partial": attr["has_owned_utm_partial"],
            "foreign_sk_after": attr["foreign_sk_after"],
            "af_on_global_after": attr["af_on_global_after"],
            "foreign_af_after": attr["foreign_af_after"],
            "foreign_utm_after": attr["foreign_utm_after"],
            "has_any_ali_ru": attr["has_any_ali_ru"],
            "has_proxy_return": attr["has_proxy_return"],
            "reason_code": reason,
            "cashback_list": row.get("cashback_list", ""),
        })

        if (i + 1) % 5000 == 0:
            print(f"    ... {i + 1:,} / {len(pc_df):,} processed")

    res_df = pd.DataFrame(rows)
    print(f"  Processed: {fmt(len(res_df))}")

    # ── Summary ────────────────────────────────────────────────────────
    print_section("Problem B — Summary")
    results = {
        "meta": {
            "report_mode": REPORT_MODE,
            "period_start": B_START.isoformat(),
            "period_end": B_END.isoformat(),
        },
        "summary": {},
        "reason_codes": {},
        "segments": {},
    }

    total_pc = len(res_df)
    matched = int(res_df["matched"].sum())
    unmatched = total_pc - matched
    print(f"  Total Purchase Completed: {fmt(total_pc)}")
    print(f"  Matched to Purchase:      {fmt(matched)} ({pct(matched, total_pc)})")
    print(f"  Unmatched (gap):          {fmt(unmatched)} ({pct(unmatched, total_pc)})")
    results["summary"] = {"total_pc": total_pc, "matched": matched, "unmatched": unmatched}

    # ── B1: Attribution evidence (split by effective region) ───────────
    print_section("B1 — Attribution evidence (by effective region)")
    b1 = {}
    for reg, mask in [
        ("Global", res_df["eff_region"] == "Global"),
        ("CIS",    res_df["eff_region"] == "CIS"),
    ]:
        sub = res_df[mask]
        g_sk    = int(sub["has_owned_sk"].sum())
        c_af    = int(sub["has_owned_af"].sum())
        c_utm   = int(sub["has_owned_utm_full"].sum())
        c_part  = int(sub["has_owned_utm_partial"].sum())
        any_ow  = int((sub["has_owned_sk"] | sub["has_owned_af"]
                       | sub["has_owned_utm_full"] | sub["has_owned_utm_partial"]).sum())
        b1[reg] = {
            "total": int(len(sub)), "any_owned": any_ow,
            "owned_sk": g_sk, "owned_af": c_af,
            "owned_utm_full": c_utm, "owned_utm_partial": c_part,
        }
        print(f"  {reg}: total={fmt(len(sub))} any_owned={fmt(any_ow)} "
              f"({pct(any_ow, len(sub))})")
        if reg == "CIS":
            print(f"    Pattern A (af=*_7685):      {fmt(c_af)}  ({pct(c_af, len(sub))})")
            print(f"    Pattern B (full UTM _7685): {fmt(c_utm)} ({pct(c_utm, len(sub))})")
            print(f"    Partial UTM only:           {fmt(c_part)} ({pct(c_part, len(sub))})")
    results["attribution"] = b1

    # ── B2: Overwrite (foreign-af vs foreign-utm separately) ───────────
    print_section("B2 — Overwrite analysis (split)")
    b2 = {}
    for reg, mask in [
        ("Global", res_df["eff_region"] == "Global"),
        ("CIS",    res_df["eff_region"] == "CIS"),
    ]:
        sub = res_df[mask]
        ow_any_owned = sub[sub["has_owned_sk"] | sub["has_owned_af"]
                           | sub["has_owned_utm_full"]]
        fsk = int(ow_any_owned["foreign_sk_after"].sum())
        fgla = int(ow_any_owned["af_on_global_after"].sum())
        faf = int(ow_any_owned["foreign_af_after"].sum())
        futm = int(ow_any_owned["foreign_utm_after"].sum())
        b2[reg] = {
            "with_owned": int(len(ow_any_owned)),
            "foreign_sk_after": fsk,
            "af_on_global_after": fgla,
            "foreign_af_after": faf,
            "foreign_utm_after": futm,
        }
        print(f"  {reg}: {fmt(len(ow_any_owned))} with owned marker")
        print(f"    foreign sk after our sk:       {fmt(fsk)} ({pct(fsk, len(ow_any_owned))})")
        print(f"    af on Global after our sk:     {fmt(fgla)} ({pct(fgla, len(ow_any_owned))})")
        print(f"    foreign af after our CIS:      {fmt(faf)} ({pct(faf, len(ow_any_owned))})")
        print(f"    foreign utm after our CIS:     {fmt(futm)} ({pct(futm, len(ow_any_owned))})")
    results["overwrite"] = b2

    # ── Reason code breakdown ──────────────────────────────────────────
    print_section("Reason code breakdown")
    unmatched_df = res_df[~res_df["matched"]]
    for reg in ["Global", "CIS", "All"]:
        if reg == "All":
            sub = unmatched_df
        else:
            sub = unmatched_df[unmatched_df["eff_region"] == reg]
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

    # ── B3: Delayed postback analysis ─────────────────────────────────
    print_section("B3 — Delayed postback analysis")
    b3 = {}
    if match_deltas_s:
        deltas = pd.Series(match_deltas_s)
        b3["matched_count"] = int(len(deltas))
        b3["p50_sec"] = float(deltas.median())
        b3["p95_sec"] = float(deltas.quantile(0.95))
        b3["max_sec"] = float(deltas.abs().max())
        print(f"  matched: {fmt(b3['matched_count'])}  "
              f"p50={b3['p50_sec']:.0f}s  p95={b3['p95_sec']:.0f}s  "
              f"max={b3['max_sec']:.0f}s")
    else:
        b3["matched_count"] = 0
    ld = int(res_df["likely_delayed"].sum())
    ld_share = pct_f(ld, len(res_df))
    b3["likely_delayed"] = ld
    b3["likely_delayed_share_pct"] = ld_share
    print(f"  unmatched with Purchase within ±24h (likely delayed): "
          f"{fmt(ld)} ({ld_share:.1f}%)")
    results["delayed_postback"] = b3

    # ── Enrich res_df with hub / version / multiclient for B5 ─────────
    if gsh is not None and len(gsh):
        gsh_sorted = gsh.sort_values("config_ts")
        latest = gsh_sorted.drop_duplicates(subset="guest_id", keep="last")
        cfg_map = latest.set_index("guest_id")["domain"].to_dict()
        res_df["cfg_domain"] = res_df["user_id"].map(cfg_map).fillna("<no_config>")
    else:
        res_df["cfg_domain"] = "<no_config>"

    if clients is not None and len(clients):
        mult = clients.groupby("guest_id").size()
        res_df["client_count"] = (
            res_df["user_id"].map(mult).fillna(0).astype(int)
        )
        res_df["multiclient"] = res_df["client_count"].apply(
            lambda n: "multi" if n >= 2 else ("single" if n == 1 else "no_client"))
    else:
        res_df["client_count"] = 0
        res_df["multiclient"] = "no_client"

    # ── B4: Matching sensitivity ──────────────────────────────────────
    print_section("B4 — Matching sensitivity check")
    b4 = []
    for window_s in [5 * 60, 10 * 60, 15 * 60, 20 * 60]:
        matched_count = 0
        for _, row in pc_df.iterrows():
            uid = row["user_id"]
            pc_time = row["time_utc"]
            if match_purchase(uid, pc_time, purchase_idx, window_s):
                matched_count += 1
        b4.append({"window_min": window_s // 60, "matched": int(matched_count),
                   "pct": pct_f(matched_count, len(pc_df))})
        print(f"  Window {window_s // 60}min: {fmt(matched_count)}/{fmt(len(pc_df))} "
              f"({pct(matched_count, len(pc_df))})")
    results["matching_sensitivity"] = b4

    # ── B5: Segment-level loss rate ───────────────────────────────────
    print_section("B5 — Segment-level loss rate")
    res_df["browser_fam"] = res_df["browser"].apply(browser_family)
    # Lineage here uses UA fallback only (PC events don't carry build_app)
    res_df["lineage"] = res_df["browser"].apply(
        lambda b: lineage_segment(None, b))

    seg_dims = {
        "eff_region": "eff_region",
        "browser": "browser_fam",
        "lineage": "lineage",
        "country": "country",
        "hub": "cfg_domain",
        "version": "version",
        "multiclient": "multiclient",
    }
    for seg_name, col in seg_dims.items():
        print(f"\n  ── By {seg_name} ──")
        seg = res_df.groupby(col, dropna=False).agg(
            total=("matched", "count"),
            matched=("matched", "sum"),
        ).reset_index()
        seg["unmatched"] = seg["total"] - seg["matched"]
        seg["loss_rate"] = seg.apply(lambda r: pct_f(r["unmatched"], r["total"]), axis=1)
        seg = seg.sort_values("total", ascending=False).head(20)
        print(tabulate(seg, headers="keys", tablefmt="simple",
                       floatfmt=".1f", showindex=False))
        results["segments"][seg_name] = seg.to_dict("records")

    # ── B6: PC field validation ───────────────────────────────────────
    print_section("B6 — PC field validation (events vs client-side last_*)")
    v_df = pd.DataFrame(pc_validation)
    b6 = {}
    for kind in ("sk", "af", "utm"):
        checked_col = f"{kind}_checked"
        agree_col = f"{kind}_agree"
        checked = int(v_df[checked_col].sum())
        agree = int(v_df[v_df[checked_col] == True][agree_col].sum())
        b6[kind] = {
            "checked": checked,
            "agree": agree,
            "agree_pct": pct_f(agree, checked),
        }
        print(f"  {kind}: checked={fmt(checked)} agree={fmt(agree)} "
              f"({pct(agree, checked)})")
    results["pc_field_validation"] = b6

    # ── Save ──────────────────────────────────────────────────────────
    out_path = CACHE_DIR / f"results_b__{CACHE_SUFFIX}.pkl"
    with open(out_path, "wb") as f:
        pickle.dump(results, f)
    detail_path = CACHE_DIR / f"results_b_detail__{CACHE_SUFFIX}.pkl"
    with open(detail_path, "wb") as f:
        pickle.dump(res_df, f)
    print(f"\n  Results saved to {out_path}")
    return results


# ── Main ─────────────────────────────────────────────────────────────────────

def run():
    if not PROBLEM_B_ENABLED:
        print("Problem B disabled in mode=pulse — skipping.")
        return
    events_b = _load_pkl("events_b")
    pc_raw = _load_json("pc_b")
    p_raw = _load_json("purchase_b")
    ac_raw = _load_json("aff_click_a")
    clients = _load_pkl("clients")
    gsh = _load_pkl("gsh")
    analyze(events_b, pc_raw, p_raw, ac_raw, clients=clients, gsh=gsh)


if __name__ == "__main__":
    run()
