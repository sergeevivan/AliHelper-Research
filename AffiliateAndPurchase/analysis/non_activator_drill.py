#!/usr/bin/env python3
"""
Non-activator deep drill — users who did NOT reach the hub (no Affiliate Click).

Answers:
  1. What pages were they on? (host / subtype / eligibility)
  2. How many pages per session? (sessions per user, events per session)
  3. How many eligible pages did they have?
  4. Distribution by build_app / client_version / lineage
"""

import json
import os
import pickle
from collections import defaultdict
from datetime import timedelta
from pathlib import Path
from urllib.parse import urlparse

import numpy as np
import pandas as pd
from tabulate import tabulate

from src.config import CACHE_DIR, CACHE_SUFFIX, SESSION_GAP_S, A_START, A_END

# Allow pinning to an existing cache slice (e.g. CACHE_SUFFIX_OVERRIDE=pulse__2026-04-14__2026-04-20)
SUFFIX = os.getenv("CACHE_SUFFIX_OVERRIDE", CACHE_SUFFIX)
from src.utils import (
    browser_family, lineage_segment, is_aliexpress_ru,
    matches_check_list_urls, is_eligible_product_page,
    product_page_subtype,
)


def _key(name):
    return f"{name}__{SUFFIX}"


def _load_pkl(name):
    path = CACHE_DIR / f"{_key(name)}.pkl"
    if not path.exists():
        legacy = CACHE_DIR / f"{name}.pkl"
        if legacy.exists():
            path = legacy
    with open(path, "rb") as f:
        return pickle.load(f)


def _load_json(name):
    with open(CACHE_DIR / f"{_key(name)}.json") as f:
        return json.load(f)


def _host(url: str) -> str:
    if not url:
        return "<empty>"
    try:
        h = (urlparse(url).hostname or "").lower()
        return h or "<empty>"
    except Exception:
        return "<parse_error>"


def _host_bucket(host: str) -> str:
    """Bucket hostnames into the relevant categories."""
    if not host or host == "<empty>":
        return "<empty>"
    if "aliexpress.ru" in host:
        return "aliexpress.ru"
    if "aliexpress." in host or "tmall." in host:
        return "aliexpress.global"
    return "other"


def _non_product_pagekind(url: str, host_bucket: str) -> str:
    """Rough URL-intent classifier for pages that are NOT eligible products."""
    if host_bucket == "other" or host_bucket == "<empty>":
        return "non_ali"
    try:
        p = urlparse(url)
        path = (p.path or "").lower()
    except Exception:
        return "other_ali"
    if path in ("", "/", "/index.html"):
        return "home"
    if "/wholesale" in path or "/category" in path or "/categories" in path:
        return "category_search"
    if "/search" in path or "/w/" in path:
        return "search"
    if "/cart" in path:
        return "cart"
    if "/store/" in path and "/product/" not in path:
        return "store_page"
    if "/p/" in path:
        return "p_page"
    if "/item/" in path:
        return "item_other"
    if "/ssr/" in path:
        return "ssr_other"
    if "/af/" in path or "/afs/" in path:
        return "affiliate_landing"
    return "other_ali"


def main():
    print(f"\n{'=' * 72}")
    print(f"  Non-activator drill — suffix {SUFFIX}")
    print(f"{'=' * 72}\n")

    # ── Load ─────────────────────────────────────────────────────────
    ev = _load_pkl("events_a")
    clients = _load_pkl("clients")
    ac_raw = _load_json("aff_click_a")

    # Flatten Mixpanel
    ac_rows = [r.get("properties", {}) for r in ac_raw]
    ac_df = pd.DataFrame(ac_rows)
    if "$user_id" in ac_df.columns:
        ac_users = set(ac_df["$user_id"].dropna().astype(str).unique())
    else:
        ac_users = set()
    print(f"Affiliate Click users (all Mixpanel rows): {len(ac_users):,}")

    # ── Enrich events ────────────────────────────────────────────────
    cl = clients.drop_duplicates(subset="guest_id", keep="last")
    cl = cl.set_index("guest_id")[["browser", "country", "client_version",
                                   "os", "build_app"]]
    ev = ev.join(cl, on="guest_id", how="left")
    ev["browser_fam"] = ev["browser"].apply(browser_family)
    ev["lineage"] = ev.apply(
        lambda r: lineage_segment(r.get("build_app"), r.get("browser")), axis=1
    )
    ev["host"] = ev["url"].apply(_host)
    ev["host_bucket"] = ev["host"].apply(_host_bucket)
    ev["subtype"] = ev["url"].apply(product_page_subtype)
    ev["eligible"] = ev.apply(
        lambda r: (matches_check_list_urls(r["url"])
                   if r["lineage"] == "auto_redirect"
                   else (is_eligible_product_page(r["product_id"])
                         if r["lineage"] == "dogi" else False)),
        axis=1,
    )
    ev["pagekind"] = ev.apply(
        lambda r: r["subtype"] if r["subtype"]
        else _non_product_pagekind(r["url"], r["host_bucket"]),
        axis=1,
    )

    # ── Split activator / non-activator ──────────────────────────────
    ev["is_activator"] = ev["guest_id"].isin(ac_users)
    non_ev = ev[~ev["is_activator"]].copy()
    act_ev = ev[ev["is_activator"]].copy()

    n_non_users = non_ev["guest_id"].nunique()
    n_act_users = act_ev["guest_id"].nunique()
    n_total_users = ev["guest_id"].nunique()
    print(f"Total users in window:   {n_total_users:,}")
    print(f"Activators (reached hub): {n_act_users:,} "
          f"({100*n_act_users/n_total_users:.1f}%)")
    print(f"Non-activators:           {n_non_users:,} "
          f"({100*n_non_users/n_total_users:.1f}%)\n")

    # ────────────────────────────────────────────────────────────────
    # 1. What pages did non-activators visit?
    # ────────────────────────────────────────────────────────────────
    print("=" * 72)
    print(" 1. Page distribution — non-activators vs activators")
    print("=" * 72)

    def _host_dist(df):
        s = df["host_bucket"].value_counts()
        total = len(df)
        return {k: (int(v), 100*v/total if total else 0.0) for k, v in s.items()}

    non_h = _host_dist(non_ev)
    act_h = _host_dist(act_ev)
    all_hosts = sorted(set(non_h) | set(act_h))
    rows = []
    for h in all_hosts:
        n_c, n_p = non_h.get(h, (0, 0.0))
        a_c, a_p = act_h.get(h, (0, 0.0))
        rows.append([h, f"{n_c:,}", f"{n_p:.1f}%", f"{a_c:,}", f"{a_p:.1f}%"])
    print("\n── Host bucket (event-level) ──")
    print(tabulate(rows, headers=["host", "non_act events", "non_act %",
                                  "act events", "act %"], tablefmt="simple"))

    # Page kind — combine subtype + non-product bucket
    def _pagekind_dist(df):
        s = df["pagekind"].fillna("<none>").value_counts().head(20)
        total = len(df)
        return [(k, int(v), 100*v/total if total else 0.0) for k, v in s.items()]

    print("\n── Page kind (top 20, non-activator events) ──")
    pk_non = _pagekind_dist(non_ev)
    print(tabulate([[k, f"{c:,}", f"{p:.1f}%"] for k, c, p in pk_non],
                   headers=["pagekind", "events", "%"], tablefmt="simple"))

    print("\n── Page kind (top 20, activator events) ──")
    pk_act = _pagekind_dist(act_ev)
    print(tabulate([[k, f"{c:,}", f"{p:.1f}%"] for k, c, p in pk_act],
                   headers=["pagekind", "events", "%"], tablefmt="simple"))

    # Eligible events: what fraction of non-activator visits are on product pages?
    non_elig = int(non_ev["eligible"].sum())
    non_total_ev = len(non_ev)
    act_elig = int(act_ev["eligible"].sum())
    act_total_ev = len(act_ev)
    print("\n── Eligible-page share (event-level) ──")
    print(tabulate([
        ["non-activator", f"{non_elig:,}", f"{non_total_ev:,}",
         f"{100*non_elig/non_total_ev:.1f}%"],
        ["activator", f"{act_elig:,}", f"{act_total_ev:,}",
         f"{100*act_elig/act_total_ev:.1f}%"],
    ], headers=["cohort", "eligible events", "total events", "eligible %"],
       tablefmt="simple"))

    # ────────────────────────────────────────────────────────────────
    # 2. Sessions — pages per session, sessions per user
    # ────────────────────────────────────────────────────────────────
    print("\n" + "=" * 72)
    print(" 2. Session metrics — non-activators vs activators")
    print("=" * 72)

    def _sessionize(df):
        if len(df) == 0:
            return pd.DataFrame()
        df = df.sort_values(["guest_id", "created_ts"]).copy()
        grp = df.groupby("guest_id")["created_ts"]
        gap = grp.diff().dt.total_seconds().fillna(SESSION_GAP_S + 1)
        new_sess = (gap > SESSION_GAP_S).astype(int)
        df["session_idx"] = grp.transform(lambda _s: 0)  # placeholder
        df["session_idx"] = new_sess.groupby(df["guest_id"]).cumsum()
        return df

    print("\n[sessionising non-activators...]")
    non_sess = _sessionize(non_ev)
    print("[sessionising activators...]")
    act_sess = _sessionize(act_ev)

    def _session_stats(df):
        if len(df) == 0:
            return {}
        sess = df.groupby(["guest_id", "session_idx"]).agg(
            events=("created_ts", "count"),
            duration_s=("created_ts",
                        lambda s: (s.max() - s.min()).total_seconds()),
            eligible_hits=("eligible", "sum"),
            product_hits=("subtype",
                          lambda s: int(s.notna().sum())),
            distinct_pagekinds=("pagekind", "nunique"),
        ).reset_index()
        per_user = sess.groupby("guest_id").agg(
            sessions=("session_idx", "count"),
            total_events=("events", "sum"),
            total_eligible=("eligible_hits", "sum"),
            total_product=("product_hits", "sum"),
        ).reset_index()
        return {"sess": sess, "per_user": per_user}

    non_s = _session_stats(non_sess)
    act_s = _session_stats(act_sess)

    def _describe(series, label):
        if len(series) == 0:
            return [label, "—", "—", "—", "—", "—", "—"]
        return [label,
                f"{series.mean():.2f}",
                f"{series.median():.2f}",
                f"{series.quantile(0.75):.2f}",
                f"{series.quantile(0.9):.2f}",
                f"{series.max():.0f}",
                f"{len(series):,}"]

    print("\n── Sessions / user ──")
    print(tabulate([
        _describe(non_s["per_user"]["sessions"], "non-activator"),
        _describe(act_s["per_user"]["sessions"], "activator"),
    ], headers=["cohort", "mean", "median", "p75", "p90", "max", "users"],
       tablefmt="simple"))

    print("\n── Events / session ──")
    print(tabulate([
        _describe(non_s["sess"]["events"], "non-activator"),
        _describe(act_s["sess"]["events"], "activator"),
    ], headers=["cohort", "mean", "median", "p75", "p90", "max", "sessions"],
       tablefmt="simple"))

    print("\n── Session duration (seconds) ──")
    print(tabulate([
        _describe(non_s["sess"]["duration_s"], "non-activator"),
        _describe(act_s["sess"]["duration_s"], "activator"),
    ], headers=["cohort", "mean", "median", "p75", "p90", "max", "sessions"],
       tablefmt="simple"))

    print("\n── Eligible hits / session ──")
    print(tabulate([
        _describe(non_s["sess"]["eligible_hits"], "non-activator"),
        _describe(act_s["sess"]["eligible_hits"], "activator"),
    ], headers=["cohort", "mean", "median", "p75", "p90", "max", "sessions"],
       tablefmt="simple"))

    # Bounce rate (single-event session) + zero-eligible session share
    def _ratios(sess):
        n = len(sess)
        if n == 0:
            return {"bounce": "—", "zero_elig": "—"}
        return {
            "bounce": f"{100*int((sess['events'] == 1).sum())/n:.1f}%",
            "zero_elig": f"{100*int((sess['eligible_hits'] == 0).sum())/n:.1f}%",
        }
    print("\n── Session quality ──")
    print(tabulate([
        ["non-activator", _ratios(non_s["sess"])["bounce"],
         _ratios(non_s["sess"])["zero_elig"]],
        ["activator", _ratios(act_s["sess"])["bounce"],
         _ratios(act_s["sess"])["zero_elig"]],
    ], headers=["cohort", "single-event sessions",
                "sessions with 0 eligible"], tablefmt="simple"))

    # ────────────────────────────────────────────────────────────────
    # 3. Eligible pages per user
    # ────────────────────────────────────────────────────────────────
    print("\n" + "=" * 72)
    print(" 3. Eligible pages per user (window total)")
    print("=" * 72)

    def _user_elig(df, users):
        e = df.groupby("guest_id")["eligible"].sum()
        e = e.reindex(list(users), fill_value=0)
        return e

    non_ids = set(non_ev["guest_id"].unique())
    act_ids = set(act_ev["guest_id"].unique())
    non_user_e = _user_elig(ev, non_ids)
    act_user_e = _user_elig(ev, act_ids)

    print("\n── Eligible pages / user ──")
    print(tabulate([
        _describe(non_user_e, "non-activator"),
        _describe(act_user_e, "activator"),
    ], headers=["cohort", "mean", "median", "p75", "p90", "max", "users"],
       tablefmt="simple"))

    bucket_edges = [0, 1, 2, 5, 10, 50, 1000000]
    bucket_labels = ["0", "1", "2-4", "5-9", "10-49", "50+"]
    non_b = pd.cut(non_user_e, bucket_edges, right=False, labels=bucket_labels) \
            .value_counts().sort_index()
    act_b = pd.cut(act_user_e, bucket_edges, right=False, labels=bucket_labels) \
            .value_counts().sort_index()
    rows = []
    for lbl in bucket_labels:
        n_v = int(non_b.get(lbl, 0))
        a_v = int(act_b.get(lbl, 0))
        rows.append([lbl, f"{n_v:,}",
                     f"{100*n_v/len(non_user_e):.1f}%" if len(non_user_e) else "—",
                     f"{a_v:,}",
                     f"{100*a_v/len(act_user_e):.1f}%" if len(act_user_e) else "—"])
    print("\n── Eligible pages buckets ──")
    print(tabulate(rows, headers=["bucket", "non-act users", "non-act %",
                                   "act users", "act %"], tablefmt="simple"))

    # ────────────────────────────────────────────────────────────────
    # 4. Build / version breakdown with non-activator rate
    # ────────────────────────────────────────────────────────────────
    print("\n" + "=" * 72)
    print(" 4. Non-activator rate by build_app / lineage / version")
    print("=" * 72)

    # Build per-user frame (one row per user; take first non-null value per dim)
    user_df = ev.groupby("guest_id").agg(
        build_app=("build_app", "first"),
        browser_fam=("browser_fam", "first"),
        lineage=("lineage", "first"),
        client_version=("client_version", "first"),
        country=("country", "first"),
        total_events=("url", "count"),
        eligible_events=("eligible", "sum"),
    ).reset_index()
    user_df["is_activator"] = user_df["guest_id"].isin(ac_users)

    def _rate_by(dim):
        g = user_df.groupby(dim, dropna=False).agg(
            users=("guest_id", "count"),
            activators=("is_activator", "sum"),
        )
        g["non_activators"] = g["users"] - g["activators"]
        g["non_rate"] = 100 * g["non_activators"] / g["users"]
        g["pct_of_non"] = 100 * g["non_activators"] / g["non_activators"].sum()
        g = g.sort_values("users", ascending=False).head(25).reset_index()
        return g

    for dim in ("build_app", "lineage", "client_version", "browser_fam"):
        print(f"\n── By {dim} ──")
        df = _rate_by(dim)
        rows = [[r[dim], f"{int(r['users']):,}",
                 f"{int(r['activators']):,}",
                 f"{int(r['non_activators']):,}",
                 f"{r['non_rate']:.1f}%",
                 f"{r['pct_of_non']:.1f}%"]
                for _, r in df.iterrows()]
        print(tabulate(rows, headers=[dim, "users", "activators",
                                      "non-activators", "non-rate",
                                      "% of non-act"],
                       tablefmt="simple"))

    # Cross: build_app × lineage × non_rate
    print("\n── build_app × lineage ──")
    cross = user_df.groupby(["build_app", "lineage"], dropna=False).agg(
        users=("guest_id", "count"),
        activators=("is_activator", "sum"),
    ).reset_index()
    cross["non"] = cross["users"] - cross["activators"]
    cross["non_rate"] = 100 * cross["non"] / cross["users"]
    cross = cross.sort_values("users", ascending=False).head(20)
    rows = [[r["build_app"], r["lineage"], f"{int(r['users']):,}",
             f"{int(r['activators']):,}", f"{int(r['non']):,}",
             f"{r['non_rate']:.1f}%"]
            for _, r in cross.iterrows()]
    print(tabulate(rows, headers=["build_app", "lineage", "users",
                                  "activators", "non", "non-rate"],
                   tablefmt="simple"))

    # ────────────────────────────────────────────────────────────────
    # Save full dump
    # ────────────────────────────────────────────────────────────────
    out = CACHE_DIR / f"non_activator_drill__{SUFFIX}.pkl"
    with open(out, "wb") as f:
        pickle.dump({
            "host_dist_non": non_h,
            "host_dist_act": act_h,
            "pagekind_non": pk_non,
            "pagekind_act": pk_act,
            "user_df": user_df,
        }, f)
    print(f"\nDump saved: {out}")


if __name__ == "__main__":
    main()
