#!/usr/bin/env python3
"""
Bouncer drill — non-activators who dropped in and left.

Cohort definition:
  - Non-activator (no Affiliate Click in Mixpanel during window)
  - Total events in window ≤ 2  (equivalent to 1 session with 1-2 pageviews)

Per-user profile: landing page kind, country, client_version, build_app, lineage.
Aggregated stats on what URL they bounced on.
"""

import json
import os
import pickle
from collections import Counter
from urllib.parse import urlparse

import pandas as pd
from tabulate import tabulate

from src.config import CACHE_DIR, CACHE_SUFFIX
from src.utils import (
    browser_family, lineage_segment, is_aliexpress_ru,
    matches_check_list_urls, is_eligible_product_page,
    product_page_subtype,
)

SUFFIX = os.getenv("CACHE_SUFFIX_OVERRIDE", CACHE_SUFFIX)


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


def _host(url):
    if not url:
        return "<empty>"
    try:
        return (urlparse(url).hostname or "<empty>").lower()
    except Exception:
        return "<parse_error>"


def _host_bucket(host):
    if not host or host == "<empty>":
        return "<empty>"
    if "aliexpress.ru" in host:
        return "aliexpress.ru"
    if "aliexpress." in host or "tmall." in host:
        return "aliexpress.global"
    return "other"


def _pagekind(url, host_bucket, subtype):
    """If subtype is set, it's a product page. Otherwise classify non-product intent."""
    if subtype:
        return subtype
    if host_bucket == "other" or host_bucket == "<empty>":
        return "non_ali"
    try:
        path = (urlparse(url).path or "").lower()
    except Exception:
        return "other_ali"
    if path in ("", "/", "/index.html"):
        return "home"
    if "/wholesale" in path or "/category" in path or "/categories" in path:
        return "category"
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
    if "/gcp/" in path:
        return "gcp_page"
    return "other_ali"


def _path_pattern(url):
    """Collapse numeric ids to get a path-family pattern."""
    try:
        p = urlparse(url)
        path = p.path or ""
    except Exception:
        return "<parse_error>"
    # collapse numeric ids and long alphanumerics
    import re
    pat = re.sub(r"\d{3,}", "<N>", path)
    pat = re.sub(r"/[A-Za-z0-9_\-]{25,}", "/<LONG>", pat)
    return pat[:80]


def main():
    print(f"\n{'=' * 72}")
    print(f"  Bouncer drill — suffix {SUFFIX}")
    print(f"{'=' * 72}\n")

    ev = _load_pkl("events_a")
    clients = _load_pkl("clients")
    ac_raw = _load_json("aff_click_a")

    ac_users = {str(r.get("properties", {}).get("$user_id", ""))
                for r in ac_raw} - {""}

    # Enrich
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
    ev["pagekind"] = ev.apply(
        lambda r: _pagekind(r["url"], r["host_bucket"], r["subtype"]), axis=1
    )
    ev["eligible"] = ev.apply(
        lambda r: (matches_check_list_urls(r["url"])
                   if r["lineage"] == "auto_redirect"
                   else (is_eligible_product_page(r["product_id"])
                         if r["lineage"] == "dogi" else False)),
        axis=1,
    )

    # Per-user event count
    per_user_ev = ev.groupby("guest_id").size().rename("total_events")
    user_df = per_user_ev.to_frame().reset_index()
    user_df["is_activator"] = user_df["guest_id"].isin(ac_users)

    # Bouncer cohort = non-activator with total_events <= 2
    bouncer_ids = set(user_df.query("not is_activator and total_events <= 2")["guest_id"])
    non_act_ids = set(user_df.query("not is_activator")["guest_id"])
    act_ids = set(user_df.query("is_activator")["guest_id"])

    total_users = len(user_df)
    print(f"Total users:            {total_users:,}")
    print(f"Activators:             {len(act_ids):,} "
          f"({100*len(act_ids)/total_users:.1f}%)")
    print(f"Non-activators:         {len(non_act_ids):,} "
          f"({100*len(non_act_ids)/total_users:.1f}%)")
    print(f"Bouncers (≤2 events):   {len(bouncer_ids):,} "
          f"({100*len(bouncer_ids)/total_users:.1f}% of all, "
          f"{100*len(bouncer_ids)/len(non_act_ids):.1f}% of non-act)")

    # Also for reference: strict single-event bouncers
    strict_bouncers = set(user_df.query("not is_activator and total_events == 1")["guest_id"])
    print(f"Strict bouncers (=1 event): {len(strict_bouncers):,} "
          f"({100*len(strict_bouncers)/len(non_act_ids):.1f}% of non-act)")

    # All events belonging to bouncers
    b_ev = ev[ev["guest_id"].isin(bouncer_ids)].copy()
    print(f"\nBouncer events (denominator for page tables): {len(b_ev):,}")

    # ── Page landings ──────────────────────────────────────────────
    print("\n" + "=" * 72)
    print(" Pages bouncers landed on")
    print("=" * 72)

    # Host bucket
    print("\n── Host bucket ──")
    hb = b_ev["host_bucket"].value_counts()
    rows = [[k, f"{int(v):,}", f"{100*v/len(b_ev):.1f}%"] for k, v in hb.items()]
    print(tabulate(rows, headers=["host", "events", "%"], tablefmt="simple"))

    # Full host
    print("\n── Full host (top 15) ──")
    fh = b_ev["host"].value_counts().head(15)
    rows = [[k, f"{int(v):,}", f"{100*v/len(b_ev):.1f}%"] for k, v in fh.items()]
    print(tabulate(rows, headers=["host", "events", "%"], tablefmt="simple"))

    # Pagekind
    print("\n── Page kind (landing intent) ──")
    pk = b_ev["pagekind"].value_counts()
    rows = [[k, f"{int(v):,}", f"{100*v/len(b_ev):.1f}%"] for k, v in pk.items()]
    print(tabulate(rows, headers=["pagekind", "events", "%"], tablefmt="simple"))

    # Eligibility of landing
    elig = int(b_ev["eligible"].sum())
    print(f"\nEligible landings: {elig:,} ({100*elig/len(b_ev):.1f}%)")
    print(f"Ineligible landings: {len(b_ev)-elig:,} "
          f"({100*(len(b_ev)-elig)/len(b_ev):.1f}%)")

    # Top path patterns
    b_ev["path_pat"] = b_ev["url"].apply(_path_pattern)
    print("\n── Top URL path patterns (top 20) ──")
    pp = b_ev["path_pat"].value_counts().head(20)
    rows = [[k, f"{int(v):,}", f"{100*v/len(b_ev):.1f}%"] for k, v in pp.items()]
    print(tabulate(rows, headers=["path pattern", "events", "%"],
                   tablefmt="simple"))

    # Pagekind split for eligible vs ineligible landings
    print("\n── Pagekind × eligibility ──")
    tx = b_ev.groupby(["pagekind", "eligible"]).size().unstack(fill_value=0)
    if True in tx.columns and False in tx.columns:
        tx["total"] = tx[True] + tx[False]
        tx = tx.sort_values("total", ascending=False).head(15)
        rows = [[idx, int(r.get(True, 0)), int(r.get(False, 0)), int(r["total"]),
                 f"{100*r.get(True, 0)/r['total']:.0f}%" if r["total"] else "—"]
                for idx, r in tx.iterrows()]
        print(tabulate(rows, headers=["pagekind", "eligible", "ineligible",
                                      "total", "eligible %"],
                       tablefmt="simple"))

    # ── Per-user profile ───────────────────────────────────────────
    print("\n" + "=" * 72)
    print(" Bouncer profile (per-user)")
    print("=" * 72)

    bu = user_df[user_df["guest_id"].isin(bouncer_ids)].merge(
        ev.groupby("guest_id").agg(
            country=("country", "first"),
            client_version=("client_version", "first"),
            build_app=("build_app", "first"),
            lineage=("lineage", "first"),
            browser_fam=("browser_fam", "first"),
        ).reset_index(),
        on="guest_id", how="left"
    )

    def _dist(df, col, top=25):
        s = df[col].fillna("<missing>").astype(str).value_counts().head(top)
        total = len(df)
        return [[k, f"{int(v):,}", f"{100*v/total:.1f}%"] for k, v in s.items()]

    # Country
    print("\n── Country (top 25) ──")
    print(tabulate(_dist(bu, "country"),
                   headers=["country", "users", "%"], tablefmt="simple"))

    # Client version
    print("\n── client_version ──")
    print(tabulate(_dist(bu, "client_version"),
                   headers=["version", "users", "%"], tablefmt="simple"))

    # Build_app
    print("\n── build_app ──")
    print(tabulate(_dist(bu, "build_app"),
                   headers=["build_app", "users", "%"], tablefmt="simple"))

    # Lineage
    print("\n── Lineage ──")
    print(tabulate(_dist(bu, "lineage"),
                   headers=["lineage", "users", "%"], tablefmt="simple"))

    # Browser family
    print("\n── Browser family ──")
    print(tabulate(_dist(bu, "browser_fam"),
                   headers=["browser", "users", "%"], tablefmt="simple"))

    # Cross: country × lineage (top 20 cells)
    print("\n── Country × lineage (top 20 cells) ──")
    c = bu.groupby(["country", "lineage"]).size().rename("users") \
          .reset_index().sort_values("users", ascending=False).head(20)
    rows = [[r["country"], r["lineage"], f"{int(r['users']):,}",
             f"{100*r['users']/len(bu):.1f}%"] for _, r in c.iterrows()]
    print(tabulate(rows, headers=["country", "lineage", "users", "%"],
                   tablefmt="simple"))

    # ── Comparison: bouncer vs non-bouncer non-activator ──────────
    print("\n" + "=" * 72)
    print(" Bouncer vs other non-activator (context)")
    print("=" * 72)

    other_non_ids = non_act_ids - bouncer_ids
    other_nu = ev[ev["guest_id"].isin(other_non_ids)].groupby("guest_id").agg(
        country=("country", "first"),
        client_version=("client_version", "first"),
        lineage=("lineage", "first"),
    ).reset_index()
    # Restrict activators similarly for context
    act_nu = ev[ev["guest_id"].isin(act_ids)].groupby("guest_id").agg(
        country=("country", "first"),
        client_version=("client_version", "first"),
        lineage=("lineage", "first"),
    ).reset_index()

    def _top_counts(df, col, top=10):
        s = df[col].fillna("<missing>").astype(str).value_counts().head(top)
        tot = len(df)
        return {k: (int(v), round(100*v/tot, 1)) for k, v in s.items()}

    for col in ("country", "client_version", "lineage"):
        print(f"\n── Side-by-side top {col} ──")
        b_top = _top_counts(bu, col)
        o_top = _top_counts(other_nu, col)
        a_top = _top_counts(act_nu, col)
        keys = list(b_top)[:12]
        rows = []
        for k in keys:
            bc, bp = b_top.get(k, (0, 0))
            oc, op = o_top.get(k, (0, 0))
            ac, ap = a_top.get(k, (0, 0))
            rows.append([k, f"{bc:,} ({bp}%)",
                         f"{oc:,} ({op}%)", f"{ac:,} ({ap}%)"])
        print(tabulate(rows,
                       headers=[col, "bouncer", "other non-act", "activator"],
                       tablefmt="simple"))

    out = CACHE_DIR / f"bouncer_drill__{SUFFIX}.pkl"
    with open(out, "wb") as f:
        pickle.dump({
            "bouncer_ids": list(bouncer_ids),
            "strict_bouncer_ids": list(strict_bouncers),
            "bouncer_profile": bu,
        }, f)
    print(f"\nDump saved: {out}")


if __name__ == "__main__":
    main()
