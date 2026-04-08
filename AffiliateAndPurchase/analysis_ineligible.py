"""
AliHelper — Ineligible Page Taxonomy Analysis
Focus: Raw Activity -> Eligible gap in Problem A

CACHE REUSE:
  REUSED : cache/mongo_problem_a.pkl  — user-level aggregations
  REUSED : cache/aff_click_a.json     — Affiliate Click (to tag who reached hub)
  NEW    : cache/ineligible_url_taxonomy.pkl  — URL counts from non-eligible events
  NEW    : cache/first_events_a.pkl           — first event per user

Run: python3 -u analysis_ineligible.py 2>&1 | tee /tmp/ineligible_output.txt
"""

import json, pickle, time, csv
from pathlib import Path
from datetime import datetime, timezone, timedelta
from collections import defaultdict, Counter

import pandas as pd
import numpy as np
from bson import ObjectId
from tabulate import tabulate

from src.config import CACHE_DIR, A_START, A_END, OUR_SKS, CIS_COUNTRIES
from src.db import mongo_tunnel
from src.utils import (
    oid_from_dt, pct, print_section,
    normalize_url, is_homepage, is_product_page,
)


# ─────────────────────────────────────────────────────────────────────────────
# MONGODB QUERIES
# ─────────────────────────────────────────────────────────────────────────────

def run_ineligible_url_query(db) -> dict:
    cache_file = CACHE_DIR / "ineligible_url_taxonomy.pkl"
    if cache_file.exists():
        print("  [cache] Loading ineligible URL taxonomy from cache")
        with open(cache_file, "rb") as f:
            return pickle.load(f)

    print("  [mongo] Querying non-eligible events (no productId, not homepage)...")
    events = db["events"]
    oid_start = oid_from_dt(A_START)
    oid_end   = oid_from_dt(A_END)
    t0 = time.time()

    pipeline_urls = [
        {"$match": {
            "_id": {"$gte": oid_start, "$lte": oid_end},
            "payload.productId": None,
            "payload.url": {
                "$not": {"$regex": r"^https?://[^/]*aliexpress\.[^/]*(/(#.*)?)?$", "$options": "i"},
                "$exists": True,
                "$ne": None,
                "$ne": "",
            }
        }},
        {"$group": {
            "_id": "$payload.url",
            "count": {"$sum": 1},
            "guest_ids": {"$addToSet": "$guest_id"},
        }},
        {"$project": {
            "url": "$_id", "count": 1,
            "unique_users": {"$size": "$guest_ids"},
        }},
        {"$sort": {"count": -1}},
        {"$limit": 3000},
    ]

    print("  [mongo]   Pipeline 1: top URLs by hit count...")
    url_counts = list(events.aggregate(pipeline_urls, allowDiskUse=True))
    print(f"    -> {len(url_counts):,} distinct URLs in {time.time()-t0:.1f}s")

    print("  [mongo]   Pipeline 2: per-user non-eligible event count + URL sample...")
    t1 = time.time()
    pipeline_user_noelig = [
        {"$match": {
            "_id": {"$gte": oid_start, "$lte": oid_end},
            "payload.productId": None,
            "payload.url": {
                "$not": {"$regex": r"^https?://[^/]*aliexpress\.[^/]*(/(#.*)?)?$", "$options": "i"},
                "$exists": True, "$ne": None,
            }
        }},
        {"$group": {
            "_id": "$guest_id",
            "noelig_count": {"$sum": 1},
            "urls": {"$push": "$payload.url"},
        }},
        {"$project": {
            "noelig_count": 1,
            "urls": {"$slice": ["$urls", 10]},
        }},
    ]
    user_noelig = list(events.aggregate(pipeline_user_noelig, allowDiskUse=True))
    print(f"    -> {len(user_noelig):,} users with non-eligible events in {time.time()-t1:.1f}s")

    result = {"url_counts": url_counts, "user_noelig": user_noelig}
    with open(cache_file, "wb") as f:
        pickle.dump(result, f)
    return result


def run_first_events_query(db) -> list:
    cache_file = CACHE_DIR / "first_events_a.pkl"
    if cache_file.exists():
        print("  [cache] Loading first_events_a from cache")
        with open(cache_file, "rb") as f:
            return pickle.load(f)

    print("  [mongo] Querying first event per user...")
    events = db["events"]
    oid_start = oid_from_dt(A_START)
    oid_end   = oid_from_dt(A_END)
    t0 = time.time()

    pipeline = [
        {"$match": {"_id": {"$gte": oid_start, "$lte": oid_end}}},
        {"$sort": {"_id": 1}},
        {"$group": {
            "_id": "$guest_id",
            "first_oid": {"$first": "$_id"},
            "first_url": {"$first": "$payload.url"},
            "first_product_id": {"$first": "$payload.productId"},
            "country": {"$first": "$country"},
        }},
    ]
    first_events = list(events.aggregate(pipeline, allowDiskUse=True))
    print(f"    -> {len(first_events):,} users' first events in {time.time()-t0:.1f}s")

    with open(cache_file, "wb") as f:
        pickle.dump(first_events, f)
    return first_events


# ─────────────────────────────────────────────────────────────────────────────
# ANALYSIS
# ─────────────────────────────────────────────────────────────────────────────

def load_existing_cache():
    print_section("Loading existing caches")

    with open(CACHE_DIR / "mongo_problem_a.pkl", "rb") as f:
        mongo_a = pickle.load(f)

    homepage_data = mongo_a["homepage_data"]
    client_map    = mongo_a["client_map"]
    user_rows     = mongo_a["user_events"]

    user_df = pd.DataFrame([
        {
            "user_id":         str(r["_id"]),
            "total_events":    r.get("total_events", 0),
            "product_events":  r.get("product_events", 0),
            "country":         r.get("country", ""),
            "our_sk_events":   r.get("our_sk_events", 0),
            "foreign_sk_events": r.get("foreign_sk_events", 0),
        }
        for r in user_rows
    ])
    user_df["homepage_events"] = user_df["user_id"].map(homepage_data).fillna(0).astype(int)
    user_df["is_eligible"] = (user_df["product_events"] > 0) | (user_df["homepage_events"] > 0)
    user_df["is_cis"] = user_df["country"].apply(lambda c: str(c).upper() in CIS_COUNTRIES)
    user_df["browser"] = user_df["user_id"].map(
        lambda uid: client_map.get(uid, {}).get("browser", "") or "")

    print(f"  Total users: {len(user_df):,}")
    print(f"  Eligible:    {user_df['is_eligible'].sum():,}")

    aff_click_file = CACHE_DIR / "aff_click_a.json"
    print(f"  Loading Affiliate Click cache...")
    with open(aff_click_file) as f:
        ac_raw = json.load(f)
    ac_users = set()
    for r in ac_raw:
        uid = r.get("properties", {}).get("$user_id") or r.get("properties", {}).get("distinct_id")
        if uid:
            ac_users.add(str(uid))
    user_df["reached_hub"] = user_df["user_id"].isin(ac_users)
    print(f"  Reached hub: {user_df['reached_hub'].sum():,}")

    return user_df, mongo_a


def build_url_taxonomy(inelig_data: dict, user_df: pd.DataFrame):
    print_section("Building URL taxonomy")

    url_counts = inelig_data["url_counts"]
    user_noelig = inelig_data["user_noelig"]

    total_raw_users = len(user_df)
    inelig_users_set = set(user_df.loc[~user_df["is_eligible"], "user_id"])
    total_inelig_users = len(inelig_users_set)

    cat_hits = defaultdict(int)
    cat_urls = defaultdict(list)
    cat_pattern_counts = defaultdict(Counter)

    for row in url_counts:
        url = row.get("url") or row.get("_id") or ""
        count = row.get("count", 0)
        cat, path_norm = normalize_url(url)
        cat_hits[cat] += count
        cat_urls[cat].append((url, count))
        cat_pattern_counts[cat][path_norm] += count

    user_cats = defaultdict(set)
    cat_user_counts = defaultdict(set)
    for row in user_noelig:
        uid = str(row["_id"])
        urls = row.get("urls") or []
        for url in urls:
            cat, _ = normalize_url(url)
            user_cats[uid].add(cat)
            cat_user_counts[cat].add(uid)

    rows = []
    for cat in sorted(cat_hits.keys(), key=lambda c: -cat_hits[c]):
        hit_count = cat_hits[cat]
        u_count = len(cat_user_counts[cat])
        top_urls = sorted(cat_urls[cat], key=lambda x: -x[1])[:5]
        top_patterns = cat_pattern_counts[cat].most_common(5)

        rows.append({
            "category": cat,
            "total_hits": hit_count,
            "unique_users_sample": u_count,
            "pct_raw_users": 100 * u_count / total_raw_users if total_raw_users else 0,
            "pct_inelig_users": 100 * u_count / total_inelig_users if total_inelig_users else 0,
            "top_patterns": top_patterns,
            "examples": [u for u, _ in top_urls],
        })

    return pd.DataFrame(rows), user_cats


def first_entry_analysis(first_events: list, user_df: pd.DataFrame, user_cats: dict):
    print_section("First entry point analysis")

    user_meta = user_df.set_index("user_id")
    total_users = len(user_df)

    rows = []
    for r in first_events:
        uid = str(r["_id"])
        url = r.get("first_url") or ""
        product_id = r.get("first_product_id")

        if is_product_page(product_id):
            first_cat = "product_page"
        elif is_homepage(url):
            first_cat = "homepage"
        else:
            first_cat, _ = normalize_url(url)

        meta = user_meta.loc[uid] if uid in user_meta.index else None
        is_elig = bool(meta["is_eligible"]) if meta is not None else False
        reached = bool(meta["reached_hub"]) if meta is not None else False

        rows.append({
            "user_id": uid, "first_cat": first_cat,
            "first_url": url[:120],
            "is_eligible": is_elig, "reached_hub": reached,
            "country": r.get("country", ""),
        })

    fe_df = pd.DataFrame(rows)
    print(f"  First events: {len(fe_df):,} users")

    c1 = fe_df[fe_df["first_cat"].isin(["product_page", "homepage"])]
    c2 = fe_df[(~fe_df["first_cat"].isin(["product_page", "homepage"])) & fe_df["is_eligible"]]
    c3 = fe_df[(~fe_df["first_cat"].isin(["product_page", "homepage"])) & ~fe_df["is_eligible"]]

    print(f"\n  Cohort 1 — first page eligible: {len(c1):,} ({pct(len(c1), total_users)})")
    print(f"  Cohort 2 — first non-eligible, later eligible: {len(c2):,} ({pct(len(c2), total_users)})")
    print(f"  Cohort 3 — first non-eligible, never eligible: {len(c3):,} ({pct(len(c3), total_users)})")

    entry_stats = []
    for cat, grp in fe_df.groupby("first_cat"):
        n_users = len(grp)
        n_later_elig = grp["is_eligible"].sum()
        entry_stats.append({
            "first_page_type": cat, "unique_users": n_users,
            "pct_all_users": 100 * n_users / total_users,
            "later_reached_eligible": int(n_later_elig),
            "never_reached_eligible": int(n_users - n_later_elig),
            "pct_later_eligible": 100 * n_later_elig / n_users if n_users else 0,
            "reached_hub": int(grp["reached_hub"].sum()),
        })

    entry_df = pd.DataFrame(entry_stats).sort_values("unique_users", ascending=False)
    return fe_df, entry_df, c1, c2, c3


def top_missed_entry_before_eligible(fe_df: pd.DataFrame, c_noelig_then_elig: pd.DataFrame):
    grp = c_noelig_then_elig.groupby("first_cat").size().reset_index(name="users")
    grp = grp.sort_values("users", ascending=False)

    PRIORITY = {
        "search_results":    ("worth_testing",    "Users actively shopping"),
        "category_listing":  ("worth_testing",    "Browsing categories — high purchase intent"),
        "promo_landing":     ("worth_testing",    "Visiting deals/promos"),
        "seller_store":      ("worth_testing",    "Browsing store pages"),
        "brand_collection":  ("worth_testing",    "Browsing brand pages"),
        "feed_recommendations": ("low_priority",  "Passive browsing"),
        "cart":              ("high_priority",    "Cart page = strongest purchase signal"),
        "order_checkout":    ("not_worth",        "Post-purchase or checkout flow"),
        "account_profile":   ("not_worth",        "Account management"),
        "help_service":      ("not_worth",        "Help/support pages"),
        "review_rating":     ("low_priority",     "Reading reviews"),
        "other":             ("low_priority",     "Unknown/misc pages"),
        "homepage_variant":  ("high_priority",    "Homepage variant — should be covered"),
        "unknown":           ("low_priority",     "Unclassified URLs"),
    }

    rows = []
    for _, row in grp.iterrows():
        cat = row["first_cat"]
        priority, reasoning = PRIORITY.get(cat, ("low_priority", ""))
        rows.append({
            "page_group": cat,
            "users_transitioning_to_eligible": int(row["users"]),
            "activation_priority": priority, "reasoning": reasoning,
        })
    return pd.DataFrame(rows)


# ─────────────────────────────────────────────────────────────────────────────
# REPORT
# ─────────────────────────────────────────────────────────────────────────────

def print_full_report(user_df, taxonomy_df, fe_df, entry_df, c1, c2, c3, missed_df):
    total_users = len(user_df)
    inelig_count = (~user_df["is_eligible"]).sum()
    elig_count = user_df["is_eligible"].sum()

    print_section("INELIGIBLE PAGE TAXONOMY — FULL REPORT")

    print(f"\n  Total raw users:   {total_users:,}")
    print(f"  Eligible users:    {elig_count:,} ({pct(elig_count, total_users)})")
    print(f"  Ineligible users:  {inelig_count:,} ({pct(inelig_count, total_users)})")

    print("\n## Taxonomy of Non-Eligible Pages\n")
    tax_rows = []
    for _, r in taxonomy_df.iterrows():
        top_pats = "; ".join(f"{pat}" for pat, cnt in r["top_patterns"][:3])
        tax_rows.append([
            r["category"], f"{r['total_hits']:,}", f"{r['unique_users_sample']:,}",
            f"{r['pct_raw_users']:.1f}%", f"{r['pct_inelig_users']:.1f}%", top_pats[:80],
        ])
    print(tabulate(tax_rows,
        headers=["Category","Hits","Users(sample)","% raw","% inelig","Top patterns"],
        tablefmt="github"))

    print(f"\n## First Entry Point Analysis")
    print(f"\n  Cohort 1 — first page eligible: {len(c1):,}")
    print(f"  Cohort 2 — first non-eligible, later eligible: {len(c2):,}")
    print(f"  Cohort 3 — first non-eligible, never eligible: {len(c3):,}")

    print("\n### First Entry by Page Type\n")
    entry_rows = []
    for _, r in entry_df.head(15).iterrows():
        entry_rows.append([
            r["first_page_type"], f"{r['unique_users']:,}", f"{r['pct_all_users']:.1f}%",
            f"{r['later_reached_eligible']:,}", f"{r['never_reached_eligible']:,}",
            f"{r['pct_later_eligible']:.1f}%", f"{r['reached_hub']:,}",
        ])
    print(tabulate(entry_rows,
        headers=["First page type","Users","% all","Later elig","Never elig","% later elig","Reached hub"],
        tablefmt="github"))

    print("\n### Top Missed Entry Points\n")
    miss_rows = []
    for _, r in missed_df.iterrows():
        miss_rows.append([r["page_group"], f"{r['users_transitioning_to_eligible']:,}",
                          r["activation_priority"], r["reasoning"][:80]])
    print(tabulate(miss_rows,
        headers=["Page group","Users->elig","Priority","Reasoning"], tablefmt="github"))


def write_csv(taxonomy_df, missed_df, entry_df):
    csv_path = Path("./data/ineligible_taxonomy.csv")
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["category","total_hits","unique_users_sample",
                         "pct_raw_users","pct_inelig_users",
                         "top_pattern_1","top_pattern_2","top_pattern_3",
                         "example_url_1","example_url_2","example_url_3"])
        for _, r in taxonomy_df.iterrows():
            pats = [p for p, _ in r["top_patterns"][:3]]
            while len(pats) < 3: pats.append("")
            exs = r["examples"][:3]
            while len(exs) < 3: exs.append("")
            writer.writerow([
                r["category"], r["total_hits"], r["unique_users_sample"],
                f"{r['pct_raw_users']:.2f}", f"{r['pct_inelig_users']:.2f}",
                pats[0], pats[1], pats[2], exs[0], exs[1], exs[2],
            ])
    print(f"\n  [csv] Saved {csv_path}")

    csv2_path = Path("./data/first_entry_taxonomy.csv")
    entry_df.to_csv(csv2_path, index=False)
    print(f"  [csv] Saved {csv2_path}")


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

def main():
    user_df, mongo_a = load_existing_cache()

    print_section("MongoDB: new targeted queries")
    with mongo_tunnel() as db:
        inelig_data  = run_ineligible_url_query(db)
        first_events = run_first_events_query(db)

    print_section("Analysis")
    taxonomy_df, user_cats = build_url_taxonomy(inelig_data, user_df)
    fe_df, entry_df, c1, c2, c3 = first_entry_analysis(first_events, user_df, user_cats)
    missed_df = top_missed_entry_before_eligible(fe_df, c2)

    print_full_report(user_df, taxonomy_df, fe_df, entry_df, c1, c2, c3, missed_df)
    write_csv(taxonomy_df, missed_df, entry_df)

    print_section("Done")


if __name__ == "__main__":
    main()
