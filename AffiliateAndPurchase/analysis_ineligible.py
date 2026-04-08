"""
AliHelper — Ineligible Page Taxonomy Analysis
Focus: Raw Activity → Eligible gap in Problem A

CACHE REUSE:
  REUSED : cache/mongo_problem_a.pkl  — user-level aggregations (63,916 users)
  REUSED : cache/aff_click_a.json     — Affiliate Click (to tag who reached hub)
  NEW    : cache/ineligible_url_taxonomy.pkl  — URL counts from non-eligible events
  NEW    : cache/first_events_a.pkl           — first event per user (URL + eligibility)

Run: python3 -u analysis_ineligible.py 2>&1 | tee /tmp/ineligible_output.txt
"""

import os, json, pickle, re, time, csv
from pathlib import Path
from datetime import datetime, timezone, timedelta
from collections import defaultdict, Counter
from urllib.parse import urlparse, parse_qs

import pandas as pd
import numpy as np
from dotenv import load_dotenv
import sshtunnel
import pymongo
from bson import ObjectId
from tabulate import tabulate

load_dotenv()

CACHE_DIR = Path("./cache")
CACHE_DIR.mkdir(exist_ok=True)

# MongoDB
SSH_HOST   = os.getenv("MONGO_SSH_HOST")
SSH_USER   = os.getenv("MONGO_SSH_USER")
DB_HOST    = os.getenv("MONGO_DB_HOST")
DB_PORT    = int(os.getenv("MONGO_DB_PORT", 27017))
LOCAL_PORT = int(os.getenv("MONGO_LOCAL_PORT", 27018))
DB_NAME    = os.getenv("MONGO_DB_NAME")
MONGO_USER = os.getenv("MONGO_USER")
MONGO_PASS = os.getenv("MONGO_PASSWORD")
AUTH_DB    = os.getenv("MONGO_AUTH_DB", "admin")

A_START = datetime(2026, 3,  6,  0,  0,  0, tzinfo=timezone.utc)
A_END   = datetime(2026, 4,  2, 23, 59, 59, tzinfo=timezone.utc)

OUR_SKS = {"_c36PoUEj", "_d6jWDbY", "_AnTGXs", "_olPBn9X", "_dVh6yw5"}
CIS_COUNTRIES = {"RU", "BY", "KZ", "UZ", "AZ", "AM", "GE", "KG", "MD", "TJ", "TM"}

def oid_from_dt(dt):
    ts = int(dt.timestamp())
    return ObjectId(f"{ts:08x}0000000000000000")

def pct(n, d):
    return f"{100*n/d:.1f}%" if d else "N/A"

def print_section(t):
    print(f"\n{'='*70}\n  {t}\n{'='*70}")


# ─────────────────────────────────────────────────────────────────────────────
# URL NORMALIZER
# ─────────────────────────────────────────────────────────────────────────────

# Homepage regex: empty or just / or /#... on any AliExpress host
HOMEPAGE_RE = re.compile(r'^https?://[^/]*aliexpress\.[^/]*(/(#.*)?)?$', re.I)

def is_homepage(url: str) -> bool:
    if not url:
        return False
    return bool(HOMEPAGE_RE.match(url))

def is_product_page(product_id) -> bool:
    return product_id is not None and product_id != ""

def normalize_url(url: str) -> tuple[str, str]:
    """
    Returns (category, normalized_path) for an AliExpress URL.
    category = one of the taxonomy labels
    normalized_path = cleaned path with IDs replaced by {id}
    """
    if not url:
        return ("unknown", "")
    try:
        p = urlparse(url)
        host = (p.hostname or "").lower()
        path = p.path or "/"
        qs = p.query or ""
    except Exception:
        return ("unknown", url[:80])

    # Normalize host (strip www., m., etc.)
    host_norm = re.sub(r'^(www\.|m\.|ru\.)', '', host)

    # Replace numeric IDs and hex IDs in path segments
    def clean_path(path_str):
        # Replace long numeric sequences (product IDs, item IDs)
        s = re.sub(r'/\d{6,}', '/{id}', path_str)
        # Replace short numeric IDs in specific contexts
        s = re.sub(r'/(item|product|store|category)/\d+', r'/\1/{id}', s, flags=re.I)
        # Normalize trailing slashes and .htm
        s = re.sub(r'\.htm(l)?$', '', s, flags=re.I)
        s = s.rstrip('/')
        return s or '/'

    path_norm = clean_path(path)
    path_lower = path.lower()
    qs_lower = qs.lower()

    # ── SEARCH ──────────────────────────────────────────────────────────────
    if any([
        '/search' in path_lower,
        'searchtext=' in qs_lower,
        's.aliexpress.' in host,
        path_lower.startswith('/wholesale'),
        '/search/' in path_lower,
    ]):
        return ("search_results", clean_path(re.sub(r'[^/]+', lambda m: '{q}' if len(m.group()) > 6 else m.group(), path_norm)))

    # ── CATEGORY / LISTING ──────────────────────────────────────────────────
    if any([
        '/category/' in path_lower,
        re.match(r'^/[a-z0-9-]+-cat-\d', path_lower),
        '/all-wholesale-' in path_lower,
        path_lower.startswith('/categories'),
        '/browse/' in path_lower,
        '/tag/' in path_lower,
    ]):
        return ("category_listing", path_norm[:80])

    # ── CART ────────────────────────────────────────────────────────────────
    if any([
        'shoppingcart' in path_lower,
        '/cart' in path_lower,
        '/basket' in path_lower,
    ]):
        return ("cart", path_norm[:80])

    # ── ORDER / CHECKOUT / PAYMENT ──────────────────────────────────────────
    if any([
        '/orderlist' in path_lower,
        '/order/' in path_lower,
        '/orders/' in path_lower,
        '/trade/' in path_lower,
        '/pay/' in path_lower,
        '/checkout' in path_lower,
        '/confirm_order' in path_lower,
        '/payment' in path_lower,
        'order_confirm' in path_lower,
        '/purchase' in path_lower.replace('?','').replace('#',''),
    ]):
        return ("order_checkout", path_norm[:80])

    # ── SELLER / STORE ───────────────────────────────────────────────────────
    if any([
        '/store/' in path_lower,
        path_lower.startswith('/store'),
        '/seller/' in path_lower,
        '/shop/' in path_lower,
        re.search(r'/[a-z0-9-]+-store-\d', path_lower),
    ]):
        return ("seller_store", clean_path(re.sub(r'/store/\d+', '/store/{id}', path)))

    # ── PROMO / CAMPAIGN / LANDING ───────────────────────────────────────────
    if any([
        '/gcp/' in path_lower,
        '/promotion/' in path_lower,
        '/deals/' in path_lower,
        '/promo/' in path_lower,
        '/sale/' in path_lower,
        '/event/' in path_lower,
        '/campaign/' in path_lower,
        '/hotproducts' in path_lower,
        '/hot-products' in path_lower,
        '/flash_deals' in path_lower,
        '/flashdeals' in path_lower,
        '/coupon' in path_lower,
        '/top-picks' in path_lower,
        '/landing' in path_lower,
    ]):
        return ("promo_landing", path_norm[:80])

    # ── FEED / RECOMMENDATIONS ──────────────────────────────────────────────
    if any([
        path_lower in ('/', ''),
        '/home' in path_lower,
        '/feed' in path_lower,
        '/recommend' in path_lower,
        '/discovery' in path_lower,
        '/newuser' in path_lower,
        '/new-user' in path_lower,
        '/just4u' in path_lower,
        'just-for-you' in path_lower,
        '/stream' in path_lower,
        '/video' in path_lower and '/product' not in path_lower,
    ]):
        return ("feed_recommendations", path_norm[:80])

    # ── ACCOUNT / PROFILE ────────────────────────────────────────────────────
    if any([
        '/account' in path_lower,
        '/myprofile' in path_lower,
        '/mypurse' in path_lower,
        '/myfollowing' in path_lower,
        '/myfavorites' in path_lower,
        '/mywishlist' in path_lower,
        '/personal-info' in path_lower,
        '/member/overview' in path_lower,
        '/member/overview' in path_lower,
        path_lower.startswith('/usercenter'),
    ]):
        return ("account_profile", path_norm[:80])

    # ── HELP / SERVICE ──────────────────────────────────────────────────────
    if any([
        '/help' in path_lower,
        '/service' in path_lower,
        '/dispute' in path_lower,
        '/refund' in path_lower,
        '/after-sale' in path_lower,
        '/complaint' in path_lower,
        '/contact' in path_lower,
        '/feedback' in path_lower,
        '/buynow' in path_lower,
    ]):
        return ("help_service", path_norm[:80])

    # ── BRAND / COLLECTION ──────────────────────────────────────────────────
    if any([
        '/brand/' in path_lower,
        '/collection/' in path_lower,
        '/handpick' in path_lower,
        '/topic/' in path_lower,
        '/list/' in path_lower,
    ]):
        return ("brand_collection", path_norm[:80])

    # ── REVIEW / RATING ─────────────────────────────────────────────────────
    if any([
        '/review' in path_lower,
        '/rating' in path_lower,
        '/feedback' in path_lower,
    ]):
        return ("review_rating", path_norm[:80])

    # ── HOMEPAGE (catch-all for variants) ────────────────────────────────────
    # Already handled above, but catch sub-paths that look like home
    if path in ('/', ''):
        return ("homepage_variant", path_norm[:80])

    # ── UNKNOWN ─────────────────────────────────────────────────────────────
    return ("other", path_norm[:80])


# ─────────────────────────────────────────────────────────────────────────────
# MONGODB QUERY: URL taxonomy for non-eligible events
# ─────────────────────────────────────────────────────────────────────────────

def run_ineligible_url_query(db) -> dict:
    """
    Aggregates non-eligible events in Problem A window by URL.
    Returns dict with:
      - url_counts: list of {url, count, unique_users_approx}  (top URLs)
      - events_sample: list of {url, product_id, guest_id} (sample for Python categorization)
      - total_noeligible_events: int
    """
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

    # Pipeline 1: count non-eligible events per URL pattern (path only, top 2000)
    pipeline_urls = [
        {"$match": {
            "_id": {"$gte": oid_start, "$lte": oid_end},
            "payload.productId": None,
            # Exclude homepage pattern
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
            "url": "$_id",
            "count": 1,
            "unique_users": {"$size": "$guest_ids"},
        }},
        {"$sort": {"count": -1}},
        {"$limit": 3000},
    ]

    print("  [mongo]   Pipeline 1: top URLs by hit count...")
    url_counts = list(events.aggregate(pipeline_urls, allowDiskUse=True))
    print(f"    → {len(url_counts):,} distinct URLs in {time.time()-t0:.1f}s")

    # Pipeline 2: user-level — for each user, get ALL their non-eligible URLs
    # (to compute: user has ONLY non-eligible pages, or mixed)
    print("  [mongo]   Pipeline 2: per-user non-eligible event count + URL sample...")
    t1 = time.time()
    pipeline_user_noelig = [
        {"$match": {
            "_id": {"$gte": oid_start, "$lte": oid_end},
            "payload.productId": None,
            "payload.url": {
                "$not": {"$regex": r"^https?://[^/]*aliexpress\.[^/]*(/(#.*)?)?$", "$options": "i"},
                "$exists": True,
                "$ne": None,
            }
        }},
        {"$group": {
            "_id": "$guest_id",
            "noelig_count": {"$sum": 1},
            "urls": {"$push": "$payload.url"},  # up to first 50 collected by Mongo
        }},
        {"$project": {
            "noelig_count": 1,
            "urls": {"$slice": ["$urls", 10]},  # keep only first 10 per user
        }},
    ]
    user_noelig = list(events.aggregate(pipeline_user_noelig, allowDiskUse=True))
    print(f"    → {len(user_noelig):,} users with non-eligible events in {time.time()-t1:.1f}s")

    result = {
        "url_counts": url_counts,
        "user_noelig": user_noelig,
    }

    with open(cache_file, "wb") as f:
        pickle.dump(result, f)
    print(f"  [cache] Saved ineligible_url_taxonomy.pkl")
    return result


# ─────────────────────────────────────────────────────────────────────────────
# MONGODB QUERY: First event per user
# ─────────────────────────────────────────────────────────────────────────────

def run_first_events_query(db) -> list:
    """
    For each user in the Problem A window, get:
    - first event URL
    - first event productId (if any)
    - first event _id (timestamp)
    Returns list of dicts.
    """
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
    print(f"    → {len(first_events):,} users' first events in {time.time()-t0:.1f}s")

    with open(cache_file, "wb") as f:
        pickle.dump(first_events, f)
    print("  [cache] Saved first_events_a.pkl")
    return first_events


# ─────────────────────────────────────────────────────────────────────────────
# ANALYSIS
# ─────────────────────────────────────────────────────────────────────────────

def load_existing_cache():
    print_section("Loading existing caches")

    # mongo_problem_a.pkl
    with open(CACHE_DIR / "mongo_problem_a.pkl", "rb") as f:
        mongo_a = pickle.load(f)

    # Build user-level lookup: user_id -> {product_events, homepage_events, country, ...}
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
        lambda uid: client_map.get(uid, {}).get("browser", "") or ""
    )

    print(f"  Total users: {len(user_df):,}")
    print(f"  Eligible:    {user_df['is_eligible'].sum():,}")
    print(f"  Ineligible:  {(~user_df['is_eligible']).sum():,}")

    # Affiliate Click: who reached hub
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
    """
    Takes raw URL data from MongoDB and categorizes into taxonomy groups.
    Returns taxonomy_df: per-category stats.
    """
    print_section("Building URL taxonomy")

    url_counts = inelig_data["url_counts"]
    user_noelig = inelig_data["user_noelig"]

    total_raw_users = len(user_df)
    inelig_users_set = set(user_df.loc[~user_df["is_eligible"], "user_id"])
    total_inelig_users = len(inelig_users_set)

    # ── Categorize top URLs ─────────────────────────────────────────────────
    print(f"  Categorizing {len(url_counts):,} distinct URLs...")
    cat_hits = defaultdict(int)
    cat_users = defaultdict(set)
    cat_urls = defaultdict(list)   # store examples
    cat_pattern_counts = defaultdict(Counter)  # normalized path → count

    for row in url_counts:
        url = row.get("url") or row.get("_id") or ""
        count = row.get("count", 0)
        unique_users = row.get("unique_users", 0)
        cat, path_norm = normalize_url(url)
        cat_hits[cat] += count
        cat_urls[cat].append((url, count))
        cat_pattern_counts[cat][path_norm] += count

    # ── Per-user category breakdown ─────────────────────────────────────────
    # From user_noelig: for each user, classify their URL samples
    user_cats = defaultdict(set)  # user_id -> set of categories visited
    for row in user_noelig:
        uid = str(row["_id"])
        urls = row.get("urls") or []
        for url in urls:
            cat, _ = normalize_url(url)
            user_cats[uid].add(cat)

    # Also collect per-category unique user counts from url_counts
    # (url_counts already has unique_users per URL, so we use that for per-category totals)
    # But we need to be careful: same user may appear in multiple URLs of same category
    # So we re-derive from user_noelig for accurate unique user counts per category
    cat_user_counts = defaultdict(set)
    for row in user_noelig:
        uid = str(row["_id"])
        urls = row.get("urls") or []
        for url in urls:
            cat, _ = normalize_url(url)
            cat_user_counts[cat].add(uid)

    # Estimate total hits per category from URL counts (more accurate than user_noelig sample)
    # cat_hits already computed above from url_counts

    # ── Build taxonomy dataframe ─────────────────────────────────────────────
    rows = []
    for cat in sorted(cat_hits.keys(), key=lambda c: -cat_hits[c]):
        hit_count = cat_hits[cat]
        u_count = len(cat_user_counts[cat])
        share_raw = 100 * u_count / total_raw_users if total_raw_users else 0
        share_inelig = 100 * u_count / total_inelig_users if total_inelig_users else 0

        # Top URL examples
        top_urls = sorted(cat_urls[cat], key=lambda x: -x[1])[:5]
        examples = [u for u, _ in top_urls]

        # Top normalized patterns
        top_patterns = cat_pattern_counts[cat].most_common(5)

        rows.append({
            "category": cat,
            "total_hits": hit_count,
            "unique_users_sample": u_count,
            "pct_raw_users": share_raw,
            "pct_inelig_users": share_inelig,
            "top_patterns": top_patterns,
            "examples": examples,
        })

    taxonomy_df = pd.DataFrame(rows)
    return taxonomy_df, user_cats


def first_entry_analysis(first_events: list, user_df: pd.DataFrame, user_cats: dict):
    """
    Analyzes first observed AliExpress page per user.
    """
    print_section("First entry point analysis")

    # Build user meta lookup
    user_meta = user_df.set_index("user_id")

    total_users = len(user_df)
    inelig_users_set = set(user_df.loc[~user_df["is_eligible"], "user_id"])

    rows = []
    for r in first_events:
        uid = str(r["_id"])
        url = r.get("first_url") or ""
        product_id = r.get("first_product_id")
        oid = r.get("first_oid")
        country = r.get("country", "")

        # Classify first page
        if is_product_page(product_id):
            first_cat = "product_page"
        elif is_homepage(url):
            first_cat = "homepage"
        else:
            first_cat, _ = normalize_url(url)

        # Get user info
        meta = user_meta.loc[uid] if uid in user_meta.index else None
        is_eligible = bool(meta["is_eligible"]) if meta is not None else False
        reached_hub = bool(meta["reached_hub"]) if meta is not None else False

        # Timestamp from OID
        if oid:
            ts_ms = oid.generation_time.timestamp() if hasattr(oid, 'generation_time') else 0
        else:
            ts_ms = 0

        rows.append({
            "user_id": uid,
            "first_cat": first_cat,
            "first_url": url[:120],
            "is_eligible": is_eligible,
            "reached_hub": reached_hub,
            "country": country,
            "first_ts": ts_ms,
        })

    fe_df = pd.DataFrame(rows)
    print(f"  First events: {len(fe_df):,} users")

    # ── Cohort breakdown ────────────────────────────────────────────────────
    # Cohort 1: first page was already eligible
    c_eligible_first = fe_df[fe_df["first_cat"].isin(["product_page", "homepage"])]
    # Cohort 2: first page was non-eligible, user later became eligible
    c_noelig_then_elig = fe_df[(~fe_df["first_cat"].isin(["product_page", "homepage"])) & fe_df["is_eligible"]]
    # Cohort 3: first page was non-eligible, user never reached eligible page
    c_noelig_only = fe_df[(~fe_df["first_cat"].isin(["product_page", "homepage"])) & ~fe_df["is_eligible"]]

    print(f"\n  Cohort 1 — first page eligible (product/homepage): {len(c_eligible_first):,} ({pct(len(c_eligible_first), total_users)})")
    print(f"  Cohort 2 — first non-eligible, later reached eligible: {len(c_noelig_then_elig):,} ({pct(len(c_noelig_then_elig), total_users)})")
    print(f"  Cohort 3 — first non-eligible, never eligible:  {len(c_noelig_only):,} ({pct(len(c_noelig_only), total_users)})")

    # ── First entry ranked by category ─────────────────────────────────────
    entry_stats = []
    for cat, grp in fe_df.groupby("first_cat"):
        n_users = len(grp)
        n_later_elig = grp["is_eligible"].sum()
        n_never_elig = n_users - n_later_elig
        n_hub = grp["reached_hub"].sum()
        entry_stats.append({
            "first_page_type": cat,
            "unique_users": n_users,
            "pct_all_users": 100*n_users/total_users,
            "later_reached_eligible": int(n_later_elig),
            "never_reached_eligible": int(n_never_elig),
            "pct_later_eligible": 100*n_later_elig/n_users if n_users else 0,
            "reached_hub": int(n_hub),
        })

    entry_df = pd.DataFrame(entry_stats).sort_values("unique_users", ascending=False)
    return fe_df, entry_df, c_eligible_first, c_noelig_then_elig, c_noelig_only


def top_missed_entry_before_eligible(fe_df: pd.DataFrame, c_noelig_then_elig: pd.DataFrame):
    """
    Among users who started on a non-eligible page and later reached eligible:
    what was their first non-eligible page category?
    """
    grp = c_noelig_then_elig.groupby("first_cat").size().reset_index(name="users")
    grp = grp.sort_values("users", ascending=False)

    # Activation priority labels
    PRIORITY = {
        "search_results":    ("worth_testing",    "Users actively shopping — may benefit from activation on search results"),
        "category_listing":  ("worth_testing",    "Browsing categories — high purchase intent path"),
        "promo_landing":     ("worth_testing",    "Visiting deals/promos — often pre-purchase intent"),
        "seller_store":      ("worth_testing",    "Browsing store pages — mid-funnel shopping behavior"),
        "brand_collection":  ("worth_testing",    "Browsing brand pages — shopping intent"),
        "feed_recommendations": ("low_priority",  "Passive browsing — lower purchase intent"),
        "cart":              ("high_priority",    "Cart page = strongest purchase signal — must activate before checkout"),
        "order_checkout":    ("not_worth",        "Post-purchase or checkout flow — too late or excluded by noLogUrls"),
        "account_profile":   ("not_worth",        "Account management — no affiliate opportunity"),
        "help_service":      ("not_worth",        "Help/support pages — no affiliate opportunity"),
        "review_rating":     ("low_priority",     "Reading reviews — some pre-purchase intent"),
        "other":             ("low_priority",     "Unknown/misc pages"),
        "homepage_variant":  ("high_priority",    "Homepage variant (sub-path) — should be covered by eligibility logic"),
        "unknown":           ("low_priority",     "Unclassified URLs"),
    }

    rows = []
    for _, row in grp.iterrows():
        cat = row["first_cat"]
        priority, reasoning = PRIORITY.get(cat, ("low_priority", ""))
        rows.append({
            "page_group": cat,
            "users_transitioning_to_eligible": int(row["users"]),
            "activation_priority": priority,
            "reasoning": reasoning,
        })
    return pd.DataFrame(rows)


# ─────────────────────────────────────────────────────────────────────────────
# REPORT
# ─────────────────────────────────────────────────────────────────────────────

def print_full_report(user_df, taxonomy_df, fe_df, entry_df,
                      c_eligible_first, c_noelig_then_elig, c_noelig_only,
                      missed_df):
    total_users = len(user_df)
    inelig_count = (~user_df["is_eligible"]).sum()
    elig_count = user_df["is_eligible"].sum()

    print_section("INELIGIBLE PAGE TAXONOMY — FULL REPORT")

    print(f"""
## Overview

| Metric | Count | % of raw |
|--------|------:|--------:|
| Total raw users | {total_users:,} | 100% |
| Eligible users | {elig_count:,} | {pct(elig_count, total_users)} |
| Ineligible users | {inelig_count:,} | {pct(inelig_count, total_users)} |
""")

    print("## Taxonomy of Non-Eligible Pages\n")
    tax_rows = []
    for _, r in taxonomy_df.iterrows():
        top_pats = "; ".join(f"{pat}" for pat, cnt in r["top_patterns"][:3])
        examples_str = r["examples"][0] if r["examples"] else ""
        tax_rows.append([
            r["category"],
            f"{r['total_hits']:,}",
            f"{r['unique_users_sample']:,}",
            f"{r['pct_raw_users']:.1f}%",
            f"{r['pct_inelig_users']:.1f}%",
            top_pats[:80],
        ])
    print(tabulate(tax_rows,
        headers=["Category","Hits","Users(sample)","% raw","% inelig","Top patterns"],
        tablefmt="github"))

    print("\n\n## First Entry Point Analysis\n")
    print(f"""
**Cohorts:**
- Cohort 1 — first page was already eligible (product/homepage): **{len(c_eligible_first):,}** ({pct(len(c_eligible_first), total_users)})
- Cohort 2 — first page non-eligible, later reached eligible: **{len(c_noelig_then_elig):,}** ({pct(len(c_noelig_then_elig), total_users)})
- Cohort 3 — first page non-eligible, never reached eligible: **{len(c_noelig_only):,}** ({pct(len(c_noelig_only), total_users)})
""")

    print("### First Entry by Page Type\n")
    entry_rows = []
    for _, r in entry_df.head(15).iterrows():
        entry_rows.append([
            r["first_page_type"],
            f"{r['unique_users']:,}",
            f"{r['pct_all_users']:.1f}%",
            f"{r['later_reached_eligible']:,}",
            f"{r['never_reached_eligible']:,}",
            f"{r['pct_later_eligible']:.1f}%",
            f"{r['reached_hub']:,}",
        ])
    print(tabulate(entry_rows,
        headers=["First page type","Users","% all","Later→elig","Never elig","% later elig","Reached hub"],
        tablefmt="github"))

    print("\n\n## Top Missed Entry Points Before Eligible Pages\n")
    miss_rows = []
    for _, r in missed_df.iterrows():
        miss_rows.append([
            r["page_group"],
            f"{r['users_transitioning_to_eligible']:,}",
            r["activation_priority"],
            r["reasoning"][:80],
        ])
    print(tabulate(miss_rows,
        headers=["Page group","Users→elig","Priority","Reasoning"],
        tablefmt="github"))

    print("\n\n## Per-Category Detail\n")
    for _, r in taxonomy_df.head(12).iterrows():
        cat = r["category"]
        print(f"\n### {cat} ({r['total_hits']:,} hits, ~{r['unique_users_sample']:,} users)")
        print(f"  Share of raw users: {r['pct_raw_users']:.1f}% | Share of ineligible users: {r['pct_inelig_users']:.1f}%")
        print("  Top patterns:")
        for pat, cnt in r["top_patterns"][:5]:
            print(f"    {cnt:>7,}  {pat}")
        print("  Example URLs:")
        for ex in r["examples"][:5]:
            print(f"    {ex[:100]}")


def write_csv(taxonomy_df, missed_df, entry_df):
    """Write taxonomy and missed entry CSV files."""
    # Taxonomy CSV
    csv_path = Path("./ineligible_taxonomy.csv")
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["category","total_hits","unique_users_sample",
                         "pct_raw_users","pct_inelig_users",
                         "top_pattern_1","top_pattern_2","top_pattern_3",
                         "example_url_1","example_url_2","example_url_3"])
        for _, r in taxonomy_df.iterrows():
            pats = [p for p, _ in r["top_patterns"][:3]]
            while len(pats) < 3:
                pats.append("")
            exs = r["examples"][:3]
            while len(exs) < 3:
                exs.append("")
            writer.writerow([
                r["category"], r["total_hits"], r["unique_users_sample"],
                f"{r['pct_raw_users']:.2f}", f"{r['pct_inelig_users']:.2f}",
                pats[0], pats[1], pats[2],
                exs[0], exs[1], exs[2],
            ])
    print(f"\n  [csv] Saved {csv_path}")

    # Entry point CSV
    csv2_path = Path("./first_entry_taxonomy.csv")
    entry_df.to_csv(csv2_path, index=False)
    print(f"  [csv] Saved {csv2_path}")


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

def main():
    print_section("Cache & Extraction Plan")
    print("""
  REUSED (no re-extraction):
    cache/mongo_problem_a.pkl  — 63,916 user-level aggregations (product/homepage counts, browser, sk)
    cache/aff_click_a.json     — Affiliate Click events (who reached hub)

  NEW MongoDB queries (targeted, not full re-extraction):
    cache/ineligible_url_taxonomy.pkl  — aggregate non-eligible event URLs (top 3000 URLs by hits + per-user URL sample)
    cache/first_events_a.pkl           — first event per user (URL + productId + timestamp)
    """)

    user_df, mongo_a = load_existing_cache()

    print_section("MongoDB: new targeted queries")
    with sshtunnel.SSHTunnelForwarder(
        (SSH_HOST, 22),
        ssh_username=SSH_USER,
        remote_bind_address=(DB_HOST, DB_PORT),
        local_bind_address=("127.0.0.1", LOCAL_PORT),
    ) as tunnel:
        client = pymongo.MongoClient(
            "localhost", LOCAL_PORT,
            username=MONGO_USER, password=MONGO_PASS,
            authSource=AUTH_DB, directConnection=True,
        )
        db = client[DB_NAME]

        inelig_data  = run_ineligible_url_query(db)
        first_events = run_first_events_query(db)

        client.close()

    print_section("Analysis")
    taxonomy_df, user_cats = build_url_taxonomy(inelig_data, user_df)
    fe_df, entry_df, c1, c2, c3 = first_entry_analysis(first_events, user_df, user_cats)
    missed_df = top_missed_entry_before_eligible(fe_df, c2)

    print_full_report(user_df, taxonomy_df, fe_df, entry_df, c1, c2, c3, missed_df)
    write_csv(taxonomy_df, missed_df, entry_df)

    print_section("Done")
    print("  Outputs:")
    print("    /tmp/ineligible_output.txt   — full console output")
    print("    ineligible_taxonomy.csv      — URL category taxonomy table")
    print("    first_entry_taxonomy.csv     — first entry point stats")


if __name__ == "__main__":
    main()
