"""
Shared utility functions: formatting, browser classification, URL/UTM parsing, eligibility.
"""

import re
from datetime import datetime
from urllib.parse import urlparse, parse_qs

import pandas as pd
from bson import ObjectId

from src.config import AUTO_REDIRECT_BROWSERS, CIS_COUNTRIES, OUR_SKS


# ── ObjectId helpers ─────────────────────────────────────────────────────────

def oid_from_dt(dt: datetime) -> ObjectId:
    """Create an ObjectId from a datetime for _id-based date range queries."""
    ts = int(dt.timestamp())
    return ObjectId(f"{ts:08x}0000000000000000")


# ── Formatting ───────────────────────────────────────────────────────────────

def pct(num, denom, decimals=1):
    """Format a ratio as a percentage string."""
    if denom == 0:
        return "N/A"
    return f"{100 * num / denom:.{decimals}f}%"


def pct_f(num, denom):
    """Return float percentage, 0 if denom is 0."""
    if denom == 0:
        return 0.0
    return 100 * num / denom


def fmt(n):
    """Format number with thousands separator."""
    return f"{n:,}"


def print_section(title):
    """Print a section header."""
    print(f"\n{'=' * 70}")
    print(f"  {title}")
    print(f"{'=' * 70}")


# ── Browser / lineage classification ─────────────────────────────────────────

def browser_family(browser_str: str) -> str:
    """Normalize browser string to family name."""
    if not browser_str:
        return "unknown"
    b = str(browser_str).lower()
    if "firefox" in b:
        return "firefox"
    if "edge" in b or "edg/" in b:
        return "edge"
    if "yandex" in b or "yabrowser" in b:
        return "yandex"
    if "opera" in b or "opr/" in b:
        return "opera"
    if "chrome" in b or "chromium" in b:
        return "chrome"
    if "safari" in b:
        return "safari"
    return "other"


def lineage(bf: str) -> str:
    """Classify browser family into redirect lineage (auto-redirect vs dogi)."""
    if bf in AUTO_REDIRECT_BROWSERS:
        return "auto-redirect"
    return "dogi"


def get_lineage(browser: str) -> str:
    """Classify raw browser string into redirect lineage."""
    return lineage(browser_family(browser))


# ── Region classification ────────────────────────────────────────────────────

def is_cis(country: str) -> bool:
    """Check if a country uses CIS/EPN affiliate routing."""
    return str(country).upper() in CIS_COUNTRIES


def region_label(country: str) -> str:
    """Return 'CIS' or 'Global' based on affiliate routing."""
    return "CIS" if is_cis(country) else "Global"


# ── UTM parsing (CIS affiliate attribution) ─────────────────────────────────

def parse_utm(url: str) -> dict:
    """
    Parse UTM params from a URL query string.
    Returns dict with keys: utm_source, utm_medium, utm_campaign (or None).
    """
    if not url:
        return {"utm_source": None, "utm_medium": None, "utm_campaign": None}
    try:
        qs = parse_qs(urlparse(url).query)
        return {
            "utm_source": qs.get("utm_source", [None])[0],
            "utm_medium": qs.get("utm_medium", [None])[0],
            "utm_campaign": qs.get("utm_campaign", [None])[0],
        }
    except Exception:
        return {"utm_source": None, "utm_medium": None, "utm_campaign": None}


def is_alihelper_utm(url: str) -> bool:
    """
    Check if URL contains AliHelper-owned EPN UTM markers.
    All three must match: utm_source=aerkol, utm_medium=cpa, utm_campaign=*_7685
    """
    utm = parse_utm(url)
    return (
        utm["utm_source"] == "aerkol"
        and utm["utm_medium"] == "cpa"
        and utm["utm_campaign"] is not None
        and utm["utm_campaign"].endswith("_7685")
    )


def is_foreign_utm(url: str) -> bool:
    """
    Check if URL contains UTM params that indicate a foreign (non-AliHelper) affiliate.
    Returns True if there are UTM affiliate markers that don't match AliHelper pattern.
    """
    utm = parse_utm(url)
    has_any = utm["utm_source"] or utm["utm_medium"] or utm["utm_campaign"]
    if not has_any:
        return False
    return not is_alihelper_utm(url)


# ── Global sk parsing ────────────────────────────────────────────────────────

def parse_sk(query_sk: str) -> str | None:
    """Extract sk value from querySk field (stores the raw sk value directly)."""
    if not query_sk:
        return None
    val = str(query_sk).strip()
    return val if val else None


def is_our_sk(query_sk: str) -> bool:
    """Check if querySk is an AliHelper-owned sk."""
    sk = parse_sk(query_sk)
    return sk in OUR_SKS if sk else False


def has_foreign_sk(query_sk: str) -> bool:
    """Check if querySk is a non-AliHelper sk."""
    sk = parse_sk(query_sk)
    if not sk:
        return False
    return sk not in OUR_SKS


def has_af(query_sk: str) -> bool:
    """Check if querySk looks like an af parameter (not sk).
    Note: querySk stores raw sk values; af is not detectable from this field alone."""
    # querySk contains raw sk values, never af — af detection requires URL parsing
    return False


# ── Eligible page detection ──────────────────────────────────────────────────

# Auto-redirect eligible URL patterns (from extension checkListUrls)
CHECK_LIST_URLS = [
    re.compile(r'^https?://([\w\.]+)?aliexpress\.(com|ru|us)/item/(\d+)\.html', re.I | re.M),
    re.compile(r'^https?://([\w\.]+)?(aliexpress|tmall)\.(com|ru|us)/item/.*?/(\d+)\.html', re.I | re.M),
    re.compile(r'^https?://([\w\.]+)?aliexpress\.(com|ru|us)/i/(\d+)\.html', re.I | re.M),
    re.compile(r'^https?://([\w\.]+)?(aliexpress|tmall)\.(com|ru|us)/item/(\d+)\.html', re.I),
    re.compile(r'^https?://([\w\.]+)?aliexpress\.(com|ru|us)/store/product/.*?/(\d+)_(\d+)\.html', re.I | re.M),
    re.compile(r'^https?://group\.aliexpress\.(com|ru|us)/(\d+)-(\d+)-detail\.html', re.I | re.M),
    re.compile(r'^https?://sale\.aliexpress\.(com|ru|us)/[\S]+/affi\-item\.htm', re.I | re.M),
    re.compile(r'^https?://play\.aliexpress\.(com|ru|us)/[\S]+/productDetail\.htm', re.I | re.M),
    re.compile(r'^https?://([\w\.]+)?aliexpress\.(com|ru|us)/ssr/(\d+)/([\w\-]+)', re.I | re.M),
]


def matches_check_list_urls(url: str) -> bool:
    """Check if URL matches any auto-redirect eligible pattern."""
    if not url:
        return False
    return any(pat.search(url) for pat in CHECK_LIST_URLS)


def is_eligible_product_page(product_id) -> bool:
    """Check if event represents an eligible product page (DOGI flow)."""
    return product_id is not None and product_id != ""


def is_eligible(url: str, product_id, lineage_str: str) -> bool:
    """
    Check if a page visit is eligible for affiliate activation.
    - DOGI: product pages only (productId present)
    - Auto-redirect: URLs matching checkListUrls patterns
    """
    if lineage_str == "auto-redirect":
        return matches_check_list_urls(url)
    else:
        return is_eligible_product_page(product_id)


# ── Mixpanel DataFrame helpers ───────────────────────────────────────────────

def mp_to_df(records: list[dict]) -> pd.DataFrame:
    """Flatten Mixpanel NDJSON records to DataFrame."""
    rows = [r.get("properties", {}) for r in records]
    return pd.DataFrame(rows)


# ── AliExpress host detection ───────────────────────────────────────────────

def is_aliexpress_ru(url: str) -> bool:
    """Check if URL is on aliexpress.ru domain."""
    if not url:
        return False
    try:
        host = urlparse(url).hostname or ""
        return "aliexpress.ru" in host.lower()
    except Exception:
        return False
