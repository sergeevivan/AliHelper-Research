"""
Shared utility functions: formatting, browser/build classification, URL/UTM/af
parsing with source priority, CIS patterns A/B, eligibility, labels.

Attribution source priority (see specs/domain/attribution.md):
    sk:  events.params.sk  -> events.payload.querySk -> parse(events.payload.url)
    af:  events.params.af                             -> parse(events.payload.url)
    utm: events.params.<name>                          -> parse(events.payload.url)

Two CIS patterns on aliexpress.ru (mutually exclusive):
    Pattern A: af=*_7685        (+ utm_medium=cpa typically)
    Pattern B: utm_source=aerkol + utm_medium=cpa + utm_campaign=*_7685

Labels assigned per-event/per-user:
    GLOBAL_DIRECT, CIS_DIRECT_AF, CIS_DIRECT_UTM, CIS_PARTIAL_UTM, CIS_PROXY
"""

import re
from datetime import datetime
from urllib.parse import urlparse, parse_qs

import pandas as pd
from bson import ObjectId

from src.config import (
    AUTO_REDIRECT_BROWSERS, DOGI_BROWSERS, CIS_COUNTRIES, OUR_SKS, EPN_SUFFIX,
)


# ── ObjectId helpers ─────────────────────────────────────────────────────────

def oid_from_dt(dt: datetime) -> ObjectId:
    """Create an ObjectId from a datetime for _id-based date range queries."""
    ts = int(dt.timestamp())
    return ObjectId(f"{ts:08x}0000000000000000")


# ── Formatting ───────────────────────────────────────────────────────────────

def pct(num, denom, decimals=1):
    if denom == 0:
        return "N/A"
    return f"{100 * num / denom:.{decimals}f}%"


def pct_f(num, denom):
    if denom == 0:
        return 0.0
    return 100 * num / denom


def fmt(n):
    return f"{n:,}"


def print_section(title):
    print(f"\n{'=' * 70}")
    print(f"  {title}")
    print(f"{'=' * 70}")


# ── Browser family / build / lineage classification ──────────────────────────

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


def lineage_from_build(build_app) -> str | None:
    """Flow lineage strictly from clients.build_app. None if missing/unknown."""
    if not build_app:
        return None
    b = str(build_app).strip().lower()
    if b == "chrome":
        return "dogi"
    if b in ("firefox", "edge"):
        return "auto_redirect"
    return None


def lineage_from_ua(browser_str: str) -> str:
    """Fallback lineage from UA alone. Edge is ambiguous (build unknown)."""
    fam = browser_family(browser_str)
    if fam == "edge":
        return "edge_ambiguous_build"
    if fam == "firefox":
        return "auto_redirect"
    if fam in DOGI_BROWSERS:
        return "dogi"
    return "unknown_build"


def lineage_segment(build_app, browser_str: str) -> str:
    """
    Authoritative lineage: build_app first, UA fallback.
    Returns one of: dogi, auto_redirect, edge_ambiguous_build, unknown_build.
    """
    from_build = lineage_from_build(build_app)
    if from_build is not None:
        return from_build
    return lineage_from_ua(browser_str)


# Legacy alias — some callers still expect a simple lineage string
def lineage(bf: str) -> str:
    if bf in AUTO_REDIRECT_BROWSERS:
        return "auto_redirect"
    if bf in DOGI_BROWSERS:
        return "dogi"
    if bf == "edge":
        return "edge_ambiguous_build"
    return "unknown_build"


def get_lineage(browser: str) -> str:
    return lineage(browser_family(browser))


# ── Region classification ────────────────────────────────────────────────────

def is_cis_country(country: str) -> bool:
    """User-level CIS check by country (for segmentation only)."""
    return str(country).upper() in CIS_COUNTRIES


# Kept for backwards-compat; prefer is_cis_country for clarity
def is_cis(country: str) -> bool:
    return is_cis_country(country)


def region_label(country: str) -> str:
    return "CIS" if is_cis_country(country) else "Global"


def is_aliexpress_ru(url: str) -> bool:
    """Domain-based CIS classifier: an aliexpress.ru URL is a CIS event."""
    if not url:
        return False
    try:
        host = urlparse(url).hostname or ""
        return "aliexpress.ru" in host.lower()
    except Exception:
        return False


def is_aliexpress_host(url: str) -> bool:
    """Any aliexpress.* host (used for Global-`af` overwrite detection)."""
    if not url:
        return False
    try:
        host = urlparse(url).hostname or ""
        return "aliexpress" in host.lower()
    except Exception:
        return False


# ── URL query parsing ────────────────────────────────────────────────────────

def _parse_query(url: str) -> dict:
    if not url:
        return {}
    try:
        return {k: v[0] for k, v in parse_qs(urlparse(url).query).items() if v}
    except Exception:
        return {}


# ── Attribution param extraction with source priority ───────────────────────
#
# Each extractor returns (value, source) where source ∈ {params, querySk, url_parse, none}.
# Coverage aggregates tally `source` counts for the coverage snapshot.


def extract_sk(event: dict) -> tuple[str | None, str]:
    """
    Priority: events.params.sk -> events.payload.querySk -> URL.
    event is a row dict carrying: params (dict|None), query_sk (str|None), url (str|None).
    """
    params = event.get("params") or {}
    if isinstance(params, dict):
        v = params.get("sk")
        if v:
            return str(v), "params"
    qsk = event.get("query_sk")
    if qsk:
        return str(qsk), "querySk"
    q = _parse_query(event.get("url") or "")
    v = q.get("sk")
    if v:
        return v, "url_parse"
    return None, "none"


def extract_af(event: dict) -> tuple[str | None, str]:
    """Priority: events.params.af -> URL."""
    params = event.get("params") or {}
    if isinstance(params, dict):
        v = params.get("af")
        if v:
            return str(v), "params"
    q = _parse_query(event.get("url") or "")
    v = q.get("af")
    if v:
        return v, "url_parse"
    return None, "none"


def extract_utm(event: dict) -> tuple[dict, str]:
    """
    Priority: events.params.utm_* -> URL.
    Returns (dict with utm_source/utm_medium/utm_campaign/utm_content, source).
    """
    params = event.get("params") or {}
    out = {"utm_source": None, "utm_medium": None, "utm_campaign": None, "utm_content": None}
    if isinstance(params, dict) and any(params.get(k) for k in out):
        for k in out:
            v = params.get(k)
            if v:
                out[k] = str(v)
        return out, "params"
    q = _parse_query(event.get("url") or "")
    if any(q.get(k) for k in out):
        for k in out:
            if q.get(k):
                out[k] = q[k]
        return out, "url_parse"
    return out, "none"


# ── Global: owned vs foreign sk ──────────────────────────────────────────────

def is_our_sk_value(sk: str | None) -> bool:
    return bool(sk) and sk in OUR_SKS


def is_foreign_sk_value(sk: str | None) -> bool:
    return bool(sk) and sk not in OUR_SKS


# Legacy wrappers — accept the legacy `query_sk` string directly
def parse_sk(query_sk: str) -> str | None:
    if not query_sk:
        return None
    v = str(query_sk).strip()
    return v or None


def is_our_sk(query_sk: str) -> bool:
    return is_our_sk_value(parse_sk(query_sk))


def has_foreign_sk(query_sk: str) -> bool:
    return is_foreign_sk_value(parse_sk(query_sk))


# ── CIS: af-based (Pattern A) and UTM-based (Pattern B) ──────────────────────

def is_our_af_value(af: str | None) -> bool:
    """af ends with _7685 → AliHelper-owned (Pattern A)."""
    return bool(af) and str(af).endswith(EPN_SUFFIX)


def is_foreign_af_value(af: str | None) -> bool:
    """af present with a non-7685 suffix → third-party CIS affiliate."""
    if not af:
        return False
    s = str(af)
    # Only treat suffixed `*_<digits>` as affiliate-style
    if not re.search(r"_\d+$", s):
        return False
    return not s.endswith(EPN_SUFFIX)


def classify_cis_utm(utm: dict) -> str:
    """
    Returns one of: owned_full, owned_partial, foreign, none.

    owned_full   : utm_source=aerkol + utm_medium=cpa + utm_campaign=*_7685
    owned_partial: utm_campaign=*_7685 but missing source or medium
    foreign      : any affiliate-style UTM not ours
    none         : no affiliate UTM signal
    """
    src = utm.get("utm_source")
    med = utm.get("utm_medium")
    cam = utm.get("utm_campaign")

    camp_7685 = bool(cam) and str(cam).endswith(EPN_SUFFIX)

    if camp_7685 and src == "aerkol" and med == "cpa":
        return "owned_full"
    if camp_7685:
        return "owned_partial"

    # Foreign evidence: campaign ends with some other _<digits>, or source != aerkol with cpa medium
    if cam and re.search(r"_\d+$", str(cam)):
        return "foreign"
    if med == "cpa" and src and src != "aerkol":
        return "foreign"
    if src and src != "aerkol" and cam:
        return "foreign"
    return "none"


# ── Event-level classification (CIS = URL domain, not country) ──────────────

def classify_event(event: dict) -> dict:
    """
    Classify a single event by extracting sk/af/utm and assigning a label.
    Returns dict with: sk, sk_source, af, af_source, utm, utm_source_tier,
                      is_ali_ru, label, is_owned, is_foreign.

    Label is one of: GLOBAL_DIRECT, CIS_DIRECT_AF, CIS_DIRECT_UTM,
                     CIS_PARTIAL_UTM, None (non-affiliate event).
    Foreign-only events get label=None but is_foreign=True with kind.
    """
    url = event.get("url") or ""
    ali_ru = is_aliexpress_ru(url)

    sk, sk_src = extract_sk(event)
    af, af_src = extract_af(event)
    utm, utm_src = extract_utm(event)
    utm_kind = classify_cis_utm(utm)

    label = None
    is_owned = False
    foreign_kind = None
    # Anomaly: AliHelper EPN suffix _7685 appearing on a Global (non-aliexpress.ru)
    # AliExpress host — UA routes as Global, _7685 should only exist on CIS.
    epn_on_global = False

    if ali_ru:
        # CIS landing: prefer af (Pattern A), then UTM (Pattern B or partial)
        if is_our_af_value(af):
            label = "CIS_DIRECT_AF"
            is_owned = True
        elif utm_kind == "owned_full":
            label = "CIS_DIRECT_UTM"
            is_owned = True
        elif utm_kind == "owned_partial":
            label = "CIS_PARTIAL_UTM"
            is_owned = True          # still counts as owned per attribution spec
        elif is_foreign_af_value(af):
            foreign_kind = "af"
        elif utm_kind == "foreign":
            foreign_kind = "utm"
    else:
        # Global domain: sk whitelist
        if is_our_sk_value(sk):
            label = "GLOBAL_DIRECT"
            is_owned = True
        elif is_foreign_sk_value(sk):
            foreign_kind = "sk"
        # af on Global domain = third-party (AliHelper does not use af Globally)
        elif af and is_aliexpress_host(url):
            foreign_kind = "af_on_global"

        # Anomaly check — EPN suffix _7685 is CIS-only; its appearance on a
        # Global AliExpress host is a data-integrity flag (routing mis-hit or
        # creative misconfiguration). Raise both af and utm_campaign carry it.
        if is_aliexpress_host(url):
            if is_our_af_value(af):
                epn_on_global = True
            cam = utm.get("utm_campaign")
            if cam and str(cam).endswith(EPN_SUFFIX):
                epn_on_global = True

    return {
        "sk": sk,
        "sk_source": sk_src,
        "af": af,
        "af_source": af_src,
        "utm": utm,
        "utm_source_tier": utm_src,
        "is_ali_ru": ali_ru,
        "label": label,
        "is_owned": is_owned,
        "foreign_kind": foreign_kind,   # None / "sk" / "af" / "af_on_global" / "utm"
        "utm_kind": utm_kind,
        "epn_on_global": epn_on_global,
    }


# ── Legacy helpers retained for callers that haven't been migrated ──────────

def is_alihelper_utm(url: str) -> bool:
    utm = {k: _parse_query(url).get(k) for k in
           ("utm_source", "utm_medium", "utm_campaign", "utm_content")}
    return classify_cis_utm(utm) == "owned_full"


def is_foreign_utm(url: str) -> bool:
    utm = {k: _parse_query(url).get(k) for k in
           ("utm_source", "utm_medium", "utm_campaign", "utm_content")}
    return classify_cis_utm(utm) == "foreign"


def has_af(url_or_query: str) -> bool:
    """True if an `af` parameter is present on the URL. (Unlike the old impl,
    this does parse from the URL itself.)"""
    if not url_or_query:
        return False
    q = _parse_query(url_or_query) if "://" in url_or_query else {}
    return bool(q.get("af"))


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
    if not url:
        return False
    return any(pat.search(url) for pat in CHECK_LIST_URLS)


def is_eligible_product_page(product_id) -> bool:
    return product_id is not None and product_id != ""


# Product page subtype classifier — one of:
#   ssr, group_deal, sale_affi, play_product, store_product,
#   short_item, item_pathed, item, other
_SUBTYPE_PATTERNS = [
    ("ssr",           re.compile(r'aliexpress\.(?:com|ru|us)/ssr/\d+/', re.I)),
    ("group_deal",    re.compile(r'group\.aliexpress\.(?:com|ru|us)/\d+-\d+-detail\.html', re.I)),
    ("sale_affi",     re.compile(r'sale\.aliexpress\.(?:com|ru|us)/[\S]+/affi\-item\.htm', re.I)),
    ("play_product",  re.compile(r'play\.aliexpress\.(?:com|ru|us)/[\S]+/productDetail\.htm', re.I)),
    ("store_product", re.compile(r'aliexpress\.(?:com|ru|us)/store/product/.*?/\d+_\d+\.html', re.I)),
    ("short_item",    re.compile(r'aliexpress\.(?:com|ru|us)/i/\d+\.html', re.I)),
    ("item_pathed",   re.compile(r'(?:aliexpress|tmall)\.(?:com|ru|us)/item/.*?/\d+\.html', re.I)),
    ("item",          re.compile(r'(?:aliexpress|tmall)\.(?:com|ru|us)/item/\d+\.html', re.I)),
]


def product_page_subtype(url: str) -> str | None:
    """Classify URL into a product-page subtype. Returns None if ineligible."""
    if not url:
        return None
    for name, pat in _SUBTYPE_PATTERNS:
        if pat.search(url):
            return name
    return None


def is_eligible(url: str, product_id, lineage_str: str) -> bool:
    """
    Eligible page check per flow.
    - auto_redirect: URL matches checkListUrls
    - dogi         : productId present
    - edge_ambiguous_build / unknown_build: not eligible for either (do not pool)
    """
    if lineage_str == "auto_redirect":
        return matches_check_list_urls(url)
    if lineage_str == "dogi":
        return is_eligible_product_page(product_id)
    return False


# ── Mixpanel DataFrame helpers ───────────────────────────────────────────────

def mp_to_df(records: list[dict]) -> pd.DataFrame:
    rows = [r.get("properties", {}) for r in records]
    return pd.DataFrame(rows)
