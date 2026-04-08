"""
Shared utility functions: formatting, browser classification, URL normalization.
"""

import re
from datetime import datetime
from urllib.parse import urlparse

import pandas as pd
from bson import ObjectId

from src.config import AUTO_REDIRECT_BROWSERS, CIS_COUNTRIES


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


# ── Mixpanel DataFrame helpers ───────────────────────────────────────────────

def to_df(records: list[dict]) -> pd.DataFrame:
    """Flatten Mixpanel NDJSON records to DataFrame."""
    rows = [r.get("properties", {}) for r in records]
    return pd.DataFrame(rows)


# ── URL normalization & classification ───────────────────────────────────────

HOMEPAGE_RE = re.compile(r'^https?://[^/]*aliexpress\.[^/]*(/(#.*)?)?$', re.I)


def is_homepage(url: str) -> bool:
    """Check if URL is an AliExpress homepage."""
    if not url:
        return False
    return bool(HOMEPAGE_RE.match(url))


def is_product_page(product_id) -> bool:
    """Check if event represents a product page visit."""
    return product_id is not None and product_id != ""


def normalize_url(url: str) -> tuple[str, str]:
    """
    Classify an AliExpress URL into a page category.

    Returns:
        (category, normalized_path) where category is one of:
        search_results, category_listing, cart, order_checkout,
        seller_store, promo_landing, feed_recommendations,
        account_profile, help_service, brand_collection,
        review_rating, homepage_variant, other, unknown
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

    def clean_path(path_str):
        s = re.sub(r'/\d{6,}', '/{id}', path_str)
        s = re.sub(r'/(item|product|store|category)/\d+', r'/\1/{id}', s, flags=re.I)
        s = re.sub(r'\.htm(l)?$', '', s, flags=re.I)
        s = s.rstrip('/')
        return s or '/'

    path_norm = clean_path(path)
    path_lower = path.lower()
    qs_lower = qs.lower()

    if any(['/search' in path_lower, 'searchtext=' in qs_lower,
            's.aliexpress.' in host, path_lower.startswith('/wholesale'),
            '/search/' in path_lower]):
        return ("search_results", clean_path(
            re.sub(r'[^/]+', lambda m: '{q}' if len(m.group()) > 6 else m.group(), path_norm)))

    if any(['/category/' in path_lower,
            re.match(r'^/[a-z0-9-]+-cat-\d', path_lower),
            '/all-wholesale-' in path_lower, path_lower.startswith('/categories'),
            '/browse/' in path_lower, '/tag/' in path_lower]):
        return ("category_listing", path_norm[:80])

    if any(['shoppingcart' in path_lower, '/cart' in path_lower, '/basket' in path_lower]):
        return ("cart", path_norm[:80])

    if any(['/orderlist' in path_lower, '/order/' in path_lower, '/orders/' in path_lower,
            '/trade/' in path_lower, '/pay/' in path_lower, '/checkout' in path_lower,
            '/confirm_order' in path_lower, '/payment' in path_lower,
            'order_confirm' in path_lower,
            '/purchase' in path_lower.replace('?', '').replace('#', '')]):
        return ("order_checkout", path_norm[:80])

    if any(['/store/' in path_lower, path_lower.startswith('/store'),
            '/seller/' in path_lower, '/shop/' in path_lower,
            re.search(r'/[a-z0-9-]+-store-\d', path_lower)]):
        return ("seller_store", clean_path(re.sub(r'/store/\d+', '/store/{id}', path)))

    if any(['/gcp/' in path_lower, '/promotion/' in path_lower, '/deals/' in path_lower,
            '/promo/' in path_lower, '/sale/' in path_lower, '/event/' in path_lower,
            '/campaign/' in path_lower, '/hotproducts' in path_lower,
            '/hot-products' in path_lower, '/flash_deals' in path_lower,
            '/flashdeals' in path_lower, '/coupon' in path_lower,
            '/top-picks' in path_lower, '/landing' in path_lower]):
        return ("promo_landing", path_norm[:80])

    if any([path_lower in ('/', ''), '/home' in path_lower, '/feed' in path_lower,
            '/recommend' in path_lower, '/discovery' in path_lower,
            '/newuser' in path_lower, '/new-user' in path_lower,
            '/just4u' in path_lower, 'just-for-you' in path_lower,
            '/stream' in path_lower,
            '/video' in path_lower and '/product' not in path_lower]):
        return ("feed_recommendations", path_norm[:80])

    if any(['/account' in path_lower, '/myprofile' in path_lower,
            '/mypurse' in path_lower, '/myfollowing' in path_lower,
            '/myfavorites' in path_lower, '/mywishlist' in path_lower,
            '/personal-info' in path_lower, '/member/overview' in path_lower,
            path_lower.startswith('/usercenter')]):
        return ("account_profile", path_norm[:80])

    if any(['/help' in path_lower, '/service' in path_lower,
            '/dispute' in path_lower, '/refund' in path_lower,
            '/after-sale' in path_lower, '/complaint' in path_lower,
            '/contact' in path_lower, '/feedback' in path_lower,
            '/buynow' in path_lower]):
        return ("help_service", path_norm[:80])

    if any(['/brand/' in path_lower, '/collection/' in path_lower,
            '/handpick' in path_lower, '/topic/' in path_lower,
            '/list/' in path_lower]):
        return ("brand_collection", path_norm[:80])

    if any(['/review' in path_lower, '/rating' in path_lower,
            '/feedback' in path_lower]):
        return ("review_rating", path_norm[:80])

    if path in ('/', ''):
        return ("homepage_variant", path_norm[:80])

    return ("other", path_norm[:80])
