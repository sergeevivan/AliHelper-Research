# Ineligible Page Taxonomy — Follow-up Analysis
**Problem A: Raw Activity → Eligible gap**  
Window: 2026-03-06 to 2026-04-02 UTC  
Run date: 2026-04-04

---

## 0. Overview

| Metric | Count | % of raw |
|---|---:|---:|
| Total raw users | 63,916 | 100% |
| Eligible users (current logic) | 55,762 | 87.2% |
| Ineligible users | 8,154 | 12.8% |
| Reached hub (Affiliate Click) | 42,380 | 66.3% of raw |

Cache reuse: `mongo_problem_a.pkl` (user aggregates) + `aff_click_a.json` (hub reach).  
New queries: `ineligible_url_taxonomy.pkl` (3,000 top non-eligible URLs) + `first_events_a.pkl` (first event per user).

---

## 1. Critical Finding: Homepage Regex Misclassifies Query-String URLs

**The current homepage regex:**
```
^https?://[^/]*aliexpress\.[^/]*(/(#.*)?)?$
```

The trailing `$` anchor causes URLs like:
- `https://aliexpress.ru/?gatewayAdapt=glo2rus`
- `https://best.aliexpress.com/?browser_redirect=true`
- `https://es.aliexpress.com/?gatewayAdapt=glo2esp`

…to **fail** the match, because the query string `?…` follows the `/` and is not captured by `(#.*)?$`.

Per the product definition in CLAUDE.md: *"query parameters do not change homepage classification."*

**This regex is present in both `analysis_v2.py` (line 226) and `analysis_ineligible.py` (line 66).**

### Impact

In the top-3000 non-eligible URL sample:
- **166,594 event hits** with path `/` are flagged as non-eligible — nearly all of them are `aliexpress.{tld}/?...` homepages with query params.
- **17,657 hits** in `search_results` (path `/`) are similarly `{cc}.aliexpress.com/?gatewayAdapt=…` homepages.

The magnitude of the eligible undercount from this bug cannot be precisely quantified from the current pipeline (user-level aggregation would be needed), but it is likely to **reduce the 12.8% ineligible gap materially** once fixed.

**Required fix:** Strip or ignore the query string when evaluating homepage eligibility. Correct regex:
```python
# In Python
parsed = urlparse(url)
is_homepage = (
    re.match(r'^[^/]*aliexpress\.[^/]*$', parsed.netloc, re.I) and
    parsed.path in ('', '/', None)
)
```

Or in MongoDB `$regex`: use `"^https?://[^/]*aliexpress\\.[^/?]*(/(#.*)?)?(\\?.*)?$"` to allow an optional query string.

---

## 2. URL Taxonomy of Non-Eligible Pages

Non-eligible = `payload.productId` is null AND URL does not match homepage regex.

| Category | Hits | Users (sample) | % raw users | Activation priority |
|---|---:|---:|---:|---|
| feed_recommendations | 173,195 | ~33,209 | 52.0% | low |
| other (misc) | 133,910 | ~48,436 | 75.8% | low |
| search_results | 27,386 | ~20,397 | 31.9% | worth_testing |
| account_profile | 10,262 | ~3,890 | 6.1% | not_worth |
| cart | 7,812 | ~5,300 | 8.3% | **HIGH** |
| seller_store | 4,560 | ~6,017 | 9.4% | worth_testing |
| promo_landing | 2,861 | ~946 | 1.5% | worth_testing |
| order_checkout | 1,666 | ~1,858 | 2.9% | not_worth |
| help_service | 502 | ~427 | 0.7% | not_worth |
| review_rating | 380 | ~2,785 | 4.4% | low |
| category_listing | 334 | ~1,259 | 2.0% | worth_testing |

> Note: "Users (sample)" counts are from the top-3000 URL sample via `$addToSet`, not the full population. "% raw users" should be interpreted as a rough reach estimate.  
> Values > 100% in individual categories are expected — users visit multiple categories.

### Category Details

#### feed_recommendations (173,195 hits)
Top paths: `/` (166,594), `/management/feedbackBuyerList` (2,849), `/m_apps/homepage-pop/newhome` (1,635)  
Examples:
- `best.aliexpress.com/?browser_redirect=true`
- `pt.aliexpress.com/?gatewayAdapt=glo2bra`
- `aliexpress.ru/?gatewayAdapt=glo2rus`

> **166,594 of these hits are aliexpress subdomain homepages with query parameters — they are misclassified by the current regex. See §1.**

#### other / misc (133,910 hits)
Top paths: `/p/ug-login-page/login` (27,273), `/p/tracking/index` (13,182), `/minicart` (12,079), `/ssr/{id}/BundleDeals` (7,127), `/p/message/index` (4,833)  
Mix of login redirects, order tracking, minicart mini-pages, SSR bundles, messaging.

#### search_results (27,386 hits)
Top paths: `/` (17,657), `/w/{q}` (5,171), `/{q}` (2,222)  
> `/` entries here are also likely `{cc}.aliexpress.com/?gatewayAdapt=…` homepages (same regex bug). True search result paths like `/w/{query}` account for ~9,729 hits.

#### cart (7,812 hits)
All hits are `/cart` path.  
Examples: `aliexpress.ru/cart`, `aliexpress.ru/cart?spm=…`  
Pre-purchase signal — 8.3% of raw users visited cart at some point.

#### seller_store (4,560 hits)
Top: `/store/{id}` (3,043), `/user/seller/login` (1,043), `/store/{id}/pages/all-items` (326)

#### review_rating (380 hits)
All hits are `/item/{id}/reviews`.  
Product ID is embedded in the path but NOT in `payload.productId`.  
~2,785 users read reviews — potentially pre-purchase intent, product ID is derivable.

#### category_listing (334 hits)
Top: `/category/{id}/sports-entertainment`, `/category/{id}/home-garden-office`, `/category/{id}/home-appliances`

---

## 3. First Entry Point Cohort Analysis

| Cohort | Definition | Users | % all |
|---|---|---:|---:|
| C1 | First page was already eligible (product or homepage) | 26,498 | 41.5% |
| C2 | First page non-eligible, later reached eligible page | 29,264 | 45.8% |
| C3 | First page non-eligible, **never** reached eligible page | 8,154 | 12.8% |

### First Page Type Breakdown

| First page type | Users | % all | Later→ eligible | Never eligible | Hub reach |
|---|---:|---:|---:|---:|---:|
| product_page | 17,665 | 27.6% | 17,665 | 0 | 11,058 |
| feed_recommendations | 16,326 | 25.5% | 12,646 | 3,680 | 11,399 |
| other | 14,688 | 23.0% | 11,404 | 3,284 | 9,295 |
| homepage | 8,833 | 13.8% | 8,833 | 0 | 6,248 |
| search_results | 4,977 | 7.8% | 4,186 | 791 | 3,583 |
| seller_store | 398 | 0.6% | 327 | 71 | 269 |
| cart | 385 | 0.6% | 316 | 69 | 272 |
| category_listing | 278 | 0.4% | 102 | 176 | 74 |
| promo_landing | 141 | 0.2% | 102 | 39 | 67 |
| account_profile | 113 | 0.2% | 94 | 19 | 76 |
| order_checkout | 38 | 0.1% | 30 | 8 | 17 |
| review_rating | 20 | 0.0% | 18 | 2 | 13 |

**Key observations:**
- `feed_recommendations` is the #1 non-eligible first page (16,326 users = 25.5% of raw). 77.5% of them later hit eligible pages anyway.
- `category_listing` has the lowest conversion to eligibility: only 36.7% later reach an eligible page. These users browse category lists and often leave without going to a product page.
- `product_page` and `homepage` users already start eligible (100%) — no gap from entry point.
- Users who start on `cart`, `seller_store`, `search_results` mostly (80%+) later visit an eligible page.

---

## 4. Top Missed Entry Points Before Eligible Pages

Users who started non-eligible but later reached an eligible page — the "window to activate earlier":

| Page group | Users → later eligible | Priority | Reasoning |
|---|---:|---|---|
| feed_recommendations | 12,646 | low | Passive browsing / scroll feed — low purchase intent |
| other | 11,404 | low | Misc pages (login, tracking, messaging) — no clear affiliate opportunity |
| search_results | 4,186 | worth_testing | Actively shopping — may respond to activation on search results |
| seller_store | 327 | worth_testing | Store browsing — mid-funnel shopping behavior |
| **cart** | **316** | **HIGH** | Cart = strongest purchase signal — affiliate must be active before checkout |
| category_listing | 102 | worth_testing | Browsing categories — high purchase-intent path |
| promo_landing | 102 | worth_testing | Deals/promotions pages — often pre-purchase intent |
| account_profile | 94 | not_worth | Account management — no shopping context |
| order_checkout | 30 | not_worth | Post-purchase or checkout — too late or excluded by noLogUrls |
| review_rating | 18 | low | Reading reviews — some pre-purchase intent but low volume |
| help_service | 5 | not_worth | Support pages — no affiliate opportunity |

---

## 5. Recommendations

### Priority 1 — Fix the Homepage Regex (Quick Win, HIGH impact)

Update the homepage eligibility check to **ignore query strings** when testing for AliExpress homepages.

Fix in both `analysis_v2.py` (line 226) and any production eligibility logic:
```python
# Correct: strip query string before regex match
def is_homepage(url: str) -> bool:
    p = urlparse(url)
    return bool(
        re.match(r'^[^/]*aliexpress\.[^/?]+$', p.netloc, re.I) and
        p.path in ('', '/')
    )
```

For MongoDB aggregation pipeline, use:
```js
"payload.url": { "$regex": "^https?://[^/]*aliexpress\\.[^/?]*(/(#*)?)?(\\?.*)?$" }
```

**Expected outcome:** The 12.8% ineligible gap will decrease — many of the 8,154 "ineligible" users likely visited homepages with query parameters. Rerunning `analysis_v2.py` after this fix will give an accurate eligible denominator.

### Priority 2 — Activate on Cart Pages (HIGH business priority)

316 users started their session on the cart page, eventually reached a product/homepage, then may have purchased — but affiliate may not have been activated in time.

More critically: users in the 72h purchase window may have arrived at cart **without** ever hitting a product or homepage (if they had a direct cart link). These are exactly the `NO_OUR_SK_IN_72H` cases in Problem B.

**Recommendation:** Add cart page (`/cart`, `/cart?*`) to the eligible page list for auto-redirect. The redirect would fire before cart content loads (on `webNavigation.onBeforeNavigate`), same as product/homepage flow.

Caveat: verify that AliExpress cart pages are not excluded by `noLogUrls` in extension config. If they are, client-side events won't be stored either, so this must be coordinated with config changes.

### Priority 3 — Expand to Search Result Pages (worth_testing)

4,186 users started on search results before reaching a product page. Activating affiliate on `/w/{query}` pages would capture this pre-click moment.

For auto-redirect browsers (Firefox/Edge), this would fire before the search page loads.  
For DOGI flow, a DOGI trigger on search results would require UI design work.

**Recommendation:** A/B test eligibility expansion to search result pages for Firefox/Edge auto-redirect first (lower complexity, no UI needed).

### Priority 4 — Expand to Seller Store Pages (worth_testing)

327 users started on `/store/{id}` pages. These are active shoppers browsing a specific seller. Adding store pages to eligibility would capture this mid-funnel moment.

### Priority 5 — Review Rating Pages (tracking improvement)

380 hits to `/item/{id}/reviews` are currently non-eligible because `payload.productId` is null. However, **the product ID is embedded in the URL path** (`/item/{PRODUCT_ID}/reviews`).

**Recommendation:** Parse product ID from the URL path as a fallback when `payload.productId` is null. This would reclassify review pages as eligible product-page variants.

Implementation (Python):
```python
import re
REVIEW_RE = re.compile(r'/item/(\d+)/reviews', re.I)
m = REVIEW_RE.search(url)
if m:
    product_id = m.group(1)  # treat as product page
```

### Priority 6 — Category Listing Pages (worth_testing, with caveat)

Only 36.7% of users who first landed on category pages later hit an eligible page — the lowest conversion rate of all page types. This suggests category listings are often a dead-end browsing session.

However, category pages ARE mid-funnel (the user is actively browsing for something). Activating affiliate early here would help capture users who jump directly from category to checkout without visiting a product page detail.

**Recommendation:** Add to eligible pages, but monitor session conversion rate to ensure affiliate activation doesn't interfere with the browsing UX.

---

## 6. Data Caveats

1. **URL sample bias:** The taxonomy is based on the top 3,000 URLs by hit count. Long-tail non-eligible pages are not captured. The hit counts and user counts for rare pages are undercounted.
2. **User count estimates:** `$addToSet` within the aggregation pipeline produces deduplicated user counts per URL, not per category. Category-level user counts are approximate.
3. **`noLogUrls` exclusion:** Some AliExpress pages (checkout, payment) may be excluded from `events` logging by config-level `noLogUrls` rules. Absence of checkout/order events is expected and does not represent these pages being truly non-existent in user sessions.
4. **Homepage regex misclassification (§1):** The eligible user count in `analysis_v2.py` is understated due to this bug. All conclusions about the "ineligible 12.8%" should be re-evaluated after fixing the regex.
5. **First-event pipeline:** The first event per user is the chronologically first event in the `events` collection (by `_id` sort, proxy for time). If a user had activity just before the window start, their "first event" in window may not reflect their true first interaction.
6. **CIS observability:** For CIS users, the ineligible taxonomy is based on `events`, which does not contain `utm_*` parameters. Return-to-site signals after hub reach are not directly observable.

---

## 7. Output Files

| File | Contents |
|---|---|
| `ineligible_taxonomy.csv` | URL category taxonomy: hits, users, top patterns, examples, priority |
| `first_entry_taxonomy.csv` | First entry point by type: users, cohort assignment, hub reach |
| `ineligible_analysis_report.md` | This report |
| `/tmp/ineligible_output.txt` | Full console output with per-category examples |
