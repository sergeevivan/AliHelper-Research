# Problem A — Missing Affiliate Click

## Definition

Why do many AliExpress users not generate `Affiliate Click`?

This is a **separate problem** from Problem B. Do not mix.

---

## Analysis period

See [`specs/rules/analysis_periods.md`](../rules/analysis_periods.md):
- One-off investigation: `2026-03-06` → `2026-04-02` (UTC)
- Weekly pulse: rolling 7 days
- Monthly deep: rolling 28 days

---

## Data sources

Build primarily from MongoDB `events`, enriched with `clients`, optionally supported by `guestStateHistory`.
Use Mixpanel only for `Affiliate Click`.

Attribution params: follow priority table in [`specs/domain/attribution.md`](../domain/attribution.md) — `events.params` first, then `events.payload.querySk` (Global only), then parse `events.payload.url`.

---

## Units of analysis

At minimum:
- user-day
- user-window

Optionally: session-like windows if reconstructible (≥30-min gap = new session).

---

## Eligible pages (see `specs/domain/browser_flows.md` for details)

**Only product pages are eligible.** Homepages are NOT eligible for either flow.

- **DOGI flow:** product pages with `events.payload.productId` present
- **Auto-redirect flow:** URLs matching `checkListUrls` regex patterns (all product-related)

Do not include homepages, search, category, cart, or other non-product pages in the eligible denominator.

---

## Investigation goal

Determine which share of the gap is caused by:
- ineligible traffic
- client/browser flow logic
- config assignment / latest delivered hub config
- failing to reach the hub
- missing click tracking
- successful redirect with missing Mixpanel click event
- reaching the hub but not returning to AliExpress
- other causes

---

## Funnel methodology

### Global / Portals funnel

1. Raw AliExpress activity (from `events`)
2. Eligible opportunities (product pages matching flow-specific rules)
3. Eligible opportunities with usable latest config (from `guestStateHistory`)
4. Reached hub (`Affiliate Click` in Mixpanel)
5. Returned to AliExpress with owned `sk` (per attribution spec priority: `events.params.sk` → `querySk` → URL parsing)

### CIS / EPN funnel

1. Raw AliExpress activity (from `events`)
2. Eligible opportunities (product pages matching flow-specific rules)
3. Eligible opportunities with usable latest config (from `guestStateHistory`)
4. Reached hub (`Affiliate Click` in Mixpanel)
5. Returned to `aliexpress.ru` with AliHelper-owned signal:
   - Pattern A: `af=*_7685` (+ typically `utm_medium=cpa`)
   - Pattern B: `utm_source=aerkol` + `utm_medium=cpa` + `utm_campaign=*_7685`

Fallback for step 5: proxy return to `aliexpress.ru` within ≤120 seconds post-click (label as `CIS_PROXY`).

---

## Mandatory analyses

### A1. Raw vs eligible denominator
Estimate how much of the apparent gap is simply ineligible traffic.

### A2. Latest delivered config state
Estimate how often an eligible user/window had a latest prior config with usable hub assigned.

### A3. Reached hub
Estimate conversion from eligible + config-usable opportunity to `Affiliate Click`.

### A4. Post-hub return

**Global:**
Conversion from `Affiliate Click` to later AliExpress visit with our owned `sk`.

**CIS:**
Conversion from `Affiliate Click` to later visit on `aliexpress.ru` with either AliHelper-owned pattern (A or B).
Fallback: proxy return within ≤120s (label as `CIS_PROXY`).

### A5. Missing Mixpanel click tracking

**Global:**
Cases where `Affiliate Click` is missing, but later `sk` (via priority source) contains our whitelisted value.

**CIS:**
Cases where `Affiliate Click` is missing, but later event on `aliexpress.ru` contains AliHelper-owned signal (Pattern A or B).

### A6. Reached hub but no post-hub signal

**Global:**
`Affiliate Click` exists, but no later AliExpress visit with our owned `sk`.

**CIS:**
`Affiliate Click` exists, but no later visit with AliHelper-owned Pattern A/B signal AND no proxy return.

### A7. Non-activator deep-dive

Separate methodology for understanding *who* the non-activators are and *why* they may not have reached the ref link.

See [`specs/problems/problem_a_non_activator.md`](problem_a_non_activator.md) for the full spec (cohort, profile, behavior, hypotheses, tables).

This section is included in **both** weekly pulse (aggregate top-segments) and monthly deep (full version).

---

## Mandatory segmentations

- Region: Global vs CIS (by domain / actual routing)
- Browser family (from `clients.browser` UA)
- **Flow lineage**: `dogi` / `auto_redirect` / `edge_ambiguous_build` / `unknown_build` (see `specs/domain/browser_flows.md`)
- Build source: `clients.build_app` when available; fallback inferred lineage
- Country
- Hub from latest delivered config
- Extension version
- Product page subtype (standard item, SSR, store product, group deal, etc.)
- Users with multiple client states vs single client state

---

## Mandatory hypotheses to test

1. A significant share of raw traffic is not actually eligible.
2. Auto-redirect performance differs materially between Firefox and Edge (where `build_app` confirms Edge build) and Chrome-store builds.
3. DOGI flow materially underperforms or behaves differently from auto-redirect.
4. Some extension versions underperform.
5. Some assigned hubs underperform.
6. Part of the gap is missing Mixpanel click tracking (hub was reached, Mixpanel event lost).
7. For Global, part of the gap is users reaching the hub but not returning with our owned `sk`.
8. For CIS, part of the gap is users reaching the hub but not returning with our `af`/UTM pattern.
9. Geo / country segments differ materially.
10. Multi-client users perform worse than single-client users.
11. UA must be analyzed as Global/Portals, not CIS/EPN.
12. `edge_ambiguous_build` users may show a mixed profile — part DOGI-like, part auto-redirect-like. Do not pool into either flow.
