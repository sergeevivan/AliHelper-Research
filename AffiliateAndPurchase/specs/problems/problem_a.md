# Problem A — Missing Affiliate Click

## Definition

Why do many AliExpress users not generate `Affiliate Click`?

This is a **separate problem** from Problem B. Do not mix.

---

## Analysis period

Last 28 complete UTC days, excluding current incomplete day:
- `2026-03-06 00:00:00 UTC` to `2026-04-02 23:59:59 UTC`

---

## Data sources

Build primarily from MongoDB `events`, enriched with `clients`, optionally supported by `guestStateHistory`.
Use Mixpanel only for `Affiliate Click`.

---

## Units of analysis

At minimum:
- user-day
- user-window

Optionally: session-like windows if reconstructible.

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
5. Returned to AliExpress with owned `sk` (from `events.payload.querySk`)

### CIS / EPN funnel

1. Raw AliExpress activity (from `events`)
2. Eligible opportunities (product pages matching flow-specific rules)
3. Eligible opportunities with usable latest config (from `guestStateHistory`)
4. Reached hub (`Affiliate Click` in Mixpanel)
5. Returned to `aliexpress.ru` with AliHelper-owned UTM params (from `events.payload.url`: `utm_source=aerkol` + `utm_medium=cpa` + `utm_campaign=*_7685`)

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
Conversion from `Affiliate Click` to later visit on `aliexpress.ru` with AliHelper-owned UTM params.
Fallback: proxy return within ≤120s (label as `CIS_PROXY`).

### A5. Missing Mixpanel click tracking

**Global:**
Cases where `Affiliate Click` is missing, but later `events.payload.querySk` contains our whitelisted `sk`.

**CIS:**
Cases where `Affiliate Click` is missing, but later `events.payload.url` contains AliHelper-owned UTM params.

### A6. Reached hub but no post-hub signal

**Global:**
`Affiliate Click` exists, but no later AliExpress visit with our owned `sk`.

**CIS:**
`Affiliate Click` exists, but no later visit with AliHelper-owned UTM params AND no proxy return.

---

## Mandatory segmentations

- Region: Global vs CIS (by actual routing)
- Browser family
- Auto-redirect vs DOGI lineage
- Extension/store lineage
- Country
- Hub from latest delivered config
- Extension version
- Product page subtype (standard item, SSR, store product, group deal, etc.)
- Users with multiple client states vs single client state

---

## Mandatory hypotheses to test

1. A significant share of raw traffic is not actually eligible.
2. Auto-redirect performance differs materially between Firefox and Edge.
3. DOGI flow materially underperforms or behaves differently from auto-redirect.
4. Some extension versions underperform.
5. Some assigned hubs underperform.
6. Part of the gap is missing Mixpanel click tracking (hub was reached, Mixpanel event lost).
7. For Global, part of the gap is users reaching the hub but not returning with our owned `sk`.
8. For CIS, part of the gap is users reaching the hub but not returning with our UTM params.
9. Geo / country segments differ materially.
10. Multi-client users perform worse than single-client users.
11. UA must be analyzed as Global/Portals, not CIS/EPN.
