# AliHelper — Root-Cause Research: Missing Affiliate Activation and Missing Purchase Attribution

## Role
Act as a senior product/data investigator.

Your job is not to provide generic hypotheses. Your job is to run a reproducible investigation using MongoDB + Mixpanel and determine the real causes behind two separate problems:

1. why a large share of AliExpress users do not generate `Affiliate Click`;
2. why `Purchase Completed` is higher than commission-bearing `Purchase`.

You must produce:
- clear definitions,
- reproducible code,
- segmentation,
- ranked root causes,
- impact estimates,
- unexplained remainder,
- concrete product / engineering / tracking recommendations,
- a rebuilt HTML report.

---

## Important: there are TWO different problems

Do NOT mix them.

### Problem A — missing affiliate activation
Why do many AliExpress users not generate `Affiliate Click`?

### Problem B — missing purchase attribution
Why do we see more `Purchase Completed` than `Purchase`?

These are different problems and require different methodologies.

---

## Critical correction to previous methodology

The previous report must be partially invalidated and reworked.

We missed a crucial regional attribution difference.

### Global / Portals
For Global traffic, AliHelper attribution is directly observable in MongoDB `events` through `querySk` and related affiliate params.

AliHelper-owned Global markers:
- `_c36PoUEj`
- `_d6jWDbY`
- `_AnTGXs`
- `_olPBn9X`
- `_dVh6yw5`

For Global traffic:
- owned `sk` = direct evidence of AliHelper affiliate return
- foreign `sk` / `af` = possible overwrite evidence

### CIS / EPN
For CIS traffic, affiliate return works differently.

CIS users can be returned to `aliexpress.ru` with URLs like:
- `utm_source=aerkol`
- `utm_medium=cpa`
- `utm_campaign=<creative_id>_7685`

Known AliHelper EPN account id:
- `7685`

The prefix before `_7685` should be treated as a candidate creative/link id, but not as a mandatory stable identifier unless validated.

### Critical limitation
MongoDB `events` does NOT currently store `utm_source`, `utm_medium`, or `utm_campaign`.

Therefore for CIS historical data:
- there is NO direct observability of EPN affiliate return in `events`
- there is NO direct observability of EPN overwrite in `events`
- there is NO basis to conclude that “hub reached but no `sk`” means failed return for CIS
- there is NO basis to classify CIS purchases into `NO_OUR_SK_IN_72H` using Global-style `sk` logic

Any previous CIS conclusion that depended on observing owned `sk` in MongoDB `events` must be treated as invalid and reworked under a limited-observability framework.

---

## Regional split rule

Do not classify countries by geopolitical CIS grouping.
Classify them by actual AliExpress affiliate routing.

### Global / Portals countries
These countries use Portals-style return markers and direct observability via `sk`.

Important:
- `UA` must be treated as `Global / Portals`, not `CIS / EPN`
- any previous UA result based on CIS proxy logic is invalid and must be recomputed under Global direct logic

### CIS / EPN countries
Only countries that actually use the EPN-style affiliate return logic should be treated as CIS/EPN.

Use this EPN country list unless explicitly overridden by stronger routing evidence:
- RU
- AZ
- AM
- BY
- GE
- KZ
- KG
- MD
- TJ
- TM
- UZ

If routing evidence and country grouping disagree, routing evidence wins.

---

## Required analysis labels

Every finding in the updated report must be labeled as one of:
- `GLOBAL_DIRECT`
- `CIS_PROXY`
- `NOT_OBSERVABLE_WITH_CURRENT_DATA`

Do not present CIS proxy findings with the same confidence as Global direct findings.

---

## Analysis periods

Use different analysis periods for the two problems.

### Problem A — Missing Affiliate Click
Use the last 28 complete UTC days, excluding the current incomplete day.

Use:
- `2026-03-06 00:00:00 UTC` to `2026-04-02 23:59:59 UTC`

### Problem B — Purchase Completed without Purchase
Use a mature cohort of 28 complete UTC days, but exclude the most recent 7 days to avoid confusing true attribution loss with delayed postbacks or late data arrival.

Use `Purchase Completed` records from:
- `2026-02-27 00:00:00 UTC` to `2026-03-26 23:59:59 UTC`

For each `Purchase Completed` in Problem B:
- reconstruct the prior 72-hour attribution window from MongoDB `events`;
- match to `Purchase` primarily by user and time proximity using the 10-minute matching window.

---

## Known incident window

There was a postback issue affecting CIS users on:
- `2026-04-01`

Orders/leads were later backfilled during the same day or the following day.

Implications:
- do not use `2026-04-01` as a clean baseline day for Problem B
- if any validation analysis touches `2026-04-01` or `2026-04-02`, treat them as incident dates for CIS users
- analyze CIS and Global separately for any validation that includes those dates
- do not interpret abnormal CIS `Purchase Completed -> Purchase` gaps on those dates as normal attribution loss without explicit incident adjustment

Note:
the primary Problem B analysis window above already avoids this incident period.

---

## Cache reuse requirement

Do NOT discard existing cache unless necessary. Treat this as a methodology correction, not a full data reset.

Reuse previously extracted raw datasets wherever possible:
- MongoDB `events`
- Mixpanel `Affiliate Click`
- Mixpanel `Purchase`
- Mixpanel `Purchase Completed`
- `guestStateHistory`
- any already prepared intermediate extracts

Recompute only the derived layers that depended on incorrect CIS `sk`-based assumptions or the incorrect UA region split.

Before running, briefly state:
- which cached artifacts can be reused
- which tables/aggregations must be recomputed
- which data, if any, must be re-extracted from MongoDB or Mixpanel

---

## Core investigation goals

### For Problem A
Determine which share of the gap is caused by:
- ineligible traffic
- client/browser flow logic
- config assignment / latest delivered hub config
- failing to reach the hub
- missing click tracking
- successful redirect with missing Mixpanel click event
- reaching the hub but not returning to AliExpress
- other causes

### For Problem B
Determine which share of the gap is caused by:
- no valid AliHelper affiliate state before purchase
- overwrite by another affiliate source
- cashback interference
- timing/matching issues
- delayed postback
- tracking mismatch
- partner-program limitations
- other causes

---

## Hard rules you must follow

### Identity
Canonical user identity is:
- `guests._id`

This same user identifier is used in Mixpanel as:
- `$user_id`

Use:
- `events.guest_id` -> `guests._id`
- `clients.guest_id` -> `guests._id`
- `guestStateHistory.guest_id` -> `guests._id`

Do NOT use `clients._id` as a user identifier.

---

## Source-of-truth rules

### Primary behavioral source
Use MongoDB `events` as the source of truth for AliExpress browsing behavior.

Reason:
every AliExpress page visit is stored in MongoDB `events`, while Mixpanel only stores selected higher-level events.

Use `events` for:
- user browsing activity reconstruction
- page-type reconstruction
- homepage/product-page eligibility
- raw exposure counts
- affiliate-state reconstruction where observable
- detecting return to AliExpress
- reconstructing state in the 72-hour pre-purchase window where observable

### Mixpanel source of truth
Use Mixpanel only for:
- `Affiliate Click`
- `Purchase`
- `Purchase Completed`

### Daily AliExpress activity aggregate
Mixpanel also contains a daily aggregated event `AliExpress Activity`, but it is NOT the source of truth for behavioral root-cause analysis.
Use it only for high-level sanity checks if needed.

### Client enrichment
Use MongoDB `clients` only to enrich `events` with:
- browser
- user_agent
- os
- city
- country
- client_version
- IP context

Treat `clients` as client-state history, not as a canonical user table.

### Config delivery history
Use MongoDB `guestStateHistory` as a supporting source of client config snapshots.

Interpretation:
- each record means the client requested and received a fresh config
- `domain` is the hub assigned in that config snapshot
- `value=true` means the config included a usable hub
- `value=false` means the config did not include a usable hub

Important:
- `guestStateHistory` is NOT a history of actual redirect usage
- it is NOT proof that the user used that hub
- it is NOT proof that redirect happened
- it is only evidence of which config/hub was last delivered to the client around a given time

Use it mainly to infer:
- what hub the client would most likely use at that moment
- whether the latest known config before an eligible visit contained a usable hub
- whether config delivery differed by country / browser / version / time

When using `guestStateHistory`, always match the latest config snapshot BEFORE the analyzed event/window.

Do not use arbitrary records for the same user.
Because active users may fetch config many times per day, the correct interpretation is:
- latest prior config snapshot = best estimate of client-side hub assignment at that moment

---

## Attribution rules

Do NOT use one unified attribution model for all traffic.

### Global / Portals attribution
For Global traffic, AliHelper attribution must be identified primarily via `sk`, not `af`.

#### AliHelper-owned Global `sk` whitelist
- `_c36PoUEj`
- `_d6jWDbY`
- `_AnTGXs`
- `_olPBn9X`
- `_dVh6yw5`

#### Global interpretation
- whitelisted `sk` = AliHelper-owned affiliate state
- non-whitelisted `sk` = likely third-party affiliate state
- `af` should generally be treated as third-party affiliate evidence, because AliHelper usually does not use `af` in its own links

### CIS / EPN attribution
For CIS traffic, direct affiliate return in historical `events` is NOT observable, because `utm_*` params are not stored.

Known AliHelper EPN pattern:
- `utm_source=aerkol`
- `utm_medium=cpa`
- `utm_campaign` ending in `_7685`

This pattern can be used for future instrumentation design, but NOT for direct reconstruction from historical `events`.

### Attribution window
Default attribution window:
- 72 hours before `Purchase Completed`

### Probable overwrite
For Global:
- we had an AliHelper-owned `sk`
- then before `Purchase Completed` within the 72-hour window there was:
  - a later foreign `sk`
  - or `af`
  - or evidence of cashback interference
  - or another conflicting affiliate state

For CIS:
- direct overwrite detection is NOT observable with current historical `events`

---

## Browser / store logic

### Auto-redirect browsers
Auto-redirect is used only for:
- Firefox
- Edge

Mechanism:
- before page content loads
- on `webNavigation.onBeforeNavigate`
- if current URL matches eligible page patterns
- if 30 minutes passed since the last affiliate activation attempt
- and if cashback cooldown allows
- the client redirects the user to the hub

### DOGI flow browsers
DOGI-triggered flow is used for:
- Chrome
- Yandex
- Opera
- other Chrome-like browsers

Mechanism:
- affiliate redirect happens through interaction with DOGI coin / product thumbnail flow

### Important methodological consequence
Problem A must be segmented at least by:
- Firefox/Edge auto-redirect lineage
- Chrome-like DOGI lineage

Do not pool them as one mechanism.

---

## Eligible opportunity rules

An AliExpress visit is eligible for affiliate activation only if page type and flow rules allow it.

### Product page
If `payload.productId` is present:
- treat it as a product page

### Homepage
Homepage is:
- empty pathname on any AliExpress host/subdomain
- query parameters do not change homepage classification

### Eligible pages
For affiliate activation analysis, eligible pages are:
- product pages
- homepages on any AliExpress subdomain

### Ineligible pages
Do not include obviously ineligible page visits in the denominator.

Also be careful:
some paths may be excluded from logging by config-level URL exclusions (`noLogUrls`), so absence of `events` near checkout/order flow is not always evidence of no user activity.

---

## Technical observable signals

### Affiliate Click
`Affiliate Click` means:
- the user reached the hub

Important:
if the user did NOT reach the hub, `Affiliate Click` will not be sent.

### Global successful return to AliExpress
For Global traffic, successful landing back on AliExpress should usually be observable in MongoDB `events` as:
- `events.payload.querySk` containing at least `sk`
- ideally one of our whitelisted AliHelper `sk` values

### CIS return proxy
For CIS traffic, direct affiliate return observability is missing.
The best available historical proxy is:
- user had `Affiliate Click` (reached hub)
- followed shortly by a visit in MongoDB `events` to `aliexpress.ru`
- within a short post-click window, for example `<= 120 seconds`
- ideally with matching product/page context if reconstructible:
  - same `payload.productId`
  - or same normalized product URL / item id

This is only a proxy for return-to-site behavior.
It is NOT direct proof that the user returned with AliHelper-owned EPN parameters.

### Auto-redirect attempt observability limitation
There is no direct backend log of client-side auto-redirect attempts.
Therefore expected auto-redirect opportunities must be reconstructed indirectly from:
- eligible page visits in `events`
- browser lineage
- 30-minute rule
- latest prior config snapshot in `guestStateHistory`
- and later evidence such as `Affiliate Click` and/or return signals

---

## Cashback limitation
Cashback-site visits are tracked only in client local storage and are NOT logged to the backend.

Therefore:
- do not assume full observability of cashback interference in raw logs
- use available cashback traces from `Purchase Completed` as partial evidence
- treat cashback-related explanations as partially observable and quantify uncertainty

---

## Time handling

- MongoDB `events.created` is stored in UTC
- MongoDB ObjectId timestamps are UTC-derived
- Mixpanel project timezone is `Europe/Moscow`

Be explicit about timezone conversions whenever matching MongoDB behavior to Mixpanel events.

---

## Purchase matching rules

Do NOT rely primarily on `order_id`, because `Purchase Completed` often does not contain it.

Primary matching approach:
- match by canonical user identity (`guests._id`)
- and time proximity

Default matching tolerance window:
- up to 10 minutes

You must run sensitivity checks if needed:
- narrower window
- wider window

Document how ambiguous matches are handled.

---

## Required definitions to lock before analysis

Before starting, explicitly define and document:

1. Canonical user identity
2. Global direct affiliate state
3. CIS limited-observability state
4. Eligible opportunity
5. Attribution window
6. Client enrichment rule
7. Global overwrite rule
8. Mature purchase cohort
9. Purchase matching rule
10. Global direct-return evidence rule
11. CIS proxy-return rule
12. Latest delivered config rule
13. Routing-based regional split rule

---

## Problem A methodology — Missing Affiliate Click

Build this analysis primarily from MongoDB `events`, enriched with `clients` and optionally supported by `guestStateHistory`.

### Units of analysis
Use at least:
- user-day
- user-window

Optionally:
- session-like windows if reconstructible

### Required methodological split

#### Global / Portals
Use the direct funnel:
1. Raw AliExpress activity
2. Eligible opportunities
3. Eligible opportunities with usable latest config
4. Reached hub (`Affiliate Click`)
5. Returned to AliExpress with owned `sk`

#### CIS / EPN
Use the limited-observability funnel:
1. Raw AliExpress activity
2. Eligible opportunities
3. Eligible opportunities with usable latest config
4. Reached hub (`Affiliate Click`)
5. Proxy return to `aliexpress.ru` within the short post-click window

For CIS explicitly report:
- direct affiliate return observability = NO
- proxy site-return observability = YES (limited)
- any conclusion about “affiliate params preserved” = `NOT_OBSERVABLE_WITH_CURRENT_DATA`

### Mandatory analyses for Problem A

#### A1. Raw vs eligible denominator
Estimate how much of the apparent gap is simply ineligible traffic.

#### A2. Latest delivered config state
Estimate how often an eligible user/window had a latest prior config with usable hub assigned.

#### A3. Reached hub
Estimate conversion from eligible / latest-config-usable opportunity to `Affiliate Click`.

#### A4. Post-hub return
For Global:
- estimate conversion from `Affiliate Click` to later AliExpress visit with our owned `sk`

For CIS:
- estimate conversion from `Affiliate Click` to proxy return to `aliexpress.ru`

#### A5. Missing Mixpanel click tracking
For Global:
- explicitly estimate cases where `Affiliate Click` is missing, but later `events.payload.querySk` contains our whitelisted `sk`

For CIS:
- do not create a false direct-marker equivalent if none exists

#### A6. Reached hub but no post-hub signal
For Global:
- `Affiliate Click` exists, but there is no later AliExpress visit with our owned `sk`

For CIS:
- `Affiliate Click` exists, but there is no proxy return to `aliexpress.ru`

Do not interpret the CIS version as failed affiliate-param preservation.
Interpret it only as absence of observable site-return proxy.

### Mandatory segmentations for Problem A
At minimum segment by:
- region split based on actual routing: Global vs CIS
- browser family
- auto-redirect vs DOGI lineage
- extension/store lineage
- country
- hub from latest delivered config
- extension version
- homepage vs product page
- users with multiple client states vs single client state

### Mandatory hypotheses for Problem A
Test all of these:

1. A significant share of raw traffic is not actually eligible.
2. Auto-redirect performance differs materially between Firefox and Edge.
3. DOGI flow materially underperforms or behaves differently from auto-redirect flow.
4. Some extension versions underperform.
5. Some assigned hubs underperform.
6. Part of the gap is missing Mixpanel click tracking.
7. For Global, part of the gap is users reaching the hub but not returning with our owned `sk`.
8. For CIS, part of the gap is users reaching the hub but not showing any proxy return to `aliexpress.ru`.
9. Geo / country segments differ materially.
10. Multi-client users perform worse than single-client users.
11. UA must be analyzed as Global / Portals, not CIS / EPN.

---

## Problem B methodology — Purchase Completed without Purchase

Build the analysis primarily from:
- Mixpanel `Purchase Completed`
- Mixpanel `Purchase`
- MongoDB `events` for 72-hour pre-purchase reconstruction where observable
- optionally `clients` for enrichment

### Mature cohort rule
Use only mature `Purchase Completed` cohorts old enough that postbacks should normally have arrived.
Do NOT interpret very fresh gaps as true attribution loss.

### Required methodological split

#### Global / Portals
Keep the direct 72-hour attribution-state reconstruction using owned vs foreign `sk` and `af`.

#### CIS / EPN
Do NOT claim direct reconstruction of EPN affiliate state from `events`.
Do NOT claim direct overwrite detection from `events`.

Instead use only weaker observable signals:
- `Affiliate Click`
- proxy return after click
- `Purchase Completed`
- `Purchase`

### Required reconstruction per Purchase Completed

#### Global
For each `Purchase Completed`, reconstruct the prior 72-hour window and determine:
1. whether AliHelper-owned `sk` was seen
2. when it was last seen
3. whether a later foreign `sk` appeared
4. whether `af` appeared
5. whether there are cashback traces in available data
6. whether purchase later matched to `Purchase` within the 10-minute matching window
7. whether absence of `Purchase` is likely delayed postback, overwrite, missing valid attribution, tracking mismatch, or unknown

#### CIS
For each `Purchase Completed`, determine only the weaker observable signals:
1. whether there was a prior `Affiliate Click`
2. whether there was a proxy return to `aliexpress.ru` after click
3. whether there was `Purchase`
4. whether absence of `Purchase` is likely delayed postback, limited observability, missing hub reach, or unknown

### Required primary reason codes

#### Global reason codes
- `NO_OUR_SK_IN_72H`
- `FOREIGN_SK_AFTER_OUR_SK`
- `AF_AFTER_OUR_SK`
- `CASHBACK_TRACE`
- `LIKELY_DELAYED_POSTBACK`
- `TRACKING_MISMATCH`
- `PARTNER_RULE_EXCLUSION`
- `UNKNOWN`

#### CIS reason codes
- `CIS_NO_DIRECT_ATTRIBUTION_OBSERVABILITY`
- `CIS_NO_HUB_REACH_OBSERVED`
- `CIS_HUB_REACHED_NO_PROXY_RETURN`
- `CIS_PROXY_RETURN_OBSERVED`
- `CIS_PURCHASE_COMPLETED_WITHOUT_PURCHASE_UNDER_LIMITED_OBSERVABILITY`
- `CIS_LIKELY_DELAYED_POSTBACK`
- `CIS_UNKNOWN`

Do not force CIS cases into Global-style `sk`-based buckets.

### Mandatory analyses for Problem B

#### B1. Presence of valid attribution evidence
For Global:
- how many `Purchase Completed` cases had our `sk` in the prior 72 hours?

For CIS:
- how many `Purchase Completed` cases had prior hub reach and/or proxy return evidence?

#### B2. Overwrite analysis
For Global:
- among cases with our prior `sk`, how many later showed foreign `sk`, `af`, or conflicting affiliate evidence?

For CIS:
- direct overwrite analysis = `NOT_OBSERVABLE_WITH_CURRENT_DATA`

#### B3. Delayed postback analysis
Quantify how much of the gap may be explained by delayed `Purchase`.

#### B4. Matching stability
Run sensitivity checks around the 10-minute matching window.

#### B5. Segment-level loss rate
Compare missing `Purchase` rate by:
- region split based on actual routing: Global vs CIS
- browser family
- auto-redirect vs DOGI lineage
- country
- hub from latest delivered config before the relevant window
- version
- category
- new buyer
- hot product
- multi-client vs single-client user

### Mandatory hypotheses for Problem B
Test all of these:

1. A material share of missing commissions is explained by last-click overwrite for Global traffic.
2. Foreign `sk` is the strongest overwrite signal for Global traffic.
3. `af` is a useful third-party overwrite marker for Global traffic.
4. Cashback explains part of the gap, but observability is partial.
5. Some of the gap is delayed postback, not true commission loss.
6. Some Global `Purchase Completed` cases never had valid AliHelper-owned `sk` within 72 hours.
7. CIS cases cannot be directly interpreted with Global attribution logic.
8. Loss rate differs materially by browser / store lineage / version / geo / assigned hub.
9. Some discrepancy is tracking mismatch rather than partner non-crediting.
10. UA must be classified and analyzed as Global, including direct `sk` logic.

---

## Data quality / observability caveats you must report

You must explicitly report limitations such as:
- partial cashback observability
- missing `order_id` in `Purchase Completed`
- possible gaps caused by excluded `noLogUrls`
- uncertainty from time-based matching
- uncertainty from client enrichment
- lack of direct backend logging for auto-redirect attempts
- `guestStateHistory` representing config delivery rather than actual usage
- lack of direct EPN/CIS affiliate-marker observability in historical `events`
- any Mongo query limitations caused by indexes
- any prior misclassification of countries by geopolitical grouping instead of routing logic

---

## Expected outputs

### 1. What changed from the previous report
At the top of the new report, explicitly state:
- which previous CIS conclusions were invalidated
- why they were invalidated
- which previous UA conclusions were invalidated
- why they were invalidated
- what new methodology replaced them

### 2. Definitions locked
State all definitions before analysis.

### 3. Data quality caveats
List risks and limitations.

### 4. Findings for Problem A
With decomposition tables, segmentation, and explicit region labels.

### 5. Findings for Problem B
With decomposition tables, reason-code classification, and explicit region labels.

### 6. Ranked root causes by impact
For each cause provide:
- explanation
- how it was measured
- affected share
- estimated impact
- confidence
- observability label
- recommended fix

### 7. Unexplained remainder
Explicitly show what remains unexplained after all classification.

### 8. Recommended fixes
Split into:
- quick wins
- medium-term product changes
- tracking/instrumentation changes
- follow-up experiments/analyses

### 9. Reproducible code
Provide a reproducible pipeline:
- extraction
- transformation
- matching
- enrichment
- classification
- final metrics/tables

### 10. Rebuilt HTML report
Rebuild the HTML report from scratch using the corrected methodology.

---

## Instrumentation recommendations

Explicitly recommend that future logging should store at least:
- `utm_source`
- `utm_medium`
- `utm_campaign`

for AliExpress return visits, especially on `aliexpress.ru`.

Preferably normalize them into:
- `affiliate_provider`
- `affiliate_marker_type`
- `affiliate_account_id`
- `affiliate_creative_id`
- `is_alihelper_owned_affiliate_marker`

For CIS/EPN, AliHelper-owned markers should be derivable from:
- `utm_source=aerkol`
- `utm_medium=cpa`
- `utm_campaign` ending in `_7685`

Also recommend adding either:
- a dedicated `Affiliate Return Detected` client-side event

or at minimum persisting/reporting:
- `last_epn_campaign`
- `last_epn_source`
- `last_epn_medium`
- `last_epn_datetime`

similarly to how `last_sk` is currently handled.

---

## Important anti-mistakes

Do NOT:
- use Mixpanel `AliExpress Activity` as the behavioral denominator
- mix Problem A and Problem B
- use `clients._id` as user identity
- treat `clients` as the primary behavior log
- use one unified attribution model for Global and CIS
- classify countries by geopolitical grouping instead of actual affiliate routing
- assume one user has one client state
- require `order_id` for purchase matching
- treat lack of checkout-path `events` as proof of no user activity without considering excluded URL logging
- confuse `Affiliate Click` with successful return to AliExpress
- treat `guestStateHistory` as evidence of actual hub usage or redirect execution
- salvage old CIS `sk`-based conclusions
- salvage old UA-as-CIS conclusions

---

## Mongo / data access notes

### MongoDB
Database:
- `alihelper`

Collections:
- `guests`
- `events`
- `clients`
- `guestStateHistory`

### Important technical notes
- `events` has no index on `created`, so use `_id`-based date filtering where appropriate
- `guestStateHistory` should also be queried carefully with `_id`-based filtering due to limited indexing
- use `allowDiskUse: true` for heavy aggregations
- avoid huge `$lookup` workflows when possible

### Security
Do not store secrets in source code or notebooks.
Use environment variables for:
- Mongo credentials
- Mixpanel credentials