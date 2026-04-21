# Problem B — Purchase Completed without Purchase

## Definition

Why do we see more `Purchase Completed` than commission-bearing `Purchase`?

This is a **separate problem** from Problem A. Do not mix.

---

## Analysis period

See [`specs/rules/analysis_periods.md`](../rules/analysis_periods.md):
- One-off investigation: `2026-02-27` → `2026-03-26` (UTC)
- Monthly deep: rolling 28 days, minus 7-day maturity buffer
- Weekly pulse: **NOT applicable** for Problem B — 7-day window lacks maturity and volume. See `specs/rules/caveats.md`.

For each `Purchase Completed`:
- reconstruct the prior 72-hour attribution window from MongoDB `events`
- match to `Purchase` by user + time proximity (10-minute window)

---

## Data sources

- Mixpanel: `Purchase Completed`, `Purchase`
- MongoDB `events`: 72-hour pre-purchase reconstruction (**authoritative source**)
- MongoDB `clients`: enrichment (optional)
- `Purchase Completed` new client-side fields (`last_sk`, `last_af`, `last_utm_*`, `is_CIS`, `cashback_list`, etc.): **secondary** — for validation / coverage / fallback only, never primary. These have no 72h limit and reflect arbitrary-age client state.

Attribution params extracted from `events` follow priority in [`specs/domain/attribution.md`](../domain/attribution.md).

---

## Purchase matching rules

Do NOT rely on `order_id` (`Purchase Completed` often lacks it).

Primary matching:
- canonical user identity (`guests._id` = Mixpanel `$user_id`)
- time proximity: up to **10 minutes**

Run sensitivity checks: narrower and wider windows. Document how ambiguous matches are handled.

---

## Investigation goal

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

## Reconstruction per Purchase Completed

### Global

For each `Purchase Completed`, reconstruct the prior 72-hour window from `events`:

1. Whether AliHelper-owned `sk` was seen (priority: `events.params.sk` → `querySk` → URL parse)
2. When it was last seen
3. Whether a later foreign `sk` appeared
4. Whether `af` appeared (Global `af` = third-party)
5. Whether there are cashback traces
6. Whether purchase matched to `Purchase` within 10-minute window
7. Primary reason for absence of `Purchase`

**Optional validation**: compare reconstructed state to `Purchase Completed.last_sk` / `last_sk_datetime`. Disagreements flag as data quality issues but do NOT override events-based reconstruction.

### CIS

For each `Purchase Completed`, reconstruct the prior 72-hour window on `aliexpress.ru`:

1. Whether AliHelper-owned signal was seen:
   - Pattern A: `af=*_7685`
   - Pattern B: `utm_source=aerkol` + `utm_medium=cpa` + `utm_campaign=*_7685`
2. When it was last seen (by pattern)
3. Whether later foreign affiliate signals appeared:
   - Foreign Pattern A: `af` with suffix ≠ `_7685`
   - Foreign Pattern B: `utm_campaign` with suffix ≠ `_7685`, or `utm_source` ≠ `aerkol`
4. Whether there was a prior `Affiliate Click`
5. Whether there are cashback traces (`cashback_list` on PC as partial signal)
6. Whether purchase matched to `Purchase` within 10-minute window
7. Primary reason for absence of `Purchase`

Fallback: if URL has no recognizable CIS pattern, use proxy return (≤120s post-click) as weaker evidence (label `CIS_PROXY`).

**Optional validation**: compare to `Purchase Completed.last_af` / `last_utm_*` / `is_CIS`. Same rule — flag disagreements, do not override.

---

## Reason codes

### Global

| Code | Meaning |
|------|---------|
| `NO_OUR_SK_IN_72H` | No AliHelper-owned sk in 72h window |
| `FOREIGN_SK_AFTER_OUR_SK` | Our sk overwritten by foreign sk |
| `AF_AFTER_OUR_SK` | Our sk overwritten by af param (third-party on Global) |
| `CASHBACK_TRACE` | Cashback interference detected |
| `LIKELY_DELAYED_POSTBACK` | Purchase probably delayed, not lost |
| `TRACKING_MISMATCH` | Technical tracking discrepancy |
| `PARTNER_RULE_EXCLUSION` | Partner program rule exclusion |
| `UNKNOWN` | Unexplained |

### CIS

| Code | Meaning |
|------|---------|
| `CIS_NO_OUR_SIGNAL_IN_72H` | No AliHelper-owned af or UTM in 72h window |
| `CIS_FOREIGN_AF_AFTER_OURS` | Our signal overwritten by foreign `af` |
| `CIS_FOREIGN_UTM_AFTER_OURS` | Our signal overwritten by foreign UTM |
| `CIS_NO_HUB_REACH_OBSERVED` | No Affiliate Click before purchase |
| `CIS_HUB_REACHED_NO_RETURN` | Affiliate Click exists, no return with our signal |
| `CIS_PARTIAL_UTM_ONLY` | Only `CIS_PARTIAL_UTM` evidence (missing source/medium), incomplete |
| `CIS_CASHBACK_TRACE` | Cashback interference detected |
| `CIS_LIKELY_DELAYED_POSTBACK` | Purchase probably delayed |
| `CIS_TRACKING_MISMATCH` | Technical tracking discrepancy |
| `CIS_PROXY_ONLY` | Only proxy-return evidence available |
| `CIS_UNKNOWN` | Unexplained |

---

## Mandatory analyses

### B1. Presence of valid attribution evidence

**Global:** how many `Purchase Completed` had our `sk` in the prior 72h?

**CIS:** how many had our `af` or UTM signal in the prior 72h? Break down by Pattern A vs Pattern B.

### B2. Overwrite analysis

**Global:** among cases with our prior `sk`, how many later showed foreign `sk`, `af`, or conflicting affiliate evidence?

**CIS:** among cases with our prior signal, how many later showed:
- foreign `af` overwrite
- foreign `utm_campaign` / `utm_source` overwrite
- mixed (both types seen in window)

Report foreign-af vs foreign-utm overwrite counts separately — neither dominates a priori.

### B3. Delayed postback analysis
Quantify how much of the gap may be explained by delayed `Purchase`.

### B4. Matching stability
Sensitivity checks around the 10-minute matching window.

### B5. Segment-level loss rate
Compare missing `Purchase` rate by:
- Region: Global vs CIS (by domain)
- Browser family
- Flow lineage (`dogi` / `auto_redirect` / `edge_ambiguous_build` / `unknown_build`)
- Country
- Hub from latest delivered config
- Extension version
- Category
- New buyer
- Hot product
- Multi-client vs single-client user

### B6. PC field validation (where available)

For `Purchase Completed` events that carry new client-side fields, compare events-based reconstruction vs PC fields. Report agreement rate. Disagreements → data quality signal, not methodology change.

---

## Mandatory hypotheses to test

1. Material share of missing commissions is explained by last-click overwrite (Global: foreign sk, CIS: foreign af or UTM).
2. Foreign `sk` is the strongest overwrite signal for Global.
3. For CIS, foreign `af` and foreign `utm_campaign` contribute independently — one may dominate by traffic source.
4. `af` is a useful third-party overwrite marker for Global.
5. Cashback explains part of the gap, but observability is partial.
6. Some gap is delayed postback, not true commission loss.
7. Some `Purchase Completed` cases never had valid AliHelper affiliate state within 72h.
8. Loss rate differs materially by browser / flow lineage / version / geo / hub.
9. Some discrepancy is tracking mismatch rather than partner non-crediting.
10. UA must be classified and analyzed as Global (direct `sk` logic).
11. PC client-side fields (when available) largely agree with events-based reconstruction; disagreements flag data quality issues.
