# Problem B — Purchase Completed without Purchase

## Definition

Why do we see more `Purchase Completed` than commission-bearing `Purchase`?

This is a **separate problem** from Problem A. Do not mix.

---

## Analysis period

Mature cohort of 28 complete UTC days, excluding most recent 7 days (avoid confusing true attribution loss with delayed postbacks):
- `2026-02-27 00:00:00 UTC` to `2026-03-26 23:59:59 UTC`

For each `Purchase Completed`:
- reconstruct the prior 72-hour attribution window from MongoDB `events`
- match to `Purchase` by user + time proximity (10-minute window)

---

## Data sources

- Mixpanel: `Purchase Completed`, `Purchase`
- MongoDB `events`: 72-hour pre-purchase reconstruction
- MongoDB `clients`: enrichment (optional)

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

For each `Purchase Completed`, reconstruct the prior 72-hour window:

1. Whether AliHelper-owned `sk` was seen (in `events.payload.querySk`)
2. When it was last seen
3. Whether a later foreign `sk` appeared
4. Whether `af` appeared
5. Whether there are cashback traces
6. Whether purchase matched to `Purchase` within 10-minute window
7. Primary reason for absence of `Purchase`

### CIS

For each `Purchase Completed`, reconstruct the prior 72-hour window:

1. Whether AliHelper-owned UTM params were seen (in `events.payload.url`: `utm_source=aerkol` + `utm_medium=cpa` + `utm_campaign=*_7685`)
2. When they were last seen
3. Whether later foreign UTM / affiliate params appeared
4. Whether there was a prior `Affiliate Click`
5. Whether there are cashback traces
6. Whether purchase matched to `Purchase` within 10-minute window
7. Primary reason for absence of `Purchase`

Fallback: if URL has no UTM params, use proxy return (≤120s post-click) as weaker evidence (label `CIS_PROXY`).

---

## Reason codes

### Global

| Code | Meaning |
|------|---------|
| `NO_OUR_SK_IN_72H` | No AliHelper-owned sk in 72h window |
| `FOREIGN_SK_AFTER_OUR_SK` | Our sk overwritten by foreign sk |
| `AF_AFTER_OUR_SK` | Our sk overwritten by af param |
| `CASHBACK_TRACE` | Cashback interference detected |
| `LIKELY_DELAYED_POSTBACK` | Purchase probably delayed, not lost |
| `TRACKING_MISMATCH` | Technical tracking discrepancy |
| `PARTNER_RULE_EXCLUSION` | Partner program rule exclusion |
| `UNKNOWN` | Unexplained |

### CIS

| Code | Meaning |
|------|---------|
| `CIS_NO_OUR_UTM_IN_72H` | No AliHelper-owned UTM in 72h window |
| `CIS_FOREIGN_UTM_AFTER_OURS` | Our UTM overwritten by foreign affiliate |
| `CIS_NO_HUB_REACH_OBSERVED` | No Affiliate Click before purchase |
| `CIS_HUB_REACHED_NO_RETURN` | Affiliate Click exists, no return with our UTM |
| `CIS_CASHBACK_TRACE` | Cashback interference detected |
| `CIS_LIKELY_DELAYED_POSTBACK` | Purchase probably delayed |
| `CIS_TRACKING_MISMATCH` | Technical tracking discrepancy |
| `CIS_PROXY_ONLY` | Only proxy-return evidence available, no UTM |
| `CIS_UNKNOWN` | Unexplained |

---

## Mandatory analyses

### B1. Presence of valid attribution evidence

**Global:** how many `Purchase Completed` had our `sk` in the prior 72h?

**CIS:** how many `Purchase Completed` had our UTM params in the prior 72h?

### B2. Overwrite analysis

**Global:** among cases with our prior `sk`, how many later showed foreign `sk`, `af`, or conflicting affiliate evidence?

**CIS:** among cases with our prior UTM, how many later showed foreign UTM or conflicting affiliate evidence?

### B3. Delayed postback analysis
Quantify how much of the gap may be explained by delayed `Purchase`.

### B4. Matching stability
Sensitivity checks around the 10-minute matching window.

### B5. Segment-level loss rate
Compare missing `Purchase` rate by:
- Region: Global vs CIS (by routing)
- Browser family
- Auto-redirect vs DOGI lineage
- Country
- Hub from latest delivered config
- Version
- Category
- New buyer
- Hot product
- Multi-client vs single-client user

---

## Mandatory hypotheses to test

1. Material share of missing commissions is explained by last-click overwrite (Global: foreign sk, CIS: foreign UTM).
2. Foreign `sk` is the strongest overwrite signal for Global.
3. Foreign UTM is the strongest overwrite signal for CIS.
4. `af` is a useful third-party overwrite marker for Global.
5. Cashback explains part of the gap, but observability is partial.
6. Some gap is delayed postback, not true commission loss.
7. Some `Purchase Completed` cases never had valid AliHelper affiliate state within 72h.
8. Loss rate differs materially by browser / store lineage / version / geo / hub.
9. Some discrepancy is tracking mismatch rather than partner non-crediting.
10. UA must be classified and analyzed as Global (direct `sk` logic).
