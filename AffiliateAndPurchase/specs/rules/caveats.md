# Data Quality & Observability Caveats

## Must report in every analysis

### Partial observability

- **Cashback**: cashback-site visits are tracked only in client local storage, NOT logged to backend. Use cashback traces as partial evidence only. Treat cashback-related explanations as partially observable and quantify uncertainty.
  - `Purchase Completed.cashback_list` reflects only the **current client session** (wiped after extension sleep) — does not show all historical cashback exposure.

- **Auto-redirect attempts**: no direct backend log of client-side redirect attempts. Must reconstruct indirectly from eligible visits + build lineage + 30-min rule + config + later signals.

- **noLogUrls exclusions**: some paths may be excluded from logging by config-level URL exclusions. Absence of `events` near checkout/order flow is not always evidence of no user activity.

### Data matching uncertainty

- **Purchase matching**: time-based matching (10-min window) introduces possible false positives/negatives. Run sensitivity checks.

- **Client enrichment**: one user can have multiple clients. Enrichment from `clients` reflects the client used for a specific event, not a stable user property.

- **guestStateHistory**: represents config delivery, not actual usage. A config snapshot does not prove the user used that hub.

---

## New field coverage (limited in current analysis periods)

Several fields were added **mid-April 2026**. Coverage in current analysis periods (Problem A: 2026-03-06 → 2026-04-02; Problem B: 2026-02-27 → 2026-03-26) is **near zero or zero** — these fields become useful for future rolling reports.

| Field | Collection / event | Availability |
|-------|-------------------|-------------|
| `events.params` | MongoDB `events` | New events only (mid-April 2026 onward) |
| `clients.build_app` | MongoDB `clients` | New clients only; old clients have no value → use browser UA fallback |
| `last_sk`, `last_af`, `last_utm_*`, `is_CIS`, `cashback_list`, etc. | Mixpanel `Purchase Completed` | New PC events only |

**Report in every analysis:** a coverage snapshot — % of events with `events.params`, % of clients with `build_app`, % of PCs with new fields — so readers can judge how much methodology depends on fallbacks.

---

## Attribution-specific caveats

### Global (`sk`)

- `sk` may appear in three sources (`events.params.sk`, `events.payload.querySk`, URL parsing). Both legacy (`querySk`) and new (`params`) mechanisms coexist on new events. Reconcile: if they disagree, prefer `events.params` and flag as data quality issue.

### CIS (`af` / UTM)

- Two attribution patterns (`af`-based, full-UTM) are **mutually exclusive per URL** — a given landing URL will carry one or the other, not both.
- **Partial UTM match**: `utm_campaign=*_7685` without `utm_source=aerkol` or without `utm_medium=cpa` → label `CIS_PARTIAL_UTM`, report separately. Do NOT silently upgrade to `CIS_DIRECT_UTM`.
- **Partial `af` match**: `af=*_7685` without `utm_medium=cpa` → still treat as AliHelper-owned; the `_7685` suffix is the defining signal.
- **URL quality**: `events.payload.url` may contain malformed URLs, encoded params, or truncated values. Handle parsing errors gracefully.
- **Fallback to proxy**: when `events.payload.url` has no recognizable CIS pattern, fall back to time-based proxy return (≤120s post-click). Label as `CIS_PROXY`.

### Client-side PC fields are not authoritative for 72h attribution

- `last_sk`, `last_af`, `last_utm_*` on `Purchase Completed` reflect the last state **ever** set on the client, with NO 72-hour limit. They can be arbitrarily old.
- Server-side reconstruction from `events` is the authoritative source for 72-hour attribution.
- Use PC fields for: validation / coverage reporting / fallback when `events` is incomplete. Never as primary source for attribution logic.

---

## Flow lineage caveats

- `build_app` is the authoritative flow identifier. Missing for old clients.
- **Edge without `build_app`**: genuinely ambiguous — could be Edge store (auto-redirect) or Chrome store (DOGI, historical). Assign to `edge_ambiguous_build` segment, do NOT pool into either flow.
- Firefox/Chrome/Yandex/Opera without `build_app` can be assigned by browser UA fallback with high confidence.
- As old clients age out, `edge_ambiguous_build` and `unknown_build` shares should shrink monotonically. Track this in coverage snapshot.

---

## Short-window analysis (weekly pulse) limitations

- **Problem A on 7-day window**: valid for aggregate funnel and top-level segments. Thin segments (browser × country × version) may be statistically noisy — report confidence intervals or suppress thin cells.
- **Problem B on 7-day window**: NOT reliable. Postbacks may be delayed 3-7 days (cohort not matured). Any single-day incident dominates. Volume per reason-code is too small for classification. Use monthly deep report (rolling 28 days minus 7-day maturity buffer) instead.

---

## Technical

- **MongoDB indexing**: `events` has no index on `created` — use `_id`-based date filtering. Heavy aggregations require `allowDiskUse: true`.

- **Mixpanel timezone**: project timezone is `Europe/Moscow` (UTC+3). Explicit conversion needed for all MongoDB ↔ Mixpanel joins.

- **Missing `order_id`**: `Purchase Completed` often lacks `order_id`. Do not depend on it for matching.
