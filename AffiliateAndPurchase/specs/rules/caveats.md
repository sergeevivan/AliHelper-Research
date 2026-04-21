# Data Quality & Observability Caveats

## Must report in every analysis

### Partial observability

- **Cashback**: cashback-site visits are tracked only in client local storage, NOT logged to backend. Use cashback traces from `Purchase Completed` as partial evidence only. Treat cashback-related explanations as partially observable and quantify uncertainty.

- **Auto-redirect attempts**: no direct backend log of client-side redirect attempts. Must reconstruct indirectly from eligible visits + browser lineage + 30-min rule + config + later signals.

- **noLogUrls exclusions**: some paths may be excluded from logging by config-level URL exclusions. Absence of `events` near checkout/order flow is not always evidence of no user activity.

### Data matching uncertainty

- **Purchase matching**: time-based matching (10-min window) introduces possible false positives/negatives. Run sensitivity checks.

- **Client enrichment**: one user can have multiple clients. Enrichment from `clients` reflects the client used for a specific event, not a stable user property.

- **guestStateHistory**: represents config delivery, not actual usage. A config snapshot does not prove the user used that hub.

### UTM parsing (CIS)

- **URL quality**: `events.payload.url` may contain malformed URLs, encoded params, or truncated values. Handle parsing errors gracefully.

- **UTM completeness**: not all CIS affiliate returns may include all three UTM params. Define handling for partial matches.

- **Fallback to proxy**: when `events.payload.url` has no UTM params for a CIS event, fall back to time-based proxy return (≤120s). Label as `CIS_PROXY`.

### Technical

- **MongoDB indexing**: `events` has no index on `created` — use `_id`-based date filtering. Heavy aggregations require `allowDiskUse: true`.

- **Mixpanel timezone**: project timezone is `Europe/Moscow` (UTC+3). Explicit conversion needed for all MongoDB ↔ Mixpanel joins.

- **Missing `order_id`**: `Purchase Completed` often lacks `order_id`. Do not depend on it for matching.

