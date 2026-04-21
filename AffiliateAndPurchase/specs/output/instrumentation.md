# Instrumentation Status & Recommendations

## Already implemented (as of mid-April 2026)

### `events.params` — parsed query-params object

All query-string params from `events.payload.url` are now pre-parsed into a dedicated object `events.params` at write time.

**Coverage:** new events only (from rollout date). Old events must still be parsed from URL.

**Benefit:** faster reads, no query-time parsing, no URL-encoding fragility.

**Remaining gap:** no secondary index on individual params → range queries on e.g. `events.params.sk` still scan.

### `clients.build_app`

New field in `clients` identifying the store / build the extension was installed from. Values: `chrome`, `firefox`, `edge`. Drives flow classification (DOGI vs auto-redirect).

**Coverage:** new clients only. Old clients fall back to browser-UA inference (see `specs/domain/browser_flows.md`).

### `Purchase Completed` — new client-side fields

New properties added:
- `sk`, `af` (purchase-URL direct)
- `last_sk`, `last_sk_datetime`, `last_af`, `last_af_datetime`
- `last_utm_source`, `last_utm_medium`, `last_utm_campaign`, `last_utm_content`, `last_utm_datetime`
- `is_CIS` (domain-based client-side boolean)
- `cashback_list` (current-session cashback sites, wiped after extension sleep)
- `alihelper_version`
- `cn`, `cv`, `dp` (optional tracking params on some Global partner links)

**Coverage:** new `Purchase Completed` events only.

**Caveat:** client-side, less reliable than server-side `events`. Use for validation / fallback / coverage reporting; not as primary attribution source (see `specs/rules/caveats.md`).

---

## Still recommended

### 1. Secondary indexes on `events.params.*`

Currently `events.params` is an unindexed object. For fast reconstruction at scale, add indexes on:
- `events.params.sk`
- `events.params.af`
- `events.params.utm_campaign`
- `events.params.utm_source`

Compound index with `guest_id + _id` would speed up per-user window reconstruction.

### 2. `Affiliate Return Detected` event

Add a dedicated client-side Mixpanel event that fires when:
- the client detects it has landed on AliExpress after a hub redirect
- captures the affiliate params observed in the URL (pattern A vs B for CIS; sk for Global)

Benefit: clean return signal separate from general page-visit events, eliminates dependency on proxy-return (≤120s) fallback.

### 3. Normalized affiliate metadata on `events`

Compute and store at write time:

| Field | Logic |
|-------|-------|
| `affiliate_provider` | `alihelper_epn` / `alihelper_portals` / `third_party_epn` / `third_party_portals` / `none` |
| `affiliate_marker_type` | `sk` / `utm` / `af` |
| `affiliate_account_id` | e.g. `7685` for AliHelper EPN |
| `affiliate_creative_id` | prefix extracted from `af` or `utm_campaign` before the cabinet id |
| `is_alihelper_owned` | boolean |
| `cis_pattern` | `A_af` / `B_utm` / `partial_utm` / `none` |

Benefit: analysis reads a single boolean + classification instead of re-implementing the ownership rule on every query.

### 4. Backfill `build_app` for active clients

For clients that were installed before `build_app` rollout but are still active, derive `build_app` from fresh telemetry (extension APIs know which store the browser supports / the extension was sideloaded from) and backfill.

Benefit: shrinks `edge_ambiguous_build` and `unknown_build` segments faster than natural client churn.

### 5. CIS state tracking parity with Global (on `events`, not just PC)

For CIS/EPN, persist and expose the "last seen" state on a per-request basis:

| Field | Analogous Global field |
|-------|----------------------|
| `last_epn_campaign` | `last_sk` |
| `last_epn_source` | — |
| `last_epn_medium` | — |
| `last_epn_af` | — |
| `last_epn_datetime` | `last_sk_datetime` |

Benefit: fast real-time monitoring without full events reconstruction.

---

## Priority order

1. **Quick win**: secondary indexes on `events.params.*` (recommended #1)
2. **Medium**: normalized affiliate metadata on `events` (recommended #3) — removes ownership-rule duplication across codepaths
3. **Medium**: `Affiliate Return Detected` event (recommended #2) — improves CIS return observability
4. **Medium**: `build_app` backfill (recommended #4) — improves flow classification accuracy for Edge specifically
5. **Low**: CIS state tracking parity fields (recommended #5) — nice-to-have for monitoring
