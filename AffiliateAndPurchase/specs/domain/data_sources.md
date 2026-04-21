# Data Sources

## MongoDB (database: `alihelper`)

### `events` — primary behavioral source of truth

Every AliExpress page visit is stored here. Use for:
- User browsing activity reconstruction
- Page-type reconstruction (product page, homepage, etc.)
- Eligible opportunity counting
- Affiliate-state reconstruction:
  - **Global:** `sk` (see priority below)
  - **CIS:** `af` / UTM params (see priority below)
- Detecting return to AliExpress after hub redirect
- Reconstructing 72-hour pre-purchase attribution window

**Technical notes:**
- No index on `created` — use `_id`-based date filtering via `ObjectId`
- Use `allowDiskUse: true` for heavy aggregations
- Avoid huge `$lookup` workflows

#### Query-parameter fields: `events.params` (new), `events.payload.querySk` (legacy), `events.payload.url` (always)

All three sources can provide attribution params. Use in this priority:

| Need | First | Fallback 1 | Fallback 2 |
|------|-------|------------|------------|
| `sk` | `events.params.sk` | `events.payload.querySk` | parse from `events.payload.url` |
| `utm_source` / `utm_medium` / `utm_campaign` / `utm_content` | `events.params.<name>` | parse from `events.payload.url` | — |
| `af` | `events.params.af` | parse from `events.payload.url` | — |

**Coverage:**
- `events.params` — available on **new events only** (added mid-April 2026). Old events do not have it → must parse `events.payload.url`.
- `events.payload.querySk` — legacy, available on all events; not removed when `events.params` was added, so both coexist on new events.

Always record which source was used and report coverage (% of events with `events.params`) in every report.

### `clients` — client enrichment only

Use to enrich `events` with:
- `browser`, `user_agent`, `os`
- `city`, `country`
- `client_version`
- `build_app` — **new field**, values: `chrome`, `firefox`, `edge`. Identifies the store / build the extension was installed from. Drives flow classification (DOGI vs auto-redirect). Missing on old clients (pre-mid-April 2026).
- IP context

Treat as client-state history, NOT as a canonical user table.
One user can have multiple client records.

### `guests` — canonical user identity

`guests._id` = canonical user identifier = Mixpanel `$user_id`.

### `guestStateHistory` — config delivery snapshots

Each record = client requested and received a fresh config at that time.

| Field | Meaning |
|-------|---------|
| `domain` | Hub assigned in config snapshot |
| `value=true` | Config included a usable hub |
| `value=false` | Config did not include a usable hub |

**Important:** this is NOT:
- a history of actual redirect usage
- proof that the user used that hub
- proof that redirect happened

It IS evidence of which config/hub was last delivered.

**Usage rule:** match the latest config snapshot BEFORE the analyzed event/window. Active users may fetch config many times per day — latest prior = best estimate.

**Technical note:** limited indexing — use `_id`-based filtering.

---

## Mixpanel

Use **only** for these events:
- `Affiliate Click` — user reached the hub
- `Purchase` — commission-bearing purchase
- `Purchase Completed` — any completed purchase

Project timezone: `Europe/Moscow` (UTC+3). Be explicit about timezone conversions when matching to MongoDB.

### `Purchase Completed` — new properties (added mid-April 2026)

On new events, `Purchase Completed` carries client-side attribution state:

| Field | Meaning |
|-------|---------|
| `sk` | `sk` parameter on the purchase URL itself (direct observation) |
| `last_sk` | Last `sk` set on the client (no 72h limit; can be arbitrarily old) |
| `last_sk_datetime` | When `last_sk` was set |
| `af` | `af` on the purchase URL itself (direct observation) |
| `last_af` | Last `af` set on the client (no 72h limit) |
| `last_af_datetime` | When `last_af` was set |
| `last_utm_source` / `last_utm_medium` / `last_utm_campaign` / `last_utm_content` | Last UTM values set on the client |
| `last_utm_datetime` | When last UTM was set |
| `is_CIS` | Client-side boolean: true if the purchase domain is `aliexpress.ru`, else false |
| `cashback_list` | Cashback sites visited during the current client session (wiped after extension sleep) |
| `alihelper_version` | Extension version |
| `cn`, `cv`, `dp` | Optional tracking params some Global partner links carry; not used for ownership logic |
| `order_id` | Often null; do NOT rely on it for matching |

**Reliability caveat:** these are **client-side fields** and therefore **less reliable** than server-side `events`. When `events` exists, prefer reconstructing from `events`. Use `Purchase Completed` fields as:
- validation / spot-check against `events`-based reconstruction
- fallback when `events` coverage is incomplete
- coverage reporting (is the new schema rolling out correctly?)

**Do NOT use `last_sk` / `last_af` / `last_utm_*` as the authoritative attribution state for the 72-hour window** — they have no window limit and may reflect state set weeks ago.

### `AliExpress Activity`
Daily aggregated event. NOT the source of truth for behavioral analysis. Use only for high-level sanity checks.

---

## Key field mappings

| Need | Source | Field |
|------|--------|-------|
| User identity | `guests._id` | = Mixpanel `$user_id` |
| User from events | `events.guest_id` | → `guests._id` |
| User from clients | `clients.guest_id` | → `guests._id` |
| User from GSH | `guestStateHistory.guest_id` | → `guests._id` |
| Global affiliate state | `events.params.sk` / `events.payload.querySk` / parse `events.payload.url` | see priority table |
| CIS affiliate state | `events.params.af` / `events.params.utm_*` / parse `events.payload.url` | see priority table |
| Page type | `events.payload.productId` | non-empty = product page |
| Full page URL | `events.payload.url` | for URL classification |
| Build / flow lineage | `clients.build_app` | chrome/firefox/edge; fallback to browser UA if missing |
