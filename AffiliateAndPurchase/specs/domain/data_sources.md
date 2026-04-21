# Data Sources

## MongoDB (database: `alihelper`)

### `events` — primary behavioral source of truth

Every AliExpress page visit is stored here. Use for:
- User browsing activity reconstruction
- Page-type reconstruction (product page, homepage, etc.)
- Eligible opportunity counting
- Affiliate-state reconstruction:
  - **Global:** `events.payload.querySk` for `sk` params
  - **CIS:** parse UTM params from `events.payload.url`
- Detecting return to AliExpress after hub redirect
- Reconstructing 72-hour pre-purchase attribution window

**Technical notes:**
- No index on `created` — use `_id`-based date filtering via `ObjectId`
- Use `allowDiskUse: true` for heavy aggregations
- Avoid huge `$lookup` workflows

### `clients` — client enrichment only

Use to enrich `events` with:
- browser, user_agent, os
- city, country
- client_version
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
| Global affiliate state | `events.payload.querySk` | parse `sk` param |
| CIS affiliate state | `events.payload.url` | parse UTM params |
| Page type | `events.payload.productId` | non-empty = product page |
| Full page URL | `events.payload.url` | for URL classification & UTM |
