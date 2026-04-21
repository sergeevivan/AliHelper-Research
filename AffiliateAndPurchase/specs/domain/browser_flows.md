# Browser Flows & Eligibility

## Flow is determined by BUILD, not by browser

AliHelper ships three separate extension builds, one per store. The flow (DOGI vs auto-redirect) is hard-coded per build, not per browser.

| Store | Build | Flow | Who typically installs |
|-------|-------|------|------|
| Chrome Web Store | `chrome` | DOGI coin | Chrome, Opera, Yandex, some Edge users |
| Firefox Add-ons | `firefox` | auto-redirect | Firefox |
| Edge Add-ons | `edge` | auto-redirect | Edge (since Edge got its own store) |

**Historical note:** before Edge launched its own extension store, Edge users installed from Chrome Web Store. Some Edge users may still do so today, which means Edge users can end up with the DOGI build.

---

## Build identification

### Primary: `clients.build_app`

New clients carry a `build_app` field in `clients`:

| Value | Flow |
|-------|------|
| `chrome` | DOGI |
| `firefox` | auto-redirect |
| `edge` | auto-redirect |

### Fallback: browser UA

For clients without `build_app` (old clients, pre-mid-April-2026), infer from the browser family.

| Browser family | Fallback lineage | Confidence |
|----------------|------------------|------------|
| `firefox` | auto-redirect | High — Firefox installs only from Firefox store |
| `edge` | **ambiguous** | Low — could be Edge store (auto-redirect) or Chrome store (DOGI) |
| `chrome`, `yandex`, `opera` | DOGI | High — these browsers install from Chrome Web Store |
| `safari`, `other` | unknown | — |

### Lineage segments

| Segment | Definition |
|---------|------------|
| `dogi` | `build_app=chrome`, OR fallback to DOGI from browser UA |
| `auto_redirect` | `build_app=firefox` or `edge`, OR fallback to auto-redirect from browser UA (firefox only) |
| `edge_ambiguous_build` | `edge` browser with missing `build_app` — cannot confidently classify |
| `unknown_build` | Other browser with missing `build_app` (safari, other) |

Always report `edge_ambiguous_build` as its own segment in analyses — do NOT pool it into either flow.

---

## Two redirect mechanisms

### Auto-redirect (Firefox build, Edge build)

- Fires before page content loads
- Triggered on `webNavigation.onBeforeNavigate`
- Conditions:
  - URL matches one of the `checkListUrls` regex patterns (see below)
  - 30 minutes since last affiliate activation attempt
  - Cashback cooldown allows
- The client redirects user to the hub

### DOGI flow (Chrome build)

- Affiliate link is placed on product cards once every 30–60 minutes
- User-initiated: triggered through interaction with DOGI coin / product thumbnail
- **Eligible pages: product pages only** (pages with product cards)

### Methodological consequence

Problem A must be segmented by:
- auto-redirect lineage (`build_app` = firefox/edge, or fallback)
- DOGI lineage (`build_app` = chrome, or fallback)
- `edge_ambiguous_build` as its own row

Do NOT pool auto-redirect and DOGI as one mechanism.

---

## Eligible pages per flow

### DOGI eligible pages

Only **product pages** (pages with product cards where DOGI coin can appear).

In MongoDB: `events.payload.productId` is present and non-empty.

### Auto-redirect eligible pages (`checkListUrls`)

Only URLs matching one of these regex patterns:

```javascript
export const checkListUrls = [
  /^https?:\/\/([\w\.]+)?aliexpress\.(com|ru|us)\/item\/(\d+)\.html/mi,
  /^https?:\/\/([\w\.]+)?(aliexpress|tmall)\.(com|ru|us)\/item\/.*?\/(\d+)\.html/mi,
  /^https?:\/\/([\w\.]+)?aliexpress\.(com|ru|us)\/i\/(\d+)\.html/mi,
  /^https?:\/\/([\w\.]+)?(aliexpress|tmall)\.(com|ru|us)\/item\/(\d+)\.html/i,
  /^https?:\/\/([\w\.]+)?aliexpress\.(com|ru|us)\/store\/product\/.*?\/(\d+)_(\d+)\.html/mi,
  /^https?:\/\/group\.aliexpress\.(com|ru|us)\/(\d+)-(\d+)-detail\.html/mi,
  /^https?:\/\/sale\.aliexpress\.(com|ru|us)\/[\S]+\/affi\-item\.htm/mi,
  /^https?:\/\/play\.aliexpress\.(com|ru|us)\/[\S]+\/productDetail\.htm/mi,
  /^https?:\/\/([\w\.]+)?aliexpress\.(com|ru|us)\/ssr\/(\d+)\/([\w\-]+)/mi
];
```

All patterns are **product-related URLs**. Homepages are NOT eligible for auto-redirect.

Summary of what matches:

| Pattern | Page type |
|---------|-----------|
| `/item/{id}.html` | Standard item page |
| `/item/.../{id}.html` | Item with path segments (AliExpress + Tmall) |
| `/i/{id}.html` | Short item URL |
| `/store/product/.../{id}_{id}.html` | Store product page |
| `group.aliexpress.*/...-detail.html` | Group deal detail |
| `sale.aliexpress.*/.../affi-item.htm` | Sale affiliate item |
| `play.aliexpress.*/.../productDetail.htm` | Play product detail |
| `/ssr/{id}/{slug}` | SSR product page |

### Key clarification: homepages are NOT eligible

Neither DOGI nor auto-redirect activates on homepages. **Do not include homepage visits in the eligible denominator.**

### Everything else is ineligible

Do not include ineligible page visits (search, category, cart, checkout, account, etc.) in the denominator.

---

## Cooldown rules

| Flow | Cooldown |
|------|----------|
| Auto-redirect | 30 minutes since last affiliate activation attempt |
| DOGI | 30–60 minutes between placing affiliate link on product cards |

---

## Browser family classification (for fallback only)

When `build_app` is missing, classify the browser family from the raw browser string:

| Raw browser string contains | Family |
|-----------------------------|--------|
| `firefox` | firefox |
| `edge`, `edg/` | edge |
| `yandex`, `yabrowser` | yandex |
| `opera`, `opr/` | opera |
| `chrome`, `chromium` | chrome |
| `safari` | safari |
| other | other |

Then apply the fallback lineage table above.

---

## Observability limitations

- No direct backend log of client-side auto-redirect attempts
- Expected auto-redirect opportunities must be reconstructed indirectly from:
  - eligible page visits in `events`
  - lineage (build_app or browser fallback)
  - cooldown rule (30 min for auto-redirect, 30–60 min for DOGI)
  - latest prior config snapshot in `guestStateHistory`
  - later evidence: `Affiliate Click` and/or return signals
- `build_app` field is missing for old clients; fallback introduces uncertainty, especially for Edge
- Some paths may be excluded from logging by config-level URL exclusions (`noLogUrls`) — absence of `events` near checkout/order flow is not always evidence of no user activity
