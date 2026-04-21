# Browser Flows & Eligibility

## Two redirect mechanisms

### Auto-redirect (Firefox, Edge)

- Fires before page content loads
- Triggered on `webNavigation.onBeforeNavigate`
- Conditions:
  - URL matches one of the `checkListUrls` regex patterns (see below)
  - 30 minutes since last affiliate activation attempt
  - Cashback cooldown allows
- The client redirects user to the hub

### DOGI flow (Chrome, Yandex, Opera, other Chrome-like)

- Affiliate link is placed on product cards once every 30–60 minutes
- User-initiated: triggered through interaction with DOGI coin / product thumbnail
- **Eligible pages: product pages only** (pages with product cards)

### Methodological consequence

Problem A must be segmented by:
- Firefox/Edge auto-redirect lineage
- Chrome-like DOGI lineage

Do NOT pool them as one mechanism.

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

## Browser family classification

| Raw browser string contains | Family |
|-----------------------------|--------|
| `firefox` | firefox |
| `edge`, `edg/` | edge |
| `yandex`, `yabrowser` | yandex |
| `opera`, `opr/` | opera |
| `chrome`, `chromium` | chrome |
| `safari` | safari |
| other | other |

### Lineage mapping

| Family | Lineage |
|--------|---------|
| firefox, edge | auto-redirect |
| all others | dogi |

---

## Observability limitations

- No direct backend log of client-side auto-redirect attempts
- Expected auto-redirect opportunities must be reconstructed indirectly from:
  - eligible page visits in `events`
  - browser lineage
  - cooldown rule (30 min for auto-redirect, 30–60 min for DOGI)
  - latest prior config snapshot in `guestStateHistory`
  - later evidence: `Affiliate Click` and/or return signals
- Some paths may be excluded from logging by config-level URL exclusions (`noLogUrls`) — absence of `events` near checkout/order flow is not always evidence of no user activity
