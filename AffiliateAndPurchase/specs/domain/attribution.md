# Attribution Models

## Two attribution systems

AliHelper uses different affiliate mechanisms for Global and CIS traffic.
Both are now **directly observable** from MongoDB `events`.

---

## Parameter source priority

For any event, query parameters can come from two sources:

1. **`events.params`** — object with all parsed query-string params (new events only, added mid-April 2026)
2. **`events.payload.url`** — full URL, must be parsed at query time (all events)
3. **`events.payload.querySk`** — legacy pre-extracted `sk` field (all events, not removed)

### Read priority

| Need | First | Fallback |
|------|-------|----------|
| `sk` | `events.params.sk` | `events.payload.querySk` → parse `events.payload.url` |
| `utm_*`, `af` | `events.params.<param>` | parse `events.payload.url` |

Always record which source was used for coverage reporting.

---

## Global / Portals attribution

Identified via `sk` parameter on an AliExpress URL.

### AliHelper-owned Global `sk` whitelist

- `_c36PoUEj`
- `_d6jWDbY`
- `_AnTGXs`
- `_olPBn9X`
- `_dVh6yw5`

### Interpretation

| Signal | Meaning |
|--------|---------|
| Whitelisted `sk` | AliHelper-owned affiliate state |
| Non-whitelisted `sk` | Third-party affiliate state |
| `af` present on a Global-domain URL | Third-party (AliHelper does not use `af` for Global) |
| `cn`, `cv`, `dp` params | Optional competitor-added tracking params on Global links; do not use for ownership logic |

### Direct-return evidence (Global)

Successful Global return to AliExpress is observable when:
- an `sk` is present on the AliExpress URL
- ideally one of our whitelisted `sk` values

---

## CIS / EPN attribution

Identified via `af` OR UTM parameters on an `aliexpress.ru` URL. EPN (the CIS partner program) embeds the cabinet id in one of two mutually exclusive patterns, chosen per creative/source.

### AliHelper EPN cabinet id

**`7685`** — AliHelper's EPN cabinet id. The suffix `_7685` in either `af` or `utm_campaign` identifies our attribution. The cabinet id is stable and was preserved through the recent migration to the new ref-link mechanism.

### Two attribution patterns (mutually exclusive)

Both patterns appear on `aliexpress.ru` landing URLs after passing through an affiliate link. Each creative embeds the cabinet id in exactly ONE of these patterns:

#### Pattern A — `af`-based

```
https://aliexpress.ru/item/<id>.html?af=<prefix>_<cabinet_id>&utm_medium=cpa&...
```

Conditions:
- `af` present, value matches `*_<cabinet_id>`
- `utm_medium=cpa` typically also present (but see "Partial match" below)
- no `utm_source`, `utm_campaign`

AliHelper-owned if `af` suffix = `_7685`.

#### Pattern B — full-UTM

```
https://aliexpress.ru/item/<id>.html?utm_source=aerkol&utm_medium=cpa&utm_campaign=<prefix>_<cabinet_id>&utm_content=<n>&...
```

Conditions:
- `utm_source=aerkol`
- `utm_medium=cpa`
- `utm_campaign` matches `*_<cabinet_id>`
- no `af`

AliHelper-owned if `utm_campaign` suffix = `_7685`.

### Ownership rule (consolidated)

A CIS event is labeled AliHelper-owned when **either**:
- `af` ends with `_7685` (Pattern A), OR
- `utm_campaign` ends with `_7685` AND `utm_source=aerkol` AND `utm_medium=cpa` (Pattern B)

### Partial match handling

- `af` ends with `_7685` but `utm_medium` missing → still treat as AliHelper-owned (suffix `_7685` is the defining signal; `utm_medium=cpa` is confirmatory)
- `utm_campaign` ends with `_7685` but `utm_source` or `utm_medium` missing → label as `CIS_PARTIAL_UTM` and report separately; do NOT silently accept
- `utm_medium=cpa` alone (no `af`, no `utm_campaign`) → ambiguous; not classifiable as ours, treat as non-attributable CIS signal

### Foreign CIS affiliate evidence

Any of the following on `aliexpress.ru`:
- `af` ends with a suffix other than `_7685` (e.g. `af=860_53163`)
- `utm_campaign` ends with a suffix other than `_7685`
- `utm_source` present and not `aerkol`
- Other affiliate-style UTM patterns

---

## Attribution window

Default: **72 hours** before `Purchase Completed`.

The 72-hour window applies to **server-side `events` reconstruction only**. Client-side `last_sk` / `last_af` / `last_utm_*` fields on `Purchase Completed` have NO 72-hour limit and reflect the last state ever set on that client; they are **not** authoritative for attribution logic (see `specs/rules/caveats.md`).

---

## Overwrite detection

### Global overwrite

We had an AliHelper-owned `sk`, then within the 72-hour window:
- a later foreign `sk` appeared, OR
- `af` appeared on a Global-domain URL, OR
- cashback interference evidence

### CIS overwrite

We had an AliHelper-owned signal (`af=*_7685` or `utm_campaign=*_7685`), then within the 72-hour window on `aliexpress.ru`:
- a later `af` appeared with suffix ≠ `_7685` (foreign Pattern A), OR
- a later `utm_campaign` appeared with suffix ≠ `_7685` (foreign Pattern B), OR
- a later `utm_source` ≠ `aerkol` with affiliate-style params, OR
- cashback interference evidence

Both foreign-af and foreign-utm overwrites are equally valid signals — neither dominates the other.

---

## Analysis labels

Every finding must be labeled:

| Label | Meaning |
|-------|---------|
| `GLOBAL_DIRECT` | Global traffic, sk-based direct observation |
| `CIS_DIRECT_AF` | CIS traffic, Pattern A (`af=*_7685`) direct observation |
| `CIS_DIRECT_UTM` | CIS traffic, Pattern B (full-UTM with `_7685`) direct observation |
| `CIS_PARTIAL_UTM` | CIS traffic, partial UTM match (missing source/medium) |
| `CIS_PROXY` | CIS traffic, time-based proxy only (fallback when URL has neither pattern) |

`CIS_DIRECT_AF` and `CIS_DIRECT_UTM` can be aggregated to `CIS_DIRECT` when summarizing but should be kept separable for diagnostic purposes.
