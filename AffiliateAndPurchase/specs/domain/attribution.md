# Attribution Models

## Two attribution systems

AliHelper uses different affiliate mechanisms for Global and CIS traffic.
Both are now **directly observable** from MongoDB `events`.

---

## Global / Portals attribution

Identified via `events.payload.querySk` containing the `sk` parameter.

### AliHelper-owned Global `sk` whitelist

- `_c36PoUEj`
- `_d6jWDbY`
- `_AnTGXs`
- `_olPBn9X`
- `_dVh6yw5`

### Interpretation

| Signal | Meaning |
|--------|---------|
| Whitelisted `sk` in `querySk` | AliHelper-owned affiliate state |
| Non-whitelisted `sk` in `querySk` | Third-party affiliate state |
| `af` parameter | Generally third-party (AliHelper does not use `af`) |

### Direct-return evidence

Successful Global return to AliExpress is observable when:
- `events.payload.querySk` contains at least `sk`
- ideally one of our whitelisted `sk` values

---

## CIS / EPN attribution

Identified via UTM parameters parsed from `events.payload.url`.

### How it works

CIS users are returned to `aliexpress.ru` with URLs containing UTM parameters:
```
https://aliexpress.ru/item/...?utm_source=aerkol&utm_medium=cpa&utm_campaign=<creative_id>_7685
```

### AliHelper-owned CIS markers

All three conditions must hold:
- `utm_source=aerkol`
- `utm_medium=cpa`
- `utm_campaign` ending in `_7685`

Known AliHelper EPN account id: `7685`

The prefix before `_7685` in `utm_campaign` is a candidate creative/link id (not a mandatory stable identifier unless validated).

### Extraction method

Parse query string from `events.payload.url`:
1. Extract `utm_source`, `utm_medium`, `utm_campaign` from URL query params
2. Apply AliHelper ownership check (all three conditions above)
3. Any UTM with different source/medium/campaign = third-party affiliate evidence

### Direct-return evidence

Successful CIS return to AliExpress is observable when:
- `events.payload.url` on `aliexpress.ru` contains `utm_source=aerkol` + `utm_medium=cpa` + `utm_campaign` matching `*_7685`

### Foreign CIS affiliate evidence

Any of:
- `utm_source` != `aerkol`
- `utm_medium` = `cpa` but `utm_campaign` not ending in `_7685`
- other affiliate-style UTM patterns on `aliexpress.ru`

---

## Attribution window

Default: **72 hours** before `Purchase Completed`.

---

## Overwrite detection

### Global overwrite

We had an AliHelper-owned `sk`, then within the 72-hour window:
- a later foreign `sk` appeared, OR
- `af` appeared, OR
- cashback interference evidence

### CIS overwrite

We had AliHelper-owned UTM params (`aerkol` / `cpa` / `*_7685`), then within the 72-hour window:
- a later event with foreign UTM params appeared on `aliexpress.ru`, OR
- a later event with non-AliHelper `utm_campaign` appeared, OR
- cashback interference evidence

---

## Analysis labels

Every finding must be labeled:

| Label | Meaning |
|-------|---------|
| `GLOBAL_DIRECT` | Global traffic, sk-based direct observation |
| `CIS_DIRECT` | CIS traffic, UTM-based direct observation from `events.payload.url` |
| `CIS_PROXY` | CIS traffic, time-based proxy only (fallback when URL has no UTM) |

