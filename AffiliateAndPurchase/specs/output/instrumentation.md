# Instrumentation Recommendations

## Current state

UTM parameters ARE available in `events.payload.url` for CIS traffic. They can be parsed from the full URL. However, this requires URL parsing at query time, which is:
- slower than reading dedicated fields
- fragile (malformed URLs, encoding issues)
- not indexed

---

## Recommended: extract UTM to dedicated fields

Store parsed UTM params as dedicated fields in `events.payload` or a related structure:

| Field | Source | Example |
|-------|--------|---------|
| `utm_source` | from URL query string | `aerkol` |
| `utm_medium` | from URL query string | `cpa` |
| `utm_campaign` | from URL query string | `creative123_7685` |

---

## Recommended: normalized affiliate metadata

Compute and store normalized affiliate fields:

| Field | Logic |
|-------|-------|
| `affiliate_provider` | e.g. `alihelper_epn`, `alihelper_portals`, `third_party` |
| `affiliate_marker_type` | `sk`, `utm`, `af` |
| `affiliate_account_id` | e.g. `7685` for AliHelper EPN |
| `affiliate_creative_id` | prefix from `utm_campaign` before `_7685` |
| `is_alihelper_owned` | boolean: true if owned sk/UTM matches |

---

## Recommended: Affiliate Return Detected event

Add a dedicated client-side event `Affiliate Return Detected` that fires when:
- the client detects it has landed on AliExpress after a hub redirect
- captures the affiliate params observed in the URL

This provides a clean signal separate from general page-visit events.

---

## Recommended: CIS state tracking (parity with Global)

For CIS/EPN, persist and report:

| Field | Analogous Global field |
|-------|----------------------|
| `last_epn_campaign` | `last_sk` |
| `last_epn_source` | — |
| `last_epn_medium` | — |
| `last_epn_datetime` | `last_sk_datetime` |

---

## Priority

1. **Quick win**: extract UTM to dedicated indexed fields at write time (no data loss, faster queries)
2. **Medium**: add `Affiliate Return Detected` event
3. **Medium**: add normalized affiliate metadata fields
4. **Low**: add CIS state tracking fields (useful for real-time monitoring)
