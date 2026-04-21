# Analysis Periods & Time Handling

## Problem A — Missing Affiliate Click

28 complete UTC days, excluding current incomplete day:
- **Start:** `2026-03-06 00:00:00 UTC`
- **End:** `2026-04-02 23:59:59 UTC`

## Problem B — Purchase Completed without Purchase

Mature cohort of 28 complete UTC days, excluding most recent 7 days:
- **Start:** `2026-02-27 00:00:00 UTC`
- **End:** `2026-03-26 23:59:59 UTC`

---

## Known incident window

Postback issue affecting CIS users on **2026-04-01**. Orders/leads were later backfilled same day or the following day.

### Implications

- Do not use `2026-04-01` as a clean baseline day for Problem B
- If any validation touches `2026-04-01` or `2026-04-02`, treat them as incident dates for CIS users
- Analyze CIS and Global separately for validations including those dates
- Do not interpret abnormal CIS `Purchase Completed → Purchase` gaps on those dates as normal attribution loss without explicit incident adjustment

Note: primary Problem B window already avoids this incident period.

---

## Timezone rules

| System | Timezone |
|--------|----------|
| MongoDB `events.created` | UTC |
| MongoDB `ObjectId` timestamps | UTC-derived |
| Mixpanel project | `Europe/Moscow` (UTC+3) |

Be explicit about timezone conversions whenever matching MongoDB to Mixpanel events.

---

## Affiliate Click data coverage

`Affiliate Click` data coverage starts: `2026-03-06 00:00:00 UTC`
