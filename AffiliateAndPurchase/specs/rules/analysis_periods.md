# Analysis Periods & Time Handling

## One-off investigation periods (current)

### Problem A — Missing Affiliate Click

28 complete UTC days, excluding current incomplete day:
- **Start:** `2026-03-06 00:00:00 UTC`
- **End:** `2026-04-02 23:59:59 UTC`

### Problem B — Purchase Completed without Purchase

Mature cohort of 28 complete UTC days, excluding most recent 7 days (maturity buffer):
- **Start:** `2026-02-27 00:00:00 UTC`
- **End:** `2026-03-26 23:59:59 UTC`

---

## Recurring reports — rolling windows

See [`specs/workflows/recurring_reports.md`](../workflows/recurring_reports.md) for full cadence methodology.

### Weekly pulse

Rolling 7 UTC days ending on the last complete day (exclude current incomplete day).

- Covers Problem A (aggregate funnel, top segments, non-activator A7 aggregate)
- Does **NOT** cover Problem B (7 days is below the maturity + volume threshold — see `specs/rules/caveats.md`)

### Monthly deep

Rolling 28 UTC days:
- Problem A: ends on the last complete day (no maturity buffer needed)
- Problem B: ends 7 days before today (maturity buffer for delayed postbacks)

**Maturity buffer rationale**: postbacks commonly arrive 3-7 days after purchase. Including the most recent 7 days would inflate the "missing Purchase" count with orders that will settle later.

### Baseline

Pick a stable monthly-deep report as the longitudinal comparison baseline (e.g. first complete month of 2026). Document baseline report_id and period in every subsequent report.

---

## Known incident window

Postback issue affecting CIS users on **2026-04-01**. Orders/leads were later backfilled same day or the following day.

### Implications

- Do not use `2026-04-01` as a clean baseline day for Problem B
- If any validation touches `2026-04-01` or `2026-04-02`, treat them as incident dates for CIS users
- Analyze CIS and Global separately for validations including those dates
- Do not interpret abnormal CIS `Purchase Completed → Purchase` gaps on those dates as normal attribution loss without explicit incident adjustment

Note: the one-off Problem B window already avoids this incident period. For recurring reports, flag any rolling window overlapping `2026-04-01` and annotate affected metrics.

---

## Timezone rules

| System | Timezone |
|--------|----------|
| MongoDB `events.created` | UTC |
| MongoDB `ObjectId` timestamps | UTC-derived |
| Mixpanel project | `Europe/Moscow` (UTC+3) |

Be explicit about timezone conversions whenever matching MongoDB to Mixpanel events.

---

## Data coverage start dates

| Source | Coverage start |
|--------|----------------|
| `Affiliate Click` (Mixpanel) | `2026-03-06 00:00:00 UTC` |
| `events.params` object | mid-April 2026 (see `specs/rules/caveats.md` for coverage reporting) |
| `clients.build_app` | mid-April 2026 |
| New `Purchase Completed` fields (`last_sk`, `last_af`, `is_CIS`, etc.) | mid-April 2026 |
