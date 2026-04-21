# Recurring Reports — Pulse + Deep Methodology

## Two-tier cadence

| Cadence | Window | Scope | Purpose |
|---------|--------|-------|---------|
| **Weekly pulse** | rolling 7 UTC days (ending on last complete day) | Problem A aggregate funnel + top-level segments + A7 non-activator aggregates | Monitoring, trend signal, early anomaly detection |
| **Monthly deep** | rolling 28 UTC days, with 7-day maturity buffer for Problem B | Both problems, full segmentation, ranked root causes, A7 full | Authoritative analysis, longitudinal comparison |

### Why not weekly for Problem B
A 7-day window is below reliable threshold for Problem B:
- Postbacks can lag 3-7 days → cohort not matured
- Per-reason-code volume too small to classify
- A single-day incident (e.g. `2026-04-01`) dominates the metric

Problem B belongs in monthly deep reports only.

---

## Window definitions

### Weekly pulse window

- Start: 7 days before the last complete UTC day, 00:00:00
- End: the last complete UTC day, 23:59:59
- Current incomplete day is **always** excluded

### Monthly deep window

**Problem A:**
- Start: 28 days before the last complete UTC day, 00:00:00
- End: the last complete UTC day, 23:59:59

**Problem B:**
- End: 7 days before today, 23:59:59 UTC (maturity buffer)
- Start: 28 days before that end, 00:00:00 UTC

---

## Report metadata

Every recurring report must carry:

| Field | Example |
|-------|---------|
| `report_id` | `pulse_2026-04-14_to_2026-04-20` / `deep_2026-03-20_to_2026-04-16` |
| `report_type` | `pulse` / `deep` / `one_off` |
| `period_start_utc` | `2026-04-14T00:00:00Z` |
| `period_end_utc` | `2026-04-20T23:59:59Z` |
| `generated_at_utc` | `2026-04-21T09:15:00Z` |
| `baseline_report_id` | reference to the baseline report |
| `previous_same_type_report_id` | reference to prior pulse/deep for comparison |
| `coverage_snapshot` | see below |

### Coverage snapshot

Always report as first content section (see `specs/output/report_structure.md` section 2):

| Metric | Value |
|--------|-------|
| `% events with events.params` | 0-100 % |
| `% clients with build_app` | 0-100 % |
| `% Purchase Completed with new fields` | 0-100 % |
| `attribution source tiers` | counts per tier: `events.params` / `querySk` / `url_parse` |
| `flow lineage split` | `dogi` / `auto_redirect` / `edge_ambiguous_build` / `unknown_build` counts |

Over time, `build_app` coverage should rise and `edge_ambiguous_build` / `unknown_build` should shrink. Regression = instrumentation issue.

---

## Incremental extraction

To keep pulse generation fast and reduce MongoDB load:

1. **Date-partitioned cache** of raw extracts:
   - `cache/events/YYYY-MM-DD.parquet` (or similar)
   - `cache/purchase_completed/YYYY-MM-DD.parquet`
   - `cache/purchase/YYYY-MM-DD.parquet`
   - `cache/affiliate_click/YYYY-MM-DD.parquet`
   - `cache/guest_state_history/YYYY-MM-DD.parquet`

2. **Extraction manifest**: `cache/_manifest.json` with per-day extraction timestamps + source version.

3. **Per report**: load only days in the window from cache; extract missing days; append to cache.

4. **Invalidation**: if methodology change affects how raw data is stored (not how it's classified), bump cache version and re-extract. If only classification logic changes, keep raw cache and recompute derived layers.

5. **Client/clients table**: treat as slowly-changing; refresh snapshots on a fixed cadence (e.g. daily full snapshot), not per-report.

---

## Comparison methodology

### Against previous same-type report
- Same metric, same segmentation, same window length, shifted by cadence period
- Flag changes above noise threshold (default ±5 % relative for funnel rates, ±10 % for absolute volumes)

### Against baseline
- Fixed "reference" monthly deep report
- Document baseline rationale (why this period is a clean baseline)
- Update baseline only when structural methodology changes

### Against 4-week trailing average
- Smooths weekly noise
- Useful for spotting gradual drift vs week-over-week volatility

### Delta presentation

Every KPI table for recurring reports carries three delta columns where applicable:
- `Δ vs previous` (week or month)
- `Δ vs baseline`
- `Δ vs 4-week avg`

With directional indicators (↑ / ↓ / →) and significance flags.

---

## Alert thresholds

Weekly pulse triggers attention when:

| Metric | Threshold |
|--------|-----------|
| Affiliate click rate (overall) | Δ ≤ -5 % vs 4-week avg |
| Affiliate click rate (any major segment: `dogi` / `auto_redirect` / CIS / Global) | Δ ≤ -5 % vs 4-week avg |
| Eligible → hub reach | Δ ≤ -5 % vs 4-week avg |
| Post-hub return (either region) | Δ ≤ -5 % vs 4-week avg |
| `events.params` coverage | drops unexpectedly (regression) |
| `build_app` coverage | drops unexpectedly (regression) |
| `edge_ambiguous_build` share | rises unexpectedly (would shrink monotonically in normal state) |

Crossing a threshold triggers a recommended ad-hoc drill (either a manual deep dive on the affected segment or an expedited monthly deep run).

---

## Outputs

Each recurring report produces:
- Structured metrics (JSON / parquet) for longitudinal database
- Human-readable HTML report (per `specs/output/report_structure.md`)
- Metrics diff page highlighting deltas
- Alert summary (which thresholds were crossed)

Store all under `reports/YYYY/MM/{report_id}/` with predictable paths for cross-referencing.
