# Report Structure

This spec covers both one-off investigation reports and recurring pulse / deep reports (see [`specs/workflows/recurring_reports.md`](../workflows/recurring_reports.md) for cadence and comparison methodology).

---

## Required sections (in order)

### 1. Report metadata
- Report ID (e.g. `deep_2026-03-20_to_2026-04-16` or `pulse_2026-04-14_to_2026-04-20`)
- Report type: one-off / weekly pulse / monthly deep
- Period boundaries (UTC)
- Generated date
- Baseline report ID for comparison (if recurring)

### 2. Coverage snapshot
Report % coverage of new fields in the analyzed period:
- % of events with `events.params`
- % of clients with `build_app`
- % of `Purchase Completed` with new fields (`last_sk`, `is_CIS`, etc.)
- % of events where attribution params came from each source tier (`events.params` / `querySk` / URL parsing)

This lets the reader judge how much the report depends on fallbacks.

### 3. Definitions locked
State all definitions (see `specs/rules/identity.md` for the full list).

### 4. Data quality caveats
List risks and limitations (see `specs/rules/caveats.md`).

### 5. Findings for Problem A
With:
- decomposition tables (A1-A6)
- segmentation results
- explicit region labels (`GLOBAL_DIRECT`, `CIS_DIRECT_AF`, `CIS_DIRECT_UTM`, `CIS_PARTIAL_UTM`, `CIS_PROXY`)
- **A7 non-activator deep-dive** (full in monthly/one-off; aggregate top-segments in weekly pulse) — see `specs/problems/problem_a_non_activator.md`

### 6. Findings for Problem B
*(Skip in weekly pulse — not reliable on 7-day window)*

With:
- decomposition tables (B1-B6)
- reason-code classification (see `specs/problems/problem_b.md`)
- explicit region labels

### 7. Ranked root causes by impact
For each cause:

| Field | Content |
|-------|---------|
| Explanation | What the cause is |
| How measured | Method used |
| Affected share | % of relevant population |
| Estimated impact | Revenue / opportunity cost |
| Confidence | High / Medium / Low |
| Observability label | `GLOBAL_DIRECT` / `CIS_DIRECT_AF` / `CIS_DIRECT_UTM` / `CIS_PARTIAL_UTM` / `CIS_PROXY` |
| Recommended fix | Specific action |

### 8. Unexplained remainder
Explicitly show what remains unexplained after all classification.

### 9. Longitudinal comparison
*(Recurring reports only)*

- Current period vs previous same-type report
- Current period vs baseline
- Current period vs 4-week trailing average
- Delta with direction indicator (up/down/flat) and significance flag
- Highlight metrics exceeding alert thresholds

### 10. Recommended fixes
Split into:
- Quick wins
- Medium-term product changes
- Tracking/instrumentation changes
- Follow-up experiments/analyses

### 11. Reproducible code
Provide a reproducible pipeline:
- extraction
- transformation
- matching
- enrichment
- classification
- final metrics/tables

### 12. HTML report
Build HTML report from the analysis results.

---

## Visual & UX requirements for the HTML report

### Layout
- **Two-column layout**: sticky sidebar (≤240 px) with section links + scrollable main content
- Max content width: 1400 px
- Responsive: sidebar collapses on narrow viewports
- Smooth scroll on anchor links

### Typography
- System font stack (`-apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif`)
- Comfortable line-height (1.65)
- Clear size hierarchy: h1 > h2 > h3 > h4
- Code in monospace with subtle background

### Color palette
| Role | Hex |
|------|-----|
| Primary (blue) | `#2563eb` |
| Success (green) | `#059669` |
| Warning (amber) | `#d97706` |
| Danger (red) | `#dc2626` |
| Neutral text | `#374151` |
| Muted text | `#6b7280` |
| Surface | `#f9fafb` |
| Border | `#e5e7eb` |

### KPI cards
- Grid of 3–4 cards per section
- Large bold value (32 px+), colored by sentiment (green/amber/red)
- Small muted label below
- Subtle border and shadow
- For recurring reports: small delta indicator vs previous / baseline in the corner

### Funnel visualization
- Each funnel step rendered as a horizontal bar whose width scales with its value relative to step 1
- Bar color: green for high rate (>80%), amber for medium (50–80%), red for low (<50%)
- Labels: step name left, count + conversion rate right

### Tables
- Sticky `<thead>` on scroll
- Alternating row backgrounds (`#ffffff` / `#f9fafb`)
- Row hover highlight
- **Rate/percentage columns**: color-coded cell background by value range
  - ≥ 80 %: light green background
  - 50–79 %: light amber background
  - < 50 %: light red background
- Numeric columns right-aligned
- Caption as styled `<h4>` above the table
- For recurring reports: optional delta column (Δ vs previous) with +/- coloring

### Callout boxes
- Four kinds: `critical` (red), `warning` (amber), `info` (blue), `finding` (green)
- Left border accent + matching background tint
- Unicode icon prefix: ❗ critical, ⚠️ warning, ℹ️ info, ✅ finding

### Attribution label badges
- `GLOBAL_DIRECT`: blue pill
- `CIS_DIRECT_AF`: amber pill
- `CIS_DIRECT_UTM`: orange pill
- `CIS_PARTIAL_UTM`: light amber pill
- `CIS_PROXY`: grey pill
- Consistent size, monospace font inside

### Sidebar navigation
- Fixed position on left while scrolling
- Active-section highlight (updated via `IntersectionObserver` JS)
- Section numbers matching the report numbering
- Collapsible sub-items for long sections
