# Report Structure

## Required sections (in order)

### 1. Definitions locked
State all definitions before analysis (see `specs/rules/identity.md` for the full list).

### 2. Data quality caveats
List risks and limitations (see `specs/rules/caveats.md`).

### 3. Findings for Problem A
With:
- decomposition tables
- segmentation results
- explicit region labels (`GLOBAL_DIRECT`, `CIS_DIRECT`, `CIS_PROXY`)

### 4. Findings for Problem B
With:
- decomposition tables
- reason-code classification (see `specs/problems/problem_b.md`)
- explicit region labels

### 5. Ranked root causes by impact
For each cause:

| Field | Content |
|-------|---------|
| Explanation | What the cause is |
| How measured | Method used |
| Affected share | % of relevant population |
| Estimated impact | Revenue / opportunity cost |
| Confidence | High / Medium / Low |
| Observability label | `GLOBAL_DIRECT` / `CIS_DIRECT` / `CIS_PROXY` |
| Recommended fix | Specific action |

### 6. Unexplained remainder
Explicitly show what remains unexplained after all classification.

### 7. Recommended fixes
Split into:
- Quick wins
- Medium-term product changes
- Tracking/instrumentation changes
- Follow-up experiments/analyses

### 8. Reproducible code
Provide a reproducible pipeline:
- extraction
- transformation
- matching
- enrichment
- classification
- final metrics/tables

### 9. HTML report
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

### Callout boxes
- Four kinds: `critical` (red), `warning` (amber), `info` (blue), `finding` (green)
- Left border accent + matching background tint
- Unicode icon prefix: ❗ critical, ⚠️ warning, ℹ️ info, ✅ finding

### Attribution label badges
- `GLOBAL_DIRECT`: blue pill
- `CIS_DIRECT`: amber pill
- `CIS_PROXY`: grey pill
- Consistent size, monospace font inside

### Sidebar navigation
- Fixed position on left while scrolling
- Active-section highlight (updated via `IntersectionObserver` JS)
- Section numbers matching the report numbering
- Collapsible sub-items for long sections
