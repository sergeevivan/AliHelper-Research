# Problem A — A7: Non-Activator Deep-Dive

Qualitative and quantitative analysis of users who did not activate the AliHelper affiliate link during the analyzed period. Extends Problem A main funnel (A1-A6).

Included in **both** weekly pulse (aggregate top-segments only) and monthly/one-off deep (full version).

---

## Cohort definition

**Non-activator = user with AliExpress activity in the period AND no `Affiliate Click` in the period.**

Include both sub-groups:

| Sub-group | Definition | Why include |
|-----------|------------|------------|
| With eligible opportunities | Had ≥1 eligible product-page visit (DOGI `productId` or `checkListUrls` match) AND no `Affiliate Click` | Core target — something blocked activation |
| No eligible opportunities | Had AliExpress activity but never hit an eligible page | Explains part of gap via A1 (ineligible traffic); profile may differ systematically |

Report these two sub-groups separately in all segmentations — their drivers differ (structural flow issue vs traffic mix).

### Additional split

| Split | Definition |
|-------|------------|
| Never-activator (lifetime) | No `Affiliate Click` ever observed for this `guest_id` |
| Partial-activator | Has `Affiliate Click` before or after the period (or on other days within period) but missed opportunities within the analyzed window |

Never-activator = more likely structural (config, flow, geo). Partial-activator = more likely situational (cooldown, short session, UX).

---

## Session reconstruction

Sessions are reconstructed from `events` for per-session behavioral metrics.

**Boundary:** gap of ≥30 minutes between consecutive events for the same `guest_id` = new session.

Per-session metrics:
- Duration (first event ts → last event ts)
- Total event count
- Distinct page types visited
- Product-page count (DOGI-eligible)
- `checkListUrls` match count (auto-redirect eligible)
- Time-of-day / day-of-week
- Entry page type (first event)
- Exit page type (last event)

---

## Profile segmentation (who they are)

Level A aggregates only (no individual user archetypes).

| Dimension | Source | Notes |
|-----------|--------|-------|
| Country | `clients.country` | top-N + "other" |
| City | `clients.city` | optional, top-N for largest countries only |
| Language | `clients.user_agent` / browser locale | if extractable |
| Browser family | `clients.browser` UA | firefox / chrome / edge / yandex / opera / safari / other |
| `build_app` | `clients.build_app` | chrome / firefox / edge / missing |
| Flow lineage | derived | `dogi` / `auto_redirect` / `edge_ambiguous_build` / `unknown_build` |
| Extension version | `clients.client_version` | top-N versions |
| OS | `clients.os` | |
| Region (by routing) | domain of AliExpress events | Global / CIS |
| Tenure | first-seen date vs period | new / returning (threshold: first-seen ≥ period_start) |
| Client diversity | distinct `clients` count per `guest_id` | single / multi |

---

## Behavior (what they do)

Level A aggregates only.

### Session metrics (non-activators vs activators — comparative)

Compare non-activator distributions against activator distributions for the same period.

| Metric | Compare |
|--------|---------|
| Session duration (median, p25/p75, p95) | non-activator vs activator |
| Event count per session | distribution |
| Product-page count per session | distribution |
| Distinct product subtypes visited | distribution |
| Session depth (pages per session) | distribution |

### Page mix

For the non-activator cohort, aggregate page-type mix:
- % product page (DOGI-eligible)
- % `checkListUrls` match (auto-redirect eligible)
- % search
- % category
- % cart
- % checkout
- % homepage
- % account / other

### Entry / exit patterns

Top-N entry page types. Top-N exit page types. "Bounce" rate (single-event session).

### Eligible-but-no-activation counts

- **DOGI lineage:** for each session, count product-pages visited. Aggregate distribution. If users see many product-pages per session without activation → likely UX / cooldown, not absence of opportunity.
- **Auto-redirect lineage:** for each session, count `checkListUrls` matches. Same logic.

### Time-of-day / day-of-week

Heatmap: non-activator event volume by hour × weekday. Compare to activator heatmap — systemic differences flag geo/load issues.

---

## Hypotheses (why they didn't activate)

Each hypothesis is paired with a **measurable proxy** so the analysis can estimate its contribution.

| Hypothesis | Proxy / how to check |
|-----------|---------------------|
| DOGI cooldown (30-60 min) active | Time since last `Affiliate Click` < 30-60 min; or session shorter than cooldown window |
| Auto-redirect cooldown (30 min) active | Time since last affiliate activation attempt < 30 min |
| No usable hub in latest config | Latest `guestStateHistory.value` = false before the eligible visit |
| User didn't interact with DOGI coin trigger | DOGI lineage; product-pages visited but short session / no other event signals |
| Auto-redirect URL match but redirect didn't fire | Auto-redirect lineage; `checkListUrls` matched but no subsequent hub visit within expected time |
| Geo-specific hub failure | Non-activator rate substantially higher in country X vs baseline |
| Extension version bug | Non-activator rate substantially higher for version V vs baseline |
| UI issue for rare product subtypes | Non-activator rate higher for SSR / `group.*` / `sale.*` / `play.*` vs standard `/item/*` |
| Build/browser mismatch | `edge_ambiguous_build` anomalies (both DOGI-like and auto-redirect-like signals mixed) |
| Ineligible traffic only | Sub-group "no eligible opportunities" — fully explained by A1 |

For each hypothesis, report the **share of the non-activator cohort** it plausibly explains (with uncertainty bounds given partial observability — see `specs/rules/caveats.md`).

---

## Mandatory tables

### Table 1: Cohort sizing
- Total AliExpress-active users in period
- Non-activators, with split: with eligible / no eligible
- Non-activators, with split: never / partial
- % of total

### Table 2: Profile distribution (non-activator vs activator)
For each profile dimension: distribution on both cohorts, delta.

### Table 3: Non-activator rate × segment
- Browser × country
- `build_app` × page type
- Flow lineage × country
- Extension version × region
- Hub × country

Cells highlight where non-activator rate substantially exceeds overall baseline.

### Table 4: Session-length / depth distribution
Non-activator vs activator — overlaid distributions (duration, event count).

### Table 5: Top-N non-activator cohorts by volume
Rank by absolute user count. For each: share of total gap, dominant hypothesis candidates, suggested drill-down.

### Table 6: Hypothesis attribution
Each hypothesis → share of non-activator cohort it plausibly explains + confidence.

---

## Weekly pulse subset

In weekly pulse, include only:
- Table 1 (cohort sizing)
- Table 3 (non-activator rate × top segments only: browser, country, flow lineage)
- Table 5 (top-5 cohorts)
- Delta vs previous week + 4-week avg for non-activator rate overall and per major segment

No hypothesis attribution, no session-depth distributions, no profile comparison in weekly pulse — those belong in monthly deep.

---

## Monthly deep additions

In monthly deep, include all six tables plus:
- A narrative summary (1 paragraph per top-5 cohort) stating dominant hypothesis and recommended next step
- Cross-report longitudinal chart: non-activator rate over last N monthly deeps
- Callout for any segment where non-activator rate has grown consistently over ≥3 consecutive monthly deeps

---

## Segmentations carried forward to ranked root causes

The hypothesis attribution from Table 6 feeds directly into report section "Ranked root causes by impact" (see `specs/output/report_structure.md`). Each attributed hypothesis becomes a candidate cause with its estimated affected share and observability label.
