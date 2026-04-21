# AliHelper — Root-Cause Research: Affiliate Activation & Purchase Attribution

## Role

Act as a senior product/data investigator. Run reproducible investigations using MongoDB + Mixpanel. Produce clear definitions, reproducible code, segmentation, ranked root causes, impact estimates, and a rebuilt HTML report.

## Two independent problems — do NOT mix

- **Problem A** — why many AliExpress users don't generate `Affiliate Click`
- **Problem B** — why `Purchase Completed` count exceeds commission-bearing `Purchase`

## Specs index

Read the relevant spec BEFORE starting work. Specs are in `specs/`.

| Area | File | When to read |
|------|------|--------------|
| Attribution models (Global sk + CIS af/UTM) | [`specs/domain/attribution.md`](specs/domain/attribution.md) | Any affiliate logic |
| Regional routing (country → Global/CIS) | [`specs/domain/regional_routing.md`](specs/domain/regional_routing.md) | Country classification |
| Browser flows (build_app → DOGI / auto-redirect) | [`specs/domain/browser_flows.md`](specs/domain/browser_flows.md) | Redirect/eligibility logic |
| Data sources (MongoDB, Mixpanel, fields) | [`specs/domain/data_sources.md`](specs/domain/data_sources.md) | Any query work |
| Problem A methodology | [`specs/problems/problem_a.md`](specs/problems/problem_a.md) | Working on Problem A |
| Problem A — non-activator deep-dive (A7) | [`specs/problems/problem_a_non_activator.md`](specs/problems/problem_a_non_activator.md) | Qualitative non-activator analysis |
| Problem B methodology | [`specs/problems/problem_b.md`](specs/problems/problem_b.md) | Working on Problem B |
| Identity, enrichment & matching | [`specs/rules/identity.md`](specs/rules/identity.md) | User joins, purchase matching |
| Analysis periods & incidents | [`specs/rules/analysis_periods.md`](specs/rules/analysis_periods.md) | Date ranges |
| Anti-mistakes | [`specs/rules/anti_mistakes.md`](specs/rules/anti_mistakes.md) | Before any analysis |
| Data quality caveats | [`specs/rules/caveats.md`](specs/rules/caveats.md) | Interpreting results |
| Recurring reports (pulse + deep) | [`specs/workflows/recurring_reports.md`](specs/workflows/recurring_reports.md) | Periodic reporting cadence |
| Report structure | [`specs/output/report_structure.md`](specs/output/report_structure.md) | Building output |
| Instrumentation recs | [`specs/output/instrumentation.md`](specs/output/instrumentation.md) | Recommendations |

## Hard rules (always active)

### Identity
- Canonical user identity: `guests._id` = Mixpanel `$user_id`
- Join via `guest_id`, NEVER use `clients._id`

### Data sources
- MongoDB `events` = behavioral source of truth
- Mixpanel = only for `Affiliate Click`, `Purchase`, `Purchase Completed`

### Attribution — two models, not one
- **Global:** `sk` whitelist match. Source priority: `events.params.sk` → `events.payload.querySk` → parse `events.payload.url`.
- **CIS:** two mutually exclusive patterns on `aliexpress.ru`:
  - **Pattern A (`af`-based):** `af=*_7685` (+ typically `utm_medium=cpa`)
  - **Pattern B (full-UTM):** `utm_source=aerkol` + `utm_medium=cpa` + `utm_campaign=*_7685`
  - Source priority: `events.params.<name>` → parse `events.payload.url`
- AliHelper EPN cabinet id: **`7685`** — **CIS only, NOT Global**. Stable through ref-link migration. UA routes as Global → `_7685` should not appear on UA users; if it does, flag as anomaly.
- Attribution window: 72 hours before `Purchase Completed` (server-side `events` only; client-side `last_sk`/`last_af`/`last_utm_*` on `Purchase Completed` have NO window limit and are NOT authoritative)

### Regional routing
- Classify by actual affiliate routing, NOT geopolitical grouping
- **UA = Global/Portals**, not CIS/EPN
- Domain-based: `aliexpress.ru` = CIS routing, other AliExpress domains = Global

### Eligibility
- Only **product pages** are eligible for affiliate activation (no homepages)
- DOGI: product pages with `productId`; Auto-redirect: URLs matching `checkListUrls` patterns

### Flow lineage
- Primary: `clients.build_app` (`chrome` → DOGI, `firefox`/`edge` → auto-redirect)
- Fallback (old clients without `build_app`): infer from browser UA
- Edge browser without `build_app` = `edge_ambiguous_build` — keep as separate segment, do NOT pool into either flow

### Labels
Every finding must carry one of: `GLOBAL_DIRECT`, `CIS_DIRECT_AF`, `CIS_DIRECT_UTM`, `CIS_PARTIAL_UTM`, `CIS_PROXY`
(`CIS_DIRECT_AF` + `CIS_DIRECT_UTM` can be aggregated to `CIS_DIRECT` in summaries)

### Cache
Reuse existing extracts. Recompute only invalidated derived layers.

### Security
No secrets in code — use `.env` and environment variables.

## MongoDB quick reference

- Database: `alihelper`
- Collections: `guests`, `events`, `clients`, `guestStateHistory`
- `events` has no index on `created` — use `_id`-based date filtering
- Use `allowDiskUse: true` for heavy aggregations
- Mixpanel timezone: `Europe/Moscow` (UTC+3) — convert explicitly
