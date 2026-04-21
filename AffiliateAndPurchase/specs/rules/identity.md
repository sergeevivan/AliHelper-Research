# Identity, Enrichment & Matching Rules

## Canonical user identity

| Context | Field | Maps to |
|---------|-------|---------|
| Primary identity | `guests._id` | = Mixpanel `$user_id` |
| From events | `events.guest_id` | → `guests._id` |
| From clients | `clients.guest_id` | → `guests._id` |
| From guestStateHistory | `guestStateHistory.guest_id` | → `guests._id` |

**NEVER use `clients._id` as a user identifier.**

One user can have multiple client records. Always join through `guest_id`.

---

## Client enrichment

Use `clients` only to add context to `events`:
- browser, user_agent, os
- city, country
- client_version, IP context

Treat `clients` as client-state history, not as a canonical user table.

---

## Purchase matching

`Purchase Completed` often does not contain `order_id`. Do NOT rely on it.

### Primary matching approach

1. Match by canonical user identity (`guests._id`)
2. Match by time proximity: up to **10 minutes**

### Sensitivity checks

Run with:
- narrower window (e.g. 5 min)
- wider window (e.g. 15 min)

Document how ambiguous matches (multiple candidates) are handled.

---

## Required definitions to lock before analysis

Before starting analysis, explicitly define and document:

1. Canonical user identity
2. Global direct affiliate state (sk-based)
3. CIS direct affiliate state (UTM-based from `events.payload.url`)
4. CIS proxy-return fallback definition
5. Eligible opportunity (product pages only — per flow-specific rules)
6. Attribution window (72h)
7. Client enrichment rule
8. Global overwrite rule (foreign sk / af after our sk)
9. CIS overwrite rule (foreign UTM after our UTM)
10. Mature purchase cohort definition
11. Purchase matching rule (10-min window)
12. Global direct-return evidence rule
13. CIS direct-return evidence rule (UTM in URL)
14. CIS proxy-return rule (≤120s fallback)
15. Latest delivered config rule (latest `guestStateHistory` before event)
16. Routing-based regional split rule
