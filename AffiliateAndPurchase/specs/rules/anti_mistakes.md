# Anti-Mistakes

## Do NOT

- Use Mixpanel `AliExpress Activity` as the behavioral denominator
- Mix Problem A and Problem B
- Use `clients._id` as user identity
- Treat `clients` as the primary behavior log
- Use one unified attribution model for Global and CIS
- Classify countries by geopolitical grouping instead of actual affiliate routing
- Classify UA as CIS/EPN (UA is Global/Portals)
- Assume one user has one client state
- Require `order_id` for purchase matching
- Treat lack of checkout-path `events` as proof of no user activity (consider excluded `noLogUrls`)
- Confuse `Affiliate Click` with successful return to AliExpress
- Treat `guestStateHistory` as evidence of actual hub usage or redirect execution
- Use Global `sk`-based logic for CIS attribution (use UTM from `events.payload.url`)
- Pool auto-redirect (Firefox/Edge) and DOGI (Chrome-like) as one mechanism
- Interpret very fresh Purchase Completed → Purchase gaps as true attribution loss (could be delayed postback)
- Discard existing cache unless necessary

## Cache reuse

Treat methodology corrections as incremental, not a full data reset.

Reuse previously extracted raw datasets:
- MongoDB `events`
- Mixpanel `Affiliate Click`, `Purchase`, `Purchase Completed`
- `guestStateHistory`
- any prepared intermediate extracts

Recompute only derived layers that depended on:
- incorrect CIS sk-based assumptions
- incorrect UA region split

Before running, state:
- which cached artifacts can be reused
- which tables/aggregations must be recomputed
- which data must be re-extracted

