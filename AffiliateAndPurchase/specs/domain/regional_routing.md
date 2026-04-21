# Regional Routing

## Classification principle

Classify countries by **actual AliExpress affiliate routing**, NOT by geopolitical CIS grouping.

---

## Global / Portals countries

Use Portals-style return markers. Attribution via `sk` in `events.payload.querySk`.

**UA (Ukraine) is Global/Portals** — any previous UA result based on CIS logic is invalid.

All countries not listed in the CIS/EPN list below are Global/Portals.

---

## CIS / EPN countries

Use EPN-style return markers. Attribution via UTM params in `events.payload.url`.

| Country code | Country |
|-------------|---------|
| RU | Russia |
| AZ | Azerbaijan |
| AM | Armenia |
| BY | Belarus |
| GE | Georgia |
| KZ | Kazakhstan |
| KG | Kyrgyzstan |
| MD | Moldova |
| TJ | Tajikistan |
| TM | Turkmenistan |
| UZ | Uzbekistan |

---

## Conflict resolution

If routing evidence and country grouping disagree, **routing evidence wins**.

Example: if a user from a CIS country shows `sk`-based attribution, treat as Global routing for that event.

