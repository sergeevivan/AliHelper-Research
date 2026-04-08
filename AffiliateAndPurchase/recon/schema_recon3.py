"""Schema recon phase 3 — Purchase and Purchase Completed Mixpanel event schemas."""

import pprint

from src.db import mp_export


def sample_event(event_name, from_date="2026-03-20", to_date="2026-03-26", n=2):
    cache_key = f"_recon_{event_name.replace(' ', '_').lower()}"
    records = mp_export(event_name, from_date, to_date, cache_key)
    print(f"\n{event_name} — {len(records):,} records")
    for d in records[:n]:
        props = d.get("properties", {})
        print(f"  Keys: {sorted(props.keys())}")
        pprint.pprint(props)
        print()


if __name__ == "__main__":
    sample_event("Purchase", n=2)
    sample_event("Purchase Completed", n=2)
