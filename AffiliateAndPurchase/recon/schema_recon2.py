"""
Deep schema recon — phase 2.
Focuses on event type distribution, querySk values, URL patterns,
clients field completeness, and Mixpanel API probe.
"""

import json, pprint
from urllib.parse import urlparse

import requests
from requests.auth import HTTPBasicAuth

from src.db import mongo_tunnel
from src.utils import oid_from_dt, print_section
from src.config import (
    A_START, A_END, OUR_SKS,
    MP_ACCOUNT, MP_SECRET, MP_PROJECT, MP_BASE,
)


def run_mongo():
    oid_start = oid_from_dt(A_START)
    oid_end   = oid_from_dt(A_END)

    with mongo_tunnel() as db:
        events = db["events"]
        clients_col = db["clients"]

        print_section("1. Event type distribution")
        pipeline = [
            {"$match": {"_id": {"$gte": oid_start, "$lte": oid_end}}},
            {"$group": {"_id": "$type", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}},
        ]
        for r in events.aggregate(pipeline, allowDiskUse=True):
            print(f"  type={r['_id']!r:30s}  count={r['count']:>12,}")

        print_section("2. Non-null querySk — distribution")
        pipeline2 = [
            {"$match": {
                "_id": {"$gte": oid_start, "$lte": oid_end},
                "payload.querySk": {"$ne": None, "$exists": True}
            }},
            {"$group": {"_id": "$payload.querySk", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}},
            {"$limit": 30},
        ]
        for r in events.aggregate(pipeline2, allowDiskUse=True):
            is_ours = "OUR" if r["_id"] in OUR_SKS else "---"
            print(f"  [{is_ours}] sk={r['_id']!r:20s}  count={r['count']:>10,}")

        print_section("3. URL structure (sample 500)")
        url_types = {"homepage": 0, "product": 0, "other": 0}
        sample_other = []
        for doc in events.find(
            {"_id": {"$gte": oid_start, "$lte": oid_end}},
            {"payload.url": 1, "payload.productId": 1}
        ).limit(500):
            payload = doc.get("payload") or {}
            if payload.get("productId"):
                url_types["product"] += 1
            else:
                try:
                    path = urlparse(payload.get("url", "")).path.strip("/")
                    if path == "":
                        url_types["homepage"] += 1
                    else:
                        url_types["other"] += 1
                        if len(sample_other) < 20:
                            sample_other.append(path)
                except:
                    url_types["other"] += 1
        for k, v in url_types.items():
            print(f"  {k}: {v}")

        print_section("4. clients field completeness (100 docs)")
        for k, v in sorted(
            {k: sum(1 for d in list(clients_col.find({}).limit(100)) if k in d)
             for k in set().union(*(d.keys() for d in list(clients_col.find({}).limit(100))))}.items(),
            key=lambda x: -x[1]):
            print(f"  {k}: {v}/100")

        print_section("5. guestStateHistory domains (value=true)")
        pipeline_d = [
            {"$match": {"_id": {"$gte": oid_from_dt(A_START), "$lte": oid_from_dt(A_END)}, "value": True}},
            {"$group": {"_id": "$domain", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}}, {"$limit": 20},
        ]
        for r in db["guestStateHistory"].aggregate(pipeline_d, allowDiskUse=True):
            print(f"  domain={r['_id']!r:30s}  count={r['count']:>10,}")


def probe_mixpanel():
    print_section("MIXPANEL — API probe")
    export_url = "https://data-eu.mixpanel.com/api/2.0/export"
    resp = requests.get(export_url, auth=HTTPBasicAuth(MP_ACCOUNT, MP_SECRET),
                        params={"project_id": MP_PROJECT, "from_date": "2026-04-01",
                                "to_date": "2026-04-02", "event": '["Affiliate Click"]'},
                        timeout=30, stream=True)
    print(f"Export status: {resp.status_code}")
    lines = []
    for line in resp.iter_lines():
        if line:
            lines.append(json.loads(line))
            if len(lines) >= 3:
                break
    if lines:
        print("Sample event:")
        pprint.pprint(lines[0])


if __name__ == "__main__":
    run_mongo()
    probe_mixpanel()
