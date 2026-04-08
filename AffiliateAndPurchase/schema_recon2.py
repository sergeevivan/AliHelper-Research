"""
Deep schema recon — phase 2.
Focuses on:
1. event type distribution
2. non-null querySk values
3. URL patterns (homepage vs product vs other)
4. clients field completeness (guest_id, client_version, browser)
5. Mixpanel API probe
"""

import os, json, pprint
from datetime import datetime, timezone
from dotenv import load_dotenv
import sshtunnel
import pymongo
from bson import ObjectId
import requests
from requests.auth import HTTPBasicAuth

load_dotenv()

SSH_HOST   = os.getenv("MONGO_SSH_HOST")
SSH_USER   = os.getenv("MONGO_SSH_USER")
DB_HOST    = os.getenv("MONGO_DB_HOST")
DB_PORT    = int(os.getenv("MONGO_DB_PORT", 27017))
LOCAL_PORT = int(os.getenv("MONGO_LOCAL_PORT", 27018))
DB_NAME    = os.getenv("MONGO_DB_NAME")
MONGO_USER = os.getenv("MONGO_USER")
MONGO_PASS = os.getenv("MONGO_PASSWORD")
AUTH_DB    = os.getenv("MONGO_AUTH_DB", "admin")

MP_ACCOUNT = os.getenv("MIXPANEL_SERVICE_ACCOUNT")
MP_SECRET  = os.getenv("MIXPANEL_SECRET")
MP_PROJECT = os.getenv("MIXPANEL_PROJECT_ID")
MP_BASE    = os.getenv("MIXPANEL_BASE_URL")

AH_SKS = {"_c36PoUEj", "_d6jWDbY", "_AnTGXs", "_olPBn9X", "_dVh6yw5"}

def oid_from_date(dt):
    ts = int(dt.timestamp())
    return ObjectId(f"{ts:08x}0000000000000000")

DATE_A_START = datetime(2026, 3, 6,  tzinfo=timezone.utc)
DATE_A_END   = datetime(2026, 4, 2, 23, 59, 59, tzinfo=timezone.utc)

def print_section(title):
    print(f"\n{'='*60}")
    print(f"  {title}")
    print(f"{'='*60}")

def run_mongo():
    oid_start = oid_from_date(DATE_A_START)
    oid_end   = oid_from_date(DATE_A_END)

    print("Opening SSH tunnel...")
    with sshtunnel.SSHTunnelForwarder(
        SSH_HOST, ssh_username=SSH_USER,
        remote_bind_address=(DB_HOST, DB_PORT),
        local_bind_address=("127.0.0.1", LOCAL_PORT),
    ) as tunnel:
        client = pymongo.MongoClient(
            f"mongodb://{MONGO_USER}:{MONGO_PASS}@127.0.0.1:{LOCAL_PORT}/{AUTH_DB}",
            serverSelectionTimeoutMS=10000, directConnection=True,
        )
        db = client[DB_NAME]
        events = db["events"]
        clients_col = db["clients"]

        # ── 1. Event type distribution ────────────────────────────────
        print_section("1. Event type distribution in Problem A window")
        pipeline = [
            {"$match": {"_id": {"$gte": oid_start, "$lte": oid_end}}},
            {"$group": {"_id": "$type", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}},
        ]
        type_dist = list(events.aggregate(pipeline, allowDiskUse=True))
        for r in type_dist:
            print(f"  type={r['_id']!r:30s}  count={r['count']:>12,}")

        # ── 2. Non-null querySk values ────────────────────────────────
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
        qsk_dist = list(events.aggregate(pipeline2, allowDiskUse=True))
        print(f"Distinct querySk values (top 30):")
        for r in qsk_dist:
            is_ours = "OUR" if r["_id"] in AH_SKS else "---"
            print(f"  [{is_ours}] sk={r['_id']!r:20s}  count={r['count']:>10,}")

        total_with_qsk = sum(r["count"] for r in qsk_dist)
        print(f"\nTotal events with non-null querySk (top 30 sum): {total_with_qsk:,}")

        our_sk_count = sum(r["count"] for r in qsk_dist if r["_id"] in AH_SKS)
        print(f"Events with OUR sk: {our_sk_count:,}")

        # ── 3. URL patterns — homepage vs product vs other ────────────
        print_section("3. URL structure — hostname patterns (sample 500)")
        from urllib.parse import urlparse
        import re

        url_types = {"homepage": 0, "product": 0, "other": 0}
        sample_other_urls = []

        for doc in events.find(
            {"_id": {"$gte": oid_start, "$lte": oid_end}},
            {"payload.url": 1, "payload.productId": 1}
        ).limit(500):
            payload = doc.get("payload") or {}
            url = payload.get("url") or ""
            product_id = payload.get("productId")

            if product_id:
                url_types["product"] += 1
                continue
            try:
                parsed = urlparse(url)
                path = parsed.path.strip("/")
                if path == "":
                    url_types["homepage"] += 1
                else:
                    url_types["other"] += 1
                    if len(sample_other_urls) < 20:
                        sample_other_urls.append(parsed.path)
            except:
                url_types["other"] += 1

        print(f"In 500-doc sample:")
        for k, v in url_types.items():
            print(f"  {k}: {v}")
        print("Sample 'other' paths:")
        for p in sample_other_urls:
            print(f"  {p!r}")

        # ── 4. Eligible events count (product + homepage) ─────────────
        print_section("4. Eligible events — product pages (productId not null)")
        product_count = events.count_documents({
            "_id": {"$gte": oid_start, "$lte": oid_end},
            "payload.productId": {"$ne": None}
        })
        print(f"Events with productId (product pages): {product_count:,}")

        # Distinct users with product pages
        pipeline_users = [
            {"$match": {
                "_id": {"$gte": oid_start, "$lte": oid_end},
                "payload.productId": {"$ne": None}
            }},
            {"$group": {"_id": "$guest_id"}},
            {"$count": "total"},
        ]
        res = list(events.aggregate(pipeline_users, allowDiskUse=True))
        print(f"Distinct users with product page visits: {res[0]['total']:,}" if res else "N/A")

        # ── 5. Clients collection — field completeness ─────────────────
        print_section("5. clients — field completeness on 100 random docs")
        client_docs = list(clients_col.find({}).limit(100))
        fields_seen = {}
        for doc in client_docs:
            for k in doc.keys():
                fields_seen[k] = fields_seen.get(k, 0) + 1
        print("Fields and how many of 100 docs have them:")
        for k, v in sorted(fields_seen.items(), key=lambda x: -x[1]):
            print(f"  {k}: {v}/100")

        # Sample a doc with guest_id
        print("\nSample client doc with guest_id:")
        gdoc = clients_col.find_one({"guest_id": {"$exists": True}})
        if gdoc:
            print(json.dumps(gdoc, indent=2, default=str))
        else:
            print("  NOT FOUND — guest_id may not exist in clients!")

        # Sample a doc with client_version
        print("\nSample client doc with client_version:")
        cvdoc = clients_col.find_one({"client_version": {"$exists": True}})
        if cvdoc:
            print(json.dumps(cvdoc, indent=2, default=str))
        else:
            print("  NOT FOUND — client_version may not exist in clients!")

        # ── 6. How events link to clients ─────────────────────────────
        print_section("6. Linking events -> clients (via what field?)")
        # events has guest_id (ObjectId), clients sample showed _id but no guest_id
        # Check if clients._id == events.guest_id?
        sample_events = list(events.find(
            {"_id": {"$gte": oid_start, "$lte": oid_end}},
            {"guest_id": 1}
        ).limit(5))
        for ev in sample_events:
            gid = ev["guest_id"]
            # Try by _id
            c = clients_col.find_one({"_id": gid})
            c2 = clients_col.find_one({"guest_id": str(gid)})
            c3 = clients_col.find_one({"guest_id": gid})
            print(f"  guest_id={gid} -> clients._id match={c is not None} clients.guest_id(str)={c2 is not None} clients.guest_id(oid)={c3 is not None}")

        # ── 7. guestStateHistory — guest_id type ──────────────────────
        print_section("7. guestStateHistory — guest_id values sample")
        gsh = db["guestStateHistory"]
        oid_start_gsh = oid_from_date(DATE_A_START)
        for doc in gsh.find({"_id": {"$gte": oid_start_gsh}}, {"guest_id": 1, "value": 1, "domain": 1}).limit(5):
            print(f"  guest_id={doc.get('guest_id')!r} (type={type(doc.get('guest_id')).__name__})  value={doc.get('value')}  domain={doc.get('domain')!r}")

        # Check domain distribution
        print_section("8. guestStateHistory — domain distribution (value=true)")
        pipeline_domains = [
            {"$match": {
                "_id": {"$gte": oid_from_date(DATE_A_START), "$lte": oid_from_date(DATE_A_END)},
                "value": True
            }},
            {"$group": {"_id": "$domain", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}},
            {"$limit": 20},
        ]
        domains = list(gsh.aggregate(pipeline_domains, allowDiskUse=True))
        for r in domains:
            print(f"  domain={r['_id']!r:30s}  count={r['count']:>10,}")

        client.close()

def probe_mixpanel():
    print_section("MIXPANEL — API probe")

    # Test: get event counts for Affiliate Click in the Problem A window
    # Using Insights API / query endpoint
    # Europe/Moscow: 2026-03-06 = UTC+3, so 2026-03-06 00:00 MSK = 2026-03-05 21:00 UTC
    # But we'll query in MSK dates as Mixpanel expects project timezone

    # Use Data Export JQL endpoint
    url = f"{MP_BASE}/api/2.0/jql"

    script = """
    function main() {
      return Events({
        from_date: '2026-03-06',
        to_date: '2026-04-02',
        event_selectors: [
          {event: 'Affiliate Click'},
          {event: 'Purchase'},
          {event: 'Purchase Completed'}
        ]
      })
      .groupBy(['name'], mixpanel.reducer.count())
      .filter(function(r) { return true; });
    }
    """

    resp = requests.post(
        url,
        auth=HTTPBasicAuth(MP_ACCOUNT, MP_SECRET),
        data={"script": script, "project_id": MP_PROJECT},
        timeout=30,
    )
    print(f"JQL status: {resp.status_code}")
    if resp.ok:
        data = resp.json()
        for row in data:
            print(f"  event={row.get('key')}  count={row.get('value'):,}")
    else:
        print(f"Error: {resp.text[:500]}")

    # Also test Data Export API
    print("\nTesting Data Export API (last 2 days, Affiliate Click):")
    export_url = f"https://data-eu.mixpanel.com/api/2.0/export"
    resp2 = requests.get(
        export_url,
        auth=HTTPBasicAuth(MP_ACCOUNT, MP_SECRET),
        params={
            "project_id": MP_PROJECT,
            "from_date": "2026-04-01",
            "to_date": "2026-04-02",
            "event": '["Affiliate Click"]',
        },
        timeout=30, stream=True
    )
    print(f"Export status: {resp2.status_code}")
    lines = []
    for line in resp2.iter_lines():
        if line:
            lines.append(json.loads(line))
            if len(lines) >= 3:
                break
    if lines:
        print(f"Sample event (first):")
        pprint.pprint(lines[0])
        print(f"Properties keys: {sorted(lines[0].get('properties', {}).keys())}")
    else:
        print("No data or error")

if __name__ == "__main__":
    run_mongo()
    probe_mixpanel()
