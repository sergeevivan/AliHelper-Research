"""
Schema reconnaissance script.
Connects via SSH tunnel to MongoDB and samples each collection
to understand field structure, nesting, and available data.
"""

import os, json, pprint
from datetime import datetime, timezone
from dotenv import load_dotenv
import sshtunnel
import pymongo
from bson import ObjectId

load_dotenv()

SSH_HOST = os.getenv("MONGO_SSH_HOST")
SSH_USER = os.getenv("MONGO_SSH_USER")
DB_HOST  = os.getenv("MONGO_DB_HOST")
DB_PORT  = int(os.getenv("MONGO_DB_PORT", 27017))
LOCAL_PORT = int(os.getenv("MONGO_LOCAL_PORT", 27018))
DB_NAME  = os.getenv("MONGO_DB_NAME")
MONGO_USER = os.getenv("MONGO_USER")
MONGO_PASS = os.getenv("MONGO_PASSWORD")
AUTH_DB  = os.getenv("MONGO_AUTH_DB", "admin")

# ObjectId boundary for date filtering (avoid full collection scan)
def oid_from_date(dt: datetime) -> ObjectId:
    ts = int(dt.timestamp())
    return ObjectId(f"{ts:08x}0000000000000000")

DATE_A_START = datetime(2026, 3, 6,  tzinfo=timezone.utc)
DATE_A_END   = datetime(2026, 4, 2, 23, 59, 59, tzinfo=timezone.utc)

def flatten_keys(d, prefix="", depth=0, max_depth=4):
    """Recursively collect keys with type info."""
    result = {}
    if depth > max_depth or not isinstance(d, dict):
        return result
    for k, v in d.items():
        full_key = f"{prefix}.{k}" if prefix else k
        if isinstance(v, dict):
            result[full_key] = "object"
            result.update(flatten_keys(v, full_key, depth+1, max_depth))
        elif isinstance(v, list):
            result[full_key] = f"array[{type(v[0]).__name__ if v else 'empty'}]"
        else:
            result[full_key] = type(v).__name__
    return result

def sample_collection(col, n=5, filter_query=None):
    """Sample n docs and aggregate their keys."""
    q = filter_query or {}
    docs = list(col.find(q).limit(n))
    if not docs:
        return {}, []
    key_types = {}
    for doc in docs:
        kt = flatten_keys(doc)
        for k, t in kt.items():
            if k not in key_types:
                key_types[k] = set()
            key_types[k].add(t)
    return {k: list(v) for k, v in key_types.items()}, docs

def print_section(title):
    print(f"\n{'='*60}")
    print(f"  {title}")
    print(f"{'='*60}")

def run():
    print("Opening SSH tunnel...")
    with sshtunnel.SSHTunnelForwarder(
        SSH_HOST,
        ssh_username=SSH_USER,
        remote_bind_address=(DB_HOST, DB_PORT),
        local_bind_address=("127.0.0.1", LOCAL_PORT),
    ) as tunnel:
        print(f"Tunnel open on 127.0.0.1:{LOCAL_PORT}")

        client = pymongo.MongoClient(
            f"mongodb://{MONGO_USER}:{MONGO_PASS}@127.0.0.1:{LOCAL_PORT}/{AUTH_DB}",
            serverSelectionTimeoutMS=10000,
            directConnection=True,
        )
        db = client[DB_NAME]

        # ── Collection list ──────────────────────────────────────────
        print_section("Collections in alihelper")
        cols = db.list_collection_names()
        print(cols)

        # ── events ───────────────────────────────────────────────────
        print_section("events — counts and schema")
        events = db["events"]

        # Count in window via _id range
        oid_start = oid_from_date(DATE_A_START)
        oid_end   = oid_from_date(DATE_A_END)
        count_window = events.count_documents({"_id": {"$gte": oid_start, "$lte": oid_end}})
        print(f"Events in Problem A window (2026-03-06 to 2026-04-02): {count_window:,}")

        # Sample recent events
        key_types, docs = sample_collection(
            events, n=3,
            filter_query={"_id": {"$gte": oid_start, "$lte": oid_end}}
        )
        print("\nKey schema (key: types seen):")
        pprint.pprint(key_types)

        print("\nSample doc (first):")
        if docs:
            d = docs[0]
            # Convert ObjectId/datetime for display
            print(json.dumps(
                {k: str(v) if not isinstance(v, (str, int, float, bool, list, dict, type(None))) else v
                 for k, v in d.items()},
                indent=2, default=str
            ))

        # Check payload fields
        print_section("events — payload field variety (sample 20)")
        payload_keys = set()
        for doc in events.find(
            {"_id": {"$gte": oid_start, "$lte": oid_end}, "payload": {"$exists": True}},
            {"payload": 1}
        ).limit(200):
            if isinstance(doc.get("payload"), dict):
                payload_keys.update(doc["payload"].keys())
        print("payload keys found:", sorted(payload_keys))

        # Check querySk presence
        qsk_count = events.count_documents({
            "_id": {"$gte": oid_start, "$lte": oid_end},
            "payload.querySk": {"$exists": True}
        })
        print(f"\nevents with payload.querySk in window: {qsk_count:,}")

        # Sample querySk values
        qsk_sample = list(events.find(
            {"_id": {"$gte": oid_start, "$lte": oid_end}, "payload.querySk": {"$exists": True}},
            {"payload.querySk": 1, "payload.productId": 1, "guest_id": 1}
        ).limit(10))
        print("Sample querySk values:")
        for d in qsk_sample:
            print(f"  guest_id={d.get('guest_id')} querySk={d.get('payload',{}).get('querySk')} productId={d.get('payload',{}).get('productId')}")

        # ── clients ──────────────────────────────────────────────────
        print_section("clients — schema")
        clients = db["clients"]
        key_types, docs = sample_collection(clients, n=3)
        print("Key schema:")
        pprint.pprint(key_types)
        if docs:
            print("\nSample doc:")
            print(json.dumps(docs[0], indent=2, default=str))

        # ── guestStateHistory ─────────────────────────────────────────
        print_section("guestStateHistory — schema")
        gsh = db["guestStateHistory"]
        key_types, docs = sample_collection(gsh, n=3,
            filter_query={"_id": {"$gte": oid_start, "$lte": oid_end}})
        if not docs:
            key_types, docs = sample_collection(gsh, n=3)
        print("Key schema:")
        pprint.pprint(key_types)
        if docs:
            print("\nSample doc:")
            print(json.dumps(docs[0], indent=2, default=str))

        count_gsh_window = gsh.count_documents({"_id": {"$gte": oid_start, "$lte": oid_end}})
        print(f"\nguestStateHistory in Problem A window: {count_gsh_window:,}")

        # ── guests ────────────────────────────────────────────────────
        print_section("guests — schema")
        guests = db["guests"]
        key_types, docs = sample_collection(guests, n=3)
        print("Key schema:")
        pprint.pprint(key_types)
        if docs:
            print("\nSample doc:")
            print(json.dumps(docs[0], indent=2, default=str))

        # ── Index info ────────────────────────────────────────────────
        print_section("Indexes per collection")
        for cname in ["events", "clients", "guestStateHistory", "guests"]:
            idxs = list(db[cname].list_indexes())
            print(f"\n{cname} indexes:")
            for ix in idxs:
                print(f"  {ix['name']}: {ix['key']}")

        client.close()
    print("\nDone.")

if __name__ == "__main__":
    run()
