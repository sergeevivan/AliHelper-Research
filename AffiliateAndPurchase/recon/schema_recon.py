"""
Schema reconnaissance script — phase 1.
Connects via SSH tunnel to MongoDB and samples each collection
to understand field structure, nesting, and available data.
"""

import json

from src.db import mongo_tunnel
from src.utils import oid_from_dt, print_section
from src.config import A_START, A_END


def flatten_keys(d, prefix="", depth=0, max_depth=4):
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


def run():
    oid_start = oid_from_dt(A_START)
    oid_end   = oid_from_dt(A_END)

    with mongo_tunnel() as db:
        for col_name in ["guests", "events", "clients", "guestStateHistory"]:
            print_section(f"Collection: {col_name}")
            col = db[col_name]

            if col_name in ("events", "guestStateHistory"):
                filt = {"_id": {"$gte": oid_start, "$lte": oid_end}}
            else:
                filt = None

            keys, docs = sample_collection(col, n=5, filter_query=filt)
            print(f"  Fields ({len(keys)}):")
            for k, types in sorted(keys.items()):
                print(f"    {k:40s} {types}")

            if docs:
                print(f"\n  Sample doc:")
                print(json.dumps(docs[0], indent=2, default=str))


if __name__ == "__main__":
    run()
