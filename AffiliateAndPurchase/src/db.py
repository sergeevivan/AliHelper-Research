"""
Database connections: MongoDB via SSH tunnel, Mixpanel export API.
"""

import json
from contextlib import contextmanager

import sshtunnel
import pymongo
import requests
from requests.auth import HTTPBasicAuth

from src.config import (
    SSH_HOST, SSH_USER, DB_HOST, DB_PORT, LOCAL_PORT,
    DB_NAME, MONGO_USER, MONGO_PASS, AUTH_DB,
    MP_ACCOUNT, MP_SECRET, MP_PROJECT, CACHE_DIR,
)


@contextmanager
def mongo_tunnel():
    """
    Context manager that opens an SSH tunnel and yields a pymongo Database.

    Usage:
        with mongo_tunnel() as db:
            db["events"].find_one()
    """
    with sshtunnel.SSHTunnelForwarder(
        SSH_HOST,
        ssh_username=SSH_USER,
        remote_bind_address=(DB_HOST, DB_PORT),
        local_bind_address=("127.0.0.1", LOCAL_PORT),
        set_keepalive=30,
    ) as tunnel:
        client = pymongo.MongoClient(
            f"mongodb://{MONGO_USER}:{MONGO_PASS}@127.0.0.1:{LOCAL_PORT}/{AUTH_DB}",
            serverSelectionTimeoutMS=15000,
            socketTimeoutMS=0,
            connectTimeoutMS=20000,
            directConnection=True,
        )
        try:
            yield client[DB_NAME]
        finally:
            client.close()


def mp_export(event_name: str, from_date: str, to_date: str, cache_key: str) -> list[dict]:
    """
    Download Mixpanel event export, caching to disk as JSON.

    Args:
        event_name: Mixpanel event name (e.g. "Affiliate Click")
        from_date:  Start date string (e.g. "2026-03-06"), in project timezone
        to_date:    End date string (e.g. "2026-04-03"), in project timezone
        cache_key:  Filename stem for cache (e.g. "aff_click_a")

    Returns:
        List of raw Mixpanel event dicts.
    """
    cache_file = CACHE_DIR / f"{cache_key}.json"
    if cache_file.exists():
        print(f"  [cache] Loading {event_name} from {cache_file}")
        with open(cache_file) as f:
            return json.load(f)

    print(f"  [download] Exporting {event_name} {from_date} -> {to_date} ...")
    export_url = "https://data-eu.mixpanel.com/api/2.0/export"
    resp = requests.get(
        export_url,
        auth=HTTPBasicAuth(MP_ACCOUNT, MP_SECRET),
        params={
            "project_id": MP_PROJECT,
            "from_date": from_date,
            "to_date": to_date,
            "event": json.dumps([event_name]),
        },
        timeout=300,
        stream=True,
    )
    resp.raise_for_status()

    records = []
    for line in resp.iter_lines():
        if line:
            records.append(json.loads(line))

    with open(cache_file, "w") as f:
        json.dump(records, f)
    print(f"    -> {len(records):,} records")
    return records
