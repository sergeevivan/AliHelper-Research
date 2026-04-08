"""Schema recon phase 3 — Purchase and Purchase Completed Mixpanel event schemas."""
import os, json, pprint
from dotenv import load_dotenv
import requests
from requests.auth import HTTPBasicAuth

load_dotenv()

MP_ACCOUNT = os.getenv("MIXPANEL_SERVICE_ACCOUNT")
MP_SECRET  = os.getenv("MIXPANEL_SECRET")
MP_PROJECT = os.getenv("MIXPANEL_PROJECT_ID")
MP_BASE    = os.getenv("MIXPANEL_BASE_URL")

def sample_event(event_name, from_date="2026-03-20", to_date="2026-03-26", n=2):
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
        timeout=60, stream=True
    )
    print(f"\n{event_name} — status {resp.status_code}")
    docs = []
    for line in resp.iter_lines():
        if line:
            docs.append(json.loads(line))
            if len(docs) >= n:
                break
    for d in docs:
        props = d.get("properties", {})
        print(f"  Keys: {sorted(props.keys())}")
        pprint.pprint(props)
        print()
    return docs

if __name__ == "__main__":
    sample_event("Purchase", n=2)
    sample_event("Purchase Completed", n=2)
