import os
import time
import re
import requests
import gspread
from google.oauth2.service_account import Credentials

AFFINITY_API_KEY = os.environ["AFFINITY_API_KEY"]
AFFINITY_BASE = "https://api.affinity.co"
SPREADSHEET_ID = os.environ["SPREADSHEET_ID"]
LIST_ID = int(os.environ["AFFINITY_LIST_ID"])

session = requests.Session()
session.auth = ('', AFFINITY_API_KEY)
session.headers.update({'Content-Type': 'application/json'})

def affinity_get(endpoint, params=None):
    r = session.get(f"{AFFINITY_BASE}{endpoint}", params=params, timeout=60)
    if r.status_code == 429:
        wait = int(r.headers.get('Retry-After', 5))
        print(f"  Rate limited, waiting {wait}s...")
        time.sleep(wait)
        return affinity_get(endpoint, params)
    if r.status_code != 200:
        return None
    return r.json()

def affinity_post(endpoint, payload):
    r = session.post(f"{AFFINITY_BASE}{endpoint}", json=payload, timeout=60)
    if r.status_code == 429:
        wait = int(r.headers.get('Retry-After', 5))
        print(f"  Rate limited, waiting {wait}s...")
        time.sleep(wait)
        return affinity_post(endpoint, payload)
    return r.status_code, r.json() if r.status_code in (200, 201) else r.text

def normalize_name(name):
    if not name:
        return ""
    name = name.lower().strip()
    name = re.sub(r'\s+(inc|llc|ltd|corp|co|company)\.?$', '', name, flags=re.IGNORECASE)
    name = re.sub(r'[^a-z0-9\s]', '', name)
    name = re.sub(r'\s+', ' ', name).strip()
    return name

def main():
    print("=" * 60)
    print("Adding all companies to Affinity 1a Sourcing List")
    print("=" * 60)

    scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
    creds = Credentials.from_service_account_file('credentials.json', scopes=scopes)
    gc = gspread.authorize(creds)
    sheet = gc.open_by_key(SPREADSHEET_ID).sheet1

    all_data = sheet.get_all_values()
    headers = all_data[0]
    rows = all_data[1:]
    col = {h: i for i, h in enumerate(headers)}

    print(f"\nProcessing {len(rows)} companies...\n")

    added = 0
    already = 0
    created_org = 0
    failed = 0

    for idx, row in enumerate(rows):
        company = row[col["companyName"]].strip()
        domain = row[col["domain"]].strip() if "domain" in col else ""

        if not company:
            continue

        print(f"  [{idx+1}/{len(rows)}] {company}...", end=" ", flush=True)

        # Find org in Affinity
        org = None
        if domain:
            data = affinity_get("/organizations", {"term": domain, "page_size": 5})
            if data:
                orgs = data.get("organizations", []) if isinstance(data, dict) else data
                for o in (orgs or []):
                    if (o.get("domain") or "").lower().strip() == domain.lower():
                        org = o
                        break
        if not org and company:
            data = affinity_get("/organizations", {"term": company, "page_size": 5})
            if data:
                orgs = data.get("organizations", []) if isinstance(data, dict) else data
                norm = normalize_name(company)
                for o in (orgs or []):
                    if normalize_name(o.get("name") or "") == norm:
                        org = o
                        break

        if not org:
            # Create the org
            status_code, resp = affinity_post("/organizations", {"name": company, "domain": domain})
            if status_code in (200, 201):
                org = resp
                created_org += 1
                print("(new org) ", end="", flush=True)
            else:
                print(f"FAIL (create org: {status_code})")
                failed += 1
                time.sleep(0.15)
                continue

        org_id = org.get("id")

        # Check if already on list
        org_detail = affinity_get(f"/organizations/{org_id}")
        on_list = False
        if org_detail:
            for entry in org_detail.get("list_entries", []):
                if entry.get("list_id") == LIST_ID:
                    on_list = True
                    break

        if on_list:
            print("already on list")
            already += 1
        else:
            status_code, resp = affinity_post(f"/lists/{LIST_ID}/list-entries", {"entity_id": org_id})
            if status_code in (200, 201):
                print("ADDED")
                added += 1
            else:
                print(f"FAIL ({status_code})")
                failed += 1

        time.sleep(0.2)

    print(f"\n{'=' * 60}")
    print(f"DONE!")
    print(f"  Added to 1a Sourcing List: {added}")
    print(f"  Already on list:           {already}")
    print(f"  New orgs created:          {created_org}")
    print(f"  Failed:                    {failed}")
    print(f"{'=' * 60}")

if __name__ == "__main__":
    main()
