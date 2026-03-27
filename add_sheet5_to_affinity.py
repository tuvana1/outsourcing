#!/usr/bin/env python3
"""Read companies from Sheet5 and add them to Affinity 1a Sourcing List."""

import os
import re
import time
import requests
import gspread
from google.oauth2.service_account import Credentials
from dotenv import load_dotenv

load_dotenv()

AFFINITY_API_KEY = os.environ["AFFINITY_API_KEY"]
SPREADSHEET_ID = os.environ["SPREADSHEET_ID"]
AFFINITY_BASE = "https://api.affinity.co"
AFFINITY_AUTH = ("", AFFINITY_API_KEY)
LIST_ID = 21233

# Google Sheets setup
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]
creds = Credentials.from_service_account_file(
    "credentials.json", scopes=SCOPES
)
gc = gspread.authorize(creds)
sh = gc.open_by_key(SPREADSHEET_ID)


def normalize_name(name):
    """Lowercase, strip suffixes, remove non-alphanumeric except spaces."""
    n = name.lower().strip()
    for suffix in [" inc", " inc.", " llc", " ltd", " ltd.", " corp", " corp.", " co", " co."]:
        if n.endswith(suffix):
            n = n[: -len(suffix)].strip()
    n = re.sub(r"[^a-z0-9 ]", "", n)
    return n.strip()


def affinity_get(path, params=None):
    """GET with rate-limit retry."""
    while True:
        r = requests.get(f"{AFFINITY_BASE}{path}", auth=AFFINITY_AUTH, params=params)
        if r.status_code == 429:
            wait = int(r.headers.get("Retry-After", 5))
            print(f"  Rate limited, waiting {wait}s...")
            time.sleep(wait)
            continue
        r.raise_for_status()
        return r.json()


def affinity_post(path, body):
    """POST with rate-limit retry."""
    while True:
        r = requests.post(f"{AFFINITY_BASE}{path}", auth=AFFINITY_AUTH, json=body)
        if r.status_code == 429:
            wait = int(r.headers.get("Retry-After", 5))
            print(f"  Rate limited, waiting {wait}s...")
            time.sleep(wait)
            continue
        return r


def find_org(name, domain):
    """Search Affinity for an org by domain then by name. Returns org_id or None."""
    # Search by domain first
    if domain:
        time.sleep(0.2)
        results = affinity_get("/organizations", {"term": domain, "page_size": 5})
        if isinstance(results, dict) and "organizations" in results:
            orgs = results["organizations"]
        elif isinstance(results, list):
            orgs = results
        else:
            orgs = []
        for org in orgs:
            org_domain = (org.get("domain") or "").lower().strip()
            if org_domain == domain.lower().strip():
                return org["id"], org.get("name", "")

    # Search by name
    if name:
        time.sleep(0.2)
        results = affinity_get("/organizations", {"term": name, "page_size": 5})
        if isinstance(results, dict) and "organizations" in results:
            orgs = results["organizations"]
        elif isinstance(results, list):
            orgs = results
        else:
            orgs = []
        norm = normalize_name(name)
        for org in orgs:
            if normalize_name(org.get("name", "")) == norm:
                return org["id"], org.get("name", "")

    return None, None


def main():
    # Read Sheet5
    ws = sh.worksheet("Sheet7")
    rows = ws.get_all_records()
    print(f"Found {len(rows)} companies in Sheet5\n")

    # Detect column names (case-insensitive)
    if not rows:
        print("No data found.")
        return

    headers = list(rows[0].keys())
    print(f"Columns: {headers}")

    # Find the right column names
    name_col = None
    domain_col = None
    for h in headers:
        hl = h.lower().strip()
        if "company" in hl and "name" in hl:
            name_col = h
        elif hl == "name" and not name_col:
            name_col = h
        if hl == "domain" or hl == "website":
            domain_col = h

    if not name_col:
        # fallback: first column
        name_col = headers[0]
    if not domain_col:
        for h in headers:
            if "domain" in h.lower() or "url" in h.lower() or "website" in h.lower():
                domain_col = h
                break

    print(f"Using name column: '{name_col}', domain column: '{domain_col}'\n")

    added = 0
    already_existed = 0
    created_new = 0
    failures = 0

    for i, row in enumerate(rows, 1):
        company_name = str(row.get(name_col, "")).strip()
        domain = str(row.get(domain_col, "")).strip() if domain_col else ""

        if not company_name and not domain:
            print(f"[{i}/{len(rows)}] SKIP - empty row")
            continue

        print(f"[{i}/{len(rows)}] {company_name} ({domain})")

        # Step 1: Find or create org
        org_id, found_name = find_org(company_name, domain)

        if org_id:
            print(f"  Found existing org: {found_name} (ID {org_id})")
        else:
            # Create new org
            time.sleep(0.2)
            body = {"name": company_name}
            if domain:
                body["domain"] = domain
            resp = affinity_post("/organizations", body)
            if resp.status_code in (200, 201):
                org_data = resp.json()
                org_id = org_data["id"]
                print(f"  Created new org (ID {org_id})")
                created_new += 1
            else:
                print(f"  FAILED to create org: {resp.status_code} {resp.text}")
                failures += 1
                continue

        # Step 2: Add to list
        time.sleep(0.2)
        resp = affinity_post(f"/lists/{LIST_ID}/list-entries", {"entity_id": org_id})
        if resp.status_code in (200, 201):
            print(f"  Added to 1a Sourcing List")
            added += 1
        elif resp.status_code == 422 and "already" in resp.text.lower():
            print(f"  Already on 1a Sourcing List")
            already_existed += 1
        else:
            print(f"  FAILED to add to list: {resp.status_code} {resp.text}")
            failures += 1

    print(f"\n{'='*50}")
    print(f"SUMMARY")
    print(f"{'='*50}")
    print(f"Total companies processed: {len(rows)}")
    print(f"Added to list:            {added}")
    print(f"Already on list:          {already_existed}")
    print(f"New orgs created:         {created_new}")
    print(f"Failures:                 {failures}")


if __name__ == "__main__":
    main()
