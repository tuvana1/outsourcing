#!/usr/bin/env python3
"""Check Sheet5 companies against Affinity CRM and remove duplicates."""

import os
import re
import time
import requests
import gspread
from google.oauth2.service_account import Credentials
from dotenv import load_dotenv

load_dotenv("/Users/erdenetangads/palm drive capital/.env")

AFFINITY_API_KEY = os.environ["AFFINITY_API_KEY"]
LEMLIST_API_KEY = os.environ["LEMLIST_API_KEY"]
SPREADSHEET_ID = os.environ["SPREADSHEET_ID"]
LEMLIST_CAMPAIGN_ID = os.environ["LEMLIST_CAMPAIGN_ID"]

AFFINITY_BASE = "https://api.affinity.co"
AFFINITY_AUTH = ("", AFFINITY_API_KEY)

# --- Google Sheets setup ---
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]
creds = Credentials.from_service_account_file(
    "/Users/erdenetangads/palm drive capital/credentials.json", scopes=SCOPES
)
gc = gspread.authorize(creds)
sh = gc.open_by_key(SPREADSHEET_ID)
ws = sh.worksheet("Sheet5")


def normalize_name(name):
    """Normalize company name for comparison."""
    n = name.lower().strip()
    # Remove suffixes
    for suffix in [", inc.", ", inc", " inc.", " inc", ", llc", " llc",
                   ", ltd.", ", ltd", " ltd.", " ltd",
                   ", corp.", ", corp", " corp.", " corp",
                   ", co.", ", co", " co.", " co"]:
        if n.endswith(suffix):
            n = n[: -len(suffix)]
    # Remove non-alphanumeric except spaces
    n = re.sub(r"[^a-z0-9 ]", "", n)
    # Collapse whitespace
    n = re.sub(r"\s+", " ", n).strip()
    return n


def affinity_get(url, params=None, retries=3):
    """GET with rate-limit retry."""
    for attempt in range(retries):
        r = requests.get(url, auth=AFFINITY_AUTH, params=params)
        if r.status_code == 429:
            wait = int(r.headers.get("Retry-After", 5))
            print(f"    Rate limited, waiting {wait}s...")
            time.sleep(wait)
            continue
        r.raise_for_status()
        return r.json()
    raise Exception(f"Failed after {retries} retries: {url}")


def check_affinity(company_name, domain):
    """Check if company exists in Affinity. Returns (found, org_id, reasons)."""
    reasons = []
    org_id = None

    # 1. Search by domain
    if domain:
        time.sleep(0.15)
        results = affinity_get(f"{AFFINITY_BASE}/organizations", params={"term": domain, "page_size": 5})
        orgs = results if isinstance(results, list) else results.get("organizations", [])
        for org in orgs:
            org_domain = (org.get("domain") or "").lower().strip()
            if org_domain and org_domain == domain.lower().strip():
                org_id = org["id"]
                reasons.append(f"domain match ({domain})")
                break

    # 2. Search by name if not found
    if not org_id and company_name:
        time.sleep(0.15)
        results = affinity_get(f"{AFFINITY_BASE}/organizations", params={"term": company_name, "page_size": 5})
        orgs = results if isinstance(results, list) else results.get("organizations", [])
        norm_target = normalize_name(company_name)
        for org in orgs:
            norm_org = normalize_name(org.get("name", ""))
            if norm_org == norm_target:
                org_id = org["id"]
                reasons.append(f"name match ({org.get('name')})")
                break

    if not org_id:
        return False, None, []

    # 3. Check list entries
    time.sleep(0.15)
    detail = affinity_get(f"{AFFINITY_BASE}/organizations/{org_id}")
    list_entries = detail.get("list_entries", [])
    if list_entries:
        list_ids = [str(e.get("list_id", "")) for e in list_entries]
        sourcing = any(e.get("list_id") == 21233 for e in list_entries)
        if sourcing:
            reasons.append("on 1a Sourcing List")
        else:
            reasons.append(f"on list(s): {', '.join(list_ids)}")

    # 4. Check notes
    time.sleep(0.15)
    notes = affinity_get(f"{AFFINITY_BASE}/notes", params={"organization_id": org_id, "page_size": 5})
    note_list = notes if isinstance(notes, list) else notes.get("notes", [])
    if note_list:
        reasons.append(f"{len(note_list)} note(s)")

    is_duplicate = len(list_entries) > 0 or len(note_list) > 0
    return is_duplicate, org_id, reasons


def remove_from_lemlist(email):
    """Delete lead from Lemlist campaign."""
    url = f"https://api.lemlist.com/api/campaigns/{LEMLIST_CAMPAIGN_ID}/leads/{email}"
    r = requests.delete(url, auth=("", LEMLIST_API_KEY))
    return r.status_code in (200, 204, 404)


# === MAIN ===
print("=" * 60)
print("SHEET5 DEDUP vs AFFINITY CRM")
print("=" * 60)

# Step 1: Read Sheet5
print("\n[Step 1] Reading Sheet5...")
all_data = ws.get_all_records()
print(f"  Found {len(all_data)} rows")

# Find column names
headers = ws.row_values(1)
print(f"  Headers: {headers}")

# Identify columns (case-insensitive search)
header_lower = [h.lower().strip() for h in headers]
name_col = None
email_col = None
domain_col = None

for i, h in enumerate(header_lower):
    if "companyname" in h.replace(" ", "") or h == "company" or h == "company name":
        name_col = headers[i]
    if "email" in h:
        email_col = headers[i]
    if "domain" in h or "website" in h:
        domain_col = headers[i]

print(f"  Using columns: name={name_col}, email={email_col}, domain={domain_col}")

# Build company list
companies = []
for i, row in enumerate(all_data):
    name = str(row.get(name_col, "")).strip() if name_col else ""
    email = str(row.get(email_col, "")).strip() if email_col else ""
    domain = str(row.get(domain_col, "")).strip() if domain_col else ""
    # Try to extract domain from email if no domain column
    if not domain and email and "@" in email:
        domain = email.split("@")[1].lower()
    companies.append({"row_index": i + 2, "name": name, "email": email, "domain": domain})  # +2 for 1-indexed + header

print(f"  Loaded {len(companies)} companies\n")

# Step 2: Check against Affinity
print("[Step 2] Checking companies against Affinity...")
duplicates = []

for idx, co in enumerate(companies, 1):
    name_display = co["name"] or co["email"] or co["domain"]
    print(f"  [{idx:3d}/{len(companies)}] {name_display:<40s} ", end="", flush=True)

    try:
        is_dup, org_id, reasons = check_affinity(co["name"], co["domain"])
        if is_dup:
            print(f"DUPLICATE — {'; '.join(reasons)}")
            duplicates.append({**co, "reasons": reasons, "org_id": org_id})
        else:
            if org_id:
                print(f"found in Affinity (id={org_id}) but no list entries/notes — KEEP")
            else:
                print("not found — KEEP")
    except Exception as e:
        print(f"ERROR: {e}")

print(f"\n  Found {len(duplicates)} duplicates out of {len(companies)} companies\n")

if not duplicates:
    print("No duplicates found. Done!")
    exit(0)

# Step 3: Remove from Lemlist
print("[Step 3] Removing duplicates from Lemlist campaign...")
for co in duplicates:
    email = co["email"]
    if email:
        ok = remove_from_lemlist(email)
        status = "removed" if ok else "FAILED"
        print(f"  Lemlist: {email:<40s} — {status}")
    else:
        print(f"  Lemlist: {co['name']:<40s} — no email, skipped")
    time.sleep(0.15)

# Step 4: Remove rows from Sheet5 (bottom-to-top)
print("\n[Step 4] Removing duplicate rows from Sheet5...")
rows_to_delete = sorted([co["row_index"] for co in duplicates], reverse=True)
for row_num in rows_to_delete:
    name = next(co["name"] for co in duplicates if co["row_index"] == row_num)
    print(f"  Deleting row {row_num}: {name}")
    ws.delete_rows(row_num)
    time.sleep(0.3)  # avoid Sheets rate limit

# Step 5: Summary
print("\n" + "=" * 60)
print("SUMMARY")
print("=" * 60)
print(f"  Total companies checked: {len(companies)}")
print(f"  Duplicates found & removed: {len(duplicates)}")
print(f"  Remaining on Sheet5: {len(companies) - len(duplicates)}")
print("\n  Duplicate companies:")
for co in duplicates:
    print(f"    - {co['name']:<35s} | {'; '.join(co['reasons'])}")
print("\nDone!")
