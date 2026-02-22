import os
import time
import re
import requests
import gspread
from google.oauth2.service_account import Credentials

AFFINITY_API_KEY = os.environ["AFFINITY_API_KEY"]
AFFINITY_BASE = "https://api.affinity.co"
SPREADSHEET_ID = os.environ["SPREADSHEET_ID"]

# All YC list IDs
YC_LISTS = {
    321050: "YC F25", 305624: "YC S25", 296143: "YC X25",
    331531: "YC W26", 280931: "YC W25", 270581: "YC F24",
    247844: "YC S24", 229947: "YC W24",
}

session = requests.Session()
session.auth = ('', AFFINITY_API_KEY)
session.headers.update({'Content-Type': 'application/json'})

def affinity_get(endpoint, params=None):
    r = session.get(f"{AFFINITY_BASE}{endpoint}", params=params, timeout=60)
    if r.status_code == 429:
        wait = int(r.headers.get('Retry-After', 5))
        time.sleep(wait)
        return affinity_get(endpoint, params)
    if r.status_code != 200:
        return None
    return r.json()

def normalize_name(name):
    if not name:
        return ""
    name = name.lower().strip()
    name = re.sub(r'[^a-z0-9\s]', '', name)
    name = re.sub(r'\s+', ' ', name).strip()
    return name

def main():
    scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
    creds = Credentials.from_service_account_file('credentials.json', scopes=scopes)
    gc = gspread.authorize(creds)
    sheet = gc.open_by_key(SPREADSHEET_ID).sheet1

    all_data = sheet.get_all_values()
    headers = all_data[0]
    rows = all_data[1:]
    col = {h: i for i, h in enumerate(headers)}

    print(f"Cross-checking {len(rows)} companies against YC W26 list in Affinity...\n")

    flagged = []

    for idx, row in enumerate(rows):
        company = row[col["companyName"]].strip()
        domain = row[col["domain"]].strip() if "domain" in col else ""
        if not company:
            continue

        print(f"  [{idx+1}/{len(rows)}] {company}...", end=" ", flush=True)

        org = None
        if domain:
            data = affinity_get("/organizations", {"term": domain, "page_size": 5})
            if data:
                orgs = data.get("organizations", []) if isinstance(data, dict) else data
                for o in (orgs or []):
                    if (o.get("domain") or "").lower().strip() == domain.lower():
                        org = o
                        break
        if not org:
            data = affinity_get("/organizations", {"term": company, "page_size": 5})
            if data:
                orgs = data.get("organizations", []) if isinstance(data, dict) else data
                norm = normalize_name(company)
                for o in (orgs or []):
                    if normalize_name(o.get("name") or "") == norm:
                        org = o
                        break

        if org:
            org_detail = affinity_get(f"/organizations/{org.get('id')}")
            yc_lists_found = []
            if org_detail:
                for entry in org_detail.get("list_entries", []):
                    lid = entry.get("list_id")
                    if lid in YC_LISTS:
                        yc_lists_found.append(YC_LISTS[lid])

            if yc_lists_found:
                print(f"{'  '.join(yc_lists_found)}")
                flagged.append((company, yc_lists_found))
            else:
                print("clean")
        else:
            print("not in affinity")
        time.sleep(0.15)

    print(f"\n{'=' * 60}")
    if flagged:
        print(f"FOUND {len(flagged)} companies on YC lists:")
        for name, lists in flagged:
            print(f"  - {name}: {', '.join(lists)}")
    else:
        print("No YC companies found. All clean.")
    print(f"{'=' * 60}")

if __name__ == "__main__":
    main()
