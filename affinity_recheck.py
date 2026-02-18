import os
import time
import re
import requests
import gspread
from google.oauth2.service_account import Credentials

AFFINITY_API_KEY = os.environ["AFFINITY_API_KEY"]
AFFINITY_BASE = "https://api.affinity.co"
SPREADSHEET_ID = "1sdbE6V-qVNuKo9LKe42i8x9Nj6ENX8N5oeZsSjRwT9o"

KEY_LISTS = {
    21233: "1a Sourcing List", 62359: "Portfolio Companies",
    321050: "YC F25", 305624: "YC S25", 296143: "YC X25",
    331531: "YC W26", 280931: "YC W25", 270581: "YC F24",
    247844: "YC S24", 229947: "YC W24", 249319: "YC tracking list",
    210758: "David Yang source list",
}
FIELD_STATUS = 175381
FIELD_RESPONDED = 175387
FIELD_OUTREACH = 3721939

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

def normalize_name(name):
    if not name:
        return ""
    name = name.lower().strip()
    name = re.sub(r'\s+(inc|llc|ltd|corp|co|company)\.?$', '', name, flags=re.IGNORECASE)
    name = re.sub(r'[^a-z0-9\s]', '', name)
    name = re.sub(r'\s+', ' ', name).strip()
    return name

def truncate(s, max_len=150):
    if not s:
        return ""
    s = str(s).replace('\n', ' ').replace('\r', '').strip()
    return s[:max_len] + "..." if len(s) > max_len else s

def main():
    print("=" * 60)
    print("Deep Affinity Recheck - All Companies")
    print("=" * 60)

    # Read spreadsheet
    scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
    creds = Credentials.from_service_account_file('credentials.json', scopes=scopes)
    gc = gspread.authorize(creds)
    sheet = gc.open_by_key(SPREADSHEET_ID).sheet1

    all_data = sheet.get_all_values()
    headers = all_data[0]
    rows = all_data[1:]
    col = {h: i for i, h in enumerate(headers)}

    print(f"\nChecking {len(rows)} companies against Affinity...\n")

    stats = {"found": 0, "on_list": 0, "has_notes": 0, "clean": 0}

    for idx, row in enumerate(rows):
        company = row[col["companyName"]].strip() if "companyName" in col else ""
        domain = row[col["domain"]].strip() if "domain" in col else ""

        if not company:
            continue

        print(f"  [{idx+1}/{len(rows)}] {company}...", end=" ", flush=True)

        # Search by domain then name
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

        if org:
            org_id = org.get("id")
            stats["found"] += 1

            org_detail = affinity_get(f"/organizations/{org_id}")
            time.sleep(0.1)

            list_entries = (org_detail or {}).get("list_entries", [])
            lists_found = []
            sourcing_entry_id = None

            for entry in list_entries:
                list_id = entry.get("list_id")
                if list_id == 21233:
                    sourcing_entry_id = entry.get("id")
                    stats["on_list"] += 1
                list_name = KEY_LISTS.get(list_id, f"List #{list_id}")
                lists_found.append(list_name)

            status_text = ""
            responded_text = ""
            if sourcing_entry_id:
                fvs = affinity_get("/field-values", {"list_entry_id": sourcing_entry_id})
                time.sleep(0.1)
                for fv in (fvs or []):
                    fid = fv.get("field_id")
                    val = fv.get("value")
                    if fid == FIELD_STATUS:
                        status_text = val.get("text", "") if isinstance(val, dict) else str(val or "")
                    elif fid == FIELD_RESPONDED:
                        responded_text = val.get("text", "") if isinstance(val, dict) else str(val or "")

            notes = affinity_get("/notes", {"organization_id": org_id, "page_size": 50})
            time.sleep(0.1)
            note_list = notes if isinstance(notes, list) else (notes or {}).get("notes", [])
            total_notes = len(note_list) if note_list else 0
            if total_notes:
                stats["has_notes"] += 1

            on_list = "1a Sourcing List" in lists_found
            icon = "✓" if on_list else "⊘"
            note_info = f"{total_notes} notes" if total_notes else "no notes"
            status_info = f" [{status_text}]" if status_text else ""
            lists_info = f" Lists: {', '.join(lists_found)}" if lists_found else ""
            print(f"{icon}{status_info} {note_info}{lists_info}")
        else:
            stats["clean"] += 1
            print("✗ Not found")

        time.sleep(0.15)

    total = len(rows)
    print(f"\n{'=' * 60}")
    print(f"AFFINITY CHECK COMPLETE - {total} companies")
    print(f"{'=' * 60}")
    print(f"  Found in Affinity:         {stats['found']}/{total}")
    print(f"  On 1a Sourcing List:       {stats['on_list']}/{total}")
    print(f"  Have activity notes:       {stats['has_notes']}/{total}")
    print(f"  Net new (not in Affinity): {stats['clean']}/{total}")
    print(f"{'=' * 60}")

if __name__ == "__main__":
    main()
