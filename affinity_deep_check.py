import os, requests, time, json, re
import gspread
from google.oauth2.service_account import Credentials

AFFINITY_API_KEY = os.environ["AFFINITY_API_KEY"]
AFFINITY_BASE = "https://api.affinity.co"
TARGET_LIST_ID = int(os.environ["AFFINITY_LIST_ID"])
SPREADSHEET_ID = os.environ["SPREADSHEET_ID"]

session = requests.Session()
session.auth = ('', AFFINITY_API_KEY)
session.headers.update({'Content-Type': 'application/json'})

def aff_get(endpoint, params=None):
    r = session.get(f"{AFFINITY_BASE}{endpoint}", params=params, timeout=60)
    if r.status_code == 429:
        time.sleep(int(r.headers.get('Retry-After', 5)))
        return aff_get(endpoint, params)
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

# Read spreadsheet
scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
creds = Credentials.from_service_account_file('credentials.json', scopes=scopes)
gc = gspread.authorize(creds)
sheet = gc.open_by_key(SPREADSHEET_ID).sheet1
rows = sheet.get_all_values()

companies = []
for row in rows[1:]:
    name = row[0] if len(row) > 0 else ""
    domain = row[4] if len(row) > 4 else ""
    if name.strip():
        companies.append((name, domain))

print(f"Checking {len(companies)} companies in Affinity...\n")
print("=" * 80)

for i, (name, domain) in enumerate(companies, 1):
    print(f"\n[{i}/{len(companies)}] {name} ({domain})")
    print("-" * 60)

    # Search by domain first, then name
    org = None
    if domain:
        data = aff_get("/organizations", {"term": domain, "page_size": 5})
        if data:
            orgs = data.get("organizations", []) if isinstance(data, dict) else data
            for o in (orgs or []):
                if (o.get("domain") or "").lower().strip() == domain.lower():
                    org = o
                    break
    if not org and name:
        data = aff_get("/organizations", {"term": name, "page_size": 5})
        if data:
            orgs = data.get("organizations", []) if isinstance(data, dict) else data
            norm = normalize_name(name)
            for o in (orgs or []):
                if normalize_name(o.get("name") or "") == norm:
                    org = o
                    break

    if not org:
        print("  NOT IN AFFINITY - completely new")
        time.sleep(0.15)
        continue

    org_id = org.get("id")
    org_name = org.get("name", "")
    print(f"  Found: {org_name} (ID: {org_id})")

    # Get org detail for list entries
    org_detail = aff_get(f"/organizations/{org_id}")
    on_1a = False
    if org_detail:
        list_entries = org_detail.get("list_entries", [])
        if list_entries:
            for entry in list_entries:
                lid = entry.get("list_id")
                if lid == TARGET_LIST_ID:
                    on_1a = True
                    print(f"  *** ON 1A SOURCING LIST ***")
                else:
                    print(f"  On another Affinity list (ID: {lid})")
        else:
            print("  Not on any lists")

    # Get notes (activity feed - emails, meetings, notes)
    notes = aff_get("/notes", {"organization_id": org_id, "page_size": 20})
    if notes:
        note_list = notes if isinstance(notes, list) else notes.get("notes", [])
        if note_list:
            print(f"  ACTIVITY FEED: {len(note_list)} item(s)")
            for n in note_list[:8]:
                created = (n.get("created_at") or "")[:10]
                content = (n.get("content") or n.get("plain_text") or "")
                # Strip HTML tags
                content = re.sub(r'<[^>]+>', '', content).strip()[:200]
                ntype = n.get("type") or "note"
                creator = n.get("creator") or {}
                creator_name = f"{creator.get('first_name', '')} {creator.get('last_name', '')}".strip()
                print(f"    [{created}] {ntype} by {creator_name}")
                if content:
                    print(f"      {content[:200]}")
        else:
            print("  No activity/notes")
    else:
        print("  No activity/notes")

    if on_1a:
        print(f"  >>> ALREADY ON 1A SOURCING LIST - REMOVE FROM OUTREACH <<<")

    time.sleep(0.2)

print("\n" + "=" * 80)
print("DONE")
