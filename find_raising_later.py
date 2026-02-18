import os
import time
import requests
import gspread
from concurrent.futures import ThreadPoolExecutor, as_completed
from google.oauth2.service_account import Credentials

AFFINITY_API_KEY = os.environ["AFFINITY_API_KEY"]
AFFINITY_BASE = "https://api.affinity.co"
SPREADSHEET_ID = "1sdbE6V-qVNuKo9LKe42i8x9Nj6ENX8N5oeZsSjRwT9o"
LIST_ID = 21233

FIELD_STATUS = 175381
FIELD_RESPONDED = 175387
FIELD_OUTREACH = 3721939

RAISING_LATER_HIGH = 2573467
RAISING_LATER = 153028

def make_session():
    s = requests.Session()
    s.auth = ('', AFFINITY_API_KEY)
    return s

def affinity_get(session, endpoint, params=None):
    for attempt in range(5):
        r = session.get(f"{AFFINITY_BASE}{endpoint}", params=params, timeout=60)
        if r.status_code == 429:
            wait = int(r.headers.get('Retry-After', 2))
            time.sleep(wait)
            continue
        if r.status_code != 200:
            return None
        return r.json()
    return None

def check_entry_status(entry):
    """Check if an entry has 'Raising Later' status. Returns entry info or None."""
    entry_id = entry.get("id")
    entity_id = entry.get("entity_id")
    entity = entry.get("entity") or {}
    org_name = entity.get("name", "")

    s = make_session()
    fvs = affinity_get(s, "/field-values", {"list_entry_id": entry_id})
    if not fvs:
        return None

    status_text = ""
    responded_text = ""
    outreach_text = ""
    is_raising_later = False

    for fv in fvs:
        fid = fv.get("field_id")
        val = fv.get("value")
        if fid == FIELD_STATUS and isinstance(val, dict):
            status_text = val.get("text", "")
            if val.get("id") in (RAISING_LATER_HIGH, RAISING_LATER):
                is_raising_later = True
        elif fid == FIELD_RESPONDED:
            responded_text = val.get("text", "") if isinstance(val, dict) else str(val or "")
        elif fid == FIELD_OUTREACH:
            outreach_text = val.get("text", "") if isinstance(val, dict) else str(val or "")

    if is_raising_later:
        return {
            "entry_id": entry_id,
            "entity_id": entity_id,
            "org_name": org_name,
            "domain": (entity.get("domain") or ""),
            "status": status_text,
            "responded": responded_text,
            "outreach": outreach_text,
        }
    return None

def truncate(s, max_len=200):
    if not s:
        return ""
    s = str(s).replace('\n', ' ').replace('\r', '').strip()
    return s[:max_len] + "..." if len(s) > max_len else s

def main():
    print("=" * 60, flush=True)
    print("Find 'Raising Later' companies from 1a Sourcing List", flush=True)
    print("=" * 60, flush=True)

    session = make_session()

    # Step 1: Fetch all list entries
    print("\n[1/3] Fetching all list entries...", flush=True)
    all_entries = []
    page_token = None
    while True:
        params = {"page_size": 500}
        if page_token:
            params["page_token"] = page_token
        data = affinity_get(session, f"/lists/{LIST_ID}/list-entries", params)
        if not data:
            break
        batch = data.get("list_entries", [])
        all_entries.extend(batch)
        if len(all_entries) % 5000 == 0 or not data.get("next_page_token"):
            print(f"  {len(all_entries)} entries...", flush=True)
        page_token = data.get("next_page_token")
        if not page_token:
            break
        time.sleep(0.1)
    print(f"  Total: {len(all_entries)} entries", flush=True)

    # Step 2: Check status in parallel
    print(f"\n[2/3] Checking status field (20 concurrent workers)...", flush=True)
    raising_later = []
    checked = 0

    with ThreadPoolExecutor(max_workers=20) as executor:
        futures = {executor.submit(check_entry_status, e): e for e in all_entries}
        for future in as_completed(futures):
            checked += 1
            if checked % 500 == 0:
                print(f"  Checked {checked}/{len(all_entries)}, found {len(raising_later)} so far...", flush=True)
            result = future.result()
            if result:
                raising_later.append(result)
                print(f"  FOUND: {result['org_name']} ({result['status']})", flush=True)

    print(f"\n  Checked: {checked}", flush=True)
    print(f"  Found: {len(raising_later)} 'Raising Later' companies", flush=True)

    # Step 3: Get notes and write to spreadsheet
    print(f"\n[3/3] Getting notes and writing to spreadsheet...", flush=True)

    results = []
    for idx, item in enumerate(raising_later):
        org_id = item["entity_id"]
        print(f"  [{idx+1}/{len(raising_later)}] {item['org_name']}...", end=" ", flush=True)

        notes = affinity_get(session, "/notes", {"organization_id": org_id, "page_size": 50})
        note_list = notes if isinstance(notes, list) else (notes or {}).get("notes", [])
        total_notes = len(note_list) if note_list else 0
        latest_note_date = ""
        latest_note_preview = ""
        if note_list:
            sorted_notes = sorted(note_list, key=lambda n: n.get("created_at", ""), reverse=True)
            latest_note_date = (sorted_notes[0].get("created_at") or "")[:10]
            latest_note_preview = truncate(sorted_notes[0].get("content", ""))

        results.append({**item, "total_notes": total_notes, "latest_note_date": latest_note_date, "latest_note_preview": latest_note_preview})
        print(f"{total_notes} notes", flush=True)
        time.sleep(0.1)

    # Write to spreadsheet
    scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
    creds = Credentials.from_service_account_file('credentials.json', scopes=scopes)
    gc = gspread.authorize(creds)
    sheet = gc.open_by_key(SPREADSHEET_ID).sheet1
    sheet.clear()

    headers = [
        "companyName", "domain", "Status", "Responded?", "Outreach History",
        "Total Notes", "Latest Note Date", "Latest Note Preview"
    ]
    output_rows = [headers]
    for r in results:
        output_rows.append([
            r["org_name"], r["domain"], r["status"], r["responded"], r["outreach"],
            str(r["total_notes"]), r["latest_note_date"], r["latest_note_preview"],
        ])

    sheet.update(range_name='A1', values=output_rows)

    high = sum(1 for r in results if "High" in r["status"])
    normal = len(results) - high
    print(f"\n{'=' * 60}", flush=True)
    print(f"DONE! {len(results)} 'Raising Later' companies", flush=True)
    print(f"  High Priority: {high}", flush=True)
    print(f"  Normal:        {normal}", flush=True)
    print(f"\nSpreadsheet: https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}", flush=True)
    print(f"{'=' * 60}", flush=True)

if __name__ == "__main__":
    main()
