import os
import re
import csv
import time
from datetime import datetime
import requests
import gspread
from google.oauth2.service_account import Credentials

# ---------- Configuration ----------

AFFINITY_API_KEY = os.environ["AFFINITY_API_KEY"]
SPREADSHEET_ID = os.environ["SPREADSHEET_ID"]
AFFINITY_BASE = "https://api.affinity.co"

# Key lists to check
KEY_LISTS = {
    21233: "1a Sourcing List",
    62359: "Portfolio Companies",
    321050: "YC F25",
    305624: "YC S25",
    296143: "YC X25",
    331531: "YC W26",
    280931: "YC W25",
    270581: "YC F24",
    247844: "YC S24",
    229947: "YC W24",
    249319: "YC tracking list",
    210758: "David Yang source list",
}

# Field IDs on 1a Sourcing List
FIELD_STATUS = 175381
FIELD_RESPONDED = 175387
FIELD_OUTREACH = 3721939
FIELD_LAST_EMAIL = 3726674

# ---------- Helpers ----------

def extract_domain(email):
    if not email or "@" not in email:
        return ""
    return email.split("@")[-1].lower().strip()

def normalize_name(name):
    if not name:
        return ""
    name = name.lower().strip()
    name = re.sub(r'\s+(inc|llc|ltd|corp|co|company|inc\.|llc\.|ltd\.)\.?$', '', name, flags=re.IGNORECASE)
    name = re.sub(r'[^a-z0-9\s]', '', name)
    name = re.sub(r'\s+', ' ', name).strip()
    return name

def truncate(s, max_len=200):
    if not s:
        return ""
    s = str(s).replace('\n', ' ').replace('\r', '').strip()
    return s[:max_len] + "..." if len(s) > max_len else s

# ---------- Affinity API ----------

class AffinityClient:
    def __init__(self, api_key):
        self.session = requests.Session()
        self.session.auth = ('', api_key)
        self.session.headers.update({'Content-Type': 'application/json'})

    def _get(self, endpoint, params=None):
        url = f"{AFFINITY_BASE}{endpoint}"
        r = self.session.get(url, params=params, timeout=60)
        if r.status_code == 429:
            retry_after = int(r.headers.get('Retry-After', 5))
            print(f"  Rate limited, waiting {retry_after}s...")
            time.sleep(retry_after)
            return self._get(endpoint, params)
        if r.status_code != 200:
            return None
        return r.json()

    def search_org_by_domain(self, domain):
        if not domain:
            return None
        domain = domain.lower().strip()
        data = self._get("/organizations", {"term": domain, "page_size": 10})
        if not data:
            return None
        orgs = data.get("organizations", []) if isinstance(data, dict) else data
        for org in (orgs or []):
            if (org.get("domain") or "").lower().strip() == domain:
                return org
        return None

    def search_org_by_name(self, name):
        if not name:
            return None
        data = self._get("/organizations", {"term": name, "page_size": 10})
        if not data:
            return None
        orgs = data.get("organizations", []) if isinstance(data, dict) else data
        norm = normalize_name(name)
        for org in (orgs or []):
            if normalize_name(org.get("name") or "") == norm:
                return org
        return None

    def get_org_detail(self, org_id):
        """Get full org detail including list_entries."""
        return self._get(f"/organizations/{org_id}")

    def get_field_values(self, list_entry_id):
        data = self._get("/field-values", {"list_entry_id": list_entry_id})
        if not data:
            return []
        return data if isinstance(data, list) else data.get("field_values", [])

    def get_notes(self, org_id):
        """Get notes for an organization - this is the activity timeline."""
        data = self._get("/notes", {"organization_id": org_id, "page_size": 50})
        if not data:
            return []
        return data if isinstance(data, list) else data.get("notes", [])

# ---------- Google Sheets ----------

def get_sheet():
    scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
    creds = Credentials.from_service_account_file('credentials.json', scopes=scopes)
    gc = gspread.authorize(creds)
    return gc.open_by_key(SPREADSHEET_ID).sheet1

# ---------- Main ----------

def main():
    print("=" * 60)
    print("Deep Affinity Analysis (with Activity Timeline)")
    print("=" * 60)

    # Read CSV
    print("\nReading lemlist_leads.csv...")
    rows = []
    with open("lemlist_leads.csv", "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(row)
    print(f"  Found {len(rows)} companies")

    affinity = AffinityClient(AFFINITY_API_KEY)

    print("\nConnecting to Google Sheet...")
    sheet = get_sheet()
    sheet.clear()

    headers = [
        "companyName", "firstName", "email", "ceoName", "domain",
        # Affinity presence
        "Found in Affinity", "Affinity Org ID",
        # 1a Sourcing List details
        "On 1a Sourcing List", "Status", "Responded?", "Outreach History",
        # Other lists
        "Other Affinity Lists",
        # Activity timeline
        "Total Notes", "Latest Note Date", "Latest Note Preview",
        "All Activity Dates",
        # Full activity timeline
        "Activity Timeline",
        # Summary
        "Relationship Summary", "Last Checked"
    ]

    print(f"\nAnalyzing {len(rows)} companies with activity timeline...")
    output_rows = [headers]

    stats = {"found": 0, "on_list": 0, "has_notes": 0, "passed": 0, "responded": 0}

    for idx, row in enumerate(rows):
        company_name = row.get("companyName", "")
        first_name = row.get("firstName", "")
        email = row.get("email", "")
        ceo_name = row.get("ceoName", "")
        domain = extract_domain(email)

        print(f"  [{idx+1}/{len(rows)}] {company_name}...", end=" ", flush=True)

        # Defaults
        found_in_affinity = "No"
        org_id_str = ""
        on_sourcing_list = "No"
        status_text = ""
        responded_text = ""
        outreach_text = ""
        other_lists = ""
        total_notes = 0
        latest_note_date = ""
        latest_note_preview = ""
        all_activity_dates = ""
        activity_timeline = ""
        relationship_summary = "No prior relationship"

        # Search Affinity
        org = None
        if domain:
            org = affinity.search_org_by_domain(domain)
        if not org and company_name:
            org = affinity.search_org_by_name(company_name)

        if org:
            found_in_affinity = "Yes"
            org_id = org.get("id")
            org_id_str = str(org_id)
            stats["found"] += 1

            # Get full org detail (includes list_entries)
            org_detail = affinity.get_org_detail(org_id)
            time.sleep(0.1)

            list_entries = (org_detail or {}).get("list_entries", [])
            lists_found = []
            sourcing_entry_id = None

            for entry in list_entries:
                list_id = entry.get("list_id")
                entry_id = entry.get("id")

                if list_id == 21233:
                    on_sourcing_list = "Yes"
                    sourcing_entry_id = entry_id
                    stats["on_list"] += 1

                list_name = KEY_LISTS.get(list_id, f"List #{list_id}")
                lists_found.append(list_name)

            other_lists_display = [l for l in lists_found if l != "1a Sourcing List"]
            other_lists = ", ".join(other_lists_display) if other_lists_display else ""

            # Get field values from 1a Sourcing List
            if sourcing_entry_id:
                field_values = affinity.get_field_values(sourcing_entry_id)
                time.sleep(0.1)

                for fv in field_values:
                    field_id = fv.get("field_id")
                    value = fv.get("value")

                    if field_id == FIELD_STATUS:
                        if isinstance(value, dict):
                            status_text = value.get("text", "")
                        elif value:
                            status_text = str(value)
                        if status_text.lower() == "passed":
                            stats["passed"] += 1

                    elif field_id == FIELD_RESPONDED:
                        if isinstance(value, dict):
                            responded_text = value.get("text", "")
                        elif value:
                            responded_text = str(value)
                        if responded_text and responded_text.lower() not in ("no", "not contacted", "new", ""):
                            stats["responded"] += 1

                    elif field_id == FIELD_OUTREACH:
                        if isinstance(value, dict):
                            outreach_text = value.get("text", "")
                        elif value:
                            outreach_text = str(value)

            # GET NOTES - This is the activity timeline!
            notes = affinity.get_notes(org_id)
            time.sleep(0.1)

            if notes:
                total_notes = len(notes)
                stats["has_notes"] += 1

                # Sort by date (newest first)
                notes_sorted = sorted(notes, key=lambda n: n.get("created_at", ""), reverse=True)

                # Latest note
                latest = notes_sorted[0]
                latest_note_date = (latest.get("created_at") or "")[:10]
                latest_note_preview = truncate(latest.get("content", ""), 150)

                # All activity dates
                dates = [n.get("created_at", "")[:10] for n in notes_sorted if n.get("created_at")]
                all_activity_dates = ", ".join(dates)

                # Build full timeline
                timeline_parts = []
                for n in notes_sorted[:10]:  # Last 10 notes max
                    date = (n.get("created_at") or "")[:10]
                    content = truncate(n.get("content", ""), 120)
                    timeline_parts.append(f"[{date}] {content}")
                activity_timeline = " || ".join(timeline_parts)

            # Build relationship summary
            parts = []
            if on_sourcing_list == "Yes":
                parts.append(f"On 1a Sourcing List")
                if status_text:
                    parts.append(f"Status: {status_text}")
                if responded_text:
                    parts.append(f"Responded: {responded_text}")
                if outreach_text:
                    parts.append(f"Outreach: {outreach_text}")
            if other_lists_display:
                parts.append(f"Also on: {', '.join(other_lists_display)}")
            if total_notes > 0:
                parts.append(f"{total_notes} notes (latest: {latest_note_date})")
            if not parts:
                relationship_summary = "In Affinity DB, no tracked activity"
            else:
                relationship_summary = " | ".join(parts)

            icon = "✓" if on_sourcing_list == "Yes" else "⊘"
            note_info = f"{total_notes} notes" if total_notes else "no notes"
            status_info = f" [{status_text}]" if status_text else ""
            print(f"{icon}{status_info} {note_info}")
        else:
            print("✗ Not found")

        output_rows.append([
            company_name, first_name, email, ceo_name, domain,
            found_in_affinity, org_id_str,
            on_sourcing_list, status_text, responded_text, outreach_text,
            other_lists,
            str(total_notes), latest_note_date, latest_note_preview,
            all_activity_dates,
            activity_timeline,
            relationship_summary,
            datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        ])

        time.sleep(0.15)

    # Write to spreadsheet
    print("\nWriting to spreadsheet...")
    sheet.update(range_name='A1', values=output_rows)

    # Print summary
    total = len(rows)
    print(f"\n{'=' * 60}")
    print(f"ANALYSIS COMPLETE - {total} companies")
    print(f"{'=' * 60}")
    print(f"  Found in Affinity:        {stats['found']}/{total}")
    print(f"  On 1a Sourcing List:      {stats['on_list']}/{total}")
    print(f"  Status = Passed:          {stats['passed']}/{total}")
    print(f"  Have responded:           {stats['responded']}/{total}")
    print(f"  Have activity notes:      {stats['has_notes']}/{total}")
    print(f"  Net new (not in Affinity): {total - stats['found']}/{total}")
    print(f"\nSpreadsheet: https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}")
    print(f"{'=' * 60}")

if __name__ == "__main__":
    main()
