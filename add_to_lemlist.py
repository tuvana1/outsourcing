import os
import re
import time
import requests
import gspread
from google.oauth2.service_account import Credentials

# ---------- Configuration ----------

LEMLIST_API_KEY = os.environ["LEMLIST_API_KEY"]
CAMPAIGN_ID = os.environ["LEMLIST_CAMPAIGN_ID"]
SPREADSHEET_ID = os.environ["SPREADSHEET_ID"]
AFFINITY_API_KEY = os.environ["AFFINITY_API_KEY"]
AFFINITY_BASE = "https://api.affinity.co"

# ---------- Google Sheets ----------

def get_sheet():
    scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
    creds = Credentials.from_service_account_file('credentials.json', scopes=scopes)
    gc = gspread.authorize(creds)
    return gc.open_by_key(SPREADSHEET_ID).sheet1

# ---------- Lemlist API ----------

def add_lead_to_campaign(email, first_name, company_name, campaign_id):
    """Add a single lead to a Lemlist campaign."""
    url = f"https://api.lemlist.com/api/campaigns/{campaign_id}/leads/{email}"
    payload = {
        "firstName": first_name,
        "companyName": company_name,
    }
    r = requests.post(url, json=payload, auth=('', LEMLIST_API_KEY), timeout=60)
    return r.status_code, r.json() if r.status_code in (200, 201) else r.text

# ---------- Main ----------

def main():
    print("=" * 60)
    print(f"Adding leads to Lemlist: Tuvana's campaign (1)")
    print("=" * 60)

    # Read spreadsheet
    print("\nReading spreadsheet...")
    sheet = get_sheet()
    all_data = sheet.get_all_values()
    headers = all_data[0]
    rows = all_data[1:]

    # Find column indices
    col = {h: i for i, h in enumerate(headers)}

    # Filter to rows with company name and email (user cleared rows they don't want)
    leads = []
    skipped_no_email = []
    skipped_empty = 0

    for row in rows:
        company = row[col["companyName"]].strip() if "companyName" in col else ""
        email = row[col["email"]].strip() if "email" in col else ""
        first_name = row[col["firstName"]].strip() if "firstName" in col else ""

        if not company:
            skipped_empty += 1
            continue
        if not email:
            skipped_no_email.append(company)
            continue

        leads.append({
            "email": email,
            "firstName": first_name,
            "companyName": company,
        })

    print(f"  Total rows: {len(rows)}")
    print(f"  Cleared by you (empty): {skipped_empty}")
    print(f"  Skipped (no email): {len(skipped_no_email)}")
    if skipped_no_email:
        for s in skipped_no_email:
            print(f"    - {s}")
    print(f"  Leads to add: {len(leads)}")

    # Add leads to Lemlist
    print(f"\nAdding {len(leads)} leads to Lemlist...")
    added = 0
    already_exists = 0
    failed = 0

    for i, lead in enumerate(leads):
        print(f"  [{i+1}/{len(leads)}] {lead['companyName']} ({lead['email']})...", end=" ", flush=True)

        status_code, response = add_lead_to_campaign(
            lead["email"], lead["firstName"], lead["companyName"], CAMPAIGN_ID
        )

        if status_code in (200, 201):
            print("✓ Added")
            added += 1
        elif status_code == 409:
            print("⊘ Already exists")
            already_exists += 1
        else:
            print(f"✗ Failed ({status_code}: {str(response)[:80]})")
            failed += 1

        time.sleep(0.3)  # Rate limiting

    print(f"\n{'=' * 60}")
    print(f"DONE!")
    print(f"  Added: {added}")
    print(f"  Already existed: {already_exists}")
    print(f"  Failed: {failed}")
    print(f"{'=' * 60}")

if __name__ == "__main__":
    main()
