import os
import time
import requests
import gspread
from google.oauth2.service_account import Credentials

# ---------- Configuration ----------

AFFINITY_API_KEY = os.environ["AFFINITY_API_KEY"]
HARMONIC_API_KEY = os.environ["HARMONIC_API_KEY"]
SPREADSHEET_ID = os.environ["SPREADSHEET_ID"]
HARMONIC_BASE = "https://api.harmonic.ai"
HARMONIC_HEADERS = {"apikey": HARMONIC_API_KEY, "Content-Type": "application/json"}

PORTFOLIO_LIST_ID = 62359  # Portfolio Companies list

# ---------- Helpers ----------

def chunked(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i+n]

def get_spreadsheet():
    scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
    creds = Credentials.from_service_account_file('credentials.json', scopes=scopes)
    gc = gspread.authorize(creds)
    return gc.open_by_key(SPREADSHEET_ID)

# ---------- Main ----------

def main():
    print("=" * 60)
    print("Party Invite List - Portfolio Founders in SF")
    print("Cookin' Up the Next Batch - Mon Mar 23, 6-8:30pm @ Impulse SF")
    print("=" * 60)

    # Step 1: Get all portfolio companies from Affinity
    print("\n[1/4] Getting portfolio companies from Affinity...")
    s = requests.Session()
    s.auth = ('', AFFINITY_API_KEY)
    r = s.get(f'https://api.affinity.co/lists/{PORTFOLIO_LIST_ID}/list-entries',
              params={'page_size': 500}, timeout=60)
    r.raise_for_status()
    entries = r.json()
    if isinstance(entries, dict):
        entries = entries.get('list_entries', entries.get('entries', []))
    print(f"  Found {len(entries)} portfolio companies")

    # Extract domains
    companies_to_check = []
    for entry in entries:
        entity = entry.get("entity", {})
        name = entity.get("name", "")
        domain = entity.get("domain", "")
        domains = entity.get("domains", [])
        if domain:
            companies_to_check.append({"name": name, "domain": domain})
        elif domains:
            companies_to_check.append({"name": name, "domain": domains[0]})
        else:
            print(f"  Skipping {name} (no domain)")

    print(f"  {len(companies_to_check)} companies with domains")

    # Step 2: Look up each on Harmonic by domain, filter for SF
    print("\n[2/4] Checking Harmonic for SF location...")
    sf_companies = []

    for i, comp in enumerate(companies_to_check):
        domain = comp["domain"]
        name = comp["name"]

        try:
            r = requests.post(f"{HARMONIC_BASE}/search/companies", headers=HARMONIC_HEADERS, json={
                "query": {
                    "filter_group": {
                        "join_operator": "and",
                        "filters": [
                            {"field": "company_website_domain", "comparator": "anyOf", "filter_value": [domain]},
                        ],
                    },
                    "pagination": {"page_size": 1, "start": 0}
                }
            }, timeout=30)

            if r.status_code == 200:
                results = r.json().get("results", [])
                if results:
                    urn = results[0]
                    r2 = requests.post(f"{HARMONIC_BASE}/companies/batchGet", headers=HARMONIC_HEADERS,
                                       json={"urns": [urn]}, timeout=30)
                    if r2.status_code == 200:
                        data = r2.json()
                        company = data[0] if isinstance(data, list) and data else {}

                        location = company.get("location") or {}
                        city = (location.get("city") or "").lower()

                        if "san francisco" in city:
                            people = company.get("people") or []
                            founders = []
                            for person in people:
                                if not isinstance(person, dict):
                                    continue
                                if not person.get("is_current_position", False):
                                    continue
                                title = (person.get("title") or "").lower()
                                person_urn = person.get("person") or person.get("person_urn") or person.get("entity_urn") or ""
                                if any(kw in title for kw in ["ceo", "founder", "co-founder", "chief executive"]):
                                    founders.append({"urn": person_urn, "title": person.get("title", "")})

                            sf_companies.append({
                                "name": name,
                                "domain": domain,
                                "founders": founders,
                            })
                            print(f"  ✓ {name} ({len(founders)} founders)")
                        # else: not SF, skip silently
                else:
                    print(f"  ? {name} - not found on Harmonic")
            time.sleep(0.25)
        except Exception as e:
            print(f"  ✗ {name} - error: {e}")

        if (i + 1) % 25 == 0:
            print(f"  --- checked {i+1}/{len(companies_to_check)} ---")

    print(f"\n  SF portfolio companies: {len(sf_companies)}")

    # Step 3: Get founder contact details
    print("\n[3/4] Getting founder emails...")
    all_person_urns = []
    for comp in sf_companies:
        for f in comp["founders"]:
            if f["urn"]:
                all_person_urns.append(f["urn"])
    all_person_urns = list(dict.fromkeys(all_person_urns))

    people_by_urn = {}
    for chunk in chunked(all_person_urns, 20):
        try:
            r = requests.post(f"{HARMONIC_BASE}/persons/batchGet", headers=HARMONIC_HEADERS,
                              json={"urns": chunk}, timeout=60)
            if r.status_code == 200:
                data = r.json()
                people = data if isinstance(data, list) else data.get("results", [])
                for p in people:
                    urn = p.get("entity_urn") or p.get("person_urn")
                    if urn:
                        people_by_urn[urn] = p
        except Exception as e:
            print(f"  Warning: Person batch failed: {e}")
        time.sleep(0.3)
    print(f"  Fetched {len(people_by_urn)} person records")

    # Step 4: Write to Google Sheets
    print("\n[4/4] Writing to Google Sheets...")
    spreadsheet = get_spreadsheet()
    target_name = "Party Invites"
    try:
        target_sheet = spreadsheet.worksheet(target_name)
    except:
        target_sheet = spreadsheet.add_worksheet(title=target_name, rows=500, cols=20)
    target_sheet.clear()

    headers = ["firstName", "email", "companyName", "founderName", "title", "LinkedIn", "domain"]
    output_rows = [headers]
    added = 0

    for comp in sf_companies:
        for f in comp["founders"]:
            person = people_by_urn.get(f["urn"], {})
            full_name = person.get("full_name") or person.get("name") or ""
            first_name = full_name.split()[0] if full_name else ""

            contact = person.get("contact") or {}
            email = (contact.get("primary_email") or "").strip()
            if not email:
                emails = contact.get("emails") or []
                if emails and isinstance(emails[0], str):
                    email = emails[0].strip()

            socials = person.get("socials") or {}
            li = socials.get("LINKEDIN") or {}
            linkedin = li.get("url") or ""

            if not full_name:
                continue

            output_rows.append([first_name, email, comp["name"], full_name, f["title"], linkedin, comp["domain"]])
            added += 1

    target_sheet.update(range_name='A1', values=output_rows)

    print(f"\n{'=' * 60}")
    print(f"DONE! {added} founders from {len(sf_companies)} SF portfolio companies")
    print(f"Written to 'Party Invites' sheet")
    print(f"Spreadsheet: https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}")
    print(f"{'=' * 60}")

if __name__ == "__main__":
    main()
