import os
import re
import time
import json
from datetime import datetime
import requests
import gspread
from google.oauth2.service_account import Credentials

# ---------- Configuration ----------

HARMONIC_API_KEY = os.environ["HARMONIC_API_KEY"]
AFFINITY_API_KEY = os.environ["AFFINITY_API_KEY"]
SPREADSHEET_ID = "1sdbE6V-qVNuKo9LKe42i8x9Nj6ENX8N5oeZsSjRwT9o"

HARMONIC_BASE = "https://api.harmonic.ai"
HARMONIC_HEADERS = {"apikey": HARMONIC_API_KEY, "Content-Type": "application/json"}
AFFINITY_BASE = "https://api.affinity.co"

TARGET_LIST_ID = 21233  # 1a Sourcing List

# Joshua Browder's known portfolio from web research (supplementing Harmonic)
WEB_SOURCED_COMPANIES = [
    "Haladir", "RMFG", "Supermemory", "Pilgrim", "PromptLayer",
    "Kick", "Orchard Robotics", "Greptile", "Galvanick", "Somethings",
    "Posh", "micro1", "Wander", "Assured", "Lucent AI",
    "The Antifraud Company", "Whop", "Yuzu Health", "Slash",
    "Loyal", "Glencoco",
]

# Late-stage / unicorns to exclude (not early stage)
EXCLUDE_COMPANIES = {
    "figma", "mercury", "riverside", "jeeves", "deel", "owner.com",
    "donotpay", "coder",
}

# ---------- Helpers ----------

def chunked(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i+n]

def normalize_name(name):
    if not name:
        return ""
    name = name.lower().strip()
    name = re.sub(r'\s+(inc|llc|ltd|corp|co|company)\.?$', '', name, flags=re.IGNORECASE)
    name = re.sub(r'[^a-z0-9\s]', '', name)
    name = re.sub(r'\s+', ' ', name).strip()
    return name

# ---------- Harmonic API ----------

def harmonic_get(endpoint, params=None):
    r = requests.get(f"{HARMONIC_BASE}{endpoint}", headers=HARMONIC_HEADERS, params=params, timeout=60)
    r.raise_for_status()
    return r.json()

def harmonic_post(endpoint, payload):
    r = requests.post(f"{HARMONIC_BASE}{endpoint}", headers=HARMONIC_HEADERS, json=payload, timeout=60)
    r.raise_for_status()
    return r.json()

def search_company_by_name(name):
    """Search Harmonic for a company by name using typeahead."""
    try:
        data = harmonic_get("/search/typeahead", {"query": name})
        results = data if isinstance(data, list) else data.get("results", [])
        for r in results:
            if r.get("type") == "COMPANY":
                return r.get("entity_urn")
    except Exception as e:
        print(f"    Typeahead error for {name}: {e}")
    return None

def batch_get_companies(urns):
    out = []
    for chunk in chunked(urns, 50):
        data = harmonic_post("/companies/batchGet", {"urns": chunk})
        items = data if isinstance(data, list) else data.get("results", [])
        out.extend(items)
        time.sleep(0.2)
    return out

def batch_get_persons(urns):
    out = {}
    for chunk in chunked(urns, 50):
        data = harmonic_post("/persons/batchGet", {"urns": chunk})
        people = data if isinstance(data, list) else data.get("results", [])
        for p in people:
            urn = p.get("entity_urn") or p.get("person_urn")
            if urn:
                out[urn] = p
        time.sleep(0.2)
    return out

# ---------- Affinity Check ----------

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

    def search_org(self, name, domain=None):
        if domain:
            data = self._get("/organizations", {"term": domain, "page_size": 5})
            if data:
                orgs = data.get("organizations", []) if isinstance(data, dict) else data
                for org in (orgs or []):
                    if (org.get("domain") or "").lower().strip() == domain.lower():
                        return org
        if name:
            data = self._get("/organizations", {"term": name, "page_size": 5})
            if data:
                orgs = data.get("organizations", []) if isinstance(data, dict) else data
                norm = normalize_name(name)
                for org in (orgs or []):
                    if normalize_name(org.get("name") or "") == norm:
                        return org
        return None

    def has_any_interaction(self, org_id):
        org_detail = self._get(f"/organizations/{org_id}")
        if not org_detail:
            return False, ""
        list_entries = org_detail.get("list_entries", [])
        for entry in list_entries:
            if entry.get("list_id") == TARGET_LIST_ID:
                return True, "On 1a Sourcing List"
        notes = self._get("/notes", {"organization_id": org_id, "page_size": 5})
        if notes:
            note_list = notes if isinstance(notes, list) else notes.get("notes", [])
            if note_list:
                return True, f"{len(note_list)} notes"
        if list_entries:
            return True, "On other Affinity list"
        return False, ""

# ---------- Google Sheets ----------

def get_sheet():
    scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
    creds = Credentials.from_service_account_file('credentials.json', scopes=scopes)
    gc = gspread.authorize(creds)
    return gc.open_by_key(SPREADSHEET_ID).sheet1

# ---------- CEO Finder ----------

def find_ceo_candidates(company):
    people = company.get("people") or []
    candidates = []
    for person in people:
        if not isinstance(person, dict):
            continue
        if not person.get("is_current_position", False):
            continue
        title = (person.get("title") or "").lower()
        person_urn = person.get("person") or person.get("person_urn") or person.get("entity_urn") or ""
        if not person_urn:
            continue
        if "ceo" in title or "chief executive" in title:
            candidates.append({"urn": person_urn, "priority": 1})
        elif "founder" in title or "co-founder" in title:
            candidates.append({"urn": person_urn, "priority": 2})
    candidates.sort(key=lambda x: x["priority"])
    return candidates

# ---------- Main ----------

def main():
    print("=" * 60)
    print("Joshua Browder / Browder Capital Portfolio - Early Stage")
    print("=" * 60)
    print()

    affinity = AffinityClient(AFFINITY_API_KEY)

    # Step 1: Get Joshua Browder's investments from Harmonic person record
    print("[1/6] Finding Joshua Browder's investments on Harmonic...")

    # Find person URN via typeahead
    typeahead = harmonic_get("/search/typeahead", {"query": "Joshua Browder"})
    person_urn = None
    for r in (typeahead if isinstance(typeahead, list) else typeahead.get("results", [])):
        if r.get("type") == "PERSON" and "browder" in (r.get("text") or "").lower():
            person_urn = r.get("entity_urn")
            break

    harmonic_portfolio_urns = []
    if person_urn:
        print(f"  Found: {person_urn}")
        person_data = harmonic_get(f"/persons/{person_urn}")
        experience = person_data.get("experience") or []
        for exp in experience:
            if exp.get("role_type") == "INVESTOR":
                comp_urn = exp.get("company")
                comp_name = exp.get("company_name") or ""
                if comp_urn:
                    harmonic_portfolio_urns.append(comp_urn)
                    print(f"    Harmonic investment: {comp_name} ({comp_urn})")
        print(f"  Found {len(harmonic_portfolio_urns)} investments in Harmonic person record")
    else:
        print("  Could not find Joshua Browder on Harmonic")

    # Step 2: Search for web-sourced companies on Harmonic
    print(f"\n[2/6] Searching Harmonic for {len(WEB_SOURCED_COMPANIES)} web-sourced portfolio companies...")
    web_company_urns = []
    for name in WEB_SOURCED_COMPANIES:
        if normalize_name(name) in EXCLUDE_COMPANIES:
            print(f"  {name} - SKIP (late stage)")
            continue
        urn = search_company_by_name(name)
        if urn:
            web_company_urns.append(urn)
            print(f"  {name} - found ({urn})")
        else:
            print(f"  {name} - NOT FOUND on Harmonic")
        time.sleep(0.15)

    # Combine and dedupe URNs
    all_urns = list(dict.fromkeys(harmonic_portfolio_urns + web_company_urns))
    print(f"\n  Total unique company URNs: {len(all_urns)}")

    # Step 3: Batch fetch company details
    print(f"\n[3/6] Fetching full company details from Harmonic...")
    companies = batch_get_companies(all_urns)
    print(f"  Got {len(companies)} company records")

    # Step 4: Filter for early stage only
    print(f"\n[4/6] Filtering for early-stage startups...")
    early_stage = []
    skipped = {"late_stage": [], "excluded": [], "no_data": []}

    for c in companies:
        name = c.get("name") or ""
        stage = (c.get("stage") or "").upper()
        funding = c.get("funding") or {}
        funding_total = funding.get("funding_total") or 0
        funding_stage = (c.get("funding_stage") or "").upper()

        # Skip known late-stage
        if normalize_name(name) in EXCLUDE_COMPANIES:
            skipped["excluded"].append(name)
            continue

        # Check if early stage: pre-seed, seed, or Series A at most
        is_early = False

        # Check funding stage
        if funding_stage in ("PRE_SEED", "SEED", "SERIES_A", "ANGEL", ""):
            is_early = True
        elif stage in ("PRE_SEED", "SEED", "SERIES_A", "ANGEL", ""):
            is_early = True

        # Also accept if total funding < $15M (could be missed stage)
        if not is_early and funding_total < 15_000_000:
            is_early = True

        # But exclude if clearly late (Series B+, or very high funding)
        if funding_stage in ("SERIES_B", "SERIES_C", "SERIES_D", "SERIES_E", "IPO", "PUBLIC"):
            is_early = False
        if funding_total > 30_000_000:
            is_early = False

        if is_early:
            early_stage.append(c)
            print(f"  ✓ {name} - {funding_stage or stage or 'Unknown stage'} (${funding_total:,.0f})")
        else:
            skipped["late_stage"].append(f"{name} ({funding_stage or stage}, ${funding_total:,.0f})")
            print(f"  ✗ {name} - {funding_stage or stage} (${funding_total:,.0f}) - too late")

    print(f"\n  Early stage: {len(early_stage)}")
    print(f"  Late stage / excluded: {len(skipped['late_stage']) + len(skipped['excluded'])}")
    if skipped["late_stage"]:
        for s in skipped["late_stage"]:
            print(f"    - {s}")

    # Step 5: Get CEO info + check Affinity
    print(f"\n[5/6] Getting CEO info and checking Affinity...")

    # Collect all person URNs
    all_person_urns = []
    company_candidates = {}
    for c in early_stage:
        urn = c.get("entity_urn") or ""
        candidates = find_ceo_candidates(c)
        company_candidates[urn] = candidates
        for cand in candidates:
            all_person_urns.append(cand["urn"])

    all_person_urns = list(dict.fromkeys(all_person_urns))
    print(f"  Fetching {len(all_person_urns)} person records...")
    people_by_urn = batch_get_persons(all_person_urns) if all_person_urns else {}

    final = []
    no_ceo = []
    in_affinity = []

    for c in early_stage:
        urn = c.get("entity_urn") or ""
        name = c.get("name") or ""
        candidates = company_candidates.get(urn, [])

        # Find CEO with email
        ceo_name = ""
        first_name = ""
        email = ""

        for cand in candidates:
            person_data = people_by_urn.get(cand["urn"], {})
            contact = person_data.get("contact") or {}
            p_email = (contact.get("primary_email") or "").strip()
            if not p_email:
                emails = contact.get("emails") or []
                if emails:
                    p_email = (emails[0] if isinstance(emails[0], str) else "").strip()
            p_name = person_data.get("full_name") or person_data.get("name") or ""

            if p_email and p_name:
                ceo_name = p_name
                first_name = p_name.split()[0] if p_name else ""
                email = p_email
                break

        if not ceo_name or not email:
            no_ceo.append(name)
            print(f"  {name} - no CEO email found")
            continue

        # Check Affinity
        website = c.get("website") or {}
        domain = ""
        if isinstance(website, dict):
            url = website.get("url") or ""
            domain = url.replace("https://", "").replace("http://", "").replace("www.", "").split("/")[0] if url else ""

        print(f"  {name} ({ceo_name}, {email})...", end=" ", flush=True)

        org = affinity.search_org(name, domain)
        affinity_status = "Not in Affinity"
        if org:
            org_id = org.get("id")
            has_interaction, reason = affinity.has_any_interaction(org_id)
            if has_interaction:
                in_affinity.append(f"{name} ({reason})")
                affinity_status = reason
                print(f"⚠ {reason}")
            else:
                print("✓ In Affinity, no interactions")
                affinity_status = "In Affinity, no interactions"
        else:
            print("✓ Not in Affinity")

        final.append({
            "company": c,
            "ceo_name": ceo_name,
            "first_name": first_name,
            "email": email,
            "domain": domain,
            "affinity_status": affinity_status,
        })
        time.sleep(0.15)

    print(f"\n  Total with CEO+email: {len(final)}")
    print(f"  No CEO email: {len(no_ceo)}")
    if no_ceo:
        for n in no_ceo:
            print(f"    - {n}")
    print(f"  Already in Affinity with interactions: {len(in_affinity)}")
    if in_affinity:
        for n in in_affinity:
            print(f"    - {n}")

    # Step 6: Write to spreadsheet
    print(f"\n[6/6] Writing to spreadsheet...")
    sheet = get_sheet()
    sheet.clear()

    headers = [
        "companyName", "firstName", "email", "ceoName", "domain",
        "Investor", "Stage", "Funding Total",
        "Headcount", "Country", "City", "Customer Type",
        "Tags", "Founded", "Description",
        "Affinity Status"
    ]

    output_rows = [headers]

    for item in final:
        c = item["company"]
        funding = c.get("funding") or {}
        funding_total = funding.get("funding_total") or 0
        location = c.get("location") or {}
        tags = c.get("tags") or []
        fd = c.get("founding_date") or {}
        funding_stage = c.get("funding_stage") or c.get("stage") or ""

        headcount = c.get("headcount") or c.get("corrected_headcount") or ""
        if isinstance(headcount, dict):
            headcount = headcount.get("latest_metric_value") or ""

        founded = ""
        if fd.get("year"):
            founded = str(fd["year"])
            if fd.get("month"):
                founded = f"{fd['month']}/{fd['year']}"

        output_rows.append([
            c.get("name", ""),
            item["first_name"],
            item["email"],
            item["ceo_name"],
            item["domain"],
            "Joshua Browder / Browder Capital",
            funding_stage,
            f"${funding_total:,.0f}" if funding_total else "No funding",
            str(headcount),
            location.get("country", ""),
            location.get("city", ""),
            c.get("customer_type", ""),
            ", ".join([t.get("display_value", "") for t in tags[:5]]),
            founded,
            (c.get("description") or "")[:200],
            item["affinity_status"],
        ])

    sheet.update(range_name='A1', values=output_rows)

    print(f"\n{'=' * 60}")
    print(f"DONE! {len(final)} early-stage Browder portfolio companies")
    print(f"  All have CEO name + email")
    print(f"  Affinity status included")
    print(f"\nSpreadsheet: https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}")
    print(f"{'=' * 60}")

if __name__ == "__main__":
    main()
