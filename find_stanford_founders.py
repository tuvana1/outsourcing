#!/usr/bin/env python3
"""Find top 20 startups with Stanford-educated founders."""

import os, re, time, json, csv, io
from datetime import datetime
import requests
import gspread
from google.oauth2.service_account import Credentials

HARMONIC_API_KEY = os.environ["HARMONIC_API_KEY"]
AFFINITY_API_KEY = os.environ["AFFINITY_API_KEY"]
LEMLIST_API_KEY = os.environ["LEMLIST_API_KEY"]
SPREADSHEET_ID = os.environ["SPREADSHEET_ID"]

HARMONIC_BASE = "https://api.harmonic.ai"
HARMONIC_HEADERS = {"apikey": HARMONIC_API_KEY, "Content-Type": "application/json"}
AFFINITY_BASE = "https://api.affinity.co"
TARGET_LIST_ID = int(os.environ["AFFINITY_LIST_ID"])

EXCLUDED_TAGS_KEYWORDS = {
    "hardware", "biotech", "biotechnology", "pharmaceutical", "medical devices",
    "semiconductors", "robotics", "3d printing", "manufacturing", "clean energy",
    "solar", "battery", "cannabis", "nonprofit", "non-profit", "charity",
    "government", "real estate", "construction", "agriculture", "mining",
    "oil", "gas", "energy", "healthcare", "health care", "healthtech",
    "medical", "clinical", "patient", "hospital", "telehealth",
}

def chunked(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i+n]

def normalize_name(name):
    if not name: return ""
    name = name.lower().strip()
    name = re.sub(r'\s+(inc|llc|ltd|corp|co|company)\.?$', '', name, flags=re.IGNORECASE)
    name = re.sub(r'[^a-z0-9\s]', '', name)
    return re.sub(r'\s+', ' ', name).strip()

def search_companies(page_size=200, start=0):
    r = requests.post(f"{HARMONIC_BASE}/search/companies", headers=HARMONIC_HEADERS, json={
        "query": {
            "filter_group": {
                "join_operator": "and",
                "filters": [
                    {"field": "company_funding_stage", "comparator": "anyOf", "filter_value": ["PRE_SEED", "SEED", "SERIES_A"]},
                    {"field": "company_and_employee_highlight_count", "comparator": "greaterThanOrEquals", "filter_value": 2},
                    {"field": "company_country", "comparator": "anyOf", "filter_value": ["United States"]},
                ],
            },
            "pagination": {"page_size": page_size, "start": start}
        },
        "sort": {"field": "relevance_score", "descending": True}
    }, timeout=60)
    r.raise_for_status()
    return r.json()

def batch_get_companies(urns):
    out = []
    total = len(urns)
    for i, chunk in enumerate(chunked(urns, 50)):
        for attempt in range(3):
            try:
                r = requests.post(f"{HARMONIC_BASE}/companies/batchGet", headers=HARMONIC_HEADERS, json={"urns": chunk}, timeout=180)
                r.raise_for_status()
                data = r.json()
                out.extend(data if isinstance(data, list) else data.get("results", []))
                break
            except Exception as e:
                if attempt < 2:
                    print(f"    Retry {attempt+1} ({type(e).__name__})...")
                    time.sleep(3)
                else:
                    print(f"    Skipping batch: {e}")
        print(f"  {min((i+1)*50, total)}/{total} companies fetched", flush=True)
        time.sleep(0.3)
    return out

def batch_get_persons(urns):
    out = {}
    total = len(urns)
    for i, chunk in enumerate(chunked(urns, 20)):
        for attempt in range(3):
            try:
                r = requests.post(f"{HARMONIC_BASE}/persons/batchGet", headers=HARMONIC_HEADERS, json={"urns": chunk}, timeout=180)
                r.raise_for_status()
                data = r.json()
                people = data if isinstance(data, list) else data.get("results", [])
                for p in people:
                    urn = p.get("entity_urn") or p.get("person_urn")
                    if urn:
                        out[urn] = p
                break
            except Exception as e:
                if attempt < 2:
                    print(f"    Retry {attempt+1} ({type(e).__name__})...")
                    time.sleep(3)
                else:
                    print(f"    Skipping batch: {e}")
        if (i+1) % 5 == 0 or (i+1)*20 >= total:
            print(f"  {min((i+1)*20, total)}/{total} persons fetched", flush=True)
        time.sleep(0.3)
    return out

def is_stanford(person):
    """Check if person has Stanford education."""
    edu_list = person.get("education") or []
    for edu in edu_list:
        school = edu.get("school") or {}
        school_name = (school.get("name") or "").lower()
        if "stanford" in school_name:
            return True
    return False

def get_stanford_details(person):
    """Get Stanford degree details."""
    edu_list = person.get("education") or []
    parts = []
    for edu in edu_list:
        school = edu.get("school") or {}
        school_name = (school.get("name") or "").lower()
        if "stanford" in school_name:
            degree = edu.get("degree") or edu.get("standardized_degree") or ""
            field = edu.get("field") or ""
            detail = school.get("name", "Stanford")
            if degree and degree != "NA":
                detail += f" - {degree}"
            if field and field != "NA":
                detail += f" - {field}"
            parts.append(detail)
    return "; ".join(parts)

ELITE_COMPANIES = {
    "google", "meta", "facebook", "apple", "amazon", "microsoft", "netflix",
    "stripe", "brex", "airbnb", "uber", "lyft", "doordash",
    "coinbase", "robinhood", "plaid", "figma", "notion", "slack", "discord",
    "snowflake", "databricks", "datadog", "palantir", "salesforce",
    "twitter", "x", "linkedin", "snap", "pinterest", "spotify",
    "openai", "anthropic", "deepmind", "tesla", "spacex",
    "square", "block", "shopify", "dropbox",
    "nvidia", "oracle", "cisco",
    "mckinsey", "bain", "bcg", "goldman sachs", "morgan stanley",
    "jp morgan", "jpmorgan", "a16z", "sequoia", "y combinator", "yc",
}

def compute_score(company, person):
    score = 0
    # Traction
    tm = company.get("traction_metrics") or {}
    hc = tm.get("corrected_headcount") or {}
    hc_180 = hc.get("180d_ago", {})
    hc_change = hc_180.get("change")
    if hc_change and hc_change > 0:
        score += min(hc_change * 5, 25)
    wt = tm.get("web_traffic") or {}
    wt_pct = (wt.get("180d_ago") or {}).get("percent_change")
    if wt_pct and wt_pct > 0:
        score += min(wt_pct / 10, 20)
    li = tm.get("linkedin_follower_count") or {}
    li_pct = (li.get("180d_ago") or {}).get("percent_change")
    if li_pct and li_pct > 0:
        score += min(li_pct / 5, 15)
    highlights = company.get("highlights") or []
    score += min(len(highlights) * 3, 15)
    # Funding sweet spot
    funding = company.get("funding") or {}
    funding_total = funding.get("funding_total") or 0
    headcount = company.get("headcount") or 1
    if isinstance(headcount, dict):
        headcount = headcount.get("latest_metric_value") or 1
    if headcount > 3 and funding_total < 2_000_000:
        score += 10
    if 1_000_000 <= funding_total <= 5_000_000 and headcount > 5:
        score += 15
    # Founder background
    if person:
        experience = person.get("experience") or []
        seen_co = set()
        for exp in experience:
            co = (exp.get("company_name") or "").lower()
            title = (exp.get("title") or "").lower()
            for elite in ELITE_COMPANIES:
                if elite in co and co not in seen_co:
                    seen_co.add(co)
                    score += 15
                    if any(w in title for w in ["cto", "vp", "director", "head of", "chief", "principal"]):
                        score += 10
                    break
        p_highlights = person.get("highlights") or []
        for h in p_highlights:
            cat = (h.get("category") or "").lower()
            if "prior_exit" in cat: score += 20
            elif "serial_founder" in cat: score += 15
            elif "major_tech" in cat or "faang" in cat: score += 10
            elif "top_university" in cat: score += 5
    return score

def find_ceo_candidates(company):
    people = company.get("people") or []
    candidates = []
    for person in people:
        if not isinstance(person, dict): continue
        if not person.get("is_current_position", False): continue
        title = (person.get("title") or "").lower()
        person_urn = person.get("person") or person.get("person_urn") or person.get("entity_urn") or ""
        if not person_urn: continue
        if "ceo" in title or "chief executive" in title:
            candidates.append({"urn": person_urn, "priority": 1, "title": person.get("title", "")})
        elif "founder" in title or "co-founder" in title:
            candidates.append({"urn": person_urn, "priority": 2, "title": person.get("title", "")})
        elif "cto" in title or "chief technology" in title:
            candidates.append({"urn": person_urn, "priority": 3, "title": person.get("title", "")})
    candidates.sort(key=lambda x: x["priority"])
    return candidates

def extract_founder_background(person):
    if not person:
        return {"education": "", "prev_companies": "", "linkedin": "", "headline": "", "highlights": ""}
    edu_list = person.get("education") or []
    edu_parts = []
    for edu in edu_list:
        school = edu.get("school") or {}
        school_name = school.get("name") or ""
        degree = edu.get("degree") or edu.get("standardized_degree") or ""
        field = edu.get("field") or ""
        if school_name:
            parts = [school_name]
            if degree and degree != "NA": parts.append(degree)
            if field and field != "NA": parts.append(field)
            edu_parts.append(" - ".join(parts))
    education = "; ".join(edu_parts[:3])

    experience = person.get("experience") or []
    prev = []
    for exp in experience:
        if not exp.get("is_current_position", False):
            co = exp.get("company_name") or ""
            title = exp.get("title") or ""
            if co and co != "urn:harmonic:company:-1":
                prev.append(f"{title} @ {co}" if title else co)
    prev_companies = "; ".join(prev[:4])

    socials = person.get("socials") or {}
    li = socials.get("LINKEDIN") or {}
    linkedin = li.get("url") or ""
    headline = person.get("linkedin_headline") or ""

    p_highlights = person.get("highlights") or []
    highlight_texts = [h.get("category", "").replace("_", " ").title() for h in p_highlights if h.get("category")]
    highlights = ", ".join(highlight_texts[:5])

    return {"education": education, "prev_companies": prev_companies, "linkedin": linkedin, "headline": headline, "highlights": highlights}

def main():
    print("=" * 60)
    print("Find ALL Stanford-Founded Startups (any funding amount)")
    print("=" * 60, flush=True)

    # Load existing companies from all sheets
    scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
    creds = Credentials.from_service_account_file('credentials.json', scopes=scopes)
    gc = gspread.authorize(creds)
    spreadsheet = gc.open_by_key(SPREADSHEET_ID)

    existing_names = set()
    for ws in spreadsheet.worksheets():
        try:
            ws_data = ws.get_all_values()
            for row in ws_data[1:]:
                name = row[0] if len(row) > 0 else ""
                if name.strip():
                    existing_names.add(normalize_name(name))
        except: pass
    print(f"Existing companies to skip: {len(existing_names)}")

    # Load Lemlist contacts
    print("Loading Lemlist contacts...", flush=True)
    lemlist_emails = set()
    lemlist_companies = set()
    try:
        r = requests.get('https://api.lemlist.com/api/campaigns', auth=('', LEMLIST_API_KEY), timeout=60)
        if r.status_code == 200:
            for c in json.loads(r.text):
                cid = c.get('_id', '')
                r2 = requests.get(f'https://api.lemlist.com/api/campaigns/{cid}/export', auth=('', LEMLIST_API_KEY), timeout=60)
                if r2.status_code == 200 and r2.text:
                    for row in csv.DictReader(io.StringIO(r2.text)):
                        email = (row.get('email') or '').strip().lower()
                        company = (row.get('companyName') or '').strip().lower()
                        if email: lemlist_emails.add(email)
                        if company: lemlist_companies.add(normalize_name(company))
    except Exception as e:
        print(f"  Lemlist check failed: {e}")
    print(f"  {len(lemlist_emails)} emails, {len(lemlist_companies)} companies in Lemlist")

    # Search Harmonic
    print("\n[1/5] Searching Harmonic...", flush=True)
    all_urns = []
    start = 0
    while len(all_urns) < 10000:
        data = search_companies(page_size=200, start=start)
        urns = data.get("results", [])
        if not urns: break
        all_urns.extend(urns)
        start += len(urns)
        print(f"  {len(all_urns)} URNs (total: {data.get('count', '?')})", flush=True)
        time.sleep(0.2)

    # Fetch company details
    print(f"\n[2/5] Fetching {len(all_urns)} company details...", flush=True)
    companies = batch_get_companies(all_urns)
    print(f"  Got {len(companies)} records")

    # Filter
    print("\n[3/5] Filtering...", flush=True)
    filtered = []
    seen = set()
    for c in companies:
        name = c.get("name") or ""
        norm = normalize_name(name)
        if norm in seen or norm in existing_names: continue
        seen.add(norm)

        location = c.get("location") or {}
        country = (location.get("country") or "").lower()
        if country not in ("united states", "us", "usa"): continue

        tags = c.get("tags") or []
        if any(any(exc in (t.get("display_value") or "").lower() for exc in EXCLUDED_TAGS_KEYWORDS) for t in tags): continue

        company_type = c.get("company_type") or ""
        if company_type and company_type.upper() not in ("STARTUP", ""): continue

        funding = c.get("funding") or {}
        funding_total = funding.get("funding_total") or 0
        filtered.append(c)

    print(f"  {len(filtered)} candidates after filtering")

    # Get all person URNs
    print(f"\n[4/5] Finding Stanford founders...", flush=True)
    all_person_urns = []
    company_candidates = {}
    for c in filtered:
        urn = c.get("entity_urn") or ""
        candidates = find_ceo_candidates(c)
        company_candidates[urn] = candidates
        for cand in candidates:
            all_person_urns.append(cand["urn"])

    all_person_urns = list(dict.fromkeys(all_person_urns))
    print(f"  Fetching {len(all_person_urns)} person records...", flush=True)
    people_by_urn = batch_get_persons(all_person_urns) if all_person_urns else {}

    # Find Stanford founders with emails
    stanford_leads = []
    for c in filtered:
        urn = c.get("entity_urn") or ""
        name = c.get("name") or ""
        candidates = company_candidates.get(urn, [])

        for cand in candidates:
            person_data = people_by_urn.get(cand["urn"], {})
            if not is_stanford(person_data): continue

            contact = person_data.get("contact") or {}
            p_email = (contact.get("primary_email") or "").strip()
            if not p_email:
                emails = contact.get("emails") or []
                if emails:
                    p_email = (emails[0] if isinstance(emails[0], str) else "").strip()
            p_name = person_data.get("full_name") or person_data.get("name") or ""

            if not p_email or not p_name: continue

            # Check Lemlist dedup
            if p_email.lower() in lemlist_emails: continue
            if normalize_name(name) in lemlist_companies: continue

            stanford_detail = get_stanford_details(person_data)
            score = compute_score(c, person_data)
            stanford_leads.append({
                "company": c,
                "person": person_data,
                "ceo_name": p_name,
                "first_name": p_name.split()[0] if p_name else "",
                "email": p_email,
                "title": cand["title"],
                "stanford_detail": stanford_detail,
                "score": score,
            })
            break  # One person per company

    # Sort by score descending
    stanford_leads.sort(key=lambda x: -x["score"])
    print(f"  Found {len(stanford_leads)} Stanford-founded companies with emails")
    if stanford_leads:
        print(f"  Top scores: {[(l['company'].get('name',''), l['score']) for l in stanford_leads[:10]]}")

    # Check Affinity - collect all clean ones
    print(f"\n  Checking Affinity...", flush=True)
    aff_session = requests.Session()
    aff_session.auth = ('', AFFINITY_API_KEY)

    final = []
    for item in stanford_leads:
        if len(final) >= 9999: break
        c = item["company"]
        name = c.get("name") or ""
        website = c.get("website") or {}
        domain = ""
        if isinstance(website, dict):
            url = website.get("url") or ""
            domain = url.replace("https://", "").replace("http://", "").replace("www.", "").split("/")[0] if url else ""

        print(f"  [{len(final)+1}] {name}...", end=" ", flush=True)

        # Search Affinity
        org = None
        if domain:
            r = aff_session.get(f"{AFFINITY_BASE}/organizations", params={"term": domain, "page_size": 5}, timeout=60)
            if r.status_code == 200:
                data = r.json()
                orgs = data.get("organizations", []) if isinstance(data, dict) else data
                for o in (orgs or []):
                    if (o.get("domain") or "").lower().strip() == domain.lower():
                        org = o
                        break
        if not org and name:
            r = aff_session.get(f"{AFFINITY_BASE}/organizations", params={"term": name, "page_size": 5}, timeout=60)
            if r.status_code == 200:
                data = r.json()
                orgs = data.get("organizations", []) if isinstance(data, dict) else data
                norm = normalize_name(name)
                for o in (orgs or []):
                    if normalize_name(o.get("name") or "") == norm:
                        org = o
                        break

        if org:
            org_id = org.get("id")
            r = aff_session.get(f"{AFFINITY_BASE}/organizations/{org_id}", timeout=60)
            if r.status_code == 200:
                detail = r.json()
                list_entries = detail.get("list_entries", [])
                if any(e.get("list_id") == TARGET_LIST_ID for e in list_entries):
                    print("SKIP (on 1a list)")
                    time.sleep(0.15)
                    continue
                if list_entries:
                    print("SKIP (on other list)")
                    time.sleep(0.15)
                    continue
            r2 = aff_session.get(f"{AFFINITY_BASE}/notes", params={"organization_id": org_id, "page_size": 5}, timeout=60)
            if r2.status_code == 200:
                notes = r2.json()
                note_list = notes if isinstance(notes, list) else notes.get("notes", [])
                if note_list:
                    print(f"SKIP ({len(note_list)} notes)")
                    time.sleep(0.15)
                    continue

        item["domain"] = domain
        final.append(item)
        print(f"OK ({item['stanford_detail'][:60]})")
        time.sleep(0.15)

    print(f"\n  Final: {len(final)} Stanford-founded startups")

    # Write to sheet
    print(f"\n[5/5] Writing to sheet...", flush=True)
    existing_sheets = [ws.title for ws in spreadsheet.worksheets()]
    sheet_num = 3
    while f"Sheet{sheet_num}" in existing_sheets:
        sheet_num += 1
    target_name = f"Sheet{sheet_num}"
    try:
        target = spreadsheet.worksheet(target_name)
    except:
        target = spreadsheet.add_worksheet(title=target_name, rows=50, cols=30)
    target.clear()

    headers = [
        "companyName", "firstName", "email", "ceoName", "domain",
        "Score", "Stanford Details", "Title",
        "Stage", "Funding Total", "Headcount",
        "Country", "City", "Customer Type",
        "Founder LinkedIn", "Founder Headline",
        "Founder Education", "Founder Previous Companies",
        "Founder Highlights",
        "Tags", "Company Highlights", "Founded",
        "Description"
    ]
    output = [headers]

    for item in final:
        c = item["company"]
        funding = c.get("funding") or {}
        funding_total = funding.get("funding_total") or 0
        location = c.get("location") or {}
        tags = c.get("tags") or []
        highlights = c.get("highlights") or []
        fd = c.get("founding_date") or {}
        fd_date = fd.get("date") or ""
        founded = fd_date[:4] if fd_date else ""

        headcount = c.get("headcount") or c.get("corrected_headcount") or ""
        if isinstance(headcount, dict):
            headcount = headcount.get("latest_metric_value") or ""

        bg = extract_founder_background(item.get("person"))

        clean_name = c.get("name", "")
        clean_name = re.sub(r'\s*\([^)]*\)\s*', ' ', clean_name).strip()
        clean_name = clean_name.encode('ascii', 'ignore').decode('ascii').strip()
        clean_name = re.sub(r',\s*Inc\.?$', '', clean_name).strip()

        output.append([
            clean_name,
            item["first_name"],
            item["email"],
            item["ceo_name"],
            item["domain"],
            str(item["score"]),
            item["stanford_detail"],
            item["title"],
            c.get("stage", ""),
            f"${funding_total:,.0f}" if funding_total else "No funding",
            str(headcount),
            location.get("country", ""),
            location.get("city", ""),
            c.get("customer_type", ""),
            bg["linkedin"],
            bg["headline"][:200] if bg["headline"] else "",
            bg["education"],
            bg["prev_companies"][:300] if bg["prev_companies"] else "",
            bg["highlights"],
            ", ".join([t.get("display_value", "") for t in tags[:5]]),
            ", ".join([h.get("category", "") for h in highlights[:5]]),
            founded,
            (c.get("description") or "")[:200],
        ])

    target.update(range_name='A1', values=output)
    print(f"\n{'='*60}")
    print(f"DONE! {len(final)} Stanford-founded startups written to {target_name}")
    print(f"https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}")
    print(f"{'='*60}")

if __name__ == "__main__":
    main()
