import os
import re
import time
import json
import csv
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

EXCLUDED_COUNTRIES = {"china", "russia", "ukraine", "india"}
EXCLUDED_TAGS_KEYWORDS = {
    "hardware", "biotech", "biotechnology", "pharmaceutical", "medical devices",
    "semiconductors", "chip design", "electronics manufacturing", "robotics",
    "3d printing", "manufacturing", "clean energy", "solar", "battery",
    "cannabis", "marijuana", "nonprofit", "non-profit", "charity",
    "government", "public sector",
}
NONPROFIT_NAME_KEYWORDS = {
    "foundation", "council", "association", "institute", "society",
    "charity", "nonprofit", "non-profit", "ngo", "ministry",
    "committee", "coalition", "alliance", "federation", "bureau",
    "center for", "centre for", "crisis center", "rape crisis",
}

TARGET_LIST_ID = 21233

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

def is_excluded_country(location):
    country = (location.get("country") or "").lower().strip()
    return country in EXCLUDED_COUNTRIES

def is_excluded_industry(tags):
    for tag in (tags or []):
        display = (tag.get("display_value") or "").lower()
        if any(exc in display for exc in EXCLUDED_TAGS_KEYWORDS):
            return True
    return False

def is_nonprofit(name, company_type, tags, description):
    """Detect non-profits, NGOs, government entities."""
    name_lower = (name or "").lower()
    for kw in NONPROFIT_NAME_KEYWORDS:
        if kw in name_lower:
            return True
    ct = (company_type or "").lower()
    if ct in ("nonprofit", "government", "non_profit"):
        return True
    desc_lower = (description or "").lower()
    if "non-profit" in desc_lower or "nonprofit" in desc_lower or "501(c)" in desc_lower:
        return True
    return False

def is_pure_consumer(tags, customer_type):
    ct = (customer_type or "").lower()
    if "b2b" in ct:
        return False
    tag_texts = [(t.get("display_value") or "").lower() for t in (tags or [])]
    consumer_kw = ["consumer", "social media", "gaming", "entertainment", "fashion", "food delivery", "dating", "music", "sports"]
    b2b_kw = ["saas", "enterprise", "business", "b2b", "infrastructure", "devtools", "developer", "api", "platform", "fintech"]
    consumer_signals = sum(1 for t in tag_texts if any(w in t for w in consumer_kw))
    b2b_signals = sum(1 for t in tag_texts if any(w in t for w in b2b_kw))
    return consumer_signals > 0 and b2b_signals == 0 and "b2c" in ct

def compute_raise_score(company):
    """Score how likely a company is to raise soon. Higher = more likely."""
    score = 0
    tm = company.get("traction_metrics") or {}

    # Headcount growth (hiring = about to raise or just raised)
    hc = tm.get("corrected_headcount") or {}
    hc_180 = hc.get("180d_ago", {})
    hc_change = hc_180.get("change")
    if hc_change and hc_change > 0:
        score += min(hc_change * 5, 25)  # Up to 25 pts
    hc_90 = hc.get("90d_ago", {})
    hc_change_90 = hc_90.get("change")
    if hc_change_90 and hc_change_90 > 0:
        score += min(hc_change_90 * 8, 25)  # Recent growth more valuable

    # Web traffic growth
    wt = tm.get("web_traffic") or {}
    wt_180 = wt.get("180d_ago", {})
    wt_pct = wt_180.get("percent_change")
    if wt_pct and wt_pct > 0:
        score += min(wt_pct / 10, 20)  # Up to 20 pts

    # LinkedIn follower growth (awareness/traction)
    li = tm.get("linkedin_follower_count") or {}
    li_180 = li.get("180d_ago", {})
    li_pct = li_180.get("percent_change")
    if li_pct and li_pct > 0:
        score += min(li_pct / 5, 15)  # Up to 15 pts

    # Highlights count (more signals = more active)
    highlights = company.get("highlights") or []
    score += min(len(highlights) * 3, 15)  # Up to 15 pts

    # Stealth emergence (recently emerged = about to raise)
    emergence = company.get("stealth_emergence_date")
    if emergence:
        try:
            ed = datetime.fromisoformat(emergence.replace("Z", "+00:00"))
            days_ago = (datetime.now(ed.tzinfo) - ed).days
            if days_ago < 90:
                score += 20
            elif days_ago < 180:
                score += 15
            elif days_ago < 365:
                score += 10
        except:
            pass

    # Low funding relative to headcount = needs to raise
    funding = company.get("funding") or {}
    funding_total = funding.get("funding_total") or 0
    headcount = company.get("headcount") or company.get("corrected_headcount") or 1
    if isinstance(headcount, dict):
        headcount = headcount.get("latest_metric_value") or 1
    if headcount > 3 and funding_total < 2_000_000:
        score += 10  # Team but low funding = needs capital
    if funding_total == 0 and headcount > 2:
        score += 15  # No funding but building team = actively looking

    # Founding date - newer companies more likely raising
    fd = company.get("founding_date") or {}
    year = fd.get("year")
    if year and year >= 2024:
        score += 10
    elif year and year >= 2023:
        score += 5

    return score

# ---------- Harmonic API ----------

def search_companies(page_size=200, start=0):
    r = requests.post(f"{HARMONIC_BASE}/search/companies", headers=HARMONIC_HEADERS, json={
        "query": {
            "filter_group": {
                "join_operator": "and",
                "filters": [
                    {"field": "company_funding_stage", "comparator": "anyOf", "filter_value": ["PRE_SEED", "SEED"]},
                    {"field": "company_and_employee_highlight_count", "comparator": "greaterThanOrEquals", "filter_value": 5},
                    {"field": "company_headcount_real_change_180d_ago", "comparator": "greaterThanOrEquals", "filter_value": 1},
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
    for chunk in chunked(urns, 50):
        r = requests.post(f"{HARMONIC_BASE}/companies/batchGet", headers=HARMONIC_HEADERS, json={"urns": chunk}, timeout=60)
        r.raise_for_status()
        data = r.json()
        out.extend(data if isinstance(data, list) else data.get("results", []))
    return out

def batch_get_persons(urns):
    out = {}
    for chunk in chunked(urns, 50):
        r = requests.post(f"{HARMONIC_BASE}/persons/batchGet", headers=HARMONIC_HEADERS, json={"urns": chunk}, timeout=60)
        r.raise_for_status()
        data = r.json()
        people = data if isinstance(data, list) else data.get("results", [])
        for p in people:
            urn = p.get("entity_urn") or p.get("person_urn")
            if urn:
                out[urn] = p
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
    print("Find Top 100 Early-Stage Startups About to Raise")
    print("=" * 60)
    print("Filters: Pre-seed/Seed, growing headcount, 5+ highlights")
    print("Exclude: China/Russia/Ukraine/India, hardware, biotech, nonprofits")
    print("Require: CEO name + email")
    print()

    affinity = AffinityClient(AFFINITY_API_KEY)

    # Step 1: Search Harmonic
    print("[1/5] Searching Harmonic for high-traction early-stage startups...")
    all_urns = []
    start = 0
    while len(all_urns) < 1000:
        data = search_companies(page_size=200, start=start)
        urns = data.get("results", [])
        if not urns:
            break
        all_urns.extend(urns)
        start += len(urns)
        print(f"  Fetched {len(all_urns)} URNs (total available: {data.get('count', '?')})")
        time.sleep(0.2)
    print(f"  Total: {len(all_urns)}")

    # Step 2: Batch fetch details
    print("\n[2/5] Fetching company details...")
    companies = batch_get_companies(all_urns)
    print(f"  Got {len(companies)} records")

    # Step 3: Filter and score
    print("\n[3/5] Filtering and scoring...")
    stats = {"country": 0, "industry": 0, "nonprofit": 0, "consumer": 0, "no_startup": 0}
    scored = []

    for c in companies:
        name = c.get("name") or ""
        location = c.get("location") or {}
        tags = c.get("tags") or []
        customer_type = c.get("customer_type") or ""
        company_type = c.get("company_type") or ""
        description = c.get("description") or ""

        if is_excluded_country(location):
            stats["country"] += 1
            continue
        if is_excluded_industry(tags):
            stats["industry"] += 1
            continue
        if is_nonprofit(name, company_type, tags, description):
            stats["nonprofit"] += 1
            continue
        if is_pure_consumer(tags, customer_type):
            stats["consumer"] += 1
            continue
        if company_type and company_type.upper() not in ("STARTUP", ""):
            stats["no_startup"] += 1
            continue

        raise_score = compute_raise_score(c)
        scored.append((raise_score, c))

    # Sort by raise score (highest first)
    scored.sort(key=lambda x: -x[0])

    print(f"  Excluded - country: {stats['country']}, industry: {stats['industry']}, "
          f"nonprofit: {stats['nonprofit']}, consumer: {stats['consumer']}, not startup: {stats['no_startup']}")
    print(f"  Remaining candidates: {len(scored)}")
    print(f"  Top raise scores: {[s[0] for s in scored[:10]]}")

    # Step 4: Get CEO info, check Affinity, collect 100 with name+email
    print(f"\n[4/5] Getting CEO info and checking Affinity...")

    # First pass: get all person URNs for top candidates
    top_candidates = scored[:500]  # Check top 500 to find 100 with emails
    all_person_urns = []
    company_candidates = {}
    for _, c in top_candidates:
        urn = c.get("entity_urn") or ""
        candidates = find_ceo_candidates(c)
        company_candidates[urn] = candidates
        for cand in candidates:
            all_person_urns.append(cand["urn"])

    all_person_urns = list(dict.fromkeys(all_person_urns))
    print(f"  Fetching {len(all_person_urns)} person records...")
    people_by_urn = batch_get_persons(all_person_urns) if all_person_urns else {}

    # Second pass: find CEO+email, then check Affinity
    final = []
    skipped_no_ceo = 0
    skipped_affinity = 0

    for raise_score, c in top_candidates:
        if len(final) >= 100:
            break

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
            skipped_no_ceo += 1
            continue

        # Check Affinity
        website = c.get("website") or {}
        domain = ""
        if isinstance(website, dict):
            url = website.get("url") or ""
            domain = url.replace("https://", "").replace("http://", "").replace("www.", "").split("/")[0] if url else ""

        print(f"  [{len(final)+1}] {name} (score={raise_score:.0f})...", end=" ", flush=True)

        org = affinity.search_org(name, domain)
        if org:
            org_id = org.get("id")
            has_interaction, reason = affinity.has_any_interaction(org_id)
            if has_interaction:
                skipped_affinity += 1
                print(f"SKIP ({reason})")
                time.sleep(0.15)
                continue
            else:
                print("✓ No interactions")
        else:
            print("✓ Not in Affinity")

        final.append({
            "company": c,
            "ceo_name": ceo_name,
            "first_name": first_name,
            "email": email,
            "domain": domain,
            "raise_score": raise_score,
        })
        time.sleep(0.15)

    print(f"\n  Skipped (no CEO+email): {skipped_no_ceo}")
    print(f"  Skipped (already in Affinity): {skipped_affinity}")
    print(f"  Final list: {len(final)}")

    # Step 5: Write to spreadsheet
    print(f"\n[5/5] Writing to spreadsheet...")
    sheet = get_sheet()
    sheet.clear()

    headers = [
        "companyName", "firstName", "email", "ceoName", "domain",
        "Raise Score", "Stage", "Funding Total", "Headcount",
        "Headcount Growth (6mo)", "Web Traffic", "Web Traffic Growth (6mo)",
        "LinkedIn Followers", "LinkedIn Growth (6mo)",
        "Country", "City", "Customer Type",
        "Tags", "Highlights", "Founded",
        "Description"
    ]

    output_rows = [headers]

    for item in final:
        c = item["company"]
        funding = c.get("funding") or {}
        funding_total = funding.get("funding_total") or 0
        location = c.get("location") or {}
        tags = c.get("tags") or []
        highlights = c.get("highlights") or []
        tm = c.get("traction_metrics") or {}
        fd = c.get("founding_date") or {}

        hc = tm.get("corrected_headcount") or {}
        hc_180 = hc.get("180d_ago", {})
        wt = tm.get("web_traffic") or {}
        wt_180 = wt.get("180d_ago", {})
        li = tm.get("linkedin_follower_count") or {}
        li_180 = li.get("180d_ago", {})

        headcount = c.get("headcount") or c.get("corrected_headcount") or ""
        if isinstance(headcount, dict):
            headcount = headcount.get("latest_metric_value") or ""

        hc_change = hc_180.get("change")
        hc_growth = f"+{hc_change:.0f}" if hc_change and hc_change > 0 else str(hc_change or 0)

        wt_val = wt.get("latest_metric_value") or c.get("web_traffic") or ""
        wt_pct = wt_180.get("percent_change")
        wt_growth = f"{wt_pct:+.0f}%" if wt_pct else ""

        li_val = li.get("latest_metric_value") or ""
        li_pct = li_180.get("percent_change")
        li_growth = f"{li_pct:+.0f}%" if li_pct else ""

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
            f"{item['raise_score']:.0f}",
            c.get("stage", ""),
            f"${funding_total:,.0f}" if funding_total else "No funding",
            str(headcount),
            hc_growth,
            str(wt_val),
            wt_growth,
            str(li_val),
            li_growth,
            location.get("country", ""),
            location.get("city", ""),
            c.get("customer_type", ""),
            ", ".join([t.get("display_value", "") for t in tags[:5]]),
            ", ".join([h.get("category", "") for h in highlights[:5]]),
            founded,
            (c.get("description") or "")[:200],
        ])

    sheet.update(range_name='A1', values=output_rows)

    print(f"\n{'=' * 60}")
    print(f"DONE! {len(final)} net-new startups about to raise")
    print(f"  All have CEO name + email")
    print(f"  Zero prior Affinity interactions")
    print(f"  Sorted by 'Raise Score' (traction + growth signals)")
    print(f"\nSpreadsheet: https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}")
    print(f"{'=' * 60}")

if __name__ == "__main__":
    main()
