import os
import re
import time
import json
import csv
import io
from datetime import datetime
import requests
import gspread
from google.oauth2.service_account import Credentials

# ---------- Configuration ----------

HARMONIC_API_KEY = os.environ["HARMONIC_API_KEY"]
AFFINITY_API_KEY = os.environ["AFFINITY_API_KEY"]
LEMLIST_API_KEY = os.environ["LEMLIST_API_KEY"]
SPREADSHEET_ID = os.environ["SPREADSHEET_ID"]

HARMONIC_BASE = "https://api.harmonic.ai"
HARMONIC_HEADERS = {"apikey": HARMONIC_API_KEY, "Content-Type": "application/json"}
AFFINITY_BASE = "https://api.affinity.co"

TARGET_LIST_ID = int(os.environ["AFFINITY_LIST_ID"])

# Keywords that signal stablecoin yield / consumer fintech
STABLECOIN_YIELD_KEYWORDS = {
    "stablecoin", "stable coin", "usdc", "usdt", "dai", "yield", "savings",
    "interest", "apy", "earn", "high-yield", "high yield", "returns",
    "deposit", "dollar", "digital dollar", "crypto savings", "defi",
    "neobank", "fintech", "money market", "treasury", "t-bill", "tbill",
    "tokenized", "on-chain", "onchain", "rwa", "real world asset",
}

# Consumer signals
CONSUMER_KEYWORDS = {
    "consumer", "personal finance", "b2c", "retail", "individuals",
    "everyday", "app", "mobile", "wallet", "account", "save", "saving",
    "spend", "payment", "remittance", "transfer", "bank", "banking",
    "neobank", "challenger bank",
}

# Elite tech companies for founder scoring
ELITE_COMPANIES = {
    "google", "meta", "facebook", "apple", "amazon", "microsoft", "netflix",
    "stripe", "brex", "twitch", "airbnb", "uber", "lyft", "doordash",
    "coinbase", "robinhood", "plaid", "figma", "notion", "slack", "discord",
    "snowflake", "databricks", "datadog", "palantir", "salesforce",
    "twitter", "x", "linkedin", "snap", "pinterest", "spotify",
    "openai", "anthropic", "deepmind", "tesla", "spacex",
    "square", "block", "shopify", "instacart", "dropbox",
    "nvidia", "intel", "amd", "oracle", "ibm", "cisco",
    "mckinsey", "bain", "bcg", "goldman sachs", "morgan stanley",
    "jp morgan", "jpmorgan", "a16z", "andreessen", "sequoia",
    "benchmark", "accel", "greylock", "kleiner", "y combinator", "yc",
    "circle", "binance", "kraken", "gemini", "ftx", "consensys",
    "chainalysis", "alchemy", "fireblocks", "anchorage",
}
ELITE_SCHOOLS = {
    "stanford", "mit", "harvard", "yale", "princeton", "caltech",
    "carnegie mellon", "berkeley", "columbia", "cornell", "upenn",
    "wharton", "booth", "kellogg", "sloan", "haas",
    "oxford", "cambridge", "iit",
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

def matches_stablecoin_yield(tags, description, name):
    """Check if company is a consumer fintech offering yield through stablecoins/crypto."""
    desc_lower = (description or "").lower()
    name_lower = (name or "").lower()
    tag_texts = [(t.get("display_value") or "").lower() for t in (tags or [])]
    all_text = desc_lower + " " + name_lower + " " + " ".join(tag_texts)

    # Hard exclude: not fintech at all
    exclude_keywords = [
        "healthcare", "health care", "medical", "clinical", "patient", "hospital",
        "biotech", "pharma", "therapeutics", "diagnostics", "wellness app",
        "fitness", "meditation", "sleep", "mental health", "therapy",
        "real estate", "property", "construction", "agriculture",
        "gaming", "game studio", "esports", "music streaming", "podcast",
        "food delivery", "restaurant", "recipe", "grocery",
        "dating", "social media", "social network",
        "education", "edtech", "school", "tutoring", "learning platform",
        "hr tech", "recruiting", "hiring", "staffing",
        "legal tech", "law firm", "compliance only",
        "advertising", "ad tech", "marketing automation",
        "manufacturing", "hardware", "robotics", "semiconductor",
        "longevity", "anti-aging", "skincare", "beauty",
        "travel", "booking", "hotel",
    ]
    if any(kw in all_text for kw in exclude_keywords):
        return False

    # MUST have crypto/stablecoin/DeFi signal
    crypto_signals = ["stablecoin", "stable coin", "usdc", "usdt", "dai", "pyusd",
                      "crypto", "defi", "decentralized finance",
                      "on-chain", "onchain", "blockchain", "web3",
                      "tokenized", "rwa", "real world asset", "digital dollar",
                      "digital asset", "cryptocurrency"]
    has_crypto = any(kw in all_text for kw in crypto_signals)

    if not has_crypto:
        return False

    # Must ALSO have yield/savings/fintech consumer signal
    yield_fintech_signals = [
        "yield", "apy", "earn", "interest", "savings", "high-yield", "high yield",
        "returns", "money market", "t-bill", "tbill", "treasury",
        "neobank", "banking", "wallet", "deposit", "fintech",
        "savings account", "checking", "spend", "payment",
        "dollar account", "cash", "remittance",
    ]
    has_yield_or_fintech = any(kw in all_text for kw in yield_fintech_signals)

    return has_yield_or_fintech

def compute_founder_score(person):
    if not person:
        return 0
    score = 0
    experience = person.get("experience") or []
    seen_companies = set()
    for exp in experience:
        co = (exp.get("company_name") or "").lower().strip()
        title = (exp.get("title") or "").lower()
        for elite in ELITE_COMPANIES:
            if elite in co and co not in seen_companies:
                seen_companies.add(co)
                score += 15
                if any(w in title for w in ["cto", "vp", "director", "head of", "chief", "principal", "staff", "lead"]):
                    score += 10
                elif any(w in title for w in ["senior", "manager"]):
                    score += 5
                break
    edu_list = person.get("education") or []
    for edu in edu_list:
        school = edu.get("school") or {}
        school_name = (school.get("name") or "").lower()
        for elite in ELITE_SCHOOLS:
            if elite in school_name:
                score += 10
                break
    p_highlights = person.get("highlights") or []
    for h in p_highlights:
        cat = (h.get("category") or "").lower()
        if "prior_exit" in cat or "prior exit" in cat:
            score += 20
        elif "top_university" in cat or "top university" in cat:
            score += 5
        elif "major_tech" in cat or "faang" in cat:
            score += 10
        elif "serial_founder" in cat or "serial founder" in cat:
            score += 15
    return score

def compute_relevance_score(company):
    """Score how relevant company is to stablecoin yield for consumers."""
    score = 0
    desc_lower = (company.get("description") or "").lower()
    name_lower = (company.get("name") or "").lower()
    tags = company.get("tags") or []
    tag_texts = [(t.get("display_value") or "").lower() for t in tags]
    all_text = desc_lower + " " + name_lower + " " + " ".join(tag_texts)

    # Strong stablecoin signals
    for kw in ["stablecoin", "stable coin", "usdc", "usdt"]:
        if kw in all_text:
            score += 20
    # Yield signals
    for kw in ["yield", "apy", "earn", "interest", "savings", "high-yield"]:
        if kw in all_text:
            score += 15
    # Crypto/DeFi signals
    for kw in ["defi", "crypto", "on-chain", "onchain", "blockchain", "web3", "tokenized", "rwa"]:
        if kw in all_text:
            score += 10
    # Consumer fintech signals
    for kw in ["neobank", "wallet", "consumer", "personal finance", "banking app", "savings account"]:
        if kw in all_text:
            score += 10
    # Traction signals
    tm = company.get("traction_metrics") or {}
    hc = tm.get("corrected_headcount") or {}
    hc_180 = hc.get("180d_ago", {})
    hc_change = hc_180.get("change")
    if hc_change and hc_change > 0:
        score += min(hc_change * 3, 15)
    highlights = company.get("highlights") or []
    score += min(len(highlights) * 2, 10)

    return score

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
            if degree and degree != "NA":
                parts.append(degree)
            if field and field != "NA":
                parts.append(field)
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
    highlight_texts = []
    for h in p_highlights:
        cat = h.get("category") or ""
        if cat:
            highlight_texts.append(cat.replace("_", " ").title())
    highlights_str = ", ".join(highlight_texts[:5])
    return {
        "education": education,
        "prev_companies": prev_companies,
        "linkedin": linkedin,
        "headline": headline,
        "highlights": highlights_str,
    }

# ---------- Harmonic API ----------

def search_companies(page_size=200, start=0):
    """Search for fintech/crypto companies on Harmonic."""
    r = requests.post(f"{HARMONIC_BASE}/search/companies", headers=HARMONIC_HEADERS, json={
        "query": {
            "filter_group": {
                "join_operator": "and",
                "filters": [
                    {"field": "company_funding_stage", "comparator": "anyOf",
                     "filter_value": ["PRE_SEED", "SEED", "SERIES_A", "SERIES_B"]},
                    {"field": "company_country", "comparator": "anyOf",
                     "filter_value": ["United States"]},
                    {"field": "company_and_employee_highlight_count", "comparator": "greaterThanOrEquals",
                     "filter_value": 1},
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
    for chunk in chunked(urns, 25):
        for attempt in range(3):
            try:
                r = requests.post(f"{HARMONIC_BASE}/companies/batchGet", headers=HARMONIC_HEADERS, json={"urns": chunk}, timeout=120)
                r.raise_for_status()
                data = r.json()
                out.extend(data if isinstance(data, list) else data.get("results", []))
                break
            except (requests.exceptions.Timeout, requests.exceptions.ConnectionError, requests.exceptions.HTTPError) as e:
                if attempt < 2:
                    print(f"    Retry {attempt+1} for batch...")
                    time.sleep(2)
                else:
                    raise
        time.sleep(0.3)
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

# ---------- Lemlist Dedup ----------

def load_lemlist_contacted():
    contacted_emails = set()
    contacted_companies = set()
    try:
        r = requests.get('https://api.lemlist.com/api/campaigns', auth=('', LEMLIST_API_KEY), timeout=60)
        if r.status_code != 200:
            print(f"  Warning: Could not fetch Lemlist campaigns ({r.status_code})")
            return contacted_emails, contacted_companies
        campaigns = json.loads(r.text)
        for c in campaigns:
            cid = c.get('_id', '')
            r2 = requests.get(f'https://api.lemlist.com/api/campaigns/{cid}/export', auth=('', LEMLIST_API_KEY), timeout=60)
            if r2.status_code == 200 and r2.text:
                reader = csv.DictReader(io.StringIO(r2.text))
                for row in reader:
                    email = (row.get('email') or '').strip().lower()
                    company = (row.get('companyName') or '').strip().lower()
                    if email:
                        contacted_emails.add(email)
                    if company:
                        contacted_companies.add(normalize_name(company))
    except Exception as e:
        print(f"  Warning: Lemlist check failed: {e}")
    return contacted_emails, contacted_companies

# ---------- Google Sheets ----------

def get_spreadsheet():
    scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
    creds = Credentials.from_service_account_file('credentials.json', scopes=scopes)
    gc = gspread.authorize(creds)
    return gc.open_by_key(SPREADSHEET_ID)

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
    print("Find Consumer Fintech - Stablecoin Yield Companies")
    print("=" * 60)
    print("Target: Consumer fintech offering yield through stablecoins")
    print("Filters: US-based, Pre-seed to Series B, fintech/crypto tags")
    print("Ranked by: Relevance Score + Founder Background")
    print()

    affinity = AffinityClient(AFFINITY_API_KEY)

    # Step 0a: Load Lemlist contacts
    print("[0a] Loading contacted leads from Lemlist...")
    lemlist_emails, lemlist_companies = load_lemlist_contacted()
    print(f"  {len(lemlist_emails)} emails, {len(lemlist_companies)} companies\n")

    # Step 0b: Load existing from sheets
    print("[0b] Loading existing companies from sheets...")
    spreadsheet = get_spreadsheet()
    existing_names = set()
    for ws in spreadsheet.worksheets():
        try:
            ws_data = ws.get_all_values()
            for row in ws_data[1:]:
                name = row[0] if len(row) > 0 else ""
                if name.strip():
                    existing_names.add(normalize_name(name))
            count = len(ws_data) - 1 if len(ws_data) > 1 else 0
            print(f"  '{ws.title}': {count} companies")
        except Exception:
            pass
    print(f"  Total: {len(existing_names)} existing companies to skip\n")

    # Step 1: Search Harmonic for fintech/crypto companies
    print("[1/5] Searching Harmonic for fintech/crypto startups...")
    all_urns = []
    start = 0
    while len(all_urns) < 8000:
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

    # Step 3: Filter for stablecoin yield + consumer
    print("\n[3/5] Filtering for stablecoin yield consumer fintech...")
    scored = []
    seen_names = set()
    stats = {"duplicate": 0, "already_in_sheet": 0, "not_stablecoin_yield": 0}

    for c in companies:
        name = c.get("name") or ""
        tags = c.get("tags") or []
        description = c.get("description") or ""

        norm = normalize_name(name)
        if norm in seen_names:
            stats["duplicate"] += 1
            continue
        seen_names.add(norm)

        if norm in existing_names:
            stats["already_in_sheet"] += 1
            continue

        if not matches_stablecoin_yield(tags, description, name):
            stats["not_stablecoin_yield"] += 1
            continue

        relevance = compute_relevance_score(c)
        scored.append((relevance, c))

    scored.sort(key=lambda x: -x[0])

    print(f"  Excluded - duplicate: {stats['duplicate']}, already in sheets: {stats['already_in_sheet']}, "
          f"not stablecoin/yield: {stats['not_stablecoin_yield']}")
    print(f"  Matching candidates: {len(scored)}")
    if scored:
        print(f"  Top relevance scores: {[(s[1].get('name',''), s[0]) for s in scored[:10]]}")

    # Step 4: Get CEO info, score founders, check Affinity
    print(f"\n[4/5] Getting CEO info and checking Affinity...")

    top_candidates = scored[:300]
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

    pre_affinity = []
    skipped_no_ceo = 0
    seen_final_names = set()

    for relevance_score, c in top_candidates:
        urn = c.get("entity_urn") or ""
        name = c.get("name") or ""
        norm = normalize_name(name)
        if norm in seen_final_names:
            continue
        seen_final_names.add(norm)

        candidates = company_candidates.get(urn, [])
        ceo_name = ""
        first_name = ""
        email = ""
        chosen_person = None

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
                chosen_person = person_data
                break

        if not ceo_name or not email:
            skipped_no_ceo += 1
            continue

        website = c.get("website") or {}
        domain = ""
        if isinstance(website, dict):
            url = website.get("url") or ""
            domain = url.replace("https://", "").replace("http://", "").replace("www.", "").split("/")[0] if url else ""

        founder_score = compute_founder_score(chosen_person)
        combined_score = relevance_score + founder_score

        pre_affinity.append({
            "company": c,
            "ceo_name": ceo_name,
            "first_name": first_name,
            "email": email,
            "domain": domain,
            "relevance_score": relevance_score,
            "founder_score": founder_score,
            "combined_score": combined_score,
            "person": chosen_person,
        })

    pre_affinity.sort(key=lambda x: -x["combined_score"])
    print(f"  {len(pre_affinity)} candidates with CEO+email (skipped {skipped_no_ceo} without)")
    if pre_affinity:
        print(f"  Top: {[(p['company'].get('name',''), p['combined_score'], p['relevance_score']) for p in pre_affinity[:10]]}")

    # Check Affinity - collect up to 50
    print(f"\n  Checking Affinity (need up to 50)...")
    final = []
    skipped_affinity = 0

    for item in pre_affinity:
        if len(final) >= 50:
            break

        c = item["company"]
        name = c.get("name") or ""
        domain = item["domain"]

        print(f"  [{len(final)+1}] {name} (combined={item['combined_score']:.0f}, rel={item['relevance_score']:.0f}, founder={item['founder_score']:.0f})...", end=" ", flush=True)

        lead_email = item["email"].strip().lower()
        lead_company_norm = normalize_name(name)
        if lead_email in lemlist_emails:
            skipped_affinity += 1
            print(f"SKIP (email in Lemlist)")
            continue
        if lead_company_norm in lemlist_companies:
            skipped_affinity += 1
            print(f"SKIP (company in Lemlist)")
            continue

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
                print("OK")
        else:
            print("OK (new)")

        final.append(item)
        time.sleep(0.15)

    print(f"\n  Skipped (already contacted): {skipped_affinity}")
    print(f"  Final list: {len(final)}")

    # Step 5: Write to sheet
    existing_sheets = [ws.title for ws in spreadsheet.worksheets()]
    sheet_num = 3
    while f"Sheet{sheet_num}" in existing_sheets:
        sheet_num += 1
    target_sheet_name = f"Sheet{sheet_num}"
    print(f"\n[5/5] Writing to {target_sheet_name}...")
    try:
        target_sheet = spreadsheet.worksheet(target_sheet_name)
    except gspread.exceptions.WorksheetNotFound:
        target_sheet = spreadsheet.add_worksheet(title=target_sheet_name, rows=200, cols=30)
    target_sheet.clear()

    headers = [
        "companyName", "firstName", "email", "ceoName", "domain",
        "Combined Score", "Relevance Score", "Founder Score",
        "Stage", "Funding Total", "Headcount",
        "Customer Type",
        "Founder LinkedIn", "Founder Headline",
        "Founder Education", "Founder Previous Companies",
        "Founder Highlights",
        "Tags", "Company Highlights", "Founded",
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
        fd = c.get("founding_date") or {}

        headcount = c.get("headcount") or c.get("corrected_headcount") or ""
        if isinstance(headcount, dict):
            headcount = headcount.get("latest_metric_value") or ""

        founded = ""
        fd_date_str = fd.get("date") or ""
        if fd_date_str:
            try:
                founded = fd_date_str[:4]
            except:
                pass

        bg = extract_founder_background(item.get("person"))

        clean_name = c.get("name", "")
        clean_name = re.sub(r'\s*\([^)]*\)\s*', ' ', clean_name).strip()
        clean_name = clean_name.encode('ascii', 'ignore').decode('ascii').strip()
        clean_name = re.sub(r',\s*Inc\.?$', '', clean_name).strip()
        clean_name = re.sub(r'\s+', ' ', clean_name).rstrip('.,').strip()

        output_rows.append([
            clean_name,
            item["first_name"],
            item["email"],
            item["ceo_name"],
            item["domain"],
            f"{item['combined_score']:.0f}",
            f"{item['relevance_score']:.0f}",
            f"{item['founder_score']:.0f}",
            c.get("stage", ""),
            f"${funding_total:,.0f}" if funding_total else "No funding",
            str(headcount),
            c.get("customer_type", ""),
            bg["linkedin"],
            bg["headline"][:200] if bg["headline"] else "",
            bg["education"],
            bg["prev_companies"][:300] if bg["prev_companies"] else "",
            bg["highlights"],
            ", ".join([t.get("display_value", "") for t in tags[:5]]),
            ", ".join([h.get("category", "") for h in highlights[:5]]),
            founded,
            (c.get("description") or "")[:300],
        ])

    target_sheet.update(range_name='A1', values=output_rows)

    print(f"\n{'=' * 60}")
    print(f"DONE! {len(final)} consumer fintech stablecoin yield companies")
    print(f"  All have CEO name + email, zero prior contact")
    print(f"  Ranked by Relevance + Founder Background")
    print(f"\nSpreadsheet: https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}")
    print(f"{'=' * 60}")

if __name__ == "__main__":
    main()
