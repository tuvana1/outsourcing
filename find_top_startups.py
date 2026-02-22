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

ALLOWED_COUNTRIES = {"united states", "us", "usa"}
EXCLUDED_TAGS_KEYWORDS = {
    "hardware", "biotech", "biotechnology", "pharmaceutical", "medical devices",
    "semiconductors", "chip design", "electronics manufacturing", "robotics",
    "3d printing", "manufacturing", "clean energy", "solar", "battery",
    "cannabis", "marijuana", "nonprofit", "non-profit", "charity",
    "government", "public sector", "real estate", "construction",
    "agriculture", "mining", "oil", "gas", "energy",
    "healthcare", "health care", "healthtech", "health tech", "medical",
    "clinical", "patient", "hospital", "telehealth", "telemedicine",
}
HEALTHCARE_NAME_KEYWORDS = {
    "health", "medical", "care", "clinic", "pharma", "bio",
    "therapeutics", "diagnostics", "wellness", "patient",
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
}
ELITE_SCHOOLS = {
    "stanford", "mit", "harvard", "yale", "princeton", "caltech",
    "carnegie mellon", "berkeley", "columbia", "cornell", "upenn",
    "wharton", "booth", "kellogg", "sloan", "haas",
    "oxford", "cambridge", "iit",
}
NONPROFIT_NAME_KEYWORDS = {
    "foundation", "council", "association", "institute", "society",
    "charity", "nonprofit", "non-profit", "ngo", "ministry",
    "committee", "coalition", "alliance", "federation", "bureau",
    "center for", "centre for", "crisis center", "rape crisis",
}

TARGET_LIST_ID = int(os.environ["AFFINITY_LIST_ID"])

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

def is_us_based(location):
    country = (location.get("country") or "").lower().strip()
    return country in ALLOWED_COUNTRIES

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

def is_b2b_saas(tags, customer_type, description):
    """Return True if company looks like B2B SaaS."""
    ct = (customer_type or "").lower()
    tag_texts = [(t.get("display_value") or "").lower() for t in (tags or [])]
    desc_lower = (description or "").lower()
    b2b_kw = ["saas", "enterprise", "business", "b2b", "infrastructure", "devtools",
              "developer", "api", "platform", "fintech", "software", "cloud",
              "analytics", "automation", "data", "cybersecurity", "ai", "machine learning"]
    consumer_kw = ["consumer", "social media", "gaming", "entertainment", "fashion",
                   "food delivery", "dating", "music", "sports", "fitness", "travel", "lifestyle"]
    b2b_signals = sum(1 for t in tag_texts if any(w in t for w in b2b_kw))
    consumer_signals = sum(1 for t in tag_texts if any(w in t for w in consumer_kw))
    # Check description for B2B signals
    desc_b2b = any(w in desc_lower for w in ["b2b", "saas", "enterprise", "businesses", "teams", "companies", "organizations", "workflow", "platform for"])
    if "b2b" in ct:
        return True
    if b2b_signals >= 2:
        return True
    if b2b_signals >= 1 and desc_b2b:
        return True
    if "b2c" in ct and consumer_signals > 0:
        return False
    if consumer_signals > b2b_signals:
        return False
    # If ambiguous but has B2B description signals, include it
    if desc_b2b and consumer_signals == 0:
        return True
    return False

def is_healthcare(name, tags, description):
    """Detect healthcare companies."""
    desc_lower = (description or "").lower()
    name_lower = (name or "").lower()
    tag_texts = [(t.get("display_value") or "").lower() for t in (tags or [])]
    hc_kw = ["healthcare", "health care", "healthtech", "medical", "clinical",
             "patient", "hospital", "telehealth", "telemedicine", "pharma",
             "therapeutics", "diagnostics", "ehr", "emr", "hipaa"]
    if any(any(kw in t for kw in hc_kw) for t in tag_texts):
        return True
    if any(kw in desc_lower for kw in hc_kw):
        return True
    return False

def compute_founder_score(person):
    """Score founder based on background. Higher = more impressive."""
    if not person:
        return 0
    score = 0

    # Check experience for elite companies
    experience = person.get("experience") or []
    seen_companies = set()
    for exp in experience:
        co = (exp.get("company_name") or "").lower().strip()
        title = (exp.get("title") or "").lower()
        for elite in ELITE_COMPANIES:
            if elite in co and co not in seen_companies:
                seen_companies.add(co)
                score += 15  # Elite company experience
                # Extra points for senior roles
                if any(w in title for w in ["cto", "vp", "director", "head of", "chief", "principal", "staff", "lead"]):
                    score += 10
                elif any(w in title for w in ["senior", "manager"]):
                    score += 5
                break

    # Check education for elite schools
    edu_list = person.get("education") or []
    for edu in edu_list:
        school = edu.get("school") or {}
        school_name = (school.get("name") or "").lower()
        for elite in ELITE_SCHOOLS:
            if elite in school_name:
                score += 10
                break

    # Person highlights (Harmonic signals like "Prior Exit", "Top University")
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
    fd_date = fd.get("date") or ""
    year = None
    if fd_date:
        try:
            year = int(fd_date[:4])
        except (ValueError, IndexError):
            pass
    if year and year >= 2024:
        score += 10
    elif year and year >= 2023:
        score += 5

    # Sweet spot for our check size ($500K-$5M): companies with $1-5M raised
    # are likely raising their next round soon
    if 1_000_000 <= funding_total <= 5_000_000 and headcount > 5:
        score += 15  # Right in our sweet spot - likely raising Series A
    elif funding_total < 1_000_000 and headcount > 3:
        score += 10  # Pre-seed looking for seed

    return score

def extract_founder_background(person):
    """Extract education, past experience, LinkedIn from person data."""
    if not person:
        return {"education": "", "prev_companies": "", "linkedin": "", "headline": "", "highlights": ""}

    # Education
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

    # Previous companies (non-current positions, most recent first)
    experience = person.get("experience") or []
    prev = []
    for exp in experience:
        if not exp.get("is_current_position", False):
            co = exp.get("company_name") or ""
            title = exp.get("title") or ""
            if co and co != "urn:harmonic:company:-1":
                prev.append(f"{title} @ {co}" if title else co)
    prev_companies = "; ".join(prev[:4])

    # LinkedIn
    socials = person.get("socials") or {}
    li = socials.get("LINKEDIN") or {}
    linkedin = li.get("url") or ""

    # Headline
    headline = person.get("linkedin_headline") or ""

    # Person highlights
    p_highlights = person.get("highlights") or []
    highlight_texts = []
    for h in p_highlights:
        cat = h.get("category") or ""
        if cat:
            highlight_texts.append(cat.replace("_", " ").title())
    highlights = ", ".join(highlight_texts[:5])

    return {
        "education": education,
        "prev_companies": prev_companies,
        "linkedin": linkedin,
        "headline": headline,
        "highlights": highlights,
    }

# ---------- Harmonic API ----------

def search_companies(page_size=200, start=0):
    r = requests.post(f"{HARMONIC_BASE}/search/companies", headers=HARMONIC_HEADERS, json={
        "query": {
            "filter_group": {
                "join_operator": "and",
                "filters": [
                    {"field": "company_funding_stage", "comparator": "anyOf", "filter_value": ["PRE_SEED", "SEED", "SERIES_A"]},
                    {"field": "company_and_employee_highlight_count", "comparator": "greaterThanOrEquals", "filter_value": 2},
                    {"field": "company_headcount_real_change_180d_ago", "comparator": "greaterThanOrEquals", "filter_value": 1},
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
    """Load all emails and company names from all Lemlist campaigns."""
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
    print("Find Top 50 US B2B SaaS Startups - Elite Founders")
    print("=" * 60)
    print("Filters: Pre-seed/Seed/Series A, US-only, B2B SaaS, founded 2023+, no healthcare")
    print("Ranked by: Raise Score + Founder Background (ex-FAANG, elite schools, prior exits)")
    print("Check size fit: $500K-$5M (Seed/Series A)")
    print("Require: CEO name + email, zero prior Affinity contact")
    print()

    affinity = AffinityClient(AFFINITY_API_KEY)

    # Step 0a: Load all previously contacted leads from Lemlist
    print("[0a] Loading all contacted leads from Lemlist (all campaigns)...")
    lemlist_emails, lemlist_companies = load_lemlist_contacted()
    print(f"  {len(lemlist_emails)} unique emails, {len(lemlist_companies)} unique companies across all campaigns\n")

    # Step 0b: Load existing companies from all sheets to exclude
    print("[0b] Loading existing companies from all sheets...")
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

    # Step 1: Search Harmonic (wider range to find fresh companies)
    print("[1/5] Searching Harmonic for high-traction early-stage startups...")
    all_urns = []
    start = 0
    while len(all_urns) < 6000:
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
    stats = {"not_us": 0, "industry": 0, "nonprofit": 0, "not_b2b": 0, "no_startup": 0, "too_much_funding": 0, "too_old": 0, "healthcare": 0, "duplicate": 0, "already_in_sheet1": 0}
    scored = []
    seen_names = set()

    for c in companies:
        name = c.get("name") or ""
        location = c.get("location") or {}
        tags = c.get("tags") or []
        customer_type = c.get("customer_type") or ""
        company_type = c.get("company_type") or ""
        description = c.get("description") or ""

        # Dedup by normalized name
        norm = normalize_name(name)
        if norm in seen_names:
            stats["duplicate"] += 1
            continue
        seen_names.add(norm)

        # Skip companies already in Sheet1
        if norm in existing_names:
            stats["already_in_sheet1"] += 1
            continue

        if not is_us_based(location):
            stats["not_us"] += 1
            continue
        if is_excluded_industry(tags):
            stats["industry"] += 1
            continue
        if is_nonprofit(name, company_type, tags, description):
            stats["nonprofit"] += 1
            continue
        if is_healthcare(name, tags, description):
            stats["healthcare"] += 1
            continue
        if not is_b2b_saas(tags, customer_type, description):
            stats["not_b2b"] += 1
            continue
        if company_type and company_type.upper() not in ("STARTUP", ""):
            stats["no_startup"] += 1
            continue
        # Require founded 2023 or later
        fd = c.get("founding_date") or {}
        fd_date = fd.get("date") or ""
        found_year = None
        if fd_date:
            try:
                found_year = int(fd_date[:4])
            except (ValueError, IndexError):
                pass
        if not found_year or found_year < 2023:
            stats["too_old"] += 1
            continue
        # Skip companies that have raised too much (> $10M = past our check size sweet spot)
        funding = c.get("funding") or {}
        funding_total = funding.get("funding_total") or 0
        if funding_total > 10_000_000:
            stats["too_much_funding"] += 1
            continue

        raise_score = compute_raise_score(c)
        scored.append((raise_score, c))

    # Sort by raise score (highest first)
    scored.sort(key=lambda x: -x[0])

    print(f"  Excluded - not US: {stats['not_us']}, industry: {stats['industry']}, "
          f"nonprofit: {stats['nonprofit']}, healthcare: {stats['healthcare']}, "
          f"not B2B: {stats['not_b2b']}, duplicate: {stats['duplicate']}, "
          f"already in sheets: {stats['already_in_sheet1']}, "
          f"not startup: {stats['no_startup']}, founded before 2023: {stats['too_old']}, "
          f"too much funding: {stats['too_much_funding']}")
    print(f"  Remaining candidates: {len(scored)}")
    print(f"  Top raise scores: {[s[0] for s in scored[:10]]}")

    # Step 4: Get CEO info, score founders, check Affinity, collect top 50
    print(f"\n[4/5] Getting CEO info, scoring founders, and checking Affinity...")

    # First pass: get all person URNs for top candidates
    top_candidates = scored[:600]  # Check top 600 to find 50 with elite founders
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

    # Second pass: find CEO+email and score founders
    pre_affinity = []
    skipped_no_ceo = 0
    seen_final_names = set()

    for raise_score, c in top_candidates:
        urn = c.get("entity_urn") or ""
        name = c.get("name") or ""

        # Dedup again at this stage
        norm = normalize_name(name)
        if norm in seen_final_names:
            continue
        seen_final_names.add(norm)

        candidates = company_candidates.get(urn, [])

        # Find CEO with email
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
        combined_score = raise_score + founder_score

        pre_affinity.append({
            "company": c,
            "ceo_name": ceo_name,
            "first_name": first_name,
            "email": email,
            "domain": domain,
            "raise_score": raise_score,
            "founder_score": founder_score,
            "combined_score": combined_score,
            "person": chosen_person,
        })

    # Sort by combined score (raise + founder background)
    pre_affinity.sort(key=lambda x: -x["combined_score"])
    print(f"  {len(pre_affinity)} candidates with CEO+email (skipped {skipped_no_ceo} without)")
    print(f"  Top combined scores: {[(p['company'].get('name',''), p['combined_score'], p['founder_score']) for p in pre_affinity[:10]]}")

    # Third pass: check Affinity for top candidates, collect 50
    print(f"\n  Checking Affinity (need 50)...")
    final = []
    skipped_affinity = 0

    for item in pre_affinity:
        if len(final) >= 50:
            break

        c = item["company"]
        name = c.get("name") or ""
        domain = item["domain"]

        print(f"  [{len(final)+1}] {name} (combined={item['combined_score']:.0f}, raise={item['raise_score']:.0f}, founder={item['founder_score']:.0f})...", end=" ", flush=True)

        # Check Lemlist first (faster than Affinity API)
        lead_email = item["email"].strip().lower()
        lead_company_norm = normalize_name(name)
        if lead_email in lemlist_emails:
            skipped_affinity += 1
            print(f"SKIP (email already in Lemlist)")
            continue
        if lead_company_norm in lemlist_companies:
            skipped_affinity += 1
            print(f"SKIP (company already in Lemlist)")
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

    print(f"\n  Skipped (already in Affinity): {skipped_affinity}")
    print(f"  Final list: {len(final)}")

    # Step 5: Write to next available sheet
    existing_sheets = [ws.title for ws in spreadsheet.worksheets()]
    # Find next sheet number
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
        "Combined Score", "Raise Score", "Founder Score",
        "Stage", "Funding Total", "Headcount",
        "Headcount Growth (6mo)", "Web Traffic", "Web Traffic Growth (6mo)",
        "LinkedIn Followers", "LinkedIn Growth (6mo)",
        "Country", "City", "Customer Type",
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
        fd_date_str = fd.get("date") or ""
        if fd_date_str:
            try:
                founded = fd_date_str[:4]
            except:
                pass

        bg = extract_founder_background(item.get("person"))

        # Clean company name (remove emojis, YC tags, Inc., etc.)
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
            f"{item['raise_score']:.0f}",
            f"{item['founder_score']:.0f}",
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

    target_sheet.update(range_name='A1', values=output_rows)

    print(f"\n{'=' * 60}")
    print(f"DONE! {len(final)} elite-founder US B2B SaaS startups")
    print(f"  All US-based, B2B SaaS, no healthcare, <$10M raised, founded 2023+")
    print(f"  All have CEO name + email")
    print(f"  Zero prior Affinity contact or Lemlist outreach")
    print(f"  Ranked by Combined Score = Raise Score + Founder Background")
    print(f"  (ex-FAANG, elite schools, prior exits, senior roles)")
    print(f"\nSpreadsheet: https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}")
    print(f"{'=' * 60}")

if __name__ == "__main__":
    main()
