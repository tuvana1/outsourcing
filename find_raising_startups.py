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
SPREADSHEET_ID = os.environ["SPREADSHEET_ID"]

HARMONIC_BASE = "https://api.harmonic.ai"
HARMONIC_HEADERS = {"apikey": HARMONIC_API_KEY, "Content-Type": "application/json"}
AFFINITY_BASE = "https://api.affinity.co"

TARGET_LIST_ID = int(os.environ["AFFINITY_LIST_ID"])  # 1a Sourcing List

EXCLUDED_COUNTRIES = {"china", "russia", "ukraine", "india"}
EXCLUDED_TAGS_KEYWORDS = {
    "hardware", "biotech", "biotechnology", "pharmaceutical", "medical devices",
    "semiconductors", "chip design", "electronics manufacturing",
    "3d printing", "manufacturing", "clean energy", "solar", "battery",
    "cannabis", "marijuana", "nonprofit", "non-profit", "charity",
    "government", "public sector",
}
NONPROFIT_NAME_KEYWORDS = {
    "foundation", "council", "association", "institute", "society",
    "charity", "nonprofit", "non-profit", "ngo", "ministry",
    "committee", "coalition", "alliance", "federation", "bureau",
    "center for", "centre for",
}

# Founder quality highlight weights
FOUNDER_HIGHLIGHT_SCORES = {
    "Prior Exit": 25,
    "Prior VC Backed Founder": 20,
    "YC Backed Founder": 20,
    "Seasoned Founder": 18,
    "Top University": 12,
    "Top Company Alum": 10,
    "Major Tech Company Experience": 8,
    "Deep Technical Background": 10,
    "Top AI Experience": 12,
    "Elite Industry Experience": 8,
    "Major Research Institution Experience": 8,
    "Seasoned Executive": 8,
    "Seasoned Operator": 6,
    "$50M+ Club": 20,
    "$45M Club": 18,
    "$40M Club": 16,
    "$35M Club": 14,
    "$20M Club": 12,
    "$15M Club": 10,
    "$10M Club": 8,
    "$5M Club": 5,
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

# ---------- Scoring ----------

def compute_raising_score(company):
    """Score how likely a company is ACTIVELY raising right now."""
    score = 0
    tm = company.get("traction_metrics") or {}

    # Headcount growth (hiring = raising or about to)
    hc = tm.get("corrected_headcount") or {}
    hc_90 = hc.get("90d_ago", {})
    hc_change_90 = hc_90.get("change")
    if hc_change_90 and hc_change_90 > 0:
        score += min(hc_change_90 * 8, 25)
    hc_180 = hc.get("180d_ago", {})
    hc_change = hc_180.get("change")
    if hc_change and hc_change > 0:
        score += min(hc_change * 4, 15)

    # Web traffic growth
    wt = tm.get("web_traffic") or {}
    wt_180 = wt.get("180d_ago", {})
    wt_pct = wt_180.get("percent_change")
    if wt_pct and wt_pct > 0:
        score += min(wt_pct / 10, 20)

    # LinkedIn growth (buzz/awareness)
    li = tm.get("linkedin_follower_count") or {}
    li_180 = li.get("180d_ago", {})
    li_pct = li_180.get("percent_change")
    if li_pct and li_pct > 0:
        score += min(li_pct / 5, 15)

    # Stealth emergence (recently emerged = actively raising)
    emergence = company.get("stealth_emergence_date")
    if emergence:
        try:
            ed = datetime.fromisoformat(emergence.replace("Z", "+00:00"))
            days_ago = (datetime.now(ed.tzinfo) - ed).days
            if days_ago < 60:
                score += 25
            elif days_ago < 120:
                score += 20
            elif days_ago < 180:
                score += 15
            elif days_ago < 365:
                score += 8
        except:
            pass

    # Low funding relative to headcount = needs capital
    funding = company.get("funding") or {}
    funding_total = funding.get("funding_total") or 0
    headcount = company.get("headcount") or company.get("corrected_headcount") or 1
    if isinstance(headcount, dict):
        headcount = headcount.get("latest_metric_value") or 1
    if headcount > 3 and funding_total < 2_000_000:
        score += 12
    if funding_total == 0 and headcount > 2:
        score += 15

    # Highlight count (more attention from investors)
    highlights = company.get("highlights") or []
    score += min(len(highlights) * 2, 10)

    # Founding date - newer companies more likely raising
    fd = company.get("founding_date") or {}
    year = fd.get("year")
    if year and year >= 2024:
        score += 10
    elif year and year >= 2023:
        score += 5

    return score

def compute_founder_score(company):
    """Score founder quality using employee highlights on company record."""
    score = 0
    seen_categories = set()

    # Use employee_highlights (contains all team member highlights)
    emp_highlights = company.get("employee_highlights") or []

    # Also check company-level highlights
    comp_highlights = company.get("highlights") or []

    all_highlights = emp_highlights + comp_highlights

    for h in all_highlights:
        category = h.get("category") or ""
        if category in seen_categories:
            continue  # Count each category once
        weight = FOUNDER_HIGHLIGHT_SCORES.get(category, 0)
        if weight > 0:
            seen_categories.add(category)
            score += weight

    return score, list(seen_categories)

def compute_total_score(company):
    raising = compute_raising_score(company)
    founder, founder_tags = compute_founder_score(company)
    # Weight: 50% raising signals, 50% founder quality
    total = raising + founder
    return total, raising, founder, founder_tags

# ---------- Harmonic API ----------

def search_companies(page_size=200, start=0):
    r = requests.post(f"{HARMONIC_BASE}/search/companies", headers=HARMONIC_HEADERS, json={
        "query": {
            "filter_group": {
                "join_operator": "and",
                "filters": [
                    {"field": "company_funding_stage", "comparator": "anyOf", "filter_value": ["PRE_SEED", "SEED"]},
                    {"field": "company_and_employee_highlight_count", "comparator": "greaterThanOrEquals", "filter_value": 8},
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
        time.sleep(0.2)
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
            candidates.append({"urn": person_urn, "priority": 1, "title": person.get("title", "")})
        elif "founder" in title or "co-founder" in title:
            candidates.append({"urn": person_urn, "priority": 2, "title": person.get("title", "")})
    candidates.sort(key=lambda x: x["priority"])
    return candidates

# ---------- Main ----------

def main():
    print("=" * 70)
    print("  Palm Drive Sourcing: High-Quality Founders Raising Now")
    print("=" * 70)
    print("  Stage:    Pre-Seed / Seed")
    print("  Signals:  8+ highlights, growing headcount")
    print("  Scoring:  50% raising-now signals + 50% founder quality")
    print("  Exclude:  China/Russia/Ukraine/India, hardware, biotech, nonprofits")
    print("  Require:  CEO/Founder name + email")
    print()

    affinity = AffinityClient(AFFINITY_API_KEY)

    # Step 1: Search Harmonic
    print("[1/5] Searching Harmonic for high-signal early-stage startups...")
    all_urns = []
    start = 0
    while len(all_urns) < 1500:
        data = search_companies(page_size=200, start=start)
        urns = data.get("results", [])
        if not urns:
            break
        all_urns.extend(urns)
        start += len(urns)
        total_available = data.get("count", "?")
        print(f"  Fetched {len(all_urns)} URNs (available: {total_available})")
        time.sleep(0.2)
    print(f"  Total URNs: {len(all_urns)}")

    # Step 2: Batch fetch details
    print(f"\n[2/5] Fetching company details...")
    companies = batch_get_companies(all_urns)
    print(f"  Got {len(companies)} company records")

    # Step 3: Filter and score
    print(f"\n[3/5] Filtering, scoring raising signals + founder quality...")
    stats = {"country": 0, "industry": 0, "nonprofit": 0, "consumer": 0, "not_startup": 0}
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
            stats["not_startup"] += 1
            continue

        total, raising, founder, founder_tags = compute_total_score(c)
        scored.append({
            "company": c,
            "total_score": total,
            "raising_score": raising,
            "founder_score": founder,
            "founder_tags": founder_tags,
        })

    # Sort by total score
    scored.sort(key=lambda x: -x["total_score"])

    print(f"  Excluded - country: {stats['country']}, industry: {stats['industry']}, "
          f"nonprofit: {stats['nonprofit']}, consumer: {stats['consumer']}, not startup: {stats['not_startup']}")
    print(f"  Candidates after filter: {len(scored)}")
    print(f"  Top 10 total scores: {[s['total_score'] for s in scored[:10]]}")
    print(f"  Top 10 founder scores: {[s['founder_score'] for s in scored[:10]]}")

    # Step 4: Get CEO info + check Affinity for top candidates
    print(f"\n[4/5] Getting CEO info and checking Affinity (top 600 candidates)...")
    top_candidates = scored[:600]

    # Batch fetch person URNs
    all_person_urns = []
    company_candidates = {}
    for item in top_candidates:
        c = item["company"]
        urn = c.get("entity_urn") or ""
        candidates = find_ceo_candidates(c)
        company_candidates[urn] = candidates
        for cand in candidates:
            all_person_urns.append(cand["urn"])

    all_person_urns = list(dict.fromkeys(all_person_urns))
    print(f"  Fetching {len(all_person_urns)} person records...")
    people_by_urn = batch_get_persons(all_person_urns) if all_person_urns else {}

    # Find CEO+email, check Affinity, collect top 100
    final = []
    skipped_no_ceo = 0
    skipped_affinity = 0
    seen_names = set()

    for item in top_candidates:
        if len(final) >= 100:
            break

        c = item["company"]
        urn = c.get("entity_urn") or ""
        name = c.get("name") or ""

        # Dedupe by name
        norm = normalize_name(name)
        if norm in seen_names:
            continue
        seen_names.add(norm)

        candidates = company_candidates.get(urn, [])

        # Find CEO with email
        ceo_name = ""
        first_name = ""
        email = ""
        ceo_title = ""

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
                ceo_title = cand.get("title", "")
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

        print(f"  [{len(final)+1:3d}] {name:<30} R={item['raising_score']:5.0f} F={item['founder_score']:5.0f} T={item['total_score']:5.0f}  ", end="", flush=True)

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
                print(f"✓ clean")
        else:
            print(f"✓ new")

        final.append({
            **item,
            "ceo_name": ceo_name,
            "first_name": first_name,
            "email": email,
            "ceo_title": ceo_title,
            "domain": domain,
        })
        time.sleep(0.15)

    print(f"\n  Final list: {len(final)}")
    print(f"  Skipped (no CEO+email): {skipped_no_ceo}")
    print(f"  Skipped (already in Affinity): {skipped_affinity}")

    # Step 5: Write to spreadsheet
    print(f"\n[5/5] Writing to spreadsheet...")
    sheet = get_sheet()
    sheet.clear()

    headers = [
        "companyName", "firstName", "email", "ceoName", "ceoTitle", "domain",
        "Total Score", "Raising Score", "Founder Score", "Founder Signals",
        "Stage", "Funding Total", "Headcount",
        "HC Growth (6mo)", "Web Traffic", "WT Growth (6mo)",
        "LinkedIn", "LI Growth (6mo)",
        "Country", "City", "Customer Type",
        "Tags", "Founded", "Description"
    ]

    output_rows = [headers]

    for item in final:
        c = item["company"]
        funding = c.get("funding") or {}
        funding_total = funding.get("funding_total") or 0
        location = c.get("location") or {}
        tags = c.get("tags") or []
        fd = c.get("founding_date") or {}
        tm = c.get("traction_metrics") or {}

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
            item["ceo_title"],
            item["domain"],
            f"{item['total_score']:.0f}",
            f"{item['raising_score']:.0f}",
            f"{item['founder_score']:.0f}",
            ", ".join(item["founder_tags"]),
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
            founded,
            (c.get("description") or "")[:200],
        ])

    sheet.update(range_name='A1', values=output_rows)

    # Summary stats
    avg_founder = sum(i["founder_score"] for i in final) / len(final) if final else 0
    avg_raising = sum(i["raising_score"] for i in final) / len(final) if final else 0
    high_quality = sum(1 for i in final if i["founder_score"] >= 30)

    print(f"\n{'=' * 70}")
    print(f"  DONE! {len(final)} startups - high-quality founders raising now")
    print(f"{'=' * 70}")
    print(f"  Avg Founder Quality Score:  {avg_founder:.0f}")
    print(f"  Avg Raising-Now Score:      {avg_raising:.0f}")
    print(f"  High-quality founders (30+): {high_quality}/{len(final)}")
    print(f"  All have CEO/Founder name + email")
    print(f"  Zero prior Affinity interactions")
    print(f"\n  Spreadsheet: https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}")
    print(f"{'=' * 70}")

if __name__ == "__main__":
    main()
