#!/usr/bin/env python3
"""Find startups related to portfolio verticals and post to Slack."""

import os, re, time, json, csv, io
from datetime import datetime
import requests
import gspread
from google.oauth2.service_account import Credentials

HARMONIC_API_KEY = os.environ["HARMONIC_API_KEY"]
AFFINITY_API_KEY = os.environ["AFFINITY_API_KEY"]
LEMLIST_API_KEY = os.environ["LEMLIST_API_KEY"]
SPREADSHEET_ID = os.environ["SPREADSHEET_ID"]
SLACK_WEBHOOK_URL = os.environ["SLACK_WEBHOOK_URL"]

HARMONIC_BASE = "https://api.harmonic.ai"
HARMONIC_HEADERS = {"apikey": HARMONIC_API_KEY, "Content-Type": "application/json"}
AFFINITY_BASE = "https://api.affinity.co"
TARGET_LIST_ID = int(os.environ.get("AFFINITY_LIST_ID", "21233"))

ELITE_COMPANIES = {
    "google", "meta", "facebook", "apple", "amazon", "microsoft", "netflix",
    "stripe", "brex", "airbnb", "uber", "lyft", "doordash",
    "coinbase", "robinhood", "plaid", "figma", "notion", "slack", "discord",
    "snowflake", "databricks", "datadog", "palantir", "salesforce",
    "twitter", "x", "linkedin", "snap", "pinterest", "spotify",
    "openai", "anthropic", "deepmind", "tesla", "spacex",
    "square", "block", "shopify", "dropbox", "nvidia", "oracle",
    "mckinsey", "bain", "bcg", "goldman sachs", "morgan stanley",
    "jp morgan", "jpmorgan", "a16z", "sequoia", "y combinator", "yc",
}
ELITE_SCHOOLS = {
    "stanford", "mit", "harvard", "yale", "princeton", "caltech",
    "carnegie mellon", "berkeley", "columbia", "cornell", "upenn",
    "wharton", "booth", "kellogg", "sloan", "haas", "oxford", "cambridge",
}
EXCLUDED_TAGS_KEYWORDS = {
    "hardware", "biotech", "biotechnology", "pharmaceutical", "medical devices",
    "semiconductors", "robotics", "3d printing", "manufacturing", "clean energy",
    "solar", "battery", "cannabis", "nonprofit", "non-profit", "charity",
    "government", "real estate", "construction", "agriculture", "mining",
    "oil", "gas", "energy", "healthcare", "health care", "healthtech",
    "medical", "clinical", "patient", "hospital", "telehealth",
}

# Portfolio verticals to search for related companies
VERTICALS = [
    {
        "name": "AI Legal Tech",
        "emoji": "\u2696\ufe0f",
        "portfolio_examples": "JIGO, FasterOutcomes, CogniSync, Cubby Law, newcase.ai",
        "search_keywords": ["legal", "law", "contract", "compliance"],
        "extra_filters": [],
    },
    {
        "name": "AI Sales & Revenue",
        "emoji": "\U0001f4b0",
        "portfolio_examples": "Raynmaker, RocketSDR, TwinsAI, Blazi AI",
        "search_keywords": ["sales", "revenue", "outbound", "pipeline", "crm"],
        "extra_filters": [],
    },
    {
        "name": "AI HR & Recruiting",
        "emoji": "\U0001f465",
        "portfolio_examples": "DianaHR, lizzyAI, BloomPath AI, Holly, Tenzo AI",
        "search_keywords": ["hiring", "recruiting", "hr", "talent", "workforce"],
        "extra_filters": [],
    },
    {
        "name": "AI Infrastructure & DevTools",
        "emoji": "\U0001f527",
        "portfolio_examples": "Konko AI, Moss, hiddenweights, smallest.ai, Orchids",
        "search_keywords": ["developer", "devtools", "infrastructure", "api", "llm"],
        "extra_filters": [],
    },
    {
        "name": "AI Cybersecurity",
        "emoji": "\U0001f6e1\ufe0f",
        "portfolio_examples": "Helmet Security, MilkStraw AI, AiStrike, Postquant Labs",
        "search_keywords": ["security", "cybersecurity", "identity", "privacy", "threat"],
        "extra_filters": [],
    },
    {
        "name": "AI Finance & Accounting",
        "emoji": "\U0001f4ca",
        "portfolio_examples": "Maximor AI, Zalos, Twocents.ai, Limited, Haraka",
        "search_keywords": ["finance", "accounting", "fintech", "payments", "banking"],
        "extra_filters": [],
    },
    {
        "name": "AI Insurance",
        "emoji": "\U0001f4cb",
        "portfolio_examples": "ResiQuant, Vivere, Strala",
        "search_keywords": ["insurance", "underwriting", "claims", "risk"],
        "extra_filters": [],
    },
    {
        "name": "AI Supply Chain & Procurement",
        "emoji": "\U0001f4e6",
        "portfolio_examples": "Emerix, Hyme AI, Jampack, InTension",
        "search_keywords": ["supply chain", "procurement", "logistics", "inventory"],
        "extra_filters": [],
    },
]

def chunked(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i+n]

def normalize_name(name):
    if not name: return ""
    name = name.lower().strip()
    name = re.sub(r'\s+(inc|llc|ltd|corp|co|company)\.?$', '', name, flags=re.IGNORECASE)
    name = re.sub(r'[^a-z0-9\s]', '', name)
    return re.sub(r'\s+', ' ', name).strip()

def compute_raise_score(company):
    score = 0
    tm = company.get("traction_metrics") or {}
    hc = tm.get("corrected_headcount") or {}
    hc_change = (hc.get("180d_ago") or {}).get("change")
    if hc_change and hc_change > 0:
        score += min(hc_change * 5, 25)
    hc_change_90 = (hc.get("90d_ago") or {}).get("change")
    if hc_change_90 and hc_change_90 > 0:
        score += min(hc_change_90 * 8, 25)
    wt_pct = ((tm.get("web_traffic") or {}).get("180d_ago") or {}).get("percent_change")
    if wt_pct and wt_pct > 0:
        score += min(wt_pct / 10, 20)
    li_pct = ((tm.get("linkedin_follower_count") or {}).get("180d_ago") or {}).get("percent_change")
    if li_pct and li_pct > 0:
        score += min(li_pct / 5, 15)
    highlights = company.get("highlights") or []
    score += min(len(highlights) * 3, 15)
    emergence = company.get("stealth_emergence_date")
    if emergence:
        try:
            ed = datetime.fromisoformat(emergence.replace("Z", "+00:00"))
            days_ago = (datetime.now(ed.tzinfo) - ed).days
            if days_ago < 90: score += 20
            elif days_ago < 180: score += 15
            elif days_ago < 365: score += 10
        except: pass
    funding = company.get("funding") or {}
    funding_total = funding.get("funding_total") or 0
    headcount = company.get("headcount") or 1
    if isinstance(headcount, dict):
        headcount = headcount.get("latest_metric_value") or 1
    if headcount > 3 and funding_total < 2_000_000: score += 10
    if funding_total == 0 and headcount > 2: score += 15
    if 1_000_000 <= funding_total <= 5_000_000 and headcount > 5: score += 15
    elif funding_total < 1_000_000 and headcount > 3: score += 10
    fd = company.get("founding_date") or {}
    fd_date = fd.get("date") or ""
    if fd_date:
        try:
            year = int(fd_date[:4])
            if year >= 2024: score += 10
            elif year >= 2023: score += 5
        except: pass
    return score

def compute_founder_score(person):
    if not person: return 0
    score = 0
    experience = person.get("experience") or []
    seen = set()
    for exp in experience:
        co = (exp.get("company_name") or "").lower()
        title = (exp.get("title") or "").lower()
        for elite in ELITE_COMPANIES:
            if elite in co and co not in seen:
                seen.add(co)
                score += 15
                if any(w in title for w in ["cto", "vp", "director", "head of", "chief", "principal", "staff"]):
                    score += 10
                elif any(w in title for w in ["senior", "manager"]):
                    score += 5
                break
    for edu in (person.get("education") or []):
        school_name = ((edu.get("school") or {}).get("name") or "").lower()
        for elite in ELITE_SCHOOLS:
            if elite in school_name:
                score += 10
                break
    for h in (person.get("highlights") or []):
        cat = (h.get("category") or "").lower()
        if "prior_exit" in cat: score += 20
        elif "serial_founder" in cat: score += 15
        elif "major_tech" in cat or "faang" in cat: score += 10
        elif "top_university" in cat: score += 5
    return score

def load_existing_companies():
    existing = set()
    try:
        scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
        creds = Credentials.from_service_account_file('credentials.json', scopes=scopes)
        gc = gspread.authorize(creds)
        spreadsheet = gc.open_by_key(SPREADSHEET_ID)
        for ws in spreadsheet.worksheets():
            try:
                for row in ws.get_all_values()[1:]:
                    if row and row[0].strip():
                        existing.add(normalize_name(row[0]))
            except: pass
    except Exception as e:
        print(f"  Sheet load error: {e}")
    try:
        r = requests.get('https://api.lemlist.com/api/campaigns', auth=('', LEMLIST_API_KEY), timeout=60)
        if r.status_code == 200:
            for c in json.loads(r.text):
                cid = c.get('_id', '')
                r2 = requests.get(f'https://api.lemlist.com/api/campaigns/{cid}/export', auth=('', LEMLIST_API_KEY), timeout=60)
                if r2.status_code == 200 and r2.text:
                    for row in csv.DictReader(io.StringIO(r2.text)):
                        company = (row.get('companyName') or '').strip()
                        if company: existing.add(normalize_name(company))
    except Exception as e:
        print(f"  Lemlist load error: {e}")
    return existing

def search_vertical(keywords, page_size=200):
    """Search Harmonic with vertical-specific keyword matching via description."""
    r = requests.post(f"{HARMONIC_BASE}/search/companies", headers=HARMONIC_HEADERS, json={
        "query": {
            "filter_group": {
                "join_operator": "and",
                "filters": [
                    {"field": "company_funding_stage", "comparator": "anyOf", "filter_value": ["PRE_SEED", "SEED", "SERIES_A"]},
                    {"field": "company_and_employee_highlight_count", "comparator": "greaterThanOrEquals", "filter_value": 1},
                    {"field": "company_country", "comparator": "anyOf", "filter_value": ["United States"]},
                ],
            },
            "pagination": {"page_size": page_size, "start": 0}
        },
        "sort": {"field": "relevance_score", "descending": True}
    }, timeout=60)
    r.raise_for_status()
    return r.json().get("results", [])

def batch_get_companies(urns):
    out = []
    for chunk in chunked(urns, 50):
        for attempt in range(3):
            try:
                r = requests.post(f"{HARMONIC_BASE}/companies/batchGet", headers=HARMONIC_HEADERS, json={"urns": chunk}, timeout=180)
                r.raise_for_status()
                data = r.json()
                out.extend(data if isinstance(data, list) else data.get("results", []))
                break
            except Exception as e:
                if attempt < 2: time.sleep(3)
                else: print(f"    Skipping batch: {e}")
        time.sleep(0.3)
    return out

def batch_get_persons(urns):
    out = {}
    for chunk in chunked(urns, 20):
        for attempt in range(3):
            try:
                r = requests.post(f"{HARMONIC_BASE}/persons/batchGet", headers=HARMONIC_HEADERS, json={"urns": chunk}, timeout=180)
                r.raise_for_status()
                data = r.json()
                for p in (data if isinstance(data, list) else data.get("results", [])):
                    urn = p.get("entity_urn") or p.get("person_urn")
                    if urn: out[urn] = p
                break
            except Exception as e:
                if attempt < 2: time.sleep(3)
                else: print(f"    Skipping batch: {e}")
        time.sleep(0.3)
    return out

def check_affinity(name, domain):
    session = requests.Session()
    session.auth = ('', AFFINITY_API_KEY)
    orgs_to_check = []
    if domain:
        r = session.get(f"{AFFINITY_BASE}/organizations", params={"term": domain, "page_size": 10}, timeout=60)
        if r.status_code == 200:
            data = r.json()
            orgs = data.get("organizations", []) if isinstance(data, dict) else data
            for o in (orgs or []):
                org_domain = (o.get("domain") or "").lower().strip()
                org_name = normalize_name(o.get("name") or "")
                if org_domain == domain.lower() or org_name == normalize_name(name):
                    orgs_to_check.append(o)
    if name:
        r = session.get(f"{AFFINITY_BASE}/organizations", params={"term": name, "page_size": 10}, timeout=60)
        if r.status_code == 200:
            data = r.json()
            orgs = data.get("organizations", []) if isinstance(data, dict) else data
            seen_ids = {o.get("id") for o in orgs_to_check}
            norm = normalize_name(name)
            for o in (orgs or []):
                if o.get("id") not in seen_ids and normalize_name(o.get("name") or "") == norm:
                    orgs_to_check.append(o)
    if not orgs_to_check:
        return False
    for org in orgs_to_check:
        org_id = org.get("id")
        r = session.get(f"{AFFINITY_BASE}/organizations/{org_id}", timeout=60)
        if r.status_code == 200:
            detail = r.json()
            if detail.get("list_entries"):
                return True
        r = session.get(f"{AFFINITY_BASE}/notes", params={"organization_id": org_id, "page_size": 5}, timeout=60)
        if r.status_code == 200:
            notes = r.json()
            note_list = notes if isinstance(notes, list) else notes.get("notes", [])
            if note_list:
                return True
        time.sleep(0.15)
    return False

def match_vertical(company, keywords):
    """Check if a company matches a vertical based on keywords in description/tags."""
    desc = (company.get("description") or "").lower()
    tags = company.get("tags") or []
    tag_texts = " ".join([(t.get("display_value") or "").lower() for t in tags])
    combined = desc + " " + tag_texts
    return any(kw in combined for kw in keywords)

def build_why_interesting(s, vertical_name, portfolio_examples):
    """Generate why this startup is interesting relative to portfolio."""
    signals = []

    signals.append(f"Related to your {vertical_name} portfolio ({portfolio_examples})")

    prev_roles = []
    if s.get("founder_prev"):
        for role in s["founder_prev"].split(";"):
            role = role.strip()
            if role: prev_roles.append(role)

    if s.get("founder_highlights"):
        hl = s["founder_highlights"].lower()
        if "prior exit" in hl:
            signals.append("Founder with a prior exit")
        elif "serial founder" in hl:
            signals.append("Serial founder")

    if not any("exit" in s or "serial" in s for s in signals[1:]) and prev_roles:
        elite_roles = []
        for role in prev_roles[:2]:
            for elite in ["Google", "Meta", "Apple", "Amazon", "Microsoft", "Stripe",
                           "OpenAI", "Anthropic", "Airbnb", "Uber", "Tesla", "SpaceX",
                           "Goldman", "McKinsey", "Sequoia", "a16z", "Palantir",
                           "Databricks", "Snowflake", "Plaid", "Figma"]:
                if elite.lower() in role.lower():
                    elite_roles.append(role.strip())
                    break
        if elite_roles:
            signals.append(f"Previously {elite_roles[0]}")

    funding = s.get("funding_total", 0)
    headcount = s.get("headcount", 0)
    try: headcount = int(headcount)
    except: headcount = 0

    if funding == 0 and headcount > 3:
        signals.append(f"Already {headcount} people with no outside funding")
    elif headcount > 10:
        signals.append(f"Team of {headcount} and scaling")

    return ". ".join(signals[:3])

def post_to_slack(startups_by_vertical):
    """Post organized by vertical to Slack."""
    today = datetime.now().strftime("%B %d, %Y")
    total = sum(len(v) for v in startups_by_vertical.values())

    if total == 0:
        payload = {"blocks": [
            {"type": "header", "text": {"type": "plain_text", "text": f"Portfolio-Adjacent Startups \u2014 {today}"}},
            {"type": "section", "text": {"type": "mrkdwn", "text": "No new related companies found today."}},
        ]}
        requests.post(SLACK_WEBHOOK_URL, json=payload, timeout=30)
        return

    # Header message
    header_blocks = [
        {"type": "header", "text": {"type": "plain_text", "text": f"Portfolio-Adjacent Startups \u2014 {today}"}},
        {"type": "context", "elements": [{"type": "mrkdwn", "text": f"{total} new companies related to your portfolio verticals \u2014 not in Affinity or Lemlist"}]},
    ]
    r = requests.post(SLACK_WEBHOOK_URL, json={"blocks": header_blocks}, timeout=30)
    print(f"  Header: {r.status_code}")

    # Post each vertical as a separate message to avoid block limits
    for vertical_name, startups in startups_by_vertical.items():
        if not startups:
            continue

        v_info = next((v for v in VERTICALS if v["name"] == vertical_name), {})
        emoji = v_info.get("emoji", "\U0001f680")
        portfolio = v_info.get("portfolio_examples", "")

        blocks = [
            {"type": "header", "text": {"type": "plain_text", "text": f"{emoji} {vertical_name}"}},
            {"type": "context", "elements": [{"type": "mrkdwn", "text": f"Related to: {portfolio}"}]},
        ]

        for s in startups[:5]:
            funding = s.get("funding_total", 0)
            funding_str = f"${funding:,.0f}" if funding else "Bootstrapped"
            stage = (s.get("stage") or "").replace("_", " ").title()
            city = s.get("city", "")
            desc = s.get("description") or ""
            if len(desc) > 200:
                cut = desc[:200].rfind(".")
                if cut > 80:
                    desc = desc[:cut+1]
                else:
                    cut = desc[:200].rfind(" ")
                    desc = desc[:cut] + "..." if cut > 0 else desc[:200] + "..."

            why = build_why_interesting(s, vertical_name, portfolio)

            founder_line = f"*{s.get('ceo_name', '')}*"
            bg_parts = []
            if s.get("founder_prev"):
                roles = [r.strip() for r in s["founder_prev"].split(";")][:2]
                bg_parts.extend(roles)
            if s.get("founder_education"):
                schools = [e.strip() for e in s["founder_education"].split(",")][:1]
                bg_parts.extend(schools)
            if bg_parts:
                founder_line += f"  \u00b7  {' \u2192 '.join(bg_parts)}"

            text = (
                f"*<{s.get('website', '')}|{s['name']}>*\n"
                f"{desc}\n\n"
                f"{founder_line}\n\n"
            )
            if why:
                text += f"*Why interesting:* {why}\n"
            text += f"{city}  \u00b7  {stage}  \u00b7  {funding_str}  \u00b7  Score {s['combined_score']:.0f}"

            blocks.append({"type": "divider"})
            blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": text}})

        r = requests.post(SLACK_WEBHOOK_URL, json={"blocks": blocks}, timeout=30)
        print(f"  {vertical_name}: {r.status_code} ({len(startups)} companies)")

    # Footer
    footer_blocks = [
        {"type": "divider"},
        {"type": "actions", "elements": [{
            "type": "button",
            "text": {"type": "plain_text", "text": "Open Spreadsheet"},
            "url": f"https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}",
        }]},
        {"type": "context", "elements": [{"type": "mrkdwn", "text": "\U0001f9e0 *Palm Drive Capital* \u00b7 Portfolio-adjacent scan by Claude"}]},
    ]
    requests.post(SLACK_WEBHOOK_URL, json={"blocks": footer_blocks}, timeout=30)

def main():
    print(f"{'='*60}")
    print(f"Portfolio-Adjacent Startup Scanner \u2014 {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"{'='*60}\n")

    # Load existing companies for dedup
    print("[1/5] Loading existing companies...", flush=True)
    existing = load_existing_companies()
    print(f"  {len(existing)} companies to skip")

    # Search Harmonic
    print("\n[2/5] Searching Harmonic...", flush=True)
    urns = search_vertical([], page_size=500)
    print(f"  Found {len(urns)} companies")

    if not urns:
        print("No companies found.")
        post_to_slack({})
        return

    # Fetch details
    print("\n[3/5] Fetching company details...", flush=True)
    companies = batch_get_companies(urns)
    print(f"  Got {len(companies)} records")

    # Classify by vertical and score
    print("\n[4/5] Classifying by vertical and scoring...", flush=True)
    seen = set()
    vertical_candidates = {v["name"]: [] for v in VERTICALS}

    for c in companies:
        name = c.get("name") or ""
        norm = normalize_name(name)
        if norm in seen or norm in existing: continue
        seen.add(norm)

        # US only
        location = c.get("location") or {}
        country = (location.get("country") or "").lower()
        if country not in ("united states", "us", "usa"): continue

        # Excluded industries
        tags = c.get("tags") or []
        if any(any(exc in (t.get("display_value") or "").lower() for exc in EXCLUDED_TAGS_KEYWORDS) for t in tags): continue

        # Startup only
        company_type = c.get("company_type") or ""
        if company_type and company_type.upper() not in ("STARTUP", ""): continue

        # Founded 2023+
        fd = c.get("founding_date") or {}
        fd_date = fd.get("date") or ""
        try:
            year = int(fd_date[:4])
            if year < 2023: continue
        except: continue

        # Funding < $10M
        funding = c.get("funding") or {}
        funding_total = funding.get("funding_total") or 0
        if funding_total > 10_000_000: continue

        raise_score = compute_raise_score(c)

        # Match to verticals
        for v in VERTICALS:
            if match_vertical(c, v["search_keywords"]):
                vertical_candidates[v["name"]].append((raise_score, c))

    for vname in vertical_candidates:
        vertical_candidates[vname].sort(key=lambda x: -x[0])
        print(f"  {vname}: {len(vertical_candidates[vname])} candidates")

    # Get CEO info and check Affinity for top candidates per vertical
    print("\n[5/5] Getting CEO info and checking Affinity...", flush=True)
    results_by_vertical = {}

    for v in VERTICALS:
        vname = v["name"]
        candidates = vertical_candidates[vname][:30]  # Check top 30 per vertical
        if not candidates:
            continue

        print(f"\n  --- {vname} ---")

        # Batch get persons
        all_person_urns = []
        company_people = {}
        for _, c in candidates:
            urn = c.get("entity_urn") or ""
            people = c.get("people") or []
            cands = []
            for p in people:
                if not isinstance(p, dict): continue
                if not p.get("is_current_position", False): continue
                title = (p.get("title") or "").lower()
                purn = p.get("person") or p.get("person_urn") or ""
                if not purn: continue
                if "ceo" in title or "chief executive" in title:
                    cands.append({"urn": purn, "priority": 1, "title": p.get("title", "")})
                elif "founder" in title or "co-founder" in title:
                    cands.append({"urn": purn, "priority": 2, "title": p.get("title", "")})
            cands.sort(key=lambda x: x["priority"])
            company_people[urn] = cands
            for cd in cands:
                all_person_urns.append(cd["urn"])

        all_person_urns = list(dict.fromkeys(all_person_urns))
        people_by_urn = batch_get_persons(all_person_urns) if all_person_urns else {}

        vresults = []
        for raise_score, c in candidates:
            if len(vresults) >= 5:
                break

            name = c.get("name") or ""
            urn = c.get("entity_urn") or ""
            cands = company_people.get(urn, [])

            ceo_name = ""
            email = ""
            chosen = None
            for cd in cands:
                pd = people_by_urn.get(cd["urn"], {})
                contact = pd.get("contact") or {}
                pe = (contact.get("primary_email") or "").strip()
                if not pe:
                    emails = contact.get("emails") or []
                    if emails:
                        pe = (emails[0] if isinstance(emails[0], str) else "").strip()
                pn = pd.get("full_name") or pd.get("name") or ""
                if pe and pn:
                    ceo_name = pn
                    email = pe
                    chosen = pd
                    break

            if not email: continue

            website = c.get("website") or {}
            domain = ""
            if isinstance(website, dict):
                url = website.get("url") or ""
                domain = url.replace("https://", "").replace("http://", "").replace("www.", "").split("/")[0] if url else ""
            web_url = (website.get("url") or "") if isinstance(website, dict) else ""

            if check_affinity(name, domain):
                print(f"    {name}: SKIP (in Affinity)")
                continue

            founder_score = compute_founder_score(chosen)
            combined = raise_score + founder_score
            if combined < 30:
                continue

            # Founder background
            bg_edu = ""
            bg_prev = ""
            bg_highlights = ""
            if chosen:
                edu_list = chosen.get("education") or []
                edu_parts = []
                for edu in edu_list[:2]:
                    school = (edu.get("school") or {}).get("name") or ""
                    degree = edu.get("degree") or ""
                    if school:
                        edu_parts.append(f"{school} ({degree})" if degree and degree != "NA" else school)
                bg_edu = ", ".join(edu_parts)

                exp = chosen.get("experience") or []
                prev = []
                for e in exp:
                    if not e.get("is_current_position", False):
                        co = e.get("company_name") or ""
                        t = e.get("title") or ""
                        if co: prev.append(f"{t} @ {co}" if t else co)
                bg_prev = "; ".join(prev[:3])

                ph = chosen.get("highlights") or []
                bg_highlights = ", ".join([h.get("category", "").replace("_", " ").title() for h in ph[:3]])

            location = c.get("location") or {}
            funding = c.get("funding") or {}
            headcount = c.get("headcount") or c.get("corrected_headcount") or ""
            if isinstance(headcount, dict):
                headcount = headcount.get("latest_metric_value") or ""

            print(f"    {name}: OK (score {combined:.0f})")

            vresults.append({
                "name": name,
                "ceo_name": ceo_name,
                "email": email,
                "domain": domain,
                "website": web_url,
                "combined_score": combined,
                "raise_score": raise_score,
                "founder_score": founder_score,
                "stage": c.get("stage", ""),
                "funding_total": funding.get("funding_total") or 0,
                "headcount": headcount,
                "city": location.get("city", ""),
                "description": c.get("description") or "",
                "founder_education": bg_edu,
                "founder_prev": bg_prev,
                "founder_highlights": bg_highlights,
            })

        if vresults:
            results_by_vertical[vname] = vresults

    total = sum(len(v) for v in results_by_vertical.values())
    print(f"\n  Total: {total} startups across {len(results_by_vertical)} verticals")

    # Post to Slack
    post_to_slack(results_by_vertical)

    print(f"\n{'='*60}")
    print(f"DONE \u2014 {total} portfolio-adjacent startups posted to Slack")
    print(f"{'='*60}")

if __name__ == "__main__":
    main()
