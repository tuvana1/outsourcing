#!/usr/bin/env python3
"""Daily Slack bot: finds top startups likely raising and posts to Slack."""

import os, re, time, json, csv, io
from datetime import datetime
import requests
import gspread
from google.oauth2.service_account import Credentials

# ---------- Config ----------

HARMONIC_API_KEY = os.environ["HARMONIC_API_KEY"]
AFFINITY_API_KEY = os.environ["AFFINITY_API_KEY"]
LEMLIST_API_KEY = os.environ["LEMLIST_API_KEY"]
SPREADSHEET_ID = os.environ["SPREADSHEET_ID"]
SLACK_WEBHOOK_URL = os.environ.get("SLACK_WEBHOOK_URL", "")
SLACK_BOT_TOKEN = os.environ.get("SLACK_BOT_TOKEN", "")
SLACK_CHANNEL_ID = os.environ.get("SLACK_CHANNEL_ID", "")

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

MIN_SCORE_TO_POST = 30  # Only post startups scoring above this

# ---------- Helpers ----------

def chunked(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i+n]

def normalize_name(name):
    if not name: return ""
    name = name.lower().strip()
    name = re.sub(r'\s+(inc|llc|ltd|corp|co|company)\.?$', '', name, flags=re.IGNORECASE)
    name = re.sub(r'[^a-z0-9\s]', '', name)
    return re.sub(r'\s+', ' ', name).strip()

# ---------- Scoring ----------

def compute_raise_score(company):
    score = 0
    tm = company.get("traction_metrics") or {}
    hc = tm.get("corrected_headcount") or {}
    hc_180 = hc.get("180d_ago", {})
    hc_change = hc_180.get("change")
    if hc_change and hc_change > 0:
        score += min(hc_change * 5, 25)
    hc_90 = hc.get("90d_ago", {})
    hc_change_90 = hc_90.get("change")
    if hc_change_90 and hc_change_90 > 0:
        score += min(hc_change_90 * 8, 25)
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
    edu_list = person.get("education") or []
    for edu in edu_list:
        school_name = ((edu.get("school") or {}).get("name") or "").lower()
        for elite in ELITE_SCHOOLS:
            if elite in school_name:
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

# ---------- Harmonic ----------

def search_recent_companies():
    """Search for recently emerged / high-signal startups."""
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
            "pagination": {"page_size": 500, "start": 0}
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

# ---------- Dedup ----------

def load_existing_companies():
    """Load all companies from sheets + Lemlist."""
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

    # Lemlist
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

def check_affinity(name, domain):
    """Returns True if company has any prior interaction in Affinity.
    Checks ALL matching orgs (by domain AND name) to avoid false negatives."""
    session = requests.Session()
    session.auth = ('', AFFINITY_API_KEY)

    orgs_to_check = []

    # Search by domain
    if domain:
        r = session.get(f"{AFFINITY_BASE}/organizations", params={"term": domain, "page_size": 10}, timeout=60)
        if r.status_code == 200:
            data = r.json()
            orgs = data.get("organizations", []) if isinstance(data, dict) else data
            for o in (orgs or []):
                org_domain = (o.get("domain") or "").lower().strip()
                org_name = normalize_name(o.get("name") or "")
                # Match by domain or name
                if org_domain == domain.lower() or org_name == normalize_name(name):
                    orgs_to_check.append(o)

    # Also search by name (catches cases where domain differs)
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
        return False  # Not in Affinity = clean

    # Check ALL matching orgs — if ANY has activity, it's a hit
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

# ---------- Slack ----------

def build_why_interesting(s):
    """Generate a compelling one-liner about why this startup matters."""
    signals = []

    # Founder career narrative (most important)
    prev_roles = []
    if s.get("founder_prev"):
        for role in s["founder_prev"].split(";"):
            role = role.strip()
            if role:
                prev_roles.append(role)

    if s.get("founder_highlights"):
        hl = s["founder_highlights"].lower()
        if "prior exit" in hl and prev_roles:
            # Find the company they exited from
            signals.append(f"Previously founded and exited ({prev_roles[0].split('@')[-1].strip() if '@' in prev_roles[0] else prev_roles[0]})")
        elif "prior exit" in hl:
            signals.append("Founder with a prior exit")
        elif "serial founder" in hl:
            signals.append("Serial founder")

    if not signals and prev_roles:
        # Build a career story from their top roles
        elite_roles = []
        for role in prev_roles[:2]:
            for elite in ["Google", "Meta", "Apple", "Amazon", "Microsoft", "Stripe", "Coinbase",
                           "OpenAI", "Anthropic", "Airbnb", "Uber", "Tesla", "SpaceX", "Goldman",
                           "McKinsey", "Sequoia", "a16z", "Y Combinator", "Netflix", "Palantir",
                           "Databricks", "Snowflake", "Plaid", "Figma"]:
                if elite.lower() in role.lower():
                    elite_roles.append(role.strip())
                    break
        if elite_roles:
            signals.append(f"Previously {elite_roles[0]}")

    # Traction signals
    funding = s.get("funding_total", 0)
    headcount = s.get("headcount", 0)
    try: headcount = int(headcount)
    except: headcount = 0

    if funding == 0 and headcount > 3:
        signals.append(f"Already {headcount} people with no outside funding")
    elif funding > 0 and funding < 2_000_000 and headcount > 5:
        signals.append(f"Growing fast — {headcount} people on just ${funding/1_000_000:.1f}M")
    elif headcount > 10:
        signals.append(f"Team of {headcount} and scaling")

    if not signals:
        # Fallback — describe the opportunity
        stage = (s.get("stage") or "").replace("_", " ").lower()
        if stage and funding:
            signals.append(f"{stage.title()} stage, likely raising next round")

    return ". ".join(signals[:2])

def slack_post(blocks):
    """Post to Slack via Bot API (preferred) or webhook fallback. Returns message ts."""
    if SLACK_BOT_TOKEN and SLACK_CHANNEL_ID:
        r = requests.post("https://slack.com/api/chat.postMessage",
            headers={"Authorization": f"Bearer {SLACK_BOT_TOKEN}", "Content-Type": "application/json"},
            json={"channel": SLACK_CHANNEL_ID, "blocks": blocks}, timeout=30)
        data = r.json()
        if data.get("ok"):
            print(f"  Posted via Bot API (ts: {data.get('ts')})")
            return data.get("ts")
        else:
            print(f"  Bot API error: {data.get('error')} — falling back to webhook")
    if SLACK_WEBHOOK_URL:
        r = requests.post(SLACK_WEBHOOK_URL, json={"blocks": blocks}, timeout=30)
        if r.status_code == 200:
            print(f"  Posted via webhook")
    return None

def slack_delete(ts):
    """Delete a Slack message by timestamp."""
    if not SLACK_BOT_TOKEN or not SLACK_CHANNEL_ID or not ts:
        print("Cannot delete: need SLACK_BOT_TOKEN, SLACK_CHANNEL_ID, and message ts")
        return False
    r = requests.post("https://slack.com/api/chat.delete",
        headers={"Authorization": f"Bearer {SLACK_BOT_TOKEN}", "Content-Type": "application/json"},
        json={"channel": SLACK_CHANNEL_ID, "ts": ts}, timeout=30)
    data = r.json()
    if data.get("ok"):
        print(f"  Deleted message {ts}")
        return True
    print(f"  Delete failed: {data.get('error')}")
    return False

def slack_get_recent_bot_messages(limit=10):
    """Get recent messages from the bot in the channel."""
    if not SLACK_BOT_TOKEN or not SLACK_CHANNEL_ID:
        return []
    r = requests.post("https://slack.com/api/conversations.history",
        headers={"Authorization": f"Bearer {SLACK_BOT_TOKEN}", "Content-Type": "application/json"},
        json={"channel": SLACK_CHANNEL_ID, "limit": limit}, timeout=30)
    data = r.json()
    if not data.get("ok"):
        print(f"  Could not fetch history: {data.get('error')}")
        return []
    # Filter to bot messages only
    return [m for m in data.get("messages", []) if m.get("bot_id") or m.get("subtype") == "bot_message"]

def post_to_slack(startups):
    """Post clean, easy-to-read startup cards to Slack."""
    today = datetime.now().strftime("%B %d, %Y")

    if not startups:
        blocks = [
            {"type": "header", "text": {"type": "plain_text", "text": f"Daily Deal Flow — {today}"}},
            {"type": "section", "text": {"type": "mrkdwn", "text": "Nothing new today. Scanning again tomorrow."}},
        ]
        slack_post(blocks)
        return

    blocks = [
        {"type": "header", "text": {"type": "plain_text", "text": f"Daily Deal Flow — {today}"}},
        {"type": "context", "elements": [{"type": "mrkdwn", "text": f"{len(startups)} new companies likely raising — not in Affinity or Lemlist"}]},
    ]

    for s in startups[:5]:
        funding = s.get("funding_total", 0)
        funding_str = f"${funding:,.0f}" if funding else "Bootstrapped"
        stage = (s.get("stage") or "").replace("_", " ").title()
        city = s.get("city", "")
        # Cut at end of sentence, not mid-word
        full_desc = s.get("description") or ""
        if len(full_desc) > 200:
            # Find last period before 200 chars
            cut = full_desc[:200].rfind(".")
            if cut > 80:
                desc = full_desc[:cut+1]
            else:
                # Find last space
                cut = full_desc[:200].rfind(" ")
                desc = full_desc[:cut] + "..." if cut > 0 else full_desc[:200] + "..."
        else:
            desc = full_desc

        why = build_why_interesting(s)

        # Founder one-liner
        founder_line = f"*{s.get('ceo_name', '')}*"
        bg_parts = []
        if s.get("founder_prev"):
            # Just the top 2 most recent roles
            roles = [r.strip() for r in s["founder_prev"].split(";")][:2]
            bg_parts.extend(roles)
        if s.get("founder_education"):
            schools = [e.strip() for e in s["founder_education"].split(",")][:1]
            bg_parts.extend(schools)
        if bg_parts:
            founder_line += f"  ·  {' → '.join(bg_parts)}"

        text = (
            f"*<{s.get('website', '')}|{s['name']}>*\n"
            f"{desc}\n\n"
            f"{founder_line}\n\n"
        )

        if why:
            text += f"*Why interesting:* {why}\n"

        text += f"{city}  ·  {stage}  ·  {funding_str}  ·  Score {s['combined_score']:.0f}"

        blocks.append({"type": "divider"})
        blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": text}})

    if len(startups) > 5:
        blocks.append({"type": "divider"})
        blocks.append({"type": "context", "elements": [{"type": "mrkdwn", "text": f"+ {len(startups) - 5} more in the spreadsheet"}]})

    blocks.append({"type": "divider"})
    blocks.append({
        "type": "actions",
        "elements": [{
            "type": "button",
            "text": {"type": "plain_text", "text": "Open Spreadsheet"},
            "url": f"https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}",
        }]
    })

    ts = slack_post(blocks)
    if ts:
        print(f"Posted {len(startups)} startups to Slack (ts: {ts})")
    else:
        print(f"Posted {len(startups)} startups to Slack")

# ---------- Main ----------

def main():
    print(f"{'='*60}")
    print(f"Daily Startup Scanner — {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"{'='*60}\n")

    # Load existing companies for dedup
    print("[1/5] Loading existing companies...", flush=True)
    existing = load_existing_companies()
    print(f"  {len(existing)} companies to skip")

    # Search Harmonic for recently emerged startups
    print("\n[2/5] Searching Harmonic for recently emerged startups...", flush=True)
    urns = search_recent_companies()
    print(f"  Found {len(urns)} recently emerged companies")

    if not urns:
        print("No new companies found. Posting empty update to Slack.")
        post_to_slack([])
        return

    # Fetch details
    print("\n[3/5] Fetching company details...", flush=True)
    companies = batch_get_companies(urns)
    print(f"  Got {len(companies)} records")

    # Filter and score
    print("\n[4/5] Filtering, scoring, and deduping...", flush=True)
    seen = set()
    candidates = []

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

        # B2B signals
        tag_texts = [(t.get("display_value") or "").lower() for t in tags]
        desc = (c.get("description") or "").lower()
        ct = (c.get("customer_type") or "").lower()
        b2b_kw = ["saas", "enterprise", "b2b", "infrastructure", "devtools", "api", "platform", "fintech", "software", "cloud", "analytics", "automation", "data", "cybersecurity", "ai"]
        if not ("b2b" in ct or any(any(w in t for w in b2b_kw) for t in tag_texts) or any(w in desc for w in ["b2b", "saas", "enterprise", "businesses", "platform for"])):
            continue

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
        candidates.append((raise_score, c))

    candidates.sort(key=lambda x: -x[0])
    print(f"  {len(candidates)} candidates after filtering")

    # Get CEO info and founder scores
    print("\n[5/5] Getting CEO info and checking Affinity...", flush=True)
    top = candidates[:200]

    # Batch get persons
    all_person_urns = []
    company_people = {}
    for _, c in top:
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

    # Build final list
    results = []
    for raise_score, c in top:
        name = c.get("name") or ""
        urn = c.get("entity_urn") or ""
        cands = company_people.get(urn, [])

        ceo_name = ""
        email = ""
        chosen = None
        ceo_title = ""
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
                ceo_title = cd["title"]
                break

        if not email: continue

        # Check Affinity
        website = c.get("website") or {}
        domain = ""
        if isinstance(website, dict):
            url = website.get("url") or ""
            domain = url.replace("https://", "").replace("http://", "").replace("www.", "").split("/")[0] if url else ""
        web_url = (website.get("url") or "") if isinstance(website, dict) else ""

        if check_affinity(name, domain):
            continue

        founder_score = compute_founder_score(chosen)
        combined = raise_score + founder_score
        if combined < MIN_SCORE_TO_POST:
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

        results.append({
            "name": name,
            "ceo_name": ceo_name,
            "ceo_title": ceo_title,
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

    results.sort(key=lambda x: -x["combined_score"])
    print(f"\n  {len(results)} startups ready to post (score >= {MIN_SCORE_TO_POST})")

    # Post to Slack
    post_to_slack(results)

    print(f"\n{'='*60}")
    print(f"DONE — {len(results)} startups posted to Slack")
    print(f"{'='*60}")

if __name__ == "__main__":
    main()
