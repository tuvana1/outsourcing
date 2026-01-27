import os
import requests
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.environ["HARMONIC_API_KEY"]
WATCHLIST_URN = os.environ["WATCHLIST_URN"]

BASE = "https://api.harmonic.ai"
HEADERS = {
    "apikey": API_KEY,
    "Content-Type": "application/json",
}

# ---------- helpers ----------

def chunked(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i+n]

def first_name(full_name: str) -> str:
    if not full_name:
        return ""
    return full_name.strip().split()[0]

def clean_company_name(name: str) -> str:
    """Remove parenthetical suffixes like (YC W24) from company names."""
    if not name:
        return ""
    # Remove anything in parentheses at the end
    import re
    return re.sub(r'\s*\([^)]*\)\s*$', '', name).strip()

def pick_fallback_email(company: dict) -> str:
    contact = company.get("contact") or {}
    primary = (contact.get("primary_email") or "").strip()
    if primary:
        return primary

    exec_emails = contact.get("exec_emails") or []
    exec_emails = [e.strip() for e in exec_emails if e and e.strip()]
    # de-dupe preserve order
    seen = set()
    exec_emails = [e for e in exec_emails if not (e in seen or seen.add(e))]

    prefs = ["ceo@", "founder@", "hello@", "team@", "info@", "contact@"]
    lower = [e.lower() for e in exec_emails]
    for p in prefs:
        for idx, e in enumerate(lower):
            if e.startswith(p):
                return exec_emails[idx]
    return exec_emails[0] if exec_emails else ""

def is_ceo_title(title: str) -> bool:
    if not title:
        return False
    t = title.lower()
    return "ceo" in t or "chief executive" in t

# ---------- Harmonic calls ----------

def get_watchlist_entries(watchlist_urn: str, size: int = 1000):
    """Fetch company URNs from watchlist."""
    url = f"{BASE}/watchlists/companies/{watchlist_urn}/entries"
    params = {"size": size, "page": 0}
    all_entries = []

    while True:
        r = requests.get(url, headers=HEADERS, params=params, timeout=60)
        r.raise_for_status()
        data = r.json()

        if "edges" in data:
            entries = []
            for edge in data["edges"]:
                node = edge.get("node") or {}
                company = node.get("company") or {}
                entries.append({"company_urn": company.get("entity_urn")})
        else:
            entries = data.get("entries") or []

        all_entries.extend(entries)

        page_info = data.get("page_info") or {}
        has_next = page_info.get("has_next")
        next_cursor = page_info.get("next")

        if not has_next or not next_cursor:
            break
        params = {"size": size, "cursor": next_cursor}

    urns = []
    for e in all_entries:
        cu = e.get("company_urn") or e.get("companyUrn")
        if cu:
            urns.append(cu)
    return list(dict.fromkeys(urns))

def companies_batch_get(company_urns):
    """Batch fetch company details including people."""
    url = f"{BASE}/companies/batchGet"
    out = []
    for chunk in chunked(company_urns, 50):
        r = requests.post(url, headers=HEADERS, json={"urns": chunk}, timeout=60)
        r.raise_for_status()
        data = r.json()
        if isinstance(data, list):
            out.extend(data)
        else:
            out.extend(data.get("results") or data.get("companies") or [])
    return out

def persons_batch_get(person_urns):
    """Batch fetch person details for names and emails."""
    url = f"{BASE}/persons/batchGet"
    out = {}
    for chunk in chunked(person_urns, 50):
        r = requests.post(url, headers=HEADERS, json={"urns": chunk}, timeout=60)
        r.raise_for_status()
        data = r.json()
        people = data if isinstance(data, list) else (data.get("results") or data.get("people") or [])
        for p in people:
            urn = p.get("entity_urn") or p.get("person_urn") or p.get("urn")
            if urn:
                out[urn] = p
    return out

# ---------- main pipeline ----------

def get_person_email(person_data: dict) -> str:
    """Extract email from person data."""
    contact = person_data.get("contact") or {}
    email = (contact.get("primary_email") or "").strip()
    if not email:
        emails = contact.get("emails") or []
        if emails and isinstance(emails, list) and emails[0]:
            email = (emails[0] if isinstance(emails[0], str) else "").strip()
    return email

def main():
    # 1) Get company URNs from watchlist
    company_urns = get_watchlist_entries(WATCHLIST_URN)
    print(f"Found {len(company_urns)} company URNs in watchlist")

    # 2) Batch fetch company details (includes people with titles)
    companies = companies_batch_get(company_urns)
    print(f"Fetched {len(companies)} company records")

    # Map by URN
    company_by_urn = {}
    for c in companies:
        urn = c.get("entity_urn") or c.get("company_urn")
        if urn:
            company_by_urn[urn] = c

    # 3) Collect ALL potential person URNs (CEOs + founders) to batch fetch
    all_person_urns = []
    company_candidates = {}  # company_urn -> list of candidate person URNs in priority order

    for urn in company_urns:
        company = company_by_urn.get(urn, {})
        people = company.get("people") or []
        
        candidates = []
        
        # Priority 1: Current CEOs
        for person in people:
            if not isinstance(person, dict):
                continue
            is_current = person.get("is_current_position", False)
            if not is_current:
                continue
            title = person.get("title") or ""
            if is_ceo_title(title):
                person_urn = person.get("person") or person.get("person_urn") or person.get("entity_urn") or ""
                if person_urn:
                    candidates.append({"urn": person_urn, "priority": 1})
                    all_person_urns.append(person_urn)
        
        # Priority 2: Current Founders
        for person in people:
            if not isinstance(person, dict):
                continue
            is_current = person.get("is_current_position", False)
            if not is_current:
                continue
            title = (person.get("title") or "").lower()
            role_type = (person.get("role_type") or "").upper()
            if "founder" in title or role_type == "FOUNDER":
                person_urn = person.get("person") or person.get("person_urn") or person.get("entity_urn") or ""
                if person_urn and not any(c["urn"] == person_urn for c in candidates):
                    candidates.append({"urn": person_urn, "priority": 2})
                    all_person_urns.append(person_urn)
        
        company_candidates[urn] = candidates

    all_person_urns = list(dict.fromkeys([u for u in all_person_urns if u]))
    print(f"Found {len(all_person_urns)} total person URNs to fetch")

    # 4) Batch fetch ALL person records
    people_by_urn = persons_batch_get(all_person_urns) if all_person_urns else {}
    print(f"Fetched {len(people_by_urn)} person records")

    # 5) Build final output - pick the best person WITH email for each company
    final = []
    for urn in company_urns:
        company = company_by_urn.get(urn, {})
        company_name = company.get("name") or ""
        fallback_email = pick_fallback_email(company)
        
        candidates = company_candidates.get(urn, [])
        
        # Find the first candidate who has an email
        chosen_person = None
        chosen_email = ""
        
        for candidate in candidates:
            person_data = people_by_urn.get(candidate["urn"], {})
            email = get_person_email(person_data)
            if email:
                chosen_person = person_data
                chosen_email = email
                break
        
        # If no candidate has email, use first candidate's name + fallback email
        if not chosen_person and candidates:
            chosen_person = people_by_urn.get(candidates[0]["urn"], {})
            chosen_email = fallback_email
        
        # Extract name
        ceo_name = ""
        first = ""
        if chosen_person:
            ceo_name = chosen_person.get("full_name") or chosen_person.get("name") or ""
            first = first_name(ceo_name)
        
        # Final email
        email = chosen_email or fallback_email

        final.append({
            "companyName": clean_company_name(company_name),
            "firstName": first,
            "email": email,
            "companyUrn": urn,
            "ceoName": ceo_name,
        })

    df = pd.DataFrame(final)
    df.to_csv("lemlist_leads.csv", index=False)
    print(f"Wrote lemlist_leads.csv with {len(final)} rows")

if __name__ == "__main__":
    main()
