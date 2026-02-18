import os
import csv
import requests
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("HARMONIC_API_KEY")
HEADERS = {"apikey": API_KEY, "Content-Type": "application/json"}
BASE = "https://api.harmonic.ai"

def get_company_details(company_urn):
    """Get full company details including contact info."""
    company_id = company_urn.split(":")[-1]
    r = requests.get(f"{BASE}/companies/{company_id}", headers=HEADERS, timeout=60)
    if r.status_code == 200:
        return r.json()
    return {}

def get_person_details(person_urn):
    """Get person details including email."""
    person_id = person_urn.split(":")[-1]
    r = requests.get(f"{BASE}/persons/{person_id}", headers=HEADERS, timeout=60)
    if r.status_code == 200:
        return r.json()
    return {}

def main():
    # Read CSV and find companies without emails
    companies_without_email = []
    with open("lemlist_leads.csv", "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if not row.get("email"):
                companies_without_email.append(row)
    
    print(f"Found {len(companies_without_email)} companies without emails\n")
    print("=" * 80)
    
    results = []
    
    for row in companies_without_email:
        company_name = row.get("companyName", "")
        company_urn = row.get("companyUrn", "")
        ceo_name = row.get("ceoName", "")
        
        print(f"\n{company_name}")
        print(f"  CEO: {ceo_name}")
        
        # Get full company details
        company = get_company_details(company_urn)
        
        # Check for company contact email
        contact_data = company.get("contact", {})
        if isinstance(contact_data, dict):
            contact_email = contact_data.get("email") or ""
        else:
            contact_email = ""
        if not contact_email:
            contact_email = company.get("email") or ""
        if isinstance(contact_email, dict):
            contact_email = contact_email.get("email") or ""
            
        website = company.get("website") or ""
        if isinstance(website, dict):
            website = website.get("url") or ""
        domain = ""
        if website and isinstance(website, str):
            domain = website.replace("https://", "").replace("http://", "").replace("www.", "").split("/")[0]
        
        print(f"  Website: {website}")
        print(f"  Domain: {domain}")
        print(f"  Contact email: {contact_email}")
        
        # Get all people with emails
        people = company.get("people", [])
        people_with_emails = []
        
        for p in people:
            if isinstance(p, dict):
                person_urn = p.get("person") or p.get("person_urn") or ""
                title = p.get("title") or ""
                is_current = p.get("is_current_position", False)
                
                if person_urn and is_current:
                    # Get detailed person info
                    person_data = get_person_details(person_urn)
                    emails = person_data.get("emails") or []
                    name = person_data.get("full_name") or person_data.get("name") or ""
                    
                    if emails:
                        for e in emails:
                            email_addr = e.get("email") or e if isinstance(e, str) else ""
                            if email_addr:
                                people_with_emails.append({
                                    "name": name,
                                    "title": title,
                                    "email": email_addr
                                })
        
        if people_with_emails:
            print(f"  People with emails:")
            for pe in people_with_emails:
                print(f"    - {pe['name']} ({pe['title']}): {pe['email']}")
        else:
            print(f"  No people with emails found")
        
        # Determine best email
        best_email = contact_email
        best_name = ceo_name
        
        # Try to find CEO/founder with email
        for pe in people_with_emails:
            title_lower = pe['title'].lower()
            if 'ceo' in title_lower or 'founder' in title_lower or 'chief executive' in title_lower:
                best_email = pe['email']
                best_name = pe['name']
                break
        
        # If no CEO email, use first person with email
        if not best_email and people_with_emails:
            best_email = people_with_emails[0]['email']
            best_name = people_with_emails[0]['name']
        
        results.append({
            "company": company_name,
            "domain": domain,
            "ceo_name": ceo_name,
            "best_email": best_email,
            "best_name": best_name,
            "all_emails": [pe['email'] for pe in people_with_emails]
        })
    
    print("\n" + "=" * 80)
    print("\nSUMMARY - Companies without emails:")
    print("=" * 80)
    
    for r in results:
        status = "✓" if r['best_email'] else "✗"
        print(f"{status} {r['company']} ({r['domain']})")
        if r['best_email']:
            print(f"   → {r['best_name']}: {r['best_email']}")
        else:
            print(f"   → CEO: {r['ceo_name']} - NO EMAIL FOUND")
        if r['all_emails']:
            print(f"   All emails: {', '.join(r['all_emails'])}")
    
    # Count
    found = sum(1 for r in results if r['best_email'])
    print(f"\n{'=' * 80}")
    print(f"Found emails for {found}/{len(results)} companies")
    print(f"Still missing: {len(results) - found}")

if __name__ == "__main__":
    main()

