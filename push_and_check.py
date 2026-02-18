import os
import re
import time
import csv
from datetime import datetime
import requests
import gspread
from google.oauth2.service_account import Credentials

# ---------- Configuration ----------

AFFINITY_API_KEY = os.environ["AFFINITY_API_KEY"]
SPREADSHEET_ID = "1sdbE6V-qVNuKo9LKe42i8x9Nj6ENX8N5oeZsSjRwT9o"
TARGET_LIST_NAME = "1a Sourcing List"
TARGET_LIST_ID = 21233

AFFINITY_BASE = "https://api.affinity.co"

# ---------- Helpers ----------

def extract_domain(email: str) -> str:
    """Extract domain from email address."""
    if not email or "@" not in email:
        return ""
    return email.split("@")[-1].lower().strip()

def normalize_name(name: str) -> str:
    """Normalize company name for matching."""
    if not name:
        return ""
    name = name.lower().strip()
    name = re.sub(r'\s+(inc|llc|ltd|corp|co|company|inc\.|llc\.|ltd\.)\.?$', '', name, flags=re.IGNORECASE)
    name = re.sub(r'[^a-z0-9\s]', '', name)
    name = re.sub(r'\s+', ' ', name).strip()
    return name

# ---------- Affinity API ----------

class AffinityClient:
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.session = requests.Session()
        self.session.auth = ('', api_key)
        self.session.headers.update({'Content-Type': 'application/json'})
        
    def _get(self, endpoint: str, params: dict = None) -> dict:
        """Make GET request with rate limiting."""
        url = f"{AFFINITY_BASE}{endpoint}"
        r = self.session.get(url, params=params, timeout=60)
        
        if r.status_code == 429:
            retry_after = int(r.headers.get('Retry-After', 5))
            print(f"  Rate limited, waiting {retry_after}s...")
            time.sleep(retry_after)
            return self._get(endpoint, params)
        
        if r.status_code != 200:
            return {}
        
        return r.json()
    
    def search_org_by_domain(self, domain: str) -> dict:
        """Search for organization by EXACT domain match."""
        if not domain:
            return None
        
        domain = domain.lower().strip()
        data = self._get("/organizations", {"term": domain, "page_size": 10})
        orgs = data.get("organizations", []) if isinstance(data, dict) else data
        
        if not orgs:
            return None
        
        for org in orgs:
            org_domain = (org.get("domain") or "").lower().strip()
            if org_domain == domain:
                return org
        
        return None
    
    def search_org_by_name(self, name: str) -> dict:
        """Search for organization by name - strict matching only."""
        if not name:
            return None
        
        data = self._get("/organizations", {"term": name, "page_size": 10})
        orgs = data.get("organizations", []) if isinstance(data, dict) else data
        
        if not orgs:
            return None
        
        norm_search = normalize_name(name)
        for org in orgs:
            org_name = org.get("name") or ""
            if normalize_name(org_name) == norm_search:
                return org
        
        return None
    
    def is_org_in_list(self, org_id: int, list_id: int) -> bool:
        """Check if organization is in a specific list."""
        data = self._get(f"/organizations/{org_id}/list-entries")
        entries = data if isinstance(data, list) else data.get("list_entries", [])
        
        for entry in entries:
            if entry.get("list_id") == list_id:
                return True
        
        return False
    
    def get_org_list_entry(self, org_id: int, list_id: int) -> dict:
        """Get the list entry for an org on a specific list."""
        data = self._get(f"/organizations/{org_id}/list-entries")
        entries = data if isinstance(data, list) else data.get("list_entries", [])
        
        for entry in entries:
            if entry.get("list_id") == list_id:
                return entry
        
        return None
    
    def get_field_values(self, list_entry_id: int) -> dict:
        """Get field values for a list entry."""
        data = self._get(f"/field-values", {"list_entry_id": list_entry_id})
        return data if isinstance(data, list) else data.get("field_values", [])
    
    def get_org_interactions(self, org_id: int) -> list:
        """Get interactions for an organization."""
        data = self._get("/interactions", {"organization_id": org_id, "page_size": 50})
        return data.get("interactions", []) if isinstance(data, dict) else data

# ---------- Google Sheets ----------

def get_sheet():
    """Connect to Google Sheet."""
    scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
    creds = Credentials.from_service_account_file('credentials.json', scopes=scopes)
    gc = gspread.authorize(creds)
    return gc.open_by_key(SPREADSHEET_ID).sheet1

# ---------- Main ----------

def main():
    print("=" * 60)
    print("Push to Spreadsheet and Check Affinity")
    print("=" * 60)
    
    # Read CSV
    print("\nReading lemlist_leads.csv...")
    rows = []
    with open("lemlist_leads.csv", "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(row)
    print(f"  Found {len(rows)} companies")
    
    # Connect to Affinity
    affinity = AffinityClient(AFFINITY_API_KEY)
    
    # Connect to Google Sheet
    print("\nConnecting to Google Sheet...")
    sheet = get_sheet()
    
    # Clear existing data and set headers
    sheet.clear()
    
    headers = [
        "companyName", "firstName", "email", "ceoName", "domain",
        "On 1a Sourcing List", "Affinity Status", "Contacted?", "Responded?",
        "Comments", "Last Checked"
    ]
    
    # Process each company
    print(f"\nProcessing {len(rows)} companies...")
    output_rows = [headers]
    
    for idx, row in enumerate(rows):
        company_name = row.get("companyName", "")
        first_name = row.get("firstName", "")
        email = row.get("email", "")
        ceo_name = row.get("ceoName", "")
        domain = extract_domain(email)
        
        print(f"  [{idx+1}/{len(rows)}] {company_name}...", end=" ", flush=True)
        
        # Default values
        on_list = "No"
        affinity_status = ""
        contacted = ""
        responded = ""
        comments = "No interactions recorded"
        
        # Search for org in Affinity
        org = None
        if domain:
            org = affinity.search_org_by_domain(domain)
        if not org and company_name:
            org = affinity.search_org_by_name(company_name)
        
        if org:
            org_id = org.get("id")
            
            # Check if in target list
            list_entry = affinity.get_org_list_entry(org_id, TARGET_LIST_ID)
            
            if list_entry:
                on_list = "Yes"
                entry_id = list_entry.get("id")
                
                # Get field values
                field_values = affinity.get_field_values(entry_id)
                for fv in field_values:
                    field_name = fv.get("field", {}).get("name", "") if isinstance(fv.get("field"), dict) else ""
                    value = fv.get("value")
                    
                    if "status" in field_name.lower():
                        affinity_status = str(value) if value else ""
                    elif "contacted" in field_name.lower() and "?" in field_name:
                        contacted = "Yes" if value else "No"
                    elif "responded" in field_name.lower():
                        responded = "Yes" if value else "No"
                
                # Get interactions
                interactions = affinity.get_org_interactions(org_id)
                if interactions:
                    interaction_list = []
                    for i in interactions[:5]:  # Last 5
                        i_type = i.get("type", "")
                        i_date = i.get("date", "")[:10] if i.get("date") else ""
                        i_subj = (i.get("subject") or "")[:50]
                        interaction_list.append(f"{i_date} {i_type}: {i_subj}")
                    comments = " | ".join(interaction_list) if interaction_list else "No interactions recorded"
                
                print(f"✓ In '{TARGET_LIST_NAME}'")
            else:
                print("⊘ Not on list")
        else:
            print("✗ Not found")
        
        last_checked = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        output_rows.append([
            company_name, first_name, email, ceo_name, domain,
            on_list, affinity_status, contacted, responded,
            comments, last_checked
        ])
        
        time.sleep(0.2)  # Rate limiting
    
    # Write to spreadsheet
    print("\nWriting to spreadsheet...")
    sheet.update('A1', output_rows)
    
    # Summary
    on_list_count = sum(1 for r in output_rows[1:] if r[5] == "Yes")
    
    print(f"\n{'=' * 60}")
    print(f"Done! Wrote {len(output_rows)-1} companies to spreadsheet")
    print(f"  On '{TARGET_LIST_NAME}': {on_list_count}/{len(output_rows)-1}")
    print(f"\nSpreadsheet: https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}")
    print(f"{'=' * 60}")

if __name__ == "__main__":
    main()




