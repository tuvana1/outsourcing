import os
import re
import time
from datetime import datetime
import requests
import gspread
from google.oauth2.service_account import Credentials

# ---------- Configuration ----------

AFFINITY_API_KEY = "WaSDWfsLyh824_yFqIPofl17mzaNcV1V2nCIZqHdA8Y"
SPREADSHEET_ID = "1sdbE6V-qVNuKo9LKe42i8x9Nj6ENX8N5oeZsSjRwT9o"
TARGET_LIST_NAME = "1a Sourcing List"
TARGET_LIST_ID = 21233  # Pre-fetched for speed

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
        
        # Search using the domain as term
        data = self._get("/organizations", {"term": domain, "page_size": 10})
        orgs = data.get("organizations", []) if isinstance(data, dict) else data
        
        if not orgs:
            return None
        
        # ONLY return if exact domain match
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
        
        # ONLY return if normalized name matches exactly
        norm_search = normalize_name(name)
        for org in orgs:
            org_name = org.get("name") or ""
            if normalize_name(org_name) == norm_search:
                return org
        
        # NO fallback - don't return wrong company
        return None
    
    def is_org_in_list(self, org_id: int, list_id: int) -> bool:
        """Check if organization is in a specific list."""
        # Get list entries for this org
        data = self._get(f"/organizations/{org_id}/list-entries")
        entries = data if isinstance(data, list) else data.get("list_entries", [])
        
        for entry in entries:
            if entry.get("list_id") == list_id:
                return True
        
        return False
    
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
    print("Affinity Check - Updating existing spreadsheet")
    print("=" * 60)
    
    # Connect to Affinity
    affinity = AffinityClient(AFFINITY_API_KEY)
    
    # Connect to Google Sheet
    print("\nConnecting to Google Sheet...")
    sheet = get_sheet()
    
    # Read all data
    all_data = sheet.get_all_values()
    if not all_data:
        print("ERROR: Spreadsheet is empty!")
        return
    
    headers = all_data[0]
    rows = all_data[1:]
    print(f"  Found {len(rows)} company rows")
    
    # Find existing column indices
    def get_col_idx(name):
        try:
            return headers.index(name)
        except ValueError:
            return -1
    
    company_col = get_col_idx("companyName")
    email_col = get_col_idx("email")
    
    if company_col == -1:
        print("ERROR: 'companyName' column not found!")
        return
    
    # Define new columns
    new_cols = ["In Affinity", "Affinity List Name", "Affinity Org ID", 
                "Contacted", "Contacted Evidence", "Last Checked"]
    
    # Find or create column indices
    col_indices = {}
    for col_name in new_cols:
        idx = get_col_idx(col_name)
        if idx == -1:
            headers.append(col_name)
            idx = len(headers) - 1
        col_indices[col_name] = idx
    
    # Update header row
    sheet.update('A1', [headers])
    
    # Process each row
    print(f"\nChecking {len(rows)} companies against Affinity '{TARGET_LIST_NAME}'...")
    updates = []
    
    for row_idx, row in enumerate(rows):
        row_num = row_idx + 2
        
        # Extend row if needed
        while len(row) < len(headers):
            row.append("")
        
        company_name = row[company_col] if company_col < len(row) else ""
        email = row[email_col] if email_col >= 0 and email_col < len(row) else ""
        domain = extract_domain(email)
        
        print(f"  [{row_num}] {company_name}...", end=" ", flush=True)
        
        # Search for org in Affinity
        org = None
        if domain:
            org = affinity.search_org_by_domain(domain)
        if not org and company_name:
            org = affinity.search_org_by_name(company_name)
        
        if org:
            org_id = org.get("id")
            
            # Check if in target list
            in_list = affinity.is_org_in_list(org_id, TARGET_LIST_ID)
            
            if in_list:
                row[col_indices["In Affinity"]] = "Yes"
                row[col_indices["Affinity List Name"]] = TARGET_LIST_NAME
                row[col_indices["Affinity Org ID"]] = str(org_id)
                row[col_indices["Contacted"]] = "Check list"
                row[col_indices["Contacted Evidence"]] = "See Responded? field in Affinity"
                print(f"✓ In '{TARGET_LIST_NAME}'")
            else:
                # Found in Affinity global DB but not on the target list
                row[col_indices["In Affinity"]] = "No"
                row[col_indices["Affinity List Name"]] = ""
                row[col_indices["Affinity Org ID"]] = str(org_id)
                row[col_indices["Contacted"]] = "Not tracked"
                row[col_indices["Contacted Evidence"]] = "Exists in Affinity, not on any list"
                print(f"⊘ In Affinity DB, not on list")
        else:
            # Not found in Affinity at all
            row[col_indices["In Affinity"]] = "No"
            row[col_indices["Affinity List Name"]] = ""
            row[col_indices["Affinity Org ID"]] = ""
            row[col_indices["Contacted"]] = "Unknown"
            row[col_indices["Contacted Evidence"]] = "Not found in Affinity"
            print("✗ Not found")
        
        row[col_indices["Last Checked"]] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        updates.append(row)
        time.sleep(0.2)  # Rate limiting
    
    # Write all updates
    print("\nWriting results to spreadsheet...")
    sheet.update('A2', updates)
    
    # Summary
    in_affinity = sum(1 for r in updates if r[col_indices["In Affinity"]] == "Yes")
    contacted = sum(1 for r in updates if r[col_indices["Contacted"]] == "Yes")
    
    print(f"\n{'=' * 60}")
    print(f"Done! Updated {len(updates)} rows")
    print(f"  In '{TARGET_LIST_NAME}': {in_affinity}/{len(updates)}")
    print(f"  Contacted: {contacted}/{len(updates)}")
    print(f"\nSpreadsheet: https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}")
    print(f"{'=' * 60}")

if __name__ == "__main__":
    main()
