import os
import gspread, re
from google.oauth2.service_account import Credentials

scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
creds = Credentials.from_service_account_file('credentials.json', scopes=scopes)
gc = gspread.authorize(creds)
sheet = gc.open_by_key(os.environ["SPREADSHEET_ID"]).sheet1

rows = sheet.get_all_values()
updates = []

for i, row in enumerate(rows[1:], 2):
    name = row[0] if len(row) > 0 else ""
    if not name.strip():
        continue
    clean = name
    # Remove (YC ...), (a16z ...), etc.
    clean = re.sub(r'\s*\([^)]*\)\s*', ' ', clean).strip()
    # Remove emojis and special unicode chars
    clean = clean.encode('ascii', 'ignore').decode('ascii').strip()
    # Remove trademark/registered symbols
    clean = clean.replace('\u2122', '').replace('\u00ae', '').strip()
    # Remove ", Inc." or ", Inc" at end
    clean = re.sub(r',\s*Inc\.?$', '', clean).strip()
    # Clean up extra spaces
    clean = re.sub(r'\s+', ' ', clean).strip()
    # Remove trailing periods or commas
    clean = clean.rstrip('.,').strip()

    if clean != name:
        updates.append((i, name, clean))
        print(f'  Row {i}: "{name}" -> "{clean}"')

if updates:
    for row_num, old, new in updates:
        sheet.update_cell(row_num, 1, new)
    print(f"\nUpdated {len(updates)} company names")
else:
    print("All company names are already clean")
