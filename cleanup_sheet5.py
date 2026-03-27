#!/usr/bin/env python3
"""Clean up Sheet5 by removing companies that are not good fits for early-stage tech VC."""

import gspread

# Companies to remove (lowercased for case-insensitive matching)
REMOVE_COMPANIES = [
    "golftrk",
    "padelhub",
    "brightside legacy",
    "the welcome company",
    "cootoh",
    "loops ai",
    "workmatch",
    "cograder",
    "candle ai",
    "mark os",
    "cookie finance",
    "known",
    "hadron energy",
    "frontlands",
]

def main():
    gc = gspread.service_account(filename="credentials.json")
    sh = gc.open_by_key("1sdbE6V-qVNuKo9LKe42i8x9Nj6ENX8N5oeZsSjRwT9o")
    ws = sh.worksheet("Sheet5")

    all_rows = ws.get_all_values()
    print(f"Total rows (including header): {len(all_rows)}")

    # Find the company name column (look in header row)
    header = [h.strip().lower() for h in all_rows[0]]
    # Try common column names
    name_col = None
    for candidate in ["company name", "company", "name", "company_name"]:
        if candidate in header:
            name_col = header.index(candidate)
            break
    if name_col is None:
        # Fall back to first column
        print(f"Header: {all_rows[0]}")
        print("Could not find company name column, using column 0")
        name_col = 0

    print(f"Using column {name_col} ('{all_rows[0][name_col]}') for company names")

    # Find rows to delete (collect 1-based row indices)
    rows_to_delete = []
    removed_names = []
    for i, row in enumerate(all_rows):
        if i == 0:  # skip header
            continue
        cell_value = row[name_col].strip().lower() if name_col < len(row) else ""
        for target in REMOVE_COMPANIES:
            if target in cell_value or cell_value in target and cell_value:
                rows_to_delete.append(i + 1)  # 1-based for gspread
                removed_names.append(row[name_col].strip())
                break

    print(f"\nFound {len(rows_to_delete)} rows to remove:")
    for name in removed_names:
        print(f"  - {name}")

    # Check which target companies were NOT found
    found_lower = [n.lower() for n in removed_names]
    for target in REMOVE_COMPANIES:
        if not any(target in f or f in target for f in found_lower):
            print(f"  WARNING: '{target}' not found in sheet")

    # Delete rows from bottom to top to avoid index shifting
    rows_to_delete.sort(reverse=True)
    for row_idx in rows_to_delete:
        ws.delete_rows(row_idx)

    # Final count
    remaining = ws.get_all_values()
    print(f"\nRemoved {len(removed_names)} companies.")
    print(f"Remaining rows (including header): {len(remaining)}")
    print(f"Remaining companies: {len(remaining) - 1}")

if __name__ == "__main__":
    main()
