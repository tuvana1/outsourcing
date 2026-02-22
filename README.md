# Outsourcing

Internal tooling for sourcing and outreach automation.

## Setup

1. Copy `.env.example` to `.env` and fill in API keys
2. Add `credentials.json` (Google service account)
3. Run scripts with: `export $(cat .env | xargs) && python <script>.py`

## Environment Variables

| Variable | Description |
|---|---|
| `HARMONIC_API_KEY` | Harmonic AI API key |
| `AFFINITY_API_KEY` | Affinity CRM API key |
| `LEMLIST_API_KEY` | Lemlist API key |
| `SPREADSHEET_ID` | Google Sheets spreadsheet ID |
| `LEMLIST_CAMPAIGN_ID` | Lemlist campaign ID |
| `AFFINITY_LIST_ID` | Affinity list ID |
| `WATCHLIST_URN` | Harmonic watchlist URN |
