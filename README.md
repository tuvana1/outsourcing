# Palm Drive Capital — Deal Flow Automation

Internal tooling for startup sourcing, CRM enrichment, and outreach automation.

## Stack

- **Harmonic AI** — company search, watchlists, founder data
- **Affinity CRM** — org lookup, deduplication, activity notes
- **Google Sheets** — pipeline tracking via gspread + service account
- **Lemlist** — cold outreach campaigns
- **Slack** — deal flow alerts and bot commands
- **Granola MCP** — meeting notes sync to Affinity

## Setup

1. Copy `.env.example` to `.env` and fill in API keys
2. Add `credentials.json` (Google service account key)
3. Install dependencies: `pip install -r requirements.txt`
4. Run scripts: `python <script>.py`

## Environment Variables

| Variable | Description |
|---|---|
| `HARMONIC_API_KEY` | Harmonic AI API key |
| `AFFINITY_API_KEY` | Affinity CRM API key |
| `LEMLIST_API_KEY` | Lemlist API key |
| `SPREADSHEET_ID` | Google Sheets spreadsheet ID |
| `LEMLIST_CAMPAIGN_ID` | Lemlist campaign ID |
| `AFFINITY_LIST_ID` | Affinity list ID (1a Sourcing) |
| `WATCHLIST_URN` | Harmonic watchlist URN |
| `SLACK_WEBHOOK_URL` | Slack incoming webhook URL |
| `SLACK_BOT_TOKEN` | Slack bot OAuth token |
| `SLACK_CHANNEL_ID` | Slack channel for deal flow posts |

## Scripts

### Sourcing
| Script | Description |
|---|---|
| `find_top_startups.py` | Harmonic search → score → filter → Affinity dedupe → Sheets |
| `find_related_startups.py` | Find startups related to portfolio verticals → Slack |
| `find_raising_startups.py` | Find founders likely raising now |
| `find_raising_later.py` | Find companies likely raising in future rounds |
| `find_stanford_founders.py` | Stanford founder search pipeline |
| `find_stablecoin_yield_fintech.py` | Vertical-specific sourcing (stablecoin/yield fintech) |
| `find_emails.py` | Enrich companies with founder emails |
| `harmonic_ceos.py` | Watchlist → CEO emails → CSV |
| `browder_portfolio.py` | Scan a specific investor's portfolio |
| `check_w26.py` | Check YC W26 batch companies |

### CRM & Enrichment
| Script | Description |
|---|---|
| `affinity_deep_check.py` | Check companies against Affinity (lists, notes, activity) |
| `affinity_check.py` | Quick Affinity org lookup |
| `affinity_recheck.py` | Re-check previously flagged companies |
| `deep_affinity_analysis.py` | Full Affinity enrichment → Sheets |
| `add_to_affinity.py` | Add companies to Affinity sourcing list |
| `add_sheet5_to_affinity.py` | Add Sheet5 companies to Affinity |
| `dedup_sheet5.py` | Dedupe Sheet5 against Affinity |
| `cleanup_sheet5.py` | Remove non-fits from Sheet5 |
| `clean_names.py` | Normalize company names |

### Outreach & Comms
| Script | Description |
|---|---|
| `add_to_lemlist.py` | Push leads from Sheets → Lemlist campaign |
| `send_briefing.py` | Daily VC intel briefing → Slack |
| `send_briefing_mar15.py` | One-off briefing (March 15) |
| `slack_bot.py` | Slack bot for deal flow commands |
| `slack_daily_startups.py` | Daily startup scan → Slack |
| `party_invite_list.py` | Event invite list builder |
| `push_and_check.py` | Push to Sheets and verify |

### Other
| Script | Description |
|---|---|
| `create_architecture_slides.py` | Generate architecture deck |
| `index.html` | Pipeline visualization dashboard |
