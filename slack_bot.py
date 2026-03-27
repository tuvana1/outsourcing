#!/usr/bin/env python3
"""Palm Drive Capital Slack Bot — listens in channel and responds to commands."""

import os, re, time, json, requests
from datetime import datetime
from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler

# Load env
from dotenv import load_dotenv
load_dotenv()

SLACK_BOT_TOKEN = os.environ["SLACK_BOT_TOKEN"]
SLACK_APP_TOKEN = os.environ["SLACK_APP_TOKEN"]
SLACK_CHANNEL_ID = os.environ.get("SLACK_CHANNEL_ID", "")
HARMONIC_API_KEY = os.environ["HARMONIC_API_KEY"]
AFFINITY_API_KEY = os.environ["AFFINITY_API_KEY"]
SPREADSHEET_ID = os.environ["SPREADSHEET_ID"]

HARMONIC_BASE = "https://api.harmonic.ai"
HARMONIC_HEADERS = {"apikey": HARMONIC_API_KEY, "Content-Type": "application/json"}
AFFINITY_BASE = "https://api.affinity.co"

app = App(token=SLACK_BOT_TOKEN)

# Track messages we've sent (for deletion)
sent_messages = []

# ---------- Helpers ----------

def post(channel, text=None, blocks=None):
    """Post a message and track its timestamp."""
    kwargs = {"channel": channel}
    if blocks:
        kwargs["blocks"] = blocks
    if text:
        kwargs["text"] = text
    elif blocks:
        kwargs["text"] = "Deal flow update"
    r = app.client.chat_postMessage(**kwargs)
    if r.get("ok"):
        sent_messages.append({"ts": r["ts"], "channel": channel})
    return r

def delete_message(channel, ts):
    """Delete a message by timestamp."""
    r = app.client.chat_delete(channel=channel, ts=ts)
    return r.get("ok", False)

def affinity_search(name, domain=None):
    """Search Affinity for an org."""
    session = requests.Session()
    session.auth = ('', AFFINITY_API_KEY)
    results = []

    for term in [domain, name]:
        if not term:
            continue
        r = session.get(f"{AFFINITY_BASE}/organizations", params={"term": term, "page_size": 5}, timeout=60)
        if r.status_code == 200:
            data = r.json()
            orgs = data.get("organizations", []) if isinstance(data, dict) else data
            for o in (orgs or []):
                if o.get("id") not in [x.get("id") for x in results]:
                    results.append(o)
    return results

def affinity_org_detail(org_id):
    """Get full org detail from Affinity."""
    session = requests.Session()
    session.auth = ('', AFFINITY_API_KEY)
    r = session.get(f"{AFFINITY_BASE}/organizations/{org_id}", timeout=60)
    if r.status_code == 200:
        return r.json()
    return None

def affinity_notes(org_id):
    """Get notes for an org."""
    session = requests.Session()
    session.auth = ('', AFFINITY_API_KEY)
    r = session.get(f"{AFFINITY_BASE}/notes", params={"organization_id": org_id, "page_size": 10}, timeout=60)
    if r.status_code == 200:
        data = r.json()
        return data if isinstance(data, list) else data.get("notes", [])
    return []

def harmonic_search_company(query, limit=5):
    """Quick Harmonic company search."""
    r = requests.post(f"{HARMONIC_BASE}/search/companies", headers=HARMONIC_HEADERS, json={
        "query": {
            "filter_group": {
                "join_operator": "and",
                "filters": [
                    {"field": "company_name", "comparator": "contains", "filter_value": query},
                ],
            },
            "pagination": {"page_size": limit, "start": 0}
        },
        "sort": {"field": "relevance_score", "descending": True}
    }, timeout=60)
    if r.status_code == 200:
        urns = r.json().get("results", [])
        if urns:
            r2 = requests.post(f"{HARMONIC_BASE}/companies/batchGet", headers=HARMONIC_HEADERS,
                               json={"urns": urns[:limit]}, timeout=60)
            if r2.status_code == 200:
                data = r2.json()
                return data if isinstance(data, list) else data.get("results", [])
    return []

# ---------- Command Handlers ----------

def handle_delete_last(say, channel):
    """Delete the last bot message."""
    # Try tracked messages first
    for msg in reversed(sent_messages):
        if msg["channel"] == channel:
            if delete_message(channel, msg["ts"]):
                sent_messages.remove(msg)
                say("Done, deleted the last message.")
                return
    # Fallback: search channel history
    r = app.client.conversations_history(channel=channel, limit=20)
    if r.get("ok"):
        bot_user = app.client.auth_test().get("user_id")
        for m in r["messages"]:
            if m.get("user") == bot_user or m.get("bot_id"):
                if delete_message(channel, m["ts"]):
                    say("Done, deleted the last message.")
                    return
    say("Couldn't find a message to delete.")

def handle_delete_n(say, channel, n):
    """Delete last N bot messages."""
    r = app.client.conversations_history(channel=channel, limit=50)
    deleted = 0
    if r.get("ok"):
        bot_user = app.client.auth_test().get("user_id")
        for m in r["messages"]:
            if deleted >= n:
                break
            if m.get("user") == bot_user or m.get("bot_id"):
                if delete_message(channel, m["ts"]):
                    deleted += 1
                    time.sleep(0.3)
    say(f"Deleted {deleted} message{'s' if deleted != 1 else ''}.")

def handle_check_affinity(say, company_name):
    """Check a company in Affinity."""
    say(f"Checking Affinity for *{company_name}*...")
    orgs = affinity_search(company_name)
    if not orgs:
        say(f"*{company_name}* is not in Affinity — completely new.")
        return

    for org in orgs[:3]:
        org_id = org.get("id")
        name = org.get("name", "")
        domain = org.get("domain", "")
        detail = affinity_org_detail(org_id)
        lines = [f"*{name}* ({domain}) — Affinity ID: {org_id}"]

        if detail:
            list_entries = detail.get("list_entries", [])
            if list_entries:
                lines.append(f"  On {len(list_entries)} list(s)")
                for entry in list_entries:
                    lines.append(f"  • List ID: {entry.get('list_id')} (added {entry.get('created_at', '')[:10]})")
            else:
                lines.append("  Not on any lists")

        notes = affinity_notes(org_id)
        if notes:
            lines.append(f"  {len(notes)} note(s) in activity feed")
            for n in notes[:3]:
                created = (n.get("created_at") or "")[:10]
                content = re.sub(r'<[^>]+>', '', (n.get("content") or n.get("plain_text") or "")).strip()[:100]
                lines.append(f"    [{created}] {content}")
        else:
            lines.append("  No notes/activity")

        say("\n".join(lines))

def handle_search(say, query):
    """Search Harmonic for companies."""
    say(f"Searching Harmonic for *{query}*...")
    companies = harmonic_search_company(query)
    if not companies:
        say(f"No results for *{query}*.")
        return

    lines = [f"*Harmonic results for \"{query}\":*\n"]
    for c in companies[:5]:
        name = c.get("name", "")
        desc = (c.get("description") or "")[:120]
        stage = (c.get("stage") or "").replace("_", " ").title()
        funding = c.get("funding", {}).get("funding_total") or 0
        funding_str = f"${funding:,.0f}" if funding else "No funding"
        website = c.get("website", {})
        url = website.get("url", "") if isinstance(website, dict) else ""
        location = c.get("location", {})
        city = location.get("city", "")

        lines.append(f"• *<{url}|{name}>* — {stage} · {funding_str} · {city}")
        if desc:
            lines.append(f"  {desc}")
        lines.append("")

    say("\n".join(lines))

def handle_help(say):
    """Show help."""
    say(
        "*Available commands:*\n\n"
        "• `delete last message` — delete the last bot message\n"
        "• `delete last N messages` — delete last N bot messages\n"
        "• `check [company]` — look up a company in Affinity\n"
        "• `search [query]` — search Harmonic for companies\n"
        "• `find startups` — run deal flow scan and post top 5\n"
        "• `help` — show this message"
    )

def handle_find_startups(say, channel):
    """Run the deal flow scanner."""
    say("Running deal flow scan... this takes a few minutes.")
    import subprocess
    result = subprocess.run(
        ["/opt/miniconda3/bin/python", "slack_daily_startups.py"],
        capture_output=True, text=True, timeout=600,
        cwd=os.path.dirname(os.path.abspath(__file__)),
        env={**os.environ}
    )
    if result.returncode == 0:
        # The script posts directly to Slack
        say("Deal flow scan complete!")
    else:
        say(f"Scan failed:\n```{result.stderr[-500:]}```")

# ---------- Message Listener ----------

@app.event("message")
def handle_message(event, say):
    """Route incoming messages to command handlers."""
    text = (event.get("text") or "").strip().lower()
    channel = event.get("channel", "")
    subtype = event.get("subtype")

    # Ignore bot messages, edits, etc.
    if subtype or not text:
        return

    # Only respond to @mentions or direct commands
    bot_user_id = app.client.auth_test().get("user_id", "")
    mentioned = f"<@{bot_user_id.lower()}>" in text.lower() if bot_user_id else False

    # Strip the mention
    if mentioned:
        text = re.sub(r'<@[A-Z0-9]+>', '', text, flags=re.IGNORECASE).strip()

    # Only respond if mentioned or in the deal flow channel
    if not mentioned and channel != SLACK_CHANNEL_ID:
        return

    # Route commands
    if text in ("help", "commands"):
        handle_help(say)
    elif text in ("delete last message", "delete last", "delete last msg"):
        handle_delete_last(say, channel)
    elif m := re.match(r'delete\s+(?:last\s+)?(\d+)\s+messages?', text):
        handle_delete_n(say, channel, int(m.group(1)))
    elif text.startswith("check "):
        company = text[6:].strip()
        handle_check_affinity(say, company)
    elif text.startswith("search "):
        query = text[7:].strip()
        handle_search(say, query)
    elif text in ("find startups", "run scan", "deal flow", "scan"):
        handle_find_startups(say, channel)
    elif mentioned:
        say("I didn't catch that. Type `help` to see what I can do.")

# ---------- Main ----------

if __name__ == "__main__":
    print("=" * 50)
    print("Palm Drive Deal Flow Bot — Online")
    print(f"Listening in channel: {SLACK_CHANNEL_ID}")
    print("=" * 50)
    handler = SocketModeHandler(app, SLACK_APP_TOKEN)
    handler.start()
