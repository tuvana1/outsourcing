#!/usr/bin/env python3
"""Send daily VC intel briefing to Slack."""
import os, json, requests

SLACK_WEBHOOK_URL = os.environ["SLACK_WEBHOOK_URL"]

def post(blocks):
    r = requests.post(SLACK_WEBHOOK_URL, json={"blocks": blocks}, timeout=30)
    print(f"  Status: {r.status_code} {r.text}")
    return r.status_code

# PART 1a: Header + first 3 founders
print("Sending Part 1a (header + Autoheal, JOYhealth, TensorMem)...")
blocks = [
    {"type": "header", "text": {"type": "plain_text", "text": "VC Daily Intel Briefing \u2014 March 11, 2026"}},
    {"type": "context", "elements": [{"type": "mrkdwn", "text": "Founders leaving stealth \u00b7 VC tweets \u00b7 Top news \u00b7 Social signals \u00b7 Product launches"}]},
    {"type": "divider"},
    {"type": "header", "text": {"type": "plain_text", "text": "PART 1: Top Founders Leaving Stealth"}},
    {"type": "context", "elements": [{"type": "mrkdwn", "text": "High-signal talent emerging from stealth mode recently"}]},

    # Autoheal AI
    {"type": "section", "text": {"type": "mrkdwn", "text": (
        "*\U0001f525 <https://autoheal.ai|Autoheal AI>* \u2014 AI Platform for Production Engineering\n"
        "*Puneet Saraswat* (Co-founder, CDO) \u00b7 <https://www.linkedin.com/in/puneet--saraswat|LinkedIn>\n\n"
        "*What they do:* First AI platform using a Production Context Graph to triage, investigate, and heal production systems. "
        "Serves enterprise SRE teams at banks, fintechs, and mission-critical platforms.\n\n"
        "*Founder:* IIT Kanpur M.Tech \u00b7 VP Engineering at Harness \u00b7 Co-founder Sid Choudhury (ex-SVP Harness, ex-Yugabyte, ex-AppDynamics/Salesforce)\n\n"
        "*Stage:* Pre-seed/stealth \u00b7 No disclosed funding \u00b7 Speaking at RSA Conference & AI Council\n\n"
        "*Why interesting:* Deep enterprise infra pedigree (Harness leadership reunion). Self-healing production systems is massive pain point for fintechs. Strong founder-market fit."
    )}},

    # JOYhealth
    {"type": "divider"},
    {"type": "section", "text": {"type": "mrkdwn", "text": (
        "*\U0001f525 <https://gojoyhealth.com|JOYhealth>* (Decipher Health) \u2014 AI-Powered Diabetes Management\n"
        "*CJ Swamy* (Co-founder, COO) \u00b7 <https://www.linkedin.com/in/jaganathswamy|LinkedIn>\n\n"
        "*What they do:* AI platform analyzing unique body profiles to create personalized food/activity recs for diabetes \u2014 "
        "without requiring CGMs. Ran largest personalized nutrition study for T2 diabetes in India (500+ patients, 14 cities).\n\n"
        "*Founder:* IIT Delhi + Wharton MBA \u00b7 McKinsey \u2192 HarbourVest VP \u2192 Northgate Capital \u00b7 Multiple exits (ThinkLink, Kiddo Health $28M+)\n\n"
        "*Stage:* Early stage \u00b7 Backed by Venture Highway, GrowthCap, Better Capital \u00b7 HQ Boston + India\n\n"
        "*Why interesting:* Serial founder with PE/VC investing experience in massive TAM (diabetes). Note: healthcare vertical \u2014 outside core B2B SaaS focus."
    )}},

    # TensorMem
    {"type": "divider"},
    {"type": "section", "text": {"type": "mrkdwn", "text": (
        "*\U0001f525 <https://tensormem.ai|TensorMem Inc>* \u2014 Software-Defined AI Data Pipeline\n"
        "*Anand A. Kekre* (Co-Founder) \u00b7 <https://www.linkedin.com/in/aakekre|LinkedIn>\n\n"
        "*What they do:* Eliminates memory/storage bottlenecks in AI training and inference. "
        "Core thesis: companies spend millions on GPUs but performance collapses because data pipelines can't keep up.\n\n"
        "*Founder:* IIT Bombay M.Tech \u00b7 60+ US patents \u00b7 Chief Architect at Veritas (10 yrs) \u00b7 Founded Vaultize (acquired 2018)\n\n"
        "*Stage:* Very early / stealth \u00b7 No disclosed funding \u00b7 Based in Pune, India\n\n"
        "*Why interesting:* Rare deep-infra credibility (60+ patents). AI infra bottleneck is real and growing. Prior exit. Note: India-based \u2014 monitor for US expansion."
    )}},
]
post(blocks)

# PART 1b: Scott AI + Trimz + InsForge
print("Sending Part 1b (Scott AI, Trimz, InsForge)...")
blocks2 = [
    # Scott AI
    {"type": "section", "text": {"type": "mrkdwn", "text": (
        "*\U0001f525 <https://tryscott.ai|Scott AI>* (YC F25) \u2014 Agentic Workspace for Engineering Alignment\n"
        "*Devin Cintron* (Co-Founder, CTO) \u00b7 <https://www.linkedin.com/in/devin-cintron|LinkedIn>\n\n"
        "*What they do:* Decision/planning layer before code generation. Teams grant multi-agent swarms (Claude, Codex) "
        "secure codebase access. Agents explore architecture paths in parallel, Scott orchestrates structured debate and surfaces disagreements.\n\n"
        "*Founders:* Devin \u2014 Stanford CS (AI) \u00b7 Kleiner Perkins Fellow \u00b7 Bain \u00b7 Led mobile eng at Comun (0 to millions of users). "
        "David Maulick \u2014 Staff Eng at Coinbase (4 yrs, platform powered 95% of UI, 700+ engineers)\n\n"
        "*Stage:* YC F25 \u00b7 2 people \u00b7 NYC \u00b7 $500K YC deal\n\n"
        "*Why interesting:* Tackles missing coordination layer in AI-assisted dev. Strong founders (Stanford + Coinbase infra). "
        "YC validation. AI devtools is white-hot. Fits B2B SaaS thesis perfectly."
    )}},

    # Trimz
    {"type": "divider"},
    {"type": "section", "text": {"type": "mrkdwn", "text": (
        "*\U0001f525 <https://trimz.ai|Trimz>* \u2014 AI-Native Short-Series Platform\n"
        "*Nikhil Gopalam* (Co-Founder) \u00b7 <https://www.linkedin.com/in/nikhil-gopalam|LinkedIn>\n\n"
        "*What they do:* Community platform for creating, watching, and remixing AI-generated short-form video series.\n\n"
        "*Founder:* Google APM \u2192 YouTube Product Leader (AI features, premium monetization, creator tools) \u2192 Stir\n\n"
        "*Stage:* Seed \u00b7 Backed by Pear VC \u00b7 SF-based \u00b7 iOS app live \u00b7 ~16K Instagram followers\n\n"
        "*Why interesting:* Ex-Google/YouTube product leader with creator economy experience. Note: B2C consumer play \u2014 outside core B2B focus."
    )}},

    # InsForge
    {"type": "divider"},
    {"type": "section", "text": {"type": "mrkdwn", "text": (
        "*\U0001f525 <https://insforge.dev|InsForge>* \u2014 Backend Platform for AI Coding Agents\n"
        "*Hang Huang* (CEO, ex-Amazon Sr PM, Yale MBA) \u00b7 *Tony Chang* (CTO, ex-Databricks)\n\n"
        "*What they do:* \"Supabase for agentic development\" \u2014 databases, auth, storage, model gateway, edge functions. "
        "Semantic layer AI agents can understand. Open source on GitHub.\n\n"
        "*Team:* 5 engineers from Amazon, Databricks, Meta, TikTok\n\n"
        "*Stage:* Raised $1.5M (MindWorks Capital) \u00b7 Founded Jul 2025 \u00b7 2K+ databases hosted, 5 enterprise teams\n\n"
        "*Traction:* 348 upvotes on Product Hunt (Mar 10) \u00b7 14% more accuracy, 1.4x faster vs Supabase on MCPMark v2\n\n"
        "*Why interesting:* Picks-and-shovels for AI coding agent wave. Strong team. Open source. Early traction. Fits B2B infra thesis."
    )}},
]
post(blocks2)

# PART 2: VC Tweets
print("Sending Part 2 (VC Tweets)...")
blocks3 = [
    {"type": "divider"},
    {"type": "header", "text": {"type": "plain_text", "text": "PART 2: Top Trending VC Tweets"}},
    {"type": "context", "elements": [{"type": "mrkdwn", "text": "Highest-velocity tweets from top-tier investors in the last 24 hours"}]},

    {"type": "section", "text": {"type": "mrkdwn", "text": (
        "\U0001f7e1 *Douglas Leone* (@dougleone)\n"
        "> Everyone knows @wiz_io broke a record reaching $100M ARR in 18 months. "
        "What everyone doesn't know is that the team was using a clever hack \u2014 leveraging their Israeli time zone..."
    )}},

    {"type": "section", "text": {"type": "mrkdwn", "text": (
        "\U0001f7e1 *Alfred Lin* (@alfred_lin)\n"
        "> Most roles must shorten decision cycles in the AI age. Founders shouldn't. "
        "They must think in 2nd & 3rd order effects."
    )}},

    {"type": "section", "text": {"type": "mrkdwn", "text": (
        "\U0001f7e1 *Troy Kirwin* (@tkexpress11)\n"
        "> The #1 mistake founders make in a fundraise process is asking for too much $ out of the gate. "
        "ALWAYS start with a lower number than you're targeting, work towards collecting the first term sheet."
    )}},

    {"type": "section", "text": {"type": "mrkdwn", "text": (
        "\U0001f7e1 *Sarah Wang* (@sarahdingwang)\n"
        "> Very few have built a vertically integrated hardware company in the modern era. "
        "@RJScaringe is one of them. At Mind, he's building the robotics partner @Rivian wanted but couldn't find."
    )}},

    {"type": "section", "text": {"type": "mrkdwn", "text": (
        "\U0001f7e1 *Olivia Moore* (@omooretweets) \u2014 a16z\n"
        "> Our team @a16z calculated AI adoption per capita across the world. "
        "The U.S. leads AI development...but it ranks down at #20 in adoption."
    )}},
]
post(blocks3)

# PART 3: Top News
print("Sending Part 3 (Top News)...")
blocks4 = [
    {"type": "divider"},
    {"type": "header", "text": {"type": "plain_text", "text": "PART 3: Top News"}},
    {"type": "context", "elements": [{"type": "mrkdwn", "text": "Today's highest-signal news for founders and investors"}]},

    {"type": "section", "text": {"type": "mrkdwn", "text": (
        "*\U0001f4b0 <https://techcrunch.com/2026/03/11/google-completes-32b-acquisition-of-wiz/|Google wraps up $32B acquisition of Wiz>*\n"
        "Record $32B acquisition underscores critical need for multi-cloud security as AI threats proliferate.\n\n"
        "*VC Takeaway:* Validates full-stack, multi-cloud security platforms. Greenfield for startups securing hybrid infra with AI."
    )}},

    {"type": "section", "text": {"type": "mrkdwn", "text": (
        "*\U0001f916 <https://techcrunch.com/2026/03/10/mandiants-founder-just-raised-190m-for-his-autonomous-ai-a-security-startup/|"
        "Mandiant founder raises $190M for Armadin>*\n"
        "Kevin Mandia launched Armadin with record $190M seed/Series A for autonomous AI agents vs AI-powered cyberattacks.\n\n"
        "*VC Takeaway:* AI vs. AI arms race is underway. Look for founders with deep domain expertise in agentic security."
    )}},

    {"type": "section", "text": {"type": "mrkdwn", "text": (
        "*\u2699\ufe0f <https://techcrunch.com/2026/03/11/rivian-mind-robotics-series-a-500m-fund-raise-industrial-ai-powered-robots/|"
        "Rivian spin-out Mind Robotics raises $500M>*\n"
        "RJ Scaringe's spin-out secured $500M Series A for industrial AI robots with human-like dexterity ($2B valuation).\n\n"
        "*VC Takeaway:* Strong appetite for practical industrial AI over general-purpose humanoids. Specialized dexterity + factory integration."
    )}},
]
post(blocks4)

# PART 4 + 5: VC Social + Product Launch
print("Sending Parts 4-5 (VC Social + Product Launch)...")
blocks5 = [
    {"type": "divider"},
    {"type": "header", "text": {"type": "plain_text", "text": "PART 4: VC Twitter \u2014 Who Smart Money Is Following"}},

    {"type": "section", "text": {"type": "mrkdwn", "text": (
        "\U0001f6a8 *Danel Dayan* (Battery Ventures) started following *Reiner Pope* and *bwb* (@Brittain_Ben)\n"
        "\U0001f6a8 *Matt Mandel* (Union Square Ventures) started following *Ken Ono* and *Niko Bonatsos*"
    )}},

    {"type": "divider"},
    {"type": "header", "text": {"type": "plain_text", "text": "PART 5: Today's Top Product Hunt Launch"}},
    {"type": "context", "elements": [{"type": "mrkdwn", "text": "Filtered for founders with VC validation"}]},

    {"type": "section", "text": {"type": "mrkdwn", "text": (
        "*<https://insforge.dev/|InsForge>* \u2014 Give agents everything they need to ship fullstack apps\n"
        "\u2b06\ufe0f 348 votes on Product Hunt (Mar 10)\n"
        "Makers: Hang Huang (ex-Amazon), Tony Chang (ex-Databricks), jwfing\n"
        "<https://www.producthunt.com/products/insforge-alpha|View on Product Hunt>"
    )}},

    {"type": "divider"},
    {"type": "context", "elements": [{"type": "mrkdwn", "text": "\U0001f9e0 *Palm Drive Capital Daily Intel* \u00b7 Research by Claude"}]},
]
post(blocks5)

print("\nDone! Full briefing posted to Slack.")
