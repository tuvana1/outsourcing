#!/usr/bin/env python3
"""Send March 15 VC intel briefing to Slack deal flow channel."""
import os, requests
from dotenv import load_dotenv
load_dotenv()

SLACK_WEBHOOK_URL = os.environ["SLACK_WEBHOOK_URL"]

def post(blocks):
    r = requests.post(SLACK_WEBHOOK_URL, json={"blocks": blocks}, timeout=30)
    print(f"  Status: {r.status_code} {r.text}")
    return r.status_code

# PART 1: Header + Who to reach out to
print("Sending header + outreach targets...")
blocks1 = [
    {"type": "header", "text": {"type": "plain_text", "text": "VC Daily Intel Briefing — March 15, 2026"}},
    {"type": "context", "elements": [{"type": "mrkdwn", "text": "Founders leaving stealth · Stealth entries · VC tweets · Top news · Funding"}]},
    {"type": "divider"},
    {"type": "header", "text": {"type": "plain_text", "text": "🎯 Recommended Outreach — Founders Leaving Stealth"}},
    {"type": "context", "elements": [{"type": "mrkdwn", "text": "Ranked by founder quality, market signal, and fit"}]},

    # Tricia Martinez-Saab — Dapple
    {"type": "section", "text": {"type": "mrkdwn", "text": (
        "*🔥 Tricia Martínez-Saab* — Founder & CEO @ *Dapple*\n"
        "<https://www.linkedin.com/in/tricianmartinez|LinkedIn>\n\n"
        "*Background:* Stanford Entrepreneur Fellow · UChicago MPP · U.S. Dept of Energy · White House · Techstars · Founded The Dala Foundation\n\n"
        "*Why reach out:* Elite pedigree (Stanford + White House). Techstars validated. Serial founder with policy + tech crossover. High-signal AI/tech venture."
    )}},

    {"type": "divider"},

    # Arnold Cheskis — QJudge
    {"type": "section", "text": {"type": "mrkdwn", "text": (
        "*🔥 Arnold Cheskis* — Forward Deployed AI Engineer @ *QJudge* + Stealth Co-founder\n"
        "<https://www.linkedin.com/in/arnold-cheskis|LinkedIn>\n\n"
        "*Background:* MIT + Cornell · MITdesignX Fellow · KALINER · Forward Deployed AI at QJudge\n\n"
        "*Why reach out:* MIT/Cornell combo. MITdesignX = startup program validation. \"Forward Deployed AI\" role signals enterprise AI deployment experience (Palantir-style). Co-founding stealth venture on the side."
    )}},

    {"type": "divider"},

    # James Fang — Architect
    {"type": "section", "text": {"type": "mrkdwn", "text": (
        "*🔥 James Fang* — Founding Member of Technical Staff @ *Architect*\n"
        "<https://www.linkedin.com/in/james-fang-uiuccs|LinkedIn>\n\n"
        "*Background:* EPFL + UIUC CS · AI/ML at NYU Medicine · Stealth startup experience\n\n"
        "*Why reach out:* Elite CS education (EPFL + UIUC). AI/ML in healthcare is high-signal. Early-stage founding role at Architect."
    )}},

    {"type": "divider"},

    # Christopher Biddle — Dashly
    {"type": "section", "text": {"type": "mrkdwn", "text": (
        "*🔥 Christopher Biddle* — Founding Software Engineer @ *Dashly* + Stealth\n"
        "<https://www.linkedin.com/in/chris01b|LinkedIn>\n\n"
        "*Background:* UPenn dual degree (Math Econ + CS) · Multiple founding engineer roles\n\n"
        "*Why reach out:* Serial founding engineer with strong quant + CS background. Pattern of joining/building 0-to-1 ventures."
    )}},
]
post(blocks1)

# PART 2: Stealth entries worth watching
print("Sending stealth entries...")
blocks2 = [
    {"type": "divider"},
    {"type": "header", "text": {"type": "plain_text", "text": "🕵️ Worth Watching — Founders Entering Stealth"}},

    {"type": "section", "text": {"type": "mrkdwn", "text": (
        "*Daniel Rupawalla* — Ex-Nuntius (YC S25) · Founder-in-Residence @ Afore Capital\n"
        "<https://www.linkedin.com/in/danielrupawalla|LinkedIn>\n"
        "YC alum + Afore FIR = strong signal. Previously built and exited. Now in stealth.\n\n"
        "*Sourabh Agarwal* — Ex-Flipkart Sr Eng · Ex-SpoonJoy Co-Founder/CTO · Angel Investor\n"
        "<https://www.linkedin.com/in/sourabhagrawal09|LinkedIn>\n"
        "IIT Roorkee · Deep India startup ecosystem experience. Building again.\n\n"
        "*Prajit Sengupta* — Imperial College London MSc · OWASP · Cybersecurity focus\n"
        "<https://www.linkedin.com/in/prajitsengupta|LinkedIn>\n"
        "Cybersecurity sector is red hot (see Wiz $32B exit). Worth tracking."
    )}},
]
post(blocks2)

# PART 3: Key news + market context
print("Sending news + market context...")
blocks3 = [
    {"type": "divider"},
    {"type": "header", "text": {"type": "plain_text", "text": "📰 Key News & Market Signals"}},

    {"type": "section", "text": {"type": "mrkdwn", "text": (
        "*🛡️ Anduril wins $20B US Army contract*\n"
        "10-year deal consolidating 120+ procurement actions. Defense tech is scaling fast.\n\n"
        "*💸 Google closes $32B Wiz acquisition*\n"
        "Largest VC-backed exit ever. Cybersecurity + multi-cloud + AI = massive valuations.\n\n"
        "*🧠 Gestala raises $21.6M for non-invasive BCI*\n"
        "Ultrasound-based brain-computer interfaces. Early stage, China-based.\n\n"
        "*🤖 Meta acquires Moltbook (AI agent social network)*\n"
        "Big Tech acqui-hiring AI agent talent aggressively."
    )}},

    {"type": "divider"},

    {"type": "header", "text": {"type": "plain_text", "text": "🐦 VC Twitter Signals"}},
    {"type": "section", "text": {"type": "mrkdwn", "text": (
        "• *Christoph Janz*: Claude becoming the UI layer for SaaS — AI wrappers adding real value\n"
        "• *Sarah Guo*: Conviction retreat lineup includes Karpathy, Brady Taylor, Parker Conrad\n"
        "• *Matt Turck*: Spotlighting Axiommath AI (math/science AI renaissance)\n"
        "• *Mark Suster*: Now following Mira Murati, Karpathy, Beff Jezos — watching AI closely"
    )}},

    {"type": "divider"},
    {"type": "context", "elements": [{"type": "mrkdwn", "text": "🧠 *Palm Drive Capital Daily Intel* · Research by Claude"}]},
]
post(blocks3)

print("\nDone! Briefing posted to Slack.")
