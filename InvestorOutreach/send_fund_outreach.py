#!/usr/bin/env python3
"""
Vora Fund Outreach Email Sender
Sends personalized cold emails to CVC and MENA VC funds.

Usage:
    # Send test emails (both variants) to yourself
    python send_fund_outreach.py --test

    # Send test for a specific variant only
    python send_fund_outreach.py --test --type cvc
    python send_fund_outreach.py --test --type mena

    # Dry run: show what would be sent
    python send_fund_outreach.py --live --dry-run

    # Live send to all funds in the list
    python send_fund_outreach.py --live
"""

import argparse
import csv
import os
import sys
import time
from dotenv import load_dotenv
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import (
    Mail, Email, To, Bcc, Personalization, Content
)

load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '.env'))

# ─── Configuration ───────────────────────────────────────────────────────────

SENDGRID_API_KEY = os.getenv('SENDGRID_API_KEY_MATIN') or os.getenv('SENDGRID_API_KEY')
FROM_EMAIL       = os.getenv('FROM_EMAIL', 'jai@askvora.com')
BCC_EMAIL        = os.getenv('BCC_EMAIL',  'jai@askvora.com')
TEST_RECIPIENT   = os.getenv('TEST_RECIPIENT', 'jaikrish15@gmail.com')

SEND_DELAY = 1.5  # seconds between sends

# ─── Vora Info ───────────────────────────────────────────────────────────────

VORA = {
    'website':   'https://askvora.com',
    'app_store': 'https://apps.apple.com/us/app/vora-health/id6754351240',
    'youtube':   'https://www.youtube.com/watch?v=SSrdtq7LTl4',
    'calendly':  'https://calendly.com/jai-askvora/30min',
    'ceo_name':  'Jai Ashar',
    'ceo_phone': '(949) 276-1808',
    'ceo_email': 'jai@askvora.com',
}

# ─── Fund List ────────────────────────────────────────────────────────────────
# Each entry: (fund_name, fund_type, contact_email)
# contact_email is a placeholder — replace with real partner emails before live send.
# fund_type: 'cvc' or 'mena'

FUNDS = [
    # ── CVC Funds ──────────────────────────────────────────────────────────
    ("Optum Ventures",                     "cvc",  "info@optumventures.com"),
    ("CVS Health Ventures",                "cvc",  "cvshealthventures@cvs.com"),
    ("The Cigna Group Ventures",           "cvc",  "ventures@cigna.com"),
    ("Kaiser Permanente Ventures",         "cvc",  "kpventures@kp.org"),
    ("Blue Venture Fund",                  "cvc",  "info@blueventurefund.com"),
    ("UPMC Enterprises",                   "cvc",  "info@enterprises.upmc.com"),
    ("Allumia Ventures",                   "cvc",  "hello@allumia.vc"),
    ("Cencora Ventures",                   "cvc",  "ventures@cencora.com"),
    ("McKesson Ventures",                  "cvc",  "ventures@mckesson.com"),
    ("Boston Scientific Ventures",         "cvc",  "bsv@bsci.com"),
    ("Philips Ventures",                   "cvc",  "ventures@philips.com"),
    ("Johnson & Johnson Innovation (JJDC)","cvc",  "jjdc@its.jnj.com"),
    ("Pfizer Ventures",                    "cvc",  "pfizer.ventures@pfizer.com"),
    ("Merck Global Health Innovation Fund","cvc",  "info@merckghifund.com"),
    ("Novartis Venture Fund",              "cvc",  "info@nvfund.com"),
    ("Medtronic Ventures",                 "cvc",  "ventures@medtronic.com"),
    # ── MENA Funds ─────────────────────────────────────────────────────────
    ("STV (Saudi Technology Ventures)",    "mena", "info@stv.vc"),
    ("Raed Ventures",                      "mena", "hello@raed.vc"),
    ("Nama Ventures",                      "mena", "hello@namaventures.com"),
    ("Vision Ventures",                    "mena", "info@visionvc.co"),
    ("Wa'ed Ventures",                     "mena", "info@waed.com"),
    ("Flat6Labs Riyadh",                   "mena", "riyadh@flat6labs.com"),
    ("BECO Capital",                       "mena", "hello@becocapital.com"),
    ("Shorooq Partners",                   "mena", "hello@shorooq.com"),
    ("Wamda Capital",                      "mena", "info@wamdacapital.com"),
    ("Global Ventures",                    "mena", "info@global.vc"),
    ("COTU Ventures",                      "mena", "hello@cotu.vc"),
    ("VentureSouq (VSQ)",                  "mena", "info@venturesouq.com"),
    ("500 Global MENA",                    "mena", "mena@500.co"),
    ("Middle East Venture Partners (MEVP)","mena", "info@mevp.com"),
    ("Algebra Ventures",                   "mena", "info@algebraventures.com"),
    ("Sawari Ventures",                    "mena", "info@sawariventures.com"),
]

# ─── Email Variants ──────────────────────────────────────────────────────────

FUND_TYPES = {

    'cvc': {
        'label': 'CVC',
        'subject': lambda fund_name: f"Vora + {fund_name} - AI health recommendations (seed)",
        'body': lambda fund_name: f"""\
<p>Hi,</p>

<p>I'm Jai, founder & CEO of <a href="{VORA['website']}" style="color:#222;">Vora Health</a>. I'm reaching out because {fund_name}'s focus on digital health and healthcare innovation is directly aligned with what we're building.</p>

<p>Vora Health is building the <strong>proactive AI health recommendations system</strong> - the intelligence layer that sits on top of all your health data and tells you exactly what to do to optimize your body. We sync with 500+ wearables (Apple Watch, Whoop, Garmin, and more) and generate fully personalized nutrition, fitness, sleep, and longevity protocols. Think of it as the Netflix recommendation engine, but for your health: the more data we have, the more precise and valuable the recommendations become.</p>

<p>We launched three weeks ago and are already at around <strong>300 daily active users</strong> with <strong>~500K organic social views</strong> across our accounts and strong early retention. We're raising a <strong>$2M seed round</strong> and have 2 committed investors. Beyond capital, we're looking for strategic partners - investors who understand the healthcare ecosystem and can help us reach the right enterprise and B2B2C channels where Vora's value is highest.</p>

<p>The long-term vision is a health OS that every person, health system, insurer, and employer uses to drive better outcomes at scale. We believe the data network effects we're building now become a significant moat.</p>

<p>You can read our full investor memo here: <a href="https://luminous-cloak-5c9.notion.site/Vora-Memo-27a73321712380b6958ff7f6b89f0eaf?pvs=74" style="color:#222;">Vora Memo</a></p>

<p>Would you be open to a conversation? Happy to also add you to our biweekly investor update so you can track our momentum. You can <a href="{VORA['calendly']}" style="color:#222;">grab a time here</a> or just reply to this email.</p>"""
    },

    'mena': {
        'label': 'MENA VC',
        'subject': lambda fund_name: f"Vora - AI health recommendations platform (seed)",
        'body': lambda fund_name: f"""\
<p>Hi,</p>

<p>I'm Jai, founder & CEO of <a href="{VORA['website']}" style="color:#222;">Vora Health</a>. I'm building what I believe will become the defining <strong>proactive AI health recommendations system</strong> of the next decade, and I wanted to reach out to {fund_name} directly.</p>

<p>Vora Health syncs with 500+ wearables (Apple Watch, Whoop, Garmin, and more) and uses AI to generate fully personalized nutrition, fitness, sleep, and longevity plans for each user. The analogy I use: Spotify's discovery algorithm changed how the world listens to music. Vora is doing the same for health - turning raw biometric data into the exact actions each person needs to live better and longer.</p>

<p>We launched three weeks ago and are already at around <strong>300 daily active users</strong> with <strong>~500K organic social views</strong> across our accounts and strong early retention. We're raising a <strong>$2M seed round</strong> with 2 committed investors already in. The MENA region, with its young mobile-first population, growing wellness culture, and government investment in digital health, is a significant part of our expansion vision.</p>

<p>We're looking for investors who can help us move fast, open doors in key markets, and build toward a Series A within 12 to 18 months. The data network effects we're building today become the moat tomorrow.</p>

<p>You can read our full investor memo here: <a href="https://luminous-cloak-5c9.notion.site/Vora-Memo-27a73321712380b6958ff7f6b89f0eaf?pvs=74" style="color:#222;">Vora Memo</a></p>

<p>Would you be open to a quick call? I can also add you to our biweekly investor update so you can track our progress. <a href="{VORA['calendly']}" style="color:#222;">Grab a time here</a> or just reply.</p>"""
    },
}

# ─── Email Builder ────────────────────────────────────────────────────────────

def build_fund_email(fund_name, fund_type, test_mode=False):
    variant  = FUND_TYPES.get(fund_type, FUND_TYPES['mena'])
    subject  = variant['subject'](fund_name)
    body     = variant['body'](fund_name)

    test_banner = ""
    if test_mode:
        test_banner = (
            f'<div style="background:#fff3cd;border:1px solid #ffc107;border-radius:6px;'
            f'padding:10px 14px;margin-bottom:16px;font-size:12px;color:#856404;">'
            f'TEST MODE - Type: <strong>{variant["label"]}</strong> - Fund: <strong>{fund_name}</strong></div>'
        )

    signature = f"""\
    <p style="margin-top:28px;">Best,<br/>Jai</p>
    <p style="margin:4px 0;color:#444;font-size:13px;">
        Jai Ashar, Founder &amp; CEO · <a href="{VORA['website']}" style="color:#444;">Vora</a><br/>
        {VORA['ceo_phone']} · <a href="mailto:{VORA['ceo_email']}" style="color:#444;">{VORA['ceo_email']}</a>
    </p>
    <p style="margin:8px 0 0 0;font-size:13px;color:#555;">
        <a href="{VORA['app_store']}" style="color:#555;">App Store</a> ·
        <a href="{VORA['youtube']}"   style="color:#555;">Demo</a> ·
        <a href="{VORA['website']}"   style="color:#555;">askvora.com</a>
    </p>"""

    footer = f"""\
    <div style="margin-top:36px;padding-top:12px;border-top:1px solid #ddd;font-size:11px;color:#999;line-height:1.5;">
        <a href="mailto:{VORA['ceo_email']}?subject=Unsubscribe" style="color:#999;">Unsubscribe</a>
    </div>"""

    html = f"""\
<div style="font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Helvetica,Arial,sans-serif;max-width:600px;margin:0 auto;color:#222;line-height:1.75;font-size:15px;">

{test_banner}

{body}

{signature}

{footer}

</div>"""

    return html, subject


# ─── Email Sending ────────────────────────────────────────────────────────────

def send_email(to_email, subject, html_content, bcc_email=None):
    message = Mail()
    message.from_email = Email(FROM_EMAIL, VORA['ceo_name'])
    message.subject    = subject

    personalization = Personalization()
    personalization.add_to(To(to_email))
    if bcc_email:
        personalization.add_bcc(Bcc(bcc_email))
    message.add_personalization(personalization)
    message.add_content(Content('text/html', html_content))

    try:
        sg       = SendGridAPIClient(SENDGRID_API_KEY)
        response = sg.send(message)
        return response.status_code, None
    except Exception as e:
        return None, str(e)


# ─── Test Mode ────────────────────────────────────────────────────────────────

TEST_RECIPIENTS_EXTRA = ['tc@askvora.com']

def send_test_emails(specific_type=None):
    types_to_send = [specific_type] if specific_type else ['cvc', 'mena']

    sample_funds = {
        'cvc':  'Optum Ventures',
        'mena': 'STV (Saudi Technology Ventures)',
    }

    all_recipients = [TEST_RECIPIENT] + TEST_RECIPIENTS_EXTRA

    print(f"\n{'='*60}")
    print(f"  VORA FUND OUTREACH - TEST MODE")
    print(f"  Sending {len(types_to_send)} variant(s) to: {', '.join(all_recipients)}")
    print(f"  BCC: {BCC_EMAIL}")
    print(f"{'='*60}\n")

    for fund_type in types_to_send:
        fund_name     = sample_funds[fund_type]
        html, subject = build_fund_email(fund_name, fund_type, test_mode=True)
        label         = FUND_TYPES[fund_type]['label']

        print(f"  Sending variant: {label}  ({fund_name})")
        print(f"    Subject: {subject}")

        for recipient in all_recipients:
            status, error = send_email(
                to_email=recipient,
                subject=subject,
                html_content=html,
                bcc_email=BCC_EMAIL if recipient == TEST_RECIPIENT else None,
            )
            if status and 200 <= status < 300:
                print(f"    Sent ✓  {recipient}  (status {status})")
            else:
                print(f"    Failed ✗  {recipient}  {error}")
            time.sleep(1)

        print()

    print(f"  Done. Check {', '.join(all_recipients)}\n")


# ─── Live Mode ────────────────────────────────────────────────────────────────

def send_live_outreach(dry_run=False, type_filter=None):
    funds = FUNDS if not type_filter else [f for f in FUNDS if f[1] == type_filter]

    mode_label = "DRY RUN" if dry_run else "LIVE SEND"
    print(f"\n{'='*60}")
    print(f"  VORA FUND OUTREACH — {mode_label}")
    print(f"  {len(funds)} fund(s) to process")
    print(f"{'='*60}\n")

    cvc_count  = sum(1 for f in funds if f[1] == 'cvc')
    mena_count = sum(1 for f in funds if f[1] == 'mena')
    print(f"  CVC:  {cvc_count}")
    print(f"  MENA: {mena_count}\n")

    for i, (fund_name, fund_type, contact_email) in enumerate(funds):
        label = FUND_TYPES.get(fund_type, {}).get('label', fund_type)
        html, subject = build_fund_email(fund_name, fund_type)

        print(f"  [{i+1}/{len(funds)}] {fund_name} ({contact_email}) — {label}")

        if dry_run:
            print(f"           Subject: {subject}")
            continue

        status, error = send_email(
            to_email=contact_email,
            subject=subject,
            html_content=html,
        )

        if status and 200 <= status < 300:
            print(f"           Sent ✓  (status {status})")
        else:
            print(f"           Failed ✗  {error}")

        time.sleep(SEND_DELAY)

    print(f"\n{'='*60}")
    if dry_run:
        print(f"  DRY RUN COMPLETE — no emails sent")
    else:
        print(f"  LIVE SEND COMPLETE — {len(funds)} funds processed")
    print(f"{'='*60}\n")


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description='Vora Fund Cold Outreach')
    parser.add_argument('--test',    action='store_true', help='Send test variants to test recipient')
    parser.add_argument('--type',    type=str, choices=['cvc', 'mena'], help='Filter by fund type')
    parser.add_argument('--live',    action='store_true', help='Send to all funds in list')
    parser.add_argument('--dry-run', action='store_true', help='Show what would be sent without sending')
    args = parser.parse_args()

    if not SENDGRID_API_KEY:
        print("ERROR: SENDGRID_API_KEY not set in ../.env")
        sys.exit(1)

    if args.test:
        send_test_emails(args.type)
    elif args.live:
        if not args.dry_run:
            confirm = input(f"\n  This will send LIVE emails to {len(FUNDS)} funds. Type 'yes' to confirm: ")
            if confirm.strip().lower() != 'yes':
                print("  Aborted.")
                sys.exit(0)
        send_live_outreach(dry_run=args.dry_run, type_filter=args.type)
    else:
        parser.print_help()


if __name__ == '__main__':
    main()
