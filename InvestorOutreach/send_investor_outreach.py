#!/usr/bin/env python3
"""
Vora Investor Cold Outreach Email Sender
Sends personalized cold emails to investor prospects from Supabase.
Supports 3 investor types: vc, accelerator, angel.

Usage:
    # Send test emails (all 4 variants) to yourself
    python send_investor_outreach.py --test

    # Send test for a specific type only
    python send_investor_outreach.py --test --type vc

    # Dry run: show what would be sent
    python send_investor_outreach.py --live --dry-run

    # Live send to all pending prospects
    python send_investor_outreach.py --live
"""

import argparse
import os
import sys
import time
from datetime import datetime, timezone
from dotenv import load_dotenv
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import (
    Mail, Email, To, Bcc, Personalization, Content
)
from supabase import create_client

load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))

# ─── Configuration ───────────────────────────────────────────────────────────

SENDGRID_API_KEY = os.getenv('SENDGRID_API_KEY')
FROM_EMAIL = os.getenv('FROM_EMAIL', 'jai@askvora.com')
BCC_EMAIL = os.getenv('BCC_EMAIL', 'jai@askvora.com')
TEST_RECIPIENT = os.getenv('TEST_RECIPIENT', 'jaikrish15@gmail.com')
SUPABASE_URL = os.getenv('SUPABASE_URL')
SUPABASE_KEY = os.getenv('SUPABASE_KEY')

SEND_DELAY = 1.0

# ─── Vora Info ───────────────────────────────────────────────────────────────

VORA = {
    'website': 'https://askvora.com',
    'app_store': 'https://apps.apple.com/us/app/vora-health/id6754351240',
    'youtube': 'https://www.youtube.com/watch?v=SSrdtq7LTl4',
    'calendly': 'https://calendly.com/jai-askvora/30min',
    'ceo_name': 'Jai Ashar',
    'ceo_phone': '(949) 276-1808',
    'ceo_email': 'jai@askvora.com',
}

# ─── Investor Type Variants ──────────────────────────────────────────────────
# Each type has: subject, opener, traction paragraph, cta
# These are self-contained - no field that could be missing or break.

INTRO_PARAGRAPH = (
    "Two weeks ago, we launched Vora, a health coach that syncs with "
    "Apple Watch, Whoop, Garmin, and 500+ devices to build personalized "
    "nutrition, fitness, sleep, and longevity plans."
)

TRACTION_PARAGRAPH = (
    "We are already at 311 daily active users, and 103K social views in "
    "three weeks with continued growth."
)

INVESTOR_TYPES = {
    'vc': {
        'label': 'VC',
        'subject_with_name': '{first_name} + Vora',
        'subject_no_name': 'Quick intro + Vora',
        'cta': (
            "We are excited about the massive opportunity in personalizing "
            "health for everyone and are currently raising a $500K pre-seed. "
            "I'm reaching out because I think you would be a value-added investor "
            "and there could be a great opportunity for us to work together. "
            "Would you be open to a conversation?"
        ),
    },
    'accelerator': {
        'label': 'Accelerator',
        'subject_with_name': '{first_name} + Vora',
        'subject_no_name': 'Quick question + Vora',
        'cta': (
            "We are excited about the massive opportunity in personalizing "
            "health for everyone and are currently raising a $500K pre-seed. "
            "I'm reaching out because I think your program could be a great fit "
            "to help us scale. Would you be open to a conversation?"
        ),
    },
    'angel': {
        'label': 'Angel Investor',
        'subject_with_name': '{first_name} + Vora',
        'subject_no_name': 'Intro + Vora',
        'cta': (
            "We are excited about the massive opportunity in personalizing "
            "health for everyone and are currently raising a $500K pre-seed. "
            "I'm reaching out because I think you would be a value-added investor "
            "and there could be a great opportunity for us to work together. "
            "Would you be open to a conversation?"
        ),
    },
}


# ─── Email Builder ───────────────────────────────────────────────────────────

def build_outreach_email(first_name, investor_type, test_mode=False):
    """
    Build a short, traction-first cold outreach email.

    Personalization is safe:
    - first_name: uses "Hi there" if empty/None
    - investor_type: falls back to 'vc' if unknown
    - Subject uses name if available, clean fallback if not
    """
    variant = INVESTOR_TYPES.get(investor_type, INVESTOR_TYPES['vc'])
    has_name = first_name and first_name.strip()

    greeting = f"Hi {first_name}," if has_name else "Hi there,"

    if has_name:
        subject = variant['subject_with_name'].replace('{first_name}', first_name.strip())
    else:
        subject = variant['subject_no_name']

    test_banner = ""
    if test_mode:
        test_banner = (
            '<div style="background: #fff3cd; border: 1px solid #ffc107; '
            'border-radius: 6px; padding: 10px 14px; margin-bottom: 16px; '
            f'font-size: 12px; color: #856404;">'
            f'TEST MODE - Type: <strong>{variant["label"]}</strong></div>'
        )

    signature = f"""\
    <p style="margin-top: 28px;">Best,<br/>Jai</p>
    <p style="margin: 4px 0; color: #444; font-size: 13px;">
        Jai Ashar, Founder & CEO, <a href="{VORA['website']}" style="color: #444;">Vora</a><br/>
        {VORA['ceo_phone']} · <a href="mailto:{VORA['ceo_email']}" style="color: #444;">{VORA['ceo_email']}</a>
    </p>"""

    footer = f"""\
    <div style="margin-top: 36px; padding-top: 12px; border-top: 1px solid #ddd; font-size: 11px; color: #999; line-height: 1.5;">
        <a href="mailto:{VORA['ceo_email']}?subject=Unsubscribe" style="color: #999;">Unsubscribe</a>
    </div>"""

    html = f"""\
<div style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif; max-width: 600px; margin: 0 auto; color: #222; line-height: 1.75; font-size: 15px;">

{test_banner}

<p style="margin-top: 0;">{greeting}</p>

<p>{INTRO_PARAGRAPH}</p>

<p>{TRACTION_PARAGRAPH}</p>

<p>{variant['cta']}</p>

<p>If you are interested, you can <a href="{VORA['calendly']}" style="color: #222;">grab a time on my calendar here</a>. We can also add you to a biweekly investor update so that you can track our momentum if you are interested.</p>

<p style="margin-top: 20px; font-size: 13px; color: #555;">
    <a href="{VORA['app_store']}" style="color: #555;">App Store</a> · 
    <a href="{VORA['youtube']}" style="color: #555;">Demo</a> · 
    <a href="{VORA['website']}" style="color: #555;">askvora.com</a>
</p>

{signature}

{footer}

</div>"""

    return html, subject


# ─── Email Sending ───────────────────────────────────────────────────────────

def send_email(to_email, subject, html_content, bcc_email=None):
    """Send email via SendGrid. Returns (status_code, error_msg)."""
    message = Mail()
    message.from_email = Email(FROM_EMAIL, VORA['ceo_name'])
    message.subject = subject

    personalization = Personalization()
    personalization.add_to(To(to_email))
    if bcc_email:
        personalization.add_bcc(Bcc(bcc_email))
    message.add_personalization(personalization)

    message.add_content(Content('text/html', html_content))

    try:
        sg = SendGridAPIClient(SENDGRID_API_KEY)
        response = sg.send(message)
        return response.status_code, None
    except Exception as e:
        return None, str(e)


# ─── Supabase Helpers ────────────────────────────────────────────────────────

def get_supabase():
    if not SUPABASE_URL or not SUPABASE_KEY:
        print("ERROR: SUPABASE_URL and SUPABASE_KEY must be set in ../.env")
        sys.exit(1)
    return create_client(SUPABASE_URL, SUPABASE_KEY)


def fetch_pending_prospects(supabase):
    """Fetch all leads with verified email and outreach_status = 'pending'."""
    all_prospects = []
    page_size = 1000
    offset = 0

    while True:
        response = (
            supabase.table('investor_leads')
            .select('*')
            .eq('outreach_status', 'pending')
            .not_.is_('verified_email', 'null')
            .range(offset, offset + page_size - 1)
            .execute()
        )
        all_prospects.extend(response.data)
        if len(response.data) < page_size:
            break
        offset += page_size

    return all_prospects


def log_outreach_sent(supabase, email, subject, status='sent'):
    """Log to investor_email_log."""
    try:
        supabase.table('investor_email_log').insert({
            'contact_email': email,
            'list_type': 'prospect',
            'email_type': 'cold_outreach',
            'subject': subject,
            'status': status,
        }).execute()
    except Exception:
        pass


def mark_prospect_sent(supabase, lead_id):
    """Update lead outreach_status to 'emailed' and set emailed_at."""
    try:
        supabase.table('investor_leads').update({
            'outreach_status': 'emailed',
            'emailed_at': datetime.now(timezone.utc).isoformat(),
        }).eq('id', lead_id).execute()
    except Exception:
        pass


# ─── Test Mode ───────────────────────────────────────────────────────────────

def send_test_emails(specific_type=None):
    """Send test emails for all 4 types (or a specific one) to test recipient."""
    types_to_send = [specific_type] if specific_type else ['vc', 'accelerator', 'angel']

    print(f"\n{'=' * 60}")
    print(f"  VORA INVESTOR OUTREACH - TEST MODE")
    print(f"  Sending {len(types_to_send)} variant(s) to: {TEST_RECIPIENT}")
    print(f"  BCC: {BCC_EMAIL}")
    print(f"{'=' * 60}\n")

    for inv_type in types_to_send:
        html, subject = build_outreach_email(
            first_name="John",
            investor_type=inv_type,
            test_mode=True,
        )

        label = INVESTOR_TYPES.get(inv_type, {}).get('label', inv_type)
        print(f"  Sending variant: {label}")
        print(f"    Subject: {subject}")

        status, error = send_email(
            to_email=TEST_RECIPIENT,
            subject=subject,
            html_content=html,
            bcc_email=BCC_EMAIL,
        )

        if status and 200 <= status < 300:
            print(f"    Sent (status {status})\n")
        else:
            print(f"    Failed: {error}\n")

        time.sleep(1)

    print(f"  Done. Check {TEST_RECIPIENT}\n")


# ─── Live Mode ───────────────────────────────────────────────────────────────

def send_live_outreach(dry_run=False):
    """Send outreach to all pending prospects."""
    supabase = get_supabase()
    prospects = fetch_pending_prospects(supabase)

    mode_label = "DRY RUN" if dry_run else "LIVE SEND"

    print(f"\n{'=' * 60}")
    print(f"  VORA INVESTOR OUTREACH - {mode_label}")
    print(f"  {len(prospects)} pending prospects")
    print(f"{'=' * 60}\n")

    if not prospects:
        print("  No pending prospects found.")
        return

    # Summary by type
    type_counts = {}
    for p in prospects:
        t = p.get('investor_type', 'vc')
        type_counts[t] = type_counts.get(t, 0) + 1
    for t, count in sorted(type_counts.items()):
        label = INVESTOR_TYPES.get(t, {}).get('label', t)
        print(f"  {label}: {count}")
    print()

    for i, p in enumerate(prospects):
        first_name = p.get('first_name', '') or ''
        email = p.get('verified_email', '')
        if not email:
            continue
        inv_type = p.get('investor_type', 'vc') or 'vc'
        # Map health_vc to vc for email variant
        if inv_type == 'health_vc':
            inv_type = 'vc'
        name = f"{first_name} {p.get('last_name', '')}".strip() or email
        label = INVESTOR_TYPES.get(inv_type, {}).get('label', inv_type)

        html, subject = build_outreach_email(
            first_name=first_name,
            investor_type=inv_type,
        )

        print(f"  [{i+1}/{len(prospects)}] {name} ({email}) - {label}")

        if dry_run:
            print(f"           Subject: {subject}")
            continue

        status, error = send_email(
            to_email=email,
            subject=subject,
            html_content=html,
        )

        if status and 200 <= status < 300:
            print(f"           Sent (status {status})")
            log_outreach_sent(supabase, email, subject, 'sent')
            mark_prospect_sent(supabase, p['id'])
        else:
            print(f"           Failed: {error}")
            log_outreach_sent(supabase, email, subject, f'failed: {error}')

        time.sleep(SEND_DELAY)

    print(f"\n{'=' * 60}")
    if dry_run:
        print(f"  DRY RUN COMPLETE - no emails sent")
    else:
        print(f"  LIVE SEND COMPLETE - {len(prospects)} processed")
    print(f"{'=' * 60}\n")


# ─── Main ────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description='Vora Investor Cold Outreach')
    parser.add_argument('--test', action='store_true',
                        help='Send test variants to test recipient')
    parser.add_argument('--type', type=str, choices=['vc', 'accelerator', 'angel'],
                        help='Send specific type variant only')
    parser.add_argument('--live', action='store_true',
                        help='Send to all pending prospects')
    parser.add_argument('--dry-run', action='store_true',
                        help='Show what would be sent without sending')
    args = parser.parse_args()

    if not SENDGRID_API_KEY:
        print("ERROR: SENDGRID_API_KEY not set in ../.env")
        sys.exit(1)

    if args.test:
        send_test_emails(args.type)
    elif args.live:
        if not args.dry_run:
            confirm = input(f"\n  This will send LIVE outreach emails. Type 'yes' to confirm: ")
            if confirm.strip().lower() != 'yes':
                print("  Aborted.")
                sys.exit(0)
        send_live_outreach(dry_run=args.dry_run)
    else:
        parser.print_help()


if __name__ == '__main__':
    main()
