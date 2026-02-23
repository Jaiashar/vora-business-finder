#!/usr/bin/env python3
"""
Vora Investor Update Email Sender
Sends weekly update emails to investor contacts from Supabase.

Usage:
    # Send test email to yourself (default — safe mode)
    python send_investor_update.py --test

    # Dry run: show what would be sent to real contacts
    python send_investor_update.py --live --dry-run

    # Live send to all active investor contacts
    python send_investor_update.py --live
"""

import argparse
import os
import sys
import time
from datetime import datetime, timezone
from dotenv import load_dotenv
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import (
    Mail, Email, To, Bcc, ReplyTo, Personalization, Content
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

SEND_DELAY = 1.0  # seconds between sends (small list, no need to rush)

# ─── Vora Info ───────────────────────────────────────────────────────────────

VORA = {
    'website': 'https://askvora.com',
    'app_store': 'https://apps.apple.com/us/app/vora-health/id6754351240',
    'youtube': 'https://www.youtube.com/watch?v=SSrdtq7LTl4',
    'calendly': 'https://calendly.com/jai-askvora/30min',
    'ceo_name': 'Jai Ashar',
    'ceo_phone': '(949) 276-1808',
    'ceo_email': 'jai@askvora.com',
    'twitter': 'https://x.com/JaiAshar',
    'instagram': 'https://www.instagram.com/askvora/',
    'linkedin': 'https://www.linkedin.com/in/jaiashar/',
}

LOGO_URL = "https://askvora.com/iOS%20LOGO%204.png"


# ─── Email Template ──────────────────────────────────────────────────────────
# Designed as a clean founder letter — not a marketing email.
# Based on YC / a16z investor update best practices:
#   - Lead with metrics, not prose
#   - Keep it in the email body (no click-throughs required)
#   - Be transparent (include challenges)
#   - End with specific asks
#   - Tone: personal, concise, honest

def build_investor_update_email(first_name, subject, update_content, test_mode=False):
    """
    Build a clean, founder-letter-style investor update email.

    Args:
        first_name: Investor's first name for personalization
        subject: Email subject line
        update_content: The main update body HTML (customize per send)
        test_mode: If True, adds a small test banner at top
    """
    greeting = f"Hi {first_name}," if first_name else "Hi there,"

    test_banner = ""
    if test_mode:
        test_banner = (
            '<div style="background: #fff3cd; border: 1px solid #ffc107; '
            'border-radius: 6px; padding: 10px 14px; margin-bottom: 16px; '
            'font-size: 12px; color: #856404;">'
            'TEST MODE - This would not be sent to real contacts.</div>'
        )

    signature = f"""\
    <p style="margin-top: 28px;">Best,<br/>Jai, Matin, TC</p>
    <p style="margin: 4px 0; color: #444; font-size: 13px;">
        Jai Ashar · Founder & CEO, <a href="{VORA['website']}" style="color: #444;">Vora</a><br/>
        {VORA['ceo_phone']} · <a href="mailto:{VORA['ceo_email']}" style="color: #444;">{VORA['ceo_email']}</a>
    </p>"""

    footer = f"""\
    <div style="margin-top: 36px; padding-top: 12px; border-top: 1px solid #ddd; font-size: 11px; color: #999; line-height: 1.5;">
        You're receiving this as a potential stakeholder of Vora. <a href="mailto:{VORA['ceo_email']}?subject=Unsubscribe" style="color: #999;">Unsubscribe</a>
    </div>"""

    html = f"""\
<div style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif; max-width: 600px; margin: 0 auto; color: #222; line-height: 1.75; font-size: 15px;">

{test_banner}

<p style="margin-top: 0;">{greeting}</p>

{update_content}

{signature}

{footer}

</div>"""

    return html


# ─── Weekly Update Content ───────────────────────────────────────────────────
# Edit this section before each weekly send.
# Format: Narrative -> Metrics Table -> Fundraise -> Challenges -> Goals -> Asks

WEEKLY_UPDATE = f"""\
<p>This is the first of our biweekly investor updates to keep you in the loop on our progress, metrics, and goals that you can help us achieve. You can expect one of these every two weeks going forward.</p>

<p>2026 has been a strong start for us. We shipped the full version of Vora in early February and have been focused on getting real users on the app and learning fast. In just two weeks since launch, we have grown to 311 users and are seeing about 100 of them come back daily, which is encouraging retention for a brand new product. Our social content has taken off organically with over 103K views in three weeks, and our college outreach campaign has reached 15,000 students at 24 universities with another 55K contacts in the pipeline. On the fundraise side, we have 2 committed investors toward our $500K pre-seed round and are pushing to close by mid-March.</p>

<p><strong>Goals</strong></p>
<ul style="padding-left: 20px; margin: 8px 0 20px 0;">
    <li style="margin-bottom: 4px;">Close the $500K round by mid-March</li>
    <li style="margin-bottom: 4px;">1,000 total users by end of March</li>
    <li style="margin-bottom: 4px;">250+ DAU by end of February</li>
    <li style="margin-bottom: 4px;">Finish 55K college outreach pipeline this week</li>
    <li style="margin-bottom: 4px;">Launch B2B2C partnerships with health and wellness businesses by mid-March</li>
</ul>

<p><strong>Asks</strong></p>
<ul style="padding-left: 20px; margin: 8px 0 20px 0;">
    <li style="margin-bottom: 4px;">Intros to angels or funds interested in health/AI at the pre-seed stage</li>
    <li style="margin-bottom: 4px;">If you know anyone in college health/wellness who would be a good partner, we would love an intro</li>
    <li style="margin-bottom: 4px;">Try the app and share honest feedback: <a href="{VORA['app_store']}" style="color: #222;">Download Vora</a></li>
</ul>

<p>Happy to jump on a call anytime, just reply or <a href="{VORA['calendly']}" style="color: #222;">grab a time here</a>.</p>"""


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


def fetch_active_contacts(supabase):
    """Fetch all active investor contacts."""
    all_contacts = []
    page_size = 1000
    offset = 0

    while True:
        response = (
            supabase.table('investor_contacts')
            .select('*')
            .eq('is_active', True)
            .range(offset, offset + page_size - 1)
            .execute()
        )
        all_contacts.extend(response.data)
        if len(response.data) < page_size:
            break
        offset += page_size

    return all_contacts


def log_email_sent(supabase, email, subject, status='sent'):
    """Log a sent email to investor_email_log."""
    try:
        supabase.table('investor_email_log').insert({
            'contact_email': email,
            'list_type': 'contact',
            'email_type': 'weekly_update',
            'subject': subject,
            'status': status,
        }).execute()
    except Exception as e:
        print(f"    Warning: failed to log send for {email}: {e}")


def update_last_emailed(supabase, email):
    """Update the last_emailed_at timestamp for a contact."""
    try:
        supabase.table('investor_contacts').update({
            'last_emailed_at': datetime.now(timezone.utc).isoformat(),
        }).eq('email', email).execute()
    except Exception:
        pass


# ─── Test Mode ───────────────────────────────────────────────────────────────

def send_test_email():
    """Send a test investor update email to the test recipient."""
    subject = "Vora Update - Feb 2026"

    html = build_investor_update_email(
        first_name="John",
        subject=subject,
        update_content=WEEKLY_UPDATE,
        test_mode=True,
    )

    print(f"\n{'=' * 60}")
    print(f"  VORA INVESTOR UPDATE — TEST MODE")
    print(f"  To: {TEST_RECIPIENT}")
    print(f"  BCC: {BCC_EMAIL}")
    print(f"  Subject: {subject}")
    print(f"{'=' * 60}\n")

    status, error = send_email(
        to_email=TEST_RECIPIENT,
        subject=subject,
        html_content=html,
    )

    if status and 200 <= status < 300:
        print(f"  ✓ Test email sent successfully (status {status})")
        print(f"  Check {TEST_RECIPIENT} and {BCC_EMAIL}")
    else:
        print(f"  ✗ Failed to send: {error}")

    print()


# ─── Live Mode ───────────────────────────────────────────────────────────────

def send_live_update(dry_run=False, subject_override=None, content_override=None):
    """Send investor update to all active contacts."""
    supabase = get_supabase()
    contacts = fetch_active_contacts(supabase)

    subject = subject_override or "Vora Update - Feb 2026"
    update_content = content_override or WEEKLY_UPDATE

    mode_label = "DRY RUN" if dry_run else "LIVE SEND"

    print(f"\n{'=' * 60}")
    print(f"  VORA INVESTOR UPDATE — {mode_label}")
    print(f"  {len(contacts)} active contacts")
    print(f"  Subject: {subject}")
    print(f"{'=' * 60}\n")

    if not contacts:
        print("  No active contacts found. Insert contacts first.")
        return

    for i, c in enumerate(contacts):
        name = f"{c.get('first_name', '')} {c.get('last_name', '')}".strip()
        company = c.get('company') or '—'
        print(f"  [{i+1}/{len(contacts)}] {name} ({c['email']}) — {company}")

        if dry_run:
            continue

        html = build_investor_update_email(
            first_name=c.get('first_name', ''),
            subject=subject,
            update_content=update_content,
        )

        status, error = send_email(
            to_email=c['email'],
            subject=subject,
            html_content=html,
        )

        if status and 200 <= status < 300:
            print(f"           ✓ Sent (status {status})")
            log_email_sent(supabase, c['email'], subject, 'sent')
            update_last_emailed(supabase, c['email'])
        else:
            print(f"           ✗ Failed: {error}")
            log_email_sent(supabase, c['email'], subject, f'failed: {error}')

        time.sleep(SEND_DELAY)

    print(f"\n{'=' * 60}")
    if dry_run:
        print(f"  DRY RUN COMPLETE — no emails sent")
    else:
        print(f"  LIVE SEND COMPLETE — {len(contacts)} emails processed")
    print(f"{'=' * 60}\n")


# ─── Main ────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description='Vora Investor Update Emails')
    parser.add_argument('--test', action='store_true',
                        help='Send test email to test recipient')
    parser.add_argument('--live', action='store_true',
                        help='Send to all active investor contacts')
    parser.add_argument('--dry-run', action='store_true',
                        help='Show what would be sent without sending')
    parser.add_argument('--subject', type=str,
                        help='Override email subject line')
    args = parser.parse_args()

    if not SENDGRID_API_KEY:
        print("ERROR: SENDGRID_API_KEY not set in ../.env")
        sys.exit(1)

    if args.test:
        send_test_email()
    elif args.live:
        if not args.dry_run:
            confirm = input(f"\n  This will send LIVE emails to investor contacts. Type 'yes' to confirm: ")
            if confirm.strip().lower() != 'yes':
                print("  Aborted.")
                sys.exit(0)
        send_live_update(dry_run=args.dry_run, subject_override=args.subject)
    else:
        parser.print_help()


if __name__ == '__main__':
    main()
