#!/usr/bin/env python3
"""
Vora College Outreach - Follow-Up #1
Sends a short, plain-text-style follow-up to everyone who received the first email.

Usage:
    # Send test to yourself
    python college_followup.py --test

    # Dry run
    python college_followup.py --live --dry-run

    # Live send
    python college_followup.py --live

    # Live send with limit
    python college_followup.py --live --limit 1000
"""

import argparse
import os
import sys
import time
from datetime import datetime, timezone
from dotenv import load_dotenv
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import (
    Mail, Email, To, Bcc, Personalization, Content, Category, CustomArg,
    TrackingSettings, OpenTracking,
)
from supabase import create_client

load_dotenv()

SENDGRID_API_KEY = os.getenv('SENDGRID_API_KEY')
FROM_EMAIL = os.getenv('FROM_EMAIL', 'jai@askvora.com')
BCC_EMAIL = os.getenv('BCC_EMAIL', 'jai@askvora.com')
TEST_RECIPIENT = os.getenv('TEST_RECIPIENT', 'jaikrish15@gmail.com')
SUPABASE_URL = os.getenv('SUPABASE_URL')
SUPABASE_KEY = os.getenv('SUPABASE_KEY')

SEND_DELAY = 0.03
BATCH_LOG_INTERVAL = 100

VORA = {
    'app_store': 'https://apps.apple.com/us/app/vora-health/id6754351240',
    'website': 'https://askvora.com',
    'calendly': 'https://calendly.com/jai-askvora/30min',
    'ceo_name': 'Jai Ashar',
    'ceo_phone': '(949) 276-1808',
    'ceo_email': 'jai@askvora.com',
}

LOGO_URL = "https://askvora.com/iOS%20LOGO%204.png"
APP_STORE_ICON = "https://upload.wikimedia.org/wikipedia/commons/thumb/6/67/App_Store_%28iOS%29.svg/120px-App_Store_%28iOS%29.svg.png"


# ── Original subject lines (to build Re: subjects) ──────────────────────────

def get_original_subject(variant, first_name, university):
    """Reconstruct the original subject line so we can prepend Re:"""
    if variant in ('A1', 'A2', 'A3'):
        if first_name:
            return f"{first_name}, {university} students get early access to Vora"
        return f"{university} students get early access to Vora"
    elif variant in ('B1', 'B2', 'B3'):
        if first_name:
            return f"{first_name}, your free AI health coach is here"
        return "Your free AI health coach is here"
    elif variant == 'grad':
        if first_name:
            return f"{first_name}, a free AI health coach built for your schedule"
        return "A free AI health coach built for your schedule"
    elif variant == 'coach':
        if first_name:
            return f"{first_name}, a free health tool for your {university} athletes"
        return f"A free AI health tool for {university} athletes"
    return "Vora"


# ── Follow-up email bodies ───────────────────────────────────────────────────

def _shared_links_and_signature():
    """Links box + logo signature block shared across all variants."""
    return f"""\
    <div style="margin: 24px 0; padding: 16px 20px; background: #f8f9fa; border-radius: 10px;">
        <table cellpadding="0" cellspacing="0" border="0" style="width: 100%;">
            <tr>
                <td style="padding: 6px 0;">
                    <table cellpadding="0" cellspacing="0" border="0"><tr>
                        <td style="vertical-align: middle; padding-right: 10px;"><img src="{APP_STORE_ICON}" alt="App Store" width="20" height="20" style="display: block;" /></td>
                        <td style="vertical-align: middle; font-size: 14px;"><a href="{VORA['app_store']}" style="color: #0066cc; text-decoration: none;">Download free on the App Store</a></td>
                    </tr></table>
                </td>
            </tr>
            <tr>
                <td style="padding: 6px 0;">
                    <table cellpadding="0" cellspacing="0" border="0"><tr>
                        <td style="vertical-align: middle; padding-right: 10px;"><img src="{LOGO_URL}" alt="Vora" width="20" height="20" style="display: block; border-radius: 4px;" /></td>
                        <td style="vertical-align: middle; font-size: 14px;"><a href="{VORA['website']}" style="color: #0066cc; text-decoration: none;">askvora.com</a></td>
                    </tr></table>
                </td>
            </tr>
        </table>
    </div>

    <p>Questions? Reply to this email, text me at <a href="tel:+19492761808" style="color: #0066cc; text-decoration: none;">{VORA['ceo_phone']}</a>, or <a href="{VORA['calendly']}" style="color: #0066cc; text-decoration: none;">grab a time on my calendar</a>.</p>

    <p>Jai</p>

    <div style="margin-top: 16px; padding-top: 16px; border-top: 1px solid #e0e0e0;">
        <table cellpadding="0" cellspacing="0" border="0">
            <tr>
                <td style="vertical-align: top; padding-right: 14px;">
                    <img src="{LOGO_URL}" alt="Vora" width="48" height="48" style="border-radius: 10px; display: block;" />
                </td>
                <td style="vertical-align: top;">
                    <p style="margin: 0; font-weight: 600; font-size: 15px;">{VORA['ceo_name']}</p>
                    <p style="margin: 2px 0; color: #555; font-size: 13px;">Founder & CEO, <a href="{VORA['website']}" style="color: #0066cc; text-decoration: none;">Vora</a></p>
                    <p style="margin: 2px 0; color: #555; font-size: 13px;"><a href="tel:+19492761808" style="color: #555; text-decoration: none;">{VORA['ceo_phone']}</a></p>
                    <p style="margin: 2px 0; color: #555; font-size: 13px;"><a href="mailto:{VORA['ceo_email']}" style="color: #555; text-decoration: none;">{VORA['ceo_email']}</a></p>
                </td>
            </tr>
        </table>
    </div>

    <div style="margin-top: 32px; padding-top: 16px; border-top: 1px solid #eee; font-size: 11px; color: #999; line-height: 1.5;">
        <p style="margin: 0;">Vora AI Inc · San Francisco, CA</p>
        <p style="margin: 4px 0 0 0;">Don't want to hear from us? <a href="mailto:{VORA['ceo_email']}?subject=Unsubscribe" style="color: #999; text-decoration: underline;">Reply unsubscribe</a> and we'll remove you right away.</p>
    </div>"""


def build_followup_body(audience, first_name, university):
    """
    Build a short, plain-text-style follow-up email.
    audience: 'undergrad', 'grad', or 'coach'
    """
    greeting = f"Hey {first_name}," if first_name else "Hey,"
    shared = _shared_links_and_signature()

    if audience == 'undergrad':
        body = f"""\
<div style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif; max-width: 600px; margin: 0 auto; color: #1a1a1a; line-height: 1.7; font-size: 15px;">

    <p>{greeting}</p>

    <p>Sent you an email last week about Vora and figured it probably got lost in your inbox. Wanted to follow up because we're getting a lot of interest from {university} students right now.</p>

    <p>The short version: Vora is a free AI health app that connects to your Apple Watch, Whoop, Garmin, or whatever wearable you use and builds you a real plan. Nutrition, workouts, sleep, recovery, all personalized to you. Not a generic template.</p>

    <p>If you're even a little curious, it takes about 2 minutes to set up and it's completely free. No trial, no credit card.</p>

{shared}

</div>"""

    elif audience == 'grad':
        body = f"""\
<div style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif; max-width: 600px; margin: 0 auto; color: #1a1a1a; line-height: 1.7; font-size: 15px;">

    <p>{greeting}</p>

    <p>Sent you a note last week about Vora. I know grad school inboxes are brutal so just wanted to bump this.</p>

    <p>Vora is a free AI health app that connects to your wearable and handles the stuff you probably don't have time to think about: what to eat, when to work out, how to sleep better. It adapts to your schedule, not the other way around.</p>

    <p>Takes about 2 minutes to set up. Completely free, no catch.</p>

{shared}

</div>"""

    elif audience == 'coach':
        body = f"""\
<div style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif; max-width: 600px; margin: 0 auto; color: #1a1a1a; line-height: 1.7; font-size: 15px;">

    <p>{greeting}</p>

    <p>Wanted to follow up on my email last week about Vora. We're a free AI health app built for college athletes. Personalized nutrition, smart workout tracking, recovery insights, and sleep monitoring, all connected to their wearable.</p>

    <p>A few {university} students are already checking it out. Would love to get it in front of your athletes too.</p>

{shared}

</div>"""

    return body


# ── Email Sending ────────────────────────────────────────────────────────────

def send_email(to_email, subject, html_content, bcc_email=None,
               categories=None, custom_args=None):
    """Send email via SendGrid. Retries once on rate limit."""
    message = Mail()
    message.from_email = Email(FROM_EMAIL, VORA['ceo_name'])
    message.subject = subject

    personalization = Personalization()
    personalization.add_to(To(to_email))
    if bcc_email:
        personalization.add_bcc(Bcc(bcc_email))

    if custom_args:
        for key, value in custom_args.items():
            personalization.add_custom_arg(CustomArg(key=key, value=str(value)))

    message.add_personalization(personalization)
    message.add_content(Content('text/html', html_content))

    message.tracking_settings = TrackingSettings(
        open_tracking=OpenTracking(enable=False),
    )

    if categories:
        for cat in categories:
            message.add_category(Category(cat))

    for attempt in range(2):
        try:
            sg = SendGridAPIClient(SENDGRID_API_KEY)
            response = sg.send(message)
            return response.status_code, None
        except Exception as e:
            error_msg = str(e)
            if attempt == 0 and ('429' in error_msg or 'rate' in error_msg.lower()):
                time.sleep(2)
                continue
            return None, error_msg

    return None, "Max retries exceeded"


# ── Supabase Helpers ─────────────────────────────────────────────────────────

def get_supabase():
    if not SUPABASE_URL or not SUPABASE_KEY:
        print("ERROR: SUPABASE_URL and SUPABASE_KEY must be set in .env")
        sys.exit(1)
    return create_client(SUPABASE_URL, SUPABASE_KEY)


def fetch_first_round_recipients(supabase):
    """Fetch everyone who was sent the first round email and hasn't received follow-up 1."""
    all_recipients = []
    page_size = 1000
    offset = 0

    while True:
        response = (
            supabase.table('college_outreach_sent')
            .select('id,email,university,variant,first_name_used')
            .eq('status', 'sent')
            .is_('followup_1_at', 'null')
            .is_('unsubscribed_at', 'null')
            .range(offset, offset + page_size - 1)
            .execute()
        )
        all_recipients.extend(response.data)
        if len(response.data) < page_size:
            break
        offset += page_size
        print(f"    ...fetched {len(all_recipients)} so far", flush=True)

    return all_recipients


def mark_followup_sent(supabase, record_id):
    """Mark a record as having received follow-up 1."""
    try:
        supabase.table('college_outreach_sent').update({
            'followup_1_at': datetime.now(timezone.utc).isoformat(),
        }).eq('id', record_id).execute()
    except Exception:
        pass


def determine_audience(variant):
    """Map original variant to audience segment."""
    if variant == 'coach':
        return 'coach'
    elif variant == 'grad':
        return 'grad'
    return 'undergrad'


# ── Test Mode ────────────────────────────────────────────────────────────────

def send_test():
    """Send all 3 follow-up variants (undergrad, grad, coach) to test recipient."""
    test_university = 'UCLA'
    test_first_name = 'John'

    tests = [
        {
            'audience': 'undergrad',
            'variant': 'A1',
            'label': 'Undergrad Follow-Up (Re: Subject A)',
        },
        {
            'audience': 'grad',
            'variant': 'grad',
            'label': 'Grad Student Follow-Up',
        },
        {
            'audience': 'coach',
            'variant': 'coach',
            'label': 'Coach Follow-Up',
        },
    ]

    print(f"\n{'='*60}")
    print(f"  VORA COLLEGE FOLLOW-UP - TEST MODE")
    print(f"  Sending {len(tests)} variant(s) to: {TEST_RECIPIENT}")
    print(f"{'='*60}\n")

    for t in tests:
        original_subject = get_original_subject(t['variant'], test_first_name, test_university)
        subject = f"Re: {original_subject}"

        html = build_followup_body(t['audience'], test_first_name, test_university)

        categories = ['college_followup', 'test', f'audience_{t["audience"]}']
        custom_args = {
            'campaign': 'college_followup_1',
            'audience': t['audience'],
            'university': test_university,
        }

        print(f"  Sending: {t['label']}")
        print(f"    Subject: {subject}")

        status, error = send_email(
            to_email=TEST_RECIPIENT,
            subject=subject,
            html_content=html,
            categories=categories,
            custom_args=custom_args,
        )

        if status and 200 <= status < 300:
            print(f"    Sent (status {status})\n")
        else:
            print(f"    Failed: {error}\n")

        time.sleep(1)

    print(f"  Done. Check {TEST_RECIPIENT} for all 3 variants.\n")


# ── Live Send ────────────────────────────────────────────────────────────────

def run_live(dry_run=False, limit=0, auto_confirm=False):
    """Send follow-ups to all first-round recipients."""
    supabase = get_supabase()

    print(f"\n{'='*60}", flush=True)
    print(f"  VORA COLLEGE FOLLOW-UP - PREPARING SEND LIST", flush=True)
    print(f"{'='*60}\n", flush=True)

    print("  Fetching first-round recipients...", flush=True)
    recipients = fetch_first_round_recipients(supabase)
    print(f"  Found {len(recipients)} first-round recipients", flush=True)

    if limit and limit > 0:
        recipients = recipients[:limit]
        print(f"  Limited to {limit}")

    # Validate: every record must have a variant we recognize
    valid_variants = {'A1', 'A2', 'A3', 'B1', 'B2', 'B3', 'grad', 'coach'}
    bad = [r for r in recipients if r.get('variant') not in valid_variants]
    if bad:
        print(f"\n  ERROR: {len(bad)} records have unrecognized variants. Aborting.")
        for b in bad[:10]:
            print(f"    {b['email']} -> variant={b.get('variant')}")
        sys.exit(1)

    audience_counts = {'undergrad': 0, 'grad': 0, 'coach': 0}
    school_counts = {}
    for r in recipients:
        aud = determine_audience(r['variant'])
        audience_counts[aud] += 1
        uni = r.get('university') or 'unknown'
        school_counts[uni] = school_counts.get(uni, 0) + 1

    print(f"\n  Audience breakdown:")
    print(f"    Undergrad: {audience_counts['undergrad']}")
    print(f"    Grad/PhD:  {audience_counts['grad']}")
    print(f"    Coach:     {audience_counts['coach']}")

    print(f"\n  By school:")
    for school, count in sorted(school_counts.items(), key=lambda x: -x[1]):
        print(f"    {school}: {count}")

    total = len(recipients)
    mode_label = "DRY RUN" if dry_run else "LIVE SEND"

    print(f"\n{'='*60}")
    print(f"  {mode_label}: {total} follow-up emails")
    print(f"  SendGrid key: SENDGRID_API_KEY (Jai)")
    print(f"{'='*60}\n")

    if not dry_run:
        if not auto_confirm:
            confirm = input(f"  Ready to send {total} follow-up emails LIVE. Type 'yes' to confirm: ")
            if confirm.strip().lower() != 'yes':
                print("  Aborted.")
                sys.exit(0)
        else:
            print(f"  --confirm flag set. Sending {total} emails LIVE.")
        print("  Starting in 5 seconds...")
        time.sleep(5)

    sent = 0
    failed = 0
    start_time = time.time()

    for i, r in enumerate(recipients):
        email = r['email']
        record_id = r.get('id')
        university = r['university'] or ''
        variant = r['variant'] or 'A1'
        first_name = r.get('first_name_used', '') or ''
        audience = determine_audience(variant)

        original_subject = get_original_subject(variant, first_name, university)
        subject = f"Re: {original_subject}"

        if dry_run:
            if i < 20 or i % 1000 == 0:
                print(f"  [{i+1}/{total}] {audience} | {university} | {first_name or '(no name)'} | {email}", flush=True)
                print(f"           Subject: {subject}", flush=True)
            continue

        html = build_followup_body(audience, first_name, university)

        categories = ['college_followup', f'audience_{audience}']
        if university:
            categories.append(university.lower().replace(' ', '_'))

        custom_args = {
            'campaign': 'college_followup_1',
            'audience': audience,
            'university': university or 'unknown',
            'original_variant': variant,
        }

        status, error = send_email(
            to_email=email,
            subject=subject,
            html_content=html,
            categories=categories,
            custom_args=custom_args,
        )

        if status and 200 <= status < 300:
            sent += 1
            mark_followup_sent(supabase, record_id)
        else:
            failed += 1

        if (i + 1) % BATCH_LOG_INTERVAL == 0:
            elapsed = time.time() - start_time
            rate = (i + 1) / elapsed if elapsed > 0 else 0
            eta_seconds = (total - i - 1) / rate if rate > 0 else 0
            eta_min = int(eta_seconds // 60)
            eta_sec = int(eta_seconds % 60)
            print(f"  [{i+1}/{total}] sent={sent} failed={failed} "
                  f"rate={rate:.1f}/sec ETA={eta_min}m{eta_sec}s", flush=True)

        time.sleep(SEND_DELAY)

    elapsed = time.time() - start_time
    elapsed_min = int(elapsed // 60)
    elapsed_sec = int(elapsed % 60)

    print(f"\n{'='*60}")
    print(f"  {mode_label} COMPLETE")
    print(f"{'='*60}")
    if dry_run:
        print(f"  Would send: {total} emails")
    else:
        print(f"  Sent: {sent}")
        print(f"  Failed: {failed}")
        print(f"  Time: {elapsed_min}m {elapsed_sec}s")
        if elapsed > 0:
            print(f"  Rate: {sent / elapsed:.1f} emails/sec")
    print()


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description='Vora College Outreach Follow-Up #1')
    parser.add_argument('--test', action='store_true', help='Send test variants to test recipient')
    parser.add_argument('--live', action='store_true', help='Send to all first-round recipients')
    parser.add_argument('--dry-run', action='store_true', help='Show what would be sent without sending')
    parser.add_argument('--confirm', action='store_true', help='Skip confirmation prompt')
    parser.add_argument('--limit', type=int, default=0, help='Limit number of emails')

    args = parser.parse_args()

    if not SENDGRID_API_KEY:
        print("ERROR: SENDGRID_API_KEY not set in .env")
        sys.exit(1)

    if args.test:
        send_test()
    elif args.live:
        run_live(dry_run=args.dry_run, limit=args.limit, auto_confirm=args.confirm)
    else:
        parser.print_help()


if __name__ == '__main__':
    main()
