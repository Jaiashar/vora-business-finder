#!/usr/bin/env python3
"""
Vora Investor Update - March 6, 2026
Usage:
    python send_march6_update.py --test     # send to test recipient only
    python send_march6_update.py --live     # send to all active investor contacts
"""

import argparse
import os
import sys
import time
from dotenv import load_dotenv
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail, Email, To, Personalization, Content

load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '.env'))

SENDGRID_API_KEY = os.getenv('SENDGRID_API_KEY_MATIN') or os.getenv('SENDGRID_API_KEY')
FROM_EMAIL     = os.getenv('FROM_EMAIL', 'jai@askvora.com')
TEST_RECIPIENT = 'jaikrish15@gmail.com'

VORA = {
    'website':   'https://askvora.com',
    'app_store': 'https://apps.apple.com/us/app/vora-health/id6754351240',
    'calendly':  'https://calendly.com/jai-askvora/30min',
    'ceo_phone': '(949) 276-1808',
    'ceo_email': 'jai@askvora.com',
}

SUBJECT = "Vora Update - March 6"

UPDATE_CONTENT = f"""\
<p>Cal AI, the AI calorie tracking app that did $40M in revenue last year, was just acquired by MyFitnessPal (nutrition tracker). Cal AI's sale validates the growing demand for AI-driven convenience in everyday health decisions. This is core to Vora's vision and our newly released app aims to provide a comprehensive health solution natively.</p>

<p>We shipped the best version of Vora this week. DAUs are between 70 and 100, and we have closed 2 investments into the round. I genuinely want you to try it: <a href="{VORA['app_store']}" style="color: #222;">download here.</a></p>

<p>A few things from our users that stood out:</p>

<ul style="padding-left: 20px; margin: 8px 0 20px 0;">
    <li style="margin-bottom: 6px;">A user reached out to migrate their full history out of MyFitnessPal into Vora.</li>
    <li style="margin-bottom: 6px;">Another user switched from Bevel, an all-in-one health app with a similar vision to Vora, to us instead.</li>
    <li style="margin-bottom: 6px;">We are getting a wave of Android users asking when they can get access. Android ships end of March.</li>
</ul>

<p>We are closing the round end of March. If anyone in your network should know about Vora, an intro would go a long way. And after you try the app, would love to hear what you think.</p>

<p><a href="{VORA['calendly']}" style="color: #222;">Grab time here</a> if easier.</p>"""


def build_email(first_name="there", test_mode=False):
    greeting = f"Hi {first_name},"

    test_banner = ""
    if test_mode:
        test_banner = (
            '<div style="background: #fff3cd; border: 1px solid #ffc107; '
            'border-radius: 6px; padding: 10px 14px; margin-bottom: 16px; '
            'font-size: 12px; color: #856404;">TEST MODE</div>'
        )

    signature = f"""\
    <p style="margin-top: 28px;">Best,<br/>Jai, Matin, TC</p>
    <p style="margin: 4px 0; color: #444; font-size: 13px;">
        Jai Ashar, Founder &amp; CEO · <a href="{VORA['website']}" style="color: #444;">Vora Health</a><br/>
        {VORA['ceo_phone']} · <a href="mailto:{VORA['ceo_email']}" style="color: #444;">{VORA['ceo_email']}</a>
    </p>"""

    footer = f"""\
    <div style="margin-top: 36px; padding-top: 12px; border-top: 1px solid #ddd; font-size: 11px; color: #999; line-height: 1.5;">
        You are receiving this as a potential stakeholder of Vora. <a href="mailto:{VORA['ceo_email']}?subject=Unsubscribe" style="color: #999;">Unsubscribe</a>
    </div>"""

    html = f"""\
<div style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif; max-width: 600px; margin: 0 auto; color: #222; line-height: 1.75; font-size: 15px;">

{test_banner}

<p style="margin-top: 0;">{greeting}</p>

{UPDATE_CONTENT}

{signature}

{footer}

</div>"""
    return html


def send_email(to_email, html):
    message = Mail()
    message.from_email = Email(FROM_EMAIL, "Jai Ashar")
    message.subject = SUBJECT

    p = Personalization()
    p.add_to(To(to_email))
    message.add_personalization(p)
    message.add_content(Content('text/html', html))

    try:
        sg = SendGridAPIClient(SENDGRID_API_KEY)
        r  = sg.send(message)
        return r.status_code, None
    except Exception as e:
        return None, str(e)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--test', action='store_true')
    parser.add_argument('--live', action='store_true')
    parser.add_argument('--yes',  action='store_true', help='Skip confirmation')
    args = parser.parse_args()

    if not SENDGRID_API_KEY:
        print("ERROR: No SendGrid API key found.")
        sys.exit(1)

    if args.test:
        html = build_email(first_name="Jai", test_mode=True)
        status, error = send_email(TEST_RECIPIENT, html)
        if status and 200 <= status < 300:
            print(f"Sent to {TEST_RECIPIENT} (status {status})")
        else:
            print(f"Failed: {error}")

    elif args.live:
        # Pull contacts from Supabase investor_contacts table
        from supabase import create_client
        supabase_url = os.getenv('SUPABASE_URL')
        supabase_key = os.getenv('SUPABASE_KEY')
        sb = create_client(supabase_url, supabase_key)

        contacts = sb.table('investor_contacts').select('*').eq('is_active', True).execute().data
        print(f"\nSending to {len(contacts)} contacts...\n")

        if not args.yes:
            confirm = input("Type 'yes' to confirm live send: ")
            if confirm.strip().lower() != 'yes':
                print("Aborted.")
                sys.exit(0)

        for i, c in enumerate(contacts):
            first_name = c.get('first_name') or 'there'
            email      = c.get('email')
            if not email:
                continue
            html = build_email(first_name=first_name)
            status, error = send_email(email, html)
            name = f"{c.get('first_name','')} {c.get('last_name','')}".strip()
            print(f"  [{i+1}/{len(contacts)}] {name} ({email}) - {'Sent' if status and status < 300 else f'Failed: {error}'}")
            time.sleep(1)

    else:
        parser.print_help()


if __name__ == '__main__':
    main()
