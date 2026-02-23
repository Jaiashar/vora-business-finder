#!/usr/bin/env python3
"""
Consumer Email Templates v4 - Personal story, honest, real.
Jai's knee surgeries + weight loss as the origin story.
~100 users and growing. Free Vora Pro offer.
"""

import os, sys, time
from dotenv import load_dotenv
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import (
    Mail, Email, To, Personalization, Content, Category, CustomArg,
    TrackingSettings, ClickTracking, OpenTracking, Ganalytics
)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(os.path.dirname(BASE_DIR), '.env'))

SENDGRID_API_KEY = os.environ.get('SENDGRID_API_KEY', '')
TEST_TO = 'jai@askvora.com'

VORA = {
    'app_store': 'https://apps.apple.com/us/app/vora-health/id6754351240',
    'website': 'https://askvora.com',
    'ceo_name': 'Jai Ashar',
    'ceo_phone': '(949) 276-1808',
    'ceo_email': 'jai@askvora.com',
}

LOGO_URL = "https://askvora.com/iOS%20LOGO%204.png"


def get_subject(variant, first_name):
    subjects = {
        'wearable': ("App for your Oura/Garmin/Whoop", "{name}, app for your Oura/Garmin/Whoop"),
        'fitness':  ("Free AI health coach", "{name}, free AI health coach"),
        'wellness': ("Free health coach app", "{name}, free health coach app"),
    }
    no_name, with_name = subjects[variant]
    if first_name:
        return with_name.replace("{name}", first_name)
    return no_name


def build_body(variant, first_name):
    greeting = f"Hey {first_name}," if first_name else "Hey,"

    if variant == 'wearable':
        body = f"""\
    <p style="margin-top: 0;">{greeting}</p>

    <p>I'm Jai. I went through two knee surgeries and worked my way back to becoming a competitive athlete. Through that whole process, I got frustrated that my wearable was collecting all this data but not actually helping me recover or perform better. So I built <a href="{VORA['website']}" style="color: #0066cc; text-decoration: none;">Vora</a>.</p>

    <p>It connects to your Oura, Garmin, Apple Watch, Whoop, Fitbit, whatever you're wearing, and turns all that data into an actual daily plan. What to eat, how hard to train, when to rest. It changes every day based on how your body's doing.</p>

    <p>We have about a hundred users right now and we're growing. I'd love for you to try it. I'll give you <strong>Vora Pro completely free</strong>, no trial, no expiration. Just download the app and reply to this email with your account email so I can upgrade you.</p>

    <p><a href="{VORA['app_store']}" style="color: #0066cc; text-decoration: none;">Download Vora on the App Store</a></p>

    <p>If you have any questions, reply here or text me at {VORA['ceo_phone']}.</p>"""

    elif variant == 'fitness':
        body = f"""\
    <p style="margin-top: 0;">{greeting}</p>

    <p>I'm Jai. I went through two knee surgeries and worked my way back to becoming a competitive athlete. Through all of that, I got tired of juggling different apps to track my workouts, food, and recovery. None of them talked to each other. So I built <a href="{VORA['website']}" style="color: #0066cc; text-decoration: none;">Vora</a>.</p>

    <p>It's an AI health coach that puts everything in one place. You can log sets by voice mid-workout, snap a photo of your food for macros, and it adjusts tomorrow's plan based on how you're recovering today. Everything actually connects.</p>

    <p>We have about a hundred users right now and we're growing. I'd love for you to try it. I'll give you <strong>Vora Pro completely free</strong>, no trial, no expiration. Download the app and reply with your account email so I can upgrade you.</p>

    <p><a href="{VORA['app_store']}" style="color: #0066cc; text-decoration: none;">Download Vora on the App Store</a></p>

    <p>Questions? Reply here or text me at {VORA['ceo_phone']}.</p>"""

    elif variant == 'wellness':
        body = f"""\
    <p style="margin-top: 0;">{greeting}</p>

    <p>I'm Jai. I went through two knee surgeries and worked my way back to becoming a competitive athlete. I tried a bunch of apps and programs along the way, and none of them really worked because they didn't adapt to my life. So I built <a href="{VORA['website']}" style="color: #0066cc; text-decoration: none;">Vora</a>.</p>

    <p>It's a health coach that builds you a personalized daily plan for meals, movement, sleep, and stress. You tell it your goals and it figures out the rest. No rigid programs, no calorie obsessing. It just meets you where you are and adjusts every day.</p>

    <p>We have about a hundred users right now and we're growing. I'd love for you to try it. I'll give you <strong>Vora Pro completely free</strong>, no trial, no expiration. Download the app and reply with your account email so I can upgrade you.</p>

    <p><a href="{VORA['app_store']}" style="color: #0066cc; text-decoration: none;">Download Vora on the App Store</a></p>

    <p>If you have any questions at all, reply here or text me at {VORA['ceo_phone']}.</p>"""

    return body


def build_email_html(variant, first_name, test_label=None):
    test_banner = ""
    if test_label:
        test_banner = (
            f'<div style="background: #fff3cd; border: 1px solid #ffc107; '
            f'border-radius: 8px; padding: 12px 16px; margin-bottom: 20px; '
            f'font-size: 13px; color: #856404;">'
            f'TEST v4 - Variant <strong>{test_label}</strong></div>'
        )

    body = build_body(variant, first_name)

    signature = f"""\
    <div style="margin-top: 24px; padding-top: 16px; border-top: 1px solid #e0e0e0;">
        <table cellpadding="0" cellspacing="0" border="0">
            <tr>
                <td style="vertical-align: top; padding-right: 14px;">
                    <img src="{LOGO_URL}" alt="Vora" width="44" height="44" style="border-radius: 10px; display: block;" />
                </td>
                <td style="vertical-align: top;">
                    <p style="margin: 0; font-weight: 600; font-size: 15px;">{VORA['ceo_name']}</p>
                    <p style="margin: 2px 0; color: #555; font-size: 13px;">Founder & CEO, <a href="{VORA['website']}" style="color: #0066cc; text-decoration: none;">Vora</a></p>
                    <p style="margin: 2px 0; color: #555; font-size: 13px;"><a href="tel:+19492761808" style="color: #555; text-decoration: none;">{VORA['ceo_phone']}</a></p>
                    <p style="margin: 2px 0; color: #555; font-size: 13px;"><a href="mailto:{VORA['ceo_email']}" style="color: #555; text-decoration: none;">{VORA['ceo_email']}</a></p>
                </td>
            </tr>
        </table>
    </div>"""

    footer = f"""\
    <div style="margin-top: 32px; padding-top: 16px; border-top: 1px solid #eee; font-size: 11px; color: #999; line-height: 1.5;">
        <p style="margin: 0;">Vora AI Inc &middot; San Francisco, CA</p>
        <p style="margin: 4px 0 0 0;">Don't want these emails? <a href="mailto:{VORA['ceo_email']}?subject=Unsubscribe" style="color: #999; text-decoration: underline;">Just reply unsubscribe</a>.</p>
    </div>"""

    html = f"""\
<div style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif; max-width: 600px; margin: 0 auto; color: #1a1a1a; line-height: 1.7; font-size: 15px;">

    {test_banner}

{body}

{signature}

{footer}

</div>"""

    return html


def send_email(to_email, subject, html, variant='', categories=None, custom_args=None):
    message = Mail()
    message.from_email = Email(VORA['ceo_email'], VORA['ceo_name'])
    message.subject = subject

    personalization = Personalization()
    personalization.add_to(To(to_email))
    if custom_args:
        for k, v in custom_args.items():
            personalization.add_custom_arg(CustomArg(key=k, value=str(v)))
    message.add_personalization(personalization)

    message.add_content(Content('text/html', html))

    if categories:
        for cat in categories:
            message.add_category(Category(cat))

    tracking = TrackingSettings()
    tracking.click_tracking = ClickTracking(enable=True, enable_text=False)
    tracking.open_tracking = OpenTracking(enable=True)
    tracking.ganalytics = Ganalytics(
        enable=True,
        utm_source='sendgrid',
        utm_medium='email',
        utm_campaign='consumer_outreach',
        utm_content=variant or 'general',
    )
    message.tracking_settings = tracking

    try:
        sg = SendGridAPIClient(SENDGRID_API_KEY)
        response = sg.send(message)
        return response.status_code
    except Exception as e:
        print(f"    ERROR: {str(e)[:80]}")
        return None


def main():
    if not SENDGRID_API_KEY:
        print("ERROR: SENDGRID_API_KEY not set in .env")
        sys.exit(1)

    print(f"\n{'='*60}")
    print(f"  VORA CONSUMER OUTREACH - TEST v4 (personal story)")
    print(f"  Sending 6 variants to: {TEST_TO}")
    print(f"{'='*60}\n")

    variants = [
        ('wearable', 'Sarah',  'WEARABLE + Name'),
        ('wearable', '',       'WEARABLE + No Name'),
        ('fitness',  'Mike',   'FITNESS + Name'),
        ('fitness',  '',       'FITNESS + No Name'),
        ('wellness', 'Rachel', 'WELLNESS + Name'),
        ('wellness', '',       'WELLNESS + No Name'),
    ]

    for variant, name, label in variants:
        subject = get_subject(variant, name)
        html = build_email_html(variant, name, test_label=label)

        categories = ['consumer_outreach', 'test_v4', f'variant_{variant}']
        custom_args = {'variant': variant, 'has_name': 'yes' if name else 'no'}

        test_subject = f"[TEST v4 - {label}] {subject}"

        print(f"  Sending: {label}")
        print(f"    Subject: {test_subject}")

        status = send_email(TEST_TO, test_subject, html, variant=variant,
                           categories=categories, custom_args=custom_args)

        if status and 200 <= status < 300:
            print(f"    Sent ({status})\n")
        else:
            print(f"    Failed\n")

        time.sleep(1)

    print(f"{'='*60}")
    print(f"  Done! Check {TEST_TO} for v4 variants.")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
