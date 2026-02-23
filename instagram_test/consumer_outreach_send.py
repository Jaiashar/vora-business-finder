#!/usr/bin/env python3
"""
Production Consumer Outreach - Round 2.
Uses Matin SendGrid account. Fresh progress file.
Real-time bounce monitoring via SendGrid API - pauses if bounce rate spikes.
"""

import os, sys, json, time, urllib.request, urllib.parse, ssl
from dotenv import load_dotenv
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import (
    Mail, Email, To, Personalization, Content, Category, CustomArg,
    TrackingSettings, ClickTracking, OpenTracking, Ganalytics
)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(os.path.dirname(BASE_DIR), '.env'))

SENDGRID_API_KEY = os.environ.get('SENDGRID_API_KEY_MATIN', '')
SUPABASE_URL = os.environ['SUPABASE_URL']
SUPABASE_KEY = os.environ['SUPABASE_KEY']
SSL_CTX = ssl.create_default_context()

PROGRESS_FILE = os.path.join(BASE_DIR, 'outreach_progress_r2.json')

VORA = {
    'app_store': 'https://apps.apple.com/us/app/vora-health/id6754351240',
    'website': 'https://askvora.com',
    'ceo_name': 'Jai Ashar',
    'ceo_phone': '(949) 276-1808',
    'ceo_email': 'jai@askvora.com',
}

LOGO_URL = "https://askvora.com/iOS%20LOGO%204.png"

CATEGORY_TO_VARIANT = {
    'wearable_user': 'wearable',
    'wellness': 'wellness',
    'fitness_consumer': 'fitness',
    'fitness_influencer': 'fitness',
    'fitness_wearable': 'wearable',
    'athlete': 'fitness',
}

SEND_DELAY = 0.5
BATCH_SIZE = 50
BATCH_PAUSE = 10
BOUNCE_CHECK_EVERY = 100
MAX_BOUNCE_RATE = 0.03


def fetch_all_contacts():
    all_contacts = []
    batch = 1000
    for offset in range(0, 20000, batch):
        url = (f"{SUPABASE_URL}/rest/v1/consumer_leads"
               f"?select=email,name,category&limit={batch}&offset={offset}&order=email")
        req = urllib.request.Request(url, headers={
            "apikey": SUPABASE_KEY,
            "Authorization": f"Bearer {SUPABASE_KEY}",
        })
        resp = urllib.request.urlopen(req, context=SSL_CTX)
        data = json.loads(resp.read().decode())
        if not data:
            break
        all_contacts.extend(data)
    return all_contacts


def load_progress():
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE) as f:
            return json.load(f)
    return {"sent": [], "failed": [], "total_sent": 0, "total_failed": 0}


def save_progress(progress):
    with open(PROGRESS_FILE, 'w') as f:
        json.dump(progress, f)


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


def build_email_html(variant, first_name):
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

    sg = SendGridAPIClient(SENDGRID_API_KEY)
    response = sg.send(message)
    return response.status_code


def check_bounces(sent_emails):
    """Check how many of our sent emails have bounced."""
    try:
        url = "https://api.sendgrid.com/v3/suppression/bounces?limit=500"
        req = urllib.request.Request(url, headers={
            'Authorization': f'Bearer {SENDGRID_API_KEY}',
            'Content-Type': 'application/json',
        })
        resp = urllib.request.urlopen(req, context=SSL_CTX)
        bounces = json.loads(resp.read().decode())
        bounced_set = set(b['email'] for b in bounces)
        our_bounces = [e for e in sent_emails if e in bounced_set]
        return our_bounces
    except Exception as e:
        print(f"  [WARN] Could not check bounces: {e}")
        return []


def main():
    if not SENDGRID_API_KEY:
        print("ERROR: SENDGRID_API_KEY_MATIN not set in .env")
        sys.exit(1)

    print(f"\n{'='*60}")
    print(f"  VORA CONSUMER OUTREACH - ROUND 2 (Matin Account)")
    print(f"{'='*60}")

    # Load unsent contacts from pre-built file
    unsent_path = os.path.join(BASE_DIR, 'final_unsent.json')
    if not os.path.exists(unsent_path):
        print("ERROR: final_unsent.json not found. Run the prep step first.")
        sys.exit(1)
    contacts = json.load(open(unsent_path))
    print(f"\n  Loaded {len(contacts)} unsent contacts from final_unsent.json")

    progress = load_progress()
    already_sent_r2 = set(progress["sent"])
    print(f"  Already sent in R2: {len(already_sent_r2)}")

    to_send = [c for c in contacts if c['email'] not in already_sent_r2]
    print(f"  Remaining to send: {len(to_send)}")

    if not to_send:
        print("\n  Nothing to send! All contacts already emailed.")
        return

    print(f"\n  Pacing: {SEND_DELAY}s delay, {BATCH_PAUSE}s pause every {BATCH_SIZE}")
    print(f"  Bounce check every {BOUNCE_CHECK_EVERY} emails")
    print(f"{'='*60}\n")

    sent_count = 0
    fail_count = 0
    batch_count = 0
    this_run_sent = []

    for i, contact in enumerate(to_send):
        email = contact['email']
        name = contact.get('name') or ''
        category = contact.get('category', 'fitness_consumer')
        variant = CATEGORY_TO_VARIANT.get(category, 'fitness')

        subject = get_subject(variant, name)
        html = build_email_html(variant, name)

        categories = ['consumer_outreach_r2', f'variant_{variant}']
        custom_args = {
            'variant': variant,
            'has_name': 'yes' if name else 'no',
            'category': category,
        }

        try:
            status = send_email(email, subject, html, variant=variant,
                               categories=categories, custom_args=custom_args)

            if status and 200 <= status < 300:
                sent_count += 1
                this_run_sent.append(email)
                progress["sent"].append(email)
                progress["total_sent"] = len(progress["sent"])
            else:
                fail_count += 1
                progress["failed"].append(email)
                progress["total_failed"] = len(progress["failed"])
                print(f"  [{i+1}/{len(to_send)}] FAIL ({status}): {email}")

        except Exception as e:
            fail_count += 1
            progress["failed"].append(email)
            progress["total_failed"] = len(progress["failed"])
            err_msg = str(e)[:80]
            print(f"  [{i+1}/{len(to_send)}] ERROR: {email} - {err_msg}")

            if '429' in str(e) or 'Too Many' in str(e):
                print(f"  >> Rate limited! Pausing 60s...")
                save_progress(progress)
                time.sleep(60)
            elif '401' in str(e) or 'Unauthorized' in str(e):
                print(f"  >> API KEY REVOKED. Stopping.")
                save_progress(progress)
                sys.exit(1)

        batch_count += 1

        if batch_count % 10 == 0:
            save_progress(progress)

        if batch_count % BATCH_SIZE == 0:
            print(f"  >> [{batch_count}/{len(to_send)}] Sent: {sent_count} | Failed: {fail_count} | Pausing {BATCH_PAUSE}s...")
            save_progress(progress)
            time.sleep(BATCH_PAUSE)

        # Bounce logging (no stopping - just report)
        if batch_count % BOUNCE_CHECK_EVERY == 0 and sent_count > 0:
            time.sleep(5)
            our_bounces = check_bounces(this_run_sent)
            bounce_rate = len(our_bounces) / sent_count
            print(f"  >> Bounce check: {len(our_bounces)}/{sent_count} ({bounce_rate*100:.1f}%)")

        if batch_count % BATCH_SIZE != 0:
            time.sleep(SEND_DELAY)

    save_progress(progress)

    time.sleep(30)
    our_bounces = check_bounces(this_run_sent)

    print(f"\n{'='*60}")
    print(f"  DONE!")
    print(f"  Sent: {sent_count}")
    print(f"  API Failures: {fail_count}")
    print(f"  Bounces detected: {len(our_bounces)}")
    print(f"  Effective bounce rate: {len(our_bounces)/max(sent_count,1)*100:.1f}%")
    print(f"{'='*60}\n")

    if our_bounces:
        with open(os.path.join(BASE_DIR, 'r2_bounces.json'), 'w') as f:
            json.dump(our_bounces, f, indent=2)
        print(f"  Bounced emails saved to r2_bounces.json")


if __name__ == "__main__":
    main()
