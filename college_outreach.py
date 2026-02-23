#!/usr/bin/env python3
"""
Vora College Outreach Campaign
Sends personalized emails to college contacts with A/B test variants.

Usage:
    # Send test variants to yourself
    python college_outreach.py --test

    # Send test for a specific variant only
    python college_outreach.py --test --variant A1

    # Dry run: show what would be sent without actually sending
    python college_outreach.py --live --dry-run

    # Dry run with limit
    python college_outreach.py --live --dry-run --limit 50

    # Live mode: send to actual college contacts
    python college_outreach.py --live

    # Live mode with limit
    python college_outreach.py --live --limit 1000
"""

import argparse
import os
import re
import sys
import time
import random
import json
from datetime import datetime, timezone
from dotenv import load_dotenv
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import (
    Mail, Email, To, Bcc, Personalization, Content, Category, CustomArg
)
from supabase import create_client

load_dotenv()

# ─── Configuration ───────────────────────────────────────────────────────────

SENDGRID_API_KEY = os.getenv('SENDGRID_API_KEY')
FROM_EMAIL = os.getenv('FROM_EMAIL', 'jai@askvora.com')
BCC_EMAIL = os.getenv('BCC_EMAIL', 'jai@askvora.com')
TEST_RECIPIENT = os.getenv('TEST_RECIPIENT', 'jaikrish15@gmail.com')
SUPABASE_URL = os.getenv('SUPABASE_URL')
SUPABASE_KEY = os.getenv('SUPABASE_KEY')

SEND_DELAY = 0.03  # seconds between sends (~33/sec)
BATCH_LOG_INTERVAL = 100  # print progress every N emails

# ─── Vora Info ───────────────────────────────────────────────────────────────

VORA = {
    'youtube': 'https://www.youtube.com/watch?v=SSrdtq7LTl4',
    'app_store': 'https://apps.apple.com/us/app/vora-health/id6754351240',
    'website': 'https://askvora.com',
    'calendly': 'https://calendly.com/jai-askvora/30min',
    'ceo_name': 'Jai Ashar',
    'ceo_phone': '(949) 276-1808',
    'ceo_email': 'jai@askvora.com',
    'twitter': 'https://x.com/JaiAshar',
    'instagram': 'https://www.instagram.com/askvora/',
    'linkedin': 'https://www.linkedin.com/in/jaiashar/',
}

LOGO_URL = "https://askvora.com/iOS%20LOGO%204.png"
YOUTUBE_ICON = "https://upload.wikimedia.org/wikipedia/commons/thumb/0/09/YouTube_full-color_icon_%282017%29.svg/120px-YouTube_full-color_icon_%282017%29.svg.png"
APP_STORE_ICON = "https://upload.wikimedia.org/wikipedia/commons/thumb/6/67/App_Store_%28iOS%29.svg/120px-App_Store_%28iOS%29.svg.png"

# ─── School Branding ─────────────────────────────────────────────────────────

SCHOOL_BRANDING = {
    'NYU': {
        'color': '#57068C',
        'identity': 'Violet',
        'full_name': 'New York University',
    },
    'Columbia': {
        'color': '#B9D9EB',
        'identity': 'Lion',
        'full_name': 'Columbia University',
    },
    'UCLA': {
        'color': '#2774AE',
        'identity': 'Bruin',
        'full_name': 'UCLA',
    },
    'USC': {
        'color': '#990000',
        'identity': 'Trojan',
        'full_name': 'USC',
    },
    'Ohio State': {
        'color': '#BB0000',
        'identity': 'Buckeye',
        'full_name': 'The Ohio State University',
    },
    'Texas A&M': {
        'color': '#500000',
        'identity': 'Aggie',
        'full_name': 'Texas A&M University',
    },
    'Stanford': {
        'color': '#8C1515',
        'identity': 'Cardinal',
        'full_name': 'Stanford University',
    },
    'University of Iowa': {
        'color': '#FFCD00',
        'identity': 'Hawkeye',
        'full_name': 'The University of Iowa',
    },
    'University of Michigan': {
        'color': '#00274C',
        'identity': 'Wolverine',
        'full_name': 'University of Michigan',
    },
    'Penn State': {
        'color': '#041E42',
        'identity': 'Nittany Lion',
        'full_name': 'Penn State University',
    },
    'UC Berkeley': {
        'color': '#003262',
        'identity': 'Golden Bear',
        'full_name': 'UC Berkeley',
    },
    'Wisconsin': {
        'color': '#C5050C',
        'identity': 'Badger',
        'full_name': 'University of Wisconsin',
    },
    'Clemson': {
        'color': '#F56600',
        'identity': 'Tiger',
        'full_name': 'Clemson University',
    },
    'Duke': {
        'color': '#003087',
        'identity': 'Blue Devil',
        'full_name': 'Duke University',
    },
    'University of Georgia': {
        'color': '#BA0C2F',
        'identity': 'Bulldog',
        'full_name': 'University of Georgia',
    },
    'UNC Chapel Hill': {
        'color': '#4B9CD3',
        'identity': 'Tar Heel',
        'full_name': 'UNC Chapel Hill',
    },
    'University of Alabama': {
        'color': '#9E1B32',
        'identity': 'Crimson Tide',
        'full_name': 'University of Alabama',
    },
    'Auburn': {
        'color': '#0C2340',
        'identity': 'Tiger',
        'full_name': 'Auburn University',
    },
    'University of Oregon': {
        'color': '#154733',
        'identity': 'Duck',
        'full_name': 'University of Oregon',
    },
    'University of Florida': {
        'color': '#0021A5',
        'identity': 'Gator',
        'full_name': 'University of Florida',
    },
    'Michigan State': {
        'color': '#18453B',
        'identity': 'Spartan',
        'full_name': 'Michigan State University',
    },
    'LSU': {
        'color': '#461D7C',
        'identity': 'Tiger',
        'full_name': 'Louisiana State University',
    },
    'Notre Dame': {
        'color': '#0C2340',
        'identity': 'Fighting Irish',
        'full_name': 'University of Notre Dame',
    },
    'Florida State': {
        'color': '#782F40',
        'identity': 'Seminole',
        'full_name': 'Florida State University',
    },
    'University of Tennessee': {
        'color': '#FF8200',
        'identity': 'Volunteer',
        'full_name': 'University of Tennessee',
    },
    'UT Austin': {
        'color': '#BF5700',
        'identity': 'Longhorn',
        'full_name': 'University of Texas at Austin',
    },
    'Georgia Tech': {
        'color': '#B3A369',
        'identity': 'Yellow Jacket',
        'full_name': 'Georgia Institute of Technology',
    },
    'University of Oklahoma': {
        'color': '#841617',
        'identity': 'Sooner',
        'full_name': 'University of Oklahoma',
    },
}

DEFAULT_BRANDING = {
    'color': '#0066cc',
    'identity': 'student',
    'full_name': '',
}


def get_branding(university):
    """Get school-specific branding or sensible defaults."""
    brand = SCHOOL_BRANDING.get(university, DEFAULT_BRANDING.copy())
    if not brand.get('full_name'):
        brand['full_name'] = university
    return brand


# ─── Name Cleaning ───────────────────────────────────────────────────────────

# Names that are clearly not individuals
NON_INDIVIDUAL_PATTERNS = re.compile(
    r'(program|office\b|center\b|committee|association|council|club|society|'
    r'institute|certificate|administrative|university|questions about|'
    r'office hours|systems programmer|business center|director of|'
    r'members$|^ph\.?d\.?\s)',
    re.IGNORECASE
)

# Mislabeled staff/faculty: names or departments that indicate non-students
MISLABELED_STAFF_NAME = re.compile(
    r'(Professor|Lecturer|Adjunct|Visiting\s+(Associate|Assistant)|'
    r'Fellow\b|Instructor|Coordinator|Specialist|Advisor|Adviser|'
    r'Director|Manager|Executive|Postdoc|Researcher|'
    r'Assoc\s+(AD|VP)|Sr\.?\s+Assoc|Asst\.?\s+AD|'
    r'Assistant\s+to|Chief\s+of|Head\s+Coach|Associate\s+Dean)',
    re.IGNORECASE
)

MISLABELED_STAFF_DEPT = re.compile(
    r'(Athletics\s*[-–]\s*(Assoc|Sr|Exec|Asst|Dir|Head|Admin|Chief|VP)|'
    r'Administration\b|Administrative\b|'
    r'Athletics.*Staff|Fighting Irish Staff|Aggies Athletics.*Staff)',
    re.IGNORECASE
)


def is_mislabeled_staff(name, department):
    """Check if a contact labeled as 'student' is actually staff/faculty."""
    if name and MISLABELED_STAFF_NAME.search(name):
        return True
    if department and MISLABELED_STAFF_DEPT.search(department):
        return True
    return False


GRAD_DEPT_PATTERNS = re.compile(
    r'(ph\.?d|doctoral|master|grad\b|graduate|lab\b|postdoc|fellow\b|research)',
    re.IGNORECASE
)


def is_grad_student(segment, department):
    """Check if a contact is a graduate student based on segment or department."""
    if segment == 'grad_student':
        return True
    if department and GRAD_DEPT_PATTERNS.search(department):
        return True
    return False

# Title keywords that get glued onto names (Stanford engineering pattern)
TITLE_KEYWORDS = re.compile(
    r'(Graduate|Professor|Associate Professor|Assistant Professor|'
    r'Adjunct Professor|Adjunct Lecturer|Affiliate|Lecturer|Fellow|'
    r'Instructor|Coordinator|Specialist|Analyst|Adviser|Advisor|'
    r'Postdoc|Researcher|Academic Program|Staff\b|Architect)',
    re.IGNORECASE
)

# Title keywords that appear after comma in "Name, Title" format
COMMA_TITLE_PATTERNS = re.compile(
    r'(officer|manager|professor|coordinator|specialist|director|'
    r'chair|lecturer|instructor|analyst|adviser|advisor|architect|'
    r'programmer|fellow|student affairs|building emergency|operations)',
    re.IGNORECASE
)


def is_non_individual(name):
    """Check if a name is actually an org/program/office, not a person."""
    if not name:
        return True
    return bool(NON_INDIVIDUAL_PATTERNS.search(name))


def clean_name(raw_name):
    """
    Extract a usable first name from raw name data.
    Returns the first name string, or empty string if unusable.
    """
    if not raw_name or not raw_name.strip():
        return ''

    name = raw_name.strip()

    # Remove HTML entities
    name = name.replace('&amp;', '&').replace('&rsquo;', "'").replace('&nbsp;', ' ')

    # Skip non-individual names
    if is_non_individual(name):
        return ''

    # Handle "NameTitle" pattern (title glued directly to name)
    # e.g. "Blake MastersGraduate, Computer Science"
    title_match = TITLE_KEYWORDS.search(name)
    if title_match:
        # Take everything before the title keyword
        name = name[:title_match.start()].strip()
        if not name:
            return ''

    # Handle comma-separated names
    if ',' in name:
        parts = [p.strip() for p in name.split(',', 1)]

        # Check if the part after comma is a title/role
        if len(parts) == 2 and COMMA_TITLE_PATTERNS.search(parts[1]):
            # "Ana Guido, Student Affairs Officer" -> use "Ana Guido"
            name = parts[0]
        elif len(parts) == 2 and len(parts[1].split()) <= 2 and len(parts[0].split()) <= 2:
            # "Sellnow, Deanna" -> "Deanna Sellnow" (Last, First format)
            name = parts[1] + ' ' + parts[0]
        else:
            # Ambiguous comma, just take the first part
            name = parts[0]

    # Now extract first name from "First Last" or "First Middle Last"
    name = name.strip()
    if not name:
        return ''

    # Handle "Dr." prefix
    name = re.sub(r'^(Dr\.?|Prof\.?|Mr\.?|Ms\.?|Mrs\.?)\s+', '', name, flags=re.IGNORECASE)

    first_name = name.split()[0] if name.split() else ''

    # Validate: must be 3+ chars, alphabetic, not a weird token
    if len(first_name) < 3:
        return ''
    if not re.match(r'^[A-Za-z\-\']+$', first_name):
        return ''
    # Skip if it looks like initials (e.g. "S." or "J.R.")
    if re.match(r'^[A-Z]\.?$', first_name):
        return ''

    # Blocklist: words that get extracted as names but aren't
    BLOCKED_NAMES = {
        'skip', 'map', 'the', 'and', 'for', 'not', 'all', 'new', 'old',
        'get', 'set', 'run', 'use', 'see', 'try', 'ask', 'add', 'end',
        'web', 'lab', 'fax', 'bio', 'pre', 'pro', 'sub', 'via', 'info',
        'main', 'test', 'help', 'home', 'more', 'view', 'page', 'link',
        'site', 'data', 'none', 'null', 'true', 'back', 'next', 'prev',
        'here', 'this', 'that', 'with', 'from', 'also', 'just', 'like',
        'will', 'can', 'may', 'has', 'had', 'was', 'are', 'been', 'each',
        'does', 'did', 'got', 'its', 'let', 'put', 'say', 'she', 'too',
        'her', 'him', 'his', 'how', 'man', 'our', 'out', 'day', 'had',
        'hot', 'oil', 'old', 'red', 'sit', 'top', 'read', 'need', 'land',
        'head', 'high', 'last', 'long', 'make', 'much', 'name', 'only',
        'over', 'such', 'take', 'than', 'them', 'then', 'turn', 'very',
        'want', 'well', 'went', 'what', 'when', 'who', 'why', 'call',
        'come', 'find', 'give', 'good', 'keep', 'know', 'look', 'move',
        'part', 'play', 'real', 'seem', 'show', 'side', 'tell', 'work',
        'year', 'your', 'about', 'after', 'being', 'could', 'every',
        'first', 'great', 'house', 'large', 'never', 'other', 'place',
        'point', 'right', 'small', 'sound', 'still', 'their', 'there',
        'these', 'think', 'those', 'under', 'water', 'where', 'which',
        'world', 'would', 'write', 'click', 'email', 'phone', 'apply',
        'staff', 'chair', 'dept', 'visit', 'login', 'admin',
    }
    if first_name.lower() in BLOCKED_NAMES:
        return ''

    # Capitalize properly
    first_name = first_name.capitalize()

    return first_name


# ─── Subject Line Variants ───────────────────────────────────────────────────

def get_subject_a(first_name, university):
    """Subject A: School-specific exclusivity."""
    if first_name:
        return f"{first_name}, {university} students get early access to Vora"
    return f"{university} students get early access to Vora"


def get_subject_b(first_name, university):
    """Subject B: Direct value proposition."""
    if first_name:
        return f"{first_name}, your free AI health coach is here"
    return f"Your free AI health coach is here"


def get_subject_grad(first_name, university):
    """Subject for grad students: professional, not college-y."""
    if first_name:
        return f"{first_name}, a free AI health coach built for your schedule"
    return f"A free AI health coach built for your schedule"


def get_subject_coach(first_name, university):
    """Subject for coaches: professional partnership angle."""
    if first_name:
        return f"{first_name}, a free health tool for your {university} athletes"
    return f"A free AI health tool for {university} athletes"


SUBJECT_VARIANTS = {
    'A': get_subject_a,
    'B': get_subject_b,
    'grad': get_subject_grad,
    'coach': get_subject_coach,
}


# ─── Email Body Builder ─────────────────────────────────────────────────────

def build_email_body(variant, first_name, university, test_label=None):
    """
    Build the full HTML email for a given variant.

    variant: '1', '2', or '3'
    first_name: contact's first name (or empty string)
    university: school name (e.g. 'UCLA')
    test_label: label for test banner (e.g. 'A1 - Subject A / Body 1')
    """
    brand = get_branding(university)
    greeting = f"Hey {first_name}," if first_name else "Hey there,"

    # Test mode banner
    test_banner = ""
    if test_label:
        test_banner = (
            f'<div style="background: #fff3cd; border: 1px solid #ffc107; '
            f'border-radius: 8px; padding: 12px 16px; margin-bottom: 20px; '
            f'font-size: 13px; color: #856404;">'
            f'TEST MODE — Variant <strong>{test_label}</strong></div>'
        )

    # School-colored accent bar at top
    accent_bar = (
        f'<div style="height: 4px; background: {brand["color"]}; '
        f'border-radius: 4px 4px 0 0; margin-bottom: 24px;"></div>'
    )

    # ── Variant-specific body content ────────────────────────────────────

    if variant == '1':
        # VARIANT 1: Apple Watch Hook
        body_content = f"""\
    <p><strong>Your Apple Watch is the most expensive notification buzzer you own.</strong></p>

    <p>You spent hundreds of dollars on a device that tracks your heart rate, sleep, workouts, and recovery. And most of that data just sits there doing nothing.</p>

    <p>I'm Jai, the founder of <a href="{VORA['website']}" style="color: #0066cc; text-decoration: none;">Vora</a>, and that's exactly why I built it. Vora connects to your Apple Watch, Whoop, Garmin, or any of 500+ wearables and actually turns all that data into a plan you can follow. It builds personalized nutrition based on your actual body and goals, tracks your workouts and tells you when to push harder and when to recover, monitors your sleep and energy so you stop guessing why you feel drained on Tuesdays, and gives you a daily game plan that adapts as you go. Think of it as the health coach you'd hire if you had the money.</p>

    <p>We're live at 24 universities across the country and <strong>{university} made the list</strong>. Right now, only students at select campuses have access. We're keeping it focused on purpose because we want to build something great with real feedback from people who actually care about their health.</p>

    <p>Oh, and it's completely free.</p>

    <p style="font-size: 14px; color: #555; font-style: italic;">You're in the most formative years of your life. The sleep patterns, eating habits, and stress responses you build NOW become your default for the next decade. This is your shot to set that foundation right.</p>"""

    elif variant == '2':
        # VARIANT 2: Stats Gut Punch
        body_content = f"""\
    <p><strong>The average college student gains 15 pounds by graduation, sleeps 6 hours a night, and builds stress habits that take a decade to undo.</strong></p>

    <p>Nobody tells you that. But it's true. And the worst part is it happens so gradually you don't even notice until it's already your normal.</p>

    <p>I'm Jai, the founder of <a href="{VORA['website']}" style="color: #0066cc; text-decoration: none;">Vora</a>, and I'm inviting {university} students to try something: <strong>a 30-day challenge to actually take control of your health.</strong></p>

    <p>For 30 days, Vora will build you a fully personalized plan: what to eat, how to train, when to recover, how to sleep better. It connects to your Apple Watch, Whoop, Garmin, or any of 500+ devices and turns that data into something you can actually use. It builds nutrition plans that adapt to your schedule and how your body responds, tracks your workouts with real insights on what's working, monitors your sleep and stress so you can finally understand your patterns, and helps you build a recovery routine from active rest to meditation. It's like having a personal trainer, nutritionist, and wellness coach all in one app.</p>

    <p>Here's what most students get wrong: they think getting healthier requires hours at the gym and expensive meal plans. It doesn't. It requires the right data and a smart plan. Vora gives you both. For free.</p>

    <p style="font-size: 14px; color: #555; font-style: italic;">You're in the most formative years of your life. The sleep patterns, eating habits, and stress responses you build NOW become your default for the next decade. This is your shot to set that foundation right.</p>"""

    elif variant == '3':
        # VARIANT 3: Morning Question
        body_content = f"""\
    <p><strong>When was the last time you actually felt good waking up?</strong></p>

    <p>Not just "not tired." Actually good. Clear head, energy, ready to go. If you can't remember, you're not alone. Late nights, dining hall food, stress eating before finals, skipping the gym for a week and calling it "rest," DoorDash at 1 AM because you deserve it after that problem set. Your body is keeping score, even when you're not paying attention.</p>

    <p>I'm Jai, the founder of <a href="{VORA['website']}" style="color: #0066cc; text-decoration: none;">Vora</a>. I built this because I watched smart, driven people quietly wreck their health in college without even realizing it. And by the time they cared, they were spending years trying to undo habits that started at 19.</p>

    <p>Vora is an AI health coach that adapts to YOUR life. It connects to your Apple Watch, Whoop, or Garmin and builds you a personalized nutrition plan (not some generic 2,000-calorie template), designs workouts that fit around your class schedule and actually progress, tracks your sleep and recovery so you can see what's really going on, and gives you mental wellness tools like stress tracking, meditation, and mindfulness check-ins. All in one place, all adapting to you.</p>

    <p>We're already live at 24 universities and rolling out to {university} students for free. No trial period. No credit card. We built this because every student deserves a health coach, not just the ones who can afford one. <strong>You don't have to be average.</strong></p>

    <p style="font-size: 14px; color: #555; font-style: italic;">You're in the most formative years of your life. The sleep patterns, eating habits, and stress responses you build NOW become your default for the next decade. This is your shot to set that foundation right.</p>"""

    elif variant == 'grad':
        # VARIANT GRAD: Tailored for PhD/Masters/researchers — professional tone
        body_content = f"""\
    <p><strong>Between research, deadlines, and whatever passes for sleep these days, your health probably isn't getting the attention it deserves.</strong></p>

    <p>I'm Jai, the founder of <a href="{VORA['website']}" style="color: #0066cc; text-decoration: none;">Vora</a>. I built this because I kept hearing the same thing from grad students and researchers: "I know what I should be doing, I just don't have the bandwidth to plan it all." So we built an app that does the planning for you.</p>

    <p>Vora is a free AI-powered health app that connects to your Apple Watch, Whoop, Garmin, or any of 500+ wearables and turns that data into a personalized daily plan. It handles nutrition based on your actual body and goals (not a generic calorie number), designs workouts that fit around an unpredictable schedule, tracks your sleep and recovery so you can see patterns you'd otherwise miss, and helps you manage stress with guided tools like meditation and mindfulness check-ins.</p>

    <p>We're live at 24 universities and available for free to everyone at {university}. No trial, no credit card. We built this because managing your health shouldn't require another project on top of everything else you're juggling.</p>"""

    elif variant == 'coach':
        # VARIANT COACH: Tailored for coaches and athletic staff
        body_content = f"""\
    <p><strong>What if every athlete on your roster had a personal AI health coach?</strong></p>

    <p>I know that sounds ambitious, but that's exactly what we built. I'm Jai, the founder of <a href="{VORA['website']}" style="color: #0066cc; text-decoration: none;">Vora</a>, and I'm reaching out because we're already live at 24 universities and I wanted to put this on your radar at {university}.</p>

    <p>Vora is a free AI-powered app that gives athletes a personalized nutrition plan, smart workout tracking, recovery insights, and sleep monitoring, all connected to their Apple Watch, Whoop, Garmin, or any of 500+ wearables. It turns the data they're already collecting into an actionable daily game plan.</p>

    <p>Here's why coaches love it: athletes actually stick with it. The app adapts to their schedule, their body, and their goals. It's not a generic template. It's personalized guidance that helps them eat better, recover faster, and perform at their best without adding anything to your plate.</p>

    <p>We're offering it completely free to {university} athletes and staff. No cost, no catch. We're building this for the long term and want to partner with programs that care about student-athlete wellness.</p>"""

    else:
        raise ValueError(f"Unknown body variant: {variant}")

    # ── Shared bottom section (links box, CTA, signature, footer) ────────

    links_box = f"""\
    <div style="margin: 24px 0; padding: 16px 20px; background: #f8f9fa; border-radius: 10px;">
        <table cellpadding="0" cellspacing="0" border="0" style="width: 100%;">
            <tr>
                <td style="padding: 6px 0;">
                    <table cellpadding="0" cellspacing="0" border="0"><tr>
                        <td style="vertical-align: middle; padding-right: 10px;"><img src="{YOUTUBE_ICON}" alt="YouTube" width="22" height="16" style="display: block;" /></td>
                        <td style="vertical-align: middle; font-size: 14px;"><a href="{VORA['youtube']}" style="color: #0066cc; text-decoration: none;">Watch a quick demo</a></td>
                    </tr></table>
                </td>
            </tr>
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
    </div>"""

    # CTA varies by variant
    if variant == '1':
        cta = f"""\
    <p>Download it, try it for a week, and see how it feels. If you have feedback, I actually read every message. Just reply to this email or text me at {VORA['ceo_phone']}.</p>"""
    elif variant == '2':
        cta = f"""\
    <p>30 days. That's all it takes to build habits that stick. Download Vora, start your challenge, and see what happens when you actually have a plan. Questions? Reply to this email or text me at {VORA['ceo_phone']}.</p>"""
    elif variant == '3':
        cta = f"""\
    <p>Download it. Try it for a week. If it doesn't change how you think about your health, delete it, no hard feelings. But I think you'll be surprised. Reply to this email or text me at {VORA['ceo_phone']} if you have any questions.</p>"""
    elif variant == 'grad':
        cta = f"""\
    <p>Give it a try. It takes about 2 minutes to set up and you'll have a full plan by tomorrow morning. If you have thoughts or feedback, I genuinely want to hear it. Reply here or text me at {VORA['ceo_phone']}.</p>"""
    elif variant == 'coach':
        cta = f"""\
    <p>I'd love to get this in front of your athletes. Happy to jump on a quick call or just point you to the app so your team can try it. Reply here or grab a time on my calendar: <a href="{VORA['calendly']}" style="color: #0066cc; text-decoration: none;">calendly.com/jai-askvora</a>. You can also text me directly at {VORA['ceo_phone']}.</p>"""

    signature = f"""\
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
    </div>"""

    if variant == 'coach':
        ps_line = f"""\
    <p style="margin-top: 24px; font-size: 14px; color: #333;"><strong>P.S.</strong> The app is completely free for athletes and staff. No contracts, no budget ask. We just want to help student-athletes perform at their best.</p>"""
    elif variant == 'grad':
        ps_line = f"""\
    <p style="margin-top: 24px; font-size: 14px; color: #333;"><strong>P.S.</strong> Completely free. No trial, no credit card, no catch. We're also looking for researchers and grad students who want to help shape what Vora becomes. If that sounds interesting, just reply.</p>"""
    else:
        ps_line = f"""\
    <p style="margin-top: 24px; font-size: 14px; color: #333;"><strong>P.S.</strong> The app is completely free. No trial, no credit card, no catch. Just download and go.</p>

    <p style="font-size: 14px; color: #333;"><strong>P.P.S.</strong> If you're studying health sciences, kinesiology, pre-med, or engineering, we're building something big and we'd love your perspective. We're looking for students who want to help shape what Vora becomes. Reply to this email if you want to be involved.</p>"""

    social_links = f"""\
    <div style="margin-top: 20px; text-align: center; font-size: 13px;">
        <a href="{VORA['twitter']}" style="color: #555; text-decoration: none; margin: 0 10px;">X</a>
        &middot;
        <a href="{VORA['instagram']}" style="color: #555; text-decoration: none; margin: 0 10px;">Instagram</a>
        &middot;
        <a href="{VORA['linkedin']}" style="color: #555; text-decoration: none; margin: 0 10px;">LinkedIn</a>
    </div>"""

    footer = f"""\
    <div style="margin-top: 32px; padding-top: 16px; border-top: 1px solid #eee; font-size: 11px; color: #999; line-height: 1.5;">
        <p style="margin: 0;">Vora AI Inc &middot; San Francisco, CA</p>
        <p style="margin: 4px 0 0 0;">Don't want to hear from us? <a href="mailto:{VORA['ceo_email']}?subject=Unsubscribe" style="color: #999; text-decoration: underline;">Reply unsubscribe</a> and we'll remove you right away.</p>
    </div>"""

    # ── Assemble full HTML ───────────────────────────────────────────────

    html = f"""\
<div style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif; max-width: 600px; margin: 0 auto; color: #1a1a1a; line-height: 1.7; font-size: 15px;">

    {test_banner}

    {accent_bar}

    <p style="margin-top: 0;">{greeting}</p>

{body_content}

{links_box}

{cta}

{signature}

{ps_line}

{social_links}

{footer}

</div>"""

    return html


# ─── Email Sending ───────────────────────────────────────────────────────────

def send_email(to_email, subject, html_content, bcc_email=None,
               categories=None, custom_args=None):
    """Send email via SendGrid with optional BCC, categories, and custom args.
    Retries once on failure."""

    message = Mail()
    message.from_email = Email(FROM_EMAIL, VORA['ceo_name'])
    message.subject = subject

    personalization = Personalization()
    personalization.add_to(To(to_email))
    if bcc_email:
        personalization.add_bcc(Bcc(bcc_email))

    # Custom args go on the personalization for per-recipient tracking
    if custom_args:
        for key, value in custom_args.items():
            personalization.add_custom_arg(CustomArg(key=key, value=str(value)))

    message.add_personalization(personalization)

    message.add_content(Content('text/html', html_content))

    # Categories go on the message level for SendGrid analytics
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
                time.sleep(2)  # wait on rate limit then retry
                continue
            return None, error_msg

    return None, "Max retries exceeded"


# ─── Supabase Helpers ────────────────────────────────────────────────────────

def get_supabase():
    """Create and return a Supabase client."""
    if not SUPABASE_URL or not SUPABASE_KEY:
        print("ERROR: SUPABASE_URL and SUPABASE_KEY must be set in .env")
        sys.exit(1)
    return create_client(SUPABASE_URL, SUPABASE_KEY)


def fetch_all_contacts(supabase):
    """Fetch all eligible contacts from college_contacts with pagination."""
    all_contacts = []
    page_size = 1000
    offset = 0

    while True:
        response = (
            supabase.table('college_contacts')
            .select('id,email,name,role,segment,university,department')
            .in_('role', ['student', 'student_org', 'coach'])
            .range(offset, offset + page_size - 1)
            .execute()
        )
        all_contacts.extend(response.data)
        if len(response.data) < page_size:
            break
        offset += page_size

    return all_contacts


def fetch_already_sent(supabase):
    """Fetch all emails already in college_outreach_sent."""
    already_sent = set()
    page_size = 1000
    offset = 0

    while True:
        response = (
            supabase.table('college_outreach_sent')
            .select('email')
            .range(offset, offset + page_size - 1)
            .execute()
        )
        for row in response.data:
            already_sent.add(row['email'].lower())
        if len(response.data) < page_size:
            break
        offset += page_size

    return already_sent


def fetch_b2b_sent(supabase):
    """Fetch emails already sent via B2B outreach (to avoid double-emailing)."""
    b2b_sent = set()
    page_size = 1000
    offset = 0

    while True:
        response = (
            supabase.table('outreach_sent_emails')
            .select('email')
            .range(offset, offset + page_size - 1)
            .execute()
        )
        for row in response.data:
            b2b_sent.add(row['email'].lower())
        if len(response.data) < page_size:
            break
        offset += page_size

    return b2b_sent


def log_sent_email(supabase, email, university, variant, first_name_used, status='sent'):
    """Log a sent email to college_outreach_sent."""
    try:
        supabase.table('college_outreach_sent').insert({
            'email': email,
            'university': university,
            'variant': variant,
            'first_name_used': first_name_used or '',
            'status': status,
        }).execute()
    except Exception as e:
        # If insert fails (e.g. duplicate), just log and continue
        pass


# ─── Live Send Pipeline ─────────────────────────────────────────────────────

VARIANT_COMBOS = ['A1', 'A2', 'A3', 'B1', 'B2', 'B3']


def prepare_send_list(supabase, limit=0):
    """
    Build the full send list:
    1. Fetch eligible contacts
    2. Remove already-sent and B2B-sent
    3. Remove non-individual names
    4. Clean names and extract first names
    5. Shuffle and assign variants round-robin
    6. Apply limit if set
    """
    print("  Fetching contacts from Supabase...")
    contacts = fetch_all_contacts(supabase)
    print(f"  Found {len(contacts)} eligible contacts (student/student_org/coach)")

    print("  Fetching already-sent emails...")
    already_sent = fetch_already_sent(supabase)
    print(f"  Found {len(already_sent)} already sent")

    print("  Fetching B2B sent emails...")
    b2b_sent = fetch_b2b_sent(supabase)
    print(f"  Found {len(b2b_sent)} B2B emails to exclude")

    # Filter out already sent, non-individuals, and mislabeled staff
    excluded_sent = 0
    excluded_non_individual = 0
    excluded_mislabeled = 0
    eligible = []

    for contact in contacts:
        email_lower = contact['email'].lower()

        # Skip already sent
        if email_lower in already_sent or email_lower in b2b_sent:
            excluded_sent += 1
            continue

        # Skip non-individual names
        raw_name = contact.get('name', '') or ''
        if raw_name.strip() and is_non_individual(raw_name):
            excluded_non_individual += 1
            continue

        # Skip mislabeled staff/faculty in student roles
        department = contact.get('department', '') or ''
        if contact.get('role') == 'student' and is_mislabeled_staff(raw_name, department):
            excluded_mislabeled += 1
            continue

        # Clean name
        first_name = clean_name(raw_name)

        eligible.append({
            'email': contact['email'],
            'university': contact.get('university', ''),
            'role': contact.get('role', ''),
            'segment': contact.get('segment', ''),
            'department': department,
            'raw_name': raw_name,
            'first_name': first_name,
        })

    print(f"  Excluded {excluded_sent} already-sent, {excluded_non_individual} non-individual names, {excluded_mislabeled} mislabeled staff")
    print(f"  Eligible to send: {len(eligible)}")

    # Separate by audience: coaches, grad students, undergrads
    coaches = [c for c in eligible if c['role'] == 'coach']
    grads = [c for c in eligible if c['role'] != 'coach' and is_grad_student(c.get('segment', ''), c.get('department', ''))]
    undergrads = [c for c in eligible if c['role'] != 'coach' and not is_grad_student(c.get('segment', ''), c.get('department', ''))]

    # Assign coach variant
    for c in coaches:
        c['variant'] = 'coach'

    # Assign grad variant
    for c in grads:
        c['variant'] = 'grad'

    # Shuffle undergrads for even distribution
    random.shuffle(undergrads)

    # Assign student variants round-robin to undergrads only
    for i, contact in enumerate(undergrads):
        contact['variant'] = VARIANT_COMBOS[i % len(VARIANT_COMBOS)]

    # Merge back together
    eligible = coaches + grads + undergrads

    print(f"\n  Audience breakdown:")
    print(f"    Coaches:  {len(coaches)}")
    print(f"    Grad/PhD: {len(grads)}")
    print(f"    Undergrad: {len(undergrads)}")

    # Apply limit
    if limit and limit > 0:
        eligible = eligible[:limit]

    # Print summary
    print(f"\n  SEND LIST SUMMARY ({len(eligible)} contacts):")
    print(f"  {'─' * 50}")

    # By school
    school_counts = {}
    for c in eligible:
        school_counts[c['university']] = school_counts.get(c['university'], 0) + 1
    for school, count in sorted(school_counts.items(), key=lambda x: -x[1]):
        print(f"    {school}: {count}")

    # By variant
    print(f"\n  By variant:")
    variant_counts = {}
    for c in eligible:
        variant_counts[c['variant']] = variant_counts.get(c['variant'], 0) + 1
    special_variants = [v for v in ['grad', 'coach'] if variant_counts.get(v, 0) > 0]
    all_variants = VARIANT_COMBOS + special_variants
    for v in all_variants:
        print(f"    {v}: {variant_counts.get(v, 0)}")

    # Names vs no names
    with_name = sum(1 for c in eligible if c['first_name'])
    without_name = sum(1 for c in eligible if not c['first_name'])
    print(f"\n  With first name: {with_name}")
    print(f"  Without (uses 'Hey there,'): {without_name}")

    return eligible


def run_live_send(send_list, supabase, dry_run=False):
    """Send emails to the full send list. If dry_run, just print without sending."""

    total = len(send_list)
    mode_label = "DRY RUN" if dry_run else "LIVE SEND"

    print(f"\n{'=' * 60}")
    print(f"  {mode_label}: {total} emails")
    print(f"{'=' * 60}\n")

    if not dry_run:
        print("  Starting in 5 seconds... (Ctrl+C to abort)")
        time.sleep(5)

    sent = 0
    failed = 0
    start_time = time.time()

    for i, contact in enumerate(send_list):
        email = contact['email']
        university = contact['university']
        first_name = contact['first_name']
        variant = contact['variant']
        role = contact.get('role', 'student')

        # Special variants use their own subject and body
        if variant in ('coach', 'grad'):
            subj_key = variant
            body_key = variant
        else:
            subj_key = variant[0]  # 'A' or 'B'
            body_key = variant[1]  # '1', '2', or '3'

        # Build subject
        subject_fn = SUBJECT_VARIANTS[subj_key]
        subject = subject_fn(first_name, university)

        if dry_run:
            if i < 20 or i % 1000 == 0:
                name_display = first_name or "(no name)"
                print(f"  [{i+1}/{total}] {variant} | {role} | {university} | {name_display} | {email}")
                print(f"           Subject: {subject}")
            continue

        # Build HTML
        html = build_email_body(
            variant=body_key,
            first_name=first_name,
            university=university,
        )

        # SendGrid tracking: categories + custom args
        categories = ['college_outreach', f'variant_{variant}']
        if university:
            categories.append(university.lower().replace(' ', '_'))

        custom_args = {
            'variant': variant,
            'university': university or 'unknown',
            'role': role,
            'has_name': 'yes' if first_name else 'no',
        }

        # Send
        status, error = send_email(
            to_email=email,
            subject=subject,
            html_content=html,
            categories=categories,
            custom_args=custom_args,
        )

        if status and 200 <= status < 300:
            sent += 1
            log_sent_email(supabase, email, university, variant, first_name, 'sent')
        else:
            failed += 1
            log_sent_email(supabase, email, university, variant, first_name, f'failed: {error}')

        # Progress logging
        if (i + 1) % BATCH_LOG_INTERVAL == 0:
            elapsed = time.time() - start_time
            rate = (i + 1) / elapsed if elapsed > 0 else 0
            eta_seconds = (total - i - 1) / rate if rate > 0 else 0
            eta_min = int(eta_seconds // 60)
            eta_sec = int(eta_seconds % 60)
            print(f"  [{i+1}/{total}] sent={sent} failed={failed} "
                  f"rate={rate:.1f}/sec ETA={eta_min}m{eta_sec}s")

        time.sleep(SEND_DELAY)

    # Final summary
    elapsed = time.time() - start_time
    elapsed_min = int(elapsed // 60)
    elapsed_sec = int(elapsed % 60)

    print(f"\n{'=' * 60}")
    print(f"  {mode_label} COMPLETE")
    print(f"{'=' * 60}")
    if dry_run:
        print(f"  Would send: {total} emails")
        print(f"  No emails were actually sent.")
    else:
        print(f"  Sent: {sent}")
        print(f"  Failed: {failed}")
        print(f"  Time: {elapsed_min}m {elapsed_sec}s")
        if elapsed > 0:
            print(f"  Rate: {sent / elapsed:.1f} emails/sec")

    # Variant breakdown
    print(f"\n  By variant:")
    variant_counts = {}
    for c in send_list:
        variant_counts[c['variant']] = variant_counts.get(c['variant'], 0) + 1
    special_variants = [v for v in ['grad', 'coach'] if variant_counts.get(v, 0) > 0]
    all_variants = VARIANT_COMBOS + special_variants
    for v in all_variants:
        print(f"    {v}: {variant_counts.get(v, 0)}")

    # School breakdown
    print(f"\n  By school:")
    school_counts = {}
    for c in send_list:
        school_counts[c['university']] = school_counts.get(c['university'], 0) + 1
    for school, count in sorted(school_counts.items(), key=lambda x: -x[1]):
        print(f"    {school}: {count}")

    print()


# ─── Test Mode ───────────────────────────────────────────────────────────────

def send_test_variants(specific_variant=None):
    """
    Send all 7 A/B test variants (or a specific one) to the test recipient.
    Includes 6 student variants + 1 coach variant.
    """
    test_university = 'NYU'
    test_first_name = 'John'

    variant_labels = {
        'A1': 'Subject A (School Exclusive) / Body 1 (Apple Watch Hook)',
        'A2': 'Subject A (School Exclusive) / Body 2 (Stats Gut Punch)',
        'A3': 'Subject A (School Exclusive) / Body 3 (Morning Question)',
        'B1': 'Subject B (Value Prop) / Body 1 (Apple Watch Hook)',
        'B2': 'Subject B (Value Prop) / Body 2 (Stats Gut Punch)',
        'B3': 'Subject B (Value Prop) / Body 3 (Morning Question)',
        'grad': 'Grad / PhD / Researcher Variant',
        'coach': 'Coach / Athletic Staff Variant',
    }

    variants_to_send = []

    if specific_variant:
        specific_variant = specific_variant.upper() if specific_variant.upper() in variant_labels else specific_variant.lower()
        if specific_variant not in variant_labels:
            print(f"Unknown variant '{specific_variant}'. Valid: {', '.join(variant_labels.keys())}")
            sys.exit(1)
        variants_to_send = [specific_variant]
    else:
        variants_to_send = ['A1', 'A2', 'A3', 'B1', 'B2', 'B3', 'grad', 'coach']

    print(f"\n{'='*60}")
    print(f"  VORA COLLEGE OUTREACH — TEST MODE")
    print(f"  Sending {len(variants_to_send)} variant(s) to: {TEST_RECIPIENT}")
    print(f"  BCC: {BCC_EMAIL}")
    print(f"  Test school: {test_university} | Test name: {test_first_name}")
    print(f"{'='*60}\n")

    results = []

    for combo in variants_to_send:
        label = variant_labels[combo]

        # Special variants use their own keys
        if combo in ('coach', 'grad'):
            subj_key = combo
            body_key = combo
            test_role = combo
        else:
            subj_key = combo[0]
            body_key = combo[1]
            test_role = 'student'

        # Build subject
        subject_fn = SUBJECT_VARIANTS[subj_key]
        subject = subject_fn(test_first_name, test_university)

        # Build body
        html = build_email_body(
            variant=body_key,
            first_name=test_first_name,
            university=test_university,
            test_label=f"{combo} — {label}",
        )

        # SendGrid tracking for tests too
        categories = ['college_outreach', 'test', f'variant_{combo}']
        custom_args = {
            'variant': combo,
            'university': test_university,
            'role': test_role,
            'has_name': 'yes',
        }

        print(f"  Sending variant {combo}: {label}")
        print(f"    Subject: {subject}")

        status, error = send_email(
            to_email=TEST_RECIPIENT,
            subject=subject,
            html_content=html,
            bcc_email=BCC_EMAIL,
            categories=categories,
            custom_args=custom_args,
        )

        if status and 200 <= status < 300:
            print(f"    ✓ Sent (status {status})\n")
            results.append((combo, True))
        else:
            print(f"    ✗ Failed: {error}\n")
            results.append((combo, False))

        time.sleep(1)

    # Summary
    print(f"\n{'='*60}")
    print(f"  RESULTS SUMMARY")
    print(f"{'='*60}")
    sent_count = sum(1 for _, ok in results if ok)
    failed_count = sum(1 for _, ok in results if not ok)
    for combo, ok in results:
        status_str = "✓ SENT" if ok else "✗ FAILED"
        print(f"  {combo} ({variant_labels[combo]}): {status_str}")
    print(f"\n  Total: {sent_count} sent, {failed_count} failed")
    print(f"  Check {TEST_RECIPIENT} and {BCC_EMAIL} for all variants\n")


# ─── Main ────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description='Vora College Outreach Campaign')
    parser.add_argument('--test', action='store_true', help='Send test variants to test recipient')
    parser.add_argument('--variant', type=str, help='Send specific variant only (e.g. A1, B3)')
    parser.add_argument('--live', action='store_true', help='Send to actual college contacts')
    parser.add_argument('--dry-run', action='store_true', help='Show what would be sent without sending')
    parser.add_argument('--confirm', action='store_true', help='Skip confirmation prompt (use with caution)')
    parser.add_argument('--limit', type=int, default=0, help='Limit number of contacts to send to')

    args = parser.parse_args()

    if not SENDGRID_API_KEY:
        print("ERROR: SENDGRID_API_KEY not set in .env")
        sys.exit(1)

    if args.test:
        send_test_variants(args.variant)
    elif args.live:
        supabase = get_supabase()
        print(f"\n{'=' * 60}")
        print(f"  VORA COLLEGE OUTREACH — PREPARING SEND LIST")
        print(f"{'=' * 60}\n")
        send_list = prepare_send_list(supabase, limit=args.limit)

        if not send_list:
            print("\n  No contacts to send to. Exiting.")
            sys.exit(0)

        if args.dry_run:
            run_live_send(send_list, supabase, dry_run=True)
        else:
            if not args.confirm:
                confirm = input(f"\n  Ready to send {len(send_list)} emails LIVE. Type 'yes' to confirm: ")
                if confirm.strip().lower() != 'yes':
                    print("  Aborted.")
                    sys.exit(0)
            else:
                print(f"\n  --confirm flag set. Sending {len(send_list)} emails LIVE.")
            run_live_send(send_list, supabase, dry_run=False)
    else:
        parser.print_help()


if __name__ == '__main__':
    main()
