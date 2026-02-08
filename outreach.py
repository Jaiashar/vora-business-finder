#!/usr/bin/env python3
"""
Vora Outreach Engine
Scrapes business info, identifies key contacts, generates personalized emails,
and sends via SendGrid with BCC tracking.

Usage:
    # Single test email (sends to test address)
    python outreach.py results/irvine_ca.csv --single --row 0

    # Test mode: all emails go to test address
    python outreach.py results/irvine_ca.csv --test --limit 5

    # Live mode: emails go to actual contacts
    python outreach.py results/irvine_ca.csv --live
"""

import argparse
import csv
import json
import os
import re
import sys
import time
import urllib.request
import urllib.error
from urllib.parse import urlparse, quote as urlquote
import ssl
from dotenv import load_dotenv
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import (
    Mail, Email, To, Bcc, Personalization, Content
)

load_dotenv()

# ─── Configuration ───────────────────────────────────────────────────────────

SENDGRID_API_KEY = os.getenv('SENDGRID_API_KEY')
FROM_EMAIL = os.getenv('FROM_EMAIL', 'jai@askvora.com')
BCC_EMAIL = os.getenv('BCC_EMAIL', 'jai@askvora.com')
TEST_RECIPIENT = os.getenv('TEST_RECIPIENT', 'jaikrish15@gmail.com')

ssl._create_default_https_context = ssl._create_unverified_context

# ─── Supabase Dedup Config ────────────────────────────────────────────────────

SUPABASE_URL = os.getenv('SUPABASE_URL')
SUPABASE_KEY = os.getenv('SUPABASE_KEY')

# ─── Junk Email Blacklist ─────────────────────────────────────────────────────

JUNK_DOMAINS = {
    'godaddy.com', 'mysite.com', 'domain.com', 'latofonts.com',
    'example.com', 'email.com', 'address.com', 'pixelspread.com',
    'typemade.mx', 'indiantypefoundry.com',
}

JUNK_EMAILS = {
    'filler@godaddy.com', 'your@email.com', 'johnsmith@gmail.com',
    'contact@mysite.com', 'email@address.com', 'emailhere@email.com',
}


def is_junk_email(email):
    """Return True if the email is a known junk/placeholder address."""
    if not email or '@' not in email:
        return True
    email_lower = email.strip().lower()
    if email_lower in JUNK_EMAILS:
        return True
    domain = email_lower.split('@')[1]
    if domain in JUNK_DOMAINS:
        return True
    if '%20' in email_lower:
        return True
    return False


# ─── Supabase Dedup Functions ─────────────────────────────────────────────────

def supabase_check_email(email):
    """Check if an email already exists in the outreach_sent_emails table."""
    try:
        email_lower = email.strip().lower()
        url = (
            f"{SUPABASE_URL}/rest/v1/outreach_sent_emails"
            f"?email=eq.{urlquote(email_lower)}&select=email&limit=1"
        )
        req = urllib.request.Request(url, headers={
            "apikey": SUPABASE_KEY,
            "Authorization": f"Bearer {SUPABASE_KEY}",
        })
        resp = urllib.request.urlopen(req, timeout=5)
        data = json.loads(resp.read())
        return len(data) > 0
    except Exception:
        return False  # On error, allow sending (don't block)


def supabase_record_sent(email, business_name, city, category=''):
    """Record a sent email in Supabase for future dedup."""
    try:
        payload = json.dumps({
            "email": email.strip().lower(),
            "business_name": business_name[:200],
            "city": city,
            "category": category[:50],
            "status": "sent",
        }).encode()
        url = f"{SUPABASE_URL}/rest/v1/outreach_sent_emails"
        req = urllib.request.Request(url, data=payload, headers={
            "apikey": SUPABASE_KEY,
            "Authorization": f"Bearer {SUPABASE_KEY}",
            "Content-Type": "application/json",
            "Prefer": "resolution=ignore-duplicates",
        }, method="POST")
        urllib.request.urlopen(req, timeout=5)
    except Exception:
        pass  # Non-critical, don't crash on tracking failures


HEADERS = {
    'User-Agent': (
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
        'AppleWebKit/537.36 (KHTML, like Gecko) '
        'Chrome/120.0.0.0 Safari/537.36'
    ),
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.5',
}

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

# ─── Business Classification ────────────────────────────────────────────────

BUSINESS_CATEGORIES = {
    'gym': ['gym', 'fitness center', 'crossfit', 'training facility', 'health club'],
    'physical_therapy': [
        'physical therapy', 'physiotherapy', 'rehabilitation',
        'rehab', 'outpatient rehab',
    ],
    'chiropractic': ['chiropractic', 'chiropractor', 'spine', 'spinal adjustment'],
    'weight_loss': [
        'weight loss', 'weight management', 'med spa', 'medspa', 'medical spa',
        'aesthetics', 'body contouring', 'coolsculpting', 'peptide',
        'ozempic', 'semaglutide', 'wegovy', 'zepbound', 'glp-1', 'glp1',
        'bariatric', 'iv therapy', 'iv hydration', 'hormone therapy',
        'regenerative medicine', 'anti-aging', 'body sculpting',
    ],
    'wellness': [
        'wellness', 'holistic', 'integrative', 'functional medicine',
        'naturopath', 'longevity', 'optimal health', 'biohacking',
    ],
    'sports_medicine': ['sports medicine', 'sports therapy', 'athletic training'],
    'pain_management': ['pain management', 'pain clinic', 'pain specialist'],
    'medical': ['medical', 'orthopedic', 'clinic', 'physician', 'doctor'],
}

# Professional titles to look for
PROFESSIONAL_SUFFIXES = (
    'MD', 'DO', 'DPT', 'PT', 'DC', 'OD', 'PhD', 'NP', 'PA',
    'RN', 'CFMP', 'CTN', 'CTP', 'CSCS', 'OCS', 'SCS', 'LAc',
    'ND', 'DACM', 'DAOM', 'MPT', 'ATC', 'FAAPMR',
)

# Leadership / decision-maker roles
DECISION_MAKER_ROLES = [
    'owner', 'founder', 'co-founder', 'ceo', 'president',
    'director', 'managing partner', 'principal', 'administrator',
    'practice manager', 'general manager', 'clinic director',
    'medical director', 'clinical director', 'chief',
]

# Email domains to skip when deriving website from email
PERSONAL_EMAIL_DOMAINS = {
    'gmail.com', 'yahoo.com', 'hotmail.com', 'outlook.com',
    'aol.com', 'icloud.com', 'me.com', 'live.com', 'msn.com',
    'comcast.net', 'cox.net', 'att.net', 'verizon.net',
    'sbcglobal.net', 'bellsouth.net', 'earthlink.net',
    'mail.com', 'protonmail.com', 'zoho.com',
}


# ─── Web Scraping Utilities ─────────────────────────────────────────────────

def fetch_page(url, timeout=10):
    """Fetch a webpage and return its HTML content."""
    try:
        if not url.startswith(('http://', 'https://')):
            url = 'https://' + url
        request = urllib.request.Request(url, headers=HEADERS)
        with urllib.request.urlopen(request, timeout=timeout) as response:
            content = response.read()
            try:
                return content.decode('utf-8')
            except UnicodeDecodeError:
                return content.decode('latin-1', errors='ignore')
    except Exception:
        return None


def strip_html(html):
    """Remove HTML tags and return clean text."""
    if not html:
        return ''
    text = re.sub(r'<script[^>]*>.*?</script>', ' ', html, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r'<style[^>]*>.*?</style>', ' ', text, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r'<!--.*?-->', ' ', text, flags=re.DOTALL)
    text = re.sub(r'<[^>]+>', ' ', text)
    text = re.sub(r'&[a-zA-Z]+;', ' ', text)
    text = re.sub(r'&#\d+;', ' ', text)
    text = re.sub(r'\s+', ' ', text)
    return text.strip()


def extract_meta(html):
    """Extract meta description, title, and OG data from HTML."""
    info = {}

    title_match = re.search(r'<title[^>]*>(.*?)</title>', html, re.IGNORECASE | re.DOTALL)
    if title_match:
        info['title'] = strip_html(title_match.group(1)).strip()

    # Meta description
    for pattern in [
        r'<meta[^>]*name=["\']description["\'][^>]*content=["\'](.*?)["\']',
        r'<meta[^>]*content=["\'](.*?)["\'][^>]*name=["\']description["\']',
    ]:
        m = re.search(pattern, html, re.IGNORECASE)
        if m:
            info['description'] = m.group(1).strip()
            break

    # OG description
    og = re.search(
        r'<meta[^>]*property=["\']og:description["\'][^>]*content=["\'](.*?)["\']',
        html, re.IGNORECASE,
    )
    if og:
        info['og_description'] = og.group(1).strip()

    return info


# ─── Contact Extraction ─────────────────────────────────────────────────────

def find_contacts_on_page(html):
    """Extract potential contact names, titles, and roles from HTML."""
    if not html:
        return []

    text = strip_html(html)
    contacts = []
    seen_names = set()

    # Pattern 1: "Dr. First Last" or "First Last, MD/DPT/DC..."
    suffixes = '|'.join(PROFESSIONAL_SUFFIXES)
    pattern1 = re.compile(
        rf'(?:Dr\.?\s+)?([A-Z][a-z]+'
        rf'(?:\s+[A-Z]\.?)?\s+[A-Z][a-z]+(?:-[A-Z][a-z]+)?)'
        rf'(?:\s*,?\s*(?:{suffixes})(?:\s*,?\s*(?:{suffixes}))*)',
        re.MULTILINE,
    )
    for m in pattern1.finditer(text):
        name = m.group(1).strip()
        if name not in seen_names and 5 < len(name) < 40:
            seen_names.add(name)
            full = m.group(0).strip()
            title = full.replace(name, '').strip(' ,')
            contacts.append({'name': name, 'title': title, 'role': ''})

    # Pattern 2: "Dr. First Last" (without suffix)
    pattern2 = re.compile(
        r'Dr\.?\s+([A-Z][a-z]+(?:\s+[A-Z]\.?)?\s+[A-Z][a-z]+(?:-[A-Z][a-z]+)?)'
    )
    for m in pattern2.finditer(text):
        name = m.group(1).strip()
        if name not in seen_names and 5 < len(name) < 40:
            seen_names.add(name)
            contacts.append({'name': name, 'title': 'Dr.', 'role': ''})

    # Pattern 3: Name — Role (e.g. "John Smith - Owner")
    roles_re = '|'.join(DECISION_MAKER_ROLES)
    pattern3 = re.compile(
        rf'([A-Z][a-z]+(?:\s+[A-Z]\.?)?\s+[A-Z][a-z]+)'
        rf'\s*[-–—|,]\s*(?:the\s+)?({roles_re})',
        re.IGNORECASE,
    )
    for m in pattern3.finditer(text):
        name = m.group(1).strip()
        role = m.group(2).strip().title()
        if name not in seen_names and 5 < len(name) < 40:
            seen_names.add(name)
            contacts.append({'name': name, 'title': '', 'role': role})

    # Pattern 4: Role — Name (e.g. "Owner: John Smith")
    pattern4 = re.compile(
        rf'(?:{roles_re})\s*[-–—:,|]\s*'
        rf'([A-Z][a-z]+(?:\s+[A-Z]\.?)?\s+[A-Z][a-z]+)',
        re.IGNORECASE,
    )
    for m in pattern4.finditer(text):
        name = m.group(1).strip()
        if name not in seen_names and 5 < len(name) < 40:
            seen_names.add(name)
            contacts.append({
                'name': name,
                'title': '',
                'role': m.group(0).split(name)[0].strip(' -–—:,|').title(),
            })

    # Sort: decision-makers first, then professionals
    def contact_priority(c):
        if c['role'].lower() in [r for r in DECISION_MAKER_ROLES]:
            return 0
        if c['title']:
            return 1
        return 2

    contacts.sort(key=contact_priority)
    return contacts[:5]


# ─── Service & Business Classification ───────────────────────────────────────

def extract_services(html):
    """Extract services/specialties from the page."""
    if not html:
        return []

    text = strip_html(html).lower()
    service_keywords = [
        'physical therapy', 'sports rehabilitation', 'pain management',
        'chiropractic care', 'wellness programs', 'orthopedic care',
        'sports medicine', 'acupuncture', 'massage therapy',
        'personal training', 'fitness programs', 'nutrition counseling',
        'weight management', 'recovery programs', 'strength training',
        'yoga', 'pilates', 'functional training', 'injury prevention',
        'post-surgical rehabilitation', 'manual therapy', 'dry needling',
        'mobility training', 'concierge medicine', 'longevity',
        'biohacking', 'red light therapy', 'cryotherapy',
        'iv therapy', 'hormone therapy', 'regenerative medicine',
    ]

    found = []
    for kw in service_keywords:
        if kw in text:
            found.append(kw)
    return found[:6]


def classify_business(business_name, services=None, description=''):
    """Classify the business type for personalized messaging."""
    combined = f"{business_name} {' '.join(services or [])} {description}".lower()

    scores = {}
    for category, keywords in BUSINESS_CATEGORIES.items():
        score = sum(1 for kw in keywords if kw.lower() in combined)
        if score > 0:
            scores[category] = score

    if scores:
        return max(scores, key=scores.get)
    return 'wellness'


def website_from_email(email):
    """Derive a likely website URL from a business email address."""
    if not email or '@' not in email:
        return None
    domain = email.split('@')[1].lower().strip()
    if domain in PERSONAL_EMAIL_DOMAINS:
        return None
    return f"https://{domain}"


# ─── Business Info Scraping ──────────────────────────────────────────────────

def scrape_business_info(business_name, website_url=None):
    """Scrape comprehensive info about a business from its website."""
    info = {
        'name': business_name,
        'website': website_url,
        'description': '',
        'services': [],
        'contacts': [],
        'category': 'wellness',
    }

    if not website_url:
        info['category'] = classify_business(business_name)
        return info

    print(f"  Scraping {website_url}...")

    # Fetch main page
    html = fetch_page(website_url)
    if html:
        meta = extract_meta(html)
        info['description'] = (
            meta.get('description')
            or meta.get('og_description')
            or meta.get('title', '')
        )
        info['services'] = extract_services(html)
        info['contacts'] = find_contacts_on_page(html)

    # Try team / about pages for more contacts
    parsed = urlparse(
        website_url if website_url.startswith('http') else 'https://' + website_url
    )
    base = f"{parsed.scheme}://{parsed.netloc}"

    team_paths = [
        '/about', '/about-us', '/our-team', '/team',
        '/staff', '/meet-the-team', '/providers', '/our-providers',
        '/doctors', '/our-doctors', '/meet-our-team',
    ]
    for path in team_paths:
        if len(info['contacts']) >= 3:
            break
        page_html = fetch_page(base + path, timeout=5)
        if page_html:
            new_contacts = find_contacts_on_page(page_html)
            existing_names = {c['name'] for c in info['contacts']}
            for c in new_contacts:
                if c['name'] not in existing_names:
                    info['contacts'].append(c)
        time.sleep(0.3)

    info['category'] = classify_business(
        business_name, info['services'], info['description']
    )

    return info


# ─── Email Generation ────────────────────────────────────────────────────────

def get_greeting_name(business_info):
    """Get the best name to use in the email greeting."""
    if business_info['contacts']:
        first_contact = business_info['contacts'][0]
        name = first_contact['name']
        first_name = name.split()[0]
        return first_name, name, first_contact.get('title', ''), first_contact.get('role', '')
    return None, None, '', ''


VALUE_PROPS = {
    'gym': {
        'member_term': 'members',
        'hook': (
            "help your members get more out of every session "
            "and actually stick with their fitness goals"
        ),
        'specific': (
            "Whether someone's training for a competition or just trying to stay "
            "consistent, Vora gives each member a plan that adapts to their body, "
            "schedule, and goals. It keeps people engaged way past that "
            "first month when most memberships go unused."
        ),
    },
    'physical_therapy': {
        'member_term': 'patients',
        'hook': (
            "help your patients stay on track between visits "
            "and build real habits beyond their treatment plan"
        ),
        'specific': (
            "Vora fills the gap between clinic visits. It guides home exercise "
            "programs, tracks recovery progress, and keeps patients engaged with "
            "their rehab goals so they actually see better outcomes and "
            "keep coming back."
        ),
    },
    'chiropractic': {
        'member_term': 'patients',
        'hook': (
            "help your patients keep their progress between visits "
            "and build wellness habits that complement their chiropractic care"
        ),
        'specific': (
            "Vora helps patients follow movement protocols, track how they're "
            "recovering, and stay consistent with the lifestyle changes that "
            "make adjustments last longer. It reinforces everything you're "
            "already doing in the office."
        ),
    },
    'wellness': {
        'member_term': 'clients',
        'hook': (
            "give your clients a daily wellness companion that "
            "ties together every part of their health journey"
        ),
        'specific': (
            "From nutrition and supplements to recovery and stress management, "
            "Vora gives your clients the daily guidance they need to see real "
            "results and stay connected to your practice long term."
        ),
    },
    'sports_medicine': {
        'member_term': 'athletes',
        'hook': (
            "give your athletes a real tool to optimize performance, "
            "prevent injuries, and recover faster"
        ),
        'specific': (
            "Vora tracks training load, monitors recovery readiness, and "
            "builds plans that help athletes push harder when they can "
            "and back off when they need to. It's the kind of thing "
            "that keeps them coming back to you."
        ),
    },
    'pain_management': {
        'member_term': 'patients',
        'hook': (
            "support your patients with daily wellness guidance that "
            "works alongside their pain management program"
        ),
        'specific': (
            "Vora helps patients track their symptoms, follow movement and "
            "recovery protocols, and build daily habits that actually reduce "
            "pain and improve quality of life between visits."
        ),
    },
    'medical': {
        'member_term': 'patients',
        'hook': (
            "give your patients a smart wellness companion "
            "that supports their health goals between visits"
        ),
        'specific': (
            "Vora helps patients take an active role in their own health, "
            "tracking nutrition, activity, sleep, and recovery with AI "
            "guidance that adapts to how their body actually responds."
        ),
    },
    'weight_loss': {
        'member_term': 'clients',
        'hook': (
            "help your clients stay on track with their transformation "
            "goals and see real, lasting results"
        ),
        'specific': (
            "Whether your clients are on a GLP-1 protocol, doing body contouring, "
            "or following a nutrition plan, Vora keeps them engaged daily. It tracks "
            "body composition, nutrition, activity, and supplements, so they stay "
            "motivated and actually follow through between visits."
        ),
    },
}


LOGO_URL = "https://askvora.com/iOS%20LOGO%204.png"
YOUTUBE_ICON = "https://upload.wikimedia.org/wikipedia/commons/thumb/0/09/YouTube_full-color_icon_%282017%29.svg/120px-YouTube_full-color_icon_%282017%29.svg.png"
APP_STORE_ICON = "https://upload.wikimedia.org/wikipedia/commons/thumb/6/67/App_Store_%28iOS%29.svg/120px-App_Store_%28iOS%29.svg.png"


# Category-specific personalization hooks that explain why Vora fits *their* business
FIT_REASONS = {
    'gym': (
        "A lot of your members are probably already wearing Apple Watches or Whoops "
        "but not really getting much out of that data. Vora connects all of it and "
        "turns it into a real plan they can follow every day. It makes your gym feel "
        "like it comes with a personal coach, even when they're not on-site."
    ),
    'physical_therapy': (
        "One of the biggest challenges in PT is keeping patients consistent between "
        "visits. Vora solves that by giving them daily guidance on exercises, recovery, "
        "and nutrition right on their phone. It basically extends your care beyond the "
        "clinic walls and helps patients actually follow through."
    ),
    'chiropractic': (
        "Most patients leave your office feeling great but don't always keep up with "
        "the stretches and lifestyle changes that make adjustments stick. Vora gives "
        "them daily movement and recovery guidance on their phone, so they show up "
        "to their next appointment in better shape."
    ),
    'wellness': (
        "Your clients are clearly invested in their health, and Vora is the kind of "
        "tool that takes that to the next level. It ties together their wearable data, "
        "nutrition, recovery, and mental wellness into one daily plan. It's the perfect "
        "complement to what you're already offering."
    ),
    'sports_medicine': (
        "Your athletes are already tracking a ton of data, but most of them don't know "
        "what to do with it. Vora takes all that info from their wearables and turns it "
        "into actual training and recovery recommendations. It's like giving them a "
        "sports science team in their pocket."
    ),
    'pain_management': (
        "Patients dealing with chronic pain need support between visits, not just during "
        "them. Vora helps them track symptoms, follow daily movement protocols, and build "
        "habits around sleep and recovery that actually make a difference. It keeps them "
        "engaged with their care plan even on the hard days."
    ),
    'medical': (
        "Most patients leave the office with good intentions but no real system for "
        "following through. Vora gives them daily guidance on nutrition, exercise, sleep, "
        "and recovery, all personalized to their body. It helps them take ownership "
        "of their health between appointments."
    ),
    'weight_loss': (
        "The hardest part of any weight loss or body transformation program is what "
        "happens between visits. Vora fills that gap with daily nutrition tracking, "
        "personalized meal plans, supplement reminders, body composition trends, and "
        "real-time progress insights. It keeps clients accountable and motivated "
        "so they actually stick with the program and see results."
    ),
}


def clean_description(desc):
    """Clean HTML entities and trim a scraped description for use in email copy."""
    if not desc:
        return ''
    import html as html_mod
    text = html_mod.unescape(desc)
    # Remove any leftover tags
    text = re.sub(r'<[^>]+>', '', text)
    # Collapse whitespace
    text = re.sub(r'\s+', ' ', text).strip()
    # Take first sentence or first ~150 chars at a word boundary
    if '. ' in text[:150]:
        text = text[:text.index('. ', 0, 150) + 1]
    elif len(text) > 150:
        text = text[:150].rsplit(' ', 1)[0]
    return text


def build_personalized_opening(business_info):
    """Build a genuinely personalized opening paragraph using scraped data."""
    biz_name = business_info['name']
    desc = clean_description(business_info.get('description', ''))
    services = business_info.get('services', [])
    category = business_info.get('category', 'wellness')

    # Best case: we have a real description to reference
    if desc and len(desc) > 30:
        desc_lower = desc.lower()

        # Pull out what makes them specific
        if 'functional' in desc_lower or 'integrative' in desc_lower:
            return (
                f"I was checking out <strong>{biz_name}</strong> and really liked "
                f"your approach to functional and integrative care. "
                f"The focus on evidence-based, whole-person health is exactly "
                f"the kind of practice Vora was built to complement."
            )
        elif 'holistic' in desc_lower or 'natural' in desc_lower:
            return (
                f"I was looking into <strong>{biz_name}</strong> and loved your "
                f"holistic approach to health. It's clear you care about treating "
                f"the whole person, not just symptoms, and that's exactly where "
                f"Vora fits in."
            )
        elif 'sport' in desc_lower or 'athlet' in desc_lower or 'performance' in desc_lower:
            return (
                f"I came across <strong>{biz_name}</strong> and really liked your "
                f"focus on performance and sports health. Your clients are clearly "
                f"serious about results, and I think Vora could take that even further."
            )
        elif 'rehab' in desc_lower or 'recovery' in desc_lower or 'physical therapy' in desc_lower:
            return (
                f"I was checking out <strong>{biz_name}</strong> and your approach to "
                f"rehabilitation really stood out. Helping people get back to full "
                f"strength is tough work, and I think Vora could make a real "
                f"difference for your patients between visits."
            )
        elif 'pain' in desc_lower:
            return (
                f"I came across <strong>{biz_name}</strong> and your work in pain "
                f"management really resonated with me. I know how hard it is for "
                f"patients to stay consistent between appointments, and that's "
                f"exactly the problem Vora solves."
            )
        elif 'chiropractic' in desc_lower or 'spine' in desc_lower or 'spinal' in desc_lower:
            return (
                f"I was looking into <strong>{biz_name}</strong> and your chiropractic "
                f"work caught my eye. Keeping patients engaged with their care "
                f"plan between adjustments is one of the biggest challenges in your "
                f"space, and that's where Vora comes in."
            )
        elif any(kw in desc_lower for kw in [
            'weight', 'peptide', 'med spa', 'medspa', 'aesthet', 'iv therapy',
            'hormone', 'regenerat', 'body contour', 'coolsculpt', 'ozempic',
            'semaglut', 'wegovy', 'zepbound', 'slim', 'nutrition',
        ]):
            return (
                f"I came across <strong>{biz_name}</strong> and really liked your "
                f"approach to helping clients transform their health. Keeping people "
                f"on track between visits is everything in your space, and I think "
                f"Vora can be a real game-changer for your clients."
            )
        # No keyword match — fall through to category-based or services-based openers below

    # Second best: we have services
    if services:
        top = services[:2]
        if len(top) > 1:
            svc_str = f"{top[0]} and {top[1]}"
        else:
            svc_str = top[0]
        return (
            f"I came across <strong>{biz_name}</strong> and saw that you're "
            f"doing great work in {svc_str}. I think there's a really natural "
            f"fit between what you offer and what Vora does."
        )

    # Fallback: just the business name + category context
    cat_openers = {
        'gym': f"I came across <strong>{biz_name}</strong> and it looks like you're building something great for your community.",
        'physical_therapy': f"I came across <strong>{biz_name}</strong> and your rehab work really caught my attention.",
        'chiropractic': f"I came across <strong>{biz_name}</strong> and your chiropractic practice caught my eye.",
        'wellness': f"I came across <strong>{biz_name}</strong> and your wellness practice really stood out to me.",
        'sports_medicine': f"I came across <strong>{biz_name}</strong> and your sports medicine work caught my attention.",
        'pain_management': f"I came across <strong>{biz_name}</strong> and your pain management practice stood out to me.",
        'medical': f"I came across <strong>{biz_name}</strong> and liked the practice you've built.",
        'weight_loss': f"I came across <strong>{biz_name}</strong> and really liked your approach to helping clients transform their health.",
    }
    return cat_openers.get(category, cat_openers['wellness'])


def generate_email(business_info, test_mode=False):
    """Generate a personalized outreach email. Returns (subject, html_content)."""

    first_name, full_name, contact_title, contact_role = get_greeting_name(business_info)
    vp = VALUE_PROPS.get(business_info['category'], VALUE_PROPS['wellness'])
    biz_name = business_info['name']
    category = business_info['category']

    # Greeting
    if first_name:
        greeting = f"Hey {first_name},"
    else:
        greeting = f"Hey {biz_name} Team,"

    # Personalized opening built from actual scraped data
    personal_opening = build_personalized_opening(business_info)

    # Why Vora specifically fits their business
    fit_reason = FIT_REASONS.get(category, FIT_REASONS['wellness'])

    # Subject line — short, punchy, intriguing
    short_name = biz_name
    if len(short_name) > 35:
        short_name = short_name[:35].rsplit(' ', 1)[0]
        short_name = short_name.rstrip(' -,')

    subject = f"{short_name} + Vora"

    # Test mode banner
    test_banner = ""
    if test_mode:
        test_banner = (
            '<div style="background: #fff3cd; border: 1px solid #ffc107; '
            'border-radius: 8px; padding: 12px 16px; margin-bottom: 20px; '
            'font-size: 13px; color: #856404;">'
            'TEST MODE: This email would normally go to the actual business contact.</div>'
        )

    html = f"""\
<div style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif; max-width: 600px; margin: 0 auto; color: #1a1a1a; line-height: 1.7; font-size: 15px;">

    {test_banner}

    <p style="margin-top: 0;">{greeting}</p>

    <p>I'm Jai, the founder of <a href="{VORA['website']}" style="color: #0066cc; text-decoration: none;">Vora</a>. {personal_opening}</p>

    <p>{fit_reason}</p>

    <p>Here's what Vora does for your {vp['member_term']}:</p>

    <ul style="padding-left: 20px; margin: 16px 0;">
        <li style="margin-bottom: 8px;">Connect 500+ wearables and devices (Apple Watch, Whoop, Garmin, InBody, etc.)</li>
        <li style="margin-bottom: 8px;">Log workouts intelligently and get real insights on performance and recovery</li>
        <li style="margin-bottom: 8px;">Get personalized nutrition plans, meal recommendations, and supplement guidance</li>
        <li style="margin-bottom: 8px;">Follow recovery protocols from physical (sauna, steam, yoga) to mental wellness (guided meditation, mindfulness)</li>
        <li style="margin-bottom: 8px;">Track energy, sleep, recovery, and long-term health trends all in one place</li>
    </ul>

    <p>Businesses using Vora have seen <strong>retention rates go up to 80%</strong>. It's basically like giving every {vp['member_term'].rstrip('s')} their own tiny genius health coach in their pocket.</p>

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
                        <td style="vertical-align: middle; font-size: 14px;"><a href="{VORA['app_store']}" style="color: #0066cc; text-decoration: none;">Download on the App Store</a></td>
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

    <p>Would love to chat about how this could work for {biz_name}. You can <a href="{VORA['calendly']}" style="color: #0066cc; text-decoration: none;">grab a time on my calendar here</a>, reply to this email, or just text me at {VORA['ceo_phone']}. Whatever's easiest.</p>

    <p>Talk soon,</p>

    <div style="margin-top: 16px; padding-top: 16px; border-top: 1px solid #e0e0e0;">
        <table cellpadding="0" cellspacing="0" border="0">
            <tr>
                <td style="vertical-align: top; padding-right: 14px;">
                    <img src="{LOGO_URL}" alt="Vora" width="48" height="48" style="border-radius: 10px; display: block;" />
                </td>
                <td style="vertical-align: top;">
                    <p style="margin: 0; font-weight: 600; font-size: 15px;">{VORA['ceo_name']}</p>
                    <p style="margin: 2px 0; color: #555; font-size: 13px;">CEO, <a href="{VORA['website']}" style="color: #0066cc; text-decoration: none;">Vora</a></p>
                    <p style="margin: 2px 0; color: #555; font-size: 13px;"><a href="tel:+19492761808" style="color: #555; text-decoration: none;">{VORA['ceo_phone']}</a></p>
                    <p style="margin: 2px 0; color: #555; font-size: 13px;"><a href="mailto:{VORA['ceo_email']}" style="color: #555; text-decoration: none;">{VORA['ceo_email']}</a></p>
                </td>
            </tr>
        </table>
    </div>

    <p style="margin-top: 24px; font-size: 14px; color: #333;"><strong>P.S.</strong> The app is free to try if you want to see it for yourself before we chat.</p>

    <div style="margin-top: 20px; text-align: center; font-size: 13px;">
        <a href="{VORA['twitter']}" style="color: #555; text-decoration: none; margin: 0 10px;">X</a>
        &middot;
        <a href="{VORA['instagram']}" style="color: #555; text-decoration: none; margin: 0 10px;">Instagram</a>
        &middot;
        <a href="{VORA['linkedin']}" style="color: #555; text-decoration: none; margin: 0 10px;">LinkedIn</a>
    </div>

    <div style="margin-top: 32px; padding-top: 16px; border-top: 1px solid #eee; font-size: 11px; color: #999; line-height: 1.5;">
        <p style="margin: 0;">Vora AI Inc &middot; Irvine, CA</p>
        <p style="margin: 4px 0 0 0;">Don't want to hear from us? <a href="mailto:{VORA['ceo_email']}?subject=Unsubscribe" style="color: #999; text-decoration: underline;">Reply unsubscribe</a> and we'll remove you right away.</p>
    </div>
</div>"""

    return subject, html


# ─── Email Sending ───────────────────────────────────────────────────────────

def send_outreach_email(to_email, subject, html_content, bcc_email=None):
    """Send email via SendGrid with optional BCC."""

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


# ─── Enrichment ──────────────────────────────────────────────────────────────

ENRICHED_FIELDS = [
    'business_name', 'email', 'website', 'category',
    'description', 'services', 'contact_name', 'contact_title',
]


def enrich_csv(csv_path, limit=None):
    """Scrape detailed info for every business and save to an enriched CSV."""

    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    # Deduplicate
    seen = set()
    unique_rows = []
    for row in rows:
        biz = row.get('business_name', '').strip()
        if biz and biz not in seen:
            seen.add(biz)
            unique_rows.append(row)

    if limit:
        unique_rows = unique_rows[:limit]

    total = len(unique_rows)
    enriched = []

    print()
    print("=" * 60)
    print("  ENRICHING LEADS")
    print(f"  Source: {csv_path}")
    print(f"  Businesses: {total}")
    print("=" * 60)
    print()

    for i, row in enumerate(unique_rows):
        business_name = row.get('business_name', 'Unknown')
        email = row.get('email', '').strip()

        print(f"[{i+1}/{total}] {business_name}")

        website = website_from_email(email)
        info = scrape_business_info(business_name, website)

        # Pick best contact
        contact_name = ''
        contact_title = ''
        if info['contacts']:
            c = info['contacts'][0]
            contact_name = c.get('name', '')
            parts = []
            if c.get('title'):
                parts.append(c['title'])
            if c.get('role'):
                parts.append(c['role'])
            contact_title = ', '.join(parts)

        enriched_row = {
            'business_name': business_name,
            'email': email,
            'website': website or '',
            'category': info['category'],
            'description': info['description'][:200] if info['description'] else '',
            'services': '; '.join(info['services'][:5]),
            'contact_name': contact_name,
            'contact_title': contact_title,
        }
        enriched.append(enriched_row)

        status = "✓" if (contact_name or info['services'] or info['description']) else "~"
        print(f"  {status} category={info['category']}, contact={contact_name or 'none'}, services={len(info['services'])}")

        time.sleep(0.3)

    # Save enriched CSV
    out_path = csv_path.replace('.csv', '_enriched.csv')
    with open(out_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=ENRICHED_FIELDS)
        writer.writeheader()
        writer.writerows(enriched)

    # Summary
    with_contacts = sum(1 for r in enriched if r['contact_name'])
    with_services = sum(1 for r in enriched if r['services'])
    with_desc = sum(1 for r in enriched if r['description'])

    print()
    print("=" * 60)
    print(f"  ENRICHMENT DONE")
    print(f"  Saved to: {out_path}")
    print(f"  Total:        {total}")
    print(f"  With contact: {with_contacts}")
    print(f"  With services:{with_services}")
    print(f"  With desc:    {with_desc}")
    print("=" * 60)

    return out_path


def build_info_from_enriched_row(row):
    """Build a business_info dict from an enriched CSV row (no re-scraping)."""
    contacts = []
    if row.get('contact_name'):
        contacts.append({
            'name': row['contact_name'],
            'title': row.get('contact_title', ''),
            'role': '',
        })

    return {
        'name': row.get('business_name', 'Unknown'),
        'website': row.get('website', ''),
        'description': row.get('description', ''),
        'services': [s.strip() for s in row.get('services', '').split(';') if s.strip()],
        'contacts': contacts,
        'category': row.get('category', 'wellness'),
    }


def is_enriched_csv(csv_path):
    """Check if a CSV has enriched columns."""
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        fields = reader.fieldnames or []
    return 'category' in fields and 'contact_name' in fields


# ─── Main Flows ──────────────────────────────────────────────────────────────

def run_single_test(csv_path=None, row_index=0):
    """Run a single test email for one business."""

    if not SENDGRID_API_KEY:
        print("ERROR: SENDGRID_API_KEY not found in .env")
        sys.exit(1)

    print()
    print("=" * 60)
    print("  VORA OUTREACH — SINGLE TEST")
    print(f"  Recipient:  {TEST_RECIPIENT}")
    print(f"  BCC:        {BCC_EMAIL}")
    print("=" * 60)
    print()

    # Get business data
    if csv_path:
        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            rows = list(reader)

        if row_index >= len(rows):
            print(f"ERROR: Row {row_index} out of range ({len(rows)} rows available)")
            sys.exit(1)

        row = rows[row_index]
        business_name = row.get('business_name', row.get('title', 'Unknown'))
        actual_email = row.get('email', 'N/A')
    else:
        business_name = "Coury & Buehler Physical Therapy"
        actual_email = "info@cbphysicaltherapy.com"

    print(f"Business:       {business_name}")
    print(f"Actual email:   {actual_email}")
    print(f"Test send to:   {TEST_RECIPIENT}")
    print()

    # ── Step 1: Get business info (use enriched data or scrape live) ──
    enriched = csv_path and is_enriched_csv(csv_path)
    if enriched:
        print("─── Step 1: Using pre-scraped enriched data ─────────────")
        business_info = build_info_from_enriched_row(row)
    else:
        print("─── Step 1: Scraping business info ─────────────────────")
        website = website_from_email(actual_email)
        if website:
            print(f"  Website (from email domain): {website}")
        else:
            print("  Could not derive website from email. Using business name only.")
        business_info = scrape_business_info(business_name, website)

    print(f"  Category:     {business_info['category']}")
    print(f"  Services:     {', '.join(business_info['services'][:4]) or 'N/A'}")
    print(f"  Contacts:     {len(business_info['contacts'])} found")
    for c in business_info['contacts'][:3]:
        label = c['name']
        if c.get('title'):
            label += f" ({c['title']})"
        if c.get('role'):
            label += f" — {c['role']}"
        print(f"    → {label}")
    desc = business_info['description']
    print(f"  Description:  {(desc[:80] + '...') if len(desc) > 80 else desc or 'N/A'}")
    print()

    # ── Step 2: Generate email ──
    print("─── Step 2: Generating personalized email ──────────────")
    subject, html = generate_email(business_info, test_mode=True)
    print(f"  Subject: {subject}")
    print()

    # ── Step 3: Send ──
    print("─── Step 3: Sending email ──────────────────────────────")
    print(f"  To:   {TEST_RECIPIENT}")
    print(f"  BCC:  {BCC_EMAIL}")
    print(f"  From: {FROM_EMAIL} ({VORA['ceo_name']})")
    print()

    status, error = send_outreach_email(
        TEST_RECIPIENT, subject, html, bcc_email=BCC_EMAIL,
    )

    if status and status in (200, 201, 202):
        print(f"  ✓ Email sent successfully! (status {status})")
        print()
        print(f"  → Check {TEST_RECIPIENT} for the test email")
        print(f"  → BCC copy sent to {BCC_EMAIL}")
    else:
        print(f"  ✗ Failed to send: {error}")

    print()
    print("=" * 60)
    return status in (200, 201, 202) if status else False


def run_batch(csv_path, test_mode=True, limit=None):
    """Run outreach on a CSV of leads."""

    if not SENDGRID_API_KEY:
        print("ERROR: SENDGRID_API_KEY not found in .env")
        sys.exit(1)

    mode_label = "TEST MODE" if test_mode else "🔴 LIVE MODE"
    print()
    print("=" * 60)
    print(f"  VORA OUTREACH — {mode_label}")
    if test_mode:
        print(f"  All emails → {TEST_RECIPIENT}")
    else:
        print("  ⚠ Emails will be sent to ACTUAL contacts!")
    print(f"  BCC: {BCC_EMAIL}")
    print("=" * 60)
    print()

    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    # Deduplicate by business name (keep first email per business)
    seen = set()
    unique_rows = []
    for row in rows:
        biz = row.get('business_name', '').strip()
        if biz and biz not in seen:
            seen.add(biz)
            unique_rows.append(row)

    if limit:
        unique_rows = unique_rows[:limit]

    total = len(unique_rows)
    sent = 0
    failed = 0
    skipped_junk = 0
    skipped_dedup = 0

    # Determine city from CSV filename for Supabase tracking
    csv_basename = os.path.basename(csv_path).replace('_enriched', '').replace('.csv', '')
    city = csv_basename.replace('_ca', ', CA').replace('_', ' ').title().replace(', Ca', ', CA')

    print(f"Processing {total} unique businesses (city: {city})\n")

    for i, row in enumerate(unique_rows):
        business_name = row.get('business_name', 'Unknown')
        actual_email = row.get('email', '').strip()

        if not actual_email:
            continue

        print(f"\n[{i+1}/{total}] {business_name}")
        print(f"  Email: {actual_email}")

        # Fix 2: Junk email filter
        if is_junk_email(actual_email):
            skipped_junk += 1
            print(f"  ⊘ Skipped (junk/placeholder email)")
            continue

        # Fix 1: Supabase global dedup (skip in test mode)
        if not test_mode and supabase_check_email(actual_email):
            skipped_dedup += 1
            print(f"  ⊘ Skipped (already sent — dedup)")
            continue

        # Use enriched data if available, otherwise scrape live
        enriched = is_enriched_csv(csv_path)
        if enriched:
            business_info = build_info_from_enriched_row(row)
        else:
            website = website_from_email(actual_email)
            business_info = scrape_business_info(business_name, website)

        # Generate email
        subject, html = generate_email(business_info, test_mode=test_mode)
        print(f"  Subject: {subject}")

        # Send
        recipient = TEST_RECIPIENT if test_mode else actual_email
        status, error = send_outreach_email(
            recipient, subject, html, bcc_email=BCC_EMAIL,
        )

        if status and status in (200, 201, 202):
            sent += 1
            print(f"  ✓ Sent to {recipient} (status {status})")
            # Fix 1: Record in Supabase after successful send
            if not test_mode:
                supabase_record_sent(
                    actual_email, business_name, city,
                    business_info.get('category', ''),
                )
        else:
            failed += 1
            print(f"  ✗ Failed: {error}")

        time.sleep(1)  # Rate limit

    print(f"\n{'=' * 60}")
    print(f"  DONE!")
    print(f"  Sent:         {sent}")
    print(f"  Failed:       {failed}")
    print(f"  Skipped junk: {skipped_junk}")
    print(f"  Skipped dedup:{skipped_dedup}")
    print(f"  Total:        {total}")
    print(f"{'=' * 60}")


# ─── CLI ─────────────────────────────────────────────────────────────────────

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Vora Outreach Engine — Personalized B2B email outreach',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""\
Examples:
  # Step 1: Enrich leads with scraped details
  python outreach.py results/irvine_ca.csv --enrich
  python outreach.py results/irvine_ca.csv --enrich --limit 1

  # Step 2: Send from enriched CSV
  python outreach.py results/irvine_ca_enriched.csv --single
  python outreach.py results/irvine_ca_enriched.csv --single --row 5
  python outreach.py results/irvine_ca_enriched.csv --test --limit 10
  python outreach.py results/irvine_ca_enriched.csv --live
        """,
    )
    parser.add_argument('csv_path', nargs='?', help='Path to CSV with leads')
    parser.add_argument(
        '--enrich', action='store_true',
        help='Scrape business details and save to enriched CSV (no emails sent)',
    )
    parser.add_argument(
        '--single', action='store_true',
        help='Send a single test email for one business',
    )
    parser.add_argument(
        '--row', type=int, default=0,
        help='Row index to use for --single test (default: 0)',
    )
    parser.add_argument(
        '--test', action='store_true', default=True,
        help='Test mode: all emails go to test address (default)',
    )
    parser.add_argument(
        '--live', action='store_true',
        help='Live mode: send to actual business contacts',
    )
    parser.add_argument(
        '--limit', type=int,
        help='Limit number of businesses to process',
    )

    args = parser.parse_args()

    if args.live:
        args.test = False

    if args.enrich:
        if not args.csv_path:
            print("ERROR: CSV path required for --enrich")
            sys.exit(1)
        enrich_csv(args.csv_path, limit=args.limit)
    elif args.single or not args.csv_path:
        run_single_test(args.csv_path, args.row)
    else:
        run_batch(args.csv_path, test_mode=args.test, limit=args.limit)
