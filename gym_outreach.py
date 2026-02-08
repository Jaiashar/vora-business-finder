#!/usr/bin/env python3
"""
Vora Gym & Fitness Outreach Engine
Specialized outreach for gyms, bootcamps, personal trainers, stretch studios,
yoga/pilates studios, martial arts, and coaches.

Usage:
    # Step 1: Enrich leads
    python gym_outreach.py results/gyms/irvine_ca.csv --enrich

    # Step 2: Send emails
    python gym_outreach.py results/gyms/irvine_ca_enriched.csv --single
    python gym_outreach.py results/gyms/irvine_ca_enriched.csv --test --limit 5
    python gym_outreach.py results/gyms/irvine_ca_enriched.csv --live
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
        return False


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
        pass


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

# ─── Gym & Fitness Business Classification ────────────────────────────────────

GYM_CATEGORIES = {
    'crossfit': [
        'crossfit', 'cross fit', 'cf gym', 'wod', 'functional fitness',
    ],
    'bootcamp': [
        'bootcamp', 'boot camp', 'hiit', 'high intensity',
        'circuit training', 'outdoor fitness', 'camp gladiator',
    ],
    'personal_training': [
        'personal trainer', 'personal training', '1-on-1', 'one on one',
        'private training', 'fitness coach', 'strength coach',
        'training studio', 'small group training',
    ],
    'stretch_recovery': [
        'stretch', 'stretchlab', 'stretch zone', 'recovery studio',
        'mobility', 'flexibility', 'foam roll', 'assisted stretch',
    ],
    'yoga': [
        'yoga', 'hot yoga', 'vinyasa', 'bikram', 'power yoga',
        'yin yoga', 'restorative yoga', 'yoga studio',
    ],
    'pilates': [
        'pilates', 'reformer', 'megaformer', 'lagree', 'barre',
        'club pilates',
    ],
    'martial_arts': [
        'martial arts', 'mma', 'boxing', 'kickboxing', 'jiu jitsu',
        'bjj', 'muay thai', 'karate', 'taekwondo', 'krav maga',
        'judo', 'self defense', 'fight gym',
    ],
    'cycling_spin': [
        'spin', 'cycling', 'soulcycle', 'peloton studio',
        'indoor cycling', 'cycle bar',
    ],
    'gym': [
        'gym', 'fitness center', 'health club', 'athletic club',
        'training facility', 'recreation center', 'weight room',
        'strength training', '24 hour fitness', 'anytime fitness',
        'planet fitness', 'equinox', 'la fitness',
    ],
    'sports_performance': [
        'sports performance', 'athletic training', 'speed training',
        'agility training', 'sports conditioning', 'performance center',
        'athlete development',
    ],
}

# Professional titles to look for in gym/fitness context
PROFESSIONAL_SUFFIXES = (
    'CSCS', 'CPT', 'ACE', 'NASM', 'ACSM', 'ISSA', 'NSCA',
    'RYT', 'E-RYT', 'CES', 'PES', 'FMS', 'CF-L1', 'CF-L2',
    'RKC', 'SFG', 'USAW', 'MS', 'PhD', 'DPT', 'PT',
)

# Leadership / decision-maker roles
DECISION_MAKER_ROLES = [
    'owner', 'founder', 'co-founder', 'ceo', 'president',
    'director', 'managing partner', 'principal', 'general manager',
    'head coach', 'head trainer', 'studio manager', 'gym manager',
    'fitness director', 'operations manager', 'program director',
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

    for pattern in [
        r'<meta[^>]*name=["\']description["\'][^>]*content=["\'](.*?)["\']',
        r'<meta[^>]*content=["\'](.*?)["\'][^>]*name=["\']description["\']',
    ]:
        m = re.search(pattern, html, re.IGNORECASE)
        if m:
            info['description'] = m.group(1).strip()
            break

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

    # Pattern 1: "First Last, CPT/CSCS/NASM..."
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

    # Pattern 2: Name — Role (e.g. "John Smith - Owner")
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

    # Pattern 3: Role — Name (e.g. "Owner: John Smith")
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
        'personal training', 'group classes', 'bootcamp', 'crossfit',
        'hiit', 'strength training', 'cardio', 'yoga', 'pilates',
        'barre', 'spin', 'cycling', 'boxing', 'kickboxing',
        'martial arts', 'stretching', 'assisted stretch', 'recovery',
        'nutrition coaching', 'weight loss', 'body composition',
        'sports performance', 'functional training', 'mobility',
        'flexibility', 'meditation', 'small group training',
        'one on one training', 'youth training', 'senior fitness',
        'prenatal fitness', 'massage therapy', 'cryotherapy',
        'infrared sauna', 'red light therapy',
    ]

    found = []
    for kw in service_keywords:
        if kw in text:
            found.append(kw)
    return found[:6]


def classify_business(business_name, services=None, description=''):
    """Classify the gym/fitness business type for personalized messaging."""
    combined = f"{business_name} {' '.join(services or [])} {description}".lower()

    scores = {}
    for category, keywords in GYM_CATEGORIES.items():
        score = sum(1 for kw in keywords if kw.lower() in combined)
        if score > 0:
            scores[category] = score

    if scores:
        return max(scores, key=scores.get)
    return 'gym'


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
        'category': 'gym',
    }

    if not website_url:
        info['category'] = classify_business(business_name)
        return info

    print(f"  Scraping {website_url}...")

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
        '/coaches', '/trainers', '/our-coaches', '/our-trainers',
        '/staff', '/instructors', '/meet-the-team',
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


# ─── Category-Specific Value Props (Gym & Fitness focused) ───────────────────

VALUE_PROPS = {
    'crossfit': {
        'member_term': 'athletes',
        'hook': (
            "help your athletes train smarter, recover faster, "
            "and stay consistent outside the box"
        ),
        'specific': (
            "CrossFit athletes are already tracking tons of data, but most aren't "
            "using it to actually optimize their training. Vora connects their "
            "wearables, tracks their nutrition and recovery, and gives them a daily "
            "plan that adapts to how their body is actually responding. It's the "
            "piece that turns good athletes into great ones."
        ),
    },
    'bootcamp': {
        'member_term': 'members',
        'hook': (
            "keep your members fired up and accountable "
            "even on the days they're not at camp"
        ),
        'specific': (
            "The energy at bootcamp is unbeatable, but keeping that momentum going "
            "outside of class is the real challenge. Vora gives each member personalized "
            "nutrition plans, recovery guidance, and daily check-ins so they stay "
            "on track between sessions. It turns your bootcamp from a workout into "
            "a full lifestyle transformation."
        ),
    },
    'personal_training': {
        'member_term': 'clients',
        'hook': (
            "give your clients 24/7 coaching support that extends "
            "what you do in every session"
        ),
        'specific': (
            "You're already doing the hard work of building custom programs for "
            "your clients. Vora makes sure they actually follow through on the "
            "other 23 hours of the day. It tracks their nutrition, sleep, recovery, "
            "and activity, and gives them real-time guidance that reinforces "
            "everything you're teaching them."
        ),
    },
    'stretch_recovery': {
        'member_term': 'clients',
        'hook': (
            "help your clients build daily mobility habits "
            "and get more out of every stretch session"
        ),
        'specific': (
            "Your clients come in because they know stretching and recovery matter, "
            "but the real results come from what they do between visits. Vora guides "
            "them through daily mobility routines, tracks their recovery, and reminds "
            "them to stay consistent. It makes your sessions even more effective "
            "and keeps clients coming back."
        ),
    },
    'yoga': {
        'member_term': 'students',
        'hook': (
            "give your students a complete wellness companion "
            "that deepens their practice beyond the mat"
        ),
        'specific': (
            "Your students already care about mind-body wellness. Vora takes that "
            "further by connecting their physical practice with nutrition, sleep, "
            "recovery, and mindfulness tracking. It gives them daily guidance "
            "personalized to their body and goals, and keeps them engaged "
            "with their wellness journey between classes."
        ),
    },
    'pilates': {
        'member_term': 'clients',
        'hook': (
            "help your clients build total body wellness habits "
            "that complement their Pilates practice"
        ),
        'specific': (
            "Your clients are already committed to strength, flexibility, and control. "
            "Vora helps them extend that mindset to nutrition, recovery, and daily "
            "movement. It tracks their progress, adapts to their body, and keeps "
            "them engaged between sessions. It's the perfect companion to "
            "what you're already building in the studio."
        ),
    },
    'martial_arts': {
        'member_term': 'athletes',
        'hook': (
            "give your fighters and students a real edge with personalized "
            "nutrition, recovery, and training insights"
        ),
        'specific': (
            "Martial arts demands peak physical and mental performance. Vora helps "
            "your athletes optimize their weight management, track recovery between "
            "hard sessions, and build nutrition habits that fuel performance. "
            "It's the kind of tool that keeps dedicated students on track and "
            "helps casual students become dedicated ones."
        ),
    },
    'cycling_spin': {
        'member_term': 'riders',
        'hook': (
            "help your riders optimize their performance "
            "and build sustainable fitness habits"
        ),
        'specific': (
            "Your riders are already pushing hard in class. Vora helps them "
            "maximize those efforts by tracking recovery, optimizing nutrition, "
            "and building daily habits that improve their endurance and performance. "
            "It connects their wearable data to real insights so they can see "
            "how their body is responding."
        ),
    },
    'gym': {
        'member_term': 'members',
        'hook': (
            "help your members get real results and actually keep "
            "their memberships active long-term"
        ),
        'specific': (
            "The biggest challenge for any gym is keeping members engaged past "
            "the first few months. Vora solves that by giving each member their "
            "own AI health coach. It builds personalized workout plans, tracks "
            "nutrition and recovery, connects to their wearables, and adapts "
            "to how their body responds. Members who see results don't cancel."
        ),
    },
    'sports_performance': {
        'member_term': 'athletes',
        'hook': (
            "give your athletes a real performance edge with "
            "data-driven training and recovery insights"
        ),
        'specific': (
            "Your athletes are already putting in the work. Vora makes sure "
            "they're getting the most out of it by connecting wearable data, "
            "tracking nutrition and recovery, and building daily plans that adapt "
            "to their body. It's the kind of tool that helps good athletes "
            "break through to the next level."
        ),
    },
}


LOGO_URL = "https://askvora.com/iOS%20LOGO%204.png"
YOUTUBE_ICON = "https://upload.wikimedia.org/wikipedia/commons/thumb/0/09/YouTube_full-color_icon_%282017%29.svg/120px-YouTube_full-color_icon_%282017%29.svg.png"
APP_STORE_ICON = "https://upload.wikimedia.org/wikipedia/commons/thumb/6/67/App_Store_%28iOS%29.svg/120px-App_Store_%28iOS%29.svg.png"


# Category-specific fit reasons for gyms & fitness
FIT_REASONS = {
    'crossfit': (
        "A lot of your athletes are already wearing Apple Watches, Whoops, or Garmin "
        "watches, but most aren't really doing anything meaningful with that data. "
        "Vora connects all of it and turns it into a real daily plan that covers "
        "training, nutrition, and recovery. It makes your box feel like it comes with "
        "a personal sports science team."
    ),
    'bootcamp': (
        "Your members love the energy and accountability of bootcamp, but what "
        "happens between sessions is what really determines their results. Vora "
        "fills that gap with personalized nutrition plans, daily activity goals, "
        "and recovery tracking. It keeps the bootcamp mentality going 24/7 and "
        "makes your program feel like a complete transformation, not just a workout."
    ),
    'personal_training': (
        "As a trainer, you know that results come from consistency outside the gym "
        "just as much as what happens during sessions. Vora is basically your "
        "digital assistant. It tracks nutrition, sleep, recovery, and activity "
        "for each client, and gives them daily guidance that reinforces everything "
        "you're teaching. It's the tool that makes your training stick."
    ),
    'stretch_recovery': (
        "Your clients already know that recovery and mobility are essential, but "
        "building daily habits around it is hard. Vora helps by giving them "
        "personalized daily routines, tracking their recovery metrics from their "
        "wearables, and keeping them engaged between visits. It makes your stretch "
        "sessions part of a bigger wellness picture."
    ),
    'yoga': (
        "Your students are already on a wellness journey, and Vora just makes it "
        "more complete. It connects their physical practice with nutrition tracking, "
        "sleep optimization, and mindfulness, all in one place. It's the kind of "
        "tool that deepens their commitment to their health and keeps them "
        "connected to your studio between classes."
    ),
    'pilates': (
        "Your clients care about precision, control, and total body wellness. "
        "Vora extends that philosophy beyond the reformer by tracking their "
        "nutrition, recovery, and daily movement patterns. It gives them "
        "personalized guidance that complements every session and keeps them "
        "progressing toward their goals."
    ),
    'martial_arts': (
        "Training in martial arts requires discipline in everything. Nutrition, "
        "recovery, weight management, mental focus. Vora brings all of that "
        "together in one app. It tracks what your athletes eat, how they recover, "
        "and adapts their daily plan to where their body is at. It's the kind of "
        "tool that helps students get serious about their training."
    ),
    'cycling_spin': (
        "Your riders are already data-driven. They love seeing their output and "
        "tracking progress. Vora takes that further by connecting their wearable "
        "data with nutrition, recovery, and daily wellness insights. It helps "
        "riders optimize their performance and build habits that keep them "
        "coming back to the bike."
    ),
    'gym': (
        "Most gym members sign up with great intentions but lose momentum after "
        "a few weeks. Vora changes that by giving each member a personalized AI "
        "health coach that tracks their workouts, nutrition, sleep, and recovery. "
        "It connects to 500+ wearables and devices and builds a daily plan that "
        "adapts to their body. Members who actually see progress don't cancel, "
        "and that's where Vora makes the biggest difference."
    ),
    'sports_performance': (
        "Your athletes are already tracking data, but most don't know how to "
        "use it to actually improve. Vora connects all their wearables and turns "
        "that raw data into actionable daily plans for training, nutrition, and "
        "recovery. It's like giving each athlete their own performance analyst "
        "in their pocket."
    ),
}


def clean_description(desc):
    """Clean HTML entities and trim a scraped description for use in email copy."""
    if not desc:
        return ''
    import html as html_mod
    text = html_mod.unescape(desc)
    text = re.sub(r'<[^>]+>', '', text)
    text = re.sub(r'\s+', ' ', text).strip()
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
    category = business_info.get('category', 'gym')

    # Best case: we have a real description to reference
    if desc and len(desc) > 30:
        desc_lower = desc.lower()

        if 'crossfit' in desc_lower or 'cross fit' in desc_lower:
            return (
                f"I was checking out <strong>{biz_name}</strong> and really liked "
                f"what you're building. CrossFit communities are some of the most "
                f"dedicated athletes out there, and I think Vora could take their "
                f"training to another level."
            )
        elif any(kw in desc_lower for kw in ['bootcamp', 'boot camp', 'hiit']):
            return (
                f"I came across <strong>{biz_name}</strong> and the energy you bring "
                f"to your bootcamp programs is exactly the kind of thing Vora was "
                f"designed to complement. Your members are clearly committed, and "
                f"I think we can help them see even better results."
            )
        elif any(kw in desc_lower for kw in ['personal train', 'private train', '1-on-1', 'one on one']):
            return (
                f"I was looking into <strong>{biz_name}</strong> and really liked "
                f"your personal training approach. I know how much goes into building "
                f"custom programs for each client, and Vora is built to make all "
                f"that work stick between sessions."
            )
        elif any(kw in desc_lower for kw in ['stretch', 'mobility', 'flexibility', 'recovery']):
            return (
                f"I came across <strong>{biz_name}</strong> and loved your focus on "
                f"stretching and recovery. Most people underestimate how important "
                f"that is, and I think Vora could help your clients build daily habits "
                f"that make your sessions even more effective."
            )
        elif any(kw in desc_lower for kw in ['yoga', 'vinyasa', 'bikram']):
            return (
                f"I was checking out <strong>{biz_name}</strong> and your yoga "
                f"practice really resonated with me. Your students clearly care "
                f"about whole-body wellness, and Vora is the perfect tool to "
                f"extend that beyond the mat."
            )
        elif any(kw in desc_lower for kw in ['pilates', 'reformer', 'barre', 'lagree']):
            return (
                f"I came across <strong>{biz_name}</strong> and loved what you're "
                f"doing with Pilates. Your clients are clearly invested in their "
                f"bodies, and I think Vora could help them bring that same discipline "
                f"to nutrition and recovery."
            )
        elif any(kw in desc_lower for kw in ['martial art', 'mma', 'boxing', 'kickboxing', 'jiu jitsu', 'bjj']):
            return (
                f"I was checking out <strong>{biz_name}</strong> and your martial arts "
                f"program looks awesome. Training at that level requires total body "
                f"commitment, and I think Vora could be a real game-changer for "
                f"your students' nutrition and recovery."
            )
        elif any(kw in desc_lower for kw in ['spin', 'cycling', 'cycle']):
            return (
                f"I came across <strong>{biz_name}</strong> and loved the cycling "
                f"community you're building. Your riders are clearly serious about "
                f"performance, and I think Vora can help them optimize what happens "
                f"off the bike too."
            )
        elif any(kw in desc_lower for kw in ['sport', 'athlet', 'performance', 'speed', 'agility']):
            return (
                f"I was looking into <strong>{biz_name}</strong> and your sports "
                f"performance work really stood out. Your athletes are already "
                f"putting in the work, and I think Vora could help them get "
                f"even more out of every session."
            )

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

    # Fallback: category-based openers
    cat_openers = {
        'crossfit': f"I came across <strong>{biz_name}</strong> and love what you're building for your athletes.",
        'bootcamp': f"I came across <strong>{biz_name}</strong> and your bootcamp energy is exactly what caught my eye.",
        'personal_training': f"I came across <strong>{biz_name}</strong> and your personal training approach really stood out.",
        'stretch_recovery': f"I came across <strong>{biz_name}</strong> and your focus on stretching and recovery is right up our alley.",
        'yoga': f"I came across <strong>{biz_name}</strong> and your yoga community really stood out to me.",
        'pilates': f"I came across <strong>{biz_name}</strong> and loved what you're doing in your studio.",
        'martial_arts': f"I came across <strong>{biz_name}</strong> and your martial arts program looks incredible.",
        'cycling_spin': f"I came across <strong>{biz_name}</strong> and the cycling community you're building caught my eye.",
        'gym': f"I came across <strong>{biz_name}</strong> and it looks like you're building something great for your community.",
        'sports_performance': f"I came across <strong>{biz_name}</strong> and your performance training really caught my attention.",
    }
    return cat_openers.get(category, cat_openers['gym'])


def generate_email(business_info, test_mode=False):
    """Generate a personalized outreach email. Returns (subject, html_content)."""

    first_name, full_name, contact_title, contact_role = get_greeting_name(business_info)
    vp = VALUE_PROPS.get(business_info['category'], VALUE_PROPS['gym'])
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
    fit_reason = FIT_REASONS.get(category, FIT_REASONS['gym'])

    # Subject line
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

    # Category-specific retention/results line
    retention_lines = {
        'crossfit': f"Boxes using Vora have seen <strong>retention rates go up to 80%</strong>. It's like giving every athlete their own sports science coach in their pocket.",
        'bootcamp': f"Bootcamps using Vora have seen <strong>retention rates go up to 80%</strong>. It turns a great workout into a complete transformation program that members don't want to leave.",
        'personal_training': f"Trainers using Vora have seen <strong>client retention go up to 80%</strong>. When clients see real results, they stick around. Vora makes that happen.",
        'stretch_recovery': f"Studios using Vora have seen <strong>retention rates go up to 80%</strong>. When clients build daily habits around recovery, they keep coming back because they feel the difference.",
        'yoga': f"Studios using Vora have seen <strong>retention rates go up to 80%</strong>. When students connect their practice to real health data, it deepens their commitment to your studio.",
        'pilates': f"Studios using Vora have seen <strong>retention rates go up to 80%</strong>. When clients see how their nutrition and recovery impact their performance, they get even more invested.",
        'martial_arts': f"Gyms using Vora have seen <strong>retention rates go up to 80%</strong>. When students start optimizing their nutrition and recovery, their training jumps to a whole new level.",
        'cycling_spin': f"Studios using Vora have seen <strong>retention rates go up to 80%</strong>. When riders connect their off-bike habits to on-bike performance, they get hooked.",
        'gym': f"Gyms using Vora have seen <strong>member retention go up to 80%</strong>. It's basically like giving every member their own tiny genius health coach in their pocket.",
        'sports_performance': f"Facilities using Vora have seen <strong>retention rates go up to 80%</strong>. When athletes see real, data-driven progress, they don't go anywhere else.",
    }
    retention_line = retention_lines.get(category, retention_lines['gym'])

    # Category-specific CTA paragraph
    cta_lines = {
        'crossfit': f'Would love to show you how Vora could work for your athletes at {biz_name}. You can <a href="{VORA["calendly"]}" style="color: #0066cc; text-decoration: none;">grab a time on my calendar</a>, reply to this email, or text me at {VORA["ceo_phone"]}. Whatever works best.',
        'bootcamp': f'Would love to chat about how Vora could level up your bootcamp at {biz_name}. You can <a href="{VORA["calendly"]}" style="color: #0066cc; text-decoration: none;">grab a time here</a>, reply to this, or text me at {VORA["ceo_phone"]}. Whatever\'s easiest.',
        'personal_training': f'Would love to show you how Vora could work alongside your training at {biz_name}. <a href="{VORA["calendly"]}" style="color: #0066cc; text-decoration: none;">Grab a time on my calendar</a>, reply here, or text me at {VORA["ceo_phone"]}.',
        'stretch_recovery': f'Would love to chat about how Vora could complement what you\'re doing at {biz_name}. <a href="{VORA["calendly"]}" style="color: #0066cc; text-decoration: none;">Grab a quick time here</a>, reply to this email, or text me at {VORA["ceo_phone"]}.',
        'yoga': f'Would love to chat about how Vora could work for your students at {biz_name}. You can <a href="{VORA["calendly"]}" style="color: #0066cc; text-decoration: none;">grab a time here</a>, reply, or text me at {VORA["ceo_phone"]}. Whatever feels right.',
        'pilates': f'Would love to chat about how Vora could fit into what you\'re building at {biz_name}. <a href="{VORA["calendly"]}" style="color: #0066cc; text-decoration: none;">Grab a time on my calendar</a>, reply here, or text me at {VORA["ceo_phone"]}.',
        'martial_arts': f'Would love to show you how Vora could work for your athletes at {biz_name}. You can <a href="{VORA["calendly"]}" style="color: #0066cc; text-decoration: none;">grab a time here</a>, reply to this, or text me at {VORA["ceo_phone"]}.',
        'cycling_spin': f'Would love to chat about how Vora could work for your riders at {biz_name}. <a href="{VORA["calendly"]}" style="color: #0066cc; text-decoration: none;">Grab a time on my calendar</a>, reply here, or text me at {VORA["ceo_phone"]}.',
        'gym': f'Would love to chat about how this could work for {biz_name}. You can <a href="{VORA["calendly"]}" style="color: #0066cc; text-decoration: none;">grab a time on my calendar here</a>, reply to this email, or just text me at {VORA["ceo_phone"]}. Whatever\'s easiest.',
        'sports_performance': f'Would love to show you how Vora could work for your athletes at {biz_name}. <a href="{VORA["calendly"]}" style="color: #0066cc; text-decoration: none;">Grab a time here</a>, reply to this, or text me at {VORA["ceo_phone"]}.',
    }
    cta_line = cta_lines.get(category, cta_lines['gym'])

    # Build category-specific bullet points
    if category in ('crossfit', 'sports_performance', 'martial_arts'):
        bullets = """
        <li style="margin-bottom: 8px;">Connect 500+ wearables (Apple Watch, Whoop, Garmin, etc.) for real training insights</li>
        <li style="margin-bottom: 8px;">Track workouts, PRs, and performance trends with AI-powered analysis</li>
        <li style="margin-bottom: 8px;">Get personalized nutrition plans and meal recommendations matched to training load</li>
        <li style="margin-bottom: 8px;">Monitor recovery readiness so athletes know when to push and when to rest</li>
        <li style="margin-bottom: 8px;">Track body composition, energy, sleep, and long-term performance trends</li>"""
    elif category in ('yoga', 'pilates', 'stretch_recovery'):
        bullets = """
        <li style="margin-bottom: 8px;">Connect 500+ wearables to track recovery, sleep, and daily wellness</li>
        <li style="margin-bottom: 8px;">Get personalized nutrition plans and supplement guidance</li>
        <li style="margin-bottom: 8px;">Follow daily mobility and recovery routines between sessions</li>
        <li style="margin-bottom: 8px;">Track mindfulness, stress, and mental wellness alongside physical health</li>
        <li style="margin-bottom: 8px;">See how their body responds over time with personalized health insights</li>"""
    elif category == 'personal_training':
        bullets = """
        <li style="margin-bottom: 8px;">Connect 500+ wearables (Apple Watch, Whoop, Garmin, etc.) for real-time insights</li>
        <li style="margin-bottom: 8px;">Log workouts and track progress with AI that adapts to each client</li>
        <li style="margin-bottom: 8px;">Get personalized nutrition plans, meal recommendations, and supplement guidance</li>
        <li style="margin-bottom: 8px;">Monitor recovery, sleep, and energy so clients show up ready to train</li>
        <li style="margin-bottom: 8px;">Keep clients accountable between sessions with daily check-ins and goals</li>"""
    elif category == 'bootcamp':
        bullets = """
        <li style="margin-bottom: 8px;">Connect 500+ wearables to track real workout data and progress</li>
        <li style="margin-bottom: 8px;">Get personalized nutrition plans that match each member's goals</li>
        <li style="margin-bottom: 8px;">Daily activity goals and recovery guidance on rest days</li>
        <li style="margin-bottom: 8px;">Body composition tracking and transformation progress over time</li>
        <li style="margin-bottom: 8px;">Keep the accountability going 24/7 with AI-powered daily check-ins</li>"""
    else:
        bullets = """
        <li style="margin-bottom: 8px;">Connect 500+ wearables and devices (Apple Watch, Whoop, Garmin, InBody, etc.)</li>
        <li style="margin-bottom: 8px;">Log workouts intelligently and get real insights on performance and recovery</li>
        <li style="margin-bottom: 8px;">Get personalized nutrition plans, meal recommendations, and supplement guidance</li>
        <li style="margin-bottom: 8px;">Follow recovery protocols from physical (sauna, steam, yoga) to mental wellness</li>
        <li style="margin-bottom: 8px;">Track energy, sleep, recovery, and long-term health trends all in one place</li>"""

    html = f"""\
<div style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif; max-width: 600px; margin: 0 auto; color: #1a1a1a; line-height: 1.7; font-size: 15px;">

    {test_banner}

    <p style="margin-top: 0;">{greeting}</p>

    <p>I'm Jai, the founder of <a href="{VORA['website']}" style="color: #0066cc; text-decoration: none;">Vora</a>. {personal_opening}</p>

    <p>{fit_reason}</p>

    <p>Here's what Vora does for your {vp['member_term']}:</p>

    <ul style="padding-left: 20px; margin: 16px 0;">
        {bullets}
    </ul>

    <p>{retention_line}</p>

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

    <p>{cta_line}</p>

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

    <p style="margin-top: 24px; font-size: 14px; color: #333;"><strong>P.S.</strong> The app is free to try. Download it and see for yourself before we chat.</p>

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
    print("  ENRICHING GYM & FITNESS LEADS")
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
        'category': row.get('category', 'gym'),
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
    print("  VORA GYM OUTREACH — SINGLE TEST")
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
        # Default test business: a well-known gym
        business_name = "CrossFit Irvine"
        actual_email = "info@crossfitirvine.com"

    print(f"Business:       {business_name}")
    print(f"Actual email:   {actual_email}")
    print(f"Test send to:   {TEST_RECIPIENT}")
    print()

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

    print("─── Step 2: Generating personalized email ──────────────")
    subject, html = generate_email(business_info, test_mode=True)
    print(f"  Subject: {subject}")
    print()

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

    mode_label = "TEST MODE" if test_mode else "LIVE MODE"
    print()
    print("=" * 60)
    print(f"  VORA GYM OUTREACH — {mode_label}")
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

    csv_basename = os.path.basename(csv_path).replace('_enriched', '').replace('.csv', '')
    city = csv_basename.replace('_ca', ', CA').replace('_', ' ').title().replace(', Ca', ', CA')

    print(f"Processing {total} unique gym/fitness businesses (city: {city})\n")

    for i, row in enumerate(unique_rows):
        business_name = row.get('business_name', 'Unknown')
        actual_email = row.get('email', '').strip()

        if not actual_email:
            continue

        print(f"\n[{i+1}/{total}] {business_name}")
        print(f"  Email: {actual_email}")

        if is_junk_email(actual_email):
            skipped_junk += 1
            print(f"  ⊘ Skipped (junk/placeholder email)")
            continue

        if not test_mode and supabase_check_email(actual_email):
            skipped_dedup += 1
            print(f"  ⊘ Skipped (already sent — dedup)")
            continue

        enriched = is_enriched_csv(csv_path)
        if enriched:
            business_info = build_info_from_enriched_row(row)
        else:
            website = website_from_email(actual_email)
            business_info = scrape_business_info(business_name, website)

        subject, html = generate_email(business_info, test_mode=test_mode)
        print(f"  Subject: {subject}")

        recipient = TEST_RECIPIENT if test_mode else actual_email
        status, error = send_outreach_email(
            recipient, subject, html, bcc_email=BCC_EMAIL,
        )

        if status and status in (200, 201, 202):
            sent += 1
            print(f"  ✓ Sent to {recipient} (status {status})")
            if not test_mode:
                supabase_record_sent(
                    actual_email, business_name, city,
                    business_info.get('category', ''),
                )
        else:
            failed += 1
            print(f"  ✗ Failed: {error}")

        time.sleep(1)

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
        description='Vora Gym & Fitness Outreach Engine',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""\
Examples:
  # Step 1: Enrich leads with scraped details
  python gym_outreach.py results/gyms/irvine_ca.csv --enrich
  python gym_outreach.py results/gyms/irvine_ca.csv --enrich --limit 1

  # Step 2: Send from enriched CSV
  python gym_outreach.py results/gyms/irvine_ca_enriched.csv --single
  python gym_outreach.py results/gyms/irvine_ca_enriched.csv --single --row 5
  python gym_outreach.py results/gyms/irvine_ca_enriched.csv --test --limit 10
  python gym_outreach.py results/gyms/irvine_ca_enriched.csv --live
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
