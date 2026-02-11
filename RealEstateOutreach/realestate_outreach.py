#!/usr/bin/env python3
"""
Real Estate Broker Preview Outreach Engine
Scrapes real estate business info, identifies key contacts, generates
personalized broker preview invitation emails, and sends via SendGrid.

Usage:
    # Enrich leads with scraped website data
    python realestate_outreach.py results/zip_92618.csv --enrich

    # Single test email (sends to test address)
    python realestate_outreach.py results/zip_92618_enriched.csv --single --row 0

    # Test mode: all emails go to test address
    python realestate_outreach.py results/zip_92618_enriched.csv --test --limit 5

    # Live mode: emails go to actual contacts
    python realestate_outreach.py results/zip_92618_enriched.csv --live
"""

import argparse
import csv
import html as html_mod
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
    Mail, Email, To, Cc, Bcc, Personalization, Content, ReplyTo, Header
)

# Load .env from THIS directory (RealEstateOutreach/)
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(SCRIPT_DIR, '.env'))

# ─── Configuration ───────────────────────────────────────────────────────────

SENDGRID_API_KEY = os.getenv('SENDGRID_API_KEY')
FROM_EMAIL = os.getenv('FROM_EMAIL', 'jai@askvora.com')
FROM_NAME = os.getenv('FROM_NAME', 'Ravina Ashar')
REPLY_TO_EMAIL = os.getenv('REPLY_TO_EMAIL', 'raveenarealtor@gmail.com')
REPLY_TO_NAME = os.getenv('REPLY_TO_NAME', 'Ravina Ashar')
BCC_EMAIL = os.getenv('BCC_EMAIL', 'raveenarealtor@gmail.com')
TEST_RECIPIENT = os.getenv('TEST_RECIPIENT', 'jaikrish15@gmail.com')

# ─── Dedup Tracking ──────────────────────────────────────────────────────────

SENT_EMAILS_FILE = os.path.join(SCRIPT_DIR, 'sent_emails.json')


def load_sent_emails():
    """Load the set of already-sent emails from JSON."""
    if os.path.exists(SENT_EMAILS_FILE):
        with open(SENT_EMAILS_FILE, 'r') as f:
            data = json.load(f)
            return data  # dict: {email: {business_name, sent_at, zip_code}}
    return {}


def save_sent_emails(sent_db):
    """Save the sent emails dict to JSON."""
    with open(SENT_EMAILS_FILE, 'w') as f:
        json.dump(sent_db, f, indent=2)


def record_sent(sent_db, email, business_name, zip_code=''):
    """Record an email as sent."""
    from datetime import datetime
    sent_db[email.lower().strip()] = {
        'business_name': business_name,
        'sent_at': datetime.now().isoformat(),
        'zip_code': zip_code,
    }
    save_sent_emails(sent_db)


def was_already_sent(sent_db, email):
    """Check if we already sent to this email."""
    return email.lower().strip() in sent_db

ssl._create_default_https_context = ssl._create_unverified_context

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


# ─── Sender / Listing Info ─────────────────────────────────────────────────

AGENT = {
    'name': 'Ravina Ashar',
    'title': 'REALTOR\u00ae',
    'dre': '01936601',
    'brokerage': 'Modha Realty Inc.',
    'phone': '949-232-9260',
    'email': 'raveenarealtor@gmail.com',
    'youtube': 'https://www.youtube.com/watch?v=nRo9EaS9_cI&feature=youtu.be',
    'instagram': 'https://www.instagram.com/asharhomes/',
}

PROPERTY = {
    'address': '7 Boone, Irvine, CA 92620',
    'neighborhood': 'Northwood',
    'school_district': 'Irvine Unified School District',
    'preview_date': 'Friday, February 13th',
    'preview_time': '4:00 PM \u2013 6:00 PM',
    'open_house_sat': 'Saturday, 2/14 | 1:00 PM \u2013 4:00 PM',
    'open_house_sun': 'Sunday, 2/15 | 1:00 PM \u2013 4:00 PM',
}

# ─── Web Scraping ────────────────────────────────────────────────────────────

HEADERS = {
    'User-Agent': (
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) '
        'AppleWebKit/537.36 (KHTML, like Gecko) '
        'Chrome/120.0.0.0 Safari/537.36'
    ),
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.5',
}


def fetch_page(url, timeout=10):
    """Fetch a webpage and return its HTML content."""
    try:
        if not url.startswith(('http://', 'https://')):
            url = 'https://' + url
        req = urllib.request.Request(url, headers=HEADERS)
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            content = resp.read()
            try:
                return content.decode('utf-8')
            except UnicodeDecodeError:
                return content.decode('latin-1', errors='ignore')
    except Exception:
        return None


def strip_html(html_text):
    """Remove HTML tags and decode entities."""
    text = re.sub(r'<script[^>]*>.*?</script>', ' ', html_text, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r'<style[^>]*>.*?</style>', ' ', text, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r'<[^>]+>', ' ', text)
    text = html_mod.unescape(text)
    text = re.sub(r'\s+', ' ', text)
    return text.strip()


def extract_meta(html_text):
    """Extract meta description, title, and OG data from HTML."""
    info = {}

    title_match = re.search(r'<title[^>]*>(.*?)</title>', html_text, re.IGNORECASE | re.DOTALL)
    if title_match:
        info['title'] = strip_html(title_match.group(1)).strip()

    for pattern in [
        r'<meta[^>]*name=["\']description["\'][^>]*content=["\'](.*?)["\']',
        r'<meta[^>]*content=["\'](.*?)["\'][^>]*name=["\']description["\']',
    ]:
        m = re.search(pattern, html_text, re.IGNORECASE)
        if m:
            info['description'] = m.group(1).strip()
            break

    og = re.search(
        r'<meta[^>]*property=["\']og:description["\'][^>]*content=["\'](.*?)["\']',
        html_text, re.IGNORECASE,
    )
    if og:
        info['og_description'] = og.group(1).strip()

    return info


# ─── Real Estate Classification ──────────────────────────────────────────────

RE_CATEGORIES = {
    'residential_brokerage': [
        'real estate', 'realty', 'realtor', 'broker', 'brokerage',
        'residential', 'homes', 'properties', 'listings',
    ],
    'luxury_real_estate': [
        'luxury', 'estate', 'mansion', 'premium', 'exclusive',
        'high-end', 'prestige', 'sotheby', 'compass',
    ],
    'commercial_real_estate': [
        'commercial', 'office space', 'retail space', 'industrial',
        'investment property', 'cre',
    ],
    'property_management': [
        'property management', 'rental', 'leasing', 'tenant',
        'landlord', 'hoa',
    ],
    'international_real_estate': [
        'international', 'global', 'overseas', 'foreign buyer',
        'chinese buyer', 'asian buyer', 'juwai', 'caimeiju',
    ],
    'development': [
        'development', 'developer', 'new construction', 'builder',
        'new homes', 'community development',
    ],
}

# Professional suffixes for real estate
RE_PROFESSIONAL_SUFFIXES = (
    'GRI', 'CRS', 'ABR', 'SRES', 'SRS', 'CIPS', 'CLHMS', 'CNE',
    'RENE', 'PSA', 'MRP', 'AHWD', 'C2EX', 'CCIM', 'CRE', 'CPM',
    'MBA', 'JD', 'CPA', 'PhD',
)

RE_DECISION_MAKER_ROLES = [
    'broker', 'managing broker', 'owner', 'founder', 'co-founder',
    'team leader', 'team lead', 'principal', 'managing partner',
    'branch manager', 'office manager', 'director', 'president',
    'ceo', 'associate broker', 'broker associate', 'designated broker',
    'realtor', 'agent', 'sales associate', 'real estate agent',
]

PERSONAL_EMAIL_DOMAINS = {
    'gmail.com', 'yahoo.com', 'hotmail.com', 'outlook.com',
    'aol.com', 'icloud.com', 'me.com', 'live.com', 'msn.com',
    'comcast.net', 'cox.net', 'att.net', 'verizon.net',
    'sbcglobal.net', 'bellsouth.net', 'earthlink.net',
    'mail.com', 'protonmail.com', 'zoho.com',
}

RE_SERVICE_KEYWORDS = [
    'residential sales', 'buyer representation', 'seller representation',
    'listing services', 'property valuation', 'market analysis',
    'luxury homes', 'first-time buyers', 'relocation',
    'investment properties', 'international buyers', 'new construction',
    'short sale', 'foreclosure', 'estate sale', 'probate',
    'property management', 'commercial real estate', 'land sales',
    'condo sales', 'townhome', 'single family', 'multi-family',
    'adu', 'accessory dwelling', 'home staging', 'open house',
]


def classify_re_business(business_name, services=None, description=''):
    """Classify the real estate business type."""
    combined = f"{business_name} {' '.join(services or [])} {description}".lower()
    scores = {}
    for category, keywords in RE_CATEGORIES.items():
        score = sum(1 for kw in keywords if kw.lower() in combined)
        if score > 0:
            scores[category] = score
    if scores:
        return max(scores, key=scores.get)
    return 'residential_brokerage'


def extract_re_services(html_text):
    """Extract real estate services from the page."""
    if not html_text:
        return []
    text = strip_html(html_text).lower()
    found = []
    for kw in RE_SERVICE_KEYWORDS:
        if kw in text:
            found.append(kw)
    return found[:6]


# ─── Contact Extraction ─────────────────────────────────────────────────────

def find_contacts_on_page(html_text):
    """Extract potential contact names from HTML."""
    if not html_text:
        return []

    text = strip_html(html_text)
    contacts = []
    seen_names = set()

    # Pattern 1: Name with real estate suffixes
    suffixes = '|'.join(RE_PROFESSIONAL_SUFFIXES)
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

    # Pattern 2: Name - Role (e.g., "John Smith - Broker")
    roles_re = '|'.join(RE_DECISION_MAKER_ROLES)
    pattern3 = re.compile(
        rf'([A-Z][a-z]+(?:\s+[A-Z]\.?)?\s+[A-Z][a-z]+)'
        rf'\s*[-\u2013\u2014|,]\s*(?:the\s+)?({roles_re})',
        re.IGNORECASE,
    )
    for m in pattern3.finditer(text):
        name = m.group(1).strip()
        role = m.group(2).strip().title()
        if name not in seen_names and 5 < len(name) < 40:
            seen_names.add(name)
            contacts.append({'name': name, 'title': '', 'role': role})

    # Pattern 3: Role - Name (e.g., "Broker: John Smith")
    pattern4 = re.compile(
        rf'(?:{roles_re})\s*[-\u2013\u2014:,|]\s*'
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
                'role': m.group(0).split(name)[0].strip(' -\u2013\u2014:,|').title(),
            })

    # Pattern 4: "REALTOR" or "DRE#" near a name
    pattern5 = re.compile(
        r'([A-Z][a-z]+(?:\s+[A-Z]\.?)?\s+[A-Z][a-z]+)'
        r'[\s,]*(?:REALTOR|DRE\s*#?\s*\d+)',
        re.IGNORECASE,
    )
    for m in pattern5.finditer(text):
        name = m.group(1).strip()
        if name not in seen_names and 5 < len(name) < 40:
            seen_names.add(name)
            contacts.append({'name': name, 'title': 'REALTOR', 'role': ''})

    # Sort: decision-makers first
    def contact_priority(c):
        if c['role'].lower() in [r for r in RE_DECISION_MAKER_ROLES]:
            return 0
        if c['title']:
            return 1
        return 2

    contacts.sort(key=contact_priority)
    return contacts[:5]


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
        'category': 'residential_brokerage',
    }

    if not website_url:
        info['category'] = classify_re_business(business_name)
        return info

    print(f"  Scraping {website_url}...")

    html_text = fetch_page(website_url)
    if html_text:
        meta = extract_meta(html_text)
        info['description'] = (
            meta.get('description')
            or meta.get('og_description')
            or meta.get('title', '')
        )
        info['services'] = extract_re_services(html_text)
        info['contacts'] = find_contacts_on_page(html_text)

    # Try team / about pages
    parsed = urlparse(
        website_url if website_url.startswith('http') else 'https://' + website_url
    )
    base = f"{parsed.scheme}://{parsed.netloc}"

    team_paths = [
        '/about', '/about-us', '/our-team', '/team',
        '/agents', '/our-agents', '/meet-the-team', '/staff',
        '/leadership', '/brokers',
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

    info['category'] = classify_re_business(
        business_name, info['services'], info['description']
    )

    return info


# ─── Name Extraction from Business Name ──────────────────────────────────────

# Words that appear in business names but are NOT person names
NOT_A_NAME = {
    'real', 'estate', 'realty', 'homes', 'home', 'group', 'team', 'properties',
    'property', 'luxury', 'premier', 'prime', 'elite', 'top', 'best', 'modern',
    'first', 'golden', 'orange', 'county', 'california', 'coastal', 'pacific',
    'seven', 'gables', 'coldwell', 'banker', 'keller', 'williams', 'compass',
    'century', 'douglas', 'elliman', 'berkshire', 'hathaway', 'sotheby',
    'international', 'associates', 'inc', 'llc', 'the', 'and', 'your', 'our',
    'all', 'new', 'north', 'south', 'east', 'west', 'blue', 'true', 'smart',
    'exp', 'max', 'one', 'usa', 'asian', 'american', 'national', 'regional',
    'office', 'agent', 'broker', 'realtor', 'selling', 'buying', 'listing',
    'investment', 'development', 'marble', 'legacy', 'spectrum', 'harvest',
    'sun', 'cal', 'angeles', 'los', 'irvine', 'tustin', 'newport',
    'than', 'other', 'find', 'more', 'about', 'this', 'that', 'with',
    'guaranteed', 'sold', 'certified', 'licensed', 'professional',
}


def is_valid_person_name(name):
    """Check if a string looks like a real person name, not a business term."""
    if not name or len(name) < 4 or len(name) > 35:
        return False
    parts = name.strip().split()
    if len(parts) < 2 or len(parts) > 4:
        return False
    # Every word must start uppercase + be alphabetic
    for p in parts:
        clean = p.replace('-', '').replace("'", '').replace('.', '')
        if not clean or not clean[0].isupper() or not clean.isalpha():
            return False
    # No word should be a known business/garbage term
    for p in parts:
        if p.lower() in NOT_A_NAME:
            return False
    return True


def extract_name_from_business(business_name):
    """Try to extract a person's name from the business name.

    Many real estate listings are like:
      'Sean Healey and The Healey Home Selling Team'
      'Brad Dhesi, The BSD Real Estate Group'
      'Jessica Hong - Realty One Group'
      'Thomas Lorini - eXp Realty DRE#02049448'
    """
    if not business_name:
        return None

    # Clean up
    name = business_name.strip()

    # Try to get the part before common separators
    for sep in [' - ', ' | ', ' @ ', ' — ', ' – ', ', ', ' at ']:
        if sep in name:
            candidate = name.split(sep)[0].strip()
            # Remove common prefixes/suffixes
            candidate = re.sub(r'\s*(REALTOR|DRE\s*#?\d+|Realtor|Agent|Broker)\s*$', '', candidate).strip()
            if is_valid_person_name(candidate):
                return candidate

    # Try first two words if they look like a name
    words = name.split()
    if len(words) >= 2:
        candidate = f"{words[0]} {words[1]}"
        if is_valid_person_name(candidate):
            return candidate

    return None


# ─── Email Generation ────────────────────────────────────────────────────────

def get_greeting_name(business_info):
    """Get the best name to use in the email greeting."""
    # First: check scraped contacts (but validate them)
    if business_info.get('contacts'):
        first_contact = business_info['contacts'][0]
        name = first_contact.get('name', '')
        if is_valid_person_name(name):
            first_name = name.split()[0]
            return first_name, name, first_contact.get('title', ''), first_contact.get('role', '')

    # Second: try to extract from business name
    biz_name = business_info.get('name', '')
    extracted = extract_name_from_business(biz_name)
    if extracted:
        first_name = extracted.split()[0]
        return first_name, extracted, '', ''

    return None, None, '', ''


def clean_description(desc):
    """Clean HTML entities and trim a scraped description."""
    if not desc:
        return ''
    text = html_mod.unescape(desc)
    text = re.sub(r'<[^>]+>', '', text)
    text = re.sub(r'\s+', ' ', text).strip()
    if '. ' in text[:150]:
        text = text[:text.index('. ', 0, 150) + 1]
    elif len(text) > 150:
        text = text[:150].rsplit(' ', 1)[0]
    return text


def build_personalized_opening(business_info):
    """Build a personalized opening line referencing the business."""
    biz_name = business_info['name']
    desc = clean_description(business_info.get('description', ''))
    services = business_info.get('services', [])
    category = business_info.get('category', 'residential_brokerage')

    # Best case: description tells us about them
    if desc and len(desc) > 30:
        desc_lower = desc.lower()

        if 'luxury' in desc_lower or 'estate' in desc_lower or 'premium' in desc_lower:
            return (
                f"I noticed <strong>{biz_name}</strong>'s strong presence in the "
                f"luxury residential market. Given your clientele, I wanted to "
                f"personally invite you to preview a standout Northwood listing "
                f"before it hits the weekend open house crowds."
            )
        elif 'international' in desc_lower or 'global' in desc_lower or 'chinese' in desc_lower or 'asian' in desc_lower:
            return (
                f"I came across <strong>{biz_name}</strong> and your work with "
                f"international buyers really stood out. This Northwood property "
                f"with its generous lot and Irvine Unified schools is exactly "
                f"the type of home that resonates with relocating families."
            )
        elif 'invest' in desc_lower or 'development' in desc_lower:
            return (
                f"I noticed <strong>{biz_name}</strong>'s work in property "
                f"investment and development. This Northwood listing offers "
                f"rare expansion potential on a 7,000+ sq ft lot that I think "
                f"your investor clients would want to see."
            )
        elif any(kw in desc_lower for kw in ['family', 'community', 'neighborhood', 'residential']):
            return (
                f"I came across <strong>{biz_name}</strong> and your focus on "
                f"helping families find the right home really resonated. This "
                f"Northwood cul-de-sac property in Irvine Unified is exactly "
                f"the kind of listing your buyers would love."
            )
        elif any(kw in desc_lower for kw in ['irvine', 'orange county', 'oc', 'newport', 'costa mesa']):
            return (
                f"As a fellow OC-area professional, I wanted to make sure "
                f"<strong>{biz_name}</strong> had early access to this "
                f"Northwood listing. The lot size and cul-de-sac location "
                f"make it a rare find in today's Irvine market."
            )

    # Second best: services tell us something
    if services:
        top = services[:2]
        svc_str = f"{top[0]} and {top[1]}" if len(top) > 1 else top[0]
        return (
            f"I noticed <strong>{biz_name}</strong>'s expertise in "
            f"{svc_str} and wanted to personally invite you to preview "
            f"this Northwood listing before the weekend open house."
        )

    # Fallback: category-based
    cat_openers = {
        'residential_brokerage': (
            f"I wanted to reach out to <strong>{biz_name}</strong> about "
            f"an upcoming broker preview for a standout Northwood listing "
            f"that I think your active buyers would love."
        ),
        'luxury_real_estate': (
            f"Given <strong>{biz_name}</strong>'s position in the luxury "
            f"market, I wanted to personally invite you to preview this "
            f"rare 7,000+ sq ft lot in Northwood before the public open house."
        ),
        'commercial_real_estate': (
            f"I wanted to connect with <strong>{biz_name}</strong> about "
            f"a Northwood residential listing with unique investment "
            f"potential, including possible ADU opportunities."
        ),
        'property_management': (
            f"I noticed <strong>{biz_name}</strong>'s work in property "
            f"management and thought this detached Northwood home would "
            f"be of interest for your investor clients."
        ),
        'international_real_estate': (
            f"I came across <strong>{biz_name}</strong> and your "
            f"international buyer network. This Irvine Unified property "
            f"with a generous lot is exactly what relocating families look for."
        ),
        'development': (
            f"Given <strong>{biz_name}</strong>'s development expertise, "
            f"I thought you'd want early access to this 7,000+ sq ft lot "
            f"in Northwood with ADU potential."
        ),
    }
    return cat_openers.get(category, cat_openers['residential_brokerage'])


def generate_email(business_info, test_mode=False):
    """Generate a personalized broker preview email. Returns (subject, html_content)."""

    first_name, full_name, contact_title, contact_role = get_greeting_name(business_info)
    biz_name = business_info['name']

    # Greeting
    if first_name:
        greeting = f"Dear {first_name},"
    else:
        greeting = "Hi,"

    # Personalized opening
    personal_opening = build_personalized_opening(business_info)

    # Subject line — short, punchy, and unique per recipient to avoid threading
    short_name = biz_name
    if len(short_name) > 35:
        short_name = short_name[:35].rsplit(' ', 1)[0].rstrip(' -,')
    if first_name:
        subject = f"{first_name}, {short_name} + Ashar Homes"
    else:
        subject = f"{short_name} + Ashar Homes"

    # Test mode banner
    test_banner = ""
    if test_mode:
        test_banner = (
            '<div style="background: #fff3cd; border: 1px solid #ffc107; '
            'border-radius: 8px; padding: 12px 16px; margin-bottom: 20px; '
            'font-size: 13px; color: #856404;">'
            f'<strong>TEST MODE</strong> \u2014 This email would normally go to: '
            f'{business_info.get("email", "the actual agent")} '
            f'({biz_name})</div>'
        )

    p = PROPERTY
    a = AGENT

    html = f"""\
<div style="font-family: Georgia, 'Times New Roman', serif; max-width: 620px; margin: 0 auto; color: #333333; line-height: 1.7; font-size: 15px;">

    {test_banner}

    <p style="margin-top: 0;">{greeting}</p>

    <p>{personal_opening}</p>

    <p>Join us for a <strong>Broker Preview this {p['preview_date']} from {p['preview_time']}</strong>
    at <strong>{p['address']}</strong>.</p>

    <p>In a market where many Irvine listings offer limited outdoor space, this
    {p['neighborhood']} residence stands out with a larger-than-typical lot on a quiet,
    private cul-de-sac. It is an ideal fit for your clients who are looking for more
    "elbow room" and the prestige of the <strong>{p['school_district']}</strong>.</p>

    <p style="font-size: 16px; font-weight: bold; color: #1a1a1a; margin: 25px 0 12px 0;">
        Why This Home Stands Out:
    </p>

    <div style="padding: 12px 16px; background-color: #faf9f6; border-left: 3px solid #c8a96e; margin-bottom: 20px;">
        <p style="margin: 0 0 8px 0;">
            <strong>Expansion Potential:</strong> A generous lot size offering rare flexibility
            for outdoor living or a potential ADU (Buyer to verify).
        </p>
        <p style="margin: 0 0 8px 0;">
            <strong>Cul-de-Sac Privacy:</strong> Minimal traffic and a strong neighborhood feel
            in one of Irvine's most established residential communities.
        </p>
        <p style="margin: 0;">
            <strong>Detached Value:</strong> A true detached single-family home suited for
            long-term ownership and multi-generational needs.
        </p>
    </div>

    <p style="font-size: 16px; font-weight: bold; color: #1a1a1a; margin: 25px 0 12px 0;">
        Preview Details:
    </p>

    <div style="padding: 16px 20px; background-color: #f0ede6; border-radius: 6px; margin-bottom: 20px;">
        <p style="margin: 0; line-height: 1.9;">
            <strong>When:</strong> {p['preview_date']} | {p['preview_time']}<br>
            <strong>Where:</strong> {p['address']}<br>
            <strong>Refreshments:</strong> Refreshments will be provided for attending agents.
        </p>
    </div>

    <p><strong>Property Video:</strong>
    <a href="{a['youtube']}" style="color: #c8a96e; text-decoration: underline;">View the Video Tour Here</a></p>

    <p style="font-size: 16px; font-weight: bold; color: #1a1a1a; margin: 25px 0 12px 0;">
        Public Open House Schedule:
    </p>

    <div style="padding: 12px 20px; background-color: #faf9f6; border-left: 3px solid #c8a96e; margin-bottom: 20px;">
        <p style="margin: 0; line-height: 1.9;">
            {p['open_house_sat']}<br>
            {p['open_house_sun']}
        </p>
    </div>

    <p style="font-size: 16px; font-weight: bold; color: #1a1a1a; margin: 25px 0 12px 0;">
        Agent Notes:
    </p>

    <p><strong>Strategic Opportunity:</strong> The lot configuration and location make this a
    high-velocity listing. I encourage you to preview the property this Friday before the
    weekend crowds.</p>

    <p>I look forward to seeing you there and discussing how this home fits your active
    buyers' needs.</p>

    <div style="margin-top: 28px; padding-top: 20px; border-top: 1px solid #e0e0e0;">
        <p style="margin: 0 0 4px 0;">Best regards,</p>
        <p style="margin: 10px 0 4px 0; font-weight: bold; font-size: 16px; color: #1a1a1a;">
            {a['name']}
        </p>
        <p style="margin: 0; font-size: 14px; color: #666666; line-height: 1.7;">
            {a['title']} | DRE# {a['dre']}<br>
            {a['brokerage']}<br>
            <a href="tel:{a['phone'].replace('-', '')}" style="color: #c8a96e; text-decoration: none;">{a['phone']}</a><br>
            <a href="mailto:{a['email']}" style="color: #c8a96e; text-decoration: none;">{a['email']}</a>
        </p>

        <p style="margin: 12px 0 0 0; font-size: 14px;">
            <a href="{a['youtube']}" style="color: #c8a96e; text-decoration: none;">YouTube</a>
            &nbsp;|&nbsp;
            <a href="{a['instagram']}" style="color: #c8a96e; text-decoration: none;">Instagram</a>
        </p>
    </div>

    <div style="margin-top: 32px; padding-top: 16px; border-top: 1px solid #eee; font-size: 11px; color: #999; line-height: 1.5;">
        <p style="margin: 0;">{a['brokerage']} &middot; Irvine, CA</p>
        <p style="margin: 4px 0 0 0;">Don't want to hear from us?
        <a href="mailto:{a['email']}?subject=Unsubscribe" style="color: #999; text-decoration: underline;">Reply unsubscribe</a>
        and we'll remove you right away.</p>
    </div>
</div>"""

    return subject, html


# ─── Email Sending ───────────────────────────────────────────────────────────

def send_outreach_email(to_email, subject, html_content, bcc_email=None):
    """Send email via SendGrid with CC to Raveena, BCC, and Reply-To."""
    message = Mail()
    message.from_email = Email(FROM_EMAIL, FROM_NAME)
    message.reply_to = ReplyTo(REPLY_TO_EMAIL, REPLY_TO_NAME)
    message.subject = subject

    personalization = Personalization()
    personalization.add_to(To(to_email))
    personalization.add_cc(Cc(REPLY_TO_EMAIL, REPLY_TO_NAME))
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
    print("  ENRICHING REAL ESTATE LEADS")
    print(f"  Source: {csv_path}")
    print(f"  Businesses: {total}")
    print("=" * 60)
    print()

    # Track contacts we already found per website domain so we don't
    # assign the same generic corporate contact to multiple businesses
    seen_domain_contacts = {}  # domain -> contact_name already used

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
            candidate_name = c.get('name', '')

            # Check if we already used this same contact from the same domain
            domain = (website or '').lower().replace('https://', '').replace('http://', '').split('/')[0]
            if domain and domain in seen_domain_contacts:
                if seen_domain_contacts[domain] == candidate_name:
                    # Same contact from same corporate site — skip it
                    print(f"  (Skipping shared contact '{candidate_name}' from {domain})")
                    candidate_name = ''
                else:
                    contact_name = candidate_name
            else:
                contact_name = candidate_name

            if contact_name:
                if domain:
                    seen_domain_contacts[domain] = contact_name
                parts = []
                if c.get('title'):
                    parts.append(c['title'])
                if c.get('role'):
                    parts.append(c['role'])
                contact_title = ', '.join(parts)

        enriched.append({
            'business_name': business_name,
            'email': email,
            'website': info['website'] or '',
            'category': info['category'],
            'description': (info['description'] or '')[:300],
            'services': '; '.join(info['services']),
            'contact_name': contact_name,
            'contact_title': contact_title,
        })

        print(f"  Category: {info['category']}")
        if contact_name:
            print(f"  Contact: {contact_name}")
        if info['services']:
            print(f"  Services: {', '.join(info['services'][:3])}")
        print()

        time.sleep(0.5)

    # Save enriched CSV
    out_path = csv_path.replace('.csv', '_enriched.csv')
    with open(out_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=ENRICHED_FIELDS)
        writer.writeheader()
        writer.writerows(enriched)

    print("=" * 60)
    print(f"  Enriched {len(enriched)} leads")
    print(f"  Saved to: {out_path}")
    print("=" * 60)
    return out_path


# ─── Build Info from Enriched Row ────────────────────────────────────────────

def build_info_from_enriched_row(row):
    """Build a business_info dict from an already-enriched CSV row."""
    contacts = []
    contact_name = row.get('contact_name', '').strip()
    if contact_name:
        contacts.append({
            'name': contact_name,
            'title': row.get('contact_title', ''),
            'role': '',
        })
    return {
        'name': row.get('business_name', 'Unknown'),
        'email': row.get('email', ''),
        'website': row.get('website', ''),
        'description': row.get('description', ''),
        'services': [s.strip() for s in row.get('services', '').split(';') if s.strip()],
        'contacts': contacts,
        'category': row.get('category', 'residential_brokerage'),
    }


def is_enriched_csv(fieldnames):
    """Check if a CSV has enrichment columns."""
    return fieldnames and 'category' in fieldnames and 'contact_name' in fieldnames


# ─── Modes ───────────────────────────────────────────────────────────────────

def run_single_test(csv_path=None, row_idx=0):
    """Send a single test email. Always sends to TEST_RECIPIENT."""

    print()
    print("=" * 60)
    print("  BROKER PREVIEW OUTREACH \u2014 SINGLE TEST")
    print(f"  Recipient:  {TEST_RECIPIENT}")
    print(f"  BCC:        {BCC_EMAIL}")
    print("=" * 60)
    print()

    # Get business data
    if csv_path:
        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            fieldnames = reader.fieldnames
            rows = list(reader)

        if row_idx >= len(rows):
            print(f"ERROR: Row {row_idx} out of range (CSV has {len(rows)} rows)")
            return

        row = rows[row_idx]
        business_name = row.get('business_name', 'Unknown')
        email = row.get('email', '').strip()

        print(f"Business: {business_name}")
        print(f"Email:    {email}")
        print()

        # Use enriched data if available
        if is_enriched_csv(fieldnames):
            print("\u2500\u2500\u2500 Using enriched data \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500")
            business_info = build_info_from_enriched_row(row)
            business_info['email'] = email
        else:
            print("\u2500\u2500\u2500 Scraping live data \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500")
            website = website_from_email(email)
            business_info = scrape_business_info(business_name, website)
            business_info['email'] = email
    else:
        # Demo mode - use a sample business
        business_info = {
            'name': 'Sample Real Estate Office',
            'email': 'test@example.com',
            'website': '',
            'description': 'Full-service real estate brokerage serving Orange County families.',
            'services': ['residential sales', 'buyer representation'],
            'contacts': [{'name': 'John Smith', 'title': 'REALTOR', 'role': 'Broker'}],
            'category': 'residential_brokerage',
        }

    print(f"  Category:    {business_info['category']}")
    print(f"  Services:    {', '.join(business_info['services'][:3]) or 'none found'}")
    print(f"  Contacts:    {business_info['contacts'][0]['name'] if business_info['contacts'] else 'none found'}")
    print(f"  Description: {business_info.get('description', '')[:80] or 'none'}")
    print()

    # Generate email
    print("\u2500\u2500\u2500 Generating email \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500")
    subject, html = generate_email(business_info, test_mode=True)
    print(f"  Subject: {subject}")
    print()

    # Send
    print("\u2500\u2500\u2500 Sending email \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500")
    print(f"  To:   {TEST_RECIPIENT}")
    print(f"  BCC:  {BCC_EMAIL}")
    print(f"  From: {FROM_EMAIL} ({FROM_NAME})")
    print()

    status, error = send_outreach_email(
        TEST_RECIPIENT, subject, html, bcc_email=BCC_EMAIL,
    )

    if status and status in (200, 201, 202):
        print(f"  \u2713 Email sent successfully! (status {status})")
        print()
        print(f"  \u2192 Check {TEST_RECIPIENT} for the test email")
        print(f"  \u2192 BCC copy sent to {BCC_EMAIL}")
    else:
        print(f"  \u2717 Failed to send: {error}")

    print()
    print("=" * 60)


def run_batch(csv_path, test_mode=True, limit=None):
    """Send emails to all leads in a CSV."""

    mode_label = "TEST MODE" if test_mode else "LIVE MODE"

    print()
    print("=" * 60)
    print(f"  BROKER PREVIEW OUTREACH \u2014 {mode_label}")
    if test_mode:
        print(f"  All emails \u2192 {TEST_RECIPIENT}")
    else:
        print("  \u26a0 Emails will be sent to ACTUAL contacts!")
    print(f"  BCC: {BCC_EMAIL}")
    print("=" * 60)
    print()

    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames
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
    sent = 0
    skipped = 0
    duped = 0
    failed = 0

    # Load dedup database
    sent_db = load_sent_emails()
    print(f"  Dedup DB: {len(sent_db)} previously sent emails loaded")
    print()

    # Try to extract zip from filename for tracking
    zip_code = ''
    base = os.path.basename(csv_path)
    zip_match = re.search(r'zip_(\d{5})', base)
    if zip_match:
        zip_code = zip_match.group(1)

    for i, row in enumerate(unique_rows):
        business_name = row.get('business_name', 'Unknown')
        actual_email = row.get('email', '').strip()

        print(f"[{i+1}/{total}] {business_name}")

        if not actual_email:
            skipped += 1
            print(f"  SKIP \u2014 no email")
            continue

        if is_junk_email(actual_email):
            skipped += 1
            print(f"  SKIP \u2014 junk email: {actual_email}")
            continue

        # Dedup check (live mode only)
        if not test_mode and was_already_sent(sent_db, actual_email):
            duped += 1
            prev = sent_db[actual_email.lower().strip()]
            print(f"  SKIP \u2014 already sent ({prev.get('business_name', '?')}, {prev.get('zip_code', '?')})")
            continue

        # Build business info
        if is_enriched_csv(fieldnames):
            business_info = build_info_from_enriched_row(row)
            business_info['email'] = actual_email
        else:
            website = website_from_email(actual_email)
            business_info = scrape_business_info(business_name, website)
            business_info['email'] = actual_email

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
            print(f"  \u2713 Sent to {recipient} (status {status})")
            # Record in dedup DB (live mode only)
            if not test_mode:
                record_sent(sent_db, actual_email, business_name, zip_code)
        else:
            failed += 1
            print(f"  \u2717 Failed: {error}")

        # Rate limit
        if i < total - 1:
            time.sleep(1)

    print()
    print("=" * 60)
    print(f"  DONE!")
    print(f"  Total:   {total}")
    print(f"  Sent:    {sent}")
    print(f"  Duped:   {duped}")
    print(f"  Skipped: {skipped}")
    print(f"  Failed:  {failed}")
    print("=" * 60)


# ─── Main ────────────────────────────────────────────────────────────────────

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Real Estate Broker Preview Outreach Engine'
    )

    parser.add_argument('csv', nargs='?', default=None,
                        help='Path to leads CSV')

    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument('--enrich', action='store_true',
                      help='Enrich CSV with scraped website data')
    mode.add_argument('--single', action='store_true',
                      help='Send single test email (to TEST_RECIPIENT)')
    mode.add_argument('--test', action='store_true',
                      help='Test mode: all emails go to TEST_RECIPIENT')
    mode.add_argument('--live', action='store_true',
                      help='Live mode: emails go to actual contacts')

    parser.add_argument('--row', type=int, default=0,
                        help='Row index for --single mode (default: 0)')
    parser.add_argument('--limit', type=int, default=None,
                        help='Limit number of emails/enrichments')

    args = parser.parse_args()

    if not SENDGRID_API_KEY:
        print("ERROR: SENDGRID_API_KEY not found in .env")
        sys.exit(1)

    if args.enrich:
        if not args.csv:
            print("ERROR: --enrich requires a CSV path")
            sys.exit(1)
        enrich_csv(args.csv, args.limit)
    elif args.single:
        run_single_test(args.csv, args.row)
    elif args.test:
        if not args.csv:
            print("ERROR: --test requires a CSV path")
            sys.exit(1)
        run_batch(args.csv, test_mode=True, limit=args.limit)
    elif args.live:
        if not args.csv:
            print("ERROR: --live requires a CSV path")
            sys.exit(1)
        run_batch(args.csv, test_mode=False, limit=args.limit)
