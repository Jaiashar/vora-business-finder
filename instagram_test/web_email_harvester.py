#!/usr/bin/env python3
"""
Web Email Harvester v2 — DuckDuckGo + Direct Website Scraping
================================================================
Searches DuckDuckGo for fitness consumer emails and scrapes result pages.
DDG directly surfaces emails in search snippets, making this very efficient.

Strategy:
  1. Search DDG for hundreds of fitness + "@gmail.com" type queries
  2. Extract emails directly from search result snippets
  3. Also visit top result pages for more emails
  4. Filter strictly for consumer-only (personal) emails
"""

import os
import re
import csv
import sys
import time
import json
import random
import urllib.request
import urllib.error
import ssl
import html as html_lib
from datetime import datetime
from urllib.parse import quote_plus, urlparse

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

SSL_CTX = ssl.create_default_context()
SSL_CTX.check_hostname = False
SSL_CTX.verify_mode = ssl.CERT_NONE

UA_LIST = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_0) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64; rv:121.0) Gecko/20100101 Firefox/121.0",
]

EMAIL_RE = re.compile(r'[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}')

JUNK_DOMAINS = {
    'sentry.io', 'w3.org', 'schema.org', 'example.com', 'domain.com',
    'wixpress.com', 'cloudflare.com', 'mailchimp.com', 'googleapis.com',
    'facebook.com', 'instagram.com', 'youtube.com',
    'twitter.com', 'x.com', 'tiktok.com', 'spotify.com', 'pinterest.com',
    'linkedin.com', 'apple.com', 'google.com', 'gstatic.com',
    'duckduckgo.com', 'bing.com', 'microsoft.com',
    'onetrust.com', 'feedspot.com', 'imginn.org',
    'mailgun.com', 'sendgrid.com', 'hubspot.com',
    'stripe.com', 'shopify.com', 'squarespace.com', 'wix.com',
    'wordpress.com', 'gravatar.com', 'amazonaws.com',
    'googletagmanager.com', 'google-analytics.com',
    'patreon.com', 'substackinc.com', 'beacons.ai',
}

PERSONAL_PROVIDERS = {
    'gmail.com', 'yahoo.com', 'hotmail.com', 'outlook.com', 'aol.com',
    'icloud.com', 'me.com', 'mac.com', 'protonmail.com', 'proton.me',
    'live.com', 'msn.com', 'ymail.com', 'rocketmail.com',
    'mail.com', 'zoho.com', 'gmx.com', 'gmx.net', 'fastmail.com',
    'hey.com', 'tutanota.com', 'pm.me', 'comcast.net', 'att.net',
    'verizon.net', 'cox.net', 'sbcglobal.net', 'charter.net',
    'bellsouth.net', 'earthlink.net', 'optonline.net',
    'yahoo.co.uk', 'hotmail.co.uk', 'btinternet.com',
    'googlemail.com', 'inbox.com',
}

BIZ_EMAIL_DOMAIN_RE = re.compile(
    r'(?:management|agency|agencies|talent|sports|media|'
    r'entertainment|creative|marketing|digital|consulting|'
    r'production|records|music|mgmt|mgt|'
    r'prfirm|publicrelations|publicity|'
    r'represent|brand|supplement|'
    r'apparel|clothing|wear|merch|'
    r'capital|ventures|holdings|group|inc|'
    r'foundation|association|federation|league|'
    r'network|solutions|services|global|'
    r'collective|partners)', re.I)

BIZ_PREFIXES = {
    'hello', 'info', 'contact', 'press', 'media', 'pr',
    'booking', 'inquiries', 'enquiries', 'enquiry',
    'team', 'sales', 'admin', 'general',
    'office', 'talent', 'management', 'agents', 'collab',
    'partnerships', 'sponsor', 'business', 'support',
    'help', 'careers', 'jobs', 'hire', 'reception',
    'frontdesk', 'orders', 'billing', 'marketing',
    'advertising', 'events', 'membership', 'magazin',
    'editorial', 'editor', 'studio', 'submissions',
    'casting', 'news', 'promo', 'merch', 'shop',
    'store', 'wholesale', 'customerservice',
}


def extract_emails(text):
    if not text:
        return []
    found = EMAIL_RE.findall(text.lower())
    return list(set(
        e for e in found
        if e.split('@')[1] not in JUNK_DOMAINS
        and not any(k in e for k in ['sentry', 'noreply', 'no-reply', 'unsubscribe',
                                      'webpack', 'placeholder', 'donotreply',
                                      'mailer-daemon', 'u003e', 'test@', 'user@'])
        and not e.endswith(('.png', '.jpg', '.gif', '.svg', '.css', '.js', '.woff'))
        and len(e.split('@')[1]) >= 4
        and len(e) < 60
    ))


def is_consumer_email(email):
    """Only accept personal/individual emails."""
    email = email.lower()
    local, domain = email.split('@', 1)
    if domain in PERSONAL_PROVIDERS:
        return True
    if BIZ_EMAIL_DOMAIN_RE.search(domain):
        return False
    if local in BIZ_PREFIXES:
        return False
    return True


def fetch_page(url, timeout=8):
    try:
        if not url.startswith(('http://', 'https://')):
            url = 'https://' + url
        req = urllib.request.Request(url, headers={
            "User-Agent": random.choice(UA_LIST),
            "Accept": "text/html,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
        })
        resp = urllib.request.urlopen(req, timeout=timeout, context=SSL_CTX)
        content = resp.read()[:300000]
        try:
            return content.decode('utf-8')
        except UnicodeDecodeError:
            return content.decode('latin-1', errors='ignore')
    except:
        return None


def ddg_search(query):
    """Search DuckDuckGo HTML and extract emails + result URLs."""
    url = f'https://html.duckduckgo.com/html/?q={quote_plus(query)}'
    html = fetch_page(url, 12)
    if not html:
        return [], []

    emails = extract_emails(html)
    consumer = [e for e in emails if is_consumer_email(e)]

    # Extract result URLs for deeper scraping
    result_urls = []
    for match in re.finditer(r'class="result__a"[^>]*href="(https?://[^"]+)"', html):
        u = match.group(1)
        parsed = urlparse(u)
        skip = ['duckduckgo.com', 'google.com', 'bing.com', 'facebook.com',
                'instagram.com', 'twitter.com', 'linkedin.com', 'youtube.com',
                'amazon.com', 'wikipedia.org', 'reddit.com', 'pinterest.com',
                'yelp.com', 'tiktok.com', 'feedspot.com']
        if not any(s in parsed.netloc for s in skip):
            result_urls.append(u)

    # Also try to get URLs from result snippets
    for match in re.finditer(r'class="result__url"[^>]*>(.*?)</', html, re.DOTALL):
        raw_url = re.sub(r'<[^>]+>', '', match.group(1)).strip()
        if raw_url and not raw_url.startswith('http'):
            raw_url = 'https://' + raw_url
        if raw_url:
            result_urls.append(raw_url)

    return consumer, list(dict.fromkeys(result_urls))[:8]


def scrape_page_emails(url):
    """Visit a page and its contact page to find consumer emails."""
    all_emails = set()

    html = fetch_page(url)
    if not html:
        return []
    raw_emails = extract_emails(html)
    all_emails.update(e for e in raw_emails if is_consumer_email(e))

    # Try contact page
    parsed = urlparse(url if url.startswith('http') else 'https://' + url)
    base = f"{parsed.scheme}://{parsed.netloc}"
    for path in ['/contact', '/about', '/contact-us']:
        if len(all_emails) >= 3:
            break
        sub = fetch_page(base + path, 5)
        if sub:
            raw = extract_emails(sub)
            all_emails.update(e for e in raw if is_consumer_email(e))
        time.sleep(0.3)

    return list(all_emails)


# ═══════════════════════════════════════════════════════════════════
# SEARCH QUERIES — designed to surface consumer emails
# ═══════════════════════════════════════════════════════════════════
def generate_queries():
    queries = []

    # Direct gmail searches — most likely to return consumer emails
    gmail_templates = [
        'personal trainer {city} @gmail.com',
        'fitness coach {city} @gmail.com',
        'yoga instructor {city} @gmail.com',
        'pilates instructor {city} @gmail.com',
        'running coach {city} @gmail.com',
        'nutrition coach {city} @gmail.com',
        'wellness coach {city} @gmail.com',
        'crossfit coach {city} @gmail.com',
        'strength coach {city} @gmail.com',
        'health coach {city} @gmail.com',
        'online trainer {city} @gmail.com',
        'swim coach {city} @gmail.com',
        'cycling coach {city} @gmail.com',
    ]

    cities = [
        "Los Angeles", "New York", "Chicago", "Houston", "Phoenix",
        "San Diego", "Dallas", "Austin", "San Francisco", "Denver",
        "Nashville", "Portland", "Seattle", "Atlanta", "Miami",
        "Minneapolis", "Tampa", "Charlotte", "Raleigh", "Orlando",
        "Salt Lake City", "Boulder", "Scottsdale", "Santa Monica",
        "San Jose", "Sacramento", "Las Vegas", "Honolulu",
        "Charleston", "Madison", "Ann Arbor", "Asheville",
        "Pittsburgh", "Columbus", "Indianapolis", "Kansas City",
        "Milwaukee", "Omaha", "Jacksonville", "Fort Lauderdale",
        "Boca Raton", "Reno", "Spokane", "Tacoma", "Bellevue",
        "Colorado Springs", "Fort Collins", "Provo",
        "Brooklyn", "Queens", "Bronx", "Manhattan",
        "Beverly Hills", "Venice Beach", "Malibu", "Pasadena",
        "Oakland", "Berkeley", "Palo Alto", "Mountain View",
        "Hoboken", "Jersey City", "Stamford", "Greenwich",
        "Bethesda", "Arlington VA", "Alexandria VA",
        "Beaverton", "Lake Oswego", "Bend",
        "Naperville", "Evanston", "Schaumburg",
        "Plano", "Frisco", "McKinney", "Katy",
        "Tempe", "Mesa", "Chandler", "Gilbert",
        "Wilmington NC", "Durham", "Chapel Hill",
        "Greenville SC", "Columbia SC",
        "Knoxville", "Chattanooga",
        "Lexington KY", "Louisville",
        "Birmingham AL", "Huntsville AL",
        "Little Rock", "Fayetteville AR",
        "Tulsa", "Norman OK",
        "Wichita", "Overland Park",
    ]

    for tmpl in gmail_templates:
        for city in cities:
            queries.append(tmpl.replace('{city}', city))

    # Generic fitness email queries
    generic = [
        'certified personal trainer email @gmail.com',
        'NASM certified trainer email @gmail.com',
        'ACE certified personal trainer @gmail.com',
        'ISSA certified trainer email @gmail.com',
        'NSCA certified trainer @gmail.com',
        'certified yoga teacher RYT email @gmail.com',
        'registered dietitian email @gmail.com',
        'CrossFit Level 1 coach email @gmail.com',
        'USAT triathlon coach email @gmail.com',
        'RRCA running coach email @gmail.com',
        'precision nutrition coach email @gmail.com',
        'online fitness coaching email @gmail.com',
        'macro coach email @gmail.com',
        'bodybuilding coach email @gmail.com',
        'powerlifting coach email @gmail.com',
        'mobility coach email @gmail.com',
        'flexibility coach email @gmail.com',
        'prenatal fitness email @gmail.com',
        'postnatal fitness email @gmail.com',
        'senior fitness trainer email @gmail.com',
        'kids fitness coach email @gmail.com',
        'sports performance coach email @gmail.com',
        'functional fitness coach email @gmail.com',
        'kettlebell instructor email @gmail.com',
        'spin instructor email @gmail.com',
        'barre instructor email @gmail.com',
        'dance fitness instructor email @gmail.com',
        'martial arts instructor email @gmail.com',
        'boxing trainer email @gmail.com',
        'outdoor fitness trainer email @gmail.com',
        'boot camp instructor email @gmail.com',
        'calisthenics coach email @gmail.com',

        # Wearable-related
        'apple watch fitness email @gmail.com',
        'garmin runner email @gmail.com',
        'oura ring wellness email @gmail.com',
        'whoop fitness email @gmail.com',
        'strava athlete email @gmail.com',

        # Linktree/bio pages
        'site:linktr.ee personal trainer @gmail.com',
        'site:linktr.ee fitness coach @gmail.com',
        'site:linktr.ee yoga teacher @gmail.com',
        'site:linktr.ee nutrition coach @gmail.com',
        'site:linktr.ee running coach @gmail.com',
        'site:linktr.ee wellness coach @gmail.com',
        'site:bio.link fitness @gmail.com',
        'site:campsite.bio fitness @gmail.com',

        # Forum/community
        'fitness coach contact email @yahoo.com',
        'personal trainer contact @hotmail.com',
        'yoga instructor @outlook.com',
        'nutrition coach @icloud.com',

        # Other providers
        'fitness coach email @yahoo.com',
        'personal trainer email @hotmail.com',
        'yoga instructor email @outlook.com',
        'running coach email @yahoo.com',
        'pilates instructor email @hotmail.com',
        'wellness coach email @outlook.com',
    ]
    queries.extend(generic)

    random.shuffle(queries)
    return queries


def main():
    print("+" + "=" * 60 + "+")
    print("|  WEB EMAIL HARVESTER v2 — DDG CONSUMER SEARCH            |")
    print(f"|  {datetime.now().strftime('%Y-%m-%d %H:%M:%S'):<57}|")
    print("+" + "=" * 60 + "+\n")

    progress_file = os.path.join(BASE_DIR, "web_harvest_progress.json")
    csv_path = os.path.join(BASE_DIR, "web_harvest_leads.csv")

    progress = {}
    if os.path.exists(progress_file):
        with open(progress_file) as f:
            progress = json.load(f)

    done_queries = set(progress.get("_done_queries", []))
    scraped_sites = set(progress.get("_scraped_sites", []))
    email_map = progress.get("_email_map", {})

    all_queries = generate_queries()
    new_queries = [q for q in all_queries if q not in done_queries]

    print(f"  {len(email_map)} emails already found")
    print(f"  {len(done_queries)}/{len(all_queries)} queries done")
    print(f"  {len(new_queries)} queries remaining\n")

    queries_done_this_run = 0
    new_emails_this_run = 0

    for qi, query in enumerate(new_queries):
        queries_done_this_run += 1
        short_q = query[:55]
        print(f"  [{queries_done_this_run}] {short_q}...", end=" ", flush=True)

        # Search DDG
        ddg_emails, result_urls = ddg_search(query)
        batch_new = 0

        for e in ddg_emails:
            if e not in email_map:
                email_map[e] = {"source": "ddg_snippet", "query": query[:60]}
                batch_new += 1
                new_emails_this_run += 1

        # Scrape top result pages for more emails
        for rurl in result_urls[:3]:
            if rurl in scraped_sites:
                continue
            scraped_sites.add(rurl)
            try:
                page_emails = scrape_page_emails(rurl)
                for e in page_emails:
                    if e not in email_map:
                        email_map[e] = {"source": rurl[:80], "query": query[:60]}
                        batch_new += 1
                        new_emails_this_run += 1
            except:
                pass
            time.sleep(random.uniform(0.5, 1.0))

        if batch_new > 0:
            print(f"+{batch_new} (total: {len(email_map)})")
        else:
            print("0")

        done_queries.add(query)

        # Save every 20 queries
        if queries_done_this_run % 20 == 0:
            progress["_done_queries"] = list(done_queries)
            progress["_scraped_sites"] = list(scraped_sites)
            progress["_email_map"] = email_map
            with open(progress_file, 'w') as f:
                json.dump(progress, f, indent=1, default=str)
            print(f"  ── saved | {len(email_map)} total emails | +{new_emails_this_run} this run ──")

        time.sleep(random.uniform(5, 10))

    # Final save
    progress["_done_queries"] = list(done_queries)
    progress["_scraped_sites"] = list(scraped_sites)
    progress["_email_map"] = email_map
    with open(progress_file, 'w') as f:
        json.dump(progress, f, indent=1, default=str)

    # Build CSV
    rows = []
    for email, meta in email_map.items():
        rows.append({
            "email": email,
            "name": "",
            "ig_username": "",
            "followers": 0,
            "platform": "website",
            "category": "fitness_influencer",
            "tags": "web_harvest",
            "bio": "",
            "external_url": meta.get("source", ""),
            "email_source": "web_search",
            "is_business": False,
            "business_category": "",
        })

    fieldnames = ["email", "name", "ig_username", "followers", "platform",
                  "category", "tags", "bio", "external_url", "email_source",
                  "is_business", "business_category"]

    with open(csv_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print(f"\n  ═══ RESULTS ═══")
    print(f"  Queries run: {queries_done_this_run}")
    print(f"  Total unique consumer emails: {len(email_map)}")
    print(f"  CSV: {csv_path}")


if __name__ == "__main__":
    main()
