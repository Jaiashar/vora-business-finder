#!/usr/bin/env python3
"""
Ecosia + Brave Search Email Harvester
========================================
Uses Ecosia (and Brave when available) to find consumer fitness emails.
Ecosia typically yields 5-7 emails per query.
Rotates between engines and uses longer delays to avoid rate limits.
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
from datetime import datetime
from urllib.parse import quote_plus

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

PERSONAL_PROVIDERS = {
    'gmail.com', 'yahoo.com', 'hotmail.com', 'outlook.com', 'aol.com',
    'icloud.com', 'me.com', 'mac.com', 'protonmail.com', 'proton.me',
    'live.com', 'msn.com', 'ymail.com', 'rocketmail.com',
    'mail.com', 'zoho.com', 'gmx.com', 'gmx.net', 'fastmail.com',
    'hey.com', 'tutanota.com', 'pm.me', 'comcast.net', 'att.net',
    'verizon.net', 'cox.net', 'sbcglobal.net', 'charter.net',
    'bellsouth.net', 'earthlink.net', 'googlemail.com',
    'yahoo.co.uk', 'hotmail.co.uk', 'btinternet.com',
}

BIZ_EMAIL_DOMAIN_RE = re.compile(
    r'(?:management|agency|agencies|talent|sports|media|'
    r'entertainment|creative|marketing|digital|consulting|'
    r'production|records|music|mgmt|mgt|'
    r'publicrelations|publicity|represent|brand|supplement|'
    r'apparel|clothing|wear|merch|capital|ventures|holdings|'
    r'group|inc|foundation|association|federation|league|'
    r'network|solutions|services|global|collective|partners)', re.I)

BIZ_PREFIXES = {
    'hello', 'info', 'contact', 'press', 'media', 'pr',
    'booking', 'inquiries', 'enquiries', 'team', 'sales', 'admin',
    'office', 'talent', 'management', 'agents', 'collab',
    'partnerships', 'sponsor', 'business', 'support', 'help',
    'marketing', 'events', 'membership', 'editorial', 'studio',
    'shop', 'store', 'wholesale', 'customerservice',
}

JUNK_DOMAINS = {
    'sentry.io', 'w3.org', 'schema.org', 'example.com', 'domain.com',
    'ecosia.org', 'brave.com', 'google.com', 'bing.com',
    'facebook.com', 'instagram.com', 'youtube.com', 'twitter.com',
    'linkedin.com', 'pinterest.com', 'tiktok.com',
    'wix.com', 'squarespace.com', 'wordpress.com', 'shopify.com',
    'mailchimp.com', 'hubspot.com', 'sendgrid.com', 'mailgun.com',
    'amazonaws.com', 'cloudflare.com', 'stanwith.me',
}


# Words that mean it's a BUSINESS gmail, not a person's email
# Only include words that individuals almost never use in personal email
BIZ_LOCAL_WORDS = {
    'gym', 'studio', 'center', 'centre',  # physical locations
    'crossfit',  # crossfit boxes are businesses
    'llc', 'inc', 'official', 'corp',  # corporate
    'brand', 'shop', 'store', 'merch',  # retail
    'academy', 'institute', 'clinic',  # institutions
    'medspa', 'salon', 'barber',
    'church', 'ministry',
    'association', 'federation', 'league', 'council',
    'corporation', 'enterprise', 'venture', 'holding',
    'foundation', 'nonprofit', 'charity',
    'district', 'department',
    'team',  # as in "team accounts" e.g. teamfitness@
    'solutions', 'services', 'group',  # business suffixes
}


def is_consumer_email(email):
    email = email.lower()
    if '@' not in email:
        return False
    local, domain = email.split('@', 1)

    if domain in JUNK_DOMAINS:
        return False

    if any(k in email for k in ['noreply', 'no-reply', 'unsubscribe', 'donotreply',
                                 'mailer-daemon', 'sentry', 'webpack', 'test@',
                                 'user@', 'placeholder', 'u003e']):
        return False

    if email.endswith(('.png', '.jpg', '.gif', '.svg', '.css', '.js')):
        return False

    if len(email) > 60 or len(domain) < 4:
        return False

    # Skip .org, .edu, .gov — these are organizations, not consumers
    if domain.endswith(('.org', '.edu', '.gov')):
        return False

    # Skip .k12 school districts
    if '.k12.' in domain:
        return False

    # Skip known company domains
    company_domains = [
        'precisionnutrition.com', 'nasm.org', 'acefitness.org',
        'issa.com', 'nsca.com', 'crossfit.com',
        'fleetfeet', 'titleboxing', 'orangetheory',
        'anytimefitness', 'planetfitness', 'goldsgym',
        'equinox', '24hourfitness', 'lifetime',
        'crunch.com', 'lafitness', 'ymca',
        'lululemon', 'nike.com', 'adidas.com', 'underarmour',
        'peloton.com', 'beachbody', 'herbalife',
    ]
    if any(c in domain for c in company_domains):
        return False

    # Skip malformed emails (domain has no proper TLD)
    if not re.match(r'^[a-z0-9.-]+\.[a-z]{2,6}$', domain):
        return False

    if domain in PERSONAL_PROVIDERS:
        # Even personal provider emails can be business accounts
        for word in BIZ_LOCAL_WORDS:
            if word in local:
                return False
        # Long local parts on gmail are usually business names, not individuals
        if len(local) > 24:
            return False
        # Any combo of city+activity or activity+city
        activity_words = r'(?:martial|yoga|massage|pilates|boxing|karate|judo|bjj|taekwondo|kickbox|mma|jiu|gracie|muaythai)'
        city_words = r'(?:houston|dallas|austin|miami|tampa|denver|seattle|portland|chicago|brooklyn|nyc|atlanta|nashville|charlotte|orlando|phoenix|boulder|boise|raleigh|durham|spring|north|south|east|west|longbeach|sandiego|losangeles|sanfrancisco|scottsdale|saltlake|fortworth|kansascity|stlouis)'
        if re.search(city_words + r'.*' + activity_words, local, re.I):
            return False
        if re.search(activity_words + r'.*' + city_words, local, re.I):
            return False
        return True

    if BIZ_EMAIL_DOMAIN_RE.search(domain):
        return False

    if local in BIZ_PREFIXES:
        return False

    # Non-personal domain: check the local part too
    for word in BIZ_LOCAL_WORDS:
        if word in local:
            return False

    return True


def search_ecosia(query):
    url = f'https://www.ecosia.org/search?q={quote_plus(query)}'
    req = urllib.request.Request(url, headers={
        "User-Agent": random.choice(UA_LIST),
        "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
    })
    resp = urllib.request.urlopen(req, timeout=15, context=SSL_CTX)
    raw = resp.read().decode('utf-8', errors='ignore')
    all_emails = EMAIL_RE.findall(raw.lower())
    return [e for e in set(all_emails) if is_consumer_email(e)]


def search_brave(query):
    url = f'https://search.brave.com/search?q={quote_plus(query)}'
    req = urllib.request.Request(url, headers={
        "User-Agent": random.choice(UA_LIST),
        "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
    })
    resp = urllib.request.urlopen(req, timeout=15, context=SSL_CTX)
    raw = resp.read().decode('utf-8', errors='ignore')
    all_emails = EMAIL_RE.findall(raw.lower())
    return [e for e in set(all_emails) if is_consumer_email(e)]


def search_with_rotation(query, ecosia_ok=True, brave_ok=True):
    """Try available engines, return emails and updated availability."""
    emails = []

    if ecosia_ok:
        try:
            emails = search_ecosia(query)
            return emails, ecosia_ok, brave_ok
        except urllib.error.HTTPError as e:
            if e.code == 429:
                ecosia_ok = False
                print("(ecosia 429) ", end="", flush=True)
            else:
                print(f"(ecosia {e.code}) ", end="", flush=True)
        except Exception:
            pass

    if brave_ok:
        try:
            emails = search_brave(query)
            return emails, ecosia_ok, brave_ok
        except urllib.error.HTTPError as e:
            if e.code == 429:
                brave_ok = False
                print("(brave 429) ", end="", flush=True)
            else:
                print(f"(brave {e.code}) ", end="", flush=True)
        except Exception:
            pass

    return emails, ecosia_ok, brave_ok


def generate_queries():
    queries = []

    roles = [
        "personal trainer", "fitness coach", "yoga instructor",
        "pilates instructor", "running coach", "nutrition coach",
        "wellness coach", "crossfit coach", "strength coach",
        "health coach", "online trainer", "swim coach",
        "cycling coach", "boxing trainer", "martial arts instructor",
        "dance fitness instructor", "barre instructor",
        "spin instructor", "boot camp instructor",
        "sports performance coach", "functional fitness",
        "kettlebell instructor", "calisthenics coach",
        "mobility coach", "weight loss coach",
        "body transformation coach", "macro coach",
        "holistic health coach", "sports dietitian",
        "prenatal fitness instructor", "senior fitness trainer",
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
        "Milwaukee", "Jacksonville", "Fort Lauderdale",
        "Boca Raton", "Reno", "Spokane", "Tacoma", "Bellevue",
        "Colorado Springs", "Fort Collins", "Brooklyn", "Manhattan",
        "Beverly Hills", "Pasadena", "Oakland", "Berkeley",
        "Hoboken", "Bethesda", "Arlington VA", "Bend OR",
        "Plano", "Frisco", "Tempe", "Mesa", "Durham",
        "Knoxville", "Lexington", "Tulsa", "Omaha",
        "Chattanooga", "Savannah", "Richmond VA", "Norfolk",
        "Virginia Beach", "Wilmington NC", "Greenville SC",
        "Columbia SC", "Birmingham", "Huntsville",
        "Little Rock", "Boise", "Des Moines", "Wichita",
        "Sioux Falls", "Fargo", "Billings", "Anchorage",
        "Albuquerque", "El Paso", "Lubbock", "Tucson",
        "Bakersfield", "Fresno", "Riverside", "Long Beach",
        "Anaheim", "Irvine", "Chandler", "Gilbert",
    ]

    for role in roles:
        sampled = random.sample(cities, min(12, len(cities)))
        for city in sampled:
            queries.append(f'{role} {city} email @gmail.com')

    # Cert + gmail queries
    certs = [
        "NASM certified", "ACE certified", "ISSA certified",
        "NSCA certified", "ACSM certified", "RYT 200", "RYT 500",
        "CrossFit Level 1", "USAT triathlon coach",
        "RRCA running coach", "Precision Nutrition",
    ]
    for cert in certs:
        queries.append(f'{cert} trainer email @gmail.com')
        queries.append(f'{cert} coach contact email @gmail.com')

    # Activity queries
    activities = [
        "marathon training", "triathlon training", "5K training",
        "obstacle course racing", "powerlifting", "bodybuilding",
        "yoga retreat", "fitness retreat", "running group",
        "cycling team", "swim team masters", "kickboxing",
        "rock climbing coaching", "ski fitness instructor",
    ]
    for act in activities:
        queries.append(f'{act} coach email @gmail.com')

    # Wearable queries
    for w in ["apple watch fitness", "garmin runner", "oura ring", "whoop fitness", "strava athlete"]:
        queries.append(f'{w} email @gmail.com')
        queries.append(f'{w} contact email')

    # Other providers
    for role in random.sample(roles, 8):
        queries.append(f'{role} email @yahoo.com')
        queries.append(f'{role} email @hotmail.com')
        queries.append(f'{role} email @outlook.com')

    random.shuffle(queries)
    return queries


def main():
    print("+" + "=" * 60 + "+")
    print("|  ECOSIA/BRAVE EMAIL HARVESTER — FITNESS CONSUMERS        |")
    print(f"|  {datetime.now().strftime('%Y-%m-%d %H:%M:%S'):<57}|")
    print("+" + "=" * 60 + "+\n")

    progress_file = os.path.join(BASE_DIR, "search_harvest_progress.json")
    csv_path = os.path.join(BASE_DIR, "search_harvest_leads.csv")

    progress = {}
    if os.path.exists(progress_file):
        with open(progress_file) as f:
            progress = json.load(f)

    done_queries = set(progress.get("_done_queries", []))
    email_map = progress.get("_email_map", {})

    # Also load Brave results so we don't count them as duplicates
    brave_file = os.path.join(BASE_DIR, "brave_progress.json")
    if os.path.exists(brave_file):
        with open(brave_file) as f:
            bp = json.load(f)
        for e in bp.get("_email_map", {}):
            if e not in email_map:
                email_map[e] = bp["_email_map"][e]
        done_queries.update(bp.get("_done_queries", []))

    all_queries = generate_queries()
    new_queries = [q for q in all_queries if q not in done_queries]

    print(f"  {len(email_map)} emails already found")
    print(f"  {len(done_queries)}/{len(all_queries)} queries done")
    print(f"  {len(new_queries)} queries remaining\n")

    queries_done = 0
    new_this_run = 0
    ecosia_ok = True
    brave_ok = True
    consecutive_zero = 0

    for query in new_queries:
        if not ecosia_ok and not brave_ok:
            print("\n  Both engines rate limited. Waiting 5 min...")
            time.sleep(300)
            ecosia_ok = True
            brave_ok = True

        queries_done += 1
        short = query[:55]
        print(f"  [{queries_done}] {short}...", end=" ", flush=True)

        emails, ecosia_ok, brave_ok = search_with_rotation(query, ecosia_ok, brave_ok)

        batch_new = 0
        for e in emails:
            if e not in email_map:
                email_map[e] = {"query": query[:60]}
                batch_new += 1
                new_this_run += 1

        if batch_new > 0:
            print(f"+{batch_new} (total: {len(email_map)})")
            consecutive_zero = 0
        else:
            print("0")
            consecutive_zero += 1

        done_queries.add(query)

        if queries_done % 25 == 0:
            progress["_done_queries"] = list(done_queries)
            progress["_email_map"] = email_map
            with open(progress_file, 'w') as f:
                json.dump(progress, f, indent=1, default=str)
            print(f"  ── saved | {len(email_map)} total | +{new_this_run} this run ──")

        # Adaptive delay: longer when getting 0 results
        if consecutive_zero > 5:
            time.sleep(random.uniform(45, 75))
        else:
            time.sleep(random.uniform(25, 45))

    # Final save
    progress["_done_queries"] = list(done_queries)
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
            "external_url": "",
            "email_source": "search_harvest",
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
    print(f"  Queries: {queries_done}")
    print(f"  Total consumer emails: {len(email_map)}")
    print(f"  New this run: {new_this_run}")
    print(f"  CSV: {csv_path}")


if __name__ == "__main__":
    main()
