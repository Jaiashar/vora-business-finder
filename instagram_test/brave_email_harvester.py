#!/usr/bin/env python3
"""
Brave Search Email Harvester for Fitness Consumers
=====================================================
Brave Search surfaces @gmail.com emails directly in search snippets.
One query = 5-10 consumer emails. With 1000+ queries, we can get thousands.

No need to visit individual websites — emails appear in the search results.
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
    'brave.com', 'search.brave.com', 'google.com', 'bing.com',
    'facebook.com', 'instagram.com', 'youtube.com', 'twitter.com',
    'linkedin.com', 'pinterest.com', 'tiktok.com',
    'wix.com', 'squarespace.com', 'wordpress.com', 'shopify.com',
    'mailchimp.com', 'hubspot.com', 'sendgrid.com', 'mailgun.com',
    'amazonaws.com', 'cloudflare.com',
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

    if domain in PERSONAL_PROVIDERS:
        return True

    if BIZ_EMAIL_DOMAIN_RE.search(domain):
        return False

    if local in BIZ_PREFIXES:
        return False

    return True


def brave_search(query):
    """Search Brave and extract consumer emails from results."""
    url = f'https://search.brave.com/search?q={quote_plus(query)}'
    headers = {
        "User-Agent": random.choice(UA_LIST),
        "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
    }
    req = urllib.request.Request(url, headers=headers)
    resp = urllib.request.urlopen(req, timeout=15, context=SSL_CTX)
    raw = resp.read().decode('utf-8', errors='ignore')

    all_emails = EMAIL_RE.findall(raw.lower())
    consumer = [e for e in set(all_emails) if is_consumer_email(e)]
    return consumer


def generate_queries():
    """Generate hundreds of queries for fitness consumer emails."""
    queries = []

    # City + role + @gmail.com queries (highest yield)
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
        "mobility coach", "flexibility coach",
        "prenatal fitness", "senior fitness trainer",
        "weight loss coach", "body transformation coach",
        "macro coach", "meal prep coach",
        "holistic health coach", "integrative nutrition",
        "plant based nutrition", "sports dietitian",
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
        "Colorado Springs", "Fort Collins", "Provo",
        "Brooklyn", "Manhattan", "Beverly Hills", "Pasadena",
        "Oakland", "Berkeley", "Palo Alto", "Hoboken",
        "Bethesda", "Arlington VA", "Bend OR", "Evanston",
        "Plano", "Frisco", "McKinney", "Tempe", "Mesa",
        "Durham", "Chapel Hill", "Greenville SC",
        "Knoxville", "Lexington", "Birmingham",
        "Tulsa", "Overland Park", "Omaha",
    ]

    # Each role x random cities
    for role in roles:
        sampled_cities = random.sample(cities, min(8, len(cities)))
        for city in sampled_cities:
            queries.append(f'{role} {city} email @gmail.com')

    # Certification-based
    certs = [
        "NASM certified", "ACE certified", "ISSA certified",
        "NSCA certified", "ACSM certified", "RYT 200", "RYT 500",
        "CrossFit Level 1", "CrossFit Level 2",
        "USAT triathlon coach", "RRCA running coach",
        "Precision Nutrition", "NESTA certified",
        "PN Level 1", "PN Level 2",
    ]
    for cert in certs:
        queries.append(f'{cert} trainer email @gmail.com')
        queries.append(f'{cert} coach email contact @gmail.com')

    # Activity-based
    activities = [
        "marathon training", "5K training", "10K training",
        "triathlon training", "obstacle course racing",
        "powerlifting meet", "bodybuilding competition",
        "crossfit competition", "yoga retreat",
        "fitness retreat", "boot camp training",
        "outdoor fitness group", "running group",
        "cycling team", "swim team masters",
        "kickboxing class", "Muay Thai training",
        "BJJ training", "rock climbing coaching",
        "hiking guide fitness", "ski instructor fitness",
    ]
    for act in activities:
        queries.append(f'{act} coach email @gmail.com')
        queries.append(f'{act} instructor contact @gmail.com')

    # Wearable-related
    wearables = [
        "apple watch fitness", "garmin runner",
        "oura ring wellness", "whoop fitness",
        "fitbit coach", "strava athlete",
        "polar watch running", "coros watch",
        "suunto athlete", "wahoo cycling",
    ]
    for w in wearables:
        queries.append(f'{w} email @gmail.com')
        queries.append(f'{w} contact email')

    # Other email providers
    for role in random.sample(roles, 10):
        queries.append(f'{role} email @yahoo.com')
        queries.append(f'{role} email @hotmail.com')
        queries.append(f'{role} email @outlook.com')

    random.shuffle(queries)
    return queries


def main():
    print("+" + "=" * 60 + "+")
    print("|  BRAVE SEARCH CONSUMER EMAIL HARVESTER                   |")
    print(f"|  {datetime.now().strftime('%Y-%m-%d %H:%M:%S'):<57}|")
    print("+" + "=" * 60 + "+\n")

    progress_file = os.path.join(BASE_DIR, "brave_progress.json")
    csv_path = os.path.join(BASE_DIR, "brave_consumer_leads.csv")

    progress = {}
    if os.path.exists(progress_file):
        with open(progress_file) as f:
            progress = json.load(f)

    done_queries = set(progress.get("_done_queries", []))
    email_map = progress.get("_email_map", {})

    all_queries = generate_queries()
    new_queries = [q for q in all_queries if q not in done_queries]

    print(f"  {len(email_map)} emails already found")
    print(f"  {len(done_queries)}/{len(all_queries)} queries done")
    print(f"  {len(new_queries)} queries remaining\n")

    queries_done = 0
    new_this_run = 0

    for query in new_queries:
        queries_done += 1
        short = query[:55]
        print(f"  [{queries_done}] {short}...", end=" ", flush=True)

        try:
            emails = brave_search(query)
        except urllib.error.HTTPError as e:
            if e.code == 429:
                for wait_time in [120, 300, 600]:
                    print(f"\n  RATE LIMITED. Waiting {wait_time}s...")
                    time.sleep(wait_time)
                    try:
                        emails = brave_search(query)
                        print(f"  Recovered after {wait_time}s wait")
                        break
                    except:
                        continue
                else:
                    print("  Still blocked after long waits. Saving and exiting.")
                    break
            else:
                print(f"HTTP {e.code}")
                done_queries.add(query)
                time.sleep(10)
                continue
        except Exception as e:
            print(f"error ({str(e)[:30]})")
            done_queries.add(query)
            time.sleep(10)
            continue

        batch_new = 0
        for e in emails:
            if e not in email_map:
                email_map[e] = {"query": query[:60]}
                batch_new += 1
                new_this_run += 1

        if batch_new > 0:
            print(f"+{batch_new} (total: {len(email_map)})")
        else:
            print("0")

        done_queries.add(query)

        if queries_done % 25 == 0:
            progress["_done_queries"] = list(done_queries)
            progress["_email_map"] = email_map
            with open(progress_file, 'w') as f:
                json.dump(progress, f, indent=1, default=str)
            print(f"  ── saved | {len(email_map)} total | +{new_this_run} this run ──")

        time.sleep(random.uniform(30, 50))

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
            "email_source": "brave_search",
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
