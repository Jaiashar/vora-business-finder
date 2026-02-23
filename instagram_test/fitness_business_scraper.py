#!/usr/bin/env python3
"""
Fitness Business Email Scraper
================================
Scrapes fitness business websites found via Yelp, Google, and fitness directories
to collect business owner/manager emails. These are fitness professionals
who are consumers of wearable devices and fitness apps.

Sources:
  - Yelp fitness/gym listings
  - Fitness studio directories (ClassPass, Mindbody, etc.)
  - Bing search for local gyms, trainers, studios
  - State personal trainer registries

Outputs CSV for consumer_leads table.
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

UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
      "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

HEADERS = {
    "User-Agent": UA,
    "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
}

EMAIL_RE = re.compile(r'[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}')
JUNK_DOMAINS = {
    'sentry.io', 'w3.org', 'schema.org', 'example.com', 'domain.com',
    'wixpress.com', 'squarespace.com', 'cloudflare.com', 'googleapis.com',
    'facebook.com', 'instagram.com', 'twitter.com', 'google.com',
    'yelp.com', 'bbb.org', 'mapquest.com', 'apple.com', 'pinterest.com',
    'onetrust.com', 'tiktok.com', 'linkedin.com', 'bing.com',
    'mailchimp.com', 'hubspot.com', 'mailgun.com', 'sendgrid.com',
    'sentry-next.wixpress.com', 'cdn.com', 'cdnjs.com',
    'googletagmanager.com', 'google-analytics.com', 'hotjar.com',
    'bootstrapcdn.com', 'jquery.com', 'wordpress.com', 'wp.com',
    'gravatar.com', 'zendesk.com', 'intercom.io', 'crisp.chat',
    'acuityscheduling.com', 'mindbodyonline.com', 'classpass.com',
    'gstatic.com', 'ytimg.com',
}
JUNK_KW = ['sentry', 'noreply', 'no-reply', 'unsubscribe', 'donotreply',
           'mailer-daemon', 'abuse@', 'postmaster@', 'webmaster@',
           'webpack', 'placeholder']


def extract_emails(text):
    if not text:
        return []
    found = EMAIL_RE.findall(text.lower())
    return list(set(
        e for e in found
        if e.split('@')[1] not in JUNK_DOMAINS
        and not any(k in e for k in JUNK_KW)
        and not e.endswith(('.png', '.jpg', '.gif', '.svg', '.css', '.js', '.woff'))
        and len(e.split('@')[1]) >= 4
        and len(e) < 60
    ))


def fetch_page(url, timeout=10):
    try:
        if not url.startswith(('http://', 'https://')):
            url = 'https://' + url
        req = urllib.request.Request(url, headers=HEADERS)
        resp = urllib.request.urlopen(req, timeout=timeout, context=SSL_CTX)
        return resp.read().decode('utf-8', errors='ignore')
    except:
        return None


# US cities for location-based fitness business search
TOP_US_CITIES = [
    "Los Angeles, CA", "New York, NY", "Chicago, IL", "Houston, TX",
    "Phoenix, AZ", "Philadelphia, PA", "San Antonio, TX", "San Diego, CA",
    "Dallas, TX", "Austin, TX", "San Jose, CA", "Jacksonville, FL",
    "San Francisco, CA", "Columbus, OH", "Indianapolis, IN",
    "Charlotte, NC", "Seattle, WA", "Denver, CO", "Nashville, TN",
    "Portland, OR", "Oklahoma City, OK", "Las Vegas, NV", "Memphis, TN",
    "Louisville, KY", "Baltimore, MD", "Milwaukee, WI", "Albuquerque, NM",
    "Tucson, AZ", "Fresno, CA", "Sacramento, CA", "Mesa, AZ",
    "Atlanta, GA", "Omaha, NE", "Raleigh, NC", "Miami, FL",
    "Minneapolis, MN", "Tampa, FL", "New Orleans, LA", "Cleveland, OH",
    "Pittsburgh, PA", "Cincinnati, OH", "St. Louis, MO", "Kansas City, MO",
    "Orlando, FL", "Boise, ID", "Salt Lake City, UT", "Honolulu, HI",
    "Richmond, VA", "Spokane, WA", "Des Moines, IA",
    # Small cities too — less competition, more likely to have small biz emails
    "Boulder, CO", "Scottsdale, AZ", "Pasadena, CA", "Santa Monica, CA",
    "Berkeley, CA", "Ann Arbor, MI", "Asheville, NC", "Charleston, SC",
    "Savannah, GA", "Madison, WI", "Burlington, VT", "Eugene, OR",
]

SEARCH_TEMPLATES = [
    "{city} personal trainer email",
    "{city} fitness studio email",
    "{city} yoga studio contact",
    "{city} crossfit gym email",
    "{city} pilates studio contact",
    "{city} online fitness coach email",
    "{city} gym small business email",
    "{city} strength training coach",
    "{city} nutrition coach email contact",
    "{city} wellness coach email",
]


def search_bing_fitness_businesses(query):
    """Search Bing for fitness business websites."""
    url = f'https://www.bing.com/search?q={quote_plus(query)}&count=30'
    html = fetch_page(url)
    if not html:
        return []

    # Extract URLs from Bing results
    urls = []
    for match in re.finditer(r'<a\s+href="(https?://[^"]+)"[^>]*>', html):
        u = match.group(1)
        parsed = urlparse(u)
        skip_domains = ['bing.com', 'microsoft.com', 'google.com', 'yelp.com',
                        'facebook.com', 'instagram.com', 'twitter.com',
                        'linkedin.com', 'youtube.com', 'amazon.com',
                        'wikipedia.org', 'reddit.com', 'pinterest.com',
                        'tiktok.com', 'bbb.org', 'mapquest.com',
                        'yellowpages.com', 'thumbtack.com', 'angieslist.com',
                        'mindbodyonline.com', 'classpass.com', 'groupon.com']
        if not any(d in parsed.netloc for d in skip_domains):
            urls.append(u)

    return list(dict.fromkeys(urls))[:15]


def scrape_business_website(url):
    """Scrape a business website and contact page for emails."""
    all_emails = set()

    html = fetch_page(url)
    if not html:
        return [], ""

    # Extract emails from main page
    all_emails.update(extract_emails(html))

    # Extract business name
    name = ""
    title_match = re.search(r'<title[^>]*>(.*?)</title>', html, re.I | re.DOTALL)
    if title_match:
        name = re.sub(r'<[^>]+>', '', title_match.group(1)).strip()
        name = name.split('|')[0].split('-')[0].strip()[:60]

    # Try contact page
    parsed = urlparse(url)
    base = f"{parsed.scheme}://{parsed.netloc}"
    for path in ['/contact', '/about', '/contact-us', '/about-us']:
        if len(all_emails) >= 3:
            break
        sub = fetch_page(base + path, 5)
        if sub:
            all_emails.update(extract_emails(sub))
        time.sleep(0.2)

    return list(all_emails), name


def yelp_search_fitness(city, category="gyms"):
    """Search Yelp for fitness businesses and extract their website URLs."""
    city_slug = city.lower().replace(', ', '-').replace(' ', '-')
    categories = ["gyms", "personaltrainers", "yoga", "pilates", "bootcamps",
                  "crossfit", "martialarts", "cyclingclasses", "healthcoach"]
    urls = []

    for cat in categories[:3]:
        yelp_url = f"https://www.yelp.com/search?find_desc={cat}&find_loc={quote_plus(city)}"
        html = fetch_page(yelp_url)
        if not html:
            continue

        # Extract business website links from Yelp results
        for match in re.finditer(r'"bizUrl":"(https?://[^"]+)"', html):
            urls.append(match.group(1))

        # Also look for direct website links in Yelp business data
        for match in re.finditer(r'"website":"(https?://[^"]+)"', html):
            urls.append(match.group(1))

        time.sleep(random.uniform(1, 2))

    return list(dict.fromkeys(urls))[:20]


def main():
    print("+" + "=" * 60 + "+")
    print("|  FITNESS BUSINESS EMAIL SCRAPER                          |")
    print(f"|  {datetime.now().strftime('%Y-%m-%d %H:%M:%S'):<57}|")
    print("+" + "=" * 60 + "+\n")

    progress_file = os.path.join(BASE_DIR, "fitness_biz_progress.json")
    csv_path = os.path.join(BASE_DIR, "fitness_business_leads.csv")

    progress = {}
    if os.path.exists(progress_file):
        with open(progress_file) as f:
            progress = json.load(f)

    processed_cities = set(progress.get("_processed_cities", []))
    all_sites = {}
    for k, v in progress.items():
        if not k.startswith("_"):
            all_sites[k] = v

    emails_found = 0
    total_processed = 0

    print(f"  {len(processed_cities)} cities already processed")
    print(f"  {len(all_sites)} sites already scraped\n")

    for city_i, city in enumerate(TOP_US_CITIES):
        if city in processed_cities:
            continue

        print(f"\n  ═══ [{city_i+1}/{len(TOP_US_CITIES)}] {city} ═══\n")

        # Search Bing for fitness businesses in this city
        sites_for_city = set()
        templates_to_use = random.sample(SEARCH_TEMPLATES, min(4, len(SEARCH_TEMPLATES)))

        for tmpl in templates_to_use:
            query = tmpl.format(city=city)
            print(f"    Searching: {query[:50]}...", end=" ", flush=True)
            urls = search_bing_fitness_businesses(query)
            new = [u for u in urls if u not in all_sites]
            sites_for_city.update(new)
            print(f"+{len(new)} sites")
            time.sleep(random.uniform(2, 4))

        print(f"    Found {len(sites_for_city)} new sites to scrape\n")

        # Scrape each site
        city_emails = 0
        for j, site_url in enumerate(sites_for_city):
            if site_url in all_sites:
                continue

            print(f"    [{j+1}/{len(sites_for_city)}] {site_url[:60]}...", end=" ", flush=True)
            emails, name = scrape_business_website(site_url)
            total_processed += 1

            if emails:
                emails_found += len(set(emails))
                city_emails += 1
                all_sites[site_url] = {
                    "name": name,
                    "emails": emails,
                    "city": city,
                    "url": site_url,
                }
                print(f"EMAIL {name[:25]} | {emails[:2]}")
            else:
                all_sites[site_url] = {"url": site_url, "city": city, "error": "no_email"}
                print("no email")

            time.sleep(random.uniform(1, 2))

        processed_cities.add(city)
        progress["_processed_cities"] = list(processed_cities)
        progress.update(all_sites)
        with open(progress_file, 'w') as f:
            json.dump(progress, f, indent=1, default=str)

        print(f"    ── {city}: {city_emails} sites with email ──")

    # Build CSV
    rows = []
    for url, data in all_sites.items():
        if data.get("error"):
            continue
        for email in data.get("emails", []):
            rows.append({
                "email": email,
                "name": data.get("name", ""),
                "ig_username": "",
                "followers": 0,
                "platform": "website",
                "category": "fitness_influencer",
                "tags": f"fitness_business,{data.get('city', '')}",
                "bio": "",
                "external_url": data.get("url", ""),
                "email_source": "business_website",
                "is_business": True,
                "business_category": "Fitness Business",
            })

    fieldnames = ["email", "name", "ig_username", "followers", "platform",
                  "category", "tags", "bio", "external_url", "email_source",
                  "is_business", "business_category"]

    with open(csv_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    unique = len(set(r["email"] for r in rows))
    print(f"\n  ═══ RESULTS ═══")
    print(f"  Cities: {len(processed_cities)}")
    print(f"  Sites scraped: {total_processed}")
    print(f"  Unique emails: {unique}")
    print(f"  CSV: {csv_path}")


if __name__ == "__main__":
    main()
