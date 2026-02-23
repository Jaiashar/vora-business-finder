#!/usr/bin/env python3
"""
Fitness Blog & Directory Email Scraper
========================================
Scrapes fitness blogs and directories for contact emails.
Targets: fitness bloggers, coaches, trainers who have websites with
contact info. These are prime consumers of wearable devices.

Sources:
  - Feedspot fitness blog lists (not IG, but actual blogs)
  - Bing search for fitness blogs with emails
  - Direct website contact page scraping
"""

import os
import re
import csv
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
    'yelp.com', 'bbb.org', 'apple.com', 'pinterest.com', 'youtube.com',
    'onetrust.com', 'tiktok.com', 'linkedin.com', 'bing.com', 'feedspot.com',
    'mailchimp.com', 'hubspot.com', 'mailgun.com', 'sendgrid.com',
    'googletagmanager.com', 'google-analytics.com', 'hotjar.com',
    'wp.com', 'wordpress.com', 'gravatar.com', 'gstatic.com',
    'bootstrapcdn.com', 'jquery.com', 'amazonaws.com', 'stripe.com',
    'cloudinary.com', 'cdn.com', 'cdnjs.com', 'fontawesome.com',
}
JUNK_KW = ['sentry', 'noreply', 'no-reply', 'unsubscribe', 'donotreply',
           'mailer-daemon', 'abuse@', 'postmaster@', 'webmaster@',
           'webpack', 'placeholder', 'wix.com', 'test@']


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


FEEDSPOT_BLOG_CATEGORIES = [
    ("fitness_blogs", "https://blog.feedspot.com/fitness_blogs/"),
    ("personal_trainer_blogs", "https://blog.feedspot.com/personal_trainer_blogs/"),
    ("yoga_blogs", "https://blog.feedspot.com/yoga_blogs/"),
    ("pilates_blogs", "https://blog.feedspot.com/pilates_blogs/"),
    ("crossfit_blogs", "https://blog.feedspot.com/crossfit_blogs/"),
    ("running_blogs", "https://blog.feedspot.com/running_blogs/"),
    ("bodybuilding_blogs", "https://blog.feedspot.com/bodybuilding_blogs/"),
    ("cycling_blogs", "https://blog.feedspot.com/cycling_blogs/"),
    ("nutrition_blogs", "https://blog.feedspot.com/nutrition_blogs/"),
    ("weight_loss_blogs", "https://blog.feedspot.com/weight_loss_blogs/"),
    ("wellness_blogs", "https://blog.feedspot.com/wellness_blogs/"),
    ("health_coach_blogs", "https://blog.feedspot.com/health_coach_blogs/"),
    ("vegan_blogs", "https://blog.feedspot.com/vegan_fitness_blogs/"),
    ("keto_blogs", "https://blog.feedspot.com/keto_blogs/"),
    ("triathlon_blogs", "https://blog.feedspot.com/triathlon_blogs/"),
    ("swimming_blogs", "https://blog.feedspot.com/swimming_blogs/"),
    ("marathon_blogs", "https://blog.feedspot.com/marathon_blogs/"),
    ("strength_training_blogs", "https://blog.feedspot.com/strength_training_blogs/"),
    ("hiit_blogs", "https://blog.feedspot.com/hiit_blogs/"),
    ("workout_blogs", "https://blog.feedspot.com/workout_blogs/"),
    ("healthy_living_blogs", "https://blog.feedspot.com/healthy_living_blogs/"),
    ("health_fitness_blogs", "https://blog.feedspot.com/health_and_fitness_blogs/"),
    ("gym_blogs", "https://blog.feedspot.com/gym_blogs/"),
    ("sports_nutrition_blogs", "https://blog.feedspot.com/sports_nutrition_blogs/"),
    ("womens_fitness_blogs", "https://blog.feedspot.com/womens_fitness_blogs/"),
    ("mens_fitness_blogs", "https://blog.feedspot.com/mens_fitness_blogs/"),
]

BING_QUERIES = [
    '"fitness blog" "contact me" email @gmail.com',
    '"personal trainer blog" email contact',
    '"yoga instructor" blog email @gmail.com',
    '"fitness coach" website email contact',
    '"online trainer" "work with me" email',
    '"nutrition coach" blog "email me"',
    '"strength coach" blog contact email',
    '"running coach" blog email @gmail.com',
    '"pilates instructor" website contact email',
    '"wellness coach" "let\'s connect" email',
    '"health coach" website "get in touch" email',
    '"crossfit coach" blog contact email @gmail.com',
    '"fitness blogger" "email" "collab" OR "collaborate"',
    '"personal training" website "book" email',
    '"macro coach" website email',
    '"weight loss coach" blog email contact',
    '"certified trainer" website email @',
    'fitness professional blog contact "reach out"',
    'gym owner blog email contact us',
    '"studio owner" yoga OR pilates email contact',
]


def scrape_feedspot_blog_page(url):
    """Extract blog URLs from a Feedspot blog directory page."""
    html = fetch_page(url)
    if not html:
        return []

    urls = []
    for match in re.finditer(r'href="(https?://(?!(?:blog\.)?feedspot\.com|facebook|instagram|twitter|youtube)[^"]+)"[^>]*>', html):
        u = match.group(1)
        parsed = urlparse(u)
        skip = ['feedspot.com', 'facebook.com', 'instagram.com', 'twitter.com',
                'youtube.com', 'pinterest.com', 'linkedin.com', 'amazon.com',
                'google.com', 'apple.com', 'tiktok.com']
        if not any(d in parsed.netloc for d in skip) and parsed.path in ('', '/'):
            urls.append(u)

    return list(dict.fromkeys(urls))


def search_bing_blogs(query):
    """Search Bing for fitness blog websites."""
    url = f'https://www.bing.com/search?q={quote_plus(query)}&count=30'
    html = fetch_page(url)
    if not html:
        return []

    urls = []
    for match in re.finditer(r'<a\s+href="(https?://[^"]+)"[^>]*>', html):
        u = match.group(1)
        parsed = urlparse(u)
        skip = ['bing.com', 'microsoft.com', 'google.com', 'facebook.com',
                'instagram.com', 'twitter.com', 'linkedin.com', 'youtube.com',
                'amazon.com', 'wikipedia.org', 'reddit.com', 'pinterest.com',
                'tiktok.com', 'feedspot.com']
        if not any(d in parsed.netloc for d in skip):
            urls.append(u)

    return list(dict.fromkeys(urls))[:15]


def scrape_site_for_email(url):
    """Scrape a website and its contact page for emails."""
    all_emails = set()

    html = fetch_page(url)
    if not html:
        return [], ""

    all_emails.update(extract_emails(html))

    name = ""
    title_match = re.search(r'<title[^>]*>(.*?)</title>', html, re.I | re.DOTALL)
    if title_match:
        name = re.sub(r'<[^>]+>', '', title_match.group(1)).strip()
        name = name.split('|')[0].split('—')[0].split('-')[0].strip()[:60]

    parsed = urlparse(url if url.startswith('http') else 'https://' + url)
    base = f"{parsed.scheme}://{parsed.netloc}"
    for path in ['/contact', '/about', '/contact-us', '/about-us',
                 '/work-with-me', '/hire-me', '/coaching', '/services']:
        if len(all_emails) >= 3:
            break
        sub = fetch_page(base + path, 5)
        if sub:
            all_emails.update(extract_emails(sub))
        time.sleep(0.2)

    return list(all_emails), name


def main():
    print("+" + "=" * 60 + "+")
    print("|  FITNESS BLOG & DIRECTORY EMAIL SCRAPER                  |")
    print(f"|  {datetime.now().strftime('%Y-%m-%d %H:%M:%S'):<57}|")
    print("+" + "=" * 60 + "+\n")

    progress_file = os.path.join(BASE_DIR, "fitness_blog_progress.json")
    csv_path = os.path.join(BASE_DIR, "fitness_blog_leads.csv")

    progress = {}
    if os.path.exists(progress_file):
        with open(progress_file) as f:
            progress = json.load(f)

    processed_cats = set(progress.get("_processed_categories", []))
    processed_queries = set(progress.get("_processed_queries", []))
    all_sites = {k: v for k, v in progress.items() if not k.startswith("_")}
    site_urls = set(all_sites.keys())

    print(f"  {len(all_sites)} sites already processed\n")

    # Phase 1: Discover blogs from Feedspot
    print("  ═══ Phase 1: Feedspot blog directories ═══\n")
    for cat_name, cat_url in FEEDSPOT_BLOG_CATEGORIES:
        if cat_name in processed_cats:
            continue

        print(f"  [{cat_name}] scraping...", end=" ", flush=True)
        urls = scrape_feedspot_blog_page(cat_url)
        new = [u for u in urls if u not in site_urls]
        site_urls.update(new)
        for u in new:
            all_sites[u] = None
        print(f"+{len(new)} sites (total: {len(site_urls)})")

        processed_cats.add(cat_name)
        progress["_processed_categories"] = list(processed_cats)
        with open(progress_file, 'w') as f:
            json.dump(progress, f, indent=1, default=str)

        time.sleep(random.uniform(2, 4))

    # Phase 2: Bing search for more blogs
    print("\n  ═══ Phase 2: Bing search ═══\n")
    for query in BING_QUERIES:
        if query in processed_queries:
            continue

        print(f"  Searching: {query[:50]}...", end=" ", flush=True)
        urls = search_bing_blogs(query)
        new = [u for u in urls if u not in site_urls]
        site_urls.update(new)
        for u in new:
            all_sites[u] = None
        print(f"+{len(new)} sites (total: {len(site_urls)})")

        processed_queries.add(query)
        progress["_processed_queries"] = list(processed_queries)
        with open(progress_file, 'w') as f:
            json.dump(progress, f, indent=1, default=str)

        time.sleep(random.uniform(2, 4))

    # Phase 3: Scrape each site for emails
    to_scrape = [u for u, v in all_sites.items() if v is None]
    print(f"\n  ═══ Phase 3: Scraping {len(to_scrape)} sites for emails ═══\n")

    emails_found = 0
    for i, url in enumerate(to_scrape):
        print(f"  [{i+1}/{len(to_scrape)}] {url[:55]}...", end=" ", flush=True)

        emails, name = scrape_site_for_email(url)
        if emails:
            emails_found += 1
            all_sites[url] = {"name": name, "emails": emails, "url": url}
            progress[url] = all_sites[url]
            print(f"EMAIL {name[:25]} | {emails[:2]}")
        else:
            all_sites[url] = {"url": url, "error": "no_email"}
            progress[url] = all_sites[url]
            print("no email")

        if (i + 1) % 25 == 0:
            with open(progress_file, 'w') as f:
                json.dump(progress, f, indent=1, default=str)
            print(f"  ── saved | {emails_found} emails from {i+1} sites ──")

        time.sleep(random.uniform(1, 3))

    with open(progress_file, 'w') as f:
        json.dump(progress, f, indent=1, default=str)

    # Build CSV
    rows = []
    for url, data in all_sites.items():
        if not data or data.get("error"):
            continue
        for email in data.get("emails", []):
            rows.append({
                "email": email,
                "name": data.get("name", ""),
                "ig_username": "",
                "followers": 0,
                "platform": "website",
                "category": "fitness_influencer",
                "tags": "fitness_blogger",
                "bio": "",
                "external_url": data.get("url", ""),
                "email_source": "blog_website",
                "is_business": True,
                "business_category": "Fitness Blog",
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
    print(f"  Sites scraped: {len(to_scrape)}")
    print(f"  With email: {emails_found}")
    print(f"  Unique emails: {unique}")
    print(f"  CSV: {csv_path}")


if __name__ == "__main__":
    main()
