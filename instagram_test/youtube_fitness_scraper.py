#!/usr/bin/env python3
"""
YouTube Fitness Channel Email Scraper
=======================================
Searches YouTube for fitness/wellness channels and extracts emails
from their About pages. YouTube channels almost always have business emails.

Outputs CSV for pushing to Supabase consumer_leads table.
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
from urllib.parse import quote_plus

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

SSL_CTX = ssl.create_default_context()
SSL_CTX.check_hostname = False
SSL_CTX.verify_mode = ssl.CERT_NONE

UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
      "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

EMAIL_RE = re.compile(r'[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}')
JUNK_DOMAINS = {
    'sentry.io', 'w3.org', 'schema.org', 'example.com', 'domain.com',
    'youtube.com', 'google.com', 'googleapis.com', 'gstatic.com',
    'cloudflare.com', 'facebook.com', 'instagram.com', 'twitter.com',
    'onetrust.com', 'spotify.com', 'apple.com', 'ytimg.com',
    'googlevideo.com', 'googleusercontent.com', 'ggpht.com',
}
JUNK_KW = ['sentry', 'noreply', 'no-reply', 'unsubscribe', 'donotreply',
           'mailer-daemon', 'abuse@', 'postmaster@']


def extract_emails(text):
    if not text:
        return []
    found = EMAIL_RE.findall(text.lower())
    return list(set(
        e for e in found
        if e.split('@')[1] not in JUNK_DOMAINS
        and not any(k in e for k in JUNK_KW)
        and not e.endswith(('.png', '.jpg', '.gif', '.svg', '.css', '.js'))
        and len(e.split('@')[1]) >= 5
    ))


def fetch_page(url, timeout=12):
    try:
        req = urllib.request.Request(url, headers={
            "User-Agent": UA,
            "Accept": "text/html,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
        })
        resp = urllib.request.urlopen(req, timeout=timeout, context=SSL_CTX)
        return resp.read().decode('utf-8', errors='ignore')
    except:
        return None


# YouTube search queries for fitness channels
YOUTUBE_SEARCH_QUERIES = [
    "fitness workout channel",
    "personal trainer youtube channel",
    "home workout channel",
    "yoga channel beginner",
    "pilates workout channel",
    "crossfit training channel",
    "running training tips channel",
    "nutrition fitness meal prep",
    "weight loss fitness journey",
    "bodybuilding training channel",
    "strength training women",
    "hiit workout channel",
    "calisthenics training channel",
    "fitness coach online training",
    "gym workout routine channel",
    "healthy lifestyle wellness",
    "macro counting nutrition",
    "plant based fitness vegan",
    "cycling training tips",
    "swimming training drills",
    "martial arts fitness",
    "dance fitness workout",
    "prenatal postnatal fitness",
    "senior fitness exercise",
    "mobility stretching routine",
    "physical therapy exercises",
    "sports performance training",
    "athlete training program",
    "functional fitness movements",
    "kettlebell workout training",
]


def search_youtube_channels(query, max_results=20):
    """Search YouTube for channels and get their about pages."""
    search_url = f"https://www.youtube.com/results?search_query={quote_plus(query)}&sp=EgIQAg%253D%253D"
    html = fetch_page(search_url)
    if not html:
        return []

    # Extract channel URLs from search results
    channel_urls = re.findall(r'"canonicalBaseUrl":"(/(@[^"]+))"', html)
    channels = []
    seen = set()
    for _, handle in channel_urls:
        if handle not in seen:
            seen.add(handle)
            channels.append(handle)

    return channels[:max_results]


def get_channel_email(channel_handle):
    """Fetch a YouTube channel's about page and extract email."""
    about_url = f"https://www.youtube.com/{channel_handle}/about"
    html = fetch_page(about_url)
    if not html:
        return None

    # Extract channel name
    name = ""
    name_match = re.search(r'"channelName":"([^"]+)"', html)
    if name_match:
        name = name_match.group(1)

    # Extract subscriber count
    subs = 0
    sub_match = re.search(r'"subscriberCountText":"([\d.]+[KMB]?)\s*subscribers"', html)
    if sub_match:
        s = sub_match.group(1).upper()
        try:
            if 'K' in s:
                subs = int(float(s.replace('K', '')) * 1000)
            elif 'M' in s:
                subs = int(float(s.replace('M', '')) * 1000000)
            elif 'B' in s:
                subs = int(float(s.replace('B', '')) * 1000000000)
            else:
                subs = int(s)
        except:
            pass

    # Look for email in the page data
    # YouTube often puts business email in channelHeaderLinksViewModel or description
    all_emails = extract_emails(html)

    # Also check for "For business inquiries" pattern
    biz_match = re.search(r'business.*?inquir.*?([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})', html, re.I)
    if biz_match:
        all_emails.append(biz_match.group(1).lower())

    # Extract description
    desc = ""
    desc_match = re.search(r'"description":"(.*?)(?<!\\)"', html)
    if desc_match:
        desc = desc_match.group(1)[:300]
        desc_emails = extract_emails(desc)
        all_emails.extend(desc_emails)

    all_emails = list(set(all_emails))

    if not all_emails:
        return None

    return {
        "channel_handle": channel_handle,
        "channel_name": name,
        "subscribers": subs,
        "emails": all_emails,
        "description": desc[:200],
    }


def main():
    print("+" + "=" * 60 + "+")
    print("|  YOUTUBE FITNESS CHANNEL EMAIL SCRAPER                   |")
    print(f"|  {datetime.now().strftime('%Y-%m-%d %H:%M:%S'):<57}|")
    print("+" + "=" * 60 + "+\n")

    progress_file = os.path.join(BASE_DIR, "youtube_progress.json")
    csv_path = os.path.join(BASE_DIR, "youtube_fitness_leads.csv")

    progress = {}
    if os.path.exists(progress_file):
        with open(progress_file) as f:
            progress = json.load(f)

    all_channels = set(progress.keys())
    emails_found = 0
    processed_queries = progress.get("_processed_queries", [])

    print(f"  {len(all_channels)} channels already processed\n")
    print("  ═══ Phase 1: Discover channels ═══\n")

    for query in YOUTUBE_SEARCH_QUERIES:
        if query in processed_queries:
            continue

        print(f"  Searching: '{query}'...", end=" ", flush=True)
        channels = search_youtube_channels(query)
        new = [c for c in channels if c not in all_channels]
        all_channels.update(channels)
        print(f"+{len(new)} channels (total: {len(all_channels)})")

        processed_queries.append(query)
        progress["_processed_queries"] = processed_queries
        with open(progress_file, 'w') as f:
            json.dump(progress, f, indent=1)

        time.sleep(random.uniform(2, 4))

    to_process = [c for c in all_channels if c != "_processed_queries" and c not in progress]
    print(f"\n  ═══ Phase 2: Extract emails from {len(to_process)} channels ═══\n")

    for i, channel in enumerate(to_process):
        print(f"  [{i+1}/{len(to_process)}] {channel}...", end=" ", flush=True)

        result = get_channel_email(channel)
        if result:
            emails_found += 1
            progress[channel] = result
            print(f"EMAIL {result['channel_name'][:25]} | {result['emails'][:2]}")
        else:
            progress[channel] = {"channel_handle": channel, "error": "no_email"}
            print("no email")

        if (i + 1) % 20 == 0:
            with open(progress_file, 'w') as f:
                json.dump(progress, f, indent=1, default=str)
            print(f"  ── saved | {emails_found} emails from {i+1} channels ──")

        time.sleep(random.uniform(2, 4))

    with open(progress_file, 'w') as f:
        json.dump(progress, f, indent=1, default=str)

    # Build CSV
    rows = []
    for channel, data in progress.items():
        if channel == "_processed_queries" or data.get("error"):
            continue
        for email in data.get("emails", []):
            rows.append({
                "email": email,
                "name": data.get("channel_name", ""),
                "ig_username": "",
                "followers": data.get("subscribers", 0),
                "platform": "youtube",
                "category": "fitness_influencer",
                "tags": "",
                "bio": data.get("description", "")[:200],
                "external_url": f"https://youtube.com/{channel}",
                "email_source": "youtube_about",
                "is_business": True,
                "business_category": "Fitness",
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
    print(f"  Channels processed: {len(to_process)}")
    print(f"  With email: {emails_found}")
    print(f"  Unique emails: {unique}")
    print(f"  CSV: {csv_path}")


if __name__ == "__main__":
    main()
