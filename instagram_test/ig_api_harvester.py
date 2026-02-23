#!/usr/bin/env python3
"""
IG API Consumer Email Harvester
==================================
Uses the Instagram Public API (when available) with careful rate limiting
to extract consumer emails. The API gives us:
  - Full untruncated bios (with emails)
  - business_email field
  - external_url (linktrees etc we can scrape)
  - Follower count (to filter out celebrities)
  - Business account flag

Rate limit strategy:
  - 15-20s between requests
  - 120s cooldown every 40 requests
  - Auto-stop on 401/429, save progress for resume

Usage:
  python ig_api_harvester.py          # Run extraction
  python ig_api_harvester.py stats    # Show stats
  python ig_api_harvester.py csv      # Rebuild CSV
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
from urllib.parse import urlparse

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

SSL_CTX = ssl.create_default_context()
SSL_CTX.check_hostname = False
SSL_CTX.verify_mode = ssl.CERT_NONE

UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
      "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
IG_HEADERS = {
    "User-Agent": UA,
    "X-IG-App-ID": "936619743392459",
    "Accept": "*/*",
    "Accept-Language": "en-US,en;q=0.9",
}
WEB_HEADERS = {"User-Agent": UA, "Accept": "text/html,*/*;q=0.8", "Accept-Language": "en-US,en;q=0.5"}

EMAIL_RE = re.compile(r'[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}')

JUNK_DOMAINS = {
    'sentry.io', 'w3.org', 'schema.org', 'example.com', 'domain.com',
    'wixpress.com', 'cloudflare.com', 'mailchimp.com', 'googleapis.com',
    'facebook.com', 'instagram.com', 'fbcdn.net', 'youtube.com',
    'twitter.com', 'x.com', 'tiktok.com', 'spotify.com', 'pinterest.com',
    'linkedin.com', 'apple.com', 'google.com', 'gstatic.com',
    'onetrust.com', 'feedspot.com', 'imginn.org',
    'patreon.com', 'substackinc.com', 'beacons.ai',
    'mailgun.com', 'sendgrid.com', 'hubspot.com',
    'shopify.com', 'squarespace.com', 'wix.com', 'wordpress.com',
    'gravatar.com', 'amazonaws.com', 'stripe.com',
}

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

BIZ_KEYWORDS = re.compile(
    r'\b(?:gym|studio|fitness\s*center|crossfit\s*box|'
    r'supplement|apparel|brand|clothing|wear|shop|store|'
    r'inc\.|llc|ltd|corp|co\.|®|™|'
    r'franchise|chain|outlet|warehouse|'
    r'official\s*account|we\s*are|our\s*team|our\s*mission|'
    r'book\s*(?:a|your)\s*class|class\s*schedule|'
    r'visit\s*(?:us|our)|come\s*(?:train|visit)|'
    r'locations?\s*(?:in|near)|open\s*7\s*days|'
    r'managed\s*by|represented\s*by|signed\s*(?:with|to|by)|'
    r'talent\s*agency|sports\s*(?:management|agency)|'
    r'pro\s*athlete|professional\s*athlete|olympian|'
    r'world\s*champion|olympic\s*(?:gold|silver|bronze|medalist)|'
    r'ufc\s*fighter|nfl|nba|mlb|nhl|mls|wwe|pga|'
    r'national\s*team|team\s*(?:usa|gb|canada)|'
    r'order\s*(?:now|here|today)|shop\s*(?:now|here)|'
    r'use\s*code|discount|promo|coupon|'
    r'free\s*shipping|limited\s*(?:time|edition))\b',
    re.I
)

MAX_FOLLOWERS = 500_000
MIN_FOLLOWERS = 100
REQUESTS_PER_BATCH = 40
COOLDOWN_SECONDS = 120
DELAY_MIN = 15
DELAY_MAX = 22


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
        and not e.endswith(('.png', '.jpg', '.gif', '.svg', '.css', '.js', '.webp', '.woff', '.woff2', '.ttf', '.eot', '.ico'))
        and len(e.split('@')[1]) >= 4
        and len(e) < 60
    ))


def is_consumer_email(email):
    email = email.lower()
    local, domain = email.split('@', 1)
    if domain in PERSONAL_PROVIDERS:
        return True
    if BIZ_EMAIL_DOMAIN_RE.search(domain):
        return False
    if local in BIZ_PREFIXES:
        return False
    return True


def is_business_account(name, bio, username):
    text = f"{name} {bio} {username}".lower()
    if BIZ_KEYWORDS.search(text):
        return True
    for suffix in ['_official', '_brand', '_hq', '_inc', '_co', '_llc', '_team', '_org']:
        if username.lower().endswith(suffix):
            return True
    return False


def fetch_page(url, timeout=8):
    try:
        if not url.startswith(('http://', 'https://')):
            url = 'https://' + url
        req = urllib.request.Request(url, headers=WEB_HEADERS)
        resp = urllib.request.urlopen(req, timeout=timeout, context=SSL_CTX)
        content = resp.read()[:300000]
        try:
            return content.decode('utf-8')
        except UnicodeDecodeError:
            return content.decode('latin-1', errors='ignore')
    except:
        return None


def scrape_url_for_emails(url):
    all_emails = set()
    html = fetch_page(url)
    if not html:
        return []
    raw = extract_emails(html)
    all_emails.update(e for e in raw if is_consumer_email(e))

    linktree_domains = ['linktr.ee', 'komi.io', 'beacons.ai', 'link.bio',
                        'campsite.bio', 'stan.store', 'hoo.be', 'snipfeed.co',
                        'flow.page', 'solo.to', 'tap.bio', 'bio.link',
                        'milkshake.app', 'carrd.co', 'lynx.bio', 'bio.site']
    if any(d in url.lower() for d in linktree_domains):
        for link in re.findall(r'href="(https?://[^"]+)"', html)[:8]:
            if any(k in link.lower() for k in ['contact', 'about', 'email', 'book',
                                                'work', 'collab', 'inquiry', 'coach', 'hire']):
                sub = fetch_page(link, 5)
                if sub:
                    raw = extract_emails(sub)
                    all_emails.update(e for e in raw if is_consumer_email(e))
                time.sleep(0.3)
    else:
        parsed = urlparse(url if url.startswith('http') else 'https://' + url)
        base = f"{parsed.scheme}://{parsed.netloc}"
        for path in ['/contact', '/about', '/work-with-me']:
            if len(all_emails) >= 2:
                break
            sub = fetch_page(base + path, 5)
            if sub:
                raw = extract_emails(sub)
                all_emails.update(e for e in raw if is_consumer_email(e))
            time.sleep(0.2)

    return list(all_emails)


def fetch_ig_profile(username):
    url = f"https://www.instagram.com/api/v1/users/web_profile_info/?username={username}"
    req = urllib.request.Request(url, headers=IG_HEADERS)
    resp = urllib.request.urlopen(req, timeout=10, context=SSL_CTX)
    data = json.loads(resp.read().decode('utf-8'))
    user = data.get("data", {}).get("user", {})
    if not user:
        return None
    return {
        "full_name": user.get("full_name", ""),
        "bio": user.get("biography", ""),
        "external_url": user.get("external_url", ""),
        "followers": user.get("edge_followed_by", {}).get("count", 0),
        "is_business": user.get("is_business_account", False),
        "business_category": user.get("category_name", ""),
        "business_email": user.get("business_email", ""),
    }


def main():
    mode = sys.argv[1] if len(sys.argv) > 1 else "run"

    print("+" + "=" * 60 + "+")
    print("|  IG API CONSUMER HARVESTER                               |")
    print(f"|  {datetime.now().strftime('%Y-%m-%d %H:%M:%S'):<57}|")
    print("+" + "=" * 60 + "+\n")

    progress_file = os.path.join(BASE_DIR, "ig_api_progress.json")
    csv_path = os.path.join(BASE_DIR, "ig_api_consumer_leads.csv")

    # Load usernames
    usernames_file = os.path.join(BASE_DIR, "consumer_usernames.json")
    if not os.path.exists(usernames_file):
        print("  No usernames file found.")
        return

    with open(usernames_file) as f:
        data = json.load(f)
    all_usernames = data.get("usernames", [])

    # Load progress (merge from all sources so we don't re-process)
    progress = {}
    for pf in ["mass_progress.json", "consumer_progress.json", "ig_api_progress.json"]:
        fp = os.path.join(BASE_DIR, pf)
        if os.path.exists(fp):
            with open(fp) as f:
                p = json.load(f)
            old = len(progress)
            progress.update(p)
            print(f"  Loaded {len(p)} from {pf}")

    if mode == "stats":
        show_stats(progress, all_usernames)
        return
    if mode == "csv":
        build_csv(progress, csv_path)
        return

    to_process = [u for u in all_usernames if u not in progress]
    total = len(to_process)
    processed = 0
    emails_found = 0
    api_requests = 0
    businesses_skipped = 0

    print(f"\n  {len(progress)} already processed, {total} remaining")
    print(f"  Rate: 1 req / {DELAY_MIN}-{DELAY_MAX}s, cooldown {COOLDOWN_SECONDS}s every {REQUESTS_PER_BATCH}\n")

    for username in to_process:
        processed += 1

        # Cooldown
        if api_requests > 0 and api_requests % REQUESTS_PER_BATCH == 0:
            print(f"\n  --- Cooldown {COOLDOWN_SECONDS}s after {api_requests} API requests ---\n")
            time.sleep(COOLDOWN_SECONDS)

        print(f"  [{processed}/{total}] @{username}...", end=" ", flush=True)

        try:
            profile = fetch_ig_profile(username)
            api_requests += 1
        except urllib.error.HTTPError as e:
            if e.code in (401, 429):
                print(f"\n  RATE LIMITED after {api_requests} requests. Re-run later to continue.")
                break
            progress[username] = {"error": str(e.code)}
            print(f"http {e.code}")
            time.sleep(2)
            continue
        except Exception as e:
            progress[username] = {"error": str(e)[:50]}
            print("error")
            time.sleep(2)
            continue

        if not profile:
            progress[username] = {"error": "not_found"}
            print("not found")
            time.sleep(random.uniform(DELAY_MIN, DELAY_MAX))
            continue

        name = profile["full_name"]
        bio = profile["bio"]
        followers = profile["followers"]

        # Filter: skip businesses
        if is_business_account(name, bio, username):
            progress[username] = {"error": "business", "full_name": name}
            businesses_skipped += 1
            print(f"SKIP biz ({name[:25]})")
            time.sleep(random.uniform(DELAY_MIN, DELAY_MAX))
            continue

        # Filter: skip huge accounts (celebrities) and tiny ones (bots)
        if followers > MAX_FOLLOWERS:
            progress[username] = {"error": "celebrity", "followers": followers, "full_name": name}
            print(f"SKIP celebrity ({followers:,} flw)")
            time.sleep(random.uniform(DELAY_MIN, DELAY_MAX))
            continue
        if followers < MIN_FOLLOWERS:
            progress[username] = {"error": "too_small", "followers": followers}
            print(f"SKIP tiny ({followers} flw)")
            time.sleep(random.uniform(DELAY_MIN, DELAY_MAX))
            continue

        # Collect emails
        all_emails = set()

        # From bio
        bio_emails = [e for e in extract_emails(bio) if is_consumer_email(e)]
        all_emails.update(bio_emails)

        # From business_email field
        biz_email = profile.get("business_email", "")
        if biz_email and is_consumer_email(biz_email):
            all_emails.add(biz_email.lower())

        # From external URL (linktree, website)
        url_emails = []
        ext_url = profile.get("external_url", "")
        if not all_emails and ext_url:
            try:
                url_emails = scrape_url_for_emails(ext_url)
                all_emails.update(url_emails)
            except:
                pass

        profile["username"] = username
        profile["bio_emails"] = bio_emails
        profile["url_emails"] = url_emails
        profile["error"] = None
        progress[username] = profile

        if all_emails:
            emails_found += 1
            print(f"EMAIL {followers:>7,} flw | {list(all_emails)[:2]}")
        else:
            print(f"  ~   {followers:>7,} flw | no email")

        # Save every 20
        if processed % 20 == 0:
            with open(progress_file, 'w') as f:
                json.dump(progress, f, indent=1, default=str)
            total_e = count_consumer_emails(progress)
            rate_pct = emails_found * 100 // max(processed, 1)
            print(f"  ── saved | {processed}/{total} | {emails_found} new ({rate_pct}%) | biz={businesses_skipped} | total={total_e} ──")

        time.sleep(random.uniform(DELAY_MIN, DELAY_MAX))

    # Final save
    with open(progress_file, 'w') as f:
        json.dump(progress, f, indent=1, default=str)

    build_csv(progress, csv_path)


def count_consumer_emails(progress):
    emails = set()
    for username, p in progress.items():
        if not isinstance(p, dict) or p.get("error"):
            continue
        if is_business_account(p.get("full_name", ""), p.get("bio", ""), username):
            continue
        for e in p.get("bio_emails", []) + p.get("url_emails", []) + p.get("emails_from_bio", []):
            if is_consumer_email(e):
                emails.add(e)
        biz_e = p.get("business_email", "")
        if biz_e and is_consumer_email(biz_e):
            emails.add(biz_e.lower())
    return len(emails)


def build_csv(progress, csv_path):
    rows = []
    seen = set()

    for username, p in progress.items():
        if not isinstance(p, dict) or p.get("error"):
            continue
        name = p.get("full_name", "")
        bio = p.get("bio", "")
        if is_business_account(name, bio, username):
            continue

        all_emails = set()
        for e in p.get("bio_emails", []) + p.get("url_emails", []) + p.get("emails_from_bio", []):
            if is_consumer_email(e):
                all_emails.add(e)
        biz_e = p.get("business_email", "")
        if biz_e and is_consumer_email(biz_e):
            all_emails.add(biz_e.lower())

        if not all_emails:
            continue

        sources = []
        if p.get("bio_emails") or p.get("emails_from_bio"):
            sources.append("bio")
        if p.get("url_emails"):
            sources.append("url")
        if biz_e and is_consumer_email(biz_e):
            sources.append("biz_field")

        for email in all_emails:
            if email in seen:
                continue
            seen.add(email)
            rows.append({
                "email": email,
                "name": name,
                "ig_username": username,
                "followers": p.get("followers", 0),
                "platform": "instagram",
                "category": "fitness_influencer",
                "tags": "",
                "bio": (bio or "")[:200].replace('\n', ' '),
                "external_url": p.get("external_url", ""),
                "email_source": "+".join(sources),
                "is_business": False,
                "business_category": p.get("business_category", ""),
            })

    fieldnames = ["email", "name", "ig_username", "followers", "platform",
                  "category", "tags", "bio", "external_url", "email_source",
                  "is_business", "business_category"]

    with open(csv_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print(f"\n  CSV: {csv_path}")
    print(f"  Consumer emails: {len(rows)}")


def show_stats(progress, all_usernames):
    total = len(progress)
    errors = sum(1 for p in progress.values() if isinstance(p, dict) and p.get("error"))
    biz = sum(1 for p in progress.values() if isinstance(p, dict) and p.get("error") == "business")
    celeb = sum(1 for p in progress.values() if isinstance(p, dict) and p.get("error") == "celebrity")
    tiny = sum(1 for p in progress.values() if isinstance(p, dict) and p.get("error") == "too_small")
    consumer_emails = count_consumer_emails(progress)
    remaining = len([u for u in all_usernames if u not in progress])

    print(f"  Total processed: {total}")
    print(f"  Businesses skipped: {biz}")
    print(f"  Celebrities skipped: {celeb}")
    print(f"  Too small skipped: {tiny}")
    print(f"  Other errors: {errors - biz - celeb - tiny}")
    print(f"  Consumer emails: {consumer_emails}")
    print(f"  Remaining: {remaining}")


if __name__ == "__main__":
    main()
