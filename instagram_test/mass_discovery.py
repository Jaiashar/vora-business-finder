#!/usr/bin/env python3
"""
Mass Username Discovery + Email Extraction Pipeline
=====================================================
Scrapes multiple Feedspot category pages to build a list of thousands of
IG usernames, then processes them through imginn.org for bio emails and
linktree scraping.

Target: 10,000+ consumer emails from fitness/wellness/health IG accounts.

Usage:
  python mass_discovery.py discover    # Phase 1: scrape directories for usernames
  python mass_discovery.py extract     # Phase 2: extract emails from usernames
  python mass_discovery.py all         # Both phases
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
from urllib.parse import urlparse, quote_plus

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

IG_HEADERS = {
    "User-Agent": UA,
    "X-IG-App-ID": "936619743392459",
    "Accept": "*/*",
    "Accept-Language": "en-US,en;q=0.9",
}

EMAIL_RE = re.compile(r'[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}')
JUNK_DOMAINS = {
    'sentry.io', 'w3.org', 'schema.org', 'example.com', 'domain.com',
    'wixpress.com', 'cloudflare.com', 'mailchimp.com', 'googleapis.com',
    'facebook.com', 'instagram.com', 'fbcdn.net', 'cdninstagram.com',
    'apple.com', 'google.com', 'gstatic.com', 'youtube.com',
    'twitter.com', 'x.com', 'tiktok.com', 'spotify.com',
    'onetrust.com', 'pinterest.com', 'linkedin.com',
    'imginn.org', 'imginn.com', 'feedspot.com',
    'patreon.com', 'substackinc.com', 'stanwith.me',
    'playbookapp.io', 'joinplaybook.com', 'movesapp.com', 'vrtoapp.com',
    'boostcamp.app', 'strongstrongfriends.com', 'beacons.ai',
}
JUNK_KW = ['sentry', 'noreply', 'no-reply', 'unsubscribe', 'webpack',
           'placeholder', 'donotreply', 'mailer-daemon', 'u003e',
           'anuj@feedspot']


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
        if not url.startswith(('http://', 'https://')):
            url = 'https://' + url
        req = urllib.request.Request(url, headers=HEADERS)
        resp = urllib.request.urlopen(req, timeout=timeout, context=SSL_CTX)
        content = resp.read()
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
    all_emails.update(extract_emails(html))

    linktree_domains = ['linktr.ee', 'komi.io', 'beacons.ai', 'link.bio',
                        'campsite.bio', 'stan.store', 'hoo.be', 'snipfeed.co',
                        'flow.page', 'solo.to', 'tap.bio', 'bio.link',
                        'milkshake.app', 'carrd.co', 'lynx.bio', 'bio.site',
                        'konect.to', 'link.me']
    if any(d in url.lower() for d in linktree_domains):
        for link in re.findall(r'href="(https?://[^"]+)"', html)[:10]:
            if any(k in link.lower() for k in ['contact', 'about', 'email', 'book', 'work',
                                                'collab', 'inquiry', 'mailto', 'coach', 'hire']):
                sub = fetch_page(link, 5)
                if sub:
                    all_emails.update(extract_emails(sub))
                time.sleep(0.3)
    else:
        parsed = urlparse(url if url.startswith('http') else 'https://' + url)
        base = f"{parsed.scheme}://{parsed.netloc}"
        for path in ['/contact', '/about', '/work-with-me']:
            if len(all_emails) >= 3:
                break
            sub = fetch_page(base + path, 5)
            if sub:
                all_emails.update(extract_emails(sub))
            time.sleep(0.2)

    return list(all_emails)


# ═══════════════════════════════════════════════════════════════════
# PHASE 1: MASS DISCOVERY — Scrape directories for usernames
# ═══════════════════════════════════════════════════════════════════
FEEDSPOT_CATEGORIES = [
    # Fitness
    ("micro_fitness", "https://influencers.feedspot.com/micro_fitness_instagram_influencers/"),
    ("fitness", "https://influencers.feedspot.com/fitness_instagram_influencers/"),
    ("female_fitness", "https://influencers.feedspot.com/female_fitness_instagram_influencers/"),
    ("fitness_us", "https://influencers.feedspot.com/fitness_us_instagram_influencers/"),
    ("strength_training", "https://influencers.feedspot.com/strength_training_instagram_influencers/"),
    ("home_workout", "https://influencers.feedspot.com/home_workout_instagram_influencers/"),
    ("powerlifting", "https://influencers.feedspot.com/powerlifting_instagram_influencers/"),
    ("weightlifting", "https://influencers.feedspot.com/weightlifting_instagram_influencers/"),
    ("functional_training", "https://influencers.feedspot.com/functional_training_instagram_influencers/"),
    # Health & wellness
    ("health", "https://influencers.feedspot.com/health_instagram_influencers/"),
    ("health_coach", "https://influencers.feedspot.com/health_coach_instagram_influencers/"),
    ("nutrition", "https://influencers.feedspot.com/nutrition_instagram_influencers/"),
    ("wellness", "https://influencers.feedspot.com/wellness_instagram_influencers/"),
    ("mental_health", "https://influencers.feedspot.com/mental_health_instagram_influencers/"),
    # Yoga & Pilates
    ("yoga", "https://influencers.feedspot.com/yoga_instagram_influencers/"),
    ("pilates", "https://influencers.feedspot.com/pilates_instagram_influencers/"),
    # Running & endurance
    ("running", "https://influencers.feedspot.com/running_instagram_influencers/"),
    ("marathon", "https://influencers.feedspot.com/marathon_instagram_influencers/"),
    # CrossFit
    ("crossfit", "https://influencers.feedspot.com/crossfit_instagram_influencers/"),
    # Specific sports
    ("bodybuilding", "https://influencers.feedspot.com/bodybuilding_instagram_influencers/"),
    ("cycling", "https://influencers.feedspot.com/cycling_instagram_influencers/"),
    ("swimming", "https://influencers.feedspot.com/swimming_instagram_influencers/"),
    # Lifestyle & diet
    ("vegan", "https://influencers.feedspot.com/vegan_instagram_influencers/"),
    ("keto", "https://influencers.feedspot.com/keto_instagram_influencers/"),
    ("weight_loss", "https://influencers.feedspot.com/weight_loss_instagram_influencers/"),
    # Regional
    ("la_fitness", "https://influencers.feedspot.com/los_angeles_fitness_instagram_influencers/"),
    ("nyc_fitness", "https://influencers.feedspot.com/nyc_fitness_instagram_influencers/"),
    ("uk_fitness", "https://influencers.feedspot.com/uk_fitness_instagram_influencers/"),
    ("australian_fitness", "https://influencers.feedspot.com/australian_fitness_instagram_influencers/"),
    ("canadian_fitness", "https://influencers.feedspot.com/canadian_fitness_instagram_influencers/"),
    # Personal trainers
    ("personal_trainers", "https://influencers.feedspot.com/personal_trainers/"),
    ("personal_fitness_trainers", "https://influencers.feedspot.com/personal_fitness_trainers/"),
    # Norwegian (small niche)
    ("norwegian_fitness", "https://influencers.feedspot.com/norwegian_fitness_instagram_influencers/"),
]


def scrape_feedspot_page(url, category):
    """Extract IG usernames from a Feedspot category page."""
    html = fetch_page(url)
    if not html:
        return []

    usernames = []
    # Pattern: Instagram Handle links
    for match in re.finditer(r'instagram\.com/([a-zA-Z0-9_.]+)/?["\s<]', html):
        un = match.group(1).rstrip('/')
        if un and len(un) > 1 and un not in ('p', 'explore', 'accounts', 'reel', 'stories'):
            usernames.append(un)

    return list(dict.fromkeys(usernames))


def discover_from_google(queries, max_per_query=50):
    """Search Google/Bing for IG usernames in fitness niches."""
    all_usernames = []

    for query in queries:
        url = f'https://www.bing.com/search?q={quote_plus(query)}&count=50'
        html = fetch_page(url)
        if not html:
            continue

        for match in re.finditer(r'instagram\.com/([a-zA-Z0-9_.]{2,30})', html):
            un = match.group(1)
            if un not in ('p', 'explore', 'accounts', 'reel', 'stories', 'about', 'developer'):
                all_usernames.append(un)

        time.sleep(random.uniform(2, 4))

    return list(dict.fromkeys(all_usernames))


def phase1_discover():
    """Discover thousands of IG usernames from directories."""
    usernames_file = os.path.join(BASE_DIR, "mass_usernames.json")

    existing = {}
    if os.path.exists(usernames_file):
        with open(usernames_file) as f:
            existing = json.load(f)

    all_usernames = set(existing.get("usernames", []))
    print(f"  Starting with {len(all_usernames)} existing usernames\n")

    # 1. Scrape Feedspot categories
    print("  ═══ Scraping Feedspot categories ═══\n")
    for cat_name, cat_url in FEEDSPOT_CATEGORIES:
        if cat_name in existing.get("scraped_categories", []):
            print(f"  [{cat_name}] already scraped, skipping")
            continue

        print(f"  [{cat_name}] {cat_url[:60]}...", end=" ", flush=True)
        usernames = scrape_feedspot_page(cat_url, cat_name)
        new = [u for u in usernames if u not in all_usernames]
        all_usernames.update(usernames)
        print(f"+{len(new)} new (total: {len(all_usernames)})")

        existing.setdefault("scraped_categories", []).append(cat_name)
        existing["usernames"] = list(all_usernames)
        with open(usernames_file, 'w') as f:
            json.dump(existing, f)

        time.sleep(random.uniform(2, 5))

    # 2. Search engines for more
    print("\n  ═══ Searching Bing for more usernames ═══\n")
    search_queries = [
        'site:instagram.com "personal trainer" "email" "@gmail.com"',
        'site:instagram.com "fitness coach" "online coaching" "email"',
        'site:instagram.com "certified personal trainer" "DM"',
        'site:instagram.com "NASM" OR "ACE" OR "ISSA" "trainer"',
        'site:instagram.com "macro coach" OR "nutrition coach" email',
        'site:instagram.com "yoga teacher" "email" "@gmail.com"',
        'site:instagram.com "pilates instructor" email',
        'site:instagram.com "crossfit coach" email',
        'site:instagram.com "running coach" email',
        'site:instagram.com "strength coach" email',
        'instagram "fitness influencer" email list micro',
        'instagram "online coach" fitness email "@gmail.com" -site:instagram.com',
        '"linktr.ee" "fitness coach" "@gmail.com"',
        '"linktr.ee" "personal trainer" email',
    ]

    bing_usernames = discover_from_google(search_queries)
    new_bing = [u for u in bing_usernames if u not in all_usernames]
    all_usernames.update(bing_usernames)
    print(f"  Bing search: +{len(new_bing)} new (total: {len(all_usernames)})")

    # Save final
    existing["usernames"] = list(all_usernames)
    existing["discovery_timestamp"] = datetime.now().isoformat()
    with open(usernames_file, 'w') as f:
        json.dump(existing, f, indent=2)

    print(f"\n  ═══ Discovery complete: {len(all_usernames)} total usernames ═══")
    return list(all_usernames)


# ═══════════════════════════════════════════════════════════════════
# PHASE 2: MASS EMAIL EXTRACTION
# ═══════════════════════════════════════════════════════════════════
def fetch_imginn(username):
    """Fetch bio via imginn.org."""
    url = f'https://imginn.org/{username}/'
    req = urllib.request.Request(url, headers=HEADERS)
    resp = urllib.request.urlopen(req, timeout=15, context=SSL_CTX)
    raw = resp.read().decode('utf-8', errors='ignore')

    text = re.sub(r'<[^>]+>', ' ', raw)
    text = html_lib.unescape(text)

    bio = ''
    for pat in [
        r'class="[^"]*bio[^"]*"[^>]*>(.*?)</(?:div|span|p)',
        r'class="[^"]*description[^"]*"[^>]*>(.*?)</(?:div|span|p)',
    ]:
        m = re.search(pat, raw, re.DOTALL | re.I)
        if m:
            bio = re.sub(r'<[^>]+>', ' ', m.group(1)).strip()
            bio = html_lib.unescape(bio)
            break

    name = ''
    name_match = re.search(r'<h1[^>]*>(.*?)</h1>', raw, re.DOTALL)
    if name_match:
        name = re.sub(r'<[^>]+>', '', name_match.group(1)).strip()

    page_emails = extract_emails(text)

    return {
        "username": username,
        "full_name": name,
        "bio": bio[:500],
        "emails_from_bio": page_emails,
        "source": "imginn",
    }


def fetch_ig_api(username):
    """Fetch full profile via IG public API."""
    try:
        url = f"https://www.instagram.com/api/v1/users/web_profile_info/?username={username}"
        req = urllib.request.Request(url, headers=IG_HEADERS)
        resp = urllib.request.urlopen(req, timeout=10, context=SSL_CTX)
        data = json.loads(resp.read().decode('utf-8'))
        user = data.get("data", {}).get("user", {})
        if not user:
            return None
        return {
            "username": username,
            "full_name": user.get("full_name", ""),
            "bio": user.get("biography", ""),
            "external_url": user.get("external_url", ""),
            "followers": user.get("edge_followed_by", {}).get("count", 0),
            "is_business": user.get("is_business_account", False),
            "business_category": user.get("category_name", ""),
            "business_email": user.get("business_email", ""),
        }
    except urllib.error.HTTPError as e:
        if e.code in (401, 429):
            return "rate_limited"
        return None
    except:
        return None


def phase2_extract(max_accounts=None):
    """Extract emails from discovered usernames."""
    usernames_file = os.path.join(BASE_DIR, "mass_usernames.json")
    progress_file = os.path.join(BASE_DIR, "mass_progress.json")
    csv_path = os.path.join(BASE_DIR, "mass_consumer_leads.csv")

    if not os.path.exists(usernames_file):
        print("No usernames file. Run: python mass_discovery.py discover")
        return

    with open(usernames_file) as f:
        data = json.load(f)
    all_usernames = data.get("usernames", [])

    progress = {}
    if os.path.exists(progress_file):
        with open(progress_file) as f:
            progress = json.load(f)

    to_process = [u for u in all_usernames if u not in progress]
    if max_accounts:
        to_process = to_process[:max_accounts]

    total = len(to_process)
    emails_found = 0
    processed = 0
    ig_api_available = None  # None = untested
    ig_api_requests = 0
    IG_API_BATCH = 25  # Do 25 API requests then pause

    print(f"  {len(progress)} already processed, {total} remaining\n")

    for i, username in enumerate(to_process):
        processed += 1
        print(f"  [{processed}/{total}] @{username}...", end=" ", flush=True)

        profile = None
        all_emails = set()

        # Try imginn first (no rate limit)
        try:
            profile = fetch_imginn(username)
            all_emails.update(profile.get("emails_from_bio", []))
        except urllib.error.HTTPError as e:
            if e.code == 410:
                progress[username] = {"error": "gone"}
                print("gone")
                continue
            progress[username] = {"error": str(e)[:50]}
            print(f"error ({e.code})")
            time.sleep(1)
            continue
        except Exception as e:
            progress[username] = {"error": str(e)[:50]}
            print(f"error")
            time.sleep(1)
            continue

        # If no bio email, try IG API for external URL (with careful rate limiting)
        if not all_emails and ig_api_available is not False:
            if ig_api_requests > 0 and ig_api_requests % IG_API_BATCH == 0:
                print(f"\n  --- API cooldown (120s after {ig_api_requests} requests) ---")
                time.sleep(120)

            api_result = fetch_ig_api(username)
            ig_api_requests += 1

            if api_result == "rate_limited":
                ig_api_available = False
                print("(API limited) ", end="", flush=True)
            elif api_result:
                ig_api_available = True
                profile["followers"] = api_result["followers"]
                profile["is_business"] = api_result["is_business"]
                profile["business_category"] = api_result["business_category"]
                profile["external_url"] = api_result.get("external_url", "")

                bio_emails = extract_emails(api_result["bio"])
                all_emails.update(bio_emails)
                profile["emails_from_bio"] = list(all_emails)

                if api_result.get("business_email"):
                    all_emails.add(api_result["business_email"].lower())
                    profile["business_email"] = api_result["business_email"]

                # Scrape external URL
                ext_url = api_result.get("external_url", "")
                if not all_emails and ext_url:
                    try:
                        url_emails = scrape_url_for_emails(ext_url)
                        profile["url_emails"] = url_emails
                        all_emails.update(url_emails)
                    except:
                        pass

        profile["error"] = None
        progress[username] = profile

        if all_emails:
            emails_found += 1
            print(f"EMAIL | {list(all_emails)[:2]}")
        else:
            print(f"no email")

        # Save every 25 accounts
        if processed % 25 == 0:
            with open(progress_file, 'w') as f:
                json.dump(progress, f, indent=1, default=str)
            rate = emails_found * 100 // max(processed, 1)
            total_emails = sum(1 for p in progress.values()
                             if not p.get("error") and (p.get("emails_from_bio") or p.get("url_emails") or p.get("business_email")))
            print(f"  ── saved | {processed}/{total} | batch: {emails_found} emails ({rate}%) | all-time: {total_emails} ──")

        time.sleep(random.uniform(2, 4))

    # Final save
    with open(progress_file, 'w') as f:
        json.dump(progress, f, indent=1, default=str)

    # Build CSV
    build_csv(progress, csv_path)


def build_csv(progress, csv_path):
    """Build CSV from progress data."""
    rows = []
    for username, p in progress.items():
        if p.get("error"):
            continue

        all_emails = set(p.get("emails_from_bio", []) + p.get("url_emails", []))
        if p.get("business_email"):
            all_emails.add(p["business_email"].lower())

        if not all_emails:
            continue

        sources = []
        if p.get("emails_from_bio"):
            sources.append("bio")
        if p.get("url_emails"):
            sources.append("url")
        if p.get("business_email"):
            sources.append("business_email")

        for email in all_emails:
            rows.append({
                "email": email,
                "name": p.get("full_name", ""),
                "ig_username": username,
                "followers": p.get("followers", 0),
                "platform": "instagram",
                "category": "fitness_influencer",
                "tags": "",
                "bio": (p.get("bio") or "")[:200].replace('\n', ' '),
                "external_url": p.get("external_url", ""),
                "email_source": "+".join(sources),
                "is_business": p.get("is_business", False),
                "business_category": p.get("business_category", ""),
            })

    fieldnames = ["email", "name", "ig_username", "followers", "platform",
                  "category", "tags", "bio", "external_url", "email_source",
                  "is_business", "business_category"]

    with open(csv_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    unique = len(set(r["email"] for r in rows))
    print(f"\n  CSV: {csv_path}")
    print(f"  Rows: {len(rows)}, Unique emails: {unique}")
    return unique


def main():
    mode = sys.argv[1] if len(sys.argv) > 1 else "all"

    print("+" + "=" * 60 + "+")
    print("|  MASS CONSUMER EMAIL PIPELINE                            |")
    print(f"|  {datetime.now().strftime('%Y-%m-%d %H:%M:%S'):<57}|")
    print("+" + "=" * 60 + "+\n")

    if mode in ("discover", "all"):
        print("  PHASE 1: MASS DISCOVERY")
        print("  " + "─" * 40)
        usernames = phase1_discover()
        print()

    if mode in ("extract", "all"):
        print("  PHASE 2: EMAIL EXTRACTION")
        print("  " + "─" * 40)
        phase2_extract()


if __name__ == "__main__":
    main()
