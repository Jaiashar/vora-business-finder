#!/usr/bin/env python3
"""
Instagram Discovery Test
=========================
Tests 3 different strategies for DISCOVERING unknown micro-influencers,
UGC creators, and student athletes — NOT scraping known celebrities.

Strategy 1: Google dorking (site:instagram.com + niche keywords)
Strategy 2: Follower scraping (followers of niche gym/university accounts)
Strategy 3: Ambassador/trainer page scraping (gym websites listing their trainers)

For each strategy: discover usernames → fetch profiles → scrape URLs → extract emails
"""

import os
import re
import sys
import csv
import time
import json
import random
import urllib.request
import urllib.error
import ssl
from datetime import datetime
from urllib.parse import urlparse, quote_plus

# ─── Load .env ───────────────────────────────────────────────────
def load_env():
    env_path = os.path.join(os.path.dirname(__file__), '..', '.env')
    env = {}
    with open(env_path) as f:
        for line in f:
            line = line.strip()
            if '=' in line and not line.startswith('#'):
                key, val = line.split('=', 1)
                env[key.strip()] = val.strip()
    return env

ENV = load_env()

# ─── Shared utilities ────────────────────────────────────────────
EMAIL_RE = re.compile(r'[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}')

JUNK_EMAILS = {
    'email@example.com', 'your@email.com', 'name@domain.com',
    'filler@godaddy.com', 'contact@mysite.com', 'user@domain.com',
    'test@test.com', 'info@example.com',
}
JUNK_DOMAINS = {
    'sentry.io', 'ingest.sentry.io', 'ingest.us.sentry.io',
    'w3.org', 'schema.org', 'example.com', 'domain.com', 'mysite.com',
    'wixpress.com', 'cloudflare.com', 'mailchimp.com', 'googleapis.com',
    'facebook.com', 'instagram.com', 'fbcdn.net', 'cdninstagram.com',
    'apple.com', 'google.com', 'gstatic.com', 'youtube.com',
    'twitter.com', 'x.com', 'tiktok.com', 'spotify.com',
    'onetrust.com', 'pinterest.com', 'linkedin.com',
}

def extract_emails(text):
    if not text:
        return []
    found = EMAIL_RE.findall(text.lower())
    cleaned = []
    for e in found:
        domain = e.split('@')[1]
        if e in JUNK_EMAILS or domain in JUNK_DOMAINS:
            continue
        if any(kw in e for kw in ['sentry', 'wixpress', 'noreply', 'no-reply',
                                    'donotreply', 'unsubscribe', 'mailer-daemon',
                                    'webpack', 'placeholder']):
            continue
        if e.endswith(('.png', '.jpg', '.gif', '.svg', '.webp', '.css', '.js')):
            continue
        if len(domain) < 5:
            continue
        cleaned.append(e)
    return list(set(cleaned))


SSL_CTX = ssl.create_default_context()
SSL_CTX.check_hostname = False
SSL_CTX.verify_mode = ssl.CERT_NONE

def web_headers(referer=None):
    h = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                       "AppleWebKit/537.36 (KHTML, like Gecko) "
                       "Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
    }
    if referer:
        h["Referer"] = referer
    return h


def fetch_page(url, timeout=10, referer=None):
    try:
        if not url.startswith(('http://', 'https://')):
            url = 'https://' + url
        req = urllib.request.Request(url, headers=web_headers(referer))
        resp = urllib.request.urlopen(req, timeout=timeout, context=SSL_CTX)
        content = resp.read()
        try:
            return content.decode('utf-8')
        except UnicodeDecodeError:
            return content.decode('latin-1', errors='ignore')
    except:
        return None


def fetch_ig_profile(username):
    """Fetch Instagram profile via public API. Instant pass/fail."""
    try:
        url = f"https://www.instagram.com/api/v1/users/web_profile_info/?username={username}"
        headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                           "AppleWebKit/537.36 (KHTML, like Gecko) "
                           "Chrome/120.0.0.0 Safari/537.36",
            "X-IG-App-ID": "936619743392459",
            "Accept": "*/*",
        }
        req = urllib.request.Request(url, headers=headers)
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
        }
    except:
        return None


def scrape_url_for_emails(url):
    """Scrape a URL and sub-pages for emails."""
    all_emails = set()
    html = fetch_page(url)
    if html:
        for e in extract_emails(html):
            all_emails.add(e)
    if html:
        linktree_domains = ['linktr.ee', 'komi.io', 'beacons.ai', 'stan.store', 'hoo.be', 'snipfeed.co']
        if any(d in url.lower() for d in linktree_domains):
            links = re.findall(r'href="(https?://[^"]+)"', html)
            for link in links[:4]:
                if any(kw in link.lower() for kw in ['contact', 'about', 'email', 'book', 'work', 'collab']):
                    sub = fetch_page(link, timeout=5)
                    if sub:
                        for e in extract_emails(sub):
                            all_emails.add(e)
                    time.sleep(0.5)
        else:
            parsed = urlparse(url if url.startswith('http') else 'https://' + url)
            base = f"{parsed.scheme}://{parsed.netloc}"
            for path in ['/contact', '/contact-us', '/about']:
                if len(all_emails) >= 3:
                    break
                sub = fetch_page(base + path, timeout=5)
                if sub:
                    for e in extract_emails(sub):
                        all_emails.add(e)
                time.sleep(0.3)
    return list(all_emails)


# ═══════════════════════════════════════════════════════════════════
# STRATEGY 1: Google Dorking
# ═══════════════════════════════════════════════════════════════════
def strategy_google_dorking():
    """
    Search Google for Instagram profiles matching niche fitness keywords.
    Extracts usernames from Google results pointing to instagram.com.
    """
    print("\n" + "="*60)
    print("  STRATEGY 1: Google Dorking")
    print("  Find IG profiles via Google search")
    print("="*60)

    # Queries designed to find small/mid influencers, trainers, UGC
    QUERIES = [
        'site:instagram.com "personal trainer" "email" -kayla -gymshark',
        'site:instagram.com "fitness coach" "DM for" OR "email" followers',
        'site:instagram.com "UGC creator" "fitness" "collab"',
        'site:instagram.com "student athlete" "fitness"',
        'site:instagram.com "certified trainer" "NASM" OR "ACE" OR "ISSA"',
        'site:instagram.com "gym owner" "small business"',
        'site:instagram.com "online coaching" "fitness" "link in bio"',
        'site:instagram.com "macro coach" OR "nutrition coach" "email"',
        'site:instagram.com "yoga teacher" "RYT" "bookings"',
        'site:instagram.com "crossfit coach" "CF-L" "contact"',
    ]

    IG_USERNAME_RE = re.compile(r'instagram\.com/([a-zA-Z0-9_.]{1,30})(?:/|\?|$)')

    all_usernames = set()
    start = time.time()

    for i, query in enumerate(QUERIES):
        print(f"\n  Query {i+1}/{len(QUERIES)}: {query[:70]}...")
        encoded = quote_plus(query)
        search_url = f"https://www.google.com/search?q={encoded}&num=20"

        html = fetch_page(search_url, referer="https://www.google.com/")
        if not html:
            print("    ✗ No response from Google")
            time.sleep(3)
            continue

        # Extract Instagram usernames from Google results
        usernames = IG_USERNAME_RE.findall(html)
        # Filter out generic Instagram pages
        skip = {'explore', 'p', 'reel', 'stories', 'accounts', 'about',
                'legal', 'developer', 'privacy', 'terms', 'help', 'directory',
                'web', 'api', 'static', 'tags'}
        usernames = [u for u in usernames if u.lower() not in skip and len(u) > 2]
        new = [u for u in usernames if u not in all_usernames]
        all_usernames.update(new)

        print(f"    Found {len(usernames)} usernames, {len(new)} new → total: {len(all_usernames)}")

        time.sleep(random.uniform(3, 6))  # Be respectful to Google

    elapsed = time.time() - start
    print(f"\n  Google dorking: {len(all_usernames)} unique usernames in {elapsed:.0f}s")
    return list(all_usernames), elapsed


# ═══════════════════════════════════════════════════════════════════
# STRATEGY 2: Follower Scraping (via Instaloader logged-in)
# ═══════════════════════════════════════════════════════════════════
def strategy_follower_scraping():
    """
    Get followers of niche accounts (university rec, local gyms).
    These followers are the micro-influencers/UGC people we want.
    """
    print("\n" + "="*60)
    print("  STRATEGY 2: Follower Scraping")
    print("  Get followers of niche gym/university accounts")
    print("="*60)

    import instaloader

    IG_USERNAME = ENV.get('IG_USERNAME', '')
    IG_PASSWORD = ENV.get('IG_PASSWORD', '')

    base_dir = os.path.dirname(os.path.abspath(__file__))
    session_file = os.path.join(base_dir, f"session-{IG_USERNAME}")

    L = instaloader.Instaloader()
    try:
        L.load_session_from_file(IG_USERNAME, session_file)
        print(f"  Session loaded for @{IG_USERNAME}")
    except:
        print(f"  Logging in as @{IG_USERNAME}...")
        try:
            L.login(IG_USERNAME, IG_PASSWORD)
            L.save_session_to_file(session_file)
            print("  Login OK.")
        except Exception as e:
            print(f"  Login FAILED: {e}")
            return [], 0

    # Seed accounts — niche/local, NOT huge brands
    # Followers of these are real gym-goers, trainers, students
    SEED_ACCOUNTS = [
        "uaborelcicampusrec",    # UCI campus rec (if exists)
        "ucaborelirec",          # variations
        "theprehabguys",    # PT/rehab — followers are trainers
        "barbellmedicine",  # strength coaching — followers are coaches
        "boxrox",           # crossfit news — followers are crossfit athletes
    ]

    all_usernames = set()
    start = time.time()

    for seed in SEED_ACCOUNTS:
        print(f"\n  Seed: @{seed}...", end=" ", flush=True)
        try:
            profile = instaloader.Profile.from_username(L.context, seed)
            print(f"({profile.followers:,} followers)")

            count = 0
            for follower in profile.get_followers():
                if count >= 30:  # Take 30 from each seed
                    break
                all_usernames.add(follower.username)
                count += 1
                time.sleep(0.3)

            print(f"    Got {count} followers → total: {len(all_usernames)}")

        except Exception as e:
            print(f"✗ {str(e)[:60]}")

        time.sleep(random.uniform(5, 10))

    elapsed = time.time() - start
    print(f"\n  Follower scraping: {len(all_usernames)} unique usernames in {elapsed:.0f}s")
    return list(all_usernames), elapsed


# ═══════════════════════════════════════════════════════════════════
# STRATEGY 3: Ambassador / Trainer Page Scraping
# ═══════════════════════════════════════════════════════════════════
def strategy_ambassador_scraping():
    """
    Scrape gym/brand websites that list their trainers with IG handles.
    These are real, working trainers — exactly our target audience.
    """
    print("\n" + "="*60)
    print("  STRATEGY 3: Ambassador / Trainer Page Scraping")
    print("  Scrape gym websites for trainer Instagram handles")
    print("="*60)

    # Pages that list trainers/coaches/ambassadors with Instagram links
    PAGES = [
        # Gym trainer directories
        ("https://www.equinox.com/trainers", "Equinox Trainers"),
        ("https://www.barrys.com/instructors", "Barry's Instructors"),
        ("https://www.orangetheory.com/en-us/coaches/", "OTF Coaches"),
        ("https://www.purebarre.com/instructors", "Pure Barre"),
        ("https://www.f45training.com/trainers", "F45 Trainers"),
        # Fitness brand ambassador pages
        ("https://www.gymshark.com/pages/athletes", "Gymshark Athletes"),
        ("https://www.lululemon.com/community/ambassadors", "Lululemon Ambassadors"),
        ("https://www.myprotein.com/blog/our-ambassadors/", "MyProtein Ambassadors"),
        # CrossFit box member pages
        ("https://games.crossfit.com/athletes", "CrossFit Athletes"),
        # Trainer marketplaces
        ("https://www.thumbtack.com/k/personal-trainers/near-me/", "Thumbtack Trainers"),
        ("https://www.trainerize.com/trainers/", "Trainerize Trainers"),
    ]

    IG_HANDLE_RE = re.compile(r'(?:instagram\.com/|@)([a-zA-Z0-9_.]{3,30})')

    all_usernames = set()
    start = time.time()

    for url, label in PAGES:
        print(f"\n  {label}: {url[:60]}...", end=" ", flush=True)

        html = fetch_page(url)
        if not html:
            print("✗ no response")
            time.sleep(1)
            continue

        # Find Instagram handles
        handles = IG_HANDLE_RE.findall(html)
        # Filter generic
        skip = {'explore', 'p', 'reel', 'stories', 'accounts', 'about',
                'legal', 'developer', 'privacy', 'help', 'share',
                'gymshark', 'lululemon', 'equinox', 'barrys'}
        handles = [h for h in handles if h.lower() not in skip and len(h) > 2
                   and not h.startswith('.')]
        new = [h for h in handles if h not in all_usernames]
        all_usernames.update(new)

        print(f"found {len(handles)} handles, {len(new)} new → total: {len(all_usernames)}")

        # Also look for email addresses directly on the page
        emails = extract_emails(html)
        if emails:
            print(f"    Bonus: found {len(emails)} emails directly: {emails[:3]}")

        time.sleep(random.uniform(2, 4))

    elapsed = time.time() - start
    print(f"\n  Ambassador scraping: {len(all_usernames)} unique usernames in {elapsed:.0f}s")
    return list(all_usernames), elapsed


# ═══════════════════════════════════════════════════════════════════
# ENRICHMENT: Take discovered usernames → fetch profiles → get emails
# ═══════════════════════════════════════════════════════════════════
def enrich_usernames(usernames, label, max_profiles=40):
    """Take a list of usernames, fetch their IG profiles, scrape URLs for emails."""
    print(f"\n  Enriching up to {max_profiles} profiles from {label}...")

    results = []
    usernames = usernames[:max_profiles]

    for i, username in enumerate(usernames):
        print(f"    [{i+1}/{len(usernames)}] @{username}...", end=" ", flush=True)

        profile = fetch_ig_profile(username)
        if not profile:
            print("✗ not found")
            time.sleep(0.5)
            continue

        profile["emails_from_bio"] = extract_emails(profile["bio"])
        profile["url_emails"] = []
        profile["source"] = label

        # Scrape external URL for emails
        if profile.get("external_url"):
            try:
                url_emails = scrape_url_for_emails(profile["external_url"])
                profile["url_emails"] = url_emails
            except:
                pass

        all_emails = set(profile["emails_from_bio"] + profile["url_emails"])

        if all_emails:
            print(f"✓ {profile['followers']:,} flw | EMAILS: {list(all_emails)}")
        else:
            url_short = (profile.get("external_url") or "none")[:35]
            print(f"~ {profile['followers']:,} flw | no email | url={url_short}")

        results.append(profile)
        time.sleep(random.uniform(2, 3.5))

    with_email = sum(1 for r in results if r.get("emails_from_bio") or r.get("url_emails"))
    print(f"\n  {label}: {with_email}/{len(results)} profiles had emails ({with_email*100//max(len(results),1)}%)")

    return results


# ═══════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════
def main():
    print("╔════════════════════════════════════════════════════════════╗")
    print("║     INSTAGRAM DISCOVERY TEST — 3 STRATEGIES              ║")
    print(f"║     {datetime.now().strftime('%Y-%m-%d %H:%M:%S'):<53}║")
    print("╚════════════════════════════════════════════════════════════╝")

    base = os.path.dirname(os.path.abspath(__file__))

    # ── Run all 3 discovery strategies ──
    google_users, google_time = strategy_google_dorking()
    ambassador_users, ambassador_time = strategy_ambassador_scraping()
    follower_users, follower_time = strategy_follower_scraping()

    # ── Summary of discovery phase ──
    print("\n" + "="*60)
    print("  DISCOVERY PHASE RESULTS")
    print("="*60)
    print(f"\n  {'Strategy':<30} {'Users Found':<15} {'Time':<10}")
    print(f"  {'-'*30} {'-'*15} {'-'*10}")
    print(f"  {'Google Dorking':<30} {len(google_users):<15} {google_time:.0f}s")
    print(f"  {'Ambassador Pages':<30} {len(ambassador_users):<15} {ambassador_time:.0f}s")
    print(f"  {'Follower Scraping':<30} {len(follower_users):<15} {follower_time:.0f}s")

    # ── Enrich top results from each strategy ──
    print("\n" + "="*60)
    print("  ENRICHMENT PHASE: Fetch profiles + extract emails")
    print("="*60)

    all_profiles = []

    if google_users:
        profiles = enrich_usernames(google_users, "google", max_profiles=30)
        all_profiles.extend(profiles)

    if ambassador_users:
        # Deduplicate against already-seen
        seen = {p["username"] for p in all_profiles}
        new_amb = [u for u in ambassador_users if u not in seen]
        profiles = enrich_usernames(new_amb, "ambassador", max_profiles=30)
        all_profiles.extend(profiles)

    if follower_users:
        seen = {p["username"] for p in all_profiles}
        new_fol = [u for u in follower_users if u not in seen]
        profiles = enrich_usernames(new_fol, "follower", max_profiles=30)
        all_profiles.extend(profiles)

    # ── Final output ──
    print("\n" + "="*60)
    print("  FINAL RESULTS")
    print("="*60)

    total = len(all_profiles)
    with_email = sum(1 for p in all_profiles if p.get("emails_from_bio") or p.get("url_emails"))

    # Break down by source
    for source in ["google", "ambassador", "follower"]:
        subset = [p for p in all_profiles if p.get("source") == source]
        sub_email = sum(1 for p in subset if p.get("emails_from_bio") or p.get("url_emails"))
        if subset:
            print(f"\n  {source.upper()}:")
            print(f"    Profiles fetched:  {len(subset)}")
            print(f"    With email:        {sub_email} ({sub_email*100//max(len(subset),1)}%)")
            # Show the emails found
            for p in subset:
                emails = set(p.get("emails_from_bio", []) + p.get("url_emails", []))
                if emails:
                    print(f"    ✅ @{p['username']} ({p['followers']:,} flw) → {list(emails)}")

    print(f"\n  ── TOTALS ──")
    print(f"  Total profiles:    {total}")
    print(f"  With email:        {with_email} ({with_email*100//max(total,1)}% hit rate)")

    # Save everything
    csv_path = os.path.join(base, "discovery_results.csv")
    rows = []
    for p in all_profiles:
        all_emails = set(p.get("emails_from_bio", []) + p.get("url_emails", []))
        for email in all_emails:
            rows.append({
                "username": p["username"],
                "full_name": p.get("full_name", ""),
                "email": email,
                "followers": p.get("followers", 0),
                "bio": (p.get("bio") or "")[:150].replace('\n', ' '),
                "external_url": p.get("external_url", ""),
                "is_business": p.get("is_business", False),
                "source_strategy": p.get("source", ""),
            })

    if rows:
        with open(csv_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=rows[0].keys())
            writer.writeheader()
            writer.writerows(rows)
        print(f"\n  CSV saved: {csv_path}")

    json_path = os.path.join(base, "discovery_results.json")
    with open(json_path, 'w') as f:
        json.dump(all_profiles, f, indent=2, default=str)
    print(f"  JSON saved: {json_path}")


if __name__ == "__main__":
    main()
