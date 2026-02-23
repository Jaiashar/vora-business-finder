#!/usr/bin/env python3
"""
Instagram Discovery via Browser Automation
=============================================
Uses Playwright (headless Chrome) to:
  1. Google search for Instagram profiles of micro-influencers / UGC creators
  2. Visit those Instagram profiles in a real browser to get bio + URL
  3. Scrape external URLs for emails

This solves the JS-rendering problem that killed all 3 raw HTTP strategies.
"""

import os
import re
import csv
import time
import json
import random
import urllib.request
import ssl
from datetime import datetime
from urllib.parse import urlparse

from playwright.sync_api import sync_playwright

# ─── Email extraction (reuse proven logic) ───────────────────────
EMAIL_RE = re.compile(r'[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}')

JUNK_EMAILS = {
    'email@example.com', 'your@email.com', 'name@domain.com',
    'user@domain.com', 'test@test.com', 'info@example.com',
    'filler@godaddy.com', 'contact@mysite.com',
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

def fetch_page_simple(url, timeout=8):
    """Simple HTTP fetch for external URL scraping (non-JS sites like Linktree)."""
    try:
        if not url.startswith(('http://', 'https://')):
            url = 'https://' + url
        headers = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"}
        req = urllib.request.Request(url, headers=headers)
        resp = urllib.request.urlopen(req, timeout=timeout, context=SSL_CTX)
        return resp.read().decode('utf-8', errors='ignore')
    except:
        return None


def scrape_url_for_emails(url):
    """Scrape external URL for emails."""
    all_emails = set()
    html = fetch_page_simple(url)
    if html:
        for e in extract_emails(html):
            all_emails.add(e)

        # Follow contact-like links
        linktree_domains = ['linktr.ee', 'komi.io', 'beacons.ai', 'stan.store', 'hoo.be']
        if any(d in url.lower() for d in linktree_domains):
            links = re.findall(r'href="(https?://[^"]+)"', html)
            for link in links[:4]:
                if any(kw in link.lower() for kw in ['contact', 'about', 'email', 'book', 'work', 'collab']):
                    sub = fetch_page_simple(link, timeout=5)
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
                sub = fetch_page_simple(base + path, timeout=5)
                if sub:
                    for e in extract_emails(sub):
                        all_emails.add(e)
                time.sleep(0.3)

    return list(all_emails)


# ═══════════════════════════════════════════════════════════════════
# STEP 1: Google search with real browser to find IG usernames
# ═══════════════════════════════════════════════════════════════════
def google_discover_usernames(page):
    """Use Google in a real browser to find Instagram profiles."""

    QUERIES = [
        'site:instagram.com "personal trainer" "email me" OR "contact"',
        'site:instagram.com "fitness coach" "DM" OR "inquiries"',
        'site:instagram.com "UGC" "fitness" OR "gym" "collab"',
        'site:instagram.com "student athlete" NCAA',
        'site:instagram.com "certified trainer" NASM OR ACE OR ISSA',
        'site:instagram.com "gym owner" OR "studio owner" "fitness"',
        'site:instagram.com "online coach" "fitness" "transform"',
        'site:instagram.com "nutrition coach" "macro" OR "meal plan"',
        'site:instagram.com "yoga instructor" "RYT" OR "private sessions"',
        'site:instagram.com "crossfit" "coach" "box" OR "affiliate"',
        'site:instagram.com "pilates instructor" OR "barre instructor"',
        'site:instagram.com "strength coach" "programming" OR "training"',
    ]

    IG_USERNAME_RE = re.compile(r'instagram\.com/([a-zA-Z0-9_.]{2,30})')
    SKIP_USERS = {'explore', 'p', 'reel', 'reels', 'stories', 'accounts', 'about',
                  'legal', 'developer', 'privacy', 'terms', 'help', 'directory',
                  'web', 'api', 'static', 'tags', 'locations', 'nametag',
                  'direct', 'lite', 'emails', 'session', 'challenge'}

    all_usernames = set()

    for i, query in enumerate(QUERIES):
        print(f"\n  [{i+1}/{len(QUERIES)}] {query[:65]}...")

        try:
            page.goto(f"https://www.google.com/search?q={query}&num=30", timeout=15000)
            time.sleep(random.uniform(2, 4))

            # Get all text content from search results
            content = page.content()

            # Extract IG usernames from URLs in search results
            usernames = IG_USERNAME_RE.findall(content)
            valid = [u for u in usernames if u.lower() not in SKIP_USERS
                     and len(u) > 2 and not u.startswith('.')
                     and not u.isdigit()]

            new = [u for u in valid if u not in all_usernames]
            all_usernames.update(new)

            print(f"    Found {len(valid)} handles, {len(new)} new → total: {len(all_usernames)}")

            # Show some of what we found
            if new:
                print(f"    Sample: {new[:5]}")

        except Exception as e:
            print(f"    Error: {str(e)[:60]}")

        time.sleep(random.uniform(3, 6))

    return list(all_usernames)


# ═══════════════════════════════════════════════════════════════════
# STEP 2: Visit IG profiles in browser to get bio + external URL
# ═══════════════════════════════════════════════════════════════════
def fetch_ig_profiles_browser(page, usernames, max_count=100):
    """Visit each IG profile in the browser to extract bio and URL."""

    profiles = []
    usernames = usernames[:max_count]

    for i, username in enumerate(usernames):
        print(f"  [{i+1}/{len(usernames)}] @{username}...", end=" ", flush=True)

        try:
            page.goto(f"https://www.instagram.com/{username}/", timeout=12000)
            time.sleep(random.uniform(2, 3.5))

            content = page.content()

            # Extract data from the page
            bio_match = re.search(r'"biography"\s*:\s*"([^"]*)"', content)
            url_match = re.search(r'"external_url"\s*:\s*"([^"]*)"', content)
            followers_match = re.search(r'"edge_followed_by"\s*:\s*\{\s*"count"\s*:\s*(\d+)', content)
            name_match = re.search(r'"full_name"\s*:\s*"([^"]*)"', content)
            biz_match = re.search(r'"is_business_account"\s*:\s*(true|false)', content)
            cat_match = re.search(r'"category_name"\s*:\s*"([^"]*)"', content)

            if not bio_match and not followers_match:
                # Page didn't load profile data — might be login wall
                print("✗ no data (login wall?)")
                continue

            bio = ""
            if bio_match:
                try:
                    bio = bio_match.group(1).encode().decode('unicode_escape', errors='ignore')
                except:
                    bio = bio_match.group(1)

            ext_url = url_match.group(1) if url_match else ""
            followers = int(followers_match.group(1)) if followers_match else 0
            full_name = name_match.group(1) if name_match else ""
            is_business = biz_match.group(1) == "true" if biz_match else False
            category = cat_match.group(1) if cat_match else ""

            bio_emails = extract_emails(bio)

            profile = {
                "username": username,
                "full_name": full_name,
                "bio": bio,
                "external_url": ext_url,
                "followers": followers,
                "is_business": is_business,
                "business_category": category,
                "emails_from_bio": bio_emails,
                "url_emails": [],
            }

            email_str = bio_emails[0] if bio_emails else "none"
            url_short = (ext_url or "none")[:40]
            print(f"✓ {followers:,} flw | email={email_str} | url={url_short}")

            profiles.append(profile)

        except Exception as e:
            print(f"✗ {str(e)[:50]}")

        time.sleep(random.uniform(2, 4))

    return profiles


# ═══════════════════════════════════════════════════════════════════
# STEP 3: Scrape external URLs for emails
# ═══════════════════════════════════════════════════════════════════
def enrich_with_url_emails(profiles):
    """Scrape external URLs for each profile."""
    to_scrape = [p for p in profiles if p.get("external_url")]
    print(f"\n  Scraping {len(to_scrape)} external URLs...\n")

    for i, p in enumerate(to_scrape):
        url = p["external_url"]
        print(f"  [{i+1}/{len(to_scrape)}] @{p['username']} → {url[:50]}...", end=" ", flush=True)
        try:
            emails = scrape_url_for_emails(url)
            p["url_emails"] = emails
            if emails:
                print(f"✓ {emails}")
            else:
                print("no email")
        except Exception as e:
            print(f"error: {str(e)[:40]}")
        time.sleep(random.uniform(1, 2))

    return profiles


# ═══════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════
def main():
    print("╔════════════════════════════════════════════════════════════╗")
    print("║   INSTAGRAM DISCOVERY — BROWSER AUTOMATION (PLAYWRIGHT)  ║")
    print(f"║   {datetime.now().strftime('%Y-%m-%d %H:%M:%S'):<53}║")
    print("╚════════════════════════════════════════════════════════════╝")

    base = os.path.dirname(os.path.abspath(__file__))

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/120.0.0.0 Safari/537.36",
            viewport={"width": 1280, "height": 720},
            locale="en-US",
        )
        page = context.new_page()

        # ── Step 1: Discover usernames via Google ──
        print("\n" + "="*60)
        print("  STEP 1: Google Discovery (browser-rendered)")
        print("="*60)

        usernames = google_discover_usernames(page)
        print(f"\n  Total discovered: {len(usernames)} unique usernames")

        if not usernames:
            print("\n  No usernames found. Exiting.")
            browser.close()
            return

        # ── Step 2: Fetch IG profiles in browser ──
        print("\n" + "="*60)
        print("  STEP 2: Fetch Instagram Profiles (browser)")
        print("="*60 + "\n")

        profiles = fetch_ig_profiles_browser(page, usernames, max_count=60)
        print(f"\n  Fetched {len(profiles)} profiles")

        browser.close()

    # ── Step 3: Scrape external URLs ──
    print("\n" + "="*60)
    print("  STEP 3: Scrape External URLs for Emails")
    print("="*60)

    profiles = enrich_with_url_emails(profiles)

    # ── Results ──
    print("\n" + "="*60)
    print("  FINAL RESULTS")
    print("="*60)

    total = len(profiles)
    with_email = 0

    for p in profiles:
        all_emails = set(p.get("emails_from_bio", []) + p.get("url_emails", []))
        if all_emails:
            with_email += 1
            print(f"  ✅ @{p['username']:<25} {p['followers']:>8,} flw  →  {list(all_emails)}")

    without = total - with_email
    print(f"\n  Total profiles:    {total}")
    print(f"  With email:        {with_email} ({with_email*100//max(total,1)}% hit rate)")
    print(f"  Without email:     {without}")

    # ── Save CSV ──
    csv_path = os.path.join(base, "discovered_leads.csv")
    rows = []
    for p in profiles:
        all_emails = set(p.get("emails_from_bio", []) + p.get("url_emails", []))
        sources = []
        if p.get("emails_from_bio"): sources.append("bio")
        if p.get("url_emails"): sources.append("url")
        for email in all_emails:
            rows.append({
                "username": p["username"],
                "full_name": p.get("full_name", ""),
                "email": email,
                "source": "+".join(sources),
                "followers": p.get("followers", 0),
                "bio": (p.get("bio") or "")[:150].replace('\n', ' '),
                "external_url": p.get("external_url", ""),
                "is_business": p.get("is_business", False),
                "category": p.get("business_category", ""),
            })

    if rows:
        with open(csv_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=rows[0].keys())
            writer.writeheader()
            writer.writerows(rows)

    # Also save all profiles (with and without emails)
    all_csv = os.path.join(base, "all_discovered_profiles.csv")
    with open(all_csv, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=[
            "username", "full_name", "followers", "is_business", "category",
            "bio", "external_url", "has_email"
        ])
        writer.writeheader()
        for p in profiles:
            all_emails = set(p.get("emails_from_bio", []) + p.get("url_emails", []))
            writer.writerow({
                "username": p["username"],
                "full_name": p.get("full_name", ""),
                "followers": p.get("followers", 0),
                "is_business": p.get("is_business", False),
                "category": p.get("business_category", ""),
                "bio": (p.get("bio") or "")[:150].replace('\n', ' '),
                "external_url": p.get("external_url", ""),
                "has_email": bool(all_emails),
            })

    json_path = os.path.join(base, "discovered_leads.json")
    with open(json_path, 'w') as f:
        json.dump(profiles, f, indent=2, default=str)

    print(f"\n  Leads CSV:     {csv_path} ({len(rows)} email rows)")
    print(f"  All profiles:  {all_csv} ({len(profiles)} profiles)")
    print(f"  JSON:          {json_path}")


if __name__ == "__main__":
    main()
