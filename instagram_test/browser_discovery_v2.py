#!/usr/bin/env python3
"""
Instagram Discovery v2 — Multiple Search Engines + Instagram Search
=====================================================================
Strategy A: DuckDuckGo (doesn't block headless browsers)
Strategy B: Instagram logged-in search via browser (search hashtags/keywords)
Strategy C: Bing search (less aggressive than Google)

Then: fetch profiles + scrape URLs for emails.
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
from urllib.parse import urlparse, quote_plus

from playwright.sync_api import sync_playwright

# ─── Load .env ───
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

# ─── Email extraction ───
EMAIL_RE = re.compile(r'[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}')
JUNK_DOMAINS = {
    'sentry.io', 'ingest.sentry.io', 'w3.org', 'schema.org', 'example.com',
    'domain.com', 'wixpress.com', 'cloudflare.com', 'mailchimp.com',
    'googleapis.com', 'facebook.com', 'instagram.com', 'fbcdn.net',
    'apple.com', 'google.com', 'youtube.com', 'twitter.com', 'x.com',
    'tiktok.com', 'spotify.com', 'onetrust.com', 'pinterest.com', 'linkedin.com',
}

def extract_emails(text):
    if not text: return []
    found = EMAIL_RE.findall(text.lower())
    return list(set(e for e in found
                    if e.split('@')[1] not in JUNK_DOMAINS
                    and not any(k in e for k in ['sentry','noreply','unsubscribe','webpack','placeholder'])
                    and not e.endswith(('.png','.jpg','.gif','.svg','.css','.js'))
                    and len(e.split('@')[1]) >= 5))

SSL_CTX = ssl.create_default_context()
SSL_CTX.check_hostname = False
SSL_CTX.verify_mode = ssl.CERT_NONE

def fetch_simple(url, timeout=8):
    try:
        req = urllib.request.Request(url, headers={
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
        })
        return urllib.request.urlopen(req, timeout=timeout, context=SSL_CTX).read().decode('utf-8', errors='ignore')
    except: return None

def scrape_url_for_emails(url):
    all_emails = set()
    html = fetch_simple(url)
    if html:
        all_emails.update(extract_emails(html))
        linktree = ['linktr.ee','komi.io','beacons.ai','stan.store','hoo.be']
        if any(d in url.lower() for d in linktree):
            for link in re.findall(r'href="(https?://[^"]+)"', html)[:4]:
                if any(k in link.lower() for k in ['contact','about','email','book','work','collab']):
                    sub = fetch_simple(link, 5)
                    if sub: all_emails.update(extract_emails(sub))
                    time.sleep(0.5)
        else:
            parsed = urlparse(url if url.startswith('http') else 'https://' + url)
            base = f"{parsed.scheme}://{parsed.netloc}"
            for path in ['/contact','/about']:
                if len(all_emails) >= 3: break
                sub = fetch_simple(base + path, 5)
                if sub: all_emails.update(extract_emails(sub))
                time.sleep(0.3)
    return list(all_emails)

IG_USER_RE = re.compile(r'instagram\.com/([a-zA-Z0-9_.]{2,30})')
SKIP = {'explore','p','reel','reels','stories','accounts','about','legal',
        'developer','privacy','terms','help','directory','web','api','tags',
        'locations','nametag','direct','lite','emails','session','challenge',
        'signup','login','404','favicon'}

def clean_usernames(raw):
    return list(set(u for u in raw if u.lower() not in SKIP
                    and len(u) > 2 and not u.startswith('.') and not u.isdigit()))


# ═══════════════════════════════════════════════════════════════════
# STRATEGY A: DuckDuckGo search
# ═══════════════════════════════════════════════════════════════════
def strategy_duckduckgo(page):
    print("\n" + "="*60)
    print("  STRATEGY A: DuckDuckGo Search")
    print("="*60)

    QUERIES = [
        'instagram.com personal trainer email contact',
        'instagram.com fitness coach online coaching inquiries',
        'instagram.com UGC creator fitness gym collab',
        'instagram.com student athlete fitness NCAA',
        'instagram.com NASM certified trainer online',
        'instagram.com yoga instructor private sessions contact',
        'instagram.com crossfit coach affiliate gym',
        'instagram.com nutrition coach macro meal plan',
    ]

    all_users = set()
    for i, q in enumerate(QUERIES):
        print(f"\n  [{i+1}/{len(QUERIES)}] {q[:60]}...")
        try:
            page.goto(f"https://duckduckgo.com/?q={quote_plus(q)}", timeout=15000)
            time.sleep(random.uniform(2, 4))
            content = page.content()
            users = clean_usernames(IG_USER_RE.findall(content))
            new = [u for u in users if u not in all_users]
            all_users.update(new)
            print(f"    Found {len(users)} handles, {len(new)} new → total: {len(all_users)}")
            if new: print(f"    Sample: {new[:8]}")
        except Exception as e:
            print(f"    Error: {str(e)[:50]}")
        time.sleep(random.uniform(2, 4))

    print(f"\n  DuckDuckGo total: {len(all_users)}")
    return list(all_users)


# ═══════════════════════════════════════════════════════════════════
# STRATEGY B: Instagram search (logged in via browser)
# ═══════════════════════════════════════════════════════════════════
def strategy_instagram_search(page):
    print("\n" + "="*60)
    print("  STRATEGY B: Instagram Search (logged-in browser)")
    print("="*60)

    ig_user = ENV.get('IG_USERNAME', '')
    ig_pass = ENV.get('IG_PASSWORD', '')

    # Login to Instagram
    print(f"\n  Logging in as @{ig_user}...")
    try:
        page.goto("https://www.instagram.com/accounts/login/", timeout=15000)
        time.sleep(3)

        # Handle cookie consent if it appears
        try:
            page.click("text=Allow all cookies", timeout=3000)
            time.sleep(1)
        except: pass
        try:
            page.click("text=Accept", timeout=2000)
            time.sleep(1)
        except: pass

        # Fill login form
        page.fill('input[name="username"]', ig_user)
        time.sleep(0.5)
        page.fill('input[name="password"]', ig_pass)
        time.sleep(0.5)
        page.click('button[type="submit"]')
        time.sleep(5)

        # Check if logged in
        if 'login' in page.url.lower():
            print("  Login might have failed, checking...")
            time.sleep(3)

        # Dismiss "Save Login Info" popup if it appears
        try:
            page.click("text=Not Now", timeout=3000)
        except: pass
        try:
            page.click("text=Not Now", timeout=3000)
        except: pass

        print(f"  Current URL: {page.url}")

    except Exception as e:
        print(f"  Login error: {str(e)[:60]}")
        return []

    time.sleep(2)

    # Now search for fitness-related accounts
    SEARCH_TERMS = [
        "fitness trainer",
        "personal trainer",
        "gym coach",
        "fitness UGC",
        "student athlete",
        "yoga teacher",
        "crossfit coach",
        "nutrition coach",
    ]

    all_users = set()

    for i, term in enumerate(SEARCH_TERMS):
        print(f"\n  [{i+1}/{len(SEARCH_TERMS)}] Searching: '{term}'...")
        try:
            # Use Instagram's search URL
            page.goto(f"https://www.instagram.com/explore/search/keyword/?q={quote_plus(term)}", timeout=15000)
            time.sleep(random.uniform(3, 5))

            content = page.content()

            # Extract usernames from search results page
            users = clean_usernames(IG_USER_RE.findall(content))

            # Also try to extract from the page's JSON data
            json_matches = re.findall(r'"username"\s*:\s*"([a-zA-Z0-9_.]{2,30})"', content)
            users.extend(clean_usernames(json_matches))

            users = list(set(users))
            new = [u for u in users if u not in all_users]
            all_users.update(new)
            print(f"    Found {len(users)} users, {len(new)} new → total: {len(all_users)}")
            if new: print(f"    Sample: {new[:8]}")

        except Exception as e:
            print(f"    Error: {str(e)[:50]}")

        time.sleep(random.uniform(3, 6))

    print(f"\n  Instagram search total: {len(all_users)}")
    return list(all_users)


# ═══════════════════════════════════════════════════════════════════
# STRATEGY C: Bing search
# ═══════════════════════════════════════════════════════════════════
def strategy_bing(page):
    print("\n" + "="*60)
    print("  STRATEGY C: Bing Search")
    print("="*60)

    QUERIES = [
        'site:instagram.com "personal trainer" email contact',
        'site:instagram.com "fitness coach" "DM for" collabs',
        'site:instagram.com "student athlete" fitness',
        'site:instagram.com "certified trainer" NASM ACE',
        'site:instagram.com "gym owner" small business',
        'site:instagram.com "nutrition coach" macros',
        'site:instagram.com "yoga instructor" bookings',
        'site:instagram.com "crossfit coach" programming',
    ]

    all_users = set()
    for i, q in enumerate(QUERIES):
        print(f"\n  [{i+1}/{len(QUERIES)}] {q[:60]}...")
        try:
            page.goto(f"https://www.bing.com/search?q={quote_plus(q)}&count=30", timeout=15000)
            time.sleep(random.uniform(2, 4))
            content = page.content()
            users = clean_usernames(IG_USER_RE.findall(content))
            new = [u for u in users if u not in all_users]
            all_users.update(new)
            print(f"    Found {len(users)} handles, {len(new)} new → total: {len(all_users)}")
            if new: print(f"    Sample: {new[:8]}")
        except Exception as e:
            print(f"    Error: {str(e)[:50]}")
        time.sleep(random.uniform(2, 4))

    print(f"\n  Bing total: {len(all_users)}")
    return list(all_users)


# ═══════════════════════════════════════════════════════════════════
# ENRICHMENT: Profile fetch + email extraction
# ═══════════════════════════════════════════════════════════════════
def fetch_ig_profile_api(username):
    try:
        url = f"https://www.instagram.com/api/v1/users/web_profile_info/?username={username}"
        headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
            "X-IG-App-ID": "936619743392459", "Accept": "*/*",
        }
        req = urllib.request.Request(url, headers=headers)
        resp = urllib.request.urlopen(req, timeout=10, context=SSL_CTX)
        data = json.loads(resp.read().decode('utf-8'))
        user = data.get("data", {}).get("user", {})
        if not user: return None
        return {
            "username": username,
            "full_name": user.get("full_name", ""),
            "bio": user.get("biography", ""),
            "external_url": user.get("external_url", ""),
            "followers": user.get("edge_followed_by", {}).get("count", 0),
            "is_business": user.get("is_business_account", False),
            "business_category": user.get("category_name", ""),
        }
    except: return None


def enrich_and_extract(usernames, max_count=80):
    print(f"\n  Enriching up to {min(len(usernames), max_count)} profiles...\n")
    profiles = []
    usernames = usernames[:max_count]

    for i, u in enumerate(usernames):
        print(f"  [{i+1}/{len(usernames)}] @{u}...", end=" ", flush=True)
        p = fetch_ig_profile_api(u)
        if not p:
            print("✗ not found")
            time.sleep(0.5)
            continue

        p["emails_from_bio"] = extract_emails(p["bio"])
        p["url_emails"] = []
        if p.get("external_url"):
            try:
                p["url_emails"] = scrape_url_for_emails(p["external_url"])
            except: pass

        all_e = set(p["emails_from_bio"] + p["url_emails"])
        if all_e:
            print(f"✓ {p['followers']:,} flw | EMAILS: {list(all_e)}")
        else:
            print(f"~ {p['followers']:,} flw | url={(p['external_url'] or 'none')[:35]}")

        profiles.append(p)
        time.sleep(random.uniform(2, 3.5))

    return profiles


# ═══════════════════════════════════════════════════════════════════
def main():
    print("╔════════════════════════════════════════════════════════════╗")
    print("║   INSTAGRAM DISCOVERY v2 — MULTI-ENGINE BROWSER SEARCH   ║")
    print(f"║   {datetime.now().strftime('%Y-%m-%d %H:%M:%S'):<53}║")
    print("╚════════════════════════════════════════════════════════════╝")

    base = os.path.dirname(os.path.abspath(__file__))

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        ctx = browser.new_context(
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            viewport={"width": 1280, "height": 800}, locale="en-US",
        )
        page = ctx.new_page()

        # Run all 3 search strategies
        ddg_users = strategy_duckduckgo(page)
        bing_users = strategy_bing(page)
        ig_users = strategy_instagram_search(page)

        browser.close()

    # Combine and deduplicate
    all_users = list(dict.fromkeys(ddg_users + bing_users + ig_users))

    print("\n" + "="*60)
    print("  DISCOVERY SUMMARY")
    print("="*60)
    print(f"  DuckDuckGo:       {len(ddg_users)} usernames")
    print(f"  Bing:             {len(bing_users)} usernames")
    print(f"  Instagram search: {len(ig_users)} usernames")
    print(f"  Combined unique:  {len(all_users)} usernames")

    if not all_users:
        print("\n  No usernames discovered. Exiting.")
        return

    # Enrich
    print("\n" + "="*60)
    print("  ENRICHMENT: Fetch profiles + extract emails")
    print("="*60)

    profiles = enrich_and_extract(all_users)

    # Results
    print("\n" + "="*60)
    print("  FINAL RESULTS")
    print("="*60)

    with_email = 0
    for p in profiles:
        all_e = set(p.get("emails_from_bio", []) + p.get("url_emails", []))
        if all_e:
            with_email += 1
            print(f"  ✅ @{p['username']:<25} {p['followers']:>8,} flw  →  {list(all_e)}")

    total = len(profiles)
    print(f"\n  Profiles fetched:  {total}")
    print(f"  With email:        {with_email} ({with_email*100//max(total,1)}%)")

    # Save
    csv_path = os.path.join(base, "discovered_leads.csv")
    rows = []
    for p in profiles:
        for e in set(p.get("emails_from_bio", []) + p.get("url_emails", [])):
            rows.append({
                "username": p["username"], "full_name": p.get("full_name",""),
                "email": e, "followers": p.get("followers",0),
                "bio": (p.get("bio",""))[:150].replace('\n',' '),
                "external_url": p.get("external_url",""),
                "is_business": p.get("is_business",False),
                "category": p.get("business_category",""),
            })
    if rows:
        with open(csv_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=rows[0].keys())
            writer.writeheader()
            writer.writerows(rows)
        print(f"\n  CSV: {csv_path} ({len(rows)} rows)")

    json_path = os.path.join(base, "discovered_leads.json")
    with open(json_path, 'w') as f:
        json.dump({"discovery": {"ddg": ddg_users, "bing": bing_users, "ig": ig_users},
                   "profiles": profiles}, f, indent=2, default=str)
    print(f"  JSON: {json_path}")


if __name__ == "__main__":
    main()
