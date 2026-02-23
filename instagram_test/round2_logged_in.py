#!/usr/bin/env python3
"""
Instagram Scraper — Round 2: Logged-In Test
=============================================
Tests Instaloader with a logged-in session to unlock business_email fields.
Also scrapes external URLs (Linktree, websites) as a second pass.

Safety measures:
  - 4-6 second random delays between profile fetches
  - Only 5 profiles per run (small footprint)
  - Mimics normal browsing behavior
  - Saves session so we don't re-login every time
"""

import os
import re
import sys
import time
import json
import random
import urllib.request
import urllib.error
import ssl
from datetime import datetime

# ─── Load credentials from .env ─────────────────────────────────
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
IG_USERNAME = ENV.get('IG_USERNAME', '')
IG_PASSWORD = ENV.get('IG_PASSWORD', '')

if not IG_USERNAME or not IG_PASSWORD:
    print("ERROR: IG_USERNAME and IG_PASSWORD must be set in .env")
    sys.exit(1)

# ─── Test Accounts (5 for safety) ───────────────────────────────
# Picking accounts most likely to have business emails set up
TEST_ACCOUNTS = [
    "kayla_itsines",        # Huge fitness brand, definitely has business email
    "whitneyysimmons",      # Fitness influencer, had email in bio
    "drjohnrusin",          # Fitness business, is_business=True
    "soheefit",             # Coach/entrepreneur, is_business=True  
    "mindpumpmedia",        # Fitness podcast/brand, is_business=True
]

# ─── Email Regex ─────────────────────────────────────────────────
EMAIL_PATTERN = re.compile(
    r'[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}'
)

JUNK_EMAILS = {
    'email@example.com', 'your@email.com', 'name@domain.com',
    'filler@godaddy.com', 'contact@mysite.com', 'user@domain.com',
    'emailhere@email.com', 'johnsmith@gmail.com',
}
JUNK_DOMAINS = {
    'sentry.io', 'w3.org', 'schema.org', 'example.com',
    'wixpress.com', 'cloudflare.com', 'ingest.us.sentry.io',
    'ingest.sentry.io', 'domain.com', 'mysite.com',
}

def extract_emails_from_text(text):
    if not text:
        return []
    emails = EMAIL_PATTERN.findall(text.lower())
    cleaned = []
    for e in emails:
        domain = e.split('@')[1]
        if e not in JUNK_EMAILS and domain not in JUNK_DOMAINS:
            cleaned.append(e)
    return list(set(cleaned))


def safe_sleep(min_sec=4, max_sec=7):
    """Random sleep to mimic human browsing."""
    delay = random.uniform(min_sec, max_sec)
    print(f"    [sleeping {delay:.1f}s]")
    time.sleep(delay)


# ═══════════════════════════════════════════════════════════════════
# STEP 1: Instaloader with logged-in session
# ═══════════════════════════════════════════════════════════════════
def fetch_profiles_logged_in(accounts):
    """Fetch profiles using a logged-in Instaloader session."""
    import instaloader
    
    print("\n" + "="*60)
    print("  STEP 1: Instaloader (LOGGED IN)")
    print("="*60)
    
    L = instaloader.Instaloader()
    
    # Try to load saved session first (avoids repeated logins)
    session_file = os.path.join(os.path.dirname(__file__), f"session-{IG_USERNAME}")
    
    try:
        L.load_session_from_file(IG_USERNAME, session_file)
        print(f"  Loaded saved session for @{IG_USERNAME}")
    except (FileNotFoundError, instaloader.exceptions.ConnectionException):
        print(f"  Logging in as @{IG_USERNAME}...")
        try:
            L.login(IG_USERNAME, IG_PASSWORD)
            L.save_session_to_file(session_file)
            print(f"  Login successful! Session saved.")
        except instaloader.exceptions.TwoFactorAuthRequiredException:
            print("  ERROR: Account requires 2FA. Use an account without 2FA.")
            return []
        except instaloader.exceptions.BadCredentialsException:
            print("  ERROR: Wrong username or password.")
            return []
        except instaloader.exceptions.ConnectionException as e:
            print(f"  ERROR: Login failed - {e}")
            return []
    
    safe_sleep(2, 4)  # Wait after login before fetching
    
    results = []
    
    for i, username in enumerate(accounts):
        print(f"\n  [{i+1}/{len(accounts)}] Fetching @{username}...")
        
        entry = {
            "username": username,
            "full_name": None,
            "bio": None,
            "emails_from_bio": [],
            "business_email": None,
            "business_phone": None,
            "business_category": None,
            "external_url": None,
            "followers": None,
            "following": None,
            "is_business": None,
            "is_verified": None,
            "error": None,
        }
        
        try:
            profile = instaloader.Profile.from_username(L.context, username)
            
            entry["full_name"] = profile.full_name
            entry["bio"] = profile.biography
            entry["external_url"] = profile.external_url
            entry["followers"] = profile.followers
            entry["following"] = profile.followees
            entry["is_business"] = profile.is_business_account
            entry["is_verified"] = profile.is_verified
            
            # These fields are only available when logged in
            entry["business_email"] = getattr(profile, 'business_email', None) or None
            entry["business_phone"] = getattr(profile, 'business_phone_number', None) or None
            entry["business_category"] = getattr(profile, 'business_category_name', None) or None
            
            # Extract emails from bio text
            entry["emails_from_bio"] = extract_emails_from_text(profile.biography)
            
            # Compile all found emails
            all_emails = list(entry["emails_from_bio"])
            if entry["business_email"] and entry["business_email"] not in all_emails:
                all_emails.append(entry["business_email"])
            
            print(f"    Name:           {profile.full_name}")
            print(f"    Bio:            {profile.biography[:80]}{'...' if len(profile.biography) > 80 else ''}")
            print(f"    Business email: {entry['business_email'] or '(hidden/empty)'}")
            print(f"    Business phone: {entry['business_phone'] or '(hidden/empty)'}")
            print(f"    Biz category:   {entry['business_category'] or '(none)'}")
            print(f"    Bio emails:     {entry['emails_from_bio'] or '(none)'}")
            print(f"    External URL:   {entry['external_url'] or '(none)'}")
            print(f"    Followers:      {profile.followers:,}")
            print(f"    Is business:    {profile.is_business_account}")
            print(f"    ALL EMAILS:     {all_emails or '!! NONE !!'}")
            
        except Exception as e:
            entry["error"] = str(e)[:150]
            print(f"    ERROR: {str(e)[:100]}")
        
        results.append(entry)
        
        if i < len(accounts) - 1:
            safe_sleep(4, 7)  # Human-like delay between profiles
    
    return results


# ═══════════════════════════════════════════════════════════════════
# STEP 2: Scrape external URLs (Linktree, websites) for emails
# ═══════════════════════════════════════════════════════════════════
def scrape_external_urls(profiles):
    """For profiles with external_url, scrape that URL for emails."""
    
    print("\n" + "="*60)
    print("  STEP 2: External URL / Linktree Scraping")
    print("="*60)
    
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    
    HEADERS = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                       "AppleWebKit/537.36 (KHTML, like Gecko) "
                       "Chrome/120.0.0.0 Safari/537.36",
    }
    
    for profile in profiles:
        url = profile.get("external_url")
        if not url:
            continue
        
        username = profile["username"]
        print(f"\n  @{username} → Scraping: {url}")
        
        try:
            req = urllib.request.Request(url, headers=HEADERS)
            resp = urllib.request.urlopen(req, timeout=10, context=ctx)
            html = resp.read().decode('utf-8', errors='ignore')
            
            # Extract emails from the page
            page_emails = extract_emails_from_text(html)
            
            # For Linktree pages, also look for links to other pages
            # that might contain contact info
            link_pattern = re.compile(r'href="(https?://[^"]+)"')
            links = link_pattern.findall(html)
            
            # Filter for contact/about pages from their domain
            contact_links = []
            for link in links:
                lower = link.lower()
                if any(kw in lower for kw in ['contact', 'about', 'email', 'booking', 'work-with']):
                    contact_links.append(link)
            
            if page_emails:
                print(f"    FOUND emails: {page_emails}")
                # Add to profile
                if "url_emails" not in profile:
                    profile["url_emails"] = []
                profile["url_emails"].extend(page_emails)
            else:
                print(f"    No emails on page. Found {len(contact_links)} contact-like links:")
                for cl in contact_links[:5]:
                    print(f"      → {cl}")
                
                # Try scraping contact links (just the first 2)
                for contact_url in contact_links[:2]:
                    try:
                        print(f"    Scraping sub-link: {contact_url}")
                        req2 = urllib.request.Request(contact_url, headers=HEADERS)
                        resp2 = urllib.request.urlopen(req2, timeout=10, context=ctx)
                        html2 = resp2.read().decode('utf-8', errors='ignore')
                        sub_emails = extract_emails_from_text(html2)
                        if sub_emails:
                            print(f"    FOUND emails from sub-link: {sub_emails}")
                            if "url_emails" not in profile:
                                profile["url_emails"] = []
                            profile["url_emails"].extend(sub_emails)
                        time.sleep(1)
                    except Exception as e:
                        print(f"    Sub-link error: {str(e)[:60]}")
            
        except Exception as e:
            print(f"    ERROR scraping URL: {str(e)[:80]}")
        
        time.sleep(1.5)
    
    return profiles


# ═══════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════
def main():
    print("╔════════════════════════════════════════════════════════════╗")
    print("║   INSTAGRAM SCRAPER — ROUND 2 (LOGGED IN + URL SCRAPE)   ║")
    print(f"║   {datetime.now().strftime('%Y-%m-%d %H:%M:%S'):<53}║")
    print("╚════════════════════════════════════════════════════════════╝")
    
    # Step 1: Fetch profiles with logged-in session
    profiles = fetch_profiles_logged_in(TEST_ACCOUNTS)
    
    if not profiles:
        print("\nLogin failed. Cannot continue.")
        return
    
    # Step 2: Scrape external URLs for emails
    profiles = scrape_external_urls(profiles)
    
    # ─── Final Summary ───────────────────────────────────────────
    print("\n" + "="*60)
    print("  FINAL RESULTS")
    print("="*60)
    
    total_with_email = 0
    
    for p in profiles:
        username = p["username"]
        all_emails = set()
        
        # Collect from all sources
        if p.get("business_email"):
            all_emails.add(p["business_email"])
        for e in p.get("emails_from_bio", []):
            all_emails.add(e)
        for e in p.get("url_emails", []):
            all_emails.add(e)
        
        source = []
        if p.get("business_email"): source.append("biz_field")
        if p.get("emails_from_bio"): source.append("bio")
        if p.get("url_emails"): source.append("url_scrape")
        
        if all_emails:
            total_with_email += 1
            print(f"  ✅ @{username:<20} → {', '.join(all_emails)}")
            print(f"     Sources: {', '.join(source)}")
        else:
            print(f"  ❌ @{username:<20} → NO EMAIL FOUND")
            if p.get("error"):
                print(f"     Error: {p['error'][:60]}")
    
    print(f"\n  TOTAL: {total_with_email}/{len(profiles)} profiles yielded emails")
    
    # Compare with Round 1
    print(f"\n  Round 1 (anonymous): 1/10 emails (10% hit rate)")
    print(f"  Round 2 (logged in): {total_with_email}/{len(profiles)} emails "
          f"({total_with_email*100//max(len(profiles),1)}% hit rate)")
    
    # Save results
    output = {
        "test_date": datetime.now().isoformat(),
        "method": "instaloader_logged_in + url_scraping",
        "ig_account": IG_USERNAME,
        "accounts_tested": TEST_ACCOUNTS,
        "profiles": profiles,
        "total_with_email": total_with_email,
    }
    
    results_path = os.path.join(os.path.dirname(__file__), "round2_results.json")
    with open(results_path, "w") as f:
        json.dump(output, f, indent=2, default=str)
    
    print(f"\n  Results saved to {results_path}")


if __name__ == "__main__":
    main()
