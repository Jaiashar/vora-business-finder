#!/usr/bin/env python3
"""
Instagram 100-Profile Scraper (v3 - Fast)
==========================================
Uses Instagram's public web API directly (no Instaloader retries).
Fetches profiles fast, scrapes external URLs for emails.
NO emailing — just data collection.
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
from urllib.parse import urlparse

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

# ─── 130 Target Accounts (expect ~100 to resolve) ───────────────
# Only well-known accounts with high confidence the username is real.
TARGET_ACCOUNTS = [
    # ── Big Fitness Influencers / Trainers ──
    "kayla_itsines",
    "whitneyysimmons",
    "soheefit",
    "mindpumpmedia",
    "drjohnrusin",
    "bretcontreras1",
    "stephanie_buttermore",
    "blogilates",
    "adrienelouise",
    "natacha.oceane",
    "davidlaidofficial",
    "steficohen",
    "kinoyoga",
    "gymshark",
    "squatuniversity",
    "theprehabguys",
    "athleanx",
    "toneitup",
    "nikachenelle",
    "laurengiraldo",
    "chfrfit",
    "brittanyperille",
    "paulinarobertina",
    "qimmahrusso",
    "anaborellavitoria",
    "anllela_sagra",
    "maborelissapark",
    "fitfatale",
    "simeonpanda",
    "joesthetics",
    "cbum",
    "mattdoesfitness",
    "gregdoucette",
    "mountaindog1",
    "bradleymartynnaboreal",
    "mikeaborelahern",
    "laaborelazaryangelovjr",
    "robaborealippensgfit",
    "sadiaborealee",
    "demi_bagby",

    # ── Yoga / Pilates / Wellness ──
    "sjanaelise",
    "beachyogagirl",
    "patrickbeach",
    "laurasykora",
    "equinox",
    "pilatesanytime",
    "clubpilates",
    "yogawithtim",
    "yogajournal",
    "alo",
    "lululemon",
    "manduka",
    "gaiam",
    "corepoweryoga",
    "barrys",
    "orangetheory",
    "f45_training",
    "purebarre",

    # ── CrossFit / Strength ──
    "richfroning",
    "crossfitgames",
    "katrintanja",
    "tiaborealiaclair",
    "nohlsen",
    "brooke_wells",
    "boxrox",
    "barbellmedicine",
    "westside_barbell",
    "markaborelippoldsort",
    "juggernauttraining",
    "startingstrength",
    "strongfirst",

    # ── Nutrition / Wellness / Health ──
    "drmarkhyman",
    "mindbodygreen",
    "thefitnesschef_",
    "macrobarista",
    "meowmeix",
    "workweeklunch",
    "meals_by_amaboreal",
    "drjoshaxe",
    "wellnessmama",
    "thebalancedblonde",
    "healthmagazine",
    "eatingwell",
    "womenshealthmag",
    "maborelenshealthmag",
    "selfmagazine",
    "raborelunnersworld",
    "shaborepemagazine",

    # ── University / College Athletics ──
    "stanfordathletics",
    "michiganathletics",
    "floridagators",
    "texaslonghorns",
    "ohiostathletics",
    "pennstateathaboreletics",
    "dukebluedevils",
    "ucaborelabruins",
    "uaborelsctraborelojans",
    "pac12",
    "ncaa",
    "thebigtaborelen",
    "secnetwork",

    # ── Student Athletes / Sports ──
    "oliviadunne",
    "sunisalee_",
    "caaboreleb_dressel",
    "kaboreleighmcbean",
    "a_seaboreral",
    "ncaaswimming",
    "ncaatraborelackandfield",
    "espncollege",

    # ── Sports Performance / Coaching ──
    "exos",
    "nsaborela",
    "acsm",
    "nasm",
    "acefitness",
    "issaonline",
    "functionalpatterns",
    "movaborelu",
    "frc.mobility",
    "smashwerx",
    "mobilitywod",
    "draaronhorschig",
    "drmikemillner",

    # ── More fitness / wellness influencers ──
    "heidipowell",
    "mytrainercarmen",
    "mrandmrsmuscle",
    "robinnyogi",
    "sweatandtell",
    "healthyfit",
    "trainerjosh",
    "bodybybarbiefit",
]

# Deduplicate
TARGET_ACCOUNTS = list(dict.fromkeys(TARGET_ACCOUNTS))

# ─── Email extraction ────────────────────────────────────────────
EMAIL_RE = re.compile(r'[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}')

JUNK_EMAILS = {
    'email@example.com', 'your@email.com', 'name@domain.com',
    'filler@godaddy.com', 'contact@mysite.com', 'user@domain.com',
    'emailhere@email.com', 'test@test.com', 'info@example.com',
}

JUNK_DOMAINS = {
    'sentry.io', 'ingest.sentry.io', 'ingest.us.sentry.io',
    'w3.org', 'schema.org', 'example.com', 'domain.com', 'mysite.com',
    'wixpress.com', 'cloudflare.com', 'mailchimp.com', 'googleapis.com',
    'facebook.com', 'instagram.com', 'fbcdn.net', 'cdninstagram.com',
    'apple.com', 'google.com', 'gstatic.com', 'youtube.com',
    'twitter.com', 'x.com', 'tiktok.com', 'spotify.com',
    'onetrust.com', 'privacyportal-cdn.onetrust.com',
    'pinterest.com', 'linkedin.com',
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


# ─── Web helpers ─────────────────────────────────────────────────
SSL_CTX = ssl.create_default_context()
SSL_CTX.check_hostname = False
SSL_CTX.verify_mode = ssl.CERT_NONE

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) "
                   "Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
}

IG_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) "
                   "Chrome/120.0.0.0 Safari/537.36",
    "X-IG-App-ID": "936619743392459",
    "Accept": "*/*",
    "Accept-Language": "en-US,en;q=0.9",
}


def fetch_page(url, timeout=8):
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
    """Scrape a URL and its sub-pages for emails."""
    all_emails = set()

    html = fetch_page(url)
    if html:
        for e in extract_emails(html):
            all_emails.add(e)

    linktree_domains = ['linktr.ee', 'komi.io', 'beacons.ai', 'link.bio',
                        'campsite.bio', 'stan.store', 'hoo.be', 'snipfeed.co']
    if html and any(d in url.lower() for d in linktree_domains):
        links = re.findall(r'href="(https?://[^"]+)"', html)
        for link in links[:5]:
            if any(kw in link.lower() for kw in ['contact', 'about', 'email', 'book', 'work', 'collab', 'inquiry']):
                sub = fetch_page(link, timeout=5)
                if sub:
                    for e in extract_emails(sub):
                        all_emails.add(e)
                time.sleep(0.5)
    elif html:
        parsed = urlparse(url if url.startswith('http') else 'https://' + url)
        base = f"{parsed.scheme}://{parsed.netloc}"
        for path in ['/contact', '/contact-us', '/about', '/about-us']:
            if len(all_emails) >= 3:
                break
            sub = fetch_page(base + path, timeout=5)
            if sub:
                for e in extract_emails(sub):
                    all_emails.add(e)
            time.sleep(0.3)

    return list(all_emails)


# ═══════════════════════════════════════════════════════════════════
# PHASE 1: Fetch profiles using Instagram's public web API
# ═══════════════════════════════════════════════════════════════════
def fetch_ig_profile(username):
    """Fetch a single Instagram profile via the public web API.
    Returns profile dict or None. No retries — instant pass/fail."""
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
            "following": user.get("edge_follow", {}).get("count", 0),
            "is_business": user.get("is_business_account", False),
            "is_verified": user.get("is_verified", False),
            "business_category": user.get("category_name", ""),
            "business_email": user.get("business_email", ""),
            "profile_pic": user.get("profile_pic_url_hd", ""),
        }
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return "not_found"
        elif e.code == 429:
            return "rate_limited"
        else:
            return f"http_{e.code}"
    except Exception as e:
        return f"error: {str(e)[:60]}"


def fetch_all_profiles(accounts, progress_file):
    """Fetch all profiles with fast skip on 404s."""

    # Load progress
    existing = {}
    if os.path.exists(progress_file):
        with open(progress_file) as f:
            existing = json.load(f)

    profiles = []
    skipped = 0
    found = 0
    not_found = 0
    rate_limits = 0
    errors = 0

    for i, username in enumerate(accounts):
        # Resume from progress
        if username in existing:
            profiles.append(existing[username])
            if not existing[username].get("error"):
                found += 1
            skipped += 1
            continue

        print(f"  [{i+1}/{len(accounts)}] @{username}...", end=" ", flush=True)

        result = fetch_ig_profile(username)

        if result == "not_found":
            entry = {"username": username, "error": "not_found"}
            profiles.append(entry)
            existing[username] = entry
            not_found += 1
            print("✗ not found")
            time.sleep(0.5)  # Barely any delay for 404s
            continue

        elif result == "rate_limited":
            wait = 30 + (rate_limits * 30)
            rate_limits += 1
            print(f"⚠️ rate limited, waiting {wait}s...")
            time.sleep(wait)
            # Retry once
            result = fetch_ig_profile(username)
            if isinstance(result, str):
                entry = {"username": username, "error": result}
                profiles.append(entry)
                existing[username] = entry
                errors += 1
                print(f"  still failed: {result}")
                continue

        elif isinstance(result, str):
            entry = {"username": username, "error": result}
            profiles.append(entry)
            existing[username] = entry
            errors += 1
            print(f"✗ {result}")
            time.sleep(1)
            continue

        elif result is None:
            entry = {"username": username, "error": "empty_response"}
            profiles.append(entry)
            existing[username] = entry
            errors += 1
            print("✗ empty response")
            time.sleep(1)
            continue

        # Success!
        profile = result
        profile["emails_from_bio"] = extract_emails(profile["bio"])
        profile["url_emails"] = []
        profile["error"] = None

        found += 1
        bio_email = profile["emails_from_bio"][0] if profile["emails_from_bio"] else "none"
        url_short = (profile["external_url"] or "none")[:40]
        print(f"✓ {profile['followers']:,} flw | "
              f"biz={profile['is_business']} | "
              f"email={bio_email} | "
              f"url={url_short}")

        profiles.append(profile)
        existing[username] = profile

        # Save every 20
        if found % 20 == 0:
            with open(progress_file, 'w') as f:
                json.dump(existing, f, indent=2, default=str)
            print(f"  --- saved progress ({found} found, {not_found} not found, {errors} errors) ---")

        time.sleep(random.uniform(2, 4))  # Gentle delay between successful fetches

    # Final save
    with open(progress_file, 'w') as f:
        json.dump(existing, f, indent=2, default=str)

    if skipped:
        print(f"\n  Skipped {skipped} from cache")
    print(f"  Found: {found} | Not found: {not_found} | Rate limited: {rate_limits} | Errors: {errors}")

    return profiles


# ═══════════════════════════════════════════════════════════════════
# PHASE 2: Scrape external URLs for emails
# ═══════════════════════════════════════════════════════════════════
def scrape_urls(profiles, progress_file):
    to_scrape = [p for p in profiles
                 if p.get("external_url") and not p.get("error") and not p.get("url_emails")]

    print(f"\n  {len(to_scrape)} external URLs to scrape\n")

    for i, p in enumerate(to_scrape):
        url = p["external_url"]
        print(f"  [{i+1}/{len(to_scrape)}] @{p['username']} → {url[:55]}...", end=" ", flush=True)

        try:
            emails = scrape_url_for_emails(url)
            p["url_emails"] = emails
            if emails:
                print(f"✓ {emails}")
            else:
                print("no email")
        except Exception as e:
            p["url_emails"] = []
            print(f"error: {str(e)[:40]}")

        time.sleep(random.uniform(1, 2))

    # Save
    existing = {p["username"]: p for p in profiles}
    with open(progress_file, 'w') as f:
        json.dump(existing, f, indent=2, default=str)

    return profiles


# ═══════════════════════════════════════════════════════════════════
# PHASE 3: Output CSV
# ═══════════════════════════════════════════════════════════════════
def output_results(profiles, csv_path, json_path):
    rows = []
    total_with_email = 0

    for p in profiles:
        if p.get("error"):
            continue
        all_emails = set()
        for e in p.get("emails_from_bio", []):
            all_emails.add(e)
        for e in p.get("url_emails", []):
            all_emails.add(e)

        sources = []
        if p.get("emails_from_bio"): sources.append("bio")
        if p.get("url_emails"): sources.append("url")
        if all_emails:
            total_with_email += 1

        for email in all_emails:
            rows.append({
                "username": p["username"],
                "full_name": p.get("full_name", ""),
                "email": email,
                "source": "+".join(sources),
                "bio": (p.get("bio") or "")[:200].replace('\n', ' '),
                "external_url": p.get("external_url", ""),
                "followers": p.get("followers", 0),
                "is_business": p.get("is_business", False),
                "business_category": p.get("business_category", ""),
            })

    fieldnames = ["username", "full_name", "email", "source", "bio",
                  "external_url", "followers", "is_business", "business_category"]
    with open(csv_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    with open(json_path, 'w') as f:
        json.dump(profiles, f, indent=2, default=str)

    valid = [p for p in profiles if not p.get("error")]
    return {
        "total_accounts": len(profiles),
        "valid_profiles": len(valid),
        "not_found": sum(1 for p in profiles if p.get("error") == "not_found"),
        "errors": sum(1 for p in profiles if p.get("error") and p["error"] != "not_found"),
        "with_email": total_with_email,
        "unique_emails": len(set(r["email"] for r in rows)),
        "bio_emails": sum(1 for p in valid if p.get("emails_from_bio")),
        "url_emails": sum(1 for p in valid if p.get("url_emails")),
        "has_url": sum(1 for p in valid if p.get("external_url")),
    }


# ═══════════════════════════════════════════════════════════════════
def main():
    print("╔════════════════════════════════════════════════════════════╗")
    print("║       INSTAGRAM 100-PROFILE SCRAPER v3 (FAST)            ║")
    print(f"║       {datetime.now().strftime('%Y-%m-%d %H:%M:%S'):<53}║")
    print("╚════════════════════════════════════════════════════════════╝")

    base = os.path.dirname(os.path.abspath(__file__))
    progress_file = os.path.join(base, "progress_profiles.json")
    csv_path = os.path.join(base, "instagram_leads_100.csv")
    json_path = os.path.join(base, "instagram_leads_100.json")

    print(f"\n  {len(TARGET_ACCOUNTS)} target accounts loaded")

    # Phase 1
    print("\n" + "="*60)
    print("  PHASE 1: Fetch Instagram Profiles (public API, no retries)")
    print("="*60 + "\n")
    profiles = fetch_all_profiles(TARGET_ACCOUNTS, progress_file)

    # Phase 2
    print("\n" + "="*60)
    print("  PHASE 2: Scrape External URLs for Emails")
    print("="*60)
    profiles = scrape_urls(profiles, progress_file)

    # Phase 3
    print("\n" + "="*60)
    print("  FINAL RESULTS")
    print("="*60)
    stats = output_results(profiles, csv_path, json_path)

    print(f"""
  ── Profile Fetch ──
  Targeted:       {stats['total_accounts']}
  Found:          {stats['valid_profiles']}
  Not found:      {stats['not_found']}
  Other errors:   {stats['errors']}

  ── Email Extraction ──
  Profiles w/ email:  {stats['with_email']}
  Hit rate:           {stats['with_email']}/{stats['valid_profiles']} ({stats['with_email']*100//max(stats['valid_profiles'],1)}%)
  Unique emails:      {stats['unique_emails']}
  From bio:           {stats['bio_emails']}
  From URL scrape:    {stats['url_emails']}
  Had external URL:   {stats['has_url']}

  ── Files ──
  CSV: {csv_path}
  JSON: {json_path}
""")


if __name__ == "__main__":
    main()
